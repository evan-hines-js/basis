//! Workload-cluster kube clients + SSA apply for the per-cluster
//! Cilium BGP CRDs.
//!
//! basis-capi-provider runs in the management cluster. CAPI core
//! writes `<cluster-name>-kubeconfig` as a Secret in the same
//! namespace as the BasisCluster once kubeadm has minted the cluster
//! CA. We read that Secret, parse the kubeconfig, and build a kube
//! Client pointed at the workload cluster's apiserver.
//!
//! The connection is short-lived and per-reconcile — no caching here.
//! BasisCluster reconciliation runs at most every five minutes
//! steady-state, and the alternative (a per-cluster client cache)
//! would have to invalidate on apiserver-IP changes (CP failover,
//! kube-vip migration). Per-reconcile re-resolution is simpler and
//! the kubeconfig Secret is hot in the apiserver's etcd, so this is
//! cheap.
//!
//! On apply, we use server-side-apply with field-manager
//! `basis-capi-provider`. SSA gives us idempotent re-application
//! across reconciles and avoids field-ownership conflicts with any
//! human-edited CRDs.

use kube::api::{DynamicObject, Patch, PatchParams};
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use kube::core::GroupVersionKind;
use kube::discovery::ApiResource;
use kube::{Api, Client, ResourceExt};

use crate::cluster::ClusterError;

/// Field manager string used on every server-side-apply this module
/// performs. Distinct from the management-side reconciler's manager
/// so the two never fight for ownership of the same field.
const FIELD_MANAGER: &str = "basis-capi-provider-bgp";

/// Build a kube Client to the workload cluster named `cluster_name`,
/// reading `<cluster_name>-kubeconfig` from `namespace` (the same
/// namespace as the BasisCluster). Returns `None` if the Secret
/// isn't present yet (CAPI hasn't bootstrapped the cluster's apiserver
/// CA — the caller treats this as "not ready, retry later").
pub async fn workload_client(
    mgmt: &Client,
    cluster_name: &str,
    namespace: &str,
) -> Result<Option<Client>, ClusterError> {
    let secret_name = format!("{cluster_name}-kubeconfig");
    let secrets: Api<k8s_openapi::api::core::v1::Secret> = Api::namespaced(mgmt.clone(), namespace);
    let Some(secret) = secrets
        .get_opt(&secret_name)
        .await
        .map_err(|e| ClusterError::Internal(format!("getting {secret_name}: {e}")))?
    else {
        return Ok(None);
    };
    // CAPI stores the kubeconfig YAML under `data["value"]`. Anything
    // else means a different controller wrote this Secret and we
    // shouldn't be using it. Refuse rather than try alternative
    // keys — if the shape changes, fail fast.
    let bytes = secret
        .data
        .as_ref()
        .and_then(|d| d.get("value"))
        .ok_or_else(|| {
            ClusterError::Internal(format!(
                "Secret {namespace}/{secret_name} missing data[\"value\"] (not a CAPI kubeconfig?)"
            ))
        })?;
    let kubeconfig: Kubeconfig = serde_yaml_ng::from_slice(&bytes.0)
        .map_err(|e| ClusterError::Internal(format!("parsing kubeconfig: {e}")))?;
    let config = Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default())
        .await
        .map_err(|e| ClusterError::Internal(format!("loading kubeconfig: {e}")))?;
    let client = Client::try_from(config)
        .map_err(|e| ClusterError::Internal(format!("building workload client: {e}")))?;
    Ok(Some(client))
}

/// Server-side-apply the rendered Cilium BGP CRDs to the workload
/// cluster. Each manifest in `docs` is one JSON document already
/// shaped as a `cilium.io/v2alpha1` CRD instance (output of
/// [`crate::cilium_bgp::render_bgp_crds`]).
///
/// Returns `Ok(())` on success. Per-resource failures are logged-and-
/// skipped so a transient failure on one CRD doesn't prevent the
/// others from landing — the next reconcile retries. A genuinely
/// stuck CRD (e.g. CRD-not-found because Cilium hasn't installed
/// yet) gets the same retry treatment, so the apply path is
/// effectively a "wait until Cilium is ready" loop.
pub async fn apply_bgp_crds(workload: &Client, docs: &[String]) -> Result<(), ClusterError> {
    for doc in docs {
        let mut obj: DynamicObject = serde_json::from_str(doc)
            .map_err(|e| ClusterError::Internal(format!("parsing BGP CRD doc: {e}")))?;
        let api_version = obj
            .types
            .as_ref()
            .map(|t| t.api_version.clone())
            .ok_or_else(|| ClusterError::Internal("BGP CRD doc missing apiVersion".to_string()))?;
        let kind = obj
            .types
            .as_ref()
            .map(|t| t.kind.clone())
            .ok_or_else(|| ClusterError::Internal("BGP CRD doc missing kind".to_string()))?;
        // Split `cilium.io/v2alpha1` into (group, version) for the
        // GVK constructor. apiVersion lacking a `/` (e.g. core `v1`)
        // would mean group="", but Cilium CRDs always carry a group
        // so a missing slash is a malformed input.
        let (group, version) = api_version.split_once('/').ok_or_else(|| {
            ClusterError::Internal(format!(
                "BGP CRD apiVersion {api_version:?} missing group/version separator"
            ))
        })?;
        let gvk = GroupVersionKind::gvk(group, version, &kind);
        // Cilium's BGP CRDs are cluster-scoped, so plural is derivable
        // from the kind; we don't need a discovery round-trip.
        let api_resource = ApiResource::from_gvk(&gvk);
        let api: Api<DynamicObject> = Api::all_with(workload.clone(), &api_resource);
        let name = obj.name_any();
        // SSA wants the apply payload without `resourceVersion` /
        // `managedFields` (we own the field manager) so strip the
        // metadata noise that came out of the renderer.
        obj.metadata.resource_version = None;
        obj.metadata.managed_fields = None;
        let params = PatchParams::apply(FIELD_MANAGER).force();
        if let Err(e) = api.patch(&name, &params, &Patch::Apply(&obj)).await {
            // Cilium's CRDs may not be registered yet on a fresh
            // workload cluster (the bootstrap bundle installs them
            // but ordering is not guaranteed). Treat as a soft
            // failure; the next reconcile retries.
            tracing::warn!(
                kind = %kind,
                name = %name,
                error = %e,
                "BGP CRD apply failed; will retry on next reconcile"
            );
        } else {
            tracing::debug!(kind = %kind, name = %name, "BGP CRD applied");
        }
    }
    Ok(())
}
