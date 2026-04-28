//! Startup preconditions.
//!
//! The provider's reconcilers watch three CRDs (`BasisCluster`,
//! `BasisMachine`, `BasisMachineTemplate`). If the pod starts before
//! those CRDs are registered on the apiserver — a real race when the
//! Deployment and CRD manifests are applied concurrently — kube-rs's
//! watcher hits its default exponential backoff and sits silently for
//! up to ~5 minutes per watched kind before retrying. We block startup
//! here until every CRD we manage is listable, so that backoff path
//! can never happen in the first place.
//!
//! We probe by calling `list` on the CRs directly, not on
//! `apiextensions.k8s.io/customresourcedefinitions`. That way the check
//! uses the exact same RBAC the controllers need anyway — no extra
//! cluster role binding required.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, ListParams};
use kube::Client;
use kube::ResourceExt;
use serde::de::DeserializeOwned;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};

const CRD_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const CRD_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Block until every CRD the provider watches is listable, or give up
/// after `CRD_WAIT_TIMEOUT` so the pod crash-loops fast (letting k8s
/// show a clear restart signal) rather than staying up idle.
pub async fn wait_for_crds(client: &Client) -> anyhow::Result<()> {
    let deadline = Instant::now() + CRD_WAIT_TIMEOUT;
    wait_for_crd::<BasisCluster>(client, "BasisCluster", deadline).await?;
    wait_for_crd::<BasisMachine>(client, "BasisMachine", deadline).await?;
    wait_for_crd::<BasisMachineTemplate>(client, "BasisMachineTemplate", deadline).await?;
    Ok(())
}

/// Resolve the trust-domain identifier this provider stamps onto every
/// `BasisCluster` it creates. Two-tier lookup:
///
///   1. `BASIS_TRUST_DOMAIN` env var — if set non-empty, use it
///      verbatim. This is how a child cluster inherits its parent's
///      tree (the parent's provider sets the env on the child's
///      provider Deployment at install time) and how an operator
///      overrides the default if they want to merge two roots.
///   2. The `kube-system` Namespace UID — the root fallback. The
///      namespace is created at apiserver bootstrap and its UID is
///      immutable for the cluster's lifetime, so this is stable
///      across provider restarts and CAPI restarts.
///
/// Every `BasisCluster` spawned by the same root therefore shares a
/// tree (and a per-tree VRF on every basis host); clusters spawned by
/// different roots land in different trees and are isolated at the
/// kernel routing level. Operator-invisible at the cluster level —
/// the contract is "two clusters under the same Lattice root can
/// talk; under different roots they can't."
///
/// Fail loud if both lookups fail: running without a trust-domain
/// identity would silently merge every basis cluster the provider
/// touches into the empty-trust-domain tree and break isolation.
pub async fn read_trust_domain(client: &Client) -> anyhow::Result<String> {
    if let Ok(explicit) = std::env::var("BASIS_TRUST_DOMAIN") {
        if !explicit.is_empty() {
            info!(trust_domain = %explicit, "trust domain set via BASIS_TRUST_DOMAIN");
            return Ok(explicit);
        }
    }
    let api: Api<Namespace> = Api::all(client.clone());
    let ns = api
        .get("kube-system")
        .await
        .map_err(|e| anyhow::anyhow!("read kube-system namespace for trust domain: {e}"))?;
    let uid = ns.uid().ok_or_else(|| {
        anyhow::anyhow!(
            "kube-system namespace has no UID — apiserver state is corrupt or non-conformant"
        )
    })?;
    info!(trust_domain = %uid, "trust domain resolved from kube-system namespace UID (root fallback)");
    Ok(uid)
}

async fn wait_for_crd<K>(client: &Client, kind: &str, deadline: Instant) -> anyhow::Result<()>
where
    K: kube::Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
{
    let api: Api<K> = Api::all(client.clone());
    // `limit=1` keeps the probe cheap — we only need a success/NotFound
    // signal, not the actual contents.
    let params = ListParams::default().limit(1);
    loop {
        match api.list(&params).await {
            Ok(_) => {
                info!(crd = %kind, "CRD registered");
                return Ok(());
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out after {CRD_WAIT_TIMEOUT:?} waiting for {kind} CRD to register; \
                         make sure CRD manifests are applied before (or alongside) this Deployment"
                    );
                }
                warn!(crd = %kind, "CRD not registered yet, retrying");
                sleep(CRD_POLL_INTERVAL).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
