//! `BasisCluster` reconciler.
//!
//! Create flow is driven by two idempotency primitives:
//!   - `Basis.CreateCluster` is idempotent by name server-side (see
//!     basis-controller/src/server.rs). Calling it twice with the same
//!     `(name, parent_cluster_id)` returns the same result.
//!   - Every reconcile rewrites `spec.controlPlaneEndpoint` and the
//!     `status` fields as merge patches. Writing identical values is a
//!     no-op on the API server, so repeated reconciles converge without
//!     diverging the resource.
//!
//! Delete flow:
//!   1. Call `Basis.DeleteCluster(cluster_id)` (cascades VM deletes + releases VIP)
//!   2. Remove finalizer
//!
//! Failure surfacing: every apply/cleanup error is reflected back onto
//! the BasisCluster as a `Ready=False` condition + `failureMessage`.
//! Credential-shaped failures additionally invalidate the cached
//! `BasisClient` so a fixed Secret is picked up on the next reconcile.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{finalizer, Event};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use serde_json::json;
use tracing::{error, info, warn};

use crate::client_cache::CacheError;
use crate::conditions;
use basis_client::ClusterRequest;

use crate::crds::{BasisCluster, ParentClusterRef};
use crate::reconciler::{
    merge_spec, merge_status, namespace_of, record_failure_status, ProviderContext, ReconcileError,
};

const FINALIZER: &str = "basiscluster.infrastructure.cluster.x-k8s.io/finalizer";

/// kubeadm's apiserver always listens on 6443. We don't expose this
/// as a spec knob because the kube-vip static pod manifest the control
/// plane ships is hard-wired to the same number — making it
/// configurable would require threading the port through bootstrap
/// data templating and buys nothing for a kubeadm cluster.
const KUBE_API_PORT: u16 = 6443;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("finalizer error: {0}")]
    Finalizer(String),

    #[error("basis controller: {0}")]
    Basis(#[from] basis_client::ClientError),

    #[error("resolving credentials: {0}")]
    Credentials(#[from] CacheError),

    #[error("parent cluster '{0}' has no basisClusterId yet — retrying")]
    ParentNotReady(String),
}

impl ReconcileError for ClusterError {
    fn condition_reason(&self) -> &'static str {
        match self {
            ClusterError::Kube(_) => "KubeApiError",
            ClusterError::Finalizer(_) => "FinalizerError",
            ClusterError::Basis(e) if e.is_credentials_problem() => "BasisCredentialsInvalid",
            ClusterError::Basis(e) if e.is_transient() => "BasisBackpressure",
            ClusterError::Basis(_) => "BasisRpcError",
            ClusterError::Credentials(_) => "BasisCredentialsInvalid",
            ClusterError::ParentNotReady(_) => "ParentNotReady",
        }
    }

    fn is_credentials_problem(&self) -> bool {
        matches!(self, ClusterError::Credentials(_))
            || matches!(self, ClusterError::Basis(e) if e.is_credentials_problem())
    }

    fn is_transient(&self) -> bool {
        matches!(self, ClusterError::Basis(e) if e.is_transient())
            || matches!(self, ClusterError::ParentNotReady(_))
    }
}

/// Resolve a parent `BasisCluster` reference to the parent's
/// basis-side cluster id. Returns `None` when no parent is configured
/// (the cluster is a tree root). Errors with `ParentNotReady` if the
/// referenced parent exists but hasn't finished its own create yet —
/// the error policy requeues quickly on that.
async fn resolve_parent_id(
    client: &Client,
    own_namespace: &str,
    parent: Option<&ParentClusterRef>,
) -> Result<Option<String>, ClusterError> {
    let Some(parent) = parent else { return Ok(None) };
    let ns = parent.namespace.as_deref().unwrap_or(own_namespace);
    let api: Api<BasisCluster> = Api::namespaced(client.clone(), ns);
    let pcr = api.get(&parent.name).await?;
    let id = pcr
        .status
        .as_ref()
        .and_then(|s| s.basis_cluster_id.clone())
        .ok_or_else(|| ClusterError::ParentNotReady(pcr.name_any()))?;
    Ok(Some(id))
}

pub async fn run(
    client: Client,
    clients: Arc<crate::client_cache::BasisClientCache>,
) -> anyhow::Result<()> {
    let api: Api<BasisCluster> = Api::all(client.clone());
    let ctx = Arc::new(ProviderContext { client, clients });

    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!(?obj, "reconciled BasisCluster"),
                Err(e) => warn!(error = %e, "BasisCluster reconcile error"),
            }
        })
        .await;
    // The watch stream terminated — kube-runtime only returns from
    // for_each when the apiserver connection is irrecoverable. Surface
    // this as an error so the process exits and gets restarted by
    // Kubernetes rather than silently continuing with a dead watcher.
    Err(anyhow::anyhow!("BasisCluster watch stream terminated"))
}

async fn reconcile(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ProviderContext>,
) -> Result<Action, ClusterError> {
    let namespace = namespace_of(cluster.as_ref());
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER, cluster, |event| async move {
        let (resource, action) = match &event {
            Event::Apply(c) => (c.clone(), apply(c.clone(), ctx.clone()).await),
            Event::Cleanup(c) => (c.clone(), cleanup(c.clone(), ctx.clone()).await),
        };
        if let Err(err) = &action {
            on_failure(&resource, ctx.clone(), &namespace, err).await;
        }
        action
    })
    .await
    .map_err(|e| ClusterError::Finalizer(e.to_string()))
}

async fn apply(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ProviderContext>,
) -> Result<Action, ClusterError> {
    let name = cluster.name_any();
    let namespace = namespace_of(cluster.as_ref());
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let generation = cluster.metadata.generation;

    let basis = ctx
        .clients
        .get(&cluster.spec.credentials_ref, &namespace)
        .await?;

    // Resolve the parent's basis-side cluster id (if any). The referenced
    // `BasisCluster` must have finished its own create first —
    // `ClusterNotReady` surfaces through the error policy and requeues
    // quickly.
    let parent_cluster_id =
        resolve_parent_id(&ctx.client, &namespace, cluster.spec.parent_cluster_ref.as_ref())
            .await?;

    // Server-side `CreateCluster` materialises (or joins) a tree,
    // allocates a VIP, and is idempotent by name: retrying returns the
    // same result.
    info!(
        cluster = %name,
        parent = ?parent_cluster_id,
        "calling Basis.CreateCluster"
    );
    let created = basis
        .create_cluster(ClusterRequest {
            name: name.clone(),
            parent_cluster_id: parent_cluster_id.clone(),
        })
        .await?;

    // Write the allocated endpoint to spec first: CAPI core watches
    // `BasisCluster.spec.controlPlaneEndpoint` and propagates the value
    // onto `Cluster.spec.controlPlaneEndpoint`, which is what every
    // downstream controller (kubeadm control plane, kube-vip patcher,
    // etc.) keys off of. Status writes go second so a crash between
    // the two is self-healing: next reconcile re-applies spec
    // idempotently and finishes the status patch.
    merge_spec(
        &api,
        &name,
        &json!({
            "spec": {
                "controlPlaneEndpoint": {
                    "host": created.control_plane_endpoint,
                    "port": KUBE_API_PORT,
                }
            }
        }),
    )
    .await?;

    let mut conditions = cluster
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    conditions::upsert(
        &mut conditions,
        conditions::ready_true("Provisioned", generation),
    );

    merge_status(
        &api,
        &name,
        &json!({
            "status": {
                "basisClusterId": created.cluster_id,
                "treeId": created.tree_id,
                "vni": created.vni,
                "ready": true,
                "initialization": { "provisioned": true },
                "failureMessage": serde_json::Value::Null,
                "conditions": conditions,
            }
        }),
    )
    .await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn cleanup(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ProviderContext>,
) -> Result<Action, ClusterError> {
    if let Some(id) = cluster
        .status
        .as_ref()
        .and_then(|s| s.basis_cluster_id.clone())
    {
        let namespace = namespace_of(cluster.as_ref());
        let basis = ctx
            .clients
            .get(&cluster.spec.credentials_ref, &namespace)
            .await?;
        info!(cluster_id = %id, "calling Basis.DeleteCluster");
        basis.delete_cluster(id).await?;
    }
    Ok(Action::await_change())
}

/// Reflect a failed apply/cleanup back onto the BasisCluster as a
/// `Ready=False` condition + `failureMessage`, and invalidate the
/// cached `BasisClient` if the failure is credential-shaped.
///
/// Best-effort: errors here are logged but never propagated.
async fn on_failure(
    cluster: &BasisCluster,
    ctx: Arc<ProviderContext>,
    namespace: &str,
    err: &ClusterError,
) {
    if err.is_credentials_problem() {
        ctx.clients
            .invalidate(&cluster.spec.credentials_ref, namespace)
            .await;
        info!(
            cluster = %cluster.name_any(),
            "invalidated cached BasisClient after credentials failure"
        );
    }

    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), namespace);
    let existing_conditions = cluster
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    record_failure_status(
        &api,
        &cluster.name_any(),
        cluster.metadata.generation,
        existing_conditions,
        err,
    )
    .await;
}

fn error_policy(
    _cluster: Arc<BasisCluster>,
    error: &ClusterError,
    _ctx: Arc<ProviderContext>,
) -> Action {
    if error.is_transient() {
        // Controller-side backpressure: client already burned its
        // retry budget, don't page operators for this. Short requeue
        // so recovery is fast once the controller drains.
        info!(error = %error, "BasisCluster transient backpressure, requeueing");
        return Action::requeue(Duration::from_secs(5));
    }
    error!(error = %error, "BasisCluster reconcile error");
    Action::requeue(Duration::from_secs(15))
}
