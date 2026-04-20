//! `BasisCluster` reconciler.
//!
//! Create flow is driven by two idempotency primitives:
//!   - `Basis.CreateCluster` is idempotent by name server-side (see
//!     basis-controller/src/server.rs). Calling it twice with the same
//!     `(name, ip_pool)` returns the same `(cluster_id, endpoint)`.
//!   - Every reconcile rewrites `spec.controlPlaneEndpoint` and the
//!     `status` fields as merge patches. Writing identical values is a
//!     no-op on the API server, so repeated reconciles converge without
//!     diverging the resource.
//!
//! Delete flow:
//!   1. Call `Basis.DeleteCluster(cluster_id)` (cascades VM deletes + releases VIP)
//!   2. Remove finalizer

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

use crate::basis_client::BasisClient;
use crate::conditions;
use crate::crds::{BasisCluster, ControlPlaneEndpoint, DEFAULT_CONTROL_PLANE_PORT};
use crate::reconcile_util::{merge_spec, merge_status, namespace_of};

const FINALIZER: &str = "basiscluster.infrastructure.cluster.x-k8s.io/finalizer";

pub struct ClusterContext {
    pub client: Client,
    pub basis: Arc<BasisClient>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("finalizer error: {0}")]
    Finalizer(String),

    #[error("basis controller: {0}")]
    Basis(#[from] crate::basis_client::ClientError),
}

pub async fn run(client: Client, basis: Arc<BasisClient>) -> anyhow::Result<()> {
    let api: Api<BasisCluster> = Api::all(client.clone());
    let ctx = Arc::new(ClusterContext { client, basis });

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
    ctx: Arc<ClusterContext>,
) -> Result<Action, ClusterError> {
    let namespace = namespace_of(cluster.as_ref());
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER, cluster, |event| async {
        match event {
            Event::Apply(c) => apply(c, ctx.clone()).await,
            Event::Cleanup(c) => cleanup(c, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| ClusterError::Finalizer(e.to_string()))
}

async fn apply(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ClusterContext>,
) -> Result<Action, ClusterError> {
    let name = cluster.name_any();
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace_of(cluster.as_ref()));
    let generation = cluster.metadata.generation;

    // Server-side CreateCluster is idempotent by name — calling it again
    // with the same (name, ip_pool) returns the existing cluster_id and
    // endpoint. That lets us issue the RPC on every reconcile without
    // worrying about duplicates, which is how we recover from partial
    // writes below.
    info!(cluster = %name, ip_pool = %cluster.spec.ip_pool, "calling Basis.CreateCluster");
    let created = ctx
        .basis
        .create_cluster(name.clone(), cluster.spec.ip_pool.clone())
        .await?;

    // Write status FIRST — `basisClusterId` is the durable marker that a
    // basis-side cluster exists under this name. If the spec patch below
    // fails, the next reconcile still calls CreateCluster (idempotent)
    // and reaches the spec write again.
    let mut conditions = cluster
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    conditions::upsert(&mut conditions, conditions::ready_true("Provisioned", generation));

    merge_status(
        &api,
        &name,
        &json!({
            "status": {
                "basisClusterId": created.cluster_id,
                "ready": true,
                "initialization": { "provisioned": true },
                "conditions": conditions,
            }
        }),
    )
    .await?;

    // Spec carries controlPlaneEndpoint because KubeadmControlPlane reads
    // it from there.
    merge_spec(
        &api,
        &name,
        &json!({
            "spec": {
                "controlPlaneEndpoint": ControlPlaneEndpoint {
                    host: created.control_plane_endpoint.clone(),
                    port: DEFAULT_CONTROL_PLANE_PORT,
                }
            }
        }),
    )
    .await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn cleanup(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ClusterContext>,
) -> Result<Action, ClusterError> {
    if let Some(id) = cluster.status.as_ref().and_then(|s| s.basis_cluster_id.clone()) {
        info!(cluster_id = %id, "calling Basis.DeleteCluster");
        ctx.basis.delete_cluster(id).await?;
    }
    Ok(Action::await_change())
}

fn error_policy(
    _cluster: Arc<BasisCluster>,
    error: &ClusterError,
    _ctx: Arc<ClusterContext>,
) -> Action {
    error!(error = %error, "BasisCluster reconcile error");
    Action::requeue(Duration::from_secs(15))
}
