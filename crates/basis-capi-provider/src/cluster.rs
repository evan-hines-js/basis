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

use crate::client_cache::{BasisClientCache, CacheError};
use crate::conditions;
use crate::crds::BasisCluster;
use crate::reconcile_util::{merge_spec, merge_status, namespace_of};

const FINALIZER: &str = "basiscluster.infrastructure.cluster.x-k8s.io/finalizer";

pub struct ClusterContext {
    pub client: Client,
    pub clients: Arc<BasisClientCache>,
}

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
}

pub async fn run(client: Client, clients: Arc<BasisClientCache>) -> anyhow::Result<()> {
    let api: Api<BasisCluster> = Api::all(client.clone());
    let ctx = Arc::new(ClusterContext { client, clients });

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
    let namespace = namespace_of(cluster.as_ref());
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let generation = cluster.metadata.generation;

    let basis = ctx
        .clients
        .get(&cluster.spec.credentials_ref, &namespace)
        .await?;

    // Server-side `CreateCluster` allocates the VIP and is idempotent
    // by name: retrying returns the same `(cluster_id, endpoint)` pair,
    // so we can issue the RPC on every reconcile and self-heal from
    // partial writes below without ever creating duplicate clusters.
    info!(
        cluster = %name,
        ip_pool = %cluster.spec.ip_pool,
        "calling Basis.CreateCluster"
    );
    let created = basis
        .create_cluster(name.clone(), cluster.spec.ip_pool.clone())
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
                "ready": true,
                "initialization": { "provisioned": true },
                "conditions": conditions,
            }
        }),
    )
    .await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// kubeadm's apiserver always listens on 6443. We don't expose this
/// as a spec knob because the kube-vip static pod manifest the control
/// plane ships is hard-wired to the same number — making it
/// configurable would require threading the port through bootstrap
/// data templating and buys nothing for a kubeadm cluster.
const KUBE_API_PORT: u16 = 6443;

async fn cleanup(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ClusterContext>,
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

fn error_policy(
    _cluster: Arc<BasisCluster>,
    error: &ClusterError,
    _ctx: Arc<ClusterContext>,
) -> Action {
    error!(error = %error, "BasisCluster reconcile error");
    Action::requeue(Duration::from_secs(15))
}
