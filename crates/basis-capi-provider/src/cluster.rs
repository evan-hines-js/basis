//! `BasisCluster` reconciler.
//!
//! Create flow:
//!   1. Call `Basis.CreateCluster(name, ipPool)` → get cluster_id + VIP
//!   2. Write `status.basisClusterId` and `spec.controlPlaneEndpoint`
//!   3. Mark provisioned + ready
//!
//! Delete flow:
//!   1. Call `Basis.DeleteCluster(cluster_id)` (cascades VM deletes + releases VIP)
//!   2. Remove finalizer
//!
//! Idempotent: if `status.basisClusterId` is already set, we only refresh
//! the Ready condition.

use std::sync::Arc;
use std::time::Duration;

use basis_common::time::now_rfc3339;
use futures::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{finalizer, Event};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use serde_json::json;
use tracing::{error, info, warn};

use crate::basis_client::BasisClient;
use crate::crds::{BasisCluster, Condition, ControlPlaneEndpoint, DEFAULT_CONTROL_PLANE_PORT};

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
    Ok(())
}

async fn reconcile(
    cluster: Arc<BasisCluster>,
    ctx: Arc<ClusterContext>,
) -> Result<Action, ClusterError> {
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
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
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let api: Api<BasisCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    // Idempotency: once provisioned, we're done.
    let already_provisioned = cluster
        .status
        .as_ref()
        .and_then(|s| s.basis_cluster_id.as_ref())
        .is_some();
    if already_provisioned {
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    info!(cluster = %name, ip_pool = %cluster.spec.ip_pool, "calling Basis.CreateCluster");
    let created = ctx
        .basis
        .create_cluster(name.clone(), cluster.spec.ip_pool.clone())
        .await?;

    // Patch status + spec. spec carries controlPlaneEndpoint because
    // KubeadmControlPlane reads it from there.
    let spec_patch = json!({
        "spec": {
            "controlPlaneEndpoint": ControlPlaneEndpoint {
                host: created.control_plane_endpoint.clone(),
                port: DEFAULT_CONTROL_PLANE_PORT,
            }
        }
    });
    api.patch(&name, &PatchParams::default(), &Patch::Merge(&spec_patch))
        .await?;

    let status_patch = json!({
        "status": {
            "basisClusterId": created.cluster_id,
            "ready": true,
            "initialization": { "provisioned": true },
            "conditions": [Condition {
                kind: "Ready".to_string(),
                status: "True".to_string(),
                reason: Some("Provisioned".to_string()),
                message: None,
                last_transition_time: now_rfc3339(),
            }],
        }
    });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&status_patch))
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
