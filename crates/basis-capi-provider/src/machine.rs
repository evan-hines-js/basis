//! `BasisMachine` reconciler.
//!
//! Create flow:
//!   1. Resolve the owning `Cluster` via CAPI labels.
//!   2. Resolve the owning `Machine` via OwnerReferences and read its
//!      `bootstrap.dataSecretName`.
//!   3. Load bootstrap userdata from the named Secret.
//!   4. Resolve the `BasisCluster` owning this machine and read
//!      `status.basisClusterId` — this is the cluster_id we pass to Basis.
//!   5. Call `Basis.CreateMachine(cluster_id, ...)`.
//!   6. Write status, then spec (providerID).
//!
//! Idempotency lives at the Basis API boundary: `CreateMachine` is
//! idempotent by `(cluster_id, name)`, so retries after a partial
//! failure return the existing VM rather than creating a duplicate.
//!
//! Delete flow:
//!   1. If `status.basisVmId` is set, call `Basis.DeleteMachine`.
//!   2. Remove finalizer.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::{Api, ResourceExt};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{finalizer, Event};
use kube::runtime::watcher;
use kube::Client;
use serde_json::json;
use tracing::{error, info, warn};

use crate::basis_client::{self, BasisClient};
use crate::bootstrap;
use crate::conditions;
use crate::crds::{BasisCluster, BasisMachine, Machine as CapiMachine, MachineAddress};
use crate::reconcile_util::{merge_spec, merge_status, namespace_of};

const FINALIZER: &str = "basismachine.infrastructure.cluster.x-k8s.io/finalizer";
/// Label CAPI places on every Machine/BasisMachine naming the cluster
/// they belong to.
const CLUSTER_LABEL: &str = "cluster.x-k8s.io/cluster-name";

pub struct MachineContext {
    pub client: Client,
    pub basis: Arc<BasisClient>,
}

#[derive(Debug, thiserror::Error)]
pub enum MachineError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("finalizer error: {0}")]
    Finalizer(String),

    #[error("bootstrap: {0}")]
    Bootstrap(#[from] bootstrap::BootstrapError),

    #[error("basis controller: {0}")]
    Basis(#[from] basis_client::ClientError),

    #[error("missing required field: {0}")]
    Missing(&'static str),

    #[error("BasisCluster '{0}' has no basisClusterId yet — retrying")]
    ClusterNotReady(String),
}

pub async fn run(client: Client, basis: Arc<BasisClient>) -> anyhow::Result<()> {
    let api: Api<BasisMachine> = Api::all(client.clone());
    let ctx = Arc::new(MachineContext { client, basis });

    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => info!(?obj, "reconciled BasisMachine"),
                Err(e) => warn!(error = %e, "BasisMachine reconcile error"),
            }
        })
        .await;
    // See the sibling comment in cluster::run — surface watch-stream
    // termination as an error so the pod restarts.
    Err(anyhow::anyhow!("BasisMachine watch stream terminated"))
}

async fn reconcile(
    machine: Arc<BasisMachine>,
    ctx: Arc<MachineContext>,
) -> Result<Action, MachineError> {
    let namespace = namespace_of(machine.as_ref());
    let api: Api<BasisMachine> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER, machine, |event| async {
        match event {
            Event::Apply(m) => apply(m, ctx.clone(), &namespace).await,
            Event::Cleanup(m) => cleanup(m, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| MachineError::Finalizer(e.to_string()))
}

async fn apply(
    machine: Arc<BasisMachine>,
    ctx: Arc<MachineContext>,
    namespace: &str,
) -> Result<Action, MachineError> {
    let name = machine.name_any();
    let api: Api<BasisMachine> = Api::namespaced(ctx.client.clone(), namespace);
    let generation = machine.metadata.generation;

    let cluster_name = machine
        .labels()
        .get(CLUSTER_LABEL)
        .cloned()
        .ok_or(MachineError::Missing("cluster-name label"))?;

    let basis_cluster_id = resolve_basis_cluster_id(&ctx.client, namespace, &cluster_name).await?;

    let bootstrap_secret = find_bootstrap_secret(&ctx.client, namespace, &name).await?;
    let bootstrap_data =
        bootstrap::load_bootstrap_data(ctx.client.clone(), namespace, &bootstrap_secret).await?;

    // Server-side CreateMachine is idempotent by `(cluster_id, name)`.
    // Issuing it on every reconcile lets us self-heal partial writes:
    // if a prior attempt created the VM but crashed before patching
    // status/spec, the next call returns the existing row and we finish
    // the patches below.
    info!(machine = %name, cluster_id = %basis_cluster_id, "calling Basis.CreateMachine");
    let created = ctx
        .basis
        .create_machine(basis_cluster_id, name.clone(), &machine.spec, bootstrap_data)
        .await?;

    // If status/spec patches fail after a successful CreateMachine, the
    // basis-side VM exists but k8s doesn't know its vm_id — on BasisMachine
    // deletion, `cleanup()` skips DeleteMachine for lack of basis_vm_id and
    // the VM leaks. Roll back the create so the next reconcile starts
    // clean. `create_machine` is idempotent by name, so if the rollback
    // itself fails (e.g. controller unreachable), the next reconcile will
    // find the ghost and either finish patching it or delete it via the
    // CAPI deletion path.
    if let Err(e) = write_machine_patches(&api, &name, &created, &machine, generation).await {
        warn!(
            machine = %name,
            vm_id = %created.id,
            error = %e,
            "patches failed after CreateMachine; rolling back basis-side VM",
        );
        if let Err(rb) = ctx.basis.delete_machine(created.id.clone()).await {
            warn!(
                machine = %name,
                vm_id = %created.id,
                error = %rb,
                "rollback DeleteMachine failed; leaving cleanup to next reconcile",
            );
        }
        return Err(e);
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Patch status (with the `basisVmId` marker) first, then spec. Status
/// carries the durable id; writing it before spec means a crash between
/// the two patches is self-healing on the next reconcile — the basis-side
/// VM is re-created idempotently and spec is finished. The opposite order
/// would leave spec populated without the id k8s uses to clean up.
async fn write_machine_patches(
    api: &Api<BasisMachine>,
    name: &str,
    created: &crate::basis_client::CreatedMachine,
    machine: &BasisMachine,
    generation: Option<i64>,
) -> Result<(), MachineError> {
    let mut conditions = machine
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    conditions::upsert(&mut conditions, conditions::ready_true("VMRunning", generation));

    merge_status(
        api,
        name,
        &json!({
            "status": {
                "ready": true,
                "initialization": { "provisioned": true },
                "providerId": created.provider_id,
                "basisVmId": created.id,
                "addresses": [MachineAddress {
                    kind: "InternalIP".to_string(),
                    address: created.ip_address.clone(),
                }],
                "conditions": conditions,
            }
        }),
    )
    .await?;

    merge_spec(
        api,
        name,
        &json!({ "spec": { "providerId": created.provider_id } }),
    )
    .await?;

    Ok(())
}

async fn cleanup(
    machine: Arc<BasisMachine>,
    ctx: Arc<MachineContext>,
) -> Result<Action, MachineError> {
    if let Some(id) = machine.status.as_ref().and_then(|s| s.basis_vm_id.clone()) {
        info!(vm_id = %id, "deleting VM in Basis controller");
        ctx.basis.delete_machine(id).await?;
    }
    Ok(Action::await_change())
}

/// Find the BasisCluster matching `cluster_name` and return its
/// `status.basisClusterId`. The BasisCluster reconciler is responsible
/// for populating that field by calling `Basis.CreateCluster`.
async fn resolve_basis_cluster_id(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> Result<String, MachineError> {
    let api: Api<BasisCluster> = Api::namespaced(client.clone(), namespace);
    let cluster = api.get(cluster_name).await?;
    cluster
        .status
        .as_ref()
        .and_then(|s| s.basis_cluster_id.clone())
        .ok_or_else(|| MachineError::ClusterNotReady(cluster_name.to_string()))
}

/// Find the CAPI `Machine` owner of `basis_machine_name` and return its
/// `spec.bootstrap.dataSecretName`.
async fn find_bootstrap_secret(
    client: &Client,
    namespace: &str,
    basis_machine_name: &str,
) -> Result<String, MachineError> {
    let bm_api: Api<BasisMachine> = Api::namespaced(client.clone(), namespace);
    let bm = bm_api.get(basis_machine_name).await?;

    let owner = bm
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "Machine"))
        .ok_or(MachineError::Missing("owning Machine OwnerReference"))?;

    let machines: Api<CapiMachine> = Api::namespaced(client.clone(), namespace);
    let machine = machines.get(&owner.name).await?;
    machine
        .spec
        .bootstrap
        .data_secret_name
        .ok_or(MachineError::Missing("Machine.spec.bootstrap.dataSecretName"))
}

fn error_policy(
    _machine: Arc<BasisMachine>,
    error: &MachineError,
    _ctx: Arc<MachineContext>,
) -> Action {
    // ClusterNotReady is expected transient state — short requeue, no noise.
    if matches!(error, MachineError::ClusterNotReady(_)) {
        return Action::requeue(Duration::from_secs(5));
    }
    error!(error = %error, "BasisMachine reconcile error");
    Action::requeue(Duration::from_secs(15))
}
