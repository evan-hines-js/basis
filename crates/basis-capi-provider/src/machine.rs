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
//!
//! Failure surfacing: every apply/cleanup error is reflected back onto
//! the `BasisMachine` as a `Ready=False` condition + `failureMessage`
//! before the error propagates. Credential-shaped failures additionally
//! invalidate the cached `BasisClient` so a fixed Secret is picked up
//! on the next reconcile without restarting the pod.

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

use basis_client::{ClientError, CreatedMachine, MachineRequest};
use basis_proto::{Machine as BasisVmState, MachineState};

use crate::bootstrap;
use crate::client_cache::CacheError;
use crate::conditions;
use crate::crds::{
    BasisCluster, BasisMachine, CredentialsRef, Machine as CapiMachine, MachineAddress,
};
use crate::reconciler::{
    merge_spec, merge_status, namespace_of, record_failure_status, ProviderContext, ReconcileError,
};

const FINALIZER: &str = "basismachine.infrastructure.cluster.x-k8s.io/finalizer";
/// Label CAPI places on every Machine/BasisMachine naming the cluster
/// they belong to.
const CLUSTER_LABEL: &str = "cluster.x-k8s.io/cluster-name";

#[derive(Debug, thiserror::Error)]
pub enum MachineError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("finalizer error: {0}")]
    Finalizer(String),

    #[error("bootstrap: {0}")]
    Bootstrap(#[from] bootstrap::BootstrapError),

    #[error("basis controller: {0}")]
    Basis(#[from] ClientError),

    #[error("resolving credentials: {0}")]
    Credentials(#[from] CacheError),

    #[error("missing required field: {0}")]
    Missing(&'static str),

    #[error("BasisCluster '{0}' has no basisClusterId yet — retrying")]
    ClusterNotReady(String),

    /// Basis reported the VM is in `FAILED` state. Terminal from basis's
    /// point of view — basis does not restart VMs; recovery is replacement
    /// orchestrated by CAPI. Surfacing this as an error routes through
    /// `on_failure` → `record_failure_status`, which patches
    /// `failureMessage` and `Ready=False`. CAPI's Machine controller then
    /// propagates to `Machine.status.failureMessage`, at which point a
    /// MachineHealthCheck / MachineDeployment on the consumer side
    /// replaces the machine.
    #[error("VM reported failed state: {0}")]
    VmFailed(String),
}

impl ReconcileError for MachineError {
    fn condition_reason(&self) -> &'static str {
        match self {
            MachineError::Kube(_) => "KubeApiError",
            MachineError::Finalizer(_) => "FinalizerError",
            MachineError::Bootstrap(_) => "BootstrapNotReady",
            MachineError::Basis(e) if e.is_credentials_problem() => "BasisCredentialsInvalid",
            MachineError::Basis(e) if e.is_transient() => "BasisBackpressure",
            MachineError::Basis(_) => "BasisRpcError",
            MachineError::Credentials(_) => "BasisCredentialsInvalid",
            MachineError::Missing(_) => "Misconfigured",
            MachineError::ClusterNotReady(_) => "ClusterNotReady",
            MachineError::VmFailed(_) => "VMFailed",
        }
    }

    fn is_credentials_problem(&self) -> bool {
        matches!(self, MachineError::Credentials(_))
            || matches!(self, MachineError::Basis(e) if e.is_credentials_problem())
    }

    fn is_transient(&self) -> bool {
        matches!(self, MachineError::Basis(e) if e.is_transient())
    }
}

pub async fn run(
    client: Client,
    clients: Arc<crate::client_cache::BasisClientCache>,
) -> anyhow::Result<()> {
    let api: Api<BasisMachine> = Api::all(client.clone());
    let ctx = Arc::new(ProviderContext { client, clients });

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
    ctx: Arc<ProviderContext>,
) -> Result<Action, MachineError> {
    let namespace = namespace_of(machine.as_ref());
    let api: Api<BasisMachine> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER, machine, |event| async move {
        let (resource, action) = match &event {
            Event::Apply(m) => (m.clone(), apply(m.clone(), ctx.clone(), &namespace).await),
            Event::Cleanup(m) => (m.clone(), cleanup(m.clone(), ctx.clone()).await),
        };
        if let Err(err) = &action {
            on_failure(&resource, ctx.clone(), &namespace, err).await;
        }
        action
    })
    .await
    .map_err(|e| MachineError::Finalizer(e.to_string()))
}

async fn apply(
    machine: Arc<BasisMachine>,
    ctx: Arc<ProviderContext>,
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

    let ClusterRef {
        basis_cluster_id,
        credentials_ref,
    } = resolve_cluster_ref(&ctx.client, namespace, &cluster_name).await?;
    let basis = ctx.clients.get(&credentials_ref, namespace).await?;

    // Steady-state observation path: once we have the basis-side VM id,
    // CreateMachine adds nothing (provider_id is derivable, spec is
    // CAPI-immutable, and the server only returns success once the agent
    // has reported RUNNING). Sample live state via GetMachine so a VM
    // that dies *after* create (guest kernel panic, cloud-hypervisor
    // crash, OOM) surfaces as a failure on the BasisMachine. CAPI's
    // Machine controller then propagates to `Machine.status`, at which
    // point a MachineHealthCheck / MachineDeployment on the consumer
    // side replaces the machine — standard CAPI flow, no Basis-specific
    // hook required.
    if let Some(vm_id) = machine.status.as_ref().and_then(|s| s.basis_vm_id.clone()) {
        let vm = basis.get_machine(vm_id).await?;
        return observe_vm(&api, &name, &vm, &machine, generation).await;
    }

    let bootstrap_secret = find_bootstrap_secret(&ctx.client, namespace, &name).await?;
    let bootstrap_data =
        bootstrap::load_bootstrap_data(ctx.client.clone(), namespace, &bootstrap_secret).await?;

    info!(machine = %name, cluster_id = %basis_cluster_id, "calling Basis.CreateMachine");
    let created = basis
        .create_machine(MachineRequest {
            cluster_id: basis_cluster_id,
            name: name.clone(),
            cpu: machine.spec.cpu,
            memory_mib: machine.spec.memory_mib,
            disk_gib: machine.spec.disk_gib,
            image: machine.spec.image.clone(),
            bootstrap_data,
            gpus: machine.spec.gpus,
            min_gpu_group_size: machine
                .spec
                .gpu_constraints
                .as_ref()
                .map(|c| c.min_group_size),
            extra_disk_gibs: machine.spec.extra_disk_gibs.clone(),
        })
        .await?;

    // If status/spec patches fail after a successful CreateMachine, the
    // basis-side VM exists but k8s doesn't know its vm_id — on BasisMachine
    // deletion, `cleanup()` skips DeleteMachine for lack of basis_vm_id and
    // the VM leaks. Roll back the create so the next reconcile starts
    // clean. `create_machine` is idempotent by name, so if the rollback
    // itself fails (e.g. controller unreachable), the next reconcile will
    // find the ghost and either finish patching it or delete it via the
    // CAPI deletion path.
    if let Err(e) = write_success_status(&api, &name, &created, &machine, generation).await {
        warn!(
            machine = %name,
            vm_id = %created.id,
            error = %e,
            "patches failed after CreateMachine; rolling back basis-side VM",
        );
        if let Err(rb) = basis.delete_machine(created.id.clone()).await {
            warn!(
                machine = %name,
                vm_id = %created.id,
                error = %rb,
                "rollback DeleteMachine failed; leaving cleanup to next reconcile",
            );
        }
        return Err(e);
    }

    Ok(Action::requeue(Duration::from_secs(60)))
}

/// Steady-state state dispatch for a VM whose id we already hold. One
/// RPC (`GetMachine`) per reconcile tick; the cadence (see `apply`'s
/// requeue) is the bound on failure-detection latency.
async fn observe_vm(
    api: &Api<BasisMachine>,
    name: &str,
    vm: &BasisVmState,
    machine: &BasisMachine,
    generation: Option<i64>,
) -> Result<Action, MachineError> {
    // prost-generated enum conversion: unknown discriminants would only
    // appear on a cross-version skew between controller and provider,
    // which we don't support in the same deploy. Map anything we can't
    // decode to a short-requeue transitional state rather than crash.
    let state = MachineState::try_from(vm.state).unwrap_or(MachineState::Pending);
    match state {
        MachineState::Running => {
            // Refresh the success patch on every tick. The kube apiserver
            // merge is a no-op if nothing changed, and this is what
            // self-heals a BasisMachine whose condition set was left
            // stale by a prior transient failure (e.g. a past
            // `Ready=False` from a BasisBackpressure that has since
            // recovered to RUNNING).
            let created = CreatedMachine {
                id: vm.id.clone(),
                provider_id: vm.provider_id.clone(),
                ip_address: vm.ip_address.clone(),
            };
            write_success_status(api, name, &created, machine, generation).await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }
        MachineState::Failed => {
            let msg = if vm.error_message.is_empty() {
                "VM reported failed state (no detail from agent)".to_string()
            } else {
                vm.error_message.clone()
            };
            Err(MachineError::VmFailed(msg))
        }
        other => {
            // Pending/Creating post-create, or Stopping/Stopped on a VM
            // we did not ask to delete, are both anomalous in steady
            // state. Don't rewrite success (the VM isn't usable) and
            // don't fail permanently (the state may resolve). Short
            // requeue so we observe the next transition quickly.
            warn!(
                machine = %name,
                state = ?other,
                "unexpected VM state in steady-state observe; requeueing"
            );
            Ok(Action::requeue(Duration::from_secs(15)))
        }
    }
}

/// Patch status (with the `basisVmId` marker) first, then spec. Status
/// carries the durable id; writing it before spec means a crash between
/// the two patches is self-healing on the next reconcile — the basis-side
/// VM is re-created idempotently and spec is finished. The opposite order
/// would leave spec populated without the id k8s uses to clean up.
///
/// Also clears any prior `Ready=False` / `failureMessage` so a recovered
/// machine doesn't keep advertising its last failure.
async fn write_success_status(
    api: &Api<BasisMachine>,
    name: &str,
    created: &CreatedMachine,
    machine: &BasisMachine,
    generation: Option<i64>,
) -> Result<(), MachineError> {
    let mut conditions = machine
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    conditions::upsert(
        &mut conditions,
        conditions::ready_true("VMRunning", generation),
    );

    merge_status(
        api,
        name,
        &json!({
            "status": {
                "ready": true,
                "initialization": { "provisioned": true },
                "providerID": created.provider_id,
                "basisVmId": created.id,
                "addresses": [MachineAddress {
                    kind: "InternalIP".to_string(),
                    address: created.ip_address.clone(),
                }],
                "failureMessage": serde_json::Value::Null,
                "conditions": conditions,
            }
        }),
    )
    .await?;

    merge_spec(
        api,
        name,
        &json!({ "spec": { "providerID": created.provider_id } }),
    )
    .await?;

    Ok(())
}

async fn cleanup(
    machine: Arc<BasisMachine>,
    ctx: Arc<ProviderContext>,
) -> Result<Action, MachineError> {
    let Some(id) = machine.status.as_ref().and_then(|s| s.basis_vm_id.clone()) else {
        return Ok(Action::await_change());
    };

    let namespace = namespace_of(machine.as_ref());
    let cluster_name = machine
        .labels()
        .get(CLUSTER_LABEL)
        .cloned()
        .ok_or(MachineError::Missing("cluster-name label"))?;
    let ClusterRef {
        credentials_ref, ..
    } = resolve_cluster_ref(&ctx.client, &namespace, &cluster_name).await?;
    let basis = ctx.clients.get(&credentials_ref, &namespace).await?;

    info!(vm_id = %id, "deleting VM in Basis controller");
    basis.delete_machine(id).await?;
    Ok(Action::await_change())
}

/// Reflect a failed apply/cleanup back onto the BasisMachine as a
/// `Ready=False` condition + `failureMessage`, and invalidate the
/// cached `BasisClient` if the failure looks credential-shaped.
///
/// Best-effort: errors here are logged but never propagated, because
/// the original error is what we want callers to see.
async fn on_failure(
    machine: &BasisMachine,
    ctx: Arc<ProviderContext>,
    namespace: &str,
    err: &MachineError,
) {
    if err.is_credentials_problem() {
        // Re-resolve the cluster ref; if even that fails, there's nothing
        // sensible to invalidate.
        if let Some(cluster_name) = machine.labels().get(CLUSTER_LABEL).cloned() {
            if let Ok(ClusterRef {
                credentials_ref, ..
            }) = resolve_cluster_ref(&ctx.client, namespace, &cluster_name).await
            {
                ctx.clients.invalidate(&credentials_ref, namespace).await;
                info!(
                    machine = %machine.name_any(),
                    "invalidated cached BasisClient after credentials failure"
                );
            }
        }
    }

    let api: Api<BasisMachine> = Api::namespaced(ctx.client.clone(), namespace);
    let existing_conditions = machine
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    record_failure_status(
        &api,
        &machine.name_any(),
        machine.metadata.generation,
        existing_conditions,
        err,
    )
    .await;
}

/// What the machine reconciler needs off the owning `BasisCluster`:
/// the basis-side cluster id to call into, plus the credentials ref so
/// we can resolve a `BasisClient` keyed to the same cluster.
struct ClusterRef {
    basis_cluster_id: String,
    credentials_ref: CredentialsRef,
}

/// Look up the owning `BasisCluster` and pull out the fields the
/// machine reconciler needs. The cluster reconciler is responsible for
/// populating `status.basisClusterId`; if it hasn't yet, we surface
/// `ClusterNotReady` so the error policy requeues quickly.
async fn resolve_cluster_ref(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> Result<ClusterRef, MachineError> {
    let api: Api<BasisCluster> = Api::namespaced(client.clone(), namespace);
    let cluster = api.get(cluster_name).await?;
    let basis_cluster_id = cluster
        .status
        .as_ref()
        .and_then(|s| s.basis_cluster_id.clone())
        .ok_or_else(|| MachineError::ClusterNotReady(cluster_name.to_string()))?;
    Ok(ClusterRef {
        basis_cluster_id,
        credentials_ref: cluster.spec.credentials_ref.clone(),
    })
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
        .ok_or(MachineError::Missing(
            "Machine.spec.bootstrap.dataSecretName",
        ))
}

fn error_policy(
    _machine: Arc<BasisMachine>,
    error: &MachineError,
    _ctx: Arc<ProviderContext>,
) -> Action {
    // Expected transient states — short requeue, info-level log so
    // they don't show up as errors in operator dashboards:
    //   * ClusterNotReady — waiting for the sibling BasisCluster
    //     reconcile to stamp an id; self-resolves.
    //   * BasisError.is_transient() — controller-side load shedding
    //     (Unavailable / DeadlineExceeded past the client's retry
    //     budget); self-resolves as the controller drains.
    if matches!(error, MachineError::ClusterNotReady(_)) || error.is_transient() {
        info!(error = %error, "BasisMachine transient, requeueing");
        return Action::requeue(Duration::from_secs(5));
    }
    error!(error = %error, "BasisMachine reconcile error");
    Action::requeue(Duration::from_secs(15))
}
