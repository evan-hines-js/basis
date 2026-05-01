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

    /// We watched the BasisMachine into existence before CAPI's
    /// Machine controller finished decorating it (OwnerReference
    /// stamped, bootstrap-provider `dataSecretName` populated, etc).
    /// This is the normal pre-CAPI-reconcile state — surfacing it as
    /// `failureMessage` would propagate up to
    /// `Machine.status.failureMessage` and let MachineDeployment
    /// conclude the infra is broken. Treat it as transient: short
    /// requeue, no status patch. The `&'static str` names which
    /// field we're waiting on so the warn log + requeue log make
    /// sense.
    #[error("waiting for CAPI to populate {0}")]
    WaitingForCapi(&'static str),

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
            MachineError::WaitingForCapi(_) => "WaitingForCAPI",
            MachineError::VmFailed(_) => "VMFailed",
        }
    }

    fn is_credentials_problem(&self) -> bool {
        matches!(self, MachineError::Credentials(_))
            || matches!(self, MachineError::Basis(e) if e.is_credentials_problem())
    }

    fn is_transient(&self) -> bool {
        matches!(self, MachineError::Basis(e) if e.is_transient())
            || matches!(self, MachineError::WaitingForCapi(_))
    }
}

pub async fn run(ctx: Arc<ProviderContext>) -> anyhow::Result<()> {
    let api: Api<BasisMachine> = Api::all(ctx.client.clone());

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
        match basis.get_machine(vm_id.clone()).await {
            Ok(vm) => return observe_vm(&api, &name, &vm, &machine, generation).await,
            // VM recorded in status but absent in basis: external delete,
            // host loss with no recovery, or a stale id from an earlier
            // failed apply. Either way, retrying won't bring it back.
            // Mark terminal so CAPI's KubeadmControlPlane (or any
            // MachineHealthCheck) replaces the Machine instead of
            // looping forever on `not found: vm '...'`.
            Err(e) if e.is_not_found() => {
                record_terminal_failure(
                    &api,
                    &name,
                    generation,
                    &machine,
                    "VmDisappeared",
                    &format!("Basis VM {vm_id} not found"),
                )
                .await;
                return Ok(Action::await_change());
            }
            Err(e) => return Err(e.into()),
        }
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
            placement: placement_spec_to_proto(&machine.spec.placement),
        })
        .await?;

    // No rollback on patch failure. `Basis.CreateMachine` is idempotent
    // by name (server-side; see basis-controller/src/server.rs), so a
    // retried apply hits the same VM. Any partial commit converges:
    //   * status patch failed: no `basis_vm_id` in k8s, so the next
    //     reconcile re-enters this no-vm-id branch, calls
    //     `create_machine` (idempotent → same id), and re-attempts
    //     both patches.
    //   * status committed, spec failed: `basis_vm_id` is in k8s, so
    //     the next reconcile takes the observe path and `observe_vm`
    //     re-runs both patches idempotently. `cleanup()` can also
    //     find the VM via `basis_vm_id` if the BasisMachine is
    //     deleted in this state — no leak.
    commit_status(
        &api,
        &name,
        &created,
        &credentials_ref,
        &machine,
        generation,
    )
    .await?;
    commit_spec_provider_id(&api, &name, &created.provider_id).await?;
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
            let created = CreatedMachine {
                id: vm.id.clone(),
                provider_id: vm.provider_id.clone(),
                ip_address: vm.ip_address.clone(),
            };
            // Steady-state: credentials_ref is already in status from
            // the initial commit; re-carry it so `merge_status` doesn't
            // null it out and strand the cleanup path.
            let creds = machine
                .status
                .as_ref()
                .and_then(|s| s.credentials_ref.clone())
                .ok_or(MachineError::Missing("status.credentialsRef"))?;
            commit_status(api, name, &created, &creds, machine, generation).await?;
            commit_spec_provider_id(api, name, &created.provider_id).await?;
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

/// Commit the durable `basisVmId` plus addresses, conditions, and
/// readiness onto status. Idempotent. Splitting status from spec means
/// the failure modes are disjoint:
///   * If this fails, k8s has no `basis_vm_id`. Next reconcile re-enters
///     the no-vm-id branch and CreateMachine (idempotent by name) re-
///     produces the same VM, then re-attempts both patches.
///   * If this succeeds, `basis_vm_id` is durable. `cleanup()` can find
///     the VM on finalizer fire even if the spec patch later fails, so
///     the basis-side VM never leaks.
///
/// Also clears any prior `Ready=False` / `failureMessage` so a recovered
/// machine doesn't keep advertising its last failure.
async fn commit_status(
    api: &Api<BasisMachine>,
    name: &str,
    created: &CreatedMachine,
    credentials_ref: &CredentialsRef,
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

    // Tree-side IP is the node's InternalIP. Basis VMs are
    // single-homed; LAN reachability for VIPs is provided by the
    // host's BGP advertisement, not a per-VM uplink NIC.
    let addresses = vec![MachineAddress {
        kind: "InternalIP".to_string(),
        address: created.ip_address.clone(),
    }];

    merge_status(
        api,
        name,
        &json!({
            "status": {
                "ready": true,
                "initialization": { "provisioned": true },
                "providerID": created.provider_id,
                "basisVmId": created.id,
                "credentialsRef": credentials_ref,
                "addresses": addresses,
                "failureMessage": serde_json::Value::Null,
                "failureReason": serde_json::Value::Null,
                "conditions": conditions,
            }
        }),
    )
    .await?;

    Ok(())
}

/// Patch `spec.providerID`. Separate from status because (a) spec writes
/// can fail independently and (b) on retry through `observe_vm`, we
/// re-run both — splitting keeps each patch's intent local. Idempotent.
async fn commit_spec_provider_id(
    api: &Api<BasisMachine>,
    name: &str,
    provider_id: &str,
) -> Result<(), MachineError> {
    merge_spec(api, name, &json!({ "spec": { "providerID": provider_id } })).await?;
    Ok(())
}

/// Mark the BasisMachine as a terminal failure. Sets both `failureReason`
/// and `failureMessage`; CAPI's Machine controller propagates them onto
/// `Machine.status.failureReason` / `failureMessage`, which KCP and any
/// MachineHealthCheck treat as "this Machine is dead, replace it." Use
/// only for non-recoverable conditions — a transient blip set as terminal
/// causes a needless replacement cycle. Best-effort: a patch failure is
/// logged but never returned, since the caller has already decided to
/// stop reconciling.
async fn record_terminal_failure(
    api: &Api<BasisMachine>,
    name: &str,
    generation: Option<i64>,
    machine: &BasisMachine,
    reason: &'static str,
    message: &str,
) {
    let mut conditions = machine
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    conditions::upsert(
        &mut conditions,
        conditions::ready_false(reason, message.to_string(), generation),
    );
    let payload = json!({
        "status": {
            "ready": false,
            "failureReason": reason,
            "failureMessage": message,
            "conditions": conditions,
        }
    });
    if let Err(patch_err) = merge_status(api, name, &payload).await {
        warn!(
            machine = name,
            error = %patch_err,
            "could not patch terminal failure status",
        );
    }
}

async fn cleanup(
    machine: Arc<BasisMachine>,
    ctx: Arc<ProviderContext>,
) -> Result<Action, MachineError> {
    // No basis VM ever got created (apply failed before commit). There
    // is nothing to tear down — fall through so the finalizer lifts.
    let Some(id) = machine.status.as_ref().and_then(|s| s.basis_vm_id.clone()) else {
        return Ok(Action::await_change());
    };

    // `basis_vm_id` is only written alongside `credentials_ref` in
    // `write_success_status`, so the invariant is: if we hold the id,
    // we also hold the credentials needed to delete it. This is what
    // makes cleanup independent of the owning `BasisCluster` — the
    // BasisCluster can already be gone (normal cascade, racing
    // finalizers) and cleanup still finds the basis controller on its
    // own. Hand-stripping `status.credentialsRef` is the only way to
    // violate this; we treat that as a hard error rather than a
    // fallthrough so a missing reference surfaces loudly instead of
    // silently leaking a row on basis.
    let credentials_ref = machine
        .status
        .as_ref()
        .and_then(|s| s.credentials_ref.clone())
        .ok_or(MachineError::Missing("status.credentialsRef"))?;

    let namespace = namespace_of(machine.as_ref());
    let basis = ctx.clients.get(&credentials_ref, &namespace).await?;

    info!(vm_id = %id, "deleting VM in Basis controller");
    match basis.delete_machine(id.clone()).await {
        Ok(()) => {}
        // NotFound is success for finalizers: the resource is gone,
        // which is the goal state. Without this, a CreateMachine
        // that rolled back server-side leaves the BasisMachine's
        // status carrying a stale vm_id, and CAPI loops on
        // "controller RPC failed: not found" until the finalizer
        // is hand-stripped. Standard idempotent-delete pattern.
        Err(e) if e.is_not_found() => {
            info!(vm_id = %id, "VM already absent in Basis controller; finalizer lifting");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(Action::await_change())
}

/// Reflect a failed apply/cleanup back onto the BasisMachine as a
/// `Ready=False` condition + `failureMessage`, and invalidate the
/// cached `BasisClient` if the failure looks credential-shaped.
///
/// Skipped for transient errors — those represent a normal
/// intermediate state (e.g. `BootstrapPending` while the bootstrap
/// provider is still generating the data secret), and writing
/// `failureMessage` would propagate to the owning `Machine` and
/// let MachineDeployment conclude the infra is broken.
///
/// Best-effort: errors here are logged but never propagated, because
/// the original error is what we want callers to see.
async fn on_failure(
    machine: &BasisMachine,
    ctx: Arc<ProviderContext>,
    namespace: &str,
    err: &MachineError,
) {
    if err.is_transient() {
        return;
    }
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

    // CAPI's Machine controller stamps the OwnerReference shortly
    // after MachineDeployment creates this BasisMachine. Watching for
    // the BasisMachine's creation event fires us *before* that stamp
    // lands, so a missing owner-ref here is normal pre-CAPI-reconcile
    // state — treat it as transient instead of stamping `failureMessage`.
    let owner = bm
        .metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.kind == "Machine"))
        .ok_or(MachineError::WaitingForCapi(
            "BasisMachine.metadata.ownerReferences[Machine]",
        ))?;

    let machines: Api<CapiMachine> = Api::namespaced(client.clone(), namespace);
    let machine = machines.get(&owner.name).await?;
    machine
        .spec
        .bootstrap
        .data_secret_name
        .ok_or(MachineError::WaitingForCapi(
            "Machine.spec.bootstrap.dataSecretName",
        ))
}

/// Map the CRD's PlacementSpec to the proto sent to basis-controller.
/// Returns `None` for an empty spec so the wire form stays minimal —
/// the proto field is optional, and the server treats absent the same
/// as both lists empty.
fn placement_spec_to_proto(
    spec: &crate::crds::PlacementSpec,
) -> Option<basis_proto::PlacementSpec> {
    if spec.requires.is_empty() && spec.prefers.is_empty() {
        return None;
    }
    Some(basis_proto::PlacementSpec {
        requires: spec
            .requires
            .iter()
            .map(|r| basis_proto::PlacementRequirement {
                key: r.key.clone(),
                values: r.values.clone(),
            })
            .collect(),
        prefers: spec
            .prefers
            .iter()
            .map(|p| basis_proto::PlacementPreference {
                key: p.key.clone(),
                value: p.value.clone(),
                weight: p.weight,
            })
            .collect(),
    })
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
    //   * WaitingForCapi — pre-CAPI-reconcile state for fields CAPI
    //     populates after BasisMachine creation (OwnerReference,
    //     `Machine.spec.bootstrap.dataSecretName`); self-resolves in
    //     seconds.
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
