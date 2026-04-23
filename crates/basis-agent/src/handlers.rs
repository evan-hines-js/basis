//! VM lifecycle operations on this host.
//!
//! One source of truth for "create a VM" and "delete a VM". Called both
//! by the inbound-command loop (`CreateVmCommand` / `DeleteVmCommand`)
//! and by the post-register reconciliation path, which must delete any
//! VMs the controller has forgotten.

use std::sync::Arc;
use std::time::{Duration, Instant};

use basis_common::json::parse_owned_json;
use basis_common::time::now_rfc3339;
use basis_proto::{
    agent_message, AgentMessage, CreateVmCommand, MachineState, ReportVmStateRequest,
};
use humantime::parse_rfc3339;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::image::{GuestNetwork, ImageManager};
use crate::lvm;
use crate::metrics::Metrics;
use crate::network::NetworkManager;
use crate::vm::{unit_name_for_vm, BootArtifacts, VmManager};

/// Prepare disk, network, GPU passthrough, and spawn cloud-hypervisor.
///
/// On any failure mid-way through, every step that already ran is rolled
/// back via [`delete_vm`] so the host is left as if the create never
/// happened. The local DB row is inserted *before* the systemd-run spawn:
/// if the spawn (or any later step) fails, rollback uses the row to find
/// the GPU bindings to release; if a crash happens between insert and
/// spawn, the startup reconciler sees a row pointing at non-existent
/// disk artifacts and reports FAILED so CAPI remediates — strictly
/// better than the prior ordering, where a successful spawn followed
/// by a DB-insert failure produced a running-but-unknown VM that
/// startup reconcile killed as an orphan.
///
/// Because the DB row exists before the systemd unit, the periodic
/// `report_local_vm_states` would otherwise see a row for a VM whose
/// systemd unit hasn't spawned yet and wrongly report FAILED during
/// the create window — surfaced to the controller's in-flight RPC as
/// "VM creation failed: systemd scope gone". We bracket the whole
/// create flow with [`VmManager::mark_pending`] / `clear_pending`;
/// the reporter skips pending VMs (their authoritative state comes
/// from `spawn_create` on completion), and the orphan sweep also
/// treats pending vm_ids as live so it won't reclaim in-flight
/// resources.
pub async fn create_vm(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    vm_mgr.mark_pending(&cmd.vm_id).await;
    let result = create_vm_inner(cmd, image_mgr, vm_mgr, net_mgr, agent_db, metrics).await;
    vm_mgr.clear_pending(&cmd.vm_id).await;

    match result {
        Ok(()) => {
            info!(vm_id = %cmd.vm_id, ip = %cmd.ip_address, "VM created");
            Ok(())
        }
        Err(e) => {
            warn!(
                vm_id = %cmd.vm_id,
                error = %e,
                "create_vm failed; rolling back partial state"
            );
            // Rollback is best-effort; any step that errors is picked
            // up by the orphan sweep. We don't surface delete errors
            // here because the caller already has the create error to
            // report.
            let _ = delete_vm(&cmd.vm_id, vm_mgr, net_mgr, agent_db).await;
            Err(e)
        }
    }
}

async fn create_vm_inner(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let vm_dir = vm_mgr.vms_dir.join(&cmd.vm_id);
    std::fs::create_dir_all(&vm_dir)?;

    // Each step is timed inline. Observations happen on the `Ok` path;
    // a step that errors is not observed — its latency would skew the
    // distribution (a failed OCI pull can dominate for minutes) and
    // the error rate is visible via the controller's
    // `basis_vm_create_result_total{result="vm_failed"}` counter.
    let started = Instant::now();
    let base = image_mgr.ensure_cached(&cmd.image).await?;
    metrics
        .image_ensure_cached_seconds
        .observe(started.elapsed().as_secs_f64());

    // Persist the VM record up front so any later failure has a DB row
    // to drive rollback off of, and so a crash here can't produce a
    // running-but-unknown VM (see module doc).
    agent_db
        .insert_vm(&LocalVmRow {
            vm_id: cmd.vm_id.clone(),
            name: cmd.name.clone(),
            unit_name: unit_name_for_vm(&cmd.vm_id),
            ip_address: cmd.ip_address.clone(),
            cpu: cmd.cpu as i64,
            memory_mib: cmd.memory_mib as i64,
            disk_gib: cmd.disk_gib as i64,
            gpu_pci_addresses: serde_json::to_string(&cmd.gpu_pci_addresses)
                .expect("serializing Vec<String> to JSON is infallible"),
            image: cmd.image.clone(),
            created_at: now_rfc3339(),
        })
        .await?;

    let started = Instant::now();
    let disk_path = lvm::create_vm_lv(&cmd.vm_id, &base.image_hash, cmd.disk_gib as u64).await?;
    metrics
        .lv_snapshot_seconds
        .observe(started.elapsed().as_secs_f64());

    let started = Instant::now();
    let cloud_init_path = image_mgr
        .create_cloud_init_iso(
            &vm_dir,
            &cmd.vm_id,
            &cmd.name,
            &cmd.bootstrap_data,
            &GuestNetwork {
                ip_address: &cmd.ip_address,
                gateway: &cmd.gateway,
                prefix_len: cmd.prefix_len,
                dns_servers: &cmd.dns_servers,
            },
        )
        .await?;
    metrics
        .cloud_init_iso_seconds
        .observe(started.elapsed().as_secs_f64());

    let started = Instant::now();
    let tap = net_mgr.create_tap(&cmd.vm_id).await?;
    metrics
        .tap_create_seconds
        .observe(started.elapsed().as_secs_f64());

    let mut vfio_devices = Vec::new();
    for pci in &cmd.gpu_pci_addresses {
        let started = Instant::now();
        vfio_devices.push(gpu::bind_vfio(pci).await?);
        metrics
            .vfio_bind_seconds
            .observe(started.elapsed().as_secs_f64());
    }

    let started = Instant::now();
    vm_mgr
        .create_vm(
            cmd,
            &BootArtifacts {
                kernel: &base.kernel,
                initrd: &base.initrd,
                rootfs: &disk_path,
                cloud_init: &cloud_init_path,
            },
            &tap,
            &vfio_devices,
        )
        .await?;
    metrics
        .vm_spawn_seconds
        .observe(started.elapsed().as_secs_f64());
    Ok(())
}

/// Tear down a VM. Returns success only when every step succeeded so
/// the controller can bound its `DeleteCluster` / `DeleteMachine` RPC
/// on real cleanup completion — that's what gives workers genuine
/// backpressure instead of pipelining creates behind unresolved
/// deletes.
///
/// Row-first ordering is load-bearing. `vm_mgr.delete_vm` removes the
/// VM from the tracked map immediately but its `systemctl stop` can
/// take a few seconds and `lvremove` often waits for a still-open
/// block-device handle. If the DB row survived the whole cleanup,
/// `reconcile_running_vms` could tick during that window, see a row
/// whose VM is no longer in the tracked map, and report FAILED for a
/// VM that's just being deleted — which then surfaces to the
/// controller's in-flight RPC as a spurious "VM creation failed".
/// Dropping the row first means the reconciler simply doesn't consider
/// the VM anymore.
///
/// Errors in GPU / tap / systemd steps are best-effort (logged, do not
/// abort the delete) because those resources either don't exist yet or
/// are already gone and retrying doesn't help. The one error we
/// surface is `lvremove` — that's the leak that matters (the thin
/// pool fills with orphans and lvm2 degrades to O(N)). All lvremove
/// errors are treated as transient: the controller's client retries,
/// and the orphan sweep picks up any LV that stays stuck across
/// retries.
pub async fn delete_vm(
    vm_id: &str,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) -> anyhow::Result<()> {
    // Read the record first so we still have the GPU PCI list after the
    // row is gone. `get_vm` errors (bad JSON, sqlite failure) degrade to
    // "skip GPU unbind" — the orphan sweep can't help here, but a stuck
    // vfio binding is a single-host diagnostic.
    let record = agent_db.get_vm(vm_id).await.ok().flatten();

    if let Err(e) = agent_db.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to remove local VM record");
    }

    if let Some(record) = record {
        let addrs: Vec<String> =
            parse_owned_json(&record.gpu_pci_addresses, "local_vms.gpu_pci_addresses");
        for addr in &addrs {
            if let Err(e) = gpu::unbind_vfio(addr).await {
                warn!(vm_id, pci = %addr, error = %e, "failed to unbind GPU");
            }
        }
    }

    if let Err(e) = net_mgr.delete_tap(vm_id).await {
        warn!(vm_id, error = %e, "failed to delete tap");
    }
    if let Err(e) = vm_mgr.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to stop VM");
    }
    // lvremove comes last: cloud-hypervisor holds `/dev/basis/vm-<id>`
    // exclusively until its process exits, so `vm_mgr.delete_vm` must
    // return (with its `udevadm settle` draining pending release
    // events) before we remove the LV.
    lvm::remove_vm_lv(vm_id).await.map_err(|e| {
        warn!(vm_id, error = %e, "VM delete failed at lvremove; caller will retry");
        anyhow::Error::from(e)
    })?;
    info!(vm_id, "VM deleted");
    Ok(())
}

/// Apply the controller's authoritative VM list.
///
/// Any locally-known VM not in `expected_vm_ids` was forgotten by the
/// controller — its disk overlay, tap, and GPU bindings are garbage.
///
/// `delete_grace` defends against a buggy/incomplete `expected_vm_ids`
/// push by skipping deletion of VMs younger than the grace window. Pass
/// `Duration::ZERO` for the post-register reconcile (the agent has been
/// offline; the controller's view *is* authoritative). Pass a non-zero
/// grace for periodic pushes from `ReconcileHostCommand` so an in-flight
/// CreateMachine that hasn't yet been added to the controller's view
/// can't be wiped out by a single misbehaving push.
pub async fn reconcile_against_expected(
    expected_vm_ids: &[String],
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    delete_grace: Duration,
) -> anyhow::Result<()> {
    let expected: std::collections::HashSet<&str> =
        expected_vm_ids.iter().map(String::as_str).collect();
    let now = std::time::SystemTime::now();

    let mut to_delete: Vec<LocalVmRow> = Vec::new();
    for vm in agent_db.list_vms().await? {
        if expected.contains(vm.vm_id.as_str()) {
            continue;
        }
        if !delete_grace.is_zero() && younger_than(&vm.created_at, now, delete_grace) {
            warn!(
                vm_id = %vm.vm_id,
                created_at = %vm.created_at,
                grace_secs = delete_grace.as_secs(),
                "VM missing from controller list but within grace period; \
                 deferring delete"
            );
            continue;
        }
        warn!(vm_id = %vm.vm_id, "VM forgotten by controller, deleting locally");
        to_delete.push(vm);
    }

    // Fan out the deletes. Each one does a systemctl-stop + lvremove
    // which takes seconds per VM; serially this can block the reconcile
    // (and any other path that waits on the same task) for minutes on
    // a large forgotten set. `delete_vm` is best-effort and returns
    // `()`, so `join_all` fits — no partial-failure propagation needed.
    futures::future::join_all(
        to_delete
            .iter()
            .map(|vm| delete_vm(&vm.vm_id, vm_mgr, net_mgr, agent_db)),
    )
    .await;
    Ok(())
}

/// True iff `created_at` (RFC 3339) is less than `grace` ago. A
/// malformed timestamp falls through to `false`: an unparseable
/// created_at is older than any grace period worth defending.
fn younger_than(created_at: &str, now: std::time::SystemTime, grace: Duration) -> bool {
    parse_rfc3339(created_at)
        .ok()
        .and_then(|then| now.duration_since(then).ok())
        .map(|age| age < grace)
        .unwrap_or(false)
}

/// Send a single VM state report to the controller.
///
/// `transient` distinguishes a load-shedding failure (queue timeout,
/// semaphore full, backend busy) from a real fault. The controller
/// maps it onto `Status::Unavailable` so callers retry, and labels
/// the metric `{result="busy"}`. Ignored for state=RUNNING.
pub async fn send_vm_state(
    sender: &mpsc::Sender<AgentMessage>,
    vm_id: String,
    state: MachineState,
    error_message: String,
    transient: bool,
) {
    let msg = AgentMessage {
        payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
            vm_id,
            state: state as i32,
            error_message,
            transient,
        })),
    };
    if let Err(e) = sender.send(msg).await {
        warn!(error = %e, "dropped VM state report; controller stream is closed");
    }
}

/// Report the state of every locally-known VM to the controller:
/// `Running` if systemd still has the unit, `Failed` otherwise. Used
/// both as the one-shot post-handshake catch-up (so the controller
/// sees the full picture of VMs that survived an agent restart) and
/// as the periodic "thick agent" drift detector that catches VMs
/// whose systemd scope disappeared (crash, OOM, manual stop).
///
/// One function, one definition of "report VM state," because the
/// previous split between this and a FAILED-only variant drifted
/// apart (different error strings, different sites calling each) and
/// made it easy to forget which semantics applied where.
pub async fn report_local_vm_states(
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    for vm in agent_db.list_vms().await? {
        // Skip VMs whose create is mid-flight. `spawn_create` will
        // send the authoritative state once `systemd-run` returns;
        // reporting Running here (while the systemd unit is only
        // "pending start") would let the controller prematurely
        // resolve its pending CreateMachine, which in turn lets a
        // subsequent DeleteMachine race with the still-pending start
        // job and produce `systemd-run: Job canceled`.
        if vm_mgr.is_pending(&vm.vm_id).await {
            continue;
        }
        // `has_live_process` reads `SubState` directly from systemd so
        // we catch guest-level crashes (virtio failure, kernel panic,
        // OOM). Without this, a `cloud-hypervisor` that exited cleanly
        // with `--remain-after-exit` would look "active" to any
        // in-memory check and the VM would report `Running` forever
        // while the guest is dead, blocking CAPI from triggering
        // replacement.
        let (state, err) = if vm_mgr.has_live_process(&vm.vm_id).await {
            (MachineState::Running, String::new())
        } else {
            warn!(
                vm_id = %vm.vm_id,
                "VM process is not running — reporting FAILED"
            );
            (
                MachineState::Failed,
                "cloud-hypervisor process exited".to_string(),
            )
        };
        send_vm_state(sender, vm.vm_id, state, err, false).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_vm(id: &str) -> LocalVmRow {
        LocalVmRow {
            vm_id: id.to_string(),
            name: format!("vm-{id}"),
            unit_name: unit_name_for_vm(id),
            ip_address: "10.0.10.42".to_string(),
            cpu: 2,
            memory_mib: 4096,
            disk_gib: 50,
            gpu_pci_addresses: "[]".to_string(),
            image: "img".to_string(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn old_vm(id: &str) -> LocalVmRow {
        LocalVmRow {
            created_at: "2020-01-01T00:00:00Z".to_string(),
            ..fake_vm(id)
        }
    }

    fn fresh_vm(id: &str) -> LocalVmRow {
        LocalVmRow {
            created_at: now_rfc3339(),
            ..fake_vm(id)
        }
    }

    fn fixtures() -> (Arc<VmManager>, NetworkManager) {
        let vm_mgr = Arc::new(VmManager::new(
            std::env::temp_dir().join("basis-test-reconcile"),
        ));
        let net_mgr = NetworkManager::new("test-br".to_string(), "lo".to_string());
        (vm_mgr, net_mgr)
    }

    /// `reconcile_against_expected` deletes everything the controller has
    /// forgotten. We can't run the real delete (no systemd/network in
    /// tests) so we verify the DB-level effect: rows for forgotten VMs
    /// are gone, rows for expected VMs remain.
    #[tokio::test]
    async fn reconcile_deletes_forgotten_vm_records() {
        let db = AgentDb::open(":memory:".as_ref()).await.unwrap();
        db.insert_vm(&old_vm("keep")).await.unwrap();
        db.insert_vm(&old_vm("drop-1")).await.unwrap();
        db.insert_vm(&old_vm("drop-2")).await.unwrap();

        let (vm_mgr, net_mgr) = fixtures();
        reconcile_against_expected(
            &["keep".to_string()],
            &vm_mgr,
            &net_mgr,
            &db,
            Duration::ZERO,
        )
        .await
        .unwrap();

        let remaining: Vec<String> = db
            .list_vms()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.vm_id)
            .collect();
        assert_eq!(remaining, vec!["keep".to_string()]);
    }

    #[tokio::test]
    async fn reconcile_is_noop_when_everything_expected() {
        let db = AgentDb::open(":memory:".as_ref()).await.unwrap();
        db.insert_vm(&old_vm("a")).await.unwrap();
        db.insert_vm(&old_vm("b")).await.unwrap();

        let (vm_mgr, net_mgr) = fixtures();
        reconcile_against_expected(
            &["a".to_string(), "b".to_string()],
            &vm_mgr,
            &net_mgr,
            &db,
            Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(db.list_vms().await.unwrap().len(), 2);
    }

    /// A non-zero grace defers deletion of fresh VMs missing from the
    /// list — defends in-flight CreateMachine work against a single
    /// misbehaving controller push.
    #[tokio::test]
    async fn reconcile_grace_defers_delete_of_fresh_vms() {
        let db = AgentDb::open(":memory:".as_ref()).await.unwrap();
        db.insert_vm(&fresh_vm("just-created")).await.unwrap();
        db.insert_vm(&old_vm("legitimately-stale")).await.unwrap();

        let (vm_mgr, net_mgr) = fixtures();
        reconcile_against_expected(&[], &vm_mgr, &net_mgr, &db, Duration::from_secs(60))
            .await
            .unwrap();

        let remaining: Vec<String> = db
            .list_vms()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.vm_id)
            .collect();
        assert_eq!(
            remaining,
            vec!["just-created".to_string()],
            "fresh VM should be deferred; old VM should be deleted"
        );
    }

    /// Zero grace deletes regardless of age — used for the post-register
    /// reconcile where the controller's view is fully authoritative.
    #[tokio::test]
    async fn reconcile_zero_grace_deletes_fresh_vms() {
        let db = AgentDb::open(":memory:".as_ref()).await.unwrap();
        db.insert_vm(&fresh_vm("just-created")).await.unwrap();

        let (vm_mgr, net_mgr) = fixtures();
        reconcile_against_expected(&[], &vm_mgr, &net_mgr, &db, Duration::ZERO)
            .await
            .unwrap();
        assert!(db.list_vms().await.unwrap().is_empty());
    }
}
