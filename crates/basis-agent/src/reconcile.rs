use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tracing::{error, info, warn};

use basis_common::json::parse_owned_json;
use basis_proto::{CreateVmCommand, ExtraDisk};

use crate::metrics;

/// Max number of VMs the agent restarts in parallel after a node reboot.
/// On a host with many VMs, a serial cold-start would compound GPU-bind
/// latency into minutes of downtime. The per-VM work — disk probe, tap
/// creation, vfio-pci rebind, systemd-run spawn — is all IO-bound, and
/// the VmManager no longer serialises on a coarse mutex, so this cap
/// is just to keep us from saturating sysfs.
const RESTART_CONCURRENCY: usize = 4;

/// Max orphan LVs each sweep pass removes. Protects the create path: a
/// 500-deep backlog removed serially + one-at-a-time through the LVM
/// semaphore would hold those permits long enough to starve concurrent
/// `lvcreate`s. With the adaptive sweep loop (see
/// `spawn_orphan_sweep_loop`) a cap here just means a large backlog
/// drains across a handful of short passes instead of one long one.
const ORPHAN_LV_BATCH: usize = 50;

/// Resources the sweep reclaims. Every path follows the same shape —
/// enumerate the resources of this kind on the host, diff against the
/// set this kind treats as "known," reclaim the difference — so we
/// factor that shape into [`GarbageCollectable`] + [`collect`] rather
/// than inlining it three times.
///
/// The "id" shape is per-kind: [`UnitCollector`] and [`LvCollector`]
/// both identify resources by `vm_id`; [`TapCollector`] identifies them
/// by tap interface name (a one-way hash of `vm_id` — see
/// [`crate::network::tap_name`] — so we can't reverse it). The runner
/// is agnostic: the caller passes the `known` set in whatever space the
/// collector lists.
trait GarbageCollectable {
    /// Label used in logs and the `basis_agent_orphan_sweep_reclaimed_total`
    /// metric. Stable values: `"unit"`, `"lv"`, `"tap"`.
    fn kind(&self) -> &'static str;

    /// Enumerate every resource of this kind currently on the host.
    async fn list(&self) -> anyhow::Result<Vec<String>>;

    /// Reclaim one orphan. A single failure is logged by the runner and
    /// the sweep re-runs on the adaptive loop's next pass; no per-call
    /// retry loop.
    async fn reclaim(&self, id: &str) -> anyhow::Result<()>;
}

/// Run one sweep pass against a single resource kind: list, diff
/// against `known`, reclaim the diff (capped at `batch`) in parallel.
/// Emits `basis_agent_orphan_sweep_reclaimed_total{kind}` once per
/// successfully-reclaimed orphan.
async fn collect<G: GarbageCollectable>(gc: &G, known: &HashSet<String>, batch: usize) -> u32 {
    let listed = match gc.list().await {
        Ok(ids) => ids,
        Err(e) => {
            warn!(kind = gc.kind(), error = %e, "orphan list failed");
            return 0;
        }
    };
    let orphans: Vec<String> = listed
        .into_iter()
        .filter(|id| !known.contains(id))
        .take(batch)
        .collect();
    if orphans.is_empty() {
        return 0;
    }
    warn!(
        kind = gc.kind(),
        count = orphans.len(),
        "reclaiming orphans"
    );

    // Parallel reclaim. Per-resource concurrency caps (e.g. the LVM
    // mutation semaphore inside `lvm::remove_vm_lv`) naturally apply —
    // this just stops us from serialising on top of them.
    let results = futures::future::join_all(orphans.iter().map(|id| gc.reclaim(id))).await;

    let mut reclaimed = 0u32;
    for (id, result) in orphans.iter().zip(results) {
        match result {
            Ok(()) => reclaimed += 1,
            Err(e) => {
                warn!(kind = gc.kind(), id = %id, error = %e, "reclaim failed");
            }
        }
    }
    if reclaimed > 0 {
        if let Some(m) = metrics::global() {
            m.orphan_sweep_reclaimed_total
                .with_label_values(&[gc.kind()])
                .inc_by(u64::from(reclaimed));
        }
    }
    reclaimed
}

// --- Collectors ---

/// systemd transient units whose `vm_id` is not in the agent DB.
/// Reclaim path goes through [`crate::handlers::delete_vm`] because a
/// running unit implies we may also have a tracked VmManager entry, a
/// DB row, a tap, GPU bindings, and an LV — every layer needs cleanup,
/// and `delete_vm` is the one place that runs them in the correct order.
struct UnitCollector<'a> {
    running_vm_ids: &'a [String],
    agent_db: &'a AgentDb,
    vm_mgr: &'a Arc<VmManager>,
    net_mgr: &'a NetworkManager,
}

impl<'a> GarbageCollectable for UnitCollector<'a> {
    fn kind(&self) -> &'static str {
        "unit"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.running_vm_ids.to_vec())
    }
    async fn reclaim(&self, vm_id: &str) -> anyhow::Result<()> {
        // Propagate the delete error so the sweep runner counts this
        // as a failed reclaim and the next pass retries — otherwise a
        // transient `LvmError::Busy` would be silently counted as
        // success and the orphan unit would linger.
        crate::handlers::delete_vm(vm_id, self.vm_mgr, self.net_mgr, self.agent_db).await
    }
}

/// Thin-pool LVs whose `vm_id` is not in the agent DB. Covers the
/// leaked-snapshot case where a prior `delete_vm` hit EBUSY on
/// `lvremove` and dropped it best-effort. Unifies rootfs LVs and data
/// disk LVs into a single vm_id-keyed set so a VM whose rootfs was
/// removed but whose data disks linger gets swept on the same pass.
struct LvCollector;

impl GarbageCollectable for LvCollector {
    fn kind(&self) -> &'static str {
        "lv"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        Ok(lvm::list_managed_vm_ids().await?.into_iter().collect())
    }
    async fn reclaim(&self, vm_id: &str) -> anyhow::Result<()> {
        lvm::remove_vm_lv(vm_id).await?;
        lvm::remove_vm_data_disks(vm_id).await?;
        Ok(())
    }
}

/// Tap devices on the basis bridge whose name doesn't map back to any
/// known vm_id. Tap names are a hash of vm_id so we can't reverse one;
/// the caller passes the expected tap-name set (derived from known
/// vm_ids via `network::tap_name`) as the collector's `known` space.
struct TapCollector<'a> {
    net_mgr: &'a NetworkManager,
}

impl<'a> GarbageCollectable for TapCollector<'a> {
    fn kind(&self) -> &'static str {
        "tap"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.net_mgr.list_basis_taps().await?)
    }
    async fn reclaim(&self, tap_name: &str) -> anyhow::Result<()> {
        self.net_mgr.delete_tap_by_name(tap_name).await?;
        Ok(())
    }
}

use crate::config::HostSpec;
use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::lvm;
use crate::network::NetworkManager;
use crate::vm::{BootArtifacts, VmManager};

pub struct ReconcileReport {
    pub recovered: u32,
    pub restarted: u32,
    pub orphans: u32,
    pub lost: u32,
    pub failed: u32,
}

/// Reconcile agent state on startup.
///
/// Three cases:
///   1. **Agent restart** (no node reboot): systemd transient units survive because
///      VMs are parented to systemd, not to the agent process. We just re-track them.
///   2. **Node reboot**: systemd transient units are gone, but the agent's local SQLite
///      still has the VM records and disk overlays persist on disk. We re-launch each VM
///      from its persisted state.
///   3. **Orphaned units**: a systemd unit is running but the agent DB has no record of it.
///      This shouldn't happen in normal operation but could if the agent DB was corrupted
///      or a previous cleanup was interrupted. We kill the orphan.
///
/// Controller-side reconciliation happens later, once the agent connects and
/// receives `RegisterHostResponse.expected_vm_ids` — any local VM not in that
/// list is deleted by [`crate::handlers::reconcile_against_expected`].
pub async fn reconcile_on_startup(
    config: &HostSpec,
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    image_mgr: &crate::image::ImageManager,
) -> anyhow::Result<ReconcileReport> {
    let mut report = ReconcileReport {
        recovered: 0,
        restarted: 0,
        orphans: 0,
        lost: 0,
        failed: 0,
    };

    // Discover which VMs systemd still has running
    let running_vm_ids = vm_mgr.reconcile_running().await?;
    let running_set: std::collections::HashSet<&str> =
        running_vm_ids.iter().map(|s| s.as_str()).collect();

    // Load our persisted VM records
    let known_vms = agent_db.list_vms().await?;
    let known_ids: std::collections::HashSet<String> =
        known_vms.iter().map(|v| v.vm_id.clone()).collect();

    // --- Cases 1 & 2: VMs we know about ---
    // Partition up-front: case 1 (already running) is a pure count; case 2
    // (needs restart) is the concurrent work.
    let (running_here, to_restart): (Vec<_>, Vec<_>) = known_vms
        .iter()
        .partition(|vm| running_set.contains(vm.vm_id.as_str()));
    report.recovered = running_here.len() as u32;

    let outcomes: Vec<RestartOutcome> = stream::iter(to_restart)
        .map(|vm_record| async move {
            (
                vm_record.vm_id.clone(),
                restart_vm(config, vm_record, vm_mgr, net_mgr, image_mgr).await,
            )
        })
        .buffer_unordered(RESTART_CONCURRENCY)
        .map(|(vm_id, result)| match result {
            Ok(()) => RestartOutcome::Restarted,
            Err(RestartError::DiskMissing {
                lv_path,
                cidata_path,
            }) => {
                // Disk files missing (directory or qcow2/ISO gone) — keep
                // the DB record so an operator can see it and let the
                // controller reconciliation pass clean it up. Log with
                // enough detail that an operator can diagnose whether
                // the thin pool was reinitialized or the cidata dir was
                // wiped.
                warn!(
                    vm_id = %vm_id,
                    lv_path = %lv_path.display(),
                    cidata_path = %cidata_path.display(),
                    lv_exists = lv_path.exists(),
                    cidata_exists = cidata_path.exists(),
                    "VM disk artifacts missing after reboot — cannot restart. \
                     If lv_exists=false: the thin pool may have been reinitialized; \
                     check `lvs basis/pool`. If cidata_exists=false: dataDir was wiped; \
                     check mounts on spec.dataDir. Reporting FAILED to controller."
                );
                RestartOutcome::Lost
            }
            Err(RestartError::GpuBindFailed(e)) => {
                // A VM booted without its GPU is silently broken — K8s
                // schedules GPU pods onto it and they fail. Abort the
                // restart and let CAPI remediate.
                error!(
                    vm_id = %vm_id,
                    error = %e,
                    "GPU rebind failed, aborting VM restart — CAPI will remediate"
                );
                RestartOutcome::Failed
            }
            Err(RestartError::Other(e)) => {
                error!(vm_id = %vm_id, error = %e, "failed to restart VM");
                RestartOutcome::Failed
            }
        })
        .collect()
        .await;

    for outcome in outcomes {
        match outcome {
            RestartOutcome::Restarted => report.restarted += 1,
            RestartOutcome::Lost => report.lost += 1,
            RestartOutcome::Failed => report.failed += 1,
        }
    }

    report.orphans = sweep_orphans(&running_vm_ids, &known_ids, agent_db, vm_mgr, net_mgr).await;

    Ok(report)
}

/// Reclaim host-level resources (systemd units, LVs, taps) whose owning
/// VM-id is not in the agent DB. Safe to call at any time: the agent DB
/// is the authoritative source of live VMs on this host; anything
/// running, stored, or bound outside that set is by definition unowned.
///
/// Returns the number of resources reclaimed.
///
/// Covers three drift cases:
///   - systemd unit with no DB record (agent crashed between spawn and
///     `insert_vm`, or DB was corrupted) — delegated to
///     [`crate::handlers::delete_vm`] so the full per-VM cleanup path
///     (DB, GPU, tap, systemd, LV) runs, identical to a normal delete.
///   - LV on disk with no DB record and no systemd unit (agent crashed
///     between `lvcreate` and unit spawn, or a prior `lvremove` hit
///     EBUSY and was dropped best-effort).
///   - tap device on the bridge with no DB record and no systemd unit
///     (analogous race during create or delete).
pub async fn sweep_orphans(
    running_vm_ids: &[String],
    known_ids: &HashSet<String>,
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
) -> u32 {
    // Unit reclamation runs first because its reclaim path
    // (`handlers::delete_vm`) already drops the associated LV and tap,
    // so the later passes see only resources whose owning unit is
    // truly gone.
    let units = UnitCollector {
        running_vm_ids,
        agent_db,
        vm_mgr,
        net_mgr,
    };
    // Orphan units have no practical per-pass cap: list is bounded by
    // "units systemd is running" which is the active fleet size, not
    // by churn history. Use usize::MAX so a large crash-induced orphan
    // set gets reclaimed in one pass instead of trickling.
    let unit_reclaimed = collect(&units, known_ids, usize::MAX).await;

    let lvs = LvCollector;
    let lv_reclaimed = collect(&lvs, known_ids, ORPHAN_LV_BATCH).await;

    let expected_taps: HashSet<String> = known_ids
        .iter()
        .map(|id| crate::network::tap_name(id))
        .collect();
    let taps = TapCollector { net_mgr };
    let tap_reclaimed = collect(&taps, &expected_taps, usize::MAX).await;

    unit_reclaimed + lv_reclaimed + tap_reclaimed
}

/// Live orphan sweep: called periodically from a background task. Gathers
/// the authoritative sets on its own and delegates the per-resource logic
/// to [`sweep_orphans`].
pub async fn periodic_sweep(
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
) -> anyhow::Result<u32> {
    let running_vm_ids = vm_mgr.reconcile_running().await?;
    let known_vms = agent_db.list_vms().await?;
    // `known_ids` unions the DB with every VM the agent is actively
    // managing in memory. The DB row is *usually* in sync with the
    // in-memory state, but there are narrow windows where they
    // diverge (between `insert_vm` and the systemd-run returning, or
    // during a controller-driven reconcile that race with a fresh
    // create). If the sweep used the DB alone, it could reclaim a
    // tap / LV mid-create and crash the guest's virtio-net worker.
    // Taking the union makes the sweep conservative: a resource is
    // only reclaimable when *no* authoritative source claims it.
    let mut known_ids: HashSet<String> = known_vms.iter().map(|v| v.vm_id.clone()).collect();
    known_ids.extend(vm_mgr.live_vm_ids().await);
    Ok(sweep_orphans(&running_vm_ids, &known_ids, agent_db, vm_mgr, net_mgr).await)
}

enum RestartError {
    /// VM disk artifacts are gone after a reboot. Either the LVM thin
    /// pool was reinitialized (operator ran `lvremove` or re-provisioned
    /// the VG) or the cloud-init ISO directory was wiped. Non-recoverable
    /// at the agent layer — CAPI needs to see FAILED and reschedule.
    DiskMissing {
        lv_path: std::path::PathBuf,
        cidata_path: std::path::PathBuf,
    },
    GpuBindFailed(String),
    Other(anyhow::Error),
}

enum RestartOutcome {
    Restarted,
    Lost,
    Failed,
}

async fn restart_vm(
    config: &HostSpec,
    vm_record: &LocalVmRow,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    image_mgr: &crate::image::ImageManager,
) -> Result<(), RestartError> {
    let vm_dir = config.vms_dir().join(&vm_record.vm_id);
    let rootfs_path = lvm::vm_lv_path(&vm_record.vm_id);
    let cloud_init_path = vm_dir.join("cidata.iso");

    // `Path::exists` works on block devices — the thin LV shows up as
    // `/dev/basis/vm-<id>` while active. Missing LV means either the
    // thin pool was reinitialized or the operator removed the LV; we
    // can't restart from that state.
    if !rootfs_path.exists() || !cloud_init_path.exists() {
        return Err(RestartError::DiskMissing {
            lv_path: rootfs_path,
            cidata_path: cloud_init_path,
        });
    }

    // Reattach every data disk at the same virtio slot it occupied
    // pre-reboot. Index N in `extra_disk_gibs` must line up with the
    // `vmdata-<vm_id>-N` LV on the thin pool — missing any means the
    // guest would see a different `/dev/vd*` layout after the reboot
    // than before it, and anything persisted against the old layout
    // (filesystem, LVM PV on top, ceph OSD metadata) silently diverges.
    // Fail the restart loudly instead; CAPI sees FAILED and remediates.
    let extra_disk_gibs: Vec<u32> =
        parse_owned_json(&vm_record.extra_disk_gibs, "local_vms.extra_disk_gibs");
    let mut data_disk_paths = Vec::with_capacity(extra_disk_gibs.len());
    for index in 0..extra_disk_gibs.len() {
        let path = lvm::data_disk_lv_path(&vm_record.vm_id, index as u32);
        if !path.exists() {
            return Err(RestartError::DiskMissing {
                lv_path: path,
                cidata_path: cloud_init_path,
            });
        }
        data_disk_paths.push(path);
    }

    // Resolve the kernel + initrd paths out of the image cache. Same
    // call the create path uses; it's a no-op when the image was
    // already pulled (the normal case post-reboot).
    let cached = image_mgr
        .ensure_cached(&vm_record.image)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    info!(vm_id = %vm_record.vm_id, "restarting VM after node reboot");

    // Re-create tap device (idempotent — handles EEXIST)
    let tap_name = net_mgr
        .ensure_tap(&vm_record.vm_id)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    // Re-bind GPUs. If any GPU fails to bind, abort the entire VM restart.
    // A VM missing a GPU is silently broken — worse than being dead.
    let gpu_addrs: Vec<String> =
        parse_owned_json(&vm_record.gpu_pci_addresses, "local_vms.gpu_pci_addresses");
    let mut vfio_devices = Vec::new();
    for addr in &gpu_addrs {
        match gpu::bind_vfio(addr).await {
            Ok(path) => vfio_devices.push(path),
            Err(e) => {
                // Unbind any GPUs we already bound before aborting
                for bound in &vfio_devices {
                    // best-effort cleanup
                    let _ = gpu::unbind_vfio(bound).await;
                }
                return Err(RestartError::GpuBindFailed(format!("GPU {addr}: {e}")));
            }
        }
    }

    // Reconstruct a command for the restart. We reuse the existing disk overlay
    // and cloud-init ISO from disk — we are NOT regenerating them.
    //
    // bootstrap_data is intentionally empty: cloud-init already ran on first boot.
    // Cloud-init's instance-id check prevents re-provisioning. The correctness of
    // this restart path relies on cloud-init's first-boot semantics.
    //
    // gateway/prefix_len are zero because the cloud-init network config is already
    // baked into cidata.iso on disk. These fields are not used by create_vm for
    // the actual cloud-hypervisor invocation — they're only used when generating
    // the ISO, which we skip on restart.
    // Narrow i64 → u32 with an explicit check. DB entries originated
    // from u32-typed proto fields so truncation is not expected, but a
    // corrupt row or hand-edited DB could produce a value that silently
    // wraps — refuse to restart rather than boot a VM with wrong specs.
    let narrow_u32 = |field: &str, v: i64| -> Result<u32, RestartError> {
        u32::try_from(v).map_err(|_| {
            RestartError::Other(anyhow::anyhow!(
                "VM {} has corrupt {field}={v} in local DB (out of u32 range)",
                vm_record.vm_id
            ))
        })
    };
    let restart_cmd = CreateVmCommand {
        vm_id: vm_record.vm_id.clone(),
        name: vm_record.name.clone(),
        cpu: narrow_u32("cpu", vm_record.cpu)?,
        memory_mib: narrow_u32("memory_mib", vm_record.memory_mib)?,
        disk_gib: narrow_u32("disk_gib", vm_record.disk_gib)?,
        image: vm_record.image.clone(),
        bootstrap_data: Vec::new(),
        ip_address: vm_record.ip_address.clone(),
        gateway: String::new(),
        prefix_len: 0,
        gpus: 0,
        gpu_constraints: None,
        dns_servers: Vec::new(),
        gpu_pci_addresses: gpu_addrs,
        extra_disks: extra_disk_gibs
            .iter()
            .map(|&size_gib| ExtraDisk { size_gib })
            .collect(),
    };

    vm_mgr
        .create_vm(
            &restart_cmd,
            &BootArtifacts {
                kernel: &cached.kernel,
                initrd: &cached.initrd,
                rootfs: &rootfs_path,
                cloud_init: &cloud_init_path,
                extra_disks: &data_disk_paths,
            },
            &tap_name,
            &vfio_devices,
        )
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    info!(vm_id = %vm_record.vm_id, "VM restarted successfully");
    Ok(())
}
