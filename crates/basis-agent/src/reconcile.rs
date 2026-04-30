use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tracing::{error, info, warn};

use basis_proto::{CreateVmCommand, ExtraDisk};

use crate::config::HostSpec;
use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::lvm::Storage;
use crate::metrics;
use crate::network::NetworkManager;
use crate::vm::{BootArtifacts, VmManager};

/// Max number of VMs the agent restarts in parallel after a node reboot.
const RESTART_CONCURRENCY: usize = 4;

/// Max orphan LVs each sweep pass removes.
const ORPHAN_LV_BATCH: usize = 50;

/// Resources the sweep reclaims. Every path follows the same shape —
/// enumerate the resources of this kind on the host, diff against the
/// set this kind treats as "known," reclaim the difference.
trait GarbageCollectable {
    fn kind(&self) -> &'static str;
    async fn list(&self) -> anyhow::Result<Vec<String>>;
    async fn reclaim(&self, id: &str) -> anyhow::Result<()>;
}

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
/// Lists every `basis-vm-*.service` regardless of state — running
/// units need a stop+teardown, failed/exited units need a
/// `reset-failed` to disappear from `list-units --all`. Both flow
/// through `handlers::delete_vm`, which calls `systemctl stop` (no-op
/// on already-stopped) and then `reset-failed` via
/// `vm_mgr.delete_vm`.
struct UnitCollector<'a> {
    agent_db: &'a AgentDb,
    vm_mgr: &'a Arc<VmManager>,
    net_mgr: &'a NetworkManager,
    storage: &'a Storage,
}

impl<'a> GarbageCollectable for UnitCollector<'a> {
    fn kind(&self) -> &'static str {
        "unit"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.vm_mgr.list_all_unit_vm_ids().await?)
    }
    async fn reclaim(&self, vm_id: &str) -> anyhow::Result<()> {
        crate::handlers::delete_vm(
            vm_id,
            self.vm_mgr,
            self.net_mgr,
            self.agent_db,
            self.storage,
        )
        .await
    }
}

/// VM working directories under `<vms_dir>/<vm_id>` whose vm_id is
/// not in the agent DB. These leak when `create_vm_inner` aborts
/// after `mkdir vm_dir` but before the rollback path takes the dir
/// down — most often when a later step (`lvcreate`, tap attach, vfio
/// bind) fails and the rollback hits its own error and bails. The
/// dir holds at most a few hundred KiB (cidata.iso + an empty
/// cloud-hypervisor.sock), but at homelab scale they reach the
/// thousands and slow `ls`.
struct VmDirCollector<'a> {
    vm_mgr: &'a Arc<VmManager>,
}

impl<'a> GarbageCollectable for VmDirCollector<'a> {
    fn kind(&self) -> &'static str {
        "vm_dir"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.vm_mgr.vms_dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                ids.push(name.to_string());
            }
        }
        Ok(ids)
    }
    async fn reclaim(&self, vm_id: &str) -> anyhow::Result<()> {
        let path = self.vm_mgr.vms_dir.join(vm_id);
        tokio::fs::remove_dir_all(&path).await?;
        Ok(())
    }
}

/// LVs (rootfs in the rootfs VG, data in the data VG) whose `vm_id`
/// is not in the agent DB. Unifies both VGs into a single vm_id-keyed
/// set so the orphan sweep makes one decision per VM.
struct LvCollector<'a> {
    storage: &'a Storage,
}

impl<'a> GarbageCollectable for LvCollector<'a> {
    fn kind(&self) -> &'static str {
        "lv"
    }
    async fn list(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .storage
            .list_managed_vm_ids()
            .await?
            .into_iter()
            .collect())
    }
    async fn reclaim(&self, vm_id: &str) -> anyhow::Result<()> {
        self.storage.remove_vm_lv(vm_id).await?;
        self.storage.remove_vm_data_disks(vm_id).await?;
        Ok(())
    }
}

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
///   1. **Agent restart** (no node reboot): systemd transient units survive.
///   2. **Node reboot**: systemd units are gone; agent's local SQLite
///      still has the VM records and disk overlays persist on disk.
///   3. **Orphaned units**: a systemd unit is running but the agent DB
///      has no record of it.
///
/// Controller-side reconciliation happens later, once the agent connects
/// and receives `RegisterHostResponse.initial_state` — any local VM not
/// in that list is deleted by [`crate::handlers::reconcile_against_expected`],
/// and the per-cluster bridges are built by [`NetworkManager::reconcile_clusters`].
pub async fn reconcile_on_startup(
    config: &HostSpec,
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    image_mgr: &crate::image::ImageManager,
    storage: &Storage,
) -> anyhow::Result<ReconcileReport> {
    let mut report = ReconcileReport {
        recovered: 0,
        restarted: 0,
        orphans: 0,
        lost: 0,
        failed: 0,
    };

    let running_vm_ids = vm_mgr.reconcile_running().await?;
    let running_set: std::collections::HashSet<&str> =
        running_vm_ids.iter().map(|s| s.as_str()).collect();

    let known_vms = agent_db.list_vms().await?;
    let known_ids: std::collections::HashSet<String> =
        known_vms.iter().map(|v| v.vm_id.clone()).collect();

    let (running_here, to_restart): (Vec<_>, Vec<_>) = known_vms
        .iter()
        .partition(|vm| running_set.contains(vm.vm_id.as_str()));
    report.recovered = running_here.len() as u32;

    let outcomes: Vec<RestartOutcome> = stream::iter(to_restart)
        .map(|vm_record| async move {
            (
                vm_record.vm_id.clone(),
                restart_vm(config, vm_record, vm_mgr, net_mgr, image_mgr, storage).await,
            )
        })
        .buffer_unordered(RESTART_CONCURRENCY)
        .map(|(vm_id, result)| match result {
            Ok(()) => RestartOutcome::Restarted,
            Err(RestartError::DiskMissing {
                lv_path,
                cidata_path,
            }) => {
                warn!(
                    vm_id = %vm_id,
                    lv_path = %lv_path.display(),
                    cidata_path = %cidata_path.display(),
                    lv_exists = lv_path.exists(),
                    cidata_exists = cidata_path.exists(),
                    "VM disk artifacts missing after reboot — cannot restart; reporting FAILED"
                );
                RestartOutcome::Lost
            }
            Err(RestartError::GpuBindFailed(e)) => {
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

    report.orphans = sweep_orphans(&known_ids, agent_db, vm_mgr, net_mgr, storage).await;

    Ok(report)
}

/// Reclaim host-level resources whose owning vm_id is not in the
/// agent DB. Safe to call at any time — the DB is the authoritative
/// source of live VMs on this host.
pub async fn sweep_orphans(
    known_ids: &HashSet<String>,
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    storage: &Storage,
) -> u32 {
    // Unit reclamation runs first because its reclaim path
    // (`handlers::delete_vm`) drops the associated LV, dir, and
    // tap, and resets the failed-unit record so the same vm_id
    // doesn't show up here again on the next sweep.
    let units = UnitCollector {
        agent_db,
        vm_mgr,
        net_mgr,
        storage,
    };
    let unit_reclaimed = collect(&units, known_ids, usize::MAX).await;

    let lvs = LvCollector { storage };
    let lv_reclaimed = collect(&lvs, known_ids, ORPHAN_LV_BATCH).await;

    // Dir reclamation runs last so the previous two collectors have
    // first crack at any vm_id whose unit/LV is still around. What
    // remains is dirs whose VM never made it past mkdir — i.e.
    // creates that aborted before any unit or LV was committed.
    let dirs = VmDirCollector { vm_mgr };
    let dir_reclaimed = collect(&dirs, known_ids, usize::MAX).await;

    // Taps are deliberately NOT swept here. Taps are owned by the VM
    // lifecycle: `create_vm_inner` writes the agent DB row before
    // `attach_vm_primary` creates the tap, and `delete_vm` stops
    // cloud-hypervisor before detaching the tap. So a tap without a
    // matching DB row is structurally impossible. The only way to
    // produce one is wiping `agent.db` out from under live VMs —
    // recovered from on the next register via the controller's
    // `RegisterHostResponse.initial_state`, not by a defensive sweep.
    unit_reclaimed + lv_reclaimed + dir_reclaimed
}

/// Live orphan sweep — gathers authoritative sets on its own and
/// delegates to [`sweep_orphans`].
pub async fn periodic_sweep(
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    storage: &Storage,
) -> anyhow::Result<u32> {
    let known_vms = agent_db.list_vms().await?;
    let mut known_ids: HashSet<String> = known_vms.iter().map(|v| v.vm_id.clone()).collect();
    // Pending creates aren't in the DB yet (the agent_db row is
    // written after `mkdir vm_dir` and before `systemd-run`), so the
    // dir/unit collector would otherwise reap a VM mid-create. Pull
    // them in from the in-memory pending set.
    known_ids.extend(vm_mgr.live_vm_ids().await);
    Ok(sweep_orphans(&known_ids, agent_db, vm_mgr, net_mgr, storage).await)
}

enum RestartError {
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
    storage: &Storage,
) -> Result<(), RestartError> {
    let vm_dir = config.vms_dir().join(&vm_record.vm_id);
    let rootfs_path = storage.vm_lv_path(&vm_record.vm_id);
    let cloud_init_path = vm_dir.join("cidata.iso");

    if !rootfs_path.exists() || !cloud_init_path.exists() {
        return Err(RestartError::DiskMissing {
            lv_path: rootfs_path,
            cidata_path: cloud_init_path,
        });
    }

    let extra_disk_gibs = vm_record.extra_disks().map_err(|e| {
        RestartError::Other(anyhow::anyhow!(
            "VM {} has malformed local_vms.extra_disk_gibs: {e}",
            vm_record.vm_id,
        ))
    })?;
    let mut data_disk_paths = Vec::with_capacity(extra_disk_gibs.len());
    for index in 0..extra_disk_gibs.len() {
        let path = storage.data_disk_lv_path(&vm_record.vm_id, index as u32);
        if !path.exists() {
            return Err(RestartError::DiskMissing {
                lv_path: path,
                cidata_path: cloud_init_path,
            });
        }
        data_disk_paths.push(path);
    }

    let cached = image_mgr
        .ensure_cached(&vm_record.image, storage)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    info!(vm_id = %vm_record.vm_id, vni = vm_record.vni, "restarting VM after node reboot");

    let vni = u32::try_from(vm_record.vni).map_err(|_| {
        RestartError::Other(anyhow::anyhow!(
            "VM {} has corrupt vni={} in local DB",
            vm_record.vm_id,
            vm_record.vni
        ))
    })?;

    // Controller hasn't connected yet at cold-boot time, so the
    // cluster's peer FDB isn't known — but we can bring the bridge +
    // VXLAN up with an empty peer set so this VM's TAP can attach.
    // The first `reconcile_clusters` call fills in peers.
    net_mgr
        .ensure_bootstrap_cluster(vni)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    let primary_tap = net_mgr
        .attach_vm_primary(&vm_record.vm_id, vni)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    let gpu_addrs = vm_record.gpus().map_err(|e| {
        RestartError::Other(anyhow::anyhow!(
            "VM {} has malformed local_vms.gpu_pci_addresses: {e}",
            vm_record.vm_id,
        ))
    })?;
    let mut vfio_devices = Vec::new();
    for addr in &gpu_addrs {
        match gpu::bind_vfio(addr).await {
            Ok(path) => vfio_devices.push(path),
            Err(e) => {
                for bound in &vfio_devices {
                    let _ = gpu::unbind_vfio(bound).await;
                }
                return Err(RestartError::GpuBindFailed(format!("GPU {addr}: {e}")));
            }
        }
    }

    // Reconstruct a command for the restart. We reuse the existing
    // disk overlay and cloud-init ISO from disk — we are NOT
    // regenerating them. `bootstrap_data` is intentionally empty:
    // cloud-init already ran on first boot and its instance-id check
    // prevents re-provisioning.
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
        vni,
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
            &primary_tap,
            &vfio_devices,
        )
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    info!(vm_id = %vm_record.vm_id, "VM restarted successfully");
    Ok(())
}
