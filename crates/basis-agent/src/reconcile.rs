use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use basis_common::json::parse_owned_json;
use basis_proto::CreateVmCommand;

/// Max number of VMs the agent restarts in parallel after a node reboot.
/// On a host with many VMs, a serial cold-start would compound GPU-bind
/// latency into minutes of downtime. The per-VM work that gains from
/// parallelism (disk probe, tap creation, vfio-pci rebind) is IO-bound;
/// the systemd-run spawn at the end still serialises on the VmManager
/// mutex, which is fine.
const RESTART_CONCURRENCY: usize = 4;

use crate::config::HostSpec;
use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::lvm;
use crate::network::NetworkManager;
use crate::vm::VmManager;

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
    vm_mgr: &Arc<Mutex<VmManager>>,
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
    let running_vm_ids = vm_mgr.lock().await.reconcile_running().await?;
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
            Err(RestartError::DiskMissing { vm_id: id, lv_path, cidata_path }) => {
                // Disk files missing (directory or qcow2/ISO gone) — keep
                // the DB record so an operator can see it and let the
                // controller reconciliation pass clean it up. Log with
                // enough detail that an operator can diagnose whether
                // the thin pool was reinitialized or the cidata dir was
                // wiped.
                warn!(
                    vm_id = %id,
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

    // --- Case 3: orphaned systemd units with no DB record ---
    for vm_id in &running_vm_ids {
        if !known_ids.contains(vm_id) {
            warn!(vm_id, "orphaned VM (no DB record), cleaning up");
            vm_mgr.lock().await.delete_vm(vm_id).await.ok();
            report.orphans += 1;
        }
    }

    // --- Case 4: orphaned LVs with no DB record and no systemd unit ---
    // Can happen if the agent crashed between `lvcreate` and `insert_vm`
    // on a previous session. The LV is taking space in the thin pool
    // and will never be reused (VM id wouldn't recur), so reclaim it.
    match lvm::list_vm_lvs().await {
        Ok(lvs) => {
            for vm_id in lvs {
                if !known_ids.contains(&vm_id) && !running_set.contains(vm_id.as_str()) {
                    warn!(vm_id = %vm_id, "orphaned LV (no DB record, no systemd unit), removing");
                    if let Err(e) = lvm::remove_vm_lv(&vm_id).await {
                        warn!(vm_id = %vm_id, error = %e, "failed to remove orphan LV");
                    } else {
                        report.orphans += 1;
                    }
                }
            }
        }
        Err(e) => warn!(error = %e, "could not list VM LVs for orphan cleanup"),
    }

    // --- Case 5: orphan tap devices on the bridge ---
    // Same failure mode as orphan LVs but one layer up: delete_tap can
    // fail mid-shutdown, or the agent can crash between create_tap and
    // insert_vm. Tap names are a hash of vm_id, so we enumerate every
    // bas* tap on the bridge and diff against the set of expected tap
    // names (known VMs + currently-running units).
    match net_mgr.list_basis_taps().await {
        Ok(taps) => {
            let expected: std::collections::HashSet<String> = known_ids
                .iter()
                .chain(running_vm_ids.iter())
                .map(|id| crate::network::tap_name(id))
                .collect();
            let orphans: Vec<String> = taps
                .into_iter()
                .filter(|t| !expected.contains(t))
                .collect();
            if !orphans.is_empty() {
                warn!(count = orphans.len(), "removing orphan taps");
                net_mgr.delete_taps_by_name(&orphans).await;
                report.orphans += orphans.len() as u32;
            }
        }
        Err(e) => warn!(error = %e, "could not list bridge taps for orphan cleanup"),
    }

    Ok(report)
}

enum RestartError {
    /// VM disk artifacts are gone after a reboot. Either the LVM thin
    /// pool was reinitialized (operator ran `lvremove` or re-provisioned
    /// the VG) or the cloud-init ISO directory was wiped. Non-recoverable
    /// at the agent layer — CAPI needs to see FAILED and reschedule.
    DiskMissing {
        vm_id: String,
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
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
    image_mgr: &crate::image::ImageManager,
) -> Result<(), RestartError> {
    let vm_dir = config.vms_dir().join(&vm_record.vm_id);
    let disk_path = lvm::vm_lv_path(&vm_record.vm_id);
    let cloud_init_path = vm_dir.join("cidata.iso");

    // `Path::exists` works on block devices — the thin LV shows up as
    // `/dev/basis/vm-<id>` while active. Missing LV means either the
    // thin pool was reinitialized or the operator removed the LV; we
    // can't restart from that state.
    if !disk_path.exists() || !cloud_init_path.exists() {
        return Err(RestartError::DiskMissing {
            vm_id: vm_record.vm_id.clone(),
            lv_path: disk_path,
            cidata_path: cloud_init_path,
        });
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
    };

    vm_mgr
        .lock()
        .await
        .create_vm(
            &restart_cmd,
            &cached.kernel,
            &cached.initrd,
            &disk_path,
            &cloud_init_path,
            &tap_name,
            &vfio_devices,
        )
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    info!(vm_id = %vm_record.vm_id, "VM restarted successfully");
    Ok(())
}
