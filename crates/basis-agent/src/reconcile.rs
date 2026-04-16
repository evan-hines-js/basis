use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{error, info, warn};

use basis_proto::CreateVmCommand;

use crate::config::AgentConfig;
use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
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
/// The local agent DB is a crash-recovery cache, not the source of truth. The controller's
/// SQLite is the global authority. After this local reconciliation, the agent connects to
/// the controller and reports the state of every VM it has. The controller can then tell
/// the agent to delete VMs it has forgotten or recreate VMs the agent has lost.
///
// TODO: post-register, controller should send its authoritative VM list for this host
// and agent should reconcile (delete VMs controller has forgotten, recreate VMs controller
// expects but agent has lost). Current code trusts local DB, which can drift if controller
// makes changes while agent is offline. The agent DB should be re-derivable from the
// controller — if it's corrupted or deleted, the agent asks "what VMs should I have?"
// and rebuilds.
pub async fn reconcile_on_startup(
    config: &AgentConfig,
    agent_db: &AgentDb,
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
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
    // TODO: restart VMs with bounded parallelism (e.g., futures::stream::for_each_concurrent(4, ...))
    // to avoid serial cold-start on hosts with many VMs. Fine for v1.
    for vm_record in &known_vms {
        if running_set.contains(vm_record.vm_id.as_str()) {
            // Case 1: already running in systemd, re-tracked by reconcile_running()
            report.recovered += 1;
        } else {
            // Case 2: node rebooted, VM not running. Try to re-launch from disk.
            match restart_vm(config, vm_record, vm_mgr, net_mgr).await {
                Ok(()) => {
                    report.restarted += 1;
                }
                Err(RestartError::DiskMissing) => {
                    // VM directory exists but files are missing (anomalous) or
                    // entire directory gone. Keep the DB record so an operator
                    // can see what happened — the controller reconciliation pass
                    // will clean it up authoritatively. Report as lost.
                    warn!(
                        vm_id = %vm_record.vm_id,
                        "VM disk files missing after reboot, cannot restart — reporting FAILED to controller"
                    );
                    report.lost += 1;
                }
                Err(RestartError::GpuBindFailed(e)) => {
                    // GPU failed to rebind. A half-restored VM (booted without its
                    // GPU) is worse than a dead VM — the guest comes up, K8s schedules
                    // GPU pods, pods fail silently. Abort and let CAPI remediate.
                    error!(
                        vm_id = %vm_record.vm_id,
                        error = %e,
                        "GPU rebind failed, aborting VM restart — CAPI will remediate"
                    );
                    report.failed += 1;
                }
                Err(RestartError::Other(e)) => {
                    error!(vm_id = %vm_record.vm_id, error = %e, "failed to restart VM");
                    report.failed += 1;
                }
            }
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

    Ok(report)
}

enum RestartError {
    DiskMissing,
    GpuBindFailed(String),
    Other(anyhow::Error),
}

async fn restart_vm(
    config: &AgentConfig,
    vm_record: &LocalVmRow,
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
) -> Result<(), RestartError> {
    let vm_dir = config.vms_dir().join(&vm_record.vm_id);
    let disk_path = vm_dir.join("disk.qcow2");
    let cloud_init_path = vm_dir.join("cidata.iso");

    if !disk_path.exists() || !cloud_init_path.exists() {
        return Err(RestartError::DiskMissing);
    }

    info!(vm_id = %vm_record.vm_id, "restarting VM after node reboot");

    // Re-create tap device (idempotent — handles EEXIST)
    let tap_name = net_mgr
        .ensure_tap(&vm_record.vm_id)
        .await
        .map_err(|e| RestartError::Other(e.into()))?;

    // Re-bind GPUs. If any GPU fails to bind, abort the entire VM restart.
    // A VM missing a GPU is silently broken — worse than being dead.
    let gpu_addrs: Vec<String> =
        serde_json::from_str(&vm_record.gpu_pci_addresses).unwrap_or_default();
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
                return Err(RestartError::GpuBindFailed(format!(
                    "GPU {addr}: {e}"
                )));
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
    let restart_cmd = CreateVmCommand {
        vm_id: vm_record.vm_id.clone(),
        name: vm_record.name.clone(),
        cpu: vm_record.cpu as u32,
        memory_mib: vm_record.memory_mib as u32,
        disk_gib: vm_record.disk_gib as u32,
        image: vm_record.image.clone(),
        bootstrap_data: Vec::new(),
        ip_address: vm_record.ip_address.clone(),
        gateway: String::new(),
        prefix_len: 0,
        gpus: 0,
        gpu_constraints: None,
        dns_servers: Vec::new(),
        gpu_pci_addresses: serde_json::from_str(&vm_record.gpu_pci_addresses)
            .unwrap_or_default(),
    };

    vm_mgr
        .lock()
        .await
        .create_vm(
            &restart_cmd,
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
