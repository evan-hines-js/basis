//! VM lifecycle operations on this host.
//!
//! One source of truth for "create a VM" and "delete a VM". Called both by
//! the inbound-command loop (`CreateVmCommand` / `DeleteVmCommand`) and by
//! the post-register reconciliation path, which must delete any VMs the
//! controller has forgotten.

use std::sync::Arc;

use basis_common::json::parse_owned_json;
use basis_common::time::now_rfc3339;
use basis_proto::{
    agent_message, AgentMessage, CreateVmCommand, MachineState, ReportVmStateRequest,
};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::image::ImageManager;
use crate::network::NetworkManager;
use crate::vm::{unit_name_for_vm, VmManager};

/// Prepare disk, network, GPU passthrough, and spawn cloud-hypervisor.
///
/// On success, persists a `LocalVmRow` so the agent can recover across
/// restarts.
pub async fn create_vm(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) -> anyhow::Result<()> {
    let vms_dir = vm_mgr.lock().await.vms_dir.clone();
    let vm_dir_path = vms_dir.join(&cmd.vm_id);
    std::fs::create_dir_all(&vm_dir_path)?;

    let base = image_mgr.ensure_cached(&cmd.image).await?;

    let disk_path = image_mgr
        .create_overlay(&base.rootfs, &vm_dir_path, cmd.disk_gib)
        .await?;

    let cloud_init_path = image_mgr
        .create_cloud_init_iso(
            &vm_dir_path,
            &cmd.vm_id,
            &cmd.name,
            &cmd.bootstrap_data,
            &cmd.ip_address,
            &cmd.gateway,
            cmd.prefix_len,
            &cmd.dns_servers,
        )
        .await?;

    let tap_name = net_mgr.create_tap(&cmd.vm_id).await?;

    let mut vfio_devices = Vec::new();
    for pci_addr in &cmd.gpu_pci_addresses {
        vfio_devices.push(gpu::bind_vfio(pci_addr).await?);
    }

    vm_mgr
        .lock()
        .await
        .create_vm(
            cmd,
            &base.kernel,
            &base.initrd,
            &disk_path,
            &cloud_init_path,
            &tap_name,
            &vfio_devices,
        )
        .await?;

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

    info!(vm_id = %cmd.vm_id, ip = %cmd.ip_address, "VM created");
    Ok(())
}

/// Tear down a VM: unbind GPUs, delete tap, stop the systemd unit, remove
/// its local DB record. Best-effort — individual failures are logged but
/// don't abort the rest of the cleanup.
pub async fn delete_vm(
    vm_id: &str,
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) {
    if let Ok(Some(record)) = agent_db.get_vm(vm_id).await {
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
    if let Err(e) = vm_mgr.lock().await.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to stop VM");
    }
    if let Err(e) = agent_db.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to remove local VM record");
    }
    info!(vm_id, "VM deleted");
}

/// Apply the controller's authoritative VM list after registration.
///
/// Any locally-known VM not in `expected_vm_ids` was forgotten by the
/// controller while the agent was offline — its disk overlay, tap, and GPU
/// bindings are garbage.
pub async fn reconcile_against_expected(
    expected_vm_ids: &[String],
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) -> anyhow::Result<()> {
    let expected: std::collections::HashSet<&str> =
        expected_vm_ids.iter().map(String::as_str).collect();

    for vm in agent_db.list_vms().await? {
        if !expected.contains(vm.vm_id.as_str()) {
            warn!(
                vm_id = %vm.vm_id,
                "VM forgotten by controller while agent offline, deleting locally"
            );
            delete_vm(&vm.vm_id, vm_mgr, net_mgr, agent_db).await;
        }
    }
    Ok(())
}

/// Re-verify that every locally-tracked VM is still running. Any VM the
/// local DB knows about but systemd does not is reported as FAILED so the
/// controller (and upstream CAPI) can remediate.
///
/// Runs periodically on the agent to detect local drift without needing
/// the control plane's help — the "thick agent" property.
pub async fn reconcile_running_vms(
    agent_db: &AgentDb,
    vm_mgr: &Arc<Mutex<VmManager>>,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    let vms = agent_db.list_vms().await?;
    for vm in vms {
        let still_running = vm_mgr.lock().await.is_running(&vm.vm_id);
        if !still_running {
            warn!(
                vm_id = %vm.vm_id,
                "VM present in local DB but not running — reporting FAILED"
            );
            send_vm_state(
                sender,
                vm.vm_id,
                MachineState::Failed,
                "vm exited unexpectedly (systemd scope gone)".to_string(),
            )
            .await;
        }
    }
    Ok(())
}

/// Send a single VM state report to the controller.
pub async fn send_vm_state(
    sender: &mpsc::Sender<AgentMessage>,
    vm_id: String,
    state: MachineState,
    error_message: String,
) {
    let msg = AgentMessage {
        payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
            vm_id,
            state: state as i32,
            error_message,
        })),
    };
    if let Err(e) = sender.send(msg).await {
        warn!(error = %e, "dropped VM state report; controller stream is closed");
    }
}

/// Report the state of every locally-known VM to the controller. A VM the
/// reconciler could not restart is reported as FAILED so CAPI can remediate.
pub async fn report_local_vm_states(
    agent_db: &AgentDb,
    vm_mgr: &Arc<Mutex<VmManager>>,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    for vm in agent_db.list_vms().await? {
        let running = vm_mgr.lock().await.is_running(&vm.vm_id);
        let (state, err) = if running {
            (MachineState::Running, String::new())
        } else {
            (
                MachineState::Failed,
                "VM not running after startup reconciliation".to_string(),
            )
        };
        send_vm_state(sender, vm.vm_id, state, err).await;
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

    /// `reconcile_against_expected` deletes everything the controller has
    /// forgotten. We can't run the real delete (no systemd/network in
    /// tests) so we verify the DB-level effect: rows for forgotten VMs
    /// are gone, rows for expected VMs remain.
    #[tokio::test]
    async fn reconcile_deletes_forgotten_vm_records() {
        let db = AgentDb::open(":memory:".as_ref()).await.unwrap();
        db.insert_vm(&fake_vm("keep")).await.unwrap();
        db.insert_vm(&fake_vm("drop-1")).await.unwrap();
        db.insert_vm(&fake_vm("drop-2")).await.unwrap();

        let vm_mgr = Arc::new(Mutex::new(VmManager::new(
            std::env::temp_dir().join("basis-test-reconcile"),
        )));
        let net_mgr = NetworkManager::new("test-br".to_string(), "lo".to_string());

        reconcile_against_expected(&["keep".to_string()], &vm_mgr, &net_mgr, &db)
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
        db.insert_vm(&fake_vm("a")).await.unwrap();
        db.insert_vm(&fake_vm("b")).await.unwrap();

        let vm_mgr = Arc::new(Mutex::new(VmManager::new(
            std::env::temp_dir().join("basis-test-reconcile-noop"),
        )));
        let net_mgr = NetworkManager::new("test-br".to_string(), "lo".to_string());

        reconcile_against_expected(&["a".to_string(), "b".to_string()], &vm_mgr, &net_mgr, &db)
            .await
            .unwrap();

        assert_eq!(db.list_vms().await.unwrap().len(), 2);
    }
}
