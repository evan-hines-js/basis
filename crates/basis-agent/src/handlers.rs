//! VM lifecycle operations on this host.
//!
//! One source of truth for "create a VM" and "delete a VM". `create_vm`
//! is invoked from the inbound `CreateVmCommand` dispatch; `delete_vm`
//! is invoked from `apply_reconcile` for every entry in
//! `ReconcileHostCommand.vm_tombstones`. There is no implicit-by-absence
//! delete path — the controller's tombstone is the single source of
//! "this VM should be gone."

use std::sync::Arc;
use std::time::Instant;

use basis_common::time::now_rfc3339;
use basis_proto::{
    agent_message, AgentMessage, CreateVmCommand, MachineState, ReportVmStateRequest,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::db::{AgentDb, LocalVmRow};
use crate::gpu;
use crate::image::{GuestNetwork, ImageManager};
use crate::lvm;
use crate::metrics::Metrics;
use crate::network::NetworkManager;
use crate::vm;
use crate::vm::{unit_name_for_vm, BootArtifacts, VmManager};

/// Prepare disk, network, GPU passthrough, and spawn cloud-hypervisor.
///
/// On any failure mid-way through, every step that already ran is
/// rolled back via [`delete_vm`] so the host is left as if the create
/// never happened. The local DB row is inserted *before* the systemd-
/// run spawn so rollback can find the state it needs; if a crash
/// happens between insert and spawn, the startup reconciler sees a row
/// pointing at non-existent disk artifacts and reports FAILED.
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
            info!(vm_id = %cmd.vm_id, ip = %cmd.ip_address, vni = cmd.vni, "VM created");
            Ok(())
        }
        Err(e) => {
            warn!(
                vm_id = %cmd.vm_id,
                error = %e,
                "create_vm failed; rolling back partial state"
            );
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

    let started = Instant::now();
    let base = image_mgr.ensure_cached(&cmd.image).await?;
    metrics
        .image_ensure_cached_seconds
        .observe(started.elapsed().as_secs_f64());

    // Reject sub-image disk requests at the API boundary. The LVM layer
    // tolerates "would shrink" by leaving the snapshot at its original
    // size (see `lvm::lvextend`), but a request below the image's
    // virtual size is operator error: the caller is asking for a guest
    // disk smaller than what the image already occupies, and quietly
    // rounding up would make quota / billing accounting lie. Fail fast
    // so the surface error names the cause instead of looking like a
    // disk allocation failure later.
    if (cmd.disk_gib as u64) < base.virtual_size_gib {
        anyhow::bail!(
            "requested disk_gib ({}) is smaller than image '{}' virtual size ({} GiB) — \
             raise disk_gib to at least the image's virtual size",
            cmd.disk_gib,
            cmd.image,
            base.virtual_size_gib,
        );
    }

    // Persist the VM record up front so any later failure has a DB row
    // to drive rollback off of.
    let extra_disk_gibs: Vec<u32> = cmd.extra_disks.iter().map(|d| d.size_gib).collect();
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
            extra_disk_gibs: serde_json::to_string(&extra_disk_gibs)
                .expect("serializing Vec<u32> to JSON is infallible"),
            image: cmd.image.clone(),
            vni: cmd.vni as i64,
            created_at: now_rfc3339(),
        })
        .await?;

    let started = Instant::now();
    let rootfs_path = lvm::create_vm_lv(&cmd.vm_id, &base.image_hash, cmd.disk_gib as u64).await?;
    metrics
        .lv_snapshot_seconds
        .observe(started.elapsed().as_secs_f64());

    let started = Instant::now();
    let mut data_disk_paths = Vec::with_capacity(cmd.extra_disks.len());
    for (index, disk) in cmd.extra_disks.iter().enumerate() {
        let path = lvm::create_data_disk_lv(&cmd.vm_id, index as u32, disk.size_gib as u64).await?;
        data_disk_paths.push(path);
    }
    metrics
        .data_disk_create_seconds
        .observe(started.elapsed().as_secs_f64());

    // MAC must match what we'll pass on cloud-hypervisor's `--net mac=`
    // arg below. netplan binds the cloud-init network-config by MAC,
    // so the guest's kernel-assigned interface name (`ens3` / etc)
    // being unstable across PCI-slot reorderings is harmless.
    let primary_mac = vm::primary_mac(&cmd.vm_id);

    let started = Instant::now();
    let cloud_init_path = image_mgr
        .create_cloud_init_iso(
            &vm_dir,
            &cmd.vm_id,
            &cmd.name,
            &cmd.bootstrap_data,
            &GuestNetwork {
                mac: &primary_mac,
                ip_address: &cmd.ip_address,
                gateway: &cmd.gateway,
                prefix_len: cmd.prefix_len,
                dns_servers: &cmd.dns_servers,
                mtu: net_mgr.inner_mtu(),
            },
        )
        .await?;
    metrics
        .cloud_init_iso_seconds
        .observe(started.elapsed().as_secs_f64());

    let started = Instant::now();
    let primary_tap = net_mgr.attach_vm_primary(&cmd.vm_id, cmd.vni).await?;
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
                rootfs: &rootfs_path,
                cloud_init: &cloud_init_path,
                extra_disks: &data_disk_paths,
            },
            &primary_tap,
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
/// on real cleanup completion.
pub async fn delete_vm(
    vm_id: &str,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) -> anyhow::Result<()> {
    // Read the record first so we still have the GPU PCI list after
    // the row is gone.
    let record = agent_db.get_vm(vm_id).await.ok().flatten();

    if let Err(e) = agent_db.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to remove local VM record");
    }

    if let Some(record) = record {
        match record.gpus() {
            Ok(addrs) => {
                for addr in addrs {
                    if let Err(e) = gpu::unbind_vfio(&addr).await {
                        warn!(vm_id, pci = %addr, error = %e, "failed to unbind GPU");
                    }
                }
            }
            Err(e) => warn!(
                vm_id, error = %e,
                "failed to parse local_vms.gpu_pci_addresses; skipping VFIO unbind",
            ),
        }
    }

    net_mgr.detach_vm_taps(vm_id).await;

    if let Err(e) = vm_mgr.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to stop VM");
    }
    // lvremove comes last: cloud-hypervisor holds its LVs exclusively
    // until its process exits.
    lvm::remove_vm_lv(vm_id).await.map_err(|e| {
        warn!(vm_id, error = %e, "VM delete failed at lvremove; caller will retry");
        anyhow::Error::from(e)
    })?;
    lvm::remove_vm_data_disks(vm_id).await.map_err(|e| {
        warn!(vm_id, error = %e, "VM delete failed removing data disks; caller will retry");
        anyhow::Error::from(e)
    })?;
    info!(vm_id, "VM deleted");
    Ok(())
}

/// Error returned when the agent→controller channel has been closed.
/// Callers in one-shot contexts (post-create/delete reporting) should
/// log + move on; callers in a periodic loop should treat this as the
/// signal to exit their loop — the session owning that channel is
/// gone and a new session will have spawned fresh loops.
#[derive(Debug, thiserror::Error)]
#[error("controller stream is closed")]
pub struct ChannelClosed;

/// Send a single VM state report to the controller. Propagates
/// [`ChannelClosed`] so periodic reporters can exit cleanly on
/// session teardown instead of spamming the log on every tick.
pub async fn send_vm_state(
    sender: &mpsc::Sender<AgentMessage>,
    vm_id: String,
    state: MachineState,
    error_message: String,
    transient: bool,
) -> Result<(), ChannelClosed> {
    let msg = AgentMessage {
        payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
            vm_id,
            state: state as i32,
            error_message,
            transient,
        })),
    };
    sender.send(msg).await.map_err(|_| ChannelClosed)
}

/// Report the state of every locally-known VM to the controller.
/// Stops on the first [`ChannelClosed`] — the session is gone so
/// the remaining sends would fail too.
pub async fn report_local_vm_states(
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    for vm in agent_db.list_vms().await? {
        if vm_mgr.is_pending(&vm.vm_id).await {
            continue;
        }
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
        send_vm_state(sender, vm.vm_id, state, err, false).await?;
    }
    Ok(())
}
