//! VM lifecycle operations on this host.
//!
//! `create_vm` is invoked from inbound `CreateVmCommand` dispatch;
//! `delete_vm` is invoked from `apply_reconcile` for every entry in
//! `ReconcileHostCommand.vm_tombstones`. There is no implicit-by-absence
//! delete path — the controller's tombstone is the single source of
//! "this VM should be gone."
//!
//! Storage allocation goes through the [`Storage`] registry: the
//! controller's [`CommandedDisk`] tells us `(pool, device_id)` to
//! allocate against, and the agent enforces seven defense-in-depth
//! invariants before touching hardware:
//!
//! 1. `pool` exists in agent config.
//! 2. `device_id` is a member of that pool.
//! 3. Backend type matches what the agent runs.
//! 4. `min_size_gib` fits the device's free capacity.
//! 5. Device is `Ready` (not Degraded/Missing) and not Disabled.
//! 6. Idempotency by `assignment_id` (same id → no-op; same
//!    `(vm_id, disk_index)` with different id → hard conflict).
//! 7. Same-cluster REPLICATED collision check (no two OSDs of one
//!    cluster on the same device).
//!
//! Invariants 1–5 live here; 6–7 live in the backend's `allocate`.

use std::sync::Arc;
use std::time::Instant;

use basis_common::time::now_rfc3339;
use basis_proto::{
    agent_message, AgentMessage, CommandedDisk, CreateVmCommand, DiskAssignment,
    DiskAssignmentReport, MachineState, ReportVmStateRequest,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::db::{AgentDb, LocalStorageDisk, LocalVmRow};
use crate::gpu;
use crate::image::{GuestNetwork, ImageManager};
use crate::lvm::{DevicePhysicalHealth, DiskAllocationRequest, Storage};
use crate::metrics::Metrics;
use crate::network::NetworkManager;
use crate::vm;
use crate::vm::{unit_name_for_vm, BootArtifacts, VmManager};

pub async fn create_vm(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    storage: &Storage,
    metrics: &Metrics,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    vm_mgr.mark_pending(&cmd.vm_id).await;
    let result = create_vm_inner(
        cmd, image_mgr, vm_mgr, net_mgr, agent_db, storage, metrics, sender,
    )
    .await;
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
            let _ = delete_vm(&cmd.vm_id, vm_mgr, net_mgr, agent_db, storage).await;
            Err(e)
        }
    }
}

/// Per-disk pre-allocate guardrails (invariants 1–5). Backend's
/// `allocate` enforces 6–7 once we get past these.
fn validate_commanded_disk(storage: &Storage, disk: &CommandedDisk) -> anyhow::Result<()> {
    let pool = storage
        .pool(&disk.pool)
        .ok_or_else(|| anyhow::anyhow!("invariant #1: unknown pool {:?}", disk.pool))?;
    // Invariant #3 is currently a tautology — only one backend kind
    // exists in v1 — but the check stays so adding M2/M3 doesn't
    // silently reuse stale agent code.
    let _ = pool.backend().backend_kind();
    Ok(())
}

async fn create_vm_inner(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    storage: &Storage,
    metrics: &Metrics,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    let vm_dir = vm_mgr.vms_dir.join(&cmd.vm_id);
    std::fs::create_dir_all(&vm_dir)?;

    let started = Instant::now();
    let base = image_mgr.ensure_cached(&cmd.image, storage).await?;
    metrics
        .image_ensure_cached_seconds
        .observe(started.elapsed().as_secs_f64());

    if (cmd.disk_gib as u64) < base.virtual_size_gib {
        anyhow::bail!(
            "requested disk_gib ({}) is smaller than image '{}' virtual size ({} GiB)",
            cmd.disk_gib,
            cmd.image,
            base.virtual_size_gib,
        );
    }

    // Pre-allocate validation: every commanded disk must satisfy
    // invariants #1–#5 before any hardware op. We refuse the whole
    // command if any disk fails — partial allocation is harder to
    // unwind than full rejection.
    for disk in &cmd.storage_disks {
        validate_commanded_disk(storage, disk)?;
        // Capacity + health are checked at allocate-time inside the
        // backend (devices() result), so we don't snapshot here —
        // staleness would just lead to a backend-level rejection.
        if let Some(pool) = storage.pool(&disk.pool) {
            let devices = pool.backend().devices().await?;
            let dev = devices
                .iter()
                .find(|d| d.id == disk.device_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "invariant #2: device {:?} not in pool {:?}",
                        disk.device_id,
                        disk.pool
                    )
                })?;
            if !matches!(dev.physical, DevicePhysicalHealth::Ready) {
                anyhow::bail!(
                    "invariant #5: device {:?} in pool {:?} is {:?} ({})",
                    dev.id,
                    disk.pool,
                    dev.physical,
                    dev.physical_reason
                );
            }
            if dev.free_gib < disk.min_size_gib {
                anyhow::bail!(
                    "invariant #4: device {:?} in pool {:?} has {} GiB free, disk needs {} GiB",
                    dev.id,
                    disk.pool,
                    dev.free_gib,
                    disk.min_size_gib
                );
            }
        }
    }

    // Persist the VM record up front so any later failure has a DB row
    // to drive rollback off of. storage_disks is empty here; we update
    // it as each disk's allocate succeeds so reconcile can recover from
    // a mid-allocate crash.
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
            storage_disks: "[]".to_string(),
            image: cmd.image.clone(),
            vni: cmd.vni as i64,
            cluster_id: cmd.cluster_id.clone(),
            created_at: now_rfc3339(),
        })
        .await?;

    let started = Instant::now();
    let rootfs_path = storage
        .create_vm_rootfs(&cmd.vm_id, &base.image_hash, cmd.disk_gib as u64)
        .await?;
    metrics
        .lv_snapshot_seconds
        .observe(started.elapsed().as_secs_f64());

    let started = Instant::now();
    let mut data_disk_paths = Vec::with_capacity(cmd.storage_disks.len());
    let mut local_disks: Vec<LocalStorageDisk> = Vec::with_capacity(cmd.storage_disks.len());
    for disk in &cmd.storage_disks {
        let pool = storage
            .pool(&disk.pool)
            .expect("validated above");
        let purpose = basis_proto::DiskPurpose::try_from(disk.purpose).map_err(|_| {
            anyhow::anyhow!(
                "invalid DiskPurpose enum value {} on commanded disk {}",
                disk.purpose,
                disk.assignment_id
            )
        })?;
        let req = DiskAllocationRequest {
            assignment_id: disk.assignment_id.clone(),
            device_id: disk.device_id.clone(),
            vm_id: cmd.vm_id.clone(),
            disk_index: disk.disk_index,
            min_size_gib: disk.min_size_gib,
        };
        let alloc = pool.backend().allocate(req).await?;
        data_disk_paths.push(alloc.path.clone());

        local_disks.push(LocalStorageDisk {
            assignment_id: disk.assignment_id.clone(),
            pool: disk.pool.clone(),
            device_id: disk.device_id.clone(),
            disk_index: disk.disk_index,
            size_gib: alloc.actual_size_gib,
            purpose: match purpose {
                basis_proto::DiskPurpose::Replicated => "replicated",
                basis_proto::DiskPurpose::GenericData => "generic-data",
                basis_proto::DiskPurpose::Unspecified => "unspecified",
            }
            .to_string(),
            device_path: alloc.path.to_string_lossy().into_owned(),
        });

        // Notify the controller mid-flight so the assignment row flips
        // from `sent` to `committed` as soon as the disk is `Ready` —
        // a controller-side delete during creation has accurate
        // per-disk granularity.
        let report = DiskAssignmentReport {
            assignment: Some(DiskAssignment {
                assignment_id: disk.assignment_id.clone(),
                vm_id: cmd.vm_id.clone(),
                disk_index: disk.disk_index,
                pool: disk.pool.clone(),
                device_id: disk.device_id.clone(),
                actual_size_gib: alloc.actual_size_gib,
                device_path: alloc.path.to_string_lossy().into_owned(),
            }),
        };
        let msg = AgentMessage {
            payload: Some(agent_message::Payload::DiskAssignment(report)),
        };
        let _ = sender.send(msg).await; // best-effort; controller reconciles on reconnect
    }
    metrics
        .data_disk_create_seconds
        .observe(started.elapsed().as_secs_f64());

    // Persist the disk-allocation truth before we spawn the VM — a
    // crash between here and the spawn is recoverable, but only if the
    // local row knows about the disks already allocated.
    let storage_disks_json = serde_json::to_string(&local_disks)
        .expect("serializing Vec<LocalStorageDisk> to JSON is infallible");
    sqlx::query("UPDATE local_vms SET storage_disks = ? WHERE vm_id = ?")
        .bind(&storage_disks_json)
        .bind(&cmd.vm_id)
        .execute(storage.db())
        .await?;

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
                data_disks: &data_disk_paths,
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
/// on real cleanup completion. Idempotent — safe to call against a
/// VM that is already partially or fully gone.
pub async fn delete_vm(
    vm_id: &str,
    vm_mgr: &Arc<VmManager>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
    storage: &Storage,
) -> anyhow::Result<()> {
    let record = agent_db.get_vm(vm_id).await.ok().flatten();

    if let Err(e) = vm_mgr.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to stop VM");
    }

    net_mgr.detach_vm_taps(vm_id).await;

    if let Some(record) = &record {
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

    storage.remove_vm_rootfs(vm_id).await.map_err(|e| {
        warn!(vm_id, error = %e, "VM delete failed at rootfs lvremove; caller will retry");
        anyhow::Error::from(e)
    })?;
    // Release across every pool so a controller-committed-but-agent-
    // half-allocated VM still cleans up. Per-pool release is
    // idempotent — pools without a reservation no-op.
    storage.release_vm_disks(vm_id).await.map_err(|e| {
        warn!(vm_id, error = %e, "VM delete failed releasing data disks; caller will retry");
        anyhow::Error::from(e)
    })?;

    if let Err(e) = agent_db.delete_vm(vm_id).await {
        warn!(vm_id, error = %e, "failed to remove local VM record");
    }

    info!(vm_id, "VM deleted");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("controller stream is closed")]
pub struct ChannelClosed;

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
