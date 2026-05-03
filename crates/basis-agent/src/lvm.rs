//! Per-host VM disk storage.
//!
//! # Two parts
//!
//! ## Rootfs (`Storage::rootfs()`)
//!
//! One thin pool per host (`<rootfs.vg>/<rootfs.thin_pool>`). Per-VM
//! rootfs LVs are CoW snapshots of a golden image LV. Thin is correct
//! here: snapshot create is sub-second, overcommit is fine because an
//! unused gigabyte is just a metadata reservation, and the host can run
//! dozens of VMs from one image without paying its full virtual size
//! per VM.
//!
//! Single rootfs pool by design (M1 non-goal: multi-tier rootfs). Every
//! VM's rootfs lands in this one thin pool; data tiering happens on the
//! data side.
//!
//! ## Data pools (`Storage::pool(...)`)
//!
//! Operator-labeled, backend-typed groupings of physical devices. Each
//! pool wears `tier=…, medium=…, isolation=…` labels (operator
//! vocabulary); the controller's scheduler matches per-disk
//! [`LabelSelector`]s against those labels and picks a `(host, pool,
//! device)` tuple. The agent receives a [`CommandedDisk`] naming the
//! resolved pool and device and has nothing to decide — it just
//! allocates.
//!
//! Within a pool, each device has its own VG (one PV per VG, one VG
//! per device). This is what makes device-level failure-domain free:
//! an LV cannot span PVs because there is only one PV per VG. A
//! `(pool, device)` tuple resolves to exactly one VG.
//!
//! # Reservation semantics
//!
//! Hardware mutation cannot be made transactional with a SQLite write,
//! so reservations are stateful. A reservation goes through:
//!
//!   `Creating` → `Ready` → `Deleting`
//!
//! plus a terminal `Lost` state for hardware that disappears under a
//! `Ready` reservation. [`LvmLinearBackend::reconcile`] drives any
//! `Creating`/`Deleting` row left over from a crash forward, surfaces
//! `Lost` reservations as degraded, and cleans up basis-pattern orphan
//! LVs whose VM is gone.
//!
//! # Naming
//!
//! - `image-<sha>`              — golden raw LV in the rootfs VG.
//! - `vm-<vm_id>`               — per-VM rootfs snapshot in the rootfs VG.
//! - `basis-data-<vm_id>-<idx>` — per-VM data LV in a data pool's VG.
//!
//! The `basis-data-` prefix replaces the legacy `vmdata-` prefix that
//! existed for PVE coexistence. With the new design every basis-managed
//! VG is basis-exclusive, and the prefix doubles as the foreign-LV
//! detector (any LV in a basis-managed VG that doesn't match the
//! pattern is operator misuse and the agent refuses to start).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::process::Command;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{info, warn};

use crate::config::{HostSpec, PoolBackend, PoolSpec};
use crate::metrics;

/// Ceiling on concurrent lvm2 mutation commands per VG.
///
/// 1 by design. lvm2 takes a per-VG metadata lock for every mutation
/// internally; concurrency against the same VG just queues inside lvm2
/// and burns CPU on contending processes. Each `Pool` (and the rootfs
/// pool) owns its own [`Semaphore`] so distinct pools don't block each
/// other — only same-VG operations serialize.
const MAX_CONCURRENT_LVM_MUTATIONS_PER_VG: usize = 1;

/// Maximum wall-clock wait for an LVM mutation permit before an
/// acquirer gives up. Bounds tail latency under sustained backpressure.
const LVM_PERMIT_TIMEOUT: Duration = Duration::from_secs(60);

const IMAGE_LV_PREFIX: &str = "image-";
const VM_LV_PREFIX: &str = "vm-";
/// Prefix every data LV in every basis-managed pool VG carries. Doubles
/// as the foreign-LV detector — the startup validator refuses to start
/// if any LV in a basis-managed VG lacks this prefix.
const DATA_LV_PREFIX: &str = "basis-data-";

#[derive(Debug, thiserror::Error)]
pub enum LvmError {
    #[error(
        "volume group '{0}' not found — run the basis-prereqs ansible role on this host to \
         provision the LVM layout"
    )]
    VgMissing(String),

    #[error(
        "thin pool '{vg}/{pool}' not found — the volume group exists but the thin pool was not \
         created; re-run the basis-prereqs ansible role"
    )]
    ThinPoolMissing { vg: String, pool: String },

    #[error("'{vg}/{pool}' exists but is not a thin pool (lv_attr={attr})")]
    NotThinPool {
        vg: String,
        pool: String,
        attr: String,
    },

    #[error(
        "vg '{vg}' has {pv_count} PVs (expected exactly 1 — basis enforces one VG per device for \
         device-level failure-domain integrity)"
    )]
    VgPvCountWrong { vg: String, pv_count: usize },

    #[error(
        "vg '{vg}' contains foreign LV '{lv}' that does not match the basis-managed pattern; \
         basis-managed VGs are basis-exclusive"
    )]
    ForeignLv { vg: String, lv: String },

    #[error("pool {pool:?} not found in this agent's storage config")]
    PoolNotFound { pool: String },

    #[error("device {device_id:?} is not a member of pool {pool:?}")]
    DeviceNotInPool { pool: String, device_id: String },

    #[error("device {device_id:?} in pool {pool:?} is not Ready ({reason})")]
    DeviceNotReady {
        pool: String,
        device_id: String,
        reason: String,
    },

    #[error(
        "device {device_id:?} in pool {pool:?} has only {free_gib} GiB free; request needs {needed_gib} GiB"
    )]
    InsufficientCapacity {
        pool: String,
        device_id: String,
        free_gib: u64,
        needed_gib: u64,
    },

    #[error(
        "assignment {assignment_id:?} for vm {vm_id:?} disk {disk_index} conflicts with an \
         existing reservation owned by a different assignment_id (controller bug or stale retry)"
    )]
    AssignmentConflict {
        assignment_id: String,
        vm_id: String,
        disk_index: u32,
    },

    #[error("lvm command `{cmd}` failed: {stderr}")]
    Command { cmd: String, stderr: String },

    #[error("qemu-img convert into LV failed: {0}")]
    ImageConvert(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("agent db error: {0}")]
    Db(#[from] sqlx::Error),

    #[error(
        "lvm backend busy on {role}: could not acquire permit within {timeout:?} — \
         arrival rate exceeds service rate, shed load or retry"
    )]
    Busy {
        role: String,
        timeout: Duration,
    },
}

pub type Result<T> = std::result::Result<T, LvmError>;

/// Per-pool semaphore wrapping [`MAX_CONCURRENT_LVM_MUTATIONS_PER_VG`].
/// One per backend instance; rootfs gets its own.
struct VgGate {
    role: String,
    sem: Semaphore,
}

impl VgGate {
    fn new(role: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            sem: Semaphore::new(MAX_CONCURRENT_LVM_MUTATIONS_PER_VG),
        }
    }

    async fn acquire(&self) -> Result<SemaphorePermit<'_>> {
        let started = Instant::now();
        let result = tokio::time::timeout(LVM_PERMIT_TIMEOUT, self.sem.acquire()).await;
        if let Some(m) = metrics::global() {
            m.lv_permit_wait_seconds
                .observe(started.elapsed().as_secs_f64());
        }
        match result {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => unreachable!("VgGate semaphores are never closed"),
            Err(_) => Err(LvmError::Busy {
                role: self.role.clone(),
                timeout: LVM_PERMIT_TIMEOUT,
            }),
        }
    }
}

// --- Public types: Allocation, request, device, capacity --------------

#[derive(Debug, Clone)]
pub struct Allocation {
    pub path: PathBuf,
    pub actual_size_gib: u64,
}

/// A controller-issued allocation command, addressed to a specific
/// `(pool, device_id)` tuple. The agent's `DiskBackend::allocate` does
/// not pick placement; it executes what the controller chose.
#[derive(Debug, Clone)]
pub struct DiskAllocationRequest {
    pub assignment_id: String,
    pub device_id: String,
    pub vm_id: String,
    pub disk_index: u32,
    pub min_size_gib: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevicePhysicalHealth {
    Ready,
    Degraded,
    Missing,
}

impl DevicePhysicalHealth {
    pub fn as_proto(self) -> basis_proto::DevicePhysicalHealth {
        match self {
            Self::Ready => basis_proto::DevicePhysicalHealth::DeviceHealthReady,
            Self::Degraded => basis_proto::DevicePhysicalHealth::DeviceHealthDegraded,
            Self::Missing => basis_proto::DevicePhysicalHealth::DeviceHealthMissing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolDevice {
    pub id: String,
    pub total_gib: u64,
    pub free_gib: u64,
    pub physical: DevicePhysicalHealth,
    pub physical_reason: String,
}

/// Aggregated capacity for one rootfs thin pool.
#[derive(Debug, Clone, Copy, Default)]
pub struct RootfsBytes {
    pub total: u64,
    pub free: u64,
    pub metadata_total: u64,
    pub metadata_free: u64,
}

/// Aggregated capacity for one data pool, decomposed into the four
/// layers operators need to distinguish.
#[derive(Debug, Clone)]
pub struct PoolCapacity {
    pub pool: String,
    pub backend: PoolBackend,
    pub labels: BTreeMap<String, String>,
    pub configured_total_bytes: u64,
    pub ready_total_bytes: u64,
    pub schedulable_total_bytes: u64,
    pub schedulable_free_bytes: u64,
    pub devices: Vec<PoolDevice>,
}

#[derive(Debug, Clone)]
pub struct StorageCapacity {
    pub rootfs: RootfsBytes,
    pub pools: Vec<PoolCapacity>,
}

// --- DiskBackend trait ------------------------------------------------

/// Per-disk reconcile outcomes the agent surfaces to the controller.
/// Empty when reconcile had nothing to report.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub orphans_removed: usize,
    pub creating_resolved: usize,
    pub deleting_resolved: usize,
    pub lost_reservations: Vec<LostReservation>,
}

#[derive(Debug, Clone)]
pub struct LostReservation {
    pub assignment_id: String,
    pub vm_id: String,
    pub disk_index: u32,
    pub pool: String,
    pub device_id: String,
}

#[async_trait]
pub trait DiskBackend: Send + Sync + std::any::Any {
    /// Allocate on a specific commanded device. Returns the actual
    /// allocated size (which MAY exceed `min_size_gib` for raw-disk and
    /// nvme-namespace) and the path to hand to cloud-hypervisor.
    async fn allocate(&self, req: DiskAllocationRequest) -> Result<Allocation>;

    /// Release every disk currently bound to `vm_id`. Idempotent across
    /// `Creating` / `Ready` / `Deleting` reservation states.
    async fn release(&self, vm_id: &str) -> Result<()>;

    /// Per-device capacity and health snapshot. Drives both telemetry
    /// and the agent's storage-capacity heartbeat.
    async fn devices(&self) -> Result<Vec<PoolDevice>>;

    /// After agent restart, reconcile reservation rows against on-disk
    /// state. Drives `Creating`/`Deleting` rows forward, surfaces `Lost`
    /// reservations, removes basis-pattern orphans.
    async fn reconcile(&self, live_vm_ids: &HashSet<String>) -> Result<ReconcileReport>;

    /// Validation that runs before serving traffic. Catches ansible
    /// drift (renamed VG, foreign LV, missing PV) before a hardware op
    /// can run.
    async fn validate(&self) -> Result<()>;

    /// Pool labels — surfaced in the heartbeat so the controller can
    /// re-resolve pool→labels if its in-memory cache is stale.
    fn labels(&self) -> &BTreeMap<String, String>;

    /// Backend type, for telemetry.
    fn backend_kind(&self) -> PoolBackend;

    /// Erase to `Any` so `Storage::list_managed_vm_ids` can downcast to
    /// the concrete backend when it needs to enumerate VGs.
    fn as_any(&self) -> &dyn std::any::Any;
}

// --- One pool: name + Box<dyn DiskBackend> ---------------------------

pub struct Pool {
    name: String,
    backend: Box<dyn DiskBackend>,
}

impl Pool {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn backend(&self) -> &dyn DiskBackend {
        &*self.backend
    }
}

// --- Storage: rootfs + pool registry ----------------------------------

pub struct Storage {
    rootfs: RootfsConfig,
    rootfs_gate: VgGate,
    pools: HashMap<String, Pool>,
    /// Cluster-id of every VM the agent has live, kept in sync with the
    /// agent DB. Used to validate the agent's same-cluster collision
    /// guardrail without joining tables every allocate.
    db: SqlitePool,
}

#[derive(Debug, Clone)]
struct RootfsConfig {
    vg: String,
    thin_pool: String,
}

impl Storage {
    /// Build the [`Storage`] registry from the host's `storage:` block.
    /// Backend instances open their own reservation tables off the
    /// shared agent DB; the same DB serves as the canonical local truth
    /// for every backend.
    pub fn from_host_spec(spec: &HostSpec, db: SqlitePool) -> Self {
        let mut pools = HashMap::new();
        for pool_spec in &spec.storage.pools {
            let backend: Box<dyn DiskBackend> = match pool_spec.backend {
                PoolBackend::LvmLinear => {
                    Box::new(LvmLinearBackend::new(pool_spec.clone(), db.clone()))
                }
                PoolBackend::RawDisk | PoolBackend::NvmeNamespace => unreachable!(
                    "config::StorageSpec::validate rejected unsupported backend earlier"
                ),
            };
            pools.insert(
                pool_spec.name.clone(),
                Pool {
                    name: pool_spec.name.clone(),
                    backend,
                },
            );
        }
        Self {
            rootfs: RootfsConfig {
                vg: spec.storage.rootfs.vg.clone(),
                thin_pool: spec.storage.rootfs.thin_pool.clone(),
            },
            rootfs_gate: VgGate::new("rootfs"),
            pools,
            db,
        }
    }

    pub fn rootfs_vg(&self) -> &str {
        &self.rootfs.vg
    }

    pub fn pool(&self, name: &str) -> Option<&Pool> {
        self.pools.get(name)
    }

    pub fn pools(&self) -> impl Iterator<Item = &Pool> {
        self.pools.values()
    }

    pub fn db(&self) -> &SqlitePool {
        &self.db
    }

    /// Path to the golden image LV for `image_hash` in the rootfs VG.
    pub fn image_lv_path(&self, image_hash: &str) -> PathBuf {
        PathBuf::from(format!("/dev/{}/{IMAGE_LV_PREFIX}{image_hash}", self.rootfs.vg))
    }

    /// Path to a VM's rootfs LV in the rootfs VG.
    pub fn vm_lv_path(&self, vm_id: &str) -> PathBuf {
        PathBuf::from(format!("/dev/{}/{VM_LV_PREFIX}{vm_id}", self.rootfs.vg))
    }

    /// Validate rootfs and every pool. Fail-fast at agent startup so
    /// the agent never silently degrades.
    pub async fn validate(&self) -> Result<StorageCapacity> {
        check_vg_exists(&self.rootfs.vg).await?;
        check_thin_pool(&self.rootfs.vg, &self.rootfs.thin_pool).await?;
        for pool in self.pools.values() {
            pool.backend.validate().await?;
        }
        let cap = self.capacity().await?;
        info!(
            rootfs_vg = %self.rootfs.vg,
            rootfs_thin_pool = %self.rootfs.thin_pool,
            rootfs_data_free_gib = cap.rootfs.free / (1 << 30),
            rootfs_data_total_gib = cap.rootfs.total / (1 << 30),
            pool_count = cap.pools.len(),
            "storage ready"
        );
        Ok(cap)
    }

    /// Current capacity of every pool. Cheap enough to call on every
    /// heartbeat tick — one `lvs`/`vgs` per pool.
    pub async fn capacity(&self) -> Result<StorageCapacity> {
        let rootfs = thin_pool_capacity(&self.rootfs.vg, &self.rootfs.thin_pool).await?;
        let mut pool_caps = Vec::with_capacity(self.pools.len());
        for pool in self.pools.values() {
            pool_caps.push(pool_capacity(pool).await?);
        }
        Ok(StorageCapacity {
            rootfs,
            pools: pool_caps,
        })
    }

    /// Thin-snapshot a golden image into a writable per-VM rootfs LV
    /// extended to `disk_gib`. Idempotent — a stale snapshot from a
    /// crashed prior create is removed and recreated.
    pub async fn create_vm_rootfs(
        &self,
        vm_id: &str,
        image_hash: &str,
        disk_gib: u64,
    ) -> Result<PathBuf> {
        let vg = &self.rootfs.vg;
        let _permit = self.rootfs_gate.acquire().await?;

        let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
        let origin = format!("{IMAGE_LV_PREFIX}{image_hash}");
        let lv_path = self.vm_lv_path(vm_id);

        if lv_attr(vg, &lv_name).await?.is_some() {
            warn!(vm_id, "VM rootfs LV already exists; removing for clean recreate");
            lvremove(vg, &lv_name).await?;
        }

        run_cmd(
            "lvcreate",
            &[
                "--snapshot",
                "--name",
                &lv_name,
                "--setactivationskip",
                "n",
                "--permission",
                "rw",
                &format!("{vg}/{origin}"),
            ],
        )
        .await?;

        lvextend(vg, &lv_name, disk_gib).await?;
        info!(vm_id, lv = %lv_name, origin = %origin, disk_gib, "VM rootfs ready");
        Ok(lv_path)
    }

    /// Remove a VM's rootfs LV. No-op if it doesn't exist.
    pub async fn remove_vm_rootfs(&self, vm_id: &str) -> Result<()> {
        let vg = &self.rootfs.vg;
        let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
        if lv_attr(vg, &lv_name).await?.is_none() {
            return Ok(());
        }
        let _permit = self.rootfs_gate.acquire().await?;
        lvremove(vg, &lv_name).await
    }

    /// Ensure the golden image LV exists for `image_hash`, populated
    /// from `source_qcow2`. Idempotent; uses the LV's RO bit as the
    /// "populated" marker.
    pub async fn ensure_image_lv(
        &self,
        image_hash: &str,
        source_qcow2: &Path,
        virtual_size_gib: u64,
    ) -> Result<PathBuf> {
        let vg = &self.rootfs.vg;
        let pool = &self.rootfs.thin_pool;
        let lv_name = format!("{IMAGE_LV_PREFIX}{image_hash}");
        let lv_path = self.image_lv_path(image_hash);

        if matches!(lv_attr(vg, &lv_name).await?.as_deref(), Some(a) if attr_is_readonly(a)) {
            return Ok(lv_path);
        }

        let _permit = self.rootfs_gate.acquire().await?;

        match lv_attr(vg, &lv_name).await? {
            Some(a) if attr_is_readonly(&a) => return Ok(lv_path),
            Some(_) => {
                warn!(lv = %lv_name, "golden image LV exists but is not RO; recreating");
                lvremove(vg, &lv_name).await?;
            }
            None => {}
        }

        run_cmd(
            "lvcreate",
            &[
                "--virtualsize",
                &format!("{virtual_size_gib}G"),
                "--thin",
                "--name",
                &lv_name,
                &format!("{vg}/{pool}"),
            ],
        )
        .await?;

        info!(
            src = %source_qcow2.display(),
            dst = %lv_path.display(),
            "converting qcow2 image into golden LV"
        );
        let status = Command::new("qemu-img")
            .args([
                "convert",
                "-O",
                "raw",
                "-t",
                "none",
                "-T",
                "none",
                &source_qcow2.to_string_lossy(),
                &lv_path.to_string_lossy(),
            ])
            .status()
            .await
            .map_err(|e| LvmError::ImageConvert(format!("qemu-img spawn: {e}")))?;
        if !status.success() {
            lvremove(vg, &lv_name).await.ok();
            return Err(LvmError::ImageConvert(format!(
                "qemu-img exited {status} converting {} to {}",
                source_qcow2.display(),
                lv_path.display()
            )));
        }

        run_cmd(
            "lvchange",
            &["--permission", "r", &format!("{vg}/{lv_name}")],
        )
        .await?;
        info!(lv = %lv_name, "golden image LV ready");
        Ok(lv_path)
    }

    /// Release every data disk bound to `vm_id`, across every pool.
    /// Idempotent — pools without a reservation for `vm_id` no-op.
    /// Called from the VM-create rollback path AND from delete; covers
    /// the "controller committed against pools A, B, but only A's
    /// allocate succeeded" case correctly because both pools are tried.
    pub async fn release_vm_disks(&self, vm_id: &str) -> Result<()> {
        for pool in self.pools.values() {
            pool.backend.release(vm_id).await?;
        }
        Ok(())
    }

    /// Reconcile every backend after agent restart. `live_vm_ids` is
    /// the set of vm_ids the agent's `local_vms` table still knows
    /// about; reservations referencing absent VMs are dropped, orphans
    /// matching the basis pattern are removed.
    pub async fn reconcile(&self, live_vm_ids: &HashSet<String>) -> Result<Vec<ReconcileReport>> {
        let mut out = Vec::with_capacity(self.pools.len());
        for pool in self.pools.values() {
            out.push(pool.backend.reconcile(live_vm_ids).await?);
        }
        Ok(out)
    }

    /// Every vm_id with at least one LV on this host — rootfs LV in
    /// the rootfs VG, or any data disk LV in any pool's VG.
    pub async fn list_managed_vm_ids(&self) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();
        let rootfs_lvs = run_cmd(
            "lvs",
            &["--noheadings", "-o", "lv_name", &self.rootfs.vg],
        )
        .await?;
        for line in rootfs_lvs.lines() {
            let name = line.trim();
            if let Some(id) = name.strip_prefix(VM_LV_PREFIX) {
                ids.insert(id.to_string());
            }
        }
        for pool in self.pools.values() {
            if let Some(lvm) = pool.backend.as_any().downcast_ref::<LvmLinearBackend>() {
                for vg in lvm.vgs() {
                    let lvs = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]).await?;
                    for line in lvs.lines() {
                        let name = line.trim();
                        if let Some((vm_id, _)) = parse_data_disk_lv_name(name) {
                            ids.insert(vm_id);
                        }
                    }
                }
            }
        }
        Ok(ids)
    }
}

// --- LvmLinearBackend ------------------------------------------------

/// One row of the `lvm_reservation` table.
#[derive(Debug, Clone, sqlx::FromRow)]
struct LvmReservationRow {
    assignment_id: String,
    pool: String,
    device_id: String,
    vg: String,
    lv_name: String,
    vm_id: String,
    disk_index: i64,
    size_gib: i64,
    state: String,
}

/// LVM-linear backend: one VG per device, linear LVs for each disk.
pub struct LvmLinearBackend {
    spec: PoolSpec,
    /// Map device_id → vg, materialized once at construction so
    /// `allocate` doesn't search the spec on the hot path.
    device_vgs: HashMap<String, String>,
    db: SqlitePool,
    gate: VgGate,
}

impl LvmLinearBackend {
    pub fn new(spec: PoolSpec, db: SqlitePool) -> Self {
        let device_vgs = spec
            .devices
            .iter()
            .map(|d| (d.id.clone(), d.vg.clone().expect("vg required for lvm-linear; verified by config::StorageSpec::validate")))
            .collect();
        Self {
            gate: VgGate::new(format!("pool={}", spec.name)),
            spec,
            device_vgs,
            db,
        }
    }

    fn vg_for(&self, device_id: &str) -> Result<&str> {
        self.device_vgs
            .get(device_id)
            .map(String::as_str)
            .ok_or_else(|| LvmError::DeviceNotInPool {
                pool: self.spec.name.clone(),
                device_id: device_id.to_string(),
            })
    }

    fn vgs(&self) -> impl Iterator<Item = &str> {
        self.device_vgs.values().map(String::as_str)
    }

    fn lv_name(vm_id: &str, disk_index: u32) -> String {
        format!("{DATA_LV_PREFIX}{vm_id}-{disk_index}")
    }

    fn lv_path(vg: &str, lv_name: &str) -> PathBuf {
        PathBuf::from(format!("/dev/{vg}/{lv_name}"))
    }

    /// Idempotency check: is there already a Ready/Creating reservation
    /// for this assignment? Returns the row when matched so the caller
    /// can resolve the existing allocation without re-running lvcreate.
    async fn lookup_assignment(
        &self,
        assignment_id: &str,
    ) -> Result<Option<LvmReservationRow>> {
        Ok(sqlx::query_as::<_, LvmReservationRow>(
            "SELECT assignment_id, pool, device_id, vg, lv_name, vm_id, \
             disk_index, size_gib, state \
             FROM lvm_reservation WHERE assignment_id = ?",
        )
        .bind(assignment_id)
        .fetch_optional(&self.db)
        .await?)
    }

}

#[async_trait]
impl DiskBackend for LvmLinearBackend {
    async fn allocate(&self, req: DiskAllocationRequest) -> Result<Allocation> {
        // Invariant 6 (idempotency): same assignment_id → no-op.
        if let Some(existing) = self.lookup_assignment(&req.assignment_id).await? {
            if existing.state == "Ready" {
                return Ok(Allocation {
                    path: Self::lv_path(&existing.vg, &existing.lv_name),
                    actual_size_gib: existing.size_gib as u64,
                });
            }
            // Creating/Deleting with same id from a retry: drop forward
            // by removing the row and recreating below. Any half-made
            // LV is removed by the lvcreate path's existing `lvremove`
            // pre-step.
            sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                .bind(&req.assignment_id)
                .execute(&self.db)
                .await?;
        }

        // Invariant 6 (conflict): different assignment_id for same (vm,disk).
        let conflicting: Option<(String,)> = sqlx::query_as(
            "SELECT assignment_id FROM lvm_reservation \
             WHERE vm_id = ? AND disk_index = ? AND assignment_id != ?",
        )
        .bind(&req.vm_id)
        .bind(req.disk_index as i64)
        .bind(&req.assignment_id)
        .fetch_optional(&self.db)
        .await?;
        if conflicting.is_some() {
            return Err(LvmError::AssignmentConflict {
                assignment_id: req.assignment_id.clone(),
                vm_id: req.vm_id.clone(),
                disk_index: req.disk_index,
            });
        }

        let vg = self.vg_for(&req.device_id)?.to_string();

        let lv_name = Self::lv_name(&req.vm_id, req.disk_index);

        // Insert reservation as Creating BEFORE lvcreate. If the agent
        // crashes after lvcreate but before flipping to Ready, reconcile
        // can match by (vg, lv_name) to drive forward.
        sqlx::query(
            "INSERT INTO lvm_reservation \
             (assignment_id, pool, device_id, vg, lv_name, vm_id, \
              disk_index, size_gib, state, reserved_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Creating', ?)",
        )
        .bind(&req.assignment_id)
        .bind(&self.spec.name)
        .bind(&req.device_id)
        .bind(&vg)
        .bind(&lv_name)
        .bind(&req.vm_id)
        .bind(req.disk_index as i64)
        .bind(req.min_size_gib as i64)
        .bind(basis_common::time::now_rfc3339())
        .execute(&self.db)
        .await?;

        let _permit = self.gate.acquire().await?;

        // Sweep any stale LV with our target name before lvcreate.
        if lv_attr(&vg, &lv_name).await?.is_some() {
            warn!(
                vm_id = %req.vm_id,
                disk_index = req.disk_index,
                "stale LV {lv_name} exists in {vg}; removing"
            );
            lvremove(&vg, &lv_name).await?;
        }

        // `--wipesignatures y --yes`: data LVs are carved from extents
        // that may carry a stale `LVM2_member` magic from a prior PV.
        // Without these, lvcreate prompts on stdin (no tty here),
        // defaults to `[n]`, and aborts — surfaced upstream as agent
        // FAILED. Lesson is not re-learnable.
        run_cmd(
            "lvcreate",
            &[
                "--wipesignatures",
                "y",
                "--yes",
                "--size",
                &format!("{}G", req.min_size_gib),
                "--name",
                &lv_name,
                &vg,
            ],
        )
        .await?;

        sqlx::query("UPDATE lvm_reservation SET state = 'Ready' WHERE assignment_id = ?")
            .bind(&req.assignment_id)
            .execute(&self.db)
            .await?;

        info!(
            vm_id = %req.vm_id,
            disk_index = req.disk_index,
            lv = %lv_name,
            vg = %vg,
            size_gib = req.min_size_gib,
            "data disk LV ready"
        );

        Ok(Allocation {
            path: Self::lv_path(&vg, &lv_name),
            actual_size_gib: req.min_size_gib,
        })
    }

    async fn release(&self, vm_id: &str) -> Result<()> {
        let rows: Vec<LvmReservationRow> = sqlx::query_as::<_, LvmReservationRow>(
            "SELECT assignment_id, pool, device_id, vg, lv_name, vm_id, \
             disk_index, size_gib, state FROM lvm_reservation WHERE pool = ? AND vm_id = ?",
        )
        .bind(&self.spec.name)
        .bind(vm_id)
        .fetch_all(&self.db)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for row in &rows {
            sqlx::query("UPDATE lvm_reservation SET state = 'Deleting' WHERE assignment_id = ?")
                .bind(&row.assignment_id)
                .execute(&self.db)
                .await?;
        }
        let _permit = self.gate.acquire().await?;
        for row in &rows {
            // Tolerate "already gone" — `lvremove -f` on a missing LV
            // prints to stderr but returns 0.
            lvremove(&row.vg, &row.lv_name).await?;
            sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                .bind(&row.assignment_id)
                .execute(&self.db)
                .await?;
        }
        info!(vm_id, removed = rows.len(), pool = %self.spec.name, "VM data disks released");
        Ok(())
    }

    async fn devices(&self) -> Result<Vec<PoolDevice>> {
        let mut out = Vec::with_capacity(self.spec.devices.len());
        for dev in &self.spec.devices {
            let vg = dev
                .vg
                .as_deref()
                .expect("vg required for lvm-linear; verified at config load");
            let (physical, physical_reason, total, free) = match vg_capacity(vg).await {
                Ok(cap) => (DevicePhysicalHealth::Ready, String::new(), cap.total, cap.free),
                Err(LvmError::VgMissing(_)) => (
                    DevicePhysicalHealth::Missing,
                    format!("vg {vg} not present"),
                    dev.size_gib * (1 << 30),
                    0,
                ),
                Err(e) => (
                    DevicePhysicalHealth::Degraded,
                    format!("{e}"),
                    dev.size_gib * (1 << 30),
                    0,
                ),
            };
            out.push(PoolDevice {
                id: dev.id.clone(),
                total_gib: total / (1 << 30),
                free_gib: free / (1 << 30),
                physical,
                physical_reason,
            });
        }
        Ok(out)
    }

    async fn reconcile(&self, live_vm_ids: &HashSet<String>) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();

        let rows: Vec<LvmReservationRow> = sqlx::query_as::<_, LvmReservationRow>(
            "SELECT assignment_id, pool, device_id, vg, lv_name, vm_id, \
             disk_index, size_gib, state FROM lvm_reservation WHERE pool = ?",
        )
        .bind(&self.spec.name)
        .fetch_all(&self.db)
        .await?;

        // Per-row reconcile.
        for row in &rows {
            let lv_present = lv_attr(&row.vg, &row.lv_name).await?.is_some();
            let vm_alive = live_vm_ids.contains(&row.vm_id);

            match (row.state.as_str(), lv_present, vm_alive) {
                ("Creating", true, true) => {
                    sqlx::query(
                        "UPDATE lvm_reservation SET state = 'Ready' WHERE assignment_id = ?",
                    )
                    .bind(&row.assignment_id)
                    .execute(&self.db)
                    .await?;
                    report.creating_resolved += 1;
                }
                ("Creating", false, _) | ("Creating", _, false) => {
                    // No LV was created, or VM is gone; drop the row.
                    sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                        .bind(&row.assignment_id)
                        .execute(&self.db)
                        .await?;
                    report.creating_resolved += 1;
                }
                ("Deleting", true, _) => {
                    let _permit = self.gate.acquire().await?;
                    lvremove(&row.vg, &row.lv_name).await?;
                    sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                        .bind(&row.assignment_id)
                        .execute(&self.db)
                        .await?;
                    report.deleting_resolved += 1;
                }
                ("Deleting", false, _) => {
                    sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                        .bind(&row.assignment_id)
                        .execute(&self.db)
                        .await?;
                    report.deleting_resolved += 1;
                }
                ("Ready", false, _) => {
                    report.lost_reservations.push(LostReservation {
                        assignment_id: row.assignment_id.clone(),
                        vm_id: row.vm_id.clone(),
                        disk_index: row.disk_index as u32,
                        pool: row.pool.clone(),
                        device_id: row.device_id.clone(),
                    });
                }
                ("Ready", true, false) => {
                    // VM gone; release this reservation.
                    let _permit = self.gate.acquire().await?;
                    lvremove(&row.vg, &row.lv_name).await?;
                    sqlx::query("DELETE FROM lvm_reservation WHERE assignment_id = ?")
                        .bind(&row.assignment_id)
                        .execute(&self.db)
                        .await?;
                }
                ("Ready", true, true) => {} // healthy
                _ => {
                    warn!(
                        assignment_id = %row.assignment_id,
                        state = %row.state,
                        "reservation in unknown state; leaving alone"
                    );
                }
            }
        }

        // Orphan sweep: basis-pattern LVs in our VGs without any
        // reservation row, whose VM is also gone.
        let claimed_lvs: HashSet<(String, String)> = rows
            .iter()
            .map(|r| (r.vg.clone(), r.lv_name.clone()))
            .collect();
        for vg in self.vgs() {
            let lvs = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]).await?;
            for line in lvs.lines() {
                let name = line.trim();
                let Some((vm_id, _)) = parse_data_disk_lv_name(name) else {
                    continue;
                };
                if claimed_lvs.contains(&(vg.to_string(), name.to_string())) {
                    continue;
                }
                if !live_vm_ids.contains(&vm_id) {
                    let _permit = self.gate.acquire().await?;
                    lvremove(vg, name).await?;
                    report.orphans_removed += 1;
                    info!(vg, name, vm_id, "removed orphan data LV");
                }
            }
        }

        Ok(report)
    }

    async fn validate(&self) -> Result<()> {
        for device in &self.spec.devices {
            let vg = device
                .vg
                .as_deref()
                .expect("vg required for lvm-linear; verified at config load");
            check_vg_exists(vg).await?;
            check_vg_one_pv(vg).await?;
            check_vg_no_foreign_lvs(vg).await?;
        }
        Ok(())
    }

    fn labels(&self) -> &BTreeMap<String, String> {
        &self.spec.labels
    }

    fn backend_kind(&self) -> PoolBackend {
        PoolBackend::LvmLinear
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// --- Capacity helpers -------------------------------------------------

async fn pool_capacity(pool: &Pool) -> Result<PoolCapacity> {
    let devices = pool.backend.devices().await?;
    let mut configured = 0u64;
    let mut ready_total = 0u64;
    let mut schedulable_total = 0u64;
    let mut schedulable_free = 0u64;
    for d in &devices {
        let bytes = d.total_gib * (1 << 30);
        configured += bytes;
        if matches!(d.physical, DevicePhysicalHealth::Ready) {
            ready_total += bytes;
            // Scheduling state lives in the controller; the agent
            // reports physical Ready and lets the controller subtract
            // drained devices from the schedulable totals on its side.
            schedulable_total += bytes;
            schedulable_free += d.free_gib * (1 << 30);
        }
    }
    Ok(PoolCapacity {
        pool: pool.name.clone(),
        backend: pool.backend.backend_kind(),
        labels: pool.backend.labels().clone(),
        configured_total_bytes: configured,
        ready_total_bytes: ready_total,
        schedulable_total_bytes: schedulable_total,
        schedulable_free_bytes: schedulable_free,
        devices,
    })
}

// --- Internal LVM helpers ---------------------------------------------

/// Split `basis-data-<vm_id>-<index>` into `(vm_id, index)`.
fn parse_data_disk_lv_name(lv_name: &str) -> Option<(String, u32)> {
    let rest = lv_name.strip_prefix(DATA_LV_PREFIX)?;
    let (vm_id, index_str) = rest.rsplit_once('-')?;
    if vm_id.is_empty() {
        return None;
    }
    let index: u32 = index_str.parse().ok()?;
    Some((vm_id.to_string(), index))
}

async fn check_vg_exists(vg: &str) -> Result<()> {
    let status = Command::new("vgs")
        .args(["--noheadings", "-o", "vg_name", vg])
        .output()
        .await?;
    if !status.status.success() || status.stdout.is_empty() {
        return Err(LvmError::VgMissing(vg.to_string()));
    }
    Ok(())
}

async fn check_thin_pool(vg: &str, pool: &str) -> Result<()> {
    let out = Command::new("lvs")
        .args([
            "--noheadings",
            "--separator=|",
            "-o",
            "lv_name,lv_attr",
            &format!("{vg}/{pool}"),
        ])
        .output()
        .await?;
    if !out.status.success() {
        return Err(LvmError::ThinPoolMissing {
            vg: vg.to_string(),
            pool: pool.to_string(),
        });
    }
    let line = String::from_utf8_lossy(&out.stdout);
    let attr = line
        .trim()
        .split('|')
        .nth(1)
        .unwrap_or("")
        .trim()
        .to_string();
    if !attr.starts_with('t') {
        return Err(LvmError::NotThinPool {
            vg: vg.to_string(),
            pool: pool.to_string(),
            attr,
        });
    }
    Ok(())
}

/// Verify the VG has exactly one PV. The startup invariant rules
/// out PV-spanning LVs by construction — that's the property
/// `LvmLinearBackend` relies on for "an LV cannot span devices."
///
/// The PV's path is intentionally not cross-checked against the
/// host-config `id`: a PV can legitimately be a raw block device
/// (`/dev/sdb`), a partition (`/dev/sdb4`), an LV in another VG
/// (`/dev/pve/basis-data`), or a loopback (`/dev/loop0`), and the
/// `id` field is an operator-readable label, not a guaranteed PV
/// path. "Exactly one PV" is the property that matters.
async fn check_vg_one_pv(vg: &str) -> Result<()> {
    let out = run_cmd(
        "vgs",
        &["--noheadings", "--separator=|", "-o", "pv_name", vg],
    )
    .await?;
    let pvs: Vec<&str> = out.lines().map(str::trim).filter(|s| !s.is_empty()).collect();
    if pvs.len() != 1 {
        return Err(LvmError::VgPvCountWrong {
            vg: vg.to_string(),
            pv_count: pvs.len(),
        });
    }
    Ok(())
}

/// Refuse to start if any LV in this VG doesn't match basis's naming.
/// Basis-managed VGs are basis-exclusive — operator-placed LVs are
/// undeclared state we cannot reconcile against.
async fn check_vg_no_foreign_lvs(vg: &str) -> Result<()> {
    let out = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]).await?;
    for line in out.lines() {
        let name = line.trim();
        if name.is_empty() {
            continue;
        }
        if parse_data_disk_lv_name(name).is_none() {
            return Err(LvmError::ForeignLv {
                vg: vg.to_string(),
                lv: name.to_string(),
            });
        }
    }
    Ok(())
}

async fn thin_pool_capacity(vg: &str, pool: &str) -> Result<RootfsBytes> {
    let out = run_cmd(
        "lvs",
        &[
            "--noheadings",
            "--separator=|",
            "--units",
            "b",
            "--nosuffix",
            "-o",
            "lv_size,data_percent,lv_metadata_size,metadata_percent",
            &format!("{vg}/{pool}"),
        ],
    )
    .await?;

    let line = out.trim();
    let parts: Vec<&str> = line.split('|').map(str::trim).collect();
    let parse_f64 = |s: &&str, field: &str| -> Result<f64> {
        s.parse::<f64>().map_err(|_| LvmError::Command {
            cmd: "lvs".into(),
            stderr: format!("could not parse {field}={s:?}"),
        })
    };
    let data_total = parse_f64(parts.first().unwrap_or(&"0"), "lv_size")? as u64;
    let data_pct = parts
        .get(1)
        .filter(|s| !s.is_empty())
        .map(|s| parse_f64(s, "data_percent"))
        .transpose()?
        .unwrap_or(0.0);
    let meta_total = parse_f64(parts.get(2).unwrap_or(&"0"), "lv_metadata_size")? as u64;
    let meta_pct = parts
        .get(3)
        .filter(|s| !s.is_empty())
        .map(|s| parse_f64(s, "metadata_percent"))
        .transpose()?
        .unwrap_or(0.0);

    let data_used = (data_total as f64 * data_pct / 100.0) as u64;
    let meta_used = (meta_total as f64 * meta_pct / 100.0) as u64;
    Ok(RootfsBytes {
        total: data_total,
        free: data_total.saturating_sub(data_used),
        metadata_total: meta_total,
        metadata_free: meta_total.saturating_sub(meta_used),
    })
}

/// Capacity of a plain VG: total + free extents.
struct VgBytes {
    total: u64,
    free: u64,
}

async fn vg_capacity(vg: &str) -> Result<VgBytes> {
    let out = run_cmd(
        "vgs",
        &[
            "--noheadings",
            "--separator=|",
            "--units",
            "b",
            "--nosuffix",
            "-o",
            "vg_size,vg_free",
            vg,
        ],
    )
    .await?;
    let parts: Vec<&str> = out.trim().split('|').map(str::trim).collect();
    let parse_u64 = |s: &&str, field: &str| -> Result<u64> {
        s.parse::<u64>().map_err(|_| LvmError::Command {
            cmd: "vgs".into(),
            stderr: format!("could not parse {field}={s:?}"),
        })
    };
    let total = parse_u64(parts.first().unwrap_or(&"0"), "vg_size")?;
    let free = parse_u64(parts.get(1).unwrap_or(&"0"), "vg_free")?;
    Ok(VgBytes { total, free })
}

async fn lv_attr(vg: &str, lv_name: &str) -> Result<Option<String>> {
    let out = run_cmd(
        "lvs",
        &[
            "--noheadings",
            "-o",
            "lv_attr",
            "-S",
            &format!("lv_name={lv_name}"),
            vg,
        ],
    )
    .await?;
    let trimmed = out.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn attr_is_readonly(attr: &str) -> bool {
    attr.chars().nth(1) == Some('r')
}

async fn lvremove(vg: &str, lv_name: &str) -> Result<()> {
    run_cmd("lvremove", &["-f", &format!("{vg}/{lv_name}")])
        .await
        .map(|_| ())
}

async fn lvextend(vg: &str, lv_name: &str, size_gib: u64) -> Result<()> {
    match run_cmd(
        "lvextend",
        &["-L", &format!("{size_gib}G"), &format!("{vg}/{lv_name}")],
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(LvmError::Command { stderr, .. })
            if stderr.contains("No size change") || stderr.contains("not larger than") =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn run_cmd(tool: &'static str, args: &[&str]) -> Result<String> {
    let out = Command::new(tool).args(args).output().await?;
    if !out.status.success() {
        return Err(LvmError::Command {
            cmd: format!("{tool} {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `basis-data-` prefix and right-to-left UUID parse are
    /// load-bearing for orphan adoption — a wrong parse would let the
    /// reconciler delete the wrong VM's data disks.
    #[test]
    fn data_disk_lv_name_roundtrip() {
        let uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let name = format!("{DATA_LV_PREFIX}{uuid}-3");
        let (parsed, idx) = parse_data_disk_lv_name(&name).unwrap();
        assert_eq!(parsed, uuid);
        assert_eq!(idx, 3);
    }

    #[test]
    fn parse_data_disk_lv_name_rejects_non_data() {
        assert!(parse_data_disk_lv_name("vm-a1b2c3d4-e5f6").is_none());
        assert!(parse_data_disk_lv_name("image-sha256abc").is_none());
        assert!(parse_data_disk_lv_name("pool").is_none());
        // Legacy vmdata- prefix no longer parses.
        assert!(parse_data_disk_lv_name("vmdata-vm-1-0").is_none());
    }

    #[test]
    fn parse_data_disk_lv_name_rejects_malformed() {
        assert!(parse_data_disk_lv_name("basis-data--0").is_none());
        assert!(parse_data_disk_lv_name("basis-data-vm1-abc").is_none());
        assert!(parse_data_disk_lv_name("basis-data-vm1").is_none());
    }
}
