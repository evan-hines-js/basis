//! Per-host VM disk storage.
//!
//! Two LVM volume groups, each with one job:
//!
//!   * **rootfs** — a thin pool (`<rootfs.vg>/<rootfs.thin_pool>`).
//!     Per-VM rootfs LVs are CoW snapshots of a golden image LV. Thin
//!     is correct here: snapshot create is sub-second, overcommit is
//!     fine because an unused gigabyte is just a metadata reservation,
//!     and the host can run dozens of VMs from one image without
//!     paying its full virtual size per VM.
//!
//!   * **data** — a plain VG (`<data.vg>`). Each requested extra disk
//!     is one linear LV at the requested size, fully allocated. Linear
//!     is correct here: bluestore on dm-thin double-books allocation,
//!     bluestore on dm-linear is one-table-lookup pass-through. Guest
//!     TRIM reaches the physical NVMe through dm-linear with no
//!     metadata indirection. Pool exhaustion in the data VG is
//!     bounded — no shared blast radius with rootfs.
//!
//! The two VGs are provisioned by the basis-prereqs ansible role on
//! distinct partitions (or NVMes). The agent fail-fast validates both
//! at startup; there is intentionally no fallback to a single-pool
//! mode — silent degradation makes incident debugging miserable.
//!
//! Lifecycle:
//!   1. [`Storage::ensure_image_lv`] — one-time per image: create a
//!      golden raw LV in the rootfs pool, qemu-img convert the qcow2
//!      into it, mark RO.
//!   2. [`Storage::create_vm_lv`] — per VM: thin-snapshot the golden
//!      LV, extend to the requested rootfs size.
//!   3. [`Storage::create_data_disk_lv`] — per data disk: linear LV
//!      in the data VG, full allocation, blank.
//!   4. [`Storage::remove_vm_lv`] / [`Storage::remove_vm_data_disks`]
//!      — `lvremove`, returning extents to their respective VG. With
//!      `issue_discards = 1` in lvm.conf the TRIM propagates to the
//!      physical SSD.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::process::Command;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{info, warn};

use crate::config::StorageSpec;
use crate::metrics;

/// Ceiling on concurrent lvm2 mutation commands per VG.
///
/// Set to 1 deliberately. lvm2 takes a per-VG metadata lock for every
/// mutation internally, so any concurrency we allow against the same
/// VG doesn't actually execute in parallel — it just queues inside
/// lvm2 and burns CPU on contending processes. Measured at cap=4: each
/// `lvcreate` took ~8.5s because it was fighting three siblings for
/// the same VG lock, for zero throughput benefit. At cap=1 each call
/// hits lvm2's uncontended per-op time (sub-second on a healthy pool).
///
/// Across distinct VGs (rootfs vs data), lvm2's locks are independent,
/// so [`LvmPermits`] keeps a separate semaphore per VG and an op
/// against the rootfs pool does not block an op against the data VG.
const MAX_CONCURRENT_LVM_MUTATIONS_PER_VG: usize = 1;

/// Maximum wall-clock wait for an LVM mutation permit before an
/// acquirer gives up and returns [`LvmError::Busy`]. Bounds tail
/// latency: once arrival rate exceeds service rate the queue grows
/// without limit, and an unbounded `acquire().await` would let a
/// single caller sit in that queue for minutes. 60s tolerates a
/// transient stall (a `systemctl stop` that ran long, a background
/// metadata flush) without spurious failures, but tight enough that
/// sustained backpressure produces fast-fail responses the caller
/// can act on.
const LVM_PERMIT_TIMEOUT: Duration = Duration::from_secs(60);

/// LV name prefix for golden per-image volumes (rootfs VG).
const IMAGE_LV_PREFIX: &str = "image-";
/// LV name prefix for per-VM rootfs snapshots (rootfs VG).
const VM_LV_PREFIX: &str = "vm-";
/// LV name prefix for per-VM raw data disks (data VG). Naming is
/// `vmdata-<vm_id>-<index>`; see [`data_disk_lv_name`].
const DATA_LV_PREFIX: &str = "vmdata-";

#[derive(Debug, thiserror::Error)]
pub enum LvmError {
    #[error(
        "volume group '{0}' not found — run the basis-prereqs ansible role on this host to \
         provision the LVM layout (set basis_lvm_devices in inventory first)"
    )]
    VgMissing(String),

    #[error(
        "thin pool '{vg}/{pool}' not found — the volume group exists but the thin pool was not \
         created; re-run the basis-prereqs ansible role"
    )]
    PoolMissing { vg: String, pool: String },

    #[error("'{vg}/{pool}' exists but is not a thin pool (lv_attr={attr})")]
    NotThinPool {
        vg: String,
        pool: String,
        attr: String,
    },

    #[error("lvm command `{cmd}` failed: {stderr}")]
    Command { cmd: String, stderr: String },

    #[error("qemu-img convert into LV failed: {0}")]
    ImageConvert(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error(
        "lvm backend busy on {role} VG: could not acquire permit within {timeout:?} — \
         arrival rate exceeds service rate, shed load or retry"
    )]
    Busy {
        role: &'static str,
        timeout: Duration,
    },
}

pub type Result<T> = std::result::Result<T, LvmError>;

/// Capacity numbers shared between agent (reporter) and controller
/// (consumer via heartbeat). Bytes everywhere — the GiB conversion
/// happens at the proto boundary, not here.
///
/// `metadata_*` apply only to the rootfs thin pool; the data VG
/// has no metadata extent and reports zero in those fields. Callers
/// that don't care about thin-pool metadata simply ignore the zeros.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolBytes {
    pub total: u64,
    pub free: u64,
    pub metadata_total: u64,
    pub metadata_free: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageCapacity {
    pub rootfs: PoolBytes,
    pub data: PoolBytes,
}

/// Per-VG lvm2 mutation gate. Two named semaphores rather than a
/// HashMap-by-string because we have exactly two VGs (rootfs, data)
/// and want distinct call-site names to encode the role at the type
/// level.
struct LvmPermits {
    rootfs: Semaphore,
    data: Semaphore,
}

impl LvmPermits {
    fn new() -> Self {
        Self {
            rootfs: Semaphore::new(MAX_CONCURRENT_LVM_MUTATIONS_PER_VG),
            data: Semaphore::new(MAX_CONCURRENT_LVM_MUTATIONS_PER_VG),
        }
    }

    async fn acquire<'a>(role: &'static str, sem: &'a Semaphore) -> Result<SemaphorePermit<'a>> {
        let started = Instant::now();
        let result = tokio::time::timeout(LVM_PERMIT_TIMEOUT, sem.acquire()).await;
        if let Some(m) = metrics::global() {
            m.lv_permit_wait_seconds
                .observe(started.elapsed().as_secs_f64());
        }
        match result {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => unreachable!("LvmPermits semaphores are never closed"),
            Err(_) => Err(LvmError::Busy {
                role,
                timeout: LVM_PERMIT_TIMEOUT,
            }),
        }
    }

    async fn rootfs(&self) -> Result<SemaphorePermit<'_>> {
        Self::acquire("rootfs", &self.rootfs).await
    }

    async fn data(&self) -> Result<SemaphorePermit<'_>> {
        Self::acquire("data", &self.data).await
    }
}

/// Owns the agent's view of host storage: which VGs to use, and the
/// per-VG mutation gate. Constructed once at agent startup and shared
/// behind `Arc` by every caller that creates or removes LVs.
pub struct Storage {
    spec: StorageSpec,
    permits: LvmPermits,
}

impl Storage {
    pub fn new(spec: StorageSpec) -> Self {
        Self {
            spec,
            permits: LvmPermits::new(),
        }
    }

    pub fn rootfs_vg(&self) -> &str {
        &self.spec.rootfs.vg
    }

    pub fn data_vg(&self) -> &str {
        &self.spec.data.vg
    }

    /// Path to the golden image LV for `image_hash` in the rootfs VG.
    pub fn image_lv_path(&self, image_hash: &str) -> PathBuf {
        PathBuf::from(format!(
            "/dev/{}/{IMAGE_LV_PREFIX}{image_hash}",
            self.spec.rootfs.vg
        ))
    }

    /// Path to a VM's rootfs LV in the rootfs VG.
    pub fn vm_lv_path(&self, vm_id: &str) -> PathBuf {
        PathBuf::from(format!(
            "/dev/{}/{VM_LV_PREFIX}{vm_id}",
            self.spec.rootfs.vg
        ))
    }

    /// Path to a VM's `index`-th data disk LV in the data VG.
    pub fn data_disk_lv_path(&self, vm_id: &str, index: u32) -> PathBuf {
        PathBuf::from(format!(
            "/dev/{}/{}",
            self.spec.data.vg,
            data_disk_lv_name(vm_id, index)
        ))
    }

    /// Validate both VGs exist and are healthy. Fail-fast at agent
    /// startup so the agent never silently degrades.
    pub async fn validate(&self) -> Result<StorageCapacity> {
        check_vg_exists(&self.spec.rootfs.vg).await?;
        check_thin_pool(&self.spec.rootfs.vg, &self.spec.rootfs.thin_pool).await?;
        check_vg_exists(&self.spec.data.vg).await?;

        let cap = self.capacity().await?;
        info!(
            rootfs_vg = %self.spec.rootfs.vg,
            rootfs_thin_pool = %self.spec.rootfs.thin_pool,
            rootfs_data_free_gib = cap.rootfs.free / (1 << 30),
            rootfs_data_total_gib = cap.rootfs.total / (1 << 30),
            rootfs_metadata_free_mib = cap.rootfs.metadata_free / (1 << 20),
            rootfs_metadata_total_mib = cap.rootfs.metadata_total / (1 << 20),
            data_vg = %self.spec.data.vg,
            data_free_gib = cap.data.free / (1 << 30),
            data_total_gib = cap.data.total / (1 << 30),
            "storage ready"
        );
        Ok(cap)
    }

    /// Current capacity of both VGs. Cheap enough to call on every
    /// heartbeat tick — three lvs/vgs invocations, sub-100ms each on
    /// a healthy host.
    pub async fn capacity(&self) -> Result<StorageCapacity> {
        let rootfs = thin_pool_capacity(&self.spec.rootfs.vg, &self.spec.rootfs.thin_pool).await?;
        let data = vg_capacity(&self.spec.data.vg).await?;
        Ok(StorageCapacity { rootfs, data })
    }

    /// Ensure a golden image LV exists for the given image, populated
    /// from the qcow2 source. Idempotent: if the LV is already present
    /// AND read-only (the "populated" marker set after a successful
    /// convert), returns the existing path. Otherwise creates,
    /// converts, and marks RO.
    pub async fn ensure_image_lv(
        &self,
        image_hash: &str,
        source_qcow2: &Path,
        virtual_size_gib: u64,
    ) -> Result<PathBuf> {
        let vg = &self.spec.rootfs.vg;
        let pool = &self.spec.rootfs.thin_pool;
        let lv_name = format!("{IMAGE_LV_PREFIX}{image_hash}");
        let lv_path = self.image_lv_path(image_hash);

        // Fast-path the already-populated case before taking the
        // permit — otherwise every warm-cache VM create needlessly
        // queues on the rootfs permit.
        if matches!(lv_attr(vg, &lv_name).await?.as_deref(), Some(a) if attr_is_readonly(a)) {
            return Ok(lv_path);
        }

        let _permit = self.permits.rootfs().await?;

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
                "none", // O_DIRECT on the output side — skip host page cache
                "-T",
                "none", // O_DIRECT on the input side — same rationale
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

    /// Create a thin snapshot LV for a VM, extended to the requested
    /// rootfs size. The returned path is the raw block device cloud-
    /// hypervisor attaches with `--disk path=...,direct=on`.
    pub async fn create_vm_lv(
        &self,
        vm_id: &str,
        image_hash: &str,
        disk_gib: u64,
    ) -> Result<PathBuf> {
        let vg = &self.spec.rootfs.vg;
        let _permit = self.permits.rootfs().await?;

        let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
        let origin = format!("{IMAGE_LV_PREFIX}{image_hash}");
        let lv_path = self.vm_lv_path(vm_id);

        if lv_attr(vg, &lv_name).await?.is_some() {
            warn!(vm_id, "VM LV already exists; removing for clean recreate");
            lvremove(vg, &lv_name).await?;
        }

        // A snapshot of a thin LV is implicitly thin (lives in the
        // origin's pool) — `--thin` is rejected with `--snapshot`.
        // `--setactivationskip n` overrides thin-snapshot's default
        // "skip activation" so the LV is active immediately for cloud-
        // hypervisor (otherwise we'd need a follow-up `lvchange -Kay`).
        // `--permission rw` is the default but explicit beats implicit
        // here — the origin is RO and the snapshot must be writable.
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

        // Grow the virtual size to the requested disk. cloud-init's
        // growpart + resize2fs claims the extra space inside the
        // guest on first boot. If the request matches the image size,
        // `lvextend`'s "No size change" / "not larger than existing"
        // exits are tolerated by [`lvextend`] as no-ops.
        lvextend(vg, &lv_name, disk_gib).await?;

        info!(vm_id, lv = %lv_name, origin = %origin, disk_gib, "VM LV ready");
        Ok(lv_path)
    }

    /// Remove a VM's rootfs LV. No-op if it doesn't exist.
    pub async fn remove_vm_lv(&self, vm_id: &str) -> Result<()> {
        let vg = &self.spec.rootfs.vg;
        let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
        // Existence check is read-only and cheap — keep it outside
        // the permit so the no-op fast path doesn't queue.
        if lv_attr(vg, &lv_name).await?.is_none() {
            return Ok(());
        }
        let _permit = self.permits.rootfs().await?;
        lvremove(vg, &lv_name).await
    }

    /// Create a blank linear LV in the data VG.
    ///
    /// Linear (not thin) is the right shape under bluestore: stable,
    /// fully-allocated, no CoW indirection. Rook wipes OSD devices at
    /// claim time so there's no benefit to lazy allocation, and
    /// linear's pass-through TRIM means guest discards reach the
    /// physical NVMe immediately.
    ///
    /// Idempotent on `(vm_id, index)`: a stale LV from a crashed
    /// prior create is removed and recreated.
    pub async fn create_data_disk_lv(
        &self,
        vm_id: &str,
        index: u32,
        size_gib: u64,
    ) -> Result<PathBuf> {
        let vg = &self.spec.data.vg;
        let _permit = self.permits.data().await?;

        let lv_name = data_disk_lv_name(vm_id, index);
        let lv_path = self.data_disk_lv_path(vm_id, index);

        if lv_attr(vg, &lv_name).await?.is_some() {
            warn!(
                vm_id,
                index, "data disk LV already exists; recreating clean"
            );
            lvremove(vg, &lv_name).await?;
        }

        run_cmd(
            "lvcreate",
            &["--size", &format!("{size_gib}G"), "--name", &lv_name, vg],
        )
        .await?;

        info!(vm_id, index, lv = %lv_name, size_gib, "data disk LV ready");
        Ok(lv_path)
    }

    /// Remove every data disk LV belonging to `vm_id`. No-op if the VM
    /// has no data disks. Enumerates from `lvs` rather than from a
    /// caller-provided list so a mid-create crash that left orphan LVs
    /// behind gets cleaned up on delete.
    pub async fn remove_vm_data_disks(&self, vm_id: &str) -> Result<()> {
        let vg = &self.spec.data.vg;
        let names = list_data_disk_lv_names_for(vg, vm_id).await?;
        if names.is_empty() {
            return Ok(());
        }
        let _permit = self.permits.data().await?;
        for lv_name in &names {
            lvremove(vg, lv_name).await?;
        }
        info!(vm_id, removed = names.len(), "VM data disks removed");
        Ok(())
    }

    /// Every vm_id with at least one LV on this host — rootfs LV in
    /// the rootfs VG *or* any data disk LV in the data VG. One pass
    /// per VG; the orphan sweep diffs this against the agent DB and
    /// reclaims the difference via [`Self::remove_vm_lv`] +
    /// [`Self::remove_vm_data_disks`].
    pub async fn list_managed_vm_ids(&self) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();

        let rootfs_lvs = run_cmd(
            "lvs",
            &["--noheadings", "-o", "lv_name", &self.spec.rootfs.vg],
        )
        .await?;
        for line in rootfs_lvs.lines() {
            let name = line.trim();
            if let Some(id) = name.strip_prefix(VM_LV_PREFIX) {
                ids.insert(id.to_string());
            }
        }

        let data_lvs = run_cmd(
            "lvs",
            &["--noheadings", "-o", "lv_name", &self.spec.data.vg],
        )
        .await?;
        for line in data_lvs.lines() {
            let name = line.trim();
            if let Some((vm_id, _index)) = parse_data_disk_lv_name(name) {
                ids.insert(vm_id);
            }
        }

        Ok(ids)
    }
}

// --- internal helpers ---------------------------------------------------

/// LV name for the `index`-th extra data disk of a VM.
///
/// `vmdata-<vm_id>-<index>`. The trailing `-<index>` is parsed
/// right-to-left in [`parse_data_disk_lv_name`] so vm_ids that
/// themselves contain `-` (UUIDs do) parse unambiguously as long as
/// the index is a pure integer.
fn data_disk_lv_name(vm_id: &str, index: u32) -> String {
    format!("{DATA_LV_PREFIX}{vm_id}-{index}")
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
        return Err(LvmError::PoolMissing {
            vg: vg.to_string(),
            pool: pool.to_string(),
        });
    }
    let line = String::from_utf8_lossy(&out.stdout);
    // `lv_attr` is a 10-char status string; the first char is the
    // volume type — 't' means thin pool (see lvs(8) "Lv attr bits").
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

/// Capacity of a thin pool: data extents + metadata extents.
async fn thin_pool_capacity(vg: &str, pool: &str) -> Result<PoolBytes> {
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
    // The units=b output doesn't always zero-pad integers;
    // `data_percent` can be "0.00" or missing for a fresh pool.
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
    Ok(PoolBytes {
        total: data_total,
        free: data_total.saturating_sub(data_used),
        metadata_total: meta_total,
        metadata_free: meta_total.saturating_sub(meta_used),
    })
}

/// Capacity of a plain VG: total + free extents. No metadata accounting
/// (linear LVs don't have a separate metadata extent the way thin
/// pools do — VG metadata is fixed-size and not user-relevant).
async fn vg_capacity(vg: &str) -> Result<PoolBytes> {
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
    Ok(PoolBytes {
        total,
        free,
        metadata_total: 0,
        metadata_free: 0,
    })
}

/// Read-only state query for a single LV in a specific VG. `None`
/// means the LV is not present; `Some(attr)` returns its 10-char
/// lv_attr bitfield (see lvs(8) "Lv attr bits").
///
/// Uses LVM's `-S lv_name=<name>` reporting selection rather than a
/// positional `VG/LV` argument. Selection always exits 0 — empty
/// output when nothing matches, one row when it does — so a non-zero
/// exit is reserved for real failures (lvm daemon down, VG
/// disappeared mid-flight, permission error).
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

/// `true` when a 10-char lv_attr bitfield marks the LV read-only.
/// Per lvs(8), the second character is permission: 'r' = read-only,
/// 'w' = writable.
fn attr_is_readonly(attr: &str) -> bool {
    attr.chars().nth(1) == Some('r')
}

async fn lvremove(vg: &str, lv_name: &str) -> Result<()> {
    run_cmd("lvremove", &["-f", &format!("{vg}/{lv_name}")])
        .await
        .map(|_| ())
}

/// Grow an LV to *at least* `size_gib` GiB. Idempotent and never
/// shrinks: the post-condition we care about is "guest sees ≥
/// size_gib", so a snapshot that already meets or exceeds the
/// request is success, not failure. `lvextend` itself surfaces the
/// two flavours of "no work to do" with non-zero exits that we
/// collapse here:
///
///   * "No size change" — the LV is exactly the requested size.
///   * "New size given (N extents) not larger than existing size (M
///     extents)" — the LV is already larger. We never shrink an LV
///     out from under a running guest's filesystem.
///
/// `-L 10G` (uppercase) is gibibytes (2^30). Lowercase `g` is SI
/// gigabytes (10^9) and is wrong for the API contract.
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

/// Data-disk LV names belonging to a specific vm_id.
async fn list_data_disk_lv_names_for(vg: &str, vm_id: &str) -> Result<Vec<String>> {
    let out = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]).await?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|n| {
            parse_data_disk_lv_name(n)
                .map(|(owner, _)| owner == vm_id)
                .unwrap_or(false)
        })
        .map(str::to_string)
        .collect())
}

/// Split `vmdata-<vm_id>-<index>` into `(vm_id, index)`. Returns
/// `None` when the name doesn't match the expected shape.
///
/// Right-to-left split on `-` is load-bearing: vm_ids are UUIDs and
/// contain `-` themselves. `rsplit_once` peels only the trailing
/// segment, and the index must parse as a pure integer — a malformed
/// tail fails the parse rather than silently producing a wrong
/// vm_id.
fn parse_data_disk_lv_name(lv_name: &str) -> Option<(String, u32)> {
    let rest = lv_name.strip_prefix(DATA_LV_PREFIX)?;
    let (vm_id, index_str) = rest.rsplit_once('-')?;
    if vm_id.is_empty() {
        return None;
    }
    let index: u32 = index_str.parse().ok()?;
    Some((vm_id.to_string(), index))
}

/// Spawn a tool, return stdout on success. On non-zero exit, returns
/// `LvmError::Command` carrying the stderr so callers (and operators
/// reading logs) can see why it failed. I/O errors (binary missing,
/// fork fails) propagate as `LvmError::Io`.
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
    use crate::config::{DataSpec, RootfsSpec};

    fn test_spec() -> StorageSpec {
        StorageSpec {
            rootfs: RootfsSpec {
                vg: "basis".into(),
                thin_pool: "pool".into(),
            },
            data: DataSpec {
                vg: "basis-data".into(),
            },
        }
    }

    /// LV paths must be VG-correct: rootfs in the rootfs VG, data in
    /// the data VG. A wrong path here would leak rootfs LVs into the
    /// data VG (or vice-versa) and the orphan sweep would wipe live
    /// VMs' disks.
    #[test]
    fn lv_paths_route_to_correct_vg() {
        let s = Storage::new(test_spec());
        assert_eq!(
            s.image_lv_path("abc"),
            PathBuf::from("/dev/basis/image-abc")
        );
        assert_eq!(s.vm_lv_path("vm-1"), PathBuf::from("/dev/basis/vm-vm-1"));
        assert_eq!(
            s.data_disk_lv_path("vm-1", 0),
            PathBuf::from("/dev/basis-data/vmdata-vm-1-0"),
        );
    }

    /// A wrong parse here would let the orphan sweep delete the
    /// wrong VM's data disks.
    #[test]
    fn data_disk_lv_name_roundtrip() {
        let uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let name = data_disk_lv_name(uuid, 3);
        assert_eq!(name, "vmdata-a1b2c3d4-e5f6-7890-abcd-ef1234567890-3");
        let (parsed_id, parsed_idx) = parse_data_disk_lv_name(&name).unwrap();
        assert_eq!(parsed_id, uuid);
        assert_eq!(parsed_idx, 3);
    }

    #[test]
    fn parse_data_disk_lv_name_rejects_non_data() {
        assert!(parse_data_disk_lv_name("vm-a1b2c3d4-e5f6").is_none());
        assert!(parse_data_disk_lv_name("image-sha256abc").is_none());
        assert!(parse_data_disk_lv_name("pool").is_none());
    }

    #[test]
    fn parse_data_disk_lv_name_rejects_malformed() {
        // Empty vm_id.
        assert!(parse_data_disk_lv_name("vmdata--0").is_none());
        // Non-numeric index.
        assert!(parse_data_disk_lv_name("vmdata-vm1-abc").is_none());
        // No index suffix.
        assert!(parse_data_disk_lv_name("vmdata-vm1").is_none());
    }
}
