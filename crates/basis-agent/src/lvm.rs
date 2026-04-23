//! LVM thin pool management for VM rootfs disks.
//!
//! Every basis VM gets a thin snapshot LV of a golden image LV, attached
//! raw to cloud-hypervisor with O_DIRECT. No host filesystem journal or
//! qcow2 metadata on the fsync path — what guest etcd needs to commit
//! WAL entries in single-digit milliseconds instead of multiple seconds.
//!
//! Convention-driven: the pool is always `basis/pool`, provisioned by
//! the `basis-prereqs` ansible role. The agent validates the pool at
//! startup and refuses to run if it's missing. There is intentionally
//! no fallback to a file-based backend — silent fallback would degrade
//! etcd performance without operators noticing.
//!
//! Lifecycle:
//!   1. `ensure_image_lv` — one-time per image: create a golden raw LV
//!      of the image's virtual size, qemu-img convert the qcow2 into
//!      it, mark RO. Idempotent; subsequent VM creates reuse it.
//!   2. `create_vm_lv` — per VM: thin-snapshot the golden LV, extend to
//!      the requested disk size, hand the `/dev/basis/vm-<id>` path to
//!      cloud-hypervisor. Sub-second.
//!   3. `remove_lv` — on VM delete: `lvremove` returns the extents to
//!      the thin pool; with `issue_discards = 1` in lvm.conf the TRIM
//!      propagates to the physical SSD.

use std::path::{Path, PathBuf};
use std::time::Instant;

use tokio::process::Command;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::metrics;

/// Fixed volume group name. The ansible role creates exactly this.
pub const VG: &str = "basis";
/// Fixed thin pool name within the VG.
pub const POOL: &str = "pool";

/// Ceiling on concurrent lvm2 mutation commands (lvcreate/lvremove/
/// lvextend/lvchange) in flight from this agent.
///
/// Set to 1 deliberately. lvm2 takes a per-VG metadata lock for every
/// mutation internally, so any concurrency we allow here doesn't
/// actually execute in parallel — it just queues inside lvm2 and
/// burns CPU on contending processes. Measured at cap=4: each
/// `lvcreate` took ~8.5s because it was fighting three siblings for
/// the same VG lock, for zero throughput benefit. At cap=1 each call
/// should hit lvm2's uncontended per-op time (sub-second on a healthy
/// pool) and end-to-end latency is bounded by `queue_depth × per_op`
/// rather than `queue_depth × contended_per_op`. If a future storage
/// backend exposes real parallelism (e.g. a per-LV metadata model)
/// raising this makes sense; for lvm2 thin it's counter-productive.
const MAX_CONCURRENT_LVM_MUTATIONS: usize = 1;

/// Maximum wall-clock wait for the LVM mutation permit before an
/// acquirer gives up and returns `LvmError::Busy`. Bounds tail latency:
/// once arrival rate exceeds service rate the queue grows without
/// limit, and an unbounded `acquire().await` would let a single caller
/// sit in that queue for minutes while the queue continues to grow
/// behind it. 60s is generous enough that a transient stall (a
/// `systemctl stop` that took an unusual amount of time, a background
/// metadata flush) clears without a spurious failure, but tight enough
/// that sustained backpressure produces fast-fail responses the caller
/// can act on rather than minute-long hangs.
const LVM_PERMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

static LVM_MUTATION_SEMAPHORE: Semaphore = Semaphore::const_new(MAX_CONCURRENT_LVM_MUTATIONS);

async fn acquire_lvm_permit() -> Result<tokio::sync::SemaphorePermit<'static>> {
    let started = Instant::now();
    let result = tokio::time::timeout(LVM_PERMIT_TIMEOUT, LVM_MUTATION_SEMAPHORE.acquire()).await;
    if let Some(m) = metrics::global() {
        m.lv_permit_wait_seconds
            .observe(started.elapsed().as_secs_f64());
    }
    match result {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => unreachable!("LVM_MUTATION_SEMAPHORE is a static and is never closed"),
        Err(_) => Err(LvmError::Busy(LVM_PERMIT_TIMEOUT)),
    }
}

/// LV name prefix for golden per-image volumes.
const IMAGE_LV_PREFIX: &str = "image-";
/// LV name prefix for per-VM snapshot volumes.
const VM_LV_PREFIX: &str = "vm-";
/// LV name prefix for per-VM raw data disks (extra disks requested by
/// the caller, handed to the guest unformatted). Kept distinct from
/// [`VM_LV_PREFIX`] so a data LV isn't misread as a rootfs LV by the
/// orphan sweep; the strict-prefix check runs via
/// [`parse_data_disk_lv_name`]. Naming is `vmdata-<vm_id>-<N>`; see
/// [`data_disk_lv_name`] for the layout.
const DATA_LV_PREFIX: &str = "vmdata-";

#[derive(Debug, thiserror::Error)]
pub enum LvmError {
    #[error(
        "volume group '{0}' not found — run the basis-prereqs ansible role on this host to \
         provision the LVM thin pool (set basis_lvm_devices in inventory first)"
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
        "lvm backend busy: could not acquire permit within {0:?} — arrival rate exceeds \
         service rate, shed load or retry"
    )]
    Busy(std::time::Duration),
}

pub type Result<T> = std::result::Result<T, LvmError>;

/// Fully-qualified device path to a golden image LV.
pub fn image_lv_path(image_hash: &str) -> PathBuf {
    PathBuf::from(format!("/dev/{VG}/{IMAGE_LV_PREFIX}{image_hash}"))
}

/// Fully-qualified device path to a VM's rootfs LV.
pub fn vm_lv_path(vm_id: &str) -> PathBuf {
    PathBuf::from(format!("/dev/{VG}/{VM_LV_PREFIX}{vm_id}"))
}

/// LV name for the `index`-th extra data disk of a VM.
///
/// `vmdata-<vm_id>-<index>`. The trailing `-<index>` is parsed right-to-left
/// in [`list_data_disk_vm_ids`] so vm_ids that themselves contain `-`
/// (UUIDs do) parse unambiguously as long as the index is a pure integer.
fn data_disk_lv_name(vm_id: &str, index: u32) -> String {
    format!("{DATA_LV_PREFIX}{vm_id}-{index}")
}

/// Fully-qualified device path to a VM's `index`-th extra data disk.
pub fn data_disk_lv_path(vm_id: &str, index: u32) -> PathBuf {
    PathBuf::from(format!("/dev/{VG}/{}", data_disk_lv_name(vm_id, index)))
}

/// Capacity snapshot of the thin pool; reported on heartbeat so the
/// scheduler can reject placements that wouldn't fit.
#[derive(Debug, Clone, Copy)]
pub struct PoolCapacity {
    pub data_total_bytes: u64,
    pub data_free_bytes: u64,
    pub metadata_total_bytes: u64,
    pub metadata_free_bytes: u64,
}

/// Validate the thin pool exists and is healthy. Fail-fast at agent
/// startup so we never silently degrade to a slower backend.
pub async fn validate_pool() -> Result<PoolCapacity> {
    // Check VG — `vgs <vg>` prints a row if present, exits non-zero if not.
    let status = Command::new("vgs")
        .args(["--noheadings", "-o", "vg_name", VG])
        .output()
        .await?;
    if !status.status.success() || status.stdout.is_empty() {
        return Err(LvmError::VgMissing(VG.to_string()));
    }

    // Check pool LV exists and is a thin pool. `lv_attr` is a 10-char
    // status string; the first char is the volume type — 't' means thin
    // pool (see lvs(8) "Lv attr bits").
    let out = Command::new("lvs")
        .args([
            "--noheadings",
            "--separator=|",
            "-o",
            "lv_name,lv_attr",
            &format!("{VG}/{POOL}"),
        ])
        .output()
        .await?;
    if !out.status.success() {
        return Err(LvmError::PoolMissing {
            vg: VG.to_string(),
            pool: POOL.to_string(),
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
            vg: VG.to_string(),
            pool: POOL.to_string(),
            attr,
        });
    }

    let cap = pool_capacity().await?;
    info!(
        vg = VG,
        pool = POOL,
        data_free_gib = cap.data_free_bytes / (1 << 30),
        data_total_gib = cap.data_total_bytes / (1 << 30),
        metadata_free_mib = cap.metadata_free_bytes / (1 << 20),
        metadata_total_mib = cap.metadata_total_bytes / (1 << 20),
        "thin pool ready"
    );
    Ok(cap)
}

/// Current capacity of the thin pool. Queries `lvs` with explicit sizes
/// in bytes (`--units b`) and data/metadata percentages.
pub async fn pool_capacity() -> Result<PoolCapacity> {
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
            &format!("{VG}/{POOL}"),
        ],
    )
    .await?;

    let line = out.trim();
    let parts: Vec<&str> = line.split('|').map(str::trim).collect();
    // The units=b output doesn't always zero-pad integers; `data_percent`
    // can be "0.00" or missing for a fresh pool. Tolerate both.
    let parse = |s: &&str, field: &str| -> Result<f64> {
        s.parse::<f64>().map_err(|_| LvmError::Command {
            cmd: "lvs".into(),
            stderr: format!("could not parse {field}={s:?}"),
        })
    };
    let data_total = parse(parts.first().unwrap_or(&"0"), "lv_size")? as u64;
    let data_pct = parts
        .get(1)
        .filter(|s| !s.is_empty())
        .map(|s| parse(s, "data_percent"))
        .transpose()?
        .unwrap_or(0.0);
    let meta_total = parse(parts.get(2).unwrap_or(&"0"), "lv_metadata_size")? as u64;
    let meta_pct = parts
        .get(3)
        .filter(|s| !s.is_empty())
        .map(|s| parse(s, "metadata_percent"))
        .transpose()?
        .unwrap_or(0.0);

    let data_used = (data_total as f64 * data_pct / 100.0) as u64;
    let meta_used = (meta_total as f64 * meta_pct / 100.0) as u64;
    Ok(PoolCapacity {
        data_total_bytes: data_total,
        data_free_bytes: data_total.saturating_sub(data_used),
        metadata_total_bytes: meta_total,
        metadata_free_bytes: meta_total.saturating_sub(meta_used),
    })
}

/// Ensure a golden image LV exists for the given image, populated from
/// the qcow2 source. Idempotent: if the LV is already present AND its
/// permission is 'r' (read-only, set at end of a successful convert),
/// returns the existing path. Otherwise creates, converts, and marks RO.
///
/// `virtual_size_gib` is the *image's* virtual size (10 GiB for the
/// Ubuntu cloud image today). Per-VM LVs extend from this when the
/// requested disk size is larger.
///
/// The "permission=r when populated" marker is what makes this safe to
/// re-run: a partial convert leaves a writable LV, which we'll overwrite
/// on the retry. A fully-converted LV is RO and skipped.
pub async fn ensure_image_lv(
    image_hash: &str,
    source_qcow2: &Path,
    virtual_size_gib: u64,
) -> Result<PathBuf> {
    let lv_name = format!("{IMAGE_LV_PREFIX}{image_hash}");
    let lv_path = image_lv_path(image_hash);

    // Fast-path the already-populated case before taking the permit —
    // otherwise every warm-cache VM create needlessly queues on the
    // global LVM semaphore.
    if matches!(lv_attr(&lv_name).await?.as_deref(), Some(a) if attr_is_readonly(a)) {
        return Ok(lv_path);
    }

    let _permit = acquire_lvm_permit().await?;

    // Re-check under the permit and branch on the full state in one
    // call: another caller may have finished while we queued (Ready),
    // a previous attempt may have left a writable LV from a crashed
    // convert (Stale), or the LV may be absent (Missing).
    match lv_attr(&lv_name).await? {
        Some(a) if attr_is_readonly(&a) => return Ok(lv_path),
        Some(_) => {
            warn!(lv = %lv_name, "golden image LV exists but is not RO; recreating");
            lvremove(&lv_name).await?;
        }
        None => {}
    }

    // Create a writable thin volume of the image's virtual size. Thin
    // means no extents are consumed until qemu-img writes to them.
    run_cmd(
        "lvcreate",
        &[
            "--virtualsize",
            &format!("{virtual_size_gib}G"),
            "--thin",
            "--name",
            &lv_name,
            &format!("{VG}/{POOL}"),
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
        // Remove the half-populated LV so the next retry starts clean.
        lvremove(&lv_name).await.ok();
        return Err(LvmError::ImageConvert(format!(
            "qemu-img exited {status} converting {} to {}",
            source_qcow2.display(),
            lv_path.display()
        )));
    }

    // Mark RO. This is both a correctness guarantee (snapshots see a
    // stable origin) and the "populated" marker `ensure_image_lv` keys
    // off on re-entry.
    run_cmd(
        "lvchange",
        &["--permission", "r", &format!("{VG}/{lv_name}")],
    )
    .await?;

    info!(lv = %lv_name, "golden image LV ready");
    Ok(lv_path)
}

/// Create a thin snapshot LV for a VM, extended to the requested size.
/// The returned path is the raw block device cloud-hypervisor attaches
/// with `--disk path=...,direct=on`.
///
/// Idempotent: if the VM LV already exists (stale from a crashed
/// create), remove it first. Creating-then-extending-then-activating is
/// fast enough (<500ms on tested hardware) that the idempotency cost is
/// negligible.
pub async fn create_vm_lv(vm_id: &str, image_hash: &str, disk_gib: u64) -> Result<PathBuf> {
    let _permit = acquire_lvm_permit().await?;

    let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
    let origin = format!("{IMAGE_LV_PREFIX}{image_hash}");
    let lv_path = vm_lv_path(vm_id);

    if lv_attr(&lv_name).await?.is_some() {
        warn!(vm_id, "VM LV already exists; removing for clean recreate");
        lvremove(&lv_name).await?;
    }

    // A snapshot of a thin LV is implicitly thin (lives in the origin's
    // pool) — `--thin` is only for creating new thin volumes and is
    // rejected with `--snapshot`. Flags that matter:
    //   --setactivationskip n  thin snapshots default to "skip" so they
    //                          need `lvchange -Kay` to activate. Disable
    //                          the flag at create time; we want the LV
    //                          active immediately for cloud-hypervisor.
    //   --permission rw        the origin is RO; the snapshot must be
    //                          writable. Default is rw, but explicit
    //                          beats implicit here.
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
            &format!("{VG}/{origin}"),
        ],
    )
    .await?;

    // Grow the virtual size to the requested disk. A 10G image + 40G
    // request becomes a 40G LV; cloud-init's growpart + resize2fs claims
    // the extra space inside the guest on first boot. If the request
    // matches the image size (10G image + 10G request), lvextend
    // reports "No size change" — tolerated as a no-op by `lvextend`
    // below, because the snapshot is already the right size.
    lvextend(&lv_name, disk_gib).await?;

    info!(vm_id, lv = %lv_name, origin = %origin, disk_gib, "VM LV ready");
    Ok(lv_path)
}

/// Remove a VM's LV. No-op if it doesn't exist (returns Ok).
pub async fn remove_vm_lv(vm_id: &str) -> Result<()> {
    let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
    // Existence check is read-only and cheap — keep it outside the
    // permit so the fast no-op path doesn't queue.
    if lv_attr(&lv_name).await?.is_none() {
        return Ok(());
    }
    let _permit = acquire_lvm_permit().await?;
    lvremove(&lv_name).await
}

/// Every vm_id with at least one LV on this host — rootfs
/// (`vm-<id>`) *or* any data disk (`vmdata-<id>-<N>`). One pass over
/// `lvs`; the orphan sweep diffs this against the agent DB and
/// reclaims the difference via [`remove_vm_lv`] + [`remove_vm_data_disks`].
///
/// Returns a set so a VM with both rootfs and data disks only shows
/// up once. `str::strip_prefix("vm-")` would match `vmdata-...` too,
/// so the data-disk prefix check runs first.
pub async fn list_managed_vm_ids() -> Result<std::collections::HashSet<String>> {
    let out = run_cmd("lvs", &["--noheadings", "-o", "lv_name", VG]).await?;
    let mut ids = std::collections::HashSet::new();
    for line in out.lines() {
        let name = line.trim();
        if let Some((vm_id, _index)) = parse_data_disk_lv_name(name) {
            ids.insert(vm_id);
        } else if let Some(id) = name.strip_prefix(VM_LV_PREFIX) {
            ids.insert(id.to_string());
        }
    }
    Ok(ids)
}

/// Create a blank thin LV to hand to the guest as an extra raw disk.
///
/// Unlike [`create_vm_lv`] this is NOT a snapshot of a golden image —
/// the primary consumer (Rook/Ceph) wipes its OSD devices at claim time
/// and expects them to be empty on arrival. A snapshot would copy-on-
/// write the wipe into metadata churn for no benefit. A plain thin
/// volume of the requested size is the minimum that satisfies the
/// guest-visible contract.
///
/// Idempotent on `(vm_id, index)`: if an LV of that name already exists
/// (from a crashed prior create) it is removed and recreated. Cheap —
/// thin allocation means the old LV held no extents.
pub async fn create_data_disk_lv(vm_id: &str, index: u32, size_gib: u64) -> Result<PathBuf> {
    let _permit = acquire_lvm_permit().await?;

    let lv_name = data_disk_lv_name(vm_id, index);
    let lv_path = data_disk_lv_path(vm_id, index);

    if lv_attr(&lv_name).await?.is_some() {
        warn!(vm_id, index, "data disk LV already exists; recreating clean");
        lvremove(&lv_name).await?;
    }

    run_cmd(
        "lvcreate",
        &[
            "--virtualsize",
            &format!("{size_gib}G"),
            "--thin",
            "--name",
            &lv_name,
            &format!("{VG}/{POOL}"),
        ],
    )
    .await?;

    info!(vm_id, index, lv = %lv_name, size_gib, "data disk LV ready");
    Ok(lv_path)
}

/// Remove every data disk LV belonging to `vm_id`. Idempotent — a VM
/// with no data disks is a no-op.
///
/// Enumerates from `lvs` rather than from the agent DB: a mid-create
/// crash that left a partial set of LVs behind gets cleaned up on
/// delete without needing separate reconciler logic.
pub async fn remove_vm_data_disks(vm_id: &str) -> Result<()> {
    let names = list_data_disk_lv_names_for(vm_id).await?;
    if names.is_empty() {
        return Ok(());
    }
    let _permit = acquire_lvm_permit().await?;
    for lv_name in &names {
        lvremove(lv_name).await?;
    }
    info!(vm_id, removed = names.len(), "VM data disks removed");
    Ok(())
}

// --- internal helpers ---------------------------------------------------

/// Read-only state query for a single LV in the basis VG. `None` means
/// the LV is not present; `Some(attr)` returns its 10-char lv_attr
/// bitfield (see lvs(8) "Lv attr bits").
///
/// Uses LVM's `-S lv_name=<name>` reporting selection rather than a
/// positional `VG/LV` argument. Selection always exits 0 — empty
/// output when nothing matches, one row when it does — so exit-nonzero
/// is reserved for real failures (lvm daemon down, VG disappeared
/// mid-flight, permission error). That lets us propagate real errors
/// instead of silently treating them as "LV not present" the way a
/// raw `lvs VG/LV` would.
async fn lv_attr(lv_name: &str) -> Result<Option<String>> {
    let out = run_cmd(
        "lvs",
        &[
            "--noheadings",
            "-o",
            "lv_attr",
            "-S",
            &format!("lv_name={lv_name}"),
            VG,
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

/// `true` when a 10-char lv_attr bitfield marks the LV read-only. Per
/// lvs(8), the second character is permission: 'r' = read-only,
/// 'w' = writable.
fn attr_is_readonly(attr: &str) -> bool {
    attr.chars().nth(1) == Some('r')
}

async fn lvremove(lv_name: &str) -> Result<()> {
    run_cmd("lvremove", &["-f", &format!("{VG}/{lv_name}")])
        .await
        .map(|_| ())
}

/// Extend an LV to `size_gib` GiB. Idempotent: if the LV is already at
/// the requested size (snapshot of an image LV that's already the right
/// size), LVM exits non-zero with "No size change" — treated as success
/// since the post-condition ("LV is `size_gib` GiB") holds.
///
/// `-L 10G` (uppercase) is gibibytes (2^30). Lowercase `g` is SI
/// gigabytes (10^9) and is wrong for the API contract the caller wants.
async fn lvextend(lv_name: &str, size_gib: u64) -> Result<()> {
    match run_cmd(
        "lvextend",
        &["-L", &format!("{size_gib}G"), &format!("{VG}/{lv_name}")],
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(LvmError::Command { stderr, .. }) if stderr.contains("No size change") => Ok(()),
        Err(e) => Err(e),
    }
}

/// Data-disk LV names belonging to a specific vm_id.
async fn list_data_disk_lv_names_for(vm_id: &str) -> Result<Vec<String>> {
    let out = run_cmd("lvs", &["--noheadings", "-o", "lv_name", VG]).await?;
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

/// Split `vmdata-<vm_id>-<index>` into `(vm_id, index)`. Returns `None`
/// when the name doesn't match the expected shape (an unexpected LV in
/// the pool, or a name we didn't create).
///
/// Right-to-left split on `-` is load-bearing: vm_ids are UUIDs and
/// contain `-` themselves. `rsplit_once` peels only the trailing
/// segment, and the index must parse as a pure integer — a malformed
/// tail fails the parse rather than silently producing a wrong vm_id.
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
/// reading logs) can see why it failed. I/O errors (the binary isn't
/// on PATH, fork fails) propagate as `LvmError::Io`.
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

    /// A wrong parse here would let the orphan sweep delete the wrong
    /// VM's data disks, so pin the happy path, UUID-with-dashes case,
    /// and every rejection case.
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
