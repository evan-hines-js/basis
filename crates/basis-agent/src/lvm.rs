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

use tokio::process::Command;
use tracing::{info, warn};

/// Fixed volume group name. The ansible role creates exactly this.
pub const VG: &str = "basis";
/// Fixed thin pool name within the VG.
pub const POOL: &str = "pool";

/// LV name prefix for golden per-image volumes.
const IMAGE_LV_PREFIX: &str = "image-";
/// LV name prefix for per-VM snapshot volumes.
const VM_LV_PREFIX: &str = "vm-";

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
    let out = run_lvs_pipe(&[
        "--noheadings",
        "--separator=|",
        "--units",
        "b",
        "--nosuffix",
        "-o",
        "lv_size,data_percent,lv_metadata_size,metadata_percent",
        &format!("{VG}/{POOL}"),
    ])
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

    if lv_is_readonly(&lv_name).await? {
        return Ok(lv_path);
    }

    // Either doesn't exist, or exists but not-yet-populated (previous run
    // crashed between create and RO-flip). Remove and recreate for a
    // clean slate; the convert below is what actually matters.
    if lv_exists(&lv_name).await? {
        warn!(lv = %lv_name, "golden image LV exists but is not RO; recreating");
        lvremove(&lv_name).await?;
    }

    // Create a writable thin volume of the image's virtual size. Thin
    // means no extents are consumed until qemu-img writes to them.
    run_lvm(
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
    run_lvm(
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
pub async fn create_vm_lv(
    vm_id: &str,
    image_hash: &str,
    disk_gib: u64,
) -> Result<PathBuf> {
    let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
    let origin = format!("{IMAGE_LV_PREFIX}{image_hash}");
    let lv_path = vm_lv_path(vm_id);

    if lv_exists(&lv_name).await? {
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
    run_lvm(
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
    // the extra space inside the guest on first boot.
    // `-L 40G` (uppercase) is gibibytes (2^30). Lowercase `g` is SI
    // gigabytes (10^9) — 40g is 37.25 GiB, not 40 GiB. The user asked
    // for disk_gib GiB; give them GiB.
    run_lvm(
        "lvextend",
        &["-L", &format!("{disk_gib}G"), &format!("{VG}/{lv_name}")],
    )
    .await?;

    info!(vm_id, lv = %lv_name, origin = %origin, disk_gib, "VM LV ready");
    Ok(lv_path)
}

/// Remove a VM's LV. No-op if it doesn't exist (returns Ok).
pub async fn remove_vm_lv(vm_id: &str) -> Result<()> {
    let lv_name = format!("{VM_LV_PREFIX}{vm_id}");
    if !lv_exists(&lv_name).await? {
        return Ok(());
    }
    lvremove(&lv_name).await
}

/// List all VM LVs currently in the basis VG. Used by reconcile to find
/// orphan LVs (VMs the controller has forgotten).
pub async fn list_vm_lvs() -> Result<Vec<String>> {
    let out = run_lvs_pipe(&["--noheadings", "-o", "lv_name", VG]).await?;
    let mut vm_ids = Vec::new();
    for line in out.lines() {
        let name = line.trim();
        if let Some(id) = name.strip_prefix(VM_LV_PREFIX) {
            vm_ids.push(id.to_string());
        }
    }
    Ok(vm_ids)
}

// --- internal helpers ---------------------------------------------------

async fn lv_exists(lv_name: &str) -> Result<bool> {
    let out = Command::new("lvs")
        .args(["--noheadings", &format!("{VG}/{lv_name}")])
        .output()
        .await?;
    Ok(out.status.success())
}

async fn lv_is_readonly(lv_name: &str) -> Result<bool> {
    let out = Command::new("lvs")
        .args([
            "--noheadings",
            "--separator=|",
            "-o",
            "lv_attr",
            &format!("{VG}/{lv_name}"),
        ])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(false);
    }
    // lv_attr's second character is permission: 'r' = read-only, 'w' = writable.
    let attr = String::from_utf8_lossy(&out.stdout);
    Ok(attr
        .trim()
        .chars()
        .nth(1)
        .map(|c| c == 'r')
        .unwrap_or(false))
}

async fn lvremove(lv_name: &str) -> Result<()> {
    run_lvm("lvremove", &["-f", &format!("{VG}/{lv_name}")]).await
}

async fn run_lvm(tool: &'static str, args: &[&str]) -> Result<()> {
    let out = Command::new(tool).args(args).output().await?;
    if !out.status.success() {
        return Err(LvmError::Command {
            cmd: format!("{tool} {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

async fn run_lvs_pipe(args: &[&str]) -> Result<String> {
    let out = Command::new("lvs").args(args).output().await?;
    if !out.status.success() {
        return Err(LvmError::Command {
            cmd: format!("lvs {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}
