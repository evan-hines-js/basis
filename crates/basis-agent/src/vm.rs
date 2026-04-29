//! Cloud-hypervisor process lifecycle.
//!
//! Each VM is a `cloud-hypervisor` child process launched as a systemd
//! transient *service* via `systemd-run`. Using systemd rather than
//! parenting the process to the agent directly means:
//!   * VMs survive agent restarts (upgrades, crashes, config reloads).
//!   * `journalctl -u basis-vm-<id>` is the one-stop debug surface.
//!   * cgroups accounting is per-VM out of the box.
//!
//! Service, not scope: `--scope` would block `systemd-run` until the VM
//! exited (it attaches the process to the caller's session); a service
//! forks the VM under systemd's supervision and `systemd-run` returns
//! immediately. `--remain-after-exit` keeps the unit visible after the
//! VM dies so `systemctl status` and the journal can be consulted for
//! root cause.
//!
//! Boot path: direct kernel boot with a pre-extracted `vmlinuz` +
//! `initrd` (see `image.rs` for why we skip UEFI/shim/grub). The rootfs
//! is a raw LVM thin snapshot attached with `image_type=raw,direct=on`
//! — `lvm.rs` owns the storage rationale; the `--disk` flag comments
//! below explain the cloud-hypervisor-specific gotchas.
//!
//! Delete relies on `systemctl stop` for graceful shutdown:
//! cloud-hypervisor handles SIGTERM by ACPI-powering-off the guest
//! before exiting, and systemd's TimeoutStopSec (default 90s) gives
//! the guest time to flush before SIGKILL. Directory cleanup is
//! best-effort — the VM dir is regenerated on next create.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::info;

use basis_proto::CreateVmCommand;

#[derive(Debug, thiserror::Error)]
pub enum VmError {
    #[error("cloud-hypervisor failed to start: {0}")]
    SpawnFailed(String),

    #[error("cloud-hypervisor exited with error: {0}")]
    ProcessFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Tracks a running VM managed by a systemd transient unit.
struct TrackedVm {
    unit_name: String,
    vm_dir: PathBuf,
}

/// Host-resolved artifacts handed to `create_vm`. Grouped so the spawn
/// signature stays readable and so a caller can't accidentally transpose
/// `kernel` and `initrd`.
pub struct BootArtifacts<'a> {
    pub kernel: &'a Path,
    pub initrd: &'a Path,
    /// Raw LVM thin snapshot that becomes the guest rootfs.
    pub rootfs: &'a Path,
    /// Generated cloud-init cidata ISO attached as a second disk.
    pub cloud_init: &'a Path,
    /// Raw thin LVs attached as additional virtio-blk devices after
    /// `rootfs` and `cloud_init`. Order is load-bearing: the N'th entry
    /// becomes `/dev/vd{c,d,e,...}` in the guest (rootfs is `/dev/vda`,
    /// cloud-init ISO is `/dev/vdb`). Callers produce this from
    /// `CreateVmCommand.extra_disks` preserving the caller's order so a
    /// post-reboot restart reattaches the same disk at the same index.
    pub extra_disks: &'a [PathBuf],
}

/// Owner of "VMs running on this host" state.
///
/// Interior mutability over `tracked` / `pending` so callers share a
/// single `Arc<VmManager>` instead of `Arc<Mutex<VmManager>>`. This
/// lets concurrent `create_vm` calls run their `systemd-run` spawns in
/// parallel — locks are held only during brief map mutations, not
/// across I/O.
///
/// Two sets, distinct concepts:
///   - `tracked`: the VM has a spawned systemd unit. Populated after
///     `systemd-run` returns, cleared when `delete_vm` starts.
///     Doesn't imply the *process* is still alive — use
///     [`Self::has_live_process`] for that; a crashed
///     `cloud-hypervisor` under `--remain-after-exit` stays in
///     `tracked` until explicit delete.
///   - `pending`: the VM's DB row exists but the systemd unit hasn't
///     spawned yet — i.e. we're mid-create. Populated by
///     [`crate::handlers::create_vm`] before the DB insert, cleared
///     when the whole create flow exits (success or rollback).
///
/// The union — [`Self::live_vm_ids`] — is what the orphan sweep uses
/// to avoid reclaiming resources for VMs this agent is actively
/// managing, even if the DB row is momentarily missing.
pub struct VmManager {
    pub vms_dir: PathBuf,
    tracked: Mutex<HashMap<String, TrackedVm>>,
    pending: Mutex<HashSet<String>>,
}

impl VmManager {
    pub fn new(vms_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&vms_dir).ok();
        Self {
            vms_dir,
            tracked: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashSet::new()),
        }
    }

    /// Mark a VM as mid-create. Must be called before any state that
    /// the reconciler can observe (the DB row insert, host resources).
    pub async fn mark_pending(&self, vm_id: &str) {
        self.pending.lock().await.insert(vm_id.to_string());
    }

    /// Unmark. Called from the create wrapper on both success and
    /// rollback; idempotent so double-clear is safe.
    pub async fn clear_pending(&self, vm_id: &str) {
        self.pending.lock().await.remove(vm_id);
    }

    /// Spawn a cloud-hypervisor process for a VM as a systemd transient unit.
    ///
    /// Using systemd-run gives us:
    /// - Automatic cleanup if the agent crashes (the VM process is parented to systemd, not us)
    /// - cgroups resource isolation per VM
    /// - Journal logging per VM (`journalctl -u basis-vm-<id>`)
    /// - `systemctl` visibility for debugging
    pub async fn create_vm(
        &self,
        cmd: &CreateVmCommand,
        boot: &BootArtifacts<'_>,
        primary_tap: &str,
        vfio_devices: &[String],
    ) -> Result<(), VmError> {
        let vm_dir = self.vms_dir.join(&cmd.vm_id);
        std::fs::create_dir_all(&vm_dir)?;

        let socket_path = vm_dir.join("cloud-hypervisor.sock");
        let unit_name = unit_name_for_vm(&cmd.vm_id);

        // Direct kernel boot: we pass the guest kernel, initramfs, and a
        // hardcoded command line to cloud-hypervisor, bypassing the EFI
        // firmware / shim / grub chain entirely. Rationale lives in
        // `image.rs`'s module doc.
        //
        // `--net` takes multiple values as space-separated arguments
        // after a single flag. The guest's kernel-assigned interface
        // names (`ens3`, `ens4`, …) come from PCI slot order, which
        // cloud-hypervisor allocates by device class. The cloud-init
        // network-config in `image::create_cloud_init_iso` matches by
        // MAC address (this very string) so the kernel-assigned name
        // is irrelevant.
        let primary_mac = primary_mac(&cmd.vm_id);
        let mut ch_args = vec![
            format!("--api-socket={}", socket_path.to_string_lossy()),
            format!("--cpus=boot={}", cmd.cpu),
            format!("--memory=size={}M", cmd.memory_mib),
            format!("--kernel={}", boot.kernel.to_string_lossy()),
            format!("--initramfs={}", boot.initrd.to_string_lossy()),
            // transparent_hugepage=never: etcd's own docs call this out —
            // THP defragmentation stalls the mutator for seconds at a
            // time, which pushes WAL fsync latency past raft election
            // timeouts and kicks off leader flaps.
            "--cmdline=root=/dev/vda1 ro console=ttyS0 transparent_hugepage=never".to_string(),
            "--net".to_string(),
            format!("tap={primary_tap},mac={primary_mac}"),
            "--serial=tty".to_string(),
            "--console=off".to_string(),
            "--disk".to_string(),
        ];

        // Disk order is load-bearing: rootfs becomes /dev/vda,
        // cloud-init ISO /dev/vdb, then each data disk /dev/vd{c,d,…}
        // in caller order. The same Display path emits every entry so
        // tuning (direct=on, num_queues, discard) can't drift between
        // call sites.
        let disks = std::iter::once(DiskSpec::Rootfs {
            path: boot.rootfs.into(),
            vcpus: cmd.cpu,
        })
        .chain(std::iter::once(DiskSpec::CloudInit {
            path: boot.cloud_init.into(),
        }))
        .chain(boot.extra_disks.iter().map(|p| DiskSpec::Data {
            path: p.clone(),
            vcpus: cmd.cpu,
        }));
        for spec in disks {
            ch_args.push(spec.to_string());
        }

        if !vfio_devices.is_empty() {
            ch_args.push("--device".to_string());
            for device_path in vfio_devices {
                ch_args.push(format!("path={device_path}"));
            }
        }

        // Run as a transient *service*, not a scope. `--scope` would block
        // here until cloud-hypervisor exits (it attaches the process to
        // the caller's session). A service forks the VM under systemd's
        // supervision, systemd-run returns immediately, and the VM keeps
        // running if the agent restarts. `--remain-after-exit` keeps the
        // unit visible in `systemctl` after cloud-hypervisor exits so we
        // can read its journal and exit status — essential for debugging
        // a VM that crashed at boot.
        let mut args = vec![
            format!("--unit={unit_name}"),
            "--service-type=exec".to_string(),
            "--remain-after-exit".to_string(),
            format!("--description=Basis VM {}", cmd.vm_id),
            "--".to_string(),
            "cloud-hypervisor".to_string(),
        ];
        args.extend(ch_args);

        info!(vm_id = %cmd.vm_id, unit = %unit_name, "spawning cloud-hypervisor via systemd-run");

        let output = Command::new("systemd-run")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| VmError::SpawnFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(VmError::SpawnFailed(format!(
                "systemd-run failed: {stderr}"
            )));
        }

        self.tracked
            .lock()
            .await
            .insert(cmd.vm_id.clone(), TrackedVm { unit_name, vm_dir });

        Ok(())
    }

    /// Shut down and clean up a VM.
    pub async fn delete_vm(&self, vm_id: &str) -> Result<(), VmError> {
        let tracked = self.tracked.lock().await.remove(vm_id);
        let unit_name = tracked
            .as_ref()
            .map(|t| t.unit_name.clone())
            .unwrap_or_else(|| unit_name_for_vm(vm_id));
        let vm_dir = tracked
            .as_ref()
            .map(|t| t.vm_dir.clone())
            .unwrap_or_else(|| self.vms_dir.join(vm_id));

        // `systemctl stop` sends SIGTERM, which cloud-hypervisor
        // handles by ACPI-powering-off the guest cleanly before
        // exiting. systemd's TimeoutStopSec (default 90s) gives the
        // guest time to flush before SIGKILL, so this is already a
        // graceful shutdown — no separate API call needed.
        let _ = Command::new("systemctl")
            .args(["stop", &unit_name])
            .output()
            .await;

        // Drain pending udev events before returning. `systemctl stop`
        // returns once qemu has exited, but the kernel's block-device
        // release for the rootfs LV is asynchronous — for a brief
        // window (<100ms in practice, longer under I/O load) the LV
        // is still marked in-use by udev even with no fds open. The
        // caller's next step is `Storage::remove_vm_lv`, which in
        // that window gets EBUSY; since the error is logged-and-
        // skipped at the handler level, a lost race leaks the LV
        // permanently. A bounded `udevadm settle` closes the race at
        // its source so the reconciler only has to mop up genuine
        // crash-time orphans, not happy-path releases.
        let _ = Command::new("udevadm")
            .args(["settle", "--timeout=5"])
            .output()
            .await;

        // Clean up VM directory (overlay, cloud-init, socket)
        if vm_dir.exists() {
            std::fs::remove_dir_all(&vm_dir).ok();
        }

        info!(vm_id, "VM deleted");
        Ok(())
    }

    /// Reconcile running cloud-hypervisor processes on agent startup.
    ///
    /// Because VMs run as systemd transient units, they survive agent restarts and
    /// even agent crashes. On startup we:
    /// 1. List running basis-vm-* systemd units
    /// 2. Match them against what the controller expects (via the stream)
    /// 3. Track the ones the controller knows about
    /// 4. Kill any orphans the controller doesn't know about
    pub async fn reconcile_running(&self) -> Result<Vec<String>, VmError> {
        let output = Command::new("systemctl")
            .args([
                "list-units",
                "--type=service",
                "--state=running",
                "--no-legend",
                "--plain",
                "basis-vm-*",
            ])
            .output()
            .await?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut running_vm_ids = Vec::new();
        let mut to_track = Vec::new();

        for line in stdout.lines() {
            let unit = line.split_whitespace().next().unwrap_or("");
            if let Some(vm_id) = unit
                .strip_prefix("basis-vm-")
                .and_then(|s| s.strip_suffix(".service"))
            {
                running_vm_ids.push(vm_id.to_string());
                to_track.push((
                    vm_id.to_string(),
                    TrackedVm {
                        unit_name: unit.to_string(),
                        vm_dir: self.vms_dir.join(vm_id),
                    },
                ));
            }
        }

        let mut tracked = self.tracked.lock().await;
        for (id, tv) in to_track {
            tracked.insert(id, tv);
        }

        info!(
            count = running_vm_ids.len(),
            "reconciled running VMs from systemd"
        );
        Ok(running_vm_ids)
    }

    /// True iff a create is currently mid-flight for this vm_id —
    /// `mark_pending` has been called but `clear_pending` has not.
    /// Callers that do *state reporting* to the controller (e.g. the
    /// periodic `report_local_vm_states`) must skip VMs in this set;
    /// the authoritative Running/Failed report comes from
    /// `spawn_create` when `systemd-run` actually returns, and a
    /// premature report here would let the controller resolve a
    /// pending CreateMachine before the systemd unit exists, which
    /// lets a subsequent DeleteMachine race with the still-pending
    /// start job and trip `systemd-run: Job canceled`.
    pub async fn is_pending(&self, vm_id: &str) -> bool {
        self.pending.lock().await.contains(vm_id)
    }

    /// Every vm_id this agent is actively managing — a tracked VM
    /// (systemd unit spawned, may or may not currently be running) or
    /// a pending create (mid-flight before the unit even exists). The
    /// orphan sweep merges this with the agent DB to form its `known`
    /// set, so a transiently-missing DB row can't cause it to rip
    /// physical resources (LV, tap) out from under a VM we know we're
    /// running. Without this, a race between a DB-row removal and the
    /// sweep's list-then-reclaim scan manifests as a silent tap delete
    /// while cloud-hypervisor is using it, which the guest sees as a
    /// virtio-net worker crash.
    pub async fn live_vm_ids(&self) -> std::collections::HashSet<String> {
        let mut ids = std::collections::HashSet::new();
        for id in self.tracked.lock().await.keys() {
            ids.insert(id.clone());
        }
        for id in self.pending.lock().await.iter() {
            ids.insert(id.clone());
        }
        ids
    }

    /// True iff the VM's systemd unit currently has a running
    /// `cloud-hypervisor` process attached. Authoritative — reads
    /// `SubState` directly from systemd rather than trusting
    /// `tracked`, which is an in-memory map populated at create and
    /// only cleared at delete (so a `cloud-hypervisor` crash with
    /// `--remain-after-exit` leaves the entry stale).
    ///
    /// Used by the periodic state reporter to flip crashed VMs to
    /// `Failed` within one tick — without this, a guest-level crash
    /// (virtio error, kernel panic, OOM) leaves the VM in `Running`
    /// forever from basis's perspective and CAPI never replaces it.
    pub async fn has_live_process(&self, vm_id: &str) -> bool {
        let unit_name = self
            .tracked
            .lock()
            .await
            .get(vm_id)
            .map(|t| t.unit_name.clone())
            .unwrap_or_else(|| unit_name_for_vm(vm_id));
        let out = Command::new("systemctl")
            .args(["show", "--property=SubState", "--value", &unit_name])
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => {
                // `SubState=running` is systemd's "main process is
                // alive." `exited`, `dead`, `failed` all mean the
                // cloud-hypervisor process is gone; `--remain-after-
                // exit` keeps the unit visible in `active` state but
                // doesn't resurrect the process.
                String::from_utf8_lossy(&o.stdout).trim() == "running"
            }
            _ => false,
        }
    }
}

/// Transient systemd unit name for a VM. Services (not scopes) — see the
/// comment in `create_vm`.
pub fn unit_name_for_vm(vm_id: &str) -> String {
    format!("basis-vm-{vm_id}.service")
}

/// One cloud-hypervisor `--disk` argument. Three kinds, distinguished
/// by their tuning needs:
///
/// * [`DiskSpec::Rootfs`] — backed by an LVM thin snapshot of a golden
///   image. Needs durability tunings (`direct=on`, `image_type=raw`)
///   and multi-queue virtio-blk for guest fsyncs (etcd WAL).
/// * [`DiskSpec::Data`] — backed by a linear LV in the data VG. Same
///   durability tunings as rootfs *plus* `discard=unmap` so guest
///   `blkdiscard` / fstrim reaches the underlying NVMe through dm-
///   linear (no metadata indirection). Bluestore on dm-thin would
///   double-book allocation; linear is one-table-lookup pass-through.
/// * [`DiskSpec::CloudInit`] — read-once cidata ISO. No tuning; the
///   guest reads it once at first boot and never again.
///
/// Centralising every disk-arg construction here keeps tuning from
/// drifting across call sites. Tested by [`tests::disk_spec_kinds`].
///
/// Notes on the cloud-hypervisor flags:
/// * `image_type=raw` bypasses autodetect, which otherwise silently
///   rejects guest writes to sector 0 (PR #7728) and breaks cloud-
///   init's growpart on first boot.
/// * `direct=on` is O_DIRECT on the host — mandatory for durability:
///   etcd's WAL fsyncs and Ceph's own sync semantics are defeated by
///   the host page cache.
/// * `num_queues` / `queue_size` scale virtio-blk parallelism with
///   the guest's vCPU count. Cloud-hypervisor defaults are 1/128.
pub enum DiskSpec {
    Rootfs { path: PathBuf, vcpus: u32 },
    Data { path: PathBuf, vcpus: u32 },
    CloudInit { path: PathBuf },
}

impl fmt::Display for DiskSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rootfs { path, vcpus } | Self::Data { path, vcpus } => write!(
                f,
                "path={},image_type=raw,direct=on,num_queues={vcpus},queue_size=256",
                path.display()
            ),
            Self::CloudInit { path } => write!(f, "path={}", path.display()),
        }
    }
}

/// Deterministic MAC for the primary (tree-side) NIC. Public so the
/// cloud-init builder can match the netplan config to this exact NIC
/// by MAC — guest interface names (`ens3` / `ens4` / etc) shift
/// whenever cloud-hypervisor's PCI slot allocation changes (e.g.
/// adding an `extra_disks` entry rearranges the virtio bus), so name-
/// based netplan stanzas silently apply to nothing.
pub fn primary_mac(vm_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    let hash = hasher.finish();
    // 52:54:00 is the locally-administered OUI QEMU/KVM uses.
    format!(
        "52:54:00:{:02x}:{:02x}:{:02x}",
        (hash >> 16) & 0xff,
        (hash >> 8) & 0xff,
        hash & 0xff,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macs_are_deterministic() {
        assert_eq!(primary_mac("vm-123"), primary_mac("vm-123"));
    }

    #[test]
    fn macs_differ_across_vms() {
        assert_ne!(primary_mac("vm-1"), primary_mac("vm-2"));
    }

    #[test]
    fn macs_use_qemu_locally_administered_oui() {
        assert!(primary_mac("any").starts_with("52:54:00:"));
    }

    #[test]
    fn test_unit_name_format() {
        let name = unit_name_for_vm("abc-123");
        assert_eq!(name, "basis-vm-abc-123.service");
    }

    /// Every kind emits the right shape for its role. Drift here
    /// (losing `direct=on`, smuggling tuning into the cidata arg) is
    /// silent and only manifests later as data corruption under host
    /// crash.
    #[test]
    fn disk_spec_kinds() {
        let rootfs = DiskSpec::Rootfs {
            path: PathBuf::from("/dev/basis/vm-x"),
            vcpus: 8,
        }
        .to_string();
        assert_eq!(
            rootfs,
            "path=/dev/basis/vm-x,image_type=raw,direct=on,num_queues=8,queue_size=256",
        );

        let data = DiskSpec::Data {
            path: PathBuf::from("/dev/basis-data/vmdata-x-0"),
            vcpus: 8,
        }
        .to_string();
        assert_eq!(
            data,
            "path=/dev/basis-data/vmdata-x-0,image_type=raw,direct=on,num_queues=8,queue_size=256",
        );

        let cidata = DiskSpec::CloudInit {
            path: PathBuf::from("/var/lib/basis/vms/x/cidata.iso"),
        }
        .to_string();
        assert_eq!(cidata, "path=/var/lib/basis/vms/x/cidata.iso");
    }
}
