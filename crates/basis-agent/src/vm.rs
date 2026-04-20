use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;
use tracing::{info, warn};

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
    pub unit_name: String,
    pub vm_dir: PathBuf,
}

pub struct VmManager {
    pub vms_dir: PathBuf,
    firmware_path: PathBuf,
    tracked: HashMap<String, TrackedVm>,
}

impl VmManager {
    pub fn new(vms_dir: PathBuf, firmware_path: PathBuf) -> Self {
        std::fs::create_dir_all(&vms_dir).ok();
        Self {
            vms_dir,
            firmware_path,
            tracked: HashMap::new(),
        }
    }

    /// Spawn a cloud-hypervisor process for a VM as a systemd transient unit.
    ///
    /// Using systemd-run gives us:
    /// - Automatic cleanup if the agent crashes (the VM process is parented to systemd, not us)
    /// - cgroups resource isolation per VM
    /// - Journal logging per VM (`journalctl -u basis-vm-<id>`)
    /// - `systemctl` visibility for debugging
    pub async fn create_vm(
        &mut self,
        cmd: &CreateVmCommand,
        disk_path: &Path,
        cloud_init_path: &Path,
        tap_name: &str,
        vfio_devices: &[String],
    ) -> Result<(), VmError> {
        let vm_dir = self.vms_dir.join(&cmd.vm_id);
        std::fs::create_dir_all(&vm_dir)?;

        let socket_path = vm_dir.join("cloud-hypervisor.sock");
        let unit_name = unit_name_for_vm(&cmd.vm_id);

        // Cloud-hypervisor takes multiple values for `--disk` as
        // space-separated arguments after a single flag — NOT as repeated
        // `--disk` flags. Same shape for `--device` below.
        let mut ch_args = vec![
            format!("--api-socket={}", socket_path.to_string_lossy()),
            format!("--cpus=boot={}", cmd.cpu),
            format!("--memory=size={}M", cmd.memory_mib),
            format!("--firmware={}", self.firmware_path.to_string_lossy()),
            format!("--net=tap={tap_name},mac={}", generate_mac(&cmd.vm_id)),
            "--serial=tty".to_string(),
            "--console=off".to_string(),
            "--disk".to_string(),
            format!("path={}", disk_path.to_string_lossy()),
            format!("path={}", cloud_init_path.to_string_lossy()),
        ];

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

        self.tracked.insert(
            cmd.vm_id.clone(),
            TrackedVm {
                unit_name,
                vm_dir,
            },
        );

        Ok(())
    }

    /// Shut down and clean up a VM.
    pub async fn delete_vm(&mut self, vm_id: &str) -> Result<(), VmError> {
        let tracked = self.tracked.remove(vm_id);
        let unit_name = tracked
            .as_ref()
            .map(|t| t.unit_name.clone())
            .unwrap_or_else(|| unit_name_for_vm(vm_id));
        let vm_dir = tracked
            .as_ref()
            .map(|t| t.vm_dir.clone())
            .unwrap_or_else(|| self.vms_dir.join(vm_id));

        // Try graceful shutdown via cloud-hypervisor API socket first
        let socket_path = vm_dir.join("cloud-hypervisor.sock");
        if socket_path.exists() {
            let _ = shutdown_via_api(&socket_path).await;
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        // Stop the systemd unit (kills the process if still running)
        let _ = Command::new("systemctl")
            .args(["stop", &unit_name])
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
    pub async fn reconcile_running(&mut self) -> Result<Vec<String>, VmError> {
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

        for line in stdout.lines() {
            let unit = line.split_whitespace().next().unwrap_or("");
            if let Some(vm_id) = unit
                .strip_prefix("basis-vm-")
                .and_then(|s| s.strip_suffix(".service"))
            {
                running_vm_ids.push(vm_id.to_string());

                let vm_dir = self.vms_dir.join(vm_id);
                self.tracked.insert(
                    vm_id.to_string(),
                    TrackedVm {
                        unit_name: unit.to_string(),
                        vm_dir,
                    },
                );
            }
        }

        info!(count = running_vm_ids.len(), "reconciled running VMs from systemd");
        Ok(running_vm_ids)
    }

    pub fn is_running(&self, vm_id: &str) -> bool {
        self.tracked.contains_key(vm_id)
    }

    pub fn tracked_vm_ids(&self) -> Vec<String> {
        self.tracked.keys().cloned().collect()
    }
}

/// Send shutdown command to cloud-hypervisor via its HTTP API socket.
async fn shutdown_via_api(socket_path: &Path) -> Result<(), VmError> {
    let socket = socket_path.to_string_lossy().to_string();

    let output = Command::new("curl")
        .args([
            "--unix-socket",
            &socket,
            "-X",
            "PUT",
            "http://localhost/api/v1/vm.shutdown",
        ])
        .output()
        .await
        .map_err(|e| VmError::ProcessFailed(e.to_string()))?;

    if !output.status.success() {
        warn!(
            "shutdown API call failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

/// Transient systemd unit name for a VM. Services (not scopes) — see the
/// comment in `create_vm`.
pub fn unit_name_for_vm(vm_id: &str) -> String {
    format!("basis-vm-{vm_id}.service")
}

/// Generate a deterministic MAC address from a VM ID.
fn generate_mac(vm_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    let hash = hasher.finish();

    // Locally administered, unicast MAC (bit 1 of first octet = 1, bit 0 = 0)
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
    fn test_mac_is_deterministic() {
        let a = generate_mac("vm-123");
        let b = generate_mac("vm-123");
        assert_eq!(a, b);
    }

    #[test]
    fn test_mac_unique_for_different_vms() {
        let a = generate_mac("vm-1");
        let b = generate_mac("vm-2");
        assert_ne!(a, b);
    }

    #[test]
    fn test_mac_has_locally_administered_prefix() {
        let mac = generate_mac("any-vm");
        // 52:54:00 is the KVM/QEMU locally administered OUI
        assert!(mac.starts_with("52:54:00:"));
    }

    #[test]
    fn test_mac_format() {
        let mac = generate_mac("test");
        let parts: Vec<&str> = mac.split(':').collect();
        assert_eq!(parts.len(), 6);
        for part in &parts {
            assert_eq!(part.len(), 2);
        }
    }

    #[test]
    fn test_unit_name_format() {
        let name = unit_name_for_vm("abc-123");
        assert_eq!(name, "basis-vm-abc-123.service");
    }
}
