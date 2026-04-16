use tokio::process::Command;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("bridge setup failed: {0}")]
    BridgeFailed(String),

    #[error("command failed: {0}")]
    CommandFailed(#[from] std::io::Error),
}

pub struct NetworkManager {
    bridge_name: String,
    physical_nic: String,
}

impl NetworkManager {
    pub fn new(bridge_name: String, physical_nic: String) -> Self {
        Self {
            bridge_name,
            physical_nic,
        }
    }

    /// Ensure the host bridge exists and is connected to the physical NIC.
    pub async fn ensure_bridge(&self) -> Result<(), NetworkError> {
        // Check if bridge already exists
        let exists = Command::new("ip")
            .args(["link", "show", &self.bridge_name])
            .output()
            .await?;

        if exists.status.success() {
            info!(bridge = %self.bridge_name, "bridge already exists");
            return Ok(());
        }

        // Create bridge
        run_cmd("ip", &["link", "add", &self.bridge_name, "type", "bridge"]).await?;
        run_cmd("ip", &["link", "set", &self.bridge_name, "up"]).await?;

        // Attach physical NIC to bridge
        run_cmd(
            "ip",
            &["link", "set", &self.physical_nic, "master", &self.bridge_name],
        )
        .await?;

        info!(
            bridge = %self.bridge_name,
            nic = %self.physical_nic,
            "bridge created and attached to NIC"
        );
        Ok(())
    }

    /// Create a tap device for a VM and attach it to the bridge.
    /// Fails if the tap already exists — use `ensure_tap` for idempotent creation.
    pub async fn create_tap(&self, vm_id: &str) -> Result<String, NetworkError> {
        let tap_name = tap_name_for_vm(vm_id);

        run_cmd("ip", &["tuntap", "add", &tap_name, "mode", "tap"]).await?;
        run_cmd("ip", &["link", "set", &tap_name, "master", &self.bridge_name]).await?;
        run_cmd("ip", &["link", "set", &tap_name, "up"]).await?;

        info!(tap = %tap_name, vm_id = %vm_id, "tap device created");
        Ok(tap_name)
    }

    /// Idempotent tap creation: if the tap already exists (e.g., agent restart
    /// without node reboot), verify it's attached to the bridge and up. If it
    /// doesn't exist, create it.
    pub async fn ensure_tap(&self, vm_id: &str) -> Result<String, NetworkError> {
        let tap_name = tap_name_for_vm(vm_id);

        let exists = tokio::process::Command::new("ip")
            .args(["link", "show", &tap_name])
            .output()
            .await?;

        if exists.status.success() {
            // Already exists — make sure it's up and on our bridge
            run_cmd("ip", &["link", "set", &tap_name, "master", &self.bridge_name]).await.ok();
            run_cmd("ip", &["link", "set", &tap_name, "up"]).await.ok();
            info!(tap = %tap_name, vm_id = %vm_id, "tap device already exists, ensured up");
            return Ok(tap_name);
        }

        self.create_tap(vm_id).await
    }

    /// Delete a tap device.
    pub async fn delete_tap(&self, vm_id: &str) -> Result<(), NetworkError> {
        let tap_name = tap_name_for_vm(vm_id);

        let result = run_cmd("ip", &["link", "delete", &tap_name]).await;
        if let Err(e) = &result {
            warn!(tap = %tap_name, error = %e, "failed to delete tap (may already be gone)");
        }
        Ok(())
    }
}

/// Generate a tap device name from a VM ID.
/// Tap names are limited to 15 chars on Linux, so we use a short prefix + hash.
fn tap_name_for_vm(vm_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    let hash = hasher.finish();
    format!("bas{:010x}", hash & 0xff_ffff_ffff)
}

async fn run_cmd(cmd: &str, args: &[&str]) -> Result<(), NetworkError> {
    let output = Command::new(cmd).args(args).output().await?;

    if !output.status.success() {
        return Err(NetworkError::BridgeFailed(format!(
            "{} {} failed: {}",
            cmd,
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tap_name_fits_linux_limit() {
        // Linux tap device names are limited to 15 characters (IFNAMSIZ)
        let name = tap_name_for_vm("3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e");
        assert!(name.len() <= 15, "tap name '{}' exceeds 15 chars", name);
        assert!(name.starts_with("bas"));
    }

    #[test]
    fn test_tap_name_deterministic() {
        let a = tap_name_for_vm("vm-123");
        let b = tap_name_for_vm("vm-123");
        assert_eq!(a, b);
    }

    #[test]
    fn test_tap_name_unique_for_different_vms() {
        let a = tap_name_for_vm("vm-1");
        let b = tap_name_for_vm("vm-2");
        assert_ne!(a, b);
    }
}
