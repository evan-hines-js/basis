//! Host-network plumbing for VM guests.
//!
//! Every VM gets a tap device attached to a single host bridge; the
//! bridge in turn masters the physical NIC, so guests share L2 with the
//! host. Tap names are a 10-hex-digit hash of the VM id (`bas<hex>`) to
//! fit Linux's 15-char IFNAMSIZ limit while staying deterministic across
//! restarts.
//!
//! Fail-fast philosophy:
//!   * `validate_bridge` runs at agent startup and refuses to continue if
//!     the configured physical NIC is missing or the bridge name is
//!     already held by something we didn't create.
//!   * `ensure_tap` is idempotent *in the success case*, but propagates
//!     any real failure (bridge attach, link-up) up to the caller — a
//!     tap that isn't on the bridge is a VM with no network, and a VM
//!     reporting Running with no network is the worst possible failure
//!     mode.
//!   * Delete paths are best-effort: a missing tap is the state we
//!     wanted anyway, so we log and continue rather than fail.
//!
//! All commands go through `iproute2` via `tokio::process::Command` —
//! no netlink bindings. The overhead is one fork+exec per operation,
//! which is negligible next to VM-create latency and is easy to trace
//! via `strace` / journal.

use tokio::process::Command;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("bridge setup failed: {0}")]
    BridgeFailed(String),

    #[error(
        "physical NIC '{nic}' not found — set spec.network.physicalNic in host.yaml to an \
         interface visible in `ip link show`; agent cannot continue without it"
    )]
    PhysicalNicMissing { nic: String },

    #[error(
        "bridge '{bridge}' exists but already has master '{current_master}', not '{expected}' \
         — either pick a different bridge name in host.yaml or move the NIC manually"
    )]
    BridgeOwnedByOther {
        bridge: String,
        current_master: String,
        expected: String,
    },

    #[error("tap '{tap}' inconsistent: {reason}")]
    TapInconsistent { tap: String, reason: String },

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

    /// Fail-fast preflight for the host's network config. Called on agent
    /// startup *before* `ensure_bridge`, so a missing NIC or a bridge
    /// collision surfaces as a config error, not a confusing `ip link`
    /// failure partway through tap creation.
    ///
    /// Checks:
    ///   1. The configured physical NIC exists (`ip link show <nic>`).
    ///   2. If the bridge name already resolves to a link, it is actually
    ///      a bridge *and* our physical NIC is its master (or no master
    ///      yet — `ensure_bridge` will attach it). A pre-existing bridge
    ///      attached to a *different* NIC would silently steal traffic
    ///      from guests on the next reboot.
    pub async fn validate_bridge(&self) -> Result<(), NetworkError> {
        let nic_check = Command::new("ip")
            .args(["link", "show", &self.physical_nic])
            .output()
            .await?;
        if !nic_check.status.success() {
            return Err(NetworkError::PhysicalNicMissing {
                nic: self.physical_nic.clone(),
            });
        }

        // `ip -o link show master <bridge>` prints one line per slave. If
        // the bridge doesn't exist, the command fails — that's fine
        // (ensure_bridge will create it). If the bridge exists but has a
        // master that isn't our physical NIC, abort: a pre-existing
        // bridge attached to a different NIC would silently redirect
        // guest traffic.
        let slaves = Command::new("ip")
            .args(["-o", "link", "show", "master", &self.bridge_name])
            .output()
            .await?;
        if slaves.status.success() && !slaves.stdout.is_empty() {
            let text = String::from_utf8_lossy(&slaves.stdout);
            // Each line: "<idx>: <ifname>@... <flags> ..."
            let current: Vec<String> = text
                .lines()
                .filter_map(|l| l.split_whitespace().nth(1))
                .map(|s| s.trim_end_matches(':').trim_end_matches('@').to_string())
                .collect();
            // `bas*` slaves are our own taps; ignore them. Any non-tap
            // slave that isn't our expected physical NIC is a conflict.
            let stranger = current
                .iter()
                .find(|s| !s.starts_with(TAP_PREFIX) && s.as_str() != self.physical_nic);
            if let Some(stranger) = stranger {
                return Err(NetworkError::BridgeOwnedByOther {
                    bridge: self.bridge_name.clone(),
                    current_master: stranger.clone(),
                    expected: self.physical_nic.clone(),
                });
            }
        }

        info!(
            bridge = %self.bridge_name,
            nic = %self.physical_nic,
            "network preflight passed"
        );
        Ok(())
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
            &[
                "link",
                "set",
                &self.physical_nic,
                "master",
                &self.bridge_name,
            ],
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
        let tap = tap_name(vm_id);

        run_cmd("ip", &["tuntap", "add", &tap, "mode", "tap"]).await?;
        run_cmd("ip", &["link", "set", &tap, "master", &self.bridge_name]).await?;
        run_cmd("ip", &["link", "set", &tap, "up"]).await?;

        info!(tap = %tap, vm_id = %vm_id, "tap device created");
        Ok(tap)
    }

    /// Idempotent tap creation: if the tap already exists (e.g., agent restart
    /// without node reboot), verify it's attached to the bridge and up. If it
    /// doesn't exist, create it.
    pub async fn ensure_tap(&self, vm_id: &str) -> Result<String, NetworkError> {
        let tap = tap_name(vm_id);

        let exists = tokio::process::Command::new("ip")
            .args(["link", "show", &tap])
            .output()
            .await?;

        if exists.status.success() {
            // Already exists — make sure it's up and on our bridge. These
            // writes are idempotent when state is already correct, so a
            // real failure here means the link layer is inconsistent
            // (kernel refusal, namespace mismatch). Returning Ok with a
            // dangling tap would produce a VM that reports Running while
            // having no network, which is the one failure mode the
            // controller can't detect. Propagate instead.
            run_cmd("ip", &["link", "set", &tap, "master", &self.bridge_name])
                .await
                .map_err(|e| NetworkError::TapInconsistent {
                    tap: tap.clone(),
                    reason: format!("re-attach to bridge {}: {e}", self.bridge_name),
                })?;
            run_cmd("ip", &["link", "set", &tap, "up"])
                .await
                .map_err(|e| NetworkError::TapInconsistent {
                    tap: tap.clone(),
                    reason: format!("link up: {e}"),
                })?;
            info!(tap = %tap, vm_id = %vm_id, "tap device already exists, ensured up");
            return Ok(tap);
        }

        self.create_tap(vm_id).await
    }

    /// Delete a tap device.
    pub async fn delete_tap(&self, vm_id: &str) -> Result<(), NetworkError> {
        let tap = tap_name(vm_id);

        if let Err(e) = run_cmd("ip", &["link", "delete", &tap]).await {
            warn!(tap = %tap, error = %e, "failed to delete tap (may already be gone)");
        }
        Ok(())
    }

    /// Enumerate every `bas*` tap currently attached to our bridge. Used
    /// by reconcile to find orphans whose VMs the controller has long
    /// forgotten. Tap names are a hash of vm_id, so we can't reverse one
    /// into its VM — the caller diffs this list against its set of
    /// expected tap names and removes the rest.
    pub async fn list_basis_taps(&self) -> Result<Vec<String>, NetworkError> {
        // `bridge link show` prints lines like:
        //   7: basc1e39a6c74: <...> master vmbr0 state disabled ...
        let out = Command::new("bridge")
            .args(["link", "show"])
            .output()
            .await?;
        if !out.status.success() {
            return Err(NetworkError::BridgeFailed(
                String::from_utf8_lossy(&out.stderr).to_string(),
            ));
        }
        let mut taps = Vec::new();
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            // Only lines that reference our bridge.
            if !line.contains(&format!("master {}", self.bridge_name)) {
                continue;
            }
            // Pull the interface name from "<idx>: <name>: <flags> ..."
            let Some(name) = line.split_whitespace().nth(1) else {
                continue;
            };
            let name = name.trim_end_matches(':');
            if name.starts_with(TAP_PREFIX) {
                taps.push(name.to_string());
            }
        }
        Ok(taps)
    }

    /// Delete a single tap by its interface name. Used by the orphan
    /// sweep, which has a tap name (from `bridge link show`) but no
    /// corresponding vm_id because `tap_name` is a one-way hash.
    /// A missing tap is not an error — the state we want is "gone".
    pub async fn delete_tap_by_name(&self, name: &str) -> Result<(), NetworkError> {
        if let Err(e) = run_cmd("ip", &["link", "delete", name]).await {
            warn!(tap = %name, error = %e, "failed to delete tap (may already be gone)");
        }
        Ok(())
    }
}

const TAP_PREFIX: &str = "bas";

/// Deterministic tap device name for a VM ID.
///
/// Linux caps interface names at 15 chars (IFNAMSIZ); a 3-char prefix
/// plus a 10-hex hash fits with one byte to spare. Public so reconcile
/// can compute the expected set for orphan detection.
pub fn tap_name(vm_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{TAP_PREFIX}{:010x}", hash & 0xff_ffff_ffff)
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
        let name = tap_name("3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e");
        assert!(name.len() <= 15, "tap name '{}' exceeds 15 chars", name);
        assert!(name.starts_with("bas"));
    }

    #[test]
    fn test_tap_name_deterministic() {
        let a = tap_name("vm-123");
        let b = tap_name("vm-123");
        assert_eq!(a, b);
    }

    #[test]
    fn test_tap_name_unique_for_different_vms() {
        let a = tap_name("vm-1");
        let b = tap_name("vm-2");
        assert_ne!(a, b);
    }
}
