//! Host-network plumbing for VM guests.
//!
//! Every VM has one or two taps:
//!
//!   * **Primary tap** (`bas<hash>`): attached to the tree bridge
//!     (`brt<vni>`). The tree bridge has a VXLAN slave (`vxt<vni>`)
//!     that tunnels the tree's L2 to every other hypervisor carrying
//!     the same tree. This is the only L2 path the tree sees —
//!     physical uplink is not a bridge slave.
//!   * **Edge tap** (`bae<hash>`): optional, only when the VM is
//!     flagged `edge: true`. Attached to the uplink bridge
//!     (`basis0`), which masters the physical NIC. Gives the VM a
//!     second NIC on the underlay for Cilium BGP / LB ingress / pod
//!     egress.
//!
//! Tap names hash the vm_id (one-way, deterministic) to stay inside
//! IFNAMSIZ = 15 chars while being stable across restarts. The
//! one-way hash is fine because orphan sweeps reconstruct the
//! expected set from known vm_ids instead of reversing a tap name.
//!
//! Fail-fast philosophy:
//!   * [`UplinkBridge::validate`] runs at startup and refuses to
//!     continue if the configured physical NIC is missing, the
//!     bridge is held by something we didn't create, or the MTU is
//!     too small for VXLAN encapsulation.
//!   * Attach paths propagate real failures — a TAP that isn't on
//!     its bridge is a VM with no network, and a VM reporting
//!     Running with no network is the worst possible failure mode.
//!   * Delete paths are best-effort: a missing tap is the state we
//!     wanted anyway, so we log and continue.
//!
//! All commands go through `iproute2` via `tokio::process::Command`.
//! No netlink bindings — one fork+exec per operation, negligible
//! next to VM-create latency and trivial to trace.

pub mod tree;

pub use tree::TreeManager;

use basis_proto::TreeState;

/// One-stop network handle for the agent. Wraps the uplink bridge
/// (edge NICs live here) and the per-tree bridge + VXLAN manager
/// (every other VM NIC lives there). Everything above basis-agent
/// talks through this single type so call sites aren't juggling two
/// managers.
pub struct NetworkManager {
    uplink: UplinkBridge,
    trees: TreeManager,
}

impl NetworkManager {
    pub fn new(uplink: UplinkBridge, trees: TreeManager) -> Self {
        Self { uplink, trees }
    }

    pub fn uplink_bridge_name(&self) -> &str {
        self.uplink.bridge_name()
    }

    pub async fn validate_uplink(&self) -> Result<(), NetworkError> {
        self.uplink.validate().await
    }

    pub async fn ensure_uplink_bridge(&self) -> Result<(), NetworkError> {
        self.uplink.ensure().await
    }

    /// Converge local tree bridges + VXLAN devices + FDB entries onto
    /// the controller's authoritative list.
    pub async fn reconcile_trees(&self, desired: &[TreeState]) -> Result<(), NetworkError> {
        self.trees.reconcile(desired).await
    }

    /// Cold-boot bridge seeding: before the controller connects, an
    /// agent with persisted VMs needs their tree bridges up so the
    /// VMs can re-attach TAPs. Peer FDBs remain empty until the
    /// first `reconcile_trees` from the controller.
    pub async fn ensure_bootstrap_tree(&self, vni: u32) -> Result<(), NetworkError> {
        self.trees.ensure_bootstrap(vni).await
    }

    /// Create a VM's primary TAP on its tree's bridge and return the
    /// tap name. Fails if the tree hasn't been reconciled yet on this
    /// host — the controller must send a `ReconcileHostCommand` naming
    /// the tree before dispatching the VM's `CreateVmCommand`.
    pub async fn attach_vm_primary(&self, vm_id: &str, vni: u32) -> Result<String, NetworkError> {
        self.trees.attach_vm_primary(vm_id, vni).await
    }

    /// Create a VM's edge TAP on the uplink bridge and return the
    /// tap name. Only called for machines where `edge: true`.
    pub async fn attach_vm_edge(&self, vm_id: &str) -> Result<String, NetworkError> {
        self.uplink.attach_edge_tap(vm_id).await
    }

    /// Best-effort delete of both primary and edge TAPs for a VM.
    /// A missing TAP (never allocated, or already gone) is the state
    /// we want and is not an error.
    pub async fn detach_vm_taps(&self, vm_id: &str) {
        let _ = delete_tap_by_name(&primary_tap_name(vm_id)).await;
        let _ = delete_tap_by_name(&edge_tap_name(vm_id)).await;
    }

    /// Enumerate every agent-managed TAP on the host (both primary
    /// and edge). Used by the orphan sweep.
    pub async fn list_agent_taps(&self) -> Result<Vec<String>, NetworkError> {
        list_agent_taps().await
    }

    /// Delete an arbitrary TAP by interface name. Used by the orphan
    /// sweep when it has a tap name from `list_agent_taps` but no
    /// vm_id (tap names are one-way hashes).
    pub async fn delete_tap_by_name(&self, name: &str) -> Result<(), NetworkError> {
        delete_tap_by_name(name).await
    }
}

use tokio::process::Command;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("bridge setup failed: {0}")]
    BridgeFailed(String),

    #[error(
        "physical NIC '{nic}' not found — set spec.network.uplinkInterface in host.yaml to an \
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

    #[error(
        "uplink MTU {actual} is too small to carry standard 1500-byte frames after \
         {VXLAN_OVERHEAD}-byte VXLAN encap — set the uplink NIC's MTU to at least {} \
         (jumbo frames recommended)",
        VXLAN_MIN_UPLINK_MTU
    )]
    UplinkMtuTooSmall { actual: u32 },

    #[error("command failed: {0}")]
    CommandFailed(#[from] std::io::Error),
}

/// Bytes added to every inner frame by VXLAN encap:
/// 14 (outer eth) + 20 (outer IPv4) + 8 (UDP) + 8 (VXLAN) = 50.
pub const VXLAN_OVERHEAD: u32 = 50;

/// Minimum uplink MTU that can carry a standard 1500-byte inner frame.
pub const VXLAN_MIN_UPLINK_MTU: u32 = 1500 + VXLAN_OVERHEAD;

/// Prefix for the VM's primary TAP (attached to the tree bridge).
pub const PRIMARY_TAP_PREFIX: &str = "bas";

/// Prefix for the VM's edge TAP (attached to the uplink bridge), only
/// when the machine is flagged `edge: true`.
pub const EDGE_TAP_PREFIX: &str = "bae";

/// Deterministic primary TAP name for a VM.
pub fn primary_tap_name(vm_id: &str) -> String {
    format!("{PRIMARY_TAP_PREFIX}{}", vm_id_hash(vm_id))
}

/// Deterministic edge TAP name for a VM.
pub fn edge_tap_name(vm_id: &str) -> String {
    format!("{EDGE_TAP_PREFIX}{}", vm_id_hash(vm_id))
}

fn vm_id_hash(vm_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{:010x}", hash & 0xff_ffff_ffff)
}

/// The host's uplink bridge + physical NIC. One per host. Masters the
/// physical NIC so edge-flagged VMs can get a second tap on the
/// underlay.
pub struct UplinkBridge {
    bridge_name: String,
    physical_nic: String,
    uplink_mtu: u32,
}

impl UplinkBridge {
    pub fn new(bridge_name: String, physical_nic: String, uplink_mtu: u32) -> Self {
        Self {
            bridge_name,
            physical_nic,
            uplink_mtu,
        }
    }

    pub fn bridge_name(&self) -> &str {
        &self.bridge_name
    }

    /// Startup preflight: NIC exists, bridge is ours (or absent), MTU
    /// can carry VXLAN-encapsulated 1500-byte frames.
    pub async fn validate(&self) -> Result<(), NetworkError> {
        if self.uplink_mtu < VXLAN_MIN_UPLINK_MTU {
            return Err(NetworkError::UplinkMtuTooSmall {
                actual: self.uplink_mtu,
            });
        }

        let nic_check = Command::new("ip")
            .args(["link", "show", &self.physical_nic])
            .output()
            .await?;
        if !nic_check.status.success() {
            return Err(NetworkError::PhysicalNicMissing {
                nic: self.physical_nic.clone(),
            });
        }

        // If the bridge already exists with a master that isn't our
        // physical NIC or an agent-managed TAP, we refuse to
        // continue. Primary and edge TAPs share the `ba` prefix.
        let slaves = Command::new("ip")
            .args(["-o", "link", "show", "master", &self.bridge_name])
            .output()
            .await?;
        if slaves.status.success() && !slaves.stdout.is_empty() {
            let text = String::from_utf8_lossy(&slaves.stdout);
            let current: Vec<String> = text
                .lines()
                .filter_map(|l| l.split_whitespace().nth(1))
                .map(|s| s.trim_end_matches(':').trim_end_matches('@').to_string())
                .collect();
            let stranger = current
                .iter()
                .find(|s| !is_agent_managed_tap(s) && s.as_str() != self.physical_nic);
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
            mtu = self.uplink_mtu,
            "uplink preflight passed"
        );
        Ok(())
    }

    /// Create the bridge if missing, attach the physical NIC, bring
    /// both up. Idempotent.
    pub async fn ensure(&self) -> Result<(), NetworkError> {
        let exists = Command::new("ip")
            .args(["link", "show", &self.bridge_name])
            .output()
            .await?;

        if !exists.status.success() {
            run_cmd("ip", &["link", "add", &self.bridge_name, "type", "bridge"]).await?;
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
                "uplink bridge created"
            );
        }
        run_cmd("ip", &["link", "set", &self.bridge_name, "up"]).await?;
        Ok(())
    }

    /// Create an edge TAP for a VM and attach it to the uplink bridge.
    pub async fn attach_edge_tap(&self, vm_id: &str) -> Result<String, NetworkError> {
        let tap = edge_tap_name(vm_id);
        create_and_attach_tap(&tap, &self.bridge_name).await?;
        info!(tap = %tap, vm_id = %vm_id, "edge tap attached");
        Ok(tap)
    }
}

/// True if `name` matches the agent's TAP naming scheme exactly —
/// a 3-char prefix (`bas` or `bae`) followed by 10 hex chars. Prevents
/// us from mistaking non-tap interfaces that happen to share a prefix
/// (e.g. the uplink bridge `basis0`) for agent-managed taps.
fn is_agent_managed_tap(name: &str) -> bool {
    const PREFIX_LEN: usize = 3;
    const HASH_LEN: usize = 10;
    if name.len() != PREFIX_LEN + HASH_LEN {
        return false;
    }
    let (prefix, suffix) = name.split_at(PREFIX_LEN);
    (prefix == PRIMARY_TAP_PREFIX || prefix == EDGE_TAP_PREFIX)
        && suffix.chars().all(|c| c.is_ascii_hexdigit())
}

/// Create a new TAP and attach to the given bridge. Fails if TAP
/// already exists — callers who need idempotence use
/// [`ensure_tap_on_bridge`].
pub(crate) async fn create_and_attach_tap(
    tap: &str,
    bridge: &str,
) -> Result<(), NetworkError> {
    run_cmd("ip", &["tuntap", "add", tap, "mode", "tap"]).await?;
    run_cmd("ip", &["link", "set", tap, "master", bridge]).await?;
    run_cmd("ip", &["link", "set", tap, "up"]).await?;
    Ok(())
}

/// Idempotent attach: create the TAP if missing, (re)master it to the
/// bridge and bring it up if it already exists.
pub(crate) async fn ensure_tap_on_bridge(
    tap: &str,
    bridge: &str,
) -> Result<(), NetworkError> {
    let exists = Command::new("ip")
        .args(["link", "show", tap])
        .output()
        .await?;
    if exists.status.success() {
        run_cmd("ip", &["link", "set", tap, "master", bridge])
            .await
            .map_err(|e| NetworkError::TapInconsistent {
                tap: tap.to_string(),
                reason: format!("re-attach to bridge {bridge}: {e}"),
            })?;
        run_cmd("ip", &["link", "set", tap, "up"])
            .await
            .map_err(|e| NetworkError::TapInconsistent {
                tap: tap.to_string(),
                reason: format!("link up: {e}"),
            })?;
        return Ok(());
    }
    create_and_attach_tap(tap, bridge).await
}

/// Delete a TAP by interface name. Missing TAP is not an error.
pub async fn delete_tap_by_name(name: &str) -> Result<(), NetworkError> {
    if let Err(e) = run_cmd("ip", &["link", "delete", name]).await {
        warn!(tap = %name, error = %e, "failed to delete tap (may already be gone)");
    }
    Ok(())
}

/// Enumerate every agent-managed TAP (primary and edge) on the host.
/// Used by the orphan sweep — the caller diffs against the expected
/// set (primary + edge tap names for every known vm_id) and deletes
/// the difference.
pub async fn list_agent_taps() -> Result<Vec<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["-o", "link", "show", "type", "tuntap"])
        .output()
        .await?;
    if !out.status.success() {
        return Err(NetworkError::BridgeFailed(
            String::from_utf8_lossy(&out.stderr).to_string(),
        ));
    }
    let mut taps = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        // Format: "<idx>: <name>: <flags> ..."
        let Some(name) = line.split_whitespace().nth(1) else {
            continue;
        };
        let name = name.trim_end_matches(':').trim_end_matches('@');
        if is_agent_managed_tap(name) {
            taps.push(name.to_string());
        }
    }
    Ok(taps)
}

pub(crate) async fn run_cmd(cmd: &str, args: &[&str]) -> Result<(), NetworkError> {
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
    fn tap_names_fit_ifnamsiz() {
        let p = primary_tap_name("3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e");
        let e = edge_tap_name("3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e");
        assert!(p.len() <= 15, "primary '{p}' exceeds 15 chars");
        assert!(e.len() <= 15, "edge '{e}' exceeds 15 chars");
        assert!(p.starts_with(PRIMARY_TAP_PREFIX));
        assert!(e.starts_with(EDGE_TAP_PREFIX));
    }

    #[test]
    fn tap_names_deterministic() {
        assert_eq!(primary_tap_name("v1"), primary_tap_name("v1"));
        assert_eq!(edge_tap_name("v1"), edge_tap_name("v1"));
    }

    #[test]
    fn primary_and_edge_are_distinguishable() {
        let vm = "vm-1";
        assert_ne!(primary_tap_name(vm), edge_tap_name(vm));
    }

    #[test]
    fn is_agent_managed_recognizes_both_prefixes() {
        // Real agent taps: 3-char prefix + 10 hex chars = 13 chars.
        let primary = primary_tap_name("vm-1");
        let edge = edge_tap_name("vm-1");
        assert!(is_agent_managed_tap(&primary));
        assert!(is_agent_managed_tap(&edge));
        // Neighbor interfaces must not match: the uplink bridge
        // name `basis0` shares the `bas` prefix, and stray test
        // names that don't match the hash shape must not be
        // picked up by the orphan sweep.
        assert!(!is_agent_managed_tap("eth0"));
        assert!(!is_agent_managed_tap("basis0"));
        assert!(!is_agent_managed_tap("basabc123")); // too short
        assert!(!is_agent_managed_tap("basnonhex000")); // non-hex suffix
    }
}
