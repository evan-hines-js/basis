//! Per-tree dataplane: one Linux bridge + one VXLAN device per tree
//! that this host carries. The bridge holds the tree's VM TAPs; the
//! VXLAN device tunnels the tree's L2 to every other hypervisor
//! carrying the same tree.
//!
//! Naming is derived from the VNI — no lookup table needed:
//!
//!   * bridge: `brt<vni>` (e.g. `brt10001`)
//!   * VXLAN:  `vxt<vni>` (e.g. `vxt10001`)
//!
//! At 24 bits the VNI prints as up to 8 digits, plus a 3-char prefix
//! — well under IFNAMSIZ 15.
//!
//! The authoritative source of "which trees should exist on this host"
//! is the controller's `ReconcileHostCommand.trees`. [`TreeManager`]
//! converges local state onto that list: ensures bridges + VXLANs for
//! trees in the list, tears down bridges for trees absent from it,
//! and replaces each tree's FDB entries to match its `vtep_addresses`.
//!
//! VXLAN is configured with learning disabled (`nolearning`). A
//! misbehaving guest cannot poison the FDB on other hosts; every
//! inner-MAC → outer-VTEP mapping comes from the controller via BUM
//! flood entries (one per remote VTEP, stored as the all-zero MAC
//! default entry).

use std::collections::{HashMap, HashSet};

use basis_proto::TreeState;
use tokio::process::Command;
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::{ensure_tap_on_bridge, primary_tap_name, run_cmd, NetworkError, VXLAN_OVERHEAD};

const BRIDGE_PREFIX: &str = "brt";
const VXLAN_PREFIX: &str = "vxt";

/// Default UDP port for VXLAN encapsulation (IANA-assigned).
const VXLAN_PORT: u16 = 4789;

pub fn bridge_name(vni: u32) -> String {
    format!("{BRIDGE_PREFIX}{vni}")
}

pub fn vxlan_name(vni: u32) -> String {
    format!("{VXLAN_PREFIX}{vni}")
}

/// Tree-state cache + dataplane orchestrator. Owns the per-tree
/// bridge/VXLAN lifecycle. One instance per agent process.
pub struct TreeManager {
    /// This host's VTEP address — excluded from peer FDB entries.
    vtep_address: String,
    /// Uplink MTU. Inner MTU for each bridge is this minus
    /// [`VXLAN_OVERHEAD`].
    uplink_mtu: u32,
    /// Last-known state per tree, keyed by VNI. Mutated only by
    /// `reconcile`; readers just peek.
    state: RwLock<HashMap<u32, TreeState>>,
}

impl TreeManager {
    pub fn new(vtep_address: String, uplink_mtu: u32) -> Self {
        Self {
            vtep_address,
            uplink_mtu,
            state: RwLock::new(HashMap::new()),
        }
    }

    /// Inner-frame MTU on every tree bridge. VXLAN eats 50 bytes of
    /// outer header, and guests configure their NIC MTU to match the
    /// bridge through cloud-init.
    pub fn inner_mtu(&self) -> u32 {
        self.uplink_mtu - VXLAN_OVERHEAD
    }

    /// Apply the controller's authoritative tree list for this host.
    /// After this returns, every tree in `desired` has a bridge +
    /// VXLAN + matching FDB, and no extras exist.
    pub async fn reconcile(&self, desired: &[TreeState]) -> Result<(), NetworkError> {
        let mut state = self.state.write().await;

        let desired_vnis: HashSet<u32> = desired.iter().map(|t| t.vni).collect();

        // 1. Ensure every desired tree.
        for tree in desired {
            self.ensure_tree_inner(tree).await?;
            state.insert(tree.vni, tree.clone());
        }

        // 2. Drop trees we no longer need.
        let stale_vnis: Vec<u32> = state
            .keys()
            .copied()
            .filter(|v| !desired_vnis.contains(v))
            .collect();
        for vni in stale_vnis {
            remove_tree(vni).await?;
            state.remove(&vni);
        }

        Ok(())
    }

    /// Attach a VM's primary TAP to its tree's bridge. Fails if the
    /// tree isn't in the cache yet — the controller must push a
    /// `ReconcileHostCommand` naming the tree before dispatching the
    /// CreateVm, and mpsc preserves that order.
    pub async fn attach_vm_primary(&self, vm_id: &str, vni: u32) -> Result<String, NetworkError> {
        let state = self.state.read().await;
        if !state.contains_key(&vni) {
            return Err(NetworkError::BridgeFailed(format!(
                "tree vni={vni} not present on this host — \
                 missed a ReconcileHostCommand before CreateVm"
            )));
        }
        drop(state);
        let tap = primary_tap_name(vm_id);
        let bridge = bridge_name(vni);
        ensure_tap_on_bridge(&tap, &bridge).await?;
        info!(tap = %tap, vm_id, vni, "primary tap attached to tree bridge");
        Ok(tap)
    }

    /// Cold-boot bootstrap: ensure the tree's bridge + VXLAN exist
    /// with *empty* peer FDB so a VM persisted in the agent DB can
    /// have its TAP re-attached before the controller reconcile
    /// lands. Cross-host traffic won't work until the controller
    /// reconcile fills in peers, but intra-host traffic is up and the
    /// guest's virtio-net worker comes online.
    pub async fn ensure_bootstrap(&self, vni: u32) -> Result<(), NetworkError> {
        let tree = TreeState {
            tree_id: String::new(),
            vni,
            cidr: String::new(),
            gateway_ip: String::new(),
            prefix_len: 0,
            vtep_addresses: Vec::new(),
        };
        self.ensure_tree_inner(&tree).await?;
        self.state.write().await.insert(vni, tree);
        Ok(())
    }

    async fn ensure_tree_inner(&self, tree: &TreeState) -> Result<(), NetworkError> {
        let bridge = bridge_name(tree.vni);
        let vxlan = vxlan_name(tree.vni);
        let inner_mtu = self.inner_mtu();

        if !link_exists(&bridge).await? {
            run_cmd("ip", &["link", "add", &bridge, "type", "bridge"]).await?;
        }
        set_mtu(&bridge, inner_mtu).await?;
        run_cmd("ip", &["link", "set", &bridge, "up"]).await?;

        if !link_exists(&vxlan).await? {
            // `dstport 4789 nolearning` — no FDB learning, so a guest
            // sending a spoofed source MAC can't poison our forwarding
            // tables. `local <vtep>` pins the outer source address.
            run_cmd(
                "ip",
                &[
                    "link",
                    "add",
                    &vxlan,
                    "type",
                    "vxlan",
                    "id",
                    &tree.vni.to_string(),
                    "dstport",
                    &VXLAN_PORT.to_string(),
                    "local",
                    &self.vtep_address,
                    "nolearning",
                ],
            )
            .await?;
            run_cmd("ip", &["link", "set", &vxlan, "master", &bridge]).await?;
        }
        set_mtu(&vxlan, inner_mtu).await?;
        run_cmd("ip", &["link", "set", &vxlan, "up"]).await?;

        self.reconcile_fdb(&vxlan, &tree.vtep_addresses).await?;

        Ok(())
    }

    /// Replace BUM FDB entries on `vxlan` to match exactly `peers`
    /// (minus our own address). Idempotent — always converges, never
    /// additive.
    async fn reconcile_fdb(&self, vxlan: &str, peers: &[String]) -> Result<(), NetworkError> {
        let desired: HashSet<String> = peers
            .iter()
            .filter(|p| p != &&self.vtep_address && !p.is_empty())
            .cloned()
            .collect();

        let current = list_fdb_bum_dsts(vxlan).await?;

        for stale in current.difference(&desired) {
            let _ = run_cmd(
                "bridge",
                &["fdb", "del", "00:00:00:00:00:00", "dev", vxlan, "dst", stale],
            )
            .await;
        }
        for new in desired.difference(&current) {
            run_cmd(
                "bridge",
                &["fdb", "append", "00:00:00:00:00:00", "dev", vxlan, "dst", new],
            )
            .await?;
        }
        Ok(())
    }
}

/// Tear down a tree's bridge + VXLAN. Idempotent.
pub async fn remove_tree(vni: u32) -> Result<(), NetworkError> {
    let bridge = bridge_name(vni);
    let vxlan = vxlan_name(vni);
    // Best-effort delete: a missing device is the state we want.
    if let Err(e) = run_cmd("ip", &["link", "delete", &vxlan]).await {
        warn!(vxlan = %vxlan, error = %e, "vxlan delete (may already be gone)");
    }
    if let Err(e) = run_cmd("ip", &["link", "delete", &bridge]).await {
        warn!(bridge = %bridge, error = %e, "bridge delete (may already be gone)");
    }
    Ok(())
}

async fn link_exists(name: &str) -> Result<bool, NetworkError> {
    let out = Command::new("ip").args(["link", "show", name]).output().await?;
    Ok(out.status.success())
}

async fn set_mtu(name: &str, mtu: u32) -> Result<(), NetworkError> {
    run_cmd("ip", &["link", "set", name, "mtu", &mtu.to_string()]).await
}

/// Current BUM (all-zero MAC) destinations for a VXLAN device.
async fn list_fdb_bum_dsts(vxlan: &str) -> Result<HashSet<String>, NetworkError> {
    let out = Command::new("bridge")
        .args(["fdb", "show", "dev", vxlan])
        .output()
        .await?;
    if !out.status.success() {
        return Err(NetworkError::BridgeFailed(format!(
            "bridge fdb show dev {vxlan}: {}",
            String::from_utf8_lossy(&out.stderr)
        )));
    }
    let mut dsts = HashSet::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        // Lines look like: "00:00:00:00:00:00 dst 10.100.0.2 self permanent"
        if !line.starts_with("00:00:00:00:00:00") {
            continue;
        }
        let mut parts = line.split_whitespace();
        while let Some(tok) = parts.next() {
            if tok == "dst" {
                if let Some(ip) = parts.next() {
                    dsts.insert(ip.to_string());
                }
                break;
            }
        }
    }
    Ok(dsts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_fit_ifnamsiz() {
        for vni in [1u32, 10_000, 16_777_215] {
            assert!(bridge_name(vni).len() <= 15);
            assert!(vxlan_name(vni).len() <= 15);
        }
    }

    #[test]
    fn names_are_unique_across_vnis() {
        assert_ne!(bridge_name(1), bridge_name(2));
        assert_ne!(vxlan_name(1), vxlan_name(2));
        assert_ne!(bridge_name(1), vxlan_name(1));
    }

    #[test]
    fn inner_mtu_subtracts_encap_overhead() {
        let mgr = TreeManager::new("10.100.0.1".to_string(), 9000);
        assert_eq!(mgr.inner_mtu(), 9000 - VXLAN_OVERHEAD);
    }
}
