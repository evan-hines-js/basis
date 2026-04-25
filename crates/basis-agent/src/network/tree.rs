//! Per-tree dataplane: one Linux bridge + one VXLAN device per tree
//! this host carries, plus a source-scoped MASQUERADE rule that NATs
//! the tree's CIDR out the uplink.
//!
//! Naming derives from the VNI:
//!   * bridge: `brt<vni>`
//!   * VXLAN:  `vxt<vni>`
//!
//! `ReconcileHostCommand.trees` from the controller is authoritative —
//! bridges + FDBs converge on it. VXLAN has learning disabled so a
//! misbehaving guest can't poison another host's forwarding table;
//! BUM entries come exclusively from the controller's peer list.

use std::collections::{HashMap, HashSet};

use basis_proto::TreeState;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::warn;

use super::{
    ensure_tap_on_bridge, ensure_tree_masquerade, primary_tap_name, remove_tree_masquerade,
    run_cmd, NetworkError, VXLAN_OVERHEAD,
};

const BRIDGE_PREFIX: &str = "brt";
const VXLAN_PREFIX: &str = "vxt";
const VXLAN_PORT: u16 = 4789;

pub fn bridge_name(vni: u32) -> String {
    format!("{BRIDGE_PREFIX}{vni}")
}

pub fn vxlan_name(vni: u32) -> String {
    format!("{VXLAN_PREFIX}{vni}")
}

pub struct TreeManager {
    vtep_address: String,
    uplink_mtu: u32,
    /// Name of the uplink bridge used as the `-o` interface in the
    /// per-tree MASQUERADE rules installed by [`ensure_tree_inner`].
    uplink_bridge: String,
    /// VNIs currently materialised on this host, each mapped to the
    /// tree CIDR a MASQUERADE rule was installed for (if any). Absence
    /// means no bridge/VXLAN exists; `None` means bootstrap-only and
    /// no NAT rule has been installed yet (cold-boot before reconcile
    /// lands). `reconcile` diffs keys against the desired set for
    /// teardown; `remove_tree` uses the CIDR to remove its rule.
    live: Mutex<HashMap<u32, Option<String>>>,
}

impl TreeManager {
    pub fn new(vtep_address: String, uplink_mtu: u32, uplink_bridge: String) -> Self {
        Self {
            vtep_address,
            uplink_mtu,
            uplink_bridge,
            live: Mutex::new(HashMap::new()),
        }
    }

    fn inner_mtu(&self) -> u32 {
        self.uplink_mtu - VXLAN_OVERHEAD
    }

    /// Apply the controller's authoritative tree list. After this
    /// returns, every tree in `desired` has a bridge + VXLAN +
    /// matching FDB + MASQUERADE rule, and no extras exist.
    pub async fn reconcile(&self, desired: &[TreeState]) -> Result<(), NetworkError> {
        let mut live = self.live.lock().await;

        let desired_vnis: HashSet<u32> = desired.iter().map(|t| t.vni).collect();

        for tree in desired {
            self.ensure_tree_inner(tree).await?;
            let cidr = if tree.cidr.is_empty() {
                None
            } else {
                Some(tree.cidr.clone())
            };
            live.insert(tree.vni, cidr);
        }
        let stale: Vec<(u32, Option<String>)> = live
            .iter()
            .filter(|(vni, _)| !desired_vnis.contains(vni))
            .map(|(vni, cidr)| (*vni, cidr.clone()))
            .collect();
        for (vni, cidr) in stale {
            self.remove_tree(vni, cidr.as_deref()).await;
            live.remove(&vni);
        }
        Ok(())
    }

    /// Attach a VM's primary TAP to its tree's bridge. Also ensures
    /// the bridge + VXLAN exist — handles the narrow race where a
    /// fresh host receives `CreateVmCommand` before (or concurrently
    /// with) the controller's first `ReconcileHostCommand` that
    /// would otherwise be the one to create the bridge. Bootstrap is
    /// idempotent; the reconcile will still land afterward and fill
    /// in gateway IP, MASQUERADE rule, and peer FDB.
    pub async fn attach_vm_primary(&self, vm_id: &str, vni: u32) -> Result<String, NetworkError> {
        self.ensure_bootstrap(vni).await?;
        let tap = primary_tap_name(vm_id);
        ensure_tap_on_bridge(&tap, &bridge_name(vni)).await?;
        Ok(tap)
    }

    /// Cold-boot: ensure the tree's bridge + VXLAN exist with empty
    /// peer FDB so persisted VMs can re-attach their TAPs before the
    /// controller reconcile lands. Gateway IP and MASQUERADE rule
    /// arrive with the first reconcile.
    pub async fn ensure_bootstrap(&self, vni: u32) -> Result<(), NetworkError> {
        self.ensure_tree_inner(&TreeState {
            vni,
            gateway_ip: String::new(),
            prefix_len: 0,
            vtep_addresses: Vec::new(),
            cidr: String::new(),
            cluster_vips: Vec::new(),
        })
        .await?;
        self.live.lock().await.entry(vni).or_insert(None);
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

        // Assign this host's unique bridge IP so the kernel acquires
        // `<tree_cidr> dev brt<vni>` in its routing table — VMs use it
        // as default gateway and `ping <vm_ip>` from the host works.
        // Uniqueness-across-hosts (carved from `bridge_range` by the
        // controller) is what makes cross-host host→VM replies land on
        // the sender and not whichever host happens to be replying.
        if !tree.gateway_ip.is_empty() && tree.prefix_len > 0 {
            ensure_bridge_address(&bridge, &tree.gateway_ip, tree.prefix_len).await?;
        }

        if !tree.cidr.is_empty() {
            ensure_tree_masquerade(&tree.cidr, &self.uplink_bridge).await?;
        }

        self.reconcile_fdb(&vxlan, &tree.vtep_addresses).await
    }

    /// Best-effort teardown of the tree's dataplane. If a MASQUERADE
    /// rule was installed (cidr is known), remove it; drop the VXLAN
    /// and bridge unconditionally. Missing devices are the desired
    /// state — we log and move on.
    async fn remove_tree(&self, vni: u32, cidr: Option<&str>) {
        if let Some(cidr) = cidr {
            remove_tree_masquerade(cidr, &self.uplink_bridge).await;
        }
        if let Err(e) = run_cmd("ip", &["link", "delete", &vxlan_name(vni)]).await {
            warn!(vni, error = %e, "vxlan delete");
        }
        if let Err(e) = run_cmd("ip", &["link", "delete", &bridge_name(vni)]).await {
            warn!(vni, error = %e, "bridge delete");
        }
    }

    /// Converge BUM FDB entries on `vxlan` to exactly match `peers`
    /// (minus our own VTEP).
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
                &[
                    "fdb",
                    "del",
                    "00:00:00:00:00:00",
                    "dev",
                    vxlan,
                    "dst",
                    stale,
                ],
            )
            .await;
        }
        for new in desired.difference(&current) {
            run_cmd(
                "bridge",
                &[
                    "fdb",
                    "append",
                    "00:00:00:00:00:00",
                    "dev",
                    vxlan,
                    "dst",
                    new,
                ],
            )
            .await?;
        }
        Ok(())
    }
}

async fn link_exists(name: &str) -> Result<bool, NetworkError> {
    let out = Command::new("ip")
        .args(["link", "show", name])
        .output()
        .await?;
    Ok(out.status.success())
}

async fn set_mtu(name: &str, mtu: u32) -> Result<(), NetworkError> {
    run_cmd("ip", &["link", "set", name, "mtu", &mtu.to_string()]).await
}

/// Idempotent: ensure `bridge` has exactly `ip/prefix` assigned.
/// `ip addr replace` adds if missing, replaces otherwise.
async fn ensure_bridge_address(bridge: &str, ip: &str, prefix: u32) -> Result<(), NetworkError> {
    run_cmd(
        "ip",
        &["addr", "replace", &format!("{ip}/{prefix}"), "dev", bridge],
    )
    .await
}

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
    fn bridge_and_vxlan_names_differ() {
        assert_ne!(bridge_name(1), vxlan_name(1));
    }
}
