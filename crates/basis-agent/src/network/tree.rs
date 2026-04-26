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

use std::collections::{BTreeSet, HashMap, HashSet};

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

/// What this host has materialised for a given tree. Tracked so that
/// reconciles can diff and reclaim three kernel resources:
///
///   * the tree's MASQUERADE rule (keyed on `cidr`)
///   * one IP route per externally-advertised prefix (apiserver VIPs
///     are /32, Cilium Service blocks are /N where N<32)
///   * one proxy-ARP entry per *host address* in those prefixes, so
///     the LAN can resolve any IP in the block to this host's MAC
///
/// Routes and proxy-ARP entries are kept as separate sets because the
/// kernel resources are different (one per prefix vs. one per
/// address) — when a cluster's Service block grows from /28 to /27,
/// the route swap is one op but proxy-ARP needs 16 new entries.
#[derive(Debug, Default)]
struct TreeLive {
    /// Tree CIDR a MASQUERADE rule was installed for, if any. `None`
    /// means bootstrap-only (cold-boot before reconcile landed).
    cidr: Option<String>,
    /// Externally-advertised prefixes (`<addr>/<prefix>` strings,
    /// `<prefix>` ∈ [0..32]) for which we've installed an
    /// `ip route ... dev brt<vni>` override. The override is required
    /// because the prefix lives on a LAN pool — without a
    /// more-specific route the kernel treats it as connected on the
    /// underlay and ARP times out.
    external_prefixes: BTreeSet<String>,
    /// Individual host addresses (`<addr>` strings) for which we've
    /// installed `ip neigh proxy ... dev <uplink>` so LAN clients
    /// resolve the IP to this host's MAC. Disjoint from
    /// `external_prefixes` because proxy entries are per-address.
    proxy_arp_addrs: BTreeSet<String>,
}

pub struct TreeManager {
    vtep_address: String,
    uplink_mtu: u32,
    /// Name of the uplink bridge used as the `-o` interface in the
    /// per-tree MASQUERADE rules installed by [`ensure_tree_inner`].
    uplink_bridge: String,
    /// VNIs currently materialised on this host plus the per-tree
    /// state we've programmed (CIDR for MASQUERADE, VIP routes for
    /// LAN ingress). `reconcile` diffs keys against the desired set
    /// for teardown.
    live: Mutex<HashMap<u32, TreeLive>>,
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

    pub fn inner_mtu(&self) -> u32 {
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
            // Reconcile externally-routable cluster prefixes. Two
            // kernel resources, two diffs:
            //   1. `ip route replace <prefix> dev brt<vni>` per prefix
            //      — so LAN-incoming packets for any IP in the prefix
            //      get forwarded onto the tree bridge.
            //   2. `ip neigh replace proxy <addr> dev <uplink>` per
            //      host address in the prefix — so LAN clients ARPing
            //      get this host's MAC. With multiple hosts carrying
            //      the tree, all proxy-ARP and the LAN naturally
            //      spreads ingress across them.
            // Both ops are idempotent on `replace`.
            let bridge = bridge_name(tree.vni);
            let (desired_prefixes, desired_addrs) = expand_prefixes(&tree.cluster_vips);
            let prev = live.entry(tree.vni).or_default();
            for stale in prev.external_prefixes.difference(&desired_prefixes) {
                if let Err(e) = del_prefix_route(stale, &bridge).await {
                    warn!(prefix = %stale, vni = tree.vni, error = %e, "prefix route del");
                }
            }
            for stale in prev.proxy_arp_addrs.difference(&desired_addrs) {
                if let Err(e) = del_proxy_arp(stale, &self.uplink_bridge).await {
                    warn!(addr = %stale, vni = tree.vni, error = %e, "proxy-arp del");
                }
            }
            for new in desired_prefixes.difference(&prev.external_prefixes) {
                add_prefix_route(new, &bridge).await?;
            }
            for new in desired_addrs.difference(&prev.proxy_arp_addrs) {
                add_proxy_arp(new, &self.uplink_bridge).await?;
            }
            prev.cidr = cidr;
            prev.external_prefixes = desired_prefixes;
            prev.proxy_arp_addrs = desired_addrs;
        }
        let stale: Vec<(u32, TreeLive)> = live
            .iter()
            .filter(|(vni, _)| !desired_vnis.contains(vni))
            .map(|(vni, l)| {
                (
                    *vni,
                    TreeLive {
                        cidr: l.cidr.clone(),
                        external_prefixes: l.external_prefixes.clone(),
                        proxy_arp_addrs: l.proxy_arp_addrs.clone(),
                    },
                )
            })
            .collect();
        for (vni, l) in stale {
            self.remove_tree(vni, &l).await;
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
    /// controller reconcile lands. Gateway IP, MASQUERADE rule, and
    /// VIP routes arrive with the first reconcile.
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
        self.live.lock().await.entry(vni).or_default();
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
            // Learning enabled so peer hosts pick up Cilium's leader
            // gARP via BUM flood — bridges across all hosts in the
            // tree end up with the same VIP→VTEP FDB entry without
            // basis having to track per-cluster leader state. Safe
            // because the FORWARD-chain UDP/4789 drop in
            // `ensure_vxlan_spoof_guard` blocks tenant VMs from
            // forging cross-tree VXLAN frames.
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

    /// Best-effort teardown of the tree's dataplane. Drop the VIP /32
    /// routes (kernel auto-removes them when the bridge link goes, but
    /// be explicit so the live-map state matches), remove the
    /// MASQUERADE rule if we installed one, then delete the VXLAN +
    /// bridge. Missing devices are the desired state — we log and
    /// move on.
    async fn remove_tree(&self, vni: u32, prev: &TreeLive) {
        let bridge = bridge_name(vni);
        for prefix in &prev.external_prefixes {
            if let Err(e) = del_prefix_route(prefix, &bridge).await {
                warn!(prefix = %prefix, vni, error = %e, "prefix route del on teardown");
            }
        }
        for addr in &prev.proxy_arp_addrs {
            if let Err(e) = del_proxy_arp(addr, &self.uplink_bridge).await {
                warn!(addr = %addr, vni, error = %e, "proxy-arp del on teardown");
            }
        }
        if let Some(cidr) = prev.cidr.as_deref() {
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

/// Expand a list of CIDR strings (controller-supplied
/// `cluster_vips`) into:
///   * the prefix set we'll install routes for (one entry per CIDR)
///   * the host-address set we'll install proxy-ARP entries for
///     (each prefix expanded via `ipnet::Ipv4Net::hosts()`, which
///     yields the single addr for /32 and skips network/broadcast for
///     non-/32). Unparseable entries are dropped with a warn so a
///     malformed controller advertisement doesn't take down the
///     reconciler.
fn expand_prefixes(prefixes: &[String]) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut prefix_set = BTreeSet::new();
    let mut addr_set = BTreeSet::new();
    for s in prefixes {
        match s.parse::<ipnet::Ipv4Net>() {
            Ok(net) => {
                prefix_set.insert(s.clone());
                for addr in net.hosts() {
                    addr_set.insert(addr.to_string());
                }
            }
            Err(e) => {
                warn!(prefix = %s, error = %e, "skipping unparseable cluster prefix");
            }
        }
    }
    (prefix_set, addr_set)
}

/// Install a more-specific `<prefix> dev <bridge>` route so the
/// kernel forwards LAN-incoming packets for any address in the
/// prefix onto the tree bridge (where the FDB — populated by
/// kube-vip / Cilium gratuitous ARP — delivers them to the right VM)
/// instead of treating the destination as connected on the underlay.
async fn add_prefix_route(prefix: &str, bridge: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["route", "replace", prefix, "dev", bridge]).await
}

async fn del_prefix_route(prefix: &str, bridge: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["route", "del", prefix, "dev", bridge]).await
}

/// Make this host answer ARP for `addr` on the underlay. `replace`
/// keeps it idempotent across reconciles.
async fn add_proxy_arp(addr: &str, uplink: &str) -> Result<(), NetworkError> {
    run_cmd(
        "ip",
        &["neigh", "replace", "proxy", addr, "dev", uplink],
    )
    .await
}

async fn del_proxy_arp(addr: &str, uplink: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["neigh", "del", "proxy", addr, "dev", uplink]).await
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
