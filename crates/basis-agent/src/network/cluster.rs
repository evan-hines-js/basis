//! Per-cluster dataplane: one Linux bridge + one VXLAN device per
//! cluster this host carries, plus a source-scoped MASQUERADE rule
//! that NATs the cluster's CIDR out the uplink.
//!
//! Naming derives from the VNI:
//!   * bridge: `brc<vni>`
//!   * VXLAN:  `vxc<vni>`
//!
//! `ReconcileHostCommand.clusters` from the controller is
//! authoritative — bridges + FDBs converge on it. VXLAN has learning
//! disabled so a misbehaving guest can't poison another host's
//! forwarding table; BUM entries come exclusively from the
//! controller's peer list.

use std::collections::{BTreeSet, HashMap, HashSet};

use basis_proto::ClusterState;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::warn;

use std::collections::hash_map::Entry;

use super::{
    ensure_cluster_masquerade, ensure_tap_on_bridge, primary_tap_name, remove_cluster_masquerade,
    run_cmd, NetworkError, VXLAN_OVERHEAD,
};

const BRIDGE_PREFIX: &str = "brc";
const VXLAN_PREFIX: &str = "vxc";
const VXLAN_PORT: u16 = 4789;

pub fn bridge_name(vni: u32) -> String {
    format!("{BRIDGE_PREFIX}{vni}")
}

pub fn vxlan_name(vni: u32) -> String {
    format!("{VXLAN_PREFIX}{vni}")
}

/// What this host has materialised for a given cluster. Tracks the
/// per-cluster contributions so that on tombstone we can subtract
/// just this cluster's prefix routes + proxy-ARP entries rather than
/// rediscovering them from kernel state (which would conflate
/// contributions from other clusters on the same uplink).
///
/// The `cidr` field also lets a stale-bridge recovery path (kernel
/// has `brc<vni>` but we don't yet) read the masquerade CIDR back
/// off the bridge's own address. For brand-new clusters it's filled
/// from `ClusterState.cidr` each reconcile.
#[derive(Debug, Default, Clone)]
struct ClusterLive {
    cidr: Option<String>,
    prefixes: BTreeSet<String>,
    proxy_arp_addrs: BTreeSet<String>,
}

pub struct ClusterManager {
    vtep_address: String,
    uplink_mtu: u32,
    /// Name of the uplink bridge used as the `-o` interface in the
    /// per-cluster MASQUERADE rules installed by [`Self::ensure_cluster_inner`].
    uplink_bridge: String,
    /// VNIs currently materialised on this host plus the per-cluster
    /// state we've programmed (CIDR for MASQUERADE, VIP routes for
    /// LAN ingress). `reconcile` diffs keys against the desired set
    /// for teardown.
    live: Mutex<HashMap<u32, ClusterLive>>,
}

impl ClusterManager {
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

    /// Snapshot of every cluster the agent currently has materialised
    /// on this host: the live `(vni, cidr)` pairs from in-memory state,
    /// merged with any `brc<vni>` bridges the kernel still has but the
    /// agent doesn't yet know about (e.g. on a fresh process startup).
    /// Drives the inventory the agent sends in `RegisterHostRequest`.
    pub async fn inventory(&self) -> Result<Vec<(u32, String)>, NetworkError> {
        let live = self.live.lock().await;
        let mut out: HashMap<u32, String> = live
            .iter()
            .filter_map(|(vni, c)| c.cidr.as_ref().map(|cidr| (*vni, cidr.clone())))
            .collect();
        for vni in list_kernel_cluster_vnis().await? {
            if let std::collections::hash_map::Entry::Vacant(v) = out.entry(vni) {
                if let Some(cidr) = read_bridge_cluster_cidr(&bridge_name(vni)).await? {
                    v.insert(cidr);
                }
            }
        }
        let mut ordered: Vec<(u32, String)> = out.into_iter().collect();
        ordered.sort_by_key(|(vni, _)| *vni);
        Ok(ordered)
    }

    /// Apply the controller's authoritative cluster list — additively.
    ///
    /// For every cluster in `desired`: ensure bridge + VXLAN + MASQUERADE
    /// + FDB + the cluster's contribution to per-bridge prefix routes
    /// and uplink proxy-ARP. **Absence is not an intent signal** — a
    /// VNI present in `live` but missing from `desired` is left alone;
    /// teardown only happens via `tombstone_cluster`. This is what makes
    /// a transient empty `desired` (CAPI churn, controller race) safe:
    /// no host state is removed without an explicit tombstone.
    ///
    /// Per-cluster contributions are tracked in `ClusterLive` so:
    ///   1. The per-bridge prefix-route diff sees only THIS cluster's
    ///      previous contribution as `current`, so re-allocations
    ///      within the cluster (LB block re-carve) clean up cleanly,
    ///      but contributions from a sibling cluster on a different
    ///      bridge are never touched.
    ///   2. The proxy-ARP global diff is computed by union over all
    ///      live clusters' contributions — `tombstone_cluster` later
    ///      subtracts out its own contribution and the next reconcile
    ///      hits the diff cleanly.
    pub async fn reconcile(&self, desired: &[ClusterState]) -> Result<(), NetworkError> {
        let mut live = self.live.lock().await;

        // Recover from a fresh process: any `brc<vni>` bridge on the
        // kernel that we aren't tracking gets seeded with its own cidr
        // (read off the bridge's IP) so subsequent tombstones can
        // remove the masquerade rule. Per-cluster prefix/proxy-arp
        // contributions are seeded empty — on the next reconcile pass
        // for that cluster, `desired` repopulates them and the kernel
        // diff converges.
        for vni in list_kernel_cluster_vnis().await? {
            if let Entry::Vacant(v) = live.entry(vni) {
                let cidr = read_bridge_cluster_cidr(&bridge_name(vni)).await?;
                v.insert(ClusterLive {
                    cidr,
                    prefixes: BTreeSet::new(),
                    proxy_arp_addrs: BTreeSet::new(),
                });
            }
        }

        for cluster in desired {
            self.ensure_cluster_inner(cluster).await?;

            // Compute this cluster's desired contributions:
            //   * `cluster_vips` — Lan-scoped: bridge route AND proxy-ARP
            //     onto the uplink (so LAN clients can reach the VIP).
            //   * `internal_cluster_vips` — Tree-scoped: bridge route only,
            //     no proxy-ARP (the LAN must not reach the VIP).
            let (lan_prefixes, lan_addrs) = expand_prefixes(&cluster.cluster_vips);
            let (tree_prefixes, _) = expand_prefixes(&cluster.internal_cluster_vips);
            let mut want_prefixes = lan_prefixes;
            want_prefixes.extend(tree_prefixes);

            // Per-bridge prefix-route diff scoped to this cluster.
            // Reading the kernel up-front catches stale entries
            // (re-carved LB block) without ever touching another
            // cluster's bridge.
            let bridge = bridge_name(cluster.vni);
            let kernel_prefixes = list_kernel_prefix_routes(&bridge).await?;
            for stale in kernel_prefixes.difference(&want_prefixes) {
                if let Err(e) = del_prefix_route(stale, &bridge).await {
                    warn!(prefix = %stale, vni = cluster.vni, error = %e, "prefix route del");
                }
            }
            for new in want_prefixes.difference(&kernel_prefixes) {
                add_prefix_route(new, &bridge).await?;
            }

            let entry = live.entry(cluster.vni).or_default();
            entry.cidr = if cluster.cidr.is_empty() {
                None
            } else {
                Some(cluster.cidr.clone())
            };
            entry.prefixes = want_prefixes;
            entry.proxy_arp_addrs = lan_addrs;
        }

        // Proxy-ARP is global on the uplink, so the desired set is the
        // union over every live cluster's contribution. Diff against
        // kernel: add anything missing, remove anything that no longer
        // belongs to ANY cluster (e.g. a tombstone subtracted out the
        // last contribution before this reconcile call).
        let desired_proxy_arp: BTreeSet<String> = live
            .values()
            .flat_map(|c| c.proxy_arp_addrs.iter().cloned())
            .collect();
        let current_proxy_arp = list_kernel_proxy_arp(&self.uplink_bridge).await?;
        for stale in current_proxy_arp.difference(&desired_proxy_arp) {
            if let Err(e) = del_proxy_arp(stale, &self.uplink_bridge).await {
                warn!(addr = %stale, error = %e, "proxy-arp del");
            }
        }
        for new in desired_proxy_arp.difference(&current_proxy_arp) {
            add_proxy_arp(new, &self.uplink_bridge).await?;
        }

        Ok(())
    }

    /// Tear down a single cluster: drop its prefix routes (the bridge
    /// going away takes them with it), remove the per-cluster
    /// MASQUERADE rule, delete VXLAN + bridge, then remove the
    /// cluster's proxy-ARP contributions from the uplink. Idempotent —
    /// `ip link delete` on a missing link logs and continues, so a
    /// re-emitted tombstone after a successful teardown is a no-op.
    ///
    /// `cidr` is supplied by the controller (`ClusterTombstone.cidr`)
    /// rather than read from local state so the masquerade removal
    /// works even if the agent's `live` map lost track of the cluster
    /// (e.g. agent restart after the controller already marked the
    /// cluster pending teardown).
    pub async fn tombstone_cluster(&self, vni: u32, cidr: &str) -> Result<(), NetworkError> {
        let mut live = self.live.lock().await;

        // Subtract proxy-ARP contributions BEFORE the bridge teardown
        // so `del_proxy_arp` on entries we own actually fires (kernel
        // accepts the delete regardless, but ordering keeps the diff
        // clean for any concurrent reconcile observer).
        let removed = live.remove(&vni);
        if let Some(entry) = removed.as_ref() {
            for addr in &entry.proxy_arp_addrs {
                // Only remove if no other cluster contributes the same
                // address (shouldn't happen — VIPs are globally unique
                // — but the check is cheap and protects against
                // controller bugs).
                let still_wanted = live
                    .values()
                    .any(|other| other.proxy_arp_addrs.contains(addr));
                if !still_wanted {
                    if let Err(e) = del_proxy_arp(addr, &self.uplink_bridge).await {
                        warn!(addr = %addr, error = %e, "proxy-arp del during tombstone");
                    }
                }
            }
        }

        // Pick the cidr to use for masquerade removal: prefer the
        // controller-supplied value (authoritative), fall back to what
        // we cached locally. Empty in both cases means the rule was
        // never installed (ghost-bootstrap cluster), so skip.
        let masq_cidr = if !cidr.is_empty() {
            Some(cidr.to_string())
        } else {
            removed.and_then(|c| c.cidr)
        };
        if let Some(c) = masq_cidr {
            remove_cluster_masquerade(&c, &self.uplink_bridge).await;
        }

        if let Err(e) = run_cmd("ip", &["link", "delete", &vxlan_name(vni)]).await {
            warn!(vni, error = %e, "tombstone: vxlan delete (already gone?)");
        }
        if let Err(e) = run_cmd("ip", &["link", "delete", &bridge_name(vni)]).await {
            warn!(vni, error = %e, "tombstone: bridge delete (already gone?)");
        }
        Ok(())
    }

    /// Attach a VM's primary TAP to its cluster's bridge. Also
    /// ensures the bridge + VXLAN exist — handles the narrow race
    /// where a fresh host receives `CreateVmCommand` before (or
    /// concurrently with) the controller's first
    /// `ReconcileHostCommand` that would otherwise be the one to
    /// create the bridge. Bootstrap is idempotent; the reconcile will
    /// still land afterward and fill in gateway IP, MASQUERADE rule,
    /// and peer FDB.
    pub async fn attach_vm_primary(&self, vm_id: &str, vni: u32) -> Result<String, NetworkError> {
        self.ensure_bootstrap(vni).await?;
        let tap = primary_tap_name(vm_id);
        ensure_tap_on_bridge(&tap, &bridge_name(vni)).await?;
        Ok(tap)
    }

    /// Cold-boot: ensure the cluster's bridge + VXLAN exist with empty
    /// peer FDB so persisted VMs can re-attach their TAPs before the
    /// controller reconcile lands. Gateway IP, MASQUERADE rule, and
    /// VIP routes arrive with the first reconcile.
    pub async fn ensure_bootstrap(&self, vni: u32) -> Result<(), NetworkError> {
        self.ensure_cluster_inner(&ClusterState {
            cluster_id: String::new(),
            vni,
            gateway_ip: String::new(),
            prefix_len: 0,
            vtep_addresses: Vec::new(),
            cidr: String::new(),
            cluster_vips: Vec::new(),
            internal_cluster_vips: Vec::new(),
        })
        .await?;
        self.live.lock().await.entry(vni).or_default();
        Ok(())
    }

    async fn ensure_cluster_inner(&self, cluster: &ClusterState) -> Result<(), NetworkError> {
        let bridge = bridge_name(cluster.vni);
        let vxlan = vxlan_name(cluster.vni);
        let inner_mtu = self.inner_mtu();

        if !link_exists(&bridge).await? {
            run_cmd("ip", &["link", "add", &bridge, "type", "bridge"]).await?;
        }
        set_mtu(&bridge, inner_mtu).await?;
        run_cmd("ip", &["link", "set", &bridge, "up"]).await?;

        if !link_exists(&vxlan).await? {
            // Learning enabled so peer hosts pick up Cilium's leader
            // gARP via BUM flood — bridges across all hosts in the
            // cluster end up with the same VIP→VTEP FDB entry without
            // basis having to track per-cluster leader state. Safe
            // because the FORWARD-chain UDP/4789 drop in
            // `ensure_vxlan_spoof_guard` blocks tenant VMs from
            // forging cross-cluster VXLAN frames.
            run_cmd(
                "ip",
                &[
                    "link",
                    "add",
                    &vxlan,
                    "type",
                    "vxlan",
                    "id",
                    &cluster.vni.to_string(),
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
        // `<cluster_cidr> dev brc<vni>` in its routing table — VMs
        // use it as default gateway and `ping <vm_ip>` from the host
        // works. Uniqueness across hosts (carved from `bridge_range`
        // by the controller) is what makes cross-host host→VM replies
        // land on the sender and not whichever host happens to be
        // replying.
        if !cluster.gateway_ip.is_empty() && cluster.prefix_len > 0 {
            ensure_bridge_address(&bridge, &cluster.gateway_ip, cluster.prefix_len).await?;
        }

        if !cluster.cidr.is_empty() {
            ensure_cluster_masquerade(&cluster.cidr, &self.uplink_bridge).await?;
        }

        self.reconcile_fdb(&vxlan, &cluster.vtep_addresses).await
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
/// prefix onto the cluster bridge (where the FDB — populated by
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
    run_cmd("ip", &["neigh", "replace", "proxy", addr, "dev", uplink]).await
}

async fn del_proxy_arp(addr: &str, uplink: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["neigh", "del", "proxy", addr, "dev", uplink]).await
}

/// Enumerate every `brc<vni>` bridge currently on the host. Used at
/// reconcile start to rediscover bridges left by a prior agent
/// process so their stale routes/ARP/MASQUERADE get cleaned up.
async fn list_kernel_cluster_vnis() -> Result<Vec<u32>, NetworkError> {
    let mut vnis = Vec::new();
    let entries = match std::fs::read_dir("/sys/class/net") {
        Ok(e) => e,
        Err(_) => return Ok(vnis),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(suffix) = name.strip_prefix(BRIDGE_PREFIX) {
            if let Ok(vni) = suffix.parse::<u32>() {
                vnis.push(vni);
            }
        }
    }
    Ok(vnis)
}

/// List the agent-installed prefix routes attached to `bridge`. The
/// per-cluster connected route (`<cidr> proto kernel scope link src
/// <gateway>`) is filtered out because it isn't an agent install —
/// it's auto-acquired from the bridge's IPv4 address. Bare-address
/// destinations are normalised to `<addr>/32` so the set comparison
/// matches the `<addr>/<prefix>` form produced by `expand_prefixes`.
async fn list_kernel_prefix_routes(bridge: &str) -> Result<BTreeSet<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["route", "show", "dev", bridge])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(BTreeSet::new());
    }
    let mut prefixes = BTreeSet::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if line.contains("proto kernel") {
            continue;
        }
        let Some(first) = line.split_whitespace().next() else {
            continue;
        };
        let normalized = if first.contains('/') {
            first.to_string()
        } else {
            format!("{first}/32")
        };
        prefixes.insert(normalized);
    }
    Ok(prefixes)
}

/// List every proxy-ARP entry on the uplink. Reconciled against the
/// union of all clusters' desired prefix-host addresses.
async fn list_kernel_proxy_arp(uplink: &str) -> Result<BTreeSet<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["neigh", "show", "proxy", "dev", uplink])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(BTreeSet::new());
    }
    let mut addrs = BTreeSet::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if let Some(first) = line.split_whitespace().next() {
            addrs.insert(first.to_string());
        }
    }
    Ok(addrs)
}

/// Read the cluster CIDR back from the bridge's IPv4 address. Used
/// when reconcile rediscovers a stale `brc<vni>` after agent restart
/// — the cluster's CIDR is needed to remove its MASQUERADE rule.
/// Returns `None` if the bridge has no IPv4 address (bootstrap-only).
async fn read_bridge_cluster_cidr(bridge: &str) -> Result<Option<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["-4", "addr", "show", "dev", bridge])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(None);
    }
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut parts = line.split_whitespace();
        if parts.next() != Some("inet") {
            continue;
        }
        let Some(addr) = parts.next() else { continue };
        if let Ok(net) = addr.parse::<ipnet::Ipv4Net>() {
            return Ok(Some(format!("{}/{}", net.network(), net.prefix_len())));
        }
    }
    Ok(None)
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

    /// `/32` is the apiserver-VIP shape: one prefix, one host
    /// address. Proxy-ARP for the address makes the LAN reach it.
    #[test]
    fn expand_prefixes_slash_32_yields_one_addr() {
        let (prefixes, addrs) = expand_prefixes(&["10.0.0.5/32".to_string()]);
        assert_eq!(prefixes.len(), 1);
        assert!(prefixes.contains("10.0.0.5/32"));
        assert_eq!(addrs.len(), 1);
        assert!(addrs.contains("10.0.0.5"));
    }

    /// `/28` is the typical LB Service block shape. `Ipv4Net::hosts()`
    /// excludes network and broadcast, so the proxy-ARP set has 14
    /// entries — the LAN-reachable usable addresses.
    #[test]
    fn expand_prefixes_slash_28_yields_fourteen_addrs() {
        let (prefixes, addrs) = expand_prefixes(&["10.0.0.16/28".to_string()]);
        assert_eq!(prefixes.len(), 1);
        assert!(prefixes.contains("10.0.0.16/28"));
        assert_eq!(addrs.len(), 14);
        assert!(addrs.contains("10.0.0.17"));
        assert!(addrs.contains("10.0.0.30"));
        assert!(!addrs.contains("10.0.0.16"));
        assert!(!addrs.contains("10.0.0.31"));
    }

    /// Unparseable entries are dropped (logged at warn). Mixed inputs
    /// keep the good ones — a malformed advertisement can't take down
    /// the entire reconcile pass.
    #[test]
    fn expand_prefixes_drops_unparseable_keeps_rest() {
        let (prefixes, addrs) = expand_prefixes(&[
            "10.0.0.1/32".to_string(),
            "not-a-cidr".to_string(),
            "10.0.0.32/28".to_string(),
        ]);
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains("10.0.0.1/32"));
        assert!(prefixes.contains("10.0.0.32/28"));
        assert_eq!(addrs.len(), 1 + 14);
    }

    /// Empty input is the cold-start case (`ensure_bootstrap` ships an
    /// empty ClusterState before the first controller reconcile lands).
    #[test]
    fn expand_prefixes_empty_input_is_empty_output() {
        let (prefixes, addrs) = expand_prefixes(&[]);
        assert!(prefixes.is_empty());
        assert!(addrs.is_empty());
    }
}
