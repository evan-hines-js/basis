//! Per-cluster dataplane: one Linux bridge + one VXLAN device per
//! cluster this host carries, plus a source-scoped MASQUERADE rule
//! that NATs the cluster's CIDR out the uplink.
//!
//! Naming derives from the VNI:
//!   * bridge: `brc<vni>`
//!   * VXLAN:  `vxc<vni>`
//!
//! `ReconcileHostCommand.clusters` from the controller is
//! authoritative — bridges + FDBs converge on it for BUM destinations.
//! VXLAN learning is *enabled* so peer hosts pick up Cilium's leader
//! gARP via BUM flood without basis having to track per-cluster
//! leader state. Tenant VMs can't abuse this to poison another
//! cluster's FDB because the FORWARD-chain UDP/4789 drop in
//! `ensure_vxlan_spoof_guard` blocks forged VXLAN frames at the
//! source host before they ever reach a peer's `vxc<vni>`.

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
const VRF_PREFIX: &str = "bvrf-";
const VXLAN_PORT: u16 = 4789;

pub fn bridge_name(vni: u32) -> String {
    format!("{BRIDGE_PREFIX}{vni}")
}

pub fn vxlan_name(vni: u32) -> String {
    format!("{VXLAN_PREFIX}{vni}")
}

/// One Linux VRF per tree. The tree's `trust_domain` string maps
/// deterministically to a `(vrf_name, table_id)` pair: every host
/// participating in the tree builds the same VRF, so cross-host
/// agreement comes for free without any controller-side coordination
/// table. Different trust_domains hash to different VRFs (collisions
/// would compromise isolation, but with 40 bits of name suffix +
/// 31-bit table id and the cell-scale of trust_domains in single
/// digits, the birthday probability is irrelevant).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeVrf {
    pub name: String,
    pub table_id: u32,
}

/// Deterministic mapping `trust_domain → (vrf_name, table_id)`.
/// FNV-1a-64 hash → bottom 40 bits become the name suffix (10 hex
/// chars, fits IFNAMSIZ-1 = 15 with the `bvrf-` prefix); the bottom
/// 31 bits + 10000 floor become the route table id (avoids the
/// kernel's reserved 253/254/255 and small distro-built-in ranges).
pub fn vrf_for(trust_domain: &str) -> TreeVrf {
    let h = fnv1a_64(trust_domain.as_bytes());
    let name = format!("{VRF_PREFIX}{:010x}", h & 0xff_ffff_ffff);
    let table_id = (((h as u32) & 0x7fff_ffff) % 4_000_000_000) + 10_000;
    TreeVrf { name, table_id }
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

async fn ensure_vrf(vrf: &TreeVrf) -> Result<(), NetworkError> {
    if !link_exists(&vrf.name).await? {
        run_cmd(
            "ip",
            &[
                "link",
                "add",
                &vrf.name,
                "type",
                "vrf",
                "table",
                &vrf.table_id.to_string(),
            ],
        )
        .await?;
    }
    run_cmd("ip", &["link", "set", &vrf.name, "up"]).await
}

async fn enslave_to_vrf(bridge: &str, vrf_name: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["link", "set", bridge, "master", vrf_name]).await
}

/// Delete the VRF iff it has no remaining slaves. Called from the
/// per-cluster tombstone path after the bridge is gone.
async fn delete_vrf_if_unused(vrf_name: &str) -> Result<(), NetworkError> {
    let out = Command::new("ip")
        .args(["-o", "link", "show", "master", vrf_name])
        .output()
        .await?;
    if !out.status.success() {
        // VRF is already gone (concurrent reconcile, manual cleanup).
        return Ok(());
    }
    if out.stdout.iter().any(|c| !c.is_ascii_whitespace()) {
        // Has at least one slave — another tree-cluster bridge is
        // still in this VRF, leave it alone.
        return Ok(());
    }
    if let Err(e) = run_cmd("ip", &["link", "delete", vrf_name]).await {
        warn!(vrf = %vrf_name, error = %e, "vrf delete failed (already gone?)");
    }
    Ok(())
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
///
/// The `vrf` field captures which tree-VRF the cluster's bridge is
/// currently enslaved to. `None` means the cluster's `trust_domain`
/// was empty (LAN-pool cluster) — routes go in the main table.
/// Populated each reconcile from `cluster.trust_domain` so a tombstone
/// later can run `delete_vrf_if_unused` against the right VRF without
/// rereading kernel state.
#[derive(Debug, Default, Clone)]
struct ClusterLive {
    cidr: Option<String>,
    prefixes: BTreeSet<String>,
    proxy_arp_addrs: BTreeSet<String>,
    vrf: Option<TreeVrf>,
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
                    // The recovered bridge may already be enslaved to
                    // a tree VRF; we don't reconstruct that here
                    // because the next reconcile pass for this vni
                    // arrives with `cluster.trust_domain` and
                    // re-enslaves idempotently. A tombstone that
                    // arrives before that reconcile reads the master
                    // from the kernel directly via `read_link_master`.
                    vrf: None,
                });
            }
        }

        for cluster in desired {
            let vrf = self.ensure_cluster_inner(cluster).await?;

            // Compute this cluster's desired contributions:
            //   * `cluster_vips` — Lan-scoped: bridge route AND proxy-ARP
            //     onto the uplink (so LAN clients can reach the VIP).
            //   * `internal_cluster_vips` — Tree-scoped: bridge route only,
            //     no proxy-ARP (the LAN must not reach the VIP).
            let (lan_prefixes, lan_addrs) = expand_prefixes(&cluster.cluster_vips);
            let (tree_prefixes, _) = expand_prefixes(&cluster.internal_cluster_vips);
            let mut want_prefixes = lan_prefixes;
            want_prefixes.extend(tree_prefixes);

            // Per-bridge prefix-route diff scoped to this cluster, in
            // the cluster's VRF table when it has a trust_domain (else
            // the main table). Reading the kernel up-front catches
            // stale entries (re-carved LB block) without ever touching
            // another cluster's bridge or the wrong table.
            let bridge = bridge_name(cluster.vni);
            let table_id = vrf.as_ref().map(|v| v.table_id);
            let kernel_prefixes = list_kernel_prefix_routes(&bridge, table_id).await?;
            for stale in kernel_prefixes.difference(&want_prefixes) {
                if let Err(e) = del_prefix_route(stale, &bridge, table_id).await {
                    warn!(prefix = %stale, vni = cluster.vni, error = %e, "prefix route del");
                }
            }
            for new in want_prefixes.difference(&kernel_prefixes) {
                add_prefix_route(new, &bridge, table_id).await?;
            }

            let entry = live.entry(cluster.vni).or_default();
            entry.cidr = if cluster.cidr.is_empty() {
                None
            } else {
                Some(cluster.cidr.clone())
            };
            entry.prefixes = want_prefixes;
            entry.proxy_arp_addrs = lan_addrs;
            entry.vrf = vrf;
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
    /// MASQUERADE rule, delete VXLAN + bridge, remove the cluster's
    /// proxy-ARP contributions from the uplink, and garbage-collect
    /// the tree VRF if no other cluster on this host shares it.
    /// Idempotent — `ip link delete` on a missing link logs and
    /// continues, so a re-emitted tombstone after a successful
    /// teardown is a no-op.
    ///
    /// `cidr` is supplied by the controller (`ClusterTombstone.cidr`)
    /// rather than read from local state so the masquerade removal
    /// works even if the agent's `live` map lost track of the cluster
    /// (e.g. agent restart after the controller already marked the
    /// cluster pending teardown).
    pub async fn tombstone_cluster(&self, vni: u32, cidr: &str) -> Result<(), NetworkError> {
        let mut live = self.live.lock().await;
        let bridge = bridge_name(vni);

        // Resolve the VRF the bridge belongs to, falling back to the
        // kernel's `master` link when our in-memory state was reset
        // by an agent restart between create and tombstone.
        let removed = live.remove(&vni);
        let vrf_name = match removed.as_ref().and_then(|c| c.vrf.as_ref()) {
            Some(v) => Some(v.name.clone()),
            None => read_link_master(&bridge).await?,
        };

        // Subtract proxy-ARP contributions BEFORE the bridge teardown
        // so `del_proxy_arp` on entries we own actually fires (kernel
        // accepts the delete regardless, but ordering keeps the diff
        // clean for any concurrent reconcile observer).
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
        if let Err(e) = run_cmd("ip", &["link", "delete", &bridge]).await {
            warn!(vni, error = %e, "tombstone: bridge delete (already gone?)");
        }

        if let Some(vrf) = vrf_name {
            if vrf.starts_with(VRF_PREFIX) {
                delete_vrf_if_unused(&vrf).await?;
            }
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
    /// controller reconcile lands. Gateway IP, MASQUERADE rule, VRF
    /// enslavement, and VIP routes arrive with the first reconcile —
    /// the brief window during which the bridge is in the main table
    /// is benign because no addresses or VIP routes exist yet.
    pub async fn ensure_bootstrap(&self, vni: u32) -> Result<(), NetworkError> {
        self.ensure_cluster_inner(&ClusterState {
            cluster_id: String::new(),
            vni,
            gateway_ip: String::new(),
            prefix_len: 0,
            vtep_addresses: Vec::new(),
            cidr: String::new(),
            trust_domain: String::new(),
            cluster_vips: Vec::new(),
            internal_cluster_vips: Vec::new(),
        })
        .await?;
        self.live.lock().await.entry(vni).or_default();
        Ok(())
    }

    /// Ensure every kernel object the cluster needs exists and is in
    /// the right shape. Returns the cluster's [`TreeVrf`] when its
    /// `trust_domain` is non-empty so the caller knows which routing
    /// table to install prefix routes in. Idempotent in every step
    /// (`ip link add` is replaced by an `exists` check; `addr/route
    /// replace` are inherently idempotent; FDB diff converges).
    async fn ensure_cluster_inner(
        &self,
        cluster: &ClusterState,
    ) -> Result<Option<TreeVrf>, NetworkError> {
        let bridge = bridge_name(cluster.vni);
        let vxlan = vxlan_name(cluster.vni);
        let inner_mtu = self.inner_mtu();

        if !link_exists(&bridge).await? {
            run_cmd("ip", &["link", "add", &bridge, "type", "bridge"]).await?;
        }
        set_mtu(&bridge, inner_mtu).await?;
        run_cmd("ip", &["link", "set", &bridge, "up"]).await?;

        // Tree-pool clusters carry a `trust_domain`; we materialise
        // them inside a per-trust-domain Linux VRF so cross-tree
        // packets fail to find a route in the source bridge's table
        // and get dropped by the kernel. LAN-pool clusters have an
        // empty `trust_domain` and run in the main routing table.
        //
        // Enslaving the bridge AFTER `set up` is deliberate: the
        // kernel re-derives the bridge's connected route into the
        // VRF's table on enslave, and we want the bridge already up
        // so that derivation reflects the address we set later.
        let vrf = if cluster.trust_domain.is_empty() {
            None
        } else {
            let v = vrf_for(&cluster.trust_domain);
            ensure_vrf(&v).await?;
            enslave_to_vrf(&bridge, &v.name).await?;
            Some(v)
        };

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

        // Tree-pool reply path. Enslaving the bridge to the VRF
        // moves its connected route (`<cidr> dev brc<vni>`) out of
        // the main table and into the VRF's table — correct for
        // tree-internal lookups. But VM-initiated NAT'd egress
        // returns through the uplink, which lives in the *main*
        // namespace. With no copy of `<cidr>` in main, the kernel
        // can't resolve the un-NAT'd reverse destination on ingress
        // and silently drops every reply. Re-installing the route
        // in main fixes the reverse path without compromising tree
        // isolation: foreign-tree internal VIPs are /32s installed
        // in their own VRF tables and never appear in main, so a
        // packet from one tree's bridge looking up another tree's
        // VIP still misses everywhere and dies.
        if vrf.is_some() && !cluster.cidr.is_empty() {
            run_cmd(
                "ip",
                &[
                    "route", "replace", &cluster.cidr, "dev", &bridge, "table", "main",
                ],
            )
            .await?;
        }

        self.reconcile_fdb(&vxlan, &cluster.vtep_addresses).await?;
        Ok(vrf)
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

/// Read the master device of `link` (e.g. the VRF a bridge is
/// enslaved to) by parsing `ip -o link show <link>`. Returns `None` if
/// the link is not enslaved or doesn't exist. Used by the tombstone
/// path to resolve the tree-VRF after an agent restart wiped the
/// in-memory `ClusterLive` entry.
async fn read_link_master(link: &str) -> Result<Option<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["-o", "link", "show", link])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut tokens = stdout.split_whitespace();
    while let Some(tok) = tokens.next() {
        if tok == "master" {
            return Ok(tokens.next().map(|s| s.to_string()));
        }
    }
    Ok(None)
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
/// kernel forwards packets for any address in the prefix onto the
/// cluster bridge (where the FDB — populated by kube-vip / Cilium
/// gratuitous ARP — delivers them to the right VM) instead of
/// treating the destination as connected on the underlay.
///
/// `table` selects the routing table: `Some(id)` for a tree-VRF's
/// table (so the route only resolves for traffic ingressing through
/// a bridge in the same VRF), `None` for the main table (LAN-pool
/// cluster, no VRF — host kernel and uplink see the route directly).
async fn add_prefix_route(
    prefix: &str,
    bridge: &str,
    table: Option<u32>,
) -> Result<(), NetworkError> {
    let table_str = table.map(|t| t.to_string());
    let mut args: Vec<&str> = vec!["route", "replace", prefix, "dev", bridge];
    if let Some(t) = table_str.as_deref() {
        args.extend_from_slice(&["table", t]);
    }
    run_cmd("ip", &args).await
}

async fn del_prefix_route(
    prefix: &str,
    bridge: &str,
    table: Option<u32>,
) -> Result<(), NetworkError> {
    let table_str = table.map(|t| t.to_string());
    let mut args: Vec<&str> = vec!["route", "del", prefix, "dev", bridge];
    if let Some(t) = table_str.as_deref() {
        args.extend_from_slice(&["table", t]);
    }
    run_cmd("ip", &args).await
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

/// List the agent-installed prefix routes attached to `bridge` in the
/// given routing table. The per-cluster connected route (`<cidr> proto
/// kernel scope link src <gateway>`) is filtered out because it isn't
/// an agent install — it's auto-acquired from the bridge's IPv4
/// address. Bare-address destinations are normalised to `<addr>/32`
/// so the set comparison matches the `<addr>/<prefix>` form produced
/// by `expand_prefixes`.
///
/// `table` must match the table `add_prefix_route` writes to —
/// otherwise the diff would re-install routes that already exist in
/// the VRF table (or never delete ones we wrote there).
async fn list_kernel_prefix_routes(
    bridge: &str,
    table: Option<u32>,
) -> Result<BTreeSet<String>, NetworkError> {
    let table_str = table.map(|t| t.to_string());
    let mut args: Vec<&str> = vec!["route", "show", "dev", bridge];
    if let Some(t) = table_str.as_deref() {
        args.extend_from_slice(&["table", t]);
    }
    let out = Command::new("ip").args(&args).output().await?;
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

    /// Cross-host VRF agreement comes from `vrf_for` being a pure
    /// function of `trust_domain` — every host computes the same name
    /// + table id without controller-side coordination. Regressions
    /// here would silently break tree isolation by enslaving bridges
    /// to different VRFs on different hosts.
    #[test]
    fn vrf_for_is_deterministic() {
        let a = vrf_for("tenant-a");
        let b = vrf_for("tenant-a");
        assert_eq!(a, b);
    }

    /// Different trust_domains map to different VRFs. Collisions
    /// would silently break isolation, so check a few representative
    /// pairs.
    #[test]
    fn vrf_for_separates_distinct_trust_domains() {
        for (a, b) in [
            ("tenant-a", "tenant-b"),
            ("alpha", "alpha2"),
            ("", "default"),
            ("prod", "staging"),
        ] {
            let va = vrf_for(a);
            let vb = vrf_for(b);
            assert_ne!(va, vb, "{a:?} and {b:?} collide: {va:?}");
        }
    }

    /// VRF name must fit IFNAMSIZ-1 = 15 chars (Linux kernel limit).
    /// The format is `bvrf-` (5 chars) + 10-hex-char suffix = 15.
    #[test]
    fn vrf_name_fits_ifnamsiz() {
        for td in ["", "x", "tenant-a", "this-is-a-very-long-trust-domain-name"] {
            let v = vrf_for(td);
            assert!(v.name.len() <= 15, "vrf name too long for {td:?}: {v:?}");
            assert!(v.name.starts_with(VRF_PREFIX));
        }
    }

    /// Table id must miss the kernel's reserved range (253/254/255 +
    /// distro local ranges) by floor-shifting to >= 10_000.
    #[test]
    fn vrf_table_id_avoids_reserved_range() {
        for td in ["a", "b", "c", "tenant-x", "fleet-prod"] {
            let v = vrf_for(td);
            assert!(
                v.table_id >= 10_000,
                "table_id {} too low for {td:?}",
                v.table_id
            );
        }
    }
}
