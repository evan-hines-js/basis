//! `BasisController` resource loaded from a YAML config file.
//!
//! ```yaml
//! apiVersion: basis.dev/v1alpha1
//! kind: BasisController
//! metadata:
//!   name: primary
//! spec:
//!   listen: "0.0.0.0:7443"
//!   dataDir: /var/lib/basis
//!   tls: { ... }
//!   network:
//!     clusterSupernet: 10.0.0.0/8
//!     clusterPrefix: 24
//!     vniRange: { start: 10000, end: 16000000 }
//!     pools:
//!       - name: cell-public
//!         cidr: 192.168.100.0/24
//!   cpuOvercommitRatio: 4.0
//! ```
//!
//! Address space has two planes:
//!   * `clusterSupernet` — overlay CIDR, auto-carved per-cluster. VM
//!     primary NICs come from here. Not routable outside the cluster's
//!     VXLAN; cross-cluster traffic goes through cell-public LB VIPs
//!     (typically Istio east-west gateways), not the underlay.
//!   * `pools[]` — named LAN-routable pools. A cluster names one pool
//!     for its `externalIpPool`; both the apiserver VIP and the Cilium
//!     LoadBalancer Service block are carved from it. Allocations are
//!     advertised cell-wide via the BGP reflector.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use basis_common::resource::{load_resource, Resource, ResourceError};
use basis_common::tls::TlsConfig;
use serde::{Deserialize, Serialize};

pub const KIND: &str = "BasisController";

pub type BasisController = Resource<BasisControllerSpec>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisControllerSpec {
    /// `host:port` the gRPC server binds to.
    pub listen: String,
    /// `host:port` the plain-HTTP metrics server binds to.
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,
    /// Persistent state directory (holds `controller.db`).
    pub data_dir: PathBuf,
    pub tls: TlsConfig,
    /// Networking fabric configuration: per-cluster CIDR carving, VNI
    /// allocation bounds, and named LAN pools.
    pub network: NetworkConfig,
    /// Resolvers the agent bakes into each VM's cloud-init network config.
    /// Defaults to public Google DNS so a stock deployment boots; override
    /// in any environment without outbound 8.8.8.8 reachability.
    #[serde(default = "default_dns_servers")]
    pub dns_servers: Vec<String>,
    /// Multiplier applied to each host's `total_cpu` before the scheduler
    /// checks whether a VM fits. 1.0 means no overcommit (sum of assigned
    /// vCPU ≤ physical). Memory and disk are never overcommitted. Values
    /// below 1.0 or non-finite are rejected by [`Self::validate`].
    #[serde(default = "default_cpu_overcommit_ratio")]
    pub cpu_overcommit_ratio: f32,
    /// Cell BGP route reflector. basis-controller doesn't speak BGP
    /// itself — `holod` runs as a sibling systemd service on the
    /// same host and basis pushes config to it via gRPC.
    pub bgp: BgpConfig,
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9443".to_string()
}

fn default_dns_servers() -> Vec<String> {
    vec!["8.8.8.8".to_string(), "8.8.4.4".to_string()]
}

fn default_cpu_overcommit_ratio() -> f32 {
    4.0
}

/// BGP reflector configuration. Mapped 1:1 onto
/// [`crate::bgp::ReflectorConfig`].
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BgpConfig {
    /// Cell ASN (every speaker in the cell uses this — sessions are iBGP).
    pub asn: u32,
    /// BGP router-id. Use the controller's underlay LAN IP.
    pub router_id: String,
    /// gRPC endpoint of the local holod, e.g. `http://127.0.0.1:50051`.
    /// Defaults to holod's upstream default; override only if you've
    /// rebound holod's gRPC plugin.
    #[serde(default = "default_holod_endpoint")]
    pub holod_endpoint: String,
    /// Logical name basis registers the BGP instance under. Surfaces
    /// in `holod`'s `Get` state for debugging.
    #[serde(default = "default_bgp_instance_name")]
    pub instance_name: String,
}

fn default_holod_endpoint() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn default_bgp_instance_name() -> String {
    "basis".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    /// RFC1918 supernet that per-cluster CIDRs are carved from, e.g.
    /// `10.0.0.0/8`. Every cluster gets its own disjoint
    /// `/cluster_prefix` slice.
    pub cluster_supernet: String,

    /// Prefix length of each per-cluster CIDR. Default /24 — with
    /// `bridge_reserve` = 32, gives ~32 hosts × ~220 VMs per cluster.
    /// Widen for larger clusters; `validate()` enforces enough
    /// headroom for at least one VM after bridge + apiserver reserves.
    #[serde(default = "default_cluster_prefix")]
    pub cluster_prefix: u8,

    /// Number of addresses at the BOTTOM of each cluster's CIDR
    /// reserved for per-host bridge IPs. Each hypervisor carrying a
    /// VM in the cluster gets one IP from this range and uses it as
    /// the gateway for the cluster-local VMs it hosts. Per-host
    /// uniqueness (rather than a shared anycast IP) is required so
    /// cross-host VM→gateway replies routing back through the
    /// gateway land on the originating hypervisor without depending
    /// on EVPN-style ARP suppression. Default 32.
    #[serde(default = "default_bridge_reserve")]
    pub bridge_reserve: u32,

    /// Cell-wide default for `CreateClusterRequest.external_service_ips`
    /// when the caller passes 0. Must be a power of two so the
    /// allocator can carve an aligned /N. Default 16 (a /28).
    #[serde(default = "default_external_service_ips")]
    pub default_external_service_ips: u32,

    /// VNI allocation bounds, inclusive. Default 10000..=16_000_000 —
    /// leaves low VNIs for infrastructure, stays well below the 2^24
    /// VXLAN ceiling.
    #[serde(default = "default_vni_range")]
    pub vni_range: VniRange,

    /// Named LAN-routable pools. A cluster's `externalIpPool` must
    /// match one of these by name. Required: every cluster's LB
    /// Service block (and the apiserver VIP, when
    /// `apiserver_visibility = PUBLIC`) lives in a LAN pool so
    /// cell-wide BGP advertisement always works.
    pub pools: Vec<Pool>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VniRange {
    pub start: u32,
    pub end: u32,
}

/// Reachability scope for a pool's allocated VIPs. `Lan` is the
/// default and matches every existing config: prefixes are advertised
/// cell-wide via BGP and proxy-ARPed onto the uplink so LAN clients
/// can reach them. `Tree` is for cluster-internal VIPs that should
/// never leak onto the LAN — the controller still allocates from the
/// pool's CIDR, but the agent installs only the per-bridge route, no
/// proxy-ARP and no BGP advertisement (Phase 1). Phase 2 layers a
/// trust-domain BGP community on top so different trust_domains
/// don't mutually learn each other's tree prefixes.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PoolScope {
    #[default]
    Lan,
    Tree,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// Unique, user-chosen name. Referenced by `BasisCluster.spec`.
    /// Must not be empty.
    pub name: String,
    /// CIDR slice basis owns. For `Lan`-scoped pools this must be
    /// LAN-routable space; for `Tree`-scoped pools this should be
    /// RFC1918 space outside the LAN's broadcast domain so the LAN
    /// never tries to ARP for it. The allocator walks
    /// `[network+1, broadcast-1]` (skipping network and broadcast).
    /// Carve smaller CIDRs to express "basis only owns part of this
    /// subnet"; non-power-of-two ranges become multiple pool entries.
    pub cidr: String,
    /// Whether VIPs from this pool are LAN-routable (`Lan`, default)
    /// or restricted to per-cluster bridges within the cell (`Tree`).
    /// Omitting `scope` keeps every existing config validating
    /// untouched.
    #[serde(default)]
    pub scope: PoolScope,
}

fn default_cluster_prefix() -> u8 {
    24
}
fn default_bridge_reserve() -> u32 {
    32
}
fn default_external_service_ips() -> u32 {
    16
}
fn default_vni_range() -> VniRange {
    VniRange {
        start: 10_000,
        end: 16_000_000,
    }
}

impl BasisControllerSpec {
    /// Load and validate a `BasisController` YAML file, returning the spec.
    pub fn load(path: &Path) -> Result<Self, ResourceError> {
        let resource: BasisController = load_resource(path, KIND)?;
        Ok(resource.spec)
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("controller.db")
    }

    /// Fail-fast sanity check on config fields whose invalid values would
    /// silently corrupt scheduling rather than trip serde deserialization.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.cpu_overcommit_ratio.is_finite() || self.cpu_overcommit_ratio < 1.0 {
            anyhow::bail!(
                "cpuOvercommitRatio must be finite and >= 1.0 (got {})",
                self.cpu_overcommit_ratio
            );
        }
        self.network.validate()?;
        self.bgp.validate()?;
        Ok(())
    }
}

impl BgpConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.asn == 0 {
            anyhow::bail!("bgp.asn must be non-zero");
        }
        let _: Ipv4Addr = self
            .router_id
            .parse()
            .map_err(|e| anyhow::anyhow!("bgp.routerId '{}' invalid: {e}", self.router_id))?;
        if self.instance_name.is_empty() {
            anyhow::bail!("bgp.instanceName must not be empty");
        }
        Ok(())
    }

    pub fn router_id_ipv4(&self) -> Ipv4Addr {
        self.router_id
            .parse()
            .expect("BgpConfig::validate guarantees router_id parses")
    }
}

impl NetworkConfig {
    /// Look up a pool by name. Empty / missing name → `None`; the
    /// CreateCluster path treats `None` as a hard error since every
    /// cluster needs a pool to host its apiserver VIP and LB block.
    pub fn pool_by_name(&self, name: &str) -> Option<&Pool> {
        self.pools.iter().find(|p| p.name == name)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let supernet: ipnet::Ipv4Net =
            parse_cidr("network.clusterSupernet", &self.cluster_supernet)?;
        if self.cluster_prefix < supernet.prefix_len() || self.cluster_prefix > 30 {
            anyhow::bail!(
                "network.clusterPrefix /{} must be between /{} (supernet) and /30 inclusive",
                self.cluster_prefix,
                supernet.prefix_len()
            );
        }

        // Per-cluster CIDR sanity:
        //   1 (network) + bridge_reserve + ≥1 VM + 1 (broadcast)
        // (with `apiserver_visibility = PRIVATE` the apiserver VIP
        // takes one more, but that's a per-cluster choice rather than
        // a config invariant — checked at allocate time.)
        let addrs_per_cluster: u32 = 1u32 << (32 - self.cluster_prefix);
        let need = self.bridge_reserve.saturating_add(2);
        if addrs_per_cluster <= need {
            anyhow::bail!(
                "network.clusterPrefix /{} holds {} addresses; \
                 bridgeReserve={} + network + broadcast leaves no VM capacity",
                self.cluster_prefix,
                addrs_per_cluster,
                self.bridge_reserve,
            );
        }

        if self.vni_range.start == 0 || self.vni_range.end < self.vni_range.start {
            anyhow::bail!(
                "network.vniRange invalid: start={}, end={}",
                self.vni_range.start,
                self.vni_range.end,
            );
        }
        // 24-bit ceiling: VXLAN VNI field is 24 bits.
        if self.vni_range.end >= 1 << 24 {
            anyhow::bail!(
                "network.vniRange.end {} exceeds VXLAN 24-bit limit (16_777_215)",
                self.vni_range.end,
            );
        }

        if !self.default_external_service_ips.is_power_of_two() {
            anyhow::bail!(
                "network.defaultExternalServiceIps must be a power of two (got {})",
                self.default_external_service_ips,
            );
        }

        if self.pools.is_empty() {
            anyhow::bail!("network.pools must contain at least one entry");
        }
        if !self.pools.iter().any(|p| !p.is_tree()) {
            anyhow::bail!(
                "network.pools must contain at least one Lan-scoped pool \
                 (every cell needs at least one LAN-routable pool for public \
                 cluster apiserver VIPs)"
            );
        }
        let mut names: HashSet<&str> = HashSet::with_capacity(self.pools.len());
        let mut nets: Vec<ipnet::Ipv4Net> = Vec::with_capacity(self.pools.len());
        for pool in &self.pools {
            if !names.insert(pool.name.as_str()) {
                anyhow::bail!("network.pools[].name '{}' is duplicated", pool.name);
            }
            pool.validate()?;
            let net: ipnet::Ipv4Net = pool
                .cidr
                .parse()
                .expect("pool.validate checked cidr parses");
            if cidrs_overlap(&supernet, &net) {
                anyhow::bail!(
                    "network.clusterSupernet {supernet} overlaps pool '{}' cidr {net}",
                    pool.name
                );
            }
            for (other, other_net) in self.pools[..nets.len()].iter().zip(nets.iter()) {
                if cidrs_overlap(&net, other_net) {
                    anyhow::bail!(
                        "pool '{}' cidr {net} overlaps pool '{}' cidr {other_net}",
                        pool.name,
                        other.name
                    );
                }
            }
            // Reject the silent "fits at create time but not at allocate
            // time" trap: the allocator carves an aligned /N service
            // block out of the pool's [network+1, broadcast-1] range. A
            // /N (count=2^(32-N)) needs the pool's prefix to be at
            // least 2 bits wider than /N — otherwise every aligned /N
            // boundary inside the pool clips either the network or
            // broadcast address. A pool that's exactly the same prefix
            // as the requested service block has zero usable slots.
            // Spotting it here means the operator sees the error during
            // `ansible-playbook`, not when the first cluster apply
            // bounces with a cryptic alignment message.
            let service_prefix = 32 - self.default_external_service_ips.trailing_zeros() as u8;
            if service_prefix < 32 && net.prefix_len() + 2 > service_prefix {
                anyhow::bail!(
                    "pool '{}' cidr {net} can't fit a /{service_prefix} service block \
                     (defaultExternalServiceIps = {}); pool prefix must be at most /{} \
                     to leave aligned space for the block",
                    pool.name,
                    self.default_external_service_ips,
                    service_prefix.saturating_sub(2),
                );
            }
            nets.push(net);
        }
        Ok(())
    }
}

impl Pool {
    pub fn is_tree(&self) -> bool {
        matches!(self.scope, PoolScope::Tree)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.name.is_empty() {
            anyhow::bail!("pool name must not be empty");
        }
        let net = self.parsed_cidr()?;
        // /32 is degenerate (no allocatable host addresses). /31 is
        // also degenerate for our model: under RFC 3021 it has 2
        // hosts and no broadcast, but our allocator treats network +
        // broadcast as reserved, leaving 0 allocatable. Reject
        // upfront so the operator gets a clear error instead of
        // exhaustion later.
        if net.prefix_len() >= 31 {
            anyhow::bail!(
                "pool '{}' cidr /{} too narrow — /30 (4 addrs) is the minimum",
                self.name,
                net.prefix_len(),
            );
        }
        Ok(())
    }

    pub fn prefix_len(&self) -> u8 {
        self.parsed_cidr()
            .expect("pool.validate guarantees cidr parses")
            .prefix_len()
    }

    fn parsed_cidr(&self) -> anyhow::Result<ipnet::Ipv4Net> {
        parse_cidr(&format!("pool '{}' cidr", self.name), &self.cidr)
    }
}

fn parse_cidr(label: &str, s: &str) -> anyhow::Result<ipnet::Ipv4Net> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("{label} '{s}' invalid: {e}"))
}

fn cidrs_overlap(a: &ipnet::Ipv4Net, b: &ipnet::Ipv4Net) -> bool {
    a.contains(&b.network()) || b.contains(&a.network())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(yaml: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        f
    }

    fn base_yaml() -> String {
        r#"apiVersion: basis.dev/v1alpha1
kind: BasisController
metadata:
  name: primary
spec:
  listen: "0.0.0.0:7443"
  dataDir: /var/lib/basis
  tls:
    cert: /etc/basis/tls/controller.crt
    key: /etc/basis/tls/controller.key
    ca: /etc/basis/tls/ca.crt
  network:
    clusterSupernet: 10.0.0.0/8
    pools:
      - name: cell-internal
        cidr: 192.168.100.0/24
  bgp:
    asn: 64500
    routerId: 10.0.0.1
"#
        .to_string()
    }

    #[test]
    fn loads_with_defaults() {
        let f = write(&base_yaml());
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert_eq!(spec.network.cluster_prefix, 24);
        assert_eq!(spec.network.bridge_reserve, 32);
        assert_eq!(spec.network.default_external_service_ips, 16);
        assert_eq!(spec.network.vni_range.start, 10_000);
        assert_eq!(spec.network.vni_range.end, 16_000_000);
        assert_eq!(spec.network.pools.len(), 1);
        assert_eq!(spec.network.pools[0].name, "cell-internal");
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn rejects_pool_too_narrow_for_default_service_block() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        // Default of 16 IPs = /28, needs pool /26 or wider. /27
        // looks like it should fit (32 addrs ≥ 16) but the only
        // aligned /28 inside clips network or broadcast.
        spec.network.default_external_service_ips = 16;
        spec.network.pools[0].cidr = "192.168.100.0/27".to_string();
        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("can't fit a /28"), "got: {err}");
        spec.network.pools[0].cidr = "192.168.100.0/26".to_string();
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn rejects_non_power_of_two_default_service_ips() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.default_external_service_ips = 17;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_pool_names() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        let mut dup = spec.network.pools[0].clone();
        dup.cidr = "192.168.101.0/24".to_string();
        spec.network.pools.push(dup);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_pool_overlap_with_cluster_supernet() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.pools[0].cidr = "10.200.0.0/24".to_string();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_cluster_prefix_too_narrow_for_bridge_reserve() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        // bridge_reserve = 32 + network + broadcast = 34 sentinels.
        // /26 (64 addresses) fits with 30 VM headroom; /27 (32) is
        // exactly the cliff (no VM capacity).
        spec.network.bridge_reserve = 32;
        spec.network.cluster_prefix = 26;
        assert!(spec.validate().is_ok());
        spec.network.cluster_prefix = 27;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_two_pools_with_overlapping_cidr() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        let mut other = spec.network.pools[0].clone();
        other.name = "other".to_string();
        spec.network.pools.push(other);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_vni_past_24bit() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.vni_range.end = 1 << 24;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_pool_cidr_too_narrow() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        // Use a /32 service-block default so /30 is the smallest pool
        // that has any allocatable host range — keeps this test
        // focused on the network/broadcast guard, not on
        // service-block fit (covered separately).
        spec.network.default_external_service_ips = 1;
        spec.network.pools[0].cidr = "192.168.100.0/31".to_string();
        assert!(spec.validate().is_err());
        spec.network.pools[0].cidr = "192.168.100.0/32".to_string();
        assert!(spec.validate().is_err());
        spec.network.pools[0].cidr = "192.168.100.0/30".to_string();
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn pool_by_name_lookup() {
        let spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        assert!(spec.network.pool_by_name("cell-internal").is_some());
        assert!(spec.network.pool_by_name("nonexistent").is_none());
        assert!(spec.network.pool_by_name("").is_none());
    }

    /// Pools default to `Lan` scope when `scope:` is omitted — every
    /// existing config keeps validating without a YAML edit.
    #[test]
    fn pool_scope_defaults_to_lan() {
        let spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        assert_eq!(spec.network.pools[0].scope, PoolScope::Lan);
        assert!(!spec.network.pools[0].is_tree());
    }

    /// Adding `scope: tree` to a pool entry parses and round-trips.
    #[test]
    fn pool_scope_tree_deserializes() {
        let yaml = base_yaml().replace(
            "      - name: cell-internal\n        cidr: 192.168.100.0/24",
            "      - name: cell-public\n        cidr: 192.168.100.0/24\n      - name: cell-internal\n        cidr: 172.16.0.0/24\n        scope: tree",
        );
        let spec = BasisControllerSpec::load(write(&yaml).path()).unwrap();
        let by_name = |n| spec.network.pools.iter().find(|p| p.name == n).unwrap();
        assert_eq!(by_name("cell-public").scope, PoolScope::Lan);
        assert_eq!(by_name("cell-internal").scope, PoolScope::Tree);
        assert!(by_name("cell-internal").is_tree());
        assert!(spec.validate().is_ok());
    }

    /// At least one Lan-scoped pool must exist — every cell needs one
    /// LAN-routable pool for public cluster apiserver VIPs. A config
    /// with only Tree pools is rejected up-front.
    #[test]
    fn rejects_all_tree_pools() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.pools[0].scope = PoolScope::Tree;
        spec.network.pools[0].cidr = "10.250.0.0/24".to_string();
        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("at least one Lan-scoped pool"), "got: {err}");
    }
}
