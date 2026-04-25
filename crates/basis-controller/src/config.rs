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
//!     treeSupernet: 10.0.0.0/8
//!     treePrefix: 20
//!     vniRange: { start: 10000, end: 16000000 }
//!     pools:
//!       - name: cell-internal
//!         cidr: 192.168.100.0/24
//!         gateway: 192.168.100.1
//!         rangeStart: 192.168.100.20
//!         rangeEnd: 192.168.100.250
//!   cpuOvercommitRatio: 4.0
//! ```
//!
//! Address space has two planes:
//!   * `treeSupernet` — overlay CIDR, auto-carved per-tree. VM primary
//!     NICs, per-host bridge gateway IPs, and tree-internal cluster
//!     VIPs come from here. Not routable outside the VXLAN fabric.
//!   * `pools[]` — named LAN-routable pools. A cluster picks one pool
//!     for its apiserver VIP via `apiserverVipPool`; that same pool
//!     supplies the cluster's edge-NIC VMs.
//!
//! An empty / absent `apiserverVipPool` selects the cluster's own tree
//! `vip_range` (nested clusters, kube-vip on `ens3`, no LAN exposure);
//! any non-empty name resolves to a LAN pool and requires `edge: true`
//! on the cluster's CP VMs so kube-vip can gARP on `ens4`.

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
    /// Networking fabric configuration: per-tree CIDR carving, VNI
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    /// RFC1918 supernet that per-tree CIDRs are carved from, e.g.
    /// `10.0.0.0/8`. Every tree gets its own disjoint
    /// `/tree_prefix` slice.
    pub tree_supernet: String,

    /// Prefix length of each per-tree CIDR. Default /20 — 4094 usable
    /// addresses per tree, enough for one trust domain's worth of
    /// control-plane VMs, worker VMs, and tree-internal cluster VIPs.
    #[serde(default = "default_tree_prefix")]
    pub tree_prefix: u8,

    /// Number of addresses at the TOP of each tree's CIDR reserved
    /// for cluster VIPs allocated when a cluster's `apiserverVipPool`
    /// is empty — nested clusters whose apiservers stay inside their
    /// own tree. Default 16.
    #[serde(default = "default_vip_reserve")]
    pub vip_reserve: u32,

    /// Number of addresses at the BOTTOM of each tree's CIDR reserved
    /// for per-host bridge IPs. Each hypervisor carrying this tree is
    /// assigned one IP from this range and uses it as the gateway of
    /// every VM it hosts in this tree. Per-host uniqueness is required
    /// so cross-host replies routing back through the gateway land on
    /// the correct hypervisor. Default 32.
    #[serde(default = "default_bridge_reserve")]
    pub bridge_reserve: u32,

    /// VNI allocation bounds, inclusive. Default 10000..=16_000_000 —
    /// leaves low VNIs for infrastructure, stays well below the 2^24
    /// VXLAN ceiling.
    #[serde(default = "default_vni_range")]
    pub vni_range: VniRange,

    /// Named LAN-routable pools. A cluster's `apiserverVipPool` must
    /// match one of these by name (or be empty for tree-scoped).
    pub pools: Vec<Pool>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VniRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// Unique, user-chosen name. Referenced by `BasisCluster.spec`.
    /// Must not be empty (empty is reserved for "use the tree
    /// vip_range").
    pub name: String,
    /// CIDR the pool draws from. Must be disjoint from every other
    /// pool and from `treeSupernet`.
    pub cidr: String,
    pub gateway: String,
    pub range_start: String,
    pub range_end: String,
}

fn default_tree_prefix() -> u8 {
    20
}
fn default_vip_reserve() -> u32 {
    16
}
fn default_bridge_reserve() -> u32 {
    32
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
        Ok(())
    }
}

impl NetworkConfig {
    /// Look up a pool by name. Empty name → `None` (the caller reads
    /// this as "allocate from the tree's vip_range"); missing name →
    /// `None` as well, which the caller distinguishes via an explicit
    /// pre-check if it needs to.
    pub fn pool_by_name(&self, name: &str) -> Option<&Pool> {
        self.pools.iter().find(|p| p.name == name)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let supernet: ipnet::Ipv4Net = parse_cidr("network.treeSupernet", &self.tree_supernet)?;
        if self.tree_prefix < supernet.prefix_len() || self.tree_prefix > 30 {
            anyhow::bail!(
                "network.treePrefix /{} must be between /{} (supernet) and /30 inclusive",
                self.tree_prefix,
                supernet.prefix_len()
            );
        }

        // Per-tree CIDR layout check:
        //   1 (network) + bridge_reserve + vm range + vip_reserve + 1 (broadcast)
        // The VM range needs at least one address or a tree can hold no VMs.
        let addrs_per_tree: u32 = 1u32 << (32 - self.tree_prefix);
        let need = self
            .bridge_reserve
            .saturating_add(self.vip_reserve)
            .saturating_add(2);
        if addrs_per_tree <= need {
            anyhow::bail!(
                "network.treePrefix /{} holds {} addresses; bridgeReserve={} + vipReserve={} \
                 leaves no VM capacity",
                self.tree_prefix,
                addrs_per_tree,
                self.bridge_reserve,
                self.vip_reserve,
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

        if self.pools.is_empty() {
            anyhow::bail!("network.pools must contain at least one entry");
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
                    "network.treeSupernet {supernet} overlaps pool '{}' cidr {net}",
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
            nets.push(net);
        }
        Ok(())
    }
}

impl Pool {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.name.is_empty() {
            anyhow::bail!("pool name must not be empty");
        }
        let net: ipnet::Ipv4Net = parse_cidr(&format!("pool '{}' cidr", self.name), &self.cidr)?;
        let gw = parse_ip("gateway", &self.name, &self.gateway)?;
        let start = parse_ip("rangeStart", &self.name, &self.range_start)?;
        let end = parse_ip("rangeEnd", &self.name, &self.range_end)?;
        if !net.contains(&gw) {
            anyhow::bail!("pool '{}' gateway {gw} not in {net}", self.name);
        }
        if !net.contains(&start) || !net.contains(&end) {
            anyhow::bail!(
                "pool '{}' range [{start}..={end}] not inside {net}",
                self.name
            );
        }
        if u32::from(start) > u32::from(end) {
            anyhow::bail!("pool '{}' rangeStart {start} > rangeEnd {end}", self.name);
        }
        Ok(())
    }

    pub fn prefix_len(&self) -> u8 {
        self.cidr
            .parse::<ipnet::Ipv4Net>()
            .expect("pool.validate guarantees cidr parses")
            .prefix_len()
    }
}

fn parse_cidr(label: &str, s: &str) -> anyhow::Result<ipnet::Ipv4Net> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("{label} '{s}' invalid: {e}"))
}

fn parse_ip(field: &str, pool_name: &str, s: &str) -> anyhow::Result<Ipv4Addr> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("pool '{pool_name}' {field} '{s}' invalid: {e}"))
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
    treeSupernet: 10.0.0.0/8
    pools:
      - name: cell-internal
        cidr: 192.168.100.0/24
        gateway: 192.168.100.1
        rangeStart: 192.168.100.20
        rangeEnd: 192.168.100.250
"#
        .to_string()
    }

    #[test]
    fn loads_with_defaults() {
        let f = write(&base_yaml());
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert_eq!(spec.network.tree_prefix, 20);
        assert_eq!(spec.network.bridge_reserve, 32);
        assert_eq!(spec.network.vip_reserve, 16);
        assert_eq!(spec.network.vni_range.start, 10_000);
        assert_eq!(spec.network.vni_range.end, 16_000_000);
        assert_eq!(spec.network.pools.len(), 1);
        assert_eq!(spec.network.pools[0].name, "cell-internal");
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_pool_names() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        let mut dup = spec.network.pools[0].clone();
        dup.cidr = "192.168.101.0/24".to_string();
        dup.gateway = "192.168.101.1".to_string();
        dup.range_start = "192.168.101.20".to_string();
        dup.range_end = "192.168.101.30".to_string();
        spec.network.pools.push(dup);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_pool_overlap_with_tree_supernet() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.pools[0].cidr = "10.200.0.0/24".to_string();
        spec.network.pools[0].gateway = "10.200.0.1".to_string();
        spec.network.pools[0].range_start = "10.200.0.20".to_string();
        spec.network.pools[0].range_end = "10.200.0.30".to_string();
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
    fn rejects_pool_gateway_outside_cidr() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.pools[0].gateway = "10.99.99.99".to_string();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_tree_prefix_too_narrow_for_reserves() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.tree_prefix = 26;
        assert!(spec.validate().is_ok());
        spec.network.tree_prefix = 27;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn pool_by_name_lookup() {
        let spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        assert!(spec.network.pool_by_name("cell-internal").is_some());
        assert!(spec.network.pool_by_name("nonexistent").is_none());
        assert!(spec.network.pool_by_name("").is_none());
    }
}
