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
//!     vipReserve: 16
//!     vniRange: { start: 10000, end: 16000000 }
//!     vniCooldownSecs: 60
//!     edgePool:
//!       cidr: 192.168.100.0/24
//!       gateway: 192.168.100.1
//!       rangeStart: 192.168.100.20
//!       rangeEnd: 192.168.100.250
//!   cpuOvercommitRatio: 4.0
//! ```
//!
//! One-pool-per-controller by design: every cluster carves its own
//! sub-CIDR out of `treeSupernet`, so there's no "pick a pool by name"
//! step on create. Edge IPs (second NIC for `edge: true` machines) live
//! in a single global `edgePool` on the uplink — carve per-tree edge
//! pools if you ever need per-tree uplink isolation (not yet).

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use basis_common::resource::{load_resource, Resource, ResourceError};
use basis_common::tls::TlsConfig;
use serde::{Deserialize, Serialize};

pub const KIND: &str = "BasisController";

pub type BasisController = Resource<BasisControllerSpec>;

/// Sentinel scope value for edge-pool IP allocations. Stored in
/// `ip_allocations.scope` alongside tree UUIDs; the two never collide
/// because UUIDs can't match the literal "edge".
pub const EDGE_SCOPE: &str = "edge";

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
    /// allocation bounds, and the shared edge-NIC IP pool.
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
    /// addresses per tree, plenty for one trust domain's worth of
    /// control-plane VMs + worker VMs + VIPs.
    #[serde(default = "default_tree_prefix")]
    pub tree_prefix: u8,

    /// Number of addresses at the TOP of each tree's CIDR reserved for
    /// control-plane VIPs. Default 16 — one VIP per cluster comfortably
    /// handles cell + children.
    #[serde(default = "default_vip_reserve")]
    pub vip_reserve: u32,

    /// VNI allocation bounds, inclusive. Default 10000..=16_000_000 —
    /// leaves low VNIs for infrastructure, stays well below the 2^24
    /// VXLAN ceiling.
    #[serde(default = "default_vni_range")]
    pub vni_range: VniRange,

    /// Seconds a deleted tree's VNI is held before reuse. Protects
    /// in-flight VXLAN frames for the prior tree from being
    /// decapsulated into the new tree's bridge.
    #[serde(default = "default_vni_cooldown_secs")]
    pub vni_cooldown_secs: u64,

    /// Edge IP pool on the uplink for `edge: true` machines' second NICs.
    pub edge_pool: EdgePool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VniRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EdgePool {
    /// CIDR the uplink NIC lives on (e.g. the TOR switch's LAN).
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
fn default_vni_range() -> VniRange {
    VniRange {
        start: 10_000,
        end: 16_000_000,
    }
}
fn default_vni_cooldown_secs() -> u64 {
    60
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

/// Parse a CIDR string like `"10.0.10.0/24"` and return the prefix
/// length.
pub fn parse_cidr_prefix(cidr: &str) -> anyhow::Result<u8> {
    let net: ipnet::Ipv4Net = cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("cidr '{cidr}' invalid: {e}"))?;
    Ok(net.prefix_len())
}

impl NetworkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let supernet: ipnet::Ipv4Net = self
            .tree_supernet
            .parse()
            .map_err(|e| anyhow::anyhow!("network.treeSupernet '{}' invalid: {e}", self.tree_supernet))?;
        if self.tree_prefix < supernet.prefix_len() || self.tree_prefix > 30 {
            anyhow::bail!(
                "network.treePrefix /{} must be between /{} (supernet) and /30 inclusive",
                self.tree_prefix,
                supernet.prefix_len()
            );
        }
        // Per-tree CIDRs carry: network, broadcast, gateway (first host),
        // plus `vipReserve` VIPs at the top, and everything else is VM
        // range. For /20 that's 4096 total, 4091 VMs; even for /28 (16
        // total) the layout still leaves a handful of VMs after
        // reserving 16 VIPs is refused here.
        let addrs_per_tree: u32 = 1u32 << (32 - self.tree_prefix);
        let need = self.vip_reserve.saturating_add(3); // net + bcast + gateway
        if addrs_per_tree <= need {
            anyhow::bail!(
                "network.treePrefix /{} holds {} addresses; vipReserve={} leaves no VM capacity",
                self.tree_prefix,
                addrs_per_tree,
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
        self.edge_pool.validate()?;
        Ok(())
    }
}

impl EdgePool {
    pub fn validate(&self) -> anyhow::Result<()> {
        let net: ipnet::Ipv4Net = self
            .cidr
            .parse()
            .map_err(|e| anyhow::anyhow!("edgePool.cidr '{}' invalid: {e}", self.cidr))?;
        let parse_ip = |label: &str, s: &str| -> anyhow::Result<Ipv4Addr> {
            s.parse::<Ipv4Addr>()
                .map_err(|e| anyhow::anyhow!("edgePool.{label} '{s}' invalid: {e}"))
        };
        let gw = parse_ip("gateway", &self.gateway)?;
        let start = parse_ip("rangeStart", &self.range_start)?;
        let end = parse_ip("rangeEnd", &self.range_end)?;
        if !net.contains(&gw) {
            anyhow::bail!("edgePool.gateway {gw} not in {net}");
        }
        if !net.contains(&start) || !net.contains(&end) {
            anyhow::bail!("edgePool.range [{start}..={end}] not inside {net}");
        }
        if u32::from(start) > u32::from(end) {
            anyhow::bail!("edgePool.rangeStart {start} > rangeEnd {end}");
        }
        Ok(())
    }

    pub fn prefix_len(&self) -> anyhow::Result<u8> {
        parse_cidr_prefix(&self.cidr)
    }
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
    edgePool:
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
        assert_eq!(spec.network.vip_reserve, 16);
        assert_eq!(spec.network.vni_range.start, 10_000);
        assert_eq!(spec.network.vni_range.end, 16_000_000);
        assert_eq!(spec.network.vni_cooldown_secs, 60);
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn rejects_supernet_overflow() {
        let mut spec_str = base_yaml();
        spec_str = spec_str.replace("treeSupernet: 10.0.0.0/8", "treeSupernet: 10.0.0.0/24");
        let f = write(&spec_str);
        // /24 supernet with default /20 tree prefix is impossible.
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_vni_past_24bit() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.vni_range.end = 1 << 24;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_edge_gateway_outside_cidr() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.edge_pool.gateway = "10.99.99.99".to_string();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_tree_prefix_too_narrow_for_vip_reserve() {
        let mut spec = BasisControllerSpec::load(write(&base_yaml()).path()).unwrap();
        spec.network.tree_prefix = 29; // 8 addresses
        assert!(spec.validate().is_err());
    }
}
