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
//!   ipPools: [...]
//!   cpuOvercommitRatio: 4.0
//! ```

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
    #[serde(default)]
    pub ip_pools: Vec<IpPool>,
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

/// Inclusive IPv4 range used by the IP-pool allocators.
///
/// Both ends are parsed to `Ipv4Addr` at allocation time; this type
/// just carries the config strings verbatim so YAML round-trips cleanly.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IpRange {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IpPool {
    pub name: String,
    pub cidr: String,
    pub gateway: String,
    /// Range the controller auto-picks VM IPs from (`allocate_ip`).
    /// Must be disjoint from `vip_range` — the two allocators write to
    /// the same `ip_allocations` table and rely on range-level
    /// separation so VM auto-allocation can never race a pending VIP
    /// reservation for a sibling cluster that hasn't been created yet.
    pub vm_range: IpRange,
    /// Range the controller auto-picks control-plane VIPs from
    /// (`allocate_vip`). Sized to the number of concurrent clusters you
    /// expect; a homelab with a handful of clusters can get away with
    /// 4–8 addresses.
    pub vip_range: IpRange,
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
        for pool in &self.ip_pools {
            pool.validate()
                .map_err(|e| anyhow::anyhow!("ipPools['{}']: {e}", pool.name))?;
        }
        Ok(())
    }
}

/// Parse a CIDR string like `"10.0.10.0/24"` and return the prefix
/// length. Used by both config validation (loud anyhow error) and the
/// runtime allocator path (typed `DbError::MalformedIpPool`).
pub fn parse_cidr_prefix(cidr: &str) -> anyhow::Result<u8> {
    let (addr, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("cidr '{cidr}' must be in the form 'A.B.C.D/N'"))?;
    addr.parse::<std::net::Ipv4Addr>()
        .map_err(|e| anyhow::anyhow!("cidr '{cidr}' has bad address: {e}"))?;
    let n: u8 = prefix
        .parse()
        .map_err(|e| anyhow::anyhow!("cidr '{cidr}' prefix '{prefix}' is not a u8: {e}"))?;
    if n > 32 {
        anyhow::bail!("cidr '{cidr}' prefix /{n} exceeds 32");
    }
    Ok(n)
}

impl IpPool {
    /// Parse every IP field and confirm the ranges are non-empty and
    /// disjoint. Called from [`BasisControllerSpec::validate`] so a
    /// malformed pool is rejected at config load, not at first allocation.
    pub fn validate(&self) -> anyhow::Result<()> {
        use std::net::Ipv4Addr;
        let parse_ip = |label: &str, s: &str| -> anyhow::Result<Ipv4Addr> {
            s.parse::<Ipv4Addr>()
                .map_err(|e| anyhow::anyhow!("{label} '{s}' is not a valid IPv4 address: {e}"))
        };
        parse_cidr_prefix(&self.cidr)?;
        parse_ip("gateway", &self.gateway)?;
        let vm_start = parse_ip("vmRange.start", &self.vm_range.start)?;
        let vm_end = parse_ip("vmRange.end", &self.vm_range.end)?;
        let vip_start = parse_ip("vipRange.start", &self.vip_range.start)?;
        let vip_end = parse_ip("vipRange.end", &self.vip_range.end)?;

        if u32::from(vm_start) > u32::from(vm_end) {
            anyhow::bail!("vmRange.start ({vm_start}) must be <= vmRange.end ({vm_end})");
        }
        if u32::from(vip_start) > u32::from(vip_end) {
            anyhow::bail!("vipRange.start ({vip_start}) must be <= vipRange.end ({vip_end})");
        }

        // Ranges must be disjoint — the allocator keys off range
        // membership to decide which pool an IP came from, and overlap
        // would make reservations race.
        let (a, b) = (u32::from(vm_start), u32::from(vm_end));
        let (c, d) = (u32::from(vip_start), u32::from(vip_end));
        if a <= d && c <= b {
            anyhow::bail!(
                "vmRange ({vm_start}..={vm_end}) overlaps vipRange ({vip_start}..={vip_end}) \
                 — the allocator requires them to be disjoint"
            );
        }
        Ok(())
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

    #[test]
    fn loads_valid_controller_config() {
        let f = write(
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
  ipPools:
    - name: default
      cidr: 10.0.10.0/24
      gateway: 10.0.10.1
      vmRange:
        start: 10.0.10.20
        end: 10.0.10.250
      vipRange:
        start: 10.0.10.10
        end: 10.0.10.19
"#,
        );
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert_eq!(spec.listen, "0.0.0.0:7443");
        assert_eq!(
            spec.db_path(),
            PathBuf::from("/var/lib/basis/controller.db")
        );
        assert_eq!(spec.ip_pools.len(), 1);
        assert_eq!(spec.ip_pools[0].name, "default");
        assert_eq!(spec.ip_pools[0].vm_range.start, "10.0.10.20");
        assert_eq!(spec.ip_pools[0].vip_range.end, "10.0.10.19");
    }

    #[test]
    fn ip_pools_default_to_empty() {
        let f = write(
            r#"apiVersion: basis.dev/v1alpha1
kind: BasisController
metadata: { name: p }
spec:
  listen: "0.0.0.0:7443"
  dataDir: /var/lib/basis
  tls: { cert: /a, key: /b, ca: /c }
"#,
        );
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert!(spec.ip_pools.is_empty());
    }

    #[test]
    fn cpu_overcommit_ratio_defaults_to_4() {
        let f = write(
            r#"apiVersion: basis.dev/v1alpha1
kind: BasisController
metadata: { name: p }
spec:
  listen: "0.0.0.0:7443"
  dataDir: /var/lib/basis
  tls: { cert: /a, key: /b, ca: /c }
"#,
        );
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert_eq!(spec.cpu_overcommit_ratio, 4.0);
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_ratio_below_one() {
        let mut spec = BasisControllerSpec {
            listen: "x".to_string(),
            metrics_listen: "x".to_string(),
            data_dir: PathBuf::from("/"),
            tls: TlsConfig {
                cert: PathBuf::from("/a"),
                key: PathBuf::from("/b"),
                ca: PathBuf::from("/c"),
            },
            ip_pools: vec![],
            dns_servers: vec![],
            cpu_overcommit_ratio: 0.5,
        };
        assert!(spec.validate().is_err());
        spec.cpu_overcommit_ratio = f32::NAN;
        assert!(spec.validate().is_err());
        spec.cpu_overcommit_ratio = 1.0;
        assert!(spec.validate().is_ok());
    }

    fn valid_pool() -> IpPool {
        IpPool {
            name: "p".to_string(),
            cidr: "10.0.10.0/24".to_string(),
            gateway: "10.0.10.1".to_string(),
            vm_range: IpRange {
                start: "10.0.10.20".to_string(),
                end: "10.0.10.250".to_string(),
            },
            vip_range: IpRange {
                start: "10.0.10.10".to_string(),
                end: "10.0.10.19".to_string(),
            },
        }
    }

    #[test]
    fn ip_pool_validate_accepts_valid() {
        assert!(valid_pool().validate().is_ok());
    }

    #[test]
    fn ip_pool_validate_rejects_malformed_ip() {
        let mut p = valid_pool();
        p.vm_range.start = "not-an-ip".to_string();
        assert!(p.validate().is_err());

        let mut p = valid_pool();
        p.gateway = "10.0.10".to_string();
        assert!(p.validate().is_err());
    }

    #[test]
    fn ip_pool_validate_rejects_inverted_range() {
        let mut p = valid_pool();
        p.vm_range.start = "10.0.10.250".to_string();
        p.vm_range.end = "10.0.10.20".to_string();
        assert!(p.validate().is_err());
    }

    #[test]
    fn ip_pool_validate_rejects_overlapping_ranges() {
        let mut p = valid_pool();
        // vm range overlaps vip range.
        p.vm_range.start = "10.0.10.15".to_string();
        p.vm_range.end = "10.0.10.30".to_string();
        assert!(p.validate().is_err());
    }

    #[test]
    fn spec_validate_rejects_bad_pool() {
        let mut spec = BasisControllerSpec {
            listen: "x".to_string(),
            metrics_listen: "x".to_string(),
            data_dir: PathBuf::from("/"),
            tls: TlsConfig {
                cert: PathBuf::from("/a"),
                key: PathBuf::from("/b"),
                ca: PathBuf::from("/c"),
            },
            ip_pools: vec![valid_pool()],
            dns_servers: vec![],
            cpu_overcommit_ratio: 4.0,
        };
        assert!(spec.validate().is_ok());
        spec.ip_pools[0].gateway = "bogus".to_string();
        let err = spec.validate().unwrap_err();
        assert!(err.to_string().contains("ipPools['p']"));
    }

    #[test]
    fn rejects_non_controller_kind() {
        let f = write(
            r#"apiVersion: basis.dev/v1alpha1
kind: Host
metadata: { name: p }
spec: {}
"#,
        );
        assert!(matches!(
            BasisControllerSpec::load(f.path()),
            Err(ResourceError::Kind { .. })
        ));
    }
}
