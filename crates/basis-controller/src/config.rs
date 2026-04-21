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
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9443".to_string()
}

fn default_dns_servers() -> Vec<String> {
    vec!["8.8.8.8".to_string(), "8.8.4.4".to_string()]
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
