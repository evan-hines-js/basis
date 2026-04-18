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
    /// Persistent state directory (holds `controller.db`).
    pub data_dir: PathBuf,
    pub tls: TlsConfig,
    #[serde(default)]
    pub ip_pools: Vec<IpPool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IpPool {
    pub name: String,
    pub cidr: String,
    pub gateway: String,
    pub range_start: String,
    pub range_end: String,
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
      rangeStart: 10.0.10.10
      rangeEnd: 10.0.10.250
"#,
        );
        let spec = BasisControllerSpec::load(f.path()).unwrap();
        assert_eq!(spec.listen, "0.0.0.0:7443");
        assert_eq!(spec.db_path(), PathBuf::from("/var/lib/basis/controller.db"));
        assert_eq!(spec.ip_pools.len(), 1);
        assert_eq!(spec.ip_pools[0].name, "default");
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
