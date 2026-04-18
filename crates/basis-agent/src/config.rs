//! `Host` resource loaded from a YAML config file.
//!
//! ```yaml
//! apiVersion: basis.dev/v1alpha1
//! kind: Host
//! metadata:
//!   name: node-1
//! spec:
//!   controllerEndpoint: "https://10.0.0.1:7443"
//!   dataDir: /var/lib/basis
//!   network: { bridge: basis0, physicalNic: eno1 }
//!   tls: { ... }
//! ```
//!
//! `metadata.name` is used as the hostname the agent registers as.

use std::path::{Path, PathBuf};

use basis_common::resource::{load_resource, Resource, ResourceError};
use basis_common::tls::TlsConfig;
use serde::{Deserialize, Serialize};

pub const KIND: &str = "Host";

pub type Host = Resource<HostSpec>;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostSpec {
    pub controller_endpoint: String,
    pub data_dir: PathBuf,
    pub network: NetworkSpec,
    pub tls: TlsConfig,

    /// Path to the cloud-hypervisor firmware blob used to boot guest
    /// VMs. VMs boot in UEFI mode from their disk, which lets stock
    /// Ubuntu/Debian cloud images run unmodified (no kernel extraction).
    #[serde(default = "default_firmware_path")]
    pub firmware_path: PathBuf,
}

fn default_firmware_path() -> PathBuf {
    PathBuf::from("/usr/share/cloud-hypervisor/hypervisor-fw")
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    pub bridge: String,
    pub physical_nic: String,
}

impl HostSpec {
    pub fn images_dir(&self) -> PathBuf {
        self.data_dir.join("images")
    }

    pub fn vms_dir(&self) -> PathBuf {
        self.data_dir.join("vms")
    }
}

/// Load and validate a `Host` YAML file, returning the full resource so
/// callers have access to `metadata.name` (the hostname).
pub fn load(path: &Path) -> Result<Host, ResourceError> {
    load_resource(path, KIND)
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
    fn loads_valid_host() {
        let f = write(
            r#"apiVersion: basis.dev/v1alpha1
kind: Host
metadata:
  name: node-1
spec:
  controllerEndpoint: "https://10.0.0.1:7443"
  dataDir: /var/lib/basis
  network:
    bridge: basis0
    physicalNic: eno1
  tls:
    cert: /etc/basis/tls/agent.crt
    key: /etc/basis/tls/agent.key
    ca: /etc/basis/tls/ca.crt
"#,
        );
        let host = load(f.path()).unwrap();
        assert_eq!(host.metadata.name, "node-1");
        assert_eq!(host.spec.controller_endpoint, "https://10.0.0.1:7443");
        assert_eq!(host.spec.network.bridge, "basis0");
        assert_eq!(
            host.spec.images_dir(),
            PathBuf::from("/var/lib/basis/images")
        );
    }

    #[test]
    fn rejects_non_host_kind() {
        let f = write(
            r#"apiVersion: basis.dev/v1alpha1
kind: BasisController
metadata: { name: x }
spec: {}
"#,
        );
        assert!(matches!(load(f.path()), Err(ResourceError::Kind { .. })));
    }
}
