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
//!   network:
//!     uplinkBridge: basis0       # name of the Linux bridge that masters `uplinkInterface`
//!     uplinkInterface: eno1       # physical NIC carrying VXLAN + edge NIC traffic
//!     vtepAddress: 10.100.0.17    # this host's IP for VXLAN outer header
//!     uplinkMtu: 9000             # physical link MTU; must be ≥ 1550 (jumbo recommended)
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

    /// Credentials for private OCI registries the agent pulls VM images
    /// from. Omit or leave empty for public-only pulls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registries: Vec<RegistryCredentials>,

    /// Plain-HTTP `host:port` for the Prometheus `/metrics` endpoint.
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9444".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryCredentials {
    pub host: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    /// Linux bridge that masters the physical uplink NIC. Edge-flagged
    /// VMs attach a second TAP here. Tree VMs never touch this bridge
    /// — they live on per-VNI bridges (`brt<vni>`).
    pub uplink_bridge: String,
    /// Physical NIC name, e.g. `eno1`. Becomes a slave of
    /// `uplink_bridge` and is the egress interface for VXLAN frames.
    pub uplink_interface: String,
    /// IP address used as the outer source of VXLAN frames. Almost
    /// always the uplink NIC's address. Reported to the controller on
    /// `RegisterHostRequest.vtep_address`.
    pub vtep_address: String,
    /// Physical uplink MTU. VXLAN adds 50 bytes of outer header, so
    /// this must be ≥ 1550 to carry standard 1500-byte inner frames.
    /// Jumbo frames (9000) are strongly recommended — the tree bridges
    /// derive their inner MTU from this.
    pub uplink_mtu: u32,
}

impl HostSpec {
    pub fn images_dir(&self) -> PathBuf {
        self.data_dir.join("images")
    }

    pub fn vms_dir(&self) -> PathBuf {
        self.data_dir.join("vms")
    }
}

/// Load and validate a `Host` YAML file.
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
    uplinkBridge: basis0
    uplinkInterface: eno1
    vtepAddress: 10.100.0.17
    uplinkMtu: 9000
  tls:
    cert: /etc/basis/tls/agent.crt
    key: /etc/basis/tls/agent.key
    ca: /etc/basis/tls/ca.crt
"#,
        );
        let host = load(f.path()).unwrap();
        assert_eq!(host.metadata.name, "node-1");
        assert_eq!(host.spec.network.uplink_bridge, "basis0");
        assert_eq!(host.spec.network.uplink_interface, "eno1");
        assert_eq!(host.spec.network.vtep_address, "10.100.0.17");
        assert_eq!(host.spec.network.uplink_mtu, 9000);
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
