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
//!     bridge: basis0        # Linux bridge that masters `physicalNic`
//!     physicalNic: eno1     # physical NIC carrying VXLAN underlay traffic
//!   tls: { ... }
//! ```
//!
//! `metadata.name` is used as the hostname the agent registers as.
//!
//! The agent derives the VXLAN source address and MTU from the
//! physical NIC at startup — they are not user-configurable fields
//! because the kernel already knows them and asking the operator to
//! restate them just invites drift.

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

    /// gRPC endpoint of the local holod (the BGP daemon basis-agent
    /// drives via the YANG northbound). Defaults to holod's upstream
    /// default; override only if you've rebound holod's gRPC plugin.
    #[serde(default = "default_holod_endpoint")]
    pub holod_endpoint: String,
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9444".to_string()
}

fn default_holod_endpoint() -> String {
    "http://127.0.0.1:50051".to_string()
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
    /// Linux bridge that masters the physical NIC. Edge-flagged VMs
    /// attach a second TAP here. Tree VMs never touch this bridge —
    /// they live on per-VNI bridges (`brt<vni>`).
    pub bridge: String,
    /// Physical NIC name (e.g. `eno1`). Becomes a slave of `bridge`
    /// and is the egress interface for VXLAN frames. Its MTU and
    /// primary IPv4 address are read at startup and used as the tree
    /// bridges' MTU source and the VXLAN outer source address
    /// respectively — no separate config fields for those.
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
        assert_eq!(host.spec.network.bridge, "basis0");
        assert_eq!(host.spec.network.physical_nic, "eno1");
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
