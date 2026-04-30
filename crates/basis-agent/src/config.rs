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
//!   storage:
//!     rootfs: { vg: basis, thinPool: pool }   # thin pool for VM rootfs
//!     data:   { vg: basis-data }              # plain VG for raw data disks
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
    /// LVM volume groups backing this host's VM disks. Two distinct
    /// pools by design: a thin pool for VM rootfs (golden-image CoW
    /// snapshots are exactly what thin was built for) and a plain VG
    /// for raw data disks (linear LVs under bluestore — no thin layer
    /// double-booking allocation, no shared blast radius with rootfs).
    pub storage: StorageSpec,
    pub tls: TlsConfig,

    /// Credentials for private OCI registries the agent pulls VM images
    /// from. Omit or leave empty for public-only pulls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registries: Vec<RegistryCredentials>,

    /// Plain-HTTP `host:port` for the Prometheus `/metrics` endpoint.
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,

    /// gRPC endpoint of the local gobgpd (the BGP daemon basis-agent
    /// drives via its typed gRPC northbound). Defaults to gobgpd's
    /// upstream default; override only if you've rebound gobgpd's
    /// gRPC plugin.
    #[serde(default = "default_gobgpd_endpoint")]
    pub gobgpd_endpoint: String,

    /// Operator-assigned placement preference, lower wins. The
    /// scheduler uses this as a tiebreaker after capacity + GPU
    /// topology + anti-affinity, so two equally-good hosts go to
    /// the lower-rank one. Default 0 = "no preference"; bump
    /// deprioritized hosts (e.g. consumer-disk boxes that shouldn't
    /// carry etcd) to a higher number. Reported once at registration;
    /// changes require a basis-agent restart.
    #[serde(default)]
    pub rank: u32,

    /// Operator-assigned labels (e.g. {"tier": "fast"}). Used by the
    /// scheduler's per-Machine `placement.requires` (hard filter)
    /// and `placement.prefers` (soft tiebreak). Default empty means
    /// "no labels" — the host satisfies only an empty `requires`.
    /// Reported once at registration; changes require a restart.
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9444".to_string()
}

fn default_gobgpd_endpoint() -> String {
    "http://127.0.0.1:50051".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryCredentials {
    pub host: String,
    pub username: String,
    pub password: String,
}

/// LVM layout this agent expects on the host.
///
/// Two pools, each provisioned by the basis-prereqs ansible role on
/// distinct partitions / NVMes:
///   * `rootfs` — thin pool. VM rootfs LVs are CoW snapshots of a
///     golden image LV. Sub-second create. Tolerates overcommit (rare
///     but possible across an image's free space).
///   * `data`   — plain VG. Each requested data disk is a linear LV
///     of the requested size, fully allocated. No CoW under bluestore;
///     guest TRIM reaches the underlying NVMe through dm-linear with
///     no metadata indirection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    pub rootfs: RootfsSpec,
    pub data: DataSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RootfsSpec {
    /// Volume group containing the rootfs thin pool.
    pub vg: String,
    /// Thin pool LV name within `vg`.
    pub thin_pool: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSpec {
    /// Volume group hosting linear LVs for VM data disks.
    pub vg: String,
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
  storage:
    rootfs:
      vg: basis
      thinPool: pool
    data:
      vg: basis-data
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
        assert_eq!(host.spec.storage.rootfs.vg, "basis");
        assert_eq!(host.spec.storage.rootfs.thin_pool, "pool");
        assert_eq!(host.spec.storage.data.vg, "basis-data");
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
