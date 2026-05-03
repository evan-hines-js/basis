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
//!     rootfs: { vg: basis, thinPool: pool }
//!     pools:
//!       - name: bulk
//!         backend: lvm-linear
//!         labels: { tier: bulk, medium: sata }
//!         devices:
//!           - id: ata-INTEL_SSDSC2BX800G4_aaaa
//!             vg: basis-bulk-aaaa
//!             sizeGib: 745
//!   tls: { ... }
//! ```
//!
//! `metadata.name` is used as the hostname the agent registers as.
//!
//! The agent derives the VXLAN source address and MTU from the
//! physical NIC at startup — they are not user-configurable fields
//! because the kernel already knows them and asking the operator to
//! restate them just invites drift.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
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
    /// Storage layout: one rootfs thin pool plus zero-or-more data
    /// pools. Each data pool is a labeled, backend-typed grouping of
    /// physical devices; the controller's scheduler matches per-disk
    /// selectors against pool labels and picks a (host, pool, device)
    /// tuple to allocate from.
    pub storage: StorageSpec,
    pub tls: TlsConfig,

    /// Credentials for private OCI registries the agent pulls VM images
    /// from. Omit or leave empty for public-only pulls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registries: Vec<RegistryCredentials>,

    /// Plain-HTTP `host:port` for the Prometheus `/metrics` endpoint.
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,

    /// gRPC endpoint of the local gobgpd.
    #[serde(default = "default_gobgpd_endpoint")]
    pub gobgpd_endpoint: String,

    /// Operator-assigned placement preference, lower wins. Tiebreaker
    /// after capacity + GPU topology + anti-affinity.
    #[serde(default)]
    pub rank: u32,

    /// Operator-assigned host labels, used by `LabelSelector` against
    /// host placement (separate from per-pool labels).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
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

/// LVM rootfs and a list of labeled data pools.
///
/// `rootfs` is the thin pool VM rootfs LVs snapshot from a golden
/// image — one per host, no per-disk choice today. `pools` are the
/// scheduler-visible storage tiers: each pool wears operator labels
/// (`tier`, `medium`, etc.) and groups one or more physical devices,
/// each with its own VG. The "one VG per device" rule is what makes
/// device-level failure-domain free — an LV cannot span PVs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageSpec {
    pub rootfs: RootfsSpec,
    /// Labeled data pools. Empty is legal — a worker that doesn't
    /// carry storage workloads doesn't need any. `deny_unknown_fields`
    /// on this struct ensures stale `data: { vg: ... }` from the
    /// pre-pool schema fails loud at parse time rather than silently
    /// producing an empty pool list.
    #[serde(default)]
    pub pools: Vec<PoolSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RootfsSpec {
    /// Volume group containing the rootfs thin pool.
    pub vg: String,
    /// Thin pool LV name within `vg`.
    pub thin_pool: String,
}

/// One labeled data pool. The pool is the operator abstraction; the
/// scheduler matches selectors against its `labels`. Devices listed
/// inside it are the failure-domain units the scheduler picks among
/// when placing each disk.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolSpec {
    /// Host-local stable name. Reservations and telemetry are keyed on
    /// `(host, pool, device_id)`.
    pub name: String,
    /// Backend type. v1 ships only `lvm-linear`; `raw-disk` and
    /// `nvme-namespace` are reserved for future milestones and rejected
    /// by the agent at startup with a clear error.
    pub backend: PoolBackend,
    /// Operator-written labels — the only selector vocabulary. Anything
    /// semantic (`tier`, `medium`, `vendor`, `rack`, `firmware-class`)
    /// goes here. The scheduler treats it as opaque key/value.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Devices backing this pool. Per-device failure domain; capacity
    /// tracked per-device.
    pub devices: Vec<PoolDeviceSpec>,
}

/// Pool backend type. Future variants are deserializable so the agent
/// can produce a structured "not yet supported" error rather than a
/// raw serde failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PoolBackend {
    /// One LVM VG per device, linear LVs allocated per data disk.
    LvmLinear,
    /// Whole-device passthrough. Reserved for M2.
    RawDisk,
    /// Hardware NVMe namespace partitioning. Reserved for M3.
    NvmeNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolDeviceSpec {
    /// Stable `/dev/disk/by-id/<name>` (block device) or controller id.
    pub id: String,
    /// LVM volume group living on this device. Required for
    /// `lvm-linear`; ignored by `raw-disk`. Ansible creates the VG; the
    /// agent verifies it at startup. One PV per VG, one VG per device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vg: Option<String>,
    /// Physical capacity in GiB, as the operator declared it. The
    /// agent cross-checks against the PV's reported size at startup.
    pub size_gib: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    /// Linux bridge that masters the physical NIC.
    pub bridge: String,
    /// Physical NIC name (e.g. `eno1`).
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

impl StorageSpec {
    /// Cross-pool invariants that any storage config must satisfy.
    /// Run at agent startup before backends initialize so config drift
    /// gets caught before any hardware op runs.
    ///
    /// 1. Pool names are unique.
    /// 2. No `device.id` appears in two pools.
    /// 3. No `vg` appears in two devices (a VG belongs to one device).
    /// 4. Each `lvm-linear` device has a `vg`.
    /// 5. Backends not implemented in this build are rejected.
    pub fn validate(&self) -> Result<()> {
        let mut pool_names = std::collections::HashSet::new();
        let mut device_ids = std::collections::HashMap::new();
        let mut vgs = std::collections::HashMap::new();

        for pool in &self.pools {
            if !pool_names.insert(pool.name.as_str()) {
                return Err(anyhow!("duplicate pool name {:?}", pool.name));
            }
            match pool.backend {
                PoolBackend::LvmLinear => {}
                PoolBackend::RawDisk => {
                    return Err(anyhow!(
                        "pool {:?}: backend `raw-disk` is not yet supported in this build (M2)",
                        pool.name
                    ));
                }
                PoolBackend::NvmeNamespace => {
                    return Err(anyhow!(
                        "pool {:?}: backend `nvme-namespace` is not yet supported in this build (M3)",
                        pool.name
                    ));
                }
            }
            for device in &pool.devices {
                if let Some(prev) = device_ids.insert(device.id.clone(), pool.name.clone()) {
                    return Err(anyhow!(
                        "device {:?} appears in pools {:?} and {:?}; devices must partition the host",
                        device.id,
                        prev,
                        pool.name
                    ));
                }
                let vg = device.vg.as_deref().ok_or_else(|| {
                    anyhow!(
                        "pool {:?} device {:?}: lvm-linear devices must declare `vg`",
                        pool.name,
                        device.id
                    )
                })?;
                if let Some(prev) =
                    vgs.insert(vg.to_string(), (pool.name.clone(), device.id.clone()))
                {
                    return Err(anyhow!(
                        "vg {:?} appears on devices {:?} and {:?}; one VG per device is required",
                        vg,
                        prev.1,
                        device.id
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Load and validate a `Host` YAML file. Cross-pool invariants are
/// checked before the agent serves traffic.
pub fn load(path: &Path) -> Result<Host, ResourceError> {
    let host: Host = load_resource(path, KIND)?;
    host.spec
        .storage
        .validate()
        .map_err(|e| ResourceError::Other {
            kind: KIND.to_string(),
            source: e,
        })?;
    Ok(host)
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

    const HEADER: &str = r#"apiVersion: basis.dev/v1alpha1
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
"#;

    #[test]
    fn loads_valid_host_with_pools() {
        let f = write(&format!(
            r#"{HEADER}  storage:
    rootfs:
      vg: basis
      thinPool: pool
    pools:
      - name: bulk
        backend: lvm-linear
        labels:
          tier: bulk
          medium: sata
        devices:
          - id: ata-INTEL_SSDSC2BX800G4_aaaa
            vg: basis-bulk-aaaa
            sizeGib: 745
      - name: fast
        backend: lvm-linear
        labels:
          tier: low-latency
          medium: nvme
        devices:
          - id: nvme-INTEL_SSDPE2KX040T8_dddd
            vg: basis-fast-dddd
            sizeGib: 3725
"#,
        ));
        let host = load(f.path()).unwrap();
        assert_eq!(host.spec.storage.rootfs.vg, "basis");
        assert_eq!(host.spec.storage.pools.len(), 2);
        assert_eq!(host.spec.storage.pools[0].name, "bulk");
        assert_eq!(
            host.spec.storage.pools[0].labels.get("tier"),
            Some(&"bulk".to_string())
        );
        assert_eq!(host.spec.storage.pools[1].devices[0].vg.as_deref(), Some("basis-fast-dddd"));
    }

    #[test]
    fn rejects_duplicate_device_across_pools() {
        let f = write(&format!(
            r#"{HEADER}  storage:
    rootfs: {{ vg: basis, thinPool: pool }}
    pools:
      - name: a
        backend: lvm-linear
        devices:
          - id: ata-X
            vg: vg-a
            sizeGib: 100
      - name: b
        backend: lvm-linear
        devices:
          - id: ata-X
            vg: vg-b
            sizeGib: 100
"#,
        ));
        let err = load(f.path()).unwrap_err().to_string();
        assert!(err.contains("ata-X") && err.contains("partition"));
    }

    #[test]
    fn rejects_duplicate_vg() {
        let f = write(&format!(
            r#"{HEADER}  storage:
    rootfs: {{ vg: basis, thinPool: pool }}
    pools:
      - name: a
        backend: lvm-linear
        devices:
          - id: ata-X
            vg: shared
            sizeGib: 100
          - id: ata-Y
            vg: shared
            sizeGib: 100
"#,
        ));
        let err = load(f.path()).unwrap_err().to_string();
        assert!(err.contains("shared") && err.contains("one VG per device"));
    }

    #[test]
    fn rejects_lvm_linear_without_vg() {
        let f = write(&format!(
            r#"{HEADER}  storage:
    rootfs: {{ vg: basis, thinPool: pool }}
    pools:
      - name: a
        backend: lvm-linear
        devices:
          - id: ata-X
            sizeGib: 100
"#,
        ));
        let err = load(f.path()).unwrap_err().to_string();
        assert!(err.contains("must declare `vg`"));
    }

    #[test]
    fn rejects_unsupported_backend() {
        let f = write(&format!(
            r#"{HEADER}  storage:
    rootfs: {{ vg: basis, thinPool: pool }}
    pools:
      - name: a
        backend: raw-disk
        devices:
          - id: ata-X
            sizeGib: 100
"#,
        ));
        let err = load(f.path()).unwrap_err().to_string();
        assert!(err.contains("raw-disk") && err.contains("M2"));
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
