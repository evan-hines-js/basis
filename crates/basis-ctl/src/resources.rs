//! YAML resource types for `basis-ctl` — parsed from `-f <file>` on the
//! command line. Shape mirrors Kubernetes (`apiVersion` / `kind` /
//! `metadata` / `spec`) so it's familiar to anyone who has used `kubectl`.
//!
//! Multi-document YAML is supported: a single file may contain a Cluster
//! and one or more Machines separated by `---`.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use basis_client::MachineRequest;
use basis_proto::ApiserverVisibility;
use serde::Deserialize;

pub const API_VERSION: &str = "basis.dev/v1";

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ClusterSpec {
    /// Named LAN pool the cluster's external IPs come from — the
    /// LoadBalancer Service block always, and the apiserver VIP too
    /// when `apiserverVisibility = Public`. Required: must match a
    /// pool name in the controller's `network.pools[]`.
    #[serde(rename = "externalIpPool")]
    pub external_ip_pool: String,

    /// Number of LoadBalancer Service IPs Cilium should be configured
    /// with. 0 / unset → cell-wide default (`network.defaultExternalServiceIps`).
    /// Must be a power of two.
    #[serde(default, rename = "externalServiceIps")]
    pub external_service_ips: u32,

    /// Where the apiserver VIP lives. `Public` (default) → from the
    /// pool, BGP-advertised cell-wide; `Private` → cluster CIDR's
    /// last usable, accessed via the parent cell's API proxy.
    #[serde(default, rename = "apiserverVisibility")]
    pub apiserver_visibility: ApiserverVisibility,

    /// Trust-domain identifier; the agent maps this to a per-tree
    /// Linux VRF so clusters sharing this string can reach each
    /// other while clusters with different strings are isolated at
    /// the kernel routing level. Empty / unset is its own group
    /// (joins other empty-trust_domain clusters, doesn't merge with
    /// named ones).
    ///
    /// CAPI-managed clusters DO NOT set this — the
    /// basis-capi-provider auto-derives one identifier per
    /// management cluster and stamps it on every `BasisCluster`.
    /// This field is the lower-level admin override for direct
    /// `basisctl apply` workflows.
    #[serde(default, rename = "trustDomain")]
    pub trust_domain: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterResource {
    pub metadata: Metadata,
    pub spec: ClusterSpec,
}

#[derive(Debug, Deserialize)]
pub struct MachineSpec {
    /// Name of the Cluster resource this Machine belongs to. The cluster
    /// must already exist (apply the Cluster YAML first); basis-ctl
    /// looks it up by name via the controller's ListClusters RPC.
    pub cluster: String,
    pub cpu: u32,
    #[serde(rename = "memoryMib")]
    pub memory_mib: u32,
    #[serde(rename = "diskGib")]
    pub disk_gib: u32,
    pub image: String,
    /// Path to a file whose contents are sent as cloud-init userdata.
    /// Resolved relative to the YAML file's directory.
    #[serde(default, rename = "bootstrapDataFile")]
    pub bootstrap_data_file: Option<PathBuf>,
    #[serde(default)]
    pub gpus: u32,
    #[serde(default, rename = "minGpuGroupSize")]
    pub min_gpu_group_size: Option<u32>,
    /// Per-disk storage requests. Each disk picks a pool by label
    /// selector; `purpose: rookOsd` activates hierarchical
    /// same-cluster anti-affinity. `purpose: genericData` skips it.
    #[serde(default, rename = "storageDisks")]
    pub storage_disks: Vec<StorageDiskSpec>,
    /// Optional **host** placement (against host labels). Empty =
    /// pick any host that fits. Per-disk pool placement lives on each
    /// `storageDisks[].selector`.
    #[serde(default)]
    pub placement: LabelSelectorSpec,
}

/// One requested data disk, in YAML form.
#[derive(Debug, Deserialize)]
pub struct StorageDiskSpec {
    #[serde(rename = "minSizeGib")]
    pub min_size_gib: u64,
    #[serde(default)]
    pub selector: LabelSelectorSpec,
    pub purpose: DiskPurposeSpec,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiskPurposeSpec {
    Replicated,
    GenericData,
}

impl DiskPurposeSpec {
    fn as_proto(self) -> basis_proto::DiskPurpose {
        match self {
            Self::Replicated => basis_proto::DiskPurpose::Replicated,
            Self::GenericData => basis_proto::DiskPurpose::GenericData,
        }
    }
}

/// Label selector — used for both host placement (against host
/// labels) and pool placement (against pool labels). One YAML shape,
/// one converter.
#[derive(Debug, Default, Deserialize)]
pub struct LabelSelectorSpec {
    #[serde(default)]
    pub requires: Vec<PlacementRequirement>,
    #[serde(default)]
    pub prefers: Vec<PlacementPreference>,
}

#[derive(Debug, Deserialize)]
pub struct PlacementRequirement {
    pub key: String,
    pub values: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct PlacementPreference {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub weight: u32,
}

impl LabelSelectorSpec {
    fn is_empty(&self) -> bool {
        self.requires.is_empty() && self.prefers.is_empty()
    }

    fn to_proto_optional(&self) -> Option<basis_proto::LabelSelector> {
        if self.is_empty() {
            return None;
        }
        Some(self.to_proto())
    }

    fn to_proto(&self) -> basis_proto::LabelSelector {
        basis_proto::LabelSelector {
            requires: self
                .requires
                .iter()
                .map(|r| basis_proto::PlacementRequirement {
                    key: r.key.clone(),
                    values: r.values.clone(),
                })
                .collect(),
            prefers: self
                .prefers
                .iter()
                .map(|p| basis_proto::PlacementPreference {
                    key: p.key.clone(),
                    value: p.value.clone(),
                    weight: p.weight,
                })
                .collect(),
        }
    }
}

impl MachineSpec {
    /// Convert the YAML spec into a [`MachineRequest`] ready for
    /// `BasisClient::create_machine`. `cluster_id` is still a *name* at
    /// this point — the caller resolves it against the controller
    /// before calling.
    pub fn to_request(&self, name: &str, yaml_file: &Path) -> Result<MachineRequest> {
        let bootstrap_data = match &self.bootstrap_data_file {
            Some(rel) => {
                let abs = yaml_file
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(rel);
                fs::read(&abs)
                    .with_context(|| format!("reading bootstrap data file {}", abs.display()))?
            }
            None => Vec::new(),
        };
        Ok(MachineRequest {
            // The CLI's apply path replaces this with the resolved id
            // before calling the gRPC; keep the name here so we have it
            // for the lookup.
            cluster_id: self.cluster.clone(),
            name: name.to_string(),
            cpu: self.cpu,
            memory_mib: self.memory_mib,
            disk_gib: self.disk_gib,
            image: self.image.clone(),
            bootstrap_data,
            gpus: self.gpus,
            min_gpu_group_size: self.min_gpu_group_size,
            storage_disks: self
                .storage_disks
                .iter()
                .map(|d| basis_proto::StorageDisk {
                    min_size_gib: d.min_size_gib,
                    selector: d.selector.to_proto_optional(),
                    purpose: d.purpose.as_proto() as i32,
                })
                .collect(),
            placement: self.placement.to_proto_optional(),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct MachineResource {
    pub metadata: Metadata,
    pub spec: MachineSpec,
}

pub enum Resource {
    Cluster(ClusterResource),
    Machine(MachineResource),
}

/// Each YAML document starts with `apiVersion` + `kind`; we peek at
/// those before picking the full typed deserializer.
#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
}

pub fn load_file(path: &Path) -> Result<Vec<Resource>> {
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for doc in serde_yaml_ng::Deserializer::from_str(&body) {
        let value: serde_yaml_ng::Value = serde_yaml_ng::Value::deserialize(doc)
            .with_context(|| format!("parsing YAML in {}", path.display()))?;
        if value.is_null() {
            continue;
        }
        let env: Envelope = serde_yaml_ng::from_value(value.clone())
            .with_context(|| format!("missing apiVersion/kind in {}", path.display()))?;
        if env.api_version != API_VERSION {
            bail!(
                "{}: unsupported apiVersion '{}' (expected '{}')",
                path.display(),
                env.api_version,
                API_VERSION
            );
        }
        out.push(match env.kind.as_str() {
            "Cluster" => Resource::Cluster(serde_yaml_ng::from_value(value)?),
            "Machine" => Resource::Machine(serde_yaml_ng::from_value(value)?),
            other => bail!("{}: unknown kind '{}'", path.display(), other),
        });
    }
    Ok(out)
}
