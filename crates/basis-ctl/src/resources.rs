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
use serde::Deserialize;

pub const API_VERSION: &str = "basis.dev/v1";

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ClusterSpec {
    #[serde(rename = "ipPool")]
    pub ip_pool: String,
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
