use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AgentConfig {
    pub controller_endpoint: String,
    pub data_dir: PathBuf,
    pub network: NetworkConfig,
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub bridge: String,
    pub physical_nic: String,
}

#[derive(Debug, Deserialize)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

impl AgentConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn images_dir(&self) -> PathBuf {
        self.data_dir.join("images")
    }

    pub fn vms_dir(&self) -> PathBuf {
        self.data_dir.join("vms")
    }
}
