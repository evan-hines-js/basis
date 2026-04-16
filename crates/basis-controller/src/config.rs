use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ControllerConfig {
    pub listen: String,
    pub data_dir: PathBuf,
    pub tls: TlsConfig,
    #[serde(default)]
    pub ip_pools: Vec<IpPoolConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IpPoolConfig {
    pub name: String,
    pub cidr: String,
    pub gateway: String,
    pub range_start: String,
    pub range_end: String,
}

impl ControllerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("controller.db")
    }
}
