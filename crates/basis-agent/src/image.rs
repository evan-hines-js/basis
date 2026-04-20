//! VM disk image management.
//!
//! Disk images are published as single-layer OCI artifacts (see
//! `scripts/build-node-image.sh` which uses `oras push` with media type
//! `application/vnd.lattice.node.v1+qcow2`). The agent pulls them with
//! the `oci-client` crate — a native-Rust OCI v2 client that handles
//! token auth and streams blobs to disk, so there's no external binary
//! to depend on and no in-memory buffering of multi-GB images.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use futures::TryStreamExt;
use oci_client::client::ClientConfig;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("invalid image reference '{0}': {1}")]
    BadReference(String, String),

    #[error("image pull failed: {0}")]
    PullFailed(String),

    #[error("image manifest has no layers")]
    EmptyManifest,

    #[error("overlay creation failed: {0}")]
    OverlayFailed(String),

    #[error("cloud-init ISO creation failed: {0}")]
    CloudInitFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct ImageManager {
    images_dir: PathBuf,
    /// Per-registry credentials, keyed by registry host (e.g., "ghcr.io").
    /// Empty map means every pull is anonymous.
    auth: HashMap<String, RegistryAuth>,
}

impl ImageManager {
    pub fn new(images_dir: PathBuf) -> Self {
        Self::with_auth(images_dir, HashMap::new())
    }

    pub fn with_auth(images_dir: PathBuf, auth: HashMap<String, RegistryAuth>) -> Self {
        std::fs::create_dir_all(&images_dir).ok();
        Self { images_dir, auth }
    }

    /// Ensure the base image is cached locally. Returns the path to the
    /// cached base image.
    pub async fn ensure_cached(&self, image_ref: &str) -> Result<PathBuf, ImageError> {
        let cache_name = image_ref_to_filename(image_ref);
        let cached_path = self.images_dir.join(&cache_name);

        if cached_path.exists() {
            info!(image = %image_ref, path = %cached_path.display(), "image already cached");
            return Ok(cached_path);
        }

        info!(image = %image_ref, "pulling image");
        self.pull_oci(image_ref, &cached_path).await?;
        Ok(cached_path)
    }

    async fn pull_oci(&self, image_ref: &str, dest: &Path) -> Result<(), ImageError> {
        let reference: Reference = image_ref
            .parse()
            .map_err(|e: oci_client::ParseError| {
                ImageError::BadReference(image_ref.to_string(), e.to_string())
            })?;
        let auth = self
            .auth
            .get(reference.registry())
            .cloned()
            .unwrap_or(RegistryAuth::Anonymous);

        let client = Client::new(ClientConfig::default());
        let (manifest, _digest) = client
            .pull_image_manifest(&reference, &auth)
            .await
            .map_err(|e| ImageError::PullFailed(format!("fetching manifest: {e}")))?;

        let layer = manifest.layers.first().ok_or(ImageError::EmptyManifest)?;

        // Stream the blob straight to disk via a temp file so a failed
        // pull never leaves a truncated cache entry that a later run
        // mistakes for a valid image.
        let tmp = dest.with_extension("partial");
        let mut out = tokio::fs::File::create(&tmp).await?;
        let mut stream = client
            .pull_blob_stream(&reference, layer)
            .await
            .map_err(|e| ImageError::PullFailed(format!("fetching blob: {e}")))?;
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|e| ImageError::PullFailed(format!("reading blob: {e}")))?
        {
            out.write_all(&chunk).await?;
        }
        out.flush().await?;
        drop(out);
        tokio::fs::rename(&tmp, dest).await?;
        Ok(())
    }

    /// Create a qcow2 copy-on-write overlay backed by the base image.
    pub async fn create_overlay(
        &self,
        base_image: &Path,
        vm_dir: &Path,
        disk_gib: u32,
    ) -> Result<PathBuf, ImageError> {
        std::fs::create_dir_all(vm_dir)?;
        let overlay_path = vm_dir.join("disk.qcow2");

        let output = Command::new("qemu-img")
            .args([
                "create",
                "-f",
                "qcow2",
                "-F",
                "qcow2",
                "-b",
                &base_image.to_string_lossy(),
                &overlay_path.to_string_lossy(),
                &format!("{disk_gib}G"),
            ])
            .output()
            .await
            .map_err(|e| ImageError::OverlayFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(ImageError::OverlayFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        info!(path = %overlay_path.display(), "created qcow2 overlay");
        Ok(overlay_path)
    }

    /// Create a cloud-init ISO (cidata) with network config and userdata.
    pub async fn create_cloud_init_iso(
        &self,
        vm_dir: &Path,
        userdata: &[u8],
        ip_address: &str,
        gateway: &str,
        prefix_len: u32,
        dns_servers: &[String],
    ) -> Result<PathBuf, ImageError> {
        let cidata_dir = vm_dir.join("cidata");
        std::fs::create_dir_all(&cidata_dir)?;

        std::fs::write(cidata_dir.join("user-data"), userdata)?;
        std::fs::write(
            cidata_dir.join("meta-data"),
            "instance-id: basis\nlocal-hostname: basis\n",
        )?;

        let dns_entries: String = dns_servers
            .iter()
            .map(|s| format!("          - {s}"))
            .collect::<Vec<_>>()
            .join("\n");

        let network_config = format!(
            r#"network:
  version: 2
  ethernets:
    ens3:
      addresses:
        - {ip_address}/{prefix_len}
      gateway4: {gateway}
      nameservers:
        addresses:
{dns_entries}
"#
        );
        std::fs::write(cidata_dir.join("network-config"), &network_config)?;

        let iso_path = vm_dir.join("cidata.iso");
        let output = Command::new("mkisofs")
            .args([
                "-output",
                &iso_path.to_string_lossy(),
                "-volid",
                "cidata",
                "-joliet",
                "-rock",
                &cidata_dir.to_string_lossy(),
            ])
            .output()
            .await;

        // Fallback to genisoimage if mkisofs not available
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => Command::new("genisoimage")
                .args([
                    "-output",
                    &iso_path.to_string_lossy(),
                    "-volid",
                    "cidata",
                    "-joliet",
                    "-rock",
                    &cidata_dir.to_string_lossy(),
                ])
                .output()
                .await
                .map_err(|e| ImageError::CloudInitFailed(e.to_string()))?,
        };

        if !output.status.success() {
            return Err(ImageError::CloudInitFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        std::fs::remove_dir_all(&cidata_dir).ok();

        info!(path = %iso_path.display(), "created cloud-init ISO");
        Ok(iso_path)
    }
}

/// Convert an image reference to a safe filename for the cache.
fn image_ref_to_filename(image_ref: &str) -> String {
    image_ref.replace(['/', ':', '.'], "_") + ".qcow2"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_ref_to_filename_oci() {
        let name = image_ref_to_filename("ghcr.io/evan-hines-js/lattice-node:v1.32.0");
        assert_eq!(name, "ghcr_io_evan-hines-js_lattice-node_v1_32_0.qcow2");
        assert!(!name.contains('/'));
        assert!(!name.contains(':'));
    }

    #[test]
    fn test_image_ref_to_filename_deterministic() {
        let a = image_ref_to_filename("test:latest");
        let b = image_ref_to_filename("test:latest");
        assert_eq!(a, b);
    }
}
