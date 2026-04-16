use std::path::{Path, PathBuf};

use tokio::process::Command;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("image pull failed: {0}")]
    PullFailed(String),

    #[error("overlay creation failed: {0}")]
    OverlayFailed(String),

    #[error("cloud-init ISO creation failed: {0}")]
    CloudInitFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct ImageManager {
    images_dir: PathBuf,
}

impl ImageManager {
    pub fn new(images_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&images_dir).ok();
        Self { images_dir }
    }

    /// Ensure the base image is cached locally. Returns the path to the cached base image.
    pub async fn ensure_cached(&self, image_ref: &str) -> Result<PathBuf, ImageError> {
        let cache_name = image_ref_to_filename(image_ref);
        let cached_path = self.images_dir.join(&cache_name);

        if cached_path.exists() {
            info!(image = %image_ref, path = %cached_path.display(), "image already cached");
            return Ok(cached_path);
        }

        info!(image = %image_ref, "pulling image");

        if image_ref.starts_with("http://") || image_ref.starts_with("https://") {
            self.pull_http(image_ref, &cached_path).await?;
        } else {
            self.pull_oci(image_ref, &cached_path).await?;
        }

        Ok(cached_path)
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

        // Write userdata
        std::fs::write(cidata_dir.join("user-data"), userdata)?;

        // Write meta-data (minimal)
        std::fs::write(
            cidata_dir.join("meta-data"),
            "instance-id: basis\nlocal-hostname: basis\n",
        )?;

        // Write network-config
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

        // Create ISO
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
            _ => {
                Command::new("genisoimage")
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
                    .map_err(|e| ImageError::CloudInitFailed(e.to_string()))?
            }
        };

        if !output.status.success() {
            return Err(ImageError::CloudInitFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        // Clean up cidata dir, keep only the ISO
        std::fs::remove_dir_all(&cidata_dir).ok();

        info!(path = %iso_path.display(), "created cloud-init ISO");
        Ok(iso_path)
    }

    async fn pull_http(&self, url: &str, dest: &Path) -> Result<(), ImageError> {
        let output = Command::new("curl")
            .args(["-fSL", "-o", &dest.to_string_lossy(), url])
            .output()
            .await
            .map_err(|e| ImageError::PullFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(ImageError::PullFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        Ok(())
    }

    async fn pull_oci(&self, image_ref: &str, dest: &Path) -> Result<(), ImageError> {
        // Use skopeo to pull OCI image, then extract the qcow2 layer
        let oci_dir = dest.with_extension("oci");
        let output = Command::new("skopeo")
            .args([
                "copy",
                &format!("docker://{image_ref}"),
                &format!("oci:{}:latest", oci_dir.to_string_lossy()),
            ])
            .output()
            .await
            .map_err(|e| ImageError::PullFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(ImageError::PullFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }

        // Extract the first blob as the disk image (convention: single-layer OCI artifact)
        let blobs_dir = oci_dir.join("blobs").join("sha256");
        if let Ok(mut entries) = std::fs::read_dir(&blobs_dir) {
            // Find the largest blob (the disk image)
            let mut largest: Option<(PathBuf, u64)> = None;
            while let Some(Ok(entry)) = entries.next() {
                if let Ok(meta) = entry.metadata() {
                    match &largest {
                        Some((_, size)) if meta.len() <= *size => {}
                        _ => largest = Some((entry.path(), meta.len())),
                    }
                }
            }
            if let Some((blob_path, _)) = largest {
                std::fs::rename(&blob_path, dest)?;
            }
        }

        // Clean up OCI dir
        std::fs::remove_dir_all(&oci_dir).ok();

        Ok(())
    }
}

/// Convert an image reference to a safe filename for the cache.
fn image_ref_to_filename(image_ref: &str) -> String {
    image_ref
        .replace('/', "_")
        .replace(':', "_")
        .replace('.', "_")
        + ".qcow2"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_ref_to_filename_oci() {
        let name = image_ref_to_filename("ghcr.io/lattos/lattice-node:v1.32.0");
        assert_eq!(name, "ghcr_io_lattos_lattice-node_v1_32_0.qcow2");
        assert!(!name.contains('/'));
        assert!(!name.contains(':'));
    }

    #[test]
    fn test_image_ref_to_filename_http() {
        let name = image_ref_to_filename("https://example.com/images/node.qcow2");
        assert!(!name.contains('/'));
        assert!(!name.contains(':'));
        assert!(name.ends_with(".qcow2"));
    }

    #[test]
    fn test_image_ref_to_filename_deterministic() {
        let a = image_ref_to_filename("test:latest");
        let b = image_ref_to_filename("test:latest");
        assert_eq!(a, b);
    }
}
