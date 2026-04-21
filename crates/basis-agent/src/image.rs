//! VM disk image management.
//!
//! Node images are published as three-layer OCI artifacts (see
//! `scripts/build-node-image.sh`): a qcow2 rootfs, a Linux bzImage
//! kernel, and a matching initrd. Cloud-hypervisor's minimal firmware
//! (rust-hypervisor-firmware) doesn't implement the UEFI variable / TPM
//! surface Ubuntu's shim+grub depend on, so we skip the EFI chain and
//! boot the guest kernel directly (see `vm.rs`).
//!
//! The agent pulls all three layers with `oci-client` and caches them
//! alongside each other, keyed by media type. Layers are streamed to
//! `.partial` side files and atomically renamed so a failed or
//! interrupted pull never leaves a truncated cache entry.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::TryStreamExt;
use oci_client::client::ClientConfig;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("invalid image reference '{0}': {1}")]
    BadReference(String, String),

    #[error("image pull failed: {0}")]
    PullFailed(String),

    #[error("image manifest missing required layer with media type '{0}'")]
    MissingLayer(&'static str),

    #[error("overlay creation failed: {0}")]
    OverlayFailed(String),

    #[error("cloud-init ISO creation failed: {0}")]
    CloudInitFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Media types attached to each layer of a basis node-image artifact by
/// `scripts/build-node-image.sh`.
const MEDIA_TYPE_QCOW2: &str = "application/vnd.lattice.node.v1+qcow2";
const MEDIA_TYPE_KERNEL: &str = "application/vnd.lattice.node.v1+kernel";
const MEDIA_TYPE_INITRD: &str = "application/vnd.lattice.node.v1+initrd";

/// Paths to a node image's three cached artifacts on disk.
pub struct CachedImage {
    pub rootfs: PathBuf,
    pub kernel: PathBuf,
    pub initrd: PathBuf,
}

pub struct ImageManager {
    images_dir: PathBuf,
    /// Per-registry credentials, keyed by registry host (e.g., "ghcr.io").
    /// Empty map means every pull is anonymous.
    auth: HashMap<String, RegistryAuth>,
    /// Per-image-ref locks. When N CreateVm commands arrive at once for
    /// the same image, one winner takes the lock and pulls; the others
    /// await it, find the cache populated, and return without touching
    /// the network or the shared `.partial` side files.
    pull_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl ImageManager {
    pub fn new(images_dir: PathBuf) -> Self {
        Self::with_auth(images_dir, HashMap::new())
    }

    pub fn with_auth(images_dir: PathBuf, auth: HashMap<String, RegistryAuth>) -> Self {
        std::fs::create_dir_all(&images_dir).ok();
        Self {
            images_dir,
            auth,
            pull_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Ensure the rootfs, kernel, and initrd for `image_ref` are cached
    /// locally. Fetches any missing layer; no-op if all three are
    /// already present. Concurrent callers for the same `image_ref`
    /// serialize on a per-ref lock so only one pull runs.
    pub async fn ensure_cached(&self, image_ref: &str) -> Result<CachedImage, ImageError> {
        let prefix = image_ref_to_prefix(image_ref);
        let rootfs = self.images_dir.join(format!("{prefix}.qcow2"));
        let kernel = self.images_dir.join(format!("{prefix}.vmlinuz"));
        let initrd = self.images_dir.join(format!("{prefix}.initrd"));

        // Fast path: everything cached. Exists-check is racy against a
        // concurrent puller still writing a `.partial`, but that's what
        // the per-image lock below is for — if any file is missing we
        // take the lock and re-check under it.
        if rootfs.exists() && kernel.exists() && initrd.exists() {
            return Ok(CachedImage {
                rootfs,
                kernel,
                initrd,
            });
        }

        let lock = self.lock_for(image_ref).await;
        let _guard = lock.lock().await;

        // Re-check under the lock. If we raced an earlier puller, the
        // cache is now populated and we return without hitting the
        // network.
        if rootfs.exists() && kernel.exists() && initrd.exists() {
            return Ok(CachedImage {
                rootfs,
                kernel,
                initrd,
            });
        }

        info!(image = %image_ref, "pulling image");
        self.pull_oci(
            image_ref,
            &[
                (MEDIA_TYPE_QCOW2, rootfs.as_path()),
                (MEDIA_TYPE_KERNEL, kernel.as_path()),
                (MEDIA_TYPE_INITRD, initrd.as_path()),
            ],
        )
        .await?;
        Ok(CachedImage {
            rootfs,
            kernel,
            initrd,
        })
    }

    /// Get or create the lock for an image ref. The map grows one entry
    /// per distinct ref the agent has ever pulled — bounded by the
    /// number of node-image tags the deploy uses in practice (typically
    /// one per k8s minor version), so no reaping is needed.
    async fn lock_for(&self, image_ref: &str) -> Arc<Mutex<()>> {
        let mut locks = self.pull_locks.lock().await;
        locks
            .entry(image_ref.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn pull_oci(
        &self,
        image_ref: &str,
        targets: &[(&'static str, &Path)],
    ) -> Result<(), ImageError> {
        let reference: Reference = image_ref.parse().map_err(|e: oci_client::ParseError| {
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

        for (media_type, dest) in targets {
            if dest.exists() {
                continue;
            }
            let layer = manifest
                .layers
                .iter()
                .find(|l| l.media_type == *media_type)
                .ok_or(ImageError::MissingLayer(media_type))?;

            info!(media_type = %media_type, dest = %dest.display(), size = layer.size, "pulling layer");
            // Stream to a `.partial` side file, then atomically rename so
            // a failed pull never leaves a truncated cache entry that a
            // later run mistakes for valid.
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

            if *media_type == MEDIA_TYPE_QCOW2 {
                // Ubuntu's cloud image ships qcow2 with compressed clusters.
                // Small on the registry (~600MB) but cloud-hypervisor can't
                // read compressed clusters at runtime — `qemu-img convert`
                // without `-c` rewrites them uncompressed at the cache path.
                decompress_qcow2_in_place(&tmp, dest).await?;
                tokio::fs::remove_file(&tmp).await.ok();
            } else {
                tokio::fs::rename(&tmp, dest).await?;
            }
        }
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
    ///
    /// `instance_id` must be unique per VM: kubeadm's kubelet arg
    /// `provider-id=basis://{{ ds.meta_data.instance_id }}` expands from
    /// this, so the value has to match what
    /// `basis-controller::provider_id()` returns after the `basis://`
    /// scheme. Callers pass the basis VM id.
    ///
    /// `hostname` sets the guest's `local-hostname` so every VM's Node
    /// object has a distinct name; a shared hostname makes the cluster
    /// join the second node over the first.
    pub async fn create_cloud_init_iso(
        &self,
        vm_dir: &Path,
        instance_id: &str,
        hostname: &str,
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
            format!("instance-id: {instance_id}\nlocal-hostname: {hostname}\n"),
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

/// Convert an image reference to a safe filename stem for the cache.
/// The three layers share this stem with different extensions — see
/// `ensure_cached`.
fn image_ref_to_prefix(image_ref: &str) -> String {
    image_ref.replace(['/', ':', '.'], "_")
}

/// Run `qemu-img convert -O qcow2 src dst`, which rewrites compressed
/// clusters as uncompressed (no `-c` flag passed) so cloud-hypervisor
/// can read every cluster at runtime.
async fn decompress_qcow2_in_place(src: &Path, dst: &Path) -> Result<(), ImageError> {
    let status = Command::new("qemu-img")
        .args([
            "convert",
            "-f",
            "qcow2",
            "-O",
            "qcow2",
            &src.to_string_lossy(),
            &dst.to_string_lossy(),
        ])
        .status()
        .await
        .map_err(|e| ImageError::PullFailed(format!("qemu-img spawn: {e}")))?;
    if !status.success() {
        tokio::fs::remove_file(dst).await.ok();
        return Err(ImageError::PullFailed(
            "qemu-img convert failed stripping qcow2 compression".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_filename_safe() {
        let p = image_ref_to_prefix("ghcr.io/evan-hines-js/lattice-node:v1.32.0");
        assert_eq!(p, "ghcr_io_evan-hines-js_lattice-node_v1_32_0");
        assert!(!p.contains('/'));
        assert!(!p.contains(':'));
        assert!(!p.contains('.'));
    }

    #[test]
    fn prefix_is_deterministic() {
        assert_eq!(
            image_ref_to_prefix("test:latest"),
            image_ref_to_prefix("test:latest"),
        );
    }
}
