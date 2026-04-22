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
//!
//! Host-tool contract: the manager shells out to `qemu-img` and an
//! ISO-9660 producer (`mkisofs` or `genisoimage`). Which producer is
//! used is resolved once via [`validate_tools`] at agent startup and
//! baked into the manager — no runtime fallback — so an operator can't
//! end up with two machines silently using different tool versions.

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

use crate::lvm;

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("invalid image reference '{0}': {1}")]
    BadReference(String, String),

    #[error("image pull failed: {0}")]
    PullFailed(String),

    #[error("image manifest missing required layer with media type '{0}'")]
    MissingLayer(&'static str),

    #[error("cloud-init ISO creation failed: {0}")]
    CloudInitFailed(String),

    #[error("lvm: {0}")]
    Lvm(#[from] lvm::LvmError),

    #[error("qemu-img info failed: {0}")]
    ImageInfo(String),

    #[error(
        "creating images directory {path}: {source} — check that spec.dataDir in host.yaml \
         points at a writable filesystem and that basis-prereqs has run on this host"
    )]
    ImagesDirUnwritable {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "no ISO-9660 producer found on $PATH: {0} — install `genisoimage` (Debian/Ubuntu) or \
         `cdrkit-genisoimage` / `xorriso` (EL/Fedora); basis-prereqs ansible role normally \
         handles this"
    )]
    IsoToolMissing(String),

    #[error(
        "qemu-img not found on $PATH: {0} — install `qemu-utils` (Debian/Ubuntu) or \
         `qemu-img` (EL/Fedora); basis-prereqs ansible role normally handles this"
    )]
    QemuImgMissing(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Media types attached to each layer of a basis node-image artifact by
/// `scripts/build-node-image.sh`.
const MEDIA_TYPE_QCOW2: &str = "application/vnd.lattice.node.v1+qcow2";
const MEDIA_TYPE_KERNEL: &str = "application/vnd.lattice.node.v1+kernel";
const MEDIA_TYPE_INITRD: &str = "application/vnd.lattice.node.v1+initrd";

/// A cached node image. `kernel` and `initrd` are file paths
/// cloud-hypervisor boots directly; `image_hash` is the stable prefix
/// used to name the golden LV in the LVM thin pool — per-VM rootfs LVs
/// are thin snapshots of `/dev/basis/image-<image_hash>`.
pub struct CachedImage {
    pub image_hash: String,
    pub kernel: PathBuf,
    pub initrd: PathBuf,
}

/// Name of the ISO-9660 producer resolved at startup. `mkisofs` and
/// `genisoimage` accept the same flags for the subset of options we use
/// (`-output`, `-volid`, `-joliet`, `-rock`), so only the binary name
/// varies. Storing the resolved name in the manager keeps every ISO
/// creation deterministic — no silent tool switch between VMs.
#[derive(Debug, Clone)]
pub struct IsoTool(&'static str);

impl IsoTool {
    pub fn command(&self) -> &'static str {
        self.0
    }
}

/// Resolve host-side tools the manager depends on. Call at agent startup
/// before `ImageManager::new`; failure here should abort the agent rather
/// than let VM creation fail per-request with a confusing error.
///
/// Returns the chosen ISO producer. Deterministic preference order:
///   1. `genisoimage` — default on Debian/Ubuntu, the platform the
///      basis-prereqs role actually installs.
///   2. `mkisofs` — still available on a few distros as a cdrtools
///      symlink to genisoimage.
///
/// Also confirms `qemu-img` is on $PATH, which is needed for qcow2→raw
/// conversion into the LVM thin pool and for reading qcow2 virtual size.
pub async fn validate_tools() -> Result<IsoTool, ImageError> {
    use tokio::process::Command;

    async fn have(cmd: &str) -> bool {
        Command::new(cmd)
            .arg("--version")
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    if !have("qemu-img").await {
        return Err(ImageError::QemuImgMissing(
            "`qemu-img --version` did not succeed".to_string(),
        ));
    }

    let tool = if have("genisoimage").await {
        IsoTool("genisoimage")
    } else if have("mkisofs").await {
        IsoTool("mkisofs")
    } else {
        return Err(ImageError::IsoToolMissing(
            "tried genisoimage, mkisofs".to_string(),
        ));
    };
    info!(tool = tool.command(), "ISO producer resolved");
    Ok(tool)
}

pub struct ImageManager {
    images_dir: PathBuf,
    /// Per-registry credentials, keyed by registry host (e.g., "ghcr.io").
    /// Empty map means every pull is anonymous.
    auth: HashMap<String, RegistryAuth>,
    /// ISO-9660 producer resolved at startup. Fixed for the lifetime of
    /// the agent — see [`validate_tools`].
    iso_tool: IsoTool,
    /// Per-image-ref locks. When N CreateVm commands arrive at once for
    /// the same image, one winner takes the lock and pulls; the others
    /// await it, find the cache populated, and return without touching
    /// the network or the shared `.partial` side files.
    pull_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl ImageManager {
    pub fn new(images_dir: PathBuf, iso_tool: IsoTool) -> Result<Self, ImageError> {
        Self::with_auth(images_dir, HashMap::new(), iso_tool)
    }

    pub fn with_auth(
        images_dir: PathBuf,
        auth: HashMap<String, RegistryAuth>,
        iso_tool: IsoTool,
    ) -> Result<Self, ImageError> {
        std::fs::create_dir_all(&images_dir).map_err(|e| ImageError::ImagesDirUnwritable {
            path: images_dir.clone(),
            source: e,
        })?;
        Ok(Self {
            images_dir,
            auth,
            iso_tool,
            pull_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Ensure the kernel, initrd, and golden rootfs LV for `image_ref`
    /// are ready locally. Pulls missing layers, decompresses the qcow2,
    /// and converts it once into a raw thin LV at `/dev/basis/image-<hash>`
    /// that per-VM snapshots branch from. Concurrent callers for the
    /// same `image_ref` serialize on a per-ref lock so only one
    /// pull+convert runs.
    pub async fn ensure_cached(&self, image_ref: &str) -> Result<CachedImage, ImageError> {
        let prefix = image_ref_to_prefix(image_ref);
        let rootfs = self.images_dir.join(format!("{prefix}.qcow2"));
        let kernel = self.images_dir.join(format!("{prefix}.vmlinuz"));
        let initrd = self.images_dir.join(format!("{prefix}.initrd"));

        // Fast path: qcow2 download + golden LV both already ready.
        // Exists-checks are racy against a concurrent puller; the
        // per-image lock below is the real serialization.
        if rootfs.exists()
            && kernel.exists()
            && initrd.exists()
            && lvm::image_lv_path(&prefix).exists()
        {
            return Ok(CachedImage {
                image_hash: prefix,
                kernel,
                initrd,
            });
        }

        let lock = self.lock_for(image_ref).await;
        let _guard = lock.lock().await;

        // Pull any missing OCI layers (qcow2/kernel/initrd). No-op if
        // an earlier puller already populated them.
        if !rootfs.exists() || !kernel.exists() || !initrd.exists() {
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
        }

        // Convert qcow2 → raw into a golden thin LV. Idempotent: if the
        // LV is already populated (marked RO), this is a no-op.
        let virtual_size_gib = qcow2_virtual_size_gib(&rootfs).await?;
        lvm::ensure_image_lv(&prefix, &rootfs, virtual_size_gib).await?;

        Ok(CachedImage {
            image_hash: prefix,
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
        // Use the tool resolved at startup (see `validate_tools`). No
        // runtime fallback: silently switching producers between VMs
        // would make incident debugging miserable.
        let output = Command::new(self.iso_tool.command())
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
            .map_err(|e| {
                ImageError::CloudInitFailed(format!(
                    "spawning {}: {e}",
                    self.iso_tool.command()
                ))
            })?;

        if !output.status.success() {
            return Err(ImageError::CloudInitFailed(format!(
                "{} failed: {}",
                self.iso_tool.command(),
                String::from_utf8_lossy(&output.stderr)
            )));
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

/// Read the qcow2's declared virtual size in GiB (rounded up). Used to
/// size the golden LV so per-VM snapshots inherit the image's layout.
async fn qcow2_virtual_size_gib(qcow2: &Path) -> Result<u64, ImageError> {
    let out = Command::new("qemu-img")
        .args(["info", "--output=json", &qcow2.to_string_lossy()])
        .output()
        .await
        .map_err(|e| ImageError::ImageInfo(e.to_string()))?;
    if !out.status.success() {
        return Err(ImageError::ImageInfo(
            String::from_utf8_lossy(&out.stderr).to_string(),
        ));
    }
    let v: serde_json::Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| ImageError::ImageInfo(format!("parsing qemu-img info output: {e}")))?;
    let bytes = v["virtual-size"]
        .as_u64()
        .ok_or_else(|| ImageError::ImageInfo("virtual-size missing from qemu-img info".into()))?;
    // Round up to whole GiB — lvcreate --virtualsize takes integer gigabytes.
    Ok(bytes.div_ceil(1 << 30))
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
