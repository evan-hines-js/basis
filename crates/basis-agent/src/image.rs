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
use std::time::Duration;

use futures::TryStreamExt;
use oci_client::client::ClientConfig;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::info;

use crate::lvm::{self, Storage};

/// TCP-level connect deadline to the registry. Default is the OS
/// setting (~75s of SYN retries on Linux) which is far too long for a
/// reachable-or-not probe. 10s is enough to tolerate a slow handshake
/// without hanging the create path when the registry is unreachable.
const OCI_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard timeout on the manifest fetch. Manifests are small (a few KB)
/// so a healthy fetch completes in under a second; anything past 30s
/// is either a slow-loris rate-limit or a non-existent repo that the
/// registry is taking its time 404-ing. Callers retry on failure, so
/// this just caps the tail.
const OCI_MANIFEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Idle timeout between successive blob chunks. Applied per-chunk so a
/// legitimately-large pull (hundreds of MB) isn't killed for taking
/// minutes as long as bytes keep arriving. Once no bytes arrive for
/// this long we abort — a stalled stream is indistinguishable from a
/// dropped connection.
const OCI_BLOB_CHUNK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("invalid image reference '{0}': {1}")]
    BadReference(String, String),

    #[error("image pull failed: {0}")]
    PullFailed(String),

    #[error("image manifest missing required layer with media type '{0}'")]
    MissingLayer(&'static str),

    #[error(
        "digest mismatch on layer '{media_type}': manifest said {expected}, \
         pulled bytes hashed to {actual}"
    )]
    DigestMismatch {
        media_type: &'static str,
        expected: String,
        actual: String,
    },

    #[error(
        "unsupported digest algorithm '{0}' — basis only verifies sha256 \
         (the OCI default). Either republish with sha256 or extend `verify_digest`"
    )]
    UnsupportedDigest(String),

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
/// `virtual_size_gib` is the qcow2's declared virtual size, exposed
/// here so the create path can reject sub-image disk requests up
/// front (you can't shrink an LV out from under a guest's filesystem
/// — the LV-level tolerance in `lvm::lvextend` rounds up silently,
/// but the API boundary should also fail loud on operator error).
pub struct CachedImage {
    pub image_hash: String,
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    pub virtual_size_gib: u64,
}

/// Guest-side primary-NIC configuration (the tree-side NIC).
///
/// `mac` matches what `vm::primary_mac(vm_id)` returns and what
/// the basis-agent passed on cloud-hypervisor's `--net mac=` arg.
/// netplan binds this stanza to the NIC carrying that MAC, so the
/// guest's kernel-assigned interface name (`ens3` / `ens4` / etc) is
/// irrelevant — cloud-hypervisor's PCI slot ordering can shift
/// without breaking network config.
pub struct GuestNetwork<'a> {
    pub mac: &'a str,
    pub ip_address: &'a str,
    pub gateway: &'a str,
    pub prefix_len: u32,
    pub dns_servers: &'a [String],
    /// Inner-MTU for the cluster overlay NIC. Must equal
    /// `uplink_mtu - 50` (VXLAN header overhead) so guest IP packets
    /// fit in a single underlay frame. Without this the guest
    /// defaults to 1500, large packets silently drop on the bridge,
    /// and overlay TLS handshakes hang at ClientHello.
    pub mtu: u32,
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
    /// and converts it once into a raw thin LV in the rootfs VG that
    /// per-VM snapshots branch from. Concurrent callers for the same
    /// `image_ref` serialize on a per-ref lock so only one pull+convert
    /// runs.
    pub async fn ensure_cached(
        &self,
        image_ref: &str,
        storage: &Storage,
    ) -> Result<CachedImage, ImageError> {
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
            && storage.image_lv_path(&prefix).exists()
        {
            // qemu-img info is local file I/O + JSON parse, sub-100ms
            // on a populated qcow2 — cheap enough to call on every
            // ensure rather than caching the size separately.
            let virtual_size_gib = qcow2_virtual_size_gib(&rootfs).await?;
            return Ok(CachedImage {
                image_hash: prefix,
                kernel,
                initrd,
                virtual_size_gib,
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
        storage
            .ensure_image_lv(&prefix, &rootfs, virtual_size_gib)
            .await?;

        Ok(CachedImage {
            image_hash: prefix,
            kernel,
            initrd,
            virtual_size_gib,
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

        let client = Client::new(ClientConfig {
            connect_timeout: Some(OCI_CONNECT_TIMEOUT),
            ..ClientConfig::default()
        });

        let manifest = tokio::time::timeout(
            OCI_MANIFEST_TIMEOUT,
            client.pull_image_manifest(&reference, &auth),
        )
        .await
        .map_err(|_| {
            ImageError::PullFailed(format!(
                "fetching manifest for '{image_ref}': timed out after {OCI_MANIFEST_TIMEOUT:?}"
            ))
        })?
        .map(|(m, _digest)| m)
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
            // Stream to a `.partial` side file, hashing as we go, then
            // check the manifest digest before atomically moving the
            // bytes into place. Not a security boundary — HTTPS covers
            // the wire and OCI's content-addressed blob store covers
            // the registry — this is an integrity check that catches
            // truncated streams, silent I/O corruption, and oci-client
            // bugs before the bytes become a visible cache entry.
            let tmp = dest.with_extension("partial");
            let mut out = tokio::fs::File::create(&tmp).await?;
            let mut hasher = Sha256::new();
            let mut stream = tokio::time::timeout(
                OCI_MANIFEST_TIMEOUT,
                client.pull_blob_stream(&reference, layer),
            )
            .await
            .map_err(|_| {
                ImageError::PullFailed(format!(
                    "initiating blob pull for '{media_type}': timed out after {OCI_MANIFEST_TIMEOUT:?}"
                ))
            })?
            .map_err(|e| ImageError::PullFailed(format!("fetching blob: {e}")))?;
            // Per-chunk stall timeout: the stream must make progress at
            // least once per `OCI_BLOB_CHUNK_TIMEOUT` or we abort.
            // Applied inside the loop so a legitimately-long pull (lots
            // of chunks) is bounded only by its actual transfer time.
            loop {
                match tokio::time::timeout(OCI_BLOB_CHUNK_TIMEOUT, stream.try_next()).await {
                    Err(_) => {
                        return Err(ImageError::PullFailed(format!(
                            "blob stream for '{media_type}' stalled — no bytes in {OCI_BLOB_CHUNK_TIMEOUT:?}"
                        )));
                    }
                    Ok(Err(e)) => {
                        return Err(ImageError::PullFailed(format!("reading blob: {e}")));
                    }
                    Ok(Ok(None)) => break,
                    Ok(Ok(Some(chunk))) => {
                        hasher.update(&chunk);
                        out.write_all(&chunk).await?;
                    }
                }
            }
            out.flush().await?;
            drop(out);

            if let Err(e) = verify_digest(media_type, &layer.digest, hasher.finalize().as_slice()) {
                // Don't leave a tainted `.partial` around to seed a
                // later pull's fast path.
                tokio::fs::remove_file(&tmp).await.ok();
                return Err(e);
            }

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
        primary: &GuestNetwork<'_>,
    ) -> Result<PathBuf, ImageError> {
        let cidata_dir = vm_dir.join("cidata");
        std::fs::create_dir_all(&cidata_dir)?;

        std::fs::write(cidata_dir.join("user-data"), userdata)?;
        std::fs::write(
            cidata_dir.join("meta-data"),
            format!("instance-id: {instance_id}\nlocal-hostname: {hostname}\n"),
        )?;

        std::fs::write(cidata_dir.join("network-config"), network_config(primary))?;

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
                ImageError::CloudInitFailed(format!("spawning {}: {e}", self.iso_tool.command()))
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

/// Render the cloud-init netplan v2 network-config. The single
/// ethernet stanza uses `match: { macaddress: ... }` rather than a
/// literal kernel name (`ens3` / etc) — cloud-hypervisor's PCI slot
/// assignment shifts whenever the device list changes (extra disks,
/// VFIO devices) and a literal name would silently apply to nothing.
fn network_config(primary: &GuestNetwork<'_>) -> String {
    let mut s = format!(
        "network:\n  version: 2\n  ethernets:\n    primary:\n      match:\n        macaddress: {}\n      mtu: {}\n      addresses:\n        - {}/{}\n      gateway4: {}\n      nameservers:\n        addresses:\n",
        primary.mac, primary.mtu, primary.ip_address, primary.prefix_len, primary.gateway,
    );
    for d in primary.dns_servers {
        s.push_str(&format!("          - {d}\n"));
    }
    s
}

/// Convert an image reference to a safe filename stem for the cache.
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

/// Compare the SHA256 of the bytes we just streamed against the digest
/// the manifest promised. Integrity check only — it catches oci-client
/// bugs, truncated streams, and silent I/O corruption between the
/// registry handing us bytes and our write landing on disk. It is
/// *not* a defense against a compromised registry: an attacker with
/// write access to the registry controls both the manifest and the
/// blob, so any hash mismatch on the wire already went through them.
/// Trusting the registry is the job of image signing (cosign / notary
/// / sigstore), which basis does not implement today.
///
/// OCI digests are `"<alg>:<hex>"`. We only handle `sha256` — the
/// one mandated by the OCI image spec. Unknown algorithms surface as
/// `UnsupportedDigest` so a caller can't silently accept a manifest
/// whose integrity we can't check.
fn verify_digest(
    media_type: &'static str,
    expected: &str,
    actual: &[u8],
) -> Result<(), ImageError> {
    let (alg, hex_expected) = expected
        .split_once(':')
        .ok_or_else(|| ImageError::UnsupportedDigest(expected.to_string()))?;
    if alg != "sha256" {
        return Err(ImageError::UnsupportedDigest(alg.to_string()));
    }
    let hex_actual = hex_lower(actual);
    // Case-insensitive: some registries emit upper-case hex. A plain
    // `eq_ignore_ascii_case` keeps us from rejecting those.
    if !hex_expected.eq_ignore_ascii_case(&hex_actual) {
        return Err(ImageError::DigestMismatch {
            media_type,
            expected: expected.to_string(),
            actual: format!("sha256:{hex_actual}"),
        });
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
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

    #[test]
    fn network_config_binds_by_mac() {
        let primary = GuestNetwork {
            mac: "52:54:00:aa:bb:cc",
            ip_address: "10.1.0.20",
            gateway: "10.1.0.1",
            prefix_len: 20,
            dns_servers: &["8.8.8.8".to_string()],
            mtu: 1450,
        };
        let s = network_config(&primary);
        assert!(
            s.contains("macaddress: 52:54:00:aa:bb:cc"),
            "primary NIC must bind by MAC, never by `ensN` — that name shifts with PCI slot ordering: {s}"
        );
        assert!(s.contains("10.1.0.20/20"), "{s}");
        assert!(s.contains("gateway4: 10.1.0.1"), "{s}");
        assert!(s.contains("8.8.8.8"), "{s}");
        // Catch a regression to the broken hardcoded interface name.
        assert!(!s.contains("ens3:"), "{s}");
    }

    /// SHA256 of the empty string. Lets us exercise `verify_digest`
    /// without carrying a fixture file.
    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn verify_digest_accepts_matching_sha256() {
        let hash = Sha256::digest(b"");
        assert!(verify_digest(MEDIA_TYPE_QCOW2, &format!("sha256:{EMPTY_SHA256}"), &hash,).is_ok());
    }

    #[test]
    fn verify_digest_rejects_mismatched_bytes() {
        let hash = Sha256::digest(b"not empty");
        let err =
            verify_digest(MEDIA_TYPE_QCOW2, &format!("sha256:{EMPTY_SHA256}"), &hash).unwrap_err();
        assert!(
            matches!(err, ImageError::DigestMismatch { .. }),
            "expected DigestMismatch, got {err:?}"
        );
    }

    /// Accept upper-case hex (some registries do this). Rejecting
    /// these would break pulls against well-behaved registries.
    #[test]
    fn verify_digest_is_case_insensitive() {
        let hash = Sha256::digest(b"");
        let upper = format!("sha256:{}", EMPTY_SHA256.to_ascii_uppercase());
        assert!(verify_digest(MEDIA_TYPE_QCOW2, &upper, &hash).is_ok());
    }

    /// Don't silently skip unknown algorithms — that would let an
    /// attacker downgrade integrity by republishing under sha1.
    #[test]
    fn verify_digest_rejects_non_sha256_algorithms() {
        let hash = Sha256::digest(b"");
        let err = verify_digest(MEDIA_TYPE_QCOW2, "sha512:deadbeef", &hash).unwrap_err();
        assert!(
            matches!(err, ImageError::UnsupportedDigest(ref a) if a == "sha512"),
            "expected UnsupportedDigest(\"sha512\"), got {err:?}"
        );
    }

    #[test]
    fn verify_digest_rejects_malformed_digest_string() {
        let hash = Sha256::digest(b"");
        let err = verify_digest(MEDIA_TYPE_QCOW2, "no-colon-here", &hash).unwrap_err();
        assert!(matches!(err, ImageError::UnsupportedDigest(_)));
    }
}
