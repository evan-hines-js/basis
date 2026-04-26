//! Host-network plumbing for VM guests.
//!
//! Every VM has one TAP, `bas<hash>`, attached to its cluster's
//! bridge (`brc<vni>`). The cluster bridge has a VXLAN slave
//! (`vxc<vni>`) that tunnels the cluster's L2 to every other
//! hypervisor carrying the same cluster. VMs are single-homed on
//! the overlay; LAN reachability for VIPs is provided by the host's
//! BGP advertisement, not by a second per-VM NIC.
//!
//! Tap names hash the vm_id to stay inside IFNAMSIZ = 15 chars while
//! being stable across restarts. Orphan sweeps reconstruct the
//! expected name set from known vm_ids (rather than reversing the
//! one-way hash).

pub mod cluster;

pub use cluster::ClusterManager;

use std::hash::{Hash, Hasher};

use basis_proto::ClusterState;
use tokio::process::Command;
use tracing::{info, warn};

/// Bytes added to every inner frame by VXLAN encap:
/// 14 (outer eth) + 20 (outer IPv4) + 8 (UDP) + 8 (VXLAN) = 50.
pub const VXLAN_OVERHEAD: u32 = 50;

const PRIMARY_TAP_PREFIX: &str = "bas";

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("bridge setup failed: {0}")]
    BridgeFailed(String),

    #[error(
        "bridge '{bridge}' exists but the configured uplink NIC '{nic}' is not a slave of it \
         — either attach the NIC to the bridge manually or pick a different bridge in host.yaml"
    )]
    UplinkNicNotOnBridge { bridge: String, nic: String },

    #[error("tap '{tap}' inconsistent: {reason}")]
    TapInconsistent { tap: String, reason: String },

    #[error("probing uplink '{iface}': {reason}")]
    UplinkProbe { iface: String, reason: String },

    #[error("command failed: {0}")]
    CommandFailed(#[from] std::io::Error),
}

/// Bundles the uplink bridge and the per-cluster VXLAN manager so
/// call sites hold one handle instead of two.
pub struct NetworkManager {
    uplink: UplinkBridge,
    clusters: ClusterManager,
}

impl NetworkManager {
    pub fn new(uplink: UplinkBridge, clusters: ClusterManager) -> Self {
        Self { uplink, clusters }
    }

    pub fn uplink_bridge_name(&self) -> &str {
        self.uplink.bridge_name()
    }

    /// Cluster-overlay inner MTU (uplink minus VXLAN overhead).
    /// Plumbed to cloud-init so the guest's primary NIC matches the
    /// bridge, avoiding silent drops on >MTU egress.
    pub fn inner_mtu(&self) -> u32 {
        self.clusters.inner_mtu()
    }

    pub async fn validate_uplink(&self) -> Result<(), NetworkError> {
        self.uplink.validate().await
    }

    pub async fn ensure_uplink_bridge(&self) -> Result<(), NetworkError> {
        self.uplink.ensure().await?;
        ensure_vxlan_spoof_guard().await
    }

    pub async fn reconcile_clusters(
        &self,
        desired: &[ClusterState],
    ) -> Result<(), NetworkError> {
        self.clusters.reconcile(desired).await
    }

    /// Pre-connect cluster bootstrap: bring the bridge + VXLAN up
    /// with an empty FDB so a cold-booted VM can attach its TAP
    /// before the controller reconcile lands.
    pub async fn ensure_bootstrap_cluster(&self, vni: u32) -> Result<(), NetworkError> {
        self.clusters.ensure_bootstrap(vni).await
    }

    pub async fn attach_vm_primary(&self, vm_id: &str, vni: u32) -> Result<String, NetworkError> {
        self.clusters.attach_vm_primary(vm_id, vni).await
    }

    /// Best-effort delete of the VM's TAP.
    pub async fn detach_vm_taps(&self, vm_id: &str) {
        let _ = delete_tap_by_name(&primary_tap_name(vm_id)).await;
    }

    pub async fn list_agent_taps(&self) -> Result<Vec<String>, NetworkError> {
        list_agent_taps().await
    }

    pub async fn delete_tap_by_name(&self, name: &str) -> Result<(), NetworkError> {
        delete_tap_by_name(name).await
    }
}

/// Deterministic primary TAP name for a VM.
pub fn primary_tap_name(vm_id: &str) -> String {
    format!("{PRIMARY_TAP_PREFIX}{}", vm_id_hash(vm_id))
}

fn vm_id_hash(vm_id: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    vm_id.hash(&mut hasher);
    format!("{:010x}", hasher.finish() & 0xff_ffff_ffff)
}

pub struct UplinkProbe {
    pub mtu: u32,
    pub vtep_address: String,
}

/// Read the uplink's MTU and primary IPv4 out of the kernel. Probe
/// the bridge (not the NIC) — on a host where netplan has enslaved
/// the NIC to the bridge, the IP lives on the bridge.
pub async fn probe_uplink(bridge: &str) -> Result<UplinkProbe, NetworkError> {
    let mtu = read_mtu_sysfs(bridge)?;
    let vtep_address = read_primary_ipv4(bridge).await?;
    Ok(UplinkProbe { mtu, vtep_address })
}

fn read_mtu_sysfs(iface: &str) -> Result<u32, NetworkError> {
    let path = format!("/sys/class/net/{iface}/mtu");
    let raw = std::fs::read_to_string(&path).map_err(|e| NetworkError::UplinkProbe {
        iface: iface.to_string(),
        reason: if e.kind() == std::io::ErrorKind::NotFound {
            "interface not found (check spec.network in host.yaml)".to_string()
        } else {
            format!("reading {path}: {e}")
        },
    })?;
    raw.trim()
        .parse::<u32>()
        .map_err(|e| NetworkError::UplinkProbe {
            iface: iface.to_string(),
            reason: format!("parsing MTU '{}': {e}", raw.trim()),
        })
}

async fn read_primary_ipv4(iface: &str) -> Result<String, NetworkError> {
    // `ip -o -4 addr show dev <iface>` — one line per v4 address.
    let out = Command::new("ip")
        .args(["-o", "-4", "addr", "show", "dev", iface])
        .output()
        .await?;
    if !out.status.success() {
        return Err(NetworkError::UplinkProbe {
            iface: iface.to_string(),
            reason: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut toks = line.split_whitespace();
        while let Some(t) = toks.next() {
            if t == "inet" {
                if let Some(addr_prefix) = toks.next() {
                    if let Some((addr, _)) = addr_prefix.split_once('/') {
                        return Ok(addr.to_string());
                    }
                }
            }
        }
    }
    Err(NetworkError::UplinkProbe {
        iface: iface.to_string(),
        reason: "no IPv4 address assigned — assign one to be the VXLAN outer source".to_string(),
    })
}

/// The host's uplink bridge + physical NIC. Source of the VXLAN
/// outer IP; carries host-originated traffic and the host BGP
/// speaker's sessions to the cell reflector.
pub struct UplinkBridge {
    bridge_name: String,
    physical_nic: String,
    uplink_mtu: u32,
}

impl UplinkBridge {
    pub fn new(bridge_name: String, physical_nic: String, uplink_mtu: u32) -> Self {
        Self {
            bridge_name,
            physical_nic,
            uplink_mtu,
        }
    }

    pub fn bridge_name(&self) -> &str {
        &self.bridge_name
    }

    /// Preflight: NIC exists, and if the bridge already exists, the
    /// NIC is one of its slaves. We deliberately tolerate other slaves
    /// (PVE colocation puts firewall veth pairs and tap devices on
    /// vmbr0 for its own VMs, and basis is meant to share the bridge
    /// in that mode). The check we actually need is "this host can
    /// reach the LAN through the bridge", which is true iff our
    /// physical NIC bridges into it. No MTU check — standard 1500
    /// works fine, guests see 1450 inner and TCP MSS clamps the rest.
    pub async fn validate(&self) -> Result<(), NetworkError> {
        let nic_check = Command::new("ip")
            .args(["link", "show", &self.physical_nic])
            .output()
            .await?;
        if !nic_check.status.success() {
            return Err(NetworkError::UplinkProbe {
                iface: self.physical_nic.clone(),
                reason: "interface not found".to_string(),
            });
        }

        let bridge_exists = Command::new("ip")
            .args(["link", "show", &self.bridge_name])
            .output()
            .await?
            .status
            .success();
        if bridge_exists {
            let slaves = Command::new("ip")
                .args(["-o", "link", "show", "master", &self.bridge_name])
                .output()
                .await?;
            let text = String::from_utf8_lossy(&slaves.stdout);
            let nic_attached = text.lines().any(|l| {
                l.split_whitespace()
                    .nth(1)
                    .map(|s| {
                        let name = s.trim_end_matches(':').split('@').next().unwrap_or("");
                        name == self.physical_nic
                    })
                    .unwrap_or(false)
            });
            if !nic_attached {
                return Err(NetworkError::UplinkNicNotOnBridge {
                    bridge: self.bridge_name.clone(),
                    nic: self.physical_nic.clone(),
                });
            }
        }

        info!(
            bridge = %self.bridge_name,
            nic = %self.physical_nic,
            mtu = self.uplink_mtu,
            "uplink preflight passed"
        );
        Ok(())
    }

    /// Create the bridge if missing, attach the physical NIC, bring
    /// both up, and enable IPv4 forwarding so cluster packets can be
    /// routed off-host. Per-cluster MASQUERADE rules are owned by
    /// [`ClusterManager`] so they come and go with the cluster
    /// itself. Idempotent.
    pub async fn ensure(&self) -> Result<(), NetworkError> {
        let exists = Command::new("ip")
            .args(["link", "show", &self.bridge_name])
            .output()
            .await?;
        if !exists.status.success() {
            run_cmd("ip", &["link", "add", &self.bridge_name, "type", "bridge"]).await?;
            run_cmd(
                "ip",
                &[
                    "link",
                    "set",
                    &self.physical_nic,
                    "master",
                    &self.bridge_name,
                ],
            )
            .await?;
            info!(
                bridge = %self.bridge_name,
                nic = %self.physical_nic,
                "uplink bridge created"
            );
        }
        run_cmd("ip", &["link", "set", &self.bridge_name, "up"]).await?;
        run_cmd("sysctl", &["-w", "net.ipv4.ip_forward=1"]).await
    }
}

/// True iff `name` matches the agent's TAP shape: `bas` followed by
/// 10 hex chars. Prevents the orphan sweep from mistaking `basis0`
/// (the uplink bridge) for an agent tap.
fn is_agent_managed_tap(name: &str) -> bool {
    const PREFIX_LEN: usize = 3;
    const HASH_LEN: usize = 10;
    if name.len() != PREFIX_LEN + HASH_LEN {
        return false;
    }
    let (prefix, suffix) = name.split_at(PREFIX_LEN);
    prefix == PRIMARY_TAP_PREFIX && suffix.chars().all(|c| c.is_ascii_hexdigit())
}

pub(crate) async fn create_and_attach_tap(tap: &str, bridge: &str) -> Result<(), NetworkError> {
    run_cmd("ip", &["tuntap", "add", tap, "mode", "tap"]).await?;
    run_cmd("ip", &["link", "set", tap, "master", bridge]).await?;
    run_cmd("ip", &["link", "set", tap, "up"]).await?;
    Ok(())
}

/// Idempotent attach: create the TAP if missing, else (re)master it
/// to the bridge and bring it up.
pub(crate) async fn ensure_tap_on_bridge(tap: &str, bridge: &str) -> Result<(), NetworkError> {
    let exists = Command::new("ip")
        .args(["link", "show", tap])
        .output()
        .await?;
    if exists.status.success() {
        run_cmd("ip", &["link", "set", tap, "master", bridge])
            .await
            .map_err(|e| NetworkError::TapInconsistent {
                tap: tap.to_string(),
                reason: format!("re-attach to bridge {bridge}: {e}"),
            })?;
        run_cmd("ip", &["link", "set", tap, "up"])
            .await
            .map_err(|e| NetworkError::TapInconsistent {
                tap: tap.to_string(),
                reason: format!("link up: {e}"),
            })?;
        return Ok(());
    }
    create_and_attach_tap(tap, bridge).await
}

pub async fn delete_tap_by_name(name: &str) -> Result<(), NetworkError> {
    if let Err(e) = run_cmd("ip", &["link", "delete", name]).await {
        warn!(tap = %name, error = %e, "delete tap (may already be gone)");
    }
    Ok(())
}

/// Enumerate every agent-managed TAP on the host.
pub async fn list_agent_taps() -> Result<Vec<String>, NetworkError> {
    let out = Command::new("ip")
        .args(["-o", "link", "show", "type", "tuntap"])
        .output()
        .await?;
    if !out.status.success() {
        return Err(NetworkError::BridgeFailed(
            String::from_utf8_lossy(&out.stderr).to_string(),
        ));
    }
    let mut taps = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Some(name) = line.split_whitespace().nth(1) else {
            continue;
        };
        let name = name.trim_end_matches(':').trim_end_matches('@');
        if is_agent_managed_tap(name) {
            taps.push(name.to_string());
        }
    }
    Ok(taps)
}

/// Install a source-scoped MASQUERADE rule for `cluster_cidr`
/// egressing out `uplink`, plus a TCP MSS clamp on the forward
/// chain. Narrower than a blanket `-o uplink` catch-all: leaves
/// host-originated LAN traffic untouched. Without MASQUERADE, a
/// cluster VM's default route dead-ends at the host — packets
/// forwarded out the uplink would source from a cluster address the
/// upstream router can't reverse-route to. Without MSS clamping,
/// return packets from servers that ignore PMTUD (notably some
/// Google front-ends) get silently dropped at the bridge once
/// VXLAN's 50 bytes of overhead pushes them past the underlay MTU —
/// TCP connect hangs even though SYN/SYN-ACK got through.
///
/// Guarded by `iptables -C` existence checks so repeat calls don't
/// stack duplicates.
pub(crate) async fn ensure_cluster_masquerade(
    cluster_cidr: &str,
    uplink: &str,
) -> Result<(), NetworkError> {
    ensure_iptables_rule(
        "nat",
        "POSTROUTING",
        &["-s", cluster_cidr, "-o", uplink, "-j", "MASQUERADE"],
    )
    .await?;
    // `--clamp-mss-to-pmtu` rewrites MSS in TCP SYN/SYN-ACK to the
    // egress interface's MTU minus 40 (IPv4+TCP headers), so
    // segments fit underlay MTU even when the guest reports a
    // larger NIC MTU or PMTUD is broken end-to-end.
    ensure_iptables_rule(
        "mangle",
        "FORWARD",
        &[
            "-s",
            cluster_cidr,
            "-p",
            "tcp",
            "--tcp-flags",
            "SYN,RST",
            "SYN",
            "-j",
            "TCPMSS",
            "--clamp-mss-to-pmtu",
        ],
    )
    .await?;
    info!(
        cluster_cidr,
        uplink, "installed per-cluster MASQUERADE + MSS clamp"
    );
    Ok(())
}

/// Best-effort removal of the per-cluster MASQUERADE + MSS clamp
/// rules. Missing rules are the desired state — iptables returns
/// non-zero for `-D` on an absent match, which we log and ignore.
pub(crate) async fn remove_cluster_masquerade(cluster_cidr: &str, uplink: &str) {
    delete_iptables_rule(
        "nat",
        "POSTROUTING",
        &["-s", cluster_cidr, "-o", uplink, "-j", "MASQUERADE"],
    )
    .await;
    delete_iptables_rule(
        "mangle",
        "FORWARD",
        &[
            "-s",
            cluster_cidr,
            "-p",
            "tcp",
            "--tcp-flags",
            "SYN,RST",
            "SYN",
            "-j",
            "TCPMSS",
            "--clamp-mss-to-pmtu",
        ],
    )
    .await;
    info!(
        cluster_cidr,
        uplink, "removed per-cluster MASQUERADE + MSS clamp"
    );
}

/// Drop forwarded VXLAN traffic (UDP/4789) on this host's FORWARD
/// chain. Host-originated VXLAN encap goes through OUTPUT (untouched);
/// only frames forwarded from a tap match here. A tenant VM that
/// crafts a VXLAN packet with a foreign VNI to poison a peer's FDB
/// goes through FORWARD and gets dropped at the source.
///
/// Without this rule, enabling VXLAN learning on the receive path
/// would let any in-cluster VM spoof MAC entries for any other
/// cluster on peer hosts — a cross-tenant data leak. With it, peer
/// FDBs only learn from genuine host-originated gARP floods.
async fn ensure_vxlan_spoof_guard() -> Result<(), NetworkError> {
    ensure_iptables_rule(
        "filter",
        "FORWARD",
        &["-p", "udp", "--dport", "4789", "-j", "DROP"],
    )
    .await
}

async fn ensure_iptables_rule(table: &str, chain: &str, spec: &[&str]) -> Result<(), NetworkError> {
    let mut check_args = vec!["-t", table, "-C", chain];
    check_args.extend_from_slice(spec);
    let exists = Command::new("iptables").args(&check_args).output().await?;
    if exists.status.success() {
        return Ok(());
    }
    let mut add_args = vec!["-t", table, "-A", chain];
    add_args.extend_from_slice(spec);
    run_cmd("iptables", &add_args).await
}

async fn delete_iptables_rule(table: &str, chain: &str, spec: &[&str]) {
    let mut args = vec!["-t", table, "-D", chain];
    args.extend_from_slice(spec);
    let out = Command::new("iptables").args(&args).output().await;
    if let Err(e) = out {
        warn!(table, chain, error = %e, "spawning iptables for rule removal");
    }
}

pub(crate) async fn run_cmd(cmd: &str, args: &[&str]) -> Result<(), NetworkError> {
    let output = Command::new(cmd).args(args).output().await?;
    if !output.status.success() {
        return Err(NetworkError::BridgeFailed(format!(
            "{} {} failed: {}",
            cmd,
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tap_names_fit_ifnamsiz() {
        let vm = "3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e";
        assert!(primary_tap_name(vm).len() <= 15);
    }

    #[test]
    fn is_agent_managed_requires_full_shape() {
        assert!(is_agent_managed_tap(&primary_tap_name("v1")));
        assert!(!is_agent_managed_tap("eth0"));
        assert!(!is_agent_managed_tap("basis0"));
        assert!(!is_agent_managed_tap("basabc123")); // too short
        assert!(!is_agent_managed_tap("basnonhex000")); // non-hex suffix
    }
}
