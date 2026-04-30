//! Cell BGP route reflector — driven by GoBGP.
//!
//! basis-controller does not run BGP itself. Each host (controllers
//! and agents alike) runs `gobgpd` as a long-lived systemd service
//! installed by ansible; basis-controller connects to its *local*
//! gobgpd via the gRPC northbound. Decoupling the BGP daemon's
//! lifecycle from the controller's matters: a controller restart
//! must not drop the cell's BGP sessions, otherwise every cluster's
//! apiserver VIP flaps for the duration of the bounce.
//!
//! Per-cluster peer state is mirrored from the `hosts` table via
//! [`peer_reconciler`]; the source-IP ACL on tcp/179 is mirrored via
//! [`acl_reconciler`]. Both run on a fixed tick and only push when
//! the source set changed.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_common::gobgp::{AfiSafi, GobgpClient, PeerSpec};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::db::Db;

/// Static reflector parameters: cell ASN, router-id, and the gRPC
/// endpoint of the local gobgpd. Built once from
/// [`crate::config::BasisControllerSpec`] at boot.
#[derive(Debug, Clone)]
pub struct ReflectorConfig {
    /// Cell ASN. Both the reflector and every speaker use this — all
    /// sessions are iBGP. Per-cluster identity rides in BGP
    /// communities (Type-5 RT once EVPN ships), not ASNs.
    pub asn: u32,
    /// BGP router-id. Conventionally an IPv4 address that's stable
    /// across restarts; basis uses the controller's underlay IP.
    pub router_id: Ipv4Addr,
    /// gRPC endpoint of the local gobgpd (e.g. `http://127.0.0.1:50051`).
    pub gobgpd_endpoint: String,
}

/// One BGP peer the reflector should accept a session from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerConfig {
    pub address: IpAddr,
    pub asn: u32,
}

/// Handle to the configured reflector. The underlying gobgpd runs
/// independently of basis-controller's lifecycle; dropping this
/// handle disconnects the gRPC client but does not touch gobgpd.
pub struct Reflector {
    config: ReflectorConfig,
    client: GobgpClient,
}

impl Reflector {
    /// Connect to local gobgpd and bring up the BGP instance with
    /// the cell ASN + router-id. Idempotent: if the daemon is
    /// already running with matching config, no-op.
    pub async fn start(config: ReflectorConfig) -> anyhow::Result<Self> {
        let client = GobgpClient::connect(&config.gobgpd_endpoint).await?;
        client
            .start_bgp(config.asn, config.router_id, &[AfiSafi::Ipv4Unicast])
            .await?;
        info!(
            asn = config.asn,
            router_id = %config.router_id,
            "BGP reflector configured via gobgpd"
        );
        Ok(Self { config, client })
    }

    /// Reconcile the reflector's neighbor set to exactly `peers`.
    /// Idempotent — peers already configured at the same address are
    /// kept, missing peers are torn down, new peers come up.
    /// `route_reflector_client=true` so cell speakers get reflected
    /// routes between each other.
    pub async fn update_peers(&self, peers: Vec<PeerConfig>) -> anyhow::Result<()> {
        let specs: Vec<PeerSpec> = peers
            .iter()
            .map(|p| PeerSpec {
                address: p.address,
                asn: p.asn,
            })
            .collect();
        self.client.reconcile_peers(&specs, true).await?;
        debug!(peers = peers.len(), "BGP neighbor set updated");
        Ok(())
    }

    fn asn(&self) -> u32 {
        self.config.asn
    }
}

/// nftables table + chain holding the BGP source-IP ACL. Owned by
/// basis-controller — the reconciler [`acl_reconciler`] flushes and
/// rewrites the chain atomically on every change so we never run
/// with a partial ruleset.
const NFT_TABLE: &str = "basis_bgp";
const NFT_CHAIN: &str = "input";

/// Run `nft -f -` with the given ruleset on stdin. Returns the
/// stderr text on failure so callers can log a meaningful error.
async fn nft_apply(ruleset: &str) -> anyhow::Result<()> {
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawning nft: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin.write_all(ruleset.as_bytes()).await?;
        stdin.shutdown().await?;
    }
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        anyhow::bail!(
            "nft exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Render the nftables ruleset that allows BGP only from the given
/// permitted source set, drops everything else on tcp/179. `add table`
/// first so the flush is idempotent on the very first apply (when the
/// table doesn't exist yet); then flush so we redefine full intended
/// state with no rule-stacking across reruns.
fn render_acl_ruleset(allowed: &BTreeSet<IpAddr>) -> String {
    let mut out = String::new();
    out.push_str(&format!("add table inet {NFT_TABLE}\n"));
    out.push_str(&format!("flush table inet {NFT_TABLE}\n"));
    out.push_str(&format!("table inet {NFT_TABLE} {{\n"));
    out.push_str(&format!(
        "    chain {NFT_CHAIN} {{ type filter hook input priority -100; policy accept;\n"
    ));
    let v4: Vec<String> = allowed
        .iter()
        .filter_map(|a| match a {
            IpAddr::V4(v) => Some(v.to_string()),
            IpAddr::V6(_) => None,
        })
        .collect();
    if !v4.is_empty() {
        out.push_str(&format!(
            "        ip saddr {{ {} }} tcp dport 179 accept\n",
            v4.join(", ")
        ));
    }
    out.push_str("        tcp dport 179 drop\n");
    out.push_str("    }\n");
    out.push_str("}\n");
    out
}

/// How often the controller rebuilds the BGP peer set + ACL from the
/// `hosts` table. Short enough that a host registering shows up
/// within a tick of the heartbeat cycle; long enough that gRPC
/// pressure on gobgpd stays sub-1Hz steady-state.
const RECONCILER_INTERVAL: Duration = Duration::from_secs(10);

/// Snapshot of every legitimate iBGP source in the cell — the set
/// the reflector accepts sessions from. Two populations:
///
/// 1. **Hypervisor underlay addresses**: every basis-agent runs
///    gobgpd and advertises cluster_vips it carries.
/// 2. **K8s node cluster-overlay addresses**: every VM that hosts a
///    Cilium-on-k8s daemon peers with the RR over the cluster
///    overlay (basis's per-tree VRF + the controller's
///    `tcp_l3mdev_accept` sysctl let those sessions cross). Cilium
///    announces the per-cluster LB pool /32s; the RR reflects them
///    cell-wide.
///
/// Both reconcilers (peer + ACL) consume this same set so they can't
/// disagree on who's allowed.
async fn legitimate_sources(db: &Db) -> Result<BTreeSet<IpAddr>, crate::db::DbError> {
    let mut out = BTreeSet::new();
    for host in db.list_hosts().await? {
        if host.vtep_address.is_empty() {
            continue;
        }
        if let Ok(addr) = host.vtep_address.parse::<IpAddr>() {
            out.insert(addr);
        }
    }
    for vm in db.list_vms(None).await? {
        if vm.ip_address.is_empty() {
            continue;
        }
        if let Ok(addr) = vm.ip_address.parse::<IpAddr>() {
            out.insert(addr);
        }
    }
    Ok(out)
}

/// Background task that mirrors host underlay addresses into the
/// kernel nftables ruleset on tcp/179. The legitimate source set is
/// exactly basis's own host allocations — no preshared key, no
/// certificate exchange. The cell management LAN's address space
/// *is* the trust boundary.
pub async fn acl_reconciler(db: Db, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(RECONCILER_INTERVAL);
    let mut last: BTreeSet<IpAddr> = BTreeSet::new();
    let mut applied_once = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("BGP source-IP ACL reconciler shutting down");
                return;
            }
            _ = ticker.tick() => {
                let current = match legitimate_sources(&db).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "BGP ACL reconciler: legitimate_sources failed");
                        continue;
                    }
                };
                if applied_once && current == last {
                    continue;
                }
                let ruleset = render_acl_ruleset(&current);
                debug!(allowed = current.len(), "applying BGP source-IP ACL");
                if let Err(e) = nft_apply(&ruleset).await {
                    warn!(error = %e, "BGP ACL reconciler: nft apply failed (is nftables installed?)");
                    continue;
                }
                last = current;
                applied_once = true;
            }
        }
    }
}

/// Periodic reconciler that mirrors the `hosts` table into the BGP
/// reflector's neighbor set. Each registered host with a non-empty
/// `vtep_address` becomes a peer; vanished hosts are removed on the
/// next tick. Diffs against gobgpd's current peer set, only issues
/// Add/Delete RPCs for the difference.
pub async fn peer_reconciler(reflector: Arc<Reflector>, db: Db, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(RECONCILER_INTERVAL);
    let mut last: BTreeSet<IpAddr> = BTreeSet::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("BGP peer reconciler shutting down");
                return;
            }
            _ = ticker.tick() => {
                let current = match legitimate_sources(&db).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "BGP peer reconciler: legitimate_sources failed");
                        continue;
                    }
                };
                if current == last {
                    continue;
                }
                let peers: Vec<PeerConfig> = current
                    .iter()
                    .map(|address| PeerConfig {
                        address: *address,
                        asn: reflector.asn(),
                    })
                    .collect();
                if let Err(e) = reflector.update_peers(peers).await {
                    warn!(error = %e, "BGP peer reconciler: update_peers failed");
                    continue;
                }
                last = current;
            }
        }
    }
}
