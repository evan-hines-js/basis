//! Cell BGP route reflector — driven by `holod`.
//!
//! basis-controller does not run BGP itself. Each host (controllers
//! and agents alike) runs `holod` as a long-lived systemd service
//! installed by ansible; basis-controller connects to its *local*
//! holod via the gRPC northbound and pushes BGP configuration as
//! YANG-formatted JSON. Decoupling the BGP daemon's lifecycle from
//! the controller's matters: a controller restart must not drop the
//! cell's BGP sessions, otherwise every cluster's apiserver VIP
//! flaps for the duration of the bounce.
//!
//! The reflector role is configured by basis at boot (instance
//! create, ASN, router-id). Per-cluster peer state is mirrored from
//! the `hosts` table via [`peer_reconciler`]; the source-IP ACL on
//! tcp/179 is mirrored via [`acl_reconciler`]. Both run on a fixed
//! tick and only push when the source set changed.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_common::holo::{bgp_running_config, HolodClient};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::db::Db;

/// Static reflector parameters: cell ASN, router-id, and the gRPC
/// endpoint of the local holod. Built once from
/// [`crate::config::BasisControllerSpec`] at boot.
#[derive(Debug, Clone)]
pub struct ReflectorConfig {
    /// Cell ASN. Both the reflector and every speaker use this — all
    /// sessions are iBGP. Per-cluster identity rides in BGP
    /// communities, not ASNs.
    pub asn: u32,
    /// BGP router-id. Conventionally an IPv4 address that's stable
    /// across restarts; basis uses the controller's underlay IP.
    pub router_id: Ipv4Addr,
    /// gRPC endpoint of the local holod (e.g. `http://127.0.0.1:50051`).
    pub holod_endpoint: String,
    /// Logical name basis registers the BGP instance under. Anything
    /// non-empty works; surfaces in `holod`'s state for debugging.
    pub instance_name: String,
}

/// One BGP peer the reflector should accept a session from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerConfig {
    /// Remote-address holod should configure the neighbor under.
    pub address: IpAddr,
    /// Peer ASN. Equals [`ReflectorConfig::asn`] for cell iBGP.
    pub asn: u32,
}

/// Handle to the configured reflector. The underlying holod runs
/// independently of basis-controller's lifecycle; dropping this
/// handle disconnects the gRPC client but does not touch holod.
pub struct Reflector {
    config: ReflectorConfig,
    client: HolodClient,
}

impl Reflector {
    /// Connect to local holod and push the BGP instance + global
    /// configuration. After this returns, the reflector is listening
    /// on tcp/179 with no neighbors; populate them via
    /// [`Self::update_peers`].
    pub async fn start(config: ReflectorConfig) -> anyhow::Result<Self> {
        let client = HolodClient::connect(&config.holod_endpoint).await?;
        let reflector = Self { config, client };
        reflector.update_peers(Vec::new()).await?;
        info!(
            asn = reflector.config.asn,
            router_id = %reflector.config.router_id,
            "BGP reflector configured via holod"
        );
        Ok(reflector)
    }

    /// Replace the reflector's neighbor set. Idempotent at holod's
    /// level: peers already running at the same address are kept,
    /// missing peers are torn down, new peers come up.
    pub async fn update_peers(&self, peers: Vec<PeerConfig>) -> anyhow::Result<()> {
        let payload = bgp_running_config(
            &self.config.instance_name,
            self.config.asn,
            self.config.router_id,
            &peers.iter().map(|p| (p.address, p.asn)).collect::<Vec<_>>(),
            &[],
        );
        self.client
            .commit_replace(&payload, "basis reflector update")
            .await?;
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
/// pressure on holod stays sub-1Hz steady-state.
const RECONCILER_INTERVAL: Duration = Duration::from_secs(10);

/// Snapshot of every healthy host's underlay address — the set the
/// reflector treats as legitimate BGP sources. Both reconcilers
/// (peer + ACL) consume this same set so they can't disagree.
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
/// next tick. Pushes a full REPLACE only when the peer set actually
/// changed.
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
