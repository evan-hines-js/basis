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
        let reflector = Self { config, client };
        reflector.ensure_running().await?;
        info!(
            asn = reflector.config.asn,
            router_id = %reflector.config.router_id,
            "BGP reflector configured via gobgpd"
        );
        Ok(reflector)
    }

    /// Idempotently configure gobgpd's BGP instance. Called from
    /// every entry point that touches the daemon so a gobgpd restart
    /// (which drops in-memory state) self-heals on the next
    /// reconcile tick. `start_bgp` is a no-op when state matches.
    async fn ensure_running(&self) -> anyhow::Result<()> {
        self.client
            .start_bgp(self.config.asn, self.config.router_id, &[AfiSafi::Ipv4Unicast])
            .await
    }

    /// Reconcile the reflector's neighbor set to exactly `peers`.
    /// Idempotent — peers already configured at the same address are
    /// kept, missing peers are torn down, new peers come up.
    /// `route_reflector_client=true` so cell speakers get reflected
    /// routes between each other.
    pub async fn update_peers(&self, peers: &[PeerSpec]) -> anyhow::Result<()> {
        self.ensure_running().await?;
        self.client.reconcile_peers(peers, true).await?;
        debug!(peers = peers.len(), "BGP neighbor set updated");
        Ok(())
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
    // ACL must be applied at least once to install the drop-by-
    // default rule on tcp/179, even if `legitimate_sources` is
    // initially empty. After that, only push when the permitted set
    // changes.
    reconcile_loop(
        "BGP source-IP ACL reconciler",
        db,
        shutdown,
        ApplyOnFirstTick::Always,
        |current| async move {
            let count = current.len();
            let ruleset = render_acl_ruleset(&current);
            nft_apply(&ruleset)
                .await
                .map_err(|e| anyhow::anyhow!("nft apply: {e} (is nftables installed?)"))?;
            debug!(allowed = count, "applied BGP source-IP ACL");
            Ok(())
        },
    )
    .await
}

/// Periodic reconciler that mirrors the `hosts` table into the BGP
/// reflector's neighbor set. Each registered host with a non-empty
/// `vtep_address` becomes a peer; vanished hosts are removed on the
/// next tick. Diffs against gobgpd's current peer set, only issues
/// Add/Delete RPCs for the difference.
pub async fn peer_reconciler(reflector: Arc<Reflector>, db: Db, shutdown: CancellationToken) {
    reconcile_loop(
        "BGP peer reconciler",
        db,
        shutdown,
        ApplyOnFirstTick::OnlyIfChanged,
        |current| {
            let reflector = reflector.clone();
            async move {
                let peers: Vec<PeerSpec> = current
                    .into_iter()
                    .map(|address| PeerSpec {
                        address,
                        asn: reflector.config.asn,
                    })
                    .collect();
                reflector.update_peers(&peers).await
            }
        },
    )
    .await
}

/// Whether [`reconcile_loop`] applies on the very first tick when
/// the desired set is (initially) empty. ACL needs `Always` so the
/// kernel ruleset gets the default-drop on tcp/179 even when no
/// hosts are registered yet; peer-set updates are no-ops on the
/// empty set, so they use `OnlyIfChanged`.
#[derive(Debug, Clone, Copy)]
enum ApplyOnFirstTick {
    Always,
    OnlyIfChanged,
}

/// Tick-driven diff-and-apply loop shared by [`acl_reconciler`] and
/// [`peer_reconciler`]. Each tick samples [`legitimate_sources`]
/// (cheap — one DB read), and only invokes `apply` when the snapshot
/// has changed since the last successful apply.
///
/// Returning `Err` from `apply` is logged and the loop continues —
/// transient gobgpd or DB errors shouldn't stop reconciliation; the
/// next tick re-attempts because `last` only advances on success.
///
/// Exits cleanly on `shutdown.cancelled()`.
async fn reconcile_loop<F, Fut>(
    name: &'static str,
    db: Db,
    shutdown: CancellationToken,
    policy: ApplyOnFirstTick,
    mut apply: F,
) where
    F: FnMut(BTreeSet<IpAddr>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut ticker = tokio::time::interval(RECONCILER_INTERVAL);
    let mut last: BTreeSet<IpAddr> = BTreeSet::new();
    let mut applied_once = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!(reconciler = name, "shutting down");
                return;
            }
            _ = ticker.tick() => {
                let current = match legitimate_sources(&db).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(reconciler = name, error = %e, "legitimate_sources failed");
                        continue;
                    }
                };
                let must_apply_first = !applied_once
                    && matches!(policy, ApplyOnFirstTick::Always);
                if !must_apply_first && current == last {
                    continue;
                }
                // Move ownership into apply — `BTreeSet<IpAddr>` is
                // small (~24 bytes/entry) and the apply closure
                // ergonomically owns the data without lifetime
                // gymnastics. We retain a copy in `last` for the
                // next tick's diff.
                let snapshot = current.clone();
                if let Err(e) = apply(current).await {
                    warn!(reconciler = name, error = %e, "reconcile apply failed");
                    // Don't advance `last`/`applied_once` — a future
                    // tick retries the same diff.
                    continue;
                }
                last = snapshot;
                applied_once = true;
            }
        }
    }
}
