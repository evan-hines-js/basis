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
//! Three reconcilers run on a fixed tick and re-apply every cycle
//! (no in-process cache — gobgpd state can be wiped under us by a
//! daemon restart, so trusting "we already pushed this" causes
//! silent divergence): [`peer_reconciler`] mirrors hosts + cluster
//! VMs into gobgpd's neighbor set; [`acl_reconciler`] mirrors the
//! same set into nftables on tcp/179; [`policy_reconciler`] pushes
//! the per-cluster ingress prefix-list filter that constrains what
//! each peer is allowed to advertise.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_common::gobgp::{AfiSafi, ClusterIngress, GobgpClient, IngressPolicySpec, PeerSpec};
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
            .start_bgp(
                self.config.asn,
                self.config.router_id,
                &[AfiSafi::Ipv4Unicast],
            )
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

    /// Reconcile the reflector's ingress policy. Restricts what each
    /// peer can advertise so a compromised K8s node can't hijack a
    /// sibling cluster's prefixes by announcing arbitrary routes.
    /// See [`IngressPolicySpec`] for the trust model.
    pub async fn update_ingress_policy(&self, spec: &IngressPolicySpec) -> anyhow::Result<()> {
        self.ensure_running().await?;
        self.client.reconcile_ingress_policy(spec).await
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
    let hosts = db
        .list_hosts()
        .await?
        .into_iter()
        .filter_map(|h| h.vtep_address.parse::<IpAddr>().ok());
    let vms = db
        .list_vms(None)
        .await?
        .into_iter()
        .filter_map(|v| v.ip_address.parse::<IpAddr>().ok());
    Ok(hosts.chain(vms).collect())
}

/// Background task that mirrors host underlay addresses into the
/// kernel nftables ruleset on tcp/179. The legitimate source set is
/// exactly basis's own host allocations — no preshared key, no
/// certificate exchange. The cell management LAN's address space
/// *is* the trust boundary.
pub async fn acl_reconciler(db: Db, shutdown: CancellationToken) {
    reconcile_loop("BGP source-IP ACL reconciler", shutdown, || async {
        let allowed = legitimate_sources(&db).await?;
        let count = allowed.len();
        nft_apply(&render_acl_ruleset(&allowed))
            .await
            .map_err(|e| anyhow::anyhow!("nft apply: {e} (is nftables installed?)"))?;
        debug!(allowed = count, "applied BGP source-IP ACL");
        Ok(())
    })
    .await
}

/// Periodic reconciler that pushes the cell's ingress prefix-list
/// policy to the local gobgpd. Each tick computes the desired
/// [`IngressPolicySpec`] from the DB (cluster prefixes + per-cluster
/// K8s nodes + hypervisor IPs) and pushes it via SetPolicies +
/// DeletePolicyAssignment + AddPolicyAssignment.
pub async fn policy_reconciler(reflector: Arc<Reflector>, db: Db, shutdown: CancellationToken) {
    reconcile_loop("BGP ingress policy reconciler", shutdown, || async {
        let spec = compute_ingress_policy(&db).await?;
        reflector.update_ingress_policy(&spec).await
    })
    .await
}

/// Snapshot the DB into an [`IngressPolicySpec`]. One pass over
/// `hosts`, `clusters`, and `vms`; clusters are bucketed by id so
/// we can map each VM to its cluster's allowed prefix set.
async fn compute_ingress_policy(db: &Db) -> Result<IngressPolicySpec, crate::db::DbError> {
    let hypervisors: Vec<IpAddr> = db
        .list_hosts()
        .await?
        .into_iter()
        .filter_map(|h| h.vtep_address.parse::<IpAddr>().ok())
        .collect();

    let mut clusters: std::collections::HashMap<String, ClusterIngress> =
        std::collections::HashMap::new();
    for c in db.list_clusters().await? {
        let mut allowed: Vec<String> = Vec::new();
        if !c.cidr.is_empty() {
            allowed.push(c.cidr.clone());
        }
        if !c.service_block_cidr.is_empty() {
            allowed.push(c.service_block_cidr.clone());
        }
        // Apiserver VIP /32 — only for PUBLIC visibility. PRIVATE
        // apiservers live inside the cluster CIDR and are already
        // covered by the cluster CIDR entry above.
        if c.is_apiserver_public() && !c.control_plane_endpoint.is_empty() {
            // Endpoint format is "host:port"; strip port.
            let host = c
                .control_plane_endpoint
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(&c.control_plane_endpoint);
            if let Ok(addr) = host.parse::<IpAddr>() {
                allowed.push(format!("{addr}/32"));
            }
        }
        clusters.insert(
            c.id.clone(),
            ClusterIngress {
                cluster_id: c.id,
                allowed_prefixes: allowed,
                nodes: Vec::new(),
            },
        );
    }
    for vm in db.list_vms(None).await? {
        let Ok(addr) = vm.ip_address.parse::<IpAddr>() else {
            continue;
        };
        if let Some(cluster) = clusters.get_mut(&vm.cluster_id) {
            cluster.nodes.push(addr);
        }
    }

    Ok(IngressPolicySpec {
        clusters: clusters.into_values().collect(),
        hypervisors,
    })
}

/// Periodic reconciler that mirrors `hosts` (vtep addresses) plus
/// every cluster VM's IP — the cell's [`legitimate_sources`] — into
/// the reflector's neighbor set. `Reflector::update_peers` diffs
/// against gobgpd's live peer set and only issues Add/Delete RPCs
/// for the difference.
pub async fn peer_reconciler(reflector: Arc<Reflector>, db: Db, shutdown: CancellationToken) {
    let self_addr = IpAddr::V4(reflector.config.router_id);
    reconcile_loop("BGP peer reconciler", shutdown, || async {
        // The controller's own underlay IP is in `hosts.vtep_address`
        // and so falls into `legitimate_sources`. Adding it as a peer
        // makes gobgpd dial 127.0.0.1:179 → its own listener, accept,
        // then close with Bad-Peer-AS (Code 2 Subcode 3) every couple
        // seconds. The notification loop wedges gobgpd's management
        // goroutine and, observed here, drags every *other* peer
        // session into IDLE. Skip self.
        let peers: Vec<PeerSpec> = legitimate_sources(&db)
            .await?
            .into_iter()
            .filter(|addr| addr != &self_addr)
            .map(|address| PeerSpec {
                address,
                asn: reflector.config.asn,
            })
            .collect();
        reflector.update_peers(&peers).await
    })
    .await
}

/// Tick-driven apply loop shared by every reconciler in this module.
/// Every tick re-invokes `apply`. We do NOT short-circuit on "desired
/// set unchanged" because gobgpd / nftables are external state stores
/// that can be wiped out from under us (gobgpd restart, nft flush);
/// trusting an in-process cache there cost an outage when gobgpd
/// restarted after the controller's first push and the reconciler
/// believed it was in sync.
///
/// Each `apply` implementation is wholesale-replace and idempotent
/// against the live state, so re-running is cheap (a couple of gRPC
/// calls or one `nft -f`) and self-healing.
///
/// Returning `Err` from `apply` is logged and the loop continues —
/// transient gobgpd or DB errors shouldn't stop reconciliation; the
/// next tick re-attempts.
///
/// Exits cleanly on `shutdown.cancelled()`.
async fn reconcile_loop<F, Fut>(name: &'static str, shutdown: CancellationToken, mut apply: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut ticker = tokio::time::interval(RECONCILER_INTERVAL);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!(reconciler = name, "shutting down");
                return;
            }
            _ = ticker.tick() => {
                if let Err(e) = apply().await {
                    warn!(reconciler = name, error = %e, "reconcile apply failed");
                }
            }
        }
    }
}
