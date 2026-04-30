//! Kernel-route installer driven by gobgpd's BGP RIB.
//!
//! basis-agent doesn't speak BGP itself — gobgpd does. To turn
//! BGP-learned routes (e.g. Cilium's `LoadBalancerIP /32`s
//! advertised by K8s nodes for cell-public services) into kernel
//! routes that the host actually forwards through, this module
//! subscribes to gobgpd's best-path stream and writes one `ip route
//! replace <prefix> via <next-hop> dev brc<vni>` per learned route.
//!
//! Why `via <next-hop>`: under BGP-mode Cilium the holder K8s node
//! does *not* L2-announce the LB IP on the cluster bridge. Without a
//! more-specific route, the host's existing
//! `cluster_vip dev brc<vni>` install ends up ARPing for the LB IP
//! on a bridge where nothing answers, and traffic dies. The via-
//! form makes the kernel ARP for the *node IP* instead, which the
//! node responds to normally; the LB lookup happens inside the node
//! via Cilium's eBPF datapath.
//!
//! Reconnect strategy: gobgpd is a long-lived sibling daemon, but
//! gRPC streams can drop on its restart or transient errors. The
//! watcher loops with a short backoff so a daemon bounce
//! reconverges within seconds rather than requiring an agent
//! restart. On reconnect we get `init=true` semantics — gobgpd
//! replays its current RIB, and `ip route replace` makes every
//! install idempotent so duplicate replays don't churn.

use std::sync::Arc;
use std::time::Duration;

use basis_common::gobgp::GobgpClient;
use tracing::{debug, info, warn};

use crate::network::NetworkManager;

/// How long to wait between watcher restart attempts. Short enough
/// that a routine `systemctl restart gobgpd` reconverges before any
/// LB session times out, long enough not to spam logs when gobgpd
/// is genuinely down.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(3);

/// Run the BGP-RIB → kernel-route bridge until cancelled. Each
/// iteration of the outer loop opens a fresh subscription; an inner
/// `next()` loop drains events. Failure inside the inner loop logs
/// and breaks back to the outer loop, which re-subscribes.
pub async fn run_route_watcher(net: Arc<NetworkManager>, gobgpd_endpoint: String) {
    info!(endpoint = %gobgpd_endpoint, "BGP route watcher starting");
    loop {
        match watch_once(&net, &gobgpd_endpoint).await {
            Ok(()) => {
                debug!("BGP route watcher: stream ended, reconnecting");
            }
            Err(e) => {
                warn!(error = %e, "BGP route watcher: subscribe failed, retrying");
            }
        }
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

/// One subscribe-and-consume cycle. Returns `Ok(())` when the
/// stream cleanly terminates (gobgpd shut down, server closed); the
/// outer loop reconnects either way.
async fn watch_once(net: &NetworkManager, gobgpd_endpoint: &str) -> anyhow::Result<()> {
    let client = GobgpClient::connect(gobgpd_endpoint).await?;
    let mut stream = client.watch_paths().await?;
    while let Some(route) = stream.next().await {
        let cluster_mgr = net.cluster_mgr();
        let result = if route.is_withdraw {
            cluster_mgr
                .withdraw_learned_route(&route.prefix, route.next_hop)
                .await
        } else {
            cluster_mgr
                .install_learned_route(&route.prefix, route.next_hop)
                .await
        };
        if let Err(e) = result {
            // A single failed route shouldn't stop the watcher.
            // `ip route` can legitimately fail when a route was
            // already removed concurrently (DeletePath race) or
            // when the cluster bridge is mid-teardown.
            warn!(
                prefix = %route.prefix,
                next_hop = %route.next_hop,
                is_withdraw = route.is_withdraw,
                error = %e,
                "BGP route watcher: kernel update failed",
            );
        } else {
            debug!(
                prefix = %route.prefix,
                next_hop = %route.next_hop,
                is_withdraw = route.is_withdraw,
                "kernel route reconciled from BGP",
            );
        }
    }
    Ok(())
}
