//! Host-side BGP speaker — driven by GoBGP.
//!
//! basis-agent does not run BGP itself; the host's `gobgpd` instance
//! does. The agent connects to its local gobgpd via the gRPC northbound
//! and configures a single BGP speaker: ASN, router-id (the host's
//! underlay address), one neighbor (the cell route reflector), and
//! the prefix set this host advertises (per-cluster VIPs — apiserver
//! VIP when `APISERVER_PUBLIC` plus the LB Service block — sourced
//! from `ReconcileHostCommand.clusters[].cluster_vips`).
//!
//! Decoupling the BGP daemon's lifecycle from the agent's matters: an
//! agent restart must not drop BGP sessions. gobgpd runs under
//! systemd independently; the agent only issues Add/Delete RPCs for
//! the diff between desired and current state, so a re-push doesn't
//! disturb running sessions.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};

use basis_common::gobgp::{AfiSafi, GobgpClient, PeerSpec};
use tokio::sync::Mutex;
use tracing::info;

/// Static speaker parameters: cell ASN (learned from the controller's
/// `RegisterHostResponse`), the host's router-id (its underlay IP),
/// and the reflector address the speaker peers with.
#[derive(Debug, Clone)]
pub struct SpeakerConfig {
    pub asn: u32,
    pub router_id: Ipv4Addr,
    pub reflector_address: Ipv4Addr,
    pub gobgpd_endpoint: String,
}

/// Handle to the configured speaker. gobgpd runs independently of the
/// agent; dropping this handle disconnects the gRPC client but does
/// not touch gobgpd or its sessions.
pub struct Speaker {
    client: GobgpClient,
    /// Last-pushed prefix set, deduplicated. Cached so the reconcile
    /// path can short-circuit when an unchanged set comes back; saves
    /// a gRPC round-trip on the steady-state hot path.
    routes: Mutex<BTreeSet<String>>,
}

impl Speaker {
    /// Connect to local gobgpd, bring up the BGP instance, and add
    /// the cell reflector as the sole peer. Routes start empty;
    /// populate them via [`Self::update_routes`].
    pub async fn start(config: SpeakerConfig) -> anyhow::Result<Self> {
        let client = GobgpClient::connect(&config.gobgpd_endpoint).await?;
        client
            .start_bgp(config.asn, config.router_id, &[AfiSafi::Ipv4Unicast])
            .await?;
        client
            .reconcile_peers(
                &[PeerSpec {
                    address: IpAddr::V4(config.reflector_address),
                    asn: config.asn,
                }],
                false,
            )
            .await?;
        info!(
            asn = config.asn,
            router_id = %config.router_id,
            reflector = %config.reflector_address,
            "BGP speaker configured via gobgpd"
        );
        Ok(Self {
            client,
            routes: Mutex::new(BTreeSet::new()),
        })
    }

    /// Replace the prefix set the speaker advertises. Each prefix is
    /// announced with the speaker's router-id (this host's underlay
    /// IP) as next-hop. Idempotent — unchanged sets skip the gRPC
    /// roundtrip via the cached `routes` set.
    pub async fn update_routes(&self, prefixes: &[ipnet::Ipv4Net]) -> anyhow::Result<()> {
        let new: BTreeSet<String> = prefixes.iter().map(|p| p.to_string()).collect();
        {
            let last = self.routes.lock().await;
            if *last == new {
                return Ok(());
            }
        }
        let ordered: Vec<String> = new.iter().cloned().collect();
        self.client.reconcile_ipv4_paths(&ordered).await?;
        let mut last = self.routes.lock().await;
        *last = new;
        info!(prefixes = last.len(), "BGP advertised prefix set updated");
        Ok(())
    }
}
