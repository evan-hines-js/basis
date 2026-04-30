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

use std::net::{IpAddr, Ipv4Addr};

use basis_common::gobgp::{AfiSafi, GobgpClient, PeerSpec};
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
    config: SpeakerConfig,
    client: GobgpClient,
}

impl Speaker {
    /// Connect to local gobgpd, bring up the BGP instance, and add
    /// the cell reflector as the sole peer. Routes start empty;
    /// populate them via [`Self::update_routes`].
    pub async fn start(config: SpeakerConfig) -> anyhow::Result<Self> {
        let client = GobgpClient::connect(&config.gobgpd_endpoint).await?;
        let speaker = Self { config, client };
        speaker.ensure_running().await?;
        info!(
            asn = speaker.config.asn,
            router_id = %speaker.config.router_id,
            reflector = %speaker.config.reflector_address,
            "BGP speaker configured via gobgpd"
        );
        Ok(speaker)
    }

    /// Idempotently configure gobgpd's BGP instance + peers. Called
    /// from every entry point that touches the RIB so a gobgpd
    /// restart (which drops in-memory state) self-heals on the next
    /// reconcile tick. `start_bgp` is a no-op when state matches.
    ///
    /// Peer config is skipped when this host is co-located with the
    /// route reflector (controller and agent on the same host share
    /// one gobgpd). Adding the local underlay IP as a peer of itself
    /// would (a) trip gobgpd into trying to dial 127.0.0.1:179 in a
    /// loop, which we observed wedging the daemon's management
    /// goroutine and stalling every later RPC for ~5 minutes, and
    /// (b) fight basis-controller's `peer_reconciler`, which is the
    /// authoritative writer of the peer set on the same gobgpd. The
    /// Speaker still advertises paths via [`Self::update_routes`];
    /// those land directly in the shared global RIB and the
    /// reflector reflects them to remote peers.
    async fn ensure_running(&self) -> anyhow::Result<()> {
        self.client
            .start_bgp(self.config.asn, self.config.router_id, &[AfiSafi::Ipv4Unicast])
            .await?;
        if self.config.router_id == self.config.reflector_address {
            return Ok(());
        }
        self.client
            .reconcile_peers(
                &[PeerSpec {
                    address: IpAddr::V4(self.config.reflector_address),
                    asn: self.config.asn,
                }],
                false,
            )
            .await?;
        Ok(())
    }

    /// Replace the prefix set the speaker advertises. Each prefix is
    /// announced with the speaker's router-id (this host's underlay
    /// IP) as NEXT_HOP. Idempotent — `reconcile_ipv4_paths` diffs
    /// against gobgpd's actual RIB and issues only the necessary
    /// AddPath/DeletePath RPCs, so an unchanged set is one ListPath
    /// round-trip and no writes.
    pub async fn update_routes(&self, prefixes: &[ipnet::Ipv4Net]) -> anyhow::Result<()> {
        self.ensure_running().await?;
        let ordered: Vec<String> = prefixes.iter().map(|p| p.to_string()).collect();
        self.client
            .reconcile_ipv4_paths(&ordered, self.config.router_id)
            .await?;
        info!(prefixes = ordered.len(), "BGP advertised prefix set reconciled");
        Ok(())
    }
}
