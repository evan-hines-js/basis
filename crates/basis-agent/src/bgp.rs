//! Host-side BGP speaker — driven by `holod`.
//!
//! basis-agent does not run BGP itself; the host's `holod` instance
//! does. The agent connects to its local holod via the gRPC northbound
//! and pushes a single BGP speaker config: ASN, router-id (the host's
//! underlay address), one neighbor (the cell route reflector), and
//! the prefix set this host advertises (tree CIDRs + cluster VIPs
//! sourced from `ReconcileHostCommand`).
//!
//! Decoupling the BGP daemon's lifecycle from the agent's matters: an
//! agent restart must not drop BGP sessions. holod runs under systemd
//! independently; the agent re-pushes its desired config on every
//! reconnect, but holod only commits a transaction when the running
//! config actually changes.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use basis_common::holo::{bgp_running_config, HolodClient};
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
    pub holod_endpoint: String,
    pub instance_name: String,
}

/// Handle to the configured speaker. holod runs independently of the
/// agent; dropping this handle disconnects the gRPC client but does
/// not touch holod or its sessions.
pub struct Speaker {
    config: SpeakerConfig,
    client: HolodClient,
    /// Last-pushed prefix set, deduplicated. The reconcile path
    /// short-circuits when an unchanged set comes back so holod's
    /// rollback log doesn't fill with no-op transactions.
    routes: Mutex<BTreeSet<String>>,
}

impl Speaker {
    /// Connect to local holod and push the BGP instance + sole
    /// neighbor (the cell reflector). Routes start empty; populate
    /// them via [`Self::update_routes`].
    pub async fn start(config: SpeakerConfig) -> anyhow::Result<Self> {
        let client = HolodClient::connect(&config.holod_endpoint).await?;
        let speaker = Self {
            config,
            client,
            routes: Mutex::new(BTreeSet::new()),
        };
        speaker.push(&[]).await?;
        info!(
            asn = speaker.config.asn,
            router_id = %speaker.config.router_id,
            reflector = %speaker.config.reflector_address,
            "BGP speaker configured via holod"
        );
        Ok(speaker)
    }

    /// Replace the prefix set the speaker advertises. Each prefix is
    /// announced with the speaker's router-id (this host's underlay
    /// IP) as next-hop. Idempotent — unchanged sets skip the gRPC
    /// roundtrip.
    pub async fn update_routes(&self, prefixes: &[ipnet::Ipv4Net]) -> anyhow::Result<()> {
        let new: BTreeSet<String> = prefixes.iter().map(|p| p.to_string()).collect();
        {
            let last = self.routes.lock().await;
            if *last == new {
                return Ok(());
            }
        }
        let ordered: Vec<String> = new.iter().cloned().collect();
        self.push(&ordered).await?;
        let mut last = self.routes.lock().await;
        *last = new;
        info!(prefixes = last.len(), "BGP advertised prefix set updated");
        Ok(())
    }

    async fn push(&self, prefixes: &[String]) -> anyhow::Result<()> {
        let payload = bgp_running_config(
            &self.config.instance_name,
            self.config.asn,
            self.config.router_id,
            &[(self.config.reflector_address.into(), self.config.asn)],
            prefixes,
        );
        self.client
            .commit_replace(&payload, "basis speaker update")
            .await?;
        Ok(())
    }
}
