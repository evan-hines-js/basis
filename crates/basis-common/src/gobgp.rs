//! Thin client for the GoBGP daemon's gRPC northbound.
//!
//! Replaces [`super::holo`] for the basis BGP plane. Both basis-
//! controller (cell route reflector) and basis-agent (host speaker)
//! drive their *local* gobgpd via this module. Operations are typed
//! gRPC calls (`AddPeer`, `AddPath`, etc.) — there's no
//! `commit_replace` whole-tree analogue, so reconcilers diff against
//! the daemon's current state and issue Add/Delete RPCs to converge.
//!
//! All calls go through a single mutex so concurrent reconciliations
//! don't interleave on the wire — same property the holo client had.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_proto::gobgp::{
    attribute, family, go_bgp_service_client::GoBgpServiceClient, nlri, AddPathRequest,
    AddPeerRequest, Attribute, DeletePathRequest, DeletePeerRequest, Family, GetBgpRequest, Global,
    IpAddressPrefix, ListPathRequest, ListPeerRequest, Nlri, OriginAttribute, Path, Peer, PeerConf,
    RouteReflector, StartBgpRequest, TableType,
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::debug;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connected client to a local gobgpd instance.
#[derive(Clone)]
pub struct GobgpClient {
    inner: Arc<Mutex<GoBgpServiceClient<Channel>>>,
}

impl GobgpClient {
    /// Connect to gobgpd at `endpoint` (e.g. `http://127.0.0.1:50051`).
    pub async fn connect(endpoint: &str) -> anyhow::Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| anyhow::anyhow!("parsing gobgpd endpoint '{endpoint}': {e}"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "connecting to gobgpd at {endpoint}: {e} — is gobgpd.service running?"
                )
            })?;
        Ok(Self {
            inner: Arc::new(Mutex::new(GoBgpServiceClient::new(channel))),
        })
    }

    /// Idempotent — boots the BGP instance with the given ASN +
    /// router-id, or no-ops if it's already running.
    /// `families` is the list of AFI/SAFI pairs the speaker is
    /// configured for; pass [`AfiSafi::Ipv4Unicast`] for the Stage-1
    /// IPv4-unicast cell.
    pub async fn start_bgp(
        &self,
        asn: u32,
        router_id: Ipv4Addr,
        families: &[AfiSafi],
    ) -> anyhow::Result<()> {
        let global = Global {
            asn,
            router_id: router_id.to_string(),
            families: families.iter().map(|f| f.encoded()).collect(),
            // The rest are GoBGP defaults; explicitly defaulted so
            // they don't drift if the proto evolves.
            listen_port: 179,
            listen_addresses: Vec::new(),
            use_multiple_paths: false,
            route_selection_options: None,
            default_route_distance: None,
            confederation: None,
            graceful_restart: None,
            bind_to_device: String::new(),
        };
        let mut client = self.inner.lock().await;

        if client
            .get_bgp(GetBgpRequest {})
            .await
            .ok()
            .and_then(|r| r.into_inner().global)
            .is_some_and(|g| g.asn == asn && g.router_id == router_id.to_string())
        {
            return Ok(());
        }
        client
            .start_bgp(StartBgpRequest {
                global: Some(global),
            })
            .await
            .map_err(|e| anyhow::anyhow!("StartBgp: {e}"))?;
        debug!(asn, router_id = %router_id, "started gobgpd BGP instance");
        Ok(())
    }

    /// Reconcile the peer set to exactly `desired`. Adds peers in
    /// `desired` not currently configured; deletes ones currently
    /// configured but not in `desired`. Idempotent — issues no RPCs
    /// when state already matches.
    ///
    /// `route_reflector_client` is set on every peer in `desired`
    /// — only meaningful on the route-reflector side; agents pass
    /// `false`.
    pub async fn reconcile_peers(
        &self,
        desired: &[PeerSpec],
        route_reflector_client: bool,
    ) -> anyhow::Result<()> {
        let current = self.list_peer_addresses().await?;
        let desired_set: BTreeSet<IpAddr> = desired.iter().map(|p| p.address).collect();

        for spec in desired.iter().filter(|p| !current.contains(&p.address)) {
            self.add_peer(spec, route_reflector_client).await?;
        }
        for addr in current.difference(&desired_set) {
            self.delete_peer(*addr).await?;
        }
        Ok(())
    }

    async fn add_peer(&self, spec: &PeerSpec, rr_client: bool) -> anyhow::Result<()> {
        let peer = Peer {
            conf: Some(PeerConf {
                neighbor_address: spec.address.to_string(),
                peer_asn: spec.asn,
                ..Default::default()
            }),
            route_reflector: if rr_client {
                Some(RouteReflector {
                    route_reflector_client: true,
                    route_reflector_cluster_id: String::new(),
                })
            } else {
                None
            },
            ..Default::default()
        };
        let mut client = self.inner.lock().await;
        client
            .add_peer(AddPeerRequest { peer: Some(peer) })
            .await
            .map_err(|e| anyhow::anyhow!("AddPeer({}): {e}", spec.address))?;
        debug!(peer = %spec.address, asn = spec.asn, rr_client, "added gobgp peer");
        Ok(())
    }

    async fn delete_peer(&self, address: IpAddr) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .delete_peer(DeletePeerRequest {
                address: address.to_string(),
                interface: String::new(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("DeletePeer({address}): {e}"))?;
        debug!(peer = %address, "deleted gobgp peer");
        Ok(())
    }

    async fn list_peer_addresses(&self) -> anyhow::Result<BTreeSet<IpAddr>> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .list_peer(ListPeerRequest::default())
            .await
            .map_err(|e| anyhow::anyhow!("ListPeer: {e}"))?
            .into_inner();
        let mut out = BTreeSet::new();
        while let Some(item) = stream.next().await {
            let resp = item.map_err(|e| anyhow::anyhow!("ListPeer stream: {e}"))?;
            if let Some(addr) = resp
                .peer
                .and_then(|p| p.conf)
                .and_then(|c| c.neighbor_address.parse::<IpAddr>().ok())
            {
                out.insert(addr);
            }
        }
        Ok(out)
    }

    /// Reconcile the locally-originated IPv4-unicast prefix set to
    /// exactly `desired`. Each prefix is advertised with NEXT_HOP =
    /// the local BGP router-id (GoBGP's default for self-originated
    /// IPv4-unicast paths) and ORIGIN = IGP.
    pub async fn reconcile_ipv4_paths(&self, desired: &[String]) -> anyhow::Result<()> {
        let current = self.list_ipv4_prefixes().await?;
        let desired_set: BTreeSet<String> = desired.iter().cloned().collect();

        for prefix in desired_set.difference(&current) {
            self.add_ipv4_path(prefix).await?;
        }
        for prefix in current.difference(&desired_set) {
            self.delete_ipv4_path(prefix).await?;
        }
        Ok(())
    }

    async fn add_ipv4_path(&self, prefix: &str) -> anyhow::Result<()> {
        let path = ipv4_unicast_path(prefix)?;
        let mut client = self.inner.lock().await;
        client
            .add_path(AddPathRequest {
                table_type: TableType::Global as i32,
                vrf_id: String::new(),
                path: Some(path),
            })
            .await
            .map_err(|e| anyhow::anyhow!("AddPath({prefix}): {e}"))?;
        debug!(prefix, "advertised path via gobgp");
        Ok(())
    }

    async fn delete_ipv4_path(&self, prefix: &str) -> anyhow::Result<()> {
        let path = ipv4_unicast_path(prefix)?;
        let mut client = self.inner.lock().await;
        client
            .delete_path(DeletePathRequest {
                table_type: TableType::Global as i32,
                vrf_id: String::new(),
                family: Some(AfiSafi::Ipv4Unicast.to_family()),
                path: Some(path),
                uuid: Vec::new(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("DeletePath({prefix}): {e}"))?;
        debug!(prefix, "withdrew path via gobgp");
        Ok(())
    }

    async fn list_ipv4_prefixes(&self) -> anyhow::Result<BTreeSet<String>> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .list_path(ListPathRequest {
                table_type: TableType::Global as i32,
                family: Some(AfiSafi::Ipv4Unicast.to_family()),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("ListPath: {e}"))?
            .into_inner();
        let mut out = BTreeSet::new();
        while let Some(item) = stream.next().await {
            let resp = item.map_err(|e| anyhow::anyhow!("ListPath stream: {e}"))?;
            if let Some(dest) = resp.destination {
                out.insert(dest.prefix);
            }
        }
        Ok(out)
    }
}

/// One peer the local speaker should hold a session with.
#[derive(Debug, Clone, Copy)]
pub struct PeerSpec {
    pub address: IpAddr,
    pub asn: u32,
}

/// AFI/SAFI selector. Stage 1 only uses `Ipv4Unicast`; the EVPN
/// variant is sketched here so Stage 2 doesn't need to grow the
/// surface.
#[derive(Debug, Clone, Copy)]
pub enum AfiSafi {
    Ipv4Unicast,
    L2vpnEvpn,
}

impl AfiSafi {
    /// GoBGP encodes the (AFI<<16 | SAFI) family value as a single
    /// uint32 in `Global.families`. Wire semantics match RFC 4760.
    fn encoded(self) -> u32 {
        let f = self.to_family();
        ((f.afi as u32) << 16) | (f.safi as u32)
    }

    fn to_family(self) -> Family {
        match self {
            Self::Ipv4Unicast => Family {
                afi: family::Afi::Ip as i32,
                safi: family::Safi::Unicast as i32,
            },
            Self::L2vpnEvpn => Family {
                afi: family::Afi::L2vpn as i32,
                safi: family::Safi::Evpn as i32,
            },
        }
    }
}

/// Build a Path advertising an IPv4-unicast prefix. NLRI is the
/// prefix itself; attributes are ORIGIN=IGP only — GoBGP fills in
/// NEXT_HOP from the session's local address at AddPath time when
/// no NEXT_HOP attribute is supplied.
fn ipv4_unicast_path(prefix: &str) -> anyhow::Result<Path> {
    let (addr, len) = split_prefix(prefix)?;
    Ok(Path {
        nlri: Some(Nlri {
            nlri: Some(nlri::Nlri::Prefix(IpAddressPrefix {
                prefix: addr.to_string(),
                prefix_len: len,
            })),
        }),
        pattrs: vec![Attribute {
            attr: Some(attribute::Attr::Origin(OriginAttribute { origin: 0 })),
        }],
        family: Some(AfiSafi::Ipv4Unicast.to_family()),
        ..Default::default()
    })
}

fn split_prefix(prefix: &str) -> anyhow::Result<(IpAddr, u32)> {
    let (a, l) = prefix
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("prefix '{prefix}' missing '/<len>'"))?;
    let addr: IpAddr = a
        .parse()
        .map_err(|e| anyhow::anyhow!("parsing prefix address '{a}': {e}"))?;
    let len: u32 = l
        .parse()
        .map_err(|e| anyhow::anyhow!("parsing prefix length '{l}': {e}"))?;
    Ok((addr, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_unicast_path_round_trips_prefix() {
        let p = ipv4_unicast_path("10.0.0.212/32").unwrap();
        let prefix_nlri = match p.nlri.unwrap().nlri.unwrap() {
            nlri::Nlri::Prefix(ip) => ip,
            other => panic!("expected IPAddressPrefix, got {other:?}"),
        };
        assert_eq!(prefix_nlri.prefix, "10.0.0.212");
        assert_eq!(prefix_nlri.prefix_len, 32);
    }

    #[test]
    fn afisafi_encodes_per_rfc4760() {
        assert_eq!(AfiSafi::Ipv4Unicast.encoded(), (1 << 16) | 1);
        assert_eq!(AfiSafi::L2vpnEvpn.encoded(), (25 << 16) | 70);
    }

    #[test]
    fn split_prefix_parses_v4() {
        let (addr, len) = split_prefix("192.168.1.0/24").unwrap();
        assert_eq!(addr.to_string(), "192.168.1.0");
        assert_eq!(len, 24);
    }

    #[test]
    fn split_prefix_rejects_missing_slash() {
        assert!(split_prefix("10.0.0.0").is_err());
    }
}
