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
    attribute, family, go_bgp_service_client::GoBgpServiceClient, nlri, watch_event_request,
    watch_event_response, AddPathRequest, AddPeerRequest, Attribute, DeletePathRequest,
    DeletePeerRequest, Family, GetBgpRequest, Global, IpAddressPrefix, ListPathRequest,
    ListPeerRequest, NextHopAttribute, Nlri, OriginAttribute, Path, Peer, PeerConf, RouteReflector,
    StartBgpRequest, TableType, WatchEventRequest,
};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::debug;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-RPC deadline. Bounds the worst case where gobgpd is wedged
/// (its management goroutine stuck on a slow peer, internal mutex,
/// etc.) so basis-controller / basis-agent surface a real error
/// instead of hanging forever and silently failing to bind their
/// own listeners. 5s is generous for any sane gobgpd state operation
/// — even ListPath against a populated RIB returns within a few
/// hundred ms — and conservative enough that a pathological wedge
/// reliably trips it.
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Wrap a unary or streaming gRPC payload in a `tonic::Request` with
/// the per-call deadline applied. tonic propagates this as the
/// `grpc-timeout` metadata header so gobgpd aborts the RPC server-
/// side at the same instant the client gives up — no orphaned
/// goroutines on the daemon, no client-side leak.
fn req<T>(payload: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(payload);
    r.set_timeout(RPC_TIMEOUT);
    r
}

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

    /// Idempotent boot of the BGP instance with the given ASN +
    /// router-id + families. No-op if gobgpd already has equivalent
    /// state.
    ///
    /// gobgp v4.4.0's `GetBgp` only reports asn/router-id/listen-port,
    /// not the families list, so the families check is a probe:
    /// each desired AFI/SAFI is queried via `ListPath` — a
    /// configured family answers with its (possibly empty)
    /// destination set; an unconfigured family returns "address
    /// family: <fam> not supported."
    ///
    /// On mismatch, we issue a fresh `StartBgp`. We deliberately do
    /// NOT call `StopBgp` first: gobgpd's mgmt goroutine processes
    /// requests serially, and a back-to-back StopBgp+StartBgp from a
    /// single client deadlocks the daemon when the previous
    /// shutdown's peer-FSM drain is still in flight (we observed
    /// gobgpd silent for >5min after StopBgp before the next
    /// StartBgp made any progress). If gobgpd's state is wrong, the
    /// `StartBgp` below errors with "gobgp is already started" and
    /// the operator must `systemctl restart gobgpd` to recover. This
    /// is the only client-recovery pattern that doesn't risk
    /// wedging the daemon further; ansible's basis-gobgpd role
    /// re-applies the unit (and hence restarts gobgpd) on every
    /// site.yml run, so deploys naturally clear stale state.
    pub async fn start_bgp(
        &self,
        asn: u32,
        router_id: Ipv4Addr,
        families: &[AfiSafi],
    ) -> anyhow::Result<()> {
        if self
            .has_running_bgp_with(asn, router_id, families)
            .await?
        {
            return Ok(());
        }
        let global = Global {
            asn,
            router_id: router_id.to_string(),
            families: families.iter().map(|f| f.families_index()).collect(),
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
        client
            .start_bgp(req(StartBgpRequest {
                global: Some(global),
            }))
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "StartBgp: {e} — if gobgpd has stale state from a previous run, \
                     `systemctl restart gobgpd` and retry; basis-controller will not \
                     attempt to tear down gobgpd's BGP instance from this side."
                )
            })?;
        debug!(asn, router_id = %router_id, "started gobgpd BGP instance");
        Ok(())
    }

    /// True iff gobgpd has a Global with matching asn+router-id AND
    /// every family in `want` is in the global RIB. The families
    /// check is a probe: gobgpd's `ListPath` returns "address
    /// family: <fam> not supported" for any AFI/SAFI not in the
    /// global table, and a (possibly empty) result for any that is.
    async fn has_running_bgp_with(
        &self,
        asn: u32,
        router_id: Ipv4Addr,
        want: &[AfiSafi],
    ) -> anyhow::Result<bool> {
        let mut client = self.inner.lock().await;
        let global = match client.get_bgp(req(GetBgpRequest {})).await {
            Ok(resp) => resp.into_inner().global,
            Err(_) => return Ok(false),
        };
        let Some(g) = global else { return Ok(false) };
        if g.asn != asn || g.router_id != router_id.to_string() {
            return Ok(false);
        }
        for f in want {
            let probe = client
                .list_path(req(ListPathRequest {
                    table_type: TableType::Global as i32,
                    family: Some(f.to_family()),
                    ..Default::default()
                }))
                .await;
            // The error path matches gobgp's "address family: %s not
            // supported" — any error here means this family isn't in
            // the global RIB and we need a fresh Start.
            if probe.is_err() {
                return Ok(false);
            }
            // Drain the stream so the gRPC response slot is freed
            // before the next iteration borrows the client again.
            let mut stream = probe.unwrap().into_inner();
            while stream.next().await.is_some() {}
        }
        Ok(true)
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
            .add_peer(req(AddPeerRequest { peer: Some(peer) }))
            .await
            .map_err(|e| anyhow::anyhow!("AddPeer({}): {e}", spec.address))?;
        debug!(peer = %spec.address, asn = spec.asn, rr_client, "added gobgp peer");
        Ok(())
    }

    async fn delete_peer(&self, address: IpAddr) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        client
            .delete_peer(req(DeletePeerRequest {
                address: address.to_string(),
                interface: String::new(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("DeletePeer({address}): {e}"))?;
        debug!(peer = %address, "deleted gobgp peer");
        Ok(())
    }

    async fn list_peer_addresses(&self) -> anyhow::Result<BTreeSet<IpAddr>> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .list_peer(req(ListPeerRequest::default()))
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
    /// exactly `desired`. Each prefix is advertised with `next_hop`
    /// (the speaker's underlay IP, i.e. its BGP router-id) and
    /// ORIGIN = IGP. gobgp v4 rejects AddPath with "nexthop not
    /// found" when the NEXT_HOP attribute is absent — every path
    /// must carry it explicitly.
    pub async fn reconcile_ipv4_paths(
        &self,
        desired: &[String],
        next_hop: Ipv4Addr,
    ) -> anyhow::Result<()> {
        let current = self.list_ipv4_prefixes().await?;
        let desired_set: BTreeSet<String> = desired.iter().cloned().collect();

        for prefix in desired_set.difference(&current) {
            self.add_ipv4_path(prefix, next_hop).await?;
        }
        for prefix in current.difference(&desired_set) {
            self.delete_ipv4_path(prefix, next_hop).await?;
        }
        Ok(())
    }

    async fn add_ipv4_path(&self, prefix: &str, next_hop: Ipv4Addr) -> anyhow::Result<()> {
        let path = ipv4_unicast_path(prefix, next_hop)?;
        let mut client = self.inner.lock().await;
        client
            .add_path(req(AddPathRequest {
                table_type: TableType::Global as i32,
                vrf_id: String::new(),
                path: Some(path),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("AddPath({prefix}): {e}"))?;
        debug!(prefix, %next_hop, "advertised path via gobgp");
        Ok(())
    }

    async fn delete_ipv4_path(&self, prefix: &str, next_hop: Ipv4Addr) -> anyhow::Result<()> {
        let path = ipv4_unicast_path(prefix, next_hop)?;
        let mut client = self.inner.lock().await;
        client
            .delete_path(req(DeletePathRequest {
                table_type: TableType::Global as i32,
                vrf_id: String::new(),
                family: Some(AfiSafi::Ipv4Unicast.to_family()),
                path: Some(path),
                uuid: Vec::new(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("DeletePath({prefix}): {e}"))?;
        debug!(prefix, %next_hop, "withdrew path via gobgp");
        Ok(())
    }

    /// Subscribe to gobgpd's RIB and yield IPv4-unicast best-path
    /// updates as they happen, one [`LearnedRoute`] at a time.
    /// `init=true` on the filter so gobgpd replays its current RIB
    /// to the client on subscribe; the watcher converges to live
    /// state without a separate ListPath bootstrap.
    ///
    /// The mutex is surrendered immediately after the stream handle
    /// is acquired so other reconcile RPCs aren't blocked for the
    /// watcher's lifetime — the long-lived stream lives on a cloned
    /// gRPC client (cheap; tonic Channel is HTTP/2-multiplexed).
    pub async fn watch_paths(&self) -> anyhow::Result<LearnedRouteStream> {
        let mut client = {
            let guard = self.inner.lock().await;
            guard.clone()
        };
        let inner = client
            .watch_event(req(WatchEventRequest {
                peer: None,
                table: Some(watch_event_request::Table {
                    filters: vec![watch_event_request::table::Filter {
                        r#type: watch_event_request::table::filter::Type::Best as i32,
                        init: true,
                        peer_address: String::new(),
                        peer_group: String::new(),
                    }],
                }),
                batch_size: 0,
            }))
            .await
            .map_err(|e| anyhow::anyhow!("WatchEvent: {e}"))?
            .into_inner();
        Ok(LearnedRouteStream {
            inner,
            buffered: Vec::new(),
        })
    }

    async fn list_ipv4_prefixes(&self) -> anyhow::Result<BTreeSet<String>> {
        let mut client = self.inner.lock().await;
        let mut stream = client
            .list_path(req(ListPathRequest {
                table_type: TableType::Global as i32,
                family: Some(AfiSafi::Ipv4Unicast.to_family()),
                ..Default::default()
            }))
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

/// One IPv4-unicast best-path event from gobgpd's RIB. Maps a
/// remotely-originated `prefix` to the `next_hop` the local kernel
/// should route through. Withdrawals are signalled by
/// `is_withdraw=true` on the same shape.
#[derive(Debug, Clone)]
pub struct LearnedRoute {
    /// Prefix announced by the remote speaker, e.g. "10.0.0.209/32".
    pub prefix: String,
    /// Length of `prefix` in bits, e.g. 32 for a /32. Pre-parsed so
    /// callers don't redo the split.
    pub prefix_len: u32,
    /// Next-hop IP from the path's NEXT_HOP attribute. The address
    /// the local kernel should forward through to reach the prefix.
    pub next_hop: IpAddr,
    /// True iff this is a withdrawal — the kernel route should be
    /// removed.
    pub is_withdraw: bool,
}

/// Stream of best-path RIB events from gobgpd, one
/// [`LearnedRoute`] per `next()` call. gobgpd batches multiple paths
/// into a single TableEvent gRPC message; this stream flattens that
/// batching plus filters out non-IPv4-unicast NLRI shapes and paths
/// without a usable next-hop. Errors on the gRPC stream end the
/// iteration — callers re-subscribe after a delay if they want to
/// resume.
pub struct LearnedRouteStream {
    inner: tonic::Streaming<basis_proto::gobgp::WatchEventResponse>,
    /// Paths from a partially-consumed TableEvent. Drained
    /// FIFO-from-front; refilled when `inner.next()` yields the next
    /// TableEvent.
    buffered: Vec<Path>,
}

impl LearnedRouteStream {
    /// Yield the next route event, or `None` if the underlying gRPC
    /// stream has terminated. PeerEvent payloads are discarded —
    /// peer-state changes don't shift the RIB on their own.
    pub async fn next(&mut self) -> Option<LearnedRoute> {
        loop {
            while let Some(p) = self.buffered.pop() {
                if let Some(route) = LearnedRoute::from_path(&p) {
                    return Some(route);
                }
            }
            let resp = self.inner.next().await?.ok()?;
            match resp.event? {
                watch_event_response::Event::Table(t) => {
                    self.buffered = t.paths;
                }
                _ => continue,
            }
        }
    }
}

impl LearnedRoute {
    /// Parse one [`Path`] into a [`LearnedRoute`], or `None` if the
    /// path isn't an IPv4-unicast prefix with a NEXT_HOP we can
    /// install. Filters out:
    ///
    /// * non-prefix NLRI types (EVPN, flowspec, etc.) — Stage 1 only
    ///   handles plain ipv4 unicast, callers already constrain the
    ///   subscribe filter, but the response payload is structurally
    ///   any NLRI shape so we re-check here.
    /// * paths with no NEXT_HOP — the kernel needs one to install a
    ///   route. (Self-originated next-hops would arrive without one
    ///   from gobgpd's perspective; no harm filtering them.)
    fn from_path(p: &Path) -> Option<Self> {
        let nlri = match p.nlri.as_ref()?.nlri.as_ref()? {
            nlri::Nlri::Prefix(ip) => ip,
            _ => return None,
        };
        let prefix_addr: IpAddr = nlri.prefix.parse().ok()?;
        if !prefix_addr.is_ipv4() {
            return None;
        }
        let next_hop = next_hop_from_attrs(&p.pattrs)?;
        Some(Self {
            prefix: format!("{}/{}", nlri.prefix, nlri.prefix_len),
            prefix_len: nlri.prefix_len,
            next_hop,
            is_withdraw: p.is_withdraw,
        })
    }
}

/// Extract a NEXT_HOP IP from a path's attribute list. Returns
/// `None` if neither `NextHopAttribute` nor an IPv4 nexthop in
/// `MpReachNlriAttribute` is present — gobgp may put the next-hop
/// in either depending on the address family of the path.
fn next_hop_from_attrs(attrs: &[Attribute]) -> Option<IpAddr> {
    for attr in attrs {
        match attr.attr.as_ref()? {
            attribute::Attr::NextHop(nh) => {
                if let Ok(ip) = nh.next_hop.parse::<IpAddr>() {
                    return Some(ip);
                }
            }
            attribute::Attr::MpReach(mp) => {
                for nh in &mp.next_hops {
                    if let Ok(ip) = nh.parse::<IpAddr>() {
                        return Some(ip);
                    }
                }
            }
            _ => {}
        }
    }
    None
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
    /// GoBGP v4 keys `Global.families` by an internal enum index
    /// (`oc.IntToAfiSafiTypeMap` in
    /// `pkg/config/oc/bgp_configs.go`), NOT by the BGP wire-format
    /// `(AFI<<16) | SAFI`. Wrong indexes silently fail to register
    /// the AFI/SAFI, leaving the global RIB without the
    /// corresponding table — and every later `ListPath` / `AddPath`
    /// returns "address family: <fam> not supported". Pin the
    /// indexes here against gobgp v4.4.0; bumping gobgp_version in
    /// ansible requires re-validating these mappings.
    fn families_index(self) -> u32 {
        match self {
            Self::Ipv4Unicast => 0,
            Self::L2vpnEvpn => 9,
        }
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

/// Build a Path advertising an IPv4-unicast prefix with explicit
/// NEXT_HOP and ORIGIN=IGP. gobgp v4 rejects AddPath without a
/// NEXT_HOP attribute (the daemon doesn't synthesise one from the
/// session's local address at insert time).
fn ipv4_unicast_path(prefix: &str, next_hop: Ipv4Addr) -> anyhow::Result<Path> {
    let (addr, len) = split_prefix(prefix)?;
    Ok(Path {
        nlri: Some(Nlri {
            nlri: Some(nlri::Nlri::Prefix(IpAddressPrefix {
                prefix: addr.to_string(),
                prefix_len: len,
            })),
        }),
        pattrs: vec![
            Attribute {
                attr: Some(attribute::Attr::Origin(OriginAttribute { origin: 0 })),
            },
            Attribute {
                attr: Some(attribute::Attr::NextHop(NextHopAttribute {
                    next_hop: next_hop.to_string(),
                })),
            },
        ],
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
    fn ipv4_unicast_path_round_trips_prefix_and_nexthop() {
        let p = ipv4_unicast_path("10.0.0.212/32", "10.0.0.206".parse().unwrap()).unwrap();
        let prefix_nlri = match p.nlri.unwrap().nlri.unwrap() {
            nlri::Nlri::Prefix(ip) => ip,
            other => panic!("expected IPAddressPrefix, got {other:?}"),
        };
        assert_eq!(prefix_nlri.prefix, "10.0.0.212");
        assert_eq!(prefix_nlri.prefix_len, 32);
        // NEXT_HOP must be present — gobgp v4 rejects AddPath
        // without one.
        let nh = p
            .pattrs
            .iter()
            .find_map(|a| match a.attr.as_ref()? {
                attribute::Attr::NextHop(nh) => Some(nh.next_hop.clone()),
                _ => None,
            })
            .expect("NEXT_HOP attribute present");
        assert_eq!(nh, "10.0.0.206");
    }

    /// Pin the gobgp v4.4.0 `IntToAfiSafiTypeMap` indexes — getting
    /// these wrong silently corrupts the global RIB tables and every
    /// subsequent ListPath/AddPath fails with "address family not
    /// supported."
    #[test]
    fn afisafi_indexes_match_gobgp_v4() {
        assert_eq!(AfiSafi::Ipv4Unicast.families_index(), 0);
        assert_eq!(AfiSafi::L2vpnEvpn.families_index(), 9);
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
