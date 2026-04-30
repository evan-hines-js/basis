//! Thin client for the GoBGP daemon's gRPC northbound.
//!
//! Both basis-controller (cell route reflector) and basis-agent
//! (host speaker) drive their *local* gobgpd via this module.
//! Operations are typed gRPC calls (`AddPeer`, `AddPath`, etc.);
//! reconcilers diff against the daemon's current state and issue
//! Add/Delete RPCs to converge.
//!
//! All calls go through a single mutex so concurrent reconciliations
//! don't interleave on the wire.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_proto::gobgp::{
    attribute, family, go_bgp_service_client::GoBgpServiceClient, match_set, nlri,
    watch_event_request, watch_event_response, Actions, AddPathRequest, AddPeerRequest,
    AddPolicyAssignmentRequest, Attribute, Conditions, DefinedSet, DefinedType, DeletePathRequest,
    DeletePeerRequest, DeletePolicyAssignmentRequest, Family, GetBgpRequest, Global,
    IpAddressPrefix, ListPathRequest, ListPeerRequest, MatchSet, NextHopAttribute, Nlri,
    OriginAttribute, Path, Peer, PeerConf, Policy, PolicyAssignment, PolicyDirection, Prefix,
    RouteAction, RouteReflector, SetPoliciesRequest, StartBgpRequest, Statement, TableType,
    WatchEventRequest,
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
    ///
    /// Holds the gRPC client mutex for the whole reconcile so all
    /// list/add/delete RPCs see a single consistent view of the
    /// peer set.
    pub async fn reconcile_peers(
        &self,
        desired: &[PeerSpec],
        route_reflector_client: bool,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        let current = list_peer_addresses(&mut client).await?;
        let desired_set: BTreeSet<IpAddr> = desired.iter().map(|p| p.address).collect();

        for spec in desired.iter().filter(|p| !current.contains(&p.address)) {
            let peer = peer_message(spec, route_reflector_client);
            client
                .add_peer(req(AddPeerRequest { peer: Some(peer) }))
                .await
                .map_err(|e| anyhow::anyhow!("AddPeer({}): {e}", spec.address))?;
            debug!(peer = %spec.address, asn = spec.asn, rr_client = route_reflector_client, "added gobgp peer");
        }
        for addr in current.difference(&desired_set) {
            client
                .delete_peer(req(DeletePeerRequest {
                    address: addr.to_string(),
                    interface: String::new(),
                }))
                .await
                .map_err(|e| anyhow::anyhow!("DeletePeer({addr}): {e}"))?;
            debug!(peer = %addr, "deleted gobgp peer");
        }
        Ok(())
    }

    /// Reconcile the locally-originated IPv4-unicast prefix set to
    /// exactly `desired`. Each prefix is advertised with `next_hop`
    /// (the speaker's underlay IP, i.e. its BGP router-id) and
    /// ORIGIN = IGP. gobgp v4 rejects AddPath with "nexthop not
    /// found" when the NEXT_HOP attribute is absent — every path
    /// must carry it explicitly.
    ///
    /// Holds the gRPC client mutex for the whole reconcile so all
    /// list/add/delete RPCs see a single consistent view of the RIB.
    pub async fn reconcile_ipv4_paths(
        &self,
        desired: &[String],
        next_hop: Ipv4Addr,
    ) -> anyhow::Result<()> {
        let mut client = self.inner.lock().await;
        let current = list_ipv4_prefixes(&mut client).await?;
        let desired_set: BTreeSet<String> = desired.iter().cloned().collect();

        for prefix in desired_set.difference(&current) {
            let path = ipv4_unicast_path(prefix, next_hop)?;
            client
                .add_path(req(AddPathRequest {
                    table_type: TableType::Global as i32,
                    vrf_id: String::new(),
                    path: Some(path),
                }))
                .await
                .map_err(|e| anyhow::anyhow!("AddPath({prefix}): {e}"))?;
            debug!(prefix, %next_hop, "advertised path via gobgp");
        }
        for prefix in current.difference(&desired_set) {
            let path = ipv4_unicast_path(prefix, next_hop)?;
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
        }
        Ok(())
    }

    /// Reconcile gobgpd's ingress prefix-list policy to exactly
    /// `spec`. Idempotent: gobgp's `SetPolicies` replaces the
    /// daemon's defined-sets + policies in one shot, and the
    /// import-direction PolicyAssignment is delete-then-add
    /// (gobgp doesn't expose a "replace" RPC for assignments).
    ///
    /// Effect: every BGP session's ingress is filtered. A K8s node
    /// peer can advertise only its own cluster's allocated
    /// prefixes; hypervisor peers (trusted) can advertise anything.
    /// Anything not matching one of the per-cluster statements or
    /// the hypervisor statement is dropped before reaching the
    /// RIB, so no reflection downstream.
    pub async fn reconcile_ingress_policy(&self, spec: &IngressPolicySpec) -> anyhow::Result<()> {
        const POLICY_NAME: &str = "cell-ingress";
        const ASSIGNMENT_NAME: &str = "global";
        const HYPERVISOR_SET: &str = "hypervisors";

        let mut defined_sets: Vec<DefinedSet> = Vec::new();
        let mut statements: Vec<Statement> = Vec::new();

        // Per-cluster: one prefix-set + one neighbor-set + one
        // statement that ANDs them. Skip clusters with empty
        // prefixes or empty nodes — the statement could never
        // match and gobgpd rejects empty defined-sets.
        for cluster in &spec.clusters {
            if cluster.allowed_prefixes.is_empty() || cluster.nodes.is_empty() {
                continue;
            }
            let prefix_set_name = format!("cluster-{}-prefixes", cluster.cluster_id);
            let neighbor_set_name = format!("cluster-{}-nodes", cluster.cluster_id);
            defined_sets.push(prefix_defined_set(&prefix_set_name, &cluster.allowed_prefixes)?);
            defined_sets.push(neighbor_defined_set(&neighbor_set_name, &cluster.nodes));
            statements.push(accept_statement(
                &format!("cluster-{}-import", cluster.cluster_id),
                &neighbor_set_name,
                Some(&prefix_set_name),
            ));
        }

        // Hypervisor catch-all: trusted, accept anything they
        // advertise. Omit when no hypervisors are registered yet
        // (an empty defined-set would just match nothing).
        if !spec.hypervisors.is_empty() {
            defined_sets.push(neighbor_defined_set(HYPERVISOR_SET, &spec.hypervisors));
            statements.push(accept_statement(
                "hypervisor-import",
                HYPERVISOR_SET,
                None,
            ));
        }

        let policy = Policy {
            name: POLICY_NAME.to_string(),
            statements: statements.clone(),
        };

        let mut client = self.inner.lock().await;

        // SetPolicies replaces defined-sets + policies wholesale —
        // no diff needed, gobgp's atomic apply is idempotent.
        client
            .set_policies(req(SetPoliciesRequest {
                defined_sets,
                policies: vec![policy.clone()],
                assignments: Vec::new(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("SetPolicies: {e}"))?;

        // Re-add the import-direction assignment with default
        // REJECT. gobgp doesn't expose an idempotent "set
        // assignment" RPC, so delete-then-add. The delete is
        // best-effort because on first run the assignment doesn't
        // exist yet and DeletePolicyAssignment errors with "not
        // found" — that's a clean state, not a real failure.
        let _ = client
            .delete_policy_assignment(req(DeletePolicyAssignmentRequest {
                assignment: Some(PolicyAssignment {
                    name: ASSIGNMENT_NAME.to_string(),
                    direction: PolicyDirection::Import as i32,
                    policies: Vec::new(),
                    default_action: RouteAction::Reject as i32,
                }),
                all: false,
            }))
            .await;
        client
            .add_policy_assignment(req(AddPolicyAssignmentRequest {
                assignment: Some(PolicyAssignment {
                    name: ASSIGNMENT_NAME.to_string(),
                    direction: PolicyDirection::Import as i32,
                    policies: vec![policy],
                    default_action: RouteAction::Reject as i32,
                }),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("AddPolicyAssignment: {e}"))?;

        debug!(
            clusters = spec.clusters.len(),
            hypervisors = spec.hypervisors.len(),
            statements = statements.len(),
            "ingress policy reconciled",
        );
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

}

/// Snapshot the global IPv4-unicast prefix set currently advertised
/// by gobgpd. Free function (not `&self`) because callers hold the
/// gRPC client mutex during a reconcile and pass `&mut client` —
/// this avoids the lock-unlock-relock pattern that arises when each
/// helper re-locks internally.
async fn list_ipv4_prefixes(
    client: &mut GoBgpServiceClient<Channel>,
) -> anyhow::Result<BTreeSet<String>> {
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

/// Snapshot the configured peer addresses on gobgpd. Same lock-
/// borrow contract as [`list_ipv4_prefixes`] — caller holds the
/// mutex.
async fn list_peer_addresses(
    client: &mut GoBgpServiceClient<Channel>,
) -> anyhow::Result<BTreeSet<IpAddr>> {
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

/// Build a gobgp `Peer` message for [`reconcile_peers`]. Pure —
/// nothing async, no locks; testable in isolation.
fn peer_message(spec: &PeerSpec, rr_client: bool) -> Peer {
    Peer {
        conf: Some(PeerConf {
            neighbor_address: spec.address.to_string(),
            peer_asn: spec.asn,
            ..Default::default()
        }),
        route_reflector: rr_client.then(|| RouteReflector {
            route_reflector_client: true,
            route_reflector_cluster_id: String::new(),
        }),
        ..Default::default()
    }
}

/// One peer the local speaker should hold a session with.
#[derive(Debug, Clone, Copy)]
pub struct PeerSpec {
    pub address: IpAddr,
    pub asn: u32,
}

/// Per-cell ingress prefix-list policy for the route reflector.
/// Restricts what each peer can advertise into the cell, so a
/// compromised K8s node can't hijack a sibling cluster's IPs by
/// announcing arbitrary prefixes.
///
/// Trust model:
/// * **Hypervisors** (basis-agents) are trusted — basis owns their
///   binaries and underlay IPs. They can advertise anything.
/// * **K8s nodes** (VMs running customer workloads) are restricted
///   to their own cluster's allocated address space.
///
/// Encoded as one global IMPORT [`PolicyAssignment`] with default
/// REJECT, plus a [`Policy`] containing one [`Statement`] per
/// [`ClusterIngress`] (accept if neighbor in cluster's nodes AND
/// prefix in cluster's allowed set) and one [`Statement`] for
/// hypervisors (accept if neighbor in hypervisor set).
#[derive(Debug, Clone, Default)]
pub struct IngressPolicySpec {
    pub clusters: Vec<ClusterIngress>,
    pub hypervisors: Vec<IpAddr>,
}

/// One cluster's contribution to [`IngressPolicySpec`]. Empty
/// `nodes` or `allowed_prefixes` cause the cluster's statement to
/// be omitted entirely (a statement that can't match nothing is
/// just dead weight in gobgpd's policy evaluator).
#[derive(Debug, Clone)]
pub struct ClusterIngress {
    /// Stable cluster identifier; used to namespace the
    /// cluster-specific defined-sets and statement names so a
    /// cluster's create/delete doesn't collide with siblings'.
    pub cluster_id: String,
    /// CIDRs the cluster's K8s nodes are permitted to advertise:
    /// the LB pool slice the controller carved for this cluster,
    /// the apiserver VIP /32 if APISERVER_PUBLIC, the cluster's
    /// own overlay CIDR.
    pub allowed_prefixes: Vec<String>,
    /// Cluster-overlay IPs of every K8s node in this cluster —
    /// the peer addresses gobgpd will see on incoming sessions.
    pub nodes: Vec<IpAddr>,
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

/// Build a [`DefinedSet`] of type PREFIX from a list of CIDRs.
/// Each prefix is added without min/max length bounds, so an
/// exact-match check (`MatchSet::Type::Any` against this set
/// matches any prefix that is exactly one of the entries). Returns
/// an error on any unparseable CIDR — silently dropping malformed
/// inputs would let an attacker advertise any prefix by
/// substituting garbage in the upstream config.
fn prefix_defined_set(name: &str, prefixes: &[String]) -> anyhow::Result<DefinedSet> {
    let mut out = Vec::with_capacity(prefixes.len());
    for raw in prefixes {
        let net: ipnet::IpNet = raw
            .parse()
            .map_err(|e| anyhow::anyhow!("prefix '{raw}' in defined-set '{name}': {e}"))?;
        out.push(Prefix {
            ip_prefix: net.to_string(),
            mask_length_min: net.prefix_len() as u32,
            mask_length_max: net.prefix_len() as u32,
        });
    }
    Ok(DefinedSet {
        defined_type: DefinedType::Prefix as i32,
        name: name.to_string(),
        list: Vec::new(),
        prefixes: out,
    })
}

/// Build a [`DefinedSet`] of type NEIGHBOR. gobgpd compares
/// session source addresses against this list, so each entry is a
/// single IP rendered as a `/32` (or `/128` for IPv6) — gobgp's
/// neighbor-set parser interprets bare IPs as host-routes either
/// way, but the explicit prefix length is documented and
/// upstream-stable.
fn neighbor_defined_set(name: &str, addrs: &[IpAddr]) -> DefinedSet {
    DefinedSet {
        defined_type: DefinedType::Neighbor as i32,
        name: name.to_string(),
        list: addrs
            .iter()
            .map(|a| match a {
                IpAddr::V4(v) => format!("{v}/32"),
                IpAddr::V6(v) => format!("{v}/128"),
            })
            .collect(),
        prefixes: Vec::new(),
    }
}

/// Build a [`Statement`] that accepts when the incoming neighbor
/// is in `neighbor_set` and (optionally) the prefix is in
/// `prefix_set`. Non-matching paths fall through to the next
/// statement; the policy assignment's default action catches the
/// unmatched tail.
fn accept_statement(name: &str, neighbor_set: &str, prefix_set: Option<&str>) -> Statement {
    Statement {
        name: name.to_string(),
        conditions: Some(Conditions {
            prefix_set: prefix_set.map(|n| MatchSet {
                r#type: match_set::Type::Any as i32,
                name: n.to_string(),
            }),
            neighbor_set: Some(MatchSet {
                r#type: match_set::Type::Any as i32,
                name: neighbor_set.to_string(),
            }),
            ..Default::default()
        }),
        actions: Some(Actions {
            route_action: RouteAction::Accept as i32,
            ..Default::default()
        }),
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

    /// PREFIX defined-set must round-trip to gobgp's wire shape:
    /// `defined_type=PREFIX`, `prefixes` populated (not `list`),
    /// each Prefix's mask-length min/max pinned to the prefix's
    /// own length so an exact-match check (gobgp's MatchSet ANY)
    /// matches only the literal CIDR.
    #[test]
    fn prefix_defined_set_emits_exact_match_bounds() {
        let s = prefix_defined_set("test", &["10.0.0.0/24".to_string(), "10.0.0.5/32".to_string()])
            .unwrap();
        assert_eq!(s.defined_type, DefinedType::Prefix as i32);
        assert_eq!(s.name, "test");
        assert!(s.list.is_empty());
        assert_eq!(s.prefixes.len(), 2);
        // Both bounds equal the prefix length → exact match.
        let p24 = s.prefixes.iter().find(|p| p.ip_prefix == "10.0.0.0/24").unwrap();
        assert_eq!(p24.mask_length_min, 24);
        assert_eq!(p24.mask_length_max, 24);
        let p32 = s.prefixes.iter().find(|p| p.ip_prefix == "10.0.0.5/32").unwrap();
        assert_eq!(p32.mask_length_min, 32);
        assert_eq!(p32.mask_length_max, 32);
    }

    /// Malformed CIDRs must fail the build, not silently drop into
    /// an empty defined-set — silent drops would let an attacker
    /// neutralize the policy by submitting garbage in upstream
    /// config.
    #[test]
    fn prefix_defined_set_rejects_garbage() {
        assert!(prefix_defined_set("test", &["not-a-cidr".to_string()]).is_err());
    }

    /// NEIGHBOR defined-set must round-trip with `list` populated
    /// (not `prefixes`); IPv4 addresses are rendered as `/32`,
    /// IPv6 as `/128`. Mismatching the field gobgp expects produces
    /// a silently-empty match in the daemon.
    #[test]
    fn neighbor_defined_set_renders_v4_and_v6_as_host_routes() {
        let s = neighbor_defined_set(
            "test",
            &["10.0.0.1".parse().unwrap(), "fe80::1".parse().unwrap()],
        );
        assert_eq!(s.defined_type, DefinedType::Neighbor as i32);
        assert!(s.prefixes.is_empty());
        assert!(s.list.contains(&"10.0.0.1/32".to_string()));
        assert!(s.list.contains(&"fe80::1/128".to_string()));
    }

    /// `accept_statement` builds the standard "if neighbor in NS
    /// AND prefix in PS → accept" shape: both sets joined by ANY
    /// match-type, route-action ACCEPT, no other actions touched.
    #[test]
    fn accept_statement_with_prefix_and_neighbor() {
        let s = accept_statement("stmt", "neighbors", Some("prefixes"));
        assert_eq!(s.name, "stmt");
        let conds = s.conditions.unwrap();
        let ns = conds.neighbor_set.unwrap();
        assert_eq!(ns.name, "neighbors");
        assert_eq!(ns.r#type, match_set::Type::Any as i32);
        let ps = conds.prefix_set.unwrap();
        assert_eq!(ps.name, "prefixes");
        assert_eq!(ps.r#type, match_set::Type::Any as i32);
        let actions = s.actions.unwrap();
        assert_eq!(actions.route_action, RouteAction::Accept as i32);
    }

    /// `accept_statement` with `prefix_set: None` (the hypervisor
    /// catch-all shape) emits no prefix_set in conditions —
    /// otherwise gobgp would fail to compile a policy referencing
    /// a missing prefix-set.
    #[test]
    fn accept_statement_omits_prefix_set_when_none() {
        let s = accept_statement("stmt", "neighbors", None);
        let conds = s.conditions.unwrap();
        assert!(conds.prefix_set.is_none());
        assert!(conds.neighbor_set.is_some());
    }

    /// `peer_message` with `rr_client=true` (the route-reflector's
    /// view of cell speakers) sets `route_reflector_client=true`
    /// on the peer; with `rr_client=false` (a host speaker's view
    /// of the RR) the route-reflector field is absent entirely.
    /// Both shapes are what gobgp v4 expects on AddPeer.
    #[test]
    fn peer_message_rr_client_flag() {
        let spec = PeerSpec {
            address: "10.0.0.1".parse().unwrap(),
            asn: 64512,
        };
        let p = peer_message(&spec, true);
        let conf = p.conf.as_ref().unwrap();
        assert_eq!(conf.neighbor_address, "10.0.0.1");
        assert_eq!(conf.peer_asn, 64512);
        let rr = p.route_reflector.unwrap();
        assert!(rr.route_reflector_client);

        let p = peer_message(&spec, false);
        assert!(p.route_reflector.is_none());
    }
}
