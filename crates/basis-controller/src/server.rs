use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use basis_proto::*;
use dashmap::DashMap;
use futures::Stream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, trace, warn};

use basis_common::gpu::GpuInfo;
use basis_common::time::now_rfc3339;
use basis_common::tls;

use crate::config::{BgpConfig, NetworkConfig, Pool, SafetyConfig};
use crate::db::{
    ClusterIdentity, ClusterRow, Db, DbError, GpuAssignment, VmRow, VM_STATE_PENDING_TEARDOWN,
};
use crate::metrics::Metrics;
use crate::scheduler::{self, ScheduleRequest, SchedulerError};

/// How many times `create_machine` will re-run the
/// pick-host-then-commit cycle before giving up. A capacity race
/// loses a single attempt; in a fleet with > 5 concurrent contenders
/// for the same last slot we'd rather return an honest
/// `ResourceExhausted` than retry forever.
const MAX_SCHEDULE_ATTEMPTS: u32 = 5;

/// Map a [`DbError`] to the gRPC status the API should return.
fn db_status(e: DbError) -> Status {
    match e {
        DbError::NotFound(_) => Status::not_found(e.to_string()),
        DbError::Conflict(_) => Status::already_exists(e.to_string()),
        DbError::Exhausted(_) => Status::resource_exhausted(e.to_string()),
        DbError::HostUnavailable(_) | DbError::CapacityRaced(_) | DbError::AllocationRaced(_) => {
            Status::unavailable(e.to_string())
        }
        DbError::Sqlx(_) | DbError::Migrate(_) | DbError::Malformed(_) => {
            Status::internal(e.to_string())
        }
    }
}

/// Require that a CAPI-facing RPC was issued by a client whose peer
/// identity is [`tls::CAPI_PROVIDER_IDENTITY`].
fn require_capi_caller<T>(req: &Request<T>) -> Result<(), Status> {
    let id = peer_identity(req)?;
    if id == tls::CAPI_PROVIDER_IDENTITY {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "peer identity '{id}' is not authorized for CAPI RPCs \
             (expected '{}')",
            tls::CAPI_PROVIDER_IDENTITY,
        )))
    }
}

fn peer_identity<T>(req: &Request<T>) -> Result<String, Status> {
    match tls::request_peer_identity(req) {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(Status::unauthenticated("TLS required")),
        Err(e) => Err(Status::unauthenticated(format!("peer certificate: {e}"))),
    }
}

/// CAPI-shaped provider ID for a VM.
///
/// Must match what the kubelet inside the guest reports on
/// `Node.spec.providerID`. Lattice templates
/// `provider-id=basis://{{ ds.meta_data.instance_id }}` into kubeadm's
/// kubelet args, and basis-agent writes `instance-id: <vm_id>` into
/// cloud-init's meta-data — so the VM's reported providerID is
/// `basis://<vm_id>`.
pub fn provider_id(vm_id: &str) -> String {
    format!("basis://{vm_id}")
}

/// Classify a cluster's pool-allocated VIPs into LAN-routable vs
/// tree-scoped lists for `ClusterState.cluster_vips` and
/// `ClusterState.internal_cluster_vips` respectively.
///
/// `apiserver_visibility` decides where the apiserver VIP comes
/// FROM — `APISERVER_PUBLIC` allocates it from `external_pool`,
/// `APISERVER_PRIVATE` puts it at the last usable address in the
/// cluster's tree CIDR (and so it doesn't appear in either list,
/// since it's reachable purely via the cluster's bridge route from
/// `gateway_ip`). `pool.scope` decides where any pool-allocated VIP
/// is reachable:
///
/// * `Lan` — advertised cell-wide via BGP, proxy-ARPed onto the
///   uplink, and bridge route. LAN-reachable.
/// * `Tree` — bridge route only, no BGP/proxy-ARP. The LAN can NOT
///   reach the VIP; cross-cluster reachability within the cell flows
///   through each host's per-cluster bridge.
///
/// `APISERVER_PUBLIC + Tree` is therefore a valid combination — the
/// apiserver VIP is allocated from the tree pool's CIDR and is
/// reachable from any cluster in the cell that can route to that
/// CIDR (every host installs a bridge route via the eager-bootstrap
/// path), but never reaches the LAN. That's the intended shape for
/// internal clusters whose apiserver needs a stable named-pool
/// address without LAN exposure.
///
/// The cluster's overlay CIDR itself is intentionally NOT advertised
/// either way: VM IPs are private to the cluster's bridge, no
/// inter-cluster L3.
///
/// The caller must resolve the cluster's `external_pool` before
/// calling this — passing an explicit `&Pool` makes the
/// "what if the pool was removed from config?" question a
/// fail-loud at the lookup site rather than a silent fallback here.
fn classify_cluster_vips(
    cluster: &ClusterRow,
    visibility: ApiserverVisibility,
    pool: &Pool,
    owner_host_id: &str,
) -> (Vec<ClusterVip>, Vec<String>) {
    let mut cluster_vips: Vec<ClusterVip> = Vec::with_capacity(2);
    let mut internal_cluster_vips: Vec<String> = Vec::with_capacity(2);
    if visibility == ApiserverVisibility::ApiserverPublic {
        if let Ok(vip) = cluster.control_plane_endpoint.parse::<std::net::Ipv4Addr>() {
            if pool.is_tree() {
                internal_cluster_vips.push(format!("{vip}/32"));
            } else {
                cluster_vips.push(ClusterVip {
                    cidr: format!("{vip}/32"),
                    owner_host_id: owner_host_id.to_string(),
                });
            }
        }
    }
    if !cluster.service_block_cidr.is_empty() {
        if pool.is_tree() {
            internal_cluster_vips.push(cluster.service_block_cidr.clone());
        } else {
            cluster_vips.push(ClusterVip {
                cidr: cluster.service_block_cidr.clone(),
                owner_host_id: owner_host_id.to_string(),
            });
        }
    }
    (cluster_vips, internal_cluster_vips)
}

/// Agent-reported VM create failure. Carries enough to tell a real
/// fault apart from a load-shedding signal so the controller can map
/// the two onto different gRPC status codes and metric labels.
#[derive(Debug, Clone)]
struct VmFailure {
    message: String,
    transient: bool,
}

/// Pending CreateMachine waiting for the agent to report RUNNING.
/// Delete is async (mark pending → reconcile re-emits tombstone →
/// agent acks → DB drops the row), so there's nothing to wait on for
/// that path.
struct PendingVmOp {
    tx: oneshot::Sender<Result<(), VmFailure>>,
    /// Host this op was dispatched to. When the host's agent stream
    /// drops, we remove every entry matching this host_id so the
    /// awaiting RPC fails immediately instead of stalling for the full
    /// timeout window.
    host_id: String,
}

/// Connected agent with a command channel.
///
/// `epoch` is the per-process unique generation stamped at the moment
/// this stream was registered. Each new `stream_messages` call mints a
/// fresh epoch via [`next_agent_epoch`], so a stale stream's cleanup
/// can compare-and-remove against the live entry: if a faster reconnect
/// has already replaced this connection's slot, the old cleanup must
/// not clobber the new one's `command_tx`, `pending_ops`, or metric.
struct ConnectedAgent {
    command_tx: mpsc::Sender<ControllerCommand>,
    epoch: u64,
}

/// Mint a process-unique generation id for a new agent stream.
fn next_agent_epoch() -> u64 {
    static AGENT_EPOCH: AtomicU64 = AtomicU64::new(1);
    AGENT_EPOCH.fetch_add(1, Ordering::Relaxed)
}

/// Release the per-agent state owned by an `epoch`-tagged stream
/// whose connection just ended.
///
/// The compare-and-remove on `epoch` is the synchronisation point with
/// `stream_messages`'s `agents.insert`: a faster reconnect mints a new
/// epoch and replaces the slot, after which the *old* stream's cleanup
/// (still observing its own epoch) no-ops here. That keeps the live
/// connection's `command_tx`, in-flight `pending_ops` waiters, and
/// `agent_connected` metric untouched. When the slot still belongs to
/// the caller, drain the host's pending waiters (their dispatch path
/// is gone) and zero the metric in one place.
///
/// Free function (rather than a method) so it can be tested with
/// purpose-built `DashMap`s and a fresh `Metrics` registry, without
/// constructing a full `SharedCtx`.
fn release_agent_stream(
    agents: &DashMap<String, ConnectedAgent>,
    pending_ops: &DashMap<String, PendingVmOp>,
    metrics: &Metrics,
    host_id: &str,
    hostname: &str,
    epoch: u64,
) {
    if agents.remove_if(host_id, |_, a| a.epoch == epoch).is_none() {
        debug!(
            host_id,
            epoch, "stream cleanup skipped: superseded by newer connection"
        );
        return;
    }
    let stale: Vec<String> = pending_ops
        .iter()
        .filter(|e| e.value().host_id == host_id)
        .map(|e| e.key().clone())
        .collect();
    for vm_id in &stale {
        pending_ops.remove(vm_id);
    }
    if !stale.is_empty() {
        warn!(
            host_id,
            cancelled = stale.len(),
            "cancelled in-flight VM op waiters for disconnected agent"
        );
    }
    metrics
        .agent_connected
        .with_label_values(&[hostname])
        .set(0);
}

pub struct BasisServer {
    db: Db,
    metrics: Arc<Metrics>,
    dns_servers: Arc<Vec<String>>,
    network: Arc<NetworkConfig>,
    bgp: Arc<BgpConfig>,
    safety: Arc<SafetyConfig>,
    reconcile_interval: std::time::Duration,
    agents: Arc<DashMap<String, ConnectedAgent>>,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
}

/// How often the controller pushes the authoritative reconcile command
/// to each connected agent. Short enough that a missed incremental push
/// (network blip, agent reconnect) still converges within a minute;
/// long enough that steady-state fleet chatter stays sub-1Hz/agent.
const DEFAULT_AGENT_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Total time `CreateMachine` will wait for the agent to report RUNNING.
const CREATE_MACHINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

impl BasisServer {
    pub fn new(
        db: Db,
        metrics: Arc<Metrics>,
        dns_servers: Vec<String>,
        network: NetworkConfig,
        bgp: BgpConfig,
        safety: SafetyConfig,
    ) -> Self {
        Self {
            db,
            metrics,
            dns_servers: Arc::new(dns_servers),
            network: Arc::new(network),
            bgp: Arc::new(bgp),
            safety: Arc::new(safety),
            reconcile_interval: DEFAULT_AGENT_RECONCILE_INTERVAL,
            agents: Arc::new(DashMap::new()),
            pending_ops: Arc::new(DashMap::new()),
        }
    }

    /// Override the controller→agent reconcile cadence. For tests only.
    pub fn with_reconcile_interval(mut self, interval: std::time::Duration) -> Self {
        self.reconcile_interval = interval;
        self
    }

    /// Serve on a caller-provided TCP listener with a caller-provided TLS config.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        tls_config: ServerTlsConfig,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let addr = listener.local_addr()?;
        let (basis_svc, agent_svc) = self.into_services();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        info!(%addr, "starting gRPC server");

        Server::builder()
            .tls_config(tls_config)?
            .concurrency_limit_per_connection(64)
            .layer(tower::limit::ConcurrencyLimitLayer::new(256))
            .add_service(basis_svc)
            .add_service(agent_svc)
            .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
            .await?;

        Ok(())
    }

    fn into_services(
        self,
    ) -> (
        basis_server::BasisServer<BasisApiService>,
        basis_agent_server::BasisAgentServer<BasisAgentService>,
    ) {
        let shared = Arc::new(SharedCtx {
            db: self.db.clone(),
            metrics: self.metrics.clone(),
            dns_servers: self.dns_servers.clone(),
            network: self.network.clone(),
            bgp: self.bgp.clone(),
            safety: self.safety.clone(),
            agents: self.agents.clone(),
            pending_ops: self.pending_ops.clone(),
            placement_lock: Mutex::new(()),
        });
        let basis_svc = basis_server::BasisServer::new(BasisApiService {
            shared: shared.clone(),
        });
        let agent_svc = basis_agent_server::BasisAgentServer::new(BasisAgentService {
            shared,
            reconcile_interval: self.reconcile_interval,
        });
        (basis_svc, agent_svc)
    }
}

/// State shared across the CAPI-facing and agent-facing services.
struct SharedCtx {
    db: Db,
    metrics: Arc<Metrics>,
    dns_servers: Arc<Vec<String>>,
    network: Arc<NetworkConfig>,
    bgp: Arc<BgpConfig>,
    safety: Arc<SafetyConfig>,
    agents: Arc<DashMap<String, ConnectedAgent>>,
    /// Tracks `CreateMachine` waiters keyed by `vm_id`. Inserted by
    /// the API service before dispatching the agent command and
    /// resolved by the agent stream when the VM transitions to a
    /// terminal state. Lives on `SharedCtx` so both services share
    /// one map without each holding its own clone.
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
    /// Serializes placement: held across `pick_host` → `insert_vm` so
    /// each scoring pass sees the prior placement's commit. Without
    /// this, N concurrent creates against an empty cluster all read
    /// the same "0 VMs everywhere" snapshot, all tie at every score
    /// dimension, and all pick the first host in iteration order —
    /// stampeding one host until anti-affinity catches up on the next
    /// snapshot.
    placement_lock: Mutex<()>,
}

impl SharedCtx {
    /// Assemble the full authoritative `ReconcileHostCommand` for a
    /// host. Tombstone-driven: clusters[] is the set of (host, cluster)
    /// rows in state ACTIVE; cluster_tombstones[] is the set in state
    /// PENDING_TEARDOWN; vm_tombstones[] is every vms row on this host
    /// with state PENDING_TEARDOWN. The agent reconciles additively
    /// against clusters[] and explicitly tears down tombstones — there
    /// is no implicit-by-absence delete, so a transient empty snapshot
    /// (CAPI churn, controller restart mid-flow) doesn't tear down any
    /// host state. Same shape on every emission path: register-ack,
    /// periodic tick, and after-change broadcasts.
    async fn build_reconcile_command(
        &self,
        host_id: &str,
    ) -> Result<ReconcileHostCommand, DbError> {
        let active_memberships = self.db.list_active_host_clusters(host_id).await?;
        let mut cluster_states = Vec::with_capacity(active_memberships.len());
        let mut carried: HashSet<String> = HashSet::with_capacity(active_memberships.len());
        for membership in active_memberships {
            carried.insert(membership.id.clone());
            let cluster = membership.cluster();
            let visibility = ApiserverVisibility::try_from(cluster.apiserver_visibility as i32)
                .map_err(|_| {
                    DbError::Malformed(format!(
                        "cluster {} has unknown apiserver_visibility {}",
                        cluster.id, cluster.apiserver_visibility,
                    ))
                })?;
            // Fail loud when a stored cluster references a pool that
            // is no longer in the controller config: the operator
            // either removed/renamed the pool or the DB lost sync.
            // Either way, silently treating the cluster as Lan would
            // hide a real misconfiguration.
            let pool = self
                .network
                .pool_by_name(&cluster.external_pool)
                .ok_or_else(|| {
                    DbError::Malformed(format!(
                        "cluster {} references pool '{}' which is not defined in network.pools \
                     (config drift — restore the pool entry or update the cluster)",
                        cluster.id, cluster.external_pool,
                    ))
                })?;
            // Sticky single-responder LAN-VIP owner. See
            // `Db::elect_lan_vip_owner` for the election rule and why
            // sticky-on-member-add is the fix for the worker-placement
            // black-hole.
            let carriers = self.db.list_active_carriers(&cluster.id).await?;
            let owner_host_id = self.db.elect_lan_vip_owner(&cluster.id, &carriers).await?;
            let (cluster_vips, internal_cluster_vips) =
                classify_cluster_vips(&cluster, visibility, pool, &owner_host_id);
            trace!(
                cluster_id = %cluster.id,
                vni = cluster.vni,
                carriers = ?carriers,
                owner = %owner_host_id,
                visibility = ?visibility,
                pool = %cluster.external_pool,
                lan_vips = cluster_vips.len(),
                tree_vips = internal_cluster_vips.len(),
                "cluster reconcile classification",
            );
            cluster_states.push(ClusterState {
                cluster_id: cluster.id.clone(),
                vni: cluster.vni as u32,
                cidr: cluster.cidr.clone(),
                gateway_ip: membership.bridge_ip.clone(),
                prefix_len: cluster.prefix_len as u32,
                vtep_addresses: self.db.list_cluster_vteps(&cluster.id).await?,
                trust_domain: cluster.trust_domain.clone(),
                cluster_vips,
                internal_cluster_vips,
            });
        }

        // Eager bootstrap of tree-scoped clusters this host does not
        // carry. Tree pool VIPs are reachable only over per-cluster
        // bridges and are *not* advertised on the LAN, so a VM in
        // cluster A on host H needs `<B.vip> dev brcB` on H even if
        // no VM of cluster B runs on H. Emitting a ghost
        // `ClusterState` (`cidr` populated, `gateway_ip` empty) drives
        // the agent's existing `ensure_cluster_inner` to create the
        // bridge + VXLAN with the right peer FDB and install a
        // transit-scope link route for `cidr` in the cluster's VRF
        // table — required so a LAN-VIP VM on H replying to a source
        // in cluster B's CIDR has a non-default route in the VRF and
        // doesn't leak out the uplink. `gateway_ip` and MASQUERADE
        // are skipped because no VM of B runs here.
        //
        // Trust-domain isolation is enforced on the agent side via
        // per-tree Linux VRFs — every `brc<vni>` is enslaved to the
        // VRF named after its `trust_domain`, and route fan-out
        // installs into that VRF's table. So fan-out here is
        // unconditional: the controller ships every tree cluster to
        // every host, and the kernel makes cross-tree traffic die
        // when the source bridge's VRF table has no route to the
        // foreign tree's VIPs. trust_domain is metadata on the tree,
        // not a host attribute.
        for cluster in self.db.list_clusters().await? {
            if carried.contains(&cluster.id) {
                continue;
            }
            let pool = self
                .network
                .pool_by_name(&cluster.external_pool)
                .ok_or_else(|| {
                    DbError::Malformed(format!(
                        "cluster {} references pool '{}' which is not defined in network.pools \
                     (config drift — restore the pool entry or update the cluster)",
                        cluster.id, cluster.external_pool,
                    ))
                })?;
            if !pool.is_tree() {
                continue;
            }
            // Fan-out hosts share the same VIP classification as
            // carrier hosts; only `cluster_vips` differs (always empty
            // here — fan-out hosts don't proxy-ARP on the LAN). Reuse
            // `classify_cluster_vips` with an empty owner so the tree-
            // pool branch populates `internal_cluster_vips` exactly as
            // the carrier branch would; the LAN list is forced empty
            // because non-tree pools never reach this loop body.
            let visibility = ApiserverVisibility::try_from(cluster.apiserver_visibility as i32)
                .map_err(|_| {
                    DbError::Malformed(format!(
                        "cluster {} has unknown apiserver_visibility {}",
                        cluster.id, cluster.apiserver_visibility,
                    ))
                })?;
            let (_, internal_cluster_vips) = classify_cluster_vips(&cluster, visibility, pool, "");
            cluster_states.push(ClusterState {
                cluster_id: cluster.id.clone(),
                vni: cluster.vni as u32,
                cidr: cluster.cidr.clone(),
                gateway_ip: String::new(),
                prefix_len: 0,
                vtep_addresses: self.db.list_cluster_vteps(&cluster.id).await?,
                trust_domain: cluster.trust_domain.clone(),
                cluster_vips: Vec::new(),
                internal_cluster_vips,
            });
        }

        let cluster_tombstones = self
            .db
            .list_pending_cluster_tombstones(host_id)
            .await?
            .into_iter()
            .map(|t| ClusterTombstone {
                vni: t.vni as u32,
                cluster_id: t.cluster_id,
                cidr: t.cidr,
            })
            .collect();
        let vm_tombstones = self.db.list_pending_vm_tombstones(host_id).await?;

        Ok(ReconcileHostCommand {
            clusters: cluster_states,
            cluster_tombstones,
            vm_tombstones,
        })
    }

    /// Diff `inventory` against the DB and append synthesised
    /// tombstones to `cmd.cluster_tombstones` / `cmd.vm_tombstones`
    /// for any kernel state the agent reports but the controller
    /// doesn't track.
    ///
    /// Gated on `safety.auto_reconcile_orphan_inventory`. When the
    /// flag is OFF (default), this function logs every orphan loudly
    /// and changes nothing, so an operator can see "the agents have
    /// state I don't recognise" and decide whether the DB has been
    /// lost (don't tombstone — restore the DB) or wiped deliberately
    /// (flip the flag, restart, agents reconnect, clean up). This is
    /// the safety mechanism that prevents
    /// "controller.db lost ⇒ every VM auto-deleted on reconnect".
    ///
    /// When the flag is ON, synthesised tombstones aren't backed by
    /// DB rows — there's nothing to drop on ack — so re-emitting them
    /// on a subsequent register (if the first ack was lost) just
    /// produces another idempotent no-op teardown on the agent side.
    async fn extend_with_orphan_tombstones(
        &self,
        host_id: &str,
        inventory: &HostInventory,
        cmd: &mut ReconcileHostCommand,
    ) -> Result<(), DbError> {
        // First pass: classify every reported resource as either
        // tracked (DB has a row), already-tombstoned (in this
        // reconcile's lists), or orphan. Orphans are the candidates
        // that either get appended (flag ON) or merely logged
        // (flag OFF, default — the safety freeze).
        let already_t_vms: HashSet<String> = cmd.vm_tombstones.iter().cloned().collect();
        let mut orphan_vm_ids: Vec<String> = Vec::new();
        for vm_id in &inventory.vm_ids {
            if already_t_vms.contains(vm_id) {
                continue;
            }
            let owned_here = match self.db.get_vm(vm_id).await {
                Ok(row) => row.host_id == host_id,
                Err(DbError::NotFound(_)) => false,
                Err(e) => return Err(e),
            };
            if !owned_here {
                orphan_vm_ids.push(vm_id.clone());
            }
        }

        let mut tracked_vnis: HashSet<u32> = self
            .db
            .list_active_host_clusters(host_id)
            .await?
            .iter()
            .map(|m| m.vni as u32)
            .collect();
        for t in &self.db.list_pending_cluster_tombstones(host_id).await? {
            tracked_vnis.insert(t.vni as u32);
        }
        let already_t_vnis: HashSet<u32> = cmd.cluster_tombstones.iter().map(|t| t.vni).collect();
        let mut orphan_clusters: Vec<&InventoryCluster> = Vec::new();
        for inv_cluster in &inventory.clusters {
            if tracked_vnis.contains(&inv_cluster.vni) || already_t_vnis.contains(&inv_cluster.vni)
            {
                continue;
            }
            orphan_clusters.push(inv_cluster);
        }

        if orphan_vm_ids.is_empty() && orphan_clusters.is_empty() {
            return Ok(());
        }

        // When `safety.auto_reconcile_orphan_inventory` is OFF, log
        // every orphan loudly and return WITHOUT appending
        // tombstones. The agent's reconcile keeps everything alive —
        // exactly the behaviour we want when the controller's DB has
        // been lost or restored from an old snapshot.
        if !self.safety.auto_reconcile_orphan_inventory {
            warn!(
                host_id,
                orphan_vm_ids = ?orphan_vm_ids,
                orphan_cluster_vnis = ?orphan_clusters.iter().map(|c| c.vni).collect::<Vec<_>>(),
                "agent reports inventory the controller's DB doesn't track; preserving \
                 it untouched. Set spec.safety.autoReconcileOrphanInventory=true to \
                 auto-clean (only do that if the DB was wiped deliberately). Otherwise \
                 restore the controller DB before flipping the flag.",
            );
            return Ok(());
        }

        warn!(
            host_id,
            orphan_vm_ids = ?orphan_vm_ids,
            orphan_cluster_vnis = ?orphan_clusters.iter().map(|c| c.vni).collect::<Vec<_>>(),
            "autoReconcileOrphanInventory=true: emitting one-shot tombstones for \
             unrecognised inventory; agent will tear these down on receipt",
        );

        cmd.vm_tombstones.extend(orphan_vm_ids);
        for inv_cluster in orphan_clusters {
            cmd.cluster_tombstones.push(ClusterTombstone {
                vni: inv_cluster.vni,
                // The controller has no cluster_id for this orphan;
                // empty is fine — the agent only uses vni + cidr for
                // teardown, cluster_id is for logging/diagnostics.
                cluster_id: String::new(),
                cidr: inv_cluster.cidr.clone(),
            });
        }
        Ok(())
    }

    /// Send a fresh reconcile to the given host if its agent is
    /// connected. Best-effort — a disconnected agent picks up the
    /// state on reconnect.
    async fn push_reconcile(&self, host_id: &str) {
        let cmd = match self.build_reconcile_command(host_id).await {
            Ok(c) => c,
            Err(e) => {
                warn!(host_id, error = %e, "push_reconcile: failed to build command");
                return;
            }
        };
        if let Some(agent) = self.agents.get(host_id) {
            if let Err(e) = agent
                .command_tx
                .send(ControllerCommand {
                    request_id: String::new(),
                    command: Some(controller_command::Command::ReconcileHost(Box::new(cmd))),
                })
                .await
            {
                // Channel send only fails when the per-agent task is
                // gone (i.e. the agent disconnected). Recovery is
                // implicit: on reconnect the agent gets a fresh
                // reconcile from `register_host`. Trace-only so the
                // diagnostic exists without spamming on routine churn.
                tracing::trace!(host_id, error = %e, "push_reconcile: agent channel closed");
            }
        }
    }

    /// Re-broadcast reconcile to every host currently carrying the
    /// given cluster. Called after a VM create/delete that shifts the
    /// peer VTEP set for that cluster's overlay.
    async fn broadcast_cluster(&self, cluster_id: &str) {
        let hosts = match self.db.list_hosts_in_cluster(cluster_id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(cluster_id, error = %e, "broadcast_cluster: list hosts failed");
                return;
            }
        };
        for host_id in hosts {
            self.push_reconcile(&host_id).await;
        }
    }

    /// Roll a VM the controller knows about but no longer has on the
    /// host (failed CreateMachine, VM disappeared from agent
    /// inventory, etc.) into the tombstone-driven teardown pipeline.
    /// Mark the VM pending teardown (no-op if its row never made it
    /// in), mark its host_cluster pending teardown iff this was the
    /// last live VM there, then nudge the host so the next reconcile
    /// carries the tombstones. Allocations + the vms row drain via
    /// the agent's `TombstoneAck` — same path every other delete
    /// takes.
    async fn cleanup_failed_vm(&self, vm_id: &str, cluster_id: &str, host_id: &str) {
        match self.db.mark_vm_pending_teardown(vm_id).await {
            Ok(()) => {}
            // No vms row means insert_vm never landed (e.g. capacity
            // race lost). The orphan ip_allocations entry is released
            // below; nothing else to do.
            Err(DbError::NotFound(_)) => {}
            Err(e) => warn!(vm_id, error = %e, "cleanup: mark VM pending teardown"),
        }
        if let Err(e) = self.db.release_vm_ips(vm_id).await {
            warn!(vm_id, error = %e, "cleanup: release VM IPs");
        }
        if let Err(e) = self
            .db
            .mark_host_cluster_pending_teardown(cluster_id, host_id)
            .await
        {
            warn!(
                vm_id, cluster_id, host_id, error = %e,
                "cleanup: mark host_cluster pending teardown",
            );
        }
        self.push_reconcile(host_id).await;
    }

    /// Reverse-direction inventory reconciliation: the controller's
    /// DB has rows the agent's reported `current_inventory` doesn't
    /// include. Three cases, all converted into the standard
    /// teardown pipeline so there is exactly one "delete" code path:
    ///
    ///   * VM in PENDING/CREATING/RUNNING — call `cleanup_failed_vm`.
    ///     The row flips to PENDING_TEARDOWN, the agent gets a
    ///     tombstone (idempotent no-op since it has no such VM), and
    ///     the next ack drops the row + releases allocations.
    ///   * VM in PENDING_TEARDOWN — synthesise an ack-equivalent: the
    ///     teardown is implicitly satisfied (the resource is gone),
    ///     so drop the row + release IPs/GPUs directly via
    ///     `ack_tombstones`. Symmetric to the agent-orphan path
    ///     where the controller synthesises a one-shot tombstone.
    ///   * host_cluster in PENDING_TEARDOWN — same ack-equivalent.
    ///
    /// Terminal-state VMs (FAILED/STOPPED) are left alone — they're
    /// already in a CAPI-visible terminal state and not running, so
    /// agent-side absence isn't surprising.
    ///
    /// Active host_clusters that the agent doesn't have are NOT
    /// reaped here: the additive reconcile carried in the same
    /// register-ack will recreate the bridge from `clusters[]` on
    /// the next apply pass. Reaping the row would be wrong.
    ///
    /// Gated by `safety.auto_reconcile_orphan_inventory`: when the
    /// flag is OFF (default), log loudly and return without touching
    /// the DB. Symmetric to `extend_with_orphan_tombstones`.
    async fn reap_db_orphans_from_inventory(
        &self,
        host_id: &str,
        inventory: &HostInventory,
    ) -> Result<(), DbError> {
        let inv_vms: HashSet<&str> = inventory.vm_ids.iter().map(String::as_str).collect();
        let inv_vnis: HashSet<u32> = inventory.clusters.iter().map(|c| c.vni).collect();

        let mut live_orphans: Vec<VmRow> = Vec::new();
        let mut pending_vm_orphans: Vec<String> = Vec::new();
        for vm in self.db.list_vms_on_host(host_id).await? {
            if inv_vms.contains(vm.id.as_str()) {
                continue;
            }
            match vm.state {
                s if s == VM_STATE_PENDING_TEARDOWN => pending_vm_orphans.push(vm.id.clone()),
                s if s == MachineState::Pending as i64
                    || s == MachineState::Creating as i64
                    || s == MachineState::Running as i64 =>
                {
                    live_orphans.push(vm);
                }
                _ => {}
            }
        }

        let mut pending_cluster_orphans: Vec<u32> = Vec::new();
        for t in self.db.list_pending_cluster_tombstones(host_id).await? {
            let vni = t.vni as u32;
            if !inv_vnis.contains(&vni) {
                pending_cluster_orphans.push(vni);
            }
        }

        if live_orphans.is_empty()
            && pending_vm_orphans.is_empty()
            && pending_cluster_orphans.is_empty()
        {
            return Ok(());
        }

        if !self.safety.auto_reconcile_orphan_inventory {
            warn!(
                host_id,
                live_vm_orphans = ?live_orphans.iter().map(|v| &v.id).collect::<Vec<_>>(),
                pending_vm_orphans = ?pending_vm_orphans,
                pending_cluster_orphans = ?pending_cluster_orphans,
                "DB has rows the agent's inventory doesn't include; preserving them \
                 untouched. Set spec.safety.autoReconcileOrphanInventory=true to roll \
                 live VMs into the teardown pipeline + reap pending teardowns. \
                 Otherwise investigate manually — agent inventory may be stale or buggy.",
            );
            return Ok(());
        }

        warn!(
            host_id,
            live_vm_orphans = ?live_orphans.iter().map(|v| &v.id).collect::<Vec<_>>(),
            pending_vm_orphans = ?pending_vm_orphans,
            pending_cluster_orphans = ?pending_cluster_orphans,
            "autoReconcileOrphanInventory=true: rolling live orphan VMs into the \
             teardown pipeline; ack-completing pending teardowns the agent \
             already lacks",
        );

        for vm in &live_orphans {
            self.cleanup_failed_vm(&vm.id, &vm.cluster_id, host_id)
                .await;
        }
        if !pending_vm_orphans.is_empty() || !pending_cluster_orphans.is_empty() {
            self.db
                .ack_tombstones(host_id, &pending_cluster_orphans, &pending_vm_orphans)
                .await?;
        }
        Ok(())
    }
}

// --- CAPI-facing service ---

struct BasisApiService {
    shared: Arc<SharedCtx>,
}

impl BasisApiService {
    /// Resolve an `external_ip_pool` name against the controller
    /// config. Required (empty rejected): every cluster needs a pool
    /// for at least its LB Service block.
    fn resolve_pool(&self, name: &str) -> Result<&Pool, Status> {
        if name.is_empty() {
            return Err(Status::invalid_argument(
                "externalIpPool is required (the LB Service block always comes from a named pool)",
            ));
        }
        self.shared.network.pool_by_name(name).ok_or_else(|| {
            Status::invalid_argument(format!(
                "pool '{name}' is not defined in the controller's network.pools"
            ))
        })
    }

    /// Allocate the apiserver VIP for a cluster.
    /// `APISERVER_PUBLIC` → one /32 from `external_pool`,
    /// BGP-advertised cell-wide. `APISERVER_PRIVATE` → last usable in
    /// the cluster's CIDR, never advertised; recorded in
    /// `ip_allocations` under `cluster:<id>` scope so
    /// `allocate_cluster_vm_ip` won't hand it out as a VM IP.
    async fn allocate_apiserver_vip(
        &self,
        visibility: ApiserverVisibility,
        pool: &Pool,
        cluster_network: &crate::db::ClusterNetwork,
        cluster_id: &str,
    ) -> Result<String, Status> {
        match visibility {
            ApiserverVisibility::ApiserverPublic => self
                .shared
                .db
                .allocate_pool_vip(pool, cluster_id)
                .await
                .map_err(db_status),
            ApiserverVisibility::ApiserverPrivate => {
                let ip = cluster_network.private_apiserver_ip().to_string();
                let scope = format!("cluster:{cluster_id}");
                self.shared
                    .db
                    .reserve_specific_ip(&scope, &ip, None, Some(cluster_id))
                    .await
                    .map_err(db_status)?;
                Ok(ip)
            }
        }
    }

    /// Allocate the cluster's LoadBalancer Service block from the
    /// named pool. Returns the CIDR (or empty string when the cluster
    /// requested 0 service IPs).
    async fn allocate_service_block(
        &self,
        pool: &Pool,
        cluster_id: &str,
        count: u32,
    ) -> Result<String, Status> {
        if count == 0 {
            return Ok(String::new());
        }
        let range = crate::db::ParsedRange::parse_pool_range(pool).map_err(db_status)?;
        self.shared
            .db
            .allocate_service_block(&pool.name, &range, cluster_id, count)
            .await
            .map_err(db_status)
    }

    fn register_pending_op(
        &self,
        vm_id: &str,
        host_id: &str,
    ) -> oneshot::Receiver<Result<(), VmFailure>> {
        let (tx, rx) = oneshot::channel();
        self.shared.pending_ops.insert(
            vm_id.to_string(),
            PendingVmOp {
                tx,
                host_id: host_id.to_string(),
            },
        );
        rx
    }

    /// Initiate teardown of a single VM. Asynchronous: marks the vms
    /// row PENDING_TEARDOWN, also marks the (host, cluster) row
    /// PENDING_TEARDOWN if this was the last live VM for that pair,
    /// then nudges the agent with a reconcile push. The agent's
    /// `TombstoneAck` is what eventually drops the row + releases
    /// allocations — so a disconnected agent is fine, the next
    /// reconcile (periodic or on reconnect) re-emits the tombstone
    /// idempotently.
    ///
    /// DeleteMachine returns success once the intent is durably
    /// recorded. Callers that need to observe the eventual deletion
    /// poll `GetMachine` (state transitions ACTIVE → PENDING_TEARDOWN
    /// → 404 once acked).
    async fn initiate_vm_teardown(&self, vm: &VmRow) -> Result<(), Status> {
        self.shared
            .db
            .mark_vm_pending_teardown(&vm.id)
            .await
            .map_err(db_status)?;
        // Marking the host_clusters row idempotent — only flips to
        // pending if no live VMs remain for (host, cluster). If a
        // sibling VM is still ACTIVE on the same host+cluster, this
        // is a no-op and the cluster bridge stays put.
        self.shared
            .db
            .mark_host_cluster_pending_teardown(&vm.cluster_id, &vm.host_id)
            .await
            .map_err(db_status)?;
        self.shared.push_reconcile(&vm.host_id).await;
        info!(vm_id = %vm.id, host_id = %vm.host_id, cluster_id = %vm.cluster_id,
              "DeleteMachine: marked PENDING_TEARDOWN; tombstone will fire on next reconcile");
        Ok(())
    }

    /// Run one scheduling pass. Reads `(hosts, usage)` off the reader
    /// pool; the writer re-validates at commit time in `insert_vm`, so
    /// a stale snapshot here is always caught — never over-places.
    async fn pick_host(
        &self,
        req: &CreateMachineRequest,
    ) -> Result<(String, Vec<GpuInfo>), Status> {
        let hosts = self
            .shared
            .db
            .list_healthy_hosts()
            .await
            .map_err(db_status)?;
        let usage = self
            .shared
            .db
            .host_usage_snapshot()
            .await
            .map_err(db_status)?;

        let sched_req = ScheduleRequest::from(req);
        let ratio = self.shared.db.cpu_overcommit_ratio();
        match scheduler::schedule(&hosts, &usage, &sched_req, ratio) {
            Ok((host_id, gpus)) => {
                self.shared
                    .metrics
                    .scheduler_decisions_total
                    .with_label_values(&["placed"])
                    .inc();
                info!(
                    host_id = %host_id,
                    gpus = gpus.len(),
                    cpu = req.cpu,
                    memory_mib = req.memory_mib,
                    disk_gib = req.disk_gib,
                    "scheduler placed VM"
                );
                Ok((host_id, gpus))
            }
            Err(SchedulerError::NoCapacity(msg)) => {
                self.shared
                    .metrics
                    .scheduler_decisions_total
                    .with_label_values(&["no_capacity"])
                    .inc();
                warn!(
                    reason = %msg,
                    healthy_hosts = hosts.len(),
                    cpu = req.cpu,
                    memory_mib = req.memory_mib,
                    disk_gib = req.disk_gib,
                    gpus = req.gpus,
                    cpu_overcommit_ratio = ratio,
                    "scheduler rejected VM: no capacity"
                );
                Err(Status::resource_exhausted(msg))
            }
            Err(SchedulerError::UnsatisfiedRequirements(msg)) => {
                // Distinct from NoCapacity: a host with the right
                // labels exists somewhere or could be added; capacity
                // would just be more of the same wrong shape. Use
                // FAILED_PRECONDITION (operator must change inputs)
                // rather than RESOURCE_EXHAUSTED (try again later).
                self.shared
                    .metrics
                    .scheduler_decisions_total
                    .with_label_values(&["unsatisfied_requirements"])
                    .inc();
                warn!(
                    requires = %msg,
                    healthy_hosts = hosts.len(),
                    "scheduler rejected VM: no host satisfies placement requirements"
                );
                Err(Status::failed_precondition(format!(
                    "no host satisfies placement requirements: {msg}"
                )))
            }
        }
    }

    /// One optimistic-scheduling attempt: pick a host off a fresh
    /// snapshot, allocate the VM's cluster IP, then commit the row
    /// via `Db::insert_vm`. The writer's capacity gate + the
    /// `vm_gpus` unique constraint serve as the commit check; if we
    /// lose either race we roll back the IP allocation and return a
    /// classified error so the outer loop can retry.
    async fn try_place_vm(
        &self,
        req: &CreateMachineRequest,
        vm_id: &str,
        cluster: &ClusterRow,
        now: &str,
    ) -> Result<Placement, PlaceError> {
        // Serialize placement across the snapshot → score → commit
        // window so the next placer reads a snapshot that includes
        // this VM. The retry loop in `create_machine` is now the only
        // place that races (and only on capacity exhaustion or host
        // disappearance, not on stale-snapshot stampedes).
        let _placement_guard = self.shared.placement_lock.lock().await;

        let (host_id, gpus) = self.pick_host(req).await.map_err(|s| {
            if s.code() == tonic::Code::ResourceExhausted {
                PlaceError::NoCapacity(s)
            } else {
                PlaceError::Internal(s)
            }
        })?;

        let ip_address = self
            .shared
            .db
            .allocate_cluster_vm_ip(cluster, vm_id)
            .await
            .map_err(|e| PlaceError::Internal(db_status(e)))?;

        let extra_disk_gibs: Vec<u32> = req.extra_disks.iter().map(|d| d.size_gib).collect();
        let vm = VmRow {
            id: vm_id.to_string(),
            name: req.name.clone(),
            cluster_id: req.cluster_id.clone(),
            host_id: host_id.clone(),
            ip_address: ip_address.clone(),
            state: MachineState::Creating as i64,
            cpu: req.cpu as i64,
            memory_mib: req.memory_mib as i64,
            disk_gib: req.disk_gib as i64,
            extra_disk_gibs: serde_json::to_string(&extra_disk_gibs)
                .expect("serializing Vec<u32> to JSON is infallible"),
            image: req.image.clone(),
            error_message: String::new(),
            created_at: now.to_string(),
            updated_at: now.to_string(),
        };
        let gpu_assignments: Vec<GpuAssignment> = gpus
            .iter()
            .map(|g| GpuAssignment::from_scheduler_pick(vm_id, &host_id, g))
            .collect();

        match self.shared.db.insert_vm(&vm, &gpu_assignments).await {
            Ok(()) => Ok(Placement {
                vm,
                gpu_assignments,
            }),
            Err(e) => {
                self.release_vm_ips(vm_id).await;
                match e {
                    DbError::CapacityRaced(host) => Err(PlaceError::Raced(host)),
                    DbError::HostUnavailable(host) => Err(PlaceError::HostGone(host)),
                    DbError::Conflict(_) => Err(PlaceError::NameConflict),
                    other => Err(PlaceError::Internal(db_status(other))),
                }
            }
        }
    }

    async fn release_vm_ips(&self, vm_id: &str) {
        if let Err(e) = self.shared.db.release_vm_ips(vm_id).await {
            warn!(vm_id, error = %e, "failed to release VM IPs during rollback");
        }
    }
}

/// Result of a single [`BasisApiService::try_place_vm`] attempt. Owns
/// the state the outer handler needs to finish the create flow.
struct Placement {
    vm: VmRow,
    gpu_assignments: Vec<GpuAssignment>,
}

/// Classification of a failed placement attempt so the retry loop can
/// decide between retry, idempotent short-circuit, and give-up.
enum PlaceError {
    /// Capacity gate or GPU uniqueness lost to a concurrent create —
    /// the host is fine but the snapshot is stale. Retry with a fresh
    /// snapshot.
    Raced(String),
    /// The target host disappeared or went unhealthy between
    /// `pick_host` and commit. Retry with a fresh snapshot.
    HostGone(String),
    /// The scheduler couldn't find *any* host for the request. Not a
    /// race — retrying won't help. The `Status` is already tagged
    /// `ResourceExhausted` with the rejection reason.
    NoCapacity(Status),
    /// Another `CreateMachine` inserted a VM with our `(cluster_id,
    /// name)` while we weren't looking. The retry loop handles this
    /// by re-running the idempotency lookup.
    NameConflict,
    /// Anything else — DB errors, allocation failures.
    Internal(Status),
}

/// RAII guard that records CreateMachine latency + result on every
/// exit path — including early `?` returns, idempotent no-ops, and
/// RPC cancellations that drop the future mid-await. Each branch
/// that reaches a known outcome calls [`Self::set`] before returning;
/// anything that drops without setting is counted as `"cancelled"`
/// (future dropped) or `"error"` (error propagated via `?`) per the
/// default we pick on construction. Centralising the observation
/// here keeps every code path metered with one source of truth — the
/// alternative (a `record(...)` call per branch) was already wrong in
/// multiple places.
struct CreateOutcome<'a> {
    metrics: &'a Metrics,
    started: Instant,
    label: &'static str,
}

impl<'a> CreateOutcome<'a> {
    fn new(metrics: &'a Metrics, started: Instant) -> Self {
        // "cancelled" is the honest default: we're still running, and
        // if we get dropped without any branch having set a label,
        // something cancelled us (client disconnect, server shutdown,
        // a `?` we forgot to annotate). If it turns out operationally
        // that "error" shows up on real DB failures, that's a signal
        // to add an explicit `set("db_error")` on the relevant arms.
        Self {
            metrics,
            started,
            label: "cancelled",
        }
    }

    fn set(&mut self, label: &'static str) {
        self.label = label;
    }
}

impl Drop for CreateOutcome<'_> {
    fn drop(&mut self) {
        self.metrics
            .vm_create_result_total
            .with_label_values(&[self.label])
            .inc();
        self.metrics
            .vm_create_duration_seconds
            .with_label_values(&[self.label])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

#[tonic::async_trait]
impl basis_server::Basis for BasisApiService {
    async fn create_cluster(
        &self,
        request: Request<CreateClusterRequest>,
    ) -> Result<Response<CreateClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let visibility = ApiserverVisibility::try_from(req.apiserver_visibility).map_err(|_| {
            Status::invalid_argument(format!(
                "unknown apiserver_visibility {}",
                req.apiserver_visibility,
            ))
        })?;
        info!(
            name = %req.name,
            external_pool = %req.external_ip_pool,
            external_service_ips = req.external_service_ips,
            apiserver_visibility = ?visibility,
            trust_domain = %req.trust_domain,
            "CreateCluster received"
        );
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        // Resolve the IP count before any allocation: 0 means "use cell
        // default"; non-zero must be a power of two so the allocator
        // can carve an aligned block.
        let service_count = if req.external_service_ips == 0 {
            self.shared.network.default_external_service_ips
        } else {
            req.external_service_ips
        };
        if !service_count.is_power_of_two() {
            return Err(Status::invalid_argument(format!(
                "externalServiceIps {service_count} must be a power of two",
            )));
        }

        // Fail fast on a bad pool name before allocating anything.
        let pool = self.resolve_pool(&req.external_ip_pool)?;

        // Idempotent by name.
        if let Some(mut existing) = self
            .shared
            .db
            .get_cluster_by_name(&req.name)
            .await
            .map_err(db_status)?
        {
            // The CAPI provider stamps every CreateCluster with the
            // live trust_domain it derives from `lattice-system/lattice-ca`.
            // If the stored value disagrees, the row is from an earlier
            // install with a different CA (controller.db carried across
            // a `lattice install` re-run) and would silently isolate
            // this cluster from its siblings on the wrong tree-VRF.
            // Refresh and broadcast so agents detach the bridge from
            // the stale VRF and attach to the live one on the next
            // reconcile pass.
            if !req.trust_domain.is_empty() && req.trust_domain != existing.trust_domain {
                warn!(
                    cluster_id = %existing.id,
                    name = %req.name,
                    old = %existing.trust_domain,
                    new = %req.trust_domain,
                    "CreateCluster: trust_domain drift, refreshing stored row"
                );
                self.shared
                    .db
                    .update_cluster_trust_domain(&existing.id, &req.trust_domain)
                    .await
                    .map_err(db_status)?;
                existing.trust_domain = req.trust_domain.clone();
                self.shared.broadcast_cluster(&existing.id).await;
            }
            info!(cluster_id = %existing.id, name = %req.name, "CreateCluster idempotent return");
            return Ok(Response::new(create_cluster_response(&existing)));
        }

        // Allocate-and-insert is wrapped in a bounded retry: the
        // pre-insert allocator (`allocate_cluster_network`) only reads
        // from `clusters`, so two concurrent creates can pick the same
        // (vni, cidr) and one loses the UNIQUE constraint at insert
        // time. On that race we release any IPs we pre-allocated and
        // re-pick — the loser's snapshot now sees the winner's row and
        // moves to the next free slot. Bounded so a wedged allocator
        // surfaces as a clean error instead of an infinite loop.
        const MAX_ALLOCATION_ATTEMPTS: usize = 8;
        for attempt in 1..=MAX_ALLOCATION_ATTEMPTS {
            let cluster_network = self
                .shared
                .db
                .allocate_cluster_network(&self.shared.network)
                .await
                .map_err(db_status)?;

            let cluster_id = uuid::Uuid::new_v4().to_string();

            // Helper: drop every IP we allocated for this pending cluster
            // (apiserver VIP if from pool, service block, private apiserver
            // reservation). Used by every failure-rollback site below so
            // leaks can't accumulate.
            let rollback = async |label: &str| {
                if let Err(e) = self.shared.db.release_cluster_ips(&cluster_id).await {
                    warn!(cluster_id, error = %e, label, "rollback: release_cluster_ips");
                }
            };

            let endpoint = match self
                .allocate_apiserver_vip(visibility, pool, &cluster_network, &cluster_id)
                .await
            {
                Ok(ep) => ep,
                Err(status) => {
                    rollback("allocate_apiserver_vip").await;
                    return Err(status);
                }
            };
            let service_block = match self
                .allocate_service_block(pool, &cluster_id, service_count)
                .await
            {
                Ok(cidr) => cidr,
                Err(status) => {
                    rollback("allocate_service_block").await;
                    return Err(status);
                }
            };

            let row = ClusterRow::from_network(
                ClusterIdentity {
                    id: cluster_id.clone(),
                    name: req.name.clone(),
                    control_plane_endpoint: endpoint.clone(),
                    apiserver_visibility: visibility as i64,
                    external_pool: req.external_ip_pool.clone(),
                    service_block_cidr: service_block.clone(),
                    trust_domain: req.trust_domain.clone(),
                    created_at: now_rfc3339(),
                },
                cluster_network,
            );
            match self.shared.db.insert_cluster(&row).await {
                Ok(()) => {
                    info!(
                        cluster_id = %cluster_id,
                        name = %req.name,
                        endpoint = %endpoint,
                        vni = cluster_network.vni,
                        cidr = %cluster_network.cidr,
                        attempt,
                        "CreateCluster: new cluster provisioned"
                    );
                    return Ok(Response::new(create_cluster_response(&row)));
                }
                Err(DbError::Conflict(_)) => {
                    // Concurrent CreateCluster with the same name beat
                    // us. Return the committed row as an idempotent
                    // success.
                    rollback("insert_cluster: name conflict").await;
                    let existing = self
                        .shared
                        .db
                        .get_cluster_by_name(&req.name)
                        .await
                        .map_err(db_status)?
                        .ok_or_else(|| {
                            Status::internal(format!(
                                "cluster '{}' insert rejected as name duplicate but row not found",
                                req.name,
                            ))
                        })?;
                    return Ok(Response::new(create_cluster_response(&existing)));
                }
                Err(DbError::AllocationRaced(msg)) => {
                    // VNI or CIDR collision with a concurrent winner.
                    // Release this attempt's IPs and try again with a
                    // fresh snapshot.
                    rollback("insert_cluster: allocation race").await;
                    warn!(
                        name = %req.name, attempt, max = MAX_ALLOCATION_ATTEMPTS,
                        vni = cluster_network.vni, cidr = %cluster_network.cidr,
                        sqlite_error = %msg,
                        "CreateCluster: VNI/CIDR raced concurrent winner, retrying"
                    );
                    continue;
                }
                Err(other) => {
                    rollback("insert_cluster: error").await;
                    return Err(db_status(other));
                }
            }
        }
        Err(Status::resource_exhausted(format!(
            "CreateCluster '{}' lost VNI/CIDR allocation race {} times in a row — \
             cluster supernet may be saturated or under sustained concurrent create load",
            req.name, MAX_ALLOCATION_ATTEMPTS,
        )))
    }

    async fn delete_cluster(
        &self,
        request: Request<DeleteClusterRequest>,
    ) -> Result<Response<DeleteClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        info!(cluster_id = %req.cluster_id, "DeleteCluster received");

        // Idempotent: once the final tombstone ack drains the cluster
        // row, a retried finalizer cleanup must succeed so the K8s
        // object can be released. NotFound here is the steady state.
        match self.shared.db.get_cluster(&req.cluster_id).await {
            Ok(_) => {}
            Err(DbError::NotFound(_)) => {
                info!(
                    cluster_id = %req.cluster_id,
                    "DeleteCluster idempotent: cluster row already drained"
                );
                return Ok(Response::new(DeleteClusterResponse {}));
            }
            Err(e) => return Err(db_status(e)),
        }

        // Mark every VM of this cluster pending teardown. Each one
        // becomes a `vm_tombstone` on the next reconcile to its host.
        let vms = self
            .shared
            .db
            .list_vms(Some(&req.cluster_id))
            .await
            .map_err(db_status)?;
        for vm in &vms {
            if let Err(e) = self.shared.db.mark_vm_pending_teardown(&vm.id).await {
                warn!(vm_id = %vm.id, error = %e,
                    "DeleteCluster: failed to mark VM pending teardown");
            }
        }

        // Mark every (host, cluster) row pending teardown. Returns the
        // set of hosts whose reconcile state needs nudging.
        let affected_hosts = self
            .shared
            .db
            .mark_cluster_pending_teardown_all_hosts(&req.cluster_id)
            .await
            .map_err(db_status)?;
        for host_id in &affected_hosts {
            self.shared.push_reconcile(host_id).await;
        }

        info!(
            cluster_id = %req.cluster_id,
            vms_pending = vms.len(),
            hosts_pending = affected_hosts.len(),
            "DeleteCluster: tombstones queued; cluster row + VIP allocations \
             release once all hosts ack",
        );
        // Cluster row + VIP allocations stay until the last ack drops
        // the final host_clusters row (handled by `ack_tombstones`),
        // so the controller is consistent at every snapshot: a
        // half-acked cluster never has its cluster row vanish out
        // from under in-flight reconcile pushes.
        Ok(Response::new(DeleteClusterResponse {}))
    }

    async fn get_cluster(
        &self,
        request: Request<GetClusterRequest>,
    ) -> Result<Response<Cluster>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let cluster = self
            .shared
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;
        Ok(Response::new(cluster_to_proto(&cluster)))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        require_capi_caller(&request)?;
        let clusters = self.shared.db.list_clusters().await.map_err(db_status)?;
        let out = clusters.iter().map(cluster_to_proto).collect();
        Ok(Response::new(ListClustersResponse { clusters: out }))
    }

    async fn create_machine(
        &self,
        request: Request<CreateMachineRequest>,
    ) -> Result<Response<CreateMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let started = Instant::now();
        // One guard covers every exit path — early returns, `?` on DB
        // calls, idempotent short-circuits, and future cancellation
        // all go through Drop. Each outcome sets its label before the
        // corresponding return; anything that drops without setting
        // is counted as `"cancelled"`.
        let mut outcome = CreateOutcome::new(&self.shared.metrics, started);
        info!(
            cluster_id = %req.cluster_id,
            name = %req.name,
            cpu = req.cpu,
            memory_mib = req.memory_mib,
            disk_gib = req.disk_gib,
            gpus = req.gpus,
            image = %req.image,
            "CreateMachine received"
        );

        let cluster = self
            .shared
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(|e| {
                warn!(
                    cluster_id = %req.cluster_id, name = %req.name, error = %e,
                    "CreateMachine rejected: cluster not found"
                );
                db_status(e)
            })?;

        if let Some(existing) = self
            .shared
            .db
            .get_vm_by_name(&req.cluster_id, &req.name)
            .await
            .map_err(db_status)?
        {
            info!(
                vm_id = %existing.id,
                cluster_id = %req.cluster_id,
                name = %req.name,
                host_id = %existing.host_id,
                "CreateMachine idempotent return: VM already exists"
            );
            outcome.set("idempotent");
            return Ok(Response::new(vm_to_create_response(&existing)));
        }

        let vm_id = uuid::Uuid::new_v4().to_string();
        let now = now_rfc3339();

        // Optimistic-concurrency retry: `pick_host` reads off the
        // reader pool and picks a placement; `insert_vm` re-validates
        // it against the writer's current state. A lost race
        // (`CapacityRaced` / `HostUnavailable`) rolls the attempt back
        // and we retry with a fresh snapshot. Bounded by
        // `MAX_SCHEDULE_ATTEMPTS` so pathological contention surfaces
        // as a real error instead of retrying forever.
        let placement = {
            let mut placement = None;
            for attempt in 1..=MAX_SCHEDULE_ATTEMPTS {
                match self.try_place_vm(&req, &vm_id, &cluster, &now).await {
                    Ok(p) => {
                        placement = Some(p);
                        break;
                    }
                    Err(PlaceError::Raced(host)) | Err(PlaceError::HostGone(host))
                        if attempt < MAX_SCHEDULE_ATTEMPTS =>
                    {
                        debug!(
                            vm_id = %vm_id, host_id = %host, attempt,
                            "CreateMachine: placement raced, retrying"
                        );
                        continue;
                    }
                    Err(PlaceError::Raced(_)) | Err(PlaceError::HostGone(_)) => {
                        outcome.set("raced_exhausted");
                        warn!(
                            vm_id = %vm_id,
                            attempts = MAX_SCHEDULE_ATTEMPTS,
                            "CreateMachine: placement exhausted retries under contention"
                        );
                        return Err(Status::unavailable("scheduler contention — retry"));
                    }
                    Err(PlaceError::NoCapacity(status)) => {
                        outcome.set("no_capacity");
                        return Err(status);
                    }
                    Err(PlaceError::NameConflict) => {
                        // A concurrent CreateMachine with the same
                        // (cluster_id, name) committed ahead of us —
                        // same outcome CAPI would have seen had it
                        // arrived first. Return the committed row as
                        // an idempotent success.
                        let existing = self
                            .shared
                            .db
                            .get_vm_by_name(&req.cluster_id, &req.name)
                            .await
                            .map_err(db_status)?
                            .ok_or_else(|| {
                                Status::internal(format!(
                                    "vm '{}/{}' insert rejected as duplicate but row not found",
                                    req.cluster_id, req.name,
                                ))
                            })?;
                        outcome.set("idempotent");
                        return Ok(Response::new(vm_to_create_response(&existing)));
                    }
                    Err(PlaceError::Internal(status)) => return Err(status),
                }
            }
            placement.expect("loop body breaks on Ok or returns on every other arm")
        };

        let vm = placement.vm;
        let gpu_assignments = placement.gpu_assignments;
        let host_id = vm.host_id.clone();
        let ip_address = vm.ip_address.clone();

        // VM row now exists; flip the (host, cluster) row to ACTIVE
        // (or insert it if first VM of this cluster on this host).
        // If the row was sitting in PENDING_TEARDOWN (last sibling
        // VM was just deleted but the agent hasn't acked yet),
        // resurrection-cancels-tombstone fires here: the row goes
        // back to ACTIVE and the pending tombstone is dropped before
        // it ever leaves the controller — exactly what makes CAPI
        // delete-then-recreate churn benign.
        let host_gateway_ip = match self
            .shared
            .db
            .ensure_host_cluster_active(&cluster, &host_id)
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                warn!(
                    vm_id = %vm_id, host_id = %host_id, error = %e,
                    "host bridge IP allocation failed after insert; cleaning up"
                );
                self.shared
                    .cleanup_failed_vm(&vm_id, &cluster.id, &host_id)
                    .await;
                return Err(db_status(e));
            }
        };

        // Push the authoritative reconcile before dispatching CreateVm
        // so the agent has the cluster's bridge up before it has to
        // attach the VM's tap. mpsc preserves send order from this
        // task, so the broadcast lands first.
        self.shared.broadcast_cluster(&cluster.id).await;

        let Some(agent) = self.shared.agents.get(&host_id) else {
            outcome.set("no_agent");
            warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: scheduled host has no connected agent");
            self.shared
                .cleanup_failed_vm(&vm_id, &cluster.id, &host_id)
                .await;
            return Err(Status::unavailable(format!(
                "agent for host '{host_id}' not connected"
            )));
        };

        let wait_rx = self.register_pending_op(&vm_id, &host_id);

        let cmd = ControllerCommand {
            request_id: vm_id.clone(),
            command: Some(controller_command::Command::CreateVm(Box::new(
                CreateVmCommand {
                    vm_id: vm_id.clone(),
                    name: req.name,
                    cpu: req.cpu,
                    memory_mib: req.memory_mib,
                    disk_gib: req.disk_gib,
                    image: req.image,
                    bootstrap_data: req.bootstrap_data,
                    ip_address: ip_address.clone(),
                    gateway: host_gateway_ip.clone(),
                    prefix_len: cluster.prefix_len as u32,
                    gpus: req.gpus,
                    gpu_constraints: req.gpu_constraints,
                    dns_servers: self.shared.dns_servers.as_ref().clone(),
                    gpu_pci_addresses: gpu_assignments
                        .iter()
                        .map(|g| g.pci_address.clone())
                        .collect(),
                    extra_disks: req.extra_disks,
                    vni: cluster.vni as u32,
                },
            ))),
        };

        info!(vm_id = %vm_id, host_id = %host_id, vni = cluster.vni, "CreateMachine: dispatching CreateVm to agent");
        if agent.command_tx.send(cmd).await.is_err() {
            self.shared.pending_ops.remove(&vm_id);
            self.shared
                .cleanup_failed_vm(&vm_id, &cluster.id, &host_id)
                .await;
            outcome.set("stream_closed");
            warn!(
                vm_id = %vm_id,
                host_id = %host_id,
                "CreateMachine: agent stream closed before command delivered"
            );
            return Err(Status::unavailable("agent stream closed"));
        }
        drop(agent);

        match tokio::time::timeout(CREATE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                outcome.set("placed");
                info!(vm_id = %vm_id, host_id = %host_id, ip = %ip_address, "CreateMachine: agent reported RUNNING");
                let row = self.shared.db.get_vm(&vm_id).await.map_err(db_status)?;
                Ok(Response::new(vm_to_create_response(&row)))
            }
            Ok(Ok(Err(failure))) => {
                self.shared
                    .cleanup_failed_vm(&vm_id, &cluster.id, &host_id)
                    .await;
                if failure.transient {
                    outcome.set("busy");
                    warn!(
                        vm_id = %vm_id, host_id = %host_id, error = %failure.message,
                        "CreateMachine: agent shed for backpressure (transient)"
                    );
                    Err(Status::unavailable(format!(
                        "agent busy, retry: {}",
                        failure.message
                    )))
                } else {
                    outcome.set("vm_failed");
                    warn!(
                        vm_id = %vm_id, host_id = %host_id, error = %failure.message,
                        "CreateMachine: agent reported FAILED"
                    );
                    Err(Status::internal(format!(
                        "VM creation failed: {}",
                        failure.message
                    )))
                }
            }
            Ok(Err(_)) => {
                outcome.set("agent_error");
                warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: agent disconnected during VM creation");
                self.shared
                    .cleanup_failed_vm(&vm_id, &cluster.id, &host_id)
                    .await;
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                self.shared.pending_ops.remove(&vm_id);
                let timeout_msg = format!(
                    "VM creation timed out ({}s)",
                    CREATE_MACHINE_TIMEOUT.as_secs()
                );
                if let Err(e) = self
                    .shared
                    .db
                    .update_vm_state(
                        &vm_id,
                        MachineState::Failed as i64,
                        &timeout_msg,
                        &now_rfc3339(),
                    )
                    .await
                {
                    warn!(
                        vm_id = %vm_id,
                        host_id = %host_id,
                        error = %e,
                        "CreateMachine timeout: failed to mark VM FAILED in DB"
                    );
                }
                outcome.set("timeout");
                warn!(
                    vm_id = %vm_id,
                    host_id = %host_id,
                    timeout_s = CREATE_MACHINE_TIMEOUT.as_secs(),
                    "CreateMachine: timed out waiting for agent to report RUNNING"
                );
                Err(Status::deadline_exceeded(timeout_msg))
            }
        }
    }

    async fn delete_machine(
        &self,
        request: Request<DeleteMachineRequest>,
    ) -> Result<Response<DeleteMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        info!(vm_id = %req.id, "DeleteMachine received");
        // Idempotent across the full lifecycle:
        //   * row missing       → already tombstone-acked, return Ok so
        //                         a retried finalizer cleanup lifts.
        //   * PENDING_TEARDOWN  → nudge the host so the tombstone
        //                         re-fires on the next reconcile.
        //   * any other state   → start the teardown pipeline.
        let vm = match self.shared.db.get_vm(&req.id).await {
            Ok(v) => v,
            Err(DbError::NotFound(_)) => {
                info!(vm_id = %req.id, "DeleteMachine idempotent: VM row already drained");
                return Ok(Response::new(DeleteMachineResponse {}));
            }
            Err(e) => return Err(db_status(e)),
        };
        if vm.state != VM_STATE_PENDING_TEARDOWN {
            self.initiate_vm_teardown(&vm).await?;
        } else {
            self.shared.push_reconcile(&vm.host_id).await;
        }
        Ok(Response::new(DeleteMachineResponse {}))
    }

    async fn get_machine(
        &self,
        request: Request<GetMachineRequest>,
    ) -> Result<Response<Machine>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let vm = self.shared.db.get_vm(&req.id).await.map_err(db_status)?;
        let gpus = self
            .shared
            .db
            .gpus_for_vm(&vm.id)
            .await
            .map_err(db_status)?;
        Ok(Response::new(vm_to_machine(&vm, &gpus)?))
    }

    async fn list_machines(
        &self,
        request: Request<ListMachinesRequest>,
    ) -> Result<Response<ListMachinesResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let cluster = if req.cluster_id.is_empty() {
            None
        } else {
            Some(req.cluster_id.as_str())
        };
        let vms = self.shared.db.list_vms(cluster).await.map_err(db_status)?;
        let vm_ids: Vec<String> = vms.iter().map(|v| v.id.clone()).collect();
        let mut gpus_by_vm = self
            .shared
            .db
            .gpus_for_vms(&vm_ids)
            .await
            .map_err(db_status)?;
        let machines: Vec<Machine> = vms
            .iter()
            .map(|v| vm_to_machine(v, gpus_by_vm.remove(&v.id).as_deref().unwrap_or(&[])))
            .collect::<Result<_, Status>>()?;
        Ok(Response::new(ListMachinesResponse { machines }))
    }
}

// --- Agent-facing service ---

struct BasisAgentService {
    shared: Arc<SharedCtx>,
    reconcile_interval: std::time::Duration,
}

#[tonic::async_trait]
impl basis_agent_server::BasisAgent for BasisAgentService {
    type StreamMessagesStream =
        Pin<Box<dyn Stream<Item = Result<ControllerCommand, Status>> + Send + 'static>>;

    async fn stream_messages(
        &self,
        request: Request<Streaming<AgentMessage>>,
    ) -> Result<Response<Self::StreamMessagesStream>, Status> {
        let peer_id = peer_identity(&request)?;
        if peer_id == tls::CAPI_PROVIDER_IDENTITY {
            return Err(Status::permission_denied(format!(
                "peer identity '{peer_id}' is not authorized for agent RPCs"
            )));
        }

        let mut inbound = request.into_inner();

        let first = inbound
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty stream"))?
            .map_err(|e| Status::internal(e.to_string()))?;

        let register = match first.payload {
            Some(agent_message::Payload::Register(r)) => r,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must be RegisterHost",
                ))
            }
        };

        if peer_id != register.hostname {
            return Err(Status::permission_denied(format!(
                "agent identity '{peer_id}' does not match registered hostname '{}'",
                register.hostname
            )));
        }

        let host_id = self.register_or_lookup_host(&register).await?;

        let (command_tx, command_rx) = mpsc::channel::<ControllerCommand>(32);

        // Initial reconcile — agent uses this to build bridges +
        // process tombstones before accepting any CreateVm. The
        // inventory the agent reported gets diffed against the DB
        // in BOTH directions, gated by `safety.autoReconcileOrphanInventory`:
        //   * agent has, DB doesn't → synthesise one-shot tombstones
        //     in this reply (`extend_with_orphan_tombstones`).
        //   * DB has, agent doesn't → roll live VMs into the
        //     pipeline + ack-complete pending teardowns
        //     (`reap_db_orphans_from_inventory`). Re-build the
        //     reconcile command afterwards so the new tombstones
        //     emitted by `cleanup_failed_vm` ride this same ack.
        let mut initial_state = self
            .shared
            .build_reconcile_command(&host_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(inv) = register.current_inventory.as_ref() {
            self.shared
                .extend_with_orphan_tombstones(&host_id, inv, &mut initial_state)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            self.shared
                .reap_db_orphans_from_inventory(&host_id, inv)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            initial_state = self
                .shared
                .build_reconcile_command(&host_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            self.shared
                .extend_with_orphan_tombstones(&host_id, inv, &mut initial_state)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        command_tx
            .send(ControllerCommand {
                request_id: String::new(),
                command: Some(controller_command::Command::RegisterAck(Box::new(
                    RegisterHostResponse {
                        host_id: host_id.clone(),
                        initial_state: Some(initial_state),
                        bgp_asn: self.shared.bgp.asn,
                        bgp_reflector_address: self.shared.bgp.router_id.clone(),
                    },
                ))),
            })
            .await
            .map_err(|_| Status::internal("failed to send registration ack"))?;

        let epoch = next_agent_epoch();
        self.shared.agents.insert(
            host_id.clone(),
            ConnectedAgent {
                command_tx: command_tx.clone(),
                epoch,
            },
        );
        self.shared
            .metrics
            .agent_connected
            .with_label_values(&[&register.hostname])
            .set(1);

        // Periodic authoritative push. Same command shape as the
        // initial reconcile; single code path converges all drift.
        let reconcile_handle = {
            let shared = self.shared.clone();
            let host_id = host_id.clone();
            let interval = self.reconcile_interval;
            let command_tx = command_tx.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // Skip first tick — initial_state already sent.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let cmd = match shared.build_reconcile_command(&host_id).await {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(error = %e, host_id = %host_id, "periodic reconcile build failed");
                            continue;
                        }
                    };
                    if command_tx
                        .send(ControllerCommand {
                            request_id: String::new(),
                            command: Some(controller_command::Command::ReconcileHost(Box::new(
                                cmd,
                            ))),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            })
        };

        // Inbound handler.
        let shared = self.shared.clone();
        let agent_host_id = host_id.clone();
        let agent_hostname = register.hostname.clone();
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Err(e) = handle_agent_message(&shared, &agent_host_id, msg).await {
                            warn!(error = %e, host_id = %agent_host_id, "error handling agent message");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, host_id = %agent_host_id, "agent stream error");
                        break;
                    }
                }
            }
            info!(host_id = %agent_host_id, "agent disconnected");
            reconcile_handle.abort();
            release_agent_stream(
                &shared.agents,
                &shared.pending_ops,
                &shared.metrics,
                &agent_host_id,
                &agent_hostname,
                epoch,
            );
        });

        let output = ReceiverStream::new(command_rx).map(Ok);
        Ok(Response::new(Box::pin(output)))
    }
}

impl BasisAgentService {
    async fn register_or_lookup_host(
        &self,
        register: &RegisterHostRequest,
    ) -> Result<String, Status> {
        let gpu_inventory: Vec<GpuInfo> = register
            .gpus
            .iter()
            .map(|g| GpuInfo {
                pci_address: g.pci_address.clone(),
                model: g.model.clone(),
                iommu_group: g.iommu_group.clone(),
                nvlink_group: g.nvlink_group,
            })
            .collect();

        // Upsert is idempotent and refreshes capacity + vtep_address
        // on reconnect. For a first-time host we mint a UUID;
        // otherwise we reuse the existing id so VM rows stay stable.
        let host_id = match self
            .shared
            .db
            .get_host_by_hostname(&register.hostname)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            Some(existing) => {
                info!(host_id = %existing.id, hostname = %register.hostname, "agent reconnected");
                existing.id
            }
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                info!(host_id = %id, hostname = %register.hostname, "new host registered");
                id
            }
        };

        let capacity = register
            .storage_capacity
            .as_ref()
            .map(crate::db::StorageCapacityBytes::from_proto)
            .unwrap_or_default();
        let host = crate::db::HostRow {
            id: host_id.clone(),
            hostname: register.hostname.clone(),
            total_cpu: register.total_cpu as i64,
            total_memory_mib: register.total_memory_mib as i64,
            rootfs_total_bytes: capacity.rootfs_total_bytes,
            rootfs_free_bytes: capacity.rootfs_free_bytes,
            rootfs_metadata_total_bytes: capacity.rootfs_metadata_total_bytes,
            rootfs_metadata_free_bytes: capacity.rootfs_metadata_free_bytes,
            data_total_bytes: capacity.data_total_bytes,
            data_free_bytes: capacity.data_free_bytes,
            gpu_inventory,
            vtep_address: register.vtep_address.clone(),
            last_heartbeat: now_rfc3339(),
            healthy: true,
            rank: register.rank as i64,
            labels: register.labels.clone().into_iter().collect(),
        };
        self.shared
            .db
            .upsert_host(&host)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(host_id)
    }
}

async fn handle_agent_message(
    shared: &SharedCtx,
    host_id: &str,
    msg: AgentMessage,
) -> anyhow::Result<()> {
    match msg.payload {
        Some(agent_message::Payload::Heartbeat(hb)) => {
            // Identity is the authenticated stream's host_id, never
            // anything in the message body — see HeartbeatRequest's
            // proto comment. The body carries a fresh per-pool
            // capacity snapshot which becomes the scheduler's live
            // budget input.
            let capacity = hb
                .storage_capacity
                .as_ref()
                .map(crate::db::StorageCapacityBytes::from_proto)
                .unwrap_or_default();
            shared
                .db
                .record_heartbeat(host_id, &now_rfc3339(), &capacity)
                .await?;
        }
        Some(agent_message::Payload::VmState(report)) => {
            let state = report.state();
            let now = now_rfc3339();

            let prev = match shared.db.get_vm(&report.vm_id).await {
                Ok(row) => row,
                Err(DbError::NotFound(_)) => {
                    warn!(
                        stream_host_id = %host_id,
                        vm_id = %report.vm_id,
                        "ignoring VM state report for unknown VM"
                    );
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };

            if prev.host_id != host_id {
                warn!(
                    stream_host_id = %host_id,
                    vm_id = %report.vm_id,
                    vm_host_id = %prev.host_id,
                    "ignoring VM state report for a different host"
                );
                return Ok(());
            }

            // Don't overwrite a PENDING_TEARDOWN with a stale reading
            // — once the controller decides a VM is going away, only
            // the agent's TombstoneAck (which deletes the row) should
            // change its state. Otherwise a slow VmState report racing
            // a DeleteMachine could resurrect the row to RUNNING and
            // leak it.
            if prev.state != VM_STATE_PENDING_TEARDOWN {
                shared
                    .db
                    .update_vm_state(&report.vm_id, state as i64, &report.error_message, &now)
                    .await?;
            }

            if state == MachineState::Running {
                let first_time_running = prev.state != MachineState::Running as i64;
                if first_time_running {
                    if let Ok(created) = humantime::parse_rfc3339(&prev.created_at) {
                        if let Ok(elapsed) = std::time::SystemTime::now().duration_since(created) {
                            shared
                                .metrics
                                .vm_time_to_running_seconds
                                .observe(elapsed.as_secs_f64());
                        }
                    }
                }
            }

            // Resolve any pending CreateMachine waiting on this VM —
            // create is the only synchronous flow now; delete is
            // tombstone-driven and resolves via TombstoneAck.
            let resolves = matches!(state, MachineState::Running | MachineState::Failed);
            if resolves {
                if let Some(pending) = shared.pending_ops.get(&report.vm_id) {
                    if pending.host_id != host_id {
                        warn!(
                            stream_host_id = %host_id,
                            vm_id = %report.vm_id,
                            registered_host_id = %pending.host_id,
                            "ignoring VM state report from wrong host for pending op",
                        );
                        return Ok(());
                    }
                }
                if let Some((_, pending)) = shared.pending_ops.remove(&report.vm_id) {
                    let result = if state == MachineState::Failed {
                        Err(VmFailure {
                            message: report.error_message.clone(),
                            transient: report.transient,
                        })
                    } else {
                        Ok(())
                    };
                    let _ = pending.tx.send(result);
                }
            }
        }
        Some(agent_message::Payload::TombstoneAck(ack)) => {
            // The agent has fully torn down these resources. Drop the
            // matching DB rows (host_clusters / vms) and release any
            // remaining IP/GPU allocations. Atomic per-host so a
            // partial ack can't leave half-deleted state.
            if let Err(e) = shared
                .db
                .ack_tombstones(host_id, &ack.cluster_vnis, &ack.vm_ids)
                .await
            {
                warn!(
                    host_id, error = %e,
                    cluster_vnis = ?ack.cluster_vnis, vm_ids = ?ack.vm_ids,
                    "TombstoneAck: failed to drop pending rows; \
                     next reconcile will re-emit and the agent will re-ack",
                );
            }
        }
        Some(agent_message::Payload::Register(_)) => {
            warn!(host_id, "unexpected register message on established stream");
        }
        None => {}
    }

    Ok(())
}

// --- Proto conversions — single source of truth ---

fn cluster_to_proto(c: &ClusterRow) -> Cluster {
    Cluster {
        cluster_id: c.id.clone(),
        name: c.name.clone(),
        control_plane_endpoint: c.control_plane_endpoint.clone(),
        vni: c.vni as u32,
        cidr: c.cidr.clone(),
        service_block_cidr: c.service_block_cidr.clone(),
        apiserver_visibility: c.apiserver_visibility as i32,
        trust_domain: c.trust_domain.clone(),
    }
}

fn create_cluster_response(c: &ClusterRow) -> CreateClusterResponse {
    CreateClusterResponse {
        cluster_id: c.id.clone(),
        control_plane_endpoint: c.control_plane_endpoint.clone(),
        vni: c.vni as u32,
        cidr: c.cidr.clone(),
        service_block_cidr: c.service_block_cidr.clone(),
    }
}

/// Convert a `VmRow` (i64 columns) to its protobuf wire form (u32
/// fields). Returns `Status::data_loss` when a width that should
/// always fit in u32 doesn't — the only writer of these columns
/// inserts proto u32 values widened to i64, so an out-of-range
/// number on read means the row was hand-edited or a future code
/// path skipped that path. Surfacing it as a clean RPC error rather
/// than a panic keeps GetMachine/ListMachines from tearing down the
/// whole gRPC stream when one bad row is in the result set.
fn vm_to_machine(vm: &VmRow, gpus: &[GpuAssignment]) -> Result<Machine, Status> {
    let narrow = |field: &'static str, v: i64| -> Result<u32, Status> {
        u32::try_from(v).map_err(|_| {
            Status::data_loss(format!(
                "vms.{field} = {v} on vm '{}' is out of u32 range",
                vm.id,
            ))
        })
    };

    let extra_disks = vm
        .extra_disks()
        .map_err(|e| {
            Status::data_loss(format!(
                "vms.extra_disk_gibs on vm '{}' failed to parse: {e}",
                vm.id,
            ))
        })?
        .into_iter()
        .map(|size_gib| ExtraDisk { size_gib })
        .collect();

    Ok(Machine {
        id: vm.id.clone(),
        name: vm.name.clone(),
        cluster_id: vm.cluster_id.clone(),
        host: vm.host_id.clone(),
        provider_id: provider_id(&vm.id),
        ip_address: vm.ip_address.clone(),
        state: vm.state as i32,
        cpu: narrow("cpu", vm.cpu)?,
        memory_mib: narrow("memory_mib", vm.memory_mib)?,
        disk_gib: narrow("disk_gib", vm.disk_gib)?,
        gpus: gpus
            .iter()
            .map(|g| MachineGpu {
                pci_address: g.pci_address.clone(),
                model: g.model.clone(),
                nvlink_group: g.nvlink_group as u32,
            })
            .collect(),
        error_message: vm.error_message.clone(),
        extra_disks,
    })
}

/// Narrower response wrapper for create/idempotent-return paths.
fn vm_to_create_response(vm: &VmRow) -> CreateMachineResponse {
    CreateMachineResponse {
        id: vm.id.clone(),
        provider_id: provider_id(&vm.id),
        ip_address: vm.ip_address.clone(),
        host: vm.host_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Pool, PoolScope};

    fn cluster_with(
        external_pool: &str,
        visibility_public: bool,
        service_block: &str,
    ) -> ClusterRow {
        ClusterRow {
            id: "c1".to_string(),
            name: "cluster".to_string(),
            vni: 10_000,
            cidr: "10.0.0.0/24".to_string(),
            bridge_range_start: "10.0.0.1".to_string(),
            bridge_range_end: "10.0.0.32".to_string(),
            vm_range_start: "10.0.0.33".to_string(),
            vm_range_end: "10.0.0.254".to_string(),
            prefix_len: 24,
            control_plane_endpoint: "10.100.0.10".to_string(),
            apiserver_visibility: if visibility_public { 0 } else { 1 },
            external_pool: external_pool.to_string(),
            service_block_cidr: service_block.to_string(),
            trust_domain: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn pool(name: &str, scope: PoolScope) -> Pool {
        Pool {
            name: name.to_string(),
            cidr: "192.168.0.0/24".to_string(),
            scope,
        }
    }

    /// Lan pool + apiserver-public: both the apiserver /32 and the
    /// service block go to `cluster_vips`. Nothing in
    /// `internal_cluster_vips`.
    #[test]
    fn classify_lan_public_puts_everything_in_cluster_vips() {
        let cluster = cluster_with("cell-public", true, "192.168.0.16/28");
        let p = pool("cell-public", PoolScope::Lan);
        let (lan, tree) =
            classify_cluster_vips(&cluster, ApiserverVisibility::ApiserverPublic, &p, "host-a");
        assert_eq!(
            lan,
            vec![
                ClusterVip {
                    cidr: "10.100.0.10/32".to_string(),
                    owner_host_id: "host-a".to_string(),
                },
                ClusterVip {
                    cidr: "192.168.0.16/28".to_string(),
                    owner_host_id: "host-a".to_string(),
                }
            ]
        );
        assert!(tree.is_empty());
    }

    /// Tree pool + apiserver-private: the service block lives in
    /// `internal_cluster_vips`; the apiserver VIP is in `cluster.cidr`
    /// and never appears in either list.
    #[test]
    fn classify_tree_private_routes_service_block_to_internal() {
        let cluster = cluster_with("cell-internal", false, "10.250.0.16/28");
        let p = pool("cell-internal", PoolScope::Tree);
        let (lan, tree) = classify_cluster_vips(
            &cluster,
            ApiserverVisibility::ApiserverPrivate,
            &p,
            "host-a",
        );
        assert!(lan.is_empty());
        assert_eq!(tree, vec!["10.250.0.16/28".to_string()]);
    }

    /// Lan pool + apiserver-private + zero LB IPs: nothing pool-allocated
    /// is advertised. Only the cluster's overlay is reachable; the
    /// apiserver VIP is inside `cluster.cidr` and stays private to the
    /// cluster's bridge.
    #[test]
    fn classify_empty_service_block_emits_nothing() {
        let cluster = cluster_with("cell-public", false, "");
        let p = pool("cell-public", PoolScope::Lan);
        let (lan, tree) = classify_cluster_vips(
            &cluster,
            ApiserverVisibility::ApiserverPrivate,
            &p,
            "host-a",
        );
        assert!(lan.is_empty());
        assert!(tree.is_empty());
    }

    /// Tree pool + apiserver-public: the apiserver VIP is allocated
    /// from the tree pool's CIDR and routes through every host's
    /// per-cluster bridge — cell-wide reachable but never on the LAN.
    /// Both VIPs land in `internal_cluster_vips`.
    #[test]
    fn classify_tree_public_routes_apiserver_vip_to_internal() {
        let cluster = cluster_with("cell-internal", true, "10.250.0.16/28");
        let p = pool("cell-internal", PoolScope::Tree);
        let (lan, tree) =
            classify_cluster_vips(&cluster, ApiserverVisibility::ApiserverPublic, &p, "host-a");
        assert!(
            lan.is_empty(),
            "tree pool VIPs must never appear in `cluster_vips` (the LAN-routable list)",
        );
        assert_eq!(
            tree,
            vec!["10.100.0.10/32".to_string(), "10.250.0.16/28".to_string()],
            "both apiserver /32 and LB /28 must land in internal_cluster_vips",
        );
    }

    /// Skeleton VmRow whose narrow widths fit in u32, plus a valid
    /// extra_disks JSON. Tests override individual fields to drive
    /// `vm_to_machine` failure paths.
    fn vm_row_ok() -> VmRow {
        VmRow {
            id: "vm-1".to_string(),
            name: "vm-1".to_string(),
            cluster_id: "c-1".to_string(),
            host_id: "h-1".to_string(),
            ip_address: "10.0.0.10".to_string(),
            state: 2,
            cpu: 4,
            memory_mib: 4096,
            disk_gib: 50,
            extra_disk_gibs: "[]".to_string(),
            image: "ubuntu:22.04".to_string(),
            error_message: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    /// Sole writer of vms.cpu/memory/disk widens proto u32 → i64, so
    /// any out-of-range value at read time means the row was edited
    /// outside our writer (hand-edit, future code path skipping
    /// `insert_vm`). The conversion must surface a clean RPC error
    /// rather than panic — a panic would tear down the whole gRPC
    /// stream when one bad row lands in a ListMachines result set.
    #[test]
    fn vm_to_machine_returns_data_loss_on_oversized_cpu() {
        let mut vm = vm_row_ok();
        vm.cpu = (u32::MAX as i64) + 1;
        let err = vm_to_machine(&vm, &[]).expect_err("oversized cpu must fail");
        assert_eq!(err.code(), tonic::Code::DataLoss, "code: {err:?}");
        assert!(err.message().contains("cpu"), "message: {}", err.message());
    }

    /// Same contract for the JSON-encoded extra_disk_gibs column:
    /// malformed JSON must not panic vm_to_machine.
    #[test]
    fn vm_to_machine_returns_data_loss_on_malformed_extra_disks() {
        let mut vm = vm_row_ok();
        vm.extra_disk_gibs = "not-json".to_string();
        let err = vm_to_machine(&vm, &[]).expect_err("bad json must fail");
        assert_eq!(err.code(), tonic::Code::DataLoss, "code: {err:?}");
        assert!(
            err.message().contains("extra_disk_gibs"),
            "message: {}",
            err.message(),
        );
    }

    /// Reconnect race: a fresh stream re-inserts the host's slot
    /// before the prior stream's cleanup runs. The old cleanup must
    /// observe its slot is no longer current and leave the new
    /// connection's `command_tx`, `pending_ops` waiters, and
    /// `agent_connected` metric untouched.
    #[test]
    fn release_agent_stream_skips_when_superseded() {
        use tokio::sync::oneshot;

        let metrics = Metrics::new(1.0).expect("metrics");
        let agents: DashMap<String, ConnectedAgent> = DashMap::new();
        let pending_ops: DashMap<String, PendingVmOp> = DashMap::new();

        let host = "host-a";
        let hostname = "host-a.local";
        let old_epoch = 1;
        let new_epoch = 2;

        // New connection's command_tx — the value we must not drop.
        let (new_tx, _new_rx) = mpsc::channel::<ControllerCommand>(1);
        agents.insert(
            host.to_string(),
            ConnectedAgent {
                command_tx: new_tx.clone(),
                epoch: new_epoch,
            },
        );

        // New connection's in-flight CreateMachine waiter for vm-1.
        let (waiter_tx, waiter_rx) = oneshot::channel::<Result<(), VmFailure>>();
        pending_ops.insert(
            "vm-1".to_string(),
            PendingVmOp {
                tx: waiter_tx,
                host_id: host.to_string(),
            },
        );

        metrics
            .agent_connected
            .with_label_values(&[hostname])
            .set(1);

        release_agent_stream(&agents, &pending_ops, &metrics, host, hostname, old_epoch);

        assert_eq!(
            agents.get(host).map(|a| a.epoch),
            Some(new_epoch),
            "live slot must survive a stale stream's cleanup",
        );
        assert!(
            pending_ops.contains_key("vm-1"),
            "in-flight waiter for the live connection must not be cancelled",
        );
        assert!(!waiter_rx.is_terminated(), "waiter sender must remain live",);
        assert_eq!(
            metrics.agent_connected.with_label_values(&[hostname]).get(),
            1,
            "agent_connected gauge must not be cleared while a live stream owns it",
        );
    }

    /// Genuine disconnect (no reconnect): the slot still belongs to
    /// us, so we drain it — agents entry removed, host's pending
    /// waiters cancelled, metric zeroed.
    #[test]
    fn release_agent_stream_drains_when_still_owner() {
        use tokio::sync::oneshot;

        let metrics = Metrics::new(1.0).expect("metrics");
        let agents: DashMap<String, ConnectedAgent> = DashMap::new();
        let pending_ops: DashMap<String, PendingVmOp> = DashMap::new();

        let host = "host-a";
        let hostname = "host-a.local";
        let epoch = 7;

        let (tx, _rx) = mpsc::channel::<ControllerCommand>(1);
        agents.insert(
            host.to_string(),
            ConnectedAgent {
                command_tx: tx,
                epoch,
            },
        );

        let (waiter_tx, mut waiter_rx) = oneshot::channel::<Result<(), VmFailure>>();
        pending_ops.insert(
            "vm-1".to_string(),
            PendingVmOp {
                tx: waiter_tx,
                host_id: host.to_string(),
            },
        );
        // A waiter for a different host must be left alone — we only
        // cancel the disconnecting host's pipeline. Receiver bound
        // with `_` so it lives to the end of the test scope.
        let (other_tx, _other_rx) = oneshot::channel::<Result<(), VmFailure>>();
        pending_ops.insert(
            "vm-2".to_string(),
            PendingVmOp {
                tx: other_tx,
                host_id: "host-b".to_string(),
            },
        );

        metrics
            .agent_connected
            .with_label_values(&[hostname])
            .set(1);

        release_agent_stream(&agents, &pending_ops, &metrics, host, hostname, epoch);

        assert!(agents.get(host).is_none(), "slot must be released");
        assert!(
            !pending_ops.contains_key("vm-1"),
            "host's pending waiters must be cancelled",
        );
        assert!(
            pending_ops.contains_key("vm-2"),
            "other hosts' waiters must be left intact",
        );
        // Cancelling drops the sender; the receiver should resolve
        // with a recv error rather than a value.
        assert!(
            matches!(
                waiter_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Closed)
            ),
            "cancelled waiter receiver must observe channel closure",
        );
        assert_eq!(
            metrics.agent_connected.with_label_values(&[hostname]).get(),
            0,
            "agent_connected must be zeroed on real disconnect",
        );
    }
}
