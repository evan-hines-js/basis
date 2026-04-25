use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use basis_proto::*;
use dashmap::DashMap;
use futures::Stream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use basis_common::gpu::GpuInfo;
use basis_common::time::now_rfc3339;
use basis_common::tls;

use crate::config::{NetworkConfig, Pool};
use crate::db::{ClusterRow, Db, DbError, GpuAssignment, TreeRow, VmRow};
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
        DbError::HostUnavailable(_) | DbError::CapacityRaced(_) => {
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

/// Agent-reported VM create failure. Carries enough to tell a real
/// fault apart from a load-shedding signal so the controller can map
/// the two onto different gRPC status codes and metric labels.
#[derive(Debug, Clone)]
struct VmFailure {
    message: String,
    transient: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VmOpKind {
    Create,
    Delete,
}

/// Pending create-or-delete waiting for the agent to report terminal VM
/// state. One map holds both kinds because a vm_id is only ever waiting
/// on one op at a time.
struct PendingVmOp {
    tx: oneshot::Sender<Result<(), VmFailure>>,
    kind: VmOpKind,
    /// Host this op was dispatched to. When the host's agent stream
    /// drops, we remove every entry matching this host_id so the
    /// awaiting RPC fails immediately instead of stalling for the full
    /// timeout window.
    host_id: String,
}

/// Connected agent with a command channel.
struct ConnectedAgent {
    command_tx: mpsc::Sender<ControllerCommand>,
}

pub struct BasisServer {
    db: Db,
    metrics: Arc<Metrics>,
    dns_servers: Arc<Vec<String>>,
    network: Arc<NetworkConfig>,
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

/// Total time `DeleteMachine` / `DeleteCluster` will wait for the agent
/// to confirm teardown.
const DELETE_MACHINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

impl BasisServer {
    pub fn new(
        db: Db,
        metrics: Arc<Metrics>,
        dns_servers: Vec<String>,
        network: NetworkConfig,
    ) -> Self {
        Self {
            db,
            metrics,
            dns_servers: Arc::new(dns_servers),
            network: Arc::new(network),
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
            agents: self.agents.clone(),
        });
        let basis_svc = basis_server::BasisServer::new(BasisApiService {
            shared: shared.clone(),
            pending_ops: self.pending_ops.clone(),
        });
        let agent_svc = basis_agent_server::BasisAgentServer::new(BasisAgentService {
            shared,
            reconcile_interval: self.reconcile_interval,
            pending_ops: self.pending_ops.clone(),
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
    agents: Arc<DashMap<String, ConnectedAgent>>,
}

impl SharedCtx {
    /// Assemble the full authoritative `ReconcileHostCommand` for a
    /// host from DB state. Single source of truth: register-ack,
    /// periodic tick, and after-change broadcasts all go through here.
    async fn build_reconcile_command(
        &self,
        host_id: &str,
    ) -> Result<ReconcileHostCommand, DbError> {
        let expected_vm_ids = self.db.list_vm_ids_on_host(host_id).await?;
        let trees = self.db.list_host_trees(host_id).await?;
        let mut tree_states = Vec::with_capacity(trees.len());
        for tree in trees {
            // A host carries a tree only when it has ≥1 VM in it, and
            // `ensure_host_bridge_ip` runs before `insert_vm` — so the
            // mapping must exist. Treat its absence as DB corruption.
            let gateway_ip = self
                .db
                .get_host_bridge_ip(&tree.id, host_id)
                .await?
                .ok_or_else(|| {
                    DbError::Malformed(format!(
                        "tree {} host {host_id} has VMs but no bridge IP mapping",
                        tree.id
                    ))
                })?;
            tree_states.push(TreeState {
                vni: tree.vni as u32,
                gateway_ip,
                prefix_len: tree.prefix_len as u32,
                vtep_addresses: self.db.list_tree_vteps(&tree.id).await?,
                cidr: tree.cidr,
            });
        }
        Ok(ReconcileHostCommand {
            expected_vm_ids,
            trees: tree_states,
        })
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
            let _ = agent
                .command_tx
                .send(ControllerCommand {
                    request_id: String::new(),
                    command: Some(controller_command::Command::ReconcileHost(Box::new(cmd))),
                })
                .await;
        }
    }

    /// Re-broadcast reconcile to every host currently carrying the
    /// given tree. Called after a VM create/delete that shifts the
    /// peer VTEP set.
    async fn broadcast_tree(&self, tree_id: &str) {
        let hosts = match self.db.list_hosts_in_tree(tree_id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(tree_id, error = %e, "broadcast_tree: list hosts failed");
                return;
            }
        };
        for host_id in hosts {
            self.push_reconcile(&host_id).await;
        }
    }
}

// --- CAPI-facing service ---

struct BasisApiService {
    shared: Arc<SharedCtx>,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
}

impl BasisApiService {
    /// Resolve a `apiserver_vip_pool` / `lb_pools` name against the
    /// controller config. Empty string = tree (returns `Ok(None)`);
    /// any other name must match an entry in `network.pools[]`.
    fn resolve_pool(&self, name: &str) -> Result<Option<&Pool>, Status> {
        if name.is_empty() {
            return Ok(None);
        }
        self.shared
            .network
            .pool_by_name(name)
            .map(Some)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "pool '{name}' is not defined in the controller's network.pools"
                ))
            })
    }

    /// Allocate the apiserver VIP for a cluster. Empty `apiserver_pool`
    /// → tree vip_range; named pool → one /32 from that pool.
    async fn allocate_apiserver_vip(
        &self,
        apiserver_pool: Option<&Pool>,
        tree: &TreeRow,
        cluster_id: &str,
    ) -> Result<String, Status> {
        match apiserver_pool {
            None => self
                .shared
                .db
                .allocate_tree_vip(tree, cluster_id)
                .await
                .map_err(db_status),
            Some(pool) => self
                .shared
                .db
                .allocate_pool_vip(pool, cluster_id)
                .await
                .map_err(db_status),
        }
    }

    async fn cleanup_failed_vm(&self, vm_id: &str, tree_id: &str, host_id: &str) {
        if let Err(e) = self.shared.db.release_vm_ips(vm_id).await {
            warn!(vm_id, error = %e, "cleanup: release IPs");
        }
        if let Err(e) = self.shared.db.delete_vm(vm_id).await {
            warn!(vm_id, error = %e, "cleanup: delete VM row");
        }
        if let Err(e) = self
            .shared
            .db
            .release_host_bridge_ip_if_idle(tree_id, host_id)
            .await
        {
            warn!(vm_id, tree_id, host_id, error = %e, "cleanup: release host bridge IP");
        }
        self.shared.broadcast_tree(tree_id).await;
        // If this failure left the host without any VM in the tree,
        // `broadcast_tree` won't reach it — push directly so its bridge
        // comes down before another host claims the freed bridge IP.
        self.shared.push_reconcile(host_id).await;
    }

    fn register_pending_op(
        &self,
        vm_id: &str,
        host_id: &str,
        kind: VmOpKind,
    ) -> oneshot::Receiver<Result<(), VmFailure>> {
        let (tx, rx) = oneshot::channel();
        self.pending_ops.insert(
            vm_id.to_string(),
            PendingVmOp {
                tx,
                kind,
                host_id: host_id.to_string(),
            },
        );
        rx
    }

    /// Tear down a single VM synchronously: notify the agent, wait
    /// for terminal state, release resources. The synchronous
    /// confirmation is what bounds queue depth under load.
    async fn teardown_vm(&self, vm: &VmRow) -> Result<(), Status> {
        self.shared
            .db
            .update_vm_state(&vm.id, MachineState::Stopping as i64, "", &now_rfc3339())
            .await
            .map_err(db_status)?;

        let Some(agent) = self.shared.agents.get(&vm.host_id) else {
            info!(host_id = %vm.host_id, vm_id = %vm.id,
                "DeleteVm: agent not connected; returning Unavailable");
            return Err(Status::unavailable(format!(
                "host '{}' agent not connected",
                vm.host_id
            )));
        };

        let wait_rx = self.register_pending_op(&vm.id, &vm.host_id, VmOpKind::Delete);

        let cmd = ControllerCommand {
            request_id: vm.id.clone(),
            command: Some(controller_command::Command::DeleteVm(DeleteVmCommand {
                vm_id: vm.id.clone(),
            })),
        };
        if agent.command_tx.send(cmd).await.is_err() {
            self.pending_ops.remove(&vm.id);
            info!(host_id = %vm.host_id, vm_id = %vm.id,
                "DeleteVm: agent stream closed before command delivered");
            return Err(Status::unavailable("agent stream closed"));
        }
        drop(agent);

        // Cluster row is the source of truth for tree membership; look
        // it up before the DELETE so we can broadcast to the right
        // tree after cleanup.
        let tree_id = self
            .shared
            .db
            .get_cluster(&vm.cluster_id)
            .await
            .ok()
            .map(|c| c.tree_id);

        match tokio::time::timeout(DELETE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                if let Err(e) = self.shared.db.release_vm_ips(&vm.id).await {
                    warn!(vm_id = %vm.id, error = %e, "failed to release VM IPs during teardown");
                }
                self.shared.db.delete_vm(&vm.id).await.map_err(db_status)?;
                if let Some(tid) = tree_id {
                    if let Err(e) = self
                        .shared
                        .db
                        .release_host_bridge_ip_if_idle(&tid, &vm.host_id)
                        .await
                    {
                        warn!(vm_id = %vm.id, tree_id = %tid, host_id = %vm.host_id, error = %e,
                            "failed to release host bridge IP during teardown");
                    }
                    self.shared.broadcast_tree(&tid).await;
                    // `broadcast_tree` only reaches hosts that still
                    // carry a VM in this tree. If this delete was the
                    // last VM for (host, tree), the host itself is no
                    // longer in that list — push directly so it tears
                    // the bridge down and frees its bridge IP before
                    // the next periodic reconcile tick. Without this,
                    // the stale bridge can race a newly-allocated peer
                    // claiming the same IP on another host.
                    self.shared.push_reconcile(&vm.host_id).await;
                }
                info!(vm_id = %vm.id, host_id = %vm.host_id, "DeleteMachine: agent confirmed STOPPED");
                Ok(())
            }
            Ok(Ok(Err(failure))) if failure.transient => {
                warn!(
                    vm_id = %vm.id, host_id = %vm.host_id, error = %failure.message,
                    "DeleteMachine: agent shed for backpressure (transient)"
                );
                Err(Status::unavailable(format!(
                    "agent busy, retry: {}",
                    failure.message
                )))
            }
            Ok(Ok(Err(failure))) => {
                warn!(
                    vm_id = %vm.id, host_id = %vm.host_id, error = %failure.message,
                    "DeleteMachine: agent reported FAILED"
                );
                Err(Status::internal(format!(
                    "VM deletion failed: {}",
                    failure.message
                )))
            }
            Ok(Err(_)) => {
                warn!(vm_id = %vm.id, host_id = %vm.host_id,
                    "DeleteMachine: agent disconnected during deletion");
                Err(Status::unavailable("agent disconnected during VM deletion"))
            }
            Err(_) => {
                self.pending_ops.remove(&vm.id);
                warn!(
                    vm_id = %vm.id, host_id = %vm.host_id,
                    timeout_s = DELETE_MACHINE_TIMEOUT.as_secs(),
                    "DeleteMachine: timed out waiting for agent confirmation"
                );
                Err(Status::deadline_exceeded(format!(
                    "VM deletion timed out ({}s)",
                    DELETE_MACHINE_TIMEOUT.as_secs()
                )))
            }
        }
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
        }
    }

    /// One optimistic-scheduling attempt: pick a host off a fresh
    /// snapshot, allocate the VM's tree IP, then commit the row via
    /// `Db::insert_vm`. The writer's capacity gate + the `vm_gpus`
    /// unique constraint serve as the commit check; if we lose either
    /// race we roll back the IP allocation and return a classified
    /// error so the outer loop can retry.
    async fn try_place_vm(
        &self,
        req: &CreateMachineRequest,
        vm_id: &str,
        tree: &TreeRow,
        now: &str,
    ) -> Result<Placement, PlaceError> {
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
            .allocate_tree_vm_ip(tree, vm_id)
            .await
            .map_err(|e| PlaceError::Internal(db_status(e)))?;

        let extra_disk_gibs: Vec<u32> = req.extra_disks.iter().map(|d| d.size_gib).collect();
        let extra_disk_total_gib: i64 = extra_disk_gibs.iter().map(|&g| g as i64).sum();
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
            extra_disk_total_gib,
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
        info!(
            name = %req.name,
            parent = %req.parent_cluster_id,
            apiserver_pool = %req.apiserver_vip_pool,
            "CreateCluster received"
        );
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        // Fail fast on a bad pool name before allocating anything.
        let apiserver_pool = self.resolve_pool(&req.apiserver_vip_pool)?;

        // Idempotent by name.
        if let Some(existing) = self
            .shared
            .db
            .get_cluster_by_name(&req.name)
            .await
            .map_err(db_status)?
        {
            let tree = self
                .shared
                .db
                .get_tree(&existing.tree_id)
                .await
                .map_err(db_status)?;
            info!(cluster_id = %existing.id, name = %req.name, "CreateCluster idempotent return");
            return Ok(Response::new(create_cluster_response(&existing, &tree)));
        }

        // Resolve or allocate the tree.
        let tree = if req.parent_cluster_id.is_empty() {
            self.shared
                .db
                .allocate_tree(&self.shared.network)
                .await
                .map_err(db_status)?
        } else {
            let parent = self
                .shared
                .db
                .get_cluster(&req.parent_cluster_id)
                .await
                .map_err(|e| match e {
                    DbError::NotFound(_) => Status::not_found(format!(
                        "parent cluster '{}' not found",
                        req.parent_cluster_id
                    )),
                    other => db_status(other),
                })?;
            self.shared
                .db
                .get_tree(&parent.tree_id)
                .await
                .map_err(db_status)?
        };

        let cluster_id = uuid::Uuid::new_v4().to_string();
        let endpoint = self
            .allocate_apiserver_vip(apiserver_pool, &tree, &cluster_id)
            .await?;

        let row = ClusterRow {
            id: cluster_id.clone(),
            name: req.name.clone(),
            tree_id: tree.id.clone(),
            parent_cluster_id: if req.parent_cluster_id.is_empty() {
                None
            } else {
                Some(req.parent_cluster_id.clone())
            },
            control_plane_endpoint: endpoint.clone(),
            apiserver_pool: req.apiserver_vip_pool.clone(),
            created_at: now_rfc3339(),
        };
        if let Err(e) = self.shared.db.insert_cluster(&row).await {
            if let Err(re) = self.shared.db.release_cluster_ips(&cluster_id).await {
                warn!(cluster_id = %cluster_id, error = %re,
                    "rollback: release_cluster_ips after insert_cluster failure");
            }
            return match e {
                DbError::Conflict(_) => {
                    // Concurrent CreateCluster with the same name
                    // beat us. Return the committed row as an
                    // idempotent success.
                    let existing = self
                        .shared
                        .db
                        .get_cluster_by_name(&req.name)
                        .await
                        .map_err(db_status)?
                        .ok_or_else(|| {
                            Status::internal(format!(
                                "cluster '{}' insert rejected as duplicate but row not found",
                                req.name,
                            ))
                        })?;
                    let etree = self
                        .shared
                        .db
                        .get_tree(&existing.tree_id)
                        .await
                        .map_err(db_status)?;
                    Ok(Response::new(create_cluster_response(&existing, &etree)))
                }
                other => Err(db_status(other)),
            };
        }

        info!(
            cluster_id = %cluster_id,
            name = %req.name,
            endpoint = %endpoint,
            tree_id = %tree.id,
            vni = tree.vni,
            "CreateCluster: new cluster provisioned"
        );
        Ok(Response::new(create_cluster_response(&row, &tree)))
    }

    async fn delete_cluster(
        &self,
        request: Request<DeleteClusterRequest>,
    ) -> Result<Response<DeleteClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        info!(cluster_id = %req.cluster_id, "DeleteCluster received");

        let cluster = self
            .shared
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;

        // Refuse to orphan a subtree — children share this cluster's
        // tree membership but are their own lifecycle units in Lattice.
        let children = self
            .shared
            .db
            .list_child_clusters(&req.cluster_id)
            .await
            .map_err(db_status)?;
        if !children.is_empty() {
            return Err(Status::failed_precondition(format!(
                "cluster '{}' has {} child cluster(s); delete them first",
                cluster.name,
                children.len()
            )));
        }

        let vms = self
            .shared
            .db
            .list_vms(Some(&req.cluster_id))
            .await
            .map_err(db_status)?;
        info!(cluster_id = %req.cluster_id, vm_count = vms.len(), "DeleteCluster: cascading VM deletes");
        futures::future::try_join_all(vms.iter().map(|vm| self.teardown_vm(vm))).await?;

        if let Err(e) = self.shared.db.release_cluster_ips(&req.cluster_id).await {
            warn!(cluster_id = %req.cluster_id, error = %e,
                "failed to release cluster VIP during DeleteCluster");
        }
        self.shared
            .db
            .delete_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;

        let remaining = self
            .shared
            .db
            .count_clusters_in_tree(&cluster.tree_id)
            .await
            .map_err(db_status)?;
        if remaining == 0 {
            if let Err(e) = self.shared.db.delete_tree(&cluster.tree_id).await {
                warn!(tree_id = %cluster.tree_id, error = %e, "delete_tree failed");
            }
        }

        info!(cluster_id = %req.cluster_id, "DeleteCluster complete");
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
        let tree = self
            .shared
            .db
            .get_tree(&cluster.tree_id)
            .await
            .map_err(db_status)?;
        Ok(Response::new(cluster_to_proto(&cluster, &tree)))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        require_capi_caller(&request)?;
        let clusters = self.shared.db.list_clusters().await.map_err(db_status)?;
        // Batch-load trees into a map so we only hit the reader pool
        // once per distinct tree rather than N times per cluster.
        let mut tree_cache: HashMap<String, TreeRow> = HashMap::new();
        let mut out = Vec::with_capacity(clusters.len());
        for c in clusters {
            let tree = match tree_cache.get(&c.tree_id) {
                Some(t) => t.clone(),
                None => {
                    let t = self
                        .shared
                        .db
                        .get_tree(&c.tree_id)
                        .await
                        .map_err(db_status)?;
                    tree_cache.insert(c.tree_id.clone(), t.clone());
                    t
                }
            };
            out.push(cluster_to_proto(&c, &tree));
        }
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

        let tree = self
            .shared
            .db
            .get_tree(&cluster.tree_id)
            .await
            .map_err(db_status)?;

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
                match self.try_place_vm(&req, &vm_id, &tree, &now).await {
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

        // VM row now exists, so the host is registered as carrying
        // this tree — `release_host_bridge_ip_if_idle` running from
        // a concurrent delete will see VM count ≥ 1 and leave the
        // mapping alone. Find-or-allocate the host's bridge IP here.
        let host_gateway_ip = match self.shared.db.ensure_host_bridge_ip(&tree, &host_id).await {
            Ok(ip) => ip,
            Err(e) => {
                warn!(vm_id = %vm_id, host_id = %host_id, error = %e,
                    "host bridge IP allocation failed after insert; cleaning up");
                self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
                return Err(db_status(e));
            }
        };

        // Push the authoritative reconcile before dispatching CreateVm
        // so the agent has the tree's bridge up before it has to
        // attach the VM's tap. mpsc preserves send order from this
        // task, so the broadcast lands first.
        self.shared.broadcast_tree(&tree.id).await;

        let Some(agent) = self.shared.agents.get(&host_id) else {
            outcome.set("no_agent");
            warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: scheduled host has no connected agent");
            return Err(Status::unavailable(format!(
                "agent for host '{host_id}' not connected"
            )));
        };

        let wait_rx = self.register_pending_op(&vm_id, &host_id, VmOpKind::Create);

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
                    prefix_len: tree.prefix_len as u32,
                    gpus: req.gpus,
                    gpu_constraints: req.gpu_constraints,
                    dns_servers: self.shared.dns_servers.as_ref().clone(),
                    gpu_pci_addresses: gpu_assignments
                        .iter()
                        .map(|g| g.pci_address.clone())
                        .collect(),
                    extra_disks: req.extra_disks,
                    vni: tree.vni as u32,
                },
            ))),
        };

        info!(vm_id = %vm_id, host_id = %host_id, vni = tree.vni, "CreateMachine: dispatching CreateVm to agent");
        if agent.command_tx.send(cmd).await.is_err() {
            self.pending_ops.remove(&vm_id);
            self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
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
                self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
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
                self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                self.pending_ops.remove(&vm_id);
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
        let vm = self.shared.db.get_vm(&req.id).await.map_err(db_status)?;
        self.teardown_vm(&vm).await?;
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
        Ok(Response::new(vm_to_machine(&vm, &gpus)))
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
        let machines = vms
            .iter()
            .map(|v| vm_to_machine(v, gpus_by_vm.remove(&v.id).as_deref().unwrap_or(&[])))
            .collect();
        Ok(Response::new(ListMachinesResponse { machines }))
    }
}

// --- Agent-facing service ---

struct BasisAgentService {
    shared: Arc<SharedCtx>,
    reconcile_interval: std::time::Duration,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
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

        // Initial reconcile — agent uses this to build bridges + tear
        // down forgotten VMs before processing any CreateVm.
        let initial_state = self
            .shared
            .build_reconcile_command(&host_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        command_tx
            .send(ControllerCommand {
                request_id: String::new(),
                command: Some(controller_command::Command::RegisterAck(Box::new(
                    RegisterHostResponse {
                        host_id: host_id.clone(),
                        initial_state: Some(initial_state),
                    },
                ))),
            })
            .await
            .map_err(|_| Status::internal("failed to send registration ack"))?;

        self.shared.agents.insert(
            host_id.clone(),
            ConnectedAgent {
                command_tx: command_tx.clone(),
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
        let pending_ops = self.pending_ops.clone();
        let agent_host_id = host_id.clone();
        let agent_hostname = register.hostname.clone();
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Err(e) =
                            handle_agent_message(&shared, &pending_ops, &agent_host_id, msg).await
                        {
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
            shared.agents.remove(&agent_host_id);
            reconcile_handle.abort();
            let stale: Vec<String> = pending_ops
                .iter()
                .filter(|e| e.value().host_id == agent_host_id)
                .map(|e| e.key().clone())
                .collect();
            for vm_id in &stale {
                pending_ops.remove(vm_id);
            }
            if !stale.is_empty() {
                warn!(
                    host_id = %agent_host_id,
                    cancelled = stale.len(),
                    "cancelled in-flight VM op waiters for disconnected agent"
                );
            }
            shared
                .metrics
                .agent_connected
                .with_label_values(&[&agent_hostname])
                .set(0);
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
        let gpu_json = serde_json::to_string(&register.gpus)
            .expect("serializing Vec<GpuDevice> to JSON is infallible");

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

        let host = crate::db::HostRow {
            id: host_id.clone(),
            hostname: register.hostname.clone(),
            total_cpu: register.total_cpu as i64,
            total_memory_mib: register.total_memory_mib as i64,
            total_disk_gib: register.total_disk_gib as i64,
            gpu_inventory: gpu_json,
            vtep_address: register.vtep_address.clone(),
            last_heartbeat: now_rfc3339(),
            healthy: true,
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
    pending_ops: &DashMap<String, PendingVmOp>,
    host_id: &str,
    msg: AgentMessage,
) -> anyhow::Result<()> {
    match msg.payload {
        Some(agent_message::Payload::Heartbeat(hb)) => {
            shared
                .db
                .update_host_heartbeat(&hb.host_id, &now_rfc3339())
                .await?;
        }
        Some(agent_message::Payload::VmState(report)) => {
            let state = report.state();
            let now = now_rfc3339();

            let prev = shared.db.get_vm(&report.vm_id).await.ok();

            shared
                .db
                .update_vm_state(&report.vm_id, state as i64, &report.error_message, &now)
                .await?;

            if state == MachineState::Running {
                let prev_state = prev.as_ref().map(|r| r.state);
                let first_time_running = prev_state != Some(MachineState::Running as i64);
                if first_time_running {
                    if let Some(row) = prev.as_ref() {
                        if let Ok(created) = humantime::parse_rfc3339(&row.created_at) {
                            if let Ok(elapsed) =
                                std::time::SystemTime::now().duration_since(created)
                            {
                                shared
                                    .metrics
                                    .vm_time_to_running_seconds
                                    .observe(elapsed.as_secs_f64());
                            }
                        }
                    }
                }
            }

            let resolves = matches!(
                (pending_ops.get(&report.vm_id).map(|p| p.kind), state),
                (Some(VmOpKind::Create), MachineState::Running)
                    | (Some(VmOpKind::Delete), MachineState::Stopped)
                    | (Some(_), MachineState::Failed)
            );
            if resolves {
                if let Some((_, pending)) = pending_ops.remove(&report.vm_id) {
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
        Some(agent_message::Payload::Register(_)) => {
            warn!(host_id, "unexpected register message on established stream");
        }
        None => {}
    }

    Ok(())
}

// --- Proto conversions — single source of truth ---

fn cluster_to_proto(c: &ClusterRow, tree: &TreeRow) -> Cluster {
    Cluster {
        cluster_id: c.id.clone(),
        name: c.name.clone(),
        tree_id: tree.id.clone(),
        parent_cluster_id: c.parent_cluster_id.clone().unwrap_or_default(),
        control_plane_endpoint: c.control_plane_endpoint.clone(),
        vni: tree.vni as u32,
    }
}

fn create_cluster_response(c: &ClusterRow, tree: &TreeRow) -> CreateClusterResponse {
    CreateClusterResponse {
        cluster_id: c.id.clone(),
        control_plane_endpoint: c.control_plane_endpoint.clone(),
        tree_id: tree.id.clone(),
        vni: tree.vni as u32,
    }
}

fn vm_to_machine(vm: &VmRow, gpus: &[GpuAssignment]) -> Machine {
    let narrow = |field: &'static str, v: i64| -> u32 {
        u32::try_from(v).unwrap_or_else(|_| {
            panic!(
                "vms.{field} = {v} on vm '{}' is out of u32 range — DB corruption",
                vm.id
            )
        })
    };

    Machine {
        id: vm.id.clone(),
        name: vm.name.clone(),
        cluster_id: vm.cluster_id.clone(),
        host: vm.host_id.clone(),
        provider_id: provider_id(&vm.id),
        ip_address: vm.ip_address.clone(),
        state: vm.state as i32,
        cpu: narrow("cpu", vm.cpu),
        memory_mib: narrow("memory_mib", vm.memory_mib),
        disk_gib: narrow("disk_gib", vm.disk_gib),
        gpus: gpus
            .iter()
            .map(|g| MachineGpu {
                pci_address: g.pci_address.clone(),
                model: g.model.clone(),
                nvlink_group: g.nvlink_group as u32,
            })
            .collect(),
        error_message: vm.error_message.clone(),
        extra_disks: vm
            .extra_disks()
            .into_iter()
            .map(|size_gib| ExtraDisk { size_gib })
            .collect(),
    }
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
