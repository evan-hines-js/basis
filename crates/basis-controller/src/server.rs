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
use tracing::{info, warn};

use basis_common::json::parse_owned_json;
use basis_common::time::now_rfc3339;
use basis_common::tls;

use crate::config::NetworkConfig;
use crate::db::{ClusterRow, Db, DbError, IpOwner, TreeRow, VmRow};
use crate::metrics::Metrics;
use crate::scheduler::{self, ScheduleRequest, SchedulerError};

/// Map a [`DbError`] to the gRPC status the API should return.
fn db_status(e: DbError) -> Status {
    match e {
        DbError::NotFound(_) => Status::not_found(e.to_string()),
        DbError::Conflict(_) => Status::already_exists(e.to_string()),
        DbError::Exhausted(_) => Status::resource_exhausted(e.to_string()),
        DbError::HostUnavailable(_) => Status::unavailable(e.to_string()),
        DbError::Sqlx(_) | DbError::Migrate(_) | DbError::MalformedTree { .. } => {
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
    cpu_overcommit_ratio: f32,
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
        cpu_overcommit_ratio: f32,
    ) -> Self {
        Self {
            db,
            metrics,
            dns_servers: Arc::new(dns_servers),
            network: Arc::new(network),
            cpu_overcommit_ratio,
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
            cpu_overcommit_ratio: self.cpu_overcommit_ratio,
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

/// State shared across the CAPI-facing and agent-facing services. Both
/// need the DB, the metrics handle, and the live agent map; sharing
/// one struct makes it easy to add helpers (like
/// `push_reconcile_to_host`) that both sides call.
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
    async fn build_reconcile_command(&self, host_id: &str) -> Result<ReconcileHostCommand, DbError> {
        let expected_vm_ids: Vec<String> = self
            .db
            .list_vms_on_host(host_id)
            .await?
            .into_iter()
            .map(|v| v.id)
            .collect();
        let trees = self.db.list_host_trees(host_id).await?;
        let mut tree_states = Vec::with_capacity(trees.len());
        for tree in trees {
            let vteps = self.db.list_tree_vteps(&tree.id).await?;
            tree_states.push(TreeState {
                tree_id: tree.id,
                vni: tree.vni as u32,
                cidr: tree.cidr,
                gateway_ip: tree.gateway_ip,
                prefix_len: tree.prefix_len as u32,
                vtep_addresses: vteps,
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
                    command: Some(controller_command::Command::ReconcileHost(cmd)),
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
    cpu_overcommit_ratio: f32,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
}

impl BasisApiService {
    async fn cleanup_failed_vm(&self, vm_id: &str, tree_id: &str, host_id: &str) {
        if let Err(e) = self.shared.db.release_ips(IpOwner::Vm(vm_id)).await {
            warn!(vm_id, error = %e, "cleanup: release IPs");
        }
        if let Err(e) = self.shared.db.delete_vm(vm_id).await {
            warn!(vm_id, error = %e, "cleanup: delete VM row");
        }
        // Membership may have flipped if this was the host's only VM
        // in the tree; reconcile converges.
        match self
            .shared
            .db
            .remove_host_in_tree_if_empty(host_id, tree_id)
            .await
        {
            Ok(removed) => {
                if removed {
                    self.shared.broadcast_tree(tree_id).await;
                }
            }
            Err(e) => warn!(vm_id, error = %e, "cleanup: host_in_tree teardown"),
        }
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

    /// Tear down a single VM synchronously: notify the agent, wait for
    /// terminal state, release resources, update host_in_tree. The
    /// synchronous confirmation is what bounds queue depth (callers
    /// wait on the RPC rather than pipelining creates behind an
    /// unresolved delete).
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

        let tree_id = self
            .shared
            .db
            .get_cluster(&vm.cluster_id)
            .await
            .ok()
            .map(|c| c.tree_id);

        match tokio::time::timeout(DELETE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                if let Err(e) = self.shared.db.release_ips(IpOwner::Vm(&vm.id)).await {
                    warn!(vm_id = %vm.id, error = %e, "failed to release VM IPs during teardown");
                }
                self.shared.db.delete_vm(&vm.id).await.map_err(db_status)?;

                // If this was the last VM of the tree on this host,
                // drop the membership row and push a fresh reconcile
                // to everyone still in the tree so their peer lists
                // shrink.
                if let Some(tid) = tree_id {
                    match self
                        .shared
                        .db
                        .remove_host_in_tree_if_empty(&vm.host_id, &tid)
                        .await
                    {
                        Ok(true) => self.shared.broadcast_tree(&tid).await,
                        Ok(false) => self.shared.push_reconcile(&vm.host_id).await,
                        Err(e) => {
                            warn!(vm_id = %vm.id, error = %e, "teardown: host_in_tree update failed");
                        }
                    }
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

    async fn pick_host(
        &self,
        req: &CreateMachineRequest,
    ) -> Result<(String, Vec<GpuDevice>), Status> {
        let hosts = self.shared.db.list_healthy_hosts().await.map_err(db_status)?;
        let mut vms_by_host: HashMap<String, Vec<VmRow>> = HashMap::new();
        for host in &hosts {
            let vms = self
                .shared
                .db
                .list_vms_on_host(&host.id)
                .await
                .map_err(db_status)?;
            vms_by_host.insert(host.id.clone(), vms);
        }

        let sched_req = ScheduleRequest::from(req);
        match scheduler::schedule(&hosts, &vms_by_host, &sched_req, self.cpu_overcommit_ratio) {
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
                    cpu_overcommit_ratio = self.cpu_overcommit_ratio,
                    "scheduler rejected VM: no capacity"
                );
                Err(Status::resource_exhausted(msg))
            }
        }
    }

    fn record_create_result(&self, result: &'static str, started: Instant) {
        self.shared
            .metrics
            .vm_create_result_total
            .with_label_values(&[result])
            .inc();
        self.shared
            .metrics
            .vm_create_duration_seconds
            .with_label_values(&[result])
            .observe(started.elapsed().as_secs_f64());
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
            "CreateCluster received"
        );
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

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
            info!(
                cluster_id = %existing.id,
                name = %req.name,
                tree_id = %tree.id,
                vni = tree.vni,
                "CreateCluster idempotent return"
            );
            return Ok(Response::new(create_cluster_response(&existing, &tree)));
        }

        // Resolve or allocate the tree.
        let tree = if req.parent_cluster_id.is_empty() {
            let now_unix = basis_common::time::now_unix();
            self.shared
                .db
                .allocate_tree(&self.shared.network, now_unix)
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
        let control_plane_endpoint = self
            .shared
            .db
            .allocate_tree_vip(&tree, IpOwner::ClusterVip(&cluster_id))
            .await
            .map_err(db_status)?;

        let row = ClusterRow {
            id: cluster_id.clone(),
            name: req.name.clone(),
            tree_id: tree.id.clone(),
            parent_cluster_id: if req.parent_cluster_id.is_empty() {
                None
            } else {
                Some(req.parent_cluster_id.clone())
            },
            control_plane_endpoint: control_plane_endpoint.clone(),
            created_at: now_rfc3339(),
        };
        if let Err(e) = self.shared.db.insert_cluster(&row).await {
            if let Err(re) = self
                .shared
                .db
                .release_ips(IpOwner::ClusterVip(&cluster_id))
                .await
            {
                warn!(cluster_id = %cluster_id, error = %re,
                    "failed to release VIP after cluster insert failure");
            }
            return match e {
                DbError::Conflict(_) => {
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
            endpoint = %control_plane_endpoint,
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

        if let Err(e) = self
            .shared
            .db
            .release_ips(IpOwner::ClusterVip(&req.cluster_id))
            .await
        {
            warn!(cluster_id = %req.cluster_id, error = %e,
                "failed to release cluster VIP during DeleteCluster");
        }
        self.shared
            .db
            .delete_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;

        // If this was the last cluster in the tree, mark it deleted
        // (VNI cooldown kicks in). The next allocate_tree call will
        // reap it once the cooldown expires.
        let remaining = self
            .shared
            .db
            .count_clusters_in_tree(&cluster.tree_id)
            .await
            .map_err(db_status)?;
        if remaining == 0 {
            let now_unix = basis_common::time::now_unix();
            if let Err(e) = self
                .shared
                .db
                .mark_tree_deleted(&cluster.tree_id, now_unix)
                .await
            {
                warn!(tree_id = %cluster.tree_id, error = %e, "mark_tree_deleted failed");
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
                    let t = self.shared.db.get_tree(&c.tree_id).await.map_err(db_status)?;
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
        info!(
            cluster_id = %req.cluster_id,
            name = %req.name,
            cpu = req.cpu,
            memory_mib = req.memory_mib,
            disk_gib = req.disk_gib,
            gpus = req.gpus,
            edge = req.edge,
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

        let (host_id, gpu_devices) = match self.pick_host(&req).await {
            Ok(v) => v,
            Err(status) => {
                if status.code() == tonic::Code::ResourceExhausted {
                    self.record_create_result("no_capacity", started);
                }
                return Err(status);
            }
        };

        // Allocate all IPs up front. Rollback on insert failure
        // releases every row keyed by vm_id (tree + edge).
        let ip_address = self
            .shared
            .db
            .allocate_tree_vm_ip(&tree, IpOwner::Vm(&vm_id))
            .await
            .map_err(db_status)?;

        let edge_ip = if req.edge {
            match self
                .shared
                .db
                .allocate_edge_ip(&self.shared.network, IpOwner::Vm(&vm_id))
                .await
            {
                Ok(ip) => Some(ip),
                Err(e) => {
                    // All IPs keyed by this vm_id; single release drops
                    // the primary that already succeeded.
                    warn!(vm_id = %vm_id, error = %e, "edge IP allocation failed; rolling back");
                    if let Err(re) = self.shared.db.release_ips(IpOwner::Vm(&vm_id)).await {
                        warn!(vm_id = %vm_id, error = %re, "rollback failed");
                    }
                    return Err(db_status(e));
                }
            }
        } else {
            None
        };

        let gpu_json = serde_json::to_string(&gpu_devices)
            .expect("serializing Vec<GpuDevice> to JSON is infallible");
        let extra_disk_gibs: Vec<u32> = req.extra_disks.iter().map(|d| d.size_gib).collect();
        let extra_disk_json = serde_json::to_string(&extra_disk_gibs)
            .expect("serializing Vec<u32> to JSON is infallible");
        let vm = VmRow {
            id: vm_id.clone(),
            name: req.name.clone(),
            cluster_id: req.cluster_id.clone(),
            host_id: host_id.clone(),
            ip_address: ip_address.clone(),
            edge_ip: edge_ip.clone(),
            state: MachineState::Creating as i64,
            cpu: req.cpu as i64,
            memory_mib: req.memory_mib as i64,
            disk_gib: req.disk_gib as i64,
            gpu_assignments: gpu_json,
            extra_disk_gibs: extra_disk_json,
            image: req.image.clone(),
            error_message: String::new(),
            created_at: now.clone(),
            updated_at: now,
        };
        if let Err(e) = self.shared.db.insert_vm(&vm).await {
            if let Err(re) = self.shared.db.release_ips(IpOwner::Vm(&vm_id)).await {
                warn!(vm_id = %vm_id, error = %re,
                    "failed to release VM IPs after insert failure");
            }
            return match e {
                DbError::Conflict(_) => {
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
                    Ok(Response::new(vm_to_create_response(&existing)))
                }
                DbError::HostUnavailable(host) => {
                    warn!(
                        vm_id = %vm_id,
                        host_id = %host,
                        "CreateMachine: scheduled host became unhealthy before insert"
                    );
                    Err(Status::unavailable(format!(
                        "host '{host}' became unhealthy during scheduling — retry",
                    )))
                }
                other => Err(db_status(other)),
            };
        }

        // Record host ⇄ tree membership and push the authoritative
        // reconcile before we dispatch the CreateVm command. Ordering
        // matters: the agent needs the bridge for this tree on this
        // host before it processes CreateVm, and mpsc preserves send
        // order from the same task.
        let first_of_tree_on_host = self
            .shared
            .db
            .upsert_host_in_tree(&host_id, &tree.id)
            .await
            .map_err(db_status)?;
        if first_of_tree_on_host {
            // Broadcast to every host in the tree (including this one)
            // so existing peers learn the new VTEP and this host
            // learns the existing ones.
            self.shared.broadcast_tree(&tree.id).await;
        } else {
            // No membership change; still push this host so its
            // expected_vm_ids includes the new VM promptly.
            self.shared.push_reconcile(&host_id).await;
        }

        let agent = self.shared.agents.get(&host_id).ok_or_else(|| {
            self.record_create_result("no_agent", started);
            warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: scheduled host has no connected agent");
            Status::unavailable(format!("agent for host '{host_id}' not connected"))
        })?;

        let wait_rx = self.register_pending_op(&vm_id, &host_id, VmOpKind::Create);

        let cmd = ControllerCommand {
            request_id: vm_id.clone(),
            command: Some(controller_command::Command::CreateVm(CreateVmCommand {
                vm_id: vm_id.clone(),
                name: req.name,
                cpu: req.cpu,
                memory_mib: req.memory_mib,
                disk_gib: req.disk_gib,
                image: req.image,
                bootstrap_data: req.bootstrap_data,
                ip_address: ip_address.clone(),
                gateway: tree.gateway_ip.clone(),
                prefix_len: tree.prefix_len as u32,
                gpus: req.gpus,
                gpu_constraints: req.gpu_constraints,
                dns_servers: self.shared.dns_servers.as_ref().clone(),
                gpu_pci_addresses: gpu_devices.iter().map(|g| g.pci_address.clone()).collect(),
                extra_disks: req.extra_disks,
                vni: tree.vni as u32,
                edge_ip: edge_ip.clone().unwrap_or_default(),
                edge_gateway: if edge_ip.is_some() {
                    self.shared.network.edge_pool.gateway.clone()
                } else {
                    String::new()
                },
                edge_prefix_len: if edge_ip.is_some() {
                    u32::from(
                        self.shared
                            .network
                            .edge_pool
                            .prefix_len()
                            .map_err(|e| Status::internal(format!("edge prefix: {e}")))?,
                    )
                } else {
                    0
                },
            })),
        };

        info!(vm_id = %vm_id, host_id = %host_id, vni = tree.vni, "CreateMachine: dispatching CreateVm to agent");
        if agent.command_tx.send(cmd).await.is_err() {
            self.pending_ops.remove(&vm_id);
            self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
            self.record_create_result("stream_closed", started);
            warn!(
                vm_id = %vm_id,
                host_id = %host_id,
                "CreateMachine: agent stream closed before command delivered"
            );
            return Err(Status::unavailable("agent stream closed"));
        }
        drop(agent);

        let result_label: &'static str;
        let response = match tokio::time::timeout(CREATE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                result_label = "placed";
                info!(vm_id = %vm_id, host_id = %host_id, ip = %ip_address, "CreateMachine: agent reported RUNNING");
                let row = self.shared.db.get_vm(&vm_id).await.map_err(db_status)?;
                Ok(Response::new(vm_to_create_response(&row)))
            }
            Ok(Ok(Err(failure))) => {
                self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
                if failure.transient {
                    warn!(
                        vm_id = %vm_id, host_id = %host_id, error = %failure.message,
                        "CreateMachine: agent shed for backpressure (transient)"
                    );
                    result_label = "busy";
                    Err(Status::unavailable(format!(
                        "agent busy, retry: {}",
                        failure.message
                    )))
                } else {
                    warn!(
                        vm_id = %vm_id, host_id = %host_id, error = %failure.message,
                        "CreateMachine: agent reported FAILED"
                    );
                    result_label = "vm_failed";
                    Err(Status::internal(format!(
                        "VM creation failed: {}",
                        failure.message
                    )))
                }
            }
            Ok(Err(_)) => {
                warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: agent disconnected during VM creation");
                self.cleanup_failed_vm(&vm_id, &tree.id, &host_id).await;
                result_label = "agent_error";
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
                result_label = "timeout";
                warn!(
                    vm_id = %vm_id,
                    host_id = %host_id,
                    timeout_s = CREATE_MACHINE_TIMEOUT.as_secs(),
                    "CreateMachine: timed out waiting for agent to report RUNNING"
                );
                Err(Status::deadline_exceeded(timeout_msg))
            }
        };
        self.record_create_result(result_label, started);
        response
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
        Ok(Response::new(vm_to_machine(&vm)))
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
        Ok(Response::new(ListMachinesResponse {
            machines: vms.iter().map(vm_to_machine).collect(),
        }))
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
                command: Some(controller_command::Command::RegisterAck(
                    RegisterHostResponse {
                        host_id: host_id.clone(),
                        initial_state: Some(initial_state),
                    },
                )),
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
                            command: Some(controller_command::Command::ReconcileHost(cmd)),
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

fn vm_to_machine(vm: &VmRow) -> Machine {
    let gpu_devices: Vec<GpuDevice> = parse_owned_json(&vm.gpu_assignments, "vms.gpu_assignments");
    let gpus = gpu_devices
        .into_iter()
        .map(|g| MachineGpu {
            pci_address: g.pci_address,
            model: g.model,
            nvlink_group: g.nvlink_group,
        })
        .collect();

    let extra_disks: Vec<ExtraDisk> =
        parse_owned_json::<Vec<u32>>(&vm.extra_disk_gibs, "vms.extra_disk_gibs")
            .into_iter()
            .map(|size_gib| ExtraDisk { size_gib })
            .collect();

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
        gpus,
        error_message: vm.error_message.clone(),
        extra_disks,
        edge: vm.edge_ip.is_some(),
        edge_ip: vm.edge_ip.clone().unwrap_or_default(),
    }
}

/// Narrower response wrapper for create/idempotent-return paths.
fn vm_to_create_response(vm: &VmRow) -> CreateMachineResponse {
    CreateMachineResponse {
        id: vm.id.clone(),
        provider_id: provider_id(&vm.id),
        ip_address: vm.ip_address.clone(),
        host: vm.host_id.clone(),
        edge_ip: vm.edge_ip.clone().unwrap_or_default(),
    }
}
