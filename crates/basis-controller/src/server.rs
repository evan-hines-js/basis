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

use crate::db::{ClusterRow, Db, DbError, IpOwner, VmRow};
use crate::metrics::Metrics;
use crate::scheduler::{self, ScheduleRequest, SchedulerError};

/// Map a [`DbError`] to the gRPC status the API should return.
///
/// Centralised so every call site agrees: `NotFound` is 404, `Conflict`
/// is `AlreadyExists`, `Exhausted` is `ResourceExhausted`,
/// `HostUnavailable` is `Unavailable`, and everything else is `Internal`.
/// Replaces the previous ad-hoc `map_err(|e| Status::not_found(...))`
/// pattern that incorrectly surfaced transient sqlx errors as 404s.
fn db_status(e: DbError) -> Status {
    match e {
        DbError::NotFound(_) => Status::not_found(e.to_string()),
        DbError::Conflict(_) => Status::already_exists(e.to_string()),
        DbError::Exhausted(_) => Status::resource_exhausted(e.to_string()),
        DbError::HostUnavailable(_) => Status::unavailable(e.to_string()),
        DbError::Sqlx(_) | DbError::Migrate(_) | DbError::MalformedIpPool { .. } => {
            Status::internal(e.to_string())
        }
    }
}

fn cluster_response(row: ClusterRow) -> Response<CreateClusterResponse> {
    Response::new(CreateClusterResponse {
        cluster_id: row.id,
        control_plane_endpoint: row.control_plane_endpoint,
    })
}

fn create_machine_response(row: VmRow) -> Response<CreateMachineResponse> {
    Response::new(CreateMachineResponse {
        id: row.id.clone(),
        provider_id: provider_id(&row.id),
        ip_address: row.ip_address,
        host: row.host_id,
    })
}

/// Require that a CAPI-facing RPC was issued by a client whose peer
/// identity is [`tls::CAPI_PROVIDER_IDENTITY`]. Rejects any other
/// identity, including a missing one.
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

/// Extract the peer identity (SAN-DNS preferred, CN fallback) from the
/// request, treating anything less as an authentication failure. The
/// server is always TLS-terminated so the only reason this returns an
/// error is misconfiguration or a missing cert.
fn peer_identity<T>(req: &Request<T>) -> Result<String, Status> {
    match tls::request_peer_identity(req) {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(Status::unauthenticated("TLS required")),
        Err(e) => Err(Status::unauthenticated(format!("peer certificate: {e}"))),
    }
}

/// Agent-reported VM create failure. Carries enough to tell a real
/// fault apart from a load-shedding signal so the controller can map
/// the two onto different gRPC status codes and metric labels.
#[derive(Debug, Clone)]
struct VmFailure {
    message: String,
    transient: bool,
}

/// Which RPC is waiting on an agent-reported VM state transition.
/// Create waits for [`MachineState::Running`]; delete waits for
/// [`MachineState::Stopped`]. Either may instead see
/// [`MachineState::Failed`] and is resolved with the agent's error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VmOpKind {
    Create,
    Delete,
}

/// Pending create-or-delete request waiting for the agent to report
/// terminal VM state. One map holds both kinds because a vm_id is only
/// ever waiting on one op at a time (create-then-delete is strictly
/// sequential), and the agent-message handler routes terminal reports
/// to whichever is pending.
struct PendingVmOp {
    tx: oneshot::Sender<Result<(), VmFailure>>,
    kind: VmOpKind,
    /// Host this op was dispatched to. When the host's agent stream
    /// drops, the stream handler removes every entry matching this
    /// host_id so the awaiting RPC fails immediately instead of
    /// stalling for the full timeout window.
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
    cpu_overcommit_ratio: f32,
    reconcile_interval: std::time::Duration,
    agents: Arc<DashMap<String, ConnectedAgent>>,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
}

/// How often the controller pushes `ReconcileHostCommand` to each
/// connected agent. Matches the agent's own periodic local-reconcile
/// cadence so drift is converged on within one minute.
const DEFAULT_AGENT_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

impl BasisServer {
    pub fn new(
        db: Db,
        metrics: Arc<Metrics>,
        dns_servers: Vec<String>,
        cpu_overcommit_ratio: f32,
    ) -> Self {
        Self {
            db,
            metrics,
            dns_servers: Arc::new(dns_servers),
            cpu_overcommit_ratio,
            reconcile_interval: DEFAULT_AGENT_RECONCILE_INTERVAL,
            agents: Arc::new(DashMap::new()),
            pending_ops: Arc::new(DashMap::new()),
        }
    }

    /// Override the controller→agent reconcile cadence. Intended for tests
    /// that need to observe a `ReconcileHostCommand` without waiting a
    /// minute.
    pub fn with_reconcile_interval(mut self, interval: std::time::Duration) -> Self {
        self.reconcile_interval = interval;
        self
    }

    /// Serve on a caller-provided TCP listener with a caller-provided TLS
    /// config. The listener must already be bound.
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
        let basis_svc = basis_server::BasisServer::new(BasisApiService {
            db: self.db.clone(),
            metrics: self.metrics.clone(),
            dns_servers: self.dns_servers.clone(),
            cpu_overcommit_ratio: self.cpu_overcommit_ratio,
            agents: self.agents.clone(),
            pending_ops: self.pending_ops.clone(),
        });

        let agent_svc = basis_agent_server::BasisAgentServer::new(BasisAgentService {
            db: self.db.clone(),
            metrics: self.metrics.clone(),
            reconcile_interval: self.reconcile_interval,
            agents: self.agents.clone(),
            pending_ops: self.pending_ops.clone(),
        });

        (basis_svc, agent_svc)
    }
}

// --- CAPI-facing service ---

struct BasisApiService {
    db: Db,
    metrics: Arc<Metrics>,
    dns_servers: Arc<Vec<String>>,
    cpu_overcommit_ratio: f32,
    agents: Arc<DashMap<String, ConnectedAgent>>,
    pending_ops: Arc<DashMap<String, PendingVmOp>>,
}

impl BasisApiService {
    /// Clean up a VM that failed to create: release IP, delete DB record.
    async fn cleanup_failed_vm(&self, vm_id: &str) {
        if let Err(e) = self.db.release_ips(IpOwner::Vm(vm_id)).await {
            warn!(vm_id, error = %e, "failed to release IP during cleanup");
        }
        if let Err(e) = self.db.delete_vm(vm_id).await {
            warn!(vm_id, error = %e, "failed to delete VM record during cleanup");
        }
    }

    /// Register a pending create or delete and return the receiver its
    /// completion will be signalled on. The handler side
    /// (`handle_agent_message`) keys off the vm_id and routes the
    /// agent's terminal state report to the matching sender.
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

    /// Tear down a single VM: notify its agent and block on the agent's
    /// confirmation. Once we get it, release the IP and delete the DB
    /// row. The synchronous confirmation is what bounds queue depth —
    /// callers (CAPI, smoke.sh) wait on the RPC rather than pipelining
    /// creates behind an unacknowledged delete.
    ///
    /// Transient failure (agent is shedding load, e.g. LVM semaphore
    /// timeout) returns `Unavailable`; the DB row stays so the client's
    /// retry sees the VM still present and tries again. A permanent
    /// failure returns `Internal` for operator attention; the periodic
    /// reconcile still converges on the desired state eventually.
    async fn teardown_vm(&self, vm: &VmRow) -> Result<(), Status> {
        self.db
            .update_vm_state(&vm.id, MachineState::Stopping as i64, "", &now_rfc3339())
            .await
            .map_err(db_status)?;

        let Some(agent) = self.agents.get(&vm.host_id) else {
            // Agent not connected — transient by nature (it'll reconnect
            // and reconcile from `expected_vm_ids`). Surface it so the
            // client retries rather than treating the VM as deleted.
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

        match tokio::time::timeout(DELETE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                if let Err(e) = self.db.release_ips(IpOwner::Vm(&vm.id)).await {
                    warn!(vm_id = %vm.id, error = %e, "failed to release VM IP during teardown");
                }
                self.db.delete_vm(&vm.id).await.map_err(db_status)?;
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
                // Timeout: clear the waiter so a late state report
                // doesn't try to fire a dead oneshot. The DB row stays
                // — the client retries; the periodic reconcile picks up
                // any VM stuck in STOPPING.
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
}

/// Total time `CreateMachine` will wait for the agent to report RUNNING.
///
/// Sized for cold starts on a fresh agent: a node-image pull is ~600 MB
/// plus a qemu-img decompress pass on the qcow2 layer, so the first VM
/// of a given image easily takes several minutes end-to-end. Subsequent
/// VMs hit the cache and return in ~20 s. Clients that want to bail
/// earlier set their own gRPC deadline — this is only the server-side
/// ceiling.
const CREATE_MACHINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Total time `DeleteMachine` / `DeleteCluster` will wait for the agent
/// to confirm teardown. Delete is inherently bounded — the agent's work
/// is `systemctl stop` plus `lvremove`, both of which complete in
/// single-digit seconds on a healthy pool — so 120s is well clear of
/// the normal path while staying short enough that a genuinely stuck
/// delete surfaces to the client as `DeadlineExceeded` and triggers
/// retry rather than hanging indefinitely.
const DELETE_MACHINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

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
            ip_pool = %req.ip_pool,
            "CreateCluster received"
        );
        if req.name.is_empty() || req.ip_pool.is_empty() {
            return Err(Status::invalid_argument("name and ip_pool are required"));
        }

        // Idempotent by name: CAPI reconcilers retry after partial failures
        // (cluster created in basis but status patch never landed in k8s).
        // Returning the existing record lets the reconciler recover on its
        // next pass instead of spinning on AlreadyExists.
        if let Some(existing) = self
            .db
            .get_cluster_by_name(&req.name)
            .await
            .map_err(db_status)?
        {
            info!(
                cluster_id = %existing.id,
                name = %req.name,
                endpoint = %existing.control_plane_endpoint,
                "CreateCluster idempotent return: cluster already exists"
            );
            return Ok(cluster_response(existing));
        }

        let cluster_id = uuid::Uuid::new_v4().to_string();

        // Allocate a VIP from the pool's VIP sub-range before inserting
        // the cluster row so we don't commit a partial cluster on
        // failure. The VIP sub-range is disjoint from the VM auto-
        // allocation range, so this never races a concurrent VM create.
        let control_plane_endpoint = self
            .db
            .allocate_vip(&req.ip_pool, IpOwner::ClusterVip(&cluster_id))
            .await
            .map_err(db_status)?;

        let row = ClusterRow {
            id: cluster_id.clone(),
            name: req.name.clone(),
            ip_pool: req.ip_pool.clone(),
            control_plane_endpoint: control_plane_endpoint.clone(),
            created_at: now_rfc3339(),
        };
        if let Err(e) = self.db.insert_cluster(&row).await {
            // Insert failed — roll back our VIP allocation so we don't
            // leak an IP.
            if let Err(re) = self.db.release_ips(IpOwner::ClusterVip(&cluster_id)).await {
                warn!(cluster_id = %cluster_id, error = %re,
                    "failed to release VIP after cluster insert failure");
            }
            return match e {
                // Narrow race: idempotency check above passed but a
                // concurrent CreateCluster inserted the same name
                // first. Return the winner's row.
                DbError::Conflict(_) => {
                    let existing = self
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
                    Ok(cluster_response(existing))
                }
                other => Err(db_status(other)),
            };
        }

        info!(
            cluster_id = %cluster_id,
            name = %req.name,
            endpoint = %control_plane_endpoint,
            ip_pool = %req.ip_pool,
            "CreateCluster: new cluster provisioned"
        );
        Ok(Response::new(CreateClusterResponse {
            cluster_id,
            control_plane_endpoint,
        }))
    }

    async fn delete_cluster(
        &self,
        request: Request<DeleteClusterRequest>,
    ) -> Result<Response<DeleteClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        info!(cluster_id = %req.cluster_id, "DeleteCluster received");

        // Ensure the cluster exists — returns NotFound otherwise.
        self.db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;

        // Tear down every VM in the cluster first, then release the VIP,
        // then remove the row.
        let vms = self
            .db
            .list_vms(Some(&req.cluster_id))
            .await
            .map_err(db_status)?;
        info!(cluster_id = %req.cluster_id, vm_count = vms.len(), "DeleteCluster: cascading VM deletes");
        // Fan out the teardowns. Each VM's slow path is the agent-side
        // LV / systemd cleanup; serially that's O(N * per-vm), in
        // parallel it's ~max(per-vm) + per-RPC overhead. `try_join_all`
        // short-circuits on the first failure, matching the old
        // `?`-in-a-loop behavior.
        futures::future::try_join_all(vms.iter().map(|vm| self.teardown_vm(vm))).await?;

        if let Err(e) = self
            .db
            .release_ips(IpOwner::ClusterVip(&req.cluster_id))
            .await
        {
            // Cluster row delete still proceeds — operators see the
            // orphaned VIP via the leak rather than via a hung DELETE.
            warn!(
                cluster_id = %req.cluster_id,
                error = %e,
                "failed to release cluster VIP during DeleteCluster"
            );
        }
        self.db
            .delete_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;

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
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(db_status)?;
        Ok(Response::new(cluster_row_to_proto(cluster)))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        require_capi_caller(&request)?;
        let clusters = self.db.list_clusters().await.map_err(db_status)?;
        Ok(Response::new(ListClustersResponse {
            clusters: clusters.into_iter().map(cluster_row_to_proto).collect(),
        }))
    }

    async fn create_machine(
        &self,
        request: Request<CreateMachineRequest>,
    ) -> Result<Response<CreateMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        // Wall-clock start of the create — feeds `vm_create_duration_seconds`
        // at every terminal exit via `record_create_result`. Idempotent
        // fast-returns (existing VM by name, Conflict) intentionally
        // skip the histogram: they don't represent new create work.
        let started = Instant::now();
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

        let cluster = self.db.get_cluster(&req.cluster_id).await.map_err(|e| {
            warn!(
                cluster_id = %req.cluster_id, name = %req.name, error = %e,
                "CreateMachine rejected: cluster not found"
            );
            db_status(e)
        })?;

        // Idempotent by (cluster_id, name): if the CAPI provider retries
        // after a partial failure (VM created on the basis side but the
        // status patch never landed in k8s), return the existing row so
        // the reconciler can recover on the next pass instead of creating
        // a duplicate VM.
        if let Some(existing) = self
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
            return Ok(create_machine_response(existing));
        }

        let vm_id = uuid::Uuid::new_v4().to_string();
        let now = now_rfc3339();

        let (host_id, gpu_devices) = match self.pick_host(&req).await {
            Ok(v) => v,
            Err(status) => {
                // The only path pick_host can return is `ResourceExhausted`
                // (NoCapacity) — DB errors map to Internal/Unavailable and
                // are treated as infrastructure failures, not create outcomes.
                if status.code() == tonic::Code::ResourceExhausted {
                    self.record_create_result("no_capacity", started);
                }
                return Err(status);
            }
        };

        let ip_address = self
            .db
            .allocate_ip(&cluster.ip_pool, IpOwner::Vm(&vm_id))
            .await
            .map_err(db_status)?;

        let ip_pool = self
            .db
            .get_ip_pool(&cluster.ip_pool)
            .await
            .map_err(db_status)?;
        let prefix_len = u32::from(ip_pool.prefix_len().map_err(db_status)?);

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
        // Two failure modes share this rollback:
        //   - `Conflict`: a concurrent CreateMachine inserted the same
        //     (cluster_id, name) first; the UNIQUE index rejects ours,
        //     and we return the winner's row to keep the API idempotent.
        //   - `HostUnavailable`: the scheduled host went unhealthy
        //     between `pick_host` and now. Atomically detected by
        //     `insert_vm`'s `WHERE EXISTS … healthy = 1` predicate.
        if let Err(e) = self.db.insert_vm(&vm).await {
            if let Err(re) = self.db.release_ips(IpOwner::Vm(&vm_id)).await {
                warn!(vm_id = %vm_id, error = %re,
                    "failed to release VM IP after insert failure");
            }
            return match e {
                DbError::Conflict(_) => {
                    let existing = self
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
                    Ok(create_machine_response(existing))
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

        let agent = self.agents.get(&host_id).ok_or_else(|| {
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
                gateway: ip_pool.gateway,
                prefix_len,
                gpus: req.gpus,
                gpu_constraints: req.gpu_constraints,
                dns_servers: self.dns_servers.as_ref().clone(),
                gpu_pci_addresses: gpu_devices.iter().map(|g| g.pci_address.clone()).collect(),
                extra_disks: req.extra_disks,
            })),
        };

        info!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: dispatching CreateVm to agent");
        if agent.command_tx.send(cmd).await.is_err() {
            // Agent stream dropped between the lookup and the send. Roll
            // back the IP and the VM row before returning so we don't
            // leak pool capacity on a flaky link — the next CreateMachine
            // for this name will then start clean.
            self.pending_ops.remove(&vm_id);
            self.cleanup_failed_vm(&vm_id).await;
            self.record_create_result("stream_closed", started);
            warn!(
                vm_id = %vm_id,
                host_id = %host_id,
                "CreateMachine: agent stream closed before command delivered"
            );
            return Err(Status::unavailable("agent stream closed"));
        }

        let result_label: &'static str;
        let response = match tokio::time::timeout(CREATE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                result_label = "placed";
                info!(vm_id = %vm_id, host_id = %host_id, ip = %ip_address, "CreateMachine: agent reported RUNNING");
                Ok(Response::new(CreateMachineResponse {
                    id: vm_id.clone(),
                    provider_id: provider_id(&vm_id),
                    ip_address,
                    host: host_id,
                }))
            }
            Ok(Ok(Err(failure))) => {
                self.cleanup_failed_vm(&vm_id).await;
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
                self.cleanup_failed_vm(&vm_id).await;
                result_label = "agent_error";
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                // Clear the waiter so the agent's eventual state report
                // (handle_agent_message) doesn't try to fire a dead
                // oneshot. Mark the row FAILED with a clear error so the
                // VM doesn't sit in CREATING forever — if the agent
                // succeeds late, its ReportVmState(RUNNING) will overwrite
                // FAILED back to RUNNING; if it never reports, CAPI sees
                // a clear failure and either retries (idempotent by name)
                // or deletes. The IP and DB row are intentionally NOT
                // released: the agent may still be running the VM and
                // we'd be racing it.
                self.pending_ops.remove(&vm_id);
                let timeout_msg = format!(
                    "VM creation timed out ({}s)",
                    CREATE_MACHINE_TIMEOUT.as_secs()
                );
                if let Err(e) = self
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
        let vm = self.db.get_vm(&req.id).await.map_err(db_status)?;
        self.teardown_vm(&vm).await?;
        Ok(Response::new(DeleteMachineResponse {}))
    }

    async fn get_machine(
        &self,
        request: Request<GetMachineRequest>,
    ) -> Result<Response<Machine>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let vm = self.db.get_vm(&req.id).await.map_err(db_status)?;
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

        let vms = self.db.list_vms(cluster).await.map_err(db_status)?;
        Ok(Response::new(ListMachinesResponse {
            machines: vms.iter().map(vm_to_machine).collect(),
        }))
    }
}

impl BasisApiService {
    /// Collect hosts + current GPU assignments, run the scheduler, return the
    /// chosen host and the set of selected GPU devices.
    async fn pick_host(
        &self,
        req: &CreateMachineRequest,
    ) -> Result<(String, Vec<GpuDevice>), Status> {
        let hosts = self.db.list_healthy_hosts().await.map_err(db_status)?;

        // Gather all VMs currently assigned to each healthy host. The
        // scheduler uses this to compute both per-host capacity (totals
        // minus VM allocations) and GPU availability.
        let mut vms_by_host: HashMap<String, Vec<VmRow>> = HashMap::new();
        for host in &hosts {
            let vms = self
                .db
                .list_vms_on_host(&host.id)
                .await
                .map_err(db_status)?;
            vms_by_host.insert(host.id.clone(), vms);
        }

        let sched_req = ScheduleRequest::from(req);
        match scheduler::schedule(&hosts, &vms_by_host, &sched_req, self.cpu_overcommit_ratio) {
            Ok((host_id, gpus)) => {
                self.metrics
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
                self.metrics
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

    /// Record the terminal outcome of a `CreateMachine` call on both the
    /// counter and the latency histogram. Keeping the two emissions in
    /// one helper guarantees they never drift in label or call site.
    fn record_create_result(&self, result: &'static str, started: Instant) {
        self.metrics
            .vm_create_result_total
            .with_label_values(&[result])
            .inc();
        self.metrics
            .vm_create_duration_seconds
            .with_label_values(&[result])
            .observe(started.elapsed().as_secs_f64());
    }
}

// --- Agent-facing service ---

struct BasisAgentService {
    db: Db,
    metrics: Arc<Metrics>,
    reconcile_interval: std::time::Duration,
    agents: Arc<DashMap<String, ConnectedAgent>>,
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
        // Capture the peer identity before consuming the request.
        let peer_id = peer_identity(&request)?;

        // Belt-and-suspenders: the CAPI provider identity would only slip
        // through the hostname check below if someone literally registered
        // a host named `basis-capi-provider`. Reject it explicitly so the
        // agent stream is unreachable to that identity even under that
        // accident.
        if peer_id == tls::CAPI_PROVIDER_IDENTITY {
            return Err(Status::permission_denied(format!(
                "peer identity '{peer_id}' is not authorized for agent RPCs"
            )));
        }

        let mut inbound = request.into_inner();

        // First message must be a RegisterHost
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

        // Agent's peer identity must match the hostname it's registering as.
        if peer_id != register.hostname {
            return Err(Status::permission_denied(format!(
                "agent identity '{peer_id}' does not match registered hostname '{}'",
                register.hostname
            )));
        }

        // Check if host already registered
        let host_id = match self.db.get_host_by_hostname(&register.hostname).await {
            Ok(Some(existing)) => {
                info!(host_id = %existing.id, hostname = %register.hostname, "agent reconnected");
                existing.id
            }
            Ok(None) => {
                let host_id = uuid::Uuid::new_v4().to_string();
                let gpu_json = serde_json::to_string(&register.gpus)
                    .expect("serializing Vec<GpuDevice> to JSON is infallible");

                let host = crate::db::HostRow {
                    id: host_id.clone(),
                    hostname: register.hostname.clone(),
                    total_cpu: register.total_cpu as i64,
                    total_memory_mib: register.total_memory_mib as i64,
                    total_disk_gib: register.total_disk_gib as i64,
                    gpu_inventory: gpu_json,
                    last_heartbeat: now_rfc3339(),
                    healthy: true,
                };
                self.db
                    .upsert_host(&host)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                info!(host_id = %host_id, hostname = %register.hostname, "new host registered");
                host_id
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        // Set up command channel
        let (command_tx, command_rx) = mpsc::channel::<ControllerCommand>(32);

        // Authoritative VM list for this host — agent uses this to drop any
        // local VMs the controller has forgotten.
        let expected_vm_ids: Vec<String> = self
            .db
            .list_vms_on_host(&host_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|vm| vm.id)
            .collect();

        command_tx
            .send(ControllerCommand {
                request_id: String::new(),
                command: Some(controller_command::Command::RegisterAck(
                    RegisterHostResponse {
                        host_id: host_id.clone(),
                        expected_vm_ids,
                    },
                )),
            })
            .await
            .map_err(|_| Status::internal("failed to send registration ack"))?;

        self.agents.insert(
            host_id.clone(),
            ConnectedAgent {
                command_tx: command_tx.clone(),
            },
        );
        self.metrics
            .agent_connected
            .with_label_values(&[&register.hostname])
            .set(1);

        // Periodic controller→agent authoritative VM list. Runs for as
        // long as the stream is alive; send fails when the receiver side
        // drops (client disconnect or inbound handler removes the agent),
        // which is how this task terminates.
        let reconcile_db = self.db.clone();
        let reconcile_host_id = host_id.clone();
        let reconcile_interval = self.reconcile_interval;
        let reconcile_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reconcile_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // First tick fires immediately — skip it. The initial list was
            // already sent inside `RegisterHostResponse` at handshake.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let expected = match reconcile_db.list_vms_on_host(&reconcile_host_id).await {
                    Ok(vms) => vms.into_iter().map(|vm| vm.id).collect::<Vec<_>>(),
                    Err(e) => {
                        warn!(error = %e, host_id = %reconcile_host_id, "reconcile list_vms_on_host failed");
                        continue;
                    }
                };
                let cmd = ControllerCommand {
                    request_id: String::new(),
                    command: Some(controller_command::Command::ReconcileHost(
                        ReconcileHostCommand {
                            expected_vm_ids: expected,
                        },
                    )),
                };
                if command_tx.send(cmd).await.is_err() {
                    // Receiver dropped — agent stream is gone.
                    break;
                }
            }
        });

        // Spawn task to process inbound agent messages
        let db = self.db.clone();
        let agents = self.agents.clone();
        let pending_ops = self.pending_ops.clone();
        let metrics = self.metrics.clone();
        let agent_host_id = host_id.clone();
        let agent_hostname = register.hostname.clone();

        let inbound_metrics = metrics.clone();
        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Err(e) = handle_agent_message(
                            &db,
                            &pending_ops,
                            &inbound_metrics,
                            &agent_host_id,
                            msg,
                        )
                        .await
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
            agents.remove(&agent_host_id);
            reconcile_handle.abort();
            // Drop every pending VM op (create or delete) routed to
            // this host so the awaiting RPC returns immediately via
            // its "agent disconnected" branch — for creates that
            // releases the IP and VM row; for deletes that surfaces
            // `Unavailable` so the client retries. Removing the entry
            // drops the oneshot Sender, which wakes the receiver with
            // RecvError.
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
            metrics
                .agent_connected
                .with_label_values(&[&agent_hostname])
                .set(0);
        });

        let output = ReceiverStream::new(command_rx).map(Ok);
        Ok(Response::new(Box::pin(output)))
    }
}

async fn handle_agent_message(
    db: &Db,
    pending_ops: &DashMap<String, PendingVmOp>,
    metrics: &Metrics,
    host_id: &str,
    msg: AgentMessage,
) -> anyhow::Result<()> {
    match msg.payload {
        Some(agent_message::Payload::Heartbeat(hb)) => {
            db.update_host_heartbeat(&hb.host_id, &now_rfc3339())
                .await?;
        }
        Some(agent_message::Payload::VmState(report)) => {
            let state = report.state();
            let now = now_rfc3339();

            // Peek at the current row before the update so we can detect
            // the edge into RUNNING and observe `vm_time_to_running_seconds`
            // exactly once per VM (idempotent re-reports of RUNNING after
            // the first one must not re-observe).
            let prev = db.get_vm(&report.vm_id).await.ok();

            db.update_vm_state(&report.vm_id, state as i64, &report.error_message, &now)
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
                                metrics
                                    .vm_time_to_running_seconds
                                    .observe(elapsed.as_secs_f64());
                            }
                        }
                    }
                }
            }

            // Resolve the pending op — if any — for this vm_id. A
            // state only resolves a pending op if it matches:
            //   * Running resolves a pending Create
            //   * Stopped resolves a pending Delete
            //   * Failed resolves either
            // Other combinations are background noise from
            // `report_local_vm_states` on the agent (which walks every
            // tracked VM on a timer and reports state=Running for it,
            // even while a Delete is mid-flight between DeleteVm
            // dispatch and the agent's row-first `agent_db.delete_vm`).
            // Treating that noise as a mismatch would produce spurious
            // DeleteMachine failures. Peek the kind before removing so
            // a non-matching state leaves the waiter intact.
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

/// CAPI-shaped provider ID for a VM.
///
/// This value has to match what the kubelet inside the guest ends up
/// reporting on `Node.spec.providerID`. Lattice templates
/// `provider-id=basis://{{ ds.meta_data.instance_id }}` into kubeadm's
/// kubelet args, and basis-agent writes `instance-id: <vm_id>` into
/// cloud-init's meta-data — so the VM's reported providerID is
/// `basis://<vm_id>`. We return the same here. If the two ever drift,
/// CAPI's Machine reconciler never binds a NodeRef and the control
/// plane never comes up.
fn provider_id(vm_id: &str) -> String {
    format!("basis://{vm_id}")
}

fn cluster_row_to_proto(row: ClusterRow) -> Cluster {
    Cluster {
        cluster_id: row.id,
        name: row.name,
        ip_pool: row.ip_pool,
        control_plane_endpoint: row.control_plane_endpoint,
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

    // `vms.cpu/memory_mib/disk_gib` originate from u32-typed proto
    // fields and are CHECK-bounded by sqlite (see migration). A panic
    // here would mean the DB was hand-edited to an out-of-range value;
    // fail loudly rather than silently truncating the response.
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
    }
}
