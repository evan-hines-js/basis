use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

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

use crate::db::{ClusterRow, Db, IpOwner, VmRow};
use crate::metrics::Metrics;
use crate::scheduler::{self, ScheduleRequest, SchedulerError};

/// Fixed CN required for connections from the CAPI provider.
pub const CAPI_PROVIDER_CN: &str = "basis-capi-provider";

/// Require that a CAPI-facing RPC was issued by a client whose peer cert
/// CN is [`CAPI_PROVIDER_CN`]. Rejects any other CN, including a missing one.
fn require_capi_caller<T>(req: &Request<T>) -> Result<(), Status> {
    let cn = peer_cn(req)?;
    if cn == CAPI_PROVIDER_CN {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "CN '{cn}' is not authorized for CAPI RPCs (expected '{CAPI_PROVIDER_CN}')"
        )))
    }
}

/// Extract the peer CN from the request, treating anything less as an
/// authentication failure. The server is always TLS-terminated so the only
/// reason this returns an error is misconfiguration or a missing cert.
fn peer_cn<T>(req: &Request<T>) -> Result<String, Status> {
    match tls::request_peer_cn(req) {
        Ok(Some(cn)) => Ok(cn),
        Ok(None) => Err(Status::unauthenticated("TLS required")),
        Err(e) => Err(Status::unauthenticated(format!("peer certificate: {e}"))),
    }
}

/// Pending create request waiting for the agent to report VM state.
struct PendingCreate {
    tx: oneshot::Sender<Result<(), String>>,
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
    pending_creates: Arc<DashMap<String, PendingCreate>>,
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
            pending_creates: Arc::new(DashMap::new()),
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
            pending_creates: self.pending_creates.clone(),
        });

        let agent_svc = basis_agent_server::BasisAgentServer::new(BasisAgentService {
            db: self.db.clone(),
            metrics: self.metrics.clone(),
            reconcile_interval: self.reconcile_interval,
            agents: self.agents.clone(),
            pending_creates: self.pending_creates.clone(),
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
    pending_creates: Arc<DashMap<String, PendingCreate>>,
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

    /// Send a DeleteVm command to the owning host's agent if connected.
    /// Best-effort: an offline agent will discover the deletion via the
    /// authoritative VM list on its next registration.
    async fn notify_agent_delete(&self, host_id: &str, vm_id: &str) {
        if let Some(agent) = self.agents.get(host_id) {
            let cmd = ControllerCommand {
                request_id: vm_id.to_string(),
                command: Some(controller_command::Command::DeleteVm(DeleteVmCommand {
                    vm_id: vm_id.to_string(),
                })),
            };
            let _ = agent.command_tx.send(cmd).await;
        }
    }

    /// Tear down a single VM: notify its agent, release its IP, delete its
    /// DB row. Called by both `delete_machine` and cluster deletion.
    async fn teardown_vm(&self, vm: &VmRow) -> Result<(), Status> {
        self.db
            .update_vm_state(&vm.id, MachineState::Stopping as i64, "", &now_rfc3339())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.notify_agent_delete(&vm.host_id, &vm.id).await;

        let _ = self.db.release_ips(IpOwner::Vm(&vm.id)).await;
        self.db
            .delete_vm(&vm.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(())
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
            .map_err(|e| Status::internal(e.to_string()))?
        {
            info!(
                cluster_id = %existing.id,
                name = %req.name,
                endpoint = %existing.control_plane_endpoint,
                "CreateCluster idempotent return: cluster already exists"
            );
            return Ok(Response::new(CreateClusterResponse {
                cluster_id: existing.id,
                control_plane_endpoint: existing.control_plane_endpoint,
            }));
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
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

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
            let _ = self.db.release_ips(IpOwner::ClusterVip(&cluster_id)).await;
            return match e {
                // Narrow race: idempotency check above passed but a
                // concurrent CreateCluster inserted the same name
                // first. Return the winner's row.
                crate::db::DbError::Conflict(_) => {
                    let existing = self
                        .db
                        .get_cluster_by_name(&req.name)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?
                        .ok_or_else(|| {
                            Status::internal(
                                "cluster insert rejected as duplicate but row not found",
                            )
                        })?;
                    Ok(Response::new(CreateClusterResponse {
                        cluster_id: existing.id,
                        control_plane_endpoint: existing.control_plane_endpoint,
                    }))
                }
                other => Err(Status::internal(other.to_string())),
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
            .map_err(|e| Status::not_found(e.to_string()))?;

        // Tear down every VM in the cluster first, then release the VIP,
        // then remove the row.
        let vms = self
            .db
            .list_vms(Some(&req.cluster_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        info!(cluster_id = %req.cluster_id, vm_count = vms.len(), "DeleteCluster: cascading VM deletes");
        for vm in &vms {
            self.teardown_vm(vm).await?;
        }

        let _ = self
            .db
            .release_ips(IpOwner::ClusterVip(&req.cluster_id))
            .await;
        self.db
            .delete_cluster(&req.cluster_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(cluster_row_to_proto(cluster)))
    }

    async fn list_clusters(
        &self,
        request: Request<ListClustersRequest>,
    ) -> Result<Response<ListClustersResponse>, Status> {
        require_capi_caller(&request)?;
        let clusters = self
            .db
            .list_clusters()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(|e| {
                warn!(cluster_id = %req.cluster_id, name = %req.name, error = %e, "CreateMachine rejected: cluster not found");
                Status::not_found(e.to_string())
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
            .map_err(|e| Status::internal(e.to_string()))?
        {
            info!(
                vm_id = %existing.id,
                cluster_id = %req.cluster_id,
                name = %req.name,
                host_id = %existing.host_id,
                "CreateMachine idempotent return: VM already exists"
            );
            return Ok(Response::new(CreateMachineResponse {
                id: existing.id.clone(),
                provider_id: provider_id(&existing.id),
                ip_address: existing.ip_address,
                host: existing.host_id,
            }));
        }

        let vm_id = uuid::Uuid::new_v4().to_string();
        let now = now_rfc3339();

        let (host_id, gpu_devices) = self.pick_host(&req).await?;

        let ip_address = self
            .db
            .allocate_ip(&cluster.ip_pool, IpOwner::Vm(&vm_id))
            .await
            .map_err(|e| Status::resource_exhausted(e.to_string()))?;

        let ip_pool = self
            .db
            .get_ip_pool(&cluster.ip_pool)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let prefix_len = ip_pool
            .cidr
            .split('/')
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(24);

        let gpu_json = serde_json::to_string(&gpu_devices)
            .expect("serializing Vec<GpuDevice> to JSON is infallible");
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
            image: req.image.clone(),
            error_message: String::new(),
            created_at: now.clone(),
            updated_at: now,
        };
        // Narrow race: idempotency check passed but a concurrent
        // CreateMachine inserted the same `(cluster_id, name)` first. The
        // UNIQUE index rejects our insert — roll back our IP and return
        // the winner's row instead of creating a duplicate.
        if let Err(e) = self.db.insert_vm(&vm).await {
            let _ = self.db.release_ips(IpOwner::Vm(&vm_id)).await;
            return match e {
                crate::db::DbError::Conflict(_) => {
                    let existing = self
                        .db
                        .get_vm_by_name(&req.cluster_id, &req.name)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?
                        .ok_or_else(|| {
                            Status::internal("VM insert rejected as duplicate but row not found")
                        })?;
                    Ok(Response::new(CreateMachineResponse {
                        id: existing.id.clone(),
                        provider_id: provider_id(&existing.id),
                        ip_address: existing.ip_address,
                        host: existing.host_id,
                    }))
                }
                other => Err(Status::internal(other.to_string())),
            };
        }

        let agent = self.agents.get(&host_id).ok_or_else(|| {
            self.metrics
                .vm_create_result_total
                .with_label_values(&["no_agent"])
                .inc();
            warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: scheduled host has no connected agent");
            Status::unavailable(format!("agent for host '{host_id}' not connected"))
        })?;

        let (wait_tx, wait_rx) = oneshot::channel();
        self.pending_creates
            .insert(vm_id.clone(), PendingCreate { tx: wait_tx });

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
            })),
        };

        info!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: dispatching CreateVm to agent");
        agent.command_tx.send(cmd).await.map_err(|_| {
            self.pending_creates.remove(&vm_id);
            self.metrics
                .vm_create_result_total
                .with_label_values(&["stream_closed"])
                .inc();
            warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: agent stream closed before command delivered");
            Status::unavailable("agent stream closed")
        })?;

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
            Ok(Ok(Err(err))) => {
                warn!(vm_id = %vm_id, host_id = %host_id, error = %err, "CreateMachine: agent reported FAILED");
                self.cleanup_failed_vm(&vm_id).await;
                result_label = "vm_failed";
                Err(Status::internal(format!("VM creation failed: {err}")))
            }
            Ok(Err(_)) => {
                warn!(vm_id = %vm_id, host_id = %host_id, "CreateMachine: agent disconnected during VM creation");
                self.cleanup_failed_vm(&vm_id).await;
                result_label = "agent_error";
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                // Leave the VM record in place — the agent may still finish —
                // but clear the waiter so the eventual report updates state
                // rather than a dead oneshot.
                self.pending_creates.remove(&vm_id);
                result_label = "timeout";
                warn!(
                    vm_id = %vm_id,
                    host_id = %host_id,
                    timeout_s = CREATE_MACHINE_TIMEOUT.as_secs(),
                    "CreateMachine: timed out waiting for agent to report RUNNING"
                );
                Err(Status::deadline_exceeded(format!(
                    "VM creation timed out ({}s)",
                    CREATE_MACHINE_TIMEOUT.as_secs()
                )))
            }
        };
        self.metrics
            .vm_create_result_total
            .with_label_values(&[result_label])
            .inc();
        response
    }

    async fn delete_machine(
        &self,
        request: Request<DeleteMachineRequest>,
    ) -> Result<Response<DeleteMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        info!(vm_id = %req.id, "DeleteMachine received");
        let vm = self
            .db
            .get_vm(&req.id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        self.teardown_vm(&vm).await?;
        Ok(Response::new(DeleteMachineResponse {}))
    }

    async fn get_machine(
        &self,
        request: Request<GetMachineRequest>,
    ) -> Result<Response<Machine>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        let vm = self
            .db
            .get_vm(&req.id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

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

        let vms = self
            .db
            .list_vms(cluster)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
        let hosts = self
            .db
            .list_healthy_hosts()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Gather all VMs currently assigned to each healthy host. The
        // scheduler uses this to compute both per-host capacity (totals
        // minus VM allocations) and GPU availability.
        let mut vms_by_host: HashMap<String, Vec<VmRow>> = HashMap::new();
        for host in &hosts {
            let vms = self
                .db
                .list_vms_on_host(&host.id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
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
                self.metrics
                    .vm_create_result_total
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
}

// --- Agent-facing service ---

struct BasisAgentService {
    db: Db,
    metrics: Arc<Metrics>,
    reconcile_interval: std::time::Duration,
    agents: Arc<DashMap<String, ConnectedAgent>>,
    pending_creates: Arc<DashMap<String, PendingCreate>>,
}

#[tonic::async_trait]
impl basis_agent_server::BasisAgent for BasisAgentService {
    type StreamMessagesStream =
        Pin<Box<dyn Stream<Item = Result<ControllerCommand, Status>> + Send + 'static>>;

    async fn stream_messages(
        &self,
        request: Request<Streaming<AgentMessage>>,
    ) -> Result<Response<Self::StreamMessagesStream>, Status> {
        // Capture the peer CN before consuming the request.
        let peer_cn = peer_cn(&request)?;

        // Belt-and-suspenders: the CAPI provider's CN would only slip
        // through the hostname check below if someone literally registered
        // a host named `basis-capi-provider`. Reject it explicitly so the
        // agent stream is unreachable to that identity even under that
        // accident.
        if peer_cn == CAPI_PROVIDER_CN {
            return Err(Status::permission_denied(format!(
                "CN '{peer_cn}' is not authorized for agent RPCs"
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

        // Agent's cert CN must match the hostname it's registering as.
        if peer_cn != register.hostname {
            return Err(Status::permission_denied(format!(
                "agent CN '{peer_cn}' does not match registered hostname '{}'",
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
        let pending_creates = self.pending_creates.clone();
        let metrics = self.metrics.clone();
        let agent_host_id = host_id.clone();
        let agent_hostname = register.hostname.clone();

        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Err(e) =
                            handle_agent_message(&db, &pending_creates, &agent_host_id, msg).await
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
    pending_creates: &DashMap<String, PendingCreate>,
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

            db.update_vm_state(&report.vm_id, state as i64, &report.error_message, &now)
                .await?;

            // If this was a pending create, notify the waiter
            if state == MachineState::Running || state == MachineState::Failed {
                if let Some((_, pending)) = pending_creates.remove(&report.vm_id) {
                    let result = if state == MachineState::Running {
                        Ok(())
                    } else {
                        Err(report.error_message.clone())
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

    Machine {
        id: vm.id.clone(),
        name: vm.name.clone(),
        cluster_id: vm.cluster_id.clone(),
        host: vm.host_id.clone(),
        provider_id: provider_id(&vm.id),
        ip_address: vm.ip_address.clone(),
        state: vm.state as i32,
        cpu: vm.cpu as u32,
        memory_mib: vm.memory_mib as u32,
        disk_gib: vm.disk_gib as u32,
        gpus,
    }
}
