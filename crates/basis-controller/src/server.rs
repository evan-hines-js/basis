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

use basis_common::gpu::GpuInfo;
use basis_common::time::now_rfc3339;
use basis_common::tls;

use crate::db::{ClusterRow, Db, IpOwner, VmRow};
use crate::scheduler::{self, ScheduleRequest};

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
        Err(e) => Err(Status::unauthenticated(format!(
            "peer certificate: {e}"
        ))),
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
    agents: Arc<DashMap<String, ConnectedAgent>>,
    pending_creates: Arc<DashMap<String, PendingCreate>>,
}

impl BasisServer {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            agents: Arc::new(DashMap::new()),
            pending_creates: Arc::new(DashMap::new()),
        }
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
            agents: self.agents.clone(),
            pending_creates: self.pending_creates.clone(),
        });

        let agent_svc = basis_agent_server::BasisAgentServer::new(BasisAgentService {
            db: self.db.clone(),
            agents: self.agents.clone(),
            pending_creates: self.pending_creates.clone(),
        });

        (basis_svc, agent_svc)
    }
}

// --- CAPI-facing service ---

struct BasisApiService {
    db: Db,
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
const CREATE_MACHINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[tonic::async_trait]
impl basis_server::Basis for BasisApiService {
    async fn create_cluster(
        &self,
        request: Request<CreateClusterRequest>,
    ) -> Result<Response<CreateClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
        if req.name.is_empty() || req.ip_pool.is_empty() {
            return Err(Status::invalid_argument(
                "name and ip_pool are required",
            ));
        }

        let cluster_id = uuid::Uuid::new_v4().to_string();

        // Reserve the control-plane VIP from the pool before inserting the
        // cluster row so we don't commit a partial cluster on failure.
        let vip = self
            .db
            .allocate_ip(&req.ip_pool, IpOwner::ClusterVip(&cluster_id))
            .await
            .map_err(|e| Status::resource_exhausted(e.to_string()))?;

        let row = ClusterRow {
            id: cluster_id.clone(),
            name: req.name.clone(),
            ip_pool: req.ip_pool.clone(),
            control_plane_endpoint: vip.clone(),
            created_at: now_rfc3339(),
        };
        if let Err(e) = self.db.insert_cluster(&row).await {
            // Insert failed — roll back the VIP so we don't leak an IP.
            let _ = self.db.release_ips(IpOwner::ClusterVip(&cluster_id)).await;
            return Err(match e {
                crate::db::DbError::Conflict(msg) => Status::already_exists(msg),
                other => Status::internal(other.to_string()),
            });
        }

        Ok(Response::new(CreateClusterResponse {
            cluster_id,
            control_plane_endpoint: vip,
        }))
    }

    async fn delete_cluster(
        &self,
        request: Request<DeleteClusterRequest>,
    ) -> Result<Response<DeleteClusterResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();

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
        Ok(Response::new(Cluster {
            cluster_id: cluster.id,
            name: cluster.name,
            ip_pool: cluster.ip_pool,
            control_plane_endpoint: cluster.control_plane_endpoint,
        }))
    }

    async fn create_machine(
        &self,
        request: Request<CreateMachineRequest>,
    ) -> Result<Response<CreateMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();

        let cluster = self
            .db
            .get_cluster(&req.cluster_id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

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

        let gpu_json = serde_json::to_string(&gpu_devices).unwrap_or_else(|_| "[]".to_string());
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
        self.db
            .insert_vm(&vm)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let agent = self.agents.get(&host_id).ok_or_else(|| {
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
                dns_servers: vec!["8.8.8.8".to_string(), "8.8.4.4".to_string()],
                gpu_pci_addresses: gpu_devices.iter().map(|g| g.pci_address.clone()).collect(),
            })),
        };

        agent.command_tx.send(cmd).await.map_err(|_| {
            self.pending_creates.remove(&vm_id);
            Status::unavailable("agent stream closed")
        })?;

        match tokio::time::timeout(CREATE_MACHINE_TIMEOUT, wait_rx).await {
            Ok(Ok(Ok(()))) => {
                let provider_id = format!("basis://{host_id}/{vm_id}");
                Ok(Response::new(CreateMachineResponse {
                    id: vm_id,
                    provider_id,
                    ip_address,
                    host: host_id,
                }))
            }
            Ok(Ok(Err(err))) => {
                self.cleanup_failed_vm(&vm_id).await;
                Err(Status::internal(format!("VM creation failed: {err}")))
            }
            Ok(Err(_)) => {
                self.cleanup_failed_vm(&vm_id).await;
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                // Leave the VM record in place — the agent may still finish —
                // but clear the waiter so the eventual report updates state
                // rather than a dead oneshot.
                self.pending_creates.remove(&vm_id);
                Err(Status::deadline_exceeded(format!(
                    "VM creation timed out ({}s)",
                    CREATE_MACHINE_TIMEOUT.as_secs()
                )))
            }
        }
    }

    async fn delete_machine(
        &self,
        request: Request<DeleteMachineRequest>,
    ) -> Result<Response<DeleteMachineResponse>, Status> {
        require_capi_caller(&request)?;
        let req = request.into_inner();
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

        let mut assigned_gpus: HashMap<String, Vec<String>> = HashMap::new();
        for host in &hosts {
            let vms = self
                .db
                .list_vms_on_host(&host.id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut gpus = Vec::new();
            for vm in &vms {
                let gpu_devs: Vec<GpuInfo> =
                    serde_json::from_str(&vm.gpu_assignments).unwrap_or_default();
                gpus.extend(gpu_devs.into_iter().map(|g| g.pci_address));
            }
            assigned_gpus.insert(host.id.clone(), gpus);
        }

        let sched_req = ScheduleRequest::from(req);
        scheduler::schedule(&hosts, &assigned_gpus, &sched_req)
            .map_err(|e| Status::resource_exhausted(e.to_string()))
    }
}

// --- Agent-facing service ---

struct BasisAgentService {
    db: Db,
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

        let mut inbound = request.into_inner();

        // First message must be a RegisterHost
        let first = inbound
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty stream"))?
            .map_err(|e| Status::internal(e.to_string()))?;

        let register = match first.payload {
            Some(agent_message::Payload::Register(r)) => r,
            _ => return Err(Status::invalid_argument("first message must be RegisterHost")),
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
                let gpu_json =
                    serde_json::to_string(&register.gpus).unwrap_or_else(|_| "[]".to_string());

                let host = crate::db::HostRow {
                    id: host_id.clone(),
                    hostname: register.hostname.clone(),
                    address: String::new(),
                    total_cpu: register.total_cpu as i64,
                    total_memory_mib: register.total_memory_mib as i64,
                    total_disk_gib: register.total_disk_gib as i64,
                    available_cpu: register.total_cpu as i64,
                    available_memory_mib: register.total_memory_mib as i64,
                    available_disk_gib: register.total_disk_gib as i64,
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
            ConnectedAgent { command_tx },
        );

        // Spawn task to process inbound agent messages
        let db = self.db.clone();
        let agents = self.agents.clone();
        let pending_creates = self.pending_creates.clone();
        let agent_host_id = host_id.clone();

        tokio::spawn(async move {
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Err(e) = handle_agent_message(
                            &db,
                            &pending_creates,
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
            db.update_host_heartbeat(
                &hb.host_id,
                hb.available_cpu as i64,
                hb.available_memory_mib as i64,
                hb.available_disk_gib as i64,
                &now_rfc3339(),
            )
            .await?;
        }
        Some(agent_message::Payload::VmState(report)) => {
            let state = report.state();
            let now = now_rfc3339();

            db.update_vm_state(
                &report.vm_id,
                state as i64,
                &report.error_message,
                &now,
            )
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

fn vm_to_machine(vm: &VmRow) -> Machine {
    let gpu_devices: Vec<GpuDevice> =
        serde_json::from_str(&vm.gpu_assignments).unwrap_or_default();

    Machine {
        id: vm.id.clone(),
        name: vm.name.clone(),
        cluster_id: vm.cluster_id.clone(),
        host: vm.host_id.clone(),
        provider_id: format!("basis://{}/{}", vm.host_id, vm.id),
        ip_address: vm.ip_address.clone(),
        state: vm.state as i32,
        cpu: vm.cpu as u32,
        memory_mib: vm.memory_mib as u32,
        disk_gib: vm.disk_gib as u32,
        gpus: gpu_devices,
    }
}
