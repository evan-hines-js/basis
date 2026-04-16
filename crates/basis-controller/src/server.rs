use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use basis_proto::*;
use dashmap::DashMap;
use futures::Stream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::config::ControllerConfig;
use crate::db::{Db, VmRow};
use crate::host::now_rfc3339;
use crate::scheduler::{self, GpuInfo, ScheduleRequest};

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

    pub async fn serve(
        self,
        addr: SocketAddr,
        config: &ControllerConfig,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let cert_pem = std::fs::read_to_string(&config.tls.cert)?;
        let key_pem = std::fs::read_to_string(&config.tls.key)?;
        let ca_pem = std::fs::read_to_string(&config.tls.ca)?;

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert_pem, &key_pem))
            .client_ca_root(Certificate::from_pem(&ca_pem));

        let (basis_svc, agent_svc) = self.into_services();

        info!(%addr, "starting gRPC server");

        Server::builder()
            .tls_config(tls_config)?
            .concurrency_limit_per_connection(64)
            .layer(tower::limit::ConcurrencyLimitLayer::new(256))
            .add_service(basis_svc)
            .add_service(agent_svc)
            .serve_with_shutdown(addr, shutdown.cancelled())
            .await?;

        Ok(())
    }

    /// Start without TLS on a random port. Returns the actual address.
    /// Used for integration tests.
    pub async fn serve_insecure(
        self,
        shutdown: CancellationToken,
    ) -> anyhow::Result<std::net::SocketAddr> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (basis_svc, agent_svc) = self.into_services();

        info!(%addr, "starting insecure gRPC server (test mode)");

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        tokio::spawn(async move {
            Server::builder()
                .add_service(basis_svc)
                .add_service(agent_svc)
                .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
                .await
                .ok();
        });

        Ok(addr)
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
        if let Err(e) = self.db.release_ip(vm_id).await {
            warn!(vm_id, error = %e, "failed to release IP during cleanup");
        }
        if let Err(e) = self.db.delete_vm(vm_id).await {
            warn!(vm_id, error = %e, "failed to delete VM record during cleanup");
        }
    }
}

#[tonic::async_trait]
impl basis_server::Basis for BasisApiService {
    async fn create_machine(
        &self,
        request: Request<CreateMachineRequest>,
    ) -> Result<Response<CreateMachineResponse>, Status> {
        let req = request.into_inner();
        let vm_id = uuid::Uuid::new_v4().to_string();
        let now = now_rfc3339();

        // Schedule: pick a host
        let hosts = self
            .db
            .list_healthy_hosts()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Build map of GPU assignments per host from existing VMs
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

        let sched_req = ScheduleRequest::from(&req);
        let (host_id, gpu_devices) =
            scheduler::schedule(&hosts, &assigned_gpus, &sched_req)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;

        // Allocate IP
        let pool_name = if req.ip_pool.is_empty() {
            "default"
        } else {
            &req.ip_pool
        };
        let ip_address = self
            .db
            .allocate_ip(pool_name, &vm_id)
            .await
            .map_err(|e| Status::resource_exhausted(e.to_string()))?;

        let ip_pool = self
            .db
            .get_ip_pool(pool_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Parse CIDR to get prefix length
        let prefix_len = ip_pool
            .cidr
            .split('/')
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(24);

        // Insert VM record
        let gpu_json = serde_json::to_string(&gpu_devices).unwrap_or_else(|_| "[]".to_string());
        let vm = VmRow {
            id: vm_id.clone(),
            name: req.name.clone(),
            cluster: req.cluster.clone(),
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

        // Send CreateVM command to the agent
        let agent = self
            .agents
            .get(&host_id)
            .ok_or_else(|| Status::unavailable(format!("agent for host '{host_id}' not connected")))?;

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

        agent
            .command_tx
            .send(cmd)
            .await
            .map_err(|_| {
                // Agent disconnected before we could send — clean up
                self.pending_creates.remove(&vm_id);
                Status::unavailable("agent stream closed")
            })?;

        // Wait for agent to report VM running (or timeout)
        let result = tokio::time::timeout(std::time::Duration::from_secs(60), wait_rx).await;

        match result {
            Ok(Ok(Ok(()))) => {
                // VM is running
                let provider_id = format!("basis://{host_id}/{vm_id}");
                Ok(Response::new(CreateMachineResponse {
                    id: vm_id,
                    provider_id,
                    ip_address,
                    host: host_id,
                }))
            }
            Ok(Ok(Err(err))) => {
                // Agent reported FAILED — clean up leaked resources
                self.cleanup_failed_vm(&vm_id).await;
                Err(Status::internal(format!("VM creation failed: {err}")))
            }
            Ok(Err(_)) => {
                // oneshot cancelled (agent disconnected mid-create)
                self.cleanup_failed_vm(&vm_id).await;
                Err(Status::internal("agent disconnected during VM creation"))
            }
            Err(_) => {
                // Timeout — leave VM record (agent may still be working) but
                // remove the pending waiter so the agent's eventual report
                // still updates state correctly
                self.pending_creates.remove(&vm_id);
                Err(Status::deadline_exceeded("VM creation timed out (60s)"))
            }
        }
    }

    async fn delete_machine(
        &self,
        request: Request<DeleteMachineRequest>,
    ) -> Result<Response<DeleteMachineResponse>, Status> {
        let req = request.into_inner();
        let vm = self
            .db
            .get_vm(&req.id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

        // Update state to STOPPING
        self.db
            .update_vm_state(&vm.id, MachineState::Stopping as i64, "", &now_rfc3339())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Send delete command to agent
        if let Some(agent) = self.agents.get(&vm.host_id) {
            let cmd = ControllerCommand {
                request_id: vm.id.clone(),
                command: Some(controller_command::Command::DeleteVm(DeleteVmCommand {
                    vm_id: vm.id.clone(),
                })),
            };
            let _ = agent.command_tx.send(cmd).await;
        }

        // Release IP and delete VM record
        let _ = self.db.release_ip(&vm.id).await;
        self.db
            .delete_vm(&vm.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteMachineResponse {}))
    }

    async fn get_machine(
        &self,
        request: Request<GetMachineRequest>,
    ) -> Result<Response<Machine>, Status> {
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
        let req = request.into_inner();
        let cluster = if req.cluster.is_empty() {
            None
        } else {
            Some(req.cluster.as_str())
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

        // Send registration ack so the agent knows its controller-assigned host_id
        command_tx
            .send(ControllerCommand {
                request_id: String::new(),
                command: Some(controller_command::Command::RegisterAck(
                    RegisterHostResponse {
                        host_id: host_id.clone(),
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
        cluster: vm.cluster.clone(),
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
