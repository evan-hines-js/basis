use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basis_proto::*;
use clap::Parser;
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use basis_agent::config::AgentConfig;
use basis_agent::db::{AgentDb, LocalVmRow};
use basis_agent::gpu;
use basis_agent::gpu::GpuInventoryItem;
use basis_agent::image::ImageManager;
use basis_agent::network::NetworkManager;
use basis_agent::reconcile;
use basis_agent::vm::VmManager;

#[derive(Parser)]
#[command(name = "basis-agent", about = "Basis hypervisor agent")]
struct Cli {
    #[arg(short, long, default_value = "/etc/basis/agent.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("basis=info".parse().unwrap()))
        .init();

    let cli = Cli::parse();
    let config = AgentConfig::load(&cli.config)?;
    info!(
        controller = %config.controller_endpoint,
        data_dir = %config.data_dir.display(),
        "loaded config"
    );

    std::fs::create_dir_all(&config.data_dir)?;
    std::fs::create_dir_all(config.images_dir())?;
    std::fs::create_dir_all(config.vms_dir())?;

    // Open local state database
    let agent_db = AgentDb::open(&config.data_dir.join("agent.db")).await?;
    info!("agent database ready");

    let network_mgr = NetworkManager::new(
        config.network.bridge.clone(),
        config.network.physical_nic.clone(),
    );
    network_mgr.ensure_bridge().await?;

    let image_mgr = Arc::new(ImageManager::new(config.images_dir()));
    let vm_mgr = Arc::new(Mutex::new(VmManager::new(config.vms_dir())));
    let net_mgr = Arc::new(network_mgr);

    let report = reconcile::reconcile_on_startup(&config, &agent_db, &vm_mgr, &net_mgr).await?;
    info!(
        recovered = report.recovered,
        restarted = report.restarted,
        orphans = report.orphans,
        lost = report.lost,
        failed = report.failed,
        "reconciliation complete"
    );

    // Discover host resources
    let hostname = gethostname();
    let (total_cpu, total_memory_mib, total_disk_gib) = discover_host_resources(&config.data_dir);
    let gpu_inventory = gpu::discover_gpus().await.unwrap_or_default();

    info!(
        hostname = %hostname,
        cpu = total_cpu,
        memory_mib = total_memory_mib,
        disk_gib = total_disk_gib,
        gpus = gpu_inventory.len(),
        "discovered host resources"
    );

    // Connect to controller with retry loop
    loop {
        match run_agent_session(
            &config,
            &agent_db,
            &hostname,
            total_cpu,
            total_memory_mib,
            total_disk_gib,
            &gpu_inventory,
            image_mgr.clone(),
            vm_mgr.clone(),
            net_mgr.clone(),
        )
        .await
        {
            Ok(()) => {
                info!("agent session ended cleanly");
            }
            Err(e) => {
                error!(error = %e, "agent session failed");
            }
        }
        warn!("reconnecting to controller in 5s");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_agent_session(
    config: &AgentConfig,
    agent_db: &AgentDb,
    hostname: &str,
    total_cpu: u32,
    total_memory_mib: u64,
    total_disk_gib: u64,
    gpu_inventory: &[GpuInventoryItem],
    image_mgr: Arc<ImageManager>,
    vm_mgr: Arc<Mutex<VmManager>>,
    net_mgr: Arc<NetworkManager>,
) -> anyhow::Result<()> {
    // Set up mTLS client
    let cert_pem = std::fs::read_to_string(&config.tls.cert)?;
    let key_pem = std::fs::read_to_string(&config.tls.key)?;
    let ca_pem = std::fs::read_to_string(&config.tls.ca)?;

    let tls_config = ClientTlsConfig::new()
        .identity(Identity::from_pem(&cert_pem, &key_pem))
        .ca_certificate(Certificate::from_pem(&ca_pem))
        .domain_name("basis-controller");

    let channel = Endpoint::from_shared(config.controller_endpoint.clone())?
        .connect_timeout(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .tls_config(tls_config)?
        .connect()
        .await?;

    info!("connected to controller");

    let mut client = basis_agent_client::BasisAgentClient::new(channel);

    // Create outbound message channel
    let (msg_tx, msg_rx) = tokio::sync::mpsc::channel::<AgentMessage>(32);

    // Build GPU device list for registration
    let gpu_devices: Vec<GpuDevice> = gpu_inventory
        .iter()
        .map(|g| GpuDevice {
            pci_address: g.pci_address.clone(),
            model: g.model.clone(),
            iommu_group: g.iommu_group.clone(),
            nvlink_group: g.nvlink_group,
        })
        .collect();

    // Send registration as first message
    msg_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Register(RegisterHostRequest {
                hostname: hostname.to_string(),
                total_cpu,
                total_memory_mib,
                total_disk_gib,
                gpus: gpu_devices,
                iommu_groups: Vec::new(),
            })),
        })
        .await?;

    // Start bidirectional stream
    let outbound = tokio_stream::wrappers::ReceiverStream::new(msg_rx);
    let response = client.stream_messages(outbound).await?;
    let mut inbound = response.into_inner();

    // Wait for registration ack — this tells us our controller-assigned host_id
    let host_id = match inbound.next().await {
        Some(Ok(cmd)) => match cmd.command {
            Some(controller_command::Command::RegisterAck(ack)) => {
                info!(host_id = %ack.host_id, "received registration ack");
                agent_db.set_host_id(&ack.host_id).await?;
                ack.host_id
            }
            _ => anyhow::bail!("expected RegisterHostResponse as first command"),
        },
        Some(Err(e)) => anyhow::bail!("stream error waiting for registration ack: {e}"),
        None => anyhow::bail!("stream closed before registration ack"),
    };

    // Report state of all locally-known VMs to controller. This lets the
    // controller know what actually survived a restart/reboot. VMs that the
    // reconciler couldn't restart (lost disk, GPU rebind failure) are reported
    // as FAILED so CAPI can remediate them.
    let recovered_vms = agent_db.list_vms().await?;
    for vm in &recovered_vms {
        let vm_mgr_lock = vm_mgr.lock().await;
        let is_running = vm_mgr_lock.is_running(&vm.vm_id);
        drop(vm_mgr_lock);

        let (state, error_msg) = if is_running {
            (MachineState::Running, String::new())
        } else {
            (
                MachineState::Failed,
                "VM not running after startup reconciliation".to_string(),
            )
        };

        msg_tx
            .send(AgentMessage {
                payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
                    vm_id: vm.vm_id.clone(),
                    state: state as i32,
                    error_message: error_msg,
                })),
            })
            .await?;
    }

    // Spawn heartbeat task using the controller-assigned host_id
    let heartbeat_tx = msg_tx.clone();
    let heartbeat_host_id = host_id.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let msg = AgentMessage {
                payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {
                    host_id: heartbeat_host_id.clone(),
                    available_cpu: 0,
                    available_memory_mib: 0,
                    available_disk_gib: 0,
                    assigned_gpus: Vec::new(),
                })),
            };
            if heartbeat_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Process inbound commands from controller
    while let Some(result) = inbound.next().await {
        let cmd = result?;
        let report_tx = msg_tx.clone();
        let agent_db = agent_db.clone();

        match cmd.command {
            Some(controller_command::Command::RegisterAck(_)) => {
                warn!("unexpected duplicate registration ack, ignoring");
            }
            Some(controller_command::Command::CreateVm(create_cmd)) => {
                let img = image_mgr.clone();
                let vms = vm_mgr.clone();
                let net = net_mgr.clone();

                tokio::spawn(async move {
                    let result =
                        handle_create_vm(&create_cmd, img.as_ref(), &vms, net.as_ref(), &agent_db)
                            .await;

                    let (state, error_msg) = match result {
                        Ok(()) => (MachineState::Running, String::new()),
                        Err(e) => {
                            error!(vm_id = %create_cmd.vm_id, error = %e, "VM creation failed");
                            (MachineState::Failed, e.to_string())
                        }
                    };

                    let _ = report_tx
                        .send(AgentMessage {
                            payload: Some(agent_message::Payload::VmState(
                                ReportVmStateRequest {
                                    vm_id: create_cmd.vm_id,
                                    state: state as i32,
                                    error_message: error_msg,
                                },
                            )),
                        })
                        .await;
                });
            }
            Some(controller_command::Command::DeleteVm(delete_cmd)) => {
                let vms = vm_mgr.clone();
                let net = net_mgr.clone();

                tokio::spawn(async move {
                    let vm_id = delete_cmd.vm_id.clone();

                    // Look up GPU assignments to unbind
                    if let Ok(Some(vm_record)) = agent_db.get_vm(&vm_id).await {
                        let gpu_addrs: Vec<String> =
                            serde_json::from_str(&vm_record.gpu_pci_addresses)
                                .unwrap_or_default();
                        for addr in &gpu_addrs {
                            gpu::unbind_vfio(addr).await.ok();
                        }
                    }

                    net.delete_tap(&vm_id).await.ok();
                    vms.lock().await.delete_vm(&vm_id).await.ok();
                    agent_db.delete_vm(&vm_id).await.ok();

                    let _ = report_tx
                        .send(AgentMessage {
                            payload: Some(agent_message::Payload::VmState(
                                ReportVmStateRequest {
                                    vm_id,
                                    state: MachineState::Stopped as i32,
                                    error_message: String::new(),
                                },
                            )),
                        })
                        .await;
                });
            }
            None => {}
        }
    }

    Ok(())
}

async fn handle_create_vm(
    cmd: &CreateVmCommand,
    image_mgr: &ImageManager,
    vm_mgr: &Arc<Mutex<VmManager>>,
    net_mgr: &NetworkManager,
    agent_db: &AgentDb,
) -> anyhow::Result<()> {
    let vms_dir = vm_mgr.lock().await.vms_dir.clone();
    let vm_dir_path = vms_dir.join(&cmd.vm_id);
    std::fs::create_dir_all(&vm_dir_path)?;

    // 1. Pull/cache base image
    let base_image = image_mgr.ensure_cached(&cmd.image).await?;

    // 2. Create qcow2 overlay
    let disk_path = image_mgr
        .create_overlay(&base_image, &vm_dir_path, cmd.disk_gib)
        .await?;

    // 3. Write cloud-init ISO
    let cloud_init_path = image_mgr
        .create_cloud_init_iso(
            &vm_dir_path,
            &cmd.bootstrap_data,
            &cmd.ip_address,
            &cmd.gateway,
            cmd.prefix_len,
            &cmd.dns_servers,
        )
        .await?;

    // 4. Create tap device
    let tap_name = net_mgr.create_tap(&cmd.vm_id).await?;

    // 5. Bind GPUs to vfio-pci using addresses selected by the scheduler
    let mut vfio_devices = Vec::new();
    for pci_addr in &cmd.gpu_pci_addresses {
        let vfio_path = gpu::bind_vfio(pci_addr).await?;
        vfio_devices.push(vfio_path);
    }

    // 6. Spawn cloud-hypervisor via systemd
    vm_mgr
        .lock()
        .await
        .create_vm(cmd, &disk_path, &cloud_init_path, &tap_name, &vfio_devices)
        .await?;

    // 7. Record in local DB for crash recovery
    let now = humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string();
    agent_db
        .insert_vm(&LocalVmRow {
            vm_id: cmd.vm_id.clone(),
            name: cmd.name.clone(),
            unit_name: format!("basis-vm-{}.scope", cmd.vm_id),
            ip_address: cmd.ip_address.clone(),
            cpu: cmd.cpu as i64,
            memory_mib: cmd.memory_mib as i64,
            disk_gib: cmd.disk_gib as i64,
            gpu_pci_addresses: serde_json::to_string(&cmd.gpu_pci_addresses)
                .unwrap_or_else(|_| "[]".to_string()),
            image: cmd.image.clone(),
            created_at: now,
        })
        .await?;

    info!(vm_id = %cmd.vm_id, ip = %cmd.ip_address, "VM created successfully");
    Ok(())
}

fn gethostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

fn discover_host_resources(data_dir: &std::path::Path) -> (u32, u64, u64) {
    let cpu = num_cpus();
    let memory_mib = total_memory_mib();
    let disk_gib = disk_space_gib(data_dir);
    (cpu, memory_mib, disk_gib)
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

fn total_memory_mib() -> u64 {
    if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    0
}

fn disk_space_gib(path: &std::path::Path) -> u64 {
    // statvfs via std::fs isn't available in stable Rust, so shell out to df
    let output = std::process::Command::new("df")
        .args(["--output=avail", "-B1", &path.to_string_lossy()])
        .output()
        .ok();

    output
        .and_then(|o| {
            if !o.status.success() {
                return None;
            }
            let stdout = String::from_utf8_lossy(&o.stdout);
            stdout
                .lines()
                .nth(1)?
                .trim()
                .parse::<u64>()
                .ok()
                .map(|bytes| bytes / (1024 * 1024 * 1024))
        })
        .unwrap_or(0)
}
