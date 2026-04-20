use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basis_agent::config::{self, Host, HostSpec};
use basis_agent::db::AgentDb;
use basis_agent::gpu;
use basis_agent::handlers;
use basis_agent::host_info::HostResources;
use basis_agent::image::ImageManager;
use basis_agent::network::NetworkManager;
use basis_agent::reconcile;
use basis_agent::vm::VmManager;
use basis_common::gpu::GpuInfo;
use basis_proto::*;
use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Streaming;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The SAN the controller's server certificate must carry.
const CONTROLLER_SAN: &str = "basis-controller";

#[derive(Parser)]
#[command(name = "basis-agent", about = "Basis hypervisor agent")]
struct Cli {
    #[arg(short, long, default_value = "/etc/basis/host.yaml")]
    config: PathBuf,
}

/// Long-lived agent-side context: filesystem managers, databases, and
/// the host's resource snapshot. Stable across reconnects.
struct AgentRuntime {
    hostname: String,
    spec: HostSpec,
    agent_db: AgentDb,
    image_mgr: Arc<ImageManager>,
    vm_mgr: Arc<Mutex<VmManager>>,
    net_mgr: Arc<NetworkManager>,
    host_resources: HostResources,
    gpus: Vec<GpuInfo>,
    /// Flipped by the SIGHUP handler when the current stream should be
    /// torn down (e.g., `controllerEndpoint` changed).
    reconnect_signal: CancellationToken,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("basis=info".parse().expect("static directive string")),
        )
        .init();

    let cli = Cli::parse();
    let host = config::load(&cli.config)?;
    info!(
        host = %host.metadata.name,
        controller = %host.spec.controller_endpoint,
        data_dir = %host.spec.data_dir.display(),
        "loaded Host config"
    );

    let runtime = Arc::new(tokio::sync::RwLock::new(initialize_runtime(host).await?));
    spawn_reload_loop(cli.config.clone(), runtime.clone());

    loop {
        let (endpoint, reconnect) = {
            let rt = runtime.read().await;
            (rt.spec.controller_endpoint.clone(), rt.reconnect_signal.clone())
        };
        match run_session(runtime.clone(), reconnect).await {
            Ok(()) => info!("agent session ended cleanly"),
            Err(e) => error!(endpoint = %endpoint, error = %e, "agent session failed"),
        }
        warn!(retry_in = ?RECONNECT_DELAY, "reconnecting to controller");
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn initialize_runtime(host: Host) -> anyhow::Result<AgentRuntime> {
    let hostname = host.metadata.name;
    let spec = host.spec;

    std::fs::create_dir_all(&spec.data_dir)?;
    std::fs::create_dir_all(spec.images_dir())?;
    std::fs::create_dir_all(spec.vms_dir())?;

    let agent_db = AgentDb::open(&spec.data_dir.join("agent.db")).await?;
    info!("agent database ready");

    let net_mgr = NetworkManager::new(
        spec.network.bridge.clone(),
        spec.network.physical_nic.clone(),
    );
    net_mgr.ensure_bridge().await?;

    let image_mgr = Arc::new(ImageManager::with_auth(
        spec.images_dir(),
        spec.registries
            .iter()
            .map(|r| {
                (
                    r.host.clone(),
                    oci_client::secrets::RegistryAuth::Basic(
                        r.username.clone(),
                        r.password.clone(),
                    ),
                )
            })
            .collect(),
    ));
    let vm_mgr = Arc::new(Mutex::new(VmManager::new(
        spec.vms_dir(),
        spec.firmware_path.clone(),
    )));
    let net_mgr = Arc::new(net_mgr);

    let report =
        reconcile::reconcile_on_startup(&spec, &agent_db, &vm_mgr, &net_mgr).await?;
    info!(
        recovered = report.recovered,
        restarted = report.restarted,
        orphans = report.orphans,
        lost = report.lost,
        failed = report.failed,
        "reconciliation complete"
    );

    let host_resources = HostResources::discover(&spec.data_dir);
    // Fail loudly on GPU discovery errors. On a GPU host, silently
    // registering with 0 GPUs means the scheduler packs CPU workloads
    // onto it and customers never see their GPUs — the exact failure
    // mode a GPU cloud can't have.
    let gpus = gpu::discover_gpus()
        .await
        .context("discovering GPUs (set RUST_LOG=basis=debug for driver details)")?;
    info!(
        hostname = %hostname,
        cpu = host_resources.total_cpu,
        memory_mib = host_resources.total_memory_mib,
        disk_gib = host_resources.total_disk_gib,
        gpus = gpus.len(),
        "discovered host resources"
    );

    Ok(AgentRuntime {
        hostname,
        spec,
        agent_db,
        image_mgr,
        vm_mgr,
        net_mgr,
        host_resources,
        gpus,
        reconnect_signal: CancellationToken::new(),
    })
}

/// SIGHUP re-reads the config file, diffs against the running spec,
/// applies what's safe (reconnect on `controllerEndpoint` change), and
/// warns loudly about anything that would require a restart.
fn spawn_reload_loop(
    config_path: PathBuf,
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
) {
    tokio::spawn(async move {
        let Ok(mut sighup) = signal(SignalKind::hangup()) else {
            warn!("could not install SIGHUP handler; config reload disabled");
            return;
        };
        while sighup.recv().await.is_some() {
            if let Err(e) = reload_config(&config_path, &runtime).await {
                error!(error = %e, "config reload failed");
            }
        }
    });
}

async fn reload_config(
    path: &std::path::Path,
    runtime: &Arc<tokio::sync::RwLock<AgentRuntime>>,
) -> anyhow::Result<()> {
    let host = config::load(path)?;
    info!(path = %path.display(), "SIGHUP: reloading config");

    let new_spec = host.spec;
    let mut rt = runtime.write().await;

    if host.metadata.name != rt.hostname {
        warn!(
            current = %rt.hostname,
            attempted = %host.metadata.name,
            "metadata.name change ignored — restart required"
        );
    }
    if new_spec.data_dir != rt.spec.data_dir {
        warn!("spec.dataDir change ignored — restart required");
    }
    if new_spec.network != rt.spec.network {
        warn!("spec.network change ignored — restart required (VMs have taps on the current bridge)");
    }
    if new_spec.tls != rt.spec.tls {
        warn!("spec.tls change ignored — restart required");
    }
    if new_spec.registries != rt.spec.registries {
        warn!("spec.registries change ignored — restart required (image pull auth is cached)");
    }

    if new_spec.controller_endpoint != rt.spec.controller_endpoint {
        info!(
            from = %rt.spec.controller_endpoint,
            to = %new_spec.controller_endpoint,
            "controllerEndpoint changed; reconnecting"
        );
        rt.spec.controller_endpoint = new_spec.controller_endpoint;
        rt.reconnect_signal.cancel();
        rt.reconnect_signal = CancellationToken::new();
    }
    Ok(())
}

async fn run_session(
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
    reconnect: CancellationToken,
) -> anyhow::Result<()> {
    let (endpoint_url, tls) = {
        let rt = runtime.read().await;
        (
            rt.spec.controller_endpoint.clone(),
            rt.spec.tls.clone(),
        )
    };

    let tls_config = tls.client_config(CONTROLLER_SAN)?;
    let channel = Endpoint::from_shared(endpoint_url)?
        .connect_timeout(CONNECT_TIMEOUT)
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .tls_config(tls_config)?
        .connect()
        .await?;
    info!("connected to controller");

    let mut client = basis_agent_client::BasisAgentClient::new(channel);

    let (msg_tx, msg_rx) = mpsc::channel::<AgentMessage>(32);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(msg_rx);

    {
        let rt = runtime.read().await;
        send_register(&msg_tx, &rt).await?;
    }

    let response = client.stream_messages(outbound).await?;
    let mut inbound = response.into_inner();

    let host_id = handshake(&mut inbound, &runtime).await?;

    {
        let rt = runtime.read().await;
        handlers::report_local_vm_states(&rt.agent_db, &rt.vm_mgr, &msg_tx).await?;
    }

    spawn_heartbeat_loop(msg_tx.clone(), host_id.clone());
    spawn_periodic_reconciler(msg_tx.clone(), runtime.clone());

    process_inbound(&mut inbound, runtime, msg_tx, reconnect).await
}

async fn send_register(
    sender: &mpsc::Sender<AgentMessage>,
    rt: &AgentRuntime,
) -> anyhow::Result<()> {
    let gpu_devices: Vec<GpuDevice> = rt
        .gpus
        .iter()
        .map(|g| GpuDevice {
            pci_address: g.pci_address.clone(),
            model: g.model.clone(),
            iommu_group: g.iommu_group.clone(),
            nvlink_group: g.nvlink_group,
        })
        .collect();

    sender
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Register(RegisterHostRequest {
                hostname: rt.hostname.clone(),
                total_cpu: rt.host_resources.total_cpu,
                total_memory_mib: rt.host_resources.total_memory_mib,
                total_disk_gib: rt.host_resources.total_disk_gib,
                gpus: gpu_devices,
                iommu_groups: Vec::new(),
            })),
        })
        .await?;
    Ok(())
}

async fn handshake(
    inbound: &mut Streaming<ControllerCommand>,
    runtime: &Arc<tokio::sync::RwLock<AgentRuntime>>,
) -> anyhow::Result<String> {
    let cmd = inbound
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("stream closed before registration ack"))??;

    let ack = match cmd.command {
        Some(controller_command::Command::RegisterAck(ack)) => ack,
        other => anyhow::bail!("expected RegisterHostResponse as first command, got {other:?}"),
    };

    info!(
        host_id = %ack.host_id,
        expected_vms = ack.expected_vm_ids.len(),
        "received registration ack"
    );
    let rt = runtime.read().await;
    rt.agent_db.set_host_id(&ack.host_id).await?;
    handlers::reconcile_against_expected(
        &ack.expected_vm_ids,
        &rt.vm_mgr,
        &rt.net_mgr,
        &rt.agent_db,
    )
    .await?;
    Ok(ack.host_id)
}

fn spawn_heartbeat_loop(sender: mpsc::Sender<AgentMessage>, host_id: String) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            interval.tick().await;
            let msg = AgentMessage {
                payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {
                    host_id: host_id.clone(),
                })),
            };
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });
}

/// Periodic local reconciliation loop. Detects VMs the local DB believes
/// are running but that systemd has lost (crash, OOM, manual stop) and
/// reports them as FAILED. No neighbor awareness — the agent diagnoses
/// drift on its own and lets the controller decide what to do about it.
fn spawn_periodic_reconciler(
    sender: mpsc::Sender<AgentMessage>,
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
) {
    tokio::spawn(async move {
        // First tick fires immediately; skip it so we don't double up with
        // the initial post-handshake state report.
        let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let rt = runtime.read().await;
            if let Err(e) =
                handlers::reconcile_running_vms(&rt.agent_db, &rt.vm_mgr, &sender).await
            {
                warn!(error = %e, "periodic reconcile failed");
            }
        }
    });
}

async fn process_inbound(
    inbound: &mut Streaming<ControllerCommand>,
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
    sender: mpsc::Sender<AgentMessage>,
    reconnect: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = reconnect.cancelled() => {
                info!("reconnect requested; closing session");
                return Ok(());
            }
            maybe_cmd = inbound.next() => {
                let Some(cmd) = maybe_cmd else { return Ok(()) };
                let cmd = cmd?;
                let rt_snapshot = runtime.read().await.clone_for_task();
                match cmd.command {
                    Some(controller_command::Command::RegisterAck(_)) => {
                        warn!("unexpected duplicate registration ack, ignoring");
                    }
                    Some(controller_command::Command::CreateVm(create)) => {
                        info!(
                            vm_id = %create.vm_id,
                            name = %create.name,
                            image = %create.image,
                            cpu = create.cpu,
                            memory_mib = create.memory_mib,
                            disk_gib = create.disk_gib,
                            gpus = create.gpu_pci_addresses.len(),
                            "received CreateVm"
                        );
                        spawn_create(create, rt_snapshot, sender.clone());
                    }
                    Some(controller_command::Command::DeleteVm(delete)) => {
                        info!(vm_id = %delete.vm_id, "received DeleteVm");
                        spawn_delete(delete, rt_snapshot, sender.clone());
                    }
                    Some(controller_command::Command::ReconcileHost(reconcile)) => {
                        spawn_reconcile(reconcile, rt_snapshot);
                    }
                    None => {}
                }
            }
        }
    }
}

fn spawn_create(cmd: CreateVmCommand, rt: TaskContext, sender: mpsc::Sender<AgentMessage>) {
    tokio::spawn(async move {
        let result = handlers::create_vm(
            &cmd,
            rt.image_mgr.as_ref(),
            &rt.vm_mgr,
            rt.net_mgr.as_ref(),
            &rt.agent_db,
        )
        .await;

        let (state, err) = match result {
            Ok(()) => (MachineState::Running, String::new()),
            Err(e) => {
                error!(vm_id = %cmd.vm_id, error = %e, "VM creation failed");
                (MachineState::Failed, e.to_string())
            }
        };
        handlers::send_vm_state(&sender, cmd.vm_id, state, err).await;
    });
}

fn spawn_delete(cmd: DeleteVmCommand, rt: TaskContext, sender: mpsc::Sender<AgentMessage>) {
    tokio::spawn(async move {
        handlers::delete_vm(&cmd.vm_id, &rt.vm_mgr, rt.net_mgr.as_ref(), &rt.agent_db).await;
        handlers::send_vm_state(&sender, cmd.vm_id, MachineState::Stopped, String::new()).await;
    });
}

/// Apply a controller-pushed authoritative VM list. Same contract as the
/// initial list from `RegisterHostResponse` — locally-known VMs absent
/// from the list have been forgotten by the controller and must be torn
/// down. Delegated to the same handler so behavior is identical whether
/// the trigger was registration or the periodic push.
fn spawn_reconcile(cmd: ReconcileHostCommand, rt: TaskContext) {
    tokio::spawn(async move {
        if let Err(e) = handlers::reconcile_against_expected(
            &cmd.expected_vm_ids,
            &rt.vm_mgr,
            rt.net_mgr.as_ref(),
            &rt.agent_db,
        )
        .await
        {
            warn!(error = %e, "controller-driven reconcile failed");
        }
    });
}

/// Subset of `AgentRuntime` a spawned task needs. Cheap clones only.
struct TaskContext {
    agent_db: AgentDb,
    image_mgr: Arc<ImageManager>,
    vm_mgr: Arc<Mutex<VmManager>>,
    net_mgr: Arc<NetworkManager>,
}

impl AgentRuntime {
    fn clone_for_task(&self) -> TaskContext {
        TaskContext {
            agent_db: self.agent_db.clone(),
            image_mgr: self.image_mgr.clone(),
            vm_mgr: self.vm_mgr.clone(),
            net_mgr: self.net_mgr.clone(),
        }
    }
}
