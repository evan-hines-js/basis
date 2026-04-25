use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use basis_agent::bgp::{Speaker, SpeakerConfig};
use basis_agent::config::{self, Host, HostSpec};
use basis_agent::db::AgentDb;
use basis_agent::gpu;
use basis_agent::handlers;
use basis_agent::host_info::HostResources;
use basis_agent::image::ImageManager;
use basis_agent::lvm;
use basis_agent::metrics::{self, Metrics};
use basis_agent::network::{probe_uplink, NetworkManager, TreeManager, UplinkBridge};
use basis_agent::reconcile;
use basis_agent::vm::VmManager;
use basis_common::gpu::GpuInfo;
use basis_proto::*;
use clap::Parser;
use futures::StreamExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Streaming;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Cadence of the agent's heartbeat to the controller.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Cadence of the agent-side periodic local reconcile.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

const POOL_CAPACITY_INTERVAL: Duration = Duration::from_secs(120);
const POOL_CAPACITY_TIMEOUT: Duration = Duration::from_secs(5);

const ORPHAN_SWEEP_IDLE_INTERVAL: Duration = Duration::from_secs(60);
const ORPHAN_SWEEP_BUSY_INTERVAL: Duration = Duration::from_secs(5);

/// Defense-in-depth grace window for controller-pushed reconcile
/// commands — a VM younger than this is not deleted on a single push
/// that omits it.
const PERIODIC_RECONCILE_GRACE: Duration = Duration::from_secs(120);

#[derive(Parser)]
#[command(name = "basis-agent", about = "Basis hypervisor agent")]
struct Cli {
    #[arg(short, long, default_value = "/etc/basis/host.yaml")]
    config: PathBuf,
}

/// Long-lived agent-side context. Stable across reconnects.
struct AgentRuntime {
    hostname: String,
    spec: HostSpec,
    agent_db: AgentDb,
    image_mgr: Arc<ImageManager>,
    vm_mgr: Arc<VmManager>,
    net_mgr: Arc<NetworkManager>,
    metrics: Arc<Metrics>,
    host_resources: HostResources,
    gpus: Vec<GpuInfo>,
    /// Probed once at startup and reported on every RegisterHost so
    /// the controller can add this host to the VTEP peer list of any
    /// tree it hosts.
    vtep_address: String,
    reconnect_signal: CancellationToken,
    /// Lazily started in `handshake` once the controller's first
    /// `RegisterAck` provides the cell ASN + reflector address.
    /// Stays put across reconnects — re-pushing the same config to
    /// holod on every handshake would be a no-op transaction.
    bgp_speaker: Option<Speaker>,
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

    {
        let metrics_listen = runtime.read().await.spec.metrics_listen.clone();
        let metrics = runtime.read().await.metrics.clone();
        tokio::spawn(async move {
            if let Err(e) =
                metrics::run_server(metrics, &metrics_listen, CancellationToken::new()).await
            {
                error!(error = %e, "agent metrics server exited");
            }
        });
    }

    // Loops that outlive individual sessions — host-local state the
    // controller isn't involved in. Spawning these inside `run_session`
    // would leak one extra copy each time the controller stream
    // reconnects.
    spawn_pool_capacity_loop();
    spawn_orphan_sweep_loop(runtime.clone());

    loop {
        let (endpoint, reconnect) = {
            let rt = runtime.read().await;
            (
                rt.spec.controller_endpoint.clone(),
                rt.reconnect_signal.clone(),
            )
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

    // Read the uplink's MTU and primary IPv4 straight out of the
    // kernel rather than re-declaring them in host.yaml. The IP
    // lives on the bridge when the NIC is enslaved (standard netplan
    // setup), so we probe the bridge — that's authoritative whether
    // the IP is on the bridge or, less commonly, still on the NIC
    // (in which case the operator's netplan hasn't actually attached
    // the NIC to the bridge yet and `validate_uplink` will catch it
    // separately).
    let probe = probe_uplink(&spec.network.bridge).await?;
    info!(
        bridge = %spec.network.bridge,
        mtu = probe.mtu,
        vtep = %probe.vtep_address,
        "probed uplink"
    );

    let uplink = UplinkBridge::new(
        spec.network.bridge.clone(),
        spec.network.physical_nic.clone(),
        probe.mtu,
    );
    let trees = TreeManager::new(
        probe.vtep_address.clone(),
        probe.mtu,
        spec.network.bridge.clone(),
    );
    let net_mgr = NetworkManager::new(uplink, trees);

    // Preflight everything in parallel. `try_join!` short-circuits on
    // the first failure.
    let (pool_capacity, iso_tool, (), ()) = tokio::try_join!(
        async {
            lvm::validate_pool()
                .await
                .context("validating LVM thin pool (run basis-prereqs ansible role)")
        },
        async {
            basis_agent::image::validate_tools()
                .await
                .context("validating host image tools (qemu-img + genisoimage/mkisofs)")
        },
        async {
            gpu::validate_iommu()
                .await
                .context("validating kernel IOMMU (intel_iommu=on / amd_iommu=on)")
        },
        async {
            net_mgr
                .validate_uplink()
                .await
                .context("validating host uplink (bridge + NIC + MTU)")
        },
    )?;

    let agent_db = AgentDb::open(&spec.data_dir.join("agent.db")).await?;
    info!("agent database ready");

    net_mgr.ensure_uplink_bridge().await?;

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
        iso_tool,
    )?);
    let vm_mgr = Arc::new(VmManager::new(spec.vms_dir()));
    let net_mgr = Arc::new(net_mgr);

    let report =
        reconcile::reconcile_on_startup(&spec, &agent_db, &vm_mgr, &net_mgr, &image_mgr).await?;
    info!(
        recovered = report.recovered,
        restarted = report.restarted,
        orphans = report.orphans,
        lost = report.lost,
        failed = report.failed,
        "reconciliation complete"
    );

    let host_resources = HostResources::discover(pool_capacity.data_total_bytes);
    let gpus = gpu::discover_gpus()
        .await
        .context("discovering GPUs (set RUST_LOG=basis=debug for driver details)")?;
    info!(
        hostname = %hostname,
        cpu = host_resources.total_cpu,
        memory_mib = host_resources.total_memory_mib,
        disk_gib = host_resources.total_disk_gib,
        gpus = gpus.len(),
        vtep = %probe.vtep_address,
        "discovered host resources"
    );

    let metrics = Metrics::new().context("constructing agent metrics registry")?;
    metrics.install_global();

    Ok(AgentRuntime {
        hostname,
        spec,
        agent_db,
        image_mgr,
        vm_mgr,
        net_mgr,
        metrics,
        host_resources,
        gpus,
        vtep_address: probe.vtep_address,
        reconnect_signal: CancellationToken::new(),
        bgp_speaker: None,
    })
}

fn spawn_reload_loop(config_path: PathBuf, runtime: Arc<tokio::sync::RwLock<AgentRuntime>>) {
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
        warn!(
            "spec.network change ignored — restart required (VMs have taps on the current bridge)"
        );
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
        (rt.spec.controller_endpoint.clone(), rt.spec.tls.clone())
    };

    let tls_config = tls.client_config(basis_common::tls::CONTROLLER_IDENTITY)?;
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

    // Session-scoped loops: tied to `msg_tx`, exit naturally when the
    // channel closes on session teardown (heartbeat and periodic
    // reconciler both break on `ChannelClosed`). Agent-lifetime loops
    // (pool capacity, orphan sweep) are spawned once from `run` —
    // spawning them here would leak one per reconnect.
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
                vtep_address: rt.vtep_address.clone(),
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

    info!(host_id = %ack.host_id, "received registration ack");
    let host_id = ack.host_id.clone();
    let bgp_asn = ack.bgp_asn;
    let bgp_reflector = ack.bgp_reflector_address.clone();
    let initial = ack.initial_state;

    // Apply tree + VM reconcile and persist the host id while we
    // hold the read guard.
    {
        let rt = runtime.read().await;
        rt.agent_db.set_host_id(&host_id).await?;
        if let Some(initial) = initial {
            apply_reconcile(&rt, &initial, Duration::ZERO).await?;
        }
    }

    // Bring up the host BGP speaker against local holod, peering
    // with the cell reflector. holod runs as its own systemd service
    // — basis-agent restarts don't drop the session. Idempotent at
    // holod's level: re-pushing the same config on reconnect is a
    // no-op transaction.
    {
        let mut rt = runtime.write().await;
        if rt.bgp_speaker.is_none() {
            let router_id = parse_ipv4(&rt.vtep_address, "vtep_address")?;
            let reflector = parse_ipv4(&bgp_reflector, "bgp_reflector_address")?;
            let speaker = Speaker::start(SpeakerConfig {
                asn: bgp_asn,
                router_id,
                reflector_address: reflector,
                holod_endpoint: rt.spec.holod_endpoint.clone(),
                instance_name: "basis".to_string(),
            })
            .await
            .context("starting BGP speaker against local holod")?;
            rt.bgp_speaker = Some(speaker);
        }
    }

    Ok(host_id)
}

fn parse_ipv4(s: &str, field: &str) -> anyhow::Result<std::net::Ipv4Addr> {
    s.parse()
        .with_context(|| format!("{field} '{s}' is not a valid IPv4 address"))
}

/// Apply a `ReconcileHostCommand`: build/tear-down tree bridges to
/// match the command's `trees`, delete VMs absent from
/// `expected_vm_ids`, and refresh the BGP advertised prefix set so
/// the cell knows which tree CIDRs route through this host. Single
/// path used by the handshake ack, periodic pushes, and ad-hoc
/// broadcasts.
async fn apply_reconcile(
    rt: &AgentRuntime,
    cmd: &ReconcileHostCommand,
    vm_delete_grace: Duration,
) -> anyhow::Result<()> {
    rt.net_mgr.reconcile_trees(&cmd.trees).await?;
    handlers::reconcile_against_expected(
        &cmd.expected_vm_ids,
        &rt.vm_mgr,
        &rt.net_mgr,
        &rt.agent_db,
        vm_delete_grace,
    )
    .await?;
    if let Some(speaker) = rt.bgp_speaker.as_ref() {
        let mut prefixes: Vec<ipnet::Ipv4Net> = Vec::new();
        for tree in &cmd.trees {
            match tree.cidr.parse::<ipnet::Ipv4Net>() {
                Ok(p) => prefixes.push(p),
                Err(_) => warn!(
                    cidr = %tree.cidr, vni = tree.vni,
                    "BGP advertise: tree cidr unparseable, skipping"
                ),
            }
            for vip in &tree.cluster_vips {
                match vip.parse::<ipnet::Ipv4Net>() {
                    Ok(p) => prefixes.push(p),
                    Err(_) => warn!(
                        vip = %vip, vni = tree.vni,
                        "BGP advertise: cluster_vip unparseable, skipping"
                    ),
                }
            }
        }
        if let Err(e) = speaker.update_routes(&prefixes).await {
            warn!(error = %e, "BGP advertise: update_routes failed");
        }
    }
    Ok(())
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

fn spawn_pool_capacity_loop() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POOL_CAPACITY_INTERVAL);
        loop {
            interval.tick().await;
            match tokio::time::timeout(POOL_CAPACITY_TIMEOUT, lvm::pool_capacity()).await {
                Ok(Ok(c)) => info!(
                    pool_data_free_gib = c.data_free_bytes / (1 << 30),
                    pool_data_total_gib = c.data_total_bytes / (1 << 30),
                    pool_metadata_free_mib = c.metadata_free_bytes / (1 << 20),
                    pool_metadata_total_mib = c.metadata_total_bytes / (1 << 20),
                    "thin pool capacity"
                ),
                Ok(Err(e)) => warn!(error = %e, "reading thin pool capacity"),
                Err(_) => warn!(
                    timeout_secs = POOL_CAPACITY_TIMEOUT.as_secs(),
                    "thin pool capacity query timed out — LVM likely busy"
                ),
            }
        }
    });
}

fn spawn_orphan_sweep_loop(runtime: Arc<tokio::sync::RwLock<AgentRuntime>>) {
    tokio::spawn(async move {
        tokio::time::sleep(ORPHAN_SWEEP_IDLE_INTERVAL).await;
        loop {
            let sleep = {
                let rt = runtime.read().await;
                match reconcile::periodic_sweep(&rt.agent_db, &rt.vm_mgr, rt.net_mgr.as_ref()).await
                {
                    Ok(0) => ORPHAN_SWEEP_IDLE_INTERVAL,
                    Ok(n) => {
                        info!(reclaimed = n, "orphan sweep reclaimed resources");
                        ORPHAN_SWEEP_BUSY_INTERVAL
                    }
                    Err(e) => {
                        warn!(error = %e, "orphan sweep failed");
                        ORPHAN_SWEEP_IDLE_INTERVAL
                    }
                }
            };
            tokio::time::sleep(sleep).await;
        }
    });
}

fn spawn_periodic_reconciler(
    sender: mpsc::Sender<AgentMessage>,
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let rt = runtime.read().await;
            match handlers::report_local_vm_states(&rt.agent_db, &rt.vm_mgr, &sender).await {
                Ok(()) => {}
                // Controller stream is gone — session ended. A new
                // session will have spawned its own reconciler;
                // we exit so we don't leak across reconnects.
                Err(e) if e.is::<handlers::ChannelClosed>() => break,
                Err(e) => warn!(error = %e, "periodic state report failed"),
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
                            vni = create.vni,
                            "received CreateVm"
                        );
                        spawn_create(*create, rt_snapshot, sender.clone());
                    }
                    Some(controller_command::Command::DeleteVm(delete)) => {
                        info!(vm_id = %delete.vm_id, "received DeleteVm");
                        spawn_delete(delete, rt_snapshot, sender.clone());
                    }
                    Some(controller_command::Command::ReconcileHost(reconcile_cmd)) => {
                        spawn_reconcile(*reconcile_cmd, runtime.clone());
                    }
                    None => {}
                }
            }
        }
    }
}

/// If the session ended between dispatching a VM op and the op
/// finishing, the state report has nowhere to go. The controller's
/// next reconcile will discover the true state from
/// `list_vms_on_host` anyway, so a dropped report here is a cosmetic
/// delay, not a correctness issue.
async fn send_terminal_vm_state(
    sender: &mpsc::Sender<AgentMessage>,
    vm_id: String,
    state: MachineState,
    err: String,
    transient: bool,
) {
    if let Err(e) = handlers::send_vm_state(sender, vm_id.clone(), state, err, transient).await {
        warn!(vm_id, error = %e, "dropped terminal VM state report; session already closed");
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
            rt.metrics.as_ref(),
        )
        .await;

        let (state, err, transient) = match result {
            Ok(()) => (MachineState::Running, String::new(), false),
            Err(e) => {
                let transient = e.chain().any(|c| {
                    matches!(
                        c.downcast_ref::<lvm::LvmError>(),
                        Some(lvm::LvmError::Busy(_))
                    )
                });
                if transient {
                    warn!(vm_id = %cmd.vm_id, error = %e, "VM creation shed for backpressure");
                } else {
                    error!(vm_id = %cmd.vm_id, error = %e, "VM creation failed");
                }
                (MachineState::Failed, e.to_string(), transient)
            }
        };
        send_terminal_vm_state(&sender, cmd.vm_id, state, err, transient).await;
    });
}

fn spawn_delete(cmd: DeleteVmCommand, rt: TaskContext, sender: mpsc::Sender<AgentMessage>) {
    tokio::spawn(async move {
        let (state, err, transient) =
            match handlers::delete_vm(&cmd.vm_id, &rt.vm_mgr, rt.net_mgr.as_ref(), &rt.agent_db)
                .await
            {
                Ok(()) => (MachineState::Stopped, String::new(), false),
                Err(e) => (MachineState::Failed, e.to_string(), true),
            };
        send_terminal_vm_state(&sender, cmd.vm_id, state, err, transient).await;
    });
}

fn spawn_reconcile(cmd: ReconcileHostCommand, runtime: Arc<tokio::sync::RwLock<AgentRuntime>>) {
    tokio::spawn(async move {
        let rt = runtime.read().await;
        if let Err(e) = apply_reconcile(&rt, &cmd, PERIODIC_RECONCILE_GRACE).await {
            warn!(error = %e, "controller-driven reconcile failed");
        }
    });
}

/// Subset of `AgentRuntime` a spawned task needs. Cheap clones only.
struct TaskContext {
    agent_db: AgentDb,
    image_mgr: Arc<ImageManager>,
    vm_mgr: Arc<VmManager>,
    net_mgr: Arc<NetworkManager>,
    metrics: Arc<Metrics>,
}

impl AgentRuntime {
    fn clone_for_task(&self) -> TaskContext {
        TaskContext {
            agent_db: self.agent_db.clone(),
            image_mgr: self.image_mgr.clone(),
            vm_mgr: self.vm_mgr.clone(),
            net_mgr: self.net_mgr.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
