use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use basis_agent::config::{self, Host, HostSpec};
use basis_agent::db::AgentDb;
use basis_agent::gpu;
use basis_agent::handlers;
use basis_agent::host_info::HostResources;
use basis_agent::image::ImageManager;
use basis_agent::lvm;
use basis_agent::metrics::{self, Metrics};
use basis_agent::network::NetworkManager;
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

/// Cadence of the agent's heartbeat to the controller. Paired with the
/// controller's `HEARTBEAT_STALE_THRESHOLD` (90 s = three intervals) so
/// a single missed beat doesn't flip the host unhealthy.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Cadence of the agent-side periodic local reconcile, which detects
/// VMs the local DB believes are running but that systemd has lost
/// (crash, OOM, manual stop) and reports them as FAILED.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// Wait between failed connect attempts. Short enough that recovery
/// from a controller restart is sub-10s; long enough that we don't
/// hammer a permanently-down endpoint.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Per-attempt connect deadline to the controller. Generous because
/// the agent has nothing else to do; longer waits mostly hurt the
/// "controller permanently down" case which isn't latency-sensitive.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Cadence for logging thin-pool capacity. Coarser than the heartbeat
/// because operators don't need second-by-second fill graphs, and
/// because every query forks `lvs` which can stall behind a busy VG
/// lock during mass VM teardown. Decoupled from `HEARTBEAT_INTERVAL`
/// so a stuck `lvs` can never delay a heartbeat.
const POOL_CAPACITY_INTERVAL: Duration = Duration::from_secs(120);

/// Upper bound on a single `lvs` call. If LVM is wedged long enough
/// that we can't probe capacity, we log a warning and move on rather
/// than accumulating stuck tasks.
const POOL_CAPACITY_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle cadence for the periodic orphan sweep. Reclaims systemd units,
/// LVs, and taps whose owning VM id is no longer in the agent DB or
/// running unit set. Handles the residue of `delete_vm` calls whose
/// `lvremove` raced with a not-yet-released block-device handle and got
/// skipped best-effort.
///
/// Used only when the previous pass found nothing to reclaim. On a
/// non-zero pass the loop tail-chases at `ORPHAN_SWEEP_BUSY_INTERVAL`
/// so a churn-heavy fleet drains its backlog in seconds instead of
/// letting stale LVs accrete for multiples of this interval. For a
/// long-running agent that's where the value is: self-healing is load-
/// proportional rather than fixed.
const ORPHAN_SWEEP_IDLE_INTERVAL: Duration = Duration::from_secs(60);

/// Wake interval after a non-zero sweep pass. Short enough to drain a
/// large backlog quickly (hundreds of orphans in minutes, not hours);
/// not so short that failed-lvremove retries hammer the thin pool.
const ORPHAN_SWEEP_BUSY_INTERVAL: Duration = Duration::from_secs(5);

/// Defense-in-depth grace window for periodic `ReconcileHostCommand`
/// pushes: a VM younger than this is *not* deleted on a single push
/// that omits it, so an in-flight CreateMachine the controller hasn't
/// fully recorded yet can't be wiped out by a misbehaving controller.
/// Sized to comfortably exceed the controller's `CREATE_MACHINE_TIMEOUT`
/// of 600 s would be too long; 120 s covers a normal cold-start
/// (~20 s) plus headroom. The post-register reconcile *does not* apply
/// this grace — that one trusts the controller's authoritative list
/// because the agent has been offline.
const PERIODIC_RECONCILE_GRACE: Duration = Duration::from_secs(120);

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
    vm_mgr: Arc<VmManager>,
    net_mgr: Arc<NetworkManager>,
    metrics: Arc<Metrics>,
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

    // Expose Prometheus `/metrics` on a plain-HTTP port alongside the
    // agent's gRPC stream to the controller. Separate port, no TLS —
    // Prometheus scrapes it locally or via the observability stack's
    // `basis-agents` job. Lives for the process lifetime; the shutdown
    // token is a stub since the agent exits by process termination.
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

    let net_mgr = NetworkManager::new(
        spec.network.bridge.clone(),
        spec.network.physical_nic.clone(),
    );

    // Run every host-level preflight in parallel so the agent either
    // comes up with all its invariants satisfied, or fails with a
    // specific, actionable error. Silent fallback at any of these
    // layers — thin pool missing, NIC missing, IOMMU off, qemu-img
    // absent — would let a VM be "created" on a host that can't run
    // it, so we fail loudly instead. Each validator's error message
    // includes remediation (usually "run basis-prereqs").
    //
    // NOTE: `tokio::try_join!` short-circuits on the first failure, so
    // in the "everything is broken" case the operator only sees one
    // error at a time. That's fine — once they fix it, the agent
    // restarts and the next layer's error surfaces.
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
                .validate_bridge()
                .await
                .context("validating host network (bridge + physical NIC)")
        },
    )?;

    let agent_db = AgentDb::open(&spec.data_dir.join("agent.db")).await?;
    info!("agent database ready");

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
        reconnect_signal: CancellationToken::new(),
    })
}

/// SIGHUP re-reads the config file, diffs against the running spec,
/// applies what's safe (reconnect on `controllerEndpoint` change), and
/// warns loudly about anything that would require a restart.
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

    spawn_heartbeat_loop(msg_tx.clone(), host_id.clone());
    spawn_pool_capacity_loop();
    spawn_periodic_reconciler(msg_tx.clone(), runtime.clone());
    spawn_orphan_sweep_loop(runtime.clone());

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
        Duration::ZERO,
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

/// Periodically log thin-pool capacity. Metadata free is the silent
/// killer of thin pools — when it fills up the pool goes read-only and
/// every running VM's disk errors out. No prometheus yet, so the
/// journal is the warning channel.
///
/// Runs independently of the heartbeat so an `lvs` that gets stuck
/// behind a busy VG lock (e.g. during mass VM teardown) can't delay
/// liveness reporting.
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

/// Periodic orphan sweep. Reclaims host-level resources (systemd units,
/// LVs, taps) that no authoritative source claims — typically the
/// residue of `delete_vm` calls where `lvremove` raced with a held
/// block-device handle and was skipped best-effort.
///
/// Adaptive cadence, not a fixed tick: on an idle fleet this runs every
/// `ORPHAN_SWEEP_IDLE_INTERVAL`; on a churning fleet with a backlog the
/// loop immediately re-enters after a non-zero pass, draining the
/// backlog load-proportionally. This is what lets a long-lived agent
/// self-heal after hours of crash/restart noise without needing an
/// operator to prune the thin pool by hand.
fn spawn_orphan_sweep_loop(runtime: Arc<tokio::sync::RwLock<AgentRuntime>>) {
    tokio::spawn(async move {
        // Skip an immediate first sweep: startup already ran a full
        // reconcile including the orphan sweep, so there's nothing to
        // reclaim yet.
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

/// Periodic state-report loop. Tells the controller the current state
/// (Running or Failed) of every locally-known VM on `RECONCILE_INTERVAL`,
/// catching VMs whose systemd scope has disappeared (crash, OOM, manual
/// stop). Same function as the post-handshake catch-up so there's one
/// definition of "what state report should look like."
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
                handlers::report_local_vm_states(&rt.agent_db, &rt.vm_mgr, &sender).await
            {
                warn!(error = %e, "periodic state report failed");
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
            rt.metrics.as_ref(),
        )
        .await;

        let (state, err, transient) = match result {
            Ok(()) => (MachineState::Running, String::new(), false),
            Err(e) => {
                // Walk the error chain to detect `LvmError::Busy` — this
                // is a load-shedding signal, not a real fault. The
                // controller's caller should retry the create on this
                // response instead of bubbling a failure to the user.
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
        handlers::send_vm_state(&sender, cmd.vm_id, state, err, transient).await;
    });
}

/// Controller-initiated delete. Reports the outcome back via the
/// standard `ReportVMStateRequest` path so the controller's
/// `DeleteCluster` / `DeleteMachine` RPC can block on real cleanup
/// completion — that's what bounds queue depth under load
/// (workers wait on the delete RPC rather than pipelining the next
/// create behind an unresolved delete).
fn spawn_delete(cmd: DeleteVmCommand, rt: TaskContext, sender: mpsc::Sender<AgentMessage>) {
    tokio::spawn(async move {
        // Every surfaced delete error is retry-worthy: the only step
        // that returns an error is `lvremove`, and either the kernel
        // has the device still pending release (transient, next retry
        // succeeds) or lvm2 itself is busy (semaphore timeout). The
        // orphan sweep is the backstop if retries eventually give up.
        let (state, err, transient) =
            match handlers::delete_vm(&cmd.vm_id, &rt.vm_mgr, rt.net_mgr.as_ref(), &rt.agent_db)
                .await
            {
                Ok(()) => (MachineState::Stopped, String::new(), false),
                Err(e) => (MachineState::Failed, e.to_string(), true),
            };
        handlers::send_vm_state(&sender, cmd.vm_id, state, err, transient).await;
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
            PERIODIC_RECONCILE_GRACE,
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
