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
use basis_agent::lvm::{Storage, StorageCapacity};
use basis_agent::metrics::{self, Metrics};
use basis_agent::network::{probe_uplink, ClusterManager, NetworkManager, UplinkBridge};
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

/// Cadence of the agent's storage capacity refresh. The latest snapshot
/// is shared with the heartbeat loop so the controller's per-pool
/// scheduling budgets stay live, not registration-stale.
const STORAGE_CAPACITY_INTERVAL: Duration = Duration::from_secs(120);
const STORAGE_CAPACITY_TIMEOUT: Duration = Duration::from_secs(5);

const ORPHAN_SWEEP_IDLE_INTERVAL: Duration = Duration::from_secs(60);
const ORPHAN_SWEEP_BUSY_INTERVAL: Duration = Duration::from_secs(5);

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
    storage: Arc<Storage>,
    /// Latest [`StorageCapacity`] from the agent's local lvm queries.
    /// Refreshed on every [`STORAGE_CAPACITY_INTERVAL`] tick by
    /// [`spawn_storage_capacity_loop`]; read by the heartbeat sender
    /// so the controller's scheduling budgets are heartbeat-fresh.
    storage_capacity: Arc<tokio::sync::RwLock<StorageCapacity>>,
    metrics: Arc<Metrics>,
    host_resources: HostResources,
    gpus: Vec<GpuInfo>,
    /// Probed once at startup and reported on every RegisterHost so
    /// the controller can add this host to the VTEP peer list of any
    /// cluster overlay it carries.
    vtep_address: String,
    reconnect_signal: CancellationToken,
    /// Lazily started in `handshake` once the controller's first
    /// `RegisterAck` provides the cell ASN + reflector address.
    /// Stays put across reconnects — the speaker's prefix cache
    /// short-circuits no-op pushes against gobgpd.
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

    {
        let rt = runtime.read().await;
        let metrics = rt.metrics.clone();
        let agent_db = rt.agent_db.clone();
        let vm_mgr = rt.vm_mgr.clone();
        tokio::spawn(metrics::run_vm_poller(
            metrics,
            agent_db,
            vm_mgr,
            CancellationToken::new(),
        ));
    }

    // Loops that outlive individual sessions — host-local state the
    // controller isn't involved in. Spawning these inside `run_session`
    // would leak one extra copy each time the controller stream
    // reconnects.
    {
        let rt = runtime.read().await;
        spawn_storage_capacity_loop(rt.storage.clone(), rt.storage_capacity.clone());
    }
    spawn_orphan_sweep_loop(runtime.clone());

    // Single per-process BGP-RIB watcher: subscribes to gobgpd's
    // best-path stream and installs `<prefix> via <next-hop> dev
    // brc<vni>` for each remotely-originated path. Spawning here
    // (not inside `handshake`) means exactly one watcher per agent
    // lifetime, regardless of how many controller reconnects happen.
    {
        let rt = runtime.read().await;
        let net = rt.net_mgr.clone();
        let endpoint = rt.spec.gobgpd_endpoint.clone();
        tokio::spawn(async move {
            basis_agent::bgp_routes::run_route_watcher(net, endpoint).await;
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
    let clusters = ClusterManager::new(
        probe.vtep_address.clone(),
        probe.mtu,
        spec.network.bridge.clone(),
    );
    let net_mgr = NetworkManager::new(uplink, clusters);

    // Open the agent DB before constructing Storage — the LVM
    // backend's reservation table lives on this same SQLite file, so
    // Storage::from_host_spec needs the connection pool at build time.
    let agent_db = AgentDb::open(&spec.data_dir.join("agent.db")).await?;
    info!("agent database ready");

    let storage = Arc::new(Storage::from_host_spec(&spec, agent_db.raw_pool()));

    // Preflight everything in parallel. `try_join!` short-circuits on
    // the first failure.
    let (initial_capacity, iso_tool, (), ()) = tokio::try_join!(
        async {
            storage
                .validate()
                .await
                .context("validating LVM layout (run basis-prereqs ansible role)")
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
        reconcile::reconcile_on_startup(&spec, &agent_db, &vm_mgr, &net_mgr, &image_mgr, &storage)
            .await?;
    info!(
        recovered = report.recovered,
        restarted = report.restarted,
        orphans = report.orphans,
        lost = report.lost,
        failed = report.failed,
        "reconciliation complete"
    );

    let host_resources = HostResources::discover();
    let gpus = gpu::discover_gpus()
        .await
        .context("discovering GPUs (set RUST_LOG=basis=debug for driver details)")?;
    info!(
        hostname = %hostname,
        cpu = host_resources.total_cpu,
        memory_mib = host_resources.total_memory_mib,
        rootfs_total_gib = initial_capacity.rootfs.total / (1 << 30),
        data_pool_count = initial_capacity.pools.len(),
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
        storage,
        storage_capacity: Arc::new(tokio::sync::RwLock::new(initial_capacity)),
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

    // The handshake's `host_id` return is no longer consumed in this
    // session-scoped loop (heartbeats no longer carry host_id — the
    // controller derives it from the authenticated stream). Drop on
    // the floor; the handshake's side effects on `runtime` are what we
    // care about.
    handshake(&mut inbound, &runtime, &msg_tx).await?;

    {
        let rt = runtime.read().await;
        handlers::report_local_vm_states(&rt.agent_db, &rt.vm_mgr, &msg_tx).await?;
    }

    // Session-scoped loops: tied to `msg_tx`, exit naturally when the
    // channel closes on session teardown (heartbeat and periodic
    // reconciler both break on `ChannelClosed`). Agent-lifetime loops
    // (pool capacity, orphan sweep) are spawned once from `run` —
    // spawning them here would leak one per reconnect.
    let capacity = runtime.read().await.storage_capacity.clone();
    spawn_heartbeat_loop(msg_tx.clone(), capacity);
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

    // Snapshot the agent's live state so the controller can synthesise
    // tombstones for orphans on register. Errors here would block the
    // registration over what's a best-effort housekeeping signal —
    // log + send an empty inventory instead, and rely on the next
    // periodic reconcile to converge.
    let vm_ids: Vec<String> = match rt.agent_db.list_vms().await {
        Ok(rows) => rows.into_iter().map(|r| r.vm_id).collect(),
        Err(e) => {
            warn!(error = %e, "register: list_vms for inventory failed; sending empty");
            Vec::new()
        }
    };
    let clusters: Vec<InventoryCluster> = match rt.net_mgr.cluster_inventory().await {
        Ok(pairs) => pairs
            .into_iter()
            .map(|(vni, cidr)| InventoryCluster { vni, cidr })
            .collect(),
        Err(e) => {
            warn!(error = %e,
                "register: cluster_inventory failed; sending empty");
            Vec::new()
        }
    };

    let storage_capacity = storage_capacity_to_proto(&*rt.storage_capacity.read().await);

    sender
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Register(RegisterHostRequest {
                hostname: rt.hostname.clone(),
                total_cpu: rt.host_resources.total_cpu,
                total_memory_mib: rt.host_resources.total_memory_mib,
                storage_capacity: Some(storage_capacity),
                gpus: gpu_devices,
                vtep_address: rt.vtep_address.clone(),
                rank: rt.spec.rank,
                labels: rt.spec.labels.clone().into_iter().collect(),
                current_inventory: Some(HostInventory { vm_ids, clusters }),
            })),
        })
        .await?;
    Ok(())
}

/// Convert the agent's internal [`StorageCapacity`] to its proto form.
/// One construction site so the field mapping doesn't drift between
/// register, heartbeat, and any future capacity carriers.
fn storage_capacity_to_proto(c: &StorageCapacity) -> basis_proto::StorageCapacity {
    basis_proto::StorageCapacity {
        rootfs: Some(basis_proto::RootfsCapacity {
            total_bytes: c.rootfs.total,
            free_bytes: c.rootfs.free,
            metadata_total_bytes: c.rootfs.metadata_total,
            metadata_free_bytes: c.rootfs.metadata_free,
        }),
        pools: c.pools.iter().map(pool_capacity_to_proto).collect(),
    }
}

fn pool_capacity_to_proto(p: &basis_agent::lvm::PoolCapacity) -> basis_proto::PoolCapacity {
    basis_proto::PoolCapacity {
        pool: p.pool.clone(),
        backend: match p.backend {
            basis_agent::config::PoolBackend::LvmLinear => "lvm-linear".into(),
            basis_agent::config::PoolBackend::RawDisk => "raw-disk".into(),
            basis_agent::config::PoolBackend::NvmeNamespace => "nvme-namespace".into(),
        },
        labels: p
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        configured_total_bytes: p.configured_total_bytes,
        ready_total_bytes: p.ready_total_bytes,
        schedulable_total_bytes: p.schedulable_total_bytes,
        schedulable_free_bytes: p.schedulable_free_bytes,
        devices: p
            .devices
            .iter()
            .map(|d| basis_proto::DeviceCapacity {
                device_id: d.id.clone(),
                total_bytes: d.total_gib * (1 << 30),
                free_bytes: d.free_gib * (1 << 30),
                physical: d.physical.as_proto() as i32,
                physical_reason: d.physical_reason.clone(),
            })
            .collect(),
    }
}

async fn handshake(
    inbound: &mut Streaming<ControllerCommand>,
    runtime: &Arc<tokio::sync::RwLock<AgentRuntime>>,
    sender: &mpsc::Sender<AgentMessage>,
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

    // Apply cluster + VM reconcile and persist the host id while we
    // hold the read guard. Tombstones from the inline initial_state
    // ack via `sender` here — same path as periodic reconciles.
    {
        let rt = runtime.read().await;
        rt.agent_db.set_host_id(&host_id).await?;
        // Hand the host_id to the network manager BEFORE the first
        // reconcile so LAN-VIP proxy-ARP gating sees a non-empty self
        // identity. Empty would skip all proxy-ARP and the cell would
        // black-hole LAN ingress until next reconcile.
        rt.net_mgr.cluster_mgr().set_host_id(host_id.clone()).await;
        if let Some(initial) = initial {
            apply_reconcile(&rt, &initial, sender).await?;
        }
    }

    // Bring up the host BGP speaker against local gobgpd, peering
    // with the cell reflector. gobgpd runs as its own systemd
    // service — basis-agent restarts don't drop the session. BGP
    // setup is best-effort: a wedged or stale-state gobgpd is the
    // operator's problem to recover (e.g. `systemctl restart
    // gobgpd`), not a reason to crashloop the agent. The agent's
    // primary job is VM hosting; failing the handshake on a
    // sidecar concern would mask real handshake failures and lock
    // VMs out of reconciles for as long as gobgpd is unhealthy.
    let mut rt = runtime.write().await;
    let router_id = parse_ipv4(&rt.vtep_address, "vtep_address")?;
    let reflector = parse_ipv4(&bgp_reflector, "bgp_reflector_address")?;
    if rt.bgp_speaker.is_none() {
        match Speaker::start(SpeakerConfig {
            asn: bgp_asn,
            router_id,
            reflector_address: reflector,
            gobgpd_endpoint: rt.spec.gobgpd_endpoint.clone(),
        })
        .await
        {
            Ok(speaker) => rt.bgp_speaker = Some(speaker),
            Err(e) => warn!(
                error = %e,
                "BGP speaker start failed; agent continues without local BGP. \
                 Recover with `systemctl restart gobgpd` on this host."
            ),
        }
    }
    drop(rt);

    Ok(host_id)
}

fn parse_ipv4(s: &str, field: &str) -> anyhow::Result<std::net::Ipv4Addr> {
    s.parse()
        .with_context(|| format!("{field} '{s}' is not a valid IPv4 address"))
}

/// Apply a `ReconcileHostCommand` and emit a `TombstoneAck` if the
/// command carried any tombstones. Single path used by the handshake
/// ack, periodic pushes, and ad-hoc broadcasts.
///
/// Reconcile semantics are additive + tombstone-driven:
///   * `clusters[]` — ensure each exists locally with the right
///     bridge/VXLAN/FDB/VIP-route state. Untouched clusters not in
///     this list keep running (no implicit-by-absence delete).
///   * `cluster_tombstones[]` — explicit per-cluster teardown:
///     remove that cluster's contribution from proxy-ARP, delete
///     bridge + VXLAN, drop the per-cluster MASQUERADE rule.
///   * `vm_tombstones[]` — explicit per-VM teardown.
///
/// Successful tombstone application acks back so the controller can
/// drop the matching DB rows. Failures are swallowed (logged) — the
/// next reconcile re-emits the same tombstones so we self-heal.
///
/// Cluster overlay CIDRs are intentionally NOT advertised — VM
/// IPs are private to the cluster's bridge by design (no
/// inter-cluster L3 reachability). Only the `cluster_vips` set
/// (apiserver VIP when `APISERVER_PUBLIC` + LB Service block from a
/// `Lan`-scoped pool) is announced to the cell.
/// `internal_cluster_vips` (Tree-scoped pool VIPs) are deliberately
/// omitted from BGP — their reachability is established through
/// bridge routes installed by the agent's network reconciler in the
/// cluster's tree-VRF table, rather than via underlay routing.
/// Trust-domain isolation lives in those VRFs: every host
/// participating in tree T enslaves T's bridges to a deterministic
/// per-T Linux VRF and installs T's prefix routes in that VRF's
/// table; cross-tree traffic from a different tree's bridge fails to
/// find a route in its own tree's table and is dropped by the
/// kernel.
async fn apply_reconcile(
    rt: &AgentRuntime,
    cmd: &ReconcileHostCommand,
    sender: &mpsc::Sender<AgentMessage>,
) -> anyhow::Result<()> {
    rt.net_mgr.reconcile_clusters(&cmd.clusters).await?;

    let mut acked_cluster_vnis: Vec<u32> = Vec::with_capacity(cmd.cluster_tombstones.len());
    for tomb in &cmd.cluster_tombstones {
        match rt.net_mgr.tombstone_cluster(tomb.vni, &tomb.cidr).await {
            Ok(()) => acked_cluster_vnis.push(tomb.vni),
            Err(e) => warn!(
                vni = tomb.vni, cluster_id = %tomb.cluster_id, error = %e,
                "tombstone_cluster failed; controller will re-emit on next reconcile",
            ),
        }
    }

    let mut acked_vm_ids: Vec<String> = Vec::with_capacity(cmd.vm_tombstones.len());
    for vm_id in &cmd.vm_tombstones {
        match handlers::delete_vm(vm_id, &rt.vm_mgr, &rt.net_mgr, &rt.agent_db, &rt.storage).await {
            Ok(()) => acked_vm_ids.push(vm_id.clone()),
            Err(e) => warn!(
                vm_id = %vm_id, error = %e,
                "VM tombstone teardown failed; controller will re-emit on next reconcile",
            ),
        }
    }

    if !acked_cluster_vnis.is_empty() || !acked_vm_ids.is_empty() {
        let ack = TombstoneAck {
            cluster_vnis: acked_cluster_vnis,
            vm_ids: acked_vm_ids,
        };
        let msg = AgentMessage {
            payload: Some(agent_message::Payload::TombstoneAck(ack)),
        };
        if let Err(e) = sender.send(msg).await {
            warn!(error = %e,
                "TombstoneAck send failed (stream closed); next reconcile will re-emit \
                 and we'll re-ack on reconnect");
        }
    }

    if let Some(speaker) = rt.bgp_speaker.as_ref() {
        let mut prefixes: Vec<ipnet::Ipv4Net> = Vec::new();
        for cluster in &cmd.clusters {
            // Every carrier advertises every VIP via BGP regardless of
            // proxy-ARP ownership — BGP is the inter-host route plane,
            // proxy-ARP ownership is only the L2 LAN responder.
            for vip in &cluster.cluster_vips {
                match vip.cidr.parse::<ipnet::Ipv4Net>() {
                    Ok(p) => prefixes.push(p),
                    Err(_) => warn!(
                        vip = %vip.cidr, vni = cluster.vni,
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

fn spawn_heartbeat_loop(
    sender: mpsc::Sender<AgentMessage>,
    capacity: Arc<tokio::sync::RwLock<StorageCapacity>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            interval.tick().await;
            let snapshot = storage_capacity_to_proto(&*capacity.read().await);
            let msg = AgentMessage {
                payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {
                    storage_capacity: Some(snapshot),
                })),
            };
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });
}

fn spawn_storage_capacity_loop(
    storage: Arc<Storage>,
    shared: Arc<tokio::sync::RwLock<StorageCapacity>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(STORAGE_CAPACITY_INTERVAL);
        loop {
            interval.tick().await;
            match tokio::time::timeout(STORAGE_CAPACITY_TIMEOUT, storage.capacity()).await {
                Ok(Ok(c)) => {
                    let data_free_gib: u64 = c
                        .pools
                        .iter()
                        .map(|p| p.schedulable_free_bytes / (1 << 30))
                        .sum();
                    let data_total_gib: u64 = c
                        .pools
                        .iter()
                        .map(|p| p.schedulable_total_bytes / (1 << 30))
                        .sum();
                    info!(
                        rootfs_data_free_gib = c.rootfs.free / (1 << 30),
                        rootfs_data_total_gib = c.rootfs.total / (1 << 30),
                        rootfs_metadata_free_mib = c.rootfs.metadata_free / (1 << 20),
                        rootfs_metadata_total_mib = c.rootfs.metadata_total / (1 << 20),
                        data_pool_count = c.pools.len(),
                        data_free_gib,
                        data_total_gib,
                        "storage capacity"
                    );
                    // Same snapshot drives both the heartbeat-bound
                    // shared state and the Prometheus gauges, so the
                    // two views can never disagree.
                    if let Some(m) = basis_agent::metrics::global() {
                        basis_agent::metrics::refresh_storage_gauges(m, &c);
                    }
                    *shared.write().await = c;
                }
                Ok(Err(e)) => warn!(error = %e, "reading storage capacity"),
                Err(_) => warn!(
                    timeout_secs = STORAGE_CAPACITY_TIMEOUT.as_secs(),
                    "storage capacity query timed out — LVM likely busy"
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
                match reconcile::periodic_sweep(
                    &rt.agent_db,
                    &rt.vm_mgr,
                    rt.net_mgr.as_ref(),
                    rt.storage.as_ref(),
                )
                .await
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
                    Some(controller_command::Command::ReconcileHost(reconcile_cmd)) => {
                        spawn_reconcile(*reconcile_cmd, runtime.clone(), sender.clone());
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
            rt.storage.as_ref(),
            rt.metrics.as_ref(),
            &sender,
        )
        .await;

        let (state, err, transient) = match result {
            Ok(()) => (MachineState::Running, String::new(), false),
            Err(e) => {
                let transient = e.chain().any(|c| {
                    matches!(
                        c.downcast_ref::<basis_agent::lvm::LvmError>(),
                        Some(basis_agent::lvm::LvmError::Busy { .. })
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

fn spawn_reconcile(
    cmd: ReconcileHostCommand,
    runtime: Arc<tokio::sync::RwLock<AgentRuntime>>,
    sender: mpsc::Sender<AgentMessage>,
) {
    tokio::spawn(async move {
        let rt = runtime.read().await;
        if let Err(e) = apply_reconcile(&rt, &cmd, &sender).await {
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
    storage: Arc<Storage>,
    metrics: Arc<Metrics>,
}

impl AgentRuntime {
    fn clone_for_task(&self) -> TaskContext {
        TaskContext {
            agent_db: self.agent_db.clone(),
            image_mgr: self.image_mgr.clone(),
            vm_mgr: self.vm_mgr.clone(),
            net_mgr: self.net_mgr.clone(),
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
