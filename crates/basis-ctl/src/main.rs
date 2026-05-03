//! `basis-ctl` — talk to a Basis controller the same way `basis-capi-provider`
//! does, but from the command line. Intended for tests and bring-up; not
//! part of any production path.
//!
//! Resources are declared as YAML. The CLI is deliberately thin — it
//! parses YAML, maps to `basis_client::MachineRequest` / etc., and calls
//! the gRPC. There's no reconcile loop, no state, no retries beyond what
//! the controller already handles (name-based idempotency on
//! `CreateCluster` / `CreateMachine`).
//!
//! Usage examples:
//!   basis-ctl apply  -f fixtures/cluster.yaml
//!   basis-ctl apply  -f fixtures/machine-debug.yaml
//!   basis-ctl get    machines --cluster <id>
//!   basis-ctl delete -f fixtures/machine-debug.yaml

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use basis_client::BasisClient;
use basis_common::tls::TlsConfig;
use basis_proto::MachineState;
use clap::{Parser, Subcommand};

mod resources;
use resources::{load_file, Resource};

#[derive(Parser)]
#[command(name = "basis-ctl", about = "Basis controller CLI")]
struct Cli {
    /// gRPC endpoint of the Basis controller (e.g. https://10.0.0.206:7443).
    #[arg(long, env = "BASIS_ENDPOINT", global = true)]
    endpoint: Option<String>,

    /// Client cert. CN must be `basis-capi-provider`.
    #[arg(long, env = "BASIS_TLS_CERT", global = true)]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "BASIS_TLS_KEY", global = true)]
    tls_key: Option<PathBuf>,

    #[arg(long, env = "BASIS_TLS_CA", global = true)]
    tls_ca: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create (idempotent) the resources in a YAML file.
    Apply {
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    /// Delete the resources in a YAML file.
    Delete {
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    /// List machines, optionally filtered by cluster.
    GetMachines {
        #[arg(long)]
        cluster: Option<String>,
    },
    /// Storage pool subcommands.
    #[command(subcommand)]
    Pool(PoolCmd),
}

#[derive(Subcommand)]
enum PoolCmd {
    /// One-line-per-pool summary across every host (or one host).
    List {
        #[arg(long)]
        host: Option<String>,
    },
    /// Full per-device breakdown for one pool.
    Show {
        /// `<host>/<pool>` selector.
        target: String,
    },
    /// Global health view; degraded/unhealthy pools and devices.
    Health,
    /// Mark a device disabled: the controller's scheduler stops
    /// placing new disks on it. Live reservations are NOT touched —
    /// rebalancing data off an OSD is the storage system's job
    /// (Ceph/Rook/Longhorn/etc), not basis's. This flag exists so
    /// operators can fence a drive ahead of physical replacement
    /// without basis racing the eviction.
    Disable {
        /// `<host>/<pool>/<device>` selector.
        target: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Re-enable a previously disabled device. Placement resumes once
    /// the device is also physically `Ready`.
    Enable { target: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cli = Cli::parse();
    let client = connect(&cli)?;
    match cli.cmd {
        Command::Apply { file } => apply(&client, &file).await,
        Command::Delete { file } => delete(&client, &file).await,
        Command::GetMachines { cluster } => get_machines(&client, cluster).await,
        Command::Pool(p) => match p {
            PoolCmd::List { host } => pool_list(&client, host).await,
            PoolCmd::Show { target } => pool_show(&client, &target).await,
            PoolCmd::Health => pool_health(&client).await,
            PoolCmd::Disable { target, reason } => pool_disable(&client, &target, reason).await,
            PoolCmd::Enable { target } => pool_enable(&client, &target).await,
        },
    }
}

fn parse_host_pool(s: &str) -> Result<(String, String)> {
    let (host, pool) = s
        .split_once('/')
        .with_context(|| format!("expected <host>/<pool>, got {s:?}"))?;
    Ok((host.to_string(), pool.to_string()))
}

fn parse_host_pool_device(s: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = s.splitn(3, '/').collect();
    if parts.len() != 3 {
        anyhow::bail!("expected <host>/<pool>/<device>, got {s:?}");
    }
    Ok((parts[0].into(), parts[1].into(), parts[2].into()))
}

async fn pool_list(client: &BasisClient, host: Option<String>) -> Result<()> {
    let pools = client.list_pools(host.unwrap_or_default()).await?;
    println!(
        "{:24} {:16} {:14} {:>10} {:>10}  LABELS",
        "HOST", "POOL", "BACKEND", "FREE_GIB", "TOTAL_GIB"
    );
    for p in pools {
        let labels = p
            .labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:24} {:16} {:14} {:>10} {:>10}  {}",
            p.host_id,
            p.pool,
            p.backend,
            p.schedulable_free_bytes / (1 << 30),
            p.schedulable_total_bytes / (1 << 30),
            labels
        );
    }
    Ok(())
}

async fn pool_show(client: &BasisClient, target: &str) -> Result<()> {
    let (host, pool) = parse_host_pool(target)?;
    let pools = client.list_pools(host.clone()).await?;
    let p = pools
        .into_iter()
        .find(|p| p.pool == pool)
        .with_context(|| format!("pool {pool:?} not found on host {host:?}"))?;
    println!("Host:           {}", p.host_id);
    println!("Pool:           {}", p.pool);
    println!("Backend:        {}", p.backend);
    println!(
        "Capacity (GiB): configured={} ready={} schedulable_total={} schedulable_free={}",
        p.configured_total_bytes / (1 << 30),
        p.ready_total_bytes / (1 << 30),
        p.schedulable_total_bytes / (1 << 30),
        p.schedulable_free_bytes / (1 << 30),
    );
    println!("Labels:");
    for (k, v) in &p.labels {
        println!("  {k}: {v}");
    }
    println!("Devices:");
    for d in &p.devices {
        let physical = match basis_proto::DevicePhysicalHealth::try_from(d.physical) {
            Ok(basis_proto::DevicePhysicalHealth::DeviceHealthReady) => "Ready",
            Ok(basis_proto::DevicePhysicalHealth::DeviceHealthDegraded) => "Degraded",
            Ok(basis_proto::DevicePhysicalHealth::DeviceHealthMissing) => "Missing",
            _ => "?",
        };
        println!(
            "  {}  free={}GiB total={}GiB physical={}{}{}  scheduling={}{}",
            d.device_id,
            d.free_bytes / (1 << 30),
            d.total_bytes / (1 << 30),
            physical,
            if d.physical_reason.is_empty() {
                ""
            } else {
                " ("
            },
            if d.physical_reason.is_empty() {
                String::new()
            } else {
                format!("{})", d.physical_reason)
            },
            d.scheduling_state,
            if d.scheduling_reason.is_empty() {
                String::new()
            } else {
                format!(" ({})", d.scheduling_reason)
            },
        );
        for r in &d.reservations {
            println!("      cluster={} count={}", r.cluster_id, r.count);
        }
    }
    Ok(())
}

async fn pool_health(client: &BasisClient) -> Result<()> {
    let pools = client.list_pools(String::new()).await?;
    for p in pools {
        let pool_state = match basis_proto::PoolHealthState::try_from(p.pool_health) {
            Ok(basis_proto::PoolHealthState::PoolHealthReady) => "Ready",
            Ok(basis_proto::PoolHealthState::PoolHealthDegraded) => "Degraded",
            Ok(basis_proto::PoolHealthState::PoolHealthUnhealthy) => "Unhealthy",
            _ => "?",
        };
        if pool_state == "Ready" {
            continue;
        }
        println!("{}/{}: {}", p.host_id, p.pool, pool_state);
        for d in &p.devices {
            let physical = match basis_proto::DevicePhysicalHealth::try_from(d.physical) {
                Ok(basis_proto::DevicePhysicalHealth::DeviceHealthReady) => "Ready",
                Ok(basis_proto::DevicePhysicalHealth::DeviceHealthDegraded) => "Degraded",
                Ok(basis_proto::DevicePhysicalHealth::DeviceHealthMissing) => "Missing",
                _ => "?",
            };
            if physical == "Ready" && d.scheduling_state == "enabled" {
                continue;
            }
            println!(
                "  device={}  physical={} ({})  scheduling={}",
                d.device_id, physical, d.physical_reason, d.scheduling_state,
            );
        }
    }
    Ok(())
}

async fn pool_disable(client: &BasisClient, target: &str, reason: Option<String>) -> Result<()> {
    let (host, pool, device) = parse_host_pool_device(target)?;
    client
        .set_device_scheduling_state(
            host,
            pool,
            device,
            "disabled".into(),
            reason.unwrap_or_default(),
        )
        .await?;
    println!("ok");
    Ok(())
}

async fn pool_enable(client: &BasisClient, target: &str) -> Result<()> {
    let (host, pool, device) = parse_host_pool_device(target)?;
    client
        .set_device_scheduling_state(host, pool, device, "enabled".into(), String::new())
        .await?;
    println!("ok");
    Ok(())
}

fn connect(cli: &Cli) -> Result<BasisClient> {
    let endpoint = cli
        .endpoint
        .clone()
        .context("--endpoint / BASIS_ENDPOINT required")?;
    let tls = TlsConfig {
        cert: cli
            .tls_cert
            .clone()
            .context("--tls-cert / BASIS_TLS_CERT required")?,
        key: cli
            .tls_key
            .clone()
            .context("--tls-key / BASIS_TLS_KEY required")?,
        ca: cli
            .tls_ca
            .clone()
            .context("--tls-ca / BASIS_TLS_CA required")?,
    };
    Ok(BasisClient::new(endpoint, tls.load_identity()?))
}

async fn apply(client: &BasisClient, file: &Path) -> Result<()> {
    for resource in load_file(file)? {
        match resource {
            Resource::Cluster(c) => {
                let created = client
                    .create_cluster(basis_client::ClusterRequest {
                        name: c.metadata.name.clone(),
                        external_ip_pool: c.spec.external_ip_pool.clone(),
                        external_service_ips: c.spec.external_service_ips,
                        apiserver_visibility: c.spec.apiserver_visibility,
                        trust_domain: c.spec.trust_domain.clone().unwrap_or_default(),
                    })
                    .await?;
                println!(
                    "cluster  {name:30}  id={id}  vni={vni}  cidr={cidr}  endpoint={ep}  services={svc}",
                    name = c.metadata.name,
                    id = created.cluster_id,
                    vni = created.vni,
                    cidr = created.cidr,
                    ep = created.control_plane_endpoint,
                    svc = if created.service_block_cidr.is_empty() {
                        "none".to_string()
                    } else {
                        created.service_block_cidr.clone()
                    },
                );
            }
            Resource::Machine(m) => {
                let cluster_id = find_cluster_id(client, &m.spec.cluster).await?;
                let mut req = m.spec.to_request(&m.metadata.name, file)?;
                req.cluster_id = cluster_id;
                let created = client.create_machine(req).await?;
                println!(
                    "machine  {name:30}  id={id}  ip={ip}  provider={pid}",
                    name = m.metadata.name,
                    id = created.id,
                    ip = created.ip_address,
                    pid = created.provider_id,
                );
            }
        }
    }
    Ok(())
}

async fn delete(client: &BasisClient, file: &Path) -> Result<()> {
    // Delete machines first, then clusters — cluster delete cascades
    // VMs, but if a fixture has both, explicit deletion of each makes
    // the per-resource output readable.
    let mut machines = Vec::new();
    let mut clusters = Vec::new();
    for r in load_file(file)? {
        match r {
            Resource::Machine(m) => machines.push(m),
            Resource::Cluster(c) => clusters.push(c),
        }
    }

    // Snapshot cluster id + machine id ahead of time via ListClusters /
    // ListMachines. Both are read-only, so if a resource is already
    // absent we skip without accidentally re-creating it.
    let known_clusters = client.list_clusters().await?;
    let find_cluster = |name: &str| -> Option<String> {
        known_clusters
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.cluster_id.clone())
    };

    for m in machines {
        let Some(cluster_id) = find_cluster(&m.spec.cluster) else {
            println!(
                "machine  {:30}  (cluster '{}' not found, skipping)",
                m.metadata.name, m.spec.cluster
            );
            continue;
        };
        let found = client
            .list_machines(cluster_id)
            .await?
            .into_iter()
            .find(|mm| mm.name == m.metadata.name);
        let Some(mm) = found else {
            println!("machine  {:30}  (not found, skipping)", m.metadata.name);
            continue;
        };
        client.delete_machine(mm.id.clone()).await?;
        println!("machine  {:30}  id={}  deleted", m.metadata.name, mm.id);
    }
    for c in clusters {
        let Some(id) = find_cluster(&c.metadata.name) else {
            println!("cluster  {:30}  (not found, skipping)", c.metadata.name);
            continue;
        };
        client.delete_cluster(id.clone()).await?;
        println!("cluster  {:30}  id={id}  deleted", c.metadata.name);
    }
    Ok(())
}

async fn get_machines(client: &BasisClient, cluster: Option<String>) -> Result<()> {
    let machines = client.list_machines(cluster.unwrap_or_default()).await?;
    if machines.is_empty() {
        println!("(no machines)");
        return Ok(());
    }
    println!(
        "{:36}  {:32}  {:10}  {:15}  HOST",
        "ID", "NAME", "STATE", "IP",
    );
    for m in machines {
        println!(
            "{:36}  {:32}  {:10}  {:15}  {}",
            m.id,
            m.name,
            state_name(m.state),
            m.ip_address,
            m.host,
        );
    }
    Ok(())
}

/// Read-only name → id lookup via ListClusters. The cluster must
/// already exist (applied separately); callers get an error if not.
/// Never mutates — safe on both apply and delete paths.
async fn find_cluster_id(client: &BasisClient, name: &str) -> Result<String> {
    client
        .list_clusters()
        .await?
        .into_iter()
        .find(|c| c.name == name)
        .map(|c| c.cluster_id)
        .with_context(|| format!("cluster '{name}' does not exist — apply its YAML first"))
}

fn state_name(state: i32) -> &'static str {
    MachineState::try_from(state)
        .map(|s| s.as_str_name())
        .unwrap_or("UNKNOWN")
}
