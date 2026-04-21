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

use std::path::PathBuf;

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
    }
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

async fn apply(client: &BasisClient, file: &PathBuf) -> Result<()> {
    for resource in load_file(file)? {
        match resource {
            Resource::Cluster(c) => {
                let created = client
                    .create_cluster(c.metadata.name.clone(), c.spec.ip_pool.clone())
                    .await?;
                println!(
                    "cluster  {name:30}  id={id}  endpoint={ep}",
                    name = c.metadata.name,
                    id = created.cluster_id,
                    ep = created.control_plane_endpoint,
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

async fn delete(client: &BasisClient, file: &PathBuf) -> Result<()> {
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
        "{:36}  {:32}  {:10}  {:15}  {}",
        "ID", "NAME", "STATE", "IP", "HOST"
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
