//! Entry point for basis-capi-provider.
//!
//! The provider is a thin translator between CAPI CRDs and the Basis
//! controller's gRPC API. It holds no infrastructure credentials at
//! startup — every `BasisCluster` names a `credentialsRef` Secret that
//! carries its own basis-controller endpoint + mTLS identity, and the
//! reconcilers resolve a `BasisClient` per-cluster on demand. That's
//! why the pod has zero CLI args for connection info: there's no global
//! "the controller" to point at, and one install of the provider can
//! drive clusters that target different basis-controllers.

use std::sync::Arc;

use axum::{routing::get, Router};
use basis_capi_provider::client_cache::BasisClientCache;
use basis_capi_provider::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};
use basis_capi_provider::{cluster, machine, startup};
use clap::Parser;
use kube::{Client, CustomResourceExt};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

const HEALTH_ADDR: &str = "0.0.0.0:9443";

#[derive(Parser)]
#[command(
    name = "basis-capi-provider",
    about = "CAPI infrastructure provider for Basis"
)]
struct Cli {
    /// Print every CRD this binary manages as a multi-document YAML stream,
    /// then exit. Pipe to `kubectl apply -f -` to install them.
    #[arg(long)]
    print_crds: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.print_crds {
        return print_crds();
    }
    run().await
}

async fn run() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Default filter: our crates at info + kube-runtime/kube-client at warn.
    // The latter is critical — watcher list/watch failures, CRD-not-found
    // backoff, and deserialize errors all log at `warn` under those targets.
    // Without them, the provider can sit in silent 5-minute exponential
    // backoff (e.g. while waiting for its own CRDs to register) with zero
    // output. `RUST_LOG` still wins when set, so `RUST_LOG=debug` works as
    // expected for deeper investigation.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,basis=info,kube_runtime=warn,kube_client=warn")
        }))
        .init();

    info!("starting basis-capi-provider");

    let kube = Client::try_default().await?;

    // Block on CRDs being Established before starting reconcilers. Without
    // this, a pod that wins the race against the CRD install sits in
    // kube-runtime's exponential list/watch backoff for up to ~5 minutes
    // per watched kind — visible to no one except the watcher itself.
    startup::wait_for_crds(&kube).await?;

    let clients = Arc::new(BasisClientCache::new(kube.clone()));

    let cluster_task = tokio::spawn(cluster::run(kube.clone(), clients.clone()));
    let machine_task = tokio::spawn(machine::run(kube.clone(), clients.clone()));
    let health_task = tokio::spawn(serve_health());

    tokio::try_join!(
        flatten(cluster_task),
        flatten(machine_task),
        flatten(health_task),
    )?;
    Ok(())
}

async fn serve_health() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }));
    let listener = TcpListener::bind(HEALTH_ADDR).await?;
    info!(addr = HEALTH_ADDR, "health server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn print_crds() -> anyhow::Result<()> {
    // One multi-doc YAML stream so the whole output can be piped to
    // `kubectl apply -f -`. Order matters only for readability.
    print_crd::<BasisCluster>()?;
    print_crd::<BasisMachineTemplate>()?;
    print_crd::<BasisMachine>()?;
    Ok(())
}

fn print_crd<T: CustomResourceExt>() -> anyhow::Result<()> {
    let yaml = serde_yaml_ng::to_string(&T::crd())?;
    println!("---");
    print!("{yaml}");
    Ok(())
}

async fn flatten(handle: tokio::task::JoinHandle<anyhow::Result<()>>) -> anyhow::Result<()> {
    match handle.await {
        Ok(res) => res,
        Err(e) => Err(anyhow::anyhow!("reconciler task panicked: {e}")),
    }
}
