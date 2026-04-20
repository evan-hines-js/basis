use std::path::PathBuf;
use std::sync::Arc;

use axum::{routing::get, Router};
use basis_capi_provider::basis_client::BasisClient;
use basis_capi_provider::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};
use basis_capi_provider::{cluster, machine};
use clap::Parser;
use kube::{Client, CustomResourceExt};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

const HEALTH_ADDR: &str = "0.0.0.0:9443";

#[derive(Parser)]
#[command(name = "basis-capi-provider", about = "CAPI infrastructure provider for Basis")]
struct Cli {
    /// Print every CRD this binary manages as a multi-document YAML stream,
    /// then exit. Pipe to `kubectl apply -f -` to install them.
    #[arg(long)]
    print_crds: bool,

    /// gRPC endpoint of the Basis controller.
    #[arg(long, env = "BASIS_CONTROLLER_ENDPOINT", required_unless_present = "print_crds")]
    controller_endpoint: Option<String>,

    /// Client cert used to authenticate with the Basis controller.
    /// CN MUST be `basis-capi-provider`.
    #[arg(long, env = "BASIS_TLS_CERT", required_unless_present = "print_crds")]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "BASIS_TLS_KEY", required_unless_present = "print_crds")]
    tls_key: Option<PathBuf>,

    #[arg(long, env = "BASIS_TLS_CA", required_unless_present = "print_crds")]
    tls_ca: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.print_crds {
        return print_crds();
    }
    run(cli).await
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("basis=info".parse().expect("static directive string")),
        )
        .init();

    let controller_endpoint = cli.controller_endpoint.expect("required by clap");
    let tls_cert = cli.tls_cert.expect("required by clap");
    let tls_key = cli.tls_key.expect("required by clap");
    let tls_ca = cli.tls_ca.expect("required by clap");

    info!(controller = %controller_endpoint, "starting basis-capi-provider");

    let basis = Arc::new(BasisClient::new(
        controller_endpoint,
        tls_cert,
        tls_key,
        tls_ca,
    ));

    let kube = Client::try_default().await?;

    let cluster_task = tokio::spawn(cluster::run(kube.clone(), basis.clone()));
    let machine_task = tokio::spawn(machine::run(kube.clone(), basis.clone()));
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
