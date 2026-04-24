use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use basis_controller::config::BasisControllerSpec;
use basis_controller::db::Db;
use basis_controller::metrics::Metrics;

#[derive(Parser)]
#[command(name = "basis-controller", about = "Basis hypervisor controller")]
struct Cli {
    #[arg(short, long, default_value = "/etc/basis/controller.toml")]
    config: PathBuf,
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

    let config = BasisControllerSpec::load(&cli.config)?;
    config.validate()?;
    info!(
        listen = %config.listen,
        data_dir = %config.data_dir.display(),
        cpu_overcommit_ratio = config.cpu_overcommit_ratio,
        "loaded config",
    );

    std::fs::create_dir_all(&config.data_dir)?;

    let db = Db::open(&config.db_path()).await?;
    info!(path = %config.db_path().display(), "database ready");

    let shutdown = CancellationToken::new();

    let metrics = Metrics::new(config.cpu_overcommit_ratio)?;

    let health_db = db.clone();
    let health_shutdown = shutdown.clone();
    tokio::spawn(async move {
        basis_controller::host::host_health_checker(health_db, health_shutdown).await;
    });

    let poller_metrics = metrics.clone();
    let poller_db = db.clone();
    let poller_shutdown = shutdown.clone();
    tokio::spawn(async move {
        basis_controller::metrics::run_poller(poller_metrics, poller_db, poller_shutdown).await;
    });

    let metrics_server_metrics = metrics.clone();
    let metrics_server_listen = config.metrics_listen.clone();
    let metrics_server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = basis_controller::metrics::run_server(
            metrics_server_metrics,
            &metrics_server_listen,
            metrics_server_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "metrics server exited with error");
        }
    });

    let listener = TcpListener::bind(&config.listen).await?;
    let tls_config = config.tls.server_config()?;
    basis_controller::server::BasisServer::new(
        db,
        metrics,
        config.dns_servers,
        config.network,
        config.cpu_overcommit_ratio,
    )
    .serve(listener, tls_config, shutdown)
    .await?;

    Ok(())
}
