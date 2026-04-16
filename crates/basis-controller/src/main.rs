use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use basis_controller::config::ControllerConfig;
use basis_controller::db::Db;

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
        .with_env_filter(EnvFilter::from_default_env().add_directive("basis=info".parse().unwrap()))
        .init();

    let cli = Cli::parse();

    let config = ControllerConfig::load(&cli.config)?;
    info!(listen = %config.listen, data_dir = %config.data_dir.display(), "loaded config");

    std::fs::create_dir_all(&config.data_dir)?;

    let db = Db::open(&config.db_path()).await?;
    info!(path = %config.db_path().display(), "database ready");

    basis_controller::ip::seed_ip_pools(&db, &config.ip_pools).await?;
    info!(count = config.ip_pools.len(), "IP pools seeded");

    let shutdown = CancellationToken::new();

    let health_db = db.clone();
    let health_shutdown = shutdown.clone();
    tokio::spawn(async move {
        basis_controller::host::host_health_checker(health_db, health_shutdown).await;
    });

    let addr = config.listen.parse()?;
    let basis_server = basis_controller::server::BasisServer::new(db);
    basis_server.serve(addr, &config, shutdown.clone()).await?;

    Ok(())
}
