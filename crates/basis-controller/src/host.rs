use std::time::Duration;

use basis_common::time::rfc3339_ago;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::Db;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_STALE_THRESHOLD: Duration = Duration::from_secs(90);

/// Background task that periodically checks for stale hosts and marks them unhealthy.
pub async fn host_health_checker(db: Db, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("host health checker shutting down");
                return;
            }
            _ = interval.tick() => {
                let cutoff = rfc3339_ago(HEARTBEAT_STALE_THRESHOLD);
                match db.mark_stale_hosts_unhealthy(&cutoff).await {
                    Ok(stale) => {
                        for host_id in &stale {
                            warn!(host_id, "marked host unhealthy (missed heartbeats)");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to check host health");
                    }
                }
            }
        }
    }
}
