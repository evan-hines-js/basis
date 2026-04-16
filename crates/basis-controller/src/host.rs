use std::time::Duration;

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
                let cutoff = chrono_now_minus(HEARTBEAT_STALE_THRESHOLD);
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

fn chrono_now_minus(duration: Duration) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cutoff = now.saturating_sub(duration.as_secs());

    // Format as ISO 8601 - basic but sufficient for SQLite string comparison
    let dt = std::time::UNIX_EPOCH + std::time::Duration::from_secs(cutoff);
    humantime::format_rfc3339_seconds(dt).to_string()
}

pub fn now_rfc3339() -> String {
    humantime::format_rfc3339_seconds(std::time::SystemTime::now()).to_string()
}
