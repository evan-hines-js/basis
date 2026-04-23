use std::time::Duration;

use basis_common::time::rfc3339_ago;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::Db;

/// How often the controller scans for stale hosts. Must match the
/// agent's `HEARTBEAT_INTERVAL` so the controller doesn't reap a host
/// that's about to send a heartbeat.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// A host is marked unhealthy after missing this much continuous
/// heartbeat — exactly three intervals at 30 s each. Three was chosen
/// to absorb a single missed beat (network blip, agent restart) plus
/// one slow tick, while still flipping unhealthy within ~90 s of a
/// genuine outage so CAPI can begin remediation.
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
