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
/// heartbeat. 240s = 4 minutes = eight heartbeat intervals.
///
/// Sized deliberately against k8s's pod-eviction timeline so the
/// replacement node is provisioned and joined *before* pods need a
/// place to land:
///
/// ```text
///   T=0     host dies
///   T=40s   k8s marks Node NotReady (node-monitor-grace-period)
///   T=240s  basis cascades VMs to Failed → CAPI creates replacement
///   T=~300s new VM provisioning + kubeadm join completes
///   T=340s  k8s evicts pods from the dead Node (default
///           tolerationSeconds=300 after NotReady)
///   →       evicted pods schedule onto the fresh node we just created
/// ```
///
/// Also chosen to tolerate false positives from short-lived outages
/// (basis-agent crash-restart, network blip): 4 minutes is longer than
/// the recovery window for every normal transient we've seen, so a
/// cascade that fires always represents a real host-gone event.
const HEARTBEAT_STALE_THRESHOLD: Duration = Duration::from_secs(240);

/// Message stamped on VMs failed out due to host heartbeat loss.
/// Stable on purpose — CAPI operators greping for this reason in
/// BasisMachine.status.failureMessage is the intended signal that
/// "this VM didn't fail on its own, the host it lived on went away."
const HOST_STALE_VM_REASON: &str =
    "host heartbeat stale — VM presumed lost, replace via CAPI reconcile";

/// Background task that periodically checks for stale hosts and
/// marks them — and every VM they owned — as failed. Flipping the
/// host flag alone stops the scheduler from placing new VMs there;
/// flipping the VMs is what gives CAPI the signal it needs to
/// remediate. Without this, VMs on a dead host stay in `Running`
/// forever and CAPI never creates replacements.
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
                let stale = match db.mark_stale_hosts_unhealthy(&cutoff).await {
                    Ok(stale) => stale,
                    Err(e) => {
                        warn!(error = %e, "failed to check host health");
                        continue;
                    }
                };
                for host_id in &stale {
                    match db
                        .mark_vms_failed_on_host(host_id, HOST_STALE_VM_REASON)
                        .await
                    {
                        Ok(0) => {
                            warn!(host_id, "marked host unhealthy (no VMs to fail)");
                        }
                        Ok(n) => {
                            warn!(
                                host_id,
                                vms_failed = n,
                                "marked host unhealthy, cascaded VMs to FAILED for CAPI remediation"
                            );
                        }
                        Err(e) => {
                            warn!(
                                host_id,
                                error = %e,
                                "marked host unhealthy but failed to cascade VM failures; \
                                 CAPI may not see the replacement signal until next tick"
                            );
                        }
                    }
                }
            }
        }
    }
}
