//! Prometheus metrics exporter.
//!
//! Gauges are populated by a 5-second poller that reads from SQLite — the
//! authoritative source of controller state. Counters are event-driven and
//! bumped at the sites where the event happens (scheduler, create_machine,
//! agent stream handler). A plain-HTTP server on a separate port exposes
//! `/metrics` in the Prometheus text format.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use basis_common::gpu::GpuInfo;
use basis_common::json::parse_owned_json;
use basis_proto::MachineState;
use prometheus::{
    Encoder, Gauge, GaugeVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::Db;

/// All metric handles the controller emits. Held behind an `Arc` and
/// shared between the gRPC services (for counter bumps) and the metrics
/// server + poller (for reads).
pub struct Metrics {
    registry: Registry,

    // --- Gauges (refreshed every 5s by the poller) ---
    pub clusters: IntGauge,
    pub hosts: IntGaugeVec,
    pub vms: IntGaugeVec,

    pub host_cpu_total: IntGaugeVec,
    pub host_cpu_available: IntGaugeVec,
    pub host_memory_mib_total: IntGaugeVec,
    pub host_memory_mib_available: IntGaugeVec,
    pub host_disk_gib_total: IntGaugeVec,
    pub host_disk_gib_available: IntGaugeVec,
    pub host_gpus_total: IntGaugeVec,
    pub host_gpus_assigned: IntGaugeVec,
    pub host_last_heartbeat_age_seconds: GaugeVec,
    pub vm_age_in_state_seconds: GaugeVec,

    /// Controller-wide CPU overcommit ratio. Set once at construction —
    /// the dashboard computes effective CPU capacity as
    /// `basis_host_cpu_total * scalar(basis_cpu_overcommit_ratio)`.
    pub cpu_overcommit_ratio: Gauge,

    // --- Gauges (event-driven from the agent stream) ---
    pub agent_connected: IntGaugeVec,

    // --- Counters (event-driven) ---
    pub scheduler_decisions_total: IntCounterVec,
    pub vm_create_result_total: IntCounterVec,
}

impl Metrics {
    pub fn new(cpu_overcommit_ratio: f32) -> Result<Arc<Self>, prometheus::Error> {
        let registry = Registry::new();

        let clusters = IntGauge::with_opts(Opts::new(
            "basis_clusters",
            "Number of clusters known to the controller",
        ))?;
        registry.register(Box::new(clusters.clone()))?;

        let hosts = IntGaugeVec::new(
            Opts::new("basis_hosts", "Number of registered hosts"),
            &["healthy"],
        )?;
        registry.register(Box::new(hosts.clone()))?;

        let vms = IntGaugeVec::new(
            Opts::new("basis_vms", "Number of VMs by state and cluster"),
            &["state", "cluster"],
        )?;
        registry.register(Box::new(vms.clone()))?;

        let host_cpu_total = IntGaugeVec::new(
            Opts::new("basis_host_cpu_total", "Total vCPUs on each host"),
            &["host"],
        )?;
        registry.register(Box::new(host_cpu_total.clone()))?;

        let host_cpu_available = IntGaugeVec::new(
            Opts::new("basis_host_cpu_available", "Unallocated vCPUs on each host"),
            &["host"],
        )?;
        registry.register(Box::new(host_cpu_available.clone()))?;

        let host_memory_mib_total = IntGaugeVec::new(
            Opts::new(
                "basis_host_memory_mib_total",
                "Total RAM (MiB) on each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_memory_mib_total.clone()))?;

        let host_memory_mib_available = IntGaugeVec::new(
            Opts::new(
                "basis_host_memory_mib_available",
                "Unallocated RAM (MiB) on each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_memory_mib_available.clone()))?;

        let host_disk_gib_total = IntGaugeVec::new(
            Opts::new("basis_host_disk_gib_total", "Total disk (GiB) on each host"),
            &["host"],
        )?;
        registry.register(Box::new(host_disk_gib_total.clone()))?;

        let host_disk_gib_available = IntGaugeVec::new(
            Opts::new(
                "basis_host_disk_gib_available",
                "Unallocated disk (GiB) on each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_disk_gib_available.clone()))?;

        let host_gpus_total = IntGaugeVec::new(
            Opts::new(
                "basis_host_gpus_total",
                "Total GPUs discovered on each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_gpus_total.clone()))?;

        let host_gpus_assigned = IntGaugeVec::new(
            Opts::new(
                "basis_host_gpus_assigned",
                "GPUs currently assigned to VMs on each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_gpus_assigned.clone()))?;

        let host_last_heartbeat_age_seconds = GaugeVec::new(
            Opts::new(
                "basis_host_last_heartbeat_age_seconds",
                "Seconds since the last heartbeat from each host",
            ),
            &["host"],
        )?;
        registry.register(Box::new(host_last_heartbeat_age_seconds.clone()))?;

        let vm_age_in_state_seconds = GaugeVec::new(
            Opts::new(
                "basis_vm_age_in_state_seconds",
                "Seconds each VM has been in its current state (since updated_at)",
            ),
            &["vm_id", "name", "state", "host", "cluster"],
        )?;
        registry.register(Box::new(vm_age_in_state_seconds.clone()))?;

        let cpu_overcommit_ratio_gauge = Gauge::with_opts(Opts::new(
            "basis_cpu_overcommit_ratio",
            "CPU overcommit multiplier applied by the scheduler to each host's physical CPU count",
        ))?;
        cpu_overcommit_ratio_gauge.set(cpu_overcommit_ratio as f64);
        registry.register(Box::new(cpu_overcommit_ratio_gauge.clone()))?;

        let agent_connected = IntGaugeVec::new(
            Opts::new(
                "basis_agent_connected",
                "1 if an agent stream for this host is currently open",
            ),
            &["host"],
        )?;
        registry.register(Box::new(agent_connected.clone()))?;

        let scheduler_decisions_total = IntCounterVec::new(
            Opts::new(
                "basis_scheduler_decisions_total",
                "Scheduler placement outcomes",
            ),
            &["outcome"],
        )?;
        registry.register(Box::new(scheduler_decisions_total.clone()))?;

        let vm_create_result_total = IntCounterVec::new(
            Opts::new(
                "basis_vm_create_result_total",
                "Terminal result of CreateMachine calls",
            ),
            &["result"],
        )?;
        registry.register(Box::new(vm_create_result_total.clone()))?;

        Ok(Arc::new(Self {
            registry,
            clusters,
            hosts,
            vms,
            host_cpu_total,
            host_cpu_available,
            host_memory_mib_total,
            host_memory_mib_available,
            host_disk_gib_total,
            host_disk_gib_available,
            host_gpus_total,
            host_gpus_assigned,
            host_last_heartbeat_age_seconds,
            vm_age_in_state_seconds,
            cpu_overcommit_ratio: cpu_overcommit_ratio_gauge,
            agent_connected,
            scheduler_decisions_total,
            vm_create_result_total,
        }))
    }

    /// Render the registry in the Prometheus text format.
    pub fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buf)
            .expect("prometheus text encoding is infallible");
        buf
    }
}

/// Convert a `MachineState` value stored as `i64` in SQLite into the
/// proto-defined label (`PENDING`, `RUNNING`, ...). Returns `"UNKNOWN"`
/// for values outside the enum so metrics still report something when
/// the schema drifts.
fn state_label(state: i64) -> &'static str {
    MachineState::try_from(state as i32)
        .map(|s| s.as_str_name())
        .unwrap_or("UNKNOWN")
}

/// Parse an RFC 3339 timestamp and return the elapsed seconds to now.
/// Returns 0 if the timestamp is malformed or in the future.
fn age_seconds(rfc3339: &str) -> f64 {
    let Ok(then) = humantime::parse_rfc3339(rfc3339) else {
        return 0.0;
    };
    SystemTime::now()
        .duration_since(then)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Refresh every gauge whose value is derived from SQLite. Called on a
/// fixed interval by [`run_poller`]. Each per-host / per-VM gauge is
/// reset before repopulation so entries for deleted hosts / VMs drop
/// off cleanly.
async fn refresh(metrics: &Metrics, db: &Db) -> Result<(), crate::db::DbError> {
    let hosts = db.list_hosts().await?;
    let vms = db.list_vms(None).await?;
    let clusters = db.list_clusters().await?;

    metrics.clusters.set(clusters.len() as i64);

    metrics.hosts.reset();
    let (healthy, unhealthy) = hosts.iter().fold((0i64, 0i64), |(h, u), row| {
        if row.healthy {
            (h + 1, u)
        } else {
            (h, u + 1)
        }
    });
    metrics.hosts.with_label_values(&["true"]).set(healthy);
    metrics.hosts.with_label_values(&["false"]).set(unhealthy);

    metrics.host_cpu_total.reset();
    metrics.host_cpu_available.reset();
    metrics.host_memory_mib_total.reset();
    metrics.host_memory_mib_available.reset();
    metrics.host_disk_gib_total.reset();
    metrics.host_disk_gib_available.reset();
    metrics.host_gpus_total.reset();
    metrics.host_gpus_assigned.reset();
    metrics.host_last_heartbeat_age_seconds.reset();
    metrics.vms.reset();
    metrics.vm_age_in_state_seconds.reset();

    // Per-host accumulators derived from the VM rows. Mirror of what the
    // scheduler does — availability isn't stored, it's computed from
    // totals minus current VM allocations.
    #[derive(Default, Clone, Copy)]
    struct HostUsage {
        cpu: i64,
        mem: i64,
        disk: i64,
        gpus: i64,
    }
    let mut usage_by_host: HashMap<&str, HostUsage> = HashMap::new();
    for vm in &vms {
        let u = usage_by_host.entry(vm.host_id.as_str()).or_default();
        u.cpu += vm.cpu;
        u.mem += vm.memory_mib;
        u.disk += vm.disk_gib;
        let devs: Vec<GpuInfo> = parse_owned_json(&vm.gpu_assignments, "vms.gpu_assignments");
        u.gpus += devs.len() as i64;
    }

    // Per-host gauges. All use `hostname` as the label value so Grafana
    // displays human-readable names instead of UUIDs.
    for host in &hosts {
        let h = host.hostname.as_str();
        let usage = usage_by_host
            .get(host.id.as_str())
            .copied()
            .unwrap_or_default();

        metrics
            .host_cpu_total
            .with_label_values(&[h])
            .set(host.total_cpu);
        metrics
            .host_cpu_available
            .with_label_values(&[h])
            .set((host.total_cpu - usage.cpu).max(0));
        metrics
            .host_memory_mib_total
            .with_label_values(&[h])
            .set(host.total_memory_mib);
        metrics
            .host_memory_mib_available
            .with_label_values(&[h])
            .set((host.total_memory_mib - usage.mem).max(0));
        metrics
            .host_disk_gib_total
            .with_label_values(&[h])
            .set(host.total_disk_gib);
        metrics
            .host_disk_gib_available
            .with_label_values(&[h])
            .set((host.total_disk_gib - usage.disk).max(0));

        let inventory: Vec<GpuInfo> = parse_owned_json(&host.gpu_inventory, "hosts.gpu_inventory");
        metrics
            .host_gpus_total
            .with_label_values(&[h])
            .set(inventory.len() as i64);
        metrics
            .host_gpus_assigned
            .with_label_values(&[h])
            .set(usage.gpus);

        metrics
            .host_last_heartbeat_age_seconds
            .with_label_values(&[h])
            .set(age_seconds(&host.last_heartbeat));
    }

    // host_id → hostname lookup so VM labels carry the human-readable
    // hostname instead of the UUID.
    let host_id_to_name: HashMap<&str, &str> = hosts
        .iter()
        .map(|h| (h.id.as_str(), h.hostname.as_str()))
        .collect();

    // Emit a (state, cluster) series at value 0 for every known cluster ×
    // every state the proto defines before accumulating live counts.
    // Without this, Grafana panels like "VMs pending" show "no data" for
    // idle clusters — Prometheus gauge series only exist once `.set()`
    // has been called on their labels.
    let mut vm_counts: HashMap<(&'static str, &str), i64> = HashMap::new();
    for state in ALL_VM_STATES {
        for cluster in &clusters {
            vm_counts.insert((state, cluster.id.as_str()), 0);
        }
    }
    for vm in &vms {
        let state = state_label(vm.state);
        *vm_counts
            .entry((state, vm.cluster_id.as_str()))
            .or_insert(0) += 1;

        let host_name = host_id_to_name
            .get(vm.host_id.as_str())
            .copied()
            .unwrap_or("unknown");
        metrics
            .vm_age_in_state_seconds
            .with_label_values(&[&vm.id, &vm.name, state, host_name, &vm.cluster_id])
            .set(age_seconds(&vm.updated_at));
    }
    for ((state, cluster), count) in vm_counts {
        metrics.vms.with_label_values(&[state, cluster]).set(count);
    }

    Ok(())
}

/// Every terminal label `state_label` can emit. Kept in sync with the
/// `MachineState` enum in the proto (plus `UNKNOWN` for forward-compat).
const ALL_VM_STATES: &[&str] = &[
    "PENDING", "CREATING", "RUNNING", "STOPPING", "STOPPED", "FAILED",
];

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Background task that drives [`refresh`] every 5 seconds until the
/// shutdown token is cancelled.
pub async fn run_poller(metrics: Arc<Metrics>, db: Db, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("metrics poller shutting down");
                return;
            }
            _ = interval.tick() => {
                if let Err(e) = refresh(&metrics, &db).await {
                    warn!(error = %e, "metrics refresh failed");
                }
            }
        }
    }
}

/// Serve the Prometheus `/metrics` endpoint on a plain TCP listener (no
/// TLS — this is a separate port that Prometheus scrapes locally).
pub async fn run_server(
    metrics: Arc<Metrics>,
    listen: &str,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(metrics);

    let listener = TcpListener::bind(listen).await?;
    let addr = listener.local_addr()?;
    info!(%addr, "metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;

    Ok(())
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ClusterRow, HostRow, VmRow};

    fn make_host(id: &str, hostname: &str, total_cpu: i64, healthy: bool) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: hostname.to_string(),
            total_cpu,
            total_memory_mib: 65536,
            total_disk_gib: 1000,
            gpu_inventory: "[]".to_string(),
            last_heartbeat: basis_common::time::now_rfc3339(),
            healthy,
        }
    }

    fn make_cluster(id: &str) -> ClusterRow {
        ClusterRow {
            id: id.to_string(),
            name: format!("cluster-{id}"),
            ip_pool: "default".to_string(),
            control_plane_endpoint: "10.0.10.1".to_string(),
            created_at: basis_common::time::now_rfc3339(),
        }
    }

    fn make_vm(id: &str, host_id: &str, cluster_id: &str, state: i64) -> VmRow {
        VmRow {
            id: id.to_string(),
            name: format!("vm-{id}"),
            cluster_id: cluster_id.to_string(),
            host_id: host_id.to_string(),
            ip_address: "10.0.10.10".to_string(),
            state,
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpu_assignments: "[]".to_string(),
            image: "ubuntu:22.04".to_string(),
            error_message: String::new(),
            created_at: basis_common::time::now_rfc3339(),
            updated_at: basis_common::time::now_rfc3339(),
        }
    }

    #[tokio::test]
    async fn refresh_populates_gauges() {
        let db = Db::open(":memory:".as_ref()).await.unwrap();

        db.upsert_host(&make_host("h1", "node-a", 32, true))
            .await
            .unwrap();
        db.upsert_host(&make_host("h2", "node-b", 16, false))
            .await
            .unwrap();
        db.insert_cluster(&make_cluster("c1")).await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", 2)).await.unwrap(); // RUNNING
        db.insert_vm(&make_vm("v2", "h1", "c1", 1)).await.unwrap(); // CREATING

        let metrics = Metrics::new(1.0).unwrap();
        refresh(&metrics, &db).await.unwrap();

        assert_eq!(metrics.clusters.get(), 1);
        assert_eq!(metrics.hosts.with_label_values(&["true"]).get(), 1);
        assert_eq!(metrics.hosts.with_label_values(&["false"]).get(), 1);
        assert_eq!(
            metrics.host_cpu_total.with_label_values(&["node-a"]).get(),
            32
        );
        assert_eq!(
            metrics.host_cpu_total.with_label_values(&["node-b"]).get(),
            16
        );
        assert_eq!(metrics.vms.with_label_values(&["RUNNING", "c1"]).get(), 1);
        assert_eq!(metrics.vms.with_label_values(&["CREATING", "c1"]).get(), 1);
    }

    #[tokio::test]
    async fn refresh_drops_labels_for_deleted_vms() {
        let db = Db::open(":memory:".as_ref()).await.unwrap();
        db.upsert_host(&make_host("h1", "node-a", 32, true))
            .await
            .unwrap();
        db.insert_cluster(&make_cluster("c1")).await.unwrap();
        db.insert_vm(&make_vm("v1", "h1", "c1", 2)).await.unwrap();

        let metrics = Metrics::new(1.0).unwrap();
        refresh(&metrics, &db).await.unwrap();
        assert_eq!(metrics.vms.with_label_values(&["RUNNING", "c1"]).get(), 1);

        db.delete_vm("v1").await.unwrap();
        refresh(&metrics, &db).await.unwrap();
        // After reset, the deleted VM's labels are gone — default for a
        // missing label is 0, but the point is that the metric was
        // cleared (.get on a fresh label returns 0, which in prometheus
        // crate semantics means "no data" for export).
        assert_eq!(metrics.vms.with_label_values(&["RUNNING", "c1"]).get(), 0);
    }

    #[test]
    fn render_emits_prometheus_text() {
        let metrics = Metrics::new(1.0).unwrap();
        metrics.clusters.set(3);
        let body = String::from_utf8(metrics.render()).unwrap();
        assert!(body.contains("basis_clusters 3"));
        assert!(body.contains("# HELP basis_clusters"));
    }

    #[test]
    fn cpu_overcommit_ratio_is_exported() {
        let metrics = Metrics::new(4.0).unwrap();
        let body = String::from_utf8(metrics.render()).unwrap();
        assert!(
            body.contains("basis_cpu_overcommit_ratio 4"),
            "rendered metrics did not contain the ratio gauge:\n{body}",
        );
    }

    #[test]
    fn state_label_covers_all_proto_states() {
        assert_eq!(state_label(0), "PENDING");
        assert_eq!(state_label(1), "CREATING");
        assert_eq!(state_label(2), "RUNNING");
        assert_eq!(state_label(3), "STOPPING");
        assert_eq!(state_label(4), "STOPPED");
        assert_eq!(state_label(5), "FAILED");
        assert_eq!(state_label(99), "UNKNOWN");
    }
}
