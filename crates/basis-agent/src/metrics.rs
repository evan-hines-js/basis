//! Agent-side Prometheus metrics.
//!
//! Histograms are observed at the call sites inside `handlers::create_vm_inner`
//! so operators can localise where a slow VM create is spending its time:
//! image cache / LVM snapshot / cloud-init ISO / TAP bring-up / VFIO bind /
//! systemd-run. The controller already publishes end-to-end CreateMachine
//! latency; these break that down per step on the agent host.
//!
//! A plain-HTTP `/metrics` listener mirrors the controller's shape — no TLS,
//! separate port, Prometheus scrapes it directly.

use std::sync::{Arc, OnceLock};

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounterVec, Opts, Registry, TextEncoder};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Process-wide `Metrics` handle.
///
/// Module-private singleton written once at agent startup. Exists so
/// cross-cutting observers (e.g. the LVM permit queue inside `lvm.rs`)
/// can observe without threading `Arc<Metrics>` through every function
/// signature in the crate. `global()` returns `None` until the agent's
/// startup path has installed a handle — observations before that are
/// silently dropped rather than panicking, matching the way metrics are
/// best-effort elsewhere in the codebase.
static INSTANCE: OnceLock<Arc<Metrics>> = OnceLock::new();

/// Access the installed process-wide metrics handle, if any.
pub fn global() -> Option<&'static Arc<Metrics>> {
    INSTANCE.get()
}

/// Per-step timings for `handlers::create_vm_inner`.
///
/// Shape chosen so a step-change in any one of them localises the slow
/// step immediately on the Grafana "Per-step agent latency" panel —
/// without these, the controller's `basis_vm_create_duration_seconds`
/// only tells you *that* the agent slowed down, not which step did.
pub struct Metrics {
    registry: Registry,

    pub image_ensure_cached_seconds: Histogram,
    pub lv_snapshot_seconds: Histogram,
    pub cloud_init_iso_seconds: Histogram,
    pub tap_create_seconds: Histogram,
    pub vfio_bind_seconds: Histogram,
    pub vm_spawn_seconds: Histogram,

    /// Seconds spent waiting for a permit on the LVM mutation semaphore
    /// (see `lvm.rs`). Split from `lv_snapshot_seconds` so operators can
    /// tell "queueing behind other agents' lvcreates" apart from "lvm2
    /// itself is slow" — `actual_lvcreate = lv_snapshot - lv_permit_wait`.
    pub lv_permit_wait_seconds: Histogram,

    /// Resources the orphan sweep has reclaimed, broken down by kind
    /// (`unit`, `lv`, `tap`). A non-zero sustained rate means the agent
    /// is self-healing — delete paths raced and left something behind,
    /// and the sweep picked it up. A rising rate that doesn't taper
    /// means the leak is faster than the sweep; investigate the happy
    /// path rather than tuning the sweep.
    pub orphan_sweep_reclaimed_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Result<Arc<Self>, prometheus::Error> {
        let registry = Registry::new();

        // Bucket range spans the full envelope each step can legitimately
        // occupy. An image cache hit is sub-millisecond; a cold OCI pull
        // + qcow2 decompress is minutes. LVM/systemd/netlink are
        // typically <1s but can stall under contention. One bucket set
        // covers everything — separate per-metric tuning is more
        // bookkeeping than it's worth.
        let buckets = vec![
            0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
        ];

        let image_ensure_cached_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_image_ensure_cached_seconds",
                "Elapsed wall-clock seconds for image_mgr.ensure_cached — \
                 OCI manifest + layer pulls + qcow2 decompress + golden LV \
                 convert. Dominated by cache hit (fast) vs miss (slow)",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(image_ensure_cached_seconds.clone()))?;

        let lv_snapshot_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_lv_snapshot_seconds",
                "Elapsed seconds for lvm::create_vm_lv — thin-pool snapshot \
                 of the golden image LV. Contends on the thin-pool \
                 metadata lock under concurrent creates",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(lv_snapshot_seconds.clone()))?;

        let cloud_init_iso_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_cloud_init_iso_seconds",
                "Elapsed seconds for create_cloud_init_iso — mkisofs / \
                 genisoimage over a few KB of userdata. Should be <100ms; \
                 a spike here means disk I/O is saturating",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(cloud_init_iso_seconds.clone()))?;

        let tap_create_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_tap_create_seconds",
                "Elapsed seconds for net_mgr.create_tap — ip tuntap add + \
                 ip link set master. Serialised through the kernel's \
                 rtnetlink lock under concurrent creates",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(tap_create_seconds.clone()))?;

        let vfio_bind_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_vfio_bind_seconds",
                "Elapsed seconds per GPU to unbind from its driver and \
                 bind to vfio-pci. Zero observations on GPU-less fleets; \
                 useful when GPU pass-through is in use",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(vfio_bind_seconds.clone()))?;

        let vm_spawn_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_vm_spawn_seconds",
                "Elapsed seconds for vm_mgr.create_vm — systemd-run of the \
                 cloud-hypervisor service. Does *not* include guest boot; \
                 measures systemd-run return only",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(vm_spawn_seconds.clone()))?;

        let lv_permit_wait_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_lv_permit_wait_seconds",
                "Seconds spent waiting for the agent's LVM mutation \
                 semaphore. A spike here means concurrent lvcreate / \
                 lvremove requests exceeded the configured cap; compare \
                 against lv_snapshot_seconds to tell queue from lvm2",
            )
            .buckets(buckets),
        )?;
        registry.register(Box::new(lv_permit_wait_seconds.clone()))?;

        let orphan_sweep_reclaimed_total = IntCounterVec::new(
            Opts::new(
                "basis_agent_orphan_sweep_reclaimed_total",
                "Resources reclaimed by the orphan sweep, by kind \
                 (unit | lv | tap)",
            ),
            &["kind"],
        )?;
        registry.register(Box::new(orphan_sweep_reclaimed_total.clone()))?;

        Ok(Arc::new(Self {
            registry,
            image_ensure_cached_seconds,
            lv_snapshot_seconds,
            cloud_init_iso_seconds,
            tap_create_seconds,
            vfio_bind_seconds,
            vm_spawn_seconds,
            lv_permit_wait_seconds,
            orphan_sweep_reclaimed_total,
        }))
    }

    /// Install this handle as the process-wide singleton. Call once at
    /// startup; subsequent calls are silently ignored so a hot-reload
    /// path that constructs a fresh handle can't panic, but the first
    /// installed handle is also the one `global()` returns for the
    /// process lifetime.
    pub fn install_global(self: &Arc<Self>) {
        let _ = INSTANCE.set(self.clone());
    }

    pub fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buf)
            .expect("prometheus text encoding is infallible");
        buf
    }
}

/// Serve the Prometheus `/metrics` endpoint on a plain TCP listener.
/// Mirrors the controller's `metrics::run_server` — no TLS, separate port,
/// scraped locally.
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
    info!(%addr, "agent metrics server listening");

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
