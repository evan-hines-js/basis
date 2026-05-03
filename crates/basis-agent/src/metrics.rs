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
use std::time::Duration;

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{
    Encoder, GaugeVec, Histogram, HistogramOpts, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::db::AgentDb;
use crate::vm::{unit_name_for_vm, VmManager};

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
    pub data_disk_create_seconds: Histogram,
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

    // --- Per-VM gauges (refreshed by `run_vm_poller`) ---
    /// Identity / allocation labels for every VM the agent currently
    /// manages, set to `1`. Designed to be joined into the runtime
    /// gauges below by `vm_id` so dashboards can pull `name`, `ip`,
    /// `image`, `cluster` (last segment of vm name's prefix is opaque
    /// — agent doesn't know cluster, only vm_id/name) into the same
    /// row. Cardinality is bounded by the live VM count on this host;
    /// `reset()` between polls so deleted VMs drop off cleanly.
    pub vm_info: IntGaugeVec,
    /// 1 iff the VM's systemd unit currently has a live cloud-hypervisor
    /// process (same predicate as `VmManager::has_live_process`). 0 means
    /// the unit exists but the process exited — usually FAILED.
    pub vm_running: IntGaugeVec,
    /// CPU seconds consumed by the VM's cloud-hypervisor process,
    /// accumulated by systemd via `CPUUsageNSec`. Exposed as a gauge
    /// rather than a counter because we read a wall-clock-cumulative
    /// value from systemd — Prometheus's `rate()` over the gauge gives
    /// the same answer as if we treated it as a counter, and a counter
    /// with `reset()` semantics would falsely emit a reset every poll
    /// for VMs that haven't changed.
    pub vm_cpu_seconds: GaugeVec,
    /// Resident memory currently in use by the VM, from systemd's
    /// `MemoryCurrent`. Compare against `vm_memory_limit_bytes` to spot
    /// guests that are about to OOM.
    pub vm_memory_bytes: IntGaugeVec,
    /// Allocated memory ceiling (MemoryMax) — what we asked systemd to
    /// cap the VM's cgroup at. Equal to `--memory=size=` from create.
    pub vm_memory_limit_bytes: IntGaugeVec,
    /// vCPU count assigned to the VM. Constant per VM; surfaced as a
    /// gauge so dashboards can compute "CPU seconds per vCPU" without
    /// rejoining against a separate config table.
    pub vm_cpu_quota: IntGaugeVec,
    /// Sum of root + extra-disk allocations in GiB. Constant per VM;
    /// useful as the "size" column in the per-VM table panel.
    /// Per-VM rootfs allocation, in GiB. Charges against the host's
    /// rootfs thin pool budget — see `basis_host_rootfs_bytes_*`.
    pub vm_rootfs_gib: IntGaugeVec,
    /// Per-VM data-disk allocation (sum of `storage_disks[].size_gib`), in
    /// GiB. Charges against the host's data VG budget — see
    /// `basis_host_data_bytes_*`.
    pub vm_data_gib: IntGaugeVec,

    // --- Per-pool / per-device storage gauges -----------------------
    /// Pool capacity, decomposed into the four layers operators need
    /// to distinguish:
    ///   `configured` — sum of every configured device's size, regardless of state.
    ///   `ready`      — sum of physically-Ready devices.
    ///   `schedulable_total` — Ready AND scheduling-Enabled.
    ///   `schedulable_free`  — free bytes on the schedulable subset.
    /// One metric family with a `layer` label rather than four
    /// separate metrics so dashboards can stack/diff them.
    pub pool_capacity_bytes: IntGaugeVec,
    /// Per-device byte counts. `kind="total"` or `"free"`.
    pub device_capacity_bytes: IntGaugeVec,
    /// Per-device physical health, set to `1` for the matching
    /// `physical` label and `0` for the others. Single metric family
    /// (vs three counters) so a dashboard can render `physical` as a
    /// stat per device without OR-ing across families.
    pub device_physical_state: IntGaugeVec,
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
                "Elapsed seconds for Storage::create_vm_lv — rootfs thin-pool \
                 snapshot of the golden image LV. Contends on the thin-pool \
                 metadata lock under concurrent creates",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(lv_snapshot_seconds.clone()))?;

        let data_disk_create_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "basis_agent_data_disk_create_seconds",
                "Elapsed seconds to create every extra data-disk LV for one \
                 VM. Scales with the number of data disks requested; zero \
                 observations on VMs that request none",
            )
            .buckets(buckets.clone()),
        )?;
        registry.register(Box::new(data_disk_create_seconds.clone()))?;

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

        let vm_info = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_info",
                "Per-VM identity / allocation labels (always 1). Join \
                 against per-VM runtime gauges by vm_id to enrich them \
                 with name / ip / image",
            ),
            &["vm_id", "name", "ip", "image"],
        )?;
        registry.register(Box::new(vm_info.clone()))?;

        let vm_running = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_running",
                "1 iff the VM's systemd unit currently has a live \
                 cloud-hypervisor process",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_running.clone()))?;

        let vm_cpu_seconds = GaugeVec::new(
            Opts::new(
                "basis_agent_vm_cpu_seconds",
                "Total CPU seconds consumed by the VM's cgroup, from \
                 systemd's CPUUsageNSec. Use rate() to derive vCPU usage",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_cpu_seconds.clone()))?;

        let vm_memory_bytes = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_memory_bytes",
                "Resident memory in use by the VM (systemd MemoryCurrent)",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_memory_bytes.clone()))?;

        let vm_memory_limit_bytes = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_memory_limit_bytes",
                "Memory ceiling configured for the VM cgroup (matches \
                 cloud-hypervisor --memory)",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_memory_limit_bytes.clone()))?;

        let vm_cpu_quota = IntGaugeVec::new(
            Opts::new("basis_agent_vm_cpu_quota", "vCPUs assigned to the VM"),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_cpu_quota.clone()))?;

        let vm_rootfs_gib = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_rootfs_gib",
                "Per-VM rootfs allocation, GiB. Charges against the rootfs thin pool",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_rootfs_gib.clone()))?;

        let vm_data_gib = IntGaugeVec::new(
            Opts::new(
                "basis_agent_vm_data_gib",
                "Per-VM data-disk allocation (sum of extras), GiB. Charges against the data VG",
            ),
            &["vm_id"],
        )?;
        registry.register(Box::new(vm_data_gib.clone()))?;

        let pool_capacity_bytes = IntGaugeVec::new(
            Opts::new(
                "basis_agent_pool_capacity_bytes",
                "Per-pool capacity in bytes, decomposed by layer \
                 (configured | ready | schedulable_total | schedulable_free)",
            ),
            &["pool", "backend", "layer"],
        )?;
        registry.register(Box::new(pool_capacity_bytes.clone()))?;

        let device_capacity_bytes = IntGaugeVec::new(
            Opts::new(
                "basis_agent_device_capacity_bytes",
                "Per-device byte counts, kind=total|free",
            ),
            &["pool", "device", "kind"],
        )?;
        registry.register(Box::new(device_capacity_bytes.clone()))?;

        let device_physical_state = IntGaugeVec::new(
            Opts::new(
                "basis_agent_device_physical_state",
                "Per-device physical health (Ready/Degraded/Missing), \
                 set to 1 for the matching label and 0 otherwise",
            ),
            &["pool", "device", "physical"],
        )?;
        registry.register(Box::new(device_physical_state.clone()))?;

        Ok(Arc::new(Self {
            registry,
            image_ensure_cached_seconds,
            lv_snapshot_seconds,
            data_disk_create_seconds,
            cloud_init_iso_seconds,
            tap_create_seconds,
            vfio_bind_seconds,
            vm_spawn_seconds,
            lv_permit_wait_seconds,
            orphan_sweep_reclaimed_total,
            vm_info,
            vm_running,
            vm_cpu_seconds,
            vm_memory_bytes,
            vm_memory_limit_bytes,
            vm_cpu_quota,
            vm_rootfs_gib,
            vm_data_gib,
            pool_capacity_bytes,
            device_capacity_bytes,
            device_physical_state,
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

/// Cadence of the per-VM gauge refresh. Same 5s the controller's
/// `refresh()` uses — short enough that a Grafana table panel feels
/// live, long enough that `systemctl show` for every VM doesn't show
/// up in the agent's CPU profile.
const VM_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Refresh per-pool / per-device gauges from a fresh
/// [`crate::lvm::StorageCapacity`] snapshot. Called from the
/// existing storage-capacity loop; a single capacity scan drives both
/// the heartbeat-bound proto and the agent's local metrics, so the
/// two views can never disagree.
pub fn refresh_storage_gauges(metrics: &Metrics, capacity: &crate::lvm::StorageCapacity) {
    metrics.pool_capacity_bytes.reset();
    metrics.device_capacity_bytes.reset();
    metrics.device_physical_state.reset();
    for pool in &capacity.pools {
        let backend = pool.backend.as_str();
        let labels = [
            ("configured", pool.configured_total_bytes as i64),
            ("ready", pool.ready_total_bytes as i64),
            ("schedulable_total", pool.schedulable_total_bytes as i64),
            ("schedulable_free", pool.schedulable_free_bytes as i64),
        ];
        for (layer, v) in labels {
            metrics
                .pool_capacity_bytes
                .with_label_values(&[&pool.pool, backend, layer])
                .set(v);
        }
        for d in &pool.devices {
            metrics
                .device_capacity_bytes
                .with_label_values(&[&pool.pool, &d.id, "total"])
                .set((d.total_gib * (1 << 30)) as i64);
            metrics
                .device_capacity_bytes
                .with_label_values(&[&pool.pool, &d.id, "free"])
                .set((d.free_gib * (1 << 30)) as i64);
            for state in [
                crate::lvm::DevicePhysicalHealth::Ready,
                crate::lvm::DevicePhysicalHealth::Degraded,
                crate::lvm::DevicePhysicalHealth::Missing,
            ] {
                metrics
                    .device_physical_state
                    .with_label_values(&[&pool.pool, &d.id, state.as_str()])
                    .set(if d.physical == state { 1 } else { 0 });
            }
        }
    }
}

/// Periodic poller that refreshes the per-VM gauges from the agent DB
/// and systemd's per-unit accounting. Runs for the lifetime of the
/// agent; cancellable via the shutdown token mostly so tests don't
/// leak the task.
pub async fn run_vm_poller(
    metrics: Arc<Metrics>,
    agent_db: AgentDb,
    vm_mgr: Arc<VmManager>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(VM_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                if let Err(e) = refresh_vm_gauges(&metrics, &agent_db, &vm_mgr).await {
                    warn!(error = %e, "per-VM metrics refresh failed");
                }
            }
        }
    }
}

/// One pass of the refresh loop. Resets the per-VM label sets first so
/// a deleted VM stops emitting stale `running=1` / memory readings on
/// the next scrape — same pattern the controller's `refresh()` uses.
async fn refresh_vm_gauges(
    metrics: &Metrics,
    agent_db: &AgentDb,
    vm_mgr: &Arc<VmManager>,
) -> anyhow::Result<()> {
    let vms = agent_db.list_vms().await?;

    metrics.vm_info.reset();
    metrics.vm_running.reset();
    metrics.vm_cpu_seconds.reset();
    metrics.vm_memory_bytes.reset();
    metrics.vm_memory_limit_bytes.reset();
    metrics.vm_cpu_quota.reset();
    metrics.vm_rootfs_gib.reset();
    metrics.vm_data_gib.reset();

    for vm in &vms {
        let vm_id = vm.vm_id.as_str();

        metrics
            .vm_info
            .with_label_values(&[vm_id, &vm.name, &vm.ip_address, &vm.image])
            .set(1);
        metrics.vm_cpu_quota.with_label_values(&[vm_id]).set(vm.cpu);

        let data_gib = vm
            .parsed_storage_disks()
            .map(|disks| disks.iter().map(|d| d.size_gib as i64).sum::<i64>())
            .unwrap_or_else(|e| {
                tracing::warn!(
                    vm_id, error = %e,
                    "metrics: malformed local_vms.storage_disks; reporting 0",
                );
                0
            });
        metrics
            .vm_rootfs_gib
            .with_label_values(&[vm_id])
            .set(vm.disk_gib);
        metrics
            .vm_data_gib
            .with_label_values(&[vm_id])
            .set(data_gib);
        metrics
            .vm_memory_limit_bytes
            .with_label_values(&[vm_id])
            .set(vm.memory_mib * 1024 * 1024);

        // is_pending elides the systemctl probe — for a mid-create VM
        // the unit may not exist yet and `systemctl show` returns junk
        // values that we'd otherwise publish as zeros.
        if vm_mgr.is_pending(vm_id).await {
            metrics.vm_running.with_label_values(&[vm_id]).set(0);
            continue;
        }

        let stats = read_unit_stats(&unit_name_for_vm(vm_id)).await;
        metrics
            .vm_running
            .with_label_values(&[vm_id])
            .set(if stats.running { 1 } else { 0 });
        if let Some(ns) = stats.cpu_nsec {
            metrics
                .vm_cpu_seconds
                .with_label_values(&[vm_id])
                .set(ns as f64 / 1e9);
        }
        if let Some(b) = stats.memory_current {
            metrics
                .vm_memory_bytes
                .with_label_values(&[vm_id])
                .set(b as i64);
        }
    }
    Ok(())
}

/// Per-unit accounting snapshot read from systemd via `systemctl show`.
/// `None` for a counter means "couldn't parse it" — usually because the
/// unit just exited and systemd is reporting `[not set]`. We elide the
/// gauge update in that case so a transient hiccup doesn't blip a real
/// reading down to zero.
struct UnitStats {
    running: bool,
    cpu_nsec: Option<u64>,
    memory_current: Option<u64>,
}

async fn read_unit_stats(unit: &str) -> UnitStats {
    // One `systemctl show` query for all three properties keeps this to
    // a single fork per VM per poll, vs three separate calls.
    let out = Command::new("systemctl")
        .args([
            "show",
            "--property=SubState",
            "--property=CPUUsageNSec",
            "--property=MemoryCurrent",
            unit,
        ])
        .output()
        .await;
    let mut stats = UnitStats {
        running: false,
        cpu_nsec: None,
        memory_current: None,
    };
    let Ok(out) = out else { return stats };
    if !out.status.success() {
        return stats;
    }
    let body = String::from_utf8_lossy(&out.stdout);
    for line in body.lines() {
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        match key {
            "SubState" => stats.running = val.trim() == "running",
            "CPUUsageNSec" => stats.cpu_nsec = val.trim().parse::<u64>().ok(),
            "MemoryCurrent" => stats.memory_current = val.trim().parse::<u64>().ok(),
            _ => {}
        }
    }
    stats
}
