# Basis Observability

Local Prometheus + Grafana stack for viewing Basis controller and agent
metrics. Dashboard-first: the dashboard defines what the exporter must
emit; the exporter implementation fills it in.

## Run

```sh
cd deploy/observability
docker compose up -d
```

- Grafana: http://localhost:3000 (anonymous admin, no login)
- Prometheus: http://localhost:9090

The **Basis Cluster** dashboard is auto-loaded into the "Basis" folder.

## Scrape targets

`prometheus.yml` is shipped pointing at the current dev cell:

| Job               | Target            | Host label    |
|-------------------|-------------------|---------------|
| basis-controller  | 10.0.0.206:9443   | (n/a — controller self-labels metrics by host id) |
| basis-agents      | 10.0.0.206:9444   | poweredge-md  |
| basis-agents      | 10.0.0.97:9444    | poweredge-lg  |

Each agent target lives in its own `static_configs` group so it can
carry a per-host `host=` label. The controller-emitted metrics
(`basis_host_cpu_total{host=...}` etc.) already carry a host label out
of the box; the agent-emitted metrics (`basis_agent_*`) do not, so this
label is what makes per-host Grafana panels work.

For a **different cell**, edit the targets in `prometheus.yml` and reload:

```sh
curl -X POST http://localhost:9090/-/reload
```

Add another agent host as a third group under `basis-agents`:

```yaml
      - targets:
          - 10.0.0.42:9444
        labels:
          service: basis-agent
          host: <hostname>
```

## Metrics the controller emits

Every metric the dashboard renders is backed by one of these. Anything
not listed here isn't emitted — if a panel shows "No data" the metric
name probably drifted, not the scrape.

**Gauges:**
- `basis_hosts{healthy}`, `basis_clusters`
- `basis_vms{state, cluster}`
- `basis_host_cpu_total{host}`, `basis_host_cpu_assigned{host}` (unclamped — can exceed total under overcommit)
- `basis_host_memory_mib_total{host}`, `basis_host_memory_mib_assigned{host}`
- `basis_host_disk_gib_total{host}`, `basis_host_disk_gib_assigned{host}`
- `basis_host_gpus_total{host}`, `basis_host_gpus_assigned{host}`
- `basis_host_last_heartbeat_age_seconds{host}`
- `basis_agent_connected{host}` — 1 if the agent stream is open
- `basis_vm_age_in_state_seconds{vm_id, name, state, host, cluster}` — stuck-VM detector
- `basis_cpu_overcommit_ratio` — scheduler configuration, for dashboard math

**Counters:**
- `basis_vm_create_result_total{result}` — terminal CreateMachine outcome (placed, no_capacity, no_agent, stream_closed, vm_failed, agent_error, timeout)
- `basis_scheduler_decisions_total{outcome}` — scheduler placement outcomes

**Histograms:**
- `basis_vm_create_duration_seconds{result}` — end-to-end CreateMachine latency, same `result` labels as the counter
- `basis_vm_time_to_running_seconds` — CREATING → RUNNING provisioning time, observed even after a CreateMachine timeout

## Metrics the agent emits

Per-step create latency (see `basis-agent` dashboard for the full set):
- `basis_agent_image_ensure_cached_seconds`, `basis_agent_lv_snapshot_seconds`, `basis_agent_data_disk_create_seconds`, `basis_agent_cloud_init_iso_seconds`, `basis_agent_tap_create_seconds`, `basis_agent_vfio_bind_seconds`, `basis_agent_vm_spawn_seconds`, `basis_agent_lv_permit_wait_seconds`
- `basis_agent_orphan_sweep_reclaimed_total{kind}` — counter

Per-VM gauges, refreshed every 5s from the agent DB and systemd's per-unit accounting (`CPUUsageNSec`, `MemoryCurrent`, `SubState`):
- `basis_agent_vm_info{vm_id, name, ip, image}` — always 1; join target for the runtime gauges below
- `basis_agent_vm_running{vm_id}` — 1 iff cloud-hypervisor is alive in the unit
- `basis_agent_vm_cpu_seconds{vm_id}` — cumulative cgroup CPU seconds; use `rate()` for vCPU usage
- `basis_agent_vm_memory_bytes{vm_id}`, `basis_agent_vm_memory_limit_bytes{vm_id}`
- `basis_agent_vm_cpu_quota{vm_id}`, `basis_agent_vm_disk_gib{vm_id}` — allocations

## Layout

```
deploy/observability/
  docker-compose.yml
  prometheus.yml
  grafana/
    provisioning/
      datasources/prometheus.yml   # auto-adds Prometheus datasource
      dashboards/default.yml       # auto-imports everything in dashboards/
    dashboards/
      basis.json                   # the Basis Cluster dashboard
```

Edit the dashboard JSON directly; Grafana picks up changes every 30s.
