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

By default, Prometheus scrapes `host.docker.internal:9443` — the Docker
host's loopback. This works when the Basis controller is running locally
on the same machine as Docker Desktop.

For a **remote controller**, edit `prometheus.yml` and replace the target:

```yaml
  - job_name: basis-controller
    static_configs:
      - targets:
          - 10.0.0.131:9443
```

Then reload without restarting Prometheus:

```sh
curl -X POST http://localhost:9090/-/reload
```

Add agent hosts under the `basis-agents` job the same way.

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
