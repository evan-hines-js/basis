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

## Metrics the dashboard expects

The dashboard queries metrics the controller hasn't emitted yet. The
exporter implementation will provide:

**Controller gauges:**
- `basis_hosts{healthy}`, `basis_clusters`
- `basis_vms{state, cluster}`
- `basis_host_cpu_total{host}`, `basis_host_cpu_available{host}`
- `basis_host_memory_mib_total{host}`, `basis_host_memory_mib_available{host}`
- `basis_host_gpus_total{host}`, `basis_host_gpus_assigned{host}`
- `basis_host_last_heartbeat_age_seconds{host}`
- `basis_agent_connected{host}`, `basis_agent_commands_in_flight{host}`
- `basis_vm_age_in_state_seconds{vm_id, state, host, cluster}` — stuck-VM detector

**Controller counters:**
- `basis_vm_create_result_total{result}`
- `basis_vm_state_transitions_total{from, to}`
- `basis_grpc_requests_total{method, status}`
- `basis_scheduler_decisions_total{outcome}`
- `basis_agent_commands_sent_total{type, host}`
- `basis_agent_stream_reconnects_total{host}`

**Controller histograms:**
- `basis_vm_time_to_running_seconds`
- `basis_grpc_request_duration_seconds{method}`
- `basis_agent_command_rtt_seconds{type}`

Until the exporter exists, the dashboard panels render "No data" — this
is expected and defines the implementation contract.

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
