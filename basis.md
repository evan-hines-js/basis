# Basis: Hypervisor Layer for Lattice

## Overview

Basis is a minimal hypervisor orchestration layer built on [Cloud Hypervisor](https://github.com/cloud-hypervisor/cloud-hypervisor). It runs on bare metal hosts and exposes exactly the API surface required by a CAPI infrastructure provider — nothing more. All scheduling intelligence (GPU topology, bin-packing, host health) is implicit: Basis makes smart placement decisions internally but never exposes them as API concepts.

Basis replaces general-purpose hypervisor platforms (Proxmox, VMware) for Lattice deployments. It is a single static binary per host, configured via a single file, secured with mTLS.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Bare Metal Host A                                                  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  basis-agent                                                   │  │
│  │  - Manages cloud-hypervisor processes (one per VM)            │  │
│  │  - Reports capacity: CPU, RAM, GPUs, IOMMU groups            │  │
│  │  - Executes VM create/delete/status on behalf of controller   │  │
│  │  - Prepares disk images (pull, cache, COW clone)              │  │
│  │  - Manages host networking (bridge, tap devices)              │  │
│  └──────┬────────────────────────────────────────────────────────┘  │
│         │ Unix socket per VM                                        │
│  ┌──────▼──────┐ ┌──────────────┐ ┌──────────────┐                │
│  │ cloud-hyper  │ │ cloud-hyper   │ │ cloud-hyper   │               │
│  │ visor (VM 1) │ │ visor (VM 2)  │ │ visor (VM 3)  │              │
│  └──────────────┘ └──────────────┘ └──────────────┘                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  Bare Metal Host B (or Host A — controller can be colocated)       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  basis-controller                                              │  │
│  │  - Single process, single sqlite database                     │  │
│  │  - gRPC API: the only external interface                      │  │
│  │  - Scheduler: topology-aware VM placement                     │  │
│  │  - IP allocator: assigns VM addresses from configured pools   │  │
│  │  - Host manager: tracks agent heartbeats and capacity         │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
         ▲
         │ gRPC (mTLS)
         │
┌────────┴────────────────────────────────────────────────────────────┐
│  Kubernetes (runs inside VMs created by Basis)                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  basis-capi-provider                                           │  │
│  │  - CAPI infrastructure provider controller                    │  │
│  │  - Watches BasisCluster + BasisMachine CRDs                   │  │
│  │  - Translates CAPI lifecycle into Basis gRPC calls            │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### basis-agent (per host)

A daemon that runs on every bare metal host. Responsibilities:

- **Process management.** Spawns and supervises `cloud-hypervisor` processes. Tracks PID-to-VM mapping. Cleans up orphaned processes on startup.
- **Capacity reporting.** On registration and periodically (every 30s), reports to the controller: total/available vCPUs, RAM, disk, and a full GPU inventory with PCIe topology.
- **VM execution.** Receives "create VM" commands from the controller, prepares the disk, configures networking, spawns `cloud-hypervisor`, and reports back the VM's IP and state.
- **Disk image management.** Pulls base images from an OCI registry or HTTP URL into a local cache (`/var/lib/basis/images/`). Creates qcow2 copy-on-write overlays per VM backed by the cached base image.
- **Host networking.** Creates a Linux bridge (`basis0` by default) and tap devices per VM. Attaches tap devices to the bridge. The bridge is connected to the host's physical NIC (or an existing bridge).
- **GPU device management.** Binds/unbinds PCI devices to/from `vfio-pci` driver as needed for VM creation and deletion.

**State:** Ephemeral. The agent has no persistent state beyond the disk image cache. VM records live in the controller's sqlite database. On restart, the agent reconciles running `cloud-hypervisor` processes against what the controller expects.

**Configuration** (`/etc/basis/agent.toml`):
```toml
controller_endpoint = "https://10.0.0.1:7443"
data_dir = "/var/lib/basis"

[network]
bridge = "basis0"
physical_nic = "eno1"

[tls]
cert = "/etc/basis/tls/agent.crt"
key = "/etc/basis/tls/agent.key"
ca = "/etc/basis/tls/ca.crt"
```

### basis-controller (single instance)

The central brain. A single process with an embedded sqlite database. Responsibilities:

- **gRPC API server.** The only external interface. Serves both agent-facing RPCs (registration stream) and CAPI-facing RPCs (cluster + machine lifecycle). mTLS is required; the caller's peer certificate CN determines their role (agent hostname vs. `basis-capi-provider`).
- **Cluster lifecycle.** Owns logical cluster state: reserves a control-plane VIP from the bound IP pool on `CreateCluster`, cascades machine tear-downs on `DeleteCluster`.
- **Scheduler.** Places VMs on hosts. Described in detail below.
- **IP allocator.** One allocation table keyed by `(owner_id, owner_kind)` for both VM IPs and cluster VIPs. Assigns atomically; reclaims when the owner is deleted.
- **Host health.** Marks hosts as unhealthy after 3 missed heartbeats (90s). Unhealthy hosts are excluded from scheduling but their VMs are not disturbed — CAPI handles remediation via `MachineHealthCheck`.

**State:** All state lives in a single sqlite database (`/var/lib/basis/controller.db`). Tables:

| Table | Purpose |
|-------|---------|
| `hosts` | Registered hosts: ID, address, total/available resources, GPU inventory, last heartbeat, health status |
| `clusters` | Logical clusters: ID, name, IP pool, reserved control-plane VIP, created_at |
| `vms` | All VMs: ID, name, `cluster_id`, host assignment, IP, state, resource allocation, GPU assignments, timestamps |
| `ip_pools` | Configured IP pools: CIDR, gateway, range_start, range_end |
| `ip_allocations` | One row per reserved IP: address, pool, `owner_id`, `owner_kind` (`vm` or `cluster_vip`) |

**No HA on day 1.** The controller is a single process. If it goes down, existing VMs keep running (cloud-hypervisor processes are independent). No new VMs can be created until the controller recovers. This is acceptable because:
- CAPI retries indefinitely with backoff
- Existing K8s clusters are unaffected
- The controller is stateless-ish (sqlite can be backed up or replicated later)
- Same failure mode as Proxmox losing its cluster leader

**Configuration** (`/etc/basis/controller.toml`):
```toml
listen = "0.0.0.0:7443"
data_dir = "/var/lib/basis"

[tls]
cert = "/etc/basis/tls/controller.crt"
key = "/etc/basis/tls/controller.key"
ca = "/etc/basis/tls/ca.crt"

[[ip_pools]]
name = "default"
cidr = "10.0.10.0/24"
gateway = "10.0.10.1"
range_start = "10.0.10.10"
range_end = "10.0.10.250"
```

### basis-capi-provider (runs in K8s)

A standard CAPI infrastructure provider controller. Runs inside the K8s cluster that Basis itself created (after pivot) or in a bootstrap cluster. Implements the CAPI v1beta2 contract.

**CRDs:**

```yaml
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: BasisCluster
spec:
  ipPool: "default"             # Name of a pool configured in the Basis controller
  # controlPlaneEndpoint is written by the reconciler after CreateCluster
status:
  basisClusterId: "c-3f8a..."   # Returned by Basis.CreateCluster
  initialization:
    provisioned: true
  conditions:
    - type: Ready
      status: "True"
```

```yaml
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: BasisMachine
spec:
  cpu: 4
  memoryMiB: 8192
  diskGiB: 100
  image: "ghcr.io/evan-hines-js/lattice-node:v1.32.0"
  gpus: 0                  # Number of GPUs to attach
  gpuConstraints:          # Optional: topology requirements
    minGroupSize: 0        # Minimum GPUs on same NVLink/PCIe switch
status:
  initialization:
    provisioned: true
  providerID: "basis://host-a/vm-3f8a1b2c"
  basisVmId: "vm-3f8a..."        # Returned by Basis.CreateMachine, used on delete
  addresses:
    - type: InternalIP
      address: "10.0.10.42"
```

```yaml
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: BasisMachineTemplate
spec:
  template:
    spec:
      cpu: 4
      memoryMiB: 8192
      diskGiB: 100
      image: "ghcr.io/evan-hines-js/lattice-node:v1.32.0"
      gpus: 0
      gpuConstraints: {}
```

**Reconciliation logic:**

On `BasisCluster` create:
1. Call `Basis.CreateCluster(name, ipPool)` → get `(cluster_id, vip)`
2. Patch `spec.controlPlaneEndpoint = { host: vip, port: 6443 }` — needed by `KubeadmControlPlane`
3. Patch `status.basisClusterId = cluster_id`
4. Set `status.initialization.provisioned = true`

On `BasisCluster` delete:
1. Call `Basis.DeleteCluster(status.basisClusterId)` — the controller cascades DeleteVm to every agent and releases the VIP
2. Remove finalizer

On `BasisMachine` create:
1. Resolve the owning `BasisCluster` via the `cluster.x-k8s.io/cluster-name` label; read `status.basisClusterId`
2. Read bootstrap data from the Secret referenced by the owning CAPI `Machine`
3. Call `Basis.CreateMachine(clusterId, ...)` — controller schedules, agent creates VM
4. Set `spec.providerID`, `status.basisVmId`, and `status.addresses` from response
5. Set `status.initialization.provisioned = true`

On `BasisMachine` delete:
1. Call `Basis.DeleteMachine(status.basisVmId)`
2. Remove finalizer

The provider controller is stateless. All state lives in the controller's sqlite and in the CRDs themselves.

## gRPC API

Two-level model: a caller first creates a Cluster (reserves a VIP, binds an IP pool), then creates Machines inside that cluster. All placement decisions — host, GPUs, IP — belong to the controller. Callers describe intent, not placement.

```protobuf
syntax = "proto3";
package basis.v1;

service Basis {
  // Cluster lifecycle
  rpc CreateCluster(CreateClusterRequest) returns (CreateClusterResponse);
  rpc DeleteCluster(DeleteClusterRequest) returns (DeleteClusterResponse);
  rpc GetCluster(GetClusterRequest) returns (Cluster);

  // Machine lifecycle
  rpc CreateMachine(CreateMachineRequest) returns (CreateMachineResponse);
  rpc DeleteMachine(DeleteMachineRequest) returns (DeleteMachineResponse);
  rpc GetMachine(GetMachineRequest) returns (Machine);
  rpc ListMachines(ListMachinesRequest) returns (ListMachinesResponse);
}

// Agents connect over a bidirectional stream (outbound from agent).
service BasisAgent {
  rpc StreamMessages(stream AgentMessage) returns (stream ControllerCommand);
}

// --- Cluster ---

message CreateClusterRequest {
  string name = 1;                  // Unique cluster name
  string ip_pool = 2;               // Pool used for this cluster's VIP and VM IPs
}

message CreateClusterResponse {
  string cluster_id = 1;
  string control_plane_endpoint = 2; // VIP reserved from the pool
}

message DeleteClusterRequest { string cluster_id = 1; }
message DeleteClusterResponse {}

message GetClusterRequest { string cluster_id = 1; }

message Cluster {
  string cluster_id = 1;
  string name = 2;
  string ip_pool = 3;
  string control_plane_endpoint = 4;
}

// --- Machine ---

message CreateMachineRequest {
  string cluster_id = 1;            // FK — determines the IP pool
  string name = 2;
  uint32 cpu = 3;
  uint32 memory_mib = 4;
  uint32 disk_gib = 5;
  string image = 6;
  bytes bootstrap_data = 7;         // cloud-init userdata
  uint32 gpus = 8;
  GPUConstraints gpu_constraints = 9;
}

message CreateMachineResponse {
  string id = 1;
  string provider_id = 2;           // "basis://<host>/<vm-id>"
  string ip_address = 3;
  string host = 4;
}

message DeleteMachineRequest { string id = 1; }
message DeleteMachineResponse {}

message GetMachineRequest { string id = 1; }

message ListMachinesRequest {
  string cluster_id = 1;            // Empty = all clusters
}

message ListMachinesResponse {
  repeated Machine machines = 1;
}

message Machine {
  string id = 1;
  string name = 2;
  string cluster_id = 3;
  string host = 4;
  string provider_id = 5;
  string ip_address = 6;
  MachineState state = 7;
  uint32 cpu = 8;
  uint32 memory_mib = 9;
  uint32 disk_gib = 10;
  repeated GPUDevice gpus = 11;
}

enum MachineState {
  PENDING = 0;
  CREATING = 1;
  RUNNING = 2;
  STOPPING = 3;
  STOPPED = 4;
  FAILED = 5;
}

message GPUConstraints {
  uint32 min_group_size = 1;        // GPUs that must share an NVLink domain
}

message GPUDevice {
  string pci_address = 1;           // "0000:41:00.0"
  string model = 2;                 // "NVIDIA A100"
  string iommu_group = 3;
  uint32 nvlink_group = 4;          // Populated from `nvidia-smi topo -m`; 0 = no NVLink
}

message IOMMUGroup {
  uint32 id = 1;
  repeated string pci_addresses = 2;
}

// --- Agent stream ---

message AgentMessage {
  oneof payload {
    RegisterHostRequest register = 1;
    HeartbeatRequest heartbeat = 2;
    ReportVMStateRequest vm_state = 3;
  }
}

message ControllerCommand {
  string request_id = 1;
  oneof command {
    CreateVMCommand create_vm = 2;
    DeleteVMCommand delete_vm = 3;
    RegisterHostResponse register_ack = 4;
  }
}

// Sent as the first command after the agent's RegisterHostRequest.
// `expected_vm_ids` is the controller's authoritative list of VMs this
// host should have. The agent MUST delete any local VM not in that list.
message RegisterHostResponse {
  string host_id = 1;
  repeated string expected_vm_ids = 2;
}

message RegisterHostRequest {
  string hostname = 1;
  uint32 total_cpu = 2;
  uint64 total_memory_mib = 3;
  uint64 total_disk_gib = 4;
  repeated GPUDevice gpus = 5;
  repeated IOMMUGroup iommu_groups = 6;
}

message HeartbeatRequest {
  string host_id = 1;
  uint32 available_cpu = 2;
  uint64 available_memory_mib = 3;
  uint64 available_disk_gib = 4;
  repeated string assigned_gpus = 5;
}

message ReportVMStateRequest {
  string vm_id = 1;
  MachineState state = 2;
  string error_message = 3;         // Non-empty if state == FAILED
}

// --- Controller -> Agent commands ---

message CreateVMCommand {
  string vm_id = 1;
  string name = 2;
  uint32 cpu = 3;
  uint32 memory_mib = 4;
  uint32 disk_gib = 5;
  string image = 6;
  bytes bootstrap_data = 7;
  string ip_address = 8;
  string gateway = 9;
  uint32 prefix_len = 10;
  uint32 gpus = 11;
  GPUConstraints gpu_constraints = 12;
  repeated string dns_servers = 13;
  repeated string gpu_pci_addresses = 14;   // Scheduler-selected GPUs
}

message DeleteVMCommand { string vm_id = 1; }
```

### mTLS and CN-based authorization

Every connection uses mTLS. The controller enforces role by peer certificate CN:

- **Agent connections** (`BasisAgent.StreamMessages`): CN must equal the `hostname` in the first `RegisterHostRequest`. An agent can only register as itself.
- **CAPI connections** (all `Basis.*` RPCs): CN must be `basis-capi-provider`.

Any cert that chains to the trusted CA with a different CN is rejected with `PermissionDenied`.

## Scheduler

The scheduler runs inside `basis-controller`. It is invoked synchronously during `CreateMachine` and returns a host assignment. There is no queue — if no host can satisfy the request, the RPC fails immediately and CAPI retries with backoff.

### Placement Algorithm

```
fn schedule(request) -> host:
    candidates = hosts.where(healthy AND has_capacity(request.cpu, request.memory, request.disk))

    if request.gpus > 0:
        candidates = candidates.where(has_available_gpus(request.gpus))

        if request.gpu_constraints.min_group_size > 0:
            // Filter to hosts where N GPUs share the same NVLink domain
            candidates = candidates.where(
                has_gpu_group(request.gpus, request.gpu_constraints.min_group_size)
            )

        // Prefer hosts where requested GPUs are on the same PCIe switch
        candidates.sort_by(gpu_topology_score(request.gpus), descending)

    // Break ties with best-fit bin-packing (prefer hosts closest to full)
    // This consolidates VMs and leaves empty hosts available for large GPU requests
    candidates.sort_by(remaining_capacity_after(request), ascending)

    return candidates.first() or error("no host can satisfy request")
```

### GPU Topology Scoring

When placing a multi-GPU VM, the scheduler prefers hosts where the requested GPUs are topologically close:

| Score | Meaning |
|-------|---------|
| 3 | All GPUs share NVLink (same NVLink group) |
| 2 | All GPUs share a PCIe switch (same NUMA node, no NVLink) |
| 1 | GPUs on different NUMA nodes but same host |

This scoring is best-effort. The `gpu_constraints.min_group_size` field is the hard constraint — the score is for tie-breaking among valid placements.

### GPU Assignment

Within a selected host, the agent picks specific GPUs:

1. Group available GPUs by NVLink domain
2. Find the smallest group that satisfies the request
3. If no single group suffices, pick GPUs from fewest groups possible (prefer topological locality)
4. Bind selected GPUs' PCI devices to `vfio-pci` driver
5. Pass VFIO device paths to `cloud-hypervisor --device`

## Lifecycle

### Create cluster

```
CAPI Provider                 Controller
     │                           │
     │──CreateCluster(name,pool)>│
     │                           │ allocate_ip(pool, ClusterVip)  → VIP
     │                           │ insert cluster row
     │<─(cluster_id, VIP)────────│
```

`CreateCluster` is synchronous and cheap — no VMs, no agents involved. It reserves a VIP from the pool and returns it so the CAPI provider can write it into `BasisCluster.spec.controlPlaneEndpoint` before any control-plane VMs are scheduled.

### Create machine

```
CAPI Provider                Controller                Agent                  Cloud Hypervisor
     │                           │                       │                         │
     │──CreateMachine(req)──────>│                       │                         │
     │                           │ get_cluster(id)       │                         │
     │                           │ schedule(req) -> host │                         │
     │                           │ allocate_ip(pool, Vm) │                         │
     │                           │ insert vm (CREATING)  │                         │
     │                           │──CreateVM(spec)──────>│                         │
     │                           │                       │ pull/cache image        │
     │                           │                       │ create qcow2 overlay    │
     │                           │                       │ write cloud-init ISO    │
     │                           │                       │ create tap device       │
     │                           │                       │ bind GPUs to vfio-pci   │
     │                           │                       │──spawn──────────────────>│
     │                           │                       │ POST /vm.create         │
     │                           │                       │ PUT /vm.boot            │
     │                           │                       │<──200 OK────────────────│
     │                           │<─ReportVMState(RUN)───│                         │
     │                           │ update vm (RUNNING)   │                         │
     │<─CreateMachineResponse────│                       │                         │
     │  (id, providerID, ip)     │                       │                         │
```

`CreateMachine` blocks until the agent reports RUNNING, with a 60s timeout. Cloud Hypervisor boots in ~100ms; the dominant cost is disk image preparation, which is cached after the first pull.

### Delete machine

```
CAPI Provider                Controller                Agent                  Cloud Hypervisor
     │                           │                       │                         │
     │──DeleteMachine(id)───────>│                       │                         │
     │                           │ update vm (STOPPING)  │                         │
     │                           │──DeleteVM(id)────────>│                         │
     │                           │                       │ PUT /vm.shutdown        │
     │                           │                       │ PUT /vm.delete          │
     │                           │                       │ kill process            │
     │                           │                       │ unbind GPUs from vfio   │
     │                           │                       │ delete tap device       │
     │                           │                       │ delete qcow2 overlay    │
     │                           │ release IP            │                         │
     │                           │ delete vm record      │                         │
     │<─DeleteMachineResponse────│                       │                         │
```

The DB row is dropped eagerly — the agent's eventual `ReportVMState(STOPPED)` lands on an absent row and is a no-op. This is fine: the controller is the source of truth, and treating delete as fire-and-forget keeps the caller's RPC latency low.

### Delete cluster

`DeleteCluster` cascades: the controller iterates every VM in the cluster, runs the Delete-machine flow for each (including sending `DeleteVM` to its agent), releases the VIP, and removes the cluster row.

### Reconnection & authoritative state

When an agent reconnects to the controller, the `RegisterHostResponse` contains the controller's authoritative `expected_vm_ids` list for that host. The agent MUST delete any local VM not in that list — those were forgotten by the controller (e.g., `DeleteMachine` or `DeleteCluster` while the agent was offline) and their disk overlays, tap devices, and GPU bindings are garbage. This replaces any need for separate "cleanup" RPCs.

## Networking

### Host Network Setup

Each host runs a Linux bridge (`basis0`). The bridge is attached to the host's physical NIC. Each VM gets a tap device attached to the bridge.

```
Physical NIC (eno1)
       │
  ┌────▼────┐
  │  basis0  │  (Linux bridge)
  │  bridge  │
  └┬───┬───┬┘
   │   │   │
  tap0 tap1 tap2
   │   │   │
  VM1  VM2  VM3
```

VMs receive their IP via cloud-init (static configuration, not DHCP). The controller assigns IPs from the configured pool and passes them to the agent, which writes them into the cloud-init network config.

### Cloud-Init Network Config

```yaml
network:
  version: 2
  ethernets:
    ens3:
      addresses:
        - 10.0.10.42/24
      gateway4: 10.0.10.1
      nameservers:
        addresses:
          - 8.8.8.8
          - 8.8.4.4
```

This is written to a cloud-init ISO (cidata) and attached to the VM as a virtio block device.

### Control Plane Endpoint

The VIP is reserved by Basis in `CreateCluster`, not supplied by the caller. The `basis-capi-provider` reconciler writes the returned VIP into `BasisCluster.spec.controlPlaneEndpoint`; `KubeadmControlPlane` reads it from there. kube-vip (a static pod on control plane nodes, configured via standard kubeadm cloud-init) claims the VIP on the shared interface via gratuitous ARP once the first control-plane VM boots.

This eliminates the bootstrap chicken-and-egg: the endpoint always exists before any VM is scheduled, and the caller never has to pick an IP outside the pool by hand.

## Disk Image Management

### Image Format

Base images are qcow2 disk images containing a K8s-ready OS (Ubuntu/Flatcar with containerd, kubeadm, kubelet pre-installed). They are distributed as OCI artifacts or served via HTTP.

### Image Lifecycle on Host

```
1. Agent receives CreateVM with image ref "ghcr.io/evan-hines-js/lattice-node:v1.32.0"
2. Check local cache: /var/lib/basis/images/sha256-<digest>.qcow2
3. If not cached: pull from OCI registry, verify digest, write to cache
4. Create COW overlay: /var/lib/basis/vms/<vm-id>/disk.qcow2 (backing: cached base)
5. Pass overlay path to cloud-hypervisor --disk path=/var/lib/basis/vms/<vm-id>/disk.qcow2
```

COW overlays mean:
- Base images are pulled once per host, shared across all VMs using that image
- Per-VM disk usage is only the delta from the base
- VM creation after first pull is near-instant (no copy)

## Bootstrap Integration

Basis integrates with Lattice's existing bootstrap flow. The CAPI provider generates the same `postKubeadmCommands` that call the parent cell's bootstrap webhook. The only difference from the Proxmox provider is the machine template shape.

Cloud-init userdata (the bootstrap data from CAPI) is delivered via a cidata ISO attached to the VM. Cloud Hypervisor mounts this as a virtio block device. The guest OS reads it via `cloud-init` on first boot, same as any cloud provider.

### Provider ID

Format: `basis://<host-id>/<vm-id>`

Example: `basis://host-a/3f8a1b2c-7d9e-4f1a-b5c3-2e8f6a9d0b1e`

The basis-capi-provider sets this on `BasisMachine.spec.providerID`. A minimal cloud-controller-manager (or kubelet `--provider-id` flag via cloud-init) sets the matching value on the Node's `spec.providerID` so CAPI can correlate Machine to Node.

The simplest approach: pass `--provider-id=basis://<host>/<vm-id>` to kubelet via cloud-init. No CCM needed.

## Security

### mTLS Everywhere

All gRPC connections (agent↔controller, CAPI-provider↔controller) use mutual TLS. Certificates are issued from a shared CA.

Initial deployment bootstraps the CA and issues certs. For day 1, this is manual (generate CA, distribute certs). Later, integrate with Lattice's cert-manager or Vault PKI.

TLS implementation uses `rustls` with `aws-lc-rs` backend (FIPS-compliant), consistent with the rest of Lattice.

### Agent Authentication

Each agent presents a client certificate with its hostname as the CN. The controller verifies:
- Valid certificate chain to the trusted CA
- CN matches the hostname in the `RegisterHost` request

### CAPI Provider Authentication

The CAPI provider presents a client certificate with CN `basis-capi-provider`. Every RPC on the `Basis` service (both cluster and machine) checks this CN; agents cannot call these RPCs.

### Host Security

- VMs are isolated by Cloud Hypervisor's VMM (KVM-based, Landlock LSM sandbox)
- GPU passthrough uses VFIO with IOMMU isolation
- The agent runs as root (required for KVM, VFIO, bridge management) but drops privileges for non-privileged operations

### Agents connect outbound

Agents initiate the connection to the controller via `BasisAgent.StreamMessages` — a bidirectional gRPC stream. This matches Lattice's outbound-only architecture: agents need zero inbound firewall rules, and VM commands (`CreateVM`, `DeleteVM`) are delivered as server-to-client messages over the stream.

The wire format is defined in the gRPC API section above (`AgentMessage` / `ControllerCommand`). This is the same pattern as the Lattice agent↔cell protocol.

## Lattice CAPI Provider Integration

The Lattice `lattice-capi` crate gets a new provider alongside Docker, AWS, OpenStack, and Proxmox:

```rust
// crates/lattice-capi/src/provider/basis.rs

pub struct BasisProvider {
    namespace: String,
}

impl BasisProvider {
    fn infra_ref(&self) -> InfrastructureRef<'static> {
        InfrastructureRef {
            api_group: INFRASTRUCTURE_API_GROUP,
            api_version: "infrastructure.cluster.x-k8s.io/v1alpha1",
            cluster_kind: "BasisCluster",
            machine_template_kind: "BasisMachineTemplate",
        }
    }
}

#[async_trait]
impl Provider for BasisProvider {
    async fn generate_capi_manifests(
        &self,
        cluster: &LatticeCluster,
        bootstrap: &BootstrapInfo,
    ) -> Result<Vec<CAPIManifest>> {
        // Generates: Cluster, BasisCluster, KubeadmControlPlane,
        // BasisMachineTemplate (cp), MachineDeployment (per pool),
        // BasisMachineTemplate (per pool), KubeadmConfigTemplate (per pool)
    }
}
```

**LatticeCluster CRD addition** — new provider config:

```rust
pub struct BasisConfig {
    /// gRPC endpoint of the Basis controller, e.g. `https://10.0.0.1:7443`.
    pub controller_endpoint: String,

    /// Name of the Basis IP pool this cluster draws its VIP and VM IPs from.
    pub ip_pool: String,

    /// Reference to a K8s Secret holding the mTLS client cert used by the
    /// basis-capi-provider pod. CN must be `basis-capi-provider`.
    pub credentials_secret_ref: String,
}
```

Three fields, not seven. K8s-level concerns (kube-vip image, SSH keys, DNS servers, VIP network interface) belong on cluster-wide Lattice config, not on a per-provider block — they apply to every provider identically. The control-plane VIP is reserved by Basis via `CreateCluster` and written into `BasisCluster.spec.controlPlaneEndpoint` by the reconciler, so no user input is required for it.

This is significantly simpler than `ProxmoxConfig` because Basis handles scheduling, template management, VMID allocation, and VIP reservation internally.

## Crate Structure

```
crates/
  basis-proto/              # Protobuf definitions + generated code
  basis-common/             # Shared: TLS loading, peer-CN extraction, time helpers, GpuInfo
  basis-controller/
    src/
      main.rs               # Entry point, config loading, TLS setup
      config.rs             # ControllerConfig (uses shared TlsConfig)
      server.rs             # gRPC server, cluster + machine RPCs, agent stream
      scheduler.rs          # VM placement algorithm with GPU topology scoring
      db.rs                 # sqlite schema + typed IpOwner allocator
      host.rs               # Host health tracking loop
  basis-agent/
    src/
      main.rs               # Entry point; connect loop, handshake, inbound commands
      config.rs             # AgentConfig (uses shared TlsConfig)
      host_info.rs          # Host resource discovery (CPU/mem/disk)
      vm.rs                 # Cloud Hypervisor process management (systemd-run)
      image.rs              # Disk image pull, cache, qcow2 COW overlay
      network.rs            # Bridge + tap device management
      gpu.rs                # VFIO bind/unbind + NVLink topology via nvidia-smi
      handlers.rs           # create_vm / delete_vm / reconcile_against_expected
      reconcile.rs          # Startup reconciliation (agent restart vs. node reboot)
      db.rs                 # Local agent-side sqlite (crash-recovery cache)
  basis-capi-provider/      # CAPI controller (runs in K8s)
    src/
      main.rs               # Controller entry point
      crds.rs               # BasisCluster, BasisMachine, BasisMachineTemplate
      cluster.rs            # BasisCluster reconciler (calls CreateCluster/DeleteCluster)
      machine.rs            # BasisMachine reconciler (calls CreateMachine/DeleteMachine)
      bootstrap.rs          # Load CAPI bootstrap userdata from a Secret
      basis_client.rs       # Typed wrapper around the Basis gRPC API
```

## What Basis Is Not

- **Not a general-purpose hypervisor platform.** No UI, no multi-tenant API, no storage backends, no migration UI. It creates and deletes VMs for CAPI.
- **Not a container runtime.** VMs only. Containers run inside the VMs via K8s.
- **Not highly available on day 1.** Single controller. Existing VMs survive controller failure. CAPI retries handle transient unavailability.
- **Not a network virtualization layer.** Flat L2 bridging. Overlay networking is handled by Cilium inside K8s.
- **Not FIPS-certified itself.** Uses FIPS-validated crypto libraries (aws-lc-rs) but the VMM (Cloud Hypervisor) is not FIPS-certified. The FIPS boundary is at the Lattice/K8s layer.

## Future Work (Not Day 1)

- **Live migration.** Cloud Hypervisor supports it natively. Drain a host by migrating VMs to other hosts, zero downtime. Requires shared storage or pre-copy disk sync.
- **Controller HA.** Raft consensus between 3 controllers with replicated sqlite (via LiteFS or similar). Only needed at significant scale.
- **Secure boot / measured boot.** Cloud Hypervisor supports TDX and SEV-SNP. Attest VM integrity before admitting to K8s cluster.
- **SR-IOV networking.** Virtual functions for line-rate networking without bridge overhead. Relevant for GPU-to-GPU RDMA workloads.
- **Disk encryption.** LUKS-encrypted qcow2 overlays with keys from Vault. Defense in depth for data at rest.
