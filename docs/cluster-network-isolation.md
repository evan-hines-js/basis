# Cluster Network Isolation — Basis Design

Target: implementation inside the `basis` repo
(`/Users/evanhines/lattice/work/dir/lattos/basis`), consumed by Lattice
(`/Users/evanhines/lattice/work/dir/lattos/lattice`) as an infrastructure
capability that transparently isolates Lattice cluster trees from each
other.

This doc is self-contained. It assumes the reader knows that Basis is a
minimal bare-metal VM scheduler (controller + per-host agents, LVM-thin
storage, cloud-hypervisor VMs, gRPC API + CAPI provider) and that
Lattice sits above it as the opinionated Hyperconverged Cluster
Application.

## Background

Today every Basis-managed VM lands on a single shared L2 segment. The
host bridge (e.g. `basis0` in `/etc/basis/host.yaml`) masters the
physical uplink; every VM's TAP attaches to that bridge; every host's
bridge sits on the same broadcast domain as every other host's bridge.
All clusters share one `IpPool` with a split `vm_range` / `vip_range`.
No host-level filtering exists (`nftables`, `iptables`, `ebtables` — all
absent from the codebase).

That's fine for a single-tenant fleet. It stops being fine the moment
Basis serves more than one trust domain. A compromised node in
tenant-A's cluster has unrestricted L2 access to every other tenant's
VMs, VIPs, and ARP traffic.

Lattice's model is a **tree of clusters per trust domain**: a parent
cluster (cell) can spawn child clusters; children can spawn
grandchildren; all clusters within one tree share a root CA, an Istio
trust domain, and a distributed resource fabric. **Each separate tree
is a separate tenant.** Multiple trees can coexist on the same Basis
fleet.

Goal of this work: make the **tree** the hard L2 isolation boundary in
Basis, with zero policy intelligence inside a tree. Inside a tree,
Lattice's own mesh (Cilium + Istio) already owns every fine-grained
concern. Across trees, Basis must guarantee: no L2 path, no ARP
visibility, no IP reuse collisions, no accidental leakage.

## Design summary

**One VXLAN (VNI) per tree. Each host is a VTEP. Within a tree, flat
L2; across trees, no connectivity. North/south handled by Lattice via
dedicated "edge" nodes that Basis attaches a second NIC to.**

Concretely:

- A `Tree` is a new first-class Basis object: `(id, vni, cidr, ...)`.
- Every cluster belongs to exactly one tree. Root clusters create their
  tree; child clusters inherit their parent's tree.
- On each host with at least one VM in tree `T`, the agent maintains a
  bridge `bas-T<vni>` and a VXLAN device `vxlan-T<vni>` attached to
  that bridge. VM TAPs attach to the tree bridge instead of a shared
  bridge.
- The VXLAN runs unicast with FDB-based BUM replication. The controller
  is the source of truth for "which hosts have VMs in which tree" and
  pushes VTEP peer lists to agents; agents translate those into FDB
  entries.
- IPAM becomes per-tree: every tree carves its own CIDR from a
  configured supernet. Tree CIDRs may overlap across trees — they
  never meet at L2.
- A worker pool (or individual machine) can be marked `edge: true`.
  Basis attaches a **second** NIC to those VMs, tapped into the host's
  uplink rather than any tree bridge, with an IP allocated from a
  dedicated `edge_range` pool. Edge nodes are where Cilium BGP, kube-vip
  BGP, and Cilium EgressGatewayPolicy run — all configured by Lattice,
  not Basis.
- kube-vip and Cilium LBIPAM work unchanged; the only shift is that
  external LB VIPs want to run in BGP mode on edge nodes instead of ARP
  mode on arbitrary nodes.

The rest of this document is the implementation.

## What Basis owns vs. what Lattice owns

**Basis owns (hypervisor-layer must-haves):**

- The `Tree` object: id, VNI allocation, CIDR carving.
- Per-tree dataplane: bridge + VXLAN device per host per tree.
- FDB peer synchronization across hosts.
- Per-tree IPAM (`vm_range`, `vip_range`) carved from a configured
  supernet.
- `edge_range` IP pool on the uplink and second-NIC attachment for
  edge-flagged VMs.
- VNI reuse safety (cooldown before re-assigning a deleted tree's VNI).
- Host preflight: uplink MTU ≥ 1550, UDP 4789 egress permitted.

**Not Basis's problem (pushed up to Lattice):**

- Parent/child cluster relationships at the logical level. Basis learns
  "this cluster's parent is X" from Lattice on `CreateCluster`; it does
  not itself model the hierarchy beyond tree membership.
- BGP session configuration, peer selection, route advertisement.
  Cilium BGP Control Plane on edge nodes is Lattice's territory.
- LB VIP allocation — Cilium LBIPAM. Basis provides edge-node capacity;
  it does not allocate LB VIPs.
- Pod-level and service-level network policy — Cilium NetworkPolicy,
  Istio AuthorizationPolicy. These are intra-cluster concerns.
- mTLS trust domain, shared root CA, bilateral mesh agreements — all
  Lattice.
- kube-vip / Cilium L2-vs-BGP mode selection. Basis attaches an edge
  NIC; Lattice decides how to announce VIPs out of it.

## Tree lifecycle

A tree is created **implicitly** when Basis receives a `CreateCluster`
request with no `parent_cluster_id`. That cluster becomes the tree's
root. Every subsequent cluster in the tree carries
`parent_cluster_id: <some ancestor>`, and Basis resolves its `tree_id`
by walking to the parent and copying its `tree_id`.

Why implicit: Lattice already models "first cluster in a fleet" as a
distinct thing (the cell with `parent_config`). Asking users to also
call a `CreateTree` RPC first would be ceremony for no gain. Basis just
materializes the tree record as a side effect of the first cluster.

A tree is **deleted** when its last cluster is deleted. The VNI returns
to the pool after a 60-second cooldown (see "Known gotchas").

## Concrete data model changes

### Controller database (`basis-controller/src/db.rs`)

New table:

```sql
CREATE TABLE tree (
    id           TEXT    PRIMARY KEY,
    vni          INTEGER NOT NULL UNIQUE,
    cidr         TEXT    NOT NULL,
    vm_range_start TEXT NOT NULL,
    vm_range_end   TEXT NOT NULL,
    vip_range_start TEXT NOT NULL,
    vip_range_end   TEXT NOT NULL,
    gateway_ip   TEXT    NOT NULL,
    prefix_len   INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    deleted_at   INTEGER  -- for VNI cooldown
);
```

New index tracking which hosts carry tree membership (used to compute
VTEP peer lists):

```sql
CREATE TABLE host_in_tree (
    host_id  TEXT NOT NULL,
    tree_id  TEXT NOT NULL,
    PRIMARY KEY (host_id, tree_id)
);
```

Maintained as an invariant: a row exists iff the host has ≥1 VM whose
cluster's tree is `tree_id`. The controller inserts on first VM
scheduled, deletes on last VM drained.

Extend the existing `cluster` table:

```sql
ALTER TABLE cluster ADD COLUMN tree_id TEXT NOT NULL REFERENCES tree(id);
ALTER TABLE cluster ADD COLUMN parent_cluster_id TEXT REFERENCES cluster(id);
```

Extend the existing `vm` (or `machine`) row:

```sql
ALTER TABLE vm ADD COLUMN edge_ip TEXT;  -- NULL unless edge=true
```

(The `edge` flag itself lives in the `BasisMachine` spec; the allocated
IP is what Basis persists.)

### Config (`basis-controller/src/config.rs`)

The existing `IpPool` type splits. Global-pool semantics go away; the
controller config now holds a **supernet** from which per-tree pools
are carved, and a separate **edge pool**:

```rust
#[derive(Clone, Debug, Deserialize)]
pub struct NetworkConfig {
    /// RFC1918 supernet for tree CIDR allocation (e.g. "10.0.0.0/8").
    /// Each tree is carved as a /20 from this supernet.
    pub tree_supernet: ipnet::Ipv4Net,

    /// Prefix length of each per-tree CIDR. Default /20 (4094 usable).
    #[serde(default = "default_tree_prefix")]
    pub tree_prefix: u8,

    /// Fraction of each tree's CIDR reserved for VIPs, from the top.
    /// Same split model as the old IpPool; default 16 addresses.
    #[serde(default = "default_vip_reserve")]
    pub vip_reserve: u32,

    /// VNI allocation range. Defaults 10000..16_000_000.
    #[serde(default = "default_vni_range")]
    pub vni_range: (u32, u32),

    /// Edge IP pool — allocated to `edge: true` machines' second NICs.
    /// Lives on the uplink's subnet.
    pub edge_pool: EdgePool,

    /// VNI cooldown in seconds before reuse after tree deletion.
    #[serde(default = "default_vni_cooldown")]
    pub vni_cooldown_secs: u64,
}

pub struct EdgePool {
    pub cidr: ipnet::Ipv4Net,
    pub range_start: Ipv4Addr,
    pub range_end: Ipv4Addr,
    pub gateway: Ipv4Addr,  // uplink gateway for edge nodes
}
```

The old `IpPool` is removed. Existing deployments migrate by declaring
a `tree_supernet` that contains the old pool's CIDR and a `legacy` tree
(see migration section).

### Host config (`/etc/basis/host.yaml`)

Each host declares its VTEP identity and uplink properties:

```yaml
bridge: basis0              # existing, now only used for edge second-NIC attachments
uplink_interface: eno1      # new — physical NIC used for VTEP + edge bridge
vtep_address: 10.100.0.17   # new — this host's IP on the uplink used as VXLAN src
uplink_mtu: 9000            # new — must be >= 1550
```

The agent validates at startup that `uplink_mtu >= 1550` and refuses to
start otherwise.

### Proto (`basis-proto/proto/basis.proto`)

```proto
service BasisClusters {
  // ... existing RPCs ...
}

message CreateClusterRequest {
  string name = 1;
  // ... existing fields ...
  // When unset → this cluster is a tree root; controller allocates new tree.
  // When set → cluster joins referenced cluster's tree.
  optional string parent_cluster_id = 10;
}

message CreateClusterResponse {
  string cluster_id = 1;
  string tree_id = 2;                 // new
  uint32 vni = 3;                     // new
  string control_plane_endpoint = 4;  // unchanged
}

message Tree {
  string id = 1;
  uint32 vni = 2;
  string cidr = 3;
}

message CreateMachineRequest {
  // ... existing fields ...
  bool edge = 20;  // new — request a second NIC on the uplink
}

message Machine {
  // ... existing fields ...
  bool edge = 20;              // echoed from request
  optional string edge_ip = 21; // allocated by Basis if edge=true
}

// Sent from controller to agent when VTEP membership of a tree changes.
message TreePeersUpdate {
  string tree_id = 1;
  uint32 vni = 2;
  repeated string vtep_addresses = 3;  // all hosts that have VMs in this tree
}
```

### CRD (`basis-capi-provider/src/crds.rs`)

```rust
pub struct BasisMachineSpec {
    // ... existing fields ...
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub edge: bool,
}
```

`BasisCluster` gains an optional reference to its parent:

```rust
pub struct BasisClusterSpec {
    // ... existing fields ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_cluster_ref: Option<ClusterRef>,
}
```

## Controller behaviour

### `CreateCluster`

1. If `parent_cluster_id` is `None`:
   - Allocate a VNI from `vni_range`, skipping any VNI whose owning
     tree is within `vni_cooldown_secs` of deletion.
   - Carve a free `/tree_prefix` from `tree_supernet`.
   - Insert `Tree` row.
   - Assign `cluster.tree_id` to the new tree.

2. If `parent_cluster_id` is `Some(p)`:
   - Load `cluster[p].tree_id`; assign `cluster.tree_id = p.tree_id`.
   - Fail if parent doesn't exist or is `Deleting`.

3. Allocate control-plane VIP from the tree's `vip_range` (existing
   `allocate_vip()` flow, now scoped to the tree).

4. Return `CreateClusterResponse { tree_id, vni, control_plane_endpoint }`.

### `CreateMachine`

1. Resolve `cluster.tree_id`.
2. Allocate primary IP from `tree.vm_range` (existing `allocate_ip()`,
   scoped to the tree).
3. If `edge == true`, allocate secondary IP from `edge_pool.range`.
4. Schedule: no new constraints. The scheduler's existing
   placement logic applies unchanged — trees do not pin to hosts.
5. On placement-success:
   - Upsert `host_in_tree(host_id, tree_id)`. If this is the first VM
     of this tree on this host, compute the new VTEP peer list and
     broadcast a `TreePeersUpdate` to every agent in
     `host_in_tree(*, tree_id)`.
6. Send `CreateVMCommand` to the target agent, including
   `tree_id`, `vni`, `tree_cidr`, `gateway_ip`, `primary_ip`, and
   (if edge) `edge_ip`.

### `DeleteMachine`

1. Delete VM.
2. If no more VMs in `(host_id, tree_id)`: delete `host_in_tree` row;
   broadcast updated peer list.

### `DeleteCluster`

1. Refuse if cluster has live children (children share the tree; but
   orphaning a subtree is almost certainly a bug).
2. Delete cluster row.
3. If tree has zero clusters remaining: mark `tree.deleted_at = now()`.
   Actual VNI reuse is gated by the cooldown; the tree row is preserved
   until the VNI is re-allocated.

### Reconciliation

On controller restart: validate that `host_in_tree` is consistent with
current VM placements. Rebuild if not. Push fresh `TreePeersUpdate` to
every agent.

## Agent behaviour

New module `basis-agent/src/network/tree.rs`. Alongside today's
`basis-agent/src/network.rs` (which handles the single-bridge model),
`tree.rs` owns per-tree dataplane.

### Tree bridge + VXLAN lifecycle

For each tree with local VMs, the agent maintains exactly two netdevs:

- Bridge `bas-T<vni>` (Linux bridge, MTU = `uplink_mtu - 50`).
- VXLAN `vxlan-T<vni>` (VNI from the tree, remote port 4789, local
  VTEP address from `host.yaml.vtep_address`, learning **disabled** —
  FDB is controller-driven, not learned).

The VXLAN is `master`-attached to the bridge. VM TAPs are attached to
the bridge. That's the full topology.

```text
         +-- tap-basXXXX (VM primary NIC) ---+
         |                                    |
  bas-T10001 (bridge, MTU 8950) --- vxlan-T10001 (VNI 10001)
                                            |
                              encapsulated on eno1 (MTU 9000)
```

Agent operations:

- `ensure_tree(vni, cidr, gateway_ip, peers: Vec<IpAddr>)`:
  - idempotent. Creates bridge + VXLAN if absent. Attaches VXLAN to
    bridge. Sets MTUs. Brings both up. Replaces FDB BUM entries to
    match `peers` exactly (remove stale, add new). Returns.
- `attach_vm(tap_name, vni)`:
  - Bring tap up, master-attach to `bas-T<vni>`.
- `detach_vm(tap_name)`:
  - Removes master. (Bridge left in place.)
- `remove_tree(vni)`:
  - Removes bridge + VXLAN. Called when the last local VM in the tree
    is deleted.

### FDB management

Linux VXLAN with `nolearning` requires explicit FDB entries for BUM
replication. For each remote VTEP in the peer list:

```
bridge fdb append 00:00:00:00:00:00 dev vxlan-T<vni> dst <peer_ip>
```

MAC learning is **off** — inner MAC → outer VTEP bindings are not
learned from ingress traffic. This is deliberate: with learning on, a
misbehaving guest sending a spoofed inner MAC can poison the FDB on
other hosts, routing legitimate traffic to the attacker. With learning
off and BUM flooding via FDB, every unknown-unicast is flooded to every
peer VTEP — O(n) per cluster, not an issue at expected scale.

(Aggressive optimisation could push specific MAC→VTEP entries from the
controller, eliminating BUM flood for unicast. Defer. Correctness
first.)

### `TreePeersUpdate` handling

Stream arrives on the agent's existing controller connection. For each
update:

1. Resolve `vni → bridge name` locally.
2. If bridge doesn't exist yet (host has no local VMs in this tree),
   ignore — the update will be re-applied next time we call
   `ensure_tree`.
3. Otherwise, reconcile FDB to match `vtep_addresses`.

### Edge-NIC attachment

When `CreateVMCommand` has `edge: true` and `edge_ip: <ip>`:

1. Primary TAP attaches to `bas-T<vni>` as above.
2. A second TAP is created and attached to `basis0` (the legacy host
   bridge, which is now repurposed as the uplink bridge).
3. Cloud-init receives network config for **two** interfaces:
   - `eth0`: tree IP, gateway = `tree.gateway_ip`, default route.
   - `eth1`: edge IP, gateway = `edge_pool.gateway`, **no** default
     route — the edge NIC is for specific BGP/ingress traffic, not
     general egress. Host-level routing rules (`ip rule`) inside the
     guest select which interface to use; Cilium config owns that.

Cloud-hypervisor arguments pick up a second `--net` entry when the
agent sees `edge: true`.

### Reconciliation (agent cold-start)

The existing three-case reconciliation (agent restart, node reboot,
orphan cleanup) extends with a fourth step: **network rebuild**.

On startup, after loading local VM state from `agent.db`:

1. Group VMs by tree. For each tree with ≥1 local VM, call
   `ensure_tree(vni, cidr, gateway_ip, peers=<from last controller update>)`.
2. Re-attach TAPs to their tree bridges.
3. Garbage-collect any `bas-T*` / `vxlan-T*` netdev that doesn't
   correspond to a currently-scheduled tree.

Until the controller reconnects and re-pushes `TreePeersUpdate`, the
agent uses the peer list it persisted locally. Stale peer lists cause
temporary FDB gaps (some traffic black-holes); the controller's
reconnect flood fixes them within seconds.

## MTU discipline

VXLAN outer overhead is 50 bytes (Ethernet 14 + IPv4 20 + UDP 8 +
VXLAN 8). The agent enforces:

- `uplink_mtu >= 1550` at startup. Refuse to start if not.
- `bridge mtu = uplink_mtu - 50`.
- Guest MTU = bridge MTU, configured via cloud-init.

Jumbo frames (uplink 9000) give inner MTU 8950, which most guests and
Cilium tolerate; standard 1500-inside-1550 also works. The only
unsupported case is 1500 on the uplink, which breaks standard 1500-byte
inner frames. The startup check catches it before any VM boots.

## North/south: Cilium LBIPAM + BGP on edge nodes

This is the only moving part Basis does **not** implement; it is
configured by Lattice per-cluster on top of the edge-NIC primitive.

Two VIP scopes:

1. **Internal-only VIPs** (reachable from within the tree):
   - LBIPAM allocates from a `CiliumLoadBalancerIPPool` whose CIDR is
     inside the tree's CIDR.
   - Cilium L2 announcements on the tree interface advertise via ARP;
     ARP scope is the tree's VXLAN.
   - No edge node required.

2. **External VIPs** (reachable from outside):
   - Cluster has ≥1 worker pool with `edge: true`.
   - LBIPAM allocates from a pool whose CIDR is an externally routable
     range (known to the upstream router).
   - Cilium BGP Control Plane runs on nodes matching the edge pool;
     peers with an upstream router over the edge NIC's IP (allocated
     by Basis from `edge_pool`).
   - Upstream router ECMPs LB VIP traffic to edge nodes; Cilium
     kube-proxy-replacement forwards to backend pods over the tree
     VXLAN.
   - Pod egress: `CiliumEgressGatewayPolicy` selects edge nodes as the
     SNAT gateway; pod traffic exits via edge NIC.

kube-vip follows the same split. For internal-only control planes,
ARP on the tree VXLAN. For externally reachable control planes,
kube-vip in BGP mode on control-plane nodes that are themselves flagged
`edge`.

Basis is agnostic about all of this. From Basis's perspective, an
"edge" VM has two NICs; what the guest does with them is the guest's
business.

## Implementation plan

Four phases. Each lands independently.

### Phase 1 — `Tree` object + per-tree VXLAN dataplane

Goal: trees exist, VMs in different trees cannot reach each other at
L2. No CRD/Lattice changes yet; tests use direct gRPC calls against
the controller.

1. **Controller**:
   - Add `Tree`, `host_in_tree` tables (schema migration).
   - Add `parent_cluster_id` / `tree_id` columns to `cluster`.
   - Implement VNI allocation + /20 CIDR carving from supernet.
   - `CreateCluster`: implicit tree materialization on root; inheritance
     on children.
   - `TreePeersUpdate` stream message + broadcast on membership change.
   - Config migration: deprecate old `IpPool`, introduce
     `NetworkConfig` with `tree_supernet`, `vni_range`, etc.
2. **Agent**:
   - `basis-agent/src/network/tree.rs`: `ensure_tree`, `attach_vm`,
     `detach_vm`, `remove_tree`, FDB reconcile.
   - Cold-start reconciliation rebuilds bridges from persisted VM
     state.
   - Startup MTU check on `uplink_interface`.
3. **Proto**:
   - `CreateClusterRequest.parent_cluster_id`
   - `CreateClusterResponse.tree_id`, `vni`
   - `TreePeersUpdate`
4. **Migration**:
   - On first startup after upgrade, controller detects pre-tree state
     and auto-creates a single `legacy` tree containing every existing
     cluster. All existing VMs move to the legacy tree's bridge on next
     agent reconcile (requires VM downtime for network reattach — call
     it out in release notes, or do a rolling drain+recreate in a
     follow-up migration tool).

Test at the end of Phase 1: two trees × two clusters × two hosts, with
`tcpdump` confirming zero cross-tree frames. Cross-tree `ping`
timeouts. Same-tree `ping` across hosts works.

### Phase 2 — per-tree IPAM

Goal: clusters in different trees can allocate overlapping CIDRs
without collision.

1. Drop the single-pool `IpPool`. Move `vm_range` / `vip_range` onto
   the `Tree` row, computed when the tree is created.
2. `allocate_ip` / `allocate_vip` take `tree_id`.
3. Scheduler / CAPI provider plumb `tree_id` into placement and
   response paths.
4. Controller-authoritative reconciliation of IP leases within a tree
   (the existing TODO from `project_basis_architecture.md` — tree
   scope makes this simpler to implement correctly).

Test: create tree A (CIDR `10.1.0.0/20`) and tree B (CIDR `10.1.0.0/20`
— same range, different tree). Verify:
- Both trees allocate `10.1.0.20` to a cluster node. Both work.
- From tree A's `.20`, ARP for tree B's `.20`: no reply.
- Traffic between them is dropped (no L2 path).

### Phase 3 — edge-node support

Goal: `edge: true` machines get a second NIC on the uplink.

1. **Proto**: `CreateMachineRequest.edge`, `Machine.edge`, `Machine.edge_ip`.
2. **CRD**: `BasisMachineSpec.edge: bool`.
3. **Controller**:
   - `edge_pool` config section. Allocation from `edge_range`.
   - Resource accounting: an edge machine charges against `edge_pool`
     capacity. Exhaustion surfaces as `ResourceExhausted` on the
     `BasisMachine`.
4. **Agent**:
   - Second TAP creation, `basis0` attachment, second `--net` arg to
     cloud-hypervisor.
   - Cloud-init network config generator emits two interfaces with
     distinct gateways and routing tables.
5. **Guest-side** (Lattice's problem, but call it out): Lattice's
   control plane generates `CiliumBGPClusterConfig`,
   `CiliumLoadBalancerIPPool`, `CiliumEgressGatewayPolicy` with
   nodeSelectors pointing at the edge pool's label.

Test: cluster with a 3-node edge pool. Each edge VM has `eth0` on the
tree and `eth1` on the uplink subnet. Manually configure FRR on a test
router to peer with edge nodes' `eth1` IPs; verify BGP sessions come
up; verify LB VIP advertised; verify external `curl` reaches a Service
backed by workload pods.

### Phase 4 — Lattice integration

(Lives in the Lattice repo; listed here for completeness.)

1. **CRD**: `LatticeClusterSpec.parentClusterRef: Option<ClusterRef>`.
2. **Provider**: `lattice-capi/src/provider/basis.rs` plumbs
   `parentClusterRef` → `CreateClusterRequest.parent_cluster_id` and
   threads the returned `tree_id` into `BasisCluster.status`.
3. **Worker pool**: `NodeResourceSpec.edge: Option<bool>` (new field,
   mirrors the `dataDiskGibs` pattern from the Rook integration).
4. **Cilium install**: `lattice-cilium` extends its value generation
   to emit `CiliumBGPClusterConfig` + `CiliumLoadBalancerIPPool` when a
   cluster has any edge-flagged worker pool. Peer addresses come from a
   new `LatticeClusterSpec.networking.bgpPeers` field (Lattice-level
   concern — basis doesn't know about routers).

Test plan for Phase 4 is Lattice-side; reference the Rook-integration
doc for the pattern.

## End-to-end sequence (mental model)

User in org X applies their first `LatticeCluster` — no `parentClusterRef`:

1. Lattice operator calls `basis.CreateCluster` with
   `parent_cluster_id = None`.
2. Basis allocates VNI 10001, carves CIDR 10.1.0.0/20, creates
   `tree { id=T1, vni=10001, cidr=10.1.0.0/20 }`.
3. Basis allocates control-plane VIP `10.1.0.2` from T1's `vip_range`.
4. Lattice provisions CAPI objects; `BasisMachines` created; scheduler
   places them across hosts H1, H2, H3.
5. On each host, agent calls `ensure_tree(10001, ...)`, creates
   `bas-T10001` + `vxlan-T10001`, configures FDB peers = {H1,H2,H3}.
6. VM TAPs attach to `bas-T10001`. Guests boot with IPs from T1's
   `vm_range`. kube-vip on the first control-plane guest claims
   `10.1.0.2` via gratuitous ARP — broadcast scope is T1's VXLAN only.
7. Cluster comes up; Lattice's always-on components install; Cilium
   forms the CNI inside the tree.

User adds a child cluster under that root — `parentClusterRef = <root>`:

1. Lattice → `basis.CreateCluster(parent_cluster_id=<root>)`.
2. Basis resolves root's `tree_id = T1`; assigns child's `tree_id = T1`.
3. Child control-plane VIP `10.1.0.3` allocated from T1's `vip_range`.
4. Child VMs placed on hosts (some overlap with root, some not). On
   any host new to T1, agent calls `ensure_tree` and the controller
   broadcasts an updated peer list.
5. Child VMs attach to the same `bas-T10001` bridges — they are L2
   adjacent to root VMs.
6. Child's agent bootstrap dials `<root VIP>:8443` for kubeadm, then
   `<root VIP>:50051` for the long-lived gRPC stream. ARP works because
   they're on the same VXLAN.

User in org Y applies a separate cluster — no `parentClusterRef`:

1. Basis allocates VNI 10002, CIDR 10.2.0.0/20, tree T2.
2. VMs placed on some hosts that already run T1 VMs. Each affected
   host ends up with **two** bridges (`bas-T10001`, `bas-T10002`) and
   two VXLAN devices.
3. T1 and T2 share physical infrastructure but have zero L2 path.
4. `tcpdump -i vxlan-T10001` on any host shows only T1 frames. Likewise
   for T2.

User adds an edge worker pool to org X's root cluster:

1. Worker pool's `BasisMachineTemplate.spec.edge = true`.
2. For each edge `BasisMachine`:
   - Primary IP: `10.1.0.121` from T1.
   - Secondary IP: `192.168.100.5` from `edge_pool`.
3. Agent creates two TAPs: one on `bas-T10001`, one on `basis0`.
4. Guest boots with `eth0` (tree) and `eth1` (uplink).
5. Lattice's Cilium config deploys BGP CP + LBIPAM + EgressGateway
   with nodeSelector matching the edge label.
6. Edge nodes peer with upstream router from `eth1`. LB VIPs announced.
   External clients reach Services. Pod egress SNATs through edge NICs.

## Testing

### Unit

- `basis-controller/src/ipam/tree.rs`: tree CIDR carving, VNI
  allocation (including cooldown), `vm_range`/`vip_range` split,
  overlapping-CIDR independence across trees.
- `basis-controller/src/scheduler.rs`: placement unaffected by tree
  membership (trees do not pin hosts).
- `basis-agent/src/network/tree.rs`: bridge name deterministic from
  VNI, FDB reconcile idempotent, MTU derived from uplink.

### Integration (single host, two trees)

On a throwaway host or CI runner:

1. Start controller + agent. Host has `eno1` MTU 9000, VTEP `10.100.0.10`.
2. `basis.CreateCluster(name=A-root, parent=None)` — expect
   `tree_id=T1, vni=10001`.
3. Create 4 `BasisMachines` in A-root.
4. `basis.CreateCluster(name=B-root, parent=None)` — expect
   `tree_id=T2, vni=10002`.
5. Create 4 `BasisMachines` in B-root.
6. `ip link` on the host shows `bas-T10001`, `vxlan-T10001`,
   `bas-T10002`, `vxlan-T10002`.
7. From an A-root VM, `ping` every other A-root VM: all succeed.
8. From an A-root VM, `ping` every B-root VM: all time out.
9. From an A-root VM, `arping` a B-root IP: no reply.
10. `tcpdump -i vxlan-T10001`: only T1 IPs ever seen. Likewise T10002.
11. Delete A-root. VNI 10001 held in cooldown for 60s. `ip link` shows
    `bas-T10001` removed. Creating a new tree within 60s → gets VNI
    10003, not 10001.

### Integration (multi-host)

1. Three hosts, two trees spread across them.
2. Each host has only the bridges for trees it actually hosts — verify
   `bas-T*` netdev count matches local tree set.
3. Cross-host same-tree `ping` works (VXLAN-encapsulated frames on
   `eno1`, confirmed via `tcpdump -i eno1 udp port 4789`).
4. Cross-host cross-tree `ping` fails.
5. Drop one host's `eno1` (simulate network partition); verify that
   host's VMs stop receiving peer traffic, the other two hosts continue
   to talk. Restore link; verify peer traffic resumes without
   controller intervention (FDB entries persist).
6. Kill agent on host 2 (`systemctl stop basis-agent`); cold-start.
   Verify bridges + VXLAN + FDB reconstructed from persisted state.
7. Kill controller mid-provision; restart. Verify in-flight
   `TreePeersUpdate` re-sent on reconnect; agents converge.

### Integration (edge nodes)

1. Cluster with one worker pool `edge: true` (3 replicas).
2. Each edge VM has `lsblk` + `ip addr` showing two NICs: `eth0` on the
   tree CIDR, `eth1` on the uplink CIDR.
3. On the host, each edge VM has two TAPs — one on `bas-T<vni>`, one
   on `basis0`.
4. Stand up a test FRR container peering with the edge node's `eth1`;
   manually advertise a route; confirm edge node installs it.
5. Apply a Cilium `CiliumBGPClusterConfig` with the FRR peer; apply a
   `CiliumLoadBalancerIPPool` with externally-routable CIDR; create a
   `Service type: LoadBalancer`; confirm Cilium BGP announces the
   allocated VIP; `curl` the VIP from an external host; confirm it
   reaches the backend.
6. Apply `CiliumEgressGatewayPolicy` selecting edge nodes; curl
   external endpoint from a pod on a non-edge node; confirm source IP
   seen externally is the edge node's `eth1`.

### Destructive / failure cases

- Allocate trees until `tree_supernet` is exhausted. Confirm
  `ResourceExhausted` on `CreateCluster`. Delete one; confirm the freed
  CIDR is reusable.
- Allocate edge IPs until `edge_pool` exhausted. Confirm
  `ResourceExhausted` on the offending `BasisMachine`.
- Start agent with `uplink_mtu: 1500`. Confirm agent refuses to start
  with a clear error message, not a cryptic VXLAN failure later.
- Block UDP 4789 egress at the host level (`iptables -A OUTPUT -p udp
  --dport 4789 -j DROP`). Confirm cross-host tree traffic fails
  loudly; the agent's preflight detects it.

## Known gotchas

- **VNI reuse race**: when a tree is deleted and a new tree
  immediately takes the same VNI, any in-flight VXLAN frames for the
  old tree that arrive at peer hosts are decapsulated and delivered to
  the new tree's bridge as if they were legitimate. The 60-second
  `vni_cooldown_secs` is long enough to outlast any reasonable
  TCP retransmit / OS-level flush. Do not lower it.

- **MTU failure modes are silent**: if `uplink_mtu` is 1500 and the
  agent somehow starts (preflight bypassed, misconfigured host),
  TCP paths with 1500-byte segments + VXLAN encap hit the uplink at
  1550 and get silently dropped by the NIC. Symptom: small packets work,
  large ones time out. Mitigations: (1) enforce the preflight check,
  (2) set bridge MTU to 1450 when uplink is 1500 (documented degraded
  mode — works, but guests must respect PMTUD).

- **Edge NIC is a trust-boundary hole**: edge nodes straddle the tree
  and the uplink. If an edge node is compromised, the attacker has
  L2 access to the uplink — where other trees' edge NICs live. This
  is inherent to the "edge node as BGP speaker" pattern; it cannot be
  fixed at Basis's layer. Mitigation is Lattice's job (restrict
  workload placement on edge nodes via taints/tolerations,
  least-privilege pod specs, etc.). Document in the Lattice side doc.

- **Tree deletion must cascade**: deleting the root cluster while child
  clusters still exist would orphan the tree. The controller refuses
  the delete; Lattice must drain children first. This matches how
  Lattice already handles cluster hierarchy teardown.

- **Existing pre-tree deployments**: the Phase-1 migration creates a
  single `legacy` tree covering every pre-existing cluster. The first
  agent reconcile after upgrade moves every running VM's TAP from the
  old `basis0` bridge to `bas-Tlegacy<vni>`. This **interrupts** the
  VM's L2 briefly (≈100ms). Plan for it — either schedule a
  maintenance window, or do a rolling drain+recreate via CAPI
  remediation.

- **`basis0` bridge repurposing**: the legacy host bridge stays but
  becomes the **uplink bridge** — only edge NICs attach to it. All
  normal VM traffic moves to tree bridges. Non-edge VMs never touch
  `basis0` again. Phase 1 should delete any lingering non-edge TAPs
  from `basis0` at reconcile time.

- **FDB learning off + misbehaving guest**: with `nolearning` set,
  a guest sending frames with a spoofed source MAC still transmits
  them — the frames just never cause FDB poisoning. But they can still
  reach other VMs in the same tree (shared bridge, learning or not).
  This is "same-tenant" behaviour; Lattice's Istio mTLS handles it.
  It is not a cross-tree leak.

- **Scheduler and tree affinity**: the current scheduler has no notion
  of tree. Placements are tree-oblivious. This is correct — a tree
  spans whatever hosts its VMs happen to land on. If we ever want
  "dedicate host H to tree T" (hard-tenant physical isolation), that's
  a separate feature: a `Host.exclusive_tree` field, plumbed through
  the scheduler's `filter` step. Out of scope for this design.

## Non-goals for v1

Resist the temptation:

- **Multi-DC / cross-WAN trees.** Current design assumes hosts share
  L3 reachability with low latency. Extending a VNI across WAN needs
  EVPN or equivalent — don't do it.
- **Encryption on the wire.** VXLAN frames are plaintext. The
  assumption is that the uplink is a trusted DC fabric. If a tenant
  wants on-the-wire encryption, they enable WireGuard or IPsec at the
  CNI level (Cilium encryption) inside the tree. Basis does not
  encrypt.
- **Nested trees.** A tree is flat; it is the unit of tenancy. If you
  want a "sub-tenant," create a separate tree.
- **Per-cluster isolation within a tree.** Lattice's Cilium + Istio
  own this. Adding a second policy system at Basis layer is
  redundant and creates "which layer said no" debugging.
- **Anti-spoof at TAP ingress.** The VXLAN is the isolation boundary;
  src-IP inside a tree is trusted the same way src-IP on a normal L2
  segment is. If you want strict anti-spoof (e.g. for hostile
  same-tree tenants), it's a separate feature and it doesn't change
  the tree architecture.
- **Automated uplink VLAN per tree.** The edge NIC lands on a shared
  uplink. If an environment requires per-tree uplink isolation too,
  wrap the edge NIC in a VLAN subinterface selected from an
  edge-VLAN pool. Out of scope here.
- **Cross-tree controlled routing.** If you need tree-A and tree-B to
  exchange specific traffic, either (a) merge them into one tree, or
  (b) route over the public internet via edge NICs with an explicit
  external gateway. Basis does not broker inter-tree traffic.

## Commit hygiene

- One PR for the schema migration, `Tree` object, and VNI/CIDR
  allocation (controller-only, no agent changes — purely additive).
- One PR for the agent's per-tree bridge/VXLAN/FDB module, with
  Phase-1 tests that drive it via direct gRPC.
- One PR for per-tree IPAM (Phase 2) — self-contained once Phase 1 has
  landed.
- One PR for edge-node support (Phase 3) — proto + CRD + agent +
  controller, matched by an integration test harnessing a test BGP
  peer.
- Lattice-side integration (Phase 4) lives in a sibling doc in the
  Lattice repo.

Small, reviewable chunks. Do not big-bang.

## Open questions to confirm before coding

- **Supernet size**: is `10.0.0.0/8` carved into `/20` trees
  appropriate, or do we need larger / smaller per-tree CIDRs? 4096
  trees × 4094 IPs each seems more than enough; confirm with the
  Lattice fleet-size target.
- **VNI base**: start at 10000 (leaving 0–9999 for infrastructure /
  reserved use) — confirm no conflict with any existing VXLAN on the
  uplink.
- **Edge pool semantics**: single global edge pool, or per-tree edge
  sub-pools? Single global is simpler and matches "edge NIC lands on
  shared uplink." Per-tree adds isolation at the uplink level and
  requires uplink VLAN tagging. Default: global; revisit in v2.
- **Parent reference field naming on the Lattice side**: does Lattice
  already have a convention for parent refs? Check
  `LatticeCluster`-adjacent CRDs and mirror.
- **VM live migration of network identity**: currently no live
  migration (Basis scope boundary). If a VM is recreated (CAPI
  remediation), it re-enters the same tree with a new IP from the
  tree's `vm_range`. Confirm Lattice's existing re-provisioning flows
  tolerate IP churn — they should, because every provider re-provision
  is an IP change today.

End of guide.
