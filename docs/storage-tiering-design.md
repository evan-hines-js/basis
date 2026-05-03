# Storage Tiering & Multi-Backend Disks

Status: draft for review

## Problem

Basis is not a generic VM hypervisor with arbitrary disk needs. Basis provides storage-bearing Kubernetes nodes for Lattice-managed clusters, and those nodes overwhelmingly run Rook/Ceph OSDs. The scheduler therefore needs to understand storage as a **replicated failure-domain resource per Lattice cluster**, not as host-local capacity. The current design — one linear LV in one per-host VG — is the absolute floor of what makes Ceph come up at all, and it shows: a Ceph OSD on a Crucial BX500 hits 700 ms commit latency because there is no way to tell basis "this disk goes on the enterprise drive, not whichever VG `host.yaml` happens to point at."

The product-specific shape of the workload is the design's anchor. A typical hypervisor in the field will host:

- 10 VMs across 6 Lattice clusters
- Each Lattice cluster running its own Rook/Ceph OSDs across its VMs
- Many clusters happily sharing the same physical NVMe drives
- But within any one cluster, OSDs MUST NOT share a drive — that defeats the replication

Concretely, the scheduler must enforce:

- `cluster-a/osd-0` and `cluster-a/osd-1` on **different** physical drives (correctness).
- `cluster-a/osd-0` and `cluster-b/osd-0` on the **same** drive is fine (capacity reuse across clusters).

What today's storage code blocks:

- **Per-cluster OSD media selection.** Mixing SATA bulk and NVMe fast on one host. "Cluster A's OSDs on fast, cluster B's on bulk" requires two basis fleets today.
- **Hardware isolation per OSD.** Two OSDs of the same Rook cluster sharing a drive give up failure-domain independence and bleed p99 latency at the device queue. No way to say "OSDs of this cluster must each own their drive."
- **NVMe at native rates.** virtio + dm-linear is fine for SATA. At 1M+ IOPS per drive, the dm path becomes the floor of guest p99. Hyperscaler-style namespace passthrough has nowhere to live.
- **Failure-domain-aware OSD placement.** Two OSDs of the same cluster can land on disks that share a drive on the same host; basis has no concept of device as a scheduling axis.
- **Schedulable storage requirements.** The scheduler treats data disks as one capacity total. It can't reason about "175 GiB of low-latency, NVMe-backed storage for a Rook OSD" — only "175 GiB."

## Design principles

1. **Storage is selected the same way nodes are: by label.** Operators tag pools in ansible (`tier`, `medium`, `isolation`, etc.) using whatever vocabulary they want. Workloads carry per-disk selectors with the same `requires`/`prefers` shape `PlacementSpec` already uses for host placement. Pool selectors and host selectors share matching code; nothing is special-cased for storage.
2. **Backends are an agent-side detail, never on the wire.** A request says "I need a pool with these labels"; the agent dispatches to the LVM, raw-passthrough, or NVMe-namespace backend that the matched pool happens to use. Same workload deploys on homelab dm-linear and production NVMe namespaces unchanged.
3. **Pool is the operator abstraction; device is the failure domain.** A pool is a logical grouping of devices (or NVMe controllers) that share a backend and a label set. A pool with 24 identical NVMes is one pool, not 24. The scheduler picks a `(pool, device)` tuple at placement time: selector matching narrows the pool, capacity and failure-domain anti-affinity narrow the device.
4. **Same-cluster anti-affinity is hierarchical and automatic.** OSDs of one Lattice cluster spread across hosts first, then across devices on the same host. Different Lattice clusters share devices freely (capacity reuse). The scheduler enforces this without operator-written affinity rules; the `cluster_id` carried on every disk request is the pivot.
5. **Disk `purpose` shapes scheduling semantics.** A disk declares whether it is a Rook OSD or generic data; the scheduler applies stricter spreading to OSDs and surfaces them as first-class objects in telemetry. Generic data disks (etcd snapshots, container logs, app state) skip the OSD-specific anti-affinity but still respect labels and capacity. Today every basis data disk is an OSD; tomorrow they won't be, and the wire format already says so.
6. **No compatibility stubs. No dead options. One way to do each thing.** Pre-release means we delete the old single-pool model in the same change. The legacy `extra_disk_gibs: []u32` interface goes away. `storage.data` in `host.yaml` goes away. The agent's `data` vs `rootfs` permit split goes away.
7. **Refactor while we're here.** The current storage code has accumulated coincidences worth straightening out: dual permit pools, two parallel `*_lv_path` helpers, `vmdata-` prefix held over from PVE coexistence, no abstraction for "where does this LV live." The new model justifies one storage subsystem with one Pool abstraction, one Backend trait, one Reservation ledger.

## Goals & non-goals

**v1 goals:**

- Multiple storage pools per host, each with operator-defined labels.
- One backend: `lvm-linear`, with one VG per physical device. This is the real product unlock for Rook/Ceph on basis; it gives multi-tier hosts, device-aware failure domains, and per-cluster OSD spreading without taking on the implementation/testing burden of raw passthrough or NVMe namespace lifecycles.
- Per-disk label selector (`requires`/`prefers`) on BasisMachine, structurally identical to `PlacementSpec`.
- Per-disk `purpose` field (`replicated` | `generic-data`) shaping scheduling semantics.
- Hierarchical same-cluster anti-affinity (host-level preferred, device-level enforced) for OSD disks, automatic.
- Persistent device reservations in the agent, recovered on restart.
- Outbox-based controller→agent command durability.
- Telemetry: per-pool, per-device, and per-Rook-cluster OSD layout exposed by the controller.

`raw-disk` and `nvme-namespace` backends are explicitly **deferred to later milestones** (M2/M3). The architecture admits them — the pool/device model and the `DiskBackend` trait are designed so adding them is additive — but v1 ships only `lvm-linear` to keep the scope honest. We add the next backend the moment a customer's workload proves the dm-linear path is the bottleneck.

**v1 non-goals (called out so review is honest):**

- **Live storage migration** between pools/tiers. Ceph drains its own OSDs; etcd / Postgres / Kafka all have application-level mechanisms. Adding it at the basis layer duplicates that and adds consistency-under-concurrent-writes hazards we don't need.
- **Per-disk QoS throttles** (blkio cgroup limits, virtio-blk rate limits). Useful, not blocking. v2.
- **Encryption at the basis layer.** Guests own dm-crypt / LUKS / TCG Opal; basis hands them a device.
- **Multi-tier rootfs.** Every VM still gets its rootfs from the host's single thin pool. Rootfs is small and CoW-shared; tiering it duplicates golden images and pays for a problem nobody has yet.
- **vfio-pci controller passthrough.** One VM owning an entire NVMe controller is a different feature with IOMMU-group constraints. Future scope.
- **Cross-host storage** (NVMe-oF, SPDK target, Ceph block as a basis primitive). Different problem domain.

## Pool model

A pool is the unit operators describe. Every pool has:

- **One backend** (`lvm-linear`, `raw-disk`, `nvme-namespace`) — drives how the agent allocates, releases, and exposes the disk to cloud-hypervisor.
- **N devices** (or N controllers for `nvme-namespace`) — the physical members. Each device is independently failable, drainable, and capacity-tracked. A homelab pool has 1–4 members; an enterprise NVMe pool has 24+. Operators add and remove members in `host.yaml`; ansible templates over discovered hardware trivially.
- **Operator labels** — arbitrary `key=value` map. The selector vocabulary. Labels apply to the whole pool: every device in `pool=bulk` shares `tier=bulk`, etc.
- **Capacity** — tracked per device. The pool's reported total is the sum of its devices' free space; the scheduler reasons device-by-device when picking placement so it can enforce per-device failure-domain.

Two consequences worth being explicit about:

**A device cannot belong to two pools.** Pools partition the host's storage. The agent rejects an overlapping config at startup. This keeps capacity arithmetic and reservation bookkeeping unambiguous.

**Within an `lvm-linear` pool, each device gets its own VG.** No LVM-thin, no PV-spanning LVs, no `lvcreate --vgpolicy`. The VG name is `basis-<pool>-<device-suffix>` and is created/maintained by ansible alongside the pool config. This keeps device-level failure-domain a property of the LVM layout, not a runtime constraint we have to enforce in `lvcreate` invocations. When a device dies, its VG dies with it; the pool keeps serving from its surviving members.

For homelab parity: one pool, multiple S3610s, labeled identically, the scheduler spreads cluster mates across them. For enterprise parity: one pool, 24 NVMes, labeled identically, the scheduler spreads cluster mates across them. Same config shape; the only difference is the length of the `devices:` list.

## Host config (`host.yaml`)

```yaml
host_id: 565f4783-06cc-4d43-bcd0-6da7db391706
data_dir: /var/lib/basis

storage:
  rootfs:
    vg: pve-rootfs
    thin_pool: pve-rootfs-thin

  pools:
    - name: bulk
      backend: lvm-linear
      labels:
        tier: bulk
        medium: sata
      devices:
        - id: ata-INTEL_SSDSC2BX800G4_aaaa
          vg: basis-bulk-aaaa
          size_gib: 745
        - id: ata-INTEL_SSDSC2BX800G4_bbbb
          vg: basis-bulk-bbbb
          size_gib: 745

    - name: fast
      backend: lvm-linear
      labels:
        tier: low-latency
        medium: nvme
        vendor: intel
      devices:
        - id: nvme-INTEL_SSDPE2KX040T8_dddd
          vg: basis-fast-dddd
          size_gib: 3725
        - id: nvme-INTEL_SSDPE2KX040T8_eeee
          vg: basis-fast-eeee
          size_gib: 3725
```

Schema notes (M1):

- `devices:` is a list. A pool with one member is fine, a pool with 24 members is fine; the schema doesn't change shape with scale.
- Each device entry carries the stable `/dev/disk/by-id/` identifier and the VG name ansible has created on it. The agent does not create VGs; PV/VG creation is ansible's job (it's a system-state operation that benefits from being declarative across host rebuilds).
- Pool `name` is host-local and stable; reservations and telemetry are keyed on `(host, pool, device_id)`.
- `labels` is the only operator-vocabulary field. Anything semantic — `tier`, `medium`, `isolation`, `vendor`, `firmware-class`, `rack` — goes here. The scheduler treats it as opaque key-value.
- A device may not appear in two pools. The agent verifies this at startup and refuses to start on overlap. Reservations and capacity arithmetic depend on partitioning.
- The legacy `storage.data.vg` field is removed entirely. Ansible deploys the new shape; no shim layer.

### Future backend shapes (M2/M3 — not implemented in v1)

The same `pools[]` list grows additional backend types when M2 and M3 land. Sketches for context only; the wire format and pool model don't change shape:

```yaml
# M2 — raw-disk pool. Same `devices:` shape as lvm-linear, no `vg` field.
- name: dedicated-osd
  backend: raw-disk
  labels:
    tier: low-latency
    medium: nvme
    isolation: exclusive
  devices:
    - id: nvme-INTEL_SSDPE2KX040T8_dddd
      size_gib: 3725

# M3 — nvme-namespace pool. `controllers:` instead of `devices:`.
- name: tenant-ns
  backend: nvme-namespace
  labels:
    tier: low-latency
    medium: nvme
    isolation: namespace
  controllers:
    - id: nvme-SAMSUNG_PM1733_zzzz
      path: /dev/nvme0
      total_capacity_gib: 7450
      max_namespaces: 32
```

For M1 implementers: ignore everything in this subsection. The agent's host-config parser should reject `backend: raw-disk` and `backend: nvme-namespace` with a clear "not yet supported in this build" error rather than parsing them silently.

## CAPI surface

One additive concept on `BasisMachine`: `storage_disks`. Replaces `extra_disk_gibs`.

```yaml
# BasisMachineTemplate, written once per role
apiVersion: infrastructure.cluster.x-k8s.io/v1beta1
kind: BasisMachineTemplate
metadata:
  name: ceph-osd
spec:
  template:
    spec:
      cluster: backend-pool
      cpu: 8
      memory_mib: 16384
      disk_gib: 250
      image: lattice-node:v1.32.0
      placement:
        requires:
          - key: role
            values: [storage]
      storage_disks:
        - min_size_gib: 175
          purpose: replicated
          selector:
            requires:
              - key: tier
                values: [low-latency]
            prefers:
              - key: medium
                value: nvme
                weight: 10
```

`storage_disks[].selector.requires` and `storage_disks[].selector.prefers` use **the exact same types** as `placement.requires` / `placement.prefers`. No new shapes, no new validation rules, no new docs to write — the operator who learned host placement already knows pool placement. The CAPI provider validates them with the same code path.

`purpose` is the only storage-specific concept on the disk: `replicated` activates hierarchical same-cluster anti-affinity (host first, device second), `generic-data` doesn't. **`DISK_PURPOSE_UNSPECIFIED` is invalid after CRD admission/defaulting** and the scheduler rejects any request that reaches it with an unspecified purpose. The CAPI provider's defaulting webhook is responsible for resolving it: if the requesting BasisMachineDeployment is a known storage role, default to `replicated`; otherwise default to `generic-data`. If neither inference is possible, fail validation rather than guess — accidentally defaulting an OSD disk to `generic-data` would silently disable the same-cluster anti-affinity that protects Ceph durability.

For workloads that don't care about storage tier (control planes, edge nodes), `storage_disks` carries `purpose: generic-data` with empty selectors; any pool with capacity wins. There is no implicit "default tier" on the wire — empty selector means "any" and the scheduler picks.

`extra_disk_gibs` is deleted from CRDs, proto, controller, agent, ctl, and tests in the same change. No translator, no fallback.

## Wire format

```proto
// Reused for both host placement (existing) and pool placement (new).
// One selector type, one match algorithm, one set of tests.
message LabelSelector {
  repeated PlacementRequirement requires = 1;
  repeated PlacementPreference prefers = 2;
}

message PlacementRequirement {
  string key = 1;
  repeated string values = 2;
}

message PlacementPreference {
  string key = 1;
  string value = 2;
  uint32 weight = 3;
}

message StorageDisk {
  // Minimum requested capacity. Backends MAY satisfy with more (e.g. raw-disk
  // hands the guest the whole device, nvme-namespace rounds up to the
  // controller's allocation granularity). The actual allocated size is
  // reported back in `DiskAssignment.actual_size_gib`.
  uint64 min_size_gib = 1;
  LabelSelector selector = 2;  // empty == any pool

  // Workload role. Drives scheduling semantics, not just telemetry:
  //   REPLICATED: hierarchical same-cluster anti-affinity (host first, then
  //             device); never co-locate two OSDs of the same cluster on
  //             the same device.
  //   GENERIC_DATA: respects labels + capacity, but no special spreading.
  //                 Used for etcd snapshots, container logs, app state.
  // Defaulting is deliberate: if Lattice doesn't set it, the controller
  // infers REPLICATED when the requesting MachineDeployment is a storage role,
  // GENERIC_DATA otherwise. The wire field is explicit so the inference is
  // always visible in audit/debug.
  DiskPurpose purpose = 3;
}

enum DiskPurpose {
  DISK_PURPOSE_UNSPECIFIED = 0;
  DISK_PURPOSE_REPLICATED = 1;
  DISK_PURPOSE_GENERIC_DATA = 2;
}

message CreateMachineRequest {
  // ... existing fields ...
  LabelSelector placement = N;          // host selector, was PlacementSpec
  repeated StorageDisk storage_disks = M;
}

// Agent receives a resolved pool name per disk. Selector matching happened
// at the controller; the agent never re-runs it.
message CreateVMCommand {
  // ... existing fields ...
  repeated CommandedDisk storage_disks = K;
}

message CommandedDisk {
  // Stable controller-generated UUID; the idempotency key. Resending the
  // same assignment_id is a no-op if the agent already has the disk Ready;
  // a different assignment_id for the same (vm_id, disk_index) is a hard
  // conflict the agent rejects.
  string assignment_id = 1;
  uint64 min_size_gib = 2;
  string pool = 3;             // host-local pool name, picked by scheduler
  string device_id = 4;        // failure-domain key (device or controller id)
  uint32 disk_index = 5;       // stable index for naming/recovery
  string cluster_id = 6;       // Lattice cluster id; agent uses for
                               // local same-cluster collision check
  DiskPurpose purpose = 7;     // mirrored from StorageDisk.purpose
}

// Reported back from agent to controller after a successful allocate, so
// the controller can render accurate per-disk status to operators (raw-disk
// handed the guest 3725 GiB even though the request was for 175).
message DiskAssignment {
  string vm_id = 1;
  uint32 disk_index = 2;
  string host_id = 3;
  string pool = 4;
  string device_id = 5;
  uint64 actual_size_gib = 6;
  string device_path = 7;   // e.g. /dev/disk/by-id/... or /dev/nvme0n3
}
```

The rename `PlacementSpec → LabelSelector` is the cleanup half: today's name is misleading (it's not specific to placement; it's a label selector). The selector is now used in two places (host, pool), the type and matching code are shared, and the controller has one set of golden tests.

## Scheduler

The scheduler grows two responsibilities and one shared helper.

**Shared helper: `select_by_labels(spec: &LabelSelector, labels: &Labels) -> Score`.**
Today's host-placement matching becomes one caller; pool-selector matching becomes the other. One algorithm, one fixture, one set of edge-case tests (empty `requires`, empty `prefers`, partial match, etc.). The duplicate matching logic that would exist if pools had a parallel implementation is not allowed to be born.

**Per-disk `(pool, device)` selection.** For each candidate host, for each disk in the request:

1. Filter the host's pools by `requires` (hard match on labels).
2. Within each surviving pool, enumerate **placement-eligible** devices: physically `Ready` AND scheduling state `Enabled` (see "Health" below). Degraded, Missing, or Draining devices are excluded; the rest of the pool is unaffected.
3. **Purpose-aware anti-affinity.** For `REPLICATED` disks, reject devices that already hold a same-cluster OSD assignment (failure-domain anti-affinity). For `GENERIC_DATA` disks, skip this check entirely.
4. Reject devices without enough free capacity for `min_size_gib`.
5. Score the remaining `(pool, device)` candidates: primary score is the pool's `prefers` match. Tie-break is **backend-aware**:
   - `lvm-linear`: best-fit — choose the device whose remaining capacity *after* allocation is smallest. Reduces fragmentation within the pool.
   - `raw-disk`: irrelevant once size fits; pick the lowest-`device_id` deterministically. Capacity is binary, all-or-nothing.
   - `nvme-namespace`: spread — choose the controller with the *fewest* live namespaces in this pool, to balance namespace count and queue depth across controllers. (When all controllers are equal, lowest-`controller_id`.)

If any disk in the request fails to place on the candidate host, that host is rejected and the scheduler tries the next. If no host fits, the request returns `FailedPrecondition` with a structured reason ("no pool on any host satisfies `tier=low-latency, isolation=exclusive` with 175 GiB free on a healthy device without a `cluster=foo` cluster-mate"). Same error shape host placement already uses.

**Hierarchical same-cluster anti-affinity** (the central correctness rule for Rook on basis):

For OSD disks (`purpose = REPLICATED`), candidate hosts are evaluated in this order. Steps 1–4 are evaluated as a layered comparison: a host that wins at an earlier step beats any host that doesn't, regardless of how strongly the loser would have scored at later steps. **Host-spread is not a `prefers` tie-breaker; it is a higher-priority dimension than `prefers`.**

1. **Filter by hard requirements.** Reject any host with no pool whose labels satisfy the disk's `requires`.
2. **Reject same-device collision** (hard, never violated). No `(cluster_id, host_id, device_id)` already holding a same-cluster OSD. The host-id component matters: by-id identifiers are stable per host but not guaranteed unique cross-host (different hosts can have same-model drives with the same serial in pathological cases), so the failure-domain key always includes host.
3. **Partition by host-spread rank.** Group remaining hosts by `same_cluster_osds_already_on_host`. Hosts with zero same-cluster OSDs strictly dominate hosts with one or more. Only when no zero-mate host has capacity do hosts with one mate become candidates, and so on.
4. **Within the winning rank, score by pool `prefers`.** The strongest preferred-tier match wins.
5. **Within tied pool scores, apply backend-specific device tie-break** (lvm-linear best-fit, raw-disk lowest-id, nvme-namespace fewest-namespaces).

The consequence: a strong `prefers: medium=nvme` cannot pack same-cluster OSDs onto an NVMe-heavy host while another acceptable host exists. Replication topology beats tier preference, every time. For Ceph, that is the correct trade.

For `GENERIC_DATA` disks, steps 2–3 are skipped entirely — they just respect labels and capacity. They do, however, observe one weak rule that applies to **every** disk regardless of purpose: a VM's own multiple disks prefer different devices on a host when free choice exists. This is not a hard constraint; it's expressed as a small `prefers`-style penalty for "device already holds another disk of this VM." It costs nothing and avoids silly all-eggs-one-basket placements (e.g., a VM with rootfs + log + scratch all landing on the same drive when other drives are equally good).

This handles the typical Lattice deployment (1 hypervisor, 6 Rook clusters, 24 NVMe drives) cleanly: cluster-A's OSDs occupy 3 distinct drives, cluster-B's OSDs share those drives freely, and within either cluster no two OSDs collide. Capacity reuse across clusters is a feature, not a bug.

**Reservation table:**

```sql
CREATE TABLE pool_disk_assignment (
  assignment_id    TEXT PRIMARY KEY,  -- idempotency key, sent to agent
  host_id          TEXT NOT NULL,
  pool             TEXT NOT NULL,
  device_id        TEXT NOT NULL,     -- by-id for block, controller-id for nvme
  vm_id            TEXT NOT NULL,
  disk_index       INTEGER NOT NULL,
  cluster_id       TEXT NOT NULL,
  purpose          TEXT NOT NULL,     -- replicated | generic-data
  min_size_gib     INTEGER NOT NULL,
  actual_size_gib  INTEGER,            -- NULL until agent acks
  state            TEXT NOT NULL,      -- pending | sent | committed | releasing
  UNIQUE (vm_id, disk_index)           -- one live assignment per disk slot
);
-- Failure-domain key is (host_id, device_id), not device_id alone. by-id
-- identifiers are stable per host but not guaranteed unique cross-host
-- (different hosts can have same-model drives with same serial in pathological
-- cases, and we don't want correctness to depend on hardware vendor hygiene).
CREATE INDEX idx_pool_assignment_failure_domain
  ON pool_disk_assignment (cluster_id, host_id, device_id)
  WHERE purpose = 'replicated';
```

Anti-affinity is computed against **live** assignments only — when a VM is deleted and its rows are removed, the device immediately becomes a candidate for any cluster again. There is no tombstone history. For Ceph, OSD-level data wipe is the operator's responsibility (or a future `wipe_before_allocate` knob); the scheduler doesn't try to remember that a device "used to belong to" cluster X. Live state is the truth.

Per-backend, the failure-domain identifier is the narrowest physical fault unit:

- `lvm-linear`: device by-id.
- `raw-disk` (M2): device by-id.
- `nvme-namespace` (M3): controller id (a controller fault loses every namespace under it; namespaces under one controller are not independent).

Hierarchy is **not exposed as API.** Operators with non-default topology desires (rack-level, chassis-level) write explicit selectors against ansible-applied labels. The scheduler's automatic spread runs at host then device by default because that's the Lattice/Rook deployment shape we are designing for.

**Reservation lifecycle is an outbox, not a single transaction.** "Insert reservation row and send command in one transaction" is impossible — you can't atomically commit a SQL transaction and a network send. We use the standard outbox pattern:

1. **Schedule.** In one DB transaction: insert `pool_disk_assignment` rows with `state=pending`, insert a `pending_command` row carrying the full `CreateVMCommand` payload, commit.
2. **Dispatch.** A controller-side dispatcher loop reads `pending_command` rows and writes them to the agent's gRPC stream. After write succeeds, mark `pending_command.state=sent`.
3. **Ack.** When the agent reports the VM as `Running` or `Failed`, the controller flips reservation rows to `committed` (with `actual_size_gib` populated from the agent's report) or deletes them (on failure).
4. **Recovery.** On controller restart, `pending` commands are re-dispatched; `sent` commands are reconciled against the agent's reported VM state. Rows for VMs the agent never heard about are re-sent. Rows for VMs the agent already created are flipped to `committed` from the report.

This is the same outbox shape the controller already uses for `ReconcileHostCommand` reissue on agent reconnect; we extend it to cover create. Reservation state is the controller's truth; agent local SQLite mirrors it for crash recovery only.

## Agent: backends and reservations

`Storage` becomes a registry of pools. A pool has labels and a backend; the backend owns its members (devices for `lvm-linear`/`raw-disk`, controllers for `nvme-namespace`) and exposes them as `PoolDevice`s.

```rust
struct Pool {
    name: String,
    labels: Labels,
    backend: Box<dyn DiskBackend>,
}

#[derive(Clone)]
struct PoolDevice {
    id: String,                // by-id for block, controller-id for nvme
    total_gib: u64,
    free_gib: u64,
    health: DeviceHealth,
}

enum DeviceHealth {
    Ready,
    Degraded { reason: String },   // discoverable but I/O looks suspect
    Missing { reason: String },    // configured but not found at scan time
}

struct DiskAllocationRequest {
    assignment_id: String,
    device_id: String,
    vm_id: String,
    cluster_id: String,
    disk_index: u32,
    min_size_gib: u64,
    purpose: DiskPurpose,
}

struct Allocation {
    path: PathBuf,
    actual_size_gib: u64,
}

#[async_trait]
trait DiskBackend: Send + Sync {
    /// Allocate on a specific commanded device. Returns the actual allocated
    /// size and the path to hand to cloud-hypervisor. The request carries
    /// every field the backend needs to enforce idempotency, write a complete
    /// reservation row, and check same-cluster collision — adding fields
    /// later (raw-disk, nvme-namespace) doesn't churn the trait signature.
    async fn allocate(&self, req: DiskAllocationRequest) -> Result<Allocation>;

    /// Release every disk currently bound to vm_id. Idempotent across
    /// Creating / Ready / Deleting reservation states.
    async fn release(&self, vm_id: &str) -> Result<()>;

    /// Report per-device capacity and health. Drives both telemetry and
    /// scheduler input.
    async fn devices(&self) -> Result<Vec<PoolDevice>>;

    /// Reconcile after agent restart: drop reservations whose VM no longer
    /// exists, surface reservations whose VM still exists, fail-loud on
    /// inconsistency the agent cannot resolve.
    async fn reconcile(&self, live_vm_ids: &HashSet<String>) -> Result<ReconcileReport>;
}

struct Storage {
    rootfs: RootfsBackend,     // CoW thin-snapshot path; unchanged in spirit
    pools: HashMap<String, Pool>,
    reservation_db: ReservationDb,
}
```

The agent **never re-runs selector matching** — that's the controller's job — but it does enforce a fixed set of guardrail invariants on every commanded allocate before touching hardware. These are defense-in-depth against controller bugs, stale state, or operator-driven config drift between scheduling and execution:

1. The named `pool` exists in the agent's current config.
2. The named `device_id` is one of that pool's members.
3. The pool's backend type matches what the agent currently runs (config didn't change under us).
4. `min_size_gib` fits the device's currently reported free capacity.
5. The device is `Ready` (not `Degraded` or `Missing`) and not in `Draining` scheduling state.
6. **Idempotency.** If a reservation row with the same `assignment_id` already exists in `Ready` state, return its allocation without re-running hardware ops. If a row with the same `(vm_id, disk_index)` exists with a *different* `assignment_id`, hard conflict (controller bug or stale retry).
7. **Same-cluster collision.** For `purpose=REPLICATED` commands, no live reservation owned by the same `cluster_id` exists on the same `device_id`. The agent-local key is `(cluster_id, device_id)`, not `(cluster_id, host_id, device_id)`: the reservation DB is host-local by definition (one agent per host), so `host_id` is implicit. The controller's table carries the full triple because it covers every host in the fleet; the agent's only needs the host-local pair. Defense-in-depth against scheduler bugs; agent-local reservation tables carry `cluster_id` and `purpose` precisely so this check is local and cheap.

Any failed invariant returns the command with a structured error to the controller. The agent never silently coerces or compensates.

The agent's reservation owner model:

```rust
struct ResourceOwner {
    assignment_id: String,
    vm_id: String,
    cluster_id: String,
    disk_index: u32,
    purpose: DiskPurpose,
}
```

This is what backends store with each reservation row. `cluster_id` and `purpose` are non-negotiable on the local owner record — without them invariant 7 cannot be enforced from local state alone, which means a controller bug becomes a Ceph correctness bug.

**Removed in the same change** (these are pre-release, no need to preserve):

- `LvmPermits::data` / `LvmPermits::rootfs` split — the rootfs backend keeps its own permit; data pools each get one. No global "data permit" semaphore.
- `Storage::create_data_disk_lv`, `data_disk_lv_path`, `parse_data_disk_lv_name`, `list_data_disk_lv_names_for`, `remove_vm_data_disks` — all collapse into `LvmLinearBackend::allocate` / `::release` / `::reconcile`. Naming, path computation, and lifecycle live with the backend that owns them.
- `vmdata-` prefix — replaced by `basis-data-<vm_id>-<disk_index>`. The PVE-coexistence rationale is gone with the host-level storage redesign.
- `DataSpec` / `RootfsSpec` config types — replaced by `RootfsConfig` and `Vec<PoolConfig>`. The `StorageSpec` wrapper is now justified (multiple pools).

### Backends

**`LvmLinearBackend`.** A pool over N devices, each with its own VG. State table:

```sql
CREATE TABLE lvm_reservation (
  assignment_id  TEXT PRIMARY KEY,
  pool           TEXT NOT NULL,
  device_id      TEXT NOT NULL,
  vg             TEXT NOT NULL,
  lv_name        TEXT NOT NULL,
  vm_id          TEXT NOT NULL,
  cluster_id     TEXT NOT NULL,
  purpose        TEXT NOT NULL,
  disk_index     INTEGER NOT NULL,
  size_gib       INTEGER NOT NULL,
  state          TEXT NOT NULL,    -- Creating | Ready | Deleting
  reserved_at    TEXT NOT NULL,
  UNIQUE (vm_id, disk_index),
  UNIQUE (vg, lv_name)
);
CREATE INDEX idx_lvm_reservation_cluster_device
  ON lvm_reservation (cluster_id, device_id)
  WHERE purpose = 'replicated';
```

`allocate(req: DiskAllocationRequest)`:

1. Insert reservation row with all fields from `req` plus `state=Creating`. (Hardware mutation cannot be made transactional with the SQLite write — same outbox-style pattern the controller uses, one layer down.)
2. Resolve the device's VG from host config (the scheduler already told us which `(pool, device_id)` to use).
3. `lvcreate --wipesignatures y --size <min_size_gib>G --name basis-data-<vm_id>-<disk_index> <vg>`.
4. Update row to `state=Ready`.
5. Return `/dev/<vg>/basis-data-<vm_id>-<disk_index>` and `actual_size_gib = min_size_gib`.

`release(vm_id)`: flip rows to `Deleting`, run `lvremove`, delete rows. Idempotent across crashes via state transitions.

`reconcile` after agent restart enumerates every basis-managed VG and joins against the reservation table:

- **Row `Creating`, LV exists with expected name on the row's expected VG**: forward to `Ready` (allocation succeeded right before crash, only the row update was lost).
- **Row `Creating`, no LV with the expected name anywhere**: delete the row (allocation crashed before `lvcreate`).
- **Row `Creating`, LV exists with the expected name on a different VG/device than the row claims**: hard inconsistency — surface as a per-disk error and refuse to start. This shouldn't happen under correct operation; if it does, an operator did something out-of-band and basis should not paper over it.
- **Row `Deleting`**: re-run `lvremove`, tolerate "already gone," delete the row.
- **Row `Ready`, expected LV missing on hardware**: flag `Lost`, surface as degraded. Operator wants to know.
- **Orphan LV** (basis-pattern name, no row): if the named `vm_id` is no longer in the live VM set, delete the LV. If the VM still exists, surface as inconsistency (do not adopt blindly — adopting a basis-pattern LV that the reservation table doesn't claim risks attaching the wrong storage to a live VM).

The startup-validation refusal of *foreign* LVs (anything not matching `basis-data-<vm_id>-<disk_index>`) is separate from this reconcile: foreign LVs are operator misuse and refuse the agent boot; basis-pattern orphans without a row are a recoverable state and reconcile handles them.

The wipe-signatures flag and the rationale comment carry over verbatim — that lesson is not re-learnable. The "one VG per device" rule is what makes device-level failure-domain free: an LV cannot span PVs because there is only one PV per VG. The scheduler picks the device, the backend picks the corresponding VG, and `lvcreate` has no way to put the LV on the wrong drive.

**Startup validation** runs on every agent boot before serving traffic. The backend refuses to start (and the agent reports `host_unhealthy` to the controller) if any of these invariants fail — they catch ansible drift that would silently destroy the failure-domain model:

- Every configured device is present at its `/dev/disk/by-id/...` path.
- Every configured VG exists and has exactly one PV.
- That PV's underlying device matches the configured `device.id`.
- No configured VG has multiple PVs, no PV is shared between configured VGs.
- No `device.id` appears in two pools.
- Every LV in the configured VGs follows the `basis-data-<vm_id>-<disk_index>` pattern (foreign LVs in a basis-managed VG are operator misuse and refused; basis-managed VGs are basis's exclusively).

**`RawDiskBackend` (M2).** A pool over N whole devices. State table:

```sql
CREATE TABLE raw_reservation (
  device_id      TEXT PRIMARY KEY,
  assignment_id  TEXT NOT NULL UNIQUE,
  pool           TEXT NOT NULL,
  vm_id          TEXT NOT NULL,
  cluster_id     TEXT NOT NULL,
  purpose        TEXT NOT NULL,
  disk_index     INTEGER NOT NULL,
  reserved_at    TEXT NOT NULL
);
```

`allocate(req)` checks the table under a transaction, inserts a row carrying every `req` field if the device is free, returns `/dev/disk/by-id/<device_id>` and `actual_size_gib = device.total_gib` (the entire device is handed to the guest regardless of `req.min_size_gib`). `release(vm_id)` deletes all rows for the VM. Each device is a binary occupancy slot from the scheduler's perspective — capacity is "device size if free, 0 if held."

⚠ **Data-retention security posture.** `release` does NOT wipe the device. A device released from VM A and reassigned to VM B will present VM B with VM A's old contents until B writes over them. Raw-disk reuse is only safe where the operator accepts data remanence between VM assignments, or where guests/operators perform their own wipe before reuse.

The future knob will be **`wipe_before_allocate`**, not `wipe_on_release`:

- `wipe_before_allocate: blkdiscard | nvme-format | none`
- Wipe runs at the old-owner-to-new-owner transition, which is the only transition that matters for tenant isolation.
- Release-time wipe makes delete latency hardware-dependent (a `blkdiscard` on a 4 TB drive is not free); allocate-time wipe makes create latency the variable, which is easier to reason about and easier to surface to operators ("provisioning device, please wait").

Not in v1. Posture is documented here, surfaced by `basisctl pool show`, and logged at allocate time so the trade-off is impossible to overlook.

`reconcile` on agent startup: walk the reservation table; any row whose `vm_id` is not in the live VM set is dropped. No live VM should reference a device not in the table — if it does, it's a data-loss hard error and the agent reports `VM_FAILED` for that VM (the operator shipped a manual change behind basis's back; the right response is to surface it, not paper over it).

**`NvmeNamespaceBackend` (M3).** A pool over N controllers; each controller serves up to its own `max_namespaces`. The "device" for failure-domain purposes is the controller — a controller fault loses every namespace under it. State table:

```sql
CREATE TABLE nvme_reservation (
  -- Composite identity. (vm_id, disk_index) is the owner key for release;
  -- (controller_id, namespace_id) is the hardware key, NULL until Created.
  assignment_id   TEXT NOT NULL UNIQUE,
  controller_id   TEXT NOT NULL,
  pool            TEXT NOT NULL,
  vm_id           TEXT NOT NULL,
  cluster_id      TEXT NOT NULL,
  purpose         TEXT NOT NULL,
  disk_index      INTEGER NOT NULL,
  state           TEXT NOT NULL,    -- Creating | Ready | Deleting | Lost
  namespace_id    INTEGER,           -- NULL while Creating
  nguid           TEXT NOT NULL,     -- deterministic from host_id+vm_id+disk_index
  lba_count       INTEGER NOT NULL,
  actual_size_gib INTEGER,           -- NULL while Creating
  reserved_at     TEXT NOT NULL,
  PRIMARY KEY (vm_id, disk_index),
  UNIQUE (controller_id, namespace_id)
);
```

**Reservation lifecycle is stateful.** Hardware mutation cannot be made transactional with the SQLite write, so the reservation row exists *before* hardware changes and is updated as the hardware progresses. The state machine:

```
   ┌──────────┐ insert pending row, hardware not yet touched
   │ Creating │
   └────┬─────┘
        │  nvme create-ns + attach-ns succeed
        ▼
   ┌──────────┐ namespace exists on hardware, row carries nsid + path
   │  Ready   │
   └────┬─────┘
        │  release() called
        ▼
   ┌──────────┐
   │ Deleting │ row preserved while detach + delete-ns run
   └────┬─────┘
        │  hardware ops succeed
        ▼
     (row deleted)
```

`allocate(req)`:

1. Insert row with all `req` fields, `state=Creating`, `lba_count` precomputed (rounded up to the controller's reported granularity from `nvme id-ctrl`), `nsid=NULL`.
2. `nvme create-ns <path> --nsze=<lba_count> --ncap=<lba_count>`.
3. `nvme attach-ns <path> --namespace-id=<nsid> --controllers=<cntid>`.
4. Update row: `state=Ready`, `nsid=<nsid>`, `actual_size_gib=<computed>`.
5. Return `/dev/<controller>n<nsid>` and `actual_size_gib`.

`release(vm_id)`:

1. For every row owned by `vm_id`, flip `state=Deleting`.
2. `nvme detach-ns` then `nvme delete-ns` for each.
3. Delete rows.

`reconcile` after agent restart:

- `Creating` rows: hardware ops were in flight when we crashed. Compute the row's expected NGUID; scan controller namespaces. If a namespace with that exact NGUID exists, forward the row to `Ready` (matched). If no NGUID match exists, delete the row (no namespace was created, or it was created with a different identity which we will not adopt).
- `Deleting` rows: drive forward — re-run detach + delete-ns; tolerate "already gone."
- `Ready` rows whose namespace's NGUID is missing on hardware: flag the row `Lost`, surface to controller as degraded. Do NOT silently delete — operator wants to know the controller lost state.
- Namespaces present on hardware whose NGUID matches our deterministic pattern but no row claims them: orphans, delete them. **Adoption requires exact NGUID match — never adopt by size, lba, or path inference.** Storage recovery is conservative; ambiguous cases surface to operators.

Naming nudges this along: namespaces are tagged via the `nguid` field with a deterministic value derived from `host_id || vm_id || disk_index`, hashed to 16 bytes. The host-id prefix prevents cross-host NGUID collisions if drives ever migrate physically between hosts.

**Vendor capability gate at startup.** `nvme id-ctrl <path>` reports `oacs` (optional admin command support). The agent verifies namespace-management is supported; if not, the pool is marked unhealthy and refuses placements. The controller surfaces this as a pool-level health condition. This avoids surprising operators who configure namespace pools on a controller that doesn't actually support the feature (some Samsung consumer drives advertise a namespace count >1 but reject `create-ns`).

### One reservation abstraction

Raw and namespace backends both want a "I hold these resources for these VMs" ledger with the same shape: insert under conflict-checking transaction, delete on release, reconcile on startup. Extract:

```rust
struct ReservationDb { conn: Connection }

impl ReservationDb {
    async fn reserve<K: ResourceKey>(&self, key: K, owner: ResourceOwner) -> Result<()>;
    async fn release_owner(&self, owner: &ResourceOwner) -> Result<Vec<K>>;
    async fn reconcile(&self, live_owners: &HashSet<String>) -> Result<Vec<Orphan>>;
}
```

Backed by SQLite tables (one per resource kind, defined inside the backend). The shared trait gives one set of crash-recovery tests covering "reservation rolled forward on agent restart" / "orphan cleaned" / "missing-on-hardware flagged" — once, not twice.

## Refactor of existing code (in this PR, no follow-ups)

Pre-release means cleanup runs in the same change. Concretely:

1. **Delete `extra_disk_gibs` everywhere.** Proto, `VmRow`, `LocalVmRow`, `MachineSpec`, `to_request`, scheduler request shape, controller DB schema, agent DB schema, every test that constructs one. Replaced by `storage_disks: Vec<StorageDisk>` end-to-end.
2. **Rename `PlacementSpec` to `LabelSelector` and lift it.** It's reused for storage now; the placement-only name was always misleading. One type, two usages (host, pool), one matcher, one validator. The CAPI provider's `placement_spec_to_proto` becomes `label_selector_to_proto` and gains exactly zero special cases.
3. **Collapse `Storage`'s rootfs/data API into one Pool registry.** `create_vm_lv` stays as the rootfs entry point. Everything data-related routes through `pool.allocate`. `data_disk_lv_path`, `data_disk_lv_name`, `parse_data_disk_lv_name`, `list_data_disk_lv_names_for`, `remove_vm_data_disks` all delete; their content lives inside `LvmLinearBackend`.
4. **Drop `LvmPermits`.** The two-permit split was a workaround for two-pools-per-host; with N pools we want per-pool concurrency. Each `Pool` owns its `Semaphore`.
5. **Promote `device_id` to a first-class field on `LocalVmRow.storage_disks`** so the agent can recover reservations from the local DB without re-running scheduler logic.
6. **Schema migration.** Pre-release, so the agent schema rev bumps to v2; the controller schema bumps similarly. Migration is **fail-fast destructive**: on first start with a v1 DB the agent refuses to migrate if it observes any live VMs (rows with state=Running) and exits non-zero with a clear message instructing the operator to drain the host. With no live VMs, it drops v1 tables and recreates v2. This rules out the "destroy local DB while VMs are running and orphan their disks" failure mode. Operators get a clean upgrade path: drain → upgrade → redeploy workloads, same posture basis takes for any other destructive change.
7. **`storage_capacity_to_proto` / `spawn_storage_capacity_loop`** become per-pool. The proto already needs to grow `repeated PoolCapacity pools` instead of a flat number; the loop iterates pools, the controller stores per-pool. Telemetry follows the model.
8. **Golden integration tests.** See M1 milestone below for the full Rook-shaped scenarios. Out-of-scope test setups that referenced raw-disk or nvme-namespace mocked backends are not part of v1.

What we are NOT refactoring in this PR:

- The agent ↔ controller stream protocol shape (`AgentMessage` / `ControllerCommand`). It's already clean.
- Image management. Images are rootfs's problem, not data's.
- The scheduler's overcommit / placement-mutex / cluster-mate logic at the host level. That's good; we model after it, we don't touch it.

## Health, telemetry, observability

Health is **two-level**: per pool, and per device within the pool. A pool with one dead drive out of 24 is not an unhealthy pool — it's a healthy pool with one degraded device, and the scheduler simply stops placing on that device while the other 23 continue serving.

Per-device state has two orthogonal axes: physical health and scheduling state.

**Physical health** (agent-reported from hardware scan):

- `Ready` — present, I/O working, full capacity.
- `Degraded { reason }` — present but suspect (SMART threshold tripped, persistent I/O errors, controller capability gate failed). Live VMs continue running; operator decides next step.
- `Missing { reason }` — configured but not present at scan time (drive pulled, by-id symlink gone, controller unenumerated). Live reservations against a missing device are surfaced as a host-level alert — the operator pulled a disk that had data on it, which is data-loss territory and must not be silent.

**Scheduling state** (controller-side, operator-controllable):

- `Enabled` — placeable.
- `Draining` — operator marked this device for evacuation via `basisctl pool drain`. Scheduler excludes from new placement; existing VMs continue running. Telemetry tracks drain progress (count of live reservations remaining). When zero, operators can replace or remove the device.

A device is a placement candidate iff `physical = Ready` AND `scheduling = Enabled`. Both axes are independently visible in `basisctl pool show` so the reason a device is unschedulable is never ambiguous.

Storage of these axes:

- **Physical health** is agent-reported. It lives in the agent's reported pool state on every `StorageCapacity` update; the controller caches the last-reported value but treats it as derived data. Agent restart re-reports from hardware scan.
- **Scheduling state** is controller-owned. It survives agent restarts and reflects operator intent. New table:

```sql
CREATE TABLE device_scheduling_state (
  host_id     TEXT NOT NULL,
  pool        TEXT NOT NULL,
  device_id   TEXT NOT NULL,
  state       TEXT NOT NULL,       -- enabled | draining
  reason      TEXT,                  -- operator-supplied at drain time
  updated_at  TEXT NOT NULL,
  PRIMARY KEY (host_id, pool, device_id)
);
```

The scheduler joins this against agent-reported physical health when computing placement candidates. A row absent from the table is treated as `enabled` (default); `basisctl pool drain` inserts/upserts a `draining` row; `basisctl pool enable` deletes it. Drain progress is computed by counting `pool_disk_assignment` rows for the `(host_id, device_id)` — when it hits zero the device is fully drained.

Pool-level state is computed from devices, with **mutually exclusive** values:

- `Ready` — every configured device is physically `Ready`.
- `Degraded { healthy: N, total: M }` — at least one `Ready` device AND at least one `Degraded`/`Missing` device.
- `Unhealthy { reason }` — zero `Ready` devices, or backend startup-validation failed.

Scheduling state is per-device, not per-pool. Draining a single device of a 24-device pool does not put the pool into a different state — it just reduces the pool's effective placement options by one.

The controller surfaces, per pool **and per device**:

- **Pool**: backend type, labels, `state`, capacity tiers (see below), per-cluster OSD occupancy (auditable failure-domain spreading).
- **Device**: id, configured size, free, physical `state` with reason, scheduling state, list of `(vm_id, disk_index)` reservations.

**Capacity has three layers.** Operators conflate them at their peril, so the model carries all three:

- `configured_total_gib` — sum of all configured device sizes. Static; what the operator wrote in `host.yaml`.
- `ready_total_gib` — sum of physically `Ready` devices. Reflects hardware health, ignores drain.
- `schedulable_total_gib` — sum of devices that are `Ready` AND scheduling state `Enabled`. The actual placement budget.
- `schedulable_free_gib` — free capacity on the schedulable subset. What the scheduler actually compares against `min_size_gib`.

A 24-device pool with one missing drive and one drained drive shows `configured=24*size`, `ready=23*size`, `schedulable=22*size`. `basisctl pool show` displays all four; the scheduler reads only `schedulable_free_gib`.

`basisctl`:

- `basisctl pool list [--host=<id>]` — pools per host, summary.
- `basisctl pool show <host>/<pool>` — full state including per-device breakdown and reservations.
- `basisctl pool health` — global view; degraded/unhealthy pools and devices, reasons.
- `basisctl pool drain <host>/<pool>/<device> [--reason=<text>]` — operator-initiated drain marker; scheduler stops placing on the device, existing VMs unaffected, telemetry tracks drain progress (count of remaining live reservations on the device).
- `basisctl pool enable <host>/<pool>/<device>` — clears a drain marker; placement resumes when the device is also physically `Ready`. Inverse of `pool drain`.

Metrics:

- `basis_pool_capacity_bytes{host,pool,layer="configured|ready|schedulable_total|schedulable_free"}` — the four capacity layers, per pool.
- `basis_device_capacity_bytes{host,pool,device,kind="total|free"}` — per-device byte counts.
- `basis_pool_disk_count{host,pool,device,cluster,purpose}` — reservation counts; the `cluster` and `purpose` labels make per-tenant and OSD-vs-generic dashboards trivial.
- `basis_device_health_state{host,pool,device,physical="Ready|Degraded|Missing",scheduling="Enabled|Draining"}` — both axes, separately labeled.
- `basis_pool_health_state{host,pool,state="Ready|Degraded|Unhealthy"}` — pool rollup.

Per-device labels make Grafana drill-downs work without a separate metric family. The `purpose` label on disk_count is what makes "show me OSD distribution per cluster" a single query.

## Failure handling

- **VM create fails partway through disk allocation.** On failure the controller issues `release(vm_id)` to **every pool referenced by the attempted command**, not only those whose disks reached `Ready`. Each backend's `release` is idempotent across `Creating`, `Ready`, and `Deleting` reservation states — it simply removes whatever rows match `vm_id` and reverses any hardware mutation already applied. Controller-side `pool_disk_assignment` rows for the VM are then deleted (or marked `failed` for audit, depending on operator preference). This is safer than tracking which pools "committed" because controller-side `committed` and agent-side `Ready` are not the same state and may diverge during the failure.
- **Host loses a device** (drive pulled, controller fault). Agent detects on next storage scan, marks the *device* physically `Missing` and the pool `Degraded` (the rest of the pool's devices keep serving). Live VMs whose reservations referenced the missing device continue running — cloud-hypervisor surfaces the I/O error to the guest, which is the correct behavior (Ceph's bluestore knows what to do; etcd has its own fsync error handling). The scheduler refuses new placements **on that device**; the pool's other devices remain candidates. The controller raises a per-device alert with the affected `(vm_id, disk_index)` list so operators see exactly which workloads need attention. When the device returns, the agent flips it back to `Ready` and placement resumes.
- **Agent crash mid-allocate.** Backend `reconcile` on startup sees an LV / device / namespace whose VM no longer exists (or whose reservation row is missing) and cleans up. Determinism comes from naming everything after `vm_id + disk_index` and tracking by `assignment_id`.
- **Controller crash mid-schedule (outbox semantics).** Reservation rows and a `pending_command` row are inserted in **one DB transaction**, committed before any agent network send. The dispatcher loop reads pending commands and sends them; agent acks flip rows to `committed`. On controller restart: `pending` rows re-dispatch, `sent` rows reconcile against agent-reported VM state, and any agent-reported VM the controller doesn't know about gets reconciled or torn down per existing controller logic. There is no "DB write and network send in one atomic step" — that would be a lie and is explicitly rejected here.
- **Duplicate command delivery (outbox retry).** The dispatcher may send the same `assignment_id` more than once after restart or transient failure. The agent's idempotency rule (invariant 6) handles this: same `assignment_id` already `Ready` returns the existing allocation; same `(vm_id, disk_index)` with a different `assignment_id` is a hard conflict.
- **Same-cluster collision detected at agent layer** (controller scheduled two disks for the same cluster onto the same device because of a controller bug or stale data). Agent rejects the second `allocate` with a hard error; the controller retries with the bad reservation marked dead. This is a defense-in-depth check, not an expected path.
- **Pool removed from `host.yaml`.** Agent refuses to start if the removed pool has live reservations; operator drains first via `basisctl pool drain`. Same fail-fast posture basis takes for cluster removal with live VMs.

## Milestones

Pre-release, so each milestone is a normal deployable change with no compat phase. Order is by risk + dependency.

**M1 — Core model + LVM backend (~1.5 weeks)**

The product unlock for Lattice/Rook. Ships:

- Multi-pool host config (`storage.pools[]`), one VG per device, ansible-managed VGs.
- `LabelSelector` rename + unification (host placement and pool placement share the type and matcher).
- `StorageDisk` on BasisMachine with `min_size_gib`, `selector`, `purpose` (replicated | generic-data).
- Outbox-based controller→agent command durability with `assignment_id` idempotency keys.
- Hierarchical same-cluster anti-affinity (host first, device second, both for `purpose=REPLICATED` only).
- `LvmLinearBackend` with startup validation, agent-local reservation table carrying `(assignment_id, vm_id, cluster_id, purpose)`.
- Per-device + per-pool health, `basisctl pool list/show/health/drain`, metrics with per-device labels.
- Refactor of existing storage code per the list above (delete `extra_disk_gibs`, `LvmPermits`, the dual `*_lv_path` helpers, the PVE-coexistence `vmdata-` prefix).

Golden integration test: stand up a controller + agent with multiple pools, create a Rook-shaped 3-OSD MachineDeployment for cluster A and a 3-OSD MachineDeployment for cluster B, assert:

- Each cluster's OSDs land on three distinct hosts (host-level spread for OSDs of one cluster).
- When forced onto fewer hosts (constrained host count), each cluster's OSDs occupy distinct devices on the shared host (device-level spread).
- Cluster A and cluster B freely share devices (cross-cluster co-location is allowed).
- Killing the agent mid-create and restarting recovers reservations idempotently via `assignment_id`.
- Deleting a Rook OSD frees the device; recreating that OSD may reuse any eligible device subject to **current live** cluster-mate anti-affinity (no tombstone history).

**M2 — `raw-disk` backend (~3 days, customer-driven)**

Adds the `raw-disk` backend, `raw_reservation` table with `cluster_id`/`purpose`/`assignment_id`, binary device-level capacity, device-passthrough cloud-hypervisor wiring. Scheduler is unchanged — it already reasons in `(pool, device)` tuples. Built when a customer's workload proves the dm-linear path is the bottleneck or when bare-metal-style OSD ownership is required. Tests: agent crash holding a reservation; concurrent placement of two `isolation=exclusive` requests against a 1-device pool (one wins, one rejects).

**M3 — `nvme-namespace` backend (~1.5 weeks, hardware-gated, customer-driven)**

Adds the backend, the `nvme_reservation` table with the state machine, the `nvme-cli` shellouts, the capability gate, vendor-quirk handling. Hardware loaner required for at least two vendors before declaring done (Intel + Samsung minimum; Kioxia ideal). Built when the dm-linear and raw-disk paths together are insufficient — the bar is real evidence of an IOPS or p99 ceiling, not anticipation. Tests: NGUID-based orphan cleanup; controller-reset recovery; max-namespaces ceiling enforcement.

Each milestone ships independently. M1 alone is the v1 product. M2 and M3 are scope expansions that the architecture admits without re-design.

## Decisions on previously-open questions

1. **Failure-domain anti-affinity scope.** Default is the **narrowest known physical failure domain for the backend**: device-id for `lvm-linear` and `raw-disk`, controller-id for `nvme-namespace`. No API knob in v1. Operators with rack/chassis topology that doesn't map to basis-hosts can write explicit `requires` selectors against ansible-applied labels (`rack=foo`, `chassis=bar`), but the scheduler's automatic spread runs at device granularity regardless.
2. **Raw-disk capacity reporting.** Binary per-device. Pool rollup is `devices_total / devices_free / bytes_total / bytes_exclusively_free` (the last being the sum of *free* devices' sizes — never the held ones' "remaining" bytes). The controller never reports partial byte usage for held raw devices, because the remainder is unallocatable.
3. **Device wipe between owners.** Not in v1. Posture is documented as a security caveat (above) and surfaced by `basisctl pool show`. The future knob is `wipe_before_allocate: blkdiscard | nvme-format | none` per-pool — wipe runs at the old-owner-to-new-owner transition (the only one that matters for tenant isolation), not at release time. Added when a customer asks.
4. **NVMe namespace size granularity.** Round up to the controller's reported allocation granularity from `nvme id-ctrl`; report the actual allocated size in `DiskAssignment.actual_size_gib`. The rounding rule is locked after testing with two vendors (Intel + Samsung minimum, Kioxia ideal).
5. **Per-backend concurrency.** Hardcoded backend defaults; not operator-facing in v1.
   - `lvm-linear`: 2 permits per backend instance (lvcreate is fast but parallel can deadlock on busy LVM metadata).
   - `raw-disk`: effectively unbounded — allocation is a SQL row write.
   - `nvme-namespace`: 1 permit per *controller* (some controllers serialize namespace ops in firmware).
   We add a config knob the day a customer's hardware proves a default wrong.

## Open questions (M1 — to answer before coding)

1. **Outbox retry policy.** Pending commands re-dispatch on controller restart; how aggressively do we retry on transient agent failures versus surfacing as a hard `FailedPrecondition` to the client? Lean toward exponential backoff with a per-command timeout that flips to terminal-failed and lets CAPI reconcile. The exact backoff curve and timeout values benefit from a quick empirical pass against the existing agent stream behavior before being locked in.

## Open questions (M3 — answered when M3 lands)

These don't matter until the NVMe namespace backend is on the table; listing them here so the design notes don't get lost:

1. **NGUID generation scheme.** Need a 16-byte deterministic value derived from `(host_id, vm_id, disk_index)` that won't collide across hosts. SHA-256 truncated to 16 bytes is the obvious choice but worth validating against vendor parsers (some quirky controllers reject NGUIDs with certain byte patterns).
2. **`Lost` namespace recovery UX.** When a controller resets and we discover a `Ready` reservation row whose namespace is gone from hardware, what does the operator workflow look like? Probably: surface as a per-disk alert, basisctl exposes a `--force-recreate` for reprovisioning the namespace, the affected VM goes through cloud-hypervisor's I/O error path during the gap. Worth designing the flow before the first time it happens in production.
