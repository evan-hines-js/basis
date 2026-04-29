# Lattice Rook/Ceph Integration — Implementation Guide

Target: another agent implementing automated Rook install + management in
the **Lattice** repo (`/Users/evanhines/lattice/work/dir/lattos/lattice`),
consuming the **Basis** extra-disks capability that shipped in
commit `d7c060d` of the `basis` repo
(`/Users/evanhines/lattice/work/dir/lattos/basis`).

This doc is self-contained — it assumes you haven't seen the Basis-side
work.

## Background

**Basis** is Lattice's minimal bare-metal VM scheduler. It runs a
controller + per-host agents, provisions VMs as systemd-managed
cloud-hypervisor processes on an LVM thin pool, and exposes a gRPC API
plus a CAPI provider (`basis-capi-provider`). From Lattice's point of
view, it is the infrastructure provider selected when
`LatticeCluster.spec.provider.config.basis` is set.

**Lattice** is the opinionated Hyperconverged Cluster Application. One
`LatticeCluster` CRD describes a full stack — compute on Basis, storage,
networking, cert management, monitoring, etc. — and the operator
installs every one of those layers as an always-on component. There are
no `LatticeRook` / `LatticeCilium` / `LatticeCertManager` user-facing
CRDs for the user to enable or disable things; the stack is fixed.

**Goal of this work**: make Rook/Ceph one of those always-on
components. If a `LatticeCluster` worker pool declares `dataDiskGibs`,
those disks become Ceph OSDs automatically; PVC provisioning across the
cluster "just works" against a default RBD StorageClass with no further
configuration. If no pool declares data disks, Rook is not installed —
no storage layer, explicit-opt-in by the absence of data disks.

## What Basis shipped (commit `d7c060d`)

`BasisMachine.spec.extraDiskGibs: []u32` — zero or more raw,
unformatted virtio-blk devices attached to the guest alongside the
rootfs, each backed by an LVM thin volume on the host. Basis does not
touch the disks beyond creating them: no filesystem, no partition
table, no mount point. They're handed to the guest as blank devices so
Rook/Ceph can wipe and claim them.

### CRD surface

`BasisMachineTemplate`:

```yaml
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: BasisMachineTemplate
metadata:
  name: cluster-worker-storage
spec:
  template:
    spec:
      cpu: 8
      memoryMib: 16384
      diskGib: 80
      image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
      extraDiskGibs: [500]    # one 500 GiB raw data disk per replica
```

Omit `extraDiskGibs` (or set to `[]`) to get a pool with no data disks.

### Guest device enumeration

Cloud-hypervisor attaches disks in this order, stable across reboots:

| Guest device | Host backing                                  |
| ------------ | --------------------------------------------- |
| `/dev/vda`   | rootfs LV (`vm-<vm_id>`), partition table — mount `/dev/vda1` |
| `/dev/vdb`   | cloud-init cidata ISO (read-only, ~1 MB)     |
| `/dev/vdc`   | `extraDiskGibs[0]` — `vmdata-<vm_id>-0` LV   |
| `/dev/vdd`   | `extraDiskGibs[1]` — `vmdata-<vm_id>-1` LV   |
| …            | …                                             |

Order is preserved across VM restarts: the agent persists extra-disk
sizes in its local DB and reattaches at the same index. Rook addresses
disks by `by-id` / WWN internally, so this is belt-and-suspenders — but
code that hard-codes `/dev/vdc` is safe.

### Two-pool host layout

Basis hosts run two LVM volume groups, each fit-to-purpose:

| VG | Backend | Purpose |
| --- | --- | --- |
| `basis` (default name) | thin pool `basis/pool` | VM rootfs LVs (`vm-<vm_id>`), CoW snapshots of golden images |
| `basis-data` (default name) | plain VG, linear LVs | every requested data disk (`vmdata-<vm_id>-<N>`) |

The split is the right shape for OSDs. `bluestore` on `dm-thin`
double-books allocation; `bluestore` on `dm-linear` is a one-table
pass-through. Pool exhaustion in `basis-data` is bounded — no shared
blast radius with rootfs.

Pool names are operator-configurable in `host.yaml`:

```yaml
storage:
  rootfs:
    vg: basis
    thinPool: pool
  data:
    vg: basis-data
```

The basis-prereqs ansible role provisions both VGs at host install on
distinct partitions (or NVMes). Recommended split for a single-disk
host: rootfs pool ≈ `30 GiB × max-VMs-per-host + 30%` headroom; data
VG = the rest.

### Per-data-disk tuning (already set by Basis)

Each extra disk is attached with:

```
path=/dev/basis-data/vmdata-<vm_id>-<N>,image_type=raw,direct=on,discard=unmap,num_queues=<vcpus>,queue_size=256
```

- `direct=on` — O_DIRECT on the host; ceph bluestore fsync durability isn't defeated by the host page cache.
- `discard=unmap` — guest `blkdiscard` / `fstrim` reaches the underlying NVMe through dm-linear (no metadata indirection). Ceph OSD compaction reclaims physical blocks immediately.
- `num_queues=<vcpus>` — virtio-blk parallelism scales with guest vCPU count.

Rootfs LVs use the same `direct=on` + `image_type=raw` plus
`num_queues`/`queue_size`, but rely on the thin pool's `sparse=on`
default for discard rather than `discard=unmap` (snapshots of golden
images don't need an explicit unmap directive).

### Scheduler behaviour

Basis tracks **two independent budgets per host**: rootfs pool free
bytes and data VG free bytes. `extraDiskGibs` charges only against the
data VG; `diskGib` (rootfs) charges only against the thin pool. A VM
with `diskGib: 80` + `extraDiskGibs: [500]` needs 80 GiB free in the
host's rootfs thin pool *and* 500 GiB free in the host's data VG.
Failure to fit either pool surfaces to CAPI as `ResourceExhausted` on
the `BasisMachine`, with `cpu=…, mem=…MiB, rootfs=…GiB, data=…GiB`
in the error message so operators can tell which pool ran out.

Capacity is **heartbeat-fresh**, not registration-static: every agent
heartbeat (default 30s) carries a `StorageCapacity` snapshot, and the
scheduler reads from `hosts.{rootfs,data}_total_bytes` /
`{rootfs,data}_free_bytes` updated on each tick.

### Lifecycle

- **Create**: agent creates the rootfs LV in the rootfs VG, then loops `create_data_disk_lv` per extra into the data VG.
- **Delete**: agent removes the rootfs LV from the rootfs VG and every `vmdata-<vm_id>-*` LV from the data VG.
- **Restart after host reboot**: agent reads `extra_disk_gibs` from local DB, re-resolves `vmdata-<vm_id>-N` paths in the data VG, reattaches. Missing LV → fails with `DiskMissing`; CAPI sees FAILED and remediates. Strict by design: a silent reattach with a wrong disk would corrupt ceph's OSD metadata.
- **Orphan sweep**: periodic; walks both VGs. Reclaims any `vm-<id>` (rootfs VG) or `vmdata-<id>-*` (data VG) LV whose `vm_id` is no longer in the agent DB.

### What Basis does NOT do

- Format, partition, mount, discover, claim, or manage the disks.
- Expose a StorageClass.
- Plan thin-pool / data-VG capacity (operators size the partitions).

All of that is Lattice's (or the operator's) job.

### Reference material in the Basis repo

- Proto: `basis/crates/basis-proto/proto/basis.proto` — `ExtraDisk`, `CreateMachineRequest.extra_disks`, `CreateVMCommand.extra_disks`, `Machine.extra_disks`, `StorageCapacity`, `PoolBytes`.
- CRD: `basis/crates/basis-capi-provider/src/crds.rs` — `BasisMachineSpec.extra_disk_gibs`.
- Agent storage: `basis/crates/basis-agent/src/lvm.rs` — `Storage` (per-host owner of both VGs), `Storage::create_data_disk_lv`, `Storage::remove_vm_data_disks`, `Storage::list_managed_vm_ids`, `LvmPermits` (per-VG mutation gate), `StorageCapacity`, `PoolBytes`.
- Agent host config: `basis/crates/basis-agent/src/config.rs` — `StorageSpec`, `RootfsSpec`, `DataSpec`.
- Agent VM: `basis/crates/basis-agent/src/vm.rs` — `BootArtifacts.extra_disks`, `DiskSpec` enum (Rootfs / Data / CloudInit, one Display path).
- Scheduler: `basis/crates/basis-controller/src/scheduler.rs` — `ScheduleRequest { rootfs_gib, data_gib }`, `Available { rootfs_gib, data_gib }`, dual-budget `fits()`.
- DB: `basis/crates/basis-controller/src/db.rs` — `HostRow.{rootfs,data}_{total,free}_bytes`, `HostUsage.{used_rootfs_gib, used_data_gib}`, `Db::record_heartbeat` (capacity update + heartbeat in one round-trip).

## Design: Rook is always-on, not a CRD

Lattice is opinionated. Storage is part of the stack. The user does not
choose Rook vs something else; they don't declare a `LatticeRook` CRD.
The only decision the user makes about storage is **"attach N data
disks of size X to each node in this worker pool"** via
`dataDiskGibs`. Everything else — operator install, CephCluster spec,
replication factor, StorageClass, RBD vs CephFS — is Lattice's call.

Implementation model: follow whatever Lattice already does for
always-on components (Cilium, cert-manager, ESO, Victoria Metrics).
Those live under `lattice/crates/lattice-<name>/` with an `install/`
submodule and are invoked from the main cluster reconciler during the
cluster's component-install phase. There is no user-facing CRD for
those components either; they just happen. Rook should sit in exactly
the same slot.

Read `lattice-cilium/src/install/` and `lattice-cert-manager/src/install/`
end-to-end before starting — they are the closest analogues and the
call sites in the cluster reconciler will tell you where `lattice-rook`
plugs in.

## Implementation plan

Four phases, small-to-large:

### Phase 1 — plumb `extraDiskGibs` through the CAPI generator

#### 1a. Extend `NodeResourceSpec` and `InstanceType`

File: `lattice/crates/lattice-crd/src/crd/types.rs`

Add `dataDiskGibs: Option<Vec<u32>>` to `NodeResourceSpec`:

```rust
pub struct NodeResourceSpec {
    pub cores: u32,
    pub memory_gib: u32,
    pub disk_gib: u32,                    // rootfs, unchanged
    #[serde(default = "default_sockets", skip_serializing_if = "is_default_sockets")]
    pub sockets: u32,
    /// Raw data disks (GiB each) attached alongside the rootfs, in
    /// allocation order. Currently only honoured by the Basis provider;
    /// other providers ignore the field. If non-empty on at least one
    /// worker pool, Lattice installs Rook and exposes the resulting
    /// ceph RBD pool as the cluster's default StorageClass. Omit (or
    /// set to empty) on every pool to get a cluster with no storage
    /// layer — explicit-opt-in by declaration, no implicit defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_disk_gibs: Option<Vec<u32>>,
}
```

Mirror the field on `InstanceType`, update `InstanceType::resources()`
and `as_resources()` to pass it through. Other providers (`aws`,
`openstack`, `proxmox`, `docker`) ignore the field for now — a later
pass can add provider-specific handling if required.

User-facing YAML:

```yaml
spec:
  nodes:
    workerPools:
      storage:
        replicas: 3
        instanceType:
          cores: 8
          memoryGib: 16
          diskGib: 80
          dataDiskGibs: [500]
```

#### 1b. Propagate into the Basis machine template

File: `lattice/crates/lattice-capi/src/provider/basis.rs`

Extend `MachineSizing` with `data_disk_gibs: Vec<u32>`, pull it from
`NodeResourceSpec`, and emit it in `generate_machine_template` when
non-empty (emit the field off entirely when empty, to keep manifests
byte-clean):

```rust
struct MachineSizing {
    cpu: u32,
    memory_mib: u32,
    disk_gib: u32,
    data_disk_gibs: Vec<u32>,
}

impl MachineSizing {
    fn from_instance_type(instance_type: &Option<InstanceType>, default_disk_gib: u32) -> Self {
        instance_type
            .as_ref()
            .and_then(|it| it.as_resources())
            .map(|r| Self {
                cpu: r.cores,
                memory_mib: r.memory_gib * 1024,
                disk_gib: r.disk_gib,
                data_disk_gibs: r.data_disk_gibs.unwrap_or_default(),
            })
            .unwrap_or(Self {
                cpu: 4,
                memory_mib: 8192,
                disk_gib: default_disk_gib,
                data_disk_gibs: Vec::new(),
            })
    }
}

fn generate_machine_template(&self, cluster_name: &str, sizing: MachineSizing, image: &str, suffix: &str) -> CAPIManifest {
    let mut spec = serde_json::json!({
        "cpu": sizing.cpu,
        "memoryMib": sizing.memory_mib,
        "diskGib": sizing.disk_gib,
        "image": image,
    });
    if !sizing.data_disk_gibs.is_empty() {
        spec["extraDiskGibs"] = serde_json::json!(sizing.data_disk_gibs);
    }
    CAPIManifest::new(
        BASIS_API_VERSION,
        "BasisMachineTemplate",
        format!("{}-{}", cluster_name, suffix),
        &self.namespace,
    )
    .with_spec(serde_json::json!({ "template": { "spec": spec } }))
}
```

Add a test alongside `machine_template_uses_derived_image_and_resources`
asserting:

- Pool with `dataDiskGibs: [500]` emits `extraDiskGibs: [500]` on the right `BasisMachineTemplate`.
- Pool without `dataDiskGibs` produces a template with **no** `extraDiskGibs` key (byte-compat with pre-Rook).

### Phase 2 — `lattice-rook` crate

Scaffold the component crate following the `lattice-cilium` /
`lattice-cert-manager` pattern:

```
lattice/crates/lattice-rook/
  Cargo.toml
  src/
    lib.rs
    install/
      mod.rs
      ... (match whatever pattern the reference crates use)
```

Register in `lattice/Cargo.toml` workspace members.

**No public CRD.** The crate exposes an `install` entry point that the
cluster reconciler calls during component bring-up. Match the shape of
`lattice_cilium::install::*` exactly.

### Phase 3 — Rook operator + CephCluster install

Two resources land on the workload cluster during install:

**3a. rook-ceph operator Helm chart**

From `https://charts.rook.io/release`. Use whatever Lattice already
does for upstream charts — read the Cilium / Istio installers for the
precedent. Pin the chart version; don't follow latest.

Values worth setting:

- `enableDiscoveryDaemon: true` — the daemon that enumerates available block devices per node.
- `monitoring.enabled: false` for the first cut. Follow-up PR wires it into `lattice-victoria-metrics`.

**3b. `CephCluster` CR**

```yaml
apiVersion: ceph.rook.io/v1
kind: CephCluster
metadata:
  name: rook-ceph
  namespace: rook-ceph
spec:
  cephVersion:
    image: quay.io/ceph/ceph:v18.2.4     # pin
  dataDirHostPath: /var/lib/rook
  mon:
    count: 3
    allowMultiplePerNode: false
  mgr:
    count: 2
  storage:
    useAllNodes: true
    useAllDevices: true
```

**Critical comment to leave in the installer code**: `useAllDevices:
true` is safe here specifically because Basis guarantees the extras are
unformatted blank devices. If Lattice gains a provider that hands Rook
a pre-formatted disk, this needs to become a `deviceFilter` or
`devicePathFilter`. For now, it's a Basis-specific assumption.

**3c. `CephBlockPool` + `StorageClass`**

After `CephCluster` reports `HEALTH_OK`, create a `CephBlockPool`
(`replicated.size: 3`) and a `StorageClass` pointing at it via the RBD
CSI driver (`rook-ceph.rbd.csi.ceph.com`). Set the storageclass's
`is-default-class: "true"` annotation and *first* strip the same
annotation off any existing default, so the cluster never has two
defaults.

Crib the manifest shapes from upstream rook examples; there's nothing
clever about them.

### Phase 4 — conditionally install Rook during cluster reconcile

In the cluster reconciler (wherever the always-on components are
invoked — likely `lattice-operator/src/controller_runner.rs` or
similar), detect whether any worker pool in the `LatticeCluster` has
non-empty `dataDiskGibs`. If yes, invoke `lattice_rook::install`. If
no, skip — the cluster simply has no storage layer. No default
StorageClass, no PVC provisioning. Explicit opt-in by the presence of
disks.

No webhook validation beyond what already exists for `LatticeCluster`:
if a user writes `dataDiskGibs: [500]` on only one worker pool with
`replicas: 1`, they will get a 1-OSD ceph cluster that can't meet
`replicated.size: 3` and their PVCs will stay pending. Rook's own
events will surface that. Don't try to second-guess in an admission
webhook — it's noise, and the operator's status will say the same
thing.

## End-to-end sequence (mental model)

1. User applies a `LatticeCluster` with `dataDiskGibs: [500]` on its storage worker pool.
2. CAPI generator emits `BasisMachineTemplate` with `extraDiskGibs: [500]`.
3. CAPI + `basis-capi-provider` drive `BasisMachine` → `Basis.CreateMachine` → Basis scheduler places on a host with ≥ (80 + 500) GiB free.
4. Agent creates rootfs LV + `vmdata-<vm_id>-0` LV, boots cloud-hypervisor; guest sees `/dev/vda` (rootfs), `/dev/vdb` (cidata), `/dev/vdc` (blank 500 GiB).
5. K8s cluster up; Lattice operator reconciles always-on components.
6. Because a worker pool declares `dataDiskGibs`, `lattice-rook` runs: installs the operator chart + `CephCluster` + `CephBlockPool` + RBD `StorageClass`.
7. Rook's discovery daemon finds `/dev/vdc` on each storage node, formats it, creates one OSD per node.
8. `CephCluster` reaches `HEALTH_OK`; PVCs across the cluster bind against ceph RBD with no further user action.

## Testing

### Unit

- `lattice-capi/src/provider/basis.rs`: pool with `dataDiskGibs: [500]` emits `extraDiskGibs: [500]`; absence omits the field. Byte-compat case is the important one — existing manifests must not churn.
- `lattice-rook/src/install/`: mock helm + kube clients, assert manifests applied.

### Real-cluster smoke (required before merge)

1. `LatticeCluster` with 3 workers, `dataDiskGibs: [50]` on the worker pool.
2. On each worker guest: `lsblk` shows `vdc` as a 50 GiB unformatted device.
3. On each host: `lvs basis/pool` shows `vm-<id>` + `vmdata-<id>-0` per worker.
4. `CephCluster.status` reaches `HEALTH_OK` within a few minutes.
5. `kubectl get sc` — `rook-ceph-block` is default.
6. Apply a 10 GiB PVC + a Deployment mounting it, write 1 GiB, delete the pod, re-create it, verify data persists.
7. Cordon and delete one storage node; CAPI replaces the `BasisMachine`. New node's `/dev/vdc` is blank (different `vm_id`), Rook rebalances, `HEALTH_WARN` → `HEALTH_OK`.
8. Destroy: `kubectl delete latticecluster …`. Every VM + extra LV gone on the host (`lvs basis/pool` shows only the pool itself + the golden image LVs).

Also test: `LatticeCluster` with zero `dataDiskGibs` anywhere. Expect no
Rook install, no CephCluster, no default StorageClass. Cluster works
fine without storage.

## Known Basis-side gotchas

- **Thin-pool overcommit**: Basis's scheduler caps placements by nominal pool capacity, not by current allocation. As Rook's OSDs grow (bluestore WAL + DB + data), they eat real pool extents over time. Monitor the agent's `pool_capacity` metric and `lv_permit_wait_seconds`. If a pool fills, new `lvextend` calls (i.e. new VM creates) start failing.
- **Reboot re-attach is strict**: missing `vmdata-<id>-N` after a host reboot fails the VM restart loudly. CAPI sees FAILED and re-provisions. Correct behaviour — silent reattach with a wrong disk would corrupt ceph OSD metadata — but the operator sees aggressive-looking remediation.
- **No in-place disk resize**: `extraDiskGibs` is immutable on a `BasisMachine`; changing it in the `BasisMachineTemplate` causes CAPI to re-provision. Don't expose a resize path in Lattice; ceph scales horizontally via more OSDs, not bigger ones.

## Non-goals for v1

Resist the temptation:

- CephFS / CephObjectStore. RBD only. Prove the block path first.
- Custom device filtering. `useAllDevices: true` is sufficient because Basis only exposes the devices ceph should claim.
- `cephConfigOverrides` or monitor placement tuning.
- Cross-cluster replication.
- Node-local fast-disk tiering (if ever wanted, it's a second entry in `dataDiskGibs` plus a device-class rule — tractable, not now).
- User-facing CRD for Rook config. The whole point of the opinionated HCC model is that there isn't one.

## Commit hygiene

- One PR for Phase 1 (CAPI plumbing, self-contained, lands without Rook existing).
- One PR for `lattice-rook` scaffolding.
- One PR for operator + CephCluster + StorageClass install.
- One PR for wiring into the cluster reconciler.

Small, reviewable chunks. No big-bang.

## Open questions to confirm before coding

- Which crate owns the always-on component invocation today? (`lattice-operator/src/controller_runner.rs` is a good guess; confirm by tracing how `lattice-cilium` is installed today.)
- Does Lattice have a central version-pinning mechanism (`versions.toml` at the repo root)? Mirror it for the rook chart + ceph container image.
- What's the current convention for upstream chart values — vendored, or Helm repo pull? Match it.

End of guide.
