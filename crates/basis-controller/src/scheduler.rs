use std::collections::{BTreeMap, HashMap, HashSet};

use basis_common::gpu::GpuInfo;
use basis_proto::{CreateMachineRequest, DiskPurpose, StorageDisk};

use crate::db::{HostPoolDeviceRow, HostPoolRow, HostRow, HostUsage, GIB};

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// No host has enough free CPU/memory/disk/GPUs for this request.
    #[error("no host can satisfy request: {0}")]
    NoCapacity(String),

    /// Capacity exists, but no candidate host satisfies the request's
    /// hard `placement.requires` filter. Distinct from `NoCapacity` so
    /// operators can tell "you need to add capacity" from "you need to
    /// label a host (or relax the requirement)".
    #[error("no host satisfies placement requirements: {0}")]
    UnsatisfiedRequirements(String),
}

/// Hard placement filter: `host.labels[key]` must be one of `values`.
/// Empty `values` is a parse bug — the scheduler treats such an entry
/// as un-satisfiable, which is the safe default if a CRD ever ships an
/// empty list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementRequirement {
    pub key: String,
    pub values: Vec<String>,
}

/// Soft placement preference: when the host has `key=value`, add
/// `weight` to the candidate's preference score. Multiple matches
/// across different keys sum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementPreference {
    pub key: String,
    pub value: String,
    pub weight: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placement {
    pub requires: Vec<PlacementRequirement>,
    pub prefers: Vec<PlacementPreference>,
}

impl Placement {
    /// True iff every requirement is satisfied by `host_labels`.
    fn satisfies(&self, host_labels: &std::collections::BTreeMap<String, String>) -> bool {
        self.requires.iter().all(|req| {
            host_labels
                .get(&req.key)
                .map(|v| req.values.iter().any(|allowed| allowed == v))
                .unwrap_or(false)
        })
    }

    /// Sum of preference weights matched by `host_labels`. Zero when
    /// no preferences are declared or none match.
    fn score(&self, host_labels: &std::collections::BTreeMap<String, String>) -> u32 {
        self.prefers
            .iter()
            .filter(|p| host_labels.get(&p.key).is_some_and(|v| v == &p.value))
            .map(|p| p.weight)
            .sum()
    }

    fn describe_requires(&self) -> String {
        self.requires
            .iter()
            .map(|r| format!("{}={:?}", r.key, r.values))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl From<basis_proto::LabelSelector> for Placement {
    fn from(spec: basis_proto::LabelSelector) -> Self {
        Self {
            requires: spec
                .requires
                .into_iter()
                .map(|r| PlacementRequirement {
                    key: r.key,
                    values: r.values,
                })
                .collect(),
            prefers: spec
                .prefers
                .into_iter()
                .map(|p| PlacementPreference {
                    key: p.key,
                    value: p.value,
                    weight: p.weight,
                })
                .collect(),
        }
    }
}

pub struct ScheduleRequest {
    /// Cluster this VM belongs to. Drives soft anti-affinity: hosts
    /// with fewer existing VMs from the same cluster outrank tighter
    /// fits. Empty string disables the penalty (used in tests that
    /// pre-date the cluster-aware path).
    pub cluster_id: String,
    pub cpu: u32,
    pub memory_mib: u32,
    /// Rootfs size (GiB). Charged against the host's rootfs thin
    /// pool free capacity, not the data pools.
    pub rootfs_gib: u32,
    /// Sum of every `storage_disks[].min_size_gib`. Used as a coarse
    /// pre-filter against the host's aggregate data capacity; the
    /// fine-grained per-pool capacity check happens in
    /// [`pick_disk_placements`].
    pub data_gib: u32,
    pub gpus: u32,
    pub min_group_size: u32,
    /// Operator-supplied **host** placement constraints. Empty by
    /// default. Per-disk pool selectors live on each `storage_disks`
    /// entry, not here.
    pub placement: Placement,
    /// Per-disk storage requests. The scheduler picks one
    /// `(pool, device_id)` tuple per entry on the chosen host.
    pub storage_disks: Vec<StorageDisk>,
}

impl From<&CreateMachineRequest> for ScheduleRequest {
    fn from(req: &CreateMachineRequest) -> Self {
        let data_gib: u32 = req.storage_disks.iter().map(|d| d.min_size_gib as u32).sum();
        Self {
            cluster_id: req.cluster_id.clone(),
            cpu: req.cpu,
            memory_mib: req.memory_mib,
            rootfs_gib: req.disk_gib,
            data_gib,
            gpus: req.gpus,
            min_group_size: req
                .gpu_constraints
                .as_ref()
                .map(|c| c.min_group_size)
                .unwrap_or(0),
            placement: req
                .placement
                .clone()
                .map(Placement::from)
                .unwrap_or_default(),
            storage_disks: req.storage_disks.clone(),
        }
    }
}

/// Pre-fetched per-host storage view the scheduler reads while
/// picking `(host, pool, device)` tuples. Built once per scheduling
/// pass from [`crate::db::Db::list_host_pools`] +
/// [`crate::db::Db::list_pool_devices`] +
/// [`crate::db::Db::host_drain_markers`].
///
/// Loaded synchronously into a struct (rather than queried lazily
/// inside the scheduler) so the scheduler stays a pure function over
/// data — easy to unit-test, no async deadlocks, no DB calls inside
/// the placement-mutex critical section.
#[derive(Debug, Clone, Default)]
pub struct HostStorageView {
    pub pools: Vec<PoolView>,
}

#[derive(Debug, Clone)]
pub struct PoolView {
    pub name: String,
    pub backend: String,
    pub labels: BTreeMap<String, String>,
    pub schedulable_total_bytes: i64,
    pub schedulable_free_bytes: i64,
    pub devices: Vec<DeviceView>,
}

#[derive(Debug, Clone)]
pub struct DeviceView {
    pub device_id: String,
    pub total_bytes: i64,
    pub free_bytes: i64,
    /// True iff `physical=Ready` AND scheduling state is `enabled`.
    /// The scheduler uses this single boolean; the two sources of
    /// "unschedulable" (hardware vs operator) are surfaced separately
    /// to operators via `basisctl pool show`.
    pub schedulable: bool,
    /// Set of cluster_ids that already have a REPLICATED assignment on
    /// this device. Hard-rejects same-cluster REPLICATED collision.
    pub replicated_clusters: HashSet<String>,
}

impl HostStorageView {
    /// Build from the raw DB rows. Same-cluster OSD occupancy comes
    /// from `pool_disk_assignment` rows; the caller pre-fetches them
    /// scoped to this host.
    pub fn from_rows(
        pools: Vec<HostPoolRow>,
        devices_by_pool: HashMap<String, Vec<HostPoolDeviceRow>>,
        drained_devices: &HashSet<(String, String)>,
        replicated_clusters_per_device: &HashMap<(String, String), HashSet<String>>,
    ) -> Self {
        let pools = pools
            .into_iter()
            .map(|p| {
                let labels = p.parsed_labels().unwrap_or_default();
                let pool_name = p.pool.clone();
                let devices = devices_by_pool
                    .get(&pool_name)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|d| {
                        let drained =
                            drained_devices.contains(&(pool_name.clone(), d.device_id.clone()));
                        DeviceView {
                            schedulable: d.is_ready() && !drained,
                            replicated_clusters: replicated_clusters_per_device
                                .get(&(pool_name.clone(), d.device_id.clone()))
                                .cloned()
                                .unwrap_or_default(),
                            device_id: d.device_id,
                            total_bytes: d.total_bytes,
                            free_bytes: d.free_bytes,
                        }
                    })
                    .collect();
                PoolView {
                    name: p.pool,
                    backend: p.backend,
                    labels,
                    schedulable_total_bytes: p.schedulable_total_bytes,
                    schedulable_free_bytes: p.schedulable_free_bytes,
                    devices,
                }
            })
            .collect();
        Self { pools }
    }
}

/// One per-disk placement decision made by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskPlacement {
    pub disk_index: u32,
    pub pool: String,
    pub device_id: String,
    pub min_size_gib: u64,
    pub purpose: DiskPurpose,
}

/// Pick `(pool, device_id)` for every disk in `disks`. Returns `None`
/// if any disk cannot be placed on this host — the host is then
/// rejected as a whole, identical to the existing CPU/mem fit check.
///
/// Algorithm (per the design doc):
/// 1. Filter pools by the disk's `selector.requires`.
/// 2. Within each surviving pool, enumerate `schedulable` devices.
/// 3. Reject devices without enough free capacity.
/// 4. Score the remaining `(pool, device)` candidates: pool prefers
///    score (primary), then a soft penalty for sharing a device with
///    another disk of this same VM, then backend tie-break.
///
/// No constraint rejects a placement on the basis of "this device
/// already carries a same-cluster replica" — replica spread is the
/// in-guest CSI driver's job (Ceph CRUSH, Longhorn replica
/// scheduler, …) which has the topology context basis can't see.
/// Basis nudges placement via the host-level `negative_cluster_
/// replicated_mates` score in `CandidateScore`, but never refuses.
///
/// Disks are placed in the order they appear in `disks`. Each placed
/// disk's chosen device is recorded so subsequent disks of the same
/// VM prefer different devices.
pub fn pick_disk_placements(
    cluster_id: &str,
    view: &HostStorageView,
    disks: &[StorageDisk],
) -> Option<Vec<DiskPlacement>> {
    let mut placements = Vec::with_capacity(disks.len());
    // Per-VM device occupancy accrued as we place disks of this
    // request. Used to penalize co-locating two disks of the same VM
    // on one device when alternatives exist.
    let mut this_vm_devices: HashSet<(String, String)> = HashSet::new();
    // Mutable free-bytes accounting so two disks in one request don't
    // both claim the same headroom.
    let mut device_free: HashMap<(String, String), i64> = HashMap::new();
    for pool in &view.pools {
        for d in &pool.devices {
            device_free.insert((pool.name.clone(), d.device_id.clone()), d.free_bytes);
        }
    }

    for (idx, disk) in disks.iter().enumerate() {
        let purpose = DiskPurpose::try_from(disk.purpose).ok()?;
        // Carried only for the DiskPlacement record (telemetry +
        // wire) — the scheduler itself doesn't gate on it.
        let need_bytes = (disk.min_size_gib as i64).saturating_mul(GIB);
        let selector: Placement = disk
            .selector
            .clone()
            .map(Placement::from)
            .unwrap_or_default();

        let mut best: Option<DiskCandidate> = None;
        for pool in &view.pools {
            if !selector.satisfies(&pool.labels) {
                continue;
            }
            for dev in &pool.devices {
                if !dev.schedulable {
                    continue;
                }
                let key = (pool.name.clone(), dev.device_id.clone());
                let avail = *device_free.get(&key).unwrap_or(&0);
                if avail < need_bytes {
                    continue;
                }
                let pool_score = selector.score(&pool.labels);
                // Soft same-cluster device spread. `replicated_clusters`
                // is the set of cluster_ids with a live REPLICATED
                // assignment on this device. Counting matters for the
                // "all OSDs of cluster A on device X" case: each
                // additional same-cluster mate on a device should
                // make the device less attractive than a sibling
                // with fewer (or none).
                let same_cluster_on_device = if cluster_id.is_empty() {
                    0
                } else if dev.replicated_clusters.contains(cluster_id) {
                    1
                } else {
                    0
                };
                // Soft "prefer not to share device with another disk
                // of the same VM"; small score penalty.
                let same_vm_penalty = if this_vm_devices.contains(&key) { 1 } else { 0 };
                let tie_breaker = backend_tie_breaker(&pool.backend, dev.free_bytes, need_bytes);
                let cand = DiskCandidate {
                    pool: pool.name.clone(),
                    device_id: dev.device_id.clone(),
                    score: DiskScore {
                        pool_prefers_score: pool_score,
                        negative_same_cluster_on_device: -same_cluster_on_device,
                        negative_same_vm_penalty: -same_vm_penalty,
                        tie_breaker,
                    },
                };
                if best
                    .as_ref()
                    .map(|b| cand.score > b.score)
                    .unwrap_or(true)
                {
                    best = Some(cand);
                }
            }
        }
        let pick = best?;
        let key = (pick.pool.clone(), pick.device_id.clone());
        if let Some(b) = device_free.get_mut(&key) {
            *b = b.saturating_sub(need_bytes);
        }
        this_vm_devices.insert(key);
        placements.push(DiskPlacement {
            disk_index: idx as u32,
            pool: pick.pool,
            device_id: pick.device_id,
            min_size_gib: disk.min_size_gib,
            purpose,
        });
    }
    Some(placements)
}

/// Per-backend tie-breaker, encoded so higher = better and the field
/// can be ordered as part of `DiskScore` without flipping per-call.
///
/// - `lvm-linear`: best-fit — smaller leftover wins.
/// - `raw-disk`: deterministic on `device_id`; capacity is binary so
///   leftover is irrelevant. Implemented at the caller (lowest-id
///   first by sort order); this returns 0.
/// - `nvme-namespace`: spread — fewer existing namespaces wins. The
///   caller would pass `-namespace_count` here; for M1 we don't have
///   an `nvme-namespace` backend so the branch is a no-op fallback.
fn backend_tie_breaker(backend: &str, free_bytes: i64, need_bytes: i64) -> i64 {
    match backend {
        "lvm-linear" => -(free_bytes - need_bytes), // smaller leftover wins
        _ => 0,
    }
}

/// Per-device disk score, layered most-significant-first. None of
/// these reject a placement; they only re-rank candidates that
/// already passed the hard filters (selector match + capacity +
/// schedulable). Failure-domain durability isn't a constraint basis
/// can enforce — the in-guest CSI driver knows the topology — so
/// these are nudges toward sensible defaults when there's free
/// choice.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DiskScore {
    /// Pool's `prefers` weight against the disk's selector.
    pool_prefers_score: u32,
    /// Negated count of same-cluster replicas already on this
    /// device. With three empty devices and three same-cluster OSDs,
    /// this nudges placement to spread one-per-device. With one
    /// device and two same-cluster OSDs, the second still places —
    /// just on the same device.
    negative_same_cluster_on_device: i32,
    /// Negated penalty for sharing a device with another disk of
    /// this same VM (a multi-disk VM prefers different devices).
    negative_same_vm_penalty: i32,
    tie_breaker: i64,
}

#[derive(Debug)]
struct DiskCandidate {
    pool: String,
    device_id: String,
    score: DiskScore,
}

/// Free capacity on a host after subtracting current usage. `cpu` is
/// scaled by `cpu_overcommit_ratio`; memory and both disk pools are
/// strict because oversubscribing either ends in OOM-kills or
/// ENOSPC — much worse failure modes than CPU time-slicing.
#[derive(Debug, Clone, Copy)]
struct Available {
    cpu: u32,
    memory_mib: u32,
    rootfs_gib: u32,
    data_gib: u32,
}

impl Available {
    fn from(host: &HostRow, usage: &HostUsage, cpu_overcommit_ratio: f32) -> Self {
        // Promote to f64 so large core counts with fractional ratios
        // don't lose precision in the multiply.
        let effective_cpu = (host.total_cpu as f64 * cpu_overcommit_ratio as f64) as i64;
        let rootfs_total_gib = host.rootfs_total_bytes / GIB;
        let data_total_gib = host.data_total_bytes / GIB;
        Self {
            cpu: effective_cpu.saturating_sub(usage.used_cpu).max(0) as u32,
            memory_mib: host
                .total_memory_mib
                .saturating_sub(usage.used_memory_mib)
                .max(0) as u32,
            rootfs_gib: rootfs_total_gib
                .saturating_sub(usage.used_rootfs_gib)
                .max(0) as u32,
            data_gib: data_total_gib.saturating_sub(usage.used_data_gib).max(0) as u32,
        }
    }

    fn fits(&self, req: &ScheduleRequest) -> bool {
        self.cpu >= req.cpu
            && self.memory_mib >= req.memory_mib
            && self.rootfs_gib >= req.rootfs_gib
            && self.data_gib >= req.data_gib
    }

    fn remaining_after(&self, req: &ScheduleRequest) -> u64 {
        self.cpu.saturating_sub(req.cpu) as u64
            + self.memory_mib.saturating_sub(req.memory_mib) as u64
    }
}

/// Composite score used to pick the best candidate among hosts that
/// already passed every hard filter (capacity, GPU, host placement
/// requires). Field order is the tiebreak order: `Ord` derives lex
/// compare across the tuple. **None of these fields can reject a
/// placement** — they only re-rank already-fitting candidates. A
/// placement only fails when no host has the capacity / GPUs /
/// labels the request demands; "I would have preferred to spread"
/// is never a failure.
///
/// Layered tiebreaks, highest-priority first:
///
/// - `negative_cluster_replicated_mates`: prefer hosts that aren't
///   already carrying same-cluster replicated disks. This is purely
///   a scoring nudge — a host with N same-cluster mates is still
///   eligible if it's the only one that fits. Replication failure-
///   domain durability is the in-guest CSI driver's responsibility
///   (Ceph CRUSH, Longhorn replica scheduler, …); basis only nudges
///   placement away from obvious co-location when there's free choice.
/// - `gpu_score`: higher = better topology fit (NVLink affinity).
/// - `negative_cluster_mates`: generic same-cluster spread, applies
///   to every VM.
/// - `prefers_score`: per-Machine soft preference against host
///   labels.
/// - `negative_rank`: per-host operator preference.
/// - `negative_remaining_after`: tie-of-last-resort, smallest
///   remaining wins (best-fit bin-pack).
///
/// Storing negated values keeps the comparator a single derived `Ord`
/// — much easier to test than a chain of `.then_with` flips.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CandidateScore {
    negative_cluster_replicated_mates: i64,
    gpu_score: i32,
    negative_cluster_mates: i64,
    prefers_score: u32,
    negative_rank: i64,
    negative_remaining_after: i64,
}

struct Candidate<'h> {
    host: &'h HostRow,
    score: CandidateScore,
    selected_gpus: Vec<GpuInfo>,
    disk_placements: Vec<DiskPlacement>,
}

/// Output of [`schedule`]. Wraps the host pick + the per-disk
/// placements + the GPUs so the server can act on all three without
/// needing to know the field shape.
#[derive(Debug, Clone)]
pub struct ScheduleDecision {
    pub host_id: String,
    pub gpus: Vec<GpuInfo>,
    pub disks: Vec<DiskPlacement>,
}

/// Pick the best host for a VM request and return the GPUs +
/// per-disk `(pool, device)` placements selected on that host. The
/// controller is the authoritative source of capacity: `usage_by_host`
/// comes from [`crate::db::Db::host_usage_snapshot`] and the per-host
/// `HostStorageView` map comes from [`HostStorageView::from_rows`].
///
/// `storage_by_host` is keyed by host_id; absent entries default to
/// "no data pools on this host" — fine for control-plane VMs without
/// `storage_disks`, but any VM with `storage_disks` requires the host
/// to appear in the map with a matching pool, otherwise the host is
/// rejected.
pub fn schedule(
    hosts: &[HostRow],
    usage_by_host: &HashMap<String, HostUsage>,
    storage_by_host: &HashMap<String, HostStorageView>,
    replicated_count_by_host: &HashMap<String, i64>,
    req: &ScheduleRequest,
    cpu_overcommit_ratio: f32,
) -> Result<ScheduleDecision, SchedulerError> {
    let empty_usage = HostUsage::default();
    let empty_view = HostStorageView::default();
    let mut candidates: Vec<Candidate<'_>> = Vec::new();
    // Track separately whether *any* host passed the requires filter.
    // Lets us return `UnsatisfiedRequirements` only when requires is
    // the actual cause — vs. `NoCapacity` for the more common case.
    let mut any_passed_requires = false;

    for host in hosts.iter().filter(|h| h.healthy) {
        if !req.placement.satisfies(&host.labels) {
            continue;
        }
        any_passed_requires = true;

        let usage = usage_by_host.get(&host.id).unwrap_or(&empty_usage);
        let avail = Available::from(host, usage, cpu_overcommit_ratio);
        if !avail.fits(req) {
            continue;
        }

        let free_gpus: Vec<GpuInfo> = host
            .gpu_inventory
            .iter()
            .filter(|g| !usage.assigned_pci.contains(&g.pci_address))
            .cloned()
            .collect();

        let (gpu_score, selected_gpus) = if req.gpus > 0 {
            let (score, selected) = gpu_topology_score(&free_gpus, req.gpus, req.min_group_size);
            if selected.is_empty() {
                continue;
            }
            (score, selected)
        } else {
            (0, Vec::new())
        };

        let cluster_mates = if req.cluster_id.is_empty() {
            0
        } else {
            usage
                .vms_by_cluster
                .get(&req.cluster_id)
                .copied()
                .unwrap_or(0)
        };

        // Per-host storage view + per-disk placements. If any disk
        // can't fit, this host is rejected wholesale — partial
        // allocation across disks would force the controller into
        // an "I placed disk 0 but failed disk 1; now what" mess that
        // tombstone-driven teardown handles cleanly only on whole-VM
        // boundaries.
        let storage = storage_by_host.get(&host.id).unwrap_or(&empty_view);
        let disk_placements = if req.storage_disks.is_empty() {
            Vec::new()
        } else {
            match pick_disk_placements(&req.cluster_id, storage, &req.storage_disks) {
                Some(p) => p,
                None => continue,
            }
        };

        // REPLICATED host-spread rank. Only counts when the request
        // *contains* at least one OSD — otherwise a non-storage VM's
        // placement should not be sensitive to the cluster's OSD
        // layout. Empty zero is the right default for non-OSD VMs.
        let request_has_replicated = req
            .storage_disks
            .iter()
            .any(|d| d.purpose == DiskPurpose::Replicated as i32);
        let replicated_mates = if request_has_replicated && !req.cluster_id.is_empty() {
            *replicated_count_by_host.get(&host.id).unwrap_or(&0)
        } else {
            0
        };

        candidates.push(Candidate {
            host,
            score: CandidateScore {
                negative_cluster_replicated_mates: -replicated_mates,
                gpu_score,
                negative_cluster_mates: -(cluster_mates as i64),
                prefers_score: req.placement.score(&host.labels),
                negative_rank: -host.rank,
                negative_remaining_after: -(avail.remaining_after(req) as i64),
            },
            selected_gpus,
            disk_placements,
        });
    }

    if candidates.is_empty() {
        return Err(
            if !any_passed_requires && !req.placement.requires.is_empty() {
                SchedulerError::UnsatisfiedRequirements(req.placement.describe_requires())
            } else {
                SchedulerError::NoCapacity(format!(
                    "cpu={}, mem={}MiB, rootfs={}GiB, data={}GiB, gpus={}, disks={}",
                    req.cpu,
                    req.memory_mib,
                    req.rootfs_gib,
                    req.data_gib,
                    req.gpus,
                    req.storage_disks.len()
                ))
            },
        );
    }

    let winner = candidates
        .into_iter()
        .max_by(|a, b| a.score.cmp(&b.score))
        .expect("checked non-empty above");
    Ok(ScheduleDecision {
        host_id: winner.host.id.clone(),
        gpus: winner.selected_gpus,
        disks: winner.disk_placements,
    })
}

/// Score GPU topology and return the selected GPUs.
///
/// Returns `(score, selected_gpus)`. An empty `selected_gpus` means the
/// request cannot be satisfied on this host.
///
/// Score: 3 = all GPUs share NVLink, 1 = spread across groups (no NVLink affinity).
/// A hard `min_group_size` constraint collapses this to "all in one NVLink group
/// or nothing".
fn gpu_topology_score(
    available: &[GpuInfo],
    count: u32,
    min_group_size: u32,
) -> (i32, Vec<GpuInfo>) {
    let count = count as usize;

    // Group by NVLink domain. Group 0 means "not on NVLink" and is treated
    // as distinct per-GPU (single-GPU groups) for the purpose of scoring —
    // but here we still bucket them together because the hard constraint
    // explicitly asks for a same-group guarantee.
    let mut nvlink_groups: std::collections::HashMap<u32, Vec<&GpuInfo>> =
        std::collections::HashMap::new();
    for gpu in available {
        nvlink_groups.entry(gpu.nvlink_group).or_default().push(gpu);
    }

    // Hard constraint: need `min_group_size` GPUs in the same non-zero NVLink
    // domain. Group 0 does not count as a real NVLink domain.
    if min_group_size > 0 {
        for (group_id, gpus) in &nvlink_groups {
            if *group_id != 0 && gpus.len() >= count && gpus.len() >= min_group_size as usize {
                let selected: Vec<GpuInfo> = gpus[..count].iter().map(|g| (*g).clone()).collect();
                return (3, selected);
            }
        }
        return (0, Vec::new());
    }

    // Best effort: prefer a single non-zero NVLink group that can satisfy the request.
    for (group_id, gpus) in &nvlink_groups {
        if *group_id != 0 && gpus.len() >= count {
            let selected: Vec<GpuInfo> = gpus[..count].iter().map(|g| (*g).clone()).collect();
            return (3, selected);
        }
    }

    // Fallback: any N GPUs, no topological affinity.
    if available.len() >= count {
        return (1, available[..count].to_vec());
    }

    (0, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pre-overcommit default: the scheduler's old behavior. Most existing
    /// tests should keep passing under strict capacity; separate tests
    /// below exercise ratios > 1.0.
    const STRICT: f32 = 1.0;

    /// Single-pool helper: tests that don't care about the rootfs/data
    /// split treat `disk` as combined capacity, which the helper bills
    /// 50/50 to each pool. Tests that exercise per-pool exhaustion use
    /// [`make_host_pools`] directly.
    fn make_host(id: &str, cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostRow {
        let half = (disk / 2) * GIB;
        make_host_pools(id, cpu, mem, half, half, gpus)
    }

    fn make_host_pools(
        id: &str,
        cpu: i64,
        mem: i64,
        rootfs_bytes: i64,
        data_bytes: i64,
        gpus: &[GpuInfo],
    ) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: format!("{id}.local"),
            total_cpu: cpu,
            total_memory_mib: mem,
            rootfs_total_bytes: rootfs_bytes,
            rootfs_free_bytes: rootfs_bytes,
            rootfs_metadata_total_bytes: 0,
            rootfs_metadata_free_bytes: 0,
            data_total_bytes: data_bytes,
            data_free_bytes: data_bytes,
            gpu_inventory: gpus.to_vec(),
            vtep_address: format!("10.100.0.{id}"),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
            rank: 0,
            labels: std::collections::BTreeMap::new(),
        }
    }

    fn with_labels(mut h: HostRow, labels: &[(&str, &str)]) -> HostRow {
        h.labels = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        h
    }

    fn with_rank(mut h: HostRow, rank: i64) -> HostRow {
        h.rank = rank;
        h
    }

    fn gpu(pci: &str, nvlink_group: u32) -> GpuInfo {
        GpuInfo {
            pci_address: pci.to_string(),
            model: "A100".to_string(),
            iommu_group: pci.chars().last().unwrap_or('0').to_string(),
            nvlink_group,
        }
    }

    fn basic_req(gpus: u32, min_group: u32) -> ScheduleRequest {
        ScheduleRequest {
            cluster_id: String::new(),
            cpu: 4,
            memory_mib: 8192,
            rootfs_gib: 100,
            data_gib: 0,
            gpus,
            min_group_size: min_group,
            placement: Placement::default(),
            storage_disks: Vec::new(),
        }
    }

    fn req_with_placement(p: Placement) -> ScheduleRequest {
        ScheduleRequest {
            placement: p,
            ..basic_req(0, 0)
        }
    }

    /// Summarize one host's consumed capacity — the shape
    /// `Db::host_usage_snapshot` hands to `schedule`. Rootfs and data
    /// usage are tracked separately so the scheduler's per-pool
    /// budgets stay honest.
    fn usage(cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostUsage {
        // Default helper: assume the combined `disk` was rootfs-only.
        // Tests that exercise data-pool consumption use `usage_pools`.
        usage_pools(cpu, mem, disk, 0, gpus)
    }

    fn usage_pools(
        cpu: i64,
        mem: i64,
        rootfs_gib: i64,
        data_gib: i64,
        gpus: &[GpuInfo],
    ) -> HostUsage {
        HostUsage {
            used_cpu: cpu,
            used_memory_mib: mem,
            used_rootfs_gib: rootfs_gib,
            used_data_gib: data_gib,
            assigned_pci: gpus.iter().map(|g| g.pci_address.clone()).collect(),
            vms_by_cluster: HashMap::new(),
        }
    }

    /// Like `usage`, but also seeds the per-cluster count map so tests
    /// can drive the soft anti-affinity branch.
    fn usage_with_cluster(
        cpu: i64,
        mem: i64,
        disk: i64,
        cluster_id: &str,
        cluster_vms: u32,
    ) -> HostUsage {
        let mut u = usage(cpu, mem, disk, &[]);
        u.vms_by_cluster.insert(cluster_id.to_string(), cluster_vms);
        u
    }

    /// Handy shorthand so test bodies read like "empty fleet".
    fn empty() -> HashMap<String, HostUsage> {
        HashMap::new()
    }

    /// Test wrapper around `schedule` that defaults the new
    /// storage-side args to empty maps. Existing tests don't exercise
    /// per-disk pool selection (they pre-date it); the dedicated
    /// per-pool tests at the bottom of this module construct the
    /// `storage_by_host` map explicitly.
    fn schedule_no_disks(
        hosts: &[HostRow],
        usage: &HashMap<String, HostUsage>,
        req: &ScheduleRequest,
        ratio: f32,
    ) -> Result<(String, Vec<GpuInfo>), SchedulerError> {
        let storage = HashMap::new();
        let replicated = HashMap::new();
        super::schedule(hosts, usage, &storage, &replicated, req, ratio)
            .map(|d| (d.host_id, d.gpus))
    }

    #[test]
    fn test_schedule_basic() {
        let hosts = vec![
            make_host("h1", 8, 16384, 500, &[]),
            make_host("h2", 4, 8192, 200, &[]),
        ];
        let (host_id, gpus) = schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        // h2 is a tighter fit (best-fit bin-packing prefers closest to full)
        assert_eq!(host_id, "h2");
        assert!(gpus.is_empty());
    }

    #[test]
    fn test_schedule_no_capacity() {
        let hosts = vec![make_host("h1", 2, 4096, 50, &[])];
        let req = ScheduleRequest {
            cpu: 8,
            memory_mib: 16384,
            rootfs_gib: 100,
            ..basic_req(0, 0)
        };
        assert!(matches!(
            schedule_no_disks(&hosts, &empty(), &req, STRICT),
            Err(SchedulerError::NoCapacity(_))
        ));
    }

    #[test]
    fn test_schedule_with_gpus() {
        let gpus = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let (host_id, selected) = schedule_no_disks(&hosts, &empty(), &basic_req(2, 2), STRICT).unwrap();
        assert_eq!(host_id, "h1");
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_schedule_skips_unhealthy_hosts() {
        let mut hosts = vec![
            make_host("h1", 16, 65536, 1000, &[]),
            make_host("h2", 16, 65536, 1000, &[]),
        ];
        hosts[0].healthy = false;
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_all_unhealthy_returns_error() {
        let mut hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        hosts[0].healthy = false;
        assert!(schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).is_err());
    }

    #[test]
    fn test_schedule_empty_hosts_returns_error() {
        let req = ScheduleRequest {
            cpu: 1,
            memory_mib: 1024,
            rootfs_gib: 10,
            ..basic_req(0, 0)
        };
        assert!(schedule_no_disks(&[], &empty(), &req, STRICT).is_err());
    }

    #[test]
    fn test_schedule_bin_packing_prefers_tightest_fit() {
        let hosts = vec![
            make_host("big", 64, 262144, 2000, &[]),
            make_host("medium", 16, 32768, 500, &[]),
            make_host("small", 8, 16384, 200, &[]),
        ];
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        assert_eq!(host_id, "small");
    }

    #[test]
    fn test_schedule_insufficient_disk_skips_host() {
        let hosts = vec![
            make_host("h1", 16, 65536, 50, &[]),
            make_host("h2", 16, 65536, 200, &[]),
        ];
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        assert_eq!(host_id, "h2");
    }

    /// A request charges its rootfs against the rootfs pool budget
    /// only. A host with plenty of data-pool space but a saturated
    /// rootfs pool must be refused — the two pools are independent.
    #[test]
    fn test_schedule_rejects_when_rootfs_pool_full() {
        // 200 GiB rootfs, 800 GiB data. Existing VMs already used 150
        // GiB rootfs. A 100-GiB-rootfs request must fail even though
        // data pool is empty.
        let hosts = vec![make_host_pools("h1", 64, 262144, 200 * GIB, 800 * GIB, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage_pools(4, 8192, 150, 0, &[]));
        let req = ScheduleRequest {
            rootfs_gib: 100,
            ..basic_req(0, 0)
        };
        assert!(schedule_no_disks(&hosts, &usage_by_host, &req, STRICT).is_err());
    }

    /// And the symmetric case: a rootfs-fits request whose data
    /// disks blow past the data VG must be refused. Without this
    /// split, an OSD VM with a 1 TiB extra disk could eat into the
    /// rootfs pool — exactly what the two-pool design exists to
    /// prevent.
    #[test]
    fn test_schedule_rejects_when_data_pool_full() {
        let hosts = vec![make_host_pools("h1", 64, 262144, 200 * GIB, 800 * GIB, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage_pools(4, 8192, 50, 700, &[]));
        let req = ScheduleRequest {
            rootfs_gib: 50,
            data_gib: 200,
            ..basic_req(0, 0)
        };
        assert!(schedule_no_disks(&hosts, &usage_by_host, &req, STRICT).is_err());
    }

    /// A `CreateMachineRequest` lands its rootfs on `rootfs_gib` and
    /// its extras on `data_gib` — never collapsed.
    #[test]
    fn test_schedule_request_splits_rootfs_and_data() {
        let req = CreateMachineRequest {
            cluster_id: "c".into(),
            name: "n".into(),
            cpu: 2,
            memory_mib: 1024,
            disk_gib: 100,
            image: String::new(),
            bootstrap_data: Vec::new(),
            gpus: 0,
            gpu_constraints: None,
            storage_disks: vec![
                basis_proto::StorageDisk {
                    min_size_gib: 400,
                    selector: None,
                    purpose: basis_proto::DiskPurpose::GenericData as i32,
                },
                basis_proto::StorageDisk {
                    min_size_gib: 100,
                    selector: None,
                    purpose: basis_proto::DiskPurpose::GenericData as i32,
                },
            ],
            placement: None,
        };
        let sched_req: ScheduleRequest = (&req).into();
        assert_eq!(sched_req.rootfs_gib, 100);
        assert_eq!(sched_req.data_gib, 500);
    }

    #[test]
    fn test_schedule_gpu_not_enough_available() {
        let gpus = vec![gpu("0000:41:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        assert!(schedule_no_disks(&hosts, &empty(), &basic_req(2, 0), STRICT).is_err());
    }

    #[test]
    fn test_schedule_gpu_prefers_nvlink_group() {
        // 4 GPUs: 2 in NVLink group 1, 2 in NVLink group 2. Request for 2
        // should pick from a single NVLink group.
        let gpus = vec![
            gpu("0000:41:00.0", 1),
            gpu("0000:42:00.0", 1),
            gpu("0000:81:00.0", 2),
            gpu("0000:82:00.0", 2),
        ];
        let hosts = vec![make_host("h1", 32, 131072, 2000, &gpus)];
        let (_, selected) = schedule_no_disks(&hosts, &empty(), &basic_req(2, 0), STRICT).unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].nvlink_group, selected[1].nvlink_group);
    }

    #[test]
    fn test_schedule_gpu_hard_constraint_unsatisfiable() {
        // 4 GPUs, 2 per NVLink group. Hard constraint min_group_size=4 cannot
        // be satisfied — no single group has 4 GPUs.
        let gpus = vec![
            gpu("0000:41:00.0", 1),
            gpu("0000:42:00.0", 1),
            gpu("0000:81:00.0", 2),
            gpu("0000:82:00.0", 2),
        ];
        let hosts = vec![make_host("h1", 32, 131072, 2000, &gpus)];
        assert!(schedule_no_disks(&hosts, &empty(), &basic_req(4, 4), STRICT).is_err());
    }

    #[test]
    fn test_schedule_hard_constraint_rejects_group_zero() {
        // 4 GPUs all in group 0 ("unknown / not on NVLink"). A hard same-group
        // constraint must reject this even though they share the bucket.
        let gpus = vec![
            gpu("0000:41:00.0", 0),
            gpu("0000:42:00.0", 0),
            gpu("0000:81:00.0", 0),
            gpu("0000:82:00.0", 0),
        ];
        let hosts = vec![make_host("h1", 32, 131072, 2000, &gpus)];
        assert!(schedule_no_disks(&hosts, &empty(), &basic_req(2, 2), STRICT).is_err());
    }

    #[test]
    fn test_schedule_skips_assigned_gpus() {
        let all_gpus = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &all_gpus)];

        // One VM on h1 already holds 0000:41:00.0.
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert(
            "h1".to_string(),
            usage(2, 2048, 10, &[gpu("0000:41:00.0", 1)]),
        );

        // 2 GPUs requested, only 1 free — should fail
        assert!(schedule_no_disks(&hosts, &usage_by_host, &basic_req(2, 0), STRICT).is_err());

        // 1 GPU request should pick the free one
        let (host_id, selected) =
            schedule_no_disks(&hosts, &usage_by_host, &basic_req(1, 0), STRICT).unwrap();
        assert_eq!(host_id, "h1");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].pci_address, "0000:42:00.0");
    }

    #[test]
    fn test_schedule_subtracts_vm_allocations_from_capacity() {
        // h1 has 16 vCPU, a 14-vCPU VM is on it. A request for 4 vCPU must fail.
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(14, 8192, 50, &[]));
        assert!(schedule_no_disks(&hosts, &usage_by_host, &basic_req(0, 0), STRICT).is_err());
    }

    #[test]
    fn test_schedule_multi_host_gpu_topology_tiebreak() {
        // Two hosts with equal capacity. Host "spread" has GPUs across NVLink
        // groups, "together" has them in one. Scheduler must prefer "together".
        let gpus_spread = vec![gpu("0000:41:00.0", 1), gpu("0000:81:00.0", 2)];
        let gpus_together = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];

        let hosts = vec![
            make_host("spread", 16, 65536, 1000, &gpus_spread),
            make_host("together", 16, 65536, 1000, &gpus_together),
        ];
        let (host_id, selected) = schedule_no_disks(&hosts, &empty(), &basic_req(2, 0), STRICT).unwrap();
        assert_eq!(host_id, "together");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].nvlink_group, selected[1].nvlink_group);
    }

    /// Soft anti-affinity: a second VM in the same cluster prefers a
    /// peer host with no cluster-mate over the host already running
    /// one, even when the other host is a slightly looser fit. Without
    /// this, three equal VMs on two equal hosts pile onto the
    /// best-fit winner and a single host failure kills the cluster.
    #[test]
    fn test_schedule_soft_anti_affinity_within_cluster() {
        let hosts = vec![
            make_host("h1", 16, 65536, 1000, &[]),
            make_host("h2", 16, 65536, 1000, &[]),
        ];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage_with_cluster(4, 8192, 100, "c1", 1));

        let req = ScheduleRequest {
            cluster_id: "c1".into(),
            ..basic_req(0, 0)
        };
        let (host_id, _) = schedule_no_disks(&hosts, &usage_by_host, &req, STRICT).unwrap();
        assert_eq!(host_id, "h2", "anti-affinity should outweigh tighter fit");
    }

    /// Anti-affinity is per-cluster: a VM from cluster `c2` is
    /// indifferent to how many `c1` VMs already sit on a host, so
    /// best-fit alone decides.
    #[test]
    fn test_schedule_anti_affinity_ignores_other_clusters() {
        let hosts = vec![
            make_host("h1", 16, 65536, 1000, &[]),
            make_host("h2", 16, 65536, 1000, &[]),
        ];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage_with_cluster(4, 8192, 100, "c1", 1));

        let req = ScheduleRequest {
            cluster_id: "c2".into(),
            ..basic_req(0, 0)
        };
        let (host_id, _) = schedule_no_disks(&hosts, &usage_by_host, &req, STRICT).unwrap();
        assert_eq!(host_id, "h1", "different cluster — best-fit wins");
    }

    #[test]
    fn test_overcommit_allows_placement_over_physical_cpu() {
        // 16-CPU host, one 16-vCPU VM already assigned. A second 16-vCPU
        // request fails at 1.0 but succeeds at 2.0.
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(16, 8192, 50, &[]));
        let req = ScheduleRequest {
            cpu: 16,
            rootfs_gib: 50,
            ..basic_req(0, 0)
        };
        assert!(schedule_no_disks(&hosts, &usage_by_host, &req, 1.0).is_err());
        let (host_id, _) = schedule_no_disks(&hosts, &usage_by_host, &req, 2.0).unwrap();
        assert_eq!(host_id, "h1");
    }

    #[test]
    fn test_overcommit_does_not_relax_memory_or_disk() {
        // A host whose memory is exactly consumed must refuse another VM
        // even at a very generous CPU overcommit ratio — memory is strict.
        let hosts = vec![make_host("h1", 16, 8192, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(2, 8192, 10, &[]));
        let req = ScheduleRequest {
            cpu: 2,
            memory_mib: 1024,
            rootfs_gib: 10,
            ..basic_req(0, 0)
        };
        assert!(schedule_no_disks(&hosts, &usage_by_host, &req, 8.0).is_err());

        // Same shape but rootfs pool over-subscribed: also refused
        // regardless of CPU ratio.
        let hosts = vec![make_host_pools("h2", 16, 65536, 50 * GIB, 0, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h2".to_string(), usage_pools(2, 2048, 50, 0, &[]));
        assert!(schedule_no_disks(&hosts, &usage_by_host, &req, 8.0).is_err());
    }

    #[test]
    fn test_overcommit_ratio_one_preserves_strict_behavior() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(14, 8192, 50, &[]));
        assert!(schedule_no_disks(&hosts, &usage_by_host, &basic_req(0, 0), 1.0).is_err());
    }

    // --- Placement (labels) ---

    #[test]
    fn placement_requires_filters_out_non_matching_hosts() {
        // Two roomy hosts; only one carries the required label. Even
        // though the other has plenty of capacity, the requires
        // filter is hard, so it must not be considered.
        let hosts = vec![
            with_labels(make_host("fast", 16, 65536, 1000, &[]), &[("tier", "fast")]),
            make_host("bulk", 16, 65536, 1000, &[]),
        ];
        let req = req_with_placement(Placement {
            requires: vec![PlacementRequirement {
                key: "tier".into(),
                values: vec!["fast".into()],
            }],
            prefers: vec![],
        });
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &req, STRICT).unwrap();
        assert_eq!(host_id, "fast");
    }

    #[test]
    fn placement_requires_no_match_returns_unsatisfied_requirements() {
        // Unsatisfiable requires must surface as a distinct error so
        // operators can tell "label your host" from "add capacity".
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let req = req_with_placement(Placement {
            requires: vec![PlacementRequirement {
                key: "tier".into(),
                values: vec!["fast".into()],
            }],
            prefers: vec![],
        });
        assert!(matches!(
            schedule_no_disks(&hosts, &empty(), &req, STRICT),
            Err(SchedulerError::UnsatisfiedRequirements(_))
        ));
    }

    #[test]
    fn placement_prefers_breaks_ties_in_favor_of_matching_host() {
        // Both hosts fit, no anti-affinity, no GPU difference. Only
        // the prefers score should decide.
        let hosts = vec![
            make_host("plain", 16, 65536, 1000, &[]),
            with_labels(make_host("fast", 16, 65536, 1000, &[]), &[("tier", "fast")]),
        ];
        let req = req_with_placement(Placement {
            requires: vec![],
            prefers: vec![PlacementPreference {
                key: "tier".into(),
                value: "fast".into(),
                weight: 100,
            }],
        });
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &req, STRICT).unwrap();
        assert_eq!(host_id, "fast");
    }

    #[test]
    fn placement_prefers_loses_to_anti_affinity() {
        // The "fast" host already has a cluster mate; the unlabeled
        // host doesn't. Anti-affinity sits above prefers in the chain
        // — spreading the cluster wins over the operator's tier hint.
        // Models the "first VM goes to fast, subsequent ones spread"
        // behavior callers actually want.
        let hosts = vec![
            with_labels(make_host("fast", 16, 65536, 1000, &[]), &[("tier", "fast")]),
            make_host("bulk", 16, 65536, 1000, &[]),
        ];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert(
            "fast".to_string(),
            usage_with_cluster(4, 8192, 100, "c1", 1),
        );
        let req = ScheduleRequest {
            cluster_id: "c1".into(),
            ..req_with_placement(Placement {
                requires: vec![],
                prefers: vec![PlacementPreference {
                    key: "tier".into(),
                    value: "fast".into(),
                    weight: 100,
                }],
            })
        };
        let (host_id, _) = schedule_no_disks(&hosts, &usage_by_host, &req, STRICT).unwrap();
        assert_eq!(host_id, "bulk");
    }

    #[test]
    fn placement_prefers_beats_rank() {
        // The "fast" host carries a higher rank (worse tiebreak) but
        // matches the workload's prefers — and prefers sits above rank
        // in the chain, so the workload preference wins.
        let hosts = vec![
            with_rank(
                with_labels(make_host("fast", 16, 65536, 1000, &[]), &[("tier", "fast")]),
                10,
            ),
            make_host("bulk", 16, 65536, 1000, &[]),
        ];
        let req = req_with_placement(Placement {
            requires: vec![],
            prefers: vec![PlacementPreference {
                key: "tier".into(),
                value: "fast".into(),
                weight: 100,
            }],
        });
        let (host_id, _) = schedule_no_disks(&hosts, &empty(), &req, STRICT).unwrap();
        assert_eq!(host_id, "fast");
    }

    #[test]
    fn placement_empty_is_no_op() {
        // Sanity: empty placement leaves the scheduler in pre-placement
        // behavior. Two equal hosts → best-fit / arbitrary tie.
        let hosts = vec![
            with_labels(
                make_host("labelled", 16, 65536, 1000, &[]),
                &[("tier", "fast")],
            ),
            make_host("plain", 16, 65536, 1000, &[]),
        ];
        // Both hosts are valid; we just assert the call succeeds and
        // doesn't crash on the empty placement path.
        assert!(schedule_no_disks(&hosts, &empty(), &basic_req(0, 0), STRICT).is_ok());
    }

    // -----------------------------------------------------------------
    // M1 golden tests: per-disk (pool, device) selection + hierarchical
    // same-cluster anti-affinity for REPLICATED disks. The shape of each
    // test is "build a fleet, build a request, assert the scheduler's
    // (host_id, disk_placements) are what the design says."
    // -----------------------------------------------------------------

    /// Helper: one pool with N healthy devices of equal size.
    fn pool(name: &str, backend: &str, devices: usize, dev_gib: i64) -> PoolView {
        let dev_bytes = dev_gib * (1 << 30);
        PoolView {
            name: name.to_string(),
            backend: backend.to_string(),
            labels: BTreeMap::from([("tier".into(), name.to_string())]),
            schedulable_total_bytes: dev_bytes * devices as i64,
            schedulable_free_bytes: dev_bytes * devices as i64,
            devices: (0..devices)
                .map(|i| DeviceView {
                    device_id: format!("{name}-dev-{i}"),
                    total_bytes: dev_bytes,
                    free_bytes: dev_bytes,
                    schedulable: true,
                    replicated_clusters: HashSet::new(),
                })
                .collect(),
        }
    }

    /// Helper: a request with one REPLICATED disk needing `tier=$tier`
    /// of `min_gib` GiB.
    fn replicated_disk_req(cluster: &str, tier: &str, min_gib: u64) -> ScheduleRequest {
        ScheduleRequest {
            cluster_id: cluster.to_string(),
            cpu: 4,
            memory_mib: 8192,
            rootfs_gib: 100,
            data_gib: min_gib as u32,
            gpus: 0,
            min_group_size: 0,
            placement: Placement::default(),
            storage_disks: vec![StorageDisk {
                min_size_gib: min_gib,
                selector: Some(basis_proto::LabelSelector {
                    requires: vec![basis_proto::PlacementRequirement {
                        key: "tier".into(),
                        values: vec![tier.into()],
                    }],
                    prefers: vec![],
                }),
                purpose: DiskPurpose::Replicated as i32,
            }],
        }
    }

    /// Three hosts, one fast pool with one device each. Three OSD
    /// requests for the same Lattice cluster MUST land on three
    /// different hosts — host-spread is the dominant axis for
    /// REPLICATED disks.
    #[test]
    fn three_replicas_spread_across_three_hosts() {
        let hosts = (0..3)
            .map(|i| make_host(&format!("h{i}"), 16, 65536, 1000, &[]))
            .collect::<Vec<_>>();
        let mut storage = HashMap::new();
        for h in &hosts {
            storage.insert(
                h.id.clone(),
                HostStorageView {
                    pools: vec![pool("fast", "lvm-linear", 1, 1000)],
                },
            );
        }
        let mut placed_hosts = HashSet::new();
        let mut osd_count_by_host: HashMap<String, i64> = HashMap::new();
        for i in 0..3 {
            let req = replicated_disk_req("cluster-a", "fast", 100);
            let d = super::schedule(
                &hosts,
                &empty(),
                &storage,
                &osd_count_by_host,
                &req,
                STRICT,
            )
            .expect("placement");
            assert!(
                placed_hosts.insert(d.host_id.clone()),
                "OSD #{i} landed on a host that already has a same-cluster OSD: {}",
                d.host_id
            );
            // Simulate the assignment landing: bump OSD count and
            // mark the device as carrying cluster-a.
            *osd_count_by_host.entry(d.host_id.clone()).or_insert(0) += 1;
            let view = storage.get_mut(&d.host_id).unwrap();
            for p in &mut view.pools {
                for dev in &mut p.devices {
                    if dev.device_id == d.disks[0].device_id {
                        dev.replicated_clusters.insert("cluster-a".into());
                    }
                }
            }
        }
        assert_eq!(placed_hosts.len(), 3);
    }

    /// Two clusters' OSDs on the same host MUST be allowed — capacity
    /// reuse across clusters is a feature, not a collision.
    #[test]
    fn different_clusters_can_share_a_device() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut storage = HashMap::new();
        storage.insert(
            "h1".into(),
            HostStorageView {
                pools: vec![pool("fast", "lvm-linear", 1, 1000)],
            },
        );
        let osd_count_by_host = HashMap::new();

        let req_a = replicated_disk_req("cluster-a", "fast", 100);
        let d_a = super::schedule(&hosts, &empty(), &storage, &osd_count_by_host, &req_a, STRICT)
            .expect("cluster-a placement");
        // Mark the device as holding cluster-a's OSD.
        for p in &mut storage.get_mut("h1").unwrap().pools {
            for dev in &mut p.devices {
                if dev.device_id == d_a.disks[0].device_id {
                    dev.replicated_clusters.insert("cluster-a".into());
                }
            }
        }
        // cluster-b must be allowed onto the same device — different
        // replication scheme.
        let req_b = replicated_disk_req("cluster-b", "fast", 100);
        let d_b = super::schedule(&hosts, &empty(), &storage, &osd_count_by_host, &req_b, STRICT)
            .expect("cluster-b placement");
        assert_eq!(d_b.host_id, "h1");
    }

    /// Three replicas on a 2-host fleet (the homelab case). Basis
    /// MUST place all three: same-cluster co-location is a soft
    /// preference, not a hard reject. The CSI driver in-guest
    /// (Ceph CRUSH, Longhorn, …) decides whether the resulting
    /// failure-domain layout is acceptable.
    #[test]
    fn three_replicas_on_two_hosts_succeeds() {
        let hosts = (0..2)
            .map(|i| make_host(&format!("h{i}"), 16, 65536, 1000, &[]))
            .collect::<Vec<_>>();
        let mut storage = HashMap::new();
        for h in &hosts {
            storage.insert(
                h.id.clone(),
                HostStorageView {
                    pools: vec![pool("fast", "lvm-linear", 1, 1000)],
                },
            );
        }
        let mut osd_count_by_host: HashMap<String, i64> = HashMap::new();
        let mut placed = Vec::new();
        for _ in 0..3 {
            let req = replicated_disk_req("cluster-a", "fast", 100);
            let d = super::schedule(
                &hosts,
                &empty(),
                &storage,
                &osd_count_by_host,
                &req,
                STRICT,
            )
            .expect("3-on-2 must succeed (placement is preference-only)");
            placed.push(d.host_id.clone());
            *osd_count_by_host.entry(d.host_id.clone()).or_insert(0) += 1;
            // Mirror the agent's eventual replicated_clusters update,
            // even though it no longer affects placement — this keeps
            // the test honest about the surrounding state machine.
            let view = storage.get_mut(&d.host_id).unwrap();
            for p in &mut view.pools {
                for dev in &mut p.devices {
                    if dev.device_id == d.disks[0].device_id {
                        dev.replicated_clusters.insert("cluster-a".into());
                    }
                }
            }
        }
        assert_eq!(placed.len(), 3);
        // Host-spread preference: with 2 hosts and 3 replicas, the
        // distribution should be 2/1 (or 1/2), never 3/0.
        let h0 = placed.iter().filter(|h| *h == "h0").count();
        let h1 = placed.iter().filter(|h| *h == "h1").count();
        assert!(
            h0 > 0 && h1 > 0,
            "host-spread preference must use both hosts: h0={h0}, h1={h1}"
        );
    }

    /// One host, multiple devices, many same-cluster replicas. The
    /// host-internal `negative_same_vm_penalty` doesn't apply (one
    /// disk per VM); the scheduler should prefer different devices
    /// on the host but still place every disk even when devices have
    /// to repeat. This used to assert hard-reject; rewriting to
    /// assert "all placed, prefers spread."
    #[test]
    fn many_replicas_on_one_host_all_place() {
        let hosts = vec![make_host("h1", 64, 262144, 4000, &[])];
        let mut storage = HashMap::new();
        storage.insert(
            "h1".into(),
            HostStorageView {
                pools: vec![pool("fast", "lvm-linear", 3, 1000)],
            },
        );
        let mut osd_count_by_host: HashMap<String, i64> = HashMap::new();
        let mut devs = Vec::new();
        for _ in 0..6 {
            let req = replicated_disk_req("cluster-a", "fast", 100);
            let d = super::schedule(
                &hosts,
                &empty(),
                &storage,
                &osd_count_by_host,
                &req,
                STRICT,
            )
            .expect("must place; preference-only");
            devs.push(d.disks[0].device_id.clone());
            *osd_count_by_host.entry(d.host_id.clone()).or_insert(0) += 1;
            let view = storage.get_mut("h1").unwrap();
            for p in &mut view.pools {
                for dev in &mut p.devices {
                    if dev.device_id == devs.last().unwrap().as_str() {
                        dev.replicated_clusters.insert("cluster-a".into());
                    }
                }
            }
        }
        assert_eq!(devs.len(), 6, "every disk placed");
        // 6 disks across 3 devices. The soft "no same-cluster mate
        // on this device" preference fires for the first 3, so each
        // pristine device gets one. After that the preference is a
        // tie among devices that all already carry a mate, so 4–6
        // are placed without further spread guarantee. The thing
        // that matters: every device was used at least once.
        let unique: HashSet<_> = devs.iter().collect();
        assert_eq!(
            unique.len(),
            3,
            "soft preference should have spread the first wave: {devs:?}"
        );
    }

    /// Pool selector mismatch (request wants `tier=fast`, host only
    /// has `tier=bulk`) must fail with `UnsatisfiedRequirements`-shaped
    /// rejection — the failure mode of "no host has the right pool"
    /// is identical to "no host has the right host label", and the
    /// scheduler should treat them the same.
    #[test]
    fn replicated_disk_with_no_matching_pool_fails() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut storage = HashMap::new();
        storage.insert(
            "h1".into(),
            HostStorageView {
                pools: vec![pool("bulk", "lvm-linear", 2, 1000)],
            },
        );
        let osd_count_by_host = HashMap::new();
        let req = replicated_disk_req("cluster-a", "fast", 100);
        let err = super::schedule(&hosts, &empty(), &storage, &osd_count_by_host, &req, STRICT)
            .expect_err("must reject");
        // Host placement passed (no `requires` on the host); the pool
        // selector failed inside the host. That's a NoCapacity from
        // the scheduler's POV.
        assert!(matches!(err, SchedulerError::NoCapacity(_)));
    }

    /// Disabled (operator-fenced) devices are excluded from
    /// placement even when they have free capacity. A host whose
    /// only matching device is disabled can't accept the disk.
    #[test]
    fn disabled_devices_are_excluded() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut p = pool("fast", "lvm-linear", 1, 1000);
        p.devices[0].schedulable = false;
        let mut storage = HashMap::new();
        storage.insert("h1".into(), HostStorageView { pools: vec![p] });
        let osd_count_by_host = HashMap::new();
        let req = replicated_disk_req("cluster-a", "fast", 100);
        let err = super::schedule(&hosts, &empty(), &storage, &osd_count_by_host, &req, STRICT)
            .expect_err("disabled device → no placement");
        assert!(matches!(err, SchedulerError::NoCapacity(_)));
    }

}
