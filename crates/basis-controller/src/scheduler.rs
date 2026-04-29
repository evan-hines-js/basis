use std::collections::HashMap;

use basis_common::gpu::GpuInfo;
use basis_proto::CreateMachineRequest;

use crate::db::{HostRow, HostUsage, GIB};

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

impl From<basis_proto::PlacementSpec> for Placement {
    fn from(spec: basis_proto::PlacementSpec) -> Self {
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
    /// pool free capacity, not the data VG.
    pub rootfs_gib: u32,
    /// Sum of extra-disk sizes (GiB). Charged against the host's
    /// data VG free capacity, not the rootfs pool. Mixing the two
    /// budgets would let a VM with a 1 TiB OSD disk eat into the
    /// rootfs pool — the exact failure mode the split exists to
    /// prevent.
    pub data_gib: u32,
    pub gpus: u32,
    pub min_group_size: u32,
    /// Operator-supplied placement constraints. Empty by default, in
    /// which case the scheduler picks any host that fits.
    pub placement: Placement,
}

impl From<&CreateMachineRequest> for ScheduleRequest {
    fn from(req: &CreateMachineRequest) -> Self {
        let data_gib: u32 = req.extra_disks.iter().map(|d| d.size_gib).sum();
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
        }
    }
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

/// Composite score used to pick the best candidate. Field order is
/// the tiebreak order: `Ord` derives lex compare across the tuple.
///
/// Each field's polarity matches "higher is better" so the scheduler
/// can pick `.max_by_key` without per-field flips:
/// - `gpu_score`: higher = better topology fit
/// - `negative_cluster_mates`: stored negated so fewer mates wins
/// - `prefers_score`: higher = more preference matches
/// - `negative_rank`: stored negated so lower rank wins
/// - `remaining_after`: tie-of-last-resort, smallest remaining wins
///
/// Storing negated values keeps the comparator a single derived `Ord`
/// — much easier to test than a chain of `.then_with` flips.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CandidateScore {
    gpu_score: i32,
    negative_cluster_mates: i64,
    prefers_score: u32,
    negative_rank: i64,
    // Best-fit. Wrapped in `std::cmp::Reverse` at compare time would
    // also work, but inverting here keeps the type plain `Ord`.
    negative_remaining_after: i64,
}

struct Candidate<'h> {
    host: &'h HostRow,
    score: CandidateScore,
    selected_gpus: Vec<GpuInfo>,
}

/// Pick the best host for a VM request and return the GPUs selected
/// on that host. The controller is the authoritative source of
/// capacity: `usage_by_host` comes from `Db::host_usage_snapshot`
/// and drives both the fit check and the already-claimed-GPU filter.
///
/// Tiebreak chain (encoded in `CandidateScore`'s field order):
/// - capacity + GPU-availability + placement.requires: hard filters
/// - GPU topology score: workload fit (NVLink affinity)
/// - anti-affinity: spread same-cluster VMs across hosts
/// - placement.prefers score: per-Machine soft preference (e.g.
///   "this CP prefers tier=fast"). Above rank because it's a
///   per-workload signal — more specific than a per-host one.
/// - rank: per-host operator preference (e.g. "deprioritize the
///   consumer-disk box")
/// - best-fit bin-packing: smallest remaining capacity wins
pub fn schedule(
    hosts: &[HostRow],
    usage_by_host: &HashMap<String, HostUsage>,
    req: &ScheduleRequest,
    cpu_overcommit_ratio: f32,
) -> Result<(String, Vec<GpuInfo>), SchedulerError> {
    let empty = HostUsage::default();
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

        let usage = usage_by_host.get(&host.id).unwrap_or(&empty);
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

        candidates.push(Candidate {
            host,
            score: CandidateScore {
                gpu_score,
                negative_cluster_mates: -(cluster_mates as i64),
                prefers_score: req.placement.score(&host.labels),
                negative_rank: -host.rank,
                negative_remaining_after: -(avail.remaining_after(req) as i64),
            },
            selected_gpus,
        });
    }

    if candidates.is_empty() {
        return Err(
            if !any_passed_requires && !req.placement.requires.is_empty() {
                SchedulerError::UnsatisfiedRequirements(req.placement.describe_requires())
            } else {
                SchedulerError::NoCapacity(format!(
                    "cpu={}, mem={}MiB, rootfs={}GiB, data={}GiB, gpus={}",
                    req.cpu, req.memory_mib, req.rootfs_gib, req.data_gib, req.gpus
                ))
            },
        );
    }

    let winner = candidates
        .into_iter()
        .max_by(|a, b| a.score.cmp(&b.score))
        .expect("checked non-empty above");
    Ok((winner.host.id.clone(), winner.selected_gpus))
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

    #[test]
    fn test_schedule_basic() {
        let hosts = vec![
            make_host("h1", 8, 16384, 500, &[]),
            make_host("h2", 4, 8192, 200, &[]),
        ];
        let (host_id, gpus) = schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
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
            schedule(&hosts, &empty(), &req, STRICT),
            Err(SchedulerError::NoCapacity(_))
        ));
    }

    #[test]
    fn test_schedule_with_gpus() {
        let gpus = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let (host_id, selected) = schedule(&hosts, &empty(), &basic_req(2, 2), STRICT).unwrap();
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
        let (host_id, _) = schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_all_unhealthy_returns_error() {
        let mut hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        hosts[0].healthy = false;
        assert!(schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).is_err());
    }

    #[test]
    fn test_schedule_empty_hosts_returns_error() {
        let req = ScheduleRequest {
            cpu: 1,
            memory_mib: 1024,
            rootfs_gib: 10,
            ..basic_req(0, 0)
        };
        assert!(schedule(&[], &empty(), &req, STRICT).is_err());
    }

    #[test]
    fn test_schedule_bin_packing_prefers_tightest_fit() {
        let hosts = vec![
            make_host("big", 64, 262144, 2000, &[]),
            make_host("medium", 16, 32768, 500, &[]),
            make_host("small", 8, 16384, 200, &[]),
        ];
        let (host_id, _) = schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
        assert_eq!(host_id, "small");
    }

    #[test]
    fn test_schedule_insufficient_disk_skips_host() {
        let hosts = vec![
            make_host("h1", 16, 65536, 50, &[]),
            make_host("h2", 16, 65536, 200, &[]),
        ];
        let (host_id, _) = schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).unwrap();
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
        assert!(schedule(&hosts, &usage_by_host, &req, STRICT).is_err());
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
        assert!(schedule(&hosts, &usage_by_host, &req, STRICT).is_err());
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
            extra_disks: vec![
                basis_proto::ExtraDisk { size_gib: 400 },
                basis_proto::ExtraDisk { size_gib: 100 },
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
        assert!(schedule(&hosts, &empty(), &basic_req(2, 0), STRICT).is_err());
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
        let (_, selected) = schedule(&hosts, &empty(), &basic_req(2, 0), STRICT).unwrap();
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
        assert!(schedule(&hosts, &empty(), &basic_req(4, 4), STRICT).is_err());
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
        assert!(schedule(&hosts, &empty(), &basic_req(2, 2), STRICT).is_err());
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
        assert!(schedule(&hosts, &usage_by_host, &basic_req(2, 0), STRICT).is_err());

        // 1 GPU request should pick the free one
        let (host_id, selected) =
            schedule(&hosts, &usage_by_host, &basic_req(1, 0), STRICT).unwrap();
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
        assert!(schedule(&hosts, &usage_by_host, &basic_req(0, 0), STRICT).is_err());
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
        let (host_id, selected) = schedule(&hosts, &empty(), &basic_req(2, 0), STRICT).unwrap();
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
        let (host_id, _) = schedule(&hosts, &usage_by_host, &req, STRICT).unwrap();
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
        let (host_id, _) = schedule(&hosts, &usage_by_host, &req, STRICT).unwrap();
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
        assert!(schedule(&hosts, &usage_by_host, &req, 1.0).is_err());
        let (host_id, _) = schedule(&hosts, &usage_by_host, &req, 2.0).unwrap();
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
        assert!(schedule(&hosts, &usage_by_host, &req, 8.0).is_err());

        // Same shape but rootfs pool over-subscribed: also refused
        // regardless of CPU ratio.
        let hosts = vec![make_host_pools("h2", 16, 65536, 50 * GIB, 0, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h2".to_string(), usage_pools(2, 2048, 50, 0, &[]));
        assert!(schedule(&hosts, &usage_by_host, &req, 8.0).is_err());
    }

    #[test]
    fn test_overcommit_ratio_one_preserves_strict_behavior() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(14, 8192, 50, &[]));
        assert!(schedule(&hosts, &usage_by_host, &basic_req(0, 0), 1.0).is_err());
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
        let (host_id, _) = schedule(&hosts, &empty(), &req, STRICT).unwrap();
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
            schedule(&hosts, &empty(), &req, STRICT),
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
        let (host_id, _) = schedule(&hosts, &empty(), &req, STRICT).unwrap();
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
        let (host_id, _) = schedule(&hosts, &usage_by_host, &req, STRICT).unwrap();
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
        let (host_id, _) = schedule(&hosts, &empty(), &req, STRICT).unwrap();
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
        assert!(schedule(&hosts, &empty(), &basic_req(0, 0), STRICT).is_ok());
    }
}
