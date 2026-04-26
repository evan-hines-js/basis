use std::collections::HashMap;

use basis_common::gpu::GpuInfo;
use basis_proto::CreateMachineRequest;

use crate::db::{HostRow, HostUsage};

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("no host can satisfy request: {0}")]
    NoCapacity(String),
}

pub struct ScheduleRequest {
    /// Cluster this VM belongs to. Drives soft anti-affinity: hosts
    /// with fewer existing VMs from the same cluster outrank tighter
    /// fits. Empty string disables the penalty (used in tests that
    /// pre-date the cluster-aware path).
    pub cluster_id: String,
    pub cpu: u32,
    pub memory_mib: u32,
    /// Full host-side disk footprint — rootfs plus every extra data
    /// disk. The scheduler compares this directly against the host's
    /// free thin-pool capacity; callers that only supply rootfs
    /// would silently over-place any VM that asked for extras.
    pub disk_gib: u32,
    pub gpus: u32,
    pub min_group_size: u32,
}

impl From<&CreateMachineRequest> for ScheduleRequest {
    fn from(req: &CreateMachineRequest) -> Self {
        let extras_total: u32 = req.extra_disks.iter().map(|d| d.size_gib).sum();
        Self {
            cluster_id: req.cluster_id.clone(),
            cpu: req.cpu,
            memory_mib: req.memory_mib,
            disk_gib: req.disk_gib.saturating_add(extras_total),
            gpus: req.gpus,
            min_group_size: req
                .gpu_constraints
                .as_ref()
                .map(|c| c.min_group_size)
                .unwrap_or(0),
        }
    }
}

/// Free capacity on a host after subtracting current usage. `cpu` is
/// scaled by `cpu_overcommit_ratio`; memory and disk are strict
/// because oversubscribing either ends in OOM-kills or ENOSPC — much
/// worse failure modes than CPU time-slicing.
#[derive(Debug, Clone, Copy)]
struct Available {
    cpu: u32,
    memory_mib: u32,
    disk_gib: u32,
}

impl Available {
    fn from(host: &HostRow, usage: &HostUsage, cpu_overcommit_ratio: f32) -> Self {
        // Promote to f64 so large core counts with fractional ratios
        // don't lose precision in the multiply.
        let effective_cpu = (host.total_cpu as f64 * cpu_overcommit_ratio as f64) as i64;
        Self {
            cpu: effective_cpu.saturating_sub(usage.used_cpu).max(0) as u32,
            memory_mib: host
                .total_memory_mib
                .saturating_sub(usage.used_memory_mib)
                .max(0) as u32,
            disk_gib: host
                .total_disk_gib
                .saturating_sub(usage.used_disk_gib)
                .max(0) as u32,
        }
    }

    fn fits(&self, req: &ScheduleRequest) -> bool {
        self.cpu >= req.cpu && self.memory_mib >= req.memory_mib && self.disk_gib >= req.disk_gib
    }

    fn remaining_after(&self, req: &ScheduleRequest) -> u64 {
        self.cpu.saturating_sub(req.cpu) as u64
            + self.memory_mib.saturating_sub(req.memory_mib) as u64
    }
}

/// Pick the best host for a VM request and return the GPUs selected
/// on that host. The controller is the authoritative source of
/// capacity: `usage_by_host` comes from `Db::host_usage_snapshot`
/// and drives both the fit check and the already-claimed-GPU filter.
pub fn schedule(
    hosts: &[HostRow],
    usage_by_host: &HashMap<String, HostUsage>,
    req: &ScheduleRequest,
    cpu_overcommit_ratio: f32,
) -> Result<(String, Vec<GpuInfo>), SchedulerError> {
    let empty = HostUsage::default();
    let mut candidates: Vec<(&HostRow, i32, Vec<GpuInfo>, Available, u32)> = Vec::new();

    for host in hosts.iter().filter(|h| h.healthy) {
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

        let (score, selected) = if req.gpus > 0 {
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

        candidates.push((host, score, selected, avail, cluster_mates));
    }

    let (host, _score, selected, _, _) = candidates
        .into_iter()
        // Order: GPU-topology score (higher wins) → soft anti-affinity
        // (fewer same-cluster VMs wins) → operator-assigned rank
        // (lower wins; lets you say "prefer .206 for control plane")
        // → best-fit bin-packing (smallest remaining wins).
        //
        // Rank sits below the workload-shape signals (GPU + anti-
        // affinity) so an operator preference can't override a real
        // placement constraint, but above best-fit so two equally-fit
        // hosts go to the lower-rank one — which is what the operator
        // actually meant by setting the rank.
        .max_by(|a, b| {
            a.1.cmp(&b.1)
                .then_with(|| b.4.cmp(&a.4))
                .then_with(|| b.0.rank.cmp(&a.0.rank))
                .then_with(|| b.3.remaining_after(req).cmp(&a.3.remaining_after(req)))
        })
        .ok_or_else(|| {
            SchedulerError::NoCapacity(format!(
                "cpu={}, mem={}MiB, disk={}GiB, gpus={}",
                req.cpu, req.memory_mib, req.disk_gib, req.gpus
            ))
        })?;

    Ok((host.id.clone(), selected))
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

    fn make_host(id: &str, cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: format!("{id}.local"),
            total_cpu: cpu,
            total_memory_mib: mem,
            total_disk_gib: disk,
            gpu_inventory: gpus.to_vec(),
            vtep_address: format!("10.100.0.{id}"),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
            rank: 0,
        }
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
            disk_gib: 100,
            gpus,
            min_group_size: min_group,
        }
    }

    /// Summarize one host's consumed capacity — the shape
    /// `Db::host_usage_snapshot` hands to `schedule`. `disk` here is
    /// the total footprint (rootfs + extras); there's no separate
    /// "extras" knob because the scheduler only ever looks at the sum.
    fn usage(cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostUsage {
        HostUsage {
            used_cpu: cpu,
            used_memory_mib: mem,
            used_disk_gib: disk,
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
        u.vms_by_cluster
            .insert(cluster_id.to_string(), cluster_vms);
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
            cluster_id: String::new(),
            cpu: 8,
            memory_mib: 16384,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };
        assert!(schedule(&hosts, &empty(), &req, STRICT).is_err());
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
            cluster_id: String::new(),
            cpu: 1,
            memory_mib: 1024,
            disk_gib: 10,
            gpus: 0,
            min_group_size: 0,
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

    /// A VM already on a host charges *total* disk — rootfs plus every
    /// extra data disk — against free capacity. Without this, a
    /// ceph-heavy VM with 1 TiB of extras looks like its 100 GiB rootfs
    /// to the scheduler and the next placement silently oversubscribes
    /// the thin pool into ENOSPC. The `HostUsage` rollup coming out
    /// of `Db::host_usage_snapshot` already sums extras for us.
    #[test]
    fn test_schedule_charges_extra_disks_against_host() {
        // 500 GiB host, 400 GiB already in use (100 rootfs + 300 extras).
        // A 150 GiB request must be rejected.
        let hosts = vec![make_host("h1", 64, 262144, 500, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(4, 8192, 400, &[]));
        let req = ScheduleRequest {
            cluster_id: String::new(),
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 150,
            gpus: 0,
            min_group_size: 0,
        };
        assert!(schedule(&hosts, &usage_by_host, &req, STRICT).is_err());
    }

    /// A request's extra disks count against the host just like the
    /// rootfs does — `ScheduleRequest::from(CreateMachineRequest)`
    /// collapses them into a single `disk_gib` total.
    #[test]
    fn test_schedule_request_sums_extra_disks() {
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
        };
        let sched_req: ScheduleRequest = (&req).into();
        assert_eq!(sched_req.disk_gib, 600);
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
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
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
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
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
            cluster_id: String::new(),
            cpu: 16,
            memory_mib: 8192,
            disk_gib: 50,
            gpus: 0,
            min_group_size: 0,
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
            cluster_id: String::new(),
            cpu: 2,
            memory_mib: 1024,
            disk_gib: 10,
            gpus: 0,
            min_group_size: 0,
        };
        assert!(schedule(&hosts, &usage_by_host, &req, 8.0).is_err());

        // Same shape but disk over-subscribed: also refused regardless of CPU ratio.
        let hosts = vec![make_host("h2", 16, 65536, 50, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h2".to_string(), usage(2, 2048, 50, &[]));
        assert!(schedule(&hosts, &usage_by_host, &req, 8.0).is_err());
    }

    #[test]
    fn test_overcommit_ratio_one_preserves_strict_behavior() {
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut usage_by_host = HashMap::new();
        usage_by_host.insert("h1".to_string(), usage(14, 8192, 50, &[]));
        assert!(schedule(&hosts, &usage_by_host, &basic_req(0, 0), 1.0).is_err());
    }
}
