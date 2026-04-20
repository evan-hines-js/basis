use std::collections::HashMap;

use basis_common::gpu::GpuInfo;
use basis_common::json::parse_owned_json;
use basis_proto::{CreateMachineRequest, GpuDevice};

use crate::db::{HostRow, VmRow};

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("no host can satisfy request: {0}")]
    NoCapacity(String),
}

pub struct ScheduleRequest {
    pub cpu: u32,
    pub memory_mib: u32,
    pub disk_gib: u32,
    pub gpus: u32,
    pub min_group_size: u32,
}

impl From<&CreateMachineRequest> for ScheduleRequest {
    fn from(req: &CreateMachineRequest) -> Self {
        Self {
            cpu: req.cpu,
            memory_mib: req.memory_mib,
            disk_gib: req.disk_gib,
            gpus: req.gpus,
            min_group_size: req
                .gpu_constraints
                .as_ref()
                .map(|c| c.min_group_size)
                .unwrap_or(0),
        }
    }
}

/// Derived per-host capacity, computed from the host's totals minus the
/// VMs currently assigned to it.
#[derive(Debug, Clone, Copy)]
struct HostCapacity {
    available_cpu: u32,
    available_memory_mib: u32,
    available_disk_gib: u32,
}

impl HostCapacity {
    fn compute(host: &HostRow, vms: &[VmRow]) -> Self {
        let used_cpu: i64 = vms.iter().map(|v| v.cpu).sum();
        let used_mem: i64 = vms.iter().map(|v| v.memory_mib).sum();
        let used_disk: i64 = vms.iter().map(|v| v.disk_gib).sum();

        Self {
            available_cpu: host.total_cpu.saturating_sub(used_cpu).max(0) as u32,
            available_memory_mib: host.total_memory_mib.saturating_sub(used_mem).max(0) as u32,
            available_disk_gib: host.total_disk_gib.saturating_sub(used_disk).max(0) as u32,
        }
    }
}

/// Pick the best host for a VM request and return the GPUs selected on that host.
///
/// `vms_by_host` must map every healthy host's id to the VMs currently
/// assigned to it — the controller is the authoritative source of
/// capacity, and derives availability here rather than trusting agent
/// heartbeats.
pub fn schedule(
    hosts: &[HostRow],
    vms_by_host: &HashMap<String, Vec<VmRow>>,
    req: &ScheduleRequest,
) -> Result<(String, Vec<GpuDevice>), SchedulerError> {
    let mut candidates: Vec<(&HostRow, i32, Vec<GpuInfo>, HostCapacity)> = Vec::new();

    let empty: Vec<VmRow> = Vec::new();
    for host in hosts {
        if !host.healthy {
            continue;
        }

        let vms = vms_by_host.get(&host.id).unwrap_or(&empty);
        let cap = HostCapacity::compute(host, vms);

        if cap.available_cpu < req.cpu
            || cap.available_memory_mib < req.memory_mib
            || cap.available_disk_gib < req.disk_gib
        {
            continue;
        }

        let inventory: Vec<GpuInfo> = parse_owned_json(&host.gpu_inventory, "hosts.gpu_inventory");

        let assigned_pci: Vec<String> = vms
            .iter()
            .flat_map(|vm| {
                let devs: Vec<GpuInfo> =
                    parse_owned_json(&vm.gpu_assignments, "vms.gpu_assignments");
                devs.into_iter().map(|g| g.pci_address)
            })
            .collect();

        let available_gpus: Vec<GpuInfo> = inventory
            .into_iter()
            .filter(|g| !assigned_pci.contains(&g.pci_address))
            .collect();

        if req.gpus > 0 {
            if (available_gpus.len() as u32) < req.gpus {
                continue;
            }

            let (score, selected) =
                gpu_topology_score(&available_gpus, req.gpus, req.min_group_size);
            if selected.is_empty() {
                continue;
            }

            candidates.push((host, score, selected, cap));
        } else {
            candidates.push((host, 0, Vec::new(), cap));
        }
    }

    if candidates.is_empty() {
        return Err(SchedulerError::NoCapacity(format!(
            "cpu={}, mem={}MiB, disk={}GiB, gpus={}",
            req.cpu, req.memory_mib, req.disk_gib, req.gpus
        )));
    }

    // Sort: highest GPU topology score first, then best-fit bin-packing (least remaining capacity)
    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1).then_with(|| {
            let rem_a = remaining_after_place(a.3, req);
            let rem_b = remaining_after_place(b.3, req);
            rem_a.cmp(&rem_b)
        })
    });

    let (host, _score, selected_gpus, _) = &candidates[0];
    let gpu_devices: Vec<GpuDevice> = selected_gpus
        .iter()
        .map(|g| GpuDevice {
            pci_address: g.pci_address.clone(),
            model: g.model.clone(),
            iommu_group: g.iommu_group.clone(),
            nvlink_group: g.nvlink_group,
        })
        .collect();

    Ok((host.id.clone(), gpu_devices))
}

fn remaining_after_place(cap: HostCapacity, req: &ScheduleRequest) -> u64 {
    let cpu_rem = cap.available_cpu.saturating_sub(req.cpu) as u64;
    let mem_rem = cap.available_memory_mib.saturating_sub(req.memory_mib) as u64;
    cpu_rem + mem_rem
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
            if *group_id != 0
                && gpus.len() >= count
                && gpus.len() >= min_group_size as usize
            {
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

    fn make_host(id: &str, cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: format!("{id}.local"),
            total_cpu: cpu,
            total_memory_mib: mem,
            total_disk_gib: disk,
            gpu_inventory: serde_json::to_string(gpus).unwrap(),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
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
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus,
            min_group_size: min_group,
        }
    }

    /// Build a VM that consumes `(cpu, mem, disk)` with the given PCI
    /// addresses already bound to it. Only the fields the scheduler reads
    /// are populated.
    fn vm_on(host_id: &str, cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> VmRow {
        VmRow {
            id: format!("vm-{host_id}"),
            name: "test".to_string(),
            cluster_id: "c1".to_string(),
            host_id: host_id.to_string(),
            ip_address: "10.0.0.1".to_string(),
            state: 2,
            cpu,
            memory_mib: mem,
            disk_gib: disk,
            gpu_assignments: serde_json::to_string(gpus).unwrap(),
            image: String::new(),
            error_message: String::new(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn test_schedule_basic() {
        let hosts = vec![
            make_host("h1", 8, 16384, 500, &[]),
            make_host("h2", 4, 8192, 200, &[]),
        ];
        let vms = HashMap::new();
        let (host_id, gpus) = schedule(&hosts, &vms, &basic_req(0, 0)).unwrap();
        // h2 is a tighter fit (best-fit bin-packing prefers closest to full)
        assert_eq!(host_id, "h2");
        assert!(gpus.is_empty());
    }

    #[test]
    fn test_schedule_no_capacity() {
        let hosts = vec![make_host("h1", 2, 4096, 50, &[])];
        let vms = HashMap::new();
        let req = ScheduleRequest {
            cpu: 8,
            memory_mib: 16384,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };
        assert!(schedule(&hosts, &vms, &req).is_err());
    }

    #[test]
    fn test_schedule_with_gpus() {
        let gpus = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let vms = HashMap::new();
        let (host_id, selected) = schedule(&hosts, &vms, &basic_req(2, 2)).unwrap();
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

        let vms = HashMap::new();
        let (host_id, _) = schedule(&hosts, &vms, &basic_req(0, 0)).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_all_unhealthy_returns_error() {
        let mut hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        hosts[0].healthy = false;

        let vms = HashMap::new();
        assert!(schedule(&hosts, &vms, &basic_req(0, 0)).is_err());
    }

    #[test]
    fn test_schedule_empty_hosts_returns_error() {
        let hosts = vec![];
        let vms = HashMap::new();
        let req = ScheduleRequest {
            cpu: 1,
            memory_mib: 1024,
            disk_gib: 10,
            gpus: 0,
            min_group_size: 0,
        };
        assert!(schedule(&hosts, &vms, &req).is_err());
    }

    #[test]
    fn test_schedule_bin_packing_prefers_tightest_fit() {
        let hosts = vec![
            make_host("big", 64, 262144, 2000, &[]),
            make_host("medium", 16, 32768, 500, &[]),
            make_host("small", 8, 16384, 200, &[]),
        ];
        let vms = HashMap::new();
        let (host_id, _) = schedule(&hosts, &vms, &basic_req(0, 0)).unwrap();
        assert_eq!(host_id, "small");
    }

    #[test]
    fn test_schedule_insufficient_disk_skips_host() {
        let hosts = vec![
            make_host("h1", 16, 65536, 50, &[]),
            make_host("h2", 16, 65536, 200, &[]),
        ];
        let vms = HashMap::new();
        let (host_id, _) = schedule(&hosts, &vms, &basic_req(0, 0)).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_gpu_not_enough_available() {
        let gpus = vec![gpu("0000:41:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let vms = HashMap::new();
        assert!(schedule(&hosts, &vms, &basic_req(2, 0)).is_err());
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
        let vms = HashMap::new();
        let (_, selected) = schedule(&hosts, &vms, &basic_req(2, 0)).unwrap();
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
        let vms = HashMap::new();
        assert!(schedule(&hosts, &vms, &basic_req(4, 4)).is_err());
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
        let vms = HashMap::new();
        assert!(schedule(&hosts, &vms, &basic_req(2, 2)).is_err());
    }

    #[test]
    fn test_schedule_skips_assigned_gpus() {
        let all_gpus = vec![gpu("0000:41:00.0", 1), gpu("0000:42:00.0", 1)];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &all_gpus)];

        // One VM on h1 already holds 0000:41:00.0.
        let mut vms = HashMap::new();
        vms.insert(
            "h1".to_string(),
            vec![vm_on("h1", 2, 2048, 10, &[gpu("0000:41:00.0", 1)])],
        );

        // 2 GPUs requested, only 1 free — should fail
        assert!(schedule(&hosts, &vms, &basic_req(2, 0)).is_err());

        // 1 GPU request should pick the free one
        let (host_id, selected) = schedule(&hosts, &vms, &basic_req(1, 0)).unwrap();
        assert_eq!(host_id, "h1");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].pci_address, "0000:42:00.0");
    }

    #[test]
    fn test_schedule_subtracts_vm_allocations_from_capacity() {
        // h1 has 16 vCPU, a 14-vCPU VM is on it. A request for 4 vCPU must fail.
        let hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        let mut vms = HashMap::new();
        vms.insert("h1".to_string(), vec![vm_on("h1", 14, 8192, 50, &[])]);
        assert!(schedule(&hosts, &vms, &basic_req(0, 0)).is_err()); // basic_req needs 4 cpu
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
        let vms = HashMap::new();
        let (host_id, selected) = schedule(&hosts, &vms, &basic_req(2, 0)).unwrap();
        assert_eq!(host_id, "together");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].nvlink_group, selected[1].nvlink_group);
    }
}
