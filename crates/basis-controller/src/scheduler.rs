use basis_proto::{CreateMachineRequest, GpuDevice};

use crate::db::HostRow;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("no host can satisfy request: {0}")]
    NoCapacity(String),
}

/// Inventory of a single GPU parsed from the host's JSON gpu_inventory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GpuInfo {
    pub pci_address: String,
    pub model: String,
    pub iommu_group: String,
    pub nvlink_group: u32,
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

/// Pick the best host for a VM request. Returns (host_id, gpu_assignments_json).
pub fn schedule(
    hosts: &[HostRow],
    assigned_vms: &std::collections::HashMap<String, Vec<String>>,
    req: &ScheduleRequest,
) -> Result<(String, Vec<GpuDevice>), SchedulerError> {
    let mut candidates: Vec<(&HostRow, i32, Vec<GpuInfo>)> = Vec::new();

    for host in hosts {
        if !host.healthy {
            continue;
        }
        if (host.available_cpu as u32) < req.cpu
            || (host.available_memory_mib as u32) < req.memory_mib
            || (host.available_disk_gib as u32) < req.disk_gib
        {
            continue;
        }

        let inventory: Vec<GpuInfo> =
            serde_json::from_str(&host.gpu_inventory).unwrap_or_default();
        let assigned_on_host = assigned_vms.get(&host.id);

        let available_gpus: Vec<GpuInfo> = inventory
            .into_iter()
            .filter(|g| {
                if let Some(assigned) = assigned_on_host {
                    !assigned.contains(&g.pci_address)
                } else {
                    true
                }
            })
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

            candidates.push((host, score, selected));
        } else {
            candidates.push((host, 0, Vec::new()));
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
            let remaining_a = remaining_capacity(a.0, req);
            let remaining_b = remaining_capacity(b.0, req);
            remaining_a.cmp(&remaining_b)
        })
    });

    let (host, _score, selected_gpus) = &candidates[0];
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

fn remaining_capacity(host: &HostRow, req: &ScheduleRequest) -> u64 {
    let cpu_rem = host.available_cpu.saturating_sub(req.cpu as i64) as u64;
    let mem_rem = host.available_memory_mib.saturating_sub(req.memory_mib as i64) as u64;
    cpu_rem + mem_rem
}

/// Score GPU topology and return selected GPUs.
/// Returns (score, selected_gpus). Empty selected_gpus means constraint not satisfiable.
///
/// Score: 3 = all GPUs share NVLink, 2 = same NUMA/PCIe switch, 1 = spread across host.
fn gpu_topology_score(
    available: &[GpuInfo],
    count: u32,
    min_group_size: u32,
) -> (i32, Vec<GpuInfo>) {
    let count = count as usize;

    // Group by NVLink domain
    let mut nvlink_groups: std::collections::HashMap<u32, Vec<&GpuInfo>> =
        std::collections::HashMap::new();
    for gpu in available {
        nvlink_groups.entry(gpu.nvlink_group).or_default().push(gpu);
    }

    // Try to find a single NVLink group that satisfies the request
    if min_group_size > 0 {
        // Hard constraint: need min_group_size GPUs in same NVLink domain
        for (_group_id, gpus) in &nvlink_groups {
            if gpus.len() >= count && gpus.len() >= min_group_size as usize {
                let selected: Vec<GpuInfo> = gpus[..count].iter().map(|g| (*g).clone()).collect();
                return (3, selected);
            }
        }
        // Constraint not satisfiable
        return (0, Vec::new());
    }

    // Best-effort: try NVLink group first
    let mut best: Option<(i32, Vec<GpuInfo>)> = None;

    // Score 3: all in one NVLink group
    for (_group_id, gpus) in &nvlink_groups {
        if gpus.len() >= count {
            let selected: Vec<GpuInfo> = gpus[..count].iter().map(|g| (*g).clone()).collect();
            return (3, selected);
        }
    }

    // Score 1: just pick the first N available
    if available.len() >= count {
        let selected: Vec<GpuInfo> = available[..count].to_vec();
        best = Some((1, selected));
    }

    best.unwrap_or((0, Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_host(id: &str, cpu: i64, mem: i64, disk: i64, gpus: &[GpuInfo]) -> HostRow {
        HostRow {
            id: id.to_string(),
            hostname: format!("{id}.local"),
            address: "10.0.0.1".to_string(),
            total_cpu: cpu,
            total_memory_mib: mem,
            total_disk_gib: disk,
            available_cpu: cpu,
            available_memory_mib: mem,
            available_disk_gib: disk,
            gpu_inventory: serde_json::to_string(gpus).unwrap(),
            last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
            healthy: true,
        }
    }

    #[test]
    fn test_schedule_basic() {
        let hosts = vec![
            make_host("h1", 8, 16384, 500, &[]),
            make_host("h2", 4, 8192, 200, &[]),
        ];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        let (host_id, gpus) = schedule(&hosts, &assigned, &req).unwrap();
        // h2 is a tighter fit (best-fit bin-packing prefers closest to full)
        assert_eq!(host_id, "h2");
        assert!(gpus.is_empty());
    }

    #[test]
    fn test_schedule_no_capacity() {
        let hosts = vec![make_host("h1", 2, 4096, 50, &[])];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 8,
            memory_mib: 16384,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());
    }

    #[test]
    fn test_schedule_with_gpus() {
        let gpus = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:42:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "11".to_string(),
                nvlink_group: 1,
            },
        ];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 2,
            min_group_size: 2,
        };

        let (host_id, selected) = schedule(&hosts, &assigned, &req).unwrap();
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

        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        let (host_id, _) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_all_unhealthy_returns_error() {
        let mut hosts = vec![make_host("h1", 16, 65536, 1000, &[])];
        hosts[0].healthy = false;

        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());
    }

    #[test]
    fn test_schedule_empty_hosts_returns_error() {
        let hosts = vec![];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 1,
            memory_mib: 1024,
            disk_gib: 10,
            gpus: 0,
            min_group_size: 0,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());
    }

    #[test]
    fn test_schedule_bin_packing_prefers_tightest_fit() {
        // Three hosts with decreasing capacity. Scheduler should pick the
        // one that's closest to full after placing the VM.
        let hosts = vec![
            make_host("big", 64, 262144, 2000, &[]),
            make_host("medium", 16, 32768, 500, &[]),
            make_host("small", 8, 16384, 200, &[]),
        ];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        let (host_id, _) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(host_id, "small");
    }

    #[test]
    fn test_schedule_insufficient_disk_skips_host() {
        let hosts = vec![
            make_host("h1", 16, 65536, 50, &[]),  // disk too small
            make_host("h2", 16, 65536, 200, &[]),
        ];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 0,
            min_group_size: 0,
        };

        let (host_id, _) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(host_id, "h2");
    }

    #[test]
    fn test_schedule_gpu_not_enough_available() {
        let gpus = vec![GpuInfo {
            pci_address: "0000:41:00.0".to_string(),
            model: "A100".to_string(),
            iommu_group: "10".to_string(),
            nvlink_group: 1,

        }];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];
        let assigned = std::collections::HashMap::new();

        // Asking for 2 GPUs but host only has 1
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 2,
            min_group_size: 0,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());
    }

    #[test]
    fn test_schedule_gpu_prefers_nvlink_group() {
        // Host has 4 GPUs: 2 in NVLink group 1, 2 in NVLink group 2.
        // Requesting 2 GPUs should pick from a single NVLink group.
        let gpus = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:42:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "11".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:81:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "20".to_string(),
                nvlink_group: 2,
            },
            GpuInfo {
                pci_address: "0000:82:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "21".to_string(),
                nvlink_group: 2,
            },
        ];
        let hosts = vec![make_host("h1", 32, 131072, 2000, &gpus)];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 2,
            min_group_size: 0,
        };

        let (_, selected) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(selected.len(), 2);
        // Both GPUs should be from the same NVLink group
        assert_eq!(selected[0].nvlink_group, selected[1].nvlink_group);
    }

    #[test]
    fn test_schedule_gpu_hard_constraint_unsatisfiable() {
        // 4 GPUs, 2 per NVLink group. Hard constraint min_group_size=4
        // is unsatisfiable — no single group has 4.
        let gpus = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:42:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "11".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:81:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "20".to_string(),
                nvlink_group: 2,
            },
            GpuInfo {
                pci_address: "0000:82:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "21".to_string(),
                nvlink_group: 2,
            },
        ];
        let hosts = vec![make_host("h1", 32, 131072, 2000, &gpus)];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 4,
            min_group_size: 4,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());
    }

    #[test]
    fn test_schedule_skips_assigned_gpus() {
        let gpus = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:42:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "11".to_string(),
                nvlink_group: 1,
            },
        ];
        let hosts = vec![make_host("h1", 16, 65536, 1000, &gpus)];

        // One GPU already assigned
        let mut assigned = std::collections::HashMap::new();
        assigned.insert(
            "h1".to_string(),
            vec!["0000:41:00.0".to_string()],
        );

        // Asking for 2 GPUs should fail (only 1 available)
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 2,
            min_group_size: 0,
        };

        assert!(schedule(&hosts, &assigned, &req).is_err());

        // Asking for 1 GPU should succeed
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 1,
            min_group_size: 0,
        };

        let (host_id, selected) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(host_id, "h1");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].pci_address, "0000:42:00.0");
    }

    #[test]
    fn test_schedule_multi_host_gpu_topology_tiebreak() {
        // Two hosts, both have enough GPUs. Host A has GPUs spread across
        // NVLink groups, host B has them all in one. Scheduler should prefer B.
        let gpus_spread = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:81:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "20".to_string(),
                nvlink_group: 2,
            },
        ];
        let gpus_together = vec![
            GpuInfo {
                pci_address: "0000:41:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "10".to_string(),
                nvlink_group: 1,
            },
            GpuInfo {
                pci_address: "0000:42:00.0".to_string(),
                model: "A100".to_string(),
                iommu_group: "11".to_string(),
                nvlink_group: 1,
            },
        ];

        // Same capacity so bin-packing doesn't decide
        let hosts = vec![
            make_host("spread", 16, 65536, 1000, &gpus_spread),
            make_host("together", 16, 65536, 1000, &gpus_together),
        ];
        let assigned = std::collections::HashMap::new();
        let req = ScheduleRequest {
            cpu: 4,
            memory_mib: 8192,
            disk_gib: 100,
            gpus: 2,
            min_group_size: 0,
        };

        let (host_id, selected) = schedule(&hosts, &assigned, &req).unwrap();
        assert_eq!(host_id, "together");
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].nvlink_group, selected[1].nvlink_group);
    }
}
