use std::collections::HashMap;
use std::path::Path;

use basis_common::gpu::GpuInfo;
use tokio::process::Command;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum GpuError {
    #[error("vfio bind failed for {pci_address}: {reason}")]
    BindFailed {
        pci_address: String,
        reason: String,
    },

    #[error("vfio unbind failed for {pci_address}: {reason}")]
    UnbindFailed {
        pci_address: String,
        reason: String,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Discover GPUs on the host and their NVLink topology.
///
/// Uses `lspci` for PCI inventory and `nvidia-smi topo -m` for NVLink
/// grouping. If `nvidia-smi` is unavailable (AMD-only hosts or driver not
/// installed), GPUs are returned with `nvlink_group = 0`.
pub async fn discover_gpus() -> Result<Vec<GpuInfo>, GpuError> {
    let output = Command::new("lspci").args(["-Dnn"]).output().await?;

    if !output.status.success() {
        warn!("lspci failed, returning empty GPU inventory");
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut gpus = Vec::new();

    for line in stdout.lines() {
        // NVIDIA/AMD compute accelerators present as "3D controller" or
        // "VGA compatible controller" in lspci.
        if !(line.contains("3D controller") || line.contains("VGA compatible controller")) {
            continue;
        }
        if !(line.contains("NVIDIA") || line.contains("AMD")) {
            continue;
        }

        let Some(pci_addr) = line.split_whitespace().next() else {
            continue;
        };

        let model = extract_model_name(line);
        let iommu_group = read_iommu_group(pci_addr).await.unwrap_or_default();

        gpus.push(GpuInfo {
            pci_address: pci_addr.to_string(),
            model,
            iommu_group,
            nvlink_group: 0,
        });
    }

    // Overlay NVLink topology from nvidia-smi.
    match nvlink_groups_from_nvidia_smi().await {
        Ok(groups) => {
            for gpu in &mut gpus {
                if let Some(&group) = groups.get(&gpu.pci_address.to_lowercase()) {
                    gpu.nvlink_group = group;
                }
            }
        }
        Err(e) => {
            info!(reason = %e, "nvidia-smi topology unavailable; leaving nvlink_group=0");
        }
    }

    info!(count = gpus.len(), "discovered GPUs");
    Ok(gpus)
}

/// Bind a PCI device to the vfio-pci driver.
pub async fn bind_vfio(pci_address: &str) -> Result<String, GpuError> {
    let sysfs_device = format!("/sys/bus/pci/devices/{pci_address}");

    let driver_link = format!("{sysfs_device}/driver");
    if Path::new(&driver_link).exists() {
        let current_driver = std::fs::read_link(&driver_link)
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

        if let Some(driver) = &current_driver {
            if driver == "vfio-pci" {
                // Already bound — idempotent path.
                let iommu_group = read_iommu_group(pci_address).await.unwrap_or_default();
                return Ok(format!("/dev/vfio/{iommu_group}"));
            }

            let unbind_path = format!("/sys/bus/pci/drivers/{driver}/unbind");
            std::fs::write(&unbind_path, pci_address).map_err(|e| GpuError::UnbindFailed {
                pci_address: pci_address.to_string(),
                reason: e.to_string(),
            })?;
        }
    }

    std::fs::write(format!("{sysfs_device}/driver_override"), "vfio-pci").map_err(|e| {
        GpuError::BindFailed {
            pci_address: pci_address.to_string(),
            reason: e.to_string(),
        }
    })?;

    std::fs::write("/sys/bus/pci/drivers_probe", pci_address).map_err(|e| {
        GpuError::BindFailed {
            pci_address: pci_address.to_string(),
            reason: e.to_string(),
        }
    })?;

    let iommu_group = read_iommu_group(pci_address).await.unwrap_or_default();
    let vfio_path = format!("/dev/vfio/{iommu_group}");

    info!(pci_address, vfio_path = %vfio_path, "bound GPU to vfio-pci");
    Ok(vfio_path)
}

/// Unbind a PCI device from vfio-pci (restoring it to its original driver).
pub async fn unbind_vfio(pci_address: &str) -> Result<(), GpuError> {
    let sysfs_device = format!("/sys/bus/pci/devices/{pci_address}");

    std::fs::write(format!("{sysfs_device}/driver_override"), "").map_err(|e| {
        GpuError::UnbindFailed {
            pci_address: pci_address.to_string(),
            reason: e.to_string(),
        }
    })?;

    // NotFound means the device wasn't bound to vfio-pci — benign during
    // restart or when the BIOS driver already released it. Anything else
    // is sysfs weirdness worth seeing in the logs.
    if let Err(e) = std::fs::write("/sys/bus/pci/drivers/vfio-pci/unbind", pci_address) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(pci_address, error = %e, "vfio-pci unbind failed");
        }
    }

    std::fs::write("/sys/bus/pci/drivers_probe", pci_address).map_err(|e| {
        GpuError::UnbindFailed {
            pci_address: pci_address.to_string(),
            reason: e.to_string(),
        }
    })?;

    info!(pci_address, "unbound GPU from vfio-pci");
    Ok(())
}

async fn read_iommu_group(pci_address: &str) -> Result<String, GpuError> {
    let link = format!("/sys/bus/pci/devices/{pci_address}/iommu_group");
    let target = std::fs::read_link(&link)?;
    let group = target.file_name().ok_or_else(|| GpuError::BindFailed {
        pci_address: pci_address.to_string(),
        reason: format!("iommu_group symlink has no file name: {}", target.display()),
    })?;
    Ok(group.to_string_lossy().into_owned())
}

fn extract_model_name(lspci_line: &str) -> String {
    // Format: "0000:41:00.0 3D controller [0302]: NVIDIA ... [10de:20b0] ..."
    // The last ": " typically precedes the device name.
    match lspci_line.rfind(": ") {
        Some(idx) => lspci_line[idx + 2..].trim().to_string(),
        None => "Unknown GPU".to_string(),
    }
}

/// Run `nvidia-smi topo -m` and return a map from (lowercased) full PCI
/// address → NVLink group ID.
///
/// GPUs connected by any NVLink link share a group. GPUs with no NVLink
/// edges remain ungrouped and are assigned `0` by the caller.
async fn nvlink_groups_from_nvidia_smi() -> Result<HashMap<String, u32>, GpuError> {
    let topo = Command::new("nvidia-smi").args(["topo", "-m"]).output().await?;
    if !topo.status.success() {
        return Err(GpuError::BindFailed {
            pci_address: String::new(),
            reason: format!(
                "nvidia-smi topo -m failed: {}",
                String::from_utf8_lossy(&topo.stderr)
            ),
        });
    }

    let pci = Command::new("nvidia-smi")
        .args(["--query-gpu=index,pci.bus_id", "--format=csv,noheader"])
        .output()
        .await?;
    if !pci.status.success() {
        return Err(GpuError::BindFailed {
            pci_address: String::new(),
            reason: format!(
                "nvidia-smi --query-gpu failed: {}",
                String::from_utf8_lossy(&pci.stderr)
            ),
        });
    }

    let pci_stdout = String::from_utf8_lossy(&pci.stdout);
    let topo_stdout = String::from_utf8_lossy(&topo.stdout);

    let index_to_pci = parse_index_to_pci(&pci_stdout);
    let edges = parse_nvlink_edges(&topo_stdout, index_to_pci.len());

    Ok(assign_nvlink_groups(&index_to_pci, &edges))
}

/// Parse `index,pci.bus_id` CSV lines from nvidia-smi into index→PCI map.
///
/// `pci.bus_id` is already in the full `0000:41:00.0` form but may be
/// uppercase. We lowercase it to match `lspci -D` output.
fn parse_index_to_pci(csv: &str) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    for line in csv.lines() {
        let parts: Vec<&str> = line.split(',').map(str::trim).collect();
        if parts.len() < 2 {
            continue;
        }
        let Ok(idx) = parts[0].parse::<u32>() else {
            continue;
        };
        map.insert(idx, parts[1].to_lowercase());
    }
    map
}

/// Parse the `nvidia-smi topo -m` matrix for NVLink edges.
///
/// The matrix has rows/columns like "GPU0 GPU1 GPU2 ..." and cell values
/// like `NV1`, `NV2`, `PIX`, `SYS`, `X`. Cells matching `NV\d+` indicate an
/// NVLink connection of that count. Returns `(i, j)` pairs with `i < j`.
fn parse_nvlink_edges(matrix: &str, n_gpus: usize) -> Vec<(u32, u32)> {
    let mut edges = Vec::new();
    let lines: Vec<&str> = matrix.lines().collect();

    for line in lines {
        let trimmed = line.trim_start();
        // Rows begin with "GPU<index>".
        let Some(rest) = trimmed.strip_prefix("GPU") else {
            continue;
        };
        let Some((idx_str, row)) = rest.split_once(|c: char| c.is_whitespace()) else {
            continue;
        };
        let Ok(row_idx) = idx_str.parse::<u32>() else {
            continue;
        };
        if (row_idx as usize) >= n_gpus {
            continue;
        }

        // Cells are whitespace-separated. The first `n_gpus` cells are the
        // symmetric GPU-to-GPU matrix; columns beyond that are CPU affinity
        // and NUMA info which we ignore.
        let cells: Vec<&str> = row.split_whitespace().collect();
        for (col_idx, cell) in cells.iter().take(n_gpus).enumerate() {
            let col_idx = col_idx as u32;
            if col_idx <= row_idx {
                // Only emit each edge once, skip the self-diagonal.
                continue;
            }
            if cell.starts_with("NV") && cell[2..].chars().all(|c| c.is_ascii_digit()) {
                edges.push((row_idx, col_idx));
            }
        }
    }
    edges
}

/// Assign every GPU a non-zero NVLink group ID if it has at least one edge,
/// using a union-find over the edge list. Isolated GPUs get group `0`.
fn assign_nvlink_groups(
    index_to_pci: &HashMap<u32, String>,
    edges: &[(u32, u32)],
) -> HashMap<String, u32> {
    let n = index_to_pci.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();

    fn find(parent: &mut [u32], x: u32) -> u32 {
        let mut x = x;
        while parent[x as usize] != x {
            parent[x as usize] = parent[parent[x as usize] as usize];
            x = parent[x as usize];
        }
        x
    }

    for &(a, b) in edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }

    // Identify GPUs that have at least one edge — only these get a
    // real (non-zero) group ID.
    let mut has_edge = vec![false; n];
    for &(a, b) in edges {
        has_edge[a as usize] = true;
        has_edge[b as usize] = true;
    }

    // Canonicalize roots → dense 1-based IDs.
    let mut root_to_id: HashMap<u32, u32> = HashMap::new();
    let mut next_id: u32 = 1;
    let mut result = HashMap::new();
    for i in 0..n as u32 {
        let Some(pci) = index_to_pci.get(&i) else {
            continue;
        };
        if !has_edge[i as usize] {
            // Unconnected GPU — leave at group 0.
            continue;
        }
        let root = find(&mut parent, i);
        let id = *root_to_id.entry(root).or_insert_with(|| {
            let id = next_id;
            next_id += 1;
            id
        });
        result.insert(pci.clone(), id);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_model_name() {
        let line = "0000:41:00.0 3D controller [0302]: NVIDIA Corporation GA100 [A100 SXM4 40GB] [10de:20b0]";
        assert!(extract_model_name(line).contains("A100"));
    }

    #[test]
    fn test_extract_model_name_no_colon_fallback() {
        assert_eq!(extract_model_name("garbage"), "Unknown GPU");
    }

    #[test]
    fn test_parse_index_to_pci() {
        let csv = "0, 00000000:41:00.0\n1, 00000000:42:00.0\n";
        let map = parse_index_to_pci(csv);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&0).unwrap(), "00000000:41:00.0");
    }

    #[test]
    fn test_parse_nvlink_edges_single_cluster() {
        // 2-GPU host, both connected via NV2.
        let matrix = "\
            \tGPU0\tGPU1\tCPU Affinity\n\
            GPU0\t X \tNV2\t0-47\n\
            GPU1\tNV2\t X \t0-47\n";
        let edges = parse_nvlink_edges(matrix, 2);
        assert_eq!(edges, vec![(0, 1)]);
    }

    #[test]
    fn test_parse_nvlink_edges_partial_mesh() {
        // 4 GPUs: 0↔1 and 2↔3 are NVLinked, cross pairs are PIX.
        let matrix = "\
            \tGPU0\tGPU1\tGPU2\tGPU3\n\
            GPU0\t X \tNV4\tPIX\tPIX\n\
            GPU1\tNV4\t X \tPIX\tPIX\n\
            GPU2\tPIX\tPIX\t X \tNV4\n\
            GPU3\tPIX\tPIX\tNV4\t X \n";
        let edges = parse_nvlink_edges(matrix, 4);
        assert!(edges.contains(&(0, 1)));
        assert!(edges.contains(&(2, 3)));
        assert!(!edges.contains(&(0, 2)));
        assert!(!edges.contains(&(1, 3)));
    }

    #[test]
    fn test_assign_nvlink_groups_isolated_gpus_get_zero() {
        let index_to_pci: HashMap<u32, String> = [
            (0, "0000:41:00.0".to_string()),
            (1, "0000:42:00.0".to_string()),
        ]
        .into_iter()
        .collect();
        let groups = assign_nvlink_groups(&index_to_pci, &[]);
        // No edges → no entries in the map → callers treat as group 0.
        assert!(groups.is_empty());
    }

    #[test]
    fn test_assign_nvlink_groups_union() {
        // Edges 0-1, 1-2, 3-4 → groups {0,1,2} and {3,4}.
        let index_to_pci: HashMap<u32, String> = (0..5)
            .map(|i| (i, format!("0000:4{i}:00.0")))
            .collect();
        let edges = vec![(0, 1), (1, 2), (3, 4)];
        let groups = assign_nvlink_groups(&index_to_pci, &edges);

        assert_eq!(groups.len(), 5);
        assert_eq!(groups["0000:40:00.0"], groups["0000:41:00.0"]);
        assert_eq!(groups["0000:41:00.0"], groups["0000:42:00.0"]);
        assert_eq!(groups["0000:43:00.0"], groups["0000:44:00.0"]);
        assert_ne!(groups["0000:40:00.0"], groups["0000:43:00.0"]);
        // All assigned groups are non-zero.
        for &g in groups.values() {
            assert!(g >= 1);
        }
    }
}
