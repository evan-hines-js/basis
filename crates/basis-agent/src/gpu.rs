use std::path::Path;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GpuInventoryItem {
    pub pci_address: String,
    pub model: String,
    pub iommu_group: String,
    pub nvlink_group: u32,
}

/// Discover GPUs on the host by scanning PCI devices.
pub async fn discover_gpus() -> Result<Vec<GpuInventoryItem>, GpuError> {
    let output = Command::new("lspci")
        .args(["-Dnn"])
        .output()
        .await?;

    if !output.status.success() {
        warn!("lspci failed, returning empty GPU inventory");
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut gpus = Vec::new();

    for line in stdout.lines() {
        // Match NVIDIA/AMD GPU lines: "3D controller" or "VGA compatible controller"
        if (line.contains("3D controller") || line.contains("VGA compatible controller"))
            && (line.contains("NVIDIA") || line.contains("AMD"))
        {
            if let Some(pci_addr) = line.split_whitespace().next() {
                let model = extract_model_name(line);
                let iommu_group = read_iommu_group(pci_addr).await.unwrap_or_default();

                gpus.push(GpuInventoryItem {
                    pci_address: pci_addr.to_string(),
                    model,
                    iommu_group,
                    nvlink_group: 0, // TODO: detect NVLink topology via nvidia-smi
                });
            }
        }
    }

    info!(count = gpus.len(), "discovered GPUs");
    Ok(gpus)
}

/// Bind a PCI device to the vfio-pci driver.
pub async fn bind_vfio(pci_address: &str) -> Result<String, GpuError> {
    let sysfs_device = format!("/sys/bus/pci/devices/{pci_address}");

    // Read current driver
    let driver_link = format!("{sysfs_device}/driver");
    if Path::new(&driver_link).exists() {
        // Unbind from current driver
        let current_driver = std::fs::read_link(&driver_link)
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

        if let Some(driver) = &current_driver {
            if driver == "vfio-pci" {
                // Already bound to vfio-pci
                let iommu_group = read_iommu_group(pci_address).await.unwrap_or_default();
                let vfio_path = format!("/dev/vfio/{iommu_group}");
                return Ok(vfio_path);
            }

            let unbind_path = format!("/sys/bus/pci/drivers/{driver}/unbind");
            std::fs::write(&unbind_path, pci_address).map_err(|e| GpuError::UnbindFailed {
                pci_address: pci_address.to_string(),
                reason: e.to_string(),
            })?;
        }
    }

    // Read vendor and device ID for driver_override
    // Set driver_override to vfio-pci
    std::fs::write(
        format!("{sysfs_device}/driver_override"),
        "vfio-pci",
    )
    .map_err(|e| GpuError::BindFailed {
        pci_address: pci_address.to_string(),
        reason: e.to_string(),
    })?;

    // Probe the device to bind to vfio-pci
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

/// Unbind a PCI device from vfio-pci (restore to original driver).
pub async fn unbind_vfio(pci_address: &str) -> Result<(), GpuError> {
    let sysfs_device = format!("/sys/bus/pci/devices/{pci_address}");

    // Clear driver_override
    std::fs::write(format!("{sysfs_device}/driver_override"), "").map_err(|e| {
        GpuError::UnbindFailed {
            pci_address: pci_address.to_string(),
            reason: e.to_string(),
        }
    })?;

    // Unbind from vfio-pci
    let unbind_path = "/sys/bus/pci/drivers/vfio-pci/unbind";
    std::fs::write(unbind_path, pci_address).ok(); // May fail if already unbound

    // Reprobe to bind original driver
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
    Ok(target
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string())
}

fn extract_model_name(lspci_line: &str) -> String {
    // Try to extract the part after the last colon, which is typically the device name
    if let Some(idx) = lspci_line.rfind(": ") {
        lspci_line[idx + 2..].trim().to_string()
    } else {
        "Unknown GPU".to_string()
    }
}
