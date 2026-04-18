use serde::{Deserialize, Serialize};

/// A single GPU as stored in the controller's `hosts.gpu_inventory` JSON
/// and in the agent's discovered inventory.
///
/// The controller scheduler, the agent GPU discovery, and the protobuf
/// `GPUDevice` message all describe the same physical device. This is the
/// canonical in-memory shape; the protobuf type is used only on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuInfo {
    pub pci_address: String,
    pub model: String,
    pub iommu_group: String,
    /// NVLink domain. GPUs with the same non-zero group are connected via
    /// NVLink. `0` means "not connected to NVLink" or "unknown".
    pub nvlink_group: u32,
}
