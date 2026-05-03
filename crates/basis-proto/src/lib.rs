#![allow(missing_docs)]

pub mod basis {
    pub mod v1 {
        tonic::include_proto!("basis.v1");
    }
}

pub use basis::v1::*;

pub const PROTOCOL_VERSION: u32 = 1;

impl basis::v1::DiskPurpose {
    /// String form used in DB columns and JSON storage records.
    /// Single conversion site so the vocabulary doesn't drift across
    /// the controller's `pool_disk_assignment.purpose`, the agent's
    /// `local_vms.storage_disks` JSON, or the per-cluster metric
    /// labels.
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Replicated => "replicated",
            Self::GenericData => "generic-data",
        }
    }
}

impl basis::v1::DevicePhysicalHealth {
    /// String form used in DB columns (`host_pool_devices.physical`)
    /// and metric labels. Round-trips with [`Self::from_db_str`].
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::DeviceHealthUnspecified => "Unspecified",
            Self::DeviceHealthReady => "Ready",
            Self::DeviceHealthDegraded => "Degraded",
            Self::DeviceHealthMissing => "Missing",
        }
    }

    /// Inverse of [`Self::as_db_str`]. Unknown values map to
    /// `Unspecified` so a corrupted column doesn't crash the
    /// scheduler.
    pub fn from_db_str(s: &str) -> Self {
        match s {
            "Ready" => Self::DeviceHealthReady,
            "Degraded" => Self::DeviceHealthDegraded,
            "Missing" => Self::DeviceHealthMissing,
            _ => Self::DeviceHealthUnspecified,
        }
    }
}

/// Generated client for GoBGP's gRPC northbound. Vendored from
/// osrg/gobgp v4.4.0 (`proto/api/*.proto`). Package is `api` per
/// GoBGP's proto definitions.
pub mod gobgp {
    tonic::include_proto!("api");
}
