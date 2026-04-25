#![allow(missing_docs)]

pub mod basis {
    pub mod v1 {
        tonic::include_proto!("basis.v1");
    }
}

pub use basis::v1::*;

pub const PROTOCOL_VERSION: u32 = 1;

/// Generated client for the holo daemon's gRPC northbound interface.
/// Exposed so basis-controller can drive `holod`'s YANG running config
/// via `Commit` requests.
pub mod holo {
    tonic::include_proto!("holo");
}
