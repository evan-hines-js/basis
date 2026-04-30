#![allow(missing_docs)]

pub mod basis {
    pub mod v1 {
        tonic::include_proto!("basis.v1");
    }
}

pub use basis::v1::*;

pub const PROTOCOL_VERSION: u32 = 1;

/// Generated client for GoBGP's gRPC northbound. Vendored from
/// osrg/gobgp v4.4.0 (`proto/api/*.proto`). Package is `api` per
/// GoBGP's proto definitions.
pub mod gobgp {
    tonic::include_proto!("api");
}
