#![allow(missing_docs)]

pub mod basis {
    pub mod v1 {
        tonic::include_proto!("basis.v1");
    }
}

pub use basis::v1::*;

pub const PROTOCOL_VERSION: u32 = 1;
