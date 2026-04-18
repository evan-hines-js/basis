//! Shared utilities for the Basis controller, agent, and CAPI provider.
//!
//! The rule: anything that would otherwise be duplicated across crates lives
//! here. Anything used by only one crate stays local to that crate.

pub mod gpu;
pub mod resource;
pub mod time;
pub mod tls;
