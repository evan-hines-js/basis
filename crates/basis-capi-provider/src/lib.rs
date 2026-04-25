//! Cluster API infrastructure provider for Basis.
//!
//! The provider runs inside the management Kubernetes cluster, watches
//! `BasisCluster` and `BasisMachine` CRDs, and translates their lifecycle
//! into gRPC calls to the Basis controller. No state is stored here — the
//! CRDs hold user intent, the Basis controller holds infrastructure truth.

pub mod bootstrap;
pub mod client_cache;
pub mod cluster;
pub mod components;
pub mod conditions;
pub mod crds;
pub mod machine;
pub mod reconciler;
pub mod startup;
