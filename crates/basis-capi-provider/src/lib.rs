//! Cluster API infrastructure provider for Basis.
//!
//! The provider runs inside the management Kubernetes cluster, watches
//! `BasisCluster` and `BasisMachine` CRDs, and translates their lifecycle
//! into gRPC calls to the Basis controller. No state is stored here — the
//! CRDs hold user intent, the Basis controller holds infrastructure truth.

pub mod basis_client;
pub mod bootstrap;
pub mod cluster;
pub mod crds;
pub mod machine;
