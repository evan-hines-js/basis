//! CRD definitions for the Basis CAPI provider.
//!
//! Fields are the minimum needed to drive the Basis API. K8s-level
//! concerns (SSH keys, DNS servers, kube-vip image) live on cluster-wide
//! config in Lattice, not here — Basis just creates VMs.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const API_GROUP: &str = "infrastructure.cluster.x-k8s.io";
pub const API_VERSION: &str = "v1alpha1";

/// Identifies a cluster that maps 1:1 to a Basis-side cluster.
///
/// User input is two fields: the `ipPool` to draw IPs from, and the
/// `credentialsRef` that tells the provider how to reach the basis
/// controller. The `controlPlaneEndpoint` is populated by the
/// reconciler after basis allocates a VIP from the pool's VIP sub-
/// range — per CAPI convention, the infrastructure provider is
/// authoritative for the endpoint and CAPI core propagates it onto
/// `Cluster.spec.controlPlaneEndpoint` for downstream consumers.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "infrastructure.cluster.x-k8s.io",
    version = "v1alpha1",
    kind = "BasisCluster",
    plural = "basisclusters",
    namespaced,
    status = "BasisClusterStatus",
    shortname = "bc"
)]
#[serde(rename_all = "camelCase")]
pub struct BasisClusterSpec {
    /// Name of the Basis IP pool this cluster's VM IPs and VIP are
    /// drawn from.
    pub ip_pool: String,

    /// Reference to the Kubernetes Secret holding this cluster's
    /// basis-controller connection material. The Secret must have keys
    /// `serverUrl`, `cert`, `key`, `ca`; the first is the controller's
    /// gRPC URL, the last three are PEM bytes for the mTLS client
    /// identity whose CN is `basis-capi-provider`.
    pub credentials_ref: CredentialsRef,

    /// Populated by the reconciler after `Basis.CreateCluster` returns.
    /// Never set by the user — if present on first apply, the
    /// reconciler will overwrite it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_plane_endpoint: Option<ControlPlaneEndpoint>,
}

/// Kubernetes-style object reference for a Secret. Namespace is
/// optional — when omitted, it defaults to the `BasisCluster`'s own
/// namespace.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BasisClusterStatus {
    /// Opaque cluster ID returned by `Basis.CreateCluster`. Written once
    /// and used on delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis_cluster_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization: Option<InitializationStatus>,

    #[serde(default)]
    pub ready: bool,

    #[serde(default)]
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InitializationStatus {
    pub provisioned: bool,
}

/// A single VM the Basis provider should create on behalf of CAPI.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "infrastructure.cluster.x-k8s.io",
    version = "v1alpha1",
    kind = "BasisMachine",
    plural = "basismachines",
    namespaced,
    status = "BasisMachineStatus",
    shortname = "bm"
)]
#[serde(rename_all = "camelCase")]
pub struct BasisMachineSpec {
    pub cpu: u32,
    pub memory_mib: u32,
    pub disk_gib: u32,
    pub image: String,
    #[serde(default)]
    pub gpus: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_constraints: Option<GpuConstraints>,
    /// Set by the provider after CreateMachine succeeds. The JSON field
    /// is `providerID` (not `providerId`) to match the CAPI contract —
    /// CAPI's Machine reconciler reads this exact path off the
    /// infrastructure object to bind a NodeRef.
    #[serde(
        default,
        rename = "providerID",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GpuConstraints {
    #[serde(default)]
    pub min_group_size: u32,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BasisMachineStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization: Option<InitializationStatus>,
    /// Mirrors `spec.providerID`. Kept on status only for observability
    /// (`kubectl get basismachine` printer column); CAPI reads
    /// `spec.providerID`.
    #[serde(
        default,
        rename = "providerID",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_id: Option<String>,
    /// Opaque Basis VM ID used on delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis_vm_id: Option<String>,
    #[serde(default)]
    pub addresses: Vec<MachineAddress>,
    #[serde(default)]
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MachineAddress {
    #[serde(rename = "type")]
    pub kind: String,
    pub address: String,
}

/// Minimal, read-only stand-in for CAPI's `cluster.x-k8s.io/v1beta2` `Machine`.
///
/// We only need `spec.bootstrap.dataSecretName` — defining a local shim
/// avoids a dependency on cluster-api-rs and dynamic resolution.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "cluster.x-k8s.io",
    version = "v1beta2",
    kind = "Machine",
    plural = "machines",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct CapiMachineSpec {
    pub bootstrap: CapiBootstrap,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CapiBootstrap {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_secret_name: Option<String>,
}

/// A template from which CAPI's MachineDeployment stamps out identical
/// BasisMachines.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "infrastructure.cluster.x-k8s.io",
    version = "v1alpha1",
    kind = "BasisMachineTemplate",
    plural = "basismachinetemplates",
    namespaced,
    shortname = "bmt"
)]
#[serde(rename_all = "camelCase")]
pub struct BasisMachineTemplateSpec {
    pub template: BasisMachineTemplateResource,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BasisMachineTemplateResource {
    pub spec: BasisMachineSpec,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_transition_time: String,
    /// `metadata.generation` at the time this condition was set. Consumers
    /// use this to tell whether the condition reflects the current spec.
    /// CAPI v1beta2 conditions carry this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}
