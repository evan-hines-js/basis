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
/// User input is just `credentialsRef` (how to reach the basis
/// controller). The tree this cluster joins is *not* encoded here —
/// it's implied by the basis-capi-provider instance doing the
/// reconcile: every cluster the provider creates becomes a child of
/// the cluster the provider itself runs in. See
/// `ProviderContext.parent_cluster_id`.
///
/// `controlPlaneEndpoint`, `treeId`, and `vni` are populated by the
/// reconciler after basis allocates them — per CAPI convention, the
/// infrastructure provider is authoritative for the endpoint and
/// CAPI core propagates it onto `Cluster.spec.controlPlaneEndpoint`
/// for downstream consumers.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "infrastructure.cluster.x-k8s.io",
    version = "v1alpha1",
    kind = "BasisCluster",
    plural = "basisclusters",
    namespaced,
    status = "BasisClusterStatus",
    shortname = "bc",
    category = "cluster-api",
    printcolumn = r#"{"name":"Cluster", "type":"string", "jsonPath":".metadata.labels['cluster\\.x-k8s\\.io/cluster-name']"}"#,
    printcolumn = r#"{"name":"Ready",   "type":"string", "jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Endpoint","type":"string", "jsonPath":".spec.controlPlaneEndpoint.host"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BasisClusterSpec {
    /// Reference to the Kubernetes Secret holding this cluster's
    /// basis-controller connection material.
    pub credentials_ref: CredentialsRef,

    /// Named LAN pool the cluster's external IPs (apiserver VIP and
    /// LoadBalancer Service block) are carved from. Empty / unset →
    /// the cluster's tree CIDR (nested cluster, no LAN exposure;
    /// reachable only from sibling clusters in the same tree). Any
    /// non-empty name must match a pool in the basis controller's
    /// `network.pools[]`; every host carrying the tree advertises
    /// the allocations via the cell BGP reflector with itself as
    /// next-hop, plus a proxy-ARP entry on the underlay so LAN
    /// clients can reach them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_ip_pool: String,

    /// Number of LoadBalancer Service IPs Cilium gets configured
    /// with. Must be a power of two. 0 / unset → cell-wide default.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub external_service_ips: u32,

    /// Populated by the reconciler after `Basis.CreateCluster` returns.
    /// Never set by the user — if present on first apply, the
    /// reconciler will overwrite it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_plane_endpoint: Option<ControlPlaneEndpoint>,

    /// Populated by the reconciler with the CIDR of the cluster's
    /// LoadBalancer Service block (e.g. `10.0.0.224/28`). Lattice
    /// uses this to write the workload cluster's
    /// `CiliumLoadBalancerIPPool`. Empty when the cluster requested
    /// 0 service IPs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service_block_cidr: String,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
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

    /// Tree (trust domain) this cluster belongs to. Observability-only
    /// on the K8s side — consumers that care (Lattice) read it here
    /// to sibling-check across the fleet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tree_id: Option<String>,

    /// VXLAN Network Identifier of the tree. Same purpose as `treeId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vni: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization: Option<InitializationStatus>,

    #[serde(default)]
    pub ready: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,

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
    shortname = "bm",
    category = "cluster-api",
    printcolumn = r#"{"name":"Cluster", "type":"string", "jsonPath":".metadata.labels['cluster\\.x-k8s\\.io/cluster-name']"}"#,
    printcolumn = r#"{"name":"Ready",   "type":"string", "jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"ProviderID", "type":"string", "jsonPath":".spec.providerID"}"#
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
    /// Raw data disks (GiB each) to attach alongside the rootfs. Basis
    /// hands them to the guest unformatted so a Kubernetes storage
    /// operator (Rook/Ceph) can claim and manage them. Order is stable;
    /// the N'th entry becomes `/dev/vd{c,d,...}` in the guest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_disk_gibs: Vec<u32>,
    /// Set by the provider after CreateMachine succeeds. The JSON field
    /// is `providerID` (not `providerId`) to match the CAPI contract.
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
    /// Mirrors `spec.providerID`.
    #[serde(
        default,
        rename = "providerID",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_id: Option<String>,
    /// Opaque Basis VM ID used on delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis_vm_id: Option<String>,
    /// Snapshot of the credentials reference that was used to create
    /// this VM on basis. Persisted on apply so cleanup can issue
    /// `delete_machine` without needing the owning `BasisCluster` to
    /// still exist — a BasisMachine with `basis_vm_id = Some` *must*
    /// also carry these credentials, making tombstone rows on basis
    /// impossible unless this field is hand-stripped from status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_ref: Option<CredentialsRef>,
    /// Tree-side IP assigned to this VM, exposed as the node's
    /// `InternalIP`.
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
    shortname = "bmt",
    category = "cluster-api"
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
