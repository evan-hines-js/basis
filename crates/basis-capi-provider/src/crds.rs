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
/// User input is `credentialsRef` (how to reach the basis controller),
/// `externalIpPool` (where the LB Service block — and the apiserver
/// VIP, if public — comes from), and `apiserverVisibility` (cell-public
/// vs cluster-private).
///
/// Trust-domain assignment is NOT a user-facing field: every
/// `BasisCluster` inherits its trust_domain from the provider
/// instance that creates it. The provider derives it on startup as
/// the SHA-256 of the shared `lattice-system/lattice-ca` Secret —
/// every cluster Lattice distributes its CA to gets the same value,
/// so a parent cluster and every child it spawns share one identifier
/// without any installer plumbing. Same pattern lattice-istio uses
/// for its mesh trust domain. Two clusters under the same Lattice
/// root share a tree and can talk; clusters under different roots
/// land in different per-tree VRFs and are isolated at the kernel
/// routing level. Operators never type "trust domain."
///
/// `controlPlaneEndpoint`, `vni`, and `cidr` are populated by the
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

    /// Named LAN pool the cluster's external IPs come from — the
    /// LoadBalancer Service block always, and the apiserver VIP when
    /// `apiserverVisibility = Public`. Required: must match a pool
    /// in the basis controller's `network.pools[]`. Allocations are
    /// BGP-advertised cell-wide.
    pub external_ip_pool: String,

    /// Number of LoadBalancer Service IPs Cilium gets configured
    /// with. Must be a power of two. 0 / unset → cell-wide default.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub external_service_ips: u32,

    /// Where the apiserver VIP lives. `Public` (default) — from
    /// `externalIpPool`, BGP-advertised cell-wide; safe for the root
    /// mgmt cluster. `Private` — from the cluster's CIDR (last
    /// usable), reachable only from inside the cluster's bridge.
    #[serde(default)]
    pub apiserver_visibility: ApiserverVisibility,

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

/// Where the apiserver VIP for this cluster lives.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ApiserverVisibility {
    /// Apiserver VIP from `externalIpPool`, BGP-advertised cell-wide.
    /// Required for the root mgmt cluster (operator's `lattice
    /// install` needs direct LAN reachability).
    #[default]
    Public,
    /// Apiserver VIP at the last usable address of the cluster's
    /// CIDR; never advertised. CAPI access goes through the parent
    /// cell's lattice-operator API proxy over the agent's
    /// reverse-tunneled gRPC stream.
    Private,
}

impl From<ApiserverVisibility> for basis_proto::ApiserverVisibility {
    fn from(v: ApiserverVisibility) -> Self {
        match v {
            ApiserverVisibility::Public => Self::ApiserverPublic,
            ApiserverVisibility::Private => Self::ApiserverPrivate,
        }
    }
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

    /// VXLAN Network Identifier of the cluster's overlay.
    /// Observability — Lattice reads this to render dashboards and
    /// reason about the fabric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vni: Option<u32>,

    /// CIDR of the cluster's overlay (e.g. `10.42.0.0/24`). Same
    /// observability use as `vni`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cidr: Option<String>,

    /// Cell BGP route reflector address. Every k8s node in this
    /// cluster peers with this RR (`bgpControlPlane.enabled=true`
    /// in the Cilium chart variant for basis); the per-cluster
    /// `CiliumBGPClusterConfig` + `CiliumBGPAdvertisement` CRDs are
    /// rendered from this + [`Self::bgp_asn`]. Returned by
    /// `Basis.CreateCluster` and stamped onto the BasisCluster
    /// status so consumers (the bootstrap bundle, downstream
    /// reconcilers) can read the value without re-querying the
    /// basis-controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bgp_reflector_address: Option<String>,

    /// Cell ASN. Single ASN cell-wide; same provenance as
    /// [`Self::bgp_reflector_address`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bgp_asn: Option<u32>,

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
    /// hands them to the guest unformatted; an in-cluster CSI driver
    /// (Rook, Longhorn, Mayastor, OpenEBS LocalPV, …) claims and
    /// manages them. Order is stable; the N'th entry becomes
    /// `/dev/vd{c,d,...}` in the guest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_disk_gibs: Vec<u32>,
    /// Optional placement constraints. `requires` is a hard filter
    /// (CreateMachine fails if no host matches); `prefers` is a soft
    /// score added to the scheduler's tiebreak. Default empty: pick
    /// any host that fits, same as today.
    #[serde(default, skip_serializing_if = "PlacementSpec::is_empty")]
    pub placement: PlacementSpec,
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

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<PlacementRequirement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefers: Vec<PlacementPreference>,
}

impl PlacementSpec {
    fn is_empty(&self) -> bool {
        self.requires.is_empty() && self.prefers.is_empty()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementRequirement {
    pub key: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementPreference {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub weight: u32,
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
