# Lattice Integration Design: Basis as an InfraProvider variant

Describes the changes required in `/Users/evanhines/lattice/work/dir/lattice` to add Basis as a supported provider alongside AWS, Docker, OpenStack, and Proxmox. **No code changes have been made yet** — this doc is for review against the existing `InfraProvider` / `LatticeCluster` pattern.

## The Basis API (for context)

Basis exposes a two-level gRPC API. A caller first creates a Cluster (Basis reserves a VIP and binds an IP pool), then creates Machines inside that cluster. Placement — which host, which GPUs, which IP — belongs to Basis's scheduler.

```
CreateCluster(name, ip_pool) -> (cluster_id, control_plane_endpoint)
CreateMachine(cluster_id, name, cpu, mem, disk, image, bootstrap, gpus)
```

The provider authenticates with mTLS; its client cert MUST have `CN=basis-capi-provider` (enforced controller-side).

## Existing pattern we're fitting into

Lattice already splits provider configuration along a well-worn axis:

| Kind | Holds | Example fields |
|------|-------|----------------|
| `InfraProvider` (cluster-scoped, per environment) | Connection info + credentials | `server_url`, `credentials_secret_ref` |
| `LatticeCluster.spec.provider.<kind>` (per cluster) | Cluster topology knobs | `control_plane_endpoint`, `ipv4_pool`, per-VM templates |

The Proxmox integration is the closest analogue — Basis is "Proxmox but the scheduler owns placement," so the Basis `InfraProvider` config stays minimal (no `target_node`, no `pool`, no `allowed_nodes`) and the per-cluster config is close to the bare minimum the Basis gRPC needs to identify a pool.

## File-by-file plan

### 1. `lattice-common/src/crd/types.rs`

Add `Basis` to `InfraProviderType`:

```rust
pub enum InfraProviderType {
    Aws,
    Docker,
    OpenStack,
    Proxmox,
    Basis,          // NEW
}
```

### 2. `lattice-common/src/crd/providers/basis.rs` (new file)

One struct per "side," mirroring the Proxmox split exactly.

```rust
//! Basis provider shapes.
//!
//! Connection info lives on `BasisProviderConfig` (per InfraProvider).
//! Per-cluster knobs live on `BasisConfig` (embedded in the cluster spec).
//! Credentials come from the parent `InfraProvider.spec.credentials_secret_ref`
//! (or ESO-managed `credentials`) — same pattern as AWS / Proxmox.

/// Fields carried on `InfraProvider.spec.basis`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasisProviderConfig {
    /// gRPC endpoint of the Basis controller, e.g. `https://10.0.0.1:7443`.
    pub server_url: String,
}

/// Fields carried on `LatticeCluster.spec.provider.basis`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasisConfig {
    /// Name of a pool configured in the Basis controller. Determines which
    /// subnet this cluster's VIP and VM IPs are drawn from.
    pub ipv4_pool: String,
}
```

**Why so much smaller than `ProxmoxConfig`:**

- `control_plane_endpoint` — Basis reserves the VIP via `CreateCluster`; the provider writes it into `BasisCluster.spec.controlPlaneEndpoint` at reconcile time. Never supplied by the user.
- `source_node` / `template_id` / `snap_name` — Basis uses an OCI or HTTP image reference, not Proxmox templates. Lives on the per-pool `MachineSpec`.
- `target_node` / `pool` / `allowed_nodes` — placement is the scheduler's job. Not expressible.
- `ssh_authorized_keys`, `dns_servers`, `kube_vip_image`, `virtual_ip_network_interface` — K8s-cluster concerns that apply across providers. Belong on the cluster-wide Lattice spec, not per-provider. If they're currently pinned to `ProxmoxConfig`, don't replicate that here — Basis can't fix the whole codebase, but shouldn't extend the bad pattern.
- `vmid_min` / `vmid_max` — Basis assigns opaque UUIDs, not integer VMIDs. N/A.

### 3. `lattice-common/src/crd/infra_provider.rs`

Wire `BasisProviderConfig` into `InfraProviderSpec`:

```rust
pub struct InfraProviderSpec {
    pub provider_type: InfraProviderType,
    // ...
    pub aws: Option<AwsProviderConfig>,
    pub proxmox: Option<ProxmoxProviderConfig>,
    pub openstack: Option<OpenStackProviderConfig>,
    pub basis: Option<BasisProviderConfig>,        // NEW
    // ...
}
```

### 4. `lattice-common/src/crd/cluster.rs` (or wherever `ProviderSpec` lives)

Wire `BasisConfig` into the per-cluster provider spec — same sibling pattern as existing variants.

```rust
pub struct ProviderSpec {
    // ...
    pub proxmox: Option<ProxmoxConfig>,
    pub basis: Option<BasisConfig>,                 // NEW
    // ...
}
```

### 5. Secret schema for `InfraProvider.spec.credentials_secret_ref`

The Secret the provider reads. Follows the same manual-vs-ESO pattern as other providers.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: homelab-basis-credentials
  namespace: lattice-system
type: Opaque
stringData:
  cert: |
    -----BEGIN CERTIFICATE-----
    ...                               # client cert, CN=basis-capi-provider
  key: |
    -----BEGIN PRIVATE KEY-----
    ...
  ca: |
    -----BEGIN CERTIFICATE-----
    ...                               # CA the Basis controller trusts
```

The `basis-capi-provider` Deployment that Lattice emits consumes this via three env vars (matching the CLI flags the provider binary already accepts):

```yaml
env:
  - name: BASIS_CONTROLLER_ENDPOINT
    value: "{{ provider.spec.basis.serverUrl }}"
  - name: BASIS_TLS_CERT
    value: /etc/basis/tls/cert.pem
  - name: BASIS_TLS_KEY
    value: /etc/basis/tls/key.pem
  - name: BASIS_TLS_CA
    value: /etc/basis/tls/ca.pem
volumeMounts:
  - name: credentials
    mountPath: /etc/basis/tls
    readOnly: true
volumes:
  - name: credentials
    secret:
      secretName: homelab-basis-credentials
```

### 6. `lattice-capi/src/provider/basis.rs` (new file)

Shape mirrors `proxmox.rs`. Generates the five manifest kinds Lattice already generates for other providers:

- `Cluster` (cluster.x-k8s.io) — cluster-wide, not Basis-specific
- `BasisCluster` (infrastructure.cluster.x-k8s.io) — `spec.ipPool` from `BasisConfig.ipv4_pool`; `controlPlaneEndpoint` left unset (provider reconciler fills it in)
- `KubeadmControlPlane` — standard, reads the endpoint from `BasisCluster` at runtime
- `BasisMachineTemplate` per control-plane and per worker pool — `cpu/memoryMiB/diskGiB/image/gpus/gpuConstraints` sourced from the per-pool `MachineSpec`
- `MachineDeployment` + `KubeadmConfigTemplate` per worker pool — same as other providers

```rust
#[async_trait]
impl Provider for BasisProvider {
    async fn generate_capi_manifests(
        &self,
        cluster: &LatticeCluster,
        bootstrap: &BootstrapInfo,
    ) -> Result<Vec<CAPIManifest>> {
        let basis = cluster.spec.provider.basis.as_ref()
            .ok_or_else(|| ProviderError::MissingConfig("basis"))?;
        // Emit the five manifest kinds; see proxmox.rs for shape.
        todo!()
    }

    async fn validate_spec(&self, spec: &ProviderSpec) -> Result<()> {
        validate_k8s_version(&spec.kubernetes.version)
    }

    fn required_secrets(&self, cluster: &LatticeCluster) -> Vec<(String, String)> {
        // The provider's mTLS credentials secret, copied into the CAPI
        // provider namespace so the basis-capi-provider Pod can mount it.
        vec![(
            cluster.spec.credentials_secret_name(),
            "capi-basis-system".to_string(),
        )]
    }
}
```

### 7. `lattice-capi/src/provider/mod.rs`

Factory addition:

```rust
mod basis;
pub use basis::BasisProvider;

pub fn create_provider(provider_type: ProviderType, namespace: &str) -> Result<Box<dyn Provider>> {
    match provider_type {
        ProviderType::Aws       => Ok(Box::new(AwsProvider::with_namespace(namespace))),
        ProviderType::Docker    => Ok(Box::new(DockerProvider::with_namespace(namespace))),
        ProviderType::OpenStack => Ok(Box::new(OpenStackProvider::with_namespace(namespace))),
        ProviderType::Proxmox   => Ok(Box::new(ProxmoxProvider::with_namespace(namespace))),
        ProviderType::Basis     => Ok(Box::new(BasisProvider::with_namespace(namespace))),
        ProviderType::Gcp       => Err(Error::provider("GCP provider not yet implemented")),
        ProviderType::Azure     => Err(Error::provider("Azure provider not yet implemented")),
    }
}
```

### 8. GPU fields on `MachineSpec`

If `MachineSpec` (or its per-pool equivalent) doesn't already carry GPU fields, add them — Basis is the first caller that needs them. Other providers ignore these.

```rust
pub struct MachineSpec {
    pub cpu: u32,
    pub memory_mib: u32,
    pub disk_gib: u32,
    pub image: String,
    #[serde(default)]
    pub gpus: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_constraints: Option<GpuConstraints>,
}
```

## What deploying looks like

Two YAMLs, one per environment/cluster boundary.

```yaml
# Applied once per environment
apiVersion: lattice.dev/v1alpha1
kind: InfraProvider
metadata:
  name: homelab-basis
  namespace: lattice-system
spec:
  providerType: basis
  credentialsSecretRef:
    name: homelab-basis-credentials
  basis:
    serverUrl: "https://10.0.0.206:7443"

---
# Applied per cluster
apiVersion: lattice.dev/v1alpha1
kind: LatticeCluster
metadata:
  name: homelab-cluster
  namespace: lattice-system
spec:
  providerRef: homelab-basis     # references InfraProvider by name
  provider:
    kubernetes:
      version: "1.32.0"
    basis:
      ipv4Pool: "default"        # the only Basis-specific per-cluster field
  nodes:
    controlPlane:
      replicas: 1
      machineSpec:
        cpu: 4
        memoryMiB: 8192
        diskGiB: 40
        image: "ghcr.io/evan-hines-js/lattice-node:v1.32.0"
    workerPools:
      - name: default
        replicas: 2
        machineSpec:
          cpu: 4
          memoryMiB: 8192
          diskGiB: 80
          image: "ghcr.io/evan-hines-js/lattice-node:v1.32.0"
```

Compare to the earlier (wrong) version that duplicated `controllerEndpoint` and `credentialsSecretRef` onto every `LatticeCluster`: 10 clusters using the same Basis installation would mean 10 copies of the same connection info. With `InfraProvider` it's defined once.

## Bootstrap sequence (unchanged from API perspective)

1. User applies `InfraProvider` + Secret + `LatticeCluster`.
2. Lattice emits `Cluster` + `BasisCluster` (no endpoint) + `KubeadmControlPlane` + `MachineDeployment`s + `BasisMachineTemplate`s.
3. `basis-capi-provider` reconciler sees the new `BasisCluster`, calls `Basis.CreateCluster(name, ipv4Pool)` → gets `(cluster_id, vip)`.
4. Reconciler patches `BasisCluster.spec.controlPlaneEndpoint = {host: vip, port: 6443}` and `status.basisClusterId = cluster_id`.
5. `KubeadmControlPlane` sees a ready `BasisCluster` and rolls out control-plane `Machine`s.
6. `BasisMachine` reconciler creates each VM via `Basis.CreateMachine(cluster_id, ...)`.
7. First control-plane VM boots; kube-vip claims the VIP; kubeadm init uses it; cluster comes up.

Basis never learns about SSH keys, DNS servers, or kube-vip configuration — those are baked into the bootstrap userdata Lattice already generates the same way it does for Proxmox.

## Test plan

- `lattice-common/src/crd/providers/basis.rs`: serde round-trip, schemars schema stable under rename.
- `lattice-capi/src/provider/basis.rs`: snapshot test of the five generated manifests against a fixture `LatticeCluster` + `InfraProvider` pair.
- `lattice-capi/src/provider/mod.rs`: factory dispatches `ProviderType::Basis` to `BasisProvider`.
- E2E deferred — requires a running Basis controller (out of scope for a Lattice PR).

## Open questions flagged for the reviewer

1. **Status plumbing on `InfraProvider`** — the `last_validated` timestamp implies we should do a quick connectivity check against the Basis controller on reconcile. The existing pattern (AWS, Proxmox) probably already has a `validate_credentials` helper; if so, Basis implements it by opening the gRPC stream, sending a `GetCluster("")` (expected to fail `NotFound`), and treating a `PermissionDenied` or `Unavailable` as a failed validation. If there's no such helper today, skip for v1.
2. **Whether `ProviderType` (in `lattice-capi`) and `InfraProviderType` (in `lattice-common`) are distinct enums** — the exploration summary suggested they might be. If so, both need a `Basis` variant plus a mapping.
3. **Whether the `basis-capi-provider` Deployment itself is emitted by Lattice or installed separately** — Kustomize bundle we already ship in `basis/deploy/`, or Lattice manages the install as part of `InfraProvider` reconciliation. Current Proxmox behavior is the reference.

## PR size estimate

- `lattice-common`: ~50 lines (`BasisProviderConfig` + `BasisConfig` + enum additions)
- `lattice-capi`: ~250 lines of provider + ~100 lines of snapshot tests
- Zero changes to `lattice-operator`, `lattice-agent`, or the bootstrap webhook

Total: ~400 lines, one reviewer-day.
