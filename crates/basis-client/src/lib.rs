//! Typed gRPC client for the Basis controller's CAPI-facing API.
//!
//! Callers construct a [`BasisClient`] with a controller endpoint and a
//! TLS identity whose CN is `basis-capi-provider` (see `basis-controller`
//! / `server::require_capi_caller`). Responses are unwrapped into plain
//! Rust structs so callers don't handle tonic envelopes or protobuf
//! types directly.
//!
//! The underlying channel is cached and reused across calls. On transport
//! errors the cache is dropped so the next call reconnects — without
//! this, a half-closed channel (controller restart, NAT timeout) would
//! fail every subsequent RPC forever.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use basis_common::tls::{TlsIdentity, CONTROLLER_IDENTITY};
use basis_proto::{
    basis_client::BasisClient as InnerClient, CreateClusterRequest, CreateMachineRequest,
    DeleteClusterRequest, DeleteMachineRequest, GetClusterRequest, GetMachineRequest,
    GpuConstraints, ListClustersRequest, ListMachinesRequest, ListPoolsRequest, Machine,
    PoolStatus, SetDeviceSchedulingStateRequest, StorageDisk,
};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Response};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("configuring TLS: {0}")]
    Tls(#[from] basis_common::tls::TlsError),

    #[error("invalid controller endpoint: {0}")]
    Endpoint(String),

    #[error("connecting to controller: {0}")]
    Connect(#[from] tonic::transport::Error),

    #[error("controller RPC failed: {0}")]
    Rpc(#[from] tonic::Status),

    #[error("controller returned malformed response: {0}")]
    Malformed(&'static str),
}

impl ClientError {
    /// True when the failure suggests our cached credentials or endpoint
    /// no longer match what the controller will accept — bad PEM, wrong
    /// URL, TLS handshake failure, or an `Unauthenticated`/`PermissionDenied`
    /// RPC. Callers (the CAPI provider's BasisClientCache) drop the
    /// cached entry on `true` so the next reconcile re-reads the Secret.
    /// `Unavailable`/`Unknown`/`DeadlineExceeded` are *not* in this set —
    /// `call()` already drops the underlying channel for those.
    pub fn is_credentials_problem(&self) -> bool {
        match self {
            ClientError::Tls(_) | ClientError::Endpoint(_) | ClientError::Connect(_) => true,
            ClientError::Rpc(status) => matches!(
                status.code(),
                Code::Unauthenticated | Code::PermissionDenied
            ),
            ClientError::Malformed(_) => false,
        }
    }

    /// True when the failure is load-shedding backpressure the caller
    /// should retry rather than alert on. The internal `call()` already
    /// absorbs a retry budget of ~30s on `Unavailable`; this method is
    /// for callers further up the stack (e.g. the CAPI provider) that
    /// need to pick different requeue cadence and log levels for
    /// transient vs real errors. `DeadlineExceeded` counts too: a
    /// missed server-side ceiling is operationally indistinguishable
    /// from "retry the whole call" at the controller layer.
    pub fn is_transient(&self) -> bool {
        match self {
            ClientError::Rpc(status) => {
                matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
            }
            _ => false,
        }
    }

    /// True when the controller reports the target resource doesn't
    /// exist. Idempotent delete paths (CAPI finalizers, tombstone
    /// reconcilers) treat this as success — the goal is "the
    /// resource is gone," and a missing resource is the goal state
    /// regardless of whether *we* removed it. Without this, a
    /// finalizer with a stale `basis_vm_id` (e.g. from a CreateMachine
    /// that rolled back server-side) loops forever.
    pub fn is_not_found(&self) -> bool {
        matches!(self, ClientError::Rpc(s) if s.code() == Code::NotFound)
    }
}

/// A cluster as the caller cares about it. Carries the IDs the caller
/// needs to plumb onto CAPI CRs without exposing proto envelopes.
pub struct Cluster {
    pub cluster_id: String,
    pub control_plane_endpoint: String,
    pub vni: u32,
    /// CIDR of the cluster's overlay (e.g. `10.42.0.0/24`). The
    /// bottom `bridgeReserve` addresses are reserved for per-host
    /// gateway IPs (assigned by the controller as VMs land); the
    /// rest are VM IPs (less the apiserver VIP slot when the cluster
    /// opted into a private apiserver).
    pub cidr: String,
    /// CIDR of the cluster's LoadBalancer Service block (e.g.
    /// `10.0.0.224/28`). Empty when the cluster requested 0 service IPs.
    pub service_block_cidr: String,
    /// Cell BGP route reflector address. Used by basis-capi-provider
    /// to render the per-cluster `CiliumBGPClusterConfig` so k8s
    /// nodes peer with the cell RR. Empty against an older basis
    /// controller that predates the cluster-level BGP RPC fields;
    /// callers fall back to L2-announce in that case.
    pub bgp_reflector_address: String,
    /// Cell ASN. Same provenance as [`Self::bgp_reflector_address`];
    /// `0` against an older controller.
    pub bgp_asn: u32,
}

/// A successful `CreateMachine` result.
pub struct CreatedMachine {
    pub id: String,
    pub provider_id: String,
    pub ip_address: String,
}

/// Non-cluster inputs to `create_machine`. Decoupled from any caller's
/// CRD / YAML / CLI shape — the caller maps into this type.
pub struct MachineRequest {
    pub cluster_id: String,
    pub name: String,
    pub cpu: u32,
    pub memory_mib: u32,
    pub disk_gib: u32,
    pub image: String,
    pub bootstrap_data: Vec<u8>,
    pub gpus: u32,
    pub min_gpu_group_size: Option<u32>,
    /// Per-disk storage requests. Each disk carries:
    ///   - `min_size_gib`: minimum requested capacity.
    ///   - `selector`: label-based pool match (`requires`/`prefers`).
    ///     None == match any pool with capacity.
    ///   - `purpose`: workload role (`REPLICATED` activates hierarchical
    ///     same-cluster anti-affinity; `GENERIC_DATA` doesn't).
    /// Order is stable and becomes the guest virtio-blk enumeration
    /// order after rootfs + cloud-init.
    pub storage_disks: Vec<StorageDisk>,
    /// Optional **host** placement (against host labels). `None` means
    /// no constraint — the scheduler picks any host that fits.
    /// Per-disk pool placement lives on each `StorageDisk.selector`.
    pub placement: Option<basis_proto::LabelSelector>,
}

/// Inputs to `create_cluster`.
pub struct ClusterRequest {
    pub name: String,
    /// Named LAN pool the cluster's external IPs are carved from —
    /// always the LoadBalancer Service block, and the apiserver VIP
    /// too when `apiserver_visibility = ApiserverPublic`. Required:
    /// must match an entry in the controller's `network.pools[]`.
    pub external_ip_pool: String,
    /// Number of LoadBalancer Service IPs this cluster gets, exposed
    /// to Cilium via a CiliumLoadBalancerIPPool. Must be a power of
    /// two so the controller can carve an aligned /N out of the pool.
    /// 0 falls back to the cell-wide default.
    pub external_service_ips: u32,
    /// Where the apiserver VIP lives. `ApiserverPublic` (default) —
    /// from `external_ip_pool`, BGP-advertised cell-wide; safe for
    /// the root mgmt cluster. `ApiserverPrivate` — from the cluster's
    /// CIDR (last usable), reachable only from inside the cluster's
    /// bridge; CAPI access goes through the parent cell's API proxy.
    pub apiserver_visibility: basis_proto::ApiserverVisibility,
    /// Trust-domain identifier; same semantics as the proto field. The
    /// agent maps this to a per-tree Linux VRF on every host, so two
    /// clusters with the same `trust_domain` share a routing table and
    /// can reach each other while clusters with different
    /// `trust_domain`s are isolated. Empty string is its own group
    /// (joins other empty-trust_domain clusters, doesn't merge with
    /// named ones).
    pub trust_domain: String,
}

pub struct BasisClient {
    endpoint: String,
    identity: TlsIdentity,
    /// Cached channel; re-established on demand if it drops.
    channel: Arc<Mutex<Option<Channel>>>,
}

impl BasisClient {
    /// Single constructor. Callers that load PEM from files use
    /// `TlsConfig::load_identity()` and pass the result here.
    pub fn new(endpoint: String, identity: TlsIdentity) -> Self {
        Self {
            endpoint,
            identity,
            channel: Arc::new(Mutex::new(None)),
        }
    }

    async fn connected_client(&self) -> Result<InnerClient<Channel>, ClientError> {
        let mut guard = self.channel.lock().await;
        let channel = match &*guard {
            Some(ch) => ch.clone(),
            None => {
                let tls = self.identity.client_config(CONTROLLER_IDENTITY);
                let channel = Endpoint::from_shared(self.endpoint.clone())
                    .map_err(|e| ClientError::Endpoint(e.to_string()))?
                    .connect_timeout(Duration::from_secs(10))
                    .tls_config(tls)?
                    .connect()
                    .await?;
                *guard = Some(channel.clone());
                channel
            }
        };
        Ok(InnerClient::new(channel))
    }

    /// Issue one RPC against the controller. Handles channel reuse,
    /// unwraps the response, drops the cached channel on transport-level
    /// errors, and retries `Unavailable` with exponential backoff.
    ///
    /// `Unavailable` is the controller's way of signalling "retry me" —
    /// either the controller is momentarily unreachable (channel dead)
    /// or the agent is shedding load (e.g. LVM backpressure in
    /// `handlers::create_vm`). Both are transient by contract, both
    /// idempotent at the server for our RPCs (create by name is
    /// idempotent; delete is a no-op for a missing resource), so a
    /// bounded retry loop turns transient backpressure into a
    /// success-with-latency instead of a failure at the caller.
    ///
    /// Budget: ~30s total (500ms + 1s + 2s + 4s + 8s + 15s cap), 5
    /// attempts. A sustained outage past that still surfaces as an
    /// error so operators see it; momentary blips are absorbed.
    async fn call<F, Fut, T>(&self, f: F) -> Result<T, ClientError>
    where
        F: Fn(InnerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, tonic::Status>>,
    {
        const MAX_ATTEMPTS: u32 = 5;
        let mut delay = Duration::from_millis(500);
        let mut last_err: Option<tonic::Status> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            let client = self.connected_client().await?;
            match f(client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) => {
                    if matches!(
                        status.code(),
                        Code::Unavailable | Code::Unknown | Code::DeadlineExceeded
                    ) {
                        *self.channel.lock().await = None;
                    }
                    if status.code() == Code::Unavailable && attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(15));
                        last_err = Some(status);
                        continue;
                    }
                    return Err(status.into());
                }
            }
        }
        // Unreachable in practice: the loop either returns Ok, returns
        // a non-Unavailable Err, or retries up to MAX_ATTEMPTS and
        // returns the final Unavailable. This arm preserves the last
        // error if the control flow ever changes.
        Err(last_err
            .unwrap_or_else(|| tonic::Status::unavailable("retry budget exhausted"))
            .into())
    }

    pub async fn create_cluster(&self, req: ClusterRequest) -> Result<Cluster, ClientError> {
        let request = CreateClusterRequest {
            name: req.name,
            external_ip_pool: req.external_ip_pool,
            external_service_ips: req.external_service_ips,
            apiserver_visibility: req.apiserver_visibility as i32,
            trust_domain: req.trust_domain,
        };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.create_cluster(request).await }
            })
            .await?;
        Ok(Cluster {
            cluster_id: resp.cluster_id,
            control_plane_endpoint: resp.control_plane_endpoint,
            vni: resp.vni,
            cidr: resp.cidr,
            service_block_cidr: resp.service_block_cidr,
            bgp_reflector_address: resp.bgp_reflector_address,
            bgp_asn: resp.bgp_asn,
        })
    }

    pub async fn delete_cluster(&self, cluster_id: String) -> Result<(), ClientError> {
        let request = DeleteClusterRequest { cluster_id };
        self.call(|mut c| {
            let request = request.clone();
            async move { c.delete_cluster(request).await }
        })
        .await?;
        Ok(())
    }

    pub async fn get_cluster(&self, cluster_id: String) -> Result<Cluster, ClientError> {
        let request = GetClusterRequest { cluster_id };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.get_cluster(request).await }
            })
            .await?;
        Ok(Cluster {
            cluster_id: resp.cluster_id,
            control_plane_endpoint: resp.control_plane_endpoint,
            vni: resp.vni,
            cidr: resp.cidr,
            service_block_cidr: resp.service_block_cidr,
            // `Basis.GetCluster`'s proto Cluster message doesn't
            // carry BGP info — only `CreateCluster` does. Existing
            // BasisClusters keep their reflector/ASN on
            // `BasisClusterStatus`; readers that need the values
            // post-create read from the CR, not from this client.
            bgp_reflector_address: String::new(),
            bgp_asn: 0,
        })
    }

    /// List every cluster the controller knows about. Read-only — use
    /// this for name → id resolution on delete paths where you don't
    /// want `create_cluster`'s create-if-missing side effect.
    pub async fn list_clusters(&self) -> Result<Vec<basis_proto::Cluster>, ClientError> {
        let resp = self
            .call(|mut c| async move { c.list_clusters(ListClustersRequest {}).await })
            .await?;
        Ok(resp.clusters)
    }

    pub async fn create_machine(&self, req: MachineRequest) -> Result<CreatedMachine, ClientError> {
        let request = CreateMachineRequest {
            cluster_id: req.cluster_id,
            name: req.name,
            cpu: req.cpu,
            memory_mib: req.memory_mib,
            disk_gib: req.disk_gib,
            image: req.image,
            bootstrap_data: req.bootstrap_data,
            gpus: req.gpus,
            gpu_constraints: req
                .min_gpu_group_size
                .map(|min_group_size| GpuConstraints { min_group_size }),
            storage_disks: req.storage_disks,
            placement: req.placement,
        };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.create_machine(request).await }
            })
            .await?;
        // Server-side CreateMachine guarantees these fields on success.
        if resp.id.is_empty() {
            return Err(ClientError::Malformed("CreateMachine returned empty id"));
        }
        if resp.provider_id.is_empty() {
            return Err(ClientError::Malformed(
                "CreateMachine returned empty provider_id",
            ));
        }
        if resp.ip_address.is_empty() {
            return Err(ClientError::Malformed(
                "CreateMachine returned empty ip_address",
            ));
        }
        Ok(CreatedMachine {
            id: resp.id,
            provider_id: resp.provider_id,
            ip_address: resp.ip_address,
        })
    }

    /// Fetch the current state of a single machine by id. Used by the
    /// CAPI provider's steady-state reconcile to observe VM health:
    /// `CreateMachine` only reports the outcome of the create call
    /// itself, so a VM that fails *after* creation (guest kernel panic,
    /// cloud-hypervisor crash) is only visible via `GetMachine`.
    pub async fn get_machine(&self, id: String) -> Result<Machine, ClientError> {
        let request = GetMachineRequest { id };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.get_machine(request).await }
            })
            .await?;
        Ok(resp)
    }

    pub async fn delete_machine(&self, id: String) -> Result<(), ClientError> {
        let request = DeleteMachineRequest { id };
        self.call(|mut c| {
            let request = request.clone();
            async move { c.delete_machine(request).await }
        })
        .await?;
        Ok(())
    }

    /// List machines, optionally filtered by cluster. Pass an empty
    /// string for `cluster_id` to list across all clusters.
    pub async fn list_machines(&self, cluster_id: String) -> Result<Vec<Machine>, ClientError> {
        let request = ListMachinesRequest { cluster_id };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.list_machines(request).await }
            })
            .await?;
        Ok(resp.machines)
    }

    /// Pool + per-device telemetry. Empty `host_id` returns every
    /// pool across every host the controller knows about.
    pub async fn list_pools(&self, host_id: String) -> Result<Vec<PoolStatus>, ClientError> {
        let request = ListPoolsRequest { host_id };
        let resp = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.list_pools(request).await }
            })
            .await?;
        Ok(resp.pools)
    }

    /// Set or clear a drain marker on `(host, pool, device)`. `state`
    /// is `enabled` (clear) or `draining` (set).
    pub async fn set_device_scheduling_state(
        &self,
        host_id: String,
        pool: String,
        device_id: String,
        state: String,
        reason: String,
    ) -> Result<(), ClientError> {
        let request = SetDeviceSchedulingStateRequest {
            host_id,
            pool,
            device_id,
            state,
            reason,
        };
        let _ = self
            .call(|mut c| {
                let request = request.clone();
                async move { c.set_device_scheduling_state(request).await }
            })
            .await?;
        Ok(())
    }
}
