//! Typed wrapper around the Basis controller's gRPC API.
//!
//! Exists so the reconcilers don't need to juggle tonic channel setup,
//! protobuf types, or TLS details. Responses are unwrapped into simple
//! structs so the reconcilers work with Rust types, not tonic envelopes.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basis_common::tls::TlsConfig;
use basis_proto::{
    basis_client::BasisClient as InnerClient, CreateClusterRequest, CreateMachineRequest,
    DeleteClusterRequest, DeleteMachineRequest, GpuConstraints,
};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Response};

const CONTROLLER_SAN: &str = "basis-controller";

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

/// A cluster as the capi-provider cares about it — just the two fields
/// the reconciler writes back to the `BasisCluster` status.
pub struct Cluster {
    pub cluster_id: String,
    pub control_plane_endpoint: String,
}

/// A successful `CreateMachine` result, stripped of protobuf envelopes.
pub struct CreatedMachine {
    pub id: String,
    pub provider_id: String,
    pub ip_address: String,
}

pub struct BasisClient {
    endpoint: String,
    tls: TlsConfig,
    /// Cached channel; re-established on demand if it drops.
    channel: Arc<Mutex<Option<Channel>>>,
}

impl BasisClient {
    pub fn new(endpoint: String, cert: PathBuf, key: PathBuf, ca: PathBuf) -> Self {
        Self {
            endpoint,
            tls: TlsConfig { cert, key, ca },
            channel: Arc::new(Mutex::new(None)),
        }
    }

    async fn connected_client(&self) -> Result<InnerClient<Channel>, ClientError> {
        let mut guard = self.channel.lock().await;
        if guard.is_none() {
            let tls = self.tls.client_config(CONTROLLER_SAN)?;
            let channel = Endpoint::from_shared(self.endpoint.clone())
                .map_err(|e| ClientError::Endpoint(e.to_string()))?
                .connect_timeout(Duration::from_secs(10))
                .tls_config(tls)?
                .connect()
                .await?;
            *guard = Some(channel);
        }
        Ok(InnerClient::new(guard.as_ref().unwrap().clone()))
    }

    /// Issue one RPC against the controller. Handles channel reuse,
    /// unwraps the response, and drops the cached channel on errors
    /// that suggest the underlying transport is dead — without this,
    /// a channel that half-closes (controller restart, NAT timeout)
    /// would fail every subsequent call forever.
    async fn call<F, Fut, T>(&self, f: F) -> Result<T, ClientError>
    where
        F: FnOnce(InnerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, tonic::Status>>,
    {
        let client = self.connected_client().await?;
        match f(client).await {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) => {
                if matches!(
                    status.code(),
                    Code::Unavailable | Code::Unknown | Code::DeadlineExceeded
                ) {
                    *self.channel.lock().await = None;
                }
                Err(status.into())
            }
        }
    }

    pub async fn create_cluster(
        &self,
        name: String,
        ip_pool: String,
    ) -> Result<Cluster, ClientError> {
        let resp = self
            .call(|mut c| async move {
                c.create_cluster(CreateClusterRequest { name, ip_pool }).await
            })
            .await?;
        Ok(Cluster {
            cluster_id: resp.cluster_id,
            control_plane_endpoint: resp.control_plane_endpoint,
        })
    }

    pub async fn delete_cluster(&self, cluster_id: String) -> Result<(), ClientError> {
        self.call(|mut c| async move {
            c.delete_cluster(DeleteClusterRequest { cluster_id }).await
        })
        .await?;
        Ok(())
    }

    pub async fn create_machine(
        &self,
        cluster_id: String,
        name: String,
        spec: &crate::crds::BasisMachineSpec,
        bootstrap_data: Vec<u8>,
    ) -> Result<CreatedMachine, ClientError> {
        let request = CreateMachineRequest {
            cluster_id,
            name,
            cpu: spec.cpu,
            memory_mib: spec.memory_mib,
            disk_gib: spec.disk_gib,
            image: spec.image.clone(),
            bootstrap_data,
            gpus: spec.gpus,
            gpu_constraints: spec.gpu_constraints.as_ref().map(|c| GpuConstraints {
                min_group_size: c.min_group_size,
            }),
        };
        let resp = self
            .call(|mut c| async move { c.create_machine(request).await })
            .await?;
        // Server-side CreateMachine guarantees these fields are populated
        // on success. Guard the boundary so an empty value never reaches
        // the reconciler — an empty IP would otherwise be patched into
        // `BasisMachine.status.addresses`, which the Kubernetes API
        // rejects as `status.addresses[0].address: Required value`.
        if resp.id.is_empty() {
            return Err(ClientError::Malformed("CreateMachine returned empty id"));
        }
        if resp.provider_id.is_empty() {
            return Err(ClientError::Malformed("CreateMachine returned empty provider_id"));
        }
        if resp.ip_address.is_empty() {
            return Err(ClientError::Malformed("CreateMachine returned empty ip_address"));
        }
        Ok(CreatedMachine {
            id: resp.id,
            provider_id: resp.provider_id,
            ip_address: resp.ip_address,
        })
    }

    pub async fn delete_machine(&self, id: String) -> Result<(), ClientError> {
        self.call(|mut c| async move { c.delete_machine(DeleteMachineRequest { id }).await })
            .await?;
        Ok(())
    }
}
