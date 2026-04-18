//! Typed wrapper around the Basis controller's gRPC API.
//!
//! Exists so the reconcilers don't need to juggle tonic channel setup,
//! protobuf types, or TLS details. Responses are unwrapped into simple
//! structs so the reconcilers work with Rust types, not tonic envelopes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use basis_common::tls::TlsConfig;
use basis_proto::{
    basis_client::BasisClient as InnerClient, CreateClusterRequest, CreateMachineRequest,
    DeleteClusterRequest, DeleteMachineRequest, GetClusterRequest, GpuConstraints,
};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

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

    async fn client(&self) -> Result<InnerClient<Channel>, ClientError> {
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

    pub async fn create_cluster(
        &self,
        name: String,
        ip_pool: String,
    ) -> Result<Cluster, ClientError> {
        let mut client = self.client().await?;
        let resp = client
            .create_cluster(CreateClusterRequest { name, ip_pool })
            .await?
            .into_inner();
        Ok(Cluster {
            cluster_id: resp.cluster_id,
            control_plane_endpoint: resp.control_plane_endpoint,
        })
    }

    pub async fn delete_cluster(&self, cluster_id: String) -> Result<(), ClientError> {
        let mut client = self.client().await?;
        client
            .delete_cluster(DeleteClusterRequest { cluster_id })
            .await?;
        Ok(())
    }

    pub async fn get_cluster(&self, cluster_id: String) -> Result<Cluster, ClientError> {
        let mut client = self.client().await?;
        let resp = client
            .get_cluster(GetClusterRequest { cluster_id })
            .await?
            .into_inner();
        Ok(Cluster {
            cluster_id: resp.cluster_id,
            control_plane_endpoint: resp.control_plane_endpoint,
        })
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
        let mut client = self.client().await?;
        let resp = client.create_machine(request).await?.into_inner();
        Ok(CreatedMachine {
            id: resp.id,
            provider_id: resp.provider_id,
            ip_address: resp.ip_address,
        })
    }

    pub async fn delete_machine(&self, id: String) -> Result<(), ClientError> {
        let mut client = self.client().await?;
        client.delete_machine(DeleteMachineRequest { id }).await?;
        Ok(())
    }
}
