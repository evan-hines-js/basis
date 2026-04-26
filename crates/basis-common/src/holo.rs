//! Thin client for the holo daemon's gRPC northbound.
//!
//! Both basis-controller (cell BGP route reflector) and basis-agent
//! (host BGP speaker) drive their local `holod` instance via this
//! module. The client wraps tonic's generated `NorthboundClient`,
//! tracks connection state across reconnects, and serializes every
//! call through a single mutex so concurrent commits don't interleave
//! transactions on the wire.
//!
//! holod's northbound exchanges YANG-formatted JSON via `Commit`
//! requests. Callers build a JSON document for the running config
//! they want and pass it to [`HolodClient::commit_replace`] —
//! transaction lifecycle (validate → prepare → apply or abort) is
//! handled inside holod, the client just hands it the desired state
//! and unwraps the transaction id.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use basis_proto::holo::{
    commit_request::Operation, data_tree::Data, northbound_client::NorthboundClient, CommitRequest,
    DataTree, Encoding,
};
use serde_json::json;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// How long the client waits for holod's gRPC to come up before
/// declaring it unreachable. holod is started by systemd ahead of
/// any basis process, so a long delay here means holod failed to
/// boot, not a startup race.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connected client to a local holod instance.
#[derive(Clone)]
pub struct HolodClient {
    inner: Arc<Mutex<NorthboundClient<Channel>>>,
}

impl HolodClient {
    /// Connect to holod at `endpoint` (e.g. `http://127.0.0.1:50051`).
    pub async fn connect(endpoint: &str) -> anyhow::Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| anyhow::anyhow!("parsing holod endpoint '{endpoint}': {e}"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "connecting to holod at {endpoint}: {e} — is holod.service running?"
                )
            })?;
        Ok(Self {
            inner: Arc::new(Mutex::new(NorthboundClient::new(channel))),
        })
    }

    /// Replace holod's running config with `payload`. holod runs the
    /// transaction through validate → prepare → apply (or abort on
    /// validate failure); the returned `u32` is the transaction id
    /// for cross-referencing in `holod`'s rollback log.
    pub async fn commit_replace(
        &self,
        payload: &serde_json::Value,
        comment: &str,
    ) -> anyhow::Result<u32> {
        let request = CommitRequest {
            operation: Operation::Replace as i32,
            config: Some(DataTree {
                encoding: Encoding::Json as i32,
                data: Some(Data::DataString(payload.to_string())),
            }),
            comment: comment.to_string(),
            confirmed_timeout: 0,
        };
        let mut client = self.inner.lock().await;
        let resp = client.commit(request).await.map_err(|e| {
            warn!(error = %e, comment, "holod Commit RPC failed");
            anyhow::anyhow!("holod Commit ({comment}): {e}")
        })?;
        let txn = resp.into_inner().transaction_id;
        debug!(transaction_id = txn, comment, "holod commit succeeded");
        Ok(txn)
    }
}

/// Render the YANG JSON document for a single BGP control-plane-protocol
/// instance. Shared between the cell route reflector (many neighbors,
/// no networks) and host BGP speakers (one neighbor, advertised
/// networks). Path keys follow the IETF BGP draft holo's YANG bindings
/// are derived from.
pub fn bgp_running_config(
    instance_name: &str,
    asn: u32,
    router_id: Ipv4Addr,
    neighbors: &[(IpAddr, u32)],
    networks: &[String],
) -> serde_json::Value {
    let neighbors_json: Vec<_> = neighbors
        .iter()
        .map(|(addr, peer_as)| {
            json!({
                "remote-address": addr.to_string(),
                "peer-as": peer_as,
            })
        })
        .collect();
    let networks_json: Vec<_> = networks.iter().map(|p| json!({ "prefix": p })).collect();
    let mut bgp_body = json!({
        "global": {
            "as": asn,
            "identifier": router_id.to_string(),
        },
        "neighbors": { "neighbor": neighbors_json },
    });
    if !networks_json.is_empty() {
        bgp_body["global"]["afi-safis"] = json!({
            "afi-safi": [{
                "name": "iana-bgp-types:ipv4-unicast",
                "ipv4-unicast": {
                    "network-config": { "network": networks_json }
                }
            }]
        });
    }
    json!({
        "ietf-routing:routing": {
            "control-plane-protocols": {
                "control-plane-protocol": [{
                    "type": "ietf-bgp:bgp",
                    "name": instance_name,
                    "ietf-bgp:bgp": bgp_body,
                }]
            }
        }
    })
}
