//! Startup preconditions.
//!
//! The provider's reconcilers watch three CRDs (`BasisCluster`,
//! `BasisMachine`, `BasisMachineTemplate`). If the pod starts before
//! those CRDs are registered on the apiserver — a real race when the
//! Deployment and CRD manifests are applied concurrently — kube-rs's
//! watcher hits its default exponential backoff and sits silently for
//! up to ~5 minutes per watched kind before retrying. We block startup
//! here until every CRD we manage is listable, so that backoff path
//! can never happen in the first place.
//!
//! We probe by calling `list` on the CRs directly, not on
//! `apiextensions.k8s.io/customresourcedefinitions`. That way the check
//! uses the exact same RBAC the controllers need anyway — no extra
//! cluster role binding required.

use std::time::{Duration, Instant};

use kube::api::{Api, ListParams};
use kube::Client;
use serde::de::DeserializeOwned;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};

const CRD_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const CRD_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Block until every CRD the provider watches is listable, or give up
/// after `CRD_WAIT_TIMEOUT` so the pod crash-loops fast (letting k8s
/// show a clear restart signal) rather than staying up idle.
pub async fn wait_for_crds(client: &Client) -> anyhow::Result<()> {
    let deadline = Instant::now() + CRD_WAIT_TIMEOUT;
    wait_for_crd::<BasisCluster>(client, "BasisCluster", deadline).await?;
    wait_for_crd::<BasisMachine>(client, "BasisMachine", deadline).await?;
    wait_for_crd::<BasisMachineTemplate>(client, "BasisMachineTemplate", deadline).await?;
    Ok(())
}

async fn wait_for_crd<K>(client: &Client, kind: &str, deadline: Instant) -> anyhow::Result<()>
where
    K: kube::Resource<DynamicType = ()> + Clone + DeserializeOwned + std::fmt::Debug,
{
    let api: Api<K> = Api::all(client.clone());
    // `limit=1` keeps the probe cheap — we only need a success/NotFound
    // signal, not the actual contents.
    let params = ListParams::default().limit(1);
    loop {
        match api.list(&params).await {
            Ok(_) => {
                info!(crd = %kind, "CRD registered");
                return Ok(());
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out after {CRD_WAIT_TIMEOUT:?} waiting for {kind} CRD to register; \
                         make sure CRD manifests are applied before (or alongside) this Deployment"
                    );
                }
                warn!(crd = %kind, "CRD not registered yet, retrying");
                sleep(CRD_POLL_INTERVAL).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
