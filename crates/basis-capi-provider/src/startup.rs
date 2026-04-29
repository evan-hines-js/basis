//! Startup preconditions: block until the CRDs the provider watches are
//! registered, and resolve the trust-domain identifier from cluster state.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, ListParams};
use kube::Client;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};

const CRD_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const CRD_POLL_INTERVAL: Duration = Duration::from_secs(2);

// Cross-repo contract with lattice-common: these three identifiers must match
// `lattice_common::{CA_SECRET, CA_CERT_KEY}` and
// `lattice_core::LATTICE_SYSTEM_NAMESPACE`. Duplicated here (rather than
// shared via a path dep) because basis is a separate repo with its own Docker
// build root — a cross-repo dep would break `docker build .` for any user who
// doesn't have lattice mounted alongside. The values haven't changed in the
// life of the project; if they do, lattice's snapshot test for these
// constants flags it.
const LATTICE_SYSTEM_NAMESPACE: &str = "lattice-system";
const CA_SECRET: &str = "lattice-ca";
const CA_CERT_KEY: &str = "ca.crt";

const CA_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
const CA_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Block until every CRD the provider watches is listable, or give up after
/// `CRD_WAIT_TIMEOUT` so the pod crash-loops fast (letting k8s show a clear
/// restart signal) rather than staying up idle. The watcher in kube-rs would
/// otherwise back off silently for up to ~5 minutes per watched kind on the
/// race where the Deployment lands before its CRDs.
pub async fn wait_for_crds(client: &Client) -> anyhow::Result<()> {
    let deadline = Instant::now() + CRD_WAIT_TIMEOUT;
    wait_for_crd::<BasisCluster>(client, "BasisCluster", deadline).await?;
    wait_for_crd::<BasisMachine>(client, "BasisMachine", deadline).await?;
    wait_for_crd::<BasisMachineTemplate>(client, "BasisMachineTemplate", deadline).await?;
    Ok(())
}

/// Resolve the trust-domain identifier this provider stamps onto every
/// `BasisCluster` it creates: SHA-256 hex of the `lattice-ca` Secret's
/// `ca.crt` PEM bytes.
///
/// Same pattern lattice-istio uses for its mesh trust domain. Two clusters
/// sharing the same `lattice-ca` derive the same identifier — which is what
/// "in the same Lattice tree" means — so a parent cluster and every child it
/// spawns land in the same per-tree VRF on every basis host. No env-var
/// plumbing, no parent-kubeconfig threading: the provider self-discovers
/// from the same shared resource Lattice already distributes.
///
/// Blocks until the Secret appears (with timeout). Lattice's parent-cell
/// install creates it; the bootstrap path applies basis-capi-provider
/// concurrently, so a brief absence at startup is normal.
pub async fn read_trust_domain(client: &Client) -> anyhow::Result<String> {
    let cert_pem = wait_for_lattice_ca(client).await?;
    let mut hasher = Sha256::new();
    hasher.update(cert_pem.as_bytes());
    let digest = hasher.finalize();
    let trust_domain: String = digest.iter().map(|b| format!("{:02x}", b)).collect();
    info!(trust_domain = %trust_domain, "trust domain derived from lattice-ca");
    Ok(trust_domain)
}

async fn wait_for_lattice_ca(client: &Client) -> anyhow::Result<String> {
    let api: Api<Secret> = Api::namespaced(client.clone(), LATTICE_SYSTEM_NAMESPACE);
    let deadline = Instant::now() + CA_WAIT_TIMEOUT;
    loop {
        match api.get_opt(CA_SECRET).await {
            Ok(Some(secret)) => {
                if let Some(bytes) = secret.data.as_ref().and_then(|d| d.get(CA_CERT_KEY)) {
                    if let Ok(pem) = std::str::from_utf8(&bytes.0) {
                        if !pem.is_empty() {
                            return Ok(pem.to_string());
                        }
                    }
                    anyhow::bail!(
                        "{CA_SECRET} Secret is present but {CA_CERT_KEY} is empty \
                         or not valid UTF-8 — Lattice CA install is corrupt"
                    );
                }
                anyhow::bail!(
                    "{CA_SECRET} Secret has no {CA_CERT_KEY} key — Lattice CA \
                     install did not finish writing the cert"
                );
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out after {CA_WAIT_TIMEOUT:?} waiting for \
                         {LATTICE_SYSTEM_NAMESPACE}/{CA_SECRET} Secret; Lattice's parent-cell \
                         install must run before basis-capi-provider can stamp BasisClusters"
                    );
                }
                warn!(secret = %CA_SECRET, namespace = %LATTICE_SYSTEM_NAMESPACE, "lattice-ca not present yet, retrying");
                sleep(CA_POLL_INTERVAL).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trust_domain_is_deterministic_64_hex() {
        let pem = "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJALRiMLAh0TTDMA==\n-----END CERTIFICATE-----\n";
        let mut hasher = Sha256::new();
        hasher.update(pem.as_bytes());
        let td: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        assert_eq!(td.len(), 64);
        assert!(td.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
