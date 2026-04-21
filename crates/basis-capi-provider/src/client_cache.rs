//! Per-cluster `BasisClient` cache, keyed by `credentialsRef`.
//!
//! Each `BasisCluster` names a Secret that holds its basis-controller
//! endpoint plus mTLS material. Rather than reading and re-connecting on
//! every reconcile call, we cache the constructed client keyed by
//! `(namespace, name)`. On RPC errors that suggest a bad identity
//! (transport/auth), callers invalidate the entry so the next reconcile
//! re-reads the Secret.
//!
//! Secret shape (all four keys required):
//!   - `serverUrl` — controller gRPC URL
//!   - `cert`, `key`, `ca` — PEM bytes; client CN must be `basis-capi-provider`
//!
//! This is the `basis` analogue of how every other CAPI provider reads
//! per-cluster credentials (CAPMOX, CAPA, CAPZ). It's what lets the
//! provider Pod start with zero credentials and pick them up at
//! reconcile-time.

use std::collections::HashMap;
use std::sync::Arc;

use basis_client::BasisClient;
use basis_common::tls::TlsIdentity;
use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;
use kube::Client;
use tokio::sync::Mutex;

use crate::crds::CredentialsRef;

/// A `(namespace, name)` key identifying a credentials Secret.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SecretKey {
    namespace: String,
    name: String,
}

impl SecretKey {
    fn resolve(credentials_ref: &CredentialsRef, fallback_namespace: &str) -> Self {
        Self {
            namespace: credentials_ref
                .namespace
                .clone()
                .unwrap_or_else(|| fallback_namespace.to_string()),
            name: credentials_ref.name.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("kube error reading credentials Secret {namespace}/{name}: {source}")]
    Kube {
        namespace: String,
        name: String,
        #[source]
        source: kube::Error,
    },

    #[error("credentials Secret {namespace}/{name} missing required key '{key}'")]
    MissingKey {
        namespace: String,
        name: String,
        key: &'static str,
    },

    #[error("credentials Secret {namespace}/{name} key 'serverUrl' is not valid UTF-8")]
    InvalidServerUrl { namespace: String, name: String },
}

pub struct BasisClientCache {
    kube: Client,
    entries: Mutex<HashMap<SecretKey, Arc<BasisClient>>>,
}

impl BasisClientCache {
    pub fn new(kube: Client) -> Self {
        Self {
            kube,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Look up (or construct and cache) a client for the given ref.
    /// `fallback_namespace` is used when the ref omits its namespace
    /// (mirroring how CAPI provider refs default to the referrer's
    /// namespace).
    pub async fn get(
        &self,
        credentials_ref: &CredentialsRef,
        fallback_namespace: &str,
    ) -> Result<Arc<BasisClient>, CacheError> {
        let key = SecretKey::resolve(credentials_ref, fallback_namespace);

        if let Some(client) = self.entries.lock().await.get(&key).cloned() {
            return Ok(client);
        }

        // Load outside the lock so the kube round-trip doesn't serialize
        // reconciles. A concurrent loader may win the insert — `or_insert`
        // keeps whichever got there first.
        let client = Arc::new(self.load(&key).await?);
        let mut entries = self.entries.lock().await;
        Ok(entries.entry(key).or_insert(client).clone())
    }

    /// Drop the cached entry for a ref so the next `get` re-reads the
    /// Secret. Call this when RPCs fail in ways that suggest the cached
    /// identity is stale (auth errors, channel collapse that survives
    /// the client's own reconnect loop).
    pub async fn invalidate(&self, credentials_ref: &CredentialsRef, fallback_namespace: &str) {
        let key = SecretKey::resolve(credentials_ref, fallback_namespace);
        self.entries.lock().await.remove(&key);
    }

    async fn load(&self, key: &SecretKey) -> Result<BasisClient, CacheError> {
        let api: Api<Secret> = Api::namespaced(self.kube.clone(), &key.namespace);
        let secret = api
            .get(&key.name)
            .await
            .map_err(|source| CacheError::Kube {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                source,
            })?;

        let data = secret.data.unwrap_or_default();
        let endpoint = extract_utf8(&data, "serverUrl", key)?;
        let identity = TlsIdentity {
            cert: extract_bytes(&data, "cert", key)?,
            key: extract_bytes(&data, "key", key)?,
            ca: extract_bytes(&data, "ca", key)?,
        };
        Ok(BasisClient::new(endpoint, identity))
    }
}

fn extract_bytes(
    data: &std::collections::BTreeMap<String, k8s_openapi::ByteString>,
    key: &'static str,
    secret_key: &SecretKey,
) -> Result<Vec<u8>, CacheError> {
    data.get(key)
        .map(|bs| bs.0.clone())
        .ok_or_else(|| CacheError::MissingKey {
            namespace: secret_key.namespace.clone(),
            name: secret_key.name.clone(),
            key,
        })
}

fn extract_utf8(
    data: &std::collections::BTreeMap<String, k8s_openapi::ByteString>,
    key: &'static str,
    secret_key: &SecretKey,
) -> Result<String, CacheError> {
    let bytes = extract_bytes(data, key, secret_key)?;
    String::from_utf8(bytes).map_err(|_| CacheError::InvalidServerUrl {
        namespace: secret_key.namespace.clone(),
        name: secret_key.name.clone(),
    })
}
