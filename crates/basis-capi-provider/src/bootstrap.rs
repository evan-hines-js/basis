//! Load CAPI bootstrap data for a `BasisMachine`.
//!
//! CAPI places the rendered cloud-init userdata in a Secret referenced by
//! the owning `Machine` resource's `spec.bootstrap.dataSecretName`. The
//! data is base64-encoded under the key `value`.

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::Api;
use kube::Client;

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("bootstrap secret `{0}` has no `value` field")]
    MissingValue(String),

    #[error("fetching secret `{name}`: {source}")]
    Fetch {
        name: String,
        #[source]
        source: kube::Error,
    },
}

/// Read the `value` key out of the named Secret in `namespace`.
pub async fn load_bootstrap_data(
    client: Client,
    namespace: &str,
    secret_name: &str,
) -> Result<Vec<u8>, BootstrapError> {
    let api: Api<Secret> = Api::namespaced(client, namespace);
    let secret = api
        .get(secret_name)
        .await
        .map_err(|source| BootstrapError::Fetch {
            name: secret_name.to_string(),
            source,
        })?;

    secret
        .data
        .and_then(|mut d| d.remove("value"))
        .map(|ByteString(bytes)| bytes)
        .ok_or_else(|| BootstrapError::MissingValue(secret_name.to_string()))
}
