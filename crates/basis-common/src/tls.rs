use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::Request;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("reading TLS file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("configuring TLS: {0}")]
    Config(#[from] tonic::transport::Error),

    #[error("parsing peer certificate: {0}")]
    ParseCert(String),

    #[error("no peer certificate presented")]
    NoPeerCert,
}

/// TLS file paths as configured in both the agent's `Host` and the
/// controller's `BasisController` resources.
///
/// Single shared type because both binaries load the same three files
/// (identity cert, identity key, CA root) the same way.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

impl TlsConfig {
    /// Load the three files into an in-memory `TlsIdentity`. The server
    /// and client config helpers below both go through this — file
    /// reading happens in exactly one place.
    pub fn load_identity(&self) -> Result<TlsIdentity, TlsError> {
        Ok(TlsIdentity {
            cert: read(&self.cert)?,
            key: read(&self.key)?,
            ca: read(&self.ca)?,
        })
    }

    pub fn server_config(&self) -> Result<ServerTlsConfig, TlsError> {
        Ok(self.load_identity()?.server_config())
    }

    pub fn client_config(&self, domain_name: &str) -> Result<ClientTlsConfig, TlsError> {
        Ok(self.load_identity()?.client_config(domain_name))
    }
}

/// PEM bytes for a client identity + CA root. Used when TLS material
/// comes from a Kubernetes Secret or another in-memory source rather
/// than a file on disk, so nothing has to stage bytes into temp files
/// to hand them back to tonic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsIdentity {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub ca: Vec<u8>,
}

impl TlsIdentity {
    /// mTLS server config — clients must present a cert chaining to `ca`.
    pub fn server_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(Identity::from_pem(&self.cert, &self.key))
            .client_ca_root(Certificate::from_pem(&self.ca))
    }

    /// Client config. `domain_name` must match a SAN on the server cert.
    pub fn client_config(&self, domain_name: &str) -> ClientTlsConfig {
        ClientTlsConfig::new()
            .identity(Identity::from_pem(&self.cert, &self.key))
            .ca_certificate(Certificate::from_pem(&self.ca))
            .domain_name(domain_name)
    }
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Read {
        path: path.to_path_buf(),
        source,
    })
}

/// Extract the Common Name (CN) from a DER-encoded X.509 certificate.
///
/// The controller uses the CN to decide what role a connection has — agent
/// CNs are hostnames, the CAPI provider CN is a fixed value. Returning
/// `None` means the certificate parsed but had no CN attribute.
pub fn extract_cn(cert_der: &[u8]) -> Result<Option<String>, TlsError> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
        .map_err(|e| TlsError::ParseCert(e.to_string()))?;

    for attr in cert.subject().iter_common_name() {
        if let Ok(s) = attr.as_str() {
            return Ok(Some(s.to_string()));
        }
    }
    Ok(None)
}

/// Pull the CN from the peer certificate attached to a tonic request.
///
/// Returns:
/// - `Ok(Some(cn))`: connection is mTLS and presented a parseable cert.
/// - `Ok(None)`: connection has no TLS info at all (insecure test mode).
///   Callers decide whether to allow this.
/// - `Err(_)`: TLS info was present but the cert was missing or malformed.
pub fn request_peer_cn<T>(req: &Request<T>) -> Result<Option<String>, TlsError> {
    let Some(tls_info) = req.extensions().get::<TlsConnectInfo<TcpConnectInfo>>() else {
        return Ok(None);
    };
    let certs = tls_info.peer_certs().ok_or(TlsError::NoPeerCert)?;
    let first = certs.first().ok_or(TlsError::NoPeerCert)?;
    extract_cn(first.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cn_rejects_garbage() {
        let result = extract_cn(b"not-a-cert");
        assert!(matches!(result, Err(TlsError::ParseCert(_))));
    }
}
