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
    /// Build a tonic server TLS config from the files on disk.
    ///
    /// mTLS is always required — setting a client CA forces clients to present
    /// a certificate that chains to this CA.
    pub fn server_config(&self) -> Result<ServerTlsConfig, TlsError> {
        let cert_pem = read(&self.cert)?;
        let key_pem = read(&self.key)?;
        let ca_pem = read(&self.ca)?;

        Ok(ServerTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .client_ca_root(Certificate::from_pem(ca_pem)))
    }

    /// Build a tonic client TLS config. `domain_name` must match the SAN on
    /// the controller's server certificate.
    pub fn client_config(&self, domain_name: &str) -> Result<ClientTlsConfig, TlsError> {
        let cert_pem = read(&self.cert)?;
        let key_pem = read(&self.key)?;
        let ca_pem = read(&self.ca)?;

        Ok(ClientTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .ca_certificate(Certificate::from_pem(ca_pem))
            .domain_name(domain_name))
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
