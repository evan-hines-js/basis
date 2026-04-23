use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::Request;

/// Fixed peer identity required for connections from the CAPI provider.
/// Matched against the SAN-DNS (preferred) or CN of the client cert by
/// the controller's `require_capi_caller`.
pub const CAPI_PROVIDER_IDENTITY: &str = "basis-capi-provider";

/// SAN the controller's server certificate must carry — both agent and
/// CAPI-provider clients pin it via `client_config(CONTROLLER_IDENTITY)`.
pub const CONTROLLER_IDENTITY: &str = "basis-controller";

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

/// Extract a peer identity string from a DER-encoded X.509 certificate.
///
/// Per RFC 6125 §6.4.4 we prefer a SubjectAltName DNSName over the
/// Common Name. Servers that don't carry a SAN fall back to CN so
/// existing certificates issued before this code was written keep
/// working. The controller uses the returned string to gate roles:
/// agent identities are hostnames, the CAPI provider identity is a
/// fixed value.
///
/// Returns `Ok(None)` only if the certificate parsed cleanly but
/// presented neither a SAN DNSName nor a CN — a misconfiguration the
/// caller treats as authentication failure.
pub fn extract_peer_identity(cert_der: &[u8]) -> Result<Option<String>, TlsError> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der)
        .map_err(|e| TlsError::ParseCert(e.to_string()))?;

    if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
        for name in &san_ext.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(s) = name {
                return Ok(Some((*s).to_string()));
            }
        }
    }

    for attr in cert.subject().iter_common_name() {
        if let Ok(s) = attr.as_str() {
            return Ok(Some(s.to_string()));
        }
    }
    Ok(None)
}

/// Pull the peer identity from the peer certificate attached to a tonic
/// request. See [`extract_peer_identity`] for the SAN-first selection
/// rule.
///
/// - `Ok(Some(id))`: connection is mTLS and presented a parseable cert.
/// - `Ok(None)`: connection has no TLS info at all (insecure test mode).
///   Callers decide whether to allow this.
/// - `Err(_)`: TLS info was present but the cert was missing or malformed.
pub fn request_peer_identity<T>(req: &Request<T>) -> Result<Option<String>, TlsError> {
    let Some(tls_info) = req.extensions().get::<TlsConnectInfo<TcpConnectInfo>>() else {
        return Ok(None);
    };
    let certs = tls_info.peer_certs().ok_or(TlsError::NoPeerCert)?;
    let first = certs.first().ok_or(TlsError::NoPeerCert)?;
    extract_peer_identity(first.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};

    #[test]
    fn extract_peer_identity_rejects_garbage() {
        let result = extract_peer_identity(b"not-a-cert");
        assert!(matches!(result, Err(TlsError::ParseCert(_))));
    }

    fn issue_cert(common_name: Option<&str>, sans: Vec<SanType>) -> Vec<u8> {
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        if let Some(cn) = common_name {
            dn.push(DnType::CommonName, cn);
        }
        params.distinguished_name = dn;
        params.subject_alt_names = sans;
        let key = KeyPair::generate().expect("generate key");
        let cert = params.self_signed(&key).expect("self-sign cert");
        cert.der().to_vec()
    }

    #[test]
    fn extract_peer_identity_prefers_san_dns_over_cn() {
        let der = issue_cert(
            Some("legacy-cn"),
            vec![SanType::DnsName("preferred.example".try_into().unwrap())],
        );
        assert_eq!(
            extract_peer_identity(&der).unwrap().as_deref(),
            Some("preferred.example")
        );
    }

    #[test]
    fn extract_peer_identity_falls_back_to_cn_when_no_san() {
        let der = issue_cert(Some("agent-host"), vec![]);
        assert_eq!(
            extract_peer_identity(&der).unwrap().as_deref(),
            Some("agent-host")
        );
    }

    #[test]
    fn extract_peer_identity_returns_none_when_no_san_and_no_cn() {
        let der = issue_cert(None, vec![]);
        assert_eq!(extract_peer_identity(&der).unwrap(), None);
    }
}
