//! Self-signed CA + leaf cert generation for the integration tests.
//!
//! One CA per test, one leaf per client role. Generated in-memory; no files
//! are written to disk.

use rcgen::{
    Certificate, CertificateParams, DistinguishedName, DnType, KeyPair, KeyUsagePurpose, SanType,
};
use tonic::transport::{Certificate as TonicCert, Identity, ServerTlsConfig};

pub struct TestPki {
    ca_pem: String,
    ca_cert: Certificate,
    ca_key: KeyPair,
    server_cert_pem: String,
    server_key_pem: String,
}

impl TestPki {
    pub fn new(server_san: &str) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name = dn("basis-test-ca");
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec![server_san.to_string()]).unwrap();
        server_params.distinguished_name = dn(server_san);
        server_params.subject_alt_names = vec![SanType::DnsName(server_san.try_into().unwrap())];
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();

        Self {
            ca_pem,
            ca_cert,
            ca_key,
            server_cert_pem: server_cert.pem(),
            server_key_pem: server_key.serialize_pem(),
        }
    }

    pub fn ca_pem(&self) -> &str {
        &self.ca_pem
    }

    pub fn server_tls_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &self.server_cert_pem,
                &self.server_key_pem,
            ))
            .client_ca_root(TonicCert::from_pem(&self.ca_pem))
    }

    /// Issue a leaf cert with the given CN, signed by this PKI's CA.
    /// Returns `(cert_pem, key_pem)`.
    pub fn leaf(&self, cn: &str) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = dn(cn);
        let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

fn dn(cn: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn
}
