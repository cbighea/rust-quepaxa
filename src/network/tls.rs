use crate::error::{QuePaxaError, Result};
use std::sync::Arc;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

const ALPN_PROTOCOL: &[u8] = b"quepaxa/1";

#[derive(Clone)]
pub struct TlsIdentity {
    pub certificate_chain_der: Vec<Vec<u8>>,
    pub private_key_pkcs8_der: Vec<u8>,
}

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsIdentity")
            .field("certificate_chain_len", &self.certificate_chain_der.len())
            .field("private_key_pkcs8_der", &"[redacted]")
            .finish()
    }
}

impl TlsIdentity {
    fn certificate_chain(&self) -> Vec<CertificateDer<'static>> {
        self.certificate_chain_der
            .iter()
            .cloned()
            .map(CertificateDer::from)
            .collect()
    }

    fn private_key(&self) -> PrivateKeyDer<'static> {
        PrivatePkcs8KeyDer::from(self.private_key_pkcs8_der.clone()).into()
    }
}

#[derive(Clone)]
pub struct MutualTlsConfigs {
    pub server: Arc<ServerConfig>,
    pub client: Arc<ClientConfig>,
}

impl MutualTlsConfigs {
    pub fn new(identity: &TlsIdentity, trusted_ca_der: Vec<Vec<u8>>) -> Result<Self> {
        let roots = root_store(trusted_ca_der)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .map_err(|error| {
                QuePaxaError::TransportError(format!(
                    "could not build client certificate verifier: {error}"
                ))
            })?;
        let mut server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.certificate_chain(), identity.private_key())
            .map_err(|error| {
                QuePaxaError::TransportError(format!("invalid server TLS identity: {error}"))
            })?;
        server.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];

        let mut client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(identity.certificate_chain(), identity.private_key())
            .map_err(|error| {
                QuePaxaError::TransportError(format!("invalid client TLS identity: {error}"))
            })?;
        client.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];

        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
        })
    }
}

fn root_store(certificates: Vec<Vec<u8>>) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(CertificateDer::from(certificate))
            .map_err(|error| {
                QuePaxaError::TransportError(format!("invalid trusted CA certificate: {error}"))
            })?;
    }
    if roots.is_empty() {
        return Err(QuePaxaError::TransportError(
            "at least one trusted CA certificate is required".into(),
        ));
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_identity_debug_output_redacts_private_key_material() {
        let identity = TlsIdentity {
            certificate_chain_der: vec![vec![1]],
            private_key_pkcs8_der: vec![91, 92, 93],
        };
        let output = format!("{identity:?}");

        assert!(output.contains("[redacted]"));
        assert!(!output.contains("91"));
        assert!(!output.contains("92"));
        assert!(!output.contains("93"));
    }
}
