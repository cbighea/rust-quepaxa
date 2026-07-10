use rust_quepaxa::network::{
    DeploymentId, MutualTlsConfigs, NetworkMetrics, OpenLoopLoadConfig, OpenLoopLoadGenerator,
    TlsIdentity, TlsSubmitClient,
};
use rust_quepaxa::{QuePaxaError, Result};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn parse_client_id(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32 {
        return Err(QuePaxaError::InvalidProposal(
            "client ID must contain 32 hexadecimal characters".into(),
        ));
    }
    let mut id = [0_u8; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| QuePaxaError::InvalidProposal("invalid client ID".into()))?;
    }
    Ok(id)
}

fn read(path: &str, name: &str) -> Result<Vec<u8>> {
    fs::read(path)
        .map_err(|error| QuePaxaError::StorageError(format!("could not read {name}: {error}")))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 14 {
        return Err(QuePaxaError::InvalidProposal(
            "usage: load_generator ADDRESS SERVER_NAME DEPLOYMENT_U128 CLIENT_ID_HEX CLIENT_CERT_DER CLIENT_KEY_PKCS8 CA_DER SERVER_CERT_DER RATE DURATION_SECS MAX_IN_FLIGHT SEED REPORT.json".into(),
        ));
    }
    let address = args[1]
        .parse::<SocketAddr>()
        .map_err(|_| QuePaxaError::InvalidProposal("invalid server address".into()))?;
    let deployment = DeploymentId::from_u128(
        args[3]
            .parse()
            .map_err(|_| QuePaxaError::InvalidProposal("invalid deployment ID".into()))?,
    );
    let identity = TlsIdentity {
        certificate_chain_der: vec![read(&args[5], "client certificate")?],
        private_key_pkcs8_der: read(&args[6], "client private key")?,
    };
    let tls = MutualTlsConfigs::new(&identity, vec![read(&args[7], "CA certificate")?])?;
    let client = TlsSubmitClient::new(
        parse_client_id(&args[4])?,
        deployment,
        address,
        args[2].clone(),
        read(&args[8], "server certificate")?,
        tls.client,
        Arc::new(NetworkMetrics::default()),
    );
    let generator = OpenLoopLoadGenerator::new(
        client,
        OpenLoopLoadConfig {
            requests_per_second: args[9]
                .parse()
                .map_err(|_| QuePaxaError::InvalidProposal("invalid request rate".into()))?,
            duration: Duration::from_secs(
                args[10]
                    .parse()
                    .map_err(|_| QuePaxaError::InvalidProposal("invalid duration".into()))?,
            ),
            max_in_flight: args[11]
                .parse()
                .map_err(|_| QuePaxaError::InvalidProposal("invalid in-flight limit".into()))?,
            seed: args[12]
                .parse()
                .map_err(|_| QuePaxaError::InvalidProposal("invalid seed".into()))?,
        },
    )?;
    generator.run().await?.write_json(PathBuf::from(&args[13]))
}
