//! Runnable two-live-of-three mutual-TLS QuePaxa cluster.
//!
//! The example generates ephemeral development certificates. Production nodes
//! must load independently provisioned certificates and durable file stores.

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rust_quepaxa::network::{
    DeduplicatingSubmissionHandler, DeploymentId, InMemorySubmissionJournal, MutualTlsConfigs,
    NetworkConsensusHandler, NetworkMetrics, NetworkNodeServer, PeerIdentity, SubmissionOutcome,
    TlsIdentity, TlsRecorderClient, TlsSubmitClient,
};
use rust_quepaxa::{
    AllowAllAvailability, Decision, DurableRecorderCore, InMemoryRecorderStore,
    InMemoryRuntimeStore, LaneId, RecorderConfig, ReplicaConfig, ReplicaId, ReplicaRuntimeConfig,
    Result, StateMachine,
};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const NOOP_VALUE: u64 = u64::MAX;

fn identity() -> TlsIdentity {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    TlsIdentity {
        certificate_chain_der: vec![cert.der().to_vec()],
        private_key_pkcs8_der: key_pair.serialize_der(),
    }
}

struct PrintingStateMachine(ReplicaId);

impl StateMachine<u64> for PrintingStateMachine {
    fn execute(&mut self, decision: &Decision<u64>) -> Result<()> {
        if decision.value_ids == [NOOP_VALUE] {
            return Ok(());
        }
        println!(
            "replica {} executes slot {}: {:?}",
            self.0, decision.slot, decision.value_ids
        );
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let deployment = DeploymentId::from_u128(1);
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let client_id = [42; 16];
    let identities = (0..4).map(|_| identity()).collect::<Vec<_>>();
    let roots = identities
        .iter()
        .map(|identity| identity.certificate_chain_der[0].clone())
        .collect::<Vec<_>>();
    let tls = identities
        .iter()
        .map(|identity| MutualTlsConfigs::new(identity, roots.clone()))
        .collect::<Result<Vec<_>>>()?;
    let peers = members
        .iter()
        .enumerate()
        .map(|(index, replica)| {
            (
                identities[index].certificate_chain_der[0].clone(),
                PeerIdentity::Replica(*replica),
            )
        })
        .chain([(
            identities[3].certificate_chain_der[0].clone(),
            PeerIdentity::Client(client_id),
        )])
        .collect::<BTreeMap<_, _>>();
    let reservations = (0..3)
        .map(|_| std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        .collect::<Vec<_>>();
    let addresses = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    drop(reservations);

    let metrics = Arc::new(NetworkMetrics::default());
    let mut shutdown_tokens = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..2 {
        let recorder_clients = members
            .iter()
            .enumerate()
            .map(|(target, recorder)| {
                TlsRecorderClient::new(
                    members[index],
                    *recorder,
                    deployment,
                    addresses[target],
                    "localhost",
                    identities[target].certificate_chain_der[0].clone(),
                    Arc::clone(&tls[index].client),
                    Arc::clone(&metrics),
                )
            })
            .collect::<Vec<_>>();
        // Auto schedules: every node derives the identical epoch schedule
        // from the committed log, so no external leader agreement is needed.
        let runtime_config = ReplicaRuntimeConfig::new(
            members[index],
            LaneId::new(1),
            members.clone(),
            1,
            ReplicaConfig {
                pipeline_len: 4,
                ..ReplicaConfig::default()
            },
            64,
            Duration::ZERO,
        )?
        .with_auto_schedules()?;
        let consensus = NetworkConsensusHandler::new(
            runtime_config,
            recorder_clients,
            InMemoryRuntimeStore::default(),
            PrintingStateMachine(members[index]),
            Arc::clone(&metrics),
        )
        .await?
        .with_noop_value(NOOP_VALUE);
        let submissions =
            DeduplicatingSubmissionHandler::new(consensus, InMemorySubmissionJournal::default());
        let recorder = DurableRecorderCore::new(
            RecorderConfig::new(members[index], members.clone(), 1)?,
            Arc::new(AllowAllAvailability),
            InMemoryRecorderStore::default(),
        )?;
        let server = NetworkNodeServer::bind(
            addresses[index],
            deployment,
            Arc::clone(&tls[index].server),
            peers.clone(),
            recorder,
            submissions,
            Arc::clone(&metrics),
        )
        .await?;
        let shutdown = CancellationToken::new();
        shutdown_tokens.push(shutdown.clone());
        tasks.push(tokio::spawn(server.run(shutdown)));
    }

    let client = TlsSubmitClient::new(
        client_id,
        deployment,
        addresses[0],
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[3].client),
        Arc::clone(&metrics),
    );
    match client.submit(1, vec![1001_u64]).await? {
        SubmissionOutcome::Committed(decision) => {
            println!("client sees committed slot {}", decision.slot)
        }
        outcome => println!("client sees {outcome:?}"),
    }

    for shutdown in shutdown_tokens {
        shutdown.cancel();
    }
    for task in tasks {
        task.await
            .map_err(|error| rust_quepaxa::QuePaxaError::TransportError(error.to_string()))??;
    }
    println!("network metrics: {:?}", metrics.snapshot());
    Ok(())
}
