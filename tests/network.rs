#![cfg(feature = "network")]

use futures_util::future::BoxFuture;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rust_quepaxa::network::{
    AsyncProposerCore, AsyncRecorderClient, AuthenticatedBatchService, Batch, BatchService,
    BatchServiceLimits, BoxSubmissionFuture, DeduplicatingSubmissionHandler, DeploymentId,
    FnSubmissionHandler, InMemoryBatchStore, InMemorySubmissionJournal, MutualTlsConfigs,
    NetworkConsensusHandler, NetworkMetrics, NetworkNodeServer, PeerIdentity,
    ReproducibleWanHarness, ReproducibleWanProxy, Submission, SubmissionHandler, SubmissionOutcome,
    TlsBatchClient, TlsBatchFetcher, TlsBatchPublisher, TlsIdentity, TlsRecorderClient,
    TlsSubmitClient, WanHarnessLink, WanLinkProfile,
};
use rust_quepaxa::{
    AllowAllAvailability, ClusterIdentity, Decision, EpochSchedule, InMemoryRuntimeStore, LaneId,
    MembershipChange, Priority, Proposal, ProposalKey, RecordReply, RecordRequest, RecorderConfig,
    RecorderCore, ReplicaConfig, ReplicaId, ReplicaRuntimeConfig, Result, SlotIndex, StateMachine,
    Step, ValueFetcher, XorShift64,
};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reproducible_wan_proxy_shapes_and_fails_selected_connections() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let echo = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let target = echo.local_addr().unwrap();
    let echo_task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = echo.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut bytes = [0_u8; 4];
                if stream.read_exact(&mut bytes).await.is_ok() {
                    let _ = stream.write_all(&bytes).await;
                }
            });
        }
    });
    let proxy = ReproducibleWanProxy::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        target,
        WanLinkProfile {
            seed: 42,
            base_delay: Duration::from_millis(2),
            jitter: Duration::from_millis(1),
            fail_every: Some(2),
            ..WanLinkProfile::default()
        },
    )
    .await
    .unwrap();
    let address = proxy.local_addr().unwrap();
    let observer = proxy.observer();
    let shutdown = CancellationToken::new();
    let proxy_task = tokio::spawn(proxy.run(shutdown.clone()));

    let mut first = tokio::net::TcpStream::connect(address).await.unwrap();
    first.write_all(b"ping").await.unwrap();
    let mut response = [0_u8; 4];
    first.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ping");

    let mut second = tokio::net::TcpStream::connect(address).await.unwrap();
    second.write_all(b"fail").await.unwrap();
    let mut byte = [0_u8; 1];
    let outcome = tokio::time::timeout(Duration::from_secs(1), second.read(&mut byte))
        .await
        .unwrap();
    assert!(outcome.is_err() || outcome.unwrap() == 0);

    shutdown.cancel();
    proxy_task.await.unwrap().unwrap();
    let stats = observer.snapshot();
    assert_eq!(stats.accepted_connections, 2);
    assert!(stats.failed_connections >= 1);
    assert!(stats.forwarded_bytes >= 8);
    echo_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_link_wan_harness_returns_machine_readable_stats() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let echo = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let target = echo.local_addr().unwrap();
    let echo_task = tokio::spawn(async move {
        let (mut stream, _) = echo.accept().await.unwrap();
        let mut bytes = [0_u8; 4];
        stream.read_exact(&mut bytes).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    });
    let harness = ReproducibleWanHarness::bind(vec![WanHarnessLink {
        name: "tokyo-to-dublin".into(),
        listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        target,
        profile: WanLinkProfile {
            seed: 17,
            base_delay: Duration::from_millis(1),
            ..WanLinkProfile::default()
        },
    }])
    .await
    .unwrap();
    let address = harness.endpoints()[0].1;
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(harness.run(shutdown.clone()));
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut response = [0_u8; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ping");
    shutdown.cancel();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.links[0].name, "tokyo-to-dublin");
    assert_eq!(report.links[0].seed, 17);
    assert_eq!(report.links[0].stats.accepted_connections, 1);
    assert!(report.links[0].stats.forwarded_bytes >= 8);
    assert!(
        serde_json::to_string(&report)
            .unwrap()
            .contains("forwarded_bytes")
    );
    echo_task.await.unwrap();
}

fn identity() -> TlsIdentity {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    TlsIdentity {
        certificate_chain_der: vec![cert.der().to_vec()],
        private_key_pkcs8_der: key_pair.serialize_der(),
    }
}

struct CountingMachine(Arc<AtomicUsize>);

impl StateMachine<u64> for CountingMachine {
    fn execute(&mut self, decision: &rust_quepaxa::Decision<u64>) -> Result<()> {
        self.0.fetch_add(decision.value_ids.len(), Ordering::AcqRel);
        Ok(())
    }
}

#[derive(Clone)]
struct LocalAsyncRecorder {
    id: ReplicaId,
    core: Arc<Mutex<RecorderCore<u64>>>,
    status_calls: Arc<AtomicUsize>,
}

impl AsyncRecorderClient<u64> for LocalAsyncRecorder {
    fn id(&self) -> ReplicaId {
        self.id
    }

    fn record(&self, request: RecordRequest<u64>) -> BoxFuture<'_, Result<RecordReply<u64>>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            core.lock()
                .map_err(|_| {
                    rust_quepaxa::QuePaxaError::TransportError("test lock poisoned".into())
                })?
                .record(request)
        })
    }

    fn inform_decisions(&self, decisions: Vec<Decision<u64>>) -> BoxFuture<'_, Result<()>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            let mut core = core.lock().map_err(|_| {
                rust_quepaxa::QuePaxaError::TransportError("test lock poisoned".into())
            })?;
            for decision in decisions {
                core.inform_decision(decision)?;
            }
            Ok(())
        })
    }

    fn status(&self, slot: SlotIndex) -> BoxFuture<'_, Result<Option<Step>>> {
        self.status_calls.fetch_add(1, Ordering::AcqRel);
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            Ok(core
                .lock()
                .map_err(|_| {
                    rust_quepaxa::QuePaxaError::TransportError("test lock poisoned".into())
                })?
                .status(slot))
        })
    }

    fn install_membership(&self, change: MembershipChange<u64>) -> BoxFuture<'_, Result<()>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            core.lock()
                .map_err(|_| {
                    rust_quepaxa::QuePaxaError::TransportError("test lock poisoned".into())
                })?
                .install_membership(change)
        })
    }
}

fn local_recorders(members: &[ReplicaId]) -> Vec<LocalAsyncRecorder> {
    members
        .iter()
        .map(|id| LocalAsyncRecorder {
            id: *id,
            core: Arc::new(Mutex::new(
                RecorderCore::permissive_for_tests(*id, members.to_vec()).unwrap(),
            )),
            status_calls: Arc::new(AtomicUsize::new(0)),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorder_rpc_times_out_when_a_connection_accepts_but_never_responds() {
    let local = identity();
    let remote = identity();
    let trusted = vec![
        local.certificate_chain_der[0].clone(),
        remote.certificate_chain_der[0].clone(),
    ];
    let tls = MutualTlsConfigs::new(&local, trusted).unwrap();
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let stalled = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = TlsRecorderClient::new(
        ReplicaId::new(1),
        ReplicaId::new(2),
        DeploymentId::from_u128(7),
        address,
        "localhost",
        remote.certificate_chain_der[0].clone(),
        Arc::clone(&tls.client),
        Arc::new(NetworkMetrics::default()),
    )
    .with_rpc_timeout(Duration::from_millis(30));
    let started = tokio::time::Instant::now();

    let error = client.ping::<u64>().await.unwrap_err();

    assert!(matches!(
        error,
        rust_quepaxa::QuePaxaError::TransportError(_)
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    stalled.abort();
}

struct CheckpointMachine(Arc<AtomicUsize>);

impl StateMachine<u64> for CheckpointMachine {
    fn execute(&mut self, decision: &Decision<u64>) -> Result<()> {
        self.0.fetch_add(decision.value_ids.len(), Ordering::AcqRel);
        Ok(())
    }

    fn export_checkpoint(&mut self, _through: SlotIndex) -> Result<Vec<u8>> {
        Ok(self.0.load(Ordering::Acquire).to_be_bytes().to_vec())
    }

    fn import_checkpoint(&mut self, _through: SlotIndex, checkpoint: &[u8]) -> Result<()> {
        let bytes: [u8; std::mem::size_of::<usize>()] = checkpoint.try_into().map_err(|_| {
            rust_quepaxa::QuePaxaError::StorageError("invalid test checkpoint".into())
        })?;
        self.0.store(usize::from_be_bytes(bytes), Ordering::Release);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_cluster_reaches_quorum_and_deduplicates_client_retries() {
    let deployment = DeploymentId::from_u128(42);
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let cluster = ClusterIdentity::new(members.clone(), 1).unwrap();
    let identities = (0..4).map(|_| identity()).collect::<Vec<_>>();
    let trusted = identities
        .iter()
        .map(|identity| identity.certificate_chain_der[0].clone())
        .collect::<Vec<_>>();
    let tls = identities
        .iter()
        .map(|identity| MutualTlsConfigs::new(identity, trusted.clone()).unwrap())
        .collect::<Vec<_>>();
    let client_id = [9; 16];
    let peer_map = members
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
    let metrics = Arc::new(NetworkMetrics::default());
    let mut addresses = Vec::new();
    let mut shutdown_tokens = Vec::new();
    let mut tasks = Vec::new();

    // Start only two of the three configured recorders. The missing third
    // endpoint exercises n-f quorum progress and cancellation/error handling.
    for index in 0..2 {
        let handler = DeduplicatingSubmissionHandler::new(
            FnSubmissionHandler(
                |_submission: Submission<u64>| -> BoxSubmissionFuture<'static, u64> {
                    Box::pin(async { Ok(SubmissionOutcome::Accepted) })
                },
            ),
            InMemorySubmissionJournal::default(),
        );
        let server = NetworkNodeServer::bind(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            deployment,
            Arc::clone(&tls[index].server),
            peer_map.clone(),
            RecorderCore::permissive_for_tests(members[index], members.clone()).unwrap(),
            handler,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();
        addresses.push(server.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        shutdown_tokens.push(shutdown.clone());
        tasks.push(tokio::spawn(server.run(shutdown)));
    }
    let unavailable = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    addresses.push(unavailable.local_addr().unwrap());
    drop(unavailable);

    let recorder_clients = members
        .iter()
        .enumerate()
        .map(|(index, recorder)| {
            TlsRecorderClient::new(
                members[0],
                *recorder,
                deployment,
                addresses[index],
                "localhost",
                identities[index].certificate_chain_der[0].clone(),
                Arc::clone(&tls[0].client),
                Arc::clone(&metrics),
            )
        })
        .collect::<Vec<_>>();
    let mut proposer = AsyncProposerCore::with_rng(
        members[0],
        LaneId::new(1),
        cluster,
        XorShift64::new_for_stream(7, members[0], LaneId::new(1)),
        Arc::clone(&metrics),
    );
    let decision = proposer
        .propose(
            SlotIndex::new(1),
            vec![10_u64],
            Some(members[0]),
            &recorder_clients,
        )
        .await
        .unwrap();
    assert_eq!(decision.value_ids, vec![10]);

    let submit_client = TlsSubmitClient::new(
        client_id,
        deployment,
        addresses[0],
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[3].client),
        Arc::clone(&metrics),
    );
    assert_eq!(
        submit_client.submit(1, vec![20_u64]).await.unwrap(),
        SubmissionOutcome::Accepted
    );
    assert_eq!(
        submit_client.submit(1, vec![20_u64]).await.unwrap(),
        SubmissionOutcome::Duplicate(None)
    );

    let forged_client = TlsSubmitClient::new(
        [8; 16],
        deployment,
        addresses[0],
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[3].client),
        Arc::clone(&metrics),
    );
    assert!(forged_client.submit(2, vec![30_u64]).await.is_err());

    let snapshot = metrics.snapshot();
    assert!(snapshot.accepted_connections >= 5);
    assert!(snapshot.requests >= 4);
    assert!(snapshot.request_errors >= 1);

    for shutdown in shutdown_tokens {
        shutdown.cancel();
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_batch_publish_and_replica_fetch_use_mtls() {
    let deployment = DeploymentId::from_u128(99);
    let replica = ReplicaId::new(1);
    let client_id = [4; 16];
    let identities = [identity(), identity()];
    let trusted = identities
        .iter()
        .map(|identity| identity.certificate_chain_der[0].clone())
        .collect::<Vec<_>>();
    let tls = identities
        .iter()
        .map(|identity| MutualTlsConfigs::new(identity, trusted.clone()).unwrap())
        .collect::<Vec<_>>();
    let peers = BTreeMap::from([
        (
            identities[0].certificate_chain_der[0].clone(),
            PeerIdentity::Replica(replica),
        ),
        (
            identities[1].certificate_chain_der[0].clone(),
            PeerIdentity::Client(client_id),
        ),
    ]);
    let handler = FnSubmissionHandler(
        |_submission: Submission<u64>| -> BoxSubmissionFuture<'static, u64> {
            Box::pin(async { Ok(SubmissionOutcome::Accepted) })
        },
    );
    let batches = Arc::new(
        AuthenticatedBatchService::new(
            InMemoryBatchStore::<u64>::default(),
            |value_id: &u64, payload: &[u8]| {
                if payload == value_id.to_string().as_bytes() {
                    Ok(())
                } else {
                    Err(rust_quepaxa::QuePaxaError::InvalidProposal(
                        "batch digest test failed".into(),
                    ))
                }
            },
            BatchServiceLimits::default(),
        )
        .unwrap(),
    );
    let server = NetworkNodeServer::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        deployment,
        Arc::clone(&tls[0].server),
        peers,
        RecorderCore::permissive_for_tests(replica, vec![replica]).unwrap(),
        handler,
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap();
    let peer_registry = server.peer_registry();
    let tls_registry = server.tls_registry();
    let server = server.with_batch_service(Arc::clone(&batches));
    let address = server.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(server.run(shutdown.clone()));

    let publisher = TlsBatchClient::for_client(
        client_id,
        deployment,
        address,
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[1].client),
        Arc::new(NetworkMetrics::default()),
    );
    publisher
        .publish(vec![Batch {
            value_id: 7_u64,
            payload: b"7".to_vec(),
        }])
        .await
        .unwrap();
    assert!(peer_registry.revoke(&identities[1].certificate_chain_der[0]));
    assert!(
        publisher
            .publish(vec![Batch {
                value_id: 9_u64,
                payload: b"9".to_vec(),
            }])
            .await
            .is_err()
    );
    peer_registry.authorize(
        identities[1].certificate_chain_der[0].clone(),
        PeerIdentity::Client(client_id),
    );
    publisher
        .publish(vec![Batch {
            value_id: 9_u64,
            payload: b"9".to_vec(),
        }])
        .await
        .unwrap();
    tls_registry.replace(Arc::clone(&tls[1].server));
    assert!(
        publisher
            .publish(vec![Batch {
                value_id: 12_u64,
                payload: b"12".to_vec(),
            }])
            .await
            .is_err()
    );
    TlsBatchClient::for_client(
        client_id,
        deployment,
        address,
        "localhost",
        identities[1].certificate_chain_der[0].clone(),
        Arc::clone(&tls[1].client),
        Arc::new(NetworkMetrics::default()),
    )
    .publish(vec![Batch {
        value_id: 12_u64,
        payload: b"12".to_vec(),
    }])
    .await
    .unwrap();
    tls_registry.replace(Arc::clone(&tls[0].server));
    assert!(
        publisher
            .publish(vec![Batch {
                value_id: 8_u64,
                payload: b"wrong".to_vec(),
            }])
            .await
            .is_err()
    );
    assert!(publisher.fetch::<u64>(vec![7]).await.is_err());

    let unavailable = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let unavailable_address = unavailable.local_addr().unwrap();
    drop(unavailable);
    let unavailable_client = TlsBatchClient::for_client(
        client_id,
        deployment,
        unavailable_address,
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[1].client),
        Arc::new(NetworkMetrics::default()),
    );
    let quorum_publisher =
        TlsBatchPublisher::new(vec![publisher.clone(), unavailable_client.clone()], 1).unwrap();
    quorum_publisher
        .publish(vec![Batch {
            value_id: 10_u64,
            payload: b"10".to_vec(),
        }])
        .await
        .unwrap();
    assert!(
        TlsBatchPublisher::all(vec![publisher.clone(), unavailable_client])
            .unwrap()
            .publish(vec![Batch {
                value_id: 11_u64,
                payload: b"11".to_vec(),
            }])
            .await
            .is_err()
    );

    let fetcher = TlsBatchClient::for_replica(
        replica,
        deployment,
        address,
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[0].client),
        Arc::new(NetworkMetrics::default()),
    );
    assert_eq!(
        fetcher.fetch(vec![7_u64]).await.unwrap(),
        vec![Batch {
            value_id: 7,
            payload: b"7".to_vec(),
        }]
    );
    let unavailable_fetch = TlsBatchClient::for_replica(
        replica,
        deployment,
        unavailable_address,
        "localhost",
        identities[0].certificate_chain_der[0].clone(),
        Arc::clone(&tls[0].client),
        Arc::new(NetworkMetrics::default()),
    );
    let good_fetch = fetcher.clone();
    let runtime = tokio::runtime::Handle::current();
    let fetched = tokio::task::spawn_blocking(move || {
        let mut fetcher = TlsBatchFetcher::new(
            unavailable_fetch,
            runtime,
            |value_id: &u64, payload: &[u8]| {
                if payload == value_id.to_string().as_bytes() {
                    Ok(())
                } else {
                    Err(rust_quepaxa::QuePaxaError::InvalidProposal(
                        "fetched batch verification failed".into(),
                    ))
                }
            },
        )
        .with_fallbacks([good_fetch]);
        fetcher.fetch_values(&[9])
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fetched, vec![(9, b"9".to_vec())]);
    assert_eq!(batches.prune_checkpointed(&[7]).unwrap(), 1);
    assert!(fetcher.fetch::<u64>(vec![7]).await.is_err());

    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn network_submission_commits_and_executes_on_each_live_node() {
    let deployment = DeploymentId::from_u128(84);
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let identities = (0..4).map(|_| identity()).collect::<Vec<_>>();
    let trusted = identities
        .iter()
        .map(|identity| identity.certificate_chain_der[0].clone())
        .collect::<Vec<_>>();
    let tls = identities
        .iter()
        .map(|identity| MutualTlsConfigs::new(identity, trusted.clone()).unwrap())
        .collect::<Vec<_>>();
    let client_id = [7; 16];
    let peer_map = members
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
    let executions = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
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
        )
        .unwrap();
        let consensus = NetworkConsensusHandler::new(
            runtime_config,
            recorder_clients,
            InMemoryRuntimeStore::default(),
            CountingMachine(Arc::clone(&executions[index])),
            Arc::clone(&metrics),
        )
        .await
        .unwrap();
        consensus
            .install_epoch_schedule(EpochSchedule::new(0, members.clone()).unwrap())
            .await
            .unwrap();
        let handler =
            DeduplicatingSubmissionHandler::new(consensus, InMemorySubmissionJournal::default());
        let server = NetworkNodeServer::bind(
            addresses[index],
            deployment,
            Arc::clone(&tls[index].server),
            peer_map.clone(),
            RecorderCore::permissive_for_tests(members[index], members.clone()).unwrap(),
            handler,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();
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
    let committed = client.submit(1, vec![500_u64]).await.unwrap();
    let decision = match committed {
        SubmissionOutcome::Committed(decision) => decision,
        unexpected => panic!("unexpected submission outcome: {unexpected:?}"),
    };
    assert_eq!(decision.value_ids, vec![500]);
    assert_eq!(executions[0].load(Ordering::Acquire), 1);
    assert_eq!(executions[1].load(Ordering::Acquire), 1);
    assert_eq!(
        client.submit(1, vec![500_u64]).await.unwrap(),
        SubmissionOutcome::Duplicate(Some(decision))
    );

    let concurrent = tokio::join!(
        client.submit(2, vec![501_u64]),
        client.submit(3, vec![502_u64]),
        client.submit(4, vec![503_u64]),
        client.submit(5, vec![504_u64]),
    );
    let outcomes = [concurrent.0, concurrent.1, concurrent.2, concurrent.3]
        .into_iter()
        .map(|outcome| match outcome.unwrap() {
            SubmissionOutcome::Committed(decision) => decision,
            unexpected => panic!("unexpected submission outcome: {unexpected:?}"),
        })
        .collect::<Vec<_>>();
    for (decision, expected_value) in outcomes.iter().zip(501..=504) {
        assert!(decision.value_ids.contains(&expected_value));
    }
    assert_eq!(executions[0].load(Ordering::Acquire), 5);
    assert_eq!(executions[1].load(Ordering::Acquire), 5);

    let second_node_client = TlsSubmitClient::new(
        client_id,
        deployment,
        addresses[1],
        "localhost",
        identities[1].certificate_chain_der[0].clone(),
        Arc::clone(&tls[3].client),
        Arc::clone(&metrics),
    );
    let competing = tokio::join!(
        client.submit(6, vec![600_u64]),
        second_node_client.submit(7, vec![700_u64]),
    );
    for (outcome, expected_value) in [(competing.0, 600), (competing.1, 700)] {
        let SubmissionOutcome::Committed(decision) = outcome.unwrap() else {
            panic!("competing submission did not commit");
        };
        assert!(decision.value_ids.contains(&expected_value));
    }
    assert_eq!(executions[0].load(Ordering::Acquire), 7);
    assert_eq!(executions[1].load(Ordering::Acquire), 7);

    for shutdown in shutdown_tokens {
        shutdown.cancel();
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

#[test]
fn deduplicating_state_machine_applies_a_value_id_only_once() -> Result<()> {
    use rust_quepaxa::Decision;
    use rust_quepaxa::StateMachine;
    use rust_quepaxa::Step;
    use rust_quepaxa::network::{DeduplicatingStateMachine, InMemoryExactlyOnceExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let applications = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&applications);
    let mut state_machine = DeduplicatingStateMachine::new(InMemoryExactlyOnceExecutor::new(
        move |_slot, _value: &u64| {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
    ));
    state_machine.execute(&Decision::new(
        SlotIndex::new(1),
        vec![99],
        ReplicaId::new(1),
        Step::new(4),
    )?)?;
    state_machine.execute(&Decision::new(
        SlotIndex::new(2),
        vec![99],
        ReplicaId::new(2),
        Step::new(4),
    )?)?;

    assert_eq!(applications.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn network_runtime_recovers_an_idle_gap_with_a_noop() {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let recorders = local_recorders(&members);
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(2),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    let handler = NetworkConsensusHandler::new(
        config,
        recorders.clone(),
        InMemoryRuntimeStore::default(),
        CheckpointMachine(Arc::new(AtomicUsize::new(0))),
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap()
    .with_noop_value(0);
    handler
        .install_epoch_schedule(EpochSchedule::new(0, members).unwrap())
        .await
        .unwrap();
    let later = Decision::new(
        SlotIndex::new(2),
        vec![22],
        ReplicaId::new(1),
        Step::ROUND_ONE_PHASE_ZERO,
    )
    .unwrap();
    for recorder in &recorders {
        recorder
            .inform_decisions(vec![later.clone()])
            .await
            .unwrap();
    }
    SubmissionHandler::receive_decisions(&handler, vec![later])
        .await
        .unwrap();

    assert_eq!(
        handler.decision(SlotIndex::new(1)).await.unwrap().value_ids,
        vec![0]
    );
    assert_eq!(
        handler.decision(SlotIndex::new(2)).await.unwrap().value_ids,
        vec![22]
    );
}

#[tokio::test]
async fn network_runtime_reconfigures_through_a_joint_epoch() {
    let old_members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let old_recorders = local_recorders(&old_members);
    let handler = NetworkConsensusHandler::new(
        ReplicaRuntimeConfig::new(
            ReplicaId::new(1),
            LaneId::new(1),
            old_members.clone(),
            1,
            ReplicaConfig::default(),
            16,
            Duration::ZERO,
        )
        .unwrap(),
        old_recorders.clone(),
        InMemoryRuntimeStore::default(),
        CheckpointMachine(Arc::new(AtomicUsize::new(0))),
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap();
    handler
        .install_epoch_schedule(EpochSchedule::new(0, old_members).unwrap())
        .await
        .unwrap();
    SubmissionHandler::submit(
        &handler,
        Submission {
            client_id: [1; 16],
            request_id: 1,
            value_ids: vec![100],
        },
    )
    .await
    .unwrap();
    let first = handler.decision(SlotIndex::new(1)).await.unwrap();

    let stable = ClusterIdentity::new((1..=3).map(ReplicaId::new), 1).unwrap();
    let joint = stable
        .begin_joint([ReplicaId::new(1), ReplicaId::new(4), ReplicaId::new(5)], 1)
        .unwrap();
    let mut joint_recorders = old_recorders;
    for id in [ReplicaId::new(4), ReplicaId::new(5)] {
        joint_recorders.push(LocalAsyncRecorder {
            id,
            core: Arc::new(Mutex::new(RecorderCore::new(
                RecorderConfig::from_cluster(id, joint.clone()).unwrap(),
                Arc::new(AllowAllAvailability),
            ))),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
    }
    handler
        .install_membership(
            MembershipChange::new(first, joint.clone(), 100).unwrap(),
            joint_recorders.clone(),
        )
        .await
        .unwrap();
    handler
        .install_epoch_schedule(EpochSchedule::new(0, joint.members().to_vec()).unwrap())
        .await
        .unwrap();
    SubmissionHandler::submit(
        &handler,
        Submission {
            client_id: [1; 16],
            request_id: 2,
            value_ids: vec![200],
        },
    )
    .await
    .unwrap();
    let second = handler.decision(SlotIndex::new(2)).await.unwrap();

    let finalized = joint.finalize_joint().unwrap();
    let final_recorders = joint_recorders
        .into_iter()
        .filter(|recorder| finalized.contains(recorder.id))
        .collect::<Vec<_>>();
    handler
        .install_membership(
            MembershipChange::new(second, finalized.clone(), 200).unwrap(),
            final_recorders,
        )
        .await
        .unwrap();
    handler
        .install_epoch_schedule(EpochSchedule::new(0, finalized.members().to_vec()).unwrap())
        .await
        .unwrap();
    SubmissionHandler::submit(
        &handler,
        Submission {
            client_id: [1; 16],
            request_id: 3,
            value_ids: vec![300],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        handler.decision(SlotIndex::new(3)).await.unwrap().value_ids,
        vec![300]
    );
}

#[tokio::test]
async fn network_hedging_observes_step_level_progress() {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let recorders = local_recorders(&members);
    recorders[0]
        .core
        .lock()
        .unwrap()
        .record(RecordRequest {
            sender: ReplicaId::new(1),
            slot: SlotIndex::new(1),
            round_one_leader: Some(ReplicaId::new(1)),
            step: Step::ROUND_ONE_PHASE_ZERO,
            proposal: Proposal::new(
                ProposalKey::new(Priority::MAX, ReplicaId::new(1), LaneId::new(1)),
                vec![99],
            )
            .unwrap(),
            known_decisions: Vec::new(),
        })
        .unwrap();
    let handler = NetworkConsensusHandler::new(
        ReplicaRuntimeConfig::new(
            ReplicaId::new(2),
            LaneId::new(1),
            members.clone(),
            1,
            ReplicaConfig::default(),
            16,
            Duration::from_millis(2),
        )
        .unwrap(),
        recorders.clone(),
        InMemoryRuntimeStore::default(),
        CheckpointMachine(Arc::new(AtomicUsize::new(0))),
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap();
    handler
        .install_epoch_schedule(EpochSchedule::new(0, members).unwrap())
        .await
        .unwrap();
    let outcome = SubmissionHandler::submit(
        &handler,
        Submission {
            client_id: [1; 16],
            request_id: 1,
            value_ids: vec![20],
        },
    )
    .await
    .unwrap();
    assert!(matches!(outcome, SubmissionOutcome::Committed(_)));
    assert!(
        recorders
            .iter()
            .map(|recorder| recorder.status_calls.load(Ordering::Acquire))
            .sum::<usize>()
            >= recorders.len()
    );
}

#[tokio::test]
async fn network_state_transfer_resumes_at_the_next_slot() {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let source_count = Arc::new(AtomicUsize::new(0));
    let source = NetworkConsensusHandler::new(
        ReplicaRuntimeConfig::new(
            ReplicaId::new(1),
            LaneId::new(1),
            members.clone(),
            1,
            ReplicaConfig::default(),
            16,
            Duration::ZERO,
        )
        .unwrap(),
        local_recorders(&members),
        InMemoryRuntimeStore::default(),
        CheckpointMachine(Arc::clone(&source_count)),
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap();
    source
        .install_epoch_schedule(EpochSchedule::new(0, members.clone()).unwrap())
        .await
        .unwrap();
    SubmissionHandler::submit(
        &source,
        Submission {
            client_id: [1; 16],
            request_id: 1,
            value_ids: vec![10],
        },
    )
    .await
    .unwrap();
    let transfer = source
        .create_state_transfer(SlotIndex::new(1))
        .await
        .unwrap();
    assert_eq!(transfer.checkpointed_value_ids, vec![10]);
    let batches = AuthenticatedBatchService::new(
        InMemoryBatchStore::default(),
        |_value_id: &u64, _payload: &[u8]| Ok(()),
        BatchServiceLimits::default(),
    )
    .unwrap();
    batches
        .publish(vec![Batch {
            value_id: 10,
            payload: b"ten".to_vec(),
        }])
        .unwrap();
    assert_eq!(batches.prune_state_transfer(&transfer).unwrap(), 1);
    assert!(matches!(
        batches.fetch(vec![10]),
        Err(rust_quepaxa::QuePaxaError::MissingValue)
    ));

    let destination_recorders = local_recorders(&members);
    for recorder in &destination_recorders {
        recorder
            .core
            .lock()
            .unwrap()
            .install_state_transfer_floor(SlotIndex::new(1));
    }
    let destination_count = Arc::new(AtomicUsize::new(0));
    let destination = NetworkConsensusHandler::new(
        ReplicaRuntimeConfig::new(
            ReplicaId::new(2),
            LaneId::new(1),
            members,
            1,
            ReplicaConfig::default(),
            16,
            Duration::ZERO,
        )
        .unwrap(),
        destination_recorders,
        InMemoryRuntimeStore::default(),
        CheckpointMachine(Arc::clone(&destination_count)),
        Arc::new(NetworkMetrics::default()),
    )
    .await
    .unwrap();
    destination.install_state_transfer(transfer).await.unwrap();
    assert_eq!(destination_count.load(Ordering::Acquire), 1);
    let outcome = SubmissionHandler::submit(
        &destination,
        Submission {
            client_id: [2; 16],
            request_id: 2,
            value_ids: vec![11],
        },
    )
    .await
    .unwrap();
    let SubmissionOutcome::Committed(decision) = outcome else {
        panic!("state-transferred submission did not commit")
    };
    assert_eq!(decision.slot, SlotIndex::new(2));
    assert_eq!(destination_count.load(Ordering::Acquire), 2);
}
