# Networked node layer

Enable the layer with the Cargo feature `network`. Core-only builds retain no
third-party runtime or TLS dependencies.

## Components

- `MutualTlsConfigs` builds Rustls client and server configurations from a DER
  certificate chain, PKCS#8 private key, and trusted CA certificates.
- `NetworkNodeServer` authenticates every connection, binds the leaf
  certificate to a configured replica or client identity, enforces deployment
  and wire versions, bounds frames and concurrent connections, and supports
  cancellation-token shutdown.
- `TlsRecorderClient` and `AsyncProposerCore` implement cancellable async
  Algorithm 4 quorum RPC. Outstanding connections are dropped after the
  configured membership quorum is satisfied: `n - f` authenticated replies in
  a stable epoch, or both component `n - f` thresholds in a joint epoch.
- `NetworkConsensusHandler` persists queued proposals, drives several slots
  concurrently, retries values that lose a slot, executes contiguous decisions
  in order, recovers pending proposals after restart, and disseminates decisions
  to a quorum of nodes. It uses agreed or committed-log-derived epoch schedules,
  scheduled hedging, recorder step probes, and optional no-op gap recovery.
- `TlsSubmitClient` submits globally unique value IDs using an authenticated
  client ID and stable request ID.
- `DeduplicatingSubmissionHandler` provides same-node request replay handling
  around a pluggable `SubmissionJournal`; `FileSubmissionJournal` provides the
  checksummed, fsynced reference implementation.
- `DeduplicatingStateMachine` delegates every value ID to an
  `ExactlyOnceExecutor`. `FileExactlyOnceExecutor` atomically snapshots its
  durable ID table with application state for snapshotable state machines.
- `NetworkMetrics` exposes atomic counters; the server and runtime emit
  structured `tracing` events for failures and degraded dissemination.
- `AuthenticatedBatchService`, `TlsBatchClient`, and `TlsBatchFetcher` provide
  bounded mTLS batch publish/fetch with local payload verification.
- `ReproducibleWanProxy` shapes encrypted TCP links; `ChaosRecorderClient`
  injects decoded-RPC reordering, drops, and duplication reproducibly.
- `ReproducibleWanHarness` runs several named links and writes atomic JSON
  counters. `OpenLoopLoadGenerator` produces seeded Poisson traffic and JSON
  throughput/p50/p99 reports.

## Identity configuration

Every node receives a map from exact peer leaf-certificate DER bytes to one
`PeerIdentity`. Rustls first verifies the certificate chain and purpose against
the configured roots. The exact mapping then prevents a CA-valid certificate
from claiming another replica or client ID.

Outbound clients verify the server hostname and certificate chain and also
compare the returned leaf certificate to the configured target certificate.
Requests carry a `DeploymentId`; nodes reject traffic for another deployment
even if its certificate happens to be trusted.

The example uses ephemeral self-signed certificates only to stay standalone.
Production deployments should use a private CA, short-lived certificates,
protected private keys, and an explicit rotation/revocation process.

Call `NetworkNodeServer::peer_registry` and `tls_registry` before starting the
server to retain live handles. Authorize a replacement leaf before switching
clients, hot-swap the complete rustls server configuration (certificate, key,
and trust roots) for new connections, then revoke the old DER certificate.
Issuance and protected key distribution remain deployment responsibilities.

The current wire version is `4` and the Postcard storage version is `5`. Wire
version 4 binds membership transitions to a committed command ID. Storage
version 5 adds checkpoint-covered value IDs to state-transfer snapshots; all
snapshot payloads remain wrapped in SHA-256 integrity metadata. Upgrade all
nodes together; older traffic and snapshots are intentionally rejected.

## Membership changes

Membership changes use two committed barriers:

1. Commit an application-defined command naming the desired voter set under
   the current stable membership and drain all later slots.
2. Build `current.begin_joint(new_members, new_f)` and install a
   `MembershipChange` anchored to that exact decision on the runtime and every
   active recorder. Every slot in this epoch requires both old and new `n-f`
   quorums.
3. Commit a finalize command under the joint membership, drain again, then
   install `joint.finalize_joint()` anchored to the finalize decision.

For the standard content-addressed path, create `MembershipCommand::new(next)`,
commit its `sha256_id()` as the value ID, then call `bind(decision)`. Generic
applications may construct `MembershipChange` with their own command ID, but it
must be present in the exact anchor decision and the application must preserve
the same ID/content verification contract.

Joining recorders may start directly in the joint epoch with
`RecorderConfig::from_cluster`, but must first install the source checkpoint and
pruning floor. Joining runtimes install state transfer after the source runtime
has persisted the joint epoch. The server certificate map must already contain
the joining identities (or the server must be restarted with an updated map)
before the transition.

## Batch dissemination

Attach an `AuthenticatedBatchService` with
`NetworkNodeServer::with_batch_service`. Clients and replicas may publish;
servers admit only verified ID/payload pairs. Only authenticated replicas may
fetch. Configure `TlsBatchPublisher::new` with the acknowledgement count that
implements the application's durability policy, such as `f+1` or `n-f`; use
`TlsBatchPublisher::all` for all-node durability. The caller is responsible for
calculating membership-dependent thresholds. Configure
`TlsBatchFetcher::with_fallbacks` with multiple sources. The fetcher locally
verifies every response before `FetchingAvailability` can acknowledge an ID.
`FileBatchStore` is checksummed and fsynced. Prefer
`prune_state_transfer`, which consumes the exact value-ID set captured in a
matching durable application/consensus checkpoint; `prune_checkpointed` is the
lower-level application-policy hook.

## Reproducible WAN and chaos runs

Run a transparent proxy in front of any node endpoint:

```sh
cargo run --features network --example chaos_proxy -- \
  127.0.0.1:7101 127.0.0.1:7001 42 80 20 10000000 100
```

The positional values are listen address, target, seed, base delay (ms),
jitter (ms), optional bytes/second, optional fail-every-N-connections, and
optional reset-after-bytes. Use the library API for blackholing and distinct
per-link profiles. Wrap `TlsRecorderClient` in `ChaosRecorderClient` when an
experiment needs decoded-RPC duplication or drops; duplicating raw TLS bytes
would merely corrupt the stream.

For several links with JSON result collection:

```sh
cargo run --features network --example wan_harness -- \
  60 wan-report.json \
  tokyo-dublin,127.0.0.1:7101,127.0.0.1:7001,42,120,20,10000000 \
  mumbai-dublin,127.0.0.1:7102,127.0.0.1:7002,43,90,15,10000000
```

The `load_generator` example accepts DER credentials and produces a separate
atomic JSON report with offered load, overload drops, achieved throughput, and
p50/p99 latency. `U64SubmissionClient` is the adapter boundary for driving the
same workload against comparative protocol clients.

## Persistence

`NetworkConsensusHandler` accepts any `RuntimeStateStore`; recorders should use
`DurableRecorderCore` with a `RecorderStateStore`. The `network` feature adds
bounded, versioned Postcard codecs for recorder, runtime, and state-transfer
snapshots. The existing file
stores write a temporary file, fsync it, atomically replace the snapshot, and
fsync the parent directory.

`FileSubmissionJournal` applies the same protocol to client request outcomes.
For applications whose complete durable state can be represented as a
serializable snapshot, `FileExactlyOnceExecutor` writes each application
mutation and its globally unique value ID in the same snapshot transaction.
Callbacks must only mutate that snapshot; external side effects still require
an application database transaction implementing `ExactlyOnceExecutor`.

Call `NetworkConsensusHandler::recover` after construction and before accepting
client traffic. It executes committed-but-unrecorded work idempotently and
restarts every persisted pending proposal using its scheduled hedge delay.

For retention, implement the state machine checkpoint methods and call
`NetworkConsensusHandler::create_state_transfer`. The call requires every member
to acknowledge the covered decisions before pruning and returns a portable
`StateTransferSnapshot`, including the exact checkpoint-covered value IDs for
batch retention. A joining node calls `install_state_transfer`, then
durably advances its local recorder with `install_state_transfer_floor`. The
optional `PostcardStateTransferCodec` provides bounded versioned encoding.

## Request and completion semantics

A client request is identified by `(client_id, request_id)`. Retrying the same
pair through the same node returns the journaled outcome. Different request IDs
may still contain the same global value ID, including through different nodes;
the `ExactlyOnceExecutor` is the final duplicate-execution boundary.

`Committed` is returned only when every value ID in that submission appears in
the returned decision. If a concurrent proposal wins the first slot, the losing
values stay durable and are proposed again in a later slot.

Pending client commands are local until their value IDs are disseminated. If a
client submits only to one node and that node dies before dissemination or
proposal, the client must retry the same globally unique value ID at a live
node. Same-node request journaling does not remove that requirement.

## Epochs and hedging

Install the identical `EpochSchedule` at every node, or enable
`ReplicaRuntimeConfig::with_auto_schedules` everywhere. Auto mode ranks leaders
from committed decided-step statistics, giving every replica the same inputs.
The safety-relevant epoch size/mode is persisted with runtime snapshots.

Recorders also bind each active slot to one round-one leader. A conflicting
request receives `ScheduleMismatch`, which both quorum proposers treat as fatal.
This quorum-intersection guard prevents dueling reserved-priority leaders even
if deployment configuration drifts.

Non-leaders wait their schedule position times the current base hedge delay.
With `AdaptiveHedgingConfig`, successful local consensus attempts update that
delay from a bounded rolling percentile of wall-clock completion times. This
timing is not safety-critical and may differ by node; leader order may not.
At the deadline nodes probe recorder steps, and observed advancement postpones
activation for another base interval. An incoming decision removes the queued
proposal and wakes hedges that are still waiting; it does not explicitly cancel
an `AsyncProposerCore` call that has already started. Configure
`with_noop_value` on every handler so receipt of a later decision can
automatically recover an otherwise idle gap.

## Operational notes

- The reference transport uses one bounded request per TLS connection. This
  makes cancellation and framing unambiguous; high-throughput deployments may
  front it with connection reuse or implement the same traits over HTTP/2 or
  QUIC.
- There is deliberately no consensus/view-change timeout. Each TLS RPC does
  have a configurable 30-second transport bound covering connect, handshake,
  write, and response read. Expiry is an unavailable recorder result; it never
  changes the leader or protocol step. The next call opens a fresh connection.
- Export `NetworkMetricsSnapshot` through the deployment's monitoring system
  and install a `tracing` subscriber appropriate to the environment.
