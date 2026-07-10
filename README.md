# QuePaxa in Rust

A Rust implementation of the QuePaxa consensus core, based on Algorithm 4 of
the QuePaxa paper.

The crate includes a transport-neutral consensus library, durable crash/restart
support, an optional mutual-TLS network layer, membership reconfiguration, and
fault and performance harnesses. It is intended to be embedded in an
application that supplies the replicated state machine and operational control
plane.

> [!IMPORTANT]
> This is not a ready-to-deploy replicated service. A production application
> must still provide checkpoints, state-machine execution, credentials,
> certificate lifecycle, and the meaning of proposed values. Read
> [Before production](#before-production) before deploying it.

## Quick start

The repository pins Rust 1.88. With [Rustup](https://rustup.rs/) installed, the
correct toolchain is selected automatically.

```sh
cargo run --example in_memory_cluster
```

This starts a three-recorder, in-process cluster, commits one value, executes
it, and prints:

```text
execute slot 1: [10]
notify clients for slot 1
```

The example uses in-memory storage and direct function calls so the consensus
flow is easy to follow. It is not a production configuration.

For a demonstration of durable restart and retry handling:

```sh
cargo run --example restartable_cluster
```

For a two-node mutual-TLS example in a statically configured three-replica
deployment:

```sh
cargo run --features network --example networked_cluster
```

See [NETWORKING.md](NETWORKING.md) for the network API and deployment model.

## How it fits together

```text
membership + availability checks
              |
              v
        RecorderCore  <---->  authenticated recorder endpoints
              ^
              |
agreed epoch schedule + durable state + application hooks
              |
              v
        ReplicaRuntime  ---->  submit, commit, execute, notify
```

The first replica in an agreed `EpochSchedule` is the round-one leader. If the
fast path does not decide, `ProposerCore` falls back to QuePaxa's randomized,
leaderless rounds.

The embedding application decides what a value ID means, makes its payload
available, executes committed decisions, persists application state, and
notifies clients.

## What the crate provides

- `IntervalSummaryRegister`, the paper's constant-space ISR state for each
  active slot.
- `RecorderCore` and `DurableRecorderCore`, with membership checks, bounded
  fields, payload-availability gates, persist-before-ack operation, and restart
  recovery.
- `ProposerCore`, implementing the leader fast path and randomized fallback.
  Runtime deployments use the paper's `n - f` quorum.
- `ReplicaRuntime`, with durable proposer orchestration, hedged activation,
  in-order execution, decision dissemination, client hooks, and optional no-op
  recovery after a crashed proposer.
- Checkpoint-aware state transfer and pruning for consensus state and batch
  payloads.
- Checksummed file stores, a durable submission journal, and exactly-once
  execution support for snapshotable applications.
- Consensus-anchored membership changes through stable → joint → stable epochs.
- Seeded interleaving, process-crash, WAN-chaos, load, and benchmark harnesses.
- An optional `network` feature with bounded mutual-TLS RPC, concurrent slot
  driving, batch publish/fetch, tracing hooks, and atomic metrics.

Membership reconfiguration is an extension in this repository, not a change to
the paper's per-slot safety proof. See the
[paper-parity assessment](../PAPER_PARITY.md) for a feature-by-feature account.

## Use the library

The crate has no default features. The synchronous, transport-neutral API is
available directly:

```toml
[dependencies]
rust-quepaxa = { path = "path/to/rust-quepaxa" }
```

Enable the reference network layer when authenticated RPC is needed:

```toml
[dependencies]
rust-quepaxa = { path = "path/to/rust-quepaxa", features = ["network"] }
```

Rust converts the package's hyphenated name to `rust_quepaxa` in source code:

```rust
use rust_quepaxa::{ProposerCore, RecorderCore, ReplicaRuntime};
```

`RecorderHandle` is the dependency-free synchronous adapter. Networked
deployments should use `TlsRecorderClient` with `AsyncProposerCore` or
`NetworkConsensusHandler`.

## Test and benchmark

Run the transport-neutral suite:

```sh
cargo test
```

Include the optional network layer:

```sh
cargo test --features network
```

The suite covers integration, simulation, randomized proposer interleavings,
network behavior, literal subprocess crashes at durable-write boundaries, and
full-runtime restart. Keep `tests/interleaving.rs` in the regular test matrix;
it checks agreement across thousands of same-schedule and divergent-schedule
interleavings.

Run the dependency-free throughput and p50/p99 latency benchmark with:

```sh
cargo bench --bench consensus
```

The paper's Promela models and their SPIN commands are in
[`../formal/`](../formal/). The quick smoke check can be started from the parent
directory with:

```sh
sh formal/verify.sh inline-quick
```

## Before production

QuePaxa assumes crash-stop replicas, known membership, and a content-oblivious
network adversary. A real deployment must preserve those safety boundaries:

1. Authenticate every proposer-to-recorder connection. With mTLS, bind the
   verified peer certificate to its `RecorderHandle` identity.
2. Configure the same membership and explicit fault budget `f` everywhere.
   Startup rejects `n < 2f + 1`; runtime proposals wait for `n - f` replies.
3. Agree on each epoch schedule before installing it. Never derive the
   round-one leader independently from local wall-clock observations.
4. Fetch and verify every proposed payload before acknowledging its value ID.
   The permissive availability guard is for deterministic tests only.
5. Use `DurableRecorderCore` and durable recorder/runtime stores for any node
   that may rejoin. An in-memory `RecorderCore` follows a strict crash-stop
   model and must not return after losing state.
6. Bound retained slots, values, requests, frames, and payloads. Checkpoint and
   prune only after every member has acknowledged the covered decisions.
7. Reconfigure only through a committed stable → joint → stable transition.
   Provision and state-transfer joining nodes before entering the joint epoch.
8. Keep `OsRandom` as the production priority source. `XorShift64` exists for
   reproducible tests.

The synchronous adapter bounds each phase with
`ProposerConfig::rpc_timeout`—30 seconds by default. Transport implementations
must also cancel or reconnect their underlying operations; Rust cannot stop a
blocked foreign thread after the adapter returns. The TLS client applies the
same bound to connect, handshake, write, and response read.

The crate supplies file-backed stores and codecs, but an application must still
provide and operate:

- certificate issuance, secure delivery, rotation, and revocation;
- durable storage locations and a checkpoint/restore policy;
- a snapshotable state machine or database-backed exactly-once transaction;
- metrics export, tracing collection, alerting, and capacity planning;
- client admission control and load testing suitable for the target workload.

`PeerRegistry` supports live leaf-certificate authorization and revocation, and
`ServerTlsRegistry` can swap server identity and trust roots for new
connections. They do not replace a certificate-management system.

For a more detailed operational checklist, see [PRODUCTION.md](PRODUCTION.md).

## Documentation

- [NETWORKING.md](NETWORKING.md) — mutual TLS, framing, network APIs, and node
  integration.
- [PRODUCTION.md](PRODUCTION.md) — operational requirements and deployment
  guidance.
- [Formal models](../formal/README.md) — the paper's Promela models, the joint
  consensus model, and SPIN verification profiles.
- [Paper-parity assessment](../PAPER_PARITY.md) — what matches the paper, what
  supports production readiness, and what remains environment-specific.
- [QuePaxa paper](https://bford.info/pub/os/quepaxa/quepaxa.pdf) — the paper implemented by this crate.

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).
