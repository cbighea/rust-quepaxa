# Production integration checklist

This crate supplies QuePaxa's consensus and local recovery machinery. A usable
deployment should treat the following contracts as required, not optional
optimizations.

## Persistence boundaries

1. Run every recorder behind `DurableRecorderCore`.
2. Implement `RecorderCodec` and `RuntimeCodec` with a versioned, checksummed
   format. Reject unknown versions rather than guessing.
3. Keep recorder and runtime files on durable local storage. A successful
   recorder RPC means the updated ISR state has been synced.
4. Make state-machine execution idempotent by slot. Also deduplicate globally
   unique command/value IDs so retries submitted through different replicas do
   not apply twice.
5. Implement `StateMachine::export_checkpoint` and `import_checkpoint` with a
   durable, idempotent application format.
6. Use `create_state_transfer` for pruning. It requires every configured member
   to acknowledge all covered decisions and returns the application checkpoint
   with the matching consensus snapshot.
7. A far-behind node must install the `StateTransferSnapshot` and durably call
   `install_state_transfer_floor` on its recorder before rejoining. Never let it
   re-run consensus at or below the transferred floor.
8. Keep the transfer package until both application import and runtime snapshot
   persistence have completed; the two stores cannot share a transaction in a
   generic library, so imports must be safe to retry after a crash.
9. Run `tests/crash_killpoints.rs` on each supported filesystem. It kills a real
   subprocess at every recorder, runtime, batch, submission-journal, and
   exactly-once snapshot open, write, fsync, rename, and directory-fsync
   boundary.
10. Use `FileSubmissionJournal` for durable same-node request retry suppression.
    Use `FileExactlyOnceExecutor` when all application state fits its snapshot;
    otherwise implement `ExactlyOnceExecutor` inside the application database
    transaction.

## Transport boundary

- Authenticate both ends of every replica connection and bind the verified
  credential to its configured `ReplicaId`.
- Encrypt protocol contents. QuePaxa's liveness argument assumes a
  content-oblivious network scheduler.
- Preserve one logical response per configured recorder and reject identity or
  cluster mismatches before quorum counting.
- Provide a durable retry path for decision dissemination independently of
  consensus phases. `NetworkConsensusHandler` performs an immediate best-effort
  quorum dissemination, but the deployment must retry a failed dissemination.
- Treat `ScheduleMismatch`, `ConflictingDecision`, and `SlotPruned` as fatal
  safety signals. Do not retry around the recorder that reported one.
- Bound request queues, payload sizes, concurrent RPCs, and connection retry
  work.
- Keep transport timeouts distinct from consensus/view-change logic. The
  supplied TLS client bounds a whole RPC at 30 seconds and reconnects on the
  next call; custom synchronous clients must bound/cancel their own I/O as well.
- For membership changes, commit and drain a stable-to-joint transition, then
  commit and drain joint-to-stable finalization. A joint quorum must satisfy
  both component `n-f` thresholds; a numeric union majority is insufficient.
- Pre-provision joining certificates and use state transfer before activating
  their joint-epoch recorder endpoints.
- Commit `MembershipCommand::sha256_id()` and use `bind` so the transition's
  exact voter set and fault budget are content-addressed by the anchor decision.
- Use the live `PeerRegistry` to overlap old/new leaf certificates during
  rotation, use `ServerTlsRegistry` to replace keys/trust roots for new
  connections, and revoke the old leaf after clients switch.

## Client boundary

- Assign every client command or submitted batch a globally unique value ID.
- Publish batches through `TlsBatchPublisher` to the application's required
  durability threshold before submission; configure several authenticated
  fetch sources and call `prune_state_transfer` only after receiving the
  matching durable checkpoint token.
- Acknowledge success only after the runtime reports commitment according to
  the application's durability policy.
- Retrying the same ID at the same runtime is suppressed durably. Cross-replica
  retries still require the state machine's durable ID deduplication table.
- Apply backpressure before `max_pending_values` or `max_tracked_value_ids` is
  reached, and expose overload as a retryable client response.
- Configure a reserved no-op value with `with_noop_value` and make the state
  machine recognize it, or an idle deployment can remain blocked behind a gap.
- A command submitted only to a node that dies before payload dissemination
  still requires a client retry to another live node.

## Initial deployment target

Start with three statically configured replicas and `f = 1`. Validate these
scenarios before increasing scale:

- any one recorder unavailable during proposal;
- recorder crash immediately before and after its durable save;
- proposer runtime crash before and after decision persistence;
- repeated client submissions using the same value ID;
- delayed, reordered, duplicated, and disconnected RPCs;
- seeded WAN delay/jitter/bandwidth, asymmetric blackholes, and connection
  resets through `ReproducibleWanProxy`/`ChaosRecorderClient`;
- reproducible multi-link runs plus JSON proxy/load reports through
  `wan_harness` and `load_generator`;
- stable-to-joint-to-stable reconfiguration with a joining and a removed node;
- checkpoint, pruning, restart, and continued commitment;
- state transfer into a recorder whose local pruning floor is behind;
- divergent epoch configuration, which must fail with `ScheduleMismatch`;
- an idle gap recovered through the configured no-op;
- a transport that accepts a connection but never completes an RPC.

The `restartable_cluster` example exercises local recovery. The
`networked_cluster` example and `tests/network.rs` exercise mutual TLS,
certificate-to-identity binding, one-fault quorum progress, concurrent slots,
competing proposers, client request deduplication, and remote ordered execution.

Enable the node layer with `--features network`. Use `PostcardRecorderCodec`,
`PostcardRuntimeCodec`, and `PostcardStateTransferCodec` for bounded, versioned,
SHA-256-checked snapshots, or provide codecs with equivalent version rejection,
integrity checks, and size limits.
