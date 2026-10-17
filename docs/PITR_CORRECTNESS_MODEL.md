# PITR correctness model

Status: current correctness model for exact committed-index archived recovery.

This document fixes the correctness boundary for NeuralBase archive/replay recovery. It is deliberately narrower than a general WAL/PITR promise and remains subordinate to executable tests and the repository confidence model.

## Recovery unit

The canonical recovery index is the committed Raft log index. Archive v1 records every committed Raft position after the selected operator-backup boundary, including deterministic SQL mutations, replicated identity mutations, membership commands, and control/no-op positions. Recording every committed position gives the archive one total, gap-detectable order without inventing a second ordering domain.

Archive v1 uses one logical recovery record per segment. A segment therefore has `first_index == last_index`. This intentionally trades file-count efficiency for simpler crash, retry, gap, overlap and compaction reasoning. Segment batching is a later compatible-format concern rather than a correctness prerequisite.

The archive payload for SQL and identity is the already-versioned deterministic replicated command bytes (`NBRM` / `NBRI`). Historical SQL text is never archived for replay. Membership records preserve committed membership command bytes. Other non-mutating committed positions are represented explicitly as control records, so a missing index cannot be confused with an intentionally omitted entry.

Timestamp/wall-clock recovery is **not** part of archive v1. DML has HLC commit timestamps, but DDL, identity, membership and control positions do not currently provide one proven timestamp domain. Supported targets are an exact recovery index, baseline, or latest verified archived index.

## Timeline and baseline identity

Every archive stream has an immutable timeline identifier and is bound to exactly one verified NBBK/NBEC baseline by:

- baseline backup SHA-256;
- baseline `last_included_index` and `last_included_term`;
- source membership generation/config index;
- state-machine compatibility version.

The first segment links to a deterministic baseline anchor. Every later segment links to the preceding finalized segment hash. A chain from another timeline, another baseline, a gap, overlap or conflicting duplicate fails closed.

A recovery that stops before the source timeline's latest point and then accepts new writes creates a new timeline. The child records its parent timeline and branch index. Source-future segments are never valid members of the new chain.

## Eligibility, acknowledgement and compaction

A record becomes archive-eligible only after its committed entry has completed the existing durable logical state-machine apply.

When PITR archival is configured, NeuralBase uses a synchronous archive contract:

1. Raft commits the entry.
2. The existing SQL/identity/control durable state-machine apply completes.
3. The matching archive segment is staged, fsynced, validated, atomically published, and its parent directory is synced where supported.
4. Only then does confirmed-apply completion succeed back to Raft.
5. Raft may then advance its applied frontier and acknowledge the command according to the existing lifecycle.

When PITR archival is not configured, the ordinary acknowledgement behavior is unchanged.

This placement also supplies the compaction fence. SQL-aware compaction is bounded by Raft's applied frontier. In archive-enabled mode that frontier cannot advance over a required entry whose archive publication failed. A crash after durable state-machine apply but before archive publication is recoverable: Raft has not accepted confirmed apply, and retry may observe logical state as already applied while publishing the still-missing idempotent archive record.

## Crash-safe publication and restart

Final segment files are the only advertised recovery targets. Publication uses a hidden staging file, file sync, strict decode/verification, atomic rename, and parent-directory sync where available. Staging leftovers are never interpreted as complete history and are cleaned or retried on restart.

The durable archive cursor is derived from the verified final segment chain, not volatile memory. Restart rescans immutable stream metadata plus finalized segments. Re-appending an already published identical index is idempotent; a different term/type/payload at that index is a conflict and fails closed.

## Replay

Recovery is `verified operator backup baseline -> verified archive prefix -> deterministic replay -> fresh recovery generation -> atomic publication`.

Replay never re-plans SQL. SQL and identity records are reapplied through the deterministic replicated state-machine command path. Control positions advance the durable apply frontier without inventing state. Membership commands are replayed as historical membership evidence so the selected point has the correct generation/tombstone history, but historical quorum authority is not revived.

Recovered apply/snapshot/HLC metadata may never advance beyond content actually restored and replayed.

## Fresh recovery authority

The selected source membership is evidence for fencing, not live authority. Fresh-cluster recovery preserves these rules:

- the requested recovery node ID must be fresh;
- source active and removed identities are rejected for reuse;
- historical source identities remain tombstoned/evidenced;
- a deliberate single-voter recovery topology is established;
- the recovery membership generation is greater than the selected source generation.

Branching after an earlier target additionally creates a new archive timeline before new archived writes can be accepted.

## Encryption

Encrypted archive mode uses the repository's authenticated-container discipline with out-of-band keys. Stream metadata records a non-secret key identifier, never key material. Wrong keys, altered authentication metadata and modified ciphertext fail before plaintext recovery records are trusted. Encrypted publication must never emit a parallel plaintext segment artifact.

## Retention

Retention is chain-aware and bounded. The implementation never deletes the bound baseline or a finalized segment required to reach a retained target unless a replacement recovery base/chain has first been fully written and independently verified. Failure to delete is safer than premature deletion.

## Operator surface

The operator surface supports initialization from a verified baseline, status/frontier inspection, chain verification, recovery-target listing, exact-target/latest recovery, diagnosis of corrupt or missing history, child timeline creation and conservative retirement. `pitr: true` in `CONFIDENCE.yaml` means only this exact committed-index contract.
