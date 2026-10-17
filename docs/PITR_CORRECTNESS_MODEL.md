# Phase 9 PITR correctness model

Status: active implementation design for Phase 9.

Baseline: `4d54ffe8d14b5cec1596678c497b28d86fb61737` (`main`, post-Phase-8 CI #368 green).

This document fixes the correctness boundary that Phase 9 implements. It is deliberately narrower than a general WAL/PITR promise and remains subordinate to executable tests and the repository confidence model.

## Recovery unit

The canonical recovery index is the committed Raft log index. Phase-9 archive v1 records every committed Raft position after the selected Phase-5 backup boundary, including deterministic SQL mutations, replicated identity mutations, membership commands, and control/no-op positions. Recording every committed position gives the archive one total, gap-detectable order without inventing a second ordering domain.

Archive v1 uses one logical recovery record per segment. A segment therefore has `first_index == last_index`. This intentionally trades file-count efficiency for the simplest crash, retry, gap, overlap, and compaction proof. Segment batching is a later compatible-format problem, not a Phase-9 prerequisite.

The archive payload for SQL and identity is the already-versioned deterministic replicated command bytes (`NBRM` / `NBRI`). Historical SQL text is never archived for replay. Membership records preserve the committed membership command bytes. Other non-mutating committed positions are represented explicitly as control records, so a missing index cannot be confused with an intentionally omitted entry.

Timestamp/wall-clock recovery is **not** part of archive v1. DML has HLC commit timestamps, but DDL, identity, membership, and control positions do not currently provide one proven timestamp domain. Supported targets are exact recovery index, baseline, or latest verified archived index.

## Timeline and baseline identity

Every archive stream has an immutable timeline identifier and is bound to exactly one verified NBBK/NBEC baseline by:

- baseline backup SHA-256;
- baseline `last_included_index` and `last_included_term`;
- source membership generation/config index;
- state-machine compatibility version.

The first segment links to a deterministic baseline anchor. Every later segment links to the preceding finalized segment hash. A chain from another timeline, another baseline, a gap, overlap, or conflicting duplicate fails closed.

A recovery that stops before the source timeline's latest point and then accepts new writes creates a new timeline. The new timeline records its parent timeline and branch index. Source-future segments are never valid members of the new chain.

## Eligibility, acknowledgement, and compaction

A record becomes archive-eligible only after its committed entry has completed the existing durable logical state-machine apply.

When PITR archival is configured, NeuralBase uses a synchronous RPO-0 archive contract:

1. Raft commits the entry.
2. The existing SQL/identity/control durable state-machine apply completes.
3. The matching archive segment is staged, fsynced, validated, atomically published, and its parent directory is synced where supported.
4. Only then does the confirmed-apply completion succeed back to Raft.
5. Raft may then advance its applied frontier and acknowledge the command according to the existing lifecycle.

When PITR archival is not configured, the pre-Phase-9 acknowledgement behavior is unchanged.

This placement also supplies the compaction fence. SQL-aware compaction is already bounded by Raft's applied frontier. In archive-enabled mode that frontier cannot advance over a required entry whose archive publication failed. A crash after durable state-machine apply but before archive publication is recoverable: Raft has not accepted confirmed apply, and retry may observe the logical state as already applied while publishing the still-missing idempotent archive record.

## Crash-safe publication and restart

Final segment files are the only advertised recovery targets. Publication uses a hidden staging file, file sync, strict decode/verification, atomic rename, and parent-directory sync where available. Staging leftovers are never interpreted as complete history and are cleaned or retried on restart.

The durable archive cursor is derived from the verified final segment chain, not volatile memory. Restart rescans the immutable stream metadata plus finalized segments. Re-appending an already published identical index is idempotent; a different term/type/payload at that index is a conflict and fails closed.

## Replay

Recovery is `verified Phase-5 baseline -> verified archive prefix -> deterministic replay -> fresh recovery generation -> atomic publication`.

Replay never re-plans SQL. SQL and identity records are reapplied through the deterministic replicated state-machine command path. Control positions advance the durable apply frontier without inventing state. Membership commands are replayed as historical membership evidence so the selected point has the correct generation/tombstone history, but historical quorum authority is not revived.

Recovered apply/snapshot/HLC metadata may never advance beyond content actually restored and replayed.

## Fresh recovery authority

The selected source membership is evidence for fencing, not live authority. PITR recovery preserves the Phase-5 fresh-cluster rule:

- the requested recovery node ID must be fresh;
- source active and removed identities are rejected for reuse;
- historical source identities remain tombstoned/evidenced;
- a deliberate single-voter recovery topology is established;
- the recovery membership generation is greater than the selected source generation.

Branching after an earlier target additionally creates a new archive timeline before new archived writes can be accepted.

## Encryption

Encrypted archive mode reuses the repository's established `rustls::crypto::ring` AES-256-GCM primitives and authenticated-container discipline. Keys remain out-of-band. Stream metadata records a non-secret key identifier, never key material. Wrong keys, altered authentication metadata, and modified ciphertext fail before plaintext recovery records are trusted. Encrypted publication must never emit a parallel plaintext segment artifact.

## Retention

Retention is chain-aware and bounded. Phase 9 never deletes the bound baseline or a finalized segment required to reach any retained target unless a replacement recovery base/chain has first been fully written and independently verified. Failure to delete is safer than premature deletion.

## Operator surface

The Phase-9 operator surface must support initialization from a verified baseline, status/frontier inspection, chain verification, recovery-target listing, exact-target and latest recovery, diagnosis of corrupt/missing history, and branch initialization. `pitr: true` remains forbidden until the complete process/fault/identity/membership/encryption matrix and exact-head CI evidence pass.