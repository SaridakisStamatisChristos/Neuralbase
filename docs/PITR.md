# Phase-9 point-in-time recovery

NeuralBase Phase 9 implements an **opt-in archived recovery stream with exact committed-Raft-index recovery**. It does not implement wall-clock timestamp targets or automatic disaster recovery.

## Recovery model

A stream consists of:

1. a verified operator backup baseline (`NBBK` or encrypted `NBEC`);
2. `stream.json`, which binds the baseline SHA-256, baseline index/term, source membership generation/config index, state-machine/archive versions, encryption mode and one unique timeline;
3. one archive segment for each required committed position after the baseline.

Plain segments use the versioned `NBAR` envelope. Encrypted segments use authenticated `NBPE` wrapping. Records carry the exact committed Raft index/term and one explicit kind: replicated SQL, replicated identity, membership change, or a known control entry. Unknown non-empty committed command families are rejected so a future command cannot silently become unrecoverable history.

Archive v1 deliberately uses **one logical record per segment**. The format includes a state-machine compatibility marker, timeline ID, previous-segment hash, payload hash, bounded payload length and final SHA-256 checksum. The verifier rejects unsupported versions/flags, malformed lengths, trailing bytes, corruption, gaps, overlaps, duplicate conflicts, wrong timelines and bad hash links.

## Initialize

Build the tool:

```bash
cargo build --release --locked --bin neuralbase-pitr
```

Initialize from a verified plaintext baseline:

```bash
target/release/neuralbase-pitr init \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr
```

An NBEC baseline can be supplied with `--backup-key-file`. Add `--archive-key-file /secure/pitr.key` to create an encrypted archive stream. PITR keys are raw 32-byte files; key bytes are never accepted directly on argv.

Initialization does not start archival automatically. It creates and verifies the archive namespace bound to that exact baseline.

## Runtime durability fence

Enable the already initialized stream on a clustered server with:

```bash
export NEURALBASE_PITR_ARCHIVE_DIR=/archive/neuralbase-pitr
export NEURALBASE_PITR_KEY_FILE=/secure/pitr.key   # encrypted streams only
export NEURALBASE_PITR_MAX_SEGMENTS=100000        # optional; default
```

`NEURALBASE_PITR_MAX_SEGMENTS` must be in `1..=1000000`.

When PITR archival is enabled, the confirmed-apply task orders one committed entry as:

1. deterministic durable replicated state-machine apply;
2. durable archive publication and independent record verification;
3. confirmed apply returned to Raft.

Therefore an archive publication failure is a confirmed-apply failure, not a warning. The normal non-PITR path remains unchanged when `NEURALBASE_PITR_ARCHIVE_DIR` is unset.

Startup opens and verifies the complete stream. It fails closed if the archive baseline/frontier is newer than durable logical apply, if the configured segment limit is already exceeded, or if a persisted compacted Raft snapshot is ahead of the verified archive frontier.

## Inspect recovery targets

```bash
target/release/neuralbase-pitr status \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key

target/release/neuralbase-pitr verify \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key

target/release/neuralbase-pitr targets \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key
```

The supported targets are:

- `baseline`;
- `latest`;
- an exact unsigned committed Raft index inside the verified range.

A timestamp such as `2026-09-15T12:00:00Z` is not a valid Phase-9 target. Any time-to-index mapping must come from a separate trustworthy application/operator record.

## Recover

Fence the historical topology before disaster recovery and choose a fresh node ID. The destination must not exist.

```bash
target/release/neuralbase-pitr recover \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr \
  --target 4242 \
  --target-dir /var/lib/neuralbase/recovery-pitr \
  --node-id recovery-pitr \
  --archive-key-file /secure/pitr.key
```

Recovery verifies the baseline hash/metadata and requested target before publication. It restores into a hidden staging directory, sequentially verifies and replays required records through `ReplicatedSqlStateMachine`, reconstructs the selected historical membership, then establishes a fresh single-voter recovery generation whose active voter is the requested fresh recovery node. Historical source IDs are tombstoned. The staged database is closed/reopened and independently verified before an atomic target-directory publication.

Wrong baselines, missing/corrupt segments, incompatible versions, unavailable indexes and historical node-ID reuse fail before the requested destination becomes authoritative.

## Branch after an earlier-point recovery

Recovery to an earlier index discards the parent's later logical future. Before starting the recovered database and accepting new writes, create a new child archive timeline while the recovered database is stopped:

```bash
target/release/neuralbase-pitr branch \
  --parent-archive /archive/neuralbase-pitr \
  --parent-archive-key-file /secure/pitr.key \
  --branch-target 4242 \
  --db /var/lib/neuralbase/recovery-pitr \
  --baseline-out /backups/branch-4242.nbbk \
  --archive /archive/neuralbase-pitr-branch
```

The child metadata records `parent_timeline` and `branch_index` but receives a different timeline ID and its own verified baseline. Parent segments after the branch point cannot join the child chain; tests explicitly reject old-future injection.

Use the child archive directory when starting the recovered node. Starting first can append a current-term control/no-op position and move the database beyond the intended branch baseline.

## Retention and rollover

The runtime segment limit bounds one stream; it is not an instruction to delete old files. Rollover requires a verified replacement child whose baseline/branch is exactly the old durable frontier. Quiesce the old writer before retirement, then run:

```bash
target/release/neuralbase-pitr retire \
  --archive /archive/neuralbase-pitr \
  --replacement-archive /archive/neuralbase-pitr-next \
  --archive-key-file /secure/pitr.key \
  --replacement-archive-key-file /secure/pitr-next.key
```

Retirement atomically moves the old stream out of service, revalidates its metadata/segment sequence/frontier/staging state, and deletes it only after all retirement preconditions remain true. Ambiguity preserves data.

## Encryption

NBPE uses ChaCha20-Poly1305 authenticated encryption. Archive metadata records whether encryption is required and the external key ID; raw key bytes are not stored in metadata. Wrong keys or authenticated-byte tampering fail verification/recovery.

Archive encryption protects the archive segments only. Use NBEC independently if the baseline also requires confidentiality. Neither mechanism encrypts RocksDB or internal Raft persistence at rest.

## Runtime metrics

When the relevant paths execute, the runtime emits:

- `neuralbase_pitr_archive_appends_total`;
- `neuralbase_pitr_archive_failures_total{reason}`;
- `neuralbase_pitr_startup_rejections_total{reason}`;
- `neuralbase_pitr_archive_frontier_index`;
- `neuralbase_pitr_archive_segment_limit`.

These are diagnostics, not an automatic SLO/alerting system.

## Failure semantics and tested boundaries

Phase-9 evidence covers strict format/version handling, canonical membership bytes, corruption/truncation/trailing bytes, gap/overlap/hash/timeline failures, restart staging cleanup, corrupt finalized files, duplicate/conflicting and convergent same-record publication, encrypted wrong-key/tamper behavior, bounded directory enumeration, wrong-baseline failure before publication, fresh recovery generations, child old-future rejection, conservative retention, and real-process exact-target SQL/identity recovery plus branched restart.

The real-process test proves an earlier recovery target excludes later rows and a later password rotation, then creates a new child future and verifies it survives restart.

## Explicit non-goals

Phase 9 does not claim:

- timestamp/wall-clock recovery targets;
- automatic disaster detection, failover or recovery;
- automatic remote/object-store archive replication;
- production retention, key-rotation or archive-copy policy;
- a measured RPO/RTO SLA;
- production SQL HA.

Those boundaries remain explicit even when `CONFIDENCE.yaml` reports `pitr: true`: that flag means the exact committed-index Phase-9 contract only.
