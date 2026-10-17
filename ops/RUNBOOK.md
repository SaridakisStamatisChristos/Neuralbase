# NeuralBase development and disaster-recovery runbook

This runbook documents the **tested pre-1.0 operator recovery model** plus read-consistency and managed-membership controls relevant to validation after failover or recovery. It is not a production-HA or automatic-disaster-recovery guarantee. `Local` reads may lag, strong reads require the current serving leader, membership reconciliation requires the explicit managed profile, and archived recovery is limited to exact committed Raft indexes from an explicitly managed archive stream.

## Safety rules

- Treat every backup and archive as sensitive database material. They contain table/catalog state and replicated SCRAM verifier material.
- Prefer authenticated NBEC encryption for retained/off-host backups and authenticated NBPE archive encryption when archive confidentiality is required.
- Never put backup/archive key bytes on a command line, in logs, in artifacts, or in source control.
- Never manufacture multiple voters by copying one restored RocksDB directory.
- Never reuse a historical source node ID as the designated recovery node ID.
- Never point restore at an existing directory. Restore is intentionally fresh-target-only.
- Never manually promote a `.restore-partial-*` or archive staging file into service.
- Do not infer timestamp-target PITR, automatic DR, production HA, or linearizable arbitrary-follower reads from this runbook.
- Do not treat a failed `Leader`/`Linearizable` read on a follower as permission to serve a local fallback; route/retry through an application/operator-controlled current-leader path.
- Do not change Kubernetes/Helm replica counts directly to alter cluster membership. Use the managed reconciliation path or explicit membership operations.

## Build the operator tools

```bash
cargo build --release --locked --bin neuralbase-backup
cargo build --release --locked --bin neuralbase-pitr
cargo build --release --locked --bin neuralbase-operator
cargo build --release --locked --bin neuralbase
```

The examples below assume the release binaries, existing backup/key parent directories, and data produced by a compatible NeuralBase implementation.

## Backup formats

NeuralBase has two operator backup envelopes:

- `NBBK` v1 — plaintext, versioned logical backup with integrity checks;
- `NBEC` v1 — authenticated ChaCha20-Poly1305 container around the logical NBBK payload.

Both represent one logical SQL/identity state plus committed membership/recovery metadata and an explicit Raft/state-machine boundary. They are not raw RocksDB copies and they are not the internal Raft snapshot wire format.

### Key handling

Generate and store a **raw 32-byte key** using an operating-system or secret-management mechanism suitable for the environment. On Unix, the current loader rejects key files accessible by group or other users; use mode `0600` or stricter.

```bash
umask 077
openssl rand 32 > /secure/neuralbase-backup.key
chmod 600 /secure/neuralbase-backup.key
```

NBEC does not embed the decryption key or a key identifier. Key selection and rotation are operator responsibilities. Retain an old key for as long as any retained backup still depends on it. Verify a new encrypted backup before retiring an older recovery point.

## Offline backup

Offline backup is the external CLI capture path. The source RocksDB must not be open by a NeuralBase process; RocksDB's exclusive lock enforces that boundary. The source must contain persisted Raft state and committed membership. A database written only in standalone mode does not meet that requirement.

Plaintext:

```bash
target/release/neuralbase-backup create \
  --db /var/lib/neuralbase/node-a \
  --output /backups/neuralbase.nbbk
```

Encrypted:

```bash
target/release/neuralbase-backup create \
  --db /var/lib/neuralbase/node-a \
  --output /backups/neuralbase.nbec \
  --key-file /secure/neuralbase-backup.key
```

Creation stages restrictive bytes, fsyncs them, independently reopens and validates the complete staged artifact, then publishes without overwriting an existing destination. Success is reported only after publication.

A stopped follower can be behind the leader's latest acknowledged state. An offline backup therefore captures that member's validated recovery boundary, not necessarily the globally newest cluster boundary.

### Interruption semantics

A destination is authoritative only after successful publication. A crash/write failure may leave an unpublished stage, but a stage is never automatically promoted just because its bytes happen to be valid. Retry the normal command after resolving the failure; do not manually rename a stage into service.

## Independent verification

Plaintext:

```bash
target/release/neuralbase-backup verify \
  --backup /backups/neuralbase.nbbk
```

Encrypted:

```bash
target/release/neuralbase-backup verify \
  --backup /backups/neuralbase.nbec \
  --key-file /secure/neuralbase-backup.key
```

Verification is independent of restore. It checks envelope/version, authenticated encryption where applicable, strict logical decoding, integrity hashes/checksums, manifest/boundary consistency, identity inclusion, membership/recovery metadata, limits, and unsupported state markers. Do not use a backup that fails verification.

## Online consistent backup

`OnlineBackupCoordinator` is the leader-coordinated in-process online path. It uses a confirmed Raft barrier plus a stable before/after durable-state fingerprint. A successful artifact corresponds to one committed/applied boundary. Concurrent SQL, identity, membership, compaction, or leadership movement either orders cleanly around that boundary or makes the attempt retry/fail closed.

Plaintext and encrypted online creation share the same capture path; encrypted online publication uses the same NBEC authenticated publication primitive as encrypted offline backup.

**Current invocation boundary:** the standalone `neuralbase-backup create` command is offline-only. NeuralBase does not expose an authenticated live-server backup management socket or SQL command. Do not describe online backup as a standalone live-server CLI feature.

Programmatic integrations construct [`OnlineBackupCoordinator`](../src/online_backup.rs) with the runtime's client channel, shared Raft state, storage, clock, and serving-readiness flag. Integrations must invoke it on the running clustered leader and surface failure rather than silently falling back to a follower or offline copy.

## Restore into a fresh recovery authority

Plaintext:

```bash
target/release/neuralbase-backup restore \
  --backup /backups/neuralbase.nbbk \
  --target /var/lib/neuralbase/recovery-a \
  --node-id recovery-a
```

Encrypted:

```bash
target/release/neuralbase-backup restore \
  --backup /backups/neuralbase.nbec \
  --target /var/lib/neuralbase/recovery-a \
  --node-id recovery-a \
  --key-file /secure/neuralbase-backup.key
```

The target must not exist. The recovery node ID must be fresh and absent from the source membership history.

Restore verifies the source **before target creation**, reconstructs SQL/catalog/HLC/apply/identity state in a hidden sibling directory, creates a deliberate new single-voter recovery membership generation at the backed-up Raft boundary, tombstones historical source identities, independently verifies the staged database, and only then atomically publishes the target.

### Interrupted restore

Recovery uses fail-closed abandonment, not automatic resume of arbitrary restore remnants. A crash can leave hidden `.TARGET.restore-partial-*` siblings. They are never serving authority. A retry allocates a new staging directory and publishes only a newly completed verified target. After a successful retry and health verification, stale stages may be removed manually for cleanup/forensics.

## Exact committed-index archived recovery

Archived recovery is opt-in. Its recovery coordinate is an exact committed Raft index. Wall-clock timestamp targets are intentionally unsupported.

### 1. Initialize a stream from a verified baseline

Plain baseline/archive:

```bash
target/release/neuralbase-pitr init \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr
```

Encrypted inputs use key **file paths**, never raw key bytes:

```bash
target/release/neuralbase-pitr init \
  --backup /backups/base.nbec \
  --backup-key-file /secure/backup.key \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key
```

The archive key is exactly 32 raw bytes and is managed out of band. Stream metadata binds the baseline hash/index/term, state-machine compatibility, source membership metadata, encryption mode, and a unique timeline.

### 2. Enable synchronous runtime archival

Start the clustered server with the already initialized stream:

```bash
export NEURALBASE_PITR_ARCHIVE_DIR=/archive/neuralbase-pitr
export NEURALBASE_PITR_KEY_FILE=/secure/pitr.key   # omit for plaintext NBAR
export NEURALBASE_PITR_MAX_SEGMENTS=100000        # optional; default shown
```

With archival enabled, a committed entry is durably applied to the replicated state machine, its required archive segment is durably published and verified, and only then is confirmed apply reported to Raft. Archive publication failure therefore fails closed rather than allowing confirmed apply/compaction to pass the missing recovery position. The non-archive runtime path is unchanged when `NEURALBASE_PITR_ARCHIVE_DIR` is unset.

### 3. Inspect and verify targets

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

Supported targets are `baseline`, `latest`, or an exact committed index in the verified range. An application/operator that needs time-oriented recovery must maintain a trustworthy mapping from application time/event to committed Raft index.

### 4. Recover to an exact point

Fence the old topology and choose a fresh recovery node ID. The target directory must not exist:

```bash
target/release/neuralbase-pitr recover \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr \
  --target 4242 \
  --target-dir /var/lib/neuralbase/recovery-pitr \
  --node-id recovery-pitr \
  --archive-key-file /secure/pitr.key
```

Recovery verifies the baseline and required archive chain before publication, restores into a hidden stage, deterministically replays through the selected index, reconstructs selected historical membership, creates a fresh single-voter generation with historical source IDs tombstoned, independently reopens/verifies the staged database, and only then publishes the target. A wrong baseline, missing/corrupt segment, incompatible version, wrong key, or unavailable target fails before the requested directory becomes serving authority.

### 5. Branch before creating a new future

After recovery to an earlier point, keep the database **stopped** while creating its child timeline; starting Raft can append a new current-term control position.

```bash
target/release/neuralbase-pitr branch \
  --parent-archive /archive/neuralbase-pitr \
  --parent-archive-key-file /secure/pitr.key \
  --branch-target 4242 \
  --db /var/lib/neuralbase/recovery-pitr \
  --baseline-out /backups/branch-4242.nbbk \
  --archive /archive/neuralbase-pitr-branch
```

The child receives a new timeline and records the parent timeline/branch index. Parent segments after the branch point are not valid child history. Start the recovered server with the **child** archive directory before accepting new writes.

### 6. Rollover and conservative retirement

`NEURALBASE_PITR_MAX_SEGMENTS` bounds one stream. Before the limit is reached, quiesce the old writer and create a verified replacement child whose baseline/branch is exactly the old durable frontier. Then retire the parent:

```bash
target/release/neuralbase-pitr retire \
  --archive /archive/neuralbase-pitr \
  --replacement-archive /archive/neuralbase-pitr-next \
  --archive-key-file /secure/pitr.key \
  --replacement-archive-key-file /secure/pitr-next.key
```

Retirement is conservative: mismatched lineage, boundary, term, segment sequence/count, or staging ambiguity preserves the old data. Never delete archive segments manually to bypass the configured limit.

### Archived-recovery failure boundary

- Unknown committed command encodings, unsupported archive/state-machine versions, gaps, overlaps, bad links, corrupt finalized segments, wrong timelines, and wrong keys fail closed.
- Staging leftovers are not authoritative; interrupted publication never advertises an unverified target.
- Archive verification is bounded by per-payload and per-stream limits.
- Archived recovery does not provide remote object-store replication, automatic archive copying, timestamp mapping, automatic disaster detection/failover, or a measured RPO/RTO SLA.
- `neuralbase_pitr_*` metrics expose runtime publication/frontier/limit/rejection signals; operators still need their own durable storage, alerting, retention, and key-lifecycle policy.

See [`docs/PITR.md`](../docs/PITR.md) and [`docs/PITR_CORRECTNESS_MODEL.md`](../docs/PITR_CORRECTNESS_MODEL.md).

## Complete cluster loss

Use this workflow only when recovery from the healthy existing quorum is no longer possible.

1. Fence/stop all historical nodes. Preserve disks for forensics if needed, but prevent old processes from reconnecting.
2. Select a retained backup (or verified archive target) and independently verify it.
3. Provision a new empty storage path for one **fresh** recovery node ID.
4. Restore exactly one designated recovery authority.
5. Start that node with `NEURALBASE_NODE_ID` matching the fresh recovery ID and a topology appropriate to the new single-voter generation.
6. Verify SQL data and authentication before adding members.
7. Add additional **empty-disk, fresh-ID learners** through the membership API or managed reconciler.
8. Wait for snapshot/log catch-up and confirm learner state before promotion.
9. Promote learners through joint consensus; do not copy the recovery node's RocksDB directory.
10. Verify the final voter set and confirm historical source IDs remain removed/tombstoned.
11. Perform a new acknowledged write, test leadership/failover as appropriate, and verify convergence.
12. Verify an explicit `Linearizable` read against the current leader after the write and after any failover/restart used in the recovery drill.
13. Only then return normal traffic according to the environment's operational controls.

The cluster-recovery integration suite executes the core fresh-generation lifecycle: source three-voter state, operator backup, one fresh restored authority, two fresh learners, catch-up/promotion, SQL/SCRAM convergence, new writes, leader loss/election, and full restart. The read-consistency process suite additionally restores a backup to a fresh recovery node and establishes linearizable read authority in the new generation.

There is no automatic controller that converts arbitrary static deployment replica counts into safe membership. Use the explicit managed profile if deployment reconciliation is required.

## Strong-read validation after recovery or failover

Connect to the **current leader** and use one statement per request:

```bash
psql -X -v ON_ERROR_STOP=1 -h <leader-host> -p 5432 -U <user> -d postgres \
  -c 'SET neuralbase_read_consistency = linearizable' \
  -c 'SELECT 1'
```

`Local` is the default and performs no consensus coordination. `Leader` and `Linearizable` require current-leader authority, a current-term quorum barrier, and confirmed durable local apply. Followers/recovering nodes reject strong reads explicitly; there is no silent downgrade.

## Managed membership reconciliation

For the tested local-process profile, use a versioned desired topology and the explicit operator:

```bash
python3 ops/neuralbase_operator.py bootstrap \
  --config /path/to/desired.json \
  --server target/release/neuralbase \
  --planner target/release/neuralbase-operator

python3 ops/neuralbase_operator.py reconcile --steps 100 \
  --config /path/to/desired.json \
  --server target/release/neuralbase \
  --planner target/release/neuralbase-operator
```

`plan` is advisory/dry-run; execution reobserves and revalidates authority. Fresh members begin non-voting, catch up, then promote. Replacement adds/promotes a fresh incarnation before removal of the old one; leader removal requires transfer first. Removed storage is retained. Tombstoned IDs cannot be reused.

The managed Kubernetes profile uses one StatefulSet/PVC per incarnation plus UID/resource-version and managed-field guards. It is separate from the static Helm/Compose examples. HPA and arbitrary raw replica changes remain unsupported. See [`docs/MANAGED_MEMBERSHIP.md`](../docs/MANAGED_MEMBERSHIP.md).

## Verification after a recovery drill

At minimum, record and verify:

- backup/archive independent verification succeeded;
- restored/recovered target was published only after staged verification;
- recovery node ID and membership generation are fresh;
- expected SQL rows/catalog objects are present at the selected recovery boundary;
- SCRAM authentication succeeds with the credentials expected at that boundary and fails with credentials that should not exist there;
- source identities remain removed/tombstoned;
- added learners fully catch up before promotion;
- an acknowledged post-recovery write converges to every promoted voter;
- a current-leader `Linearizable` read sees the acknowledged state;
- failover/restart, if part of the drill, preserves those properties.

## Claims boundary

This runbook supports the repository's tested operator-recovery, exact-index archive recovery, read-consistency, and managed-membership contracts. It does **not** establish production SQL HA, arbitrary-follower linearizable reads, automatic strong-read routing, arbitrary Helm/HPA scaling, rolling-upgrade orchestration, timestamp-target PITR, automatic DR, a production RPO/RTO SLA, or complete PostgreSQL compatibility.
