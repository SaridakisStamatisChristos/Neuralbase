# NeuralBase development and disaster-recovery runbook

This runbook documents the **tested pre-1.0 operator recovery model**. It is not a production-HA or automatic-disaster-recovery guarantee. Reads remain local and may lag, deployment membership reconciliation is not automatic, and PITR is not implemented.

## Safety rules

- Treat every backup as sensitive database material. Backups contain table/catalog state and replicated SCRAM verifier material.
- Prefer authenticated NBEC encryption for retained/off-host backups.
- Never put backup key bytes on a command line, in logs, in the backup artifact, or in source control.
- Never manufacture multiple voters by copying one restored RocksDB directory.
- Never reuse a historical source node ID as the designated recovery node ID.
- Never point restore at an existing directory. Restore is intentionally fresh-target-only.
- Never manually rename a `.restore-partial-*` directory into service.
- Do not infer PITR, automatic DR, production HA, or linearizable follower reads from this runbook.

## Build the operator tool

```bash
cargo build --release --locked --bin neuralbase-backup
```

The examples below assume `target/release/neuralbase-backup`.

## Backup formats

NeuralBase Phase 5 has two operator envelopes:

- `NBBK` v1 — plaintext, versioned logical backup with integrity checks;
- `NBEC` v1 — authenticated ChaCha20-Poly1305 container around the logical NBBK payload.

Both represent one logical SQL/identity state plus committed membership/recovery metadata and an explicit Raft/state-machine boundary. They are not raw RocksDB copies and they are not the internal Raft snapshot wire format.

## Key handling for NBEC v1

Generate and store a **raw 32-byte key** using an operating-system or secret-management mechanism suitable for the environment. On Unix, the current loader rejects key files accessible by group or other users; use mode `0600` or stricter.

Example with OpenSSL:

```bash
umask 077
openssl rand 32 > /secure/neuralbase-backup.key
chmod 600 /secure/neuralbase-backup.key
```

NBEC v1 does **not** embed the decryption key or a key identifier. Key selection is therefore an out-of-band operator responsibility. Record which external key protects each retained backup in the secret-management/inventory system, not inside the backup file.

Rotation semantics are deliberately simple:

1. create new backups with the new key;
2. retain the old key for as long as any old backup encrypted with it must remain recoverable;
3. verify a new encrypted backup with the new key before retiring an older recovery point;
4. do not assume NeuralBase can discover the correct key automatically;
5. a wrong key or modified NBEC artifact fails authentication.

There is no in-place re-key command in Phase 5. Re-encryption, if required operationally, should be performed by restoring/verifying under controlled conditions and creating a new backup, while retaining the original until the replacement is independently verified.

## Offline backup

Offline backup is the strongest external CLI path. The source RocksDB must not be open by a NeuralBase process; RocksDB's exclusive lock enforces that boundary.

Stop the selected source member cleanly, then create a plaintext backup:

```bash
target/release/neuralbase-backup create \
  --db /var/lib/neuralbase/node-a \
  --output /backups/neuralbase-2026-09-10.nbbk
```

Or create an encrypted backup without publishing plaintext backup bytes:

```bash
target/release/neuralbase-backup create \
  --db /var/lib/neuralbase/node-a \
  --output /backups/neuralbase-2026-09-10.nbec \
  --key-file /secure/neuralbase-backup.key
```

Creation stages restrictive bytes, fsyncs them, independently reopens and validates the complete staged artifact, then publishes without overwriting an existing destination. Success is reported only after publication.

### Offline backup interruption semantics

A destination file is authority only after successful publication. A crash or write failure may leave an unpublished staging artifact, but a staging artifact is never automatically promoted merely because its bytes happen to be valid. Retry the normal command after resolving the failure; do not manually promote temporary files.

The tested retry path fails closed when it encounters a stale staged publication, removes/quarantines that failed attempt, leaves the final destination absent, and succeeds only on a later clean creation.

## Independent verification

Plaintext NBBK:

```bash
target/release/neuralbase-backup verify \
  --backup /backups/neuralbase-2026-09-10.nbbk
```

Encrypted NBEC:

```bash
target/release/neuralbase-backup verify \
  --backup /backups/neuralbase-2026-09-10.nbec \
  --key-file /secure/neuralbase-backup.key
```

Verification is independent of restore. It checks the envelope/version, authenticated encryption where applicable, strict logical backup decoding, integrity hashes/checksums, manifest and boundary consistency, identity inclusion, membership/recovery metadata, limits, and unsupported state markers.

Do not use a backup that fails verification. Verification does not repair corrupt state.

## Online consistent backup

The Phase-5 online implementation is `OnlineBackupCoordinator`. It is leader-coordinated and uses a confirmed Raft barrier plus a stable before/after durable-state fingerprint. A successful artifact corresponds to one explicit committed/applied boundary. Concurrent SQL, identity, membership, compaction, or leadership movement either orders cleanly around that boundary or makes the attempt retry/fail closed.

Plaintext and encrypted online creation share the same capture path. Encrypted online publication uses the same NBEC authenticated publication primitive as encrypted offline backup.

**Current invocation boundary:** `OnlineBackupCoordinator` is an in-process API. The standalone `neuralbase-backup create` command is offline-only and cannot open a RocksDB database already owned by the running server. NeuralBase does not currently expose an authenticated live-server backup management socket/SQL command. Do not describe online backup as a standalone live-server CLI feature.

Programmatic integrations use the coordinator methods corresponding to:

```text
create_online_backup(...)
create_encrypted_online_backup(...)
```

An integration must invoke them on the running clustered leader/runtime and must surface failure rather than silently falling back to a follower or offline copy.

## Restore into a fresh recovery authority

Plaintext:

```bash
target/release/neuralbase-backup restore \
  --backup /backups/neuralbase-2026-09-10.nbbk \
  --target /var/lib/neuralbase/recovery-a \
  --node-id recovery-a
```

Encrypted:

```bash
target/release/neuralbase-backup restore \
  --backup /backups/neuralbase-2026-09-10.nbec \
  --target /var/lib/neuralbase/recovery-a \
  --node-id recovery-a \
  --key-file /secure/neuralbase-backup.key
```

The target must not exist. The recovery node ID must be fresh and absent from the source membership history.

Restore verifies the source artifact **before target creation**, reconstructs SQL/catalog/HLC/apply/identity state in a hidden sibling directory, creates a deliberate new single-voter recovery membership generation at the backed-up Raft boundary, tombstones historical source identities, independently verifies the staged database, and only then atomically publishes the target directory.

## Interrupted restore semantics

Phase 5 uses **fail-closed abandonment**, not automatic resume of arbitrary restore remnants.

A crash can leave hidden sibling directories named like `.TARGET.restore-partial-*`. Those directories are not the target and are never serving authority. On retry, restore allocates a fresh staging directory rather than interpreting an old stage as resumable state.

Executable tests cover representative stale stages at these boundaries:

- staging directory and durable `building` marker only;
- logical SQL/identity data restored but no authoritative recovery Raft metadata;
- a complete verified staged database before atomic target publication.

A retry must ignore all of them, construct a new verified stage, and publish only the new completed target. Stale stages remain quarantined for explicit operator cleanup/forensics.

After a successful retry and after confirming the final target is healthy, stale `.restore-partial-*` directories may be removed manually. Never rename or serve one directly.

## Complete cluster loss

Use this workflow only when recovery from the healthy existing quorum is no longer possible.

1. Fence/stop all historical nodes. Preserve disks for forensics if needed, but prevent old processes from reconnecting.
2. Select a retained backup and independently verify it.
3. Provision a new empty storage path for one **fresh** recovery node ID.
4. Restore exactly one designated recovery authority with `neuralbase-backup restore`.
5. Start that node with `NEURALBASE_NODE_ID` matching the fresh recovery ID and with a topology appropriate to the new single-voter generation.
6. Verify SQL data and authentication against the restored node before adding members.
7. Add additional **empty-disk, fresh-ID learners** through the already-tested membership API.
8. Wait for snapshot/log catch-up and confirm learner state before promotion.
9. Promote learners through the tested joint-consensus path; do not copy the recovery node's RocksDB directory.
10. Verify the final voter set and confirm historical source IDs remain removed/tombstoned.
11. Perform a new acknowledged write, test leadership/failover as appropriate, and verify convergence.
12. Only then return normal traffic according to the environment's own operational controls.

The repository's Phase-5 cluster-recovery integration test executes the core recovery lifecycle: source three-voter state, operator backup, one fresh restored authority, two fresh learners, catch-up/promotion, SQL and SCRAM convergence, new writes, leader loss/election, and full restart.

There is no automatic Kubernetes controller that performs steps 7–10. If the deployment does not expose the membership API operationally, stop at the verified single recovery node rather than inventing a multi-voter topology by disk copying.

## Loss of quorum

First distinguish **node loss with a healthy quorum** from **actual quorum loss**.

- If a healthy quorum remains, use the normal learner/removal/replacement membership lifecycle and existing snapshot/log reconstruction. Do not perform cluster-wide DR unnecessarily.
- If quorum is lost and cannot be safely restored from the persisted current membership, do not force a stale member to self-declare a new quorum. Fence the old topology and recover from a verified external backup as a fresh cluster generation.

Phase 5 does not implement an unsafe `--force-quorum` shortcut.

## Corrupt local storage on one member

If the cluster still has a healthy quorum and the logical member can be reconstructed safely, prefer the existing Phase-2/3 empty-storage snapshot + suffix recovery path. External disaster restore is for loss of recoverable cluster authority, not routine replacement of one damaged replica.

## Validation after restore

At minimum validate:

- expected tables/catalog and representative row counts/values;
- authentication with expected restored SCRAM users;
- the new recovery node ID and membership generation;
- absence of historical source IDs from the active voter set;
- successful acknowledged post-restore write;
- restart persistence;
- HLC/apply progress indirectly through successful recovery checks/tests and diagnostics available to the integration.

Example SQL smoke checks:

```bash
psql -h 127.0.0.1 -p 5432 -U <restored-user> -d postgres -c 'SELECT 1'
psql -h 127.0.0.1 -p 5432 -U <restored-user> -d postgres -c 'SELECT COUNT(*) FROM <critical-table>'
```

Use application-specific integrity checks as well. A successful command exit is not sufficient evidence that the chosen recovery point is semantically the one the application intended.

## Compatibility matrix

| Input | Current behavior |
|---|---|
| NBBK v1 with supported state/snapshot/identity/membership versions | accepted after strict verification |
| NBEC v1 + correct key + supported inner NBBK/state versions | authenticated, decrypted, then strictly verified |
| NBEC v1 + wrong key or modified authenticated bytes | authentication failure; restore target remains absent |
| newer/unknown NBBK format version | explicitly rejected as unsupported |
| newer/unknown NBEC version or algorithm | explicitly rejected as unsupported |
| unsupported future state-machine/snapshot/identity/membership markers | fail closed; no speculative deserialization |
| truncated/malformed/oversized/trailing-ambiguous artifact | rejected |
| restore into existing target | rejected; no merge/overwrite mode |
| historical source node ID reused as recovery ID | rejected |

Phase 5 does not promise forward compatibility with unknown future backup formats. No migration is inferred. A future migration path must be explicit, versioned, deterministic, tested, and non-destructive to the source backup.

## Backup storage expectations

- Copy only a successfully published and independently verified backup.
- Keep encrypted backups and their keys in **separate** security domains where practical.
- Preserve key availability for the retention lifetime of encrypted backups.
- Use storage durability/versioning appropriate to the environment; NeuralBase does not implement remote-object-store replication itself.
- Re-verify after transfer and before a disaster restore.
- Retain more than one recovery point according to the application's own recovery policy.
- Phase 5 defines no measured RPO/RTO target.

## Wrong key, corruption, or incompatible version

Do not retry restore against a new target until the cause is understood.

- **Wrong key/authentication failure:** select the correct out-of-band key. Do not modify the backup bytes to bypass authentication.
- **Corruption/checksum failure:** use another verified recovery point; do not repair bytes manually.
- **Unsupported version/state:** use a NeuralBase build with an explicitly documented compatible reader/migration path. Do not coerce version fields.

## Ordinary cluster operation

The checked-in Compose/Kubernetes/Helm topology remains a development/research topology. Writes are leader-directed; followers reject persistent table/user mutations before proposal. Reads are local and may lag committed state.

A healthy cluster can reconstruct a known member from empty storage through the tested snapshot + suffix path while keeping the reconstructing member non-serving until catch-up.

## Required repository gates

Before calling a Phase-5 code/documentation head validated:

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
```

CI also validates deployment manifests. Exact-head green CI is evidence only for that exact commit and test scope.

## Explicit non-goals

This Phase-5 runbook does **not** provide or claim:

- point-in-time recovery or archived WAL/Raft-log replay;
- automatic disaster detection/failover/recovery;
- automatic operator-safe node replacement;
- automatic Kubernetes membership reconciliation or HPA safety;
- linearizable arbitrary-follower reads;
- complete authorization/audit/security hardening;
- production SQL HA or production readiness;
- a measured RPO/RTO SLA.

For design limits also read `docs/ARCHITECTURE.md`, `docs/DISTRIBUTED.md`, `docs/THREAT_MODEL.md`, `docs/TESTING.md`, `CONFIDENCE.md`, and `ROADMAP.md`.
