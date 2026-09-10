from pathlib import Path


def repl(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    if old not in text:
        raise SystemExit(f"documentation anchor not found in {path}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1))


# Root README: update the public boundary only after Phase-5 executable evidence.
repl(
    "README.md",
    "backup/PITR/disaster-recovery workflows remain open, and authorization/security hardening is incomplete.",
    "operator backup/restore and fresh-cluster disaster recovery are implemented and tested to the documented Phase-5 scope; PITR, automatic disaster recovery, and broader authorization/security hardening remain open.",
)
repl(
    "README.md",
    "- Real multi-process failover/restart coverage for SQL state and replicated authentication.\n- PostgreSQL 16 row-for-row TPC-H Q1-Q22 reference checks at a small deterministic scale.",
    "- Real multi-process failover/restart coverage for SQL state and replicated authentication.\n- Versioned NBBK offline/online backup, independent verification, crash-safe fresh-cluster restore, and authenticated NBEC backup encryption.\n- Fresh-generation cluster recovery through one restored authority plus learner catch-up/promotion, failover and restart evidence.\n- PostgreSQL 16 row-for-row TPC-H Q1-Q22 reference checks at a small deterministic scale.",
)
repl(
    "README.md",
    "| Backup / PITR / disaster recovery | **Not implemented** |",
    "| Backup / restore / fresh-cluster DR | **Implemented and tested to Phase-5 scope** |\n| Point-in-time recovery / automatic DR | **Not implemented** |",
)
repl(
    "README.md",
    "Phases 1–4 close replicated table mutations, SQL-aware snapshot/recovery, coordinated membership, and replicated identity under the repository's tested failure model. The next high-value correctness work is operator-facing recovery/backup and stronger read-consistency semantics, followed by automatic membership orchestration and broader production hardening.",
    "Phases 1–5 close replicated table mutations, SQL-aware snapshot/recovery, coordinated membership, replicated identity, and the documented operator backup/restore/fresh-cluster DR model under the repository's tested failure model. The next high-value correctness work is explicit stronger read-consistency semantics, followed by automatic membership orchestration and broader production hardening. PITR remains a later recovery extension.",
)

# Documentation index.
repl(
    "docs/README.md",
    "| [THREAT_MODEL.md](THREAT_MODEL.md) | Threats, trust boundaries and residual risks |",
    "| [THREAT_MODEL.md](THREAT_MODEL.md) | Threats, trust boundaries and residual risks |\n| [`../ops/RUNBOOK.md`](../ops/RUNBOOK.md) | Tested backup/restore and disaster-recovery operator procedure |",
)
repl(
    "docs/README.md",
    "Configured clusters replicate persistent table mutations and SCRAM identity through Raft with quorum commit + confirmed durable local apply before success; SQL-aware snapshots preserve both table and identity state; learner/joint-consensus membership transitions are implemented and tested; this still excludes linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, backup/PITR/disaster recovery, and production-HA claims.",
    "Configured clusters replicate persistent table mutations and SCRAM identity through Raft with quorum commit + confirmed durable local apply before success; SQL-aware snapshots preserve both table and identity state; learner/joint-consensus membership transitions and Phase-5 operator backup/restore/fresh-cluster DR are implemented and tested; this still excludes PITR, automatic DR, linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, and production-HA claims.",
)

# Architecture.
repl(
    "docs/ARCHITECTURE.md",
    "- **Local reads remain local.** Arbitrary follower reads are not claimed linearizable.",
    "- **Local reads remain local.** Arbitrary follower reads are not claimed linearizable.\n- **Operator recovery is a separate artifact lifecycle.** NBBK/NBEC backup verification and fresh-cluster restore do not reuse raw internal Raft snapshot bytes or copied consensus disks.",
)
repl(
    "docs/ARCHITECTURE.md",
    "## Read-consistency boundary",
    """## Operator backup and disaster-recovery architecture

`src/backup.rs` defines the explicit versioned NBBK logical backup envelope. Offline capture reuses the deterministic SQL/identity snapshot machinery but adds committed membership/recovery metadata and operator-facing integrity/version semantics. `src/online_backup.rs` coordinates a leader barrier and requires one unchanged durable Raft/state-machine capture boundary.

`src/backup_encryption.rs` wraps logical backups in authenticated NBEC v1 ChaCha20-Poly1305 containers. Key bytes are supplied out of band from an exact raw 32-byte key file for the CLI path and are not embedded in the artifact. Plaintext and encrypted online creation share one capture path; encrypted offline/online publication share one authenticated publication primitive.

`src/restore.rs` verifies before target creation, builds one fresh recovery authority in a hidden sibling stage, validates the complete staged RocksDB state, then atomically publishes the new target. Historical source IDs are tombstoned and a fresh recovery membership generation is established. Stale `.restore-partial-*` crash remnants are never resumed automatically; a retry builds a fresh stage.

Cluster rebuilding deliberately starts from that single fresh authority. Additional nodes must join as empty-disk fresh-ID learners through the existing membership/snapshot path and be promoted normally. Copying one restored RocksDB directory to manufacture voters is outside the design and unsafe.

`OnlineBackupCoordinator` is currently an in-process runtime API rather than a standalone live-server CLI endpoint. The external `neuralbase-backup` command is the offline create/verify/restore tool.

## Read-consistency boundary""",
)
repl(
    "docs/ARCHITECTURE.md",
    "- `src/raft_persistence.rs` — RocksDB-backed Raft stable state and staged snapshots.",
    "- `src/raft_persistence.rs` — RocksDB-backed Raft stable state and staged snapshots.\n- `src/backup.rs` / `offline_backup.rs` / `online_backup.rs` — versioned operator backup contract and consistent capture.\n- `src/backup_encryption.rs` — authenticated NBEC container, key-file validation and encrypted publication.\n- `src/restore.rs` — fresh-generation staged restore and atomic target publication.",
)
repl(
    "docs/ARCHITECTURE.md",
    "Executable evidence supports replicated persistent tables, SQL-aware snapshot/recovery, coordinated membership changes and replicated SCRAM identity. Stronger production claims still require defined stronger read consistency, operator-facing backup/disaster recovery, automated deployment membership reconciliation, broader security/authorization, chaos/upgrade validation and production performance characterization.",
    "Executable evidence supports replicated persistent tables, SQL-aware snapshot/recovery, coordinated membership changes, replicated SCRAM identity, and the documented Phase-5 backup/restore/fresh-cluster DR model. Stronger production claims still require defined stronger read consistency, automatic deployment membership reconciliation, PITR/automatic DR if desired, broader security/authorization, chaos/upgrade validation and production performance characterization.",
)

# Distributed semantics.
repl(
    "docs/DISTRIBUTED.md",
    "## Deployment boundary",
    """## Operator backup and fresh-cluster recovery

Phase 5 adds a distinct operator recovery lifecycle. NBBK v1 contains a bounded/checksummed logical SQL+identity snapshot, explicit compatibility/boundary metadata and committed membership recovery semantics. Offline creation requires the source RocksDB lock to be free. Online creation is leader-coordinated around a confirmed barrier and stable durable frontier; concurrent activity must order around the recorded boundary or make the attempt retry/fail closed.

NBEC v1 provides authenticated ChaCha20-Poly1305 encryption around the logical backup. Wrong-key/tampered artifacts fail authentication before restore target creation. Keys are out-of-band and are never stored inside the backup.

Restore is fresh-target-only. It creates one fresh single-voter recovery generation, tombstones historical source IDs, preserves the backed-up logical/Raft boundary, and publishes only after complete staged verification. Cluster recovery then adds fresh learners through the existing snapshot/log and joint-consensus membership lifecycle. It never copies one restored consensus disk to create several voters.

Interrupted restore remnants remain hidden non-authoritative stages and are never automatically resumed. A retry constructs a fresh stage. The operator procedure and compatibility/key rules are in `ops/RUNBOOK.md`.

## Deployment boundary""",
)
repl(
    "docs/DISTRIBUTED.md",
    "- replicated SCRAM identity, strict migration, failover rotation/drop and process-level authentication convergence.\n\nStill open before stronger production claims: automatic operator membership reconciliation, linearizable/defined stronger reads, backup/PITR/disaster recovery, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.",
    "- replicated SCRAM identity, strict migration, failover rotation/drop and process-level authentication convergence;\n- versioned offline/online backup, independent verification, authenticated encrypted backup, fresh-target restore and fresh-generation cluster recovery.\n\nStill open before stronger production claims: automatic operator membership reconciliation, linearizable/defined stronger reads, PITR and automatic DR, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.",
)

# Deployment.
repl(
    "docs/DEPLOYMENT.md",
    "## Membership and scaling",
    """## Backup and recovery operations

The external `neuralbase-backup` binary supports offline NBBK/NBEC creation, independent verification, and fresh-target recovery. Offline creation intentionally fails while another NeuralBase/RocksDB process owns the selected database. See `ops/RUNBOOK.md` for exact commands, encryption-key handling, interruption behavior, compatibility rules, and complete-cluster-loss recovery.

The leader-coordinated online backup implementation is currently an in-process `OnlineBackupCoordinator` API. It is not exposed as a standalone live-server CLI endpoint. Deployment automation must not claim otherwise.

Restored clusters begin from exactly one fresh recovery authority. Additional members must be fresh learners admitted/caught-up/promoted through the consensus membership API. Do not clone the restored PVC/directory into multiple voters.

## Membership and scaling""",
)
repl(
    "docs/DEPLOYMENT.md",
    "Current manifests demonstrate packaging for the tested replicated-table/snapshot/membership/identity engine. Production readiness still requires operator-tested membership reconciliation, stronger read-consistency modes, backup/PITR/disaster recovery, target-environment security review, broader fault/upgrade validation and production performance characterization.",
    "Current manifests demonstrate packaging for the tested replicated-table/snapshot/membership/identity engine. Phase-5 manual backup/restore/fresh-cluster DR is tested separately from deployment automation. Production readiness still requires automatic/operator-integrated membership reconciliation, stronger read-consistency modes, target-environment security review, broader fault/upgrade validation and production performance characterization; PITR and automatic DR remain unimplemented.",
)

# Testing and evidence.
repl(
    "docs/TESTING.md",
    "## Confidence gate",
    """## Phase 5 backup/restore/DR evidence

Phase-5 focused and integration suites cover the explicit NBBK codec/limits/checksums; offline source locking and atomic publication; independent verification classifications; fresh-target restore of SQL/catalog/HLC/apply/identity state; fresh recovery membership generation and stale-source tombstones; leader-coordinated online boundary capture under SQL/identity/membership/compaction/leadership races; authenticated NBEC encryption, wrong-key/tamper rejection and restrictive permissions; interrupted backup publication and representative stale restore stages; and full fresh-generation cluster rebuilding through learner catch-up/promotion, failover and restart.

`tests/phase5_recovery_process.rs` uses real `neuralbase` and `neuralbase-backup` OS processes/binaries for backup, restore, authentication, post-restore write and restart evidence. `tests/phase5_cluster_recovery.rs` exercises the wider restored-cluster lifecycle with independent RocksDB-backed Raft nodes.

Online backup tests exercise the in-process `OnlineBackupCoordinator`; this is not evidence for a standalone live-server backup CLI endpoint.

## Confidence gate""",
)
repl(
    "docs/TESTING.md",
    "It still does **not** mean production readiness, complete PostgreSQL compatibility, linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, backup/PITR/disaster recovery, security certification, or performance superiority outside measured workloads.",
    "It still does **not** mean production readiness, complete PostgreSQL compatibility, linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, PITR or automatic DR, security certification, or performance superiority outside measured workloads.",
)

# Threat model.
repl(
    "docs/THREAT_MODEL.md",
    "Internal cluster snapshot catch-up is not an operator backup/PITR/disaster-recovery product.",
    """Internal cluster snapshot catch-up is not itself an operator backup product. Phase 5 adds distinct NBBK/NBEC operator artifacts and a fresh-cluster restore lifecycle.

## Operator backup security

Operator backups contain application rows, catalog/state metadata and replicated SCRAM verifier material, so they remain sensitive even though plaintext user passwords are not represented in the replicated identity snapshot.

NBBK is integrity-protected but plaintext. NBEC v1 adds authenticated ChaCha20-Poly1305 confidentiality/integrity. The 32-byte decryption key is supplied out of band; it is not embedded in the artifact. The Unix key loader rejects group/other-readable key files, key debug output is redacted, and executable CLI evidence checks that raw key bytes are absent from normal command output. Wrong keys and authenticated-byte tampering fail before restore target creation.

NBEC v1 does not carry a key identifier. External key inventory/rotation is therefore an operator responsibility. Old keys must remain available while backups encrypted under them are retained. RocksDB databases and internal Raft logs remain outside this backup-container encryption boundary and are not transparently encrypted at rest.""",
)
repl(
    "docs/THREAT_MODEL.md",
    "Stronger production claims require secure-by-default deployment profiles, certificate/secret lifecycle, richer authorization/auditability, automatic membership reconciliation, operator-facing backup/PITR/disaster recovery, defined stronger read modes, broader partition/storage/upgrade chaos testing, and production resource/performance characterization.",
    "Stronger production claims require secure-by-default deployment profiles, certificate/secret lifecycle, richer authorization/auditability, automatic membership reconciliation, defined stronger read modes, broader partition/storage/upgrade chaos testing, and production resource/performance characterization. PITR and automatic DR remain unimplemented; Phase-5 backup encryption does not imply general database-at-rest encryption.",
)
repl(
    "docs/THREAT_MODEL.md",
    "- `tests/raft_persistence_fail_closed.rs` and replicated SQL snapshot/process suites — durability/failure evidence.\n- `CONFIDENCE.md` / `CONFIDENCE.yaml` — machine-readable claim boundary.",
    "- `tests/raft_persistence_fail_closed.rs` and replicated SQL snapshot/process suites — durability/failure evidence.\n- `src/backup*.rs`, `src/offline_backup.rs`, `src/online_backup.rs`, `src/restore.rs` and `tests/phase5_*` — operator recovery/security evidence.\n- `CONFIDENCE.md` / `CONFIDENCE.yaml` — machine-readable claim boundary.",
)

# Roadmap: Phase 5 becomes completed, PITR remains later.
repl(
    "ROADMAP.md",
    "The next major boundaries are operator-facing recovery and stronger read consistency.",
    "The next major boundary is explicit stronger read consistency; operator-facing backup/restore/fresh-cluster disaster recovery is now covered by the tested Phase-5 scope.",
)
repl(
    "ROADMAP.md",
    """## P0 — operational recovery

Build operator-facing recovery on top of the verified logical snapshot machinery:

- offline and online consistent backup;
- checksums and backup verification;
- restore into a single node and controlled cluster bootstrap;
- explicit version-compatibility rules;
- interrupted/corrupt backup and restore tests;
- disaster-recovery runbook;
- point-in-time recovery and archived replicated-log/WAL-equivalent stream later.

Internal Raft snapshot catch-up is **not** a backup product by itself.
""",
    """## Completed Phase 5 — operational backup / restore / disaster recovery

- [x] Explicit versioned NBBK logical backup envelope with bounded canonical metadata and integrity validation.
- [x] Offline consistent backup with RocksDB lock enforcement, restrictive staged publication and independent verification.
- [x] Leader-coordinated online consistent backup with exact committed/applied boundary and race/fail-closed evidence.
- [x] Independent verification with corruption/truncation/unsupported-version classification.
- [x] Fresh-target single-node restore preserving SQL/catalog/HLC/apply/replicated identity state.
- [x] Fresh recovery membership generation with historical source-ID tombstones.
- [x] Fresh-cluster rebuild through learner catch-up/promotion, new writes, leader loss and full restart.
- [x] Authenticated NBEC v1 encryption, strict key-file handling, wrong-key/tamper rejection and restrictive permissions.
- [x] Interrupted backup publication and stale restore-stage fail-closed evidence.
- [x] Compatibility/key semantics and operator disaster-recovery runbook.
- [x] Real OS-process backup/restore/auth/write/restart evidence.

Internal Raft snapshot catch-up remains distinct from operator backup. The standalone backup CLI is offline; online backup is currently an in-process coordinator API.

### Later recovery extension — PITR

Archived replicated-log/WAL-equivalent streaming and point-in-time recovery remain separate future work. Phase 5 does not infer PITR from retained Raft logs.
""",
)
repl(
    "ROADMAP.md",
    "- backup encryption and secret management;",
    "- broader secret-management integration and backup key lifecycle automation;",
)

# Confidence prose.
repl(
    "CONFIDENCE.md",
    "NeuralBase remains pre-1.0 research/development software. The evidence boundary now includes replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated Raft membership changes, and strongly consistent replicated SCRAM identity. It remains deliberately narrower than a production-HA database claim.",
    "NeuralBase remains pre-1.0 research/development software. The evidence boundary now includes replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated Raft membership changes, strongly consistent replicated SCRAM identity, and the documented Phase-5 operator backup/restore/fresh-cluster DR model. It remains deliberately narrower than a production-HA database claim.",
)
repl(
    "CONFIDENCE.md",
    "- Strict legacy migration requires an exact `NEURALBASE_IDENTITY_MIGRATION_SHA256`; malformed, duplicate, MD5 or digest-mismatched input fails closed.\n- PostgreSQL 16 TPC-H Q1-Q22 reference comparison remains part of CI at a deterministic small scale.",
    "- Strict legacy migration requires an exact `NEURALBASE_IDENTITY_MIGRATION_SHA256`; malformed, duplicate, MD5 or digest-mismatched input fails closed.\n- Phase 5 adds versioned offline/online backup, independent verification, authenticated NBEC encryption, fresh-target restore, fresh-generation cluster rebuild, interruption evidence and a real-process recovery path.\n- PostgreSQL 16 TPC-H Q1-Q22 reference comparison remains part of CI at a deterministic small scale.",
)
repl(
    "CONFIDENCE.md",
    "- backup/restore, PITR or disaster recovery;",
    "- PITR or automatic disaster recovery beyond the documented manual fresh-cluster Phase-5 procedure;",
)

# Machine-readable confidence claim remains conservative.
repl(
    "CONFIDENCE.yaml",
    "    auth_replication: true\n    replicated_auth_method: scram-sha-256\n    automatic_membership_reconciliation: false",
    "    auth_replication: true\n    replicated_auth_method: scram-sha-256\n    operator_backup_restore: true\n    offline_backup: true\n    online_backup: true\n    encrypted_backup: true\n    fresh_cluster_dr: true\n    pitr: false\n    automatic_dr: false\n    automatic_membership_reconciliation: false",
)
repl(
    "CONFIDENCE.yaml",
    "    This does not imply production HA, linearizable follower reads, automatic\n    Kubernetes membership reconciliation, backup/restore, PITR, or disaster recovery.",
    "    This does not imply production HA, linearizable follower reads, automatic\n    Kubernetes membership reconciliation, PITR, or automatic disaster recovery.",
)
repl(
    "CONFIDENCE.yaml",
    "      Persistent table CREATE/DROP/INSERT/UPDATE/DELETE and SQL-aware snapshot\n      recovery are tested. Reads remain local and may be stale; backup/restore,\n      disaster recovery and production HA are not implemented.",
    "      Persistent table CREATE/DROP/INSERT/UPDATE/DELETE and SQL-aware snapshot\n      recovery are tested. Reads remain local and may be stale; production HA and\n      stronger follower-read guarantees are not implemented.",
)
repl(
    "CONFIDENCE.yaml",
    "  - artifact: deployment_manifests",
    """  - artifact: backup_recovery
    effective: 0.76
    status: offline_online_encrypted_fresh_cluster_dr_tested
    criticality: release_boundary
    evidence:
      - src/backup.rs
      - src/offline_backup.rs
      - src/online_backup.rs
      - src/backup_encryption.rs
      - src/restore.rs
      - src/bin/neuralbase-backup.rs
      - tests/phase5_online_backup_interactions.rs
      - tests/phase5_cluster_recovery.rs
      - tests/phase5_recovery_process.rs
      - tests/phase5_encrypted_ops.rs
    limitation: >-
      The external CLI provides offline create/verify/restore. Online backup is a
      tested in-process leader coordinator, not a standalone live-server CLI.
      Recovery creates a fresh cluster generation; it is not in-place restore into
      an existing live cluster. PITR and automatic DR are not implemented.

  - artifact: deployment_manifests""",
)
repl(
    "CONFIDENCE.yaml",
    "  - Add operator-facing backup/restore and disaster-recovery workflows using verified snapshot machinery where appropriate.\n  - Add stronger read-consistency modes",
    "  - Add stronger read-consistency modes",
)

# Make the machine-readable boundary executable.
repl(
    "tests/confidence_yaml.rs",
    "    assert_eq!(scope[\"auth_replication\"].as_bool(), Some(true));\n    assert_eq!(\n        scope[\"replicated_auth_method\"].as_str(),\n        Some(\"scram-sha-256\")\n    );",
    "    assert_eq!(scope[\"auth_replication\"].as_bool(), Some(true));\n    assert_eq!(\n        scope[\"replicated_auth_method\"].as_str(),\n        Some(\"scram-sha-256\")\n    );\n    assert_eq!(scope[\"operator_backup_restore\"].as_bool(), Some(true));\n    assert_eq!(scope[\"offline_backup\"].as_bool(), Some(true));\n    assert_eq!(scope[\"online_backup\"].as_bool(), Some(true));\n    assert_eq!(scope[\"encrypted_backup\"].as_bool(), Some(true));\n    assert_eq!(scope[\"fresh_cluster_dr\"].as_bool(), Some(true));\n    assert_eq!(scope[\"pitr\"].as_bool(), Some(false));\n    assert_eq!(scope[\"automatic_dr\"].as_bool(), Some(false));",
)
repl(
    "tests/confidence_yaml.rs",
    "    let identity = artifacts\n        .iter()\n        .find(|item| item[\"artifact\"].as_str() == Some(\"replicated_identity\"))\n        .expect(\"replicated_identity boundary must be explicit\");\n    assert_eq!(\n        identity[\"status\"].as_str(),\n        Some(\"scram_verifier_replication_and_migration_tested\")\n    );",
    "    let identity = artifacts\n        .iter()\n        .find(|item| item[\"artifact\"].as_str() == Some(\"replicated_identity\"))\n        .expect(\"replicated_identity boundary must be explicit\");\n    assert_eq!(\n        identity[\"status\"].as_str(),\n        Some(\"scram_verifier_replication_and_migration_tested\")\n    );\n\n    let recovery = artifacts\n        .iter()\n        .find(|item| item[\"artifact\"].as_str() == Some(\"backup_recovery\"))\n        .expect(\"backup_recovery boundary must be explicit\");\n    assert_eq!(\n        recovery[\"status\"].as_str(),\n        Some(\"offline_online_encrypted_fresh_cluster_dr_tested\")\n    );",
)

# Changelog.
repl(
    "CHANGELOG.md",
    "### Documentation and deployment",
    """### Operational backup / restore / disaster recovery — Phase 5

- Added explicit versioned NBBK logical backup artifacts rather than exposing raw internal Raft snapshot bytes.
- Added offline backup with source-lock enforcement, independent verification, restrictive atomic publication and strict compatibility/corruption handling.
- Added leader-coordinated online backup with one committed/applied logical boundary and race/fail-closed coverage.
- Added crash-safe fresh-target restore with SQL/catalog/HLC/apply/replicated-identity recovery, fresh membership generation and historical-node tombstones.
- Added fresh-generation cluster rebuild through learner catch-up/promotion, new writes, leader failover and full restart.
- Added authenticated NBEC v1 backup encryption, raw 32-byte key-file handling, wrong-key/tamper rejection and restrictive permissions.
- Added interruption evidence, real-process recovery evidence, compatibility/key semantics and an operator DR runbook.
- PITR, automatic DR and a standalone live-server online-backup CLI remain out of scope.

### Documentation and deployment""",
)
repl(
    "CHANGELOG.md",
    "- Backup/restore, PITR and disaster-recovery workflows are not implemented by the internal snapshot catch-up path.",
    "- Manual Phase-5 backup/restore/fresh-cluster DR is implemented and tested; PITR and automatic disaster recovery remain unimplemented.",
)

# Agent and security guidance were stale even before Phase 5.
repl(
    "AGENTS.md",
    "NeuralBase is an experimental Rust SQL engine. Configured fixed-membership clusters replicate persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE` through a deterministic Raft-backed SQL state machine. The Phase 2 lifecycle also has a SQL-aware logical snapshot path for safe compaction and recovery of an already-configured fixed logical member from empty local storage plus the remaining Raft suffix.\n\nDo not turn those scoped guarantees into a claim of general or production SQL HA. User/auth mutations remain per-node, reads are local and may lag, dynamic membership is not coordinated, automatic node replacement is not implemented, and backup/disaster-recovery workflows remain open.",
    "NeuralBase is an experimental Rust SQL engine. Configured clusters replicate persistent table mutations and SCRAM identity through deterministic Raft-backed state machines, use SQL-aware snapshots, support learner/joint-consensus membership changes, and provide the tested Phase-5 NBBK/NBEC backup/restore/fresh-cluster recovery lifecycle.\n\nDo not turn those scoped guarantees into a claim of general or production SQL HA. Reads are still local and may lag, deployment membership reconciliation and automatic node replacement are not implemented, PITR/automatic DR remain open, and the online backup coordinator is currently an in-process API rather than a standalone live-server CLI.",
)
repl(
    "AGENTS.md",
    "7. Do not enable HPA for the fixed-membership Raft deployment until coordinated membership changes exist.",
    "7. Do not enable HPA until deployment reconciliation sequences replica changes through the existing coordinated membership protocol.",
)
repl(
    "SECURITY.md",
    "- SQL data and user-registry state are per-node; they are not yet replicated through Raft.",
    "- Clustered SQL table state and SCRAM identity are replicated through Raft; standalone mode retains its local behavior. Operator backups therefore contain sensitive database and verifier material.",
)
repl(
    "SECURITY.md",
    "- dependency/supply-chain vulnerabilities.",
    "- dependency/supply-chain vulnerabilities;\n- operator backup confidentiality/integrity, recovery fencing and key handling.",
)
