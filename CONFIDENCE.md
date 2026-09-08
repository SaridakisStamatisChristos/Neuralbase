# Confidence Report

**Updated:** 2026-09-08

NeuralBase is still pre-1.0 research/development software. The confidence boundary now includes fixed-membership replicated persistent table mutations **and** the tested SQL-aware snapshot/recovery lifecycle for an already-configured fixed member. That remains deliberately narrower than a production-HA database claim.

## Strongest evidence

- PostgreSQL wire-protocol, parser/binder, DML, MVCC/HLC and RocksDB paths have executable integration/adversarial coverage.
- `tests/tpch_correctness.rs` compares checked-in TPC-H Q1-Q22 row-for-row with PostgreSQL 16 on a deterministic small dataset.
- Persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE` are versioned deterministic replicated commands in clustered mode.
- `UPDATE`/`DELETE` predicates are evaluated on the leader after a current-term apply-readiness barrier; followers receive concrete row/key effects.
- Followers reject persistent table writes before proposal.
- Normal Raft client success is withheld until quorum commit and confirmed state-machine apply.
- Replicated SQL apply writes the SQL effect and durable replay marker atomically; replay is idempotent.
- Required Raft persistence and snapshot-staging failures fail-stop the node.
- `tests/replicated_sql_process.rs` exercises three real OS processes with distinct stores through mutation convergence, leader loss/re-election, catch-up, full restart and an acknowledged-write crash race.
- The SQL snapshot codec is versioned, canonical, bounded and SHA-256 checksummed, with adversarial decode validation.
- Snapshot export reads one consistent RocksDB point; restore atomically replaces durable SQL state before publishing volatile catalog/HLC state.
- Snapshot creation is durably staged before Raft prefix truncation; follower installation restores SQL state before success acknowledgement.
- Interrupted follower installation is recovered from durable staged metadata after restart.
- `tests/replicated_sql_snapshot_cycles.rs` exercises repeated snapshot/compaction cycles, retained suffix, restart and continued mutation.
- `tests/replicated_sql_snapshot_bootstrap.rs` deletes one fixed member's entire local database, rebuilds the same logical member from SQL snapshot + Raft suffix, gates SQL serving while catching up, transfers leadership to the reconstructed member, acknowledges a write there, verifies it survives leader loss, then restarts and converges without duplicate MVCC effects.
- `tests/replicated_sql_snapshot_process.rs` extends that lifecycle across three real `neuralbase` OS processes and TCP Raft: one stopped member is durably compacted, another fixed member loses its entire RocksDB directory, the same logical node ID recovers from InstallSnapshot plus retained suffix, restarts from the reconstructed disk, and remains in a quorum after the original snapshot-source leader is killed.
- Helm/default/auth/TLS rendering and unsafe fixed-membership HPA rejection remain CI-gated.

## Replicated SQL scope

`distributed_sql_replication: true` in `CONFIDENCE.yaml` means exactly:

- configured clustered mode with durable RocksDB;
- fixed Raft membership;
- persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE`;
- follower writes rejected rather than locally applied;
- leader acknowledgement only after quorum commit and confirmed local state-machine apply;
- deterministic replicated table apply across separate processes/failover/restart;
- SQL-aware snapshot/compaction with restore-before-InstallSnapshot-ACK semantics;
- empty-storage reconstruction of an **already-configured fixed logical member ID** from snapshot plus remaining log suffix.

It does **not** mean:

- `production_ready: true`;
- linearizable reads from arbitrary followers;
- replicated `CREATE USER`, `ALTER USER`, or `DROP USER`;
- coordinated dynamic membership, learner promotion or safe HPA-driven scaling;
- automatic node replacement/orchestration or arbitrary new-node addition;
- backup/restore, PITR or disaster recovery;
- complete PostgreSQL semantic compatibility;
- production SQL HA.

## Snapshot failure semantics

A snapshot candidate is not allowed to justify log truncation until the complete SQL snapshot is validated and durably staged. On a follower, successful InstallSnapshot means the corresponding SQL state was restored durably before the Raft boundary was published and acknowledged.

A crash during follower installation is represented by a durable typed Installation stage. Restart revalidates/restores/promotes the exact boundary idempotently. Corrupt, truncated, unsupported, regressive or otherwise invalid SQL snapshots fail closed rather than producing a serving node.

Legacy opaque Raft snapshot bytes remain rejected in confirmed-SQL mode when no SQL-aware snapshot store is attached.

## Client failure semantics

An explicit follower rejection occurs before proposal and can be redirected/retried against the current leader. A fresh member still catching up rejects direct replicated-SQL writes rather than mutating/serving partial state.

A failure or timeout **after submission to a leader is outcome-uncertain**. Non-idempotent mutations must not be blindly replayed solely because the client did not receive success.

## Remaining high-value correctness work

1. Coordinated Raft membership changes, using the proven snapshot/bootstrap primitive for catch-up without weakening fixed-member safety.
2. Replicated authentication/user mutations or an equally explicit strongly consistent identity subsystem.
3. Operator-facing backup/restore, verification, PITR and disaster-recovery procedures.
4. Stronger read-consistency modes before describing arbitrary follower reads as current/linearizable.
5. Broader network/storage/upgrade chaos validation and production performance characterization.
6. PostgreSQL compatibility expansion only without weakening the above safety boundaries.

## Confidence interpretation

`CONFIDENCE.yaml` retains an internal regression score for its explicitly scoped engine, fixed-membership replicated-table path and SQL-aware fixed-member lifecycle. It is not a statistical failure probability and it is not a production-readiness score. Executable evidence and the machine-readable limitations take precedence over the number.
