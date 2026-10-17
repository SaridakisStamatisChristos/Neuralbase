# NeuralBase

[![CI](https://github.com/SaridakisStamatisChristos/Neuralbase/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SaridakisStamatisChristos/Neuralbase/actions/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/coverage-79.58%25-brightgreen.svg)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](Cargo.toml)

**NeuralBase is an experimental SQL engine in Rust** with a PostgreSQL wire endpoint, MVCC/RocksDB storage, vectorized and row-oriented execution paths, and Raft-replicated persistent table mutations and SCRAM identity over TCP/TLS. It also contains an ONNX join-order optimizer for library use and benchmarks; the live SQL planner does not currently invoke it.

> [!IMPORTANT]
> NeuralBase is **pre-1.0 research/development software**. Configured clusters provide bounded, executable evidence for replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated membership changes, replicated SCRAM identity, backup/restore and fresh-cluster recovery, explicit read-consistency modes, managed process/Kubernetes membership reconciliation, an executable SQL compatibility profile, and exact committed-index archived recovery. These capabilities do **not** establish production SQL HA, full PostgreSQL compatibility, SQL transaction blocks, arbitrary-follower linearizable reads, HPA-safe scaling, timestamp-target recovery, automatic disaster recovery, or hardware-independent performance guarantees.

## Current capabilities

- PostgreSQL wire endpoint with SCRAM authentication support.
- Parser/binder plus vectorized and row-oriented execution paths.
- MVCC, HLC timestamps, RocksDB persistence and bounded general execution.
- Deterministic Raft-replicated persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- Leader-side concrete `UPDATE`/`DELETE` materialization and follower write rejection.
- Replicated acknowledgement only after quorum commit and confirmed durable local state-machine apply.
- Versioned logical SQL snapshots with safe stage-before-compaction and restore-before-ACK ordering.
- Empty-storage member reconstruction from snapshot plus remaining log suffix.
- Learner admission/catch-up, joint-consensus promotion/removal, durable finalized membership and stale-removed-node protection.
- Replicated `CREATE USER`, `ALTER USER`, and `DROP USER` with leader-side SCRAM derivation and no plaintext password in Raft identity commands.
- Versioned NBBK offline/online backup, independent verification, crash-safe fresh-cluster recovery and authenticated NBEC backup encryption.
- Opt-in archived recovery with exact committed-Raft-index targets, authenticated archive encryption, timeline branching, bounded rollover/retirement and fail-closed replay.
- Session-scoped `Local`, `Leader`, and `Linearizable` reads. Strong modes require the current leader, use a current-term quorum barrier, wait for confirmed durable local apply, and never silently downgrade.
- Opt-in managed process/Kubernetes membership reconciliation with guarded committed-state observations, fresh learner creation/promotion, leader transfer, finalized removal, retained storage and Kubernetes UID/resource-version checks.
- Executable [`SQL_COMPATIBILITY.yaml`](SQL_COMPATIBILITY.yaml) with fail-closed anti-overclaim tests and PostgreSQL 16 semantic differential validation.
- Fail-closed unsupported DML/DDL/window semantics, exactly-one-statement request handling, and explicit unsupported SQL transaction blocks.
- Bounded typed PostgreSQL extended-protocol parameters with NULL/text/selected binary scalar encodings, statement parameter descriptions and real Rust `postgres` client evidence.
- Evidence-driven persistent-query routing that avoids unrelated synthetic TPC-H fixture construction while preserving historical fallback semantics.
- Incremental RocksDB Raft persistence, pipelined replicated-write proposal locking, persistent ordered TCP/TLS peer writers with `TCP_NODELAY`, process-incarnation fencing, durable apply-frontier recovery and randomized campaign retry deadlines.
- Docker Compose, Kubernetes StatefulSet and Helm **development** deployments.

## Quick start

### Single node

Install the [native build prerequisites](CONTRIBUTING.md#development-prerequisites). The repository pins Rust `1.88.0`.

```bash
git clone https://github.com/SaridakisStamatisChristos/Neuralbase.git
cd Neuralbase
cargo build --release --locked --bins
NEURALBASE_LISTEN_ADDR=127.0.0.1:5432 \
NEURALBASE_DB_PATH=./data/neuralbase \
cargo run --release --locked --bin neuralbase
```

Then query it with a PostgreSQL client:

```bash
psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5432 -U anon -d postgres -c 'SELECT 1'
```

Without `NEURALBASE_NODE_ID`, table and user mutations use standalone storage. An unset database path selects in-memory/demo operation; standalone RocksDB open failures also fall back to that mode. DML requires successfully opened storage. Clustered startup instead fails closed if durable storage is unavailable.

Read the [SQL support boundary](docs/SQL_SUPPORT.md) and [`SQL_COMPATIBILITY.yaml`](SQL_COMPATIBILITY.yaml) before using application drivers. Send one statement per request. SQL transaction blocks are unsupported. The typed Parse/Bind subset does not imply arbitrary PostgreSQL parameter OIDs, binary row results, full result metadata or complete extended-protocol error-state compatibility.

### Three-process development topology

```bash
docker compose up --build -d --wait
```

The SQL endpoints are exposed on ports `5432`, `5433`, and `5434`. Each node owns independent RocksDB storage. These are development defaults with authentication disabled. Writes and strong reads need the current leader; neither the first port nor a load-balanced Service is guaranteed to select it. TCP health checks establish listener reachability only. See [deployment](docs/DEPLOYMENT.md).

## Distributed write and identity semantics

Persistent table mutations and clustered user DDL are leader-routed. Followers reject before proposal. A newly elected leader establishes a current-term apply-readiness barrier before materializing state-dependent mutations.

A client-observed replicated success means the entry reached quorum commit and the local state machine confirmed durable apply. A timeout or disconnect after submission is still outcome-uncertain.

For user creation and rotation, plaintext exists only at the SQL/leader boundary needed to derive SCRAM material. Replicated identity commands contain verifier material, not plaintext. PostgreSQL MD5 verifier material is excluded because possession of it is sufficient for MD5 challenge responses.

Typed parameters are deterministically materialized before the established parser/binder and replicated mutation path. This preserves the deterministic mutation boundary; it is not a distributed multi-statement transaction mechanism.

## Read consistency

Read consistency is session-scoped and defaults to local behavior:

```sql
SET neuralbase_read_consistency = local;
SET neuralbase_read_consistency = leader;
SET neuralbase_read_consistency = linearizable;
```

`Local` performs no consensus coordination and may observe only the selected node's locally applied state. `Leader` and `Linearizable` require clustered mode and the current serving Raft leader. Both currently establish a current-term replicated control barrier and proceed only after quorum commit plus confirmed durable local apply. Followers and recovering/non-serving nodes reject strong reads explicitly; NeuralBase never silently downgrades them to `Local`.

The implementation intentionally uses one Raft log barrier per strong read. Automatic follower-to-leader routing, arbitrary-follower linearizable reads and lower-overhead ReadIndex/lease optimization are not implemented.

## Snapshot, membership and managed reconciliation

The logical snapshot contains SQL catalog/data/apply/HLC state and replicated identity state. It is validated and durably staged before log-prefix truncation. Followers restore durable state before acknowledging `InstallSnapshot`.

The consensus layer supports learners and joint old/new voter configurations. Learners must catch up before promotion; removal is coordinated, and the current leader must transfer leadership before being removed. Finalized membership is persisted and removed identities are protected against stale-disk rejoin.

The static Helm/Compose examples do **not** translate replica-count changes into membership. The opt-in [managed membership controller](docs/MANAGED_MEMBERSHIP.md) sequences local processes or per-member Kubernetes StatefulSets through the guarded membership protocol. HPA and arbitrary raw replica changes remain unsupported.

## Backup, restore and exact-index recovery

NBBK/NBEC backup is distinct from internal Raft snapshots. The external CLI provides offline create/verify/restore; online backup is a tested in-process leader coordinator rather than a standalone live-server CLI. Fresh-cluster recovery establishes a new cluster generation rather than restoring in place into a live cluster.

The opt-in archived recovery stream supports baseline/latest or an exact committed Raft index. It does not map wall-clock timestamps to indexes and does not perform automatic disaster detection or recovery. See the [PITR guide](docs/PITR.md) and [correctness model](docs/PITR_CORRECTNESS_MODEL.md).

## Performance evidence

Performance results are scoped to the recorded commit, runner and topology. The same-host three-durable-voter characterization on measured head `4fb3303534bb27c15394f2a7952207b857fad2dd` completed 27/27 cases with zero failed measured operations.

| Scenario | p50 | p95 | Throughput |
| --- | ---: | ---: | ---: |
| Replicated INSERT, 1 client | 0.704 ms | 0.731 ms | 1,436 ops/s |
| Replicated INSERT, 8 clients | 2.168 ms | 2.993 ms | 3,561 ops/s |
| Replicated INSERT, 32 clients | 10.131 ms | 14.253 ms | 3,041 ops/s |
| Replicated INSERT, 64 clients | 25.150 ms | 30.750 ms | 2,538 ops/s |

The persistent-only general-query route remained effectively flat as unrelated catalog noise increased: approximately 0.255 ms p50 with 0 unrelated tables, 0.282 ms with 16, and 0.276 ms with 64. These are CI characterization results on one same-host topology, **not** production-capacity or multi-host guarantees. See the [performance profile](docs/PERFORMANCE_PROFILE.md).

Historical benchmark output tokens retained solely for exact-base/head comparability are documented as historical identifiers; they are not product capability names.

## Capability snapshot

| Capability | Status |
|---|---|
| PostgreSQL wire endpoint | Implemented, partial compatibility surface |
| Parser + binder | Implemented with fail-closed semantic guards |
| Executable SQL compatibility profile | **Implemented and anti-overclaim tested** |
| PostgreSQL-16 selected semantic differential suite | **Implemented** |
| Typed extended Parse/Bind parameter subset | **Implemented and real-client tested** |
| Binary row results / arbitrary OID inference / full extended-protocol recovery | **Not implemented** |
| SQL transaction blocks / distributed multi-statement transactions | **Not implemented** |
| MVCC + HLC + RocksDB | Implemented |
| Replicated persistent table DDL/DML | **Implemented and process-tested** |
| SQL-aware Raft snapshot + recovery | **Implemented and tested** |
| Learner/joint-consensus membership changes | **Implemented and lifecycle-tested** |
| Replicated SCRAM user/auth DDL | **Implemented and process-tested** |
| Legacy identity migration | **Explicit digest-selected SCRAM migration implemented** |
| Session `Local` / `Leader` / `Linearizable` reads | **Implemented and tested** |
| Linearizable arbitrary-follower reads / automatic strong-read routing | **Not implemented** |
| Managed process / Kubernetes membership reconciliation | **Implemented and lifecycle-tested** |
| Arbitrary Helm replica scaling / HPA | **Unsupported** |
| Backup / restore / fresh-cluster recovery | **Implemented and tested** |
| Exact committed-index point-in-time recovery | **Implemented and tested** |
| Timestamp-target recovery / automatic DR | **Not implemented** |
| Production SQL HA | **Not claimed** |

## Verification

```bash
make test
make lint
make confidence
make adversarial
make coverage
make tpch-correctness
make sql-differential
```

Green CI is evidence for the exact checked commit and tested scopes, not a production-readiness or universal PostgreSQL-compatibility claim.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [SQL support](docs/SQL_SUPPORT.md)
- [Executable SQL compatibility profile](SQL_COMPATIBILITY.yaml)
- [Distributed semantics](docs/DISTRIBUTED.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Configuration reference](docs/CONFIGURATION.md)
- [Managed membership reconciliation](docs/MANAGED_MEMBERSHIP.md)
- [Operator recovery runbook](ops/RUNBOOK.md)
- [Exact-index PITR operator guide](docs/PITR.md)
- [PITR correctness model](docs/PITR_CORRECTNESS_MODEL.md)
- [Observability](observability/README.md)
- [Testing and evidence](docs/TESTING.md)
- [Performance profile](docs/PERFORMANCE_PROFILE.md)
- [Threat model](docs/THREAT_MODEL.md)
- [Roadmap](ROADMAP.md)
- [Confidence model](CONFIDENCE.md)
- [Historical development evidence](docs/history/README.md)

## Project maturity

NeuralBase remains pre-1.0. Its current bounded evidence surface covers replicated table mutations, SQL-aware snapshot/recovery, coordinated and managed membership, replicated identity, backup/restore/fresh-cluster recovery, explicit read consistency, an executable SQL semantic/compatibility profile, exact committed-index archive recovery, query-path optimization, and durable-distributed performance work. None of those capabilities should be read as a claim of production HA or complete PostgreSQL compatibility.

## License

Apache-2.0. See [LICENSE](LICENSE).
