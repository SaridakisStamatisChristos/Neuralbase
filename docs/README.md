# NeuralBase documentation

This directory contains the technical documentation for NeuralBase. The root `README.md` is the project entry point; these documents carry the details needed to evaluate, develop, deploy, or review the engine.

## Core documents

| Document | Audience | Scope |
|---|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Developers / reviewers | Major components, replicated mutation flow, snapshot lifecycle, state boundaries, invariants |
| [SQL_SUPPORT.md](SQL_SUPPORT.md) | Users / developers | SQL feature matrix, single-node versus clustered mutation semantics, execution limits |
| [DISTRIBUTED.md](DISTRIBUTED.md) | Distributed-systems reviewers | Raft transport, deterministic table replication, snapshot/bootstrap lifecycle, acknowledgement/apply semantics, remaining HA boundaries |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Operators / evaluators | Single-node, fixed-membership cluster, Kubernetes/Helm, TLS/auth and routing constraints |
| [TESTING.md](TESTING.md) | Contributors / reviewers | CI gates, process-level failover/restart/replacement evidence, TPC-H reference methodology and evidence limits |
| [THREAT_MODEL.md](THREAT_MODEL.md) | Security reviewers | Threats, trust boundaries, mitigations |
| [TSAN.md](TSAN.md) | Contributors | ThreadSanitizer workflow and caveats |

Repository-level material:

- [`../ROADMAP.md`](../ROADMAP.md) — completed Phase 1 and Phase 2 scope plus prioritized remaining engineering work.
- [`../CONFIDENCE.md`](../CONFIDENCE.md) / [`../CONFIDENCE.yaml`](../CONFIDENCE.yaml) — evidence-scoped confidence model and machine-readable replication boundary.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — contributor setup and quality gates.
- [`../SECURITY.md`](../SECURITY.md) — vulnerability reporting policy.
- [`../CHANGELOG.md`](../CHANGELOG.md) — development changelog.
- [`../ops/RUNBOOK.md`](../ops/RUNBOOK.md) — development operations and diagnostics.
- [`../observability/README.md`](../observability/README.md) — metrics and tracing boundary.
- [`../tests/perf/README.md`](../tests/perf/README.md) — benchmark interpretation and reproducibility rules.

## Current distributed claim in one sentence

Configured fixed-membership clusters replicate persistent table CREATE/DROP/INSERT/UPDATE/DELETE through Raft with quorum commit + confirmed durable local apply before success, and the tested Phase 2 lifecycle supports SQL-aware snapshot/compaction plus empty-storage reconstruction of an already-configured fixed logical member from snapshot + retained Raft suffix; this is **not** a production-HA claim and still excludes linearizable follower reads, replicated auth/users, coordinated dynamic membership, automatic replacement orchestration, and backup/restore/PITR/disaster recovery.

## Documentation rule

A change that alters public behavior, configuration, SQL semantics, persistence guarantees, Raft semantics, deployment topology, or test evidence should update the relevant document in the same pull request.

Documentation must distinguish:

1. code that exists;
2. behavior verified by executable tests;
3. deployment behavior exercised in CI;
4. future design intent.

Those categories are intentionally not interchangeable.
