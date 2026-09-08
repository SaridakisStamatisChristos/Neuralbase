# NeuralBase documentation

This directory contains the technical documentation for NeuralBase. The root `README.md` is the project entry point; these documents carry the details needed to evaluate, develop, deploy, or review the engine.

## Core documents

| Document | Scope |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Major components, replicated tables/identity, snapshot lifecycle and membership boundaries |
| [SQL_SUPPORT.md](SQL_SUPPORT.md) | SQL feature matrix and standalone versus clustered mutation semantics |
| [DISTRIBUTED.md](DISTRIBUTED.md) | Raft acknowledgement/apply, snapshots, membership changes and identity consistency |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Single-node, Kubernetes/Helm, migration, TLS and scaling constraints |
| [TESTING.md](TESTING.md) | CI gates and executable evidence limits |
| [THREAT_MODEL.md](THREAT_MODEL.md) | Threats, trust boundaries and residual risks |
| [TSAN.md](TSAN.md) | ThreadSanitizer workflow and caveats |

Repository-level material includes [`../ROADMAP.md`](../ROADMAP.md), [`../CONFIDENCE.md`](../CONFIDENCE.md), [`../CONFIDENCE.yaml`](../CONFIDENCE.yaml), [`../CHANGELOG.md`](../CHANGELOG.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`../SECURITY.md`](../SECURITY.md).

## Current distributed claim in one sentence

Configured clusters replicate persistent table mutations and SCRAM identity through Raft with quorum commit + confirmed durable local apply before success; SQL-aware snapshots preserve both table and identity state; learner/joint-consensus membership transitions are implemented and tested; this still excludes linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, backup/PITR/disaster recovery, and production-HA claims.

## Documentation rule

Changes to public behavior, configuration, persistence, consensus, identity, deployment topology or executable evidence should update the relevant document in the same pull request. Documentation must distinguish code that exists, behavior verified by tests, deployment behavior exercised in CI, and future design intent.
