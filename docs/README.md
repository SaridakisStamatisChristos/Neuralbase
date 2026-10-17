# NeuralBase documentation

This directory contains the technical documentation for NeuralBase. The root `README.md` is the project entry point; these documents carry the details needed to evaluate, develop, deploy, or review the engine.

## Core documents

| Document | Scope |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Major components, replicated tables/identity, read consistency, snapshot lifecycle and membership boundaries |
| [SQL_SUPPORT.md](SQL_SUPPORT.md) | SQL feature matrix, session read modes and standalone versus clustered mutation semantics |
| [DISTRIBUTED.md](DISTRIBUTED.md) | Raft acknowledgement/apply, read barriers, snapshots, membership changes and identity consistency |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Single-node, Kubernetes/Helm, migration, TLS and scaling constraints |
| [CONFIGURATION.md](CONFIGURATION.md) | Runtime defaults, aliases, TLS precedence, admission controls and fixed limits |
| [TESTING.md](TESTING.md) | CI gates and executable evidence limits |
| [THREAT_MODEL.md](THREAT_MODEL.md) | Threats, trust boundaries and residual risks |
| [MANAGED_MEMBERSHIP.md](MANAGED_MEMBERSHIP.md) | Opt-in managed process/Kubernetes membership reconciliation profile |
| [PITR.md](PITR.md) | Archived recovery stream, exact-index recovery, encryption, branching and retention |
| [PITR_CORRECTNESS_MODEL.md](PITR_CORRECTNESS_MODEL.md) | Correctness boundary for committed-index archive/replay recovery |
| [PERFORMANCE_PROFILE.md](PERFORMANCE_PROFILE.md) | Reproducible query-path performance evidence and limitations |
| [history/](history/) | Historical implementation/closure evidence; non-authoritative for current product claims |
| [`../ops/RUNBOOK.md`](../ops/RUNBOOK.md) | Tested backup/restore, strong-read validation and disaster-recovery operator procedure |
| [TSAN.md](TSAN.md) | ThreadSanitizer workflow and caveats |
| [`../observability/README.md`](../observability/README.md) | Metrics actually emitted, development services and tracing limitations |
| [`../tests/perf/README.md`](../tests/perf/README.md) | Benchmark commands and historical measurement boundaries |
| [`../fuzz/README.md`](../fuzz/README.md) | Optional fuzz targets and local campaign commands |

Repository-level material includes [`../ROADMAP.md`](../ROADMAP.md), [`../CONFIDENCE.md`](../CONFIDENCE.md), [`../CONFIDENCE.yaml`](../CONFIDENCE.yaml), [`../CHANGELOG.md`](../CHANGELOG.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`../SECURITY.md`](../SECURITY.md).

## Current distributed claim in one sentence

Configured clusters replicate persistent table mutations and SCRAM identity through Raft with quorum commit plus confirmed durable local apply before success; SQL-aware snapshots preserve table and identity state; learner/joint-consensus membership transitions, operator backup/restore/fresh-cluster recovery, leader-path `Local`/`Leader`/`Linearizable` read modes, opt-in managed process/Kubernetes membership reconciliation, and exact committed-index archived recovery are implemented and tested within their documented scopes. This excludes linearizable reads from arbitrary followers, automatic strong-read routing, timestamp-target recovery/automatic DR, arbitrary Helm/HPA scaling, rolling-upgrade orchestration, and production-HA claims.

## Documentation rule

Changes to public behavior, configuration, persistence, consensus, identity, read consistency, deployment topology or executable evidence should update the relevant document in the same pull request. Documentation must distinguish code that exists, behavior verified by tests, deployment behavior exercised in CI, historical evidence, and future design intent.
