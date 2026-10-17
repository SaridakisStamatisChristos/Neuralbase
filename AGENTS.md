# AGENTS.md

Repository-specific guidance for coding agents and automated contributors.

## Project identity

NeuralBase is an experimental Rust SQL engine. Configured clusters replicate persistent table mutations and SCRAM identity through deterministic Raft-backed state machines, use SQL-aware snapshots, support learner/joint-consensus membership changes, provide tested NBBK/NBEC backup/restore/fresh-cluster recovery, expose explicit session read-consistency modes, support opt-in managed process/Kubernetes membership reconciliation, and provide exact committed-index archived recovery.

Do not turn those scoped guarantees into a claim of general or production SQL HA. Reads default to `Local` and may lag on followers. `Leader` and `Linearizable` reads require the current serving leader and the consensus barrier; arbitrary-follower linearizable routing is not implemented. Managed reconciliation does not make arbitrary Helm/HPA scaling safe. Exact-index recovery does not provide timestamp-target recovery or automatic DR. The online backup coordinator is an in-process API rather than a standalone live-server CLI.

## Toolchain and gates

- Rust: `1.88.0`
- Core test gate: `make test`
- Formatting/Clippy: `make lint`
- Evidence assertions: `make confidence`
- Adversarial suites: `make adversarial`
- PostgreSQL reference suite: `make tpch-correctness`

Run the narrowest relevant tests during iteration and the complete affected gates before finishing.

## Engineering rules

1. Preserve existing scalar/public APIs unless a breaking change is necessary and documented.
2. Prefer explicit errors over silent fallback where persistence, consensus, identity, read consistency, or correctness is involved.
3. Keep queues, waits, joins, and materialized intermediates intentionally bounded.
4. Do not hide deterministic hangs behind timeouts; timeouts are diagnostics/safety guards.
5. Keep local durability distinct from replicated durability in code comments and docs.
6. Snapshot creation must be durable before Raft prefix truncation; snapshot installation must restore durable SQL state before success acknowledgement.
7. Do not enable HPA until deployment reconciliation sequences replica changes through the coordinated membership protocol.
8. Preserve the read contract: `Local` is the no-consensus default; strong modes require current-leader authority, quorum barrier and confirmed local apply, and must never silently downgrade.
9. Do not claim arbitrary-follower linearizability unless routing/ReadIndex/lease semantics and corresponding failure tests are actually implemented.
10. Preserve the recovery contract: PITR targets committed Raft indexes exactly; do not claim timestamp recovery or automatic DR, and do not advance confirmed apply past a required archive-publication failure.
11. Preserve verifier-only replicated identity and explicit digest-selected legacy migration semantics.
12. Update relevant docs with behavioral/configuration changes.
13. Do not commit build logs, Clippy output, temporary databases, secrets, or private keys.

## Documentation map

- Architecture: `docs/ARCHITECTURE.md`
- SQL surface: `docs/SQL_SUPPORT.md`
- Raft/distributed semantics: `docs/DISTRIBUTED.md`
- Deployment/configuration: `docs/DEPLOYMENT.md`
- Runtime defaults and aliases: `docs/CONFIGURATION.md`
- Managed membership: `docs/MANAGED_MEMBERSHIP.md`
- Operator recovery: `ops/RUNBOOK.md`
- Point-in-time recovery: `docs/PITR.md`
- PITR correctness model: `docs/PITR_CORRECTNESS_MODEL.md`
- Performance evidence: `docs/PERFORMANCE_PROFILE.md`
- Metrics/tracing: `observability/README.md`
- Tests/evidence: `docs/TESTING.md`
- Roadmap: `ROADMAP.md`
- Security: `SECURITY.md`

## Definition of done

A substantive change is not done until its semantics, failure cases, tests and documentation agree. Distributed lifecycle work additionally requires exact-head CI and post-merge `main` CI before stronger claims are synchronized.
