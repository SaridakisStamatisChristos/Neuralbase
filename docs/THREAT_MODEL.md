# NeuralBase threat model

**Updated:** 2026-09-07  
**Scope:** pre-1.0 single-node and fixed-membership three-node development/research deployments

This document is a risk inventory, not a security certification. NeuralBase should not be exposed as a production database solely because a mitigation exists in code.

## Assets

| Asset | Why it matters |
|---|---|
| SQL query text | May contain application data or secrets embedded as literals |
| Row/catalog data in RocksDB | Primary application data |
| Replicated SQL / Raft log entries | Can contain deterministic row/catalog mutation payloads; integrity and confidentiality matter |
| Raft term/vote/log metadata | Consensus safety depends on durable integrity |
| Credential registry | Per-node authentication state and password verifiers |
| TLS private keys / CA material | Protect SQL or node-to-node transport when TLS is enabled |
| Metrics / diagnostics | Can reveal topology, load, and operational state |

## Trust boundaries

1. **PostgreSQL client -> SQL listener.** Client input is untrusted; authentication and SQL TLS are configuration-dependent.
2. **Raft node -> Raft node.** Default TCP transport is not encrypted/authenticated. Feature-gated Raft TLS uses mutual certificate authentication with a configured cluster CA.
3. **Process -> local disk.** RocksDB and user-registry files are trusted durable state; NeuralBase does not claim transparent encryption at rest.
4. **Operator configuration -> cluster membership/routing.** Peer IDs, addresses, certificate paths, DB paths, and user files are trusted operator inputs.

## SQL and wire-protocol risks

SQL text is parsed into an AST and bound before execution; there is no intended raw-string SQL execution layer inside the engine. That reduces classic internal string-concatenation injection risk but does **not** make arbitrary application SQL safe. Applications remain responsible for how they construct query text, and NeuralBase does not claim complete PostgreSQL extended-protocol/parameter compatibility.

Protocol frame lengths are validated and connection admission limits exist, but resource-exhaustion risks remain for expensive queries and high concurrency. General execution has bounded intermediate-row controls; these are not a complete per-query memory/CPU isolation system.

## Authentication and authorization risks

SCRAM-SHA-256/MD5-compatible authentication support exists, but authentication is configuration-dependent and the current authorization model does not provide a production-grade row/column policy system.

`CREATE USER`, `ALTER USER`, and `DROP USER` remain **per-node** in the current replicated-table phase. This creates an explicit security/consistency risk: two SQL nodes can have different credential registries even when their table state has converged.

Do not assume a cluster-wide identity/security policy until user/auth mutations are replicated or an external strongly consistent identity design is adopted.

## SQL client TLS

SQL TLS is feature/configuration dependent rather than a safe-by-default production posture. Without TLS, a network-positioned adversary can observe or modify plaintext PostgreSQL traffic.

Operators using sensitive data should enable TLS with reviewed certificates/keys and restrict network exposure. Certificate lifecycle, rotation, revocation, and secret-management procedures remain operator responsibilities.

## Raft transport security

The default Raft transport is TCP and should be treated as trusted-network-only.

With the `tls` feature and `NEURALBASE_RAFT_TLS=1`, node transport uses rustls. The server-side acceptor requires a client certificate signed by the configured cluster CA; the client side presents its certificate and validates the peer against the configured CA/server name. This is mutual TLS transport protection.

Residual risks remain:

- certificate provisioning/rotation/revocation are not automated;
- logical Raft ID authorization is not a substitute for certificate identity design;
- a compromised node with valid cluster credentials can send protocol-valid Raft traffic;
- there is no formal TLA+ security/safety proof.

## Replicated-log confidentiality

Replicated table commands contain concrete catalog/row mutation effects. Raft logs and their RocksDB persistence should therefore be treated as sensitive application data, not harmless metadata.

Node-to-node TLS protects the transport when enabled, but NeuralBase does not claim encryption at rest for RocksDB/Raft state. Disk snapshots/backups, when future operator workflows are added, must preserve the same confidentiality assumptions.

## Consensus persistence and crash behavior

Required Raft persistence load/save failures fail-stop the Raft node. Continuing after an undurable term/vote/log transition would risk consensus safety, so availability is deliberately sacrificed instead.

Replicated SQL success waits for quorum commit and confirmed local state-machine apply. A client-observed success is tested to survive leader loss in the fixed-membership process harness.

A leader-side timeout/error after submission is outcome-uncertain: it must not be interpreted as proof the mutation did not commit.

## Read-consistency risk

Follower reads are local and can lag committed state. This is primarily a consistency risk, but it can become a security/policy risk if an application assumes an immediately effective table-based policy or state transition is visible on every follower.

Do not use arbitrary follower reads where linearizable visibility is a security requirement.

## Snapshot, replacement-node, and backup boundary

Legacy opaque Raft snapshots are rejected in replicated-SQL mode because they cannot reconstruct SQL/catalog state safely. This prevents silent state loss at the cost of disabling that compaction path.

There is not yet a SQL-aware snapshot/bootstrap, replacement-node, backup/restore, or disaster-recovery workflow. Operators must not infer those capabilities from ordinary process restart/catch-up tests.

## Membership and deployment risk

Membership is fixed from the checked-in deployment/operator perspective. Automatic HPA scaling is rejected because starting/removing pods is not equivalent to a coordinated Raft membership transition.

Peer configuration is trusted. Misconfigured IDs/addresses can affect availability; duplicate/malformed configuration should fail rather than silently create an unintended topology.

## Availability / denial-of-service risks

NeuralBase has connection admission controls and bounded execution mechanisms, but it does not claim full workload isolation. Expensive queries, connection floods within configured limits, storage stalls, a blocked quorum, certificate failures, or fail-stop persistence errors can reduce availability.

Fail-closed behavior is intentional where continuing would violate durability/consensus assumptions.

## Current production blockers from a security/operations perspective

At minimum, stronger production claims require review/testing of:

- secure-by-default SQL and Raft transport profiles;
- certificate/secret lifecycle and rotation;
- replicated or externally consistent identity/authorization state;
- SQL-aware snapshot/bootstrap and replacement-node recovery;
- backup encryption, restore validation, and disaster recovery;
- coordinated membership changes;
- linearizable/defined read-consistency modes where required;
- partition/storage-fault/upgrade chaos testing;
- least-privilege authorization and auditability appropriate to the target environment.

## Evidence references

- `src/tls.rs` — SQL/Raft TLS configuration and Raft mTLS builders.
- `src/consensus/transport.rs` — TCP/TLS Raft transport.
- `src/consensus/raft.rs` — quorum/confirmed-apply and fail-stop persistence behavior.
- `src/replicated_sql.rs` / `src/replicated_state_machine.rs` — replicated mutation format/apply.
- `tests/raft_persistence_fail_closed.rs` — injected persistence failures.
- `tests/replicated_sql_process.rs` — process failover/restart/crash-race evidence.
- `tests/replicated_sql_snapshot_guard.rs` — fail-closed snapshot boundary.
- `CONFIDENCE.md` / `CONFIDENCE.yaml` — machine-readable scope and non-production boundary.
