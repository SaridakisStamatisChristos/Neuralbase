# NeuralBase Threat Model v0.2.0

**Date**: 2026-03-04  
**Scope**: Single-node + 3-node cluster; dev + staging environments  
**Not in scope**: Cloud provider threats, supply-chain attacks

---

## 1. Assets

| Asset | Sensitivity | Notes |
|---|---|---|
| SQL query text | Medium | May embed unsanitised user input |
| Row data in RocksDB | High | Application data; GDPR-relevant if PII stored |
| Raft log entries | High | Replicated command stream; tampering breaks consistency |
| MVCC version chain | High | Data integrity across concurrent transactions |
| Wire-protocol connection | Medium | Plaintext by default |
| Prometheus scrape endpoint | Low | Aggregate metrics only; no row data |

---

## 2. Threat Categories

### 2.1 SQL Injection

**Risk**: LOW (mitigated at parser level)

- All client input enters through the PostgreSQL wire-protocol `Q` (simple query) message.
- `parse_statement()` in `src/sql.rs` invokes `sqlparser-rs` which fully lexes and parses
  the text into a typed AST before any execution occurs.
- There are no string-concatenation SQL operations in the engine.
- The binder resolves only known catalog tables; unknown table names => `BindError::TableNotFound`
  (structured error response, no panic).
- **Residual risk**: Parameterised queries (`P`/`B`/`D`/`E` protocol messages) are not
  implemented. All queries are literal SQL strings; callers must sanitise before sending.

**Mitigations**:
- Parser isolation: `sqlparser-rs` => AST => binder; no raw string execution
- Error responses follow the PostgreSQL `ErrorResponse` wire format (no stack traces)

---

### 2.2 Wire-Protocol Attacks

**Risk**: LOW (authentication available; TLS pending)

- `src/protocol.rs` validates frame magic bytes and length before allocating buffers.
- SSL Request (`SSLRequest code = 80877103`) is handled; server responds with `N` (no TLS
  in default build) then continues with plaintext.
- **Authentication**: SCRAM-SHA-256 (primary) and MD5 (fallback) are implemented in
  `src/auth.rs`. Enable with `NEURALBASE_AUTH_REQUIRED=1` and a `users.json` file.
- **No TLS in default build**: all wire traffic is plaintext. Network-positioned adversary
  can read queries and responses.

**Mitigations in place**:
- Frame-length cap: `parse_message_length()` returns error on negative/overly-large lengths
- `AsyncReadExt::read_exact()` is bounded; no unbounded allocation from wire input
- SCRAM-SHA-256 authentication available (opt-in via `NEURALBASE_AUTH_REQUIRED=1`)
- Per-IP connection limiting available (opt-in via `NEURALBASE_MAX_CONNECTIONS_PER_IP`)

**Residual risks**:
- Plaintext traffic: MitM possible (TLS not active by default; NASM required on Windows)
- Auth is opt-in: dev deployments without `NEURALBASE_AUTH_REQUIRED=1` accept all passwords
- No connection limits by default: DoS via connection flood unless env var is set

**Recommended production mitigations**:
1. Set `NEURALBASE_AUTH_REQUIRED=1` and provision `users.json`
2. Set `NEURALBASE_MAX_CONNECTIONS_PER_IP=50` for per-IP rate limiting
3. Enable TLS: install NASM, uncomment TLS deps, `--features tls`, set `TLS_ENABLED=1`
4. Bind to loopback (`127.0.0.1`) or use a network firewall for port 5432

---

### 2.3 MVCC Isolation Violations

**Risk**: LOW (mitigated; reviewed)

- Snapshot isolation is enforced by HLC timestamp ordering (`src/mvcc.rs`).
- `commit_serializer` Mutex spans the full commit critical section eliminating TOCTOU races.
- GC uses `safe_horizon = min(active_snapshot_timestamps)` to prevent collection of
  versions visible to active transactions.
- `safe_horizon` Mutex is released before I/O to prevent GC=>IO=>snapshot re-register deadlock.

**Residual risks**:
- `Ordering::Relaxed` in GC horizon counter: safe under single-GC-thread model; if a
  second GC thread is spawned, this MUST be reviewed and upgraded to `Ordering::SeqCst`
  or a `Mutex`.
- Write skew (SI anomaly): snapshot isolation does not prevent write skew without
  `SELECT ... FOR UPDATE` (not implemented).

---

### 2.4 Raft Log Security

**Risk**: MEDIUM (reviewed, not TLA+ verified)

- Log entries are replicated via TCP connections between cluster nodes (`src/consensus/transport.rs`).
- No TLS on node-to-node Raft connections in the default build.
- Leader election is based on node terms and log indices; no cryptographic identity.

**Residual risks**:
- Rogue node injection: a network adversary on the Raft port can inject `AppendEntries` or
  `RequestVote` RPC messages and potentially disrupt the cluster.
- Log compaction (`GcHandle` in `src/gc.rs`) is not replicated; a compaction on one node
  can diverge snapshots. This is a known limitation tracked in `REVIEW_REQUIRED.md`.

**Mitigations**:
- `handle_failure()` and retry logic limit blast radius of partial failures
- Quorum requirement prevents single-node log injection from committing

**Recommended**:
1. Enable TLS on Raft transport when `--features tls` is active
2. Add mTLS for node identity verification before accepting `RequestVote`
3. Write TLA+ spec to verify log safety properties

---

### 2.5 Denial of Service

**Risk**: LOW (connection admission control added; query timeout still missing)

- Global connection limit enforced by semaphore (`MAX_CONNECTIONS = 100`).
- Per-IP connection limiting available via `NEURALBASE_MAX_CONNECTIONS_PER_IP`.
- No query timeout is enforced.
- No per-connection memory cap.
- A large `GROUP BY` on a huge synthetic dataset will consume all available RAM.

**Mitigations in place**:
- Semaphore caps total simultaneous connections at `MAX_CONNECTIONS`
- Per-IP tracking (opt-in) prevents single-source floods
- Morsel scheduler (`MorselScheduler`) processes data in bounded chunks
- Frame-length validation prevents allocation bombs from wire input

**Recommended**:
- Add a query timeout (`Arc<AtomicBool>` cancellation flag through the execution path)
- Limit SQL query text length in `parse_message_length()`

---

### 2.6 Data Exfiltration

**Risk**: LOW (no column encryption; bound by access control gap)

- There are no column-level access controls or row-level security.
- Any connected client can `SELECT *` from any table if the table name is known.
- PII data stored in NeuralBase is accessible to all clients.

**Recommended for PII storage**:
- Do not store PII in NeuralBase until access controls are implemented
- Implement row-level security in the binder

---

## 3. Attack Surface Summary

| Surface | Protected | Notes |
|---|---|---|
| SQL text | + | Parser fully isolates; no string-exec |
| Wire-protocol frames | + | Length-validated; structured errors |
| MVCC isolation | + | Reviewed; commit_serializer Mutex |
| GC horizon | + | Single-thread; Relaxed ordering documented |
| TLS / encryption | - | Plaintext default; NASM required to activate |
| Authentication | + | SCRAM-SHA-256 + MD5 (opt-in; NEURALBASE_AUTH_REQUIRED=1) |
| Raft network | - | Plaintext TCP; no node identity |
| Connection limits | + | Global semaphore cap; per-IP opt-in |
| Query timeouts | - | Not implemented |
| Column access control | - | Not implemented |

---

## 4. Residual Risks Accepted for v0.2.0

The following risks are **explicitly accepted** for the v0.2.0 research release:

1. Plaintext wire protocol (TLS infrastructure complete but not active; NASM required on Windows)
2. Auth is opt-in: dev server accepts all passwords unless NEURALBASE_AUTH_REQUIRED=1 is set
3. No Raft mTLS (node-to-node connections are plaintext)
4. Write skew (SI does not prevent all SI anomalies)
5. Relaxed GC ordering (safe under current single-GC-thread design)
6. No query timeout
7. SCRAM state machine not formally verified (see REVIEW_REQUIRED.md Session 11)

These are documented in README.md Risk Budget and CONFIDENCE.md "Do Not Rely On".

---

## 5. References

- REVIEW_REQUIRED.md -- Raft + MVCC + Auth invariant checklist with human reviewer sign-offs
- CONFIDENCE.md -- Per-artifact confidence bounds and propagation analysis
- src/mvcc.rs -- inline CONFIDENCE + RISK annotations
- src/consensus/raft.rs -- inline invariant markers
- src/gc.rs -- Relaxed ordering safety argument (comment block)
- src/auth.rs -- SCRAM-SHA-256 + MD5 implementation; see REVIEW_REQUIRED.md Session 11
