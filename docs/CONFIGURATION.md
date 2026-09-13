# Configuration reference

This reference follows the environment readers in [`src/main.rs`](../src/main.rs), [`src/auth.rs`](../src/auth.rs), [`src/server_parts/prelude.rs`](../src/server_parts/prelude.rs), [`src/tls.rs`](../src/tls.rs) and the replicated identity runtime. Defaults below are **binary defaults**; Compose and Helm override some of them.

The server reads environment variables, not command-line configuration flags. There is no automatic `.env` loader. From the repository root, a Bash session can load the checked-in standalone example explicitly:

```bash
set -a
. ./.env.example
set +a
cargo run --release --locked --bin neuralbase
```

## Server and storage

| Variable | Default | Behavior / legacy alias |
|---|---|---|
| `NEURALBASE_LISTEN_ADDR` | `0.0.0.0:5432` | SQL TCP bind address; alias `LISTEN_ADDR` |
| `NEURALBASE_DB_PATH` | unset | RocksDB directory; alias `DB_PATH`. Unset means in-memory/demo mode. Standalone open failure falls back to in-memory mode; clustered mode requires successful durable open. |
| `NEURALBASE_METRICS_PORT` | `9090` | Metrics bind on `0.0.0.0`; alias `METRICS_PORT`. Invalid port text uses the default; exporter startup failure is non-fatal. |
| `NEURALBASE_AUTH_REQUIRED` | `false` | `1` or case-insensitive `true` requires authentication. Other values disable it. |
| `NEURALBASE_USERS_FILE` | `users.json` | Standalone writable registry; clustered one-time legacy migration source, never live clustered authority. No legacy alias. |
| `NEURALBASE_IDENTITY_MIGRATION_SHA256` | unset | Exact 64-hex SHA-256 of the selected strict SCRAM migration file. See [migration](DEPLOYMENT.md#cluster-identity-bootstrap-and-migration). |
| `NEURALBASE_MAX_CONNECTIONS` | `100` | Global positive connection limit; zero/invalid input uses `100`. |
| `NEURALBASE_MAX_CONNECTIONS_PER_IP` | unlimited | Positive per-IP limit; zero/invalid/unset means unlimited. |
| `NEURALBASE_MAX_CONNECTIONS_PER_USER` | unlimited | Positive per-user limit after startup/authentication; zero/invalid/unset means unlimited. |

Where a `NEURALBASE_*` / legacy pair appears above or below, the primary variable wins if present, including when empty or malformed; validation does not retry the legacy value. Paths are relative to the process working directory unless absolute.

## Raft

| Variable | Default | Behavior / legacy alias |
|---|---|---|
| `NEURALBASE_OPERATOR_NODE` | unset | Absolute managed-node JSON path; opt-in private admin socket, immutable storage incarnation and non-voting learner startup. Requires durable storage and matching node ID; malformed/mismatched/unmanaged data fails closed. See [Phase 7](PHASE7_OPERATOR.md). |
| `NEURALBASE_NODE_ID` | unset | Nonempty trimmed logical ID enables clustered mode; alias `NODE_ID`. An empty value selects standalone mode. |
| `NEURALBASE_RAFT_ADDR` | `0.0.0.0:7001` | Raft TCP bind address; alias `RAFT_ADDR` |
| `NEURALBASE_PEERS` | empty | Bootstrap peer map; alias `PEERS`. Example: `node2=host2:7001,node3=host3:7001`. Self entries are skipped and duplicate peer IDs rejected. |
| `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS` | `150` | Positive base timeout in milliseconds; zero/invalid uses `150`; alias `RAFT_ELECTION_TIMEOUT_MS` |
| `NEURALBASE_RAFT_TLS` | `false` | `1` or case-insensitive `true` selects Raft mTLS; alias `RAFT_TLS`. Requires a `tls` build; otherwise startup errors. |

Peers may also be written as `host` or `host:port`, using the host as the logical ID. An omitted port inherits the local Raft bind port (or `7001` if it cannot be parsed). Prefer explicit `id=host:port` entries. Persisted finalized membership overrides bootstrap membership on restart; editing this variable is not a coordinated membership change. An empty peer list on fresh clustered storage describes one voter, not an automatically discovered cluster.

## TLS

TLS transport requires `cargo build --release --locked --features tls --bins`. SQL TLS and Raft mTLS are separate runtime choices.

| Variable | Default / precedence | Consumer |
|---|---|---|
| `TLS_ENABLED` | Only exact `1` enables this switch | SQL TLS |
| `TLS_CERT_PATH` | Preferred over `NEURALBASE_TLS_CERT` | SQL certificate chain |
| `TLS_KEY_PATH` | Preferred over `NEURALBASE_TLS_KEY` | SQL private key |
| `NEURALBASE_TLS_CERT` | `certs/server.crt` when needed | SQL fallback and Raft certificate chain |
| `NEURALBASE_TLS_KEY` | `certs/server.key` when needed | SQL fallback and Raft private key |
| `NEURALBASE_TLS_CA_CERT` | `certs/ca.crt` | Raft peer trust roots; not SQL client authentication |
| `NEURALBASE_TLS_SERVER_NAME` | `neuralbase-node` | Name verified by outgoing Raft TLS connections |

In a `tls` build, setting either SQL certificate variable also requests SQL TLS, even without `TLS_ENABLED=1`. A key variable alone does not enable it. SQL uses server authentication without requiring a client certificate. Raft verifies peer certificates against the configured CA and outgoing connections verify the configured server name; provision matching certificate SANs. Setting the shared `NEURALBASE_TLS_CERT` for Raft also enables SQL TLS. Use SQL-specific paths when the two listeners need different certificates.

When a SQL TLS acceptor is configured, the server requires a PostgreSQL SSLRequest followed by a TLS handshake and rejects plaintext startup with `28000`. Clients should verify the server certificate (for example, `sslmode=verify-full` with the correct CA/hostname). A non-`tls` binary does not construct the SQL TLS acceptor, even when SQL TLS variables are present. Helm's `tls.enabled` currently configures SQL TLS only; it does not enable Raft mTLS or mount its CA.

## Session and fixed limits

Read consistency is configured by SQL `SET`, not an environment variable. Each connection starts in `Local`; see [SQL support](SQL_SUPPORT.md#read-consistency). The server strong-read timeout is currently a fixed five seconds (`DEFAULT_STRONG_READ_TIMEOUT`); library callers can use `prepare_read_with_timeout`.

The general executor has a 50,000-row cross-product budget and a 200,000-row join-intermediate budget. These are code constants, not total-process memory limits or environment settings. The live server does not expose ONNX model selection, an online-backup management endpoint, or membership administration through environment switches.
