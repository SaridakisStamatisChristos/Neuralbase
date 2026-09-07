# NeuralBase development runbook

This runbook covers the checked-in development/fixed-membership topology. It is not a production disaster-recovery guide.

## Single-node start

```bash
cargo build --release --locked
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

The default SQL listener is `0.0.0.0:5432` and the default Prometheus port is `9090`.

Smoke query:

```bash
psql -h 127.0.0.1 -p 5432 -U neuralbase -d neuralbase -c "SELECT 1"
```

Without `NEURALBASE_NODE_ID`, persistent table mutations use the local single-node path.

## Three-process fixed-membership topology

```bash
docker compose up --build -d --wait
```

SQL endpoints are exposed on ports `5432`, `5433`, and `5434`.

Persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE` are replicated through the elected Raft leader to independent per-node RocksDB stores.

> [!WARNING]
> Writes are leader-directed. A follower rejects a persistent table mutation before proposal rather than forwarding it. Reads are local and may lag committed state, so an arbitrary follower endpoint is not a linearizable read-after-write endpoint.

## Finding the write path

A client may probe nodes for a persistent table write. Followers return SQLSTATE `25006` and include the known leader ID when available.

An explicit follower rejection is safe to redirect/retry because no proposal occurred. Do **not** blindly retry a non-idempotent mutation after a timeout/error that occurred after submission to a leader; that outcome can be uncertain.

## Health and diagnostics

```bash
docker compose ps
docker compose logs --tail=200 node1
docker compose logs --tail=200 node2
docker compose logs --tail=200 node3
```

Scrape metrics from the configured Prometheus endpoint/port. If metrics initialization fails, inspect stderr for a port-collision warning.

## Required repository gates

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
```

The PostgreSQL reference suite requires Docker.

## Clean shutdown

Stop the Compose topology with:

```bash
docker compose down
```

Raft confirmed-apply delivery is bounded and shutdown-interruptible. Graceful shutdown attempts leader transfer before the configured drain period.

## Persistent data

Clustered startup requires `NEURALBASE_DB_PATH`/`DB_PATH`. Each node must use writable persistent storage for its own RocksDB directory. `NEURALBASE_USERS_FILE` must also be writable for runtime user DDL.

In Kubernetes/Helm, a Secret may seed users but the live registry belongs on writable persistent storage.

## Tested recovery path

The process integration suite exercises:

- convergence of table mutations across three independent stores;
- elected-leader kill and re-election;
- writes through the new leader;
- restart/catch-up of the killed node;
- full-cluster restart from persisted RocksDB/Raft state;
- a write raced against leader kill, with the guarantee that client-observed success remains recoverable.

## Recovery boundary

Do not confuse ordinary persisted restart/catch-up with replacement-node bootstrap.

Replicated-SQL mode intentionally rejects legacy opaque Raft snapshots/compaction because NeuralBase does not yet have a SQL-aware snapshot capable of reconstructing catalog/data state on a fresh replacement node. If a node permanently loses its RocksDB state, there is no documented operator-safe snapshot/bootstrap/node-replacement procedure yet.

Authentication/user mutations also remain per-node, membership is fixed, and backup/restore/disaster-recovery procedures are still open work.

See `docs/DEPLOYMENT.md`, `docs/DISTRIBUTED.md`, `CONFIDENCE.md`, and `ROADMAP.md` before making stronger HA claims.
