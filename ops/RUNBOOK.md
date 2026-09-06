# NeuralBase development runbook

This runbook covers the checked-in development topology. It is not a production disaster-recovery guide.

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

## Three-process development topology

```bash
docker compose up --build -d --wait
```

SQL endpoints are exposed on ports `5432`, `5433`, and `5434`.

> [!WARNING]
> This topology has real Raft transport/election behavior, but SQL data remains local to each node. Do not use another SQL endpoint as an assumed failover replica.

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

For process-level Raft tests, use the engine's graceful shutdown path rather than relying on a full apply channel to drain indefinitely; shutdown is designed to interrupt apply-channel backpressure.

## Persistent data

`NEURALBASE_DB_PATH` must refer to writable persistent storage when restart durability is required. `NEURALBASE_USERS_FILE` must also be writable for runtime user DDL.

In Kubernetes/Helm, a Secret may seed users but the live registry belongs on writable persistent storage.

## Known incident boundary

A healthy Raft quorum does not currently imply replicated SQL data. If one node's local RocksDB is lost, the current system cannot reconstruct that SQL state from peer SQL stores through the Raft log.

See `docs/DEPLOYMENT.md`, `docs/DISTRIBUTED.md`, and `ROADMAP.md` for the operational path required before HA claims are appropriate.
