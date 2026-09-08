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

Followers return a write rejection and include the known leader ID when available. An explicit follower rejection is safe to redirect/retry because no proposal occurred.

Do **not** blindly retry a non-idempotent mutation after a timeout/error that occurred after submission to a leader; that outcome can be uncertain.

A member still reconstructing from empty local storage does not enter SQL serving and direct replicated-gateway use reports catching-up rather than mutating partial state.

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

```bash
docker compose down
```

Raft confirmed-apply delivery is bounded and shutdown-interruptible. Graceful shutdown attempts leader transfer before the configured drain period.

## Persistent data

Clustered startup requires `NEURALBASE_DB_PATH`/`DB_PATH`. Each node must use writable persistent storage for its own RocksDB directory. `NEURALBASE_USERS_FILE` must also be writable for runtime user DDL.

Authentication/user state remains per-node and is not reconstructed by the table SQL snapshot guarantee.

## Tested ordinary recovery path

The process integration suite exercises:

- convergence of table mutations across three independent stores;
- elected-leader kill and re-election;
- writes through the new leader;
- restart/catch-up of the killed node;
- full-cluster restart from persisted RocksDB/Raft state;
- a write raced against leader kill, with the guarantee that client-observed success remains recoverable.

## Tested SQL snapshot and fixed-member recovery path

NeuralBase now has a SQL-aware logical snapshot integrated with Raft. Snapshot creation is validated and durably staged before prefix truncation. Follower installation validates/stages/restores SQL state before publishing the Raft boundary and acknowledging success. An interrupted follower installation is resumed idempotently from staged metadata on restart.

The automated fixed-member replacement test exercises this sequence:

1. start a three-member fixed cluster with independent RocksDB stores;
2. create replicated table/data;
3. create a SQL-aware leader snapshot and compact the covered Raft prefix;
4. stop one follower and destroy its entire local database directory;
5. commit another write while that member is absent, creating a post-snapshot suffix;
6. restart the **same configured logical member ID** with a truly empty directory;
7. require it to remain non-serving while it receives/restores the snapshot and applies the suffix;
8. verify exact catalog/data convergence;
9. transfer leadership to the reconstructed member and acknowledge another SQL write there;
10. kill that leader and require the acknowledged write to survive on the remaining quorum;
11. restart the reconstructed member from its recovered disk and verify exact convergence/no duplicate MVCC effects.

Repeated snapshot/compaction cycles with retained suffix, restart and continued writes are also tested.

## What operators may infer

The tested path establishes that a known fixed member can reconstruct SQL table state after complete local storage loss **when the logical membership configuration itself is unchanged** and a healthy quorum/leader can supply the SQL snapshot and remaining log.

It does not yet provide a turnkey production operator command or controller for replacing volumes/pods. Deployment automation must preserve the exact logical member ID/address assumptions and fixed voter set.

## Recovery boundaries still open

Do not reinterpret fixed-member snapshot catch-up as any of the following:

- dynamic membership or adding a new logical node ID;
- automatic node replacement/orchestration;
- changing StatefulSet replica count/HPA safely;
- cluster-wide user/auth restoration;
- backup/restore from operator-retained archives;
- point-in-time recovery;
- disaster recovery after loss of the healthy quorum;
- linearizable follower reads.

If a failure requires changing the membership set, restoring from an external backup, reconstructing authentication state, or recovering without a healthy quorum, the current runbook does not claim a safe automated procedure.

See `docs/DEPLOYMENT.md`, `docs/DISTRIBUTED.md`, `CONFIDENCE.md`, and `ROADMAP.md` before making stronger HA claims.
