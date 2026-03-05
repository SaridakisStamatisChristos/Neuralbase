# Runbook

## Start server

```bash
cargo run --locked
```

The server listens on `127.0.0.1:5432`.

## Smoke query with psql

```bash
psql -h localhost -p 5432 -U neuralbase -d neuralbase -c "SELECT 1"
```

## Required checks

```bash
make test
make lint
make confidence
```
