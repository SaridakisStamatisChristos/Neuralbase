# Observability

NeuralBase exposes a Prometheus-compatible metrics endpoint and emits runtime diagnostics to standard error.

## Metrics endpoint

`src/telemetry.rs` installs `metrics-exporter-prometheus` with an HTTP listener on:

```text
0.0.0.0:${NEURALBASE_METRICS_PORT}/metrics
```

The default metrics port is `9090` unless configuration overrides it.

`METRICS_PORT` is the legacy alias; `NEURALBASE_METRICS_PORT` wins when both are present. The listener binds all interfaces and has no independent authentication/TLS configuration.

## Metrics currently emitted

The instrumented call sites in `server_parts/prelude.rs`, `server_parts/session.rs` and `read_barrier.rs` emit:

| Metric | Meaning |
|---|---|
| `active_connections` | Current connections holding a global admission permit |
| `rejected_connections_total` | Global admission-limit rejections |
| `rejected_connections_ip_total` | Per-IP admission rejections |
| `rejected_connections_per_user_total` | Per-user admission rejections after startup/authentication |
| `neuralbase_reads_total{consistency}` | Reads reaching the consistency prerequisite successfully; labels `local`, `leader`, `linearizable`. Not completed-query counts: later binding/execution can fail. |
| `neuralbase_strong_read_rejections_total{reason}` | Instrumented `not_leader` and `timeout` failures; not every possible strong-read error |

Metrics may be absent until their code path runs. There is currently no emitted strong-read latency histogram, Raft quorum/lag gauge set, backup-age metric or automatic SLO alert policy.

The repository includes `ops/prometheus.yml` and the Compose topology includes Prometheus/Grafana-oriented development wiring.

## Failure behavior

Failure to install the Prometheus recorder is currently non-fatal; the SQL process continues and prints a warning. A common cause is a metrics-port collision when multiple local processes use the same port.

## Tracing status

The crate includes tracing dependencies, but `src/telemetry.rs` does not currently wire a complete distributed tracing/OpenTelemetry export pipeline. Do not infer end-to-end tracing from the dependency list alone.

The standalone entry point also does not install a tracing subscriber. `eprintln!` diagnostics are visible, but dependency presence and `tracing::*` call sites do not by themselves guarantee those events are rendered or exported. Compose starts Jaeger receivers/UI, yet the server does not send OTLP spans to them. Grafana is a development service; no provisioned dashboard/datasource is checked in. Helm's `metrics.serviceMonitor.enabled` is a reserved value with no ServiceMonitor template.

## Operational note

Metrics are development/diagnostic evidence, not a complete production SLO/alerting package. Production use would still require documented dashboards, alerts, cardinality controls, retention, and incident procedures.
