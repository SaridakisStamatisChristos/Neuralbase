// SPDX-License-Identifier: Apache-2.0
// Telemetry bootstrap — metrics + tracing initialisation.
//
// Exposes a Prometheus /metrics scrape endpoint on `metrics_port`.
// Uses `metrics-exporter-prometheus` with the `http-listener` feature.
//
// Tracing (tokio-tracing / OpenTelemetry) is not wired in this release;
// structured logs are emitted via eprintln! in the hot path.
//
// CONFIDENCE: raw=0.78 effective=0.76

use metrics_exporter_prometheus::PrometheusBuilder;

/// Initialise Prometheus metrics recorder and scrape endpoint.
///
/// Spawns a background Hyper server on `0.0.0.0:<metrics_port>`
/// exposing `/metrics` in Prometheus text exposition format.
pub fn init(metrics_port: u16) {
    match PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], metrics_port))
        .install()
    {
        Ok(()) => {
            eprintln!("[telemetry] Prometheus scrape: http://0.0.0.0:{metrics_port}/metrics");
        }
        Err(e) => {
            eprintln!("[telemetry] WARNING: failed to install Prometheus recorder: {e}");
            // Non-fatal: server continues without metrics export.
            // Most likely cause: port already in use (e.g. two nodes on same host).
        }
    }
}
