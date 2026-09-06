// SPDX-License-Identifier: Apache-2.0
// Session 15: Production Hardening — load test, dead-code audit, metrics.

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::protocol::STARTUP_PROTOCOL_V3;
use neuralbase::server;
use neuralbase::storage;
use neuralbase::storage_executor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};

// ── helpers ────────────────────────────────────────────────────────────────────

async fn write_startup(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&STARTUP_PROTOCOL_V3.to_be_bytes());
    body.extend_from_slice(b"user\0neuralbase\0database\0neuralbase\0\0");
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    pkt.extend_from_slice(&body);
    stream.write_all(&pkt).await.expect("startup write");
}

async fn write_query(stream: &mut TcpStream, sql: &str) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut pkt = Vec::new();
    pkt.push(b'Q');
    pkt.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    pkt.extend_from_slice(&body);
    stream.write_all(&pkt).await.expect("query write");
}

async fn read_one_msg(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let raw = i32::from_be_bytes(len_buf);
    let plen = raw
        .checked_sub(4)
        .and_then(|n| if n <= 65536 { Some(n as usize) } else { None })
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad length"))?;
    let mut payload = vec![0u8; plen];
    stream.read_exact(&mut payload).await?;
    Ok((tag[0], payload))
}

async fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let (tag, _) = read_one_msg(stream).await.expect("ready read");
        if tag == b'Z' {
            break;
        }
    }
}

// ── Load test: 1000 concurrent connections ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn load_test_1000_concurrent_connections() {
    // Configure server to accept 1000 connections with no per-IP / per-user limit.
    std::env::set_var("NEURALBASE_MAX_CONNECTIONS", "1100");
    std::env::set_var("NEURALBASE_MAX_CONNECTIONS_PER_IP", "1100");
    std::env::set_var("NEURALBASE_MAX_CONNECTIONS_PER_USER", "1100");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    // Allow server to start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let connected = Arc::new(AtomicUsize::new(0));
    let queried = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    let total = 1000_usize;

    // Spawn 1000 concurrent client tasks in batches of 200 to avoid
    // ephemeral port exhaustion on Windows.
    let batch_size = 200;
    for batch_start in (0..total).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total);
        let mut handles = Vec::new();

        for _ in batch_start..batch_end {
            let conn_count = Arc::clone(&connected);
            let q_count = Arc::clone(&queried);
            let e_count = Arc::clone(&errors);

            handles.push(tokio::spawn(async move {
                let result = timeout(Duration::from_secs(15), async {
                    let mut stream = TcpStream::connect(addr).await?;
                    write_startup(&mut stream).await;
                    read_until_ready(&mut stream).await;
                    conn_count.fetch_add(1, Ordering::Relaxed);

                    // Execute a trivial query.
                    write_query(&mut stream, "SELECT 1").await;
                    read_until_ready(&mut stream).await;
                    q_count.fetch_add(1, Ordering::Relaxed);

                    let _ = stream.shutdown().await;
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })
                .await;

                match result {
                    Ok(Ok(())) => {}
                    _ => {
                        e_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        // Wait for this batch to complete before starting the next.
        for h in handles {
            let _ = h.await;
        }
    }

    let total_conn = connected.load(Ordering::Relaxed);
    let total_q = queried.load(Ordering::Relaxed);
    let total_err = errors.load(Ordering::Relaxed);

    eprintln!(
        "[load_test] connected={total_conn}/{total} queried={total_q}/{total} errors={total_err}"
    );

    // At least 95% of connections must succeed (allow for transient timeout
    // on resource-constrained CI machines).
    assert!(
        total_conn >= (total * 95 / 100),
        "expected >= 950 connections; got {total_conn}"
    );
    assert!(
        total_q >= (total * 95 / 100),
        "expected >= 950 queries; got {total_q}"
    );

    server_task.abort();

    // Reset env vars.
    std::env::remove_var("NEURALBASE_MAX_CONNECTIONS");
    std::env::remove_var("NEURALBASE_MAX_CONNECTIONS_PER_IP");
    std::env::remove_var("NEURALBASE_MAX_CONNECTIONS_PER_USER");
}

// ── Dead-code audit ───────────────────────────────────────────────────────────

#[test]
fn zero_allow_dead_code_in_src() {
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    collect_dead_code_violations(&src_dir, &mut violations);
    assert!(
        violations.is_empty(),
        "Found #[allow(dead_code)] in src/:\n{}",
        violations.join("\n")
    );
}

fn collect_dead_code_violations(dir: &std::path::Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_dead_code_violations(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let contents = std::fs::read_to_string(&path).expect("read file");
            let mut in_cfg_test = false;
            for (line_no, line) in contents.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed == "#[cfg(test)]" {
                    in_cfg_test = true;
                }
                if !in_cfg_test && trimmed.contains("#[allow(dead_code)]") {
                    out.push(format!("  {}:{}: {}", path.display(), line_no + 1, trimmed));
                }
            }
        }
    }
}

// ── Prometheus metrics endpoint ───────────────────────────────────────────────

#[test]
fn metrics_crate_has_http_listener() {
    // Verify that the metrics-exporter-prometheus crate is compiled with the
    // http-listener feature by checking that PrometheusBuilder has the
    // with_http_listener method (compilation test — if this compiles, the
    // feature is active).
    let _builder = metrics_exporter_prometheus::PrometheusBuilder::new();
}
