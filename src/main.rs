use neuralbase::catalog::{Catalog, InMemoryCatalog, MutableCatalog};
use neuralbase::gc::GarbageCollector;
use neuralbase::hlc::HlcClock;
use neuralbase::mvcc::TransactionManager;
use neuralbase::rocksdb_catalog;
use neuralbase::server;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::StorageExecutor;
use neuralbase::telemetry;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // ── Telemetry ──────────────────────────────────────────────────────────
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    telemetry::init(metrics_port);

    // ── Env ────────────────────────────────────────────────────────────────
    // LISTEN_ADDR: SQL wire-protocol endpoint (default 0.0.0.0:5432).
    // NODE_ID:     Unique identifier for this node in the cluster.
    // RAFT_ADDR:   Address this node listens on for Raft RPCs.
    // PEERS:       Comma-separated list of peer node IDs (e.g. "node2,node3").
    // DB_PATH:     Path to RocksDB data directory (optional).
    //              If absent, the server uses the in-memory TPC-H dataset.
    let listen_addr =
        std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:5432".to_string());

    // ── Catalog ────────────────────────────────────────────────────────────
    // Start with the TPC-H lineitem schema, then load any persisted user-defined
    // tables from RocksDB (populated by previous CREATE TABLE statements).
    let catalog: Arc<InMemoryCatalog> =
        Arc::new(InMemoryCatalog::with_tpch_all_tables());

    // ── Storage engine (optional — present only when DB_PATH is set) ───────
    let storage_engine = if let Ok(db_path) = std::env::var("DB_PATH") {
        match StorageEngine::open(Path::new(&db_path)) {
            Ok(engine) => {
                tracing::info!(db_path, "RocksDB storage engine opened");
                Some(Arc::new(engine))
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to open RocksDB; using in-memory mode");
                None
            }
        }
    } else {
        tracing::info!("DB_PATH not set; running in in-memory mode");
        None
    };

    // ── Startup catalog hydration from RocksDB ─────────────────────────────
    // Restore user-created schemas persisted in previous sessions so that
    // CREATE TABLE changes survive process restarts.
    if let Some(engine) = &storage_engine {
        let rdb_catalog = rocksdb_catalog::RocksDbCatalog::new(engine.clone());
        match rdb_catalog.load_all() {
            Ok(snapshot) => {
                for schema in snapshot.all_tables() {
                    catalog.create_table(schema);
                }
            }
            Err(e) => tracing::warn!(error = %e, "Failed to restore persisted schemas"),
        }
    }

    // ── Transaction manager ────────────────────────────────────────────────
    let txn_mgr = storage_engine.as_ref().map(|engine| {
        let clock = Arc::new(HlcClock::new(500));
        Arc::new(TransactionManager::new(engine.clone(), clock))
    });

    // ── GC — constructed ONCE here, before the server accept loop ──────────
    // _gc_handle lives until main() exits.
    // Its Drop impl signals the background thread and joins it.
    let _gc_handle = txn_mgr.as_ref().zip(storage_engine.as_ref()).map(|(tm, engine)| {
        let gc = Arc::new(GarbageCollector::new(
            engine.clone(),
            tm.active_snapshots.clone(),
        ));
        gc.start(5000) // 5 s interval
    });

    // ── Storage executor ───────────────────────────────────────────────────
    let dml_exec = txn_mgr.as_ref().zip(storage_engine.as_ref()).map(|(tm, engine)| {
        Arc::new(StorageExecutor::new(
            engine.clone(),
            tm.clone(),
            catalog.clone() as Arc<dyn Catalog>,
        ))
    });

    // ── Listen ──────────────────────────────────────────────────────
    let listener = TcpListener::bind(&listen_addr).await?;
    tracing::info!(addr = %listen_addr, "NeuralBase listening");

    // ── TLS (optional — enabled via TLS_ENABLED=1 or TLS_CERT_PATH+TLS_KEY_PATH) ──
    // Activate: install NASM, uncomment TLS deps in Cargo.toml,
    // run `make gen-certs`, then `cargo run --features tls`.
    #[cfg(feature = "tls")]
    let tls_acceptor: server::TlsAcceptorOpt = {
        match neuralbase::tls::acceptor::build_acceptor() {
            Ok(acc) => acc,
            Err(e) => {
                tracing::error!(error = %e, "Failed to initialise TLS acceptor");
                return Err(e);
            }
        }
    };
    #[cfg(not(feature = "tls"))]
    let tls_acceptor: server::TlsAcceptorOpt = None;

    // ── Run server with graceful shutdown ─────────────────────────────────
    // Race the accept loop against a shutdown signal (SIGTERM / Ctrl+C).
    // On signal: stop accepting new connections, allow 30s for in-flight
    // queries to drain (spawned Tokio tasks complete independently), then exit.
    tokio::select! {
        result = server::run(listener, catalog, dml_exec, tls_acceptor, storage_engine) => {
            result
        }
        _ = shutdown_signal() => {
            // Drain period: give in-flight connections time to finish.
            eprintln!("[shutdown] waiting up to 30s for in-flight queries to drain");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            eprintln!("[shutdown] drain complete -- exiting");
            Ok(())
        }
    }
}

/// Wait for a shutdown signal (Ctrl+C on all platforms, SIGTERM on Unix).
/// On receipt, log the event and return so the caller can begin graceful drain.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
    eprintln!("[shutdown] signal received -- draining connections (max 30s)");
}
