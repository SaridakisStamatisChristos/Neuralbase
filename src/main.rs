use neuralbase::catalog::{Catalog, InMemoryCatalog, MutableCatalog};
use neuralbase::consensus::raft::RaftTaskHandle;
use neuralbase::consensus::{RaftNode, TcpTransport, Transport};
#[cfg(feature = "tls")]
use neuralbase::consensus::TlsTcpTransport;
use neuralbase::gc::GarbageCollector;
use neuralbase::hlc::HlcClock;
use neuralbase::mvcc::TransactionManager;
use neuralbase::rocksdb_catalog;
use neuralbase::server;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::StorageExecutor;
use neuralbase::telemetry;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;

const DEFAULT_RAFT_PORT: u16 = 7001;

fn env_with_legacy(primary: &str, legacy: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(legacy).ok())
}

fn read_node_id() -> Option<String> {
    env_with_legacy("NEURALBASE_NODE_ID", "NODE_ID")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn raft_peer_port(bind_addr: &str) -> u16 {
    bind_addr
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(DEFAULT_RAFT_PORT)
}

fn normalize_peer_addr(raw: &str, default_port: u16) -> String {
    let raw = raw.trim();
    let has_explicit_port = raw
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some();
    if has_explicit_port {
        raw.to_string()
    } else {
        format!("{raw}:{default_port}")
    }
}

fn implicit_peer_id(raw: &str) -> String {
    let raw = raw.trim();
    if let Some((host, port)) = raw.rsplit_once(':') {
        if port.parse::<u16>().is_ok() && !host.is_empty() {
            return host.to_string();
        }
    }
    raw.to_string()
}

/// Parse PEERS into Raft logical IDs and their connectable socket addresses.
///
/// Preferred production form:
///   NEURALBASE_PEERS="node2=node2:7001,node3=node3:7001"
///
/// Backward-compatible shorthand is also accepted:
///   PEERS="node2,node3"       -> node2:7001, node3:7001
///   PEERS="node2:7001,node3:7001"
///
/// Explicit id=address form is required whenever NODE_ID differs from the
/// connectable host name (for example Kubernetes pod name vs headless FQDN).
fn parse_peer_config(
    raw: &str,
    node_id: &str,
    raft_bind_addr: &str,
) -> io::Result<(Vec<String>, HashMap<String, String>)> {
    let default_port = raft_peer_port(raft_bind_addr);
    let mut peers = Vec::new();
    let mut addrs = HashMap::new();
    let mut seen = HashSet::new();

    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = if let Some((id, addr)) = item.split_once('=') {
            let id = id.trim();
            let addr = addr.trim();
            if id.is_empty() || addr.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid peer entry '{item}': expected id=host:port"),
                ));
            }
            (id.to_string(), normalize_peer_addr(addr, default_port))
        } else {
            (
                implicit_peer_id(item),
                normalize_peer_addr(item, default_port),
            )
        };

        if id == node_id {
            continue;
        }
        if !seen.insert(id.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate Raft peer id '{id}'"),
            ));
        }
        peers.push(id.clone());
        addrs.insert(id, addr);
    }

    Ok((peers, addrs))
}

fn raft_tls_enabled() -> bool {
    env_with_legacy("NEURALBASE_RAFT_TLS", "RAFT_TLS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn spawn_raft<T: Transport>(
    node_id: String,
    peers: Vec<String>,
    transport: Arc<T>,
    election_timeout_ms: u64,
) -> RaftTaskHandle {
    let mut node = RaftNode::new(node_id, peers, transport);
    node.set_election_timeout_ms(election_timeout_ms);
    let (_cmd_tx, _shared, handle) = node.spawn();
    handle
}

async fn start_raft_node() -> io::Result<Option<RaftTaskHandle>> {
    let Some(node_id) = read_node_id() else {
        return Ok(None);
    };

    let raft_addr = env_with_legacy("NEURALBASE_RAFT_ADDR", "RAFT_ADDR")
        .unwrap_or_else(|| "0.0.0.0:7001".to_string());
    let peer_spec = env_with_legacy("NEURALBASE_PEERS", "PEERS").unwrap_or_default();
    let (peers, peer_addrs) = parse_peer_config(&peer_spec, &node_id, &raft_addr)?;
    let election_timeout_ms = env_with_legacy(
        "NEURALBASE_RAFT_ELECTION_TIMEOUT_MS",
        "RAFT_ELECTION_TIMEOUT_MS",
    )
    .and_then(|v| v.parse::<u64>().ok())
    .filter(|v| *v > 0)
    .unwrap_or(150);

    tracing::info!(
        node_id = %node_id,
        bind = %raft_addr,
        peer_count = peers.len(),
        raft_tls = raft_tls_enabled(),
        "starting Raft transport"
    );

    #[cfg(feature = "tls")]
    if raft_tls_enabled() {
        let transport = Arc::new(
            TlsTcpTransport::listen_with_peers(
                node_id.clone(),
                &raft_addr,
                peer_addrs.clone(),
            )
            .await?,
        );
        return Ok(Some(spawn_raft(
            node_id,
            peers,
            transport,
            election_timeout_ms,
        )));
    }

    #[cfg(not(feature = "tls"))]
    if raft_tls_enabled() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NEURALBASE_RAFT_TLS=1 requires a build with --features tls",
        ));
    }

    let transport = Arc::new(
        TcpTransport::listen_with_peers(node_id.clone(), &raft_addr, peer_addrs).await?,
    );
    Ok(Some(spawn_raft(
        node_id,
        peers,
        transport,
        election_timeout_ms,
    )))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // ── Telemetry ──────────────────────────────────────────────────────────
    let metrics_port: u16 = env_with_legacy("NEURALBASE_METRICS_PORT", "METRICS_PORT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    telemetry::init(metrics_port);

    // Documented NEURALBASE_* names are authoritative. Legacy short names are
    // accepted so existing docker-compose and local scripts remain compatible.
    let listen_addr = env_with_legacy("NEURALBASE_LISTEN_ADDR", "LISTEN_ADDR")
        .unwrap_or_else(|| "0.0.0.0:5432".to_string());

    // ── Catalog ────────────────────────────────────────────────────────────
    let catalog: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::with_tpch_all_tables());

    // ── Storage engine (optional — present only when DB_PATH is set) ───────
    let storage_engine = if let Some(db_path) = env_with_legacy("NEURALBASE_DB_PATH", "DB_PATH") {
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
        tracing::info!("NEURALBASE_DB_PATH/DB_PATH not set; running in in-memory mode");
        None
    };

    // ── Startup catalog hydration from RocksDB ─────────────────────────────
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

    // ── GC ─────────────────────────────────────────────────────────────────
    let _gc_handle = txn_mgr
        .as_ref()
        .zip(storage_engine.as_ref())
        .map(|(tm, engine)| {
            let gc = Arc::new(GarbageCollector::new(
                engine.clone(),
                tm.active_snapshots.clone(),
            ));
            gc.start(5000)
        });

    // ── Storage executor ───────────────────────────────────────────────────
    let dml_exec = txn_mgr
        .as_ref()
        .zip(storage_engine.as_ref())
        .map(|(tm, engine)| {
            Arc::new(StorageExecutor::new(
                engine.clone(),
                tm.clone(),
                catalog.clone() as Arc<dyn Catalog>,
            ))
        });

    // ── SQL listener ───────────────────────────────────────────────────────
    let listener = TcpListener::bind(&listen_addr).await?;
    tracing::info!(addr = %listen_addr, "NeuralBase listening");

    // ── Client TLS (optional) ──────────────────────────────────────────────
    #[cfg(feature = "tls")]
    let tls_acceptor: server::TlsAcceptorOpt = match neuralbase::tls::acceptor::build_acceptor() {
        Ok(acc) => acc,
        Err(e) => {
            tracing::error!(error = %e, "Failed to initialise TLS acceptor");
            return Err(e);
        }
    };
    #[cfg(not(feature = "tls"))]
    let tls_acceptor: server::TlsAcceptorOpt = None;

    // ── Real Raft transport ────────────────────────────────────────────────
    let raft_handle = start_raft_node().await?;

    // ── Run server with graceful shutdown ─────────────────────────────────
    tokio::select! {
        result = server::run(listener, catalog, dml_exec, tls_acceptor, storage_engine) => {
            result
        }
        _ = shutdown_signal() => {
            if let Some(ref handle) = raft_handle {
                eprintln!("[shutdown] attempting leader transfer before drain");
                match handle.request_leader_transfer().await {
                    Ok(new_leader) => eprintln!("[shutdown] leadership transferred to {new_leader}"),
                    Err(e) => eprintln!("[shutdown] transfer skipped or failed: {e}"),
                }
            }

            eprintln!("[shutdown] waiting up to 30s for in-flight queries to drain");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            eprintln!("[shutdown] drain complete -- exiting");
            Ok(())
        }
    }
}

/// Wait for a shutdown signal (Ctrl+C on all platforms, SIGTERM on Unix).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_config_maps_logical_ids_to_addresses() {
        let (peers, addrs) = parse_peer_config(
            "node2=node2.internal:7100,node3=node3.internal:7200",
            "node1",
            "0.0.0.0:7001",
        )
        .unwrap();
        assert_eq!(peers, vec!["node2", "node3"]);
        assert_eq!(addrs["node2"], "node2.internal:7100");
        assert_eq!(addrs["node3"], "node3.internal:7200");
    }

    #[test]
    fn peer_config_keeps_shorthand_compatible() {
        let (peers, addrs) =
            parse_peer_config("node1,node2,node3", "node1", "0.0.0.0:7001").unwrap();
        assert_eq!(peers, vec!["node2", "node3"]);
        assert_eq!(addrs["node2"], "node2:7001");
        assert_eq!(addrs["node3"], "node3:7001");
    }

    #[test]
    fn peer_config_rejects_duplicate_ids() {
        let err = parse_peer_config(
            "node2=a:7001,node2=b:7001",
            "node1",
            "0.0.0.0:7001",
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
