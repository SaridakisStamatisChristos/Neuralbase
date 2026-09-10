use neuralbase::catalog::{Catalog, InMemoryCatalog, MutableCatalog};
use neuralbase::consensus::raft::RaftTaskHandle;
#[cfg(feature = "tls")]
use neuralbase::consensus::TlsTcpTransport;
use neuralbase::consensus::{
    ClientCommand, CommittedEntry, FailClosedPersistenceStore, RaftNode, RaftPersistenceStore,
    RaftShared, TcpTransport, Transport, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::gc::GarbageCollector;
use neuralbase::hlc::HlcClock;
use neuralbase::mvcc::TransactionManager;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::restore::ensure_clustered_startup_restore_safe;
use neuralbase::rocksdb_catalog;
use neuralbase::server;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::StorageExecutor;
use neuralbase::telemetry;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};

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

struct RaftRuntime {
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    serving_ready: Arc<AtomicBool>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl RaftRuntime {
    fn sql_gateway(
        &self,
        engine: Arc<StorageEngine>,
        clock: Arc<HlcClock>,
    ) -> Arc<ReplicatedSqlGateway> {
        Arc::new(ReplicatedSqlGateway::new_with_readiness(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            engine,
            clock,
            Arc::clone(&self.serving_ready),
        ))
    }

    async fn request_leader_transfer(&self) -> Result<String, String> {
        match self.handle.as_ref() {
            Some(handle) => handle.request_leader_transfer().await,
            None => Err("Raft runtime already stopped".to_string()),
        }
    }

    async fn shutdown(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown().await;
        }
        self.apply_task.abort();
        let _ = self.apply_task.await;
    }
}

fn spawn_raft<T: Transport>(
    node_id: String,
    peers: Vec<String>,
    transport: Arc<T>,
    election_timeout_ms: u64,
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
) -> io::Result<RaftRuntime> {
    let state_machine = Arc::new(
        ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .map_err(|error| {
            io::Error::other(format!("initialize replicated SQL state machine: {error}"))
        })?,
    );
    let snapshot_store = Arc::new(ReplicatedSqlSnapshotHooks::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    ));

    let raw_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));
    let strict_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw_store));

    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
    let apply_state_machine = Arc::clone(&state_machine);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            let result = apply_state_machine
                .apply_log_entry(&committed.entry)
                .map(|_| ())
                .map_err(|error| error.to_string());
            let failed = result.is_err();
            let _ = committed.completion.send(result);
            if failed {
                break;
            }
        }
    });

    let mut node = RaftNode::new(node_id, peers, transport)
        .with_snapshot_store(snapshot_store)
        .with_persistence(strict_store)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(election_timeout_ms);
    let serving_ready = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();

    Ok(RaftRuntime {
        client_tx,
        shared,
        serving_ready,
        handle: Some(handle),
        apply_task,
    })
}

async fn start_raft_node(
    storage_engine: Option<Arc<StorageEngine>>,
    catalog: Arc<InMemoryCatalog>,
    clock: Option<Arc<HlcClock>>,
) -> io::Result<Option<RaftRuntime>> {
    let Some(node_id) = read_node_id() else {
        return Ok(None);
    };

    let engine = storage_engine.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "NEURALBASE_NODE_ID requires NEURALBASE_DB_PATH/DB_PATH for durable replicated SQL",
        )
    })?;
    let clock = clock.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "clustered startup requires a durable-storage HLC",
        )
    })?;

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
        "starting durable Raft transport and replicated SQL apply loop"
    );

    #[cfg(feature = "tls")]
    if raft_tls_enabled() {
        let transport = Arc::new(
            TlsTcpTransport::listen_with_peers(node_id.clone(), &raft_addr, peer_addrs.clone())
                .await?,
        );
        return spawn_raft(
            node_id,
            peers,
            transport,
            election_timeout_ms,
            engine,
            catalog,
            clock,
        )
        .map(Some);
    }

    #[cfg(not(feature = "tls"))]
    if raft_tls_enabled() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NEURALBASE_RAFT_TLS=1 requires a build with --features tls",
        ));
    }

    let transport =
        Arc::new(TcpTransport::listen_with_peers(node_id.clone(), &raft_addr, peer_addrs).await?);
    spawn_raft(
        node_id,
        peers,
        transport,
        election_timeout_ms,
        engine,
        catalog,
        clock,
    )
    .map(Some)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let metrics_port: u16 = env_with_legacy("NEURALBASE_METRICS_PORT", "METRICS_PORT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    telemetry::init(metrics_port);

    let listen_addr = env_with_legacy("NEURALBASE_LISTEN_ADDR", "LISTEN_ADDR")
        .unwrap_or_else(|| "0.0.0.0:5432".to_string());
    let clustered = read_node_id().is_some();

    let catalog: Arc<InMemoryCatalog> = Arc::new(InMemoryCatalog::with_tpch_all_tables());

    let storage_engine = if let Some(db_path) = env_with_legacy("NEURALBASE_DB_PATH", "DB_PATH") {
        if clustered {
            ensure_clustered_startup_restore_safe(Path::new(&db_path)).map_err(|error| {
                io::Error::other(format!(
                    "clustered startup restore-safety check failed: {error}"
                ))
            })?;
        }
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

    if let Some(engine) = &storage_engine {
        let rdb_catalog = rocksdb_catalog::RocksDbCatalog::new(engine.clone());
        match rdb_catalog.load_all() {
            Ok(snapshot) => {
                for schema in snapshot.all_tables() {
                    catalog.create_table(schema);
                }
            }
            Err(error) if clustered => {
                return Err(io::Error::other(format!(
                    "clustered startup cannot hydrate persisted SQL catalog: {error}"
                )));
            }
            Err(error) => {
                tracing::warn!(error = %error, "Failed to restore persisted schemas");
            }
        }
    }

    let hlc_clock = storage_engine
        .as_ref()
        .map(|_| Arc::new(HlcClock::new(500)));
    let txn_mgr = storage_engine
        .as_ref()
        .zip(hlc_clock.as_ref())
        .map(|(engine, clock)| {
            Arc::new(TransactionManager::new(engine.clone(), Arc::clone(clock)))
        });

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

    let listener = TcpListener::bind(&listen_addr).await?;
    tracing::info!(addr = %listen_addr, "NeuralBase listening");

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

    let mut raft_runtime = start_raft_node(
        storage_engine.clone(),
        Arc::clone(&catalog),
        hlc_clock.clone(),
    )
    .await?;

    let replicated_sql_gateway = match (
        raft_runtime.as_ref(),
        storage_engine.as_ref(),
        hlc_clock.as_ref(),
    ) {
        (Some(runtime), Some(engine), Some(clock)) => {
            Some(runtime.sql_gateway(Arc::clone(engine), Arc::clone(clock)))
        }
        _ => None,
    };
    server::configure_replicated_sql_gateway(replicated_sql_gateway);

    let serving_ready = raft_runtime
        .as_ref()
        .map(|runtime| Arc::clone(&runtime.serving_ready));
    let serve = async move {
        if let Some(serving_ready) = serving_ready {
            while !serving_ready.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        server::run(listener, catalog, dml_exec, tls_acceptor, storage_engine).await
    };

    tokio::select! {
        result = serve => {
            result
        }
        _ = shutdown_signal() => {
            if let Some(runtime) = raft_runtime.as_ref() {
                eprintln!("[shutdown] attempting leader transfer before drain");
                match runtime.request_leader_transfer().await {
                    Ok(new_leader) => eprintln!("[shutdown] leadership transferred to {new_leader}"),
                    Err(e) => eprintln!("[shutdown] transfer skipped or failed: {e}"),
                }
            }

            eprintln!("[shutdown] waiting up to 30s for in-flight queries to drain");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            server::configure_replicated_sql_gateway(None);
            if let Some(runtime) = raft_runtime.take() {
                runtime.shutdown().await;
            }
            eprintln!("[shutdown] drain complete -- exiting");
            Ok(())
        }
    }
}

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
        let err =
            parse_peer_config("node2=a:7001,node2=b:7001", "node1", "0.0.0.0:7001").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
