// SPDX-License-Identifier: Apache-2.0
// PostgreSQL wire-protocol v3 server.
//
// Accepts client connections and processes simple Query ('Q') messages.
// All internal functions are generic over `S: AsyncRead + AsyncWrite + Unpin`
// so the same code path handles plaintext and TLS streams.

use crate::auth::{
    create_scram_user, read_max_per_ip, IpConnectionTracker, Md5State, ScramServer,
    StoredCredential, UserRegistry,
};
use crate::binder::{bind_nb_statement, BoundPlan};
use crate::catalog::{Catalog, InMemoryCatalog, MutableCatalog};
use crate::execution::{
    batch_to_pg_rows, build_physical_plan, execute_physical_plan, mock_const_batch, TableScanner,
};
use crate::index_advisor::{DdlResult, IndexAdvisor, IndexDecision, IndexExecutor, QueryPattern};
use crate::protocol::{
    build_auth_md5_request, build_auth_ok, build_auth_sasl_continue, build_auth_sasl_final,
    build_auth_sasl_request, build_backend_key_data, build_bind_complete, build_close_complete,
    build_command_complete, build_data_row, build_error_response, build_no_data,
    build_parameter_status, build_parse_complete, build_ready_for_query, build_row_description,
    parse_message_length, parse_sasl_initial_response, parse_startup_body, parse_startup_username,
    ProtocolError, SSL_REQUEST_CODE, STARTUP_PROTOCOL_V3,
};
use crate::query_executor::{query_result_to_batch, QueryCatalog};
use crate::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
use crate::replicated_identity_runtime::{
    allow_empty_identity_bootstrap, migrate_legacy_identity_if_configured,
    replicated_identity_initialized, replicated_user_record, ReplicatedIdentityRuntimeError,
    IDENTITY_MIGRATION_SHA256_ENV,
};
use crate::rocksdb_catalog::RocksDbCatalog;
use crate::scheduler::MorselScheduler;
use crate::sql::{parse_nb_statement, parse_statement};
use crate::storage::StorageEngine;
use crate::storage_executor::table_id_for;
use crate::storage_executor::StorageExecutor;
use crate::tpch::generate_tpch_data;
use metrics::{counter, gauge};
use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::{timeout, Duration};

const MAX_CONNECTIONS: usize = 100;
const CONNECTION_ACQUIRE_TIMEOUT_MS: u64 = 500;
const PLAN_CACHE_MAX_SIZE: usize = 500;
const STMT_CACHE_MAX_SIZE: usize = 100;

/// Optional replicated mutation gateway for clustered runtime.
///
/// `server::run` keeps its existing public signature for compatibility with
/// local/single-node callers and integration tests. `main` installs a gateway
/// only when a durable Raft node is configured. In that mode persistent table
/// and identity mutations go through the Raft leader and authentication reads
/// the durable replicated identity state on every new connection.
static REPLICATED_SQL_GATEWAY: OnceLock<Mutex<Option<Arc<ReplicatedSqlGateway>>>> = OnceLock::new();

pub fn configure_replicated_sql_gateway(gateway: Option<Arc<ReplicatedSqlGateway>>) {
    let slot = REPLICATED_SQL_GATEWAY.get_or_init(|| Mutex::new(None));
    *slot.lock().expect("replicated SQL gateway mutex poisoned") = gateway;
}

fn replicated_sql_gateway() -> Option<Arc<ReplicatedSqlGateway>> {
    REPLICATED_SQL_GATEWAY
        .get()
        .and_then(|slot| slot.lock().ok().and_then(|gateway| gateway.clone()))
}

/// Classify table mutations that need a pre-bind readiness barrier. The binder
/// depends on local catalog state, so after failover a current-term Raft barrier
/// must be applied before it resolves table names. Identity DDL does not depend
/// on the table catalog and takes its own barrier inside the replicated gateway.
fn is_persistent_table_mutation_sql(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    let Some(first) = words.next() else {
        return false;
    };
    let first = first.to_ascii_lowercase();
    match first.as_str() {
        "insert" | "update" | "delete" => true,
        "create" | "drop" => words
            .next()
            .is_some_and(|second| second.eq_ignore_ascii_case("table")),
        _ => false,
    }
}

#[cfg(feature = "tls")]
pub type TlsAcceptorOpt = Option<tokio_rustls::TlsAcceptor>;
#[cfg(not(feature = "tls"))]
pub type TlsAcceptorOpt = Option<std::convert::Infallible>;

#[derive(Clone)]
struct UserConnectionTracker {
    inner: Arc<Mutex<HashMap<String, usize>>>,
    max_per_user: usize,
}

impl UserConnectionTracker {
    fn new(max_per_user: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_per_user,
        }
    }

    fn try_acquire(&self, user: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let c = g.entry(user.to_string()).or_insert(0);
        if *c >= self.max_per_user {
            return false;
        }
        *c += 1;
        true
    }

    fn release(&self, user: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some(c) = g.get_mut(user) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.remove(user);
            }
        }
    }
}

fn read_max_per_user() -> usize {
    std::env::var("NEURALBASE_MAX_CONNECTIONS_PER_USER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(usize::MAX)
}

struct UserConnectionGuard {
    tracker: UserConnectionTracker,
    username: String,
}

impl Drop for UserConnectionGuard {
    fn drop(&mut self) {
        self.tracker.release(&self.username);
    }
}

/// LRU query plan cache shared across all connections.
pub struct PlanCache {
    entries: HashMap<String, BoundPlan>,
    order: VecDeque<String>,
    max_size: usize,
    hits: u64,
    misses: u64,
}

impl PlanCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, sql: &str) -> Option<&BoundPlan> {
        if self.entries.contains_key(sql) {
            self.hits += 1;
            self.order.retain(|k| k != sql);
            self.order.push_front(sql.to_string());
            self.entries.get(sql)
        } else {
            self.misses += 1;
            None
        }
    }

    pub fn insert(&mut self, sql: String, plan: BoundPlan) {
        if self.entries.contains_key(&sql) {
            return;
        }
        if self.entries.len() >= self.max_size {
            if let Some(lru) = self.order.pop_back() {
                self.entries.remove(&lru);
            }
        }
        self.order.push_front(sql.clone());
        self.entries.insert(sql, plan);
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[derive(Clone)]
struct PreparedStatement {
    sql: String,
}

#[derive(Clone)]
struct Portal {
    sql: String,
}

#[derive(Clone)]
struct ClientSessionContext {
    catalog: Arc<InMemoryCatalog>,
    dml_exec: Option<Arc<StorageExecutor>>,
    advisor: Arc<IndexAdvisor>,
    executor: Arc<IndexExecutor>,
    storage_engine: Option<Arc<StorageEngine>>,
    query_count: Arc<AtomicU64>,
    advisor_inflight: Arc<AtomicBool>,
    registry: Arc<RwLock<UserRegistry>>,
    users_file: Arc<String>,
    user_tracker: UserConnectionTracker,
    plan_cache: Arc<Mutex<PlanCache>>,
}

pub async fn run(
    listener: TcpListener,
    catalog: Arc<InMemoryCatalog>,
    dml_exec: Option<Arc<StorageExecutor>>,
    tls_acceptor: TlsAcceptorOpt,
    storage_engine: Option<Arc<StorageEngine>>,
) -> std::io::Result<()> {
    let advisor = Arc::new(IndexAdvisor::new(Arc::clone(&catalog) as Arc<dyn Catalog>));
    let executor = Arc::new(IndexExecutor::new());
    let query_count = Arc::new(AtomicU64::new(0));
    let advisor_inflight = Arc::new(AtomicBool::new(false));
    let max_connections = read_max_connections();
    let semaphore = Arc::new(Semaphore::new(max_connections));
    update_active_connections(&semaphore, max_connections);
    let user_tracker = UserConnectionTracker::new(read_max_per_user());
    let plan_cache = Arc::new(Mutex::new(PlanCache::new(PLAN_CACHE_MAX_SIZE)));

    let users_file =
        std::env::var("NEURALBASE_USERS_FILE").unwrap_or_else(|_| "users.json".to_string());
    let registry = Arc::new(RwLock::new(UserRegistry::load_from_file(&users_file)));
    let ip_tracker = IpConnectionTracker::new(read_max_per_ip());

    let require_auth = registry.read().await.require_auth;
    let replicated_identity = replicated_sql_gateway().is_some();
    tracing::info!(
        max_connections,
        per_ip = ip_tracker.max_per_ip,
        require_auth,
        replicated_identity,
        env_var = "NEURALBASE_MAX_CONNECTIONS",
        "connection admission control enabled"
    );

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        let peer_ip = peer_addr.ip();
        if !ip_tracker.try_acquire(peer_ip) {
            counter!("rejected_connections_ip_total").increment(1);
            tracing::warn!(
                %peer_addr,
                max = ip_tracker.max_per_ip,
                "rejecting connection: per-IP limit exceeded"
            );
            let _ = reject_too_many_connections(&mut socket).await;
            continue;
        }

        let permit = match timeout(
            Duration::from_millis(CONNECTION_ACQUIRE_TIMEOUT_MS),
            Arc::clone(&semaphore).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                ip_tracker.release(peer_ip);
                counter!("rejected_connections_total").increment(1);
                tracing::warn!(
                    %peer_addr,
                    timeout_ms = CONNECTION_ACQUIRE_TIMEOUT_MS,
                    "rejecting connection: too many connections"
                );
                let _ = reject_too_many_connections(&mut socket).await;
                continue;
            }
        };

        update_active_connections(&semaphore, max_connections);
        let ctx = ClientSessionContext {
            catalog: Arc::clone(&catalog),
            dml_exec: dml_exec.clone(),
            advisor: Arc::clone(&advisor),
            executor: Arc::clone(&executor),
            storage_engine: storage_engine.clone(),
            query_count: Arc::clone(&query_count),
            advisor_inflight: Arc::clone(&advisor_inflight),
            registry: Arc::clone(&registry),
            users_file: Arc::new(users_file.clone()),
            user_tracker: user_tracker.clone(),
            plan_cache: Arc::clone(&plan_cache),
        };
        let permit_guard = ConnectionPermit::new(
            permit,
            Arc::clone(&semaphore),
            max_connections,
            peer_ip,
            ip_tracker.clone(),
        );

        #[cfg(feature = "tls")]
        if let Some(ref acceptor) = tls_acceptor {
            let acceptor = acceptor.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _permit = permit_guard;
                let mut ssl_req = [0u8; 8];
                if socket.read_exact(&mut ssl_req).await.is_err() {
                    return;
                }
                let is_ssl_request = ssl_req == [0, 0, 0, 8, 4, 0xd2, 0x16, 0x2f];
                if !is_ssl_request {
                    tracing::warn!(%peer_addr, "Rejecting plaintext connection (TLS required, send sslmode=require)");
                    let err_bytes = build_error_response(
                        "plaintext connections not accepted -- reconnect with sslmode=require",
                        "28000",
                    );
                    let _ = socket.write_all(&err_bytes).await;
                    return;
                }
                if socket.write_all(b"S").await.is_err() {
                    return;
                }
                tracing::debug!(%peer_addr, "TLS connection accepted");
                match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        let _ = handle_client_stream(tls_stream, ctx).await;
                    }
                    Err(e) => {
                        tracing::warn!(%peer_addr, error = %e, "TLS handshake failed");
                    }
                }
            });
            continue;
        }

        #[cfg(not(feature = "tls"))]
        let _ = &tls_acceptor;

        tokio::spawn(async move {
            let _permit = permit_guard;
            tracing::debug!(%peer_addr, "plaintext connection accepted");
            let _ = handle_client_stream(socket, ctx).await;
        });
    }
}

struct ConnectionPermit {
    _permit: Option<OwnedSemaphorePermit>,
    semaphore: Arc<Semaphore>,
    max_connections: usize,
    peer_ip: std::net::IpAddr,
    ip_tracker: IpConnectionTracker,
}

impl ConnectionPermit {
    fn new(
        permit: OwnedSemaphorePermit,
        semaphore: Arc<Semaphore>,
        max_connections: usize,
        peer_ip: std::net::IpAddr,
        ip_tracker: IpConnectionTracker,
    ) -> Self {
        Self {
            _permit: Some(permit),
            semaphore,
            max_connections,
            peer_ip,
            ip_tracker,
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let _ = self._permit.take();
        self.ip_tracker.release(self.peer_ip);
        update_active_connections(&self.semaphore, self.max_connections);
    }
}

fn read_max_connections() -> usize {
    std::env::var("NEURALBASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(MAX_CONNECTIONS)
}

fn update_active_connections(semaphore: &Semaphore, max_connections: usize) {
    let active = max_connections.saturating_sub(semaphore.available_permits()) as f64;
    gauge!("active_connections").set(active);
}

async fn reject_too_many_connections<S>(socket: &mut S) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket
        .write_all(&build_error_response("too many connections", "53300"))
        .await?;
    socket.shutdown().await
}
