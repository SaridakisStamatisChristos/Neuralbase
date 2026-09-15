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
    batch_to_pg_rows, build_physical_plan, execute_physical_plan, mock_const_batch,
    try_execute_storage_only, TableScanner,
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

fn build_server_version() -> &'static str {
    concat!("NeuralBase ", env!("CARGO_PKG_VERSION"))
}

fn parameter_status_messages() -> Vec<Vec<u8>> {
    vec![
        build_parameter_status("server_version", build_server_version()),
        build_parameter_status("server_encoding", "UTF8"),
        build_parameter_status("client_encoding", "UTF8"),
        build_parameter_status("DateStyle", "ISO, MDY"),
        build_parameter_status("integer_datetimes", "on"),
    ]
}

#[derive(Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

pub struct ServerConfig {
    pub listen_addr: String,
    pub tls: Option<TlsConfig>,
    pub auth_required: bool,
    pub users_file: String,
    pub db_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:5432".to_string(),
            tls: None,
            auth_required: false,
            users_file: "users.json".to_string(),
            db_path: None,
        }
    }
}

#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    param_oids: Vec<u32>,
}

#[derive(Clone)]
struct Portal {
    sql: String,
    result_formats: Vec<i16>,
}

struct PlanCache {
    entries: HashMap<String, BoundPlan>,
    order: VecDeque<String>,
    max_size: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl PlanCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&mut self, sql: &str) -> Option<&BoundPlan> {
        if self.entries.contains_key(sql) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            if let Some(pos) = self.order.iter().position(|key| key == sql) {
                if let Some(key) = self.order.remove(pos) {
                    self.order.push_back(key);
                }
            }
            self.entries.get(sql)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&mut self, sql: String, plan: BoundPlan) {
        if self.entries.contains_key(&sql) {
            return;
        }
        if self.entries.len() >= self.max_size {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.order.push_back(sql.clone());
        self.entries.insert(sql, plan);
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}
