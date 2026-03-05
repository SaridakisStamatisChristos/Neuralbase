// SPDX-License-Identifier: Apache-2.0
// PostgreSQL wire-protocol v3 server.
//
// Accepts client connections and processes simple Query ('Q') messages.
// All internal functions are generic over `S: AsyncRead + AsyncWrite + Unpin`
// so the same code path handles both plaintext TcpStream and TLS-wrapped
// TlsStream<TcpStream> (Session 7: TLS added via `tls` Cargo feature).
//
// TLS activation:
//   Set TLS_ENABLED=1 (self-signed dev cert auto-generated via rcgen), or
//   set TLS_CERT_PATH + TLS_KEY_PATH to load a real cert.
//   Plaintext is the default when TLS_ENABLED is unset.
//
// Authentication (Session 11):
//   Enable auth:  NEURALBASE_AUTH_REQUIRED=1
//   User file:    NEURALBASE_USERS_FILE=users.json  (default: "users.json")
//   Per-IP limit: NEURALBASE_MAX_CONNECTIONS_PER_IP (default 10)
//   Methods:      SCRAM-SHA-256 (primary), MD5 (legacy fallback)

use crate::auth::{
    IpConnectionTracker, Md5State, ScramServer, StoredCredential, UserRegistry,
    create_scram_user, read_max_per_ip,
};
use crate::binder::{bind_nb_statement, BoundPlan};
use crate::catalog::{Catalog, InMemoryCatalog, MutableCatalog};
use crate::execution::{
    batch_to_pg_rows, build_physical_plan, execute_physical_plan, mock_const_batch, TableScanner,
};
use crate::query_executor::{query_result_to_batch, QueryCatalog};
use crate::rocksdb_catalog::RocksDbCatalog;
use crate::storage_executor::StorageExecutor;
use crate::index_advisor::{DdlResult, IndexAdvisor, IndexDecision, IndexExecutor, QueryPattern};
use crate::storage::StorageEngine;
use crate::protocol::{
    build_auth_md5_request, build_auth_ok, build_auth_sasl_continue, build_auth_sasl_final,
    build_auth_sasl_request, build_backend_key_data, build_command_complete, build_data_row,
    build_error_response, build_parameter_status, build_ready_for_query, build_row_description,
    parse_message_length, parse_sasl_initial_response, parse_startup_body, parse_startup_username,
    ProtocolError, SSL_REQUEST_CODE, STARTUP_PROTOCOL_V3,
};
use crate::scheduler::MorselScheduler;
use crate::sql::{parse_nb_statement, parse_statement};
use crate::storage_executor::table_id_for;
use crate::tpch::generate_tpch_data;
use metrics::{counter, gauge};
use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::{timeout, Duration};

const MAX_CONNECTIONS: usize = 100;
const CONNECTION_ACQUIRE_TIMEOUT_MS: u64 = 500;

// TLS acceptor type alias (conditional on `tls` feature).
#[cfg(feature = "tls")]
pub type TlsAcceptorOpt = Option<tokio_rustls::TlsAcceptor>;
#[cfg(not(feature = "tls"))]
pub type TlsAcceptorOpt = Option<std::convert::Infallible>;

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
}

pub async fn run(
    listener: TcpListener,
    catalog: Arc<InMemoryCatalog>,
    dml_exec: Option<Arc<StorageExecutor>>,
    tls_acceptor: TlsAcceptorOpt,
    storage_engine: Option<Arc<StorageEngine>>,
) -> std::io::Result<()> {
    // ── Shared server-wide state ───────────────────────────────────────────
    let advisor = Arc::new(IndexAdvisor::new(Arc::clone(&catalog) as Arc<dyn Catalog>));
    let executor = Arc::new(IndexExecutor::new());
    let query_count = Arc::new(AtomicU64::new(0));
    let advisor_inflight = Arc::new(AtomicBool::new(false));
    let max_connections = read_max_connections();
    let semaphore = Arc::new(Semaphore::new(max_connections));
    update_active_connections(&semaphore, max_connections);

    // ── Authentication registry (Session 11) ──────────────────────────────
    let users_file = std::env::var("NEURALBASE_USERS_FILE")
        .unwrap_or_else(|_| "users.json".to_string());
    let registry = Arc::new(RwLock::new(UserRegistry::load_from_file(&users_file)));

    // ── Per-IP connection tracking (Session 11) ───────────────────────────
    let ip_tracker = IpConnectionTracker::new(read_max_per_ip());

    let require_auth = registry.read().await.require_auth;
    tracing::info!(
        max_connections,
        per_ip = ip_tracker.max_per_ip,
        require_auth,
        env_var = "NEURALBASE_MAX_CONNECTIONS",
        "connection admission control enabled"
    );

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;

        // ── Per-IP gate ────────────────────────────────────────────────────
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

        // ── Global semaphore gate ──────────────────────────────────────────
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
                // PostgreSQL STARTTLS handshake:
                // 1. Client sends 8-byte SSLRequest (length=8, code=80877103).
                // 2. Server responds 'S' (accept) or 'N' (decline).
                // 3. Client begins TLS ClientHello after 'S'.
                let mut ssl_req = [0u8; 8];
                if socket.read_exact(&mut ssl_req).await.is_err() {
                    return;
                }
                let is_ssl_request = ssl_req == [0, 0, 0, 8, 4, 0xd2, 0x16, 0x2f];
                if !is_ssl_request {
                    // Non-TLS connection when TLS is required — reject.
                    tracing::warn!(%peer_addr, "Rejecting plaintext connection (TLS required, send sslmode=require)");
                    // Write a well-formed ErrorResponse then close.
                    let err_bytes = build_error_response(
                        "plaintext connections not accepted -- reconnect with sslmode=require",
                        "28000",
                    );
                    let _ = socket.write_all(&err_bytes).await;
                    return;
                }
                // Respond 'S' — SSL accepted.
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

        // Suppress unused variable warning when tls feature is off.
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

async fn handle_client_stream<S>(
    mut socket: S,
    ctx: ClientSessionContext,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // startup_and_auth sends AuthOk (or an error) internally.
    if startup_and_auth(&mut socket, &ctx.registry).await.is_err() {
        return Ok(());
    }

    socket.write_all(&build_parameter_status("server_version", "16.0")).await?;
    socket.write_all(&build_parameter_status("client_encoding", "UTF8")).await?;
    socket.write_all(&build_backend_key_data(42, 7)).await?;
    socket.write_all(&build_ready_for_query()).await?;

    loop {
        let mut message_type = [0_u8; 1];
        match socket.read_exact(&mut message_type).await {
            Ok(_) => {}
            Err(_) => return Ok(()),
        }

        let mut len_bytes = [0_u8; 4];
        if socket.read_exact(&mut len_bytes).await.is_err() {
            return Ok(());
        }
        let len = i32::from_be_bytes(len_bytes);
        let payload_len = match parse_message_length(len) {
            Ok(len) => len,
            Err(err) => {
                write_error_and_ready(&mut socket, &err.to_string(), "08P01").await?;
                continue;
            }
        };

        let mut payload = vec![0_u8; payload_len];
        if socket.read_exact(&mut payload).await.is_err() {
            return Ok(());
        }

        match message_type[0] {
            b'Q' => {
                let sql_text = String::from_utf8_lossy(&payload)
                    .trim_end_matches('\0')
                    .to_string();
                let start = std::time::Instant::now();
                let scanner: Option<&dyn TableScanner> =
                    ctx.dml_exec.as_deref().map(|s| s as &dyn TableScanner);
                process_query(
                    &mut socket,
                    &sql_text,
                    &ctx.catalog,
                    scanner,
                    ctx.dml_exec.as_deref(),
                    ctx.storage_engine.as_ref(),
                    &ctx.registry,
                )
                .await?;
                let elapsed_us = start.elapsed().as_micros() as u64;

                // Feed the workload monitor — drives self-tuning index recommendations.
                if let Some(pattern) = extract_query_pattern(&sql_text, elapsed_us) {
                    ctx.advisor.record_query(pattern);
                    let n = ctx.query_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(100) {
                        // Fire-and-forget: advise() + DDL is CPU-only, non-blocking.
                        let adv = Arc::clone(&ctx.advisor);
                        let exec = Arc::clone(&ctx.executor);
                        let eng = ctx.storage_engine.clone();
                        if ctx.advisor_inflight
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                        let inflight_task = Arc::clone(&ctx.advisor_inflight);
                        tokio::spawn(async move {
                            let decisions = adv.advise();

                            if let Some(engine) = eng {
                                // Sync existing index CFs so advisor tracks their age.
                                if let Ok(cfs) = engine.list_index_cfs() {
                                    for cf in cfs {
                                        adv.register_index(cf);
                                    }
                                }
                                // Apply create/drop decisions as real RocksDB DDL.
                                let results = exec.apply(&decisions, &engine);
                                for result in &results {
                                    match result {
                                        DdlResult::Created { index_name } => {
                                            // Newly created index: reset its drop-TTL clock.
                                            adv.touch_index(index_name);
                                            tracing::info!(index_name, "index created");
                                        }
                                        DdlResult::Dropped { index_name } => {
                                            tracing::info!(index_name, "index dropped");
                                        }
                                        DdlResult::Skipped { index_name, reason } => {
                                            tracing::debug!(index_name, reason, "index ddl skipped");
                                        }
                                        DdlResult::Failed { index_name, error } => {
                                            tracing::warn!(index_name, error, "index ddl failed");
                                        }
                                    }
                                }
                                tracing::debug!(
                                    active_indexes = exec.applied_indexes().len(),
                                    "advisor DDL cycle complete"
                                );
                            }

                            // Log all recommendations with full structured detail.
                            for d in &decisions {
                                match d {
                                    IndexDecision::Create {
                                        candidate,
                                        estimated_benefit,
                                        estimated_cost_bytes,
                                        reason,
                                    } => {
                                        tracing::debug!(
                                            index = candidate.index_name(),
                                            benefit = estimated_benefit,
                                            cost_bytes = estimated_cost_bytes,
                                            %reason,
                                            "advisor: CREATE INDEX"
                                        );
                                    }
                                    IndexDecision::Drop {
                                        index_name,
                                        unused_for,
                                        reason,
                                    } => {
                                        tracing::debug!(
                                            %index_name,
                                            unused_secs = unused_for.as_secs(),
                                            %reason,
                                            "advisor: DROP INDEX"
                                        );
                                    }
                                }
                            }
                            inflight_task.store(false, Ordering::Release);
                        });
                        }
                    }
                }

                socket.write_all(&build_ready_for_query()).await?;
            }
            b'X' => return Ok(()),
            _ => {
                write_error_and_ready(&mut socket, "unsupported frontend message", "0A000").await?;
            }
        }
    }
}

async fn startup_and_auth<S>(
    socket: &mut S,
    registry: &RwLock<UserRegistry>,
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ── Phase 1: read startup message ─────────────────────────────────────
    let (username, payload) = read_startup_message(socket).await?;

    // ── Phase 2: authentication ───────────────────────────────────────────
    let cred = {
        let reg = registry.read().await;
        if !reg.require_auth {
            // Dev/test mode: accept without challenge.
            drop(reg);
            socket
                .write_all(&build_auth_ok())
                .await
                .map_err(|_| ProtocolError::InvalidLength(0))?;
            return Ok(username);
        }
        match reg.get_user(&username) {
            Some(user) => user.credential.clone(),
            None => {
                drop(reg);
                let err = build_error_response(
                    &format!("password authentication failed for user \"{}\"", username),
                    "28P01",
                );
                let _ = socket.write_all(&err).await;
                return Err(ProtocolError::InvalidLength(0));
            }
        }
    };

    match cred {
        StoredCredential::ScramSha256(keys) => {
            perform_scram_auth(socket, username, keys, &payload).await
        }
        StoredCredential::Md5 { password_hash } => {
            perform_md5_auth(socket, username, password_hash).await
        }
    }
}

/// Read the startup message, handling SSL requests transparently.
/// Returns (username, raw_startup_body).
async fn read_startup_message<S>(
    socket: &mut S,
) -> Result<(String, Vec<u8>), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let mut len_bytes = [0_u8; 4];
        socket
            .read_exact(&mut len_bytes)
            .await
            .map_err(|_| ProtocolError::InvalidLength(0))?;
        let len = i32::from_be_bytes(len_bytes);
        let payload_len = parse_message_length(len)?;
        let mut payload = vec![0_u8; payload_len];
        socket
            .read_exact(&mut payload)
            .await
            .map_err(|_| ProtocolError::InvalidLength(len))?;
        let code = parse_startup_body(&payload)?;
        if code == SSL_REQUEST_CODE {
            socket
                .write_all(b"N")
                .await
                .map_err(|_| ProtocolError::InvalidLength(0))?;
            continue;
        }
        if code != STARTUP_PROTOCOL_V3 {
            return Err(ProtocolError::InvalidLength(code));
        }
        let username = parse_startup_username(&payload);
        return Ok((username, payload));
    }
}

async fn perform_scram_auth<S>(
    socket: &mut S,
    username: String,
    keys: crate::auth::ScramKeys,
    _startup_payload: &[u8],
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut scram = ScramServer::new(keys);

    // Send AuthenticationSASL.
    socket
        .write_all(&build_auth_sasl_request(&["SCRAM-SHA-256"]))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    // Read SASLInitialResponse ('p' message).
    let client_first = read_frontend_message_payload(socket).await?;
    let (_, initial_data) = parse_sasl_initial_response(&client_first)
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let client_first_str =
        String::from_utf8(initial_data).map_err(|_| ProtocolError::InvalidLength(0))?;

    let server_first = scram
        .process_client_first(&client_first_str)
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    // Send AuthenticationSASLContinue.
    socket
        .write_all(&build_auth_sasl_continue(server_first.as_bytes()))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    // Read SASLResponse ('p' message).
    let client_final_payload = read_frontend_message_payload(socket).await?;
    let client_final =
        String::from_utf8(client_final_payload).map_err(|_| ProtocolError::InvalidLength(0))?;

    let server_sig = scram
        .process_client_final(&client_final)
        .map_err(|_| {
            ProtocolError::InvalidLength(0)
        })?;

    // Send AuthenticationSASLFinal then AuthenticationOk.
    let final_msg = format!("v={}", server_sig);
    socket
        .write_all(&build_auth_sasl_final(final_msg.as_bytes()))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    socket
        .write_all(&build_auth_ok())
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    Ok(username)
}

async fn perform_md5_auth<S>(
    socket: &mut S,
    username: String,
    password_hash: String,
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let md5_state = Md5State::new(password_hash);

    // Send AuthenticationMD5Password.
    socket
        .write_all(&build_auth_md5_request(&md5_state.salt))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    // Read PasswordMessage ('p' message).
    let pw_payload = read_frontend_message_payload(socket).await?;
    let response = String::from_utf8(
        pw_payload.strip_suffix(b"\0").unwrap_or(&pw_payload).to_vec(),
    )
    .map_err(|_| ProtocolError::InvalidLength(0))?;

    if !md5_state.verify(&response) {
        let err = build_error_response(
            &format!("password authentication failed for user \"{}\"", username),
            "28P01",
        );
        let _ = socket.write_all(&err).await;
        return Err(ProtocolError::InvalidLength(0));
    }

    socket
        .write_all(&build_auth_ok())
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    Ok(username)
}

/// Read one frontend message and return its payload (type byte discarded).
/// Used for 'p' (PasswordMessage / SASLInitialResponse / SASLResponse) messages.
async fn read_frontend_message_payload<S>(socket: &mut S) -> Result<Vec<u8>, ProtocolError>
where
    S: AsyncRead + Unpin,
{
    let mut type_byte = [0u8; 1];
    socket
        .read_exact(&mut type_byte)
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let mut len_bytes = [0u8; 4];
    socket
        .read_exact(&mut len_bytes)
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let len = i32::from_be_bytes(len_bytes);
    let payload_len = parse_message_length(len)?;
    let mut payload = vec![0u8; payload_len];
    socket
        .read_exact(&mut payload)
        .await
        .map_err(|_| ProtocolError::InvalidLength(len))?;
    Ok(payload)
}


async fn process_query<S>(
    socket: &mut S,
    sql: &str,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
    dml_exec: Option<&StorageExecutor>,
    storage_engine: Option<&Arc<StorageEngine>>,
    registry: &RwLock<UserRegistry>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let nb_stmt = match parse_nb_statement(sql) {
        Ok(s) => s,
        Err(err) => {
            write_error_and_ready(socket, &err.to_string(), "42601").await?;
            return Ok(());
        }
    };

    let plan = match bind_nb_statement(&nb_stmt, catalog) {
        Ok(plan) => plan,
        Err(err) => {
            write_error_and_ready(socket, &err.to_string(), "42P01").await?;
            return Ok(());
        }
    };

    match plan {
        BoundPlan::SelectConstI64(value) => {
            let batch = mock_const_batch(value);
            write_batch(socket, &batch).await?;
        }
        BoundPlan::SelectFromTable { .. } => {
            let physical_plan = build_physical_plan(&plan);
            let dataset = generate_tpch_data(0.1);
            let scheduler = MorselScheduler::new(16_384);
            match execute_physical_plan(&physical_plan, &dataset, &scheduler, storage) {
                Ok(batch) => write_batch(socket, &batch).await?,
                Err(err) => {
                    write_error_and_ready(socket, &err.to_string(), "22000").await?;
                }
            }
        }
        BoundPlan::SelectQuery(query) => {
            let dataset = generate_tpch_data(0.1);
            let mut qcat = QueryCatalog::from_tpch(&dataset);
            // Inject any user-defined tables from the in-memory catalog.
            for schema in catalog.all_tables() {
                if !qcat.tables.contains_key(&schema.name.to_lowercase()) {
                    if let Some(scanner) = storage {
                        if let Ok(batch) = scanner.scan_table(&schema.name) {
                            qcat.add_batch(&schema.name, &batch);
                        }
                    }
                }
            }
            match crate::query_executor::execute_select_query(&query, &qcat) {
                Ok(result) => {
                    let batch = query_result_to_batch(result);
                    write_batch(socket, &batch).await?;
                }
                Err(err) => {
                    write_error_and_ready(socket, &err.to_string(), "22000").await?;
                }
            }
        }
        BoundPlan::DropTable { name } => {
            catalog.drop_table(&name);
            if let Some(engine) = storage_engine {
                let rdb = RocksDbCatalog::new(engine.clone());
                if let Err(e) = rdb.unregister_table(&name) {
                    tracing::warn!(error = %e, table = %name, "failed to remove persisted schema entry");
                }
                if let Err(e) = engine.clear_table_data(table_id_for(&name)) {
                    tracing::warn!(error = %e, table = %name, "failed to clear dropped table data");
                }
            }
            socket.write_all(&build_command_complete("DROP TABLE")).await?;
        }
        BoundPlan::CreateTable(create_plan) => {
            let schema = create_plan.to_table_schema();
            catalog.create_table(schema.clone());
            // Persist to RocksDB so CREATE TABLE survives process restart.
            if let Some(engine) = storage_engine {
                let rdb = RocksDbCatalog::new(engine.clone());
                if let Err(e) = rdb.register_table(&schema) {
                    tracing::warn!(error = %e, "Failed to persist schema to RocksDB");
                }
            }
            socket
                .write_all(&build_command_complete("CREATE TABLE"))
                .await?;
        }
        BoundPlan::Insert(insert_plan) => {
            let Some(exec) = dml_exec else {
                write_error_and_ready(socket, "storage not available (DB_PATH not set)", "55000")
                    .await?;
                return Ok(());
            };
            let mut count = 0u64;
            for row_values in &insert_plan.rows {
                // Build (col, val_string) pairs for encode_row.
                let pairs: Vec<(String, String)> = insert_plan
                    .columns
                    .iter()
                    .zip(row_values)
                    .map(|(col, val)| {
                        let s = val
                            .to_storage_string()
                            .unwrap_or_default();
                        (col.clone(), s)
                    })
                    .collect();
                let str_pairs: Vec<(&str, &str)> = pairs
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                // PK = HLC bytes from the txn manager's clock (unique monotone key).
                let pk = exec.next_pk();
                match exec.insert_row(&insert_plan.table.name, &pk, &str_pairs) {
                    Ok(()) => count += 1,
                    Err(e) => {
                        write_error_and_ready(socket, &e.to_string(), "22000").await?;
                        return Ok(());
                    }
                }
            }
            socket
                .write_all(&build_command_complete(&format!("INSERT 0 {count}")))
                .await?;
        }
        BoundPlan::Update(update_plan) => {
            let Some(exec) = dml_exec else {
                write_error_and_ready(socket, "storage not available (DB_PATH not set)", "55000")
                    .await?;
                return Ok(());
            };
            let assignments: Vec<(String, String)> = update_plan
                .assignments
                .iter()
                .map(|(col, val)| {
                    let s = val.to_storage_string().unwrap_or_default();
                    (col.clone(), s)
                })
                .collect();
            match exec.update_rows(
                &update_plan.table.name,
                &assignments,
                update_plan.predicate.as_ref(),
            ) {
                Ok(n) => {
                    socket
                        .write_all(&build_command_complete(&format!("UPDATE {n}")))
                        .await?;
                }
                Err(e) => {
                    write_error_and_ready(socket, &e.to_string(), "22000").await?;
                }
            }
        }
        BoundPlan::Delete(delete_plan) => {
            let Some(exec) = dml_exec else {
                write_error_and_ready(socket, "storage not available (DB_PATH not set)", "55000")
                    .await?;
                return Ok(());
            };
            match exec.delete_rows(&delete_plan.table.name, delete_plan.predicate.as_ref()) {
                Ok(n) => {
                    socket
                        .write_all(&build_command_complete(&format!("DELETE {n}")))
                        .await?;
                }
                Err(e) => {
                    write_error_and_ready(socket, &e.to_string(), "22000").await?;
                }
            }
        }
        BoundPlan::CreateUser { username, password } => {
            let new_record = create_scram_user(&username, &password);
            registry.write().await.add_user(new_record);
            socket.write_all(&build_command_complete("CREATE USER")).await?;
        }
        BoundPlan::AlterUser { username, new_password } => {
            let updated = create_scram_user(&username, &new_password);
            if registry.write().await.update_user(updated) {
                socket.write_all(&build_command_complete("ALTER USER")).await?;
            } else {
                write_error_and_ready(
                    socket,
                    &format!("user not found: {}", username),
                    "42704",
                )
                .await?;
            }
        }
        BoundPlan::DropUser { username, if_exists } => {
            let removed = registry.write().await.remove_user(&username);
            if !removed && !if_exists {
                write_error_and_ready(
                    socket,
                    &format!("user not found: {}", username),
                    "42704",
                )
                .await?;
            } else {
                socket.write_all(&build_command_complete("DROP USER")).await?;
            }
        }
    }

    Ok(())
}

async fn write_batch<S>(
    socket: &mut S,
    batch: &crate::vectorized::RecordBatch,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let (columns, rows) = batch_to_pg_rows(batch);
    let row_count = rows.len();
    let cols = columns
        .iter()
        .map(|(name, oid, size)| (name.as_str(), *oid, *size))
        .collect::<Vec<_>>();
    socket.write_all(&build_row_description(&cols)).await?;
    for row in rows {
        socket.write_all(&build_data_row(&row)).await?;
    }
    socket.write_all(&build_command_complete(&format!("SELECT {row_count}"))).await?;
    Ok(())
}

async fn write_error_and_ready<S>(
    socket: &mut S,
    message: &str,
    code: &str,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket.write_all(&build_error_response(message, code)).await?;
    socket.write_all(&build_ready_for_query()).await?;
    Ok(())
}

// ── Index advisor helpers ─────────────────────────────────────────────────

/// Build a `QueryPattern` from raw SQL text for the workload monitor.
///
/// Parses the SQL, extracts table names from the FROM clause and column
/// references from the WHERE clause.  Returns `None` for non-SELECT
/// statements or parse errors — the advisor is best-effort only.
fn extract_query_pattern(sql: &str, elapsed_us: u64) -> Option<QueryPattern> {
    let stmt = parse_statement(sql).ok()?;
    let Statement::Query(q) = &stmt else {
        return None;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };

    let tables: Vec<String> = sel
        .from
        .iter()
        .filter_map(|tw| {
            if let TableFactor::Table { name, .. } = &tw.relation {
                Some(name.to_string().to_lowercase())
            } else {
                None
            }
        })
        .collect();

    if tables.is_empty() {
        return None;
    }

    // Extract predicate column references for the primary table.
    let primary_table = &tables[0];
    let predicate_columns = extract_where_columns(&sel.selection, primary_table);

    Some(QueryPattern {
        tables,
        predicate_columns,
        join_columns: vec![],
        rows_scanned: 0,
        rows_returned: 0,
        elapsed_us,
        recorded_at: std::time::Instant::now(),
    })
}

/// Collect `(table, column)` pairs from a WHERE expression.
/// Best-effort: handles `BinaryOp`, `Identifier`, `CompoundIdentifier`, `Nested`.
fn extract_where_columns(
    expr: &Option<Expr>,
    table: &str,
) -> Vec<(String, String)> {
    let Some(e) = expr else {
        return vec![];
    };
    let mut cols = vec![];
    collect_col_refs(e, table, &mut cols);
    cols
}

fn collect_col_refs(expr: &Expr, table: &str, out: &mut Vec<(String, String)>) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_col_refs(left, table, out);
            collect_col_refs(right, table, out);
        }
        Expr::Identifier(ident) => {
            out.push((table.to_string(), ident.value.to_lowercase()));
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(col) = parts.last() {
                out.push((table.to_string(), col.value.to_lowercase()));
            }
        }
        Expr::Nested(inner) => collect_col_refs(inner, table, out),
        _ => {}
    }
}
