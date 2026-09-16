// SPDX-License-Identifier: Apache-2.0
//! Three real processes, independent RocksDB directories and production TCP Raft.
//! Timed operations are never retried. Setup alone tolerates known catch-up states.
//! These same-host measurements do not establish multi-host latency or reliable p99.

use std::collections::HashSet;
use std::fs::{self, File};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const START_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(20);
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const CASE_TIMEOUT: Duration = Duration::from_secs(300);
const WARMUP: usize = 5;
const REPS: usize = 100;
const DATASET_SIZES: &[usize] = &[100, 1_000, 10_000, 25_000];
const CONCURRENCY_LEVELS: &[usize] = &[1, 8, 32, 64];
const READS_PER_CLIENT: usize = 64;
const WRITES_PER_CLIENT: usize = 16;
const SEED_BATCH_ROWS: usize = 256;
const NOISE_ROWS_PER_TABLE: usize = 128;

type Rows = Vec<(i64, i64)>;

struct NodeProcess {
    id: String,
    sql_port: u16,
    log_path: PathBuf,
    child: Child,
}

struct Cluster {
    _root: TempDir,
    nodes: Vec<NodeProcess>,
    succeeded: bool,
}

impl Cluster {
    fn ports(&self) -> Vec<u16> {
        self.nodes.iter().map(|node| node.sql_port).collect()
    }

    fn check_alive(&mut self) -> Result<(), String> {
        for node in &mut self.nodes {
            if let Some(status) = node.child.try_wait().map_err(|error| error.to_string())? {
                return Err(format!("{} exited with {status}", node.id));
            }
        }
        Ok(())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if !self.succeeded {
                eprintln!(
                    "PHASE11_NODE {} port={} status={:?} log={}",
                    node.id,
                    node.sql_port,
                    node.child.try_wait(),
                    node.log_path.display()
                );
            }
            let _ = node.child.kill();
        }
        for node in &mut self.nodes {
            let _ = node.child.wait();
            if !self.succeeded {
                let log = fs::read_to_string(&node.log_path).unwrap_or_default();
                let lines: Vec<_> = log.lines().rev().take(60).collect();
                eprintln!("{} log tail:", node.id);
                for line in lines.into_iter().rev() {
                    eprintln!("{line}");
                }
            }
        }
    }
}

fn reserve_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = listener.local_addr().expect("reserved address").port();
        if used.insert(port) {
            return port;
        }
    }
}

#[derive(Debug)]
struct SqlFailure {
    code: Option<String>,
    message: String,
    context: String,
}

impl SqlFailure {
    fn from_error(error: tokio_postgres::Error, context: String) -> Self {
        let code = error.as_db_error().map(|db| db.code().code().to_string());
        let message = error
            .as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| error.to_string());
        Self {
            code,
            message,
            context: format!("{context}; details={error:?}"),
        }
    }

    fn diagnostic(&self) -> String {
        format!(
            "SQLSTATE={:?} message={}; {}",
            self.code, self.message, self.context
        )
    }

    fn pre_submit_leadership(&self) -> bool {
        matches!(self.code.as_deref(), Some("25006" | "57P03"))
    }

    fn setup_catch_up(&self, table: &str) -> bool {
        self.code.as_deref() == Some("57P03")
            || (matches!(self.code.as_deref(), Some("22000" | "42P01"))
                && self.message == format!("table not found: {table}"))
    }
}

struct SqlClient {
    client: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
    port: u16,
}

impl Drop for SqlClient {
    fn drop(&mut self) {
        // Also closes the socket when a worker or query deadline cancels its owner.
        self.connection.abort();
    }
}

async fn connect(port: u16) -> Result<SqlClient, String> {
    let config = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
    let (client, connection) = timeout(
        Duration::from_secs(2),
        tokio_postgres::connect(&config, NoTls),
    )
    .await
    .map_err(|_| format!("connect/handshake timed out on port {port}"))?
    .map_err(|error| SqlFailure::from_error(error, format!("connect port={port}")).diagnostic())?;
    Ok(SqlClient {
        client,
        connection: tokio::spawn(connection),
        port,
    })
}

async fn query(client: &SqlClient, sql: &str) -> Result<Vec<SimpleQueryMessage>, SqlFailure> {
    let context = format!(
        "port={} SQL={}",
        client.port,
        sql.chars().take(240).collect::<String>()
    );
    timeout(QUERY_TIMEOUT, client.client.simple_query(sql))
        .await
        .map_err(|_| SqlFailure {
            code: None,
            message: format!("query exceeded {QUERY_TIMEOUT:?}; outcome may be ambiguous"),
            context: context.clone(),
        })?
        .map_err(|error| SqlFailure::from_error(error, context))
}

async fn set_consistency(client: &SqlClient, mode: &str) -> Result<(), String> {
    query(
        client,
        &format!("SET neuralbase_read_consistency = '{mode}'"),
    )
    .await
    .map_err(|error| error.diagnostic())?;
    Ok(())
}

async fn execute_mutation(client: &SqlClient, sql: &str) -> Result<(), String> {
    query(client, sql)
        .await
        .map_err(|error| error.diagnostic())?;
    Ok(())
}

fn decode_rows(messages: Vec<SimpleQueryMessage>) -> Result<Rows, String> {
    let mut rows = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            if row.len() != 2 {
                return Err(format!("expected two columns, received {}", row.len()));
            }
            let value = |column| {
                row.get(column)
                    .ok_or_else(|| "unexpected NULL".to_string())?
                    .parse::<i64>()
                    .map_err(|error| error.to_string())
            };
            rows.push((value(0)?, value(1)?));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

async fn spawn_cluster(scenario: &str) -> Result<Cluster, String> {
    let root = TempDir::new().map_err(|error| error.to_string())?;
    let logs = std::env::var_os("PHASE11_RESULTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.path().to_path_buf())
        .join(format!("{scenario}-nodes"));
    fs::create_dir_all(&logs).map_err(|error| error.to_string())?;
    let mut used = HashSet::new();
    let ports: Vec<_> = (0..3)
        .map(|_| {
            (
                reserve_port(&mut used),
                reserve_port(&mut used),
                reserve_port(&mut used),
            )
        })
        .collect();
    let peers = ports
        .iter()
        .enumerate()
        .map(|(index, (_, raft, _))| format!("p11-node{index}=127.0.0.1:{raft}"))
        .collect::<Vec<_>>()
        .join(",");
    // Own children immediately so even a partial startup failure cleans them up.
    let mut cluster = Cluster {
        _root: root,
        nodes: Vec::new(),
        succeeded: false,
    };
    for (index, &(sql, raft, metrics)) in ports.iter().enumerate() {
        let id = format!("p11-node{index}");
        let log_path = logs.join(format!("{id}.log"));
        let log = File::create(&log_path).map_err(|error| error.to_string())?;
        let stderr = log.try_clone().map_err(|error| error.to_string())?;
        let child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
            .env("NEURALBASE_NODE_ID", &id)
            .env("NEURALBASE_LISTEN_ADDR", format!("127.0.0.1:{sql}"))
            .env("NEURALBASE_RAFT_ADDR", format!("127.0.0.1:{raft}"))
            .env("NEURALBASE_PEERS", &peers)
            .env(
                "NEURALBASE_DB_PATH",
                cluster._root.path().join(format!("node{index}-db")),
            )
            .env(
                "NEURALBASE_USERS_FILE",
                cluster._root.path().join(format!("node{index}-users.json")),
            )
            .env("NEURALBASE_METRICS_PORT", metrics.to_string())
            .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "1000")
            .env("NEURALBASE_AUTH_REQUIRED", "0")
            .env("NEURALBASE_RAFT_TLS", "0")
            .env("RUST_LOG", "warn")
            .env_remove("NEURALBASE_OPERATOR_NODE")
            .env_remove("NEURALBASE_PITR_ARCHIVE_DIR")
            .env_remove("NEURALBASE_PITR_KEY_FILE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| format!("spawn {id}: {error}"))?;
        cluster.nodes.push(NodeProcess {
            id,
            sql_port: sql,
            log_path,
            child,
        });
    }
    for port in cluster.ports() {
        let deadline = Instant::now() + START_TIMEOUT;
        loop {
            cluster.check_alive()?;
            match connect(port).await {
                Ok(_) => break,
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => sleep(Duration::from_millis(30)).await,
            }
        }
    }
    Ok(cluster)
}

async fn find_mutation_leader(cluster: &mut Cluster, sql: &str) -> Result<u16, String> {
    let probe = async {
        loop {
            cluster.check_alive()?;
            for port in cluster.ports() {
                let client = connect(port).await?;
                match query(&client, sql).await {
                    Ok(_) => return Ok(port),
                    Err(error) if error.pre_submit_leadership() => {}
                    Err(error) => return Err(error.diagnostic()),
                }
            }
            sleep(Duration::from_millis(30)).await;
        }
    };
    timeout(START_TIMEOUT, probe)
        .await
        .map_err(|_| "leader discovery timed out; mutation outcome may be ambiguous".to_string())?
}

fn is_replication_subset(rows: &[(i64, i64)], expected: &[(i64, i64)]) -> bool {
    rows.windows(2).all(|pair| pair[0] != pair[1])
        && rows.iter().all(|row| expected.binary_search(row).is_ok())
}

async fn wait_replicated_rows(
    cluster: &mut Cluster,
    table: &str,
    expected: &[(i64, i64)],
) -> Result<(), String> {
    let sql = format!("SELECT id, value FROM {table}");
    let mut last = String::from("no probe completed");
    let wait = async {
        for port in cluster.ports() {
            let mut reported = false;
            loop {
                cluster.check_alive()?;
                // NeuralBase closes a PostgreSQL session after a query error, so each
                // setup probe gets a fresh connection. Timed operations are unaffected.
                let client = connect(port).await?;
                set_consistency(&client, "local").await?;
                match query(&client, &sql).await {
                    Ok(messages) => {
                        let rows = decode_rows(messages)?;
                        if rows.as_slice() == expected {
                            break;
                        }
                        if !is_replication_subset(&rows, expected) {
                            return Err(format!("unexpected replicated values: port={port} table={table} rows={rows:?}"));
                        }
                        last = format!(
                            "port={port} table={table}: {} of {} rows applied",
                            rows.len(),
                            expected.len()
                        );
                    }
                    Err(error) if error.setup_catch_up(table) => last = error.diagnostic(),
                    Err(error) => return Err(error.diagnostic()),
                }
                if !reported {
                    eprintln!("PHASE11_CATCH_UP {last}");
                    reported = true;
                }
                sleep(Duration::from_millis(20)).await;
            }
        }
        Ok(())
    };
    match timeout(REPLICATION_TIMEOUT, wait).await {
        Ok(result) => result,
        Err(_) => Err(format!("replication deadline exceeded: {last}")),
    }
}

async fn seed_rows(cluster: &mut Cluster, start: usize, end: usize) -> Result<u16, String> {
    let mut leader = None;
    for batch_start in (start..end).step_by(SEED_BATCH_ROWS) {
        let batch_end = (batch_start + SEED_BATCH_ROWS).min(end);
        let values = (batch_start..batch_end)
            .map(|id| format!("({id}, {})", id * 3 + 7))
            .collect::<Vec<_>>()
            .join(",");
        leader = Some(
            find_mutation_leader(
                cluster,
                &format!("INSERT INTO p11_items VALUES {values}"),
            )
            .await?,
        );
    }
    leader.ok_or_else(|| "seed range must contain at least one row".to_string())
}

fn item_rows(count: usize) -> Rows {
    (0..count)
        .map(|id| (id as i64, (id * 3 + 7) as i64))
        .collect()
}

fn emit(prefix: &str, mut record: Value) {
    record["schema"] = json!(2);
    record["label"] = json!(std::env::var("PHASE11_LABEL").unwrap_or_else(|_| "head".to_string()));
    record["commit"] =
        json!(std::env::var("PHASE11_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".to_string()));
    // Start a fresh line even when libtest has just printed the test name.
    println!("\n{prefix} {record}");
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    sorted[((sorted.len() - 1) * percentile).div_ceil(100)]
}

fn emit_latency(name: &str, samples: &[u64], warmup: usize, extra: Value) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum: u128 = sorted.iter().map(|value| *value as u128).sum();
    emit(
        "PHASE11_RESULT",
        json!({
            "name": name, "path": "postgres_wire_three_voters_same_host",
            "unit": "ns", "warmup_iterations_per_client": warmup,
            "measured_iterations": sorted.len(), "successful_ops": sorted.len(), "failed_ops": 0,
            "min": sorted[0], "p50": percentile(&sorted, 50), "p95": percentile(&sorted, 95),
            "p99": percentile(&sorted, 99), "max": sorted[sorted.len() - 1],
            "mean": (sum / sorted.len() as u128) as u64, "extra": extra
        }),
    );
}

#[derive(Clone)]
enum Workload {
    Read {
        consistency: &'static str,
        sql: String,
        expected: (i64, i64),
    },
    Write {
        id_base: usize,
    },
}

impl Workload {
    async fn run(
        &self,
        client: &SqlClient,
        worker: usize,
        operation: usize,
        stride: usize,
    ) -> Result<u64, String> {
        let sql = match self {
            Self::Read { sql, .. } => sql.clone(),
            Self::Write { id_base } => {
                let id = id_base + worker * stride + operation;
                format!("INSERT INTO p11_write_load VALUES ({id}, {})", id * 5 + 1)
            }
        };
        let start = Instant::now();
        let messages = query(client, &sql)
            .await
            .map_err(|error| error.diagnostic())?;
        let elapsed = start.elapsed().as_nanos() as u64;
        match self {
            Self::Read { expected, .. } => {
                let rows = decode_rows(messages)?;
                if rows != vec![*expected] {
                    return Err(format!(
                        "port={} SQL={sql}: expected {expected:?}, got {rows:?}",
                        client.port
                    ));
                }
            }
            Self::Write { .. } => {
                if !messages
                    .iter()
                    .any(|message| matches!(message, SimpleQueryMessage::CommandComplete(1)))
                {
                    return Err(format!(
                        "port={} INSERT did not acknowledge exactly one row",
                        client.port
                    ));
                }
            }
        }
        Ok(elapsed)
    }
}

async fn measure_query(
    port: u16,
    workload: Workload,
    name: &str,
    extra: Value,
) -> Result<(), String> {
    emit("PHASE11_PHASE", json!({"name": name, "status": "started"}));
    let client = connect(port).await?;
    if let Workload::Read { consistency, .. } = &workload {
        set_consistency(&client, consistency).await?;
    }
    let measurement = async {
        for operation in 0..WARMUP {
            workload.run(&client, 0, operation, 0).await?;
        }
        let mut samples = Vec::with_capacity(REPS);
        for operation in 0..REPS {
            samples.push(workload.run(&client, 0, operation, 0).await?);
        }
        Ok::<_, String>(samples)
    };
    let samples = timeout(CASE_TIMEOUT, measurement)
        .await
        .map_err(|_| format!("case deadline exceeded: {name}"))??;
    emit_latency(name, &samples, WARMUP, extra);
    Ok(())
}

async fn run_concurrent(
    port: u16,
    clients: usize,
    workload: Workload,
    warmup: usize,
    operations: usize,
) -> Result<(Vec<u64>, u64), String> {
    let (ready_tx, mut ready_rx) = mpsc::channel(clients);
    let (start_tx, start_rx) = watch::channel(false);
    let mut workers = JoinSet::new();
    for worker in 0..clients {
        let ready = ready_tx.clone();
        let mut start = start_rx.clone();
        let workload = workload.clone();
        workers.spawn(async move {
            let client = connect(port).await?;
            if let Workload::Read { consistency, .. } = &workload {
                set_consistency(&client, consistency).await?;
            }
            for operation in 0..warmup {
                workload
                    .run(&client, worker, operation, warmup + operations)
                    .await?;
            }
            ready
                .send(())
                .await
                .map_err(|_| "readiness receiver closed".to_string())?;
            start
                .wait_for(|released| *released)
                .await
                .map_err(|_| "start cancelled".to_string())?;
            let mut samples = Vec::with_capacity(operations);
            for operation in warmup..warmup + operations {
                samples.push(
                    workload
                        .run(&client, worker, operation, warmup + operations)
                        .await?,
                );
            }
            Ok::<_, String>((samples, Instant::now()))
        });
    }
    drop(ready_tx);
    // A failed/panicked worker is observed during readiness, before opening the gate.
    let ready = async {
        for _ in 0..clients {
            tokio::select! {
                message = ready_rx.recv() => {
                    if message.is_none() {
                        return Err("workers disconnected before readiness".to_string());
                    }
                }
                finished = workers.join_next() => {
                    return Err(format!("worker exited before start: {finished:?}"));
                }
            }
        }
        Ok(())
    };
    timeout(CASE_TIMEOUT, ready)
        .await
        .map_err(|_| "worker readiness deadline exceeded".to_string())??;
    // Include gate release and scheduling; never let work start before this clock.
    let wall_start = Instant::now();
    start_tx
        .send(true)
        .map_err(|_| "all workers exited before start".to_string())?;
    let measurement = async {
        let mut samples = Vec::with_capacity(clients * operations);
        let mut last_finish = wall_start;
        while let Some(result) = workers.join_next().await {
            let (worker_samples, finished) =
                result.map_err(|error| format!("worker panicked: {error}"))??;
            samples.extend(worker_samples);
            last_finish = last_finish.max(finished);
        }
        if samples.len() != clients * operations {
            return Err("worker operation count mismatch".to_string());
        }
        Ok((
            samples,
            last_finish.duration_since(wall_start).as_nanos() as u64,
        ))
    };
    // Dropping JoinSet aborts outstanding workers; SqlClient then closes their sockets.
    timeout(CASE_TIMEOUT, measurement)
        .await
        .map_err(|_| "concurrent case deadline exceeded".to_string())?
}

fn process_memory_bytes(pid: u32, field: &str) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            return Some(rest.split_whitespace().next()?.parse::<u64>().ok()? * 1024);
        }
    }
    None
}

fn emit_memory(cluster: &Cluster, scenario: &str) {
    for node in &cluster.nodes {
        let pid = node.child.id();
        emit(
            "PHASE11_MEMORY",
            json!({
                "scenario": scenario, "node": node.id, "pid": pid,
                "rss_bytes": process_memory_bytes(pid, "VmRSS:"),
                "hwm_bytes": process_memory_bytes(pid, "VmHWM:")
            }),
        );
    }
}

async fn characterize() -> Result<(), String> {
    let mut cluster = spawn_cluster("dataset").await?;
    let mut port = find_mutation_leader(
        &mut cluster,
        "CREATE TABLE p11_items (id BIGINT, value BIGINT)",
    )
    .await?;
    let mut current_rows = 0;
    for &rows in DATASET_SIZES {
        emit(
            "PHASE11_PHASE",
            json!({"name": "seed", "dataset_rows": rows, "status": "started"}),
        );
        let start = Instant::now();
        port = seed_rows(&mut cluster, current_rows, rows).await?;
        let seed_ns = start.elapsed().as_nanos() as u64;
        current_rows = rows;
        wait_replicated_rows(&mut cluster, "p11_items", &item_rows(rows)).await?;
        emit(
            "PHASE11_SETUP",
            json!({"dataset_rows": rows, "incremental_seed_ns": seed_ns, "seed_batch_rows": SEED_BATCH_ROWS, "voters": 3}),
        );
        for consistency in ["local", "leader", "linearizable"] {
            measure_query(
                port,
                Workload::Read {
                    consistency,
                    sql: format!("SELECT id, value FROM p11_items WHERE id = {}", rows - 1),
                    expected: ((rows - 1) as i64, ((rows - 1) * 3 + 7) as i64),
                },
                &format!("scan_filter_read_{consistency}_{rows}_rows"),
                json!({"voters": 3, "dataset_rows": rows, "consistency": consistency}),
            )
            .await?;
        }
    }
    port = find_mutation_leader(
        &mut cluster,
        "CREATE TABLE p11_write_load (id BIGINT, value BIGINT)",
    )
    .await?;
    let mut writes = Vec::new();
    for &clients in CONCURRENCY_LEVELS {
        for consistency in ["local", "linearizable"] {
            let name = format!("concurrent_{consistency}_read_c{clients}");
            emit("PHASE11_PHASE", json!({"name": name, "status": "started"}));
            let workload = Workload::Read {
                consistency,
                sql: format!(
                    "SELECT id, value FROM p11_items WHERE id = {}",
                    current_rows - 1
                ),
                expected: (
                    (current_rows - 1) as i64,
                    ((current_rows - 1) * 3 + 7) as i64,
                ),
            };
            let (samples, wall_ns) =
                run_concurrent(port, clients, workload, WARMUP, READS_PER_CLIENT).await?;
            emit_latency(
                &name,
                &samples,
                WARMUP,
                json!({"voters": 3, "clients": clients, "dataset_rows": current_rows, "wall_ns": wall_ns, "throughput_ops_per_sec": samples.len() as f64 * 1_000_000_000.0 / wall_ns as f64}),
            );
        }
        let name = format!("concurrent_replicated_insert_c{clients}");
        emit("PHASE11_PHASE", json!({"name": name, "status": "started"}));
        let base = 1_000_000 + clients * 10_000;
        let (samples, wall_ns) = run_concurrent(
            port,
            clients,
            Workload::Write { id_base: base },
            WARMUP,
            WRITES_PER_CLIENT,
        )
        .await?;
        writes.extend(
            (base..base + clients * (WARMUP + WRITES_PER_CLIENT))
                .map(|id| (id as i64, (id * 5 + 1) as i64)),
        );
        writes.sort_unstable();
        wait_replicated_rows(&mut cluster, "p11_write_load", &writes).await?;
        emit_latency(
            &name,
            &samples,
            WARMUP,
            json!({"voters": 3, "clients": clients, "wall_ns": wall_ns, "throughput_ops_per_sec": samples.len() as f64 * 1_000_000_000.0 / wall_ns as f64}),
        );
    }
    emit_memory(&cluster, "dataset");
    cluster.succeeded = true;
    drop(cluster);
    characterize_noise().await?;
    emit("PHASE11_COMPLETE", json!({"result_count": 27}));
    Ok(())
}

async fn characterize_noise() -> Result<(), String> {
    // A fresh cluster makes zero unrelated tables an actual zero-table baseline.
    let mut cluster = spawn_cluster("noise").await?;
    let mut port = find_mutation_leader(
        &mut cluster,
        "CREATE TABLE p11_target (id BIGINT, value BIGINT)",
    )
    .await?;
    port = find_mutation_leader(
        &mut cluster,
        "INSERT INTO p11_target VALUES (0,10),(1,11),(2,12),(3,13)",
    )
    .await?;
    wait_replicated_rows(
        &mut cluster,
        "p11_target",
        &[(0, 10), (1, 11), (2, 12), (3, 13)],
    )
    .await?;
    let mut existing = 0;
    for total in [0, 16, 64] {
        for table in existing..total {
            port = find_mutation_leader(
                &mut cluster,
                &format!("CREATE TABLE p11_noise_{table:02} (id BIGINT)"),
            )
            .await?;
            let values = (0..NOISE_ROWS_PER_TABLE)
                .map(|row| format!("({row})"))
                .collect::<Vec<_>>()
                .join(",");
            port = find_mutation_leader(
                &mut cluster,
                &format!("INSERT INTO p11_noise_{table:02} VALUES {values}"),
            )
            .await?;
        }
        existing = total;
        measure_query(port, Workload::Read { consistency: "local", sql: "SELECT a.id, a.value FROM p11_target a JOIN p11_target b ON a.id = b.id WHERE a.id = 1".to_string(), expected: (1, 11) }, &format!("general_query_with_{total}_unrelated_tables"), json!({"voters": 3, "target_rows": 4, "unrelated_tables": total, "rows_per_unrelated_table": NOISE_ROWS_PER_TABLE})).await?;
    }
    emit_memory(&cluster, "noise");
    cluster.succeeded = true;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Phase 11 real-process performance characterization"]
async fn three_durable_voters_dataset_and_concurrency_characterization() {
    characterize().await.expect("three-voter characterization");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_voters_smoke() {
    let mut cluster = spawn_cluster("smoke").await.expect("cluster startup");
    let mut port = find_mutation_leader(
        &mut cluster,
        "CREATE TABLE p11_write_load (id BIGINT, value BIGINT)",
    )
    .await
    .expect("leader election");
    let client = connect(port).await.expect("leader connect");
    let missing = query(&client, "SELECT id, value FROM p11_items")
        .await
        .expect_err("table not yet created");
    assert!(
        missing.setup_catch_up("p11_items"),
        "{}",
        missing.diagnostic()
    );
    // The expected query error closes this NeuralBase protocol session.
    drop(client);
    // Let the missing-schema condition be observable, then refresh leadership for
    // every untimed setup mutation. Timed benchmark operations are never retried.
    sleep(Duration::from_millis(100)).await;
    port = find_mutation_leader(
        &mut cluster,
        "CREATE TABLE p11_items (id BIGINT, value BIGINT)",
    )
    .await
    .expect("create smoke table");
    port = seed_rows(&mut cluster, 0, 100)
        .await
        .expect("seed smoke rows");
    let expected = item_rows(100);
    wait_replicated_rows(&mut cluster, "p11_items", &expected)
        .await
        .expect("all three voters caught up");
    for consistency in ["local", "leader", "linearizable"] {
        let workload = Workload::Read {
            consistency,
            sql: "SELECT id, value FROM p11_items WHERE id = 99".to_string(),
            expected: (99, 304),
        };
        run_concurrent(port, 4, workload, 1, 2)
            .await
            .expect("concurrent smoke reads");
    }
    run_concurrent(port, 4, Workload::Write { id_base: 1000 }, 1, 2)
        .await
        .expect("concurrent smoke writes");
    let writes: Rows = (1000..1012).map(|id| (id, id * 5 + 1)).collect();
    wait_replicated_rows(&mut cluster, "p11_write_load", &writes)
        .await
        .expect("all acknowledged writes replicated");
    cluster.succeeded = true;
}

#[tokio::test]
async fn worker_connection_failure_does_not_hang_start_gate() {
    let workload = Workload::Read {
        consistency: "local",
        sql: "SELECT id, value FROM p11_items".to_string(),
        expected: (0, 7),
    };
    let result = timeout(Duration::from_secs(4), run_concurrent(0, 4, workload, 1, 2)).await;
    assert!(result.expect("startup failure must be bounded").is_err());
}

#[test]
fn setup_retry_is_restricted_to_expected_schema() {
    let mut error = SqlFailure {
        code: Some("22000".to_string()),
        message: "table not found: p11_items".to_string(),
        context: "test".to_string(),
    };
    assert!(error.setup_catch_up("p11_items"));
    assert!(!error.setup_catch_up("other_table"));
    error.message = "column not found: missing".to_string();
    assert!(!error.setup_catch_up("p11_items"));
    error.message = "table not found: p11_items".to_string();
    error.code = Some("42P01".to_string());
    assert!(error.setup_catch_up("p11_items"));
    error.code = Some("25006".to_string());
    assert!(error.pre_submit_leadership());
    error.code = Some("57P03".to_string());
    assert!(error.pre_submit_leadership());
    error.code = Some("XX000".to_string());
    assert!(!error.setup_catch_up("p11_items"));
    assert!(!error.pre_submit_leadership());
}

#[test]
fn replication_checks_reject_wrong_values_and_duplicates() {
    let expected = item_rows(100);
    assert!(is_replication_subset(&item_rows(50), &expected));
    assert!(!is_replication_subset(&[(0, 8)], &expected));
    assert!(!is_replication_subset(&[(0, 7), (0, 7)], &expected));
}
