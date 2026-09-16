// SPDX-License-Identifier: Apache-2.0
//! Phase 11 performance characterization using three real NeuralBase processes.
//!
//! This benchmark intentionally exercises the production clustered startup path:
//! three separate processes, three RocksDB directories, TCP Raft transport, and
//! confirmed durable state-machine apply. It records evidence; it asserts
//! correctness/readiness, but does not impose performance thresholds.

use std::collections::HashSet;
use std::fs::{self, File};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(20);
const WARMUP: usize = 5;
const REPS: usize = 40;
const DATASET_SIZES: &[usize] = &[100, 1_000, 10_000, 25_000];
const CONCURRENCY_LEVELS: &[usize] = &[1, 8, 32, 64];
const READS_PER_CLIENT: usize = 16;
const WRITES_PER_CLIENT: usize = 8;
const NOISE_ROWS_PER_TABLE: usize = 128;

#[derive(Clone)]
struct NodeSpec {
    id: String,
    sql_port: u16,
    raft_port: u16,
    metrics_port: u16,
    db_path: std::path::PathBuf,
    users_path: std::path::PathBuf,
    log_path: std::path::PathBuf,
}

struct NodeProcess {
    spec: NodeSpec,
    child: Child,
}

struct Cluster {
    _root: TempDir,
    nodes: Vec<NodeProcess>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.kill();
        }
        for node in &mut self.nodes {
            let _ = node.child.wait();
        }
    }
}

fn reserve_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = listener.local_addr().expect("reserved address").port();
        drop(listener);
        if used.insert(port) {
            return port;
        }
    }
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=2"),
        NoTls,
    )
}

fn is_retryable_leadership_error(error: &postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| matches!(db.code().code(), "25006" | "57P03"))
}

fn wait_socket(node: &mut NodeProcess) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = node.child.try_wait().expect("inspect child") {
            let log = fs::read_to_string(&node.spec.log_path).unwrap_or_default();
            panic!(
                "{} exited during startup with {status}; log:\n{log}",
                node.spec.id
            );
        }
        if connect(node.spec.sql_port).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} SQL socket did not start",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(30));
    }
}

fn spawn_cluster() -> Cluster {
    let root = TempDir::new().expect("cluster tempdir");
    let mut used = HashSet::new();
    let mut specs = Vec::with_capacity(3);

    for index in 0..3 {
        specs.push(NodeSpec {
            id: format!("p11-node{}", index + 1),
            sql_port: reserve_port(&mut used),
            raft_port: reserve_port(&mut used),
            metrics_port: reserve_port(&mut used),
            db_path: root.path().join(format!("node{index}-db")),
            users_path: root.path().join(format!("node{index}-users.json")),
            log_path: root.path().join(format!("node{index}.log")),
        });
    }

    let peer_spec = specs
        .iter()
        .map(|spec| format!("{}=127.0.0.1:{}", spec.id, spec.raft_port))
        .collect::<Vec<_>>()
        .join(",");

    let mut nodes = Vec::with_capacity(3);
    for spec in specs {
        let log = File::create(&spec.log_path).expect("create node log");
        let log_err = log.try_clone().expect("clone node log");
        let child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
            .env("NEURALBASE_NODE_ID", &spec.id)
            .env(
                "NEURALBASE_LISTEN_ADDR",
                format!("127.0.0.1:{}", spec.sql_port),
            )
            .env(
                "NEURALBASE_RAFT_ADDR",
                format!("127.0.0.1:{}", spec.raft_port),
            )
            .env("NEURALBASE_PEERS", &peer_spec)
            .env("NEURALBASE_DB_PATH", &spec.db_path)
            .env("NEURALBASE_USERS_FILE", &spec.users_path)
            .env("NEURALBASE_METRICS_PORT", spec.metrics_port.to_string())
            .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "300")
            .env("NEURALBASE_AUTH_REQUIRED", "0")
            .env("NEURALBASE_RAFT_TLS", "0")
            .env("RUST_LOG", "warn")
            .env_remove("NEURALBASE_OPERATOR_NODE")
            .env_remove("NEURALBASE_PITR_DIR")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .expect("spawn durable NeuralBase node");
        nodes.push(NodeProcess { spec, child });
    }

    for node in &mut nodes {
        wait_socket(node);
    }

    Cluster { _root: root, nodes }
}

fn sql_ports(cluster: &Cluster) -> Vec<u16> {
    cluster.nodes.iter().map(|node| node.spec.sql_port).collect()
}

fn find_mutation_leader(ports: &[u16], sql: &str) -> u16 {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        for &port in ports {
            let Some(mut client) = connect(port).ok() else {
                continue;
            };
            match client.simple_query(sql) {
                Ok(_) => return port,
                Err(error) if is_retryable_leadership_error(&error) => {}
                Err(error) => panic!("leader probe mutation failed on {port}: {error}; SQL={sql}"),
            }
        }
        assert!(Instant::now() < deadline, "three-voter cluster never elected a writable leader");
        thread::sleep(Duration::from_millis(30));
    }
}

fn execute_mutation(client: &mut Client, sql: &str) {
    client
        .simple_query(sql)
        .unwrap_or_else(|error| panic!("mutation failed: {error}; SQL={sql}"));
}

fn query_row_count(client: &mut Client, sql: &str) -> usize {
    client
        .simple_query(sql)
        .unwrap_or_else(|error| panic!("query failed: {error}; SQL={sql}"))
        .into_iter()
        .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
        .count()
}

fn set_consistency(client: &mut Client, consistency: &str) {
    client
        .simple_query(&format!(
            "SET neuralbase_read_consistency = '{consistency}'"
        ))
        .unwrap_or_else(|error| panic!("SET {consistency} failed: {error}"));
}

fn wait_replicated_row(ports: &[u16], id: usize) {
    let deadline = Instant::now() + REPLICATION_TIMEOUT;
    let sql = format!("SELECT id FROM p11_items WHERE id = {id}");
    for &port in ports {
        loop {
            if let Ok(mut client) = connect(port) {
                set_consistency(&mut client, "local");
                if query_row_count(&mut client, &sql) == 1 {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "row {id} did not become locally visible on voter port {port}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

fn seed_rows(leader_port: u16, start: usize, end: usize, workers: usize) {
    if start >= end {
        return;
    }
    let count = end - start;
    let workers = workers.min(count).max(1);
    let chunk = count.div_ceil(workers);
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let from = start + worker * chunk;
        let to = (from + chunk).min(end);
        if from >= to {
            continue;
        }
        handles.push(thread::spawn(move || {
            let mut client = connect(leader_port).expect("seed client connect");
            for id in from..to {
                execute_mutation(
                    &mut client,
                    &format!("INSERT INTO p11_items VALUES ({id}, {})", id * 3 + 7),
                );
            }
        }));
    }
    for handle in handles {
        handle.join().expect("seed worker");
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    assert!(!sorted.is_empty());
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index.min(sorted.len() - 1)]
}

fn emit_latency_result(name: &str, path: &str, samples: &[u64], extra: &str) {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum: u128 = sorted.iter().map(|value| *value as u128).sum();
    let mean = (sum / sorted.len() as u128) as u64;
    let label = std::env::var("PHASE11_LABEL").unwrap_or_else(|_| "head".to_string());
    let commit = std::env::var("PHASE11_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    println!(
        "PHASE11_RESULT {{\"schema\":1,\"label\":\"{}\",\"commit\":\"{}\",\"name\":\"{}\",\"path\":\"{}\",\"unit\":\"ns\",\"warmup_iterations\":{},\"measured_iterations\":{},\"min\":{},\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{},\"mean\":{},\"extra\":{}}}",
        label,
        commit,
        name,
        path,
        WARMUP,
        sorted.len(),
        sorted[0],
        percentile(&sorted, 50),
        percentile(&sorted, 95),
        percentile(&sorted, 99),
        sorted[sorted.len() - 1],
        mean,
        extra
    );
}

fn measure_query(
    port: u16,
    consistency: &str,
    sql: &str,
    name: &str,
    extra: &str,
) {
    let mut client = connect(port).expect("benchmark client connect");
    set_consistency(&mut client, consistency);
    for _ in 0..WARMUP {
        let _ = query_row_count(&mut client, sql);
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let _ = query_row_count(&mut client, sql);
        samples.push(start.elapsed().as_nanos() as u64);
    }
    emit_latency_result(name, "postgres_wire_three_durable_voters", &samples, extra);
}

fn run_concurrent_reads(
    port: u16,
    clients: usize,
    consistency: &str,
    sql: &str,
    dataset_rows: usize,
) {
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);
    for _ in 0..clients {
        let barrier = Arc::clone(&barrier);
        let consistency = consistency.to_string();
        let sql = sql.to_string();
        handles.push(thread::spawn(move || {
            let mut client = connect(port).expect("concurrent read connect");
            set_consistency(&mut client, &consistency);
            barrier.wait();
            let mut latencies = Vec::with_capacity(READS_PER_CLIENT);
            for _ in 0..READS_PER_CLIENT {
                let start = Instant::now();
                let _ = query_row_count(&mut client, &sql);
                latencies.push(start.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    barrier.wait();
    let wall_start = Instant::now();
    let mut samples = Vec::with_capacity(clients * READS_PER_CLIENT);
    for handle in handles {
        samples.extend(handle.join().expect("concurrent read worker"));
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let total_ops = clients * READS_PER_CLIENT;
    let throughput = total_ops as f64 / (wall_ns as f64 / 1_000_000_000.0);
    emit_latency_result(
        &format!("concurrent_{consistency}_read_c{clients}"),
        "postgres_wire_three_durable_voters_concurrent",
        &samples,
        &format!(
            "{{\"voters\":3,\"clients\":{clients},\"ops_per_client\":{READS_PER_CLIENT},\"total_ops\":{total_ops},\"dataset_rows\":{dataset_rows},\"wall_ns\":{wall_ns},\"throughput_ops_per_sec\":{throughput:.3}}}"
        ),
    );
}

fn run_concurrent_writes(port: u16, clients: usize, id_base: usize) {
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);
    for client_index in 0..clients {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut client = connect(port).expect("concurrent write connect");
            barrier.wait();
            let mut latencies = Vec::with_capacity(WRITES_PER_CLIENT);
            for operation in 0..WRITES_PER_CLIENT {
                let id = id_base + client_index * WRITES_PER_CLIENT + operation;
                let start = Instant::now();
                execute_mutation(
                    &mut client,
                    &format!("INSERT INTO p11_write_load VALUES ({id}, {})", id * 5 + 1),
                );
                latencies.push(start.elapsed().as_nanos() as u64);
            }
            latencies
        }));
    }

    barrier.wait();
    let wall_start = Instant::now();
    let mut samples = Vec::with_capacity(clients * WRITES_PER_CLIENT);
    for handle in handles {
        samples.extend(handle.join().expect("concurrent write worker"));
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let total_ops = clients * WRITES_PER_CLIENT;
    let throughput = total_ops as f64 / (wall_ns as f64 / 1_000_000_000.0);
    emit_latency_result(
        &format!("concurrent_replicated_insert_c{clients}"),
        "postgres_wire_three_durable_voters_concurrent",
        &samples,
        &format!(
            "{{\"voters\":3,\"clients\":{clients},\"ops_per_client\":{WRITES_PER_CLIENT},\"total_ops\":{total_ops},\"wall_ns\":{wall_ns},\"throughput_ops_per_sec\":{throughput:.3}}}"
        ),
    );
}

fn add_noise_tables(port: u16, from: usize, to: usize) {
    let mut client = connect(port).expect("noise DDL connect");
    for table in from..to {
        execute_mutation(
            &mut client,
            &format!("CREATE TABLE p11_noise_{table:02} (id BIGINT)"),
        );
    }

    let table_count = to.saturating_sub(from);
    if table_count == 0 {
        return;
    }
    let workers = 8.min(table_count).max(1);
    let chunk = table_count.div_ceil(workers);
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let table_from = from + worker * chunk;
        let table_to = (table_from + chunk).min(to);
        if table_from >= table_to {
            continue;
        }
        handles.push(thread::spawn(move || {
            let mut client = connect(port).expect("noise seed connect");
            for table in table_from..table_to {
                for row in 0..NOISE_ROWS_PER_TABLE {
                    execute_mutation(
                        &mut client,
                        &format!("INSERT INTO p11_noise_{table:02} VALUES ({row})"),
                    );
                }
            }
        }));
    }
    for handle in handles {
        handle.join().expect("noise seed worker");
    }
}

fn process_memory_bytes(pid: u32, field: &str) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn emit_memory(cluster: &Cluster) {
    let label = std::env::var("PHASE11_LABEL").unwrap_or_else(|_| "head".to_string());
    let commit = std::env::var("PHASE11_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    for node in &cluster.nodes {
        let pid = node.child.id();
        let rss = process_memory_bytes(pid, "VmRSS:").unwrap_or(0);
        let hwm = process_memory_bytes(pid, "VmHWM:").unwrap_or(0);
        println!(
            "PHASE11_MEMORY {{\"schema\":1,\"label\":\"{}\",\"commit\":\"{}\",\"node\":\"{}\",\"pid\":{},\"rss_bytes\":{},\"hwm_bytes\":{}}}",
            label, commit, node.spec.id, pid, rss, hwm
        );
    }
}

#[test]
#[ignore = "Phase 11 real-process performance characterization"]
fn three_durable_voters_dataset_and_concurrency_characterization() {
    let cluster = spawn_cluster();
    let ports = sql_ports(&cluster);

    let leader_port = find_mutation_leader(
        &ports,
        "CREATE TABLE p11_leader_probe (value BIGINT)",
    );
    let mut leader = connect(leader_port).expect("leader setup connect");
    execute_mutation(&mut leader, "CREATE TABLE p11_items (id BIGINT, value BIGINT)");
    execute_mutation(
        &mut leader,
        "CREATE TABLE p11_write_load (id BIGINT, value BIGINT)",
    );
    execute_mutation(&mut leader, "CREATE TABLE p11_target (id BIGINT, value BIGINT)");
    for id in 0..4 {
        execute_mutation(
            &mut leader,
            &format!("INSERT INTO p11_target VALUES ({id}, {})", id + 10),
        );
    }
    drop(leader);

    let mut current_rows = 0;
    for &target_rows in DATASET_SIZES {
        let seed_start = Instant::now();
        seed_rows(leader_port, current_rows, target_rows, 8);
        let seed_ns = seed_start.elapsed().as_nanos() as u64;
        current_rows = target_rows;
        wait_replicated_row(&ports, target_rows - 1);

        println!(
            "PHASE11_SETUP {{\"dataset_rows\":{target_rows},\"incremental_seed_ns\":{seed_ns},\"voters\":3}}"
        );
        let point_sql = format!(
            "SELECT id, value FROM p11_items WHERE id = {}",
            target_rows - 1
        );
        for consistency in ["local", "leader", "linearizable"] {
            measure_query(
                leader_port,
                consistency,
                &point_sql,
                &format!("point_read_{consistency}_{target_rows}_rows"),
                &format!(
                    "{{\"voters\":3,\"dataset_rows\":{target_rows},\"consistency\":\"{consistency}\"}}"
                ),
            );
        }
    }

    let point_sql = format!(
        "SELECT id, value FROM p11_items WHERE id = {}",
        current_rows - 1
    );
    for &clients in CONCURRENCY_LEVELS {
        run_concurrent_reads(
            leader_port,
            clients,
            "local",
            &point_sql,
            current_rows,
        );
        run_concurrent_reads(
            leader_port,
            clients,
            "linearizable",
            &point_sql,
            current_rows,
        );
        run_concurrent_writes(leader_port, clients, 1_000_000 + clients * 10_000);
    }

    let general_query = "SELECT a.id, a.value FROM p11_target a JOIN p11_target b ON a.id = b.id WHERE a.id = 1";
    measure_query(
        leader_port,
        "local",
        general_query,
        "general_query_with_0_unrelated_tables",
        "{\"voters\":3,\"target_rows\":4,\"unrelated_tables\":0,\"rows_per_unrelated_table\":0}",
    );
    add_noise_tables(leader_port, 0, 16);
    measure_query(
        leader_port,
        "local",
        general_query,
        "general_query_with_16_unrelated_tables",
        &format!(
            "{{\"voters\":3,\"target_rows\":4,\"unrelated_tables\":16,\"rows_per_unrelated_table\":{NOISE_ROWS_PER_TABLE}}}"
        ),
    );
    add_noise_tables(leader_port, 16, 64);
    measure_query(
        leader_port,
        "local",
        general_query,
        "general_query_with_64_unrelated_tables",
        &format!(
            "{{\"voters\":3,\"target_rows\":4,\"unrelated_tables\":64,\"rows_per_unrelated_table\":{NOISE_ROWS_PER_TABLE}}}"
        ),
    );

    // Confirm the largest dataset row remains visible from every durable voter.
    wait_replicated_row(&ports, current_rows - 1);
    emit_memory(&cluster);
}
