// SPDX-License-Identifier: Apache-2.0
//! Phase-10 in-process consensus performance characterization.
//!
//! This harness deliberately reuses the Phase-6 safety shape: three voters,
//! confirmed apply, and explicit partitions. It measures the current barrier and
//! replication mechanisms without changing their semantics. No timing threshold
//! is asserted.

use std::collections::{HashMap, HashSet};
use std::hint::black_box;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use neuralbase::consensus::{
    ClientCommand, CommittedEntry, NodeId, RaftMessage, RaftNode, RaftRole, RaftShared,
    RaftTaskHandle, Transport, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::read_barrier::prepare_read_with_timeout;
use neuralbase::read_consistency::ReadConsistency;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::storage::StorageEngine;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const IDS: [&str; 3] = ["p10-a", "p10-b", "p10-c"];
const WAIT: Duration = Duration::from_secs(5);
const WARMUP: usize = 2;
const REPS: usize = 9;

type Bus = Arc<Mutex<HashMap<NodeId, mpsc::Sender<(NodeId, RaftMessage)>>>>;
type Blocks = Arc<Mutex<HashSet<(NodeId, NodeId)>>>;

struct PartitionTransport {
    id: NodeId,
    bus: Bus,
    blocked: Blocks,
    rx: Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>,
}

impl PartitionTransport {
    async fn register(id: NodeId, bus: Bus, blocked: Blocks) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        bus.lock().await.insert(id.clone(), tx);
        Self {
            id,
            bus,
            blocked,
            rx: Mutex::new(rx),
        }
    }
}

#[async_trait]
impl Transport for PartitionTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) {
        if self
            .blocked
            .lock()
            .await
            .contains(&(self.id.clone(), to.clone()))
        {
            return;
        }
        if let Some(tx) = self.bus.lock().await.get(to).cloned() {
            let _ = tx.send((self.id.clone(), msg)).await;
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

struct TestNode {
    _db: TempDir,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl TestNode {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new_with_readiness(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
            Arc::new(AtomicBool::new(true)),
        )
    }

    async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown().await;
        }
        self.apply_task.abort();
        let _ = (&mut self.apply_task).await;
    }
}

async fn spawn_node(id: &str, bus: Bus, blocked: Blocks, election_ms: u64) -> TestNode {
    let db = TempDir::new().expect("temp db");
    let engine = Arc::new(StorageEngine::open(db.path()).expect("open storage"));
    let clock = Arc::new(HlcClock::new(500));
    let transport = Arc::new(PartitionTransport::register(id.to_string(), bus, blocked).await);
    let peers = IDS
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(str::to_string)
        .collect();

    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            let _ = committed.completion.send(Ok(()));
        }
    });

    let mut raft =
        RaftNode::new(id.to_string(), peers, transport).with_confirmed_apply_tx(apply_tx);
    raft.set_election_timeout_ms(election_ms);
    let (client_tx, shared, handle) = raft.spawn();
    TestNode {
        _db: db,
        engine,
        clock,
        client_tx,
        shared,
        handle: Some(handle),
        apply_task,
    }
}

async fn cluster() -> (Vec<TestNode>, Blocks) {
    let bus = Arc::new(Mutex::new(HashMap::new()));
    let blocked = Arc::new(Mutex::new(HashSet::new()));
    let mut nodes = Vec::new();
    for (idx, id) in IDS.iter().enumerate() {
        nodes.push(
            spawn_node(
                id,
                Arc::clone(&bus),
                Arc::clone(&blocked),
                70 + idx as u64 * 140,
            )
            .await,
        );
    }
    (nodes, blocked)
}

async fn leader(nodes: &[TestNode], excluded: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        for (idx, node) in nodes.iter().enumerate() {
            if excluded == Some(idx) || node.handle.is_none() {
                continue;
            }
            if node.shared.lock().await.role == RaftRole::Leader {
                return idx;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Phase-10 leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn submit(tx: &mpsc::Sender<ClientCommand>, payload: &[u8]) -> u64 {
    let (reply, response) = oneshot::channel();
    tx.send(ClientCommand {
        payload: payload.to_vec(),
        reply,
    })
    .await
    .expect("submit command");
    tokio::time::timeout(WAIT, response)
        .await
        .expect("command response timeout")
        .expect("command response channel")
        .expect("command must commit/apply")
}

async fn isolate(index: usize, blocked: &Blocks) {
    let isolated = IDS[index].to_string();
    let mut guard = blocked.lock().await;
    for other in IDS.iter().copied().filter(|id| *id != IDS[index]) {
        guard.insert((isolated.clone(), other.to_string()));
        guard.insert((other.to_string(), isolated.clone()));
    }
}

async fn stop_all(nodes: &mut [TestNode]) {
    for node in nodes {
        node.stop().await;
    }
}

fn label() -> String {
    std::env::var("PHASE10_LABEL").unwrap_or_else(|_| "unlabeled".into())
}

fn measured_commit() -> String {
    std::env::var("PHASE10_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".into())
}

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

fn report(name: &str, samples: &[u128], extra: serde_json::Value) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().copied().sum::<u128>() as f64 / sorted.len() as f64;
    let payload = json!({
        "schema": 1,
        "label": label(),
        "commit": measured_commit(),
        "name": name,
        "path": "in_process_3_voter_raft_confirmed_apply",
        "unit": "ns",
        "warmup_iterations": WARMUP,
        "measured_iterations": samples.len(),
        "min": sorted[0],
        "p50": percentile(&sorted, 0.50),
        "p95": percentile(&sorted, 0.95),
        "max": sorted[sorted.len() - 1],
        "mean": mean,
        "extra": extra,
    });
    println!("PHASE10_RESULT {payload}");
}

async fn measure_strong_reads(node: &TestNode, mode: ReadConsistency, name: &str) -> Vec<u128> {
    let gateway = node.gateway();
    for _ in 0..WARMUP {
        prepare_read_with_timeout(Some(&gateway), mode, WAIT)
            .await
            .expect("strong read warmup");
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        prepare_read_with_timeout(Some(&gateway), mode, WAIT)
            .await
            .expect("strong read measurement");
        samples.push(start.elapsed().as_nanos());
    }
    report(
        name,
        &samples,
        json!({"consistency": mode.to_string(), "current_mechanism": "replicated_control_entry"}),
    );
    samples
}

#[tokio::test]
#[ignore = "manual Phase-10 performance characterization"]
async fn phase10_replication_and_strong_read_characterization() {
    let (mut nodes, _) = cluster().await;
    let leader_idx = leader(&nodes, None).await;

    for i in 0..WARMUP {
        let payload = format!("phase10-write-warmup-{i}");
        black_box(submit(&nodes[leader_idx].client_tx, payload.as_bytes()).await);
    }

    let mut write_samples = Vec::with_capacity(REPS);
    for i in 0..REPS {
        let payload = format!("phase10-write-{i}");
        let start = Instant::now();
        let index = submit(&nodes[leader_idx].client_tx, payload.as_bytes()).await;
        write_samples.push(start.elapsed().as_nanos());
        black_box(index);
    }
    report(
        "replicated_write_quorum_confirmed_apply",
        &write_samples,
        json!({"voters": 3, "payload_kind": "opaque_client_command", "durability_contract": "quorum_commit_plus_confirmed_local_apply"}),
    );

    let leader_samples = measure_strong_reads(
        &nodes[leader_idx],
        ReadConsistency::Leader,
        "leader_read_barrier",
    )
    .await;
    let linearizable_samples = measure_strong_reads(
        &nodes[leader_idx],
        ReadConsistency::Linearizable,
        "linearizable_read_barrier",
    )
    .await;
    assert_eq!(leader_samples.len(), REPS);
    assert_eq!(linearizable_samples.len(), REPS);

    let state = nodes[leader_idx].shared.lock().await.clone();
    assert_eq!(state.last_applied, state.commit_index);
    stop_all(&mut nodes).await;
}

#[tokio::test]
#[ignore = "manual Phase-10 performance characterization"]
async fn phase10_failover_and_stale_leader_failure_characterization() {
    let (mut nodes, blocked) = cluster().await;
    let old_leader = leader(&nodes, None).await;
    prepare_read_with_timeout(
        Some(&nodes[old_leader].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .expect("initial strong read");

    let failover_start = Instant::now();
    isolate(old_leader, &blocked).await;
    let new_leader = leader(&nodes, Some(old_leader)).await;
    let failover_ns = failover_start.elapsed().as_nanos();
    assert_ne!(old_leader, new_leader);
    report(
        "leader_failover_after_partition",
        &[failover_ns],
        json!({"voters": 3, "old_leader": IDS[old_leader], "new_leader": IDS[new_leader]}),
    );

    let stale_gateway = nodes[old_leader].gateway();
    let stale_start = Instant::now();
    let stale_result = prepare_read_with_timeout(
        Some(&stale_gateway),
        ReadConsistency::Linearizable,
        Duration::from_millis(300),
    )
    .await;
    let stale_failure_ns = stale_start.elapsed().as_nanos();
    assert!(
        stale_result.is_err(),
        "isolated former leader served a strong read"
    );
    report(
        "stale_leader_strong_read_fail_closed",
        &[stale_failure_ns],
        json!({"timeout_ms": 300, "result": "error"}),
    );

    prepare_read_with_timeout(
        Some(&nodes[new_leader].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .expect("new leader must serve strong read");
    stop_all(&mut nodes).await;
}
