// SPDX-License-Identifier: Apache-2.0
//! Independent replicated-SQL guarantees over the real Raft state machine.
//!
//! These tests deliberately give every Raft member a separate RocksDB directory,
//! catalog, HLC, persistence store, and deterministic SQL state machine. A pass
//! therefore proves convergence through committed log application rather than
//! accidental shared-memory state.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::binder::{DmlCmpOp, DmlPredicate, InsertPlan, SqlValue, UpdatePlan};
use neuralbase::catalog::{Catalog, ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    ChannelTransport, ClientCommand, CommittedEntry, FailClosedPersistenceStore, RaftNode,
    RaftPersistenceStore, RaftRole, RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::{HlcClock, HlcTimestamp};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::{decode_row, table_id_for};
use tempfile::TempDir;
use tokio::sync::{mpsc, Mutex};

struct NodeHarness {
    id: String,
    _dir: TempDir,
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl NodeHarness {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
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

fn schema() -> TableSchema {
    TableSchema {
        name: "replicated_items".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
    }
}

async fn spawn_three_node_cluster() -> Vec<NodeHarness> {
    let bus = ChannelTransport::new_bus();
    let ids = ["sql-a", "sql-b", "sql-c"];
    let mut nodes = Vec::new();

    for (ordinal, id) in ids.iter().enumerate() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let state_machine = Arc::new(
            ReplicatedSqlStateMachine::new(
                Arc::clone(&engine),
                Arc::clone(&catalog),
                Arc::clone(&clock),
            )
            .unwrap(),
        );

        let transport = Arc::new(
            ChannelTransport::register((*id).to_string(), Arc::clone(&bus)).await,
        );
        let peers = ids
            .iter()
            .filter(|peer| *peer != id)
            .map(|peer| (*peer).to_string())
            .collect();
        let raw_store: Arc<dyn RaftPersistenceStore> = Arc::new(
            RocksDbRaftPersistenceStore::new(Arc::clone(&engine)),
        );
        let strict_store: Arc<dyn RaftPersistenceStore> =
            Arc::new(FailClosedPersistenceStore::new(raw_store));
        let (apply_tx, mut apply_rx) =
            mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
        let sm = Arc::clone(&state_machine);
        let apply_task = tokio::spawn(async move {
            while let Some(committed) = apply_rx.recv().await {
                let result = sm
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

        let mut node = RaftNode::new((*id).to_string(), peers, transport)
            .with_persistence(strict_store)
            .with_confirmed_apply_tx(apply_tx);
        // Small deterministic offset reduces needless simultaneous elections
        // while retaining Raft's own random jitter.
        node.set_election_timeout_ms(50 + ordinal as u64 * 15);
        let (client_tx, shared, handle) = node.spawn();

        nodes.push(NodeHarness {
            id: (*id).to_string(),
            _dir: dir,
            engine,
            catalog,
            clock,
            client_tx,
            shared,
            handle: Some(handle),
            apply_task,
        });
    }

    nodes
}

async fn wait_for_leader(nodes: &[NodeHarness], excluded: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) || node.handle.is_none() {
                continue;
            }
            if node.shared.lock().await.role == RaftRole::Leader {
                return index;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_row_count(nodes: &[NodeHarness], count: usize, excluded: Option<usize>) {
    let tid = table_id_for("replicated_items");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let mut converged = true;
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) || node.handle.is_none() {
                continue;
            }
            let rows = node.engine.scan_table(tid, HlcTimestamp::MAX).unwrap();
            converged &= rows.len() == count;
        }
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replicated SQL state did not converge"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn acknowledged_write_survives_leader_loss_and_new_leader_can_mutate() {
    let mut nodes = spawn_three_node_cluster().await;
    let leader = wait_for_leader(&nodes, None).await;
    let gateway = nodes[leader].gateway();

    let table = schema();
    let create = gateway.create_table(table.clone()).await.unwrap();
    assert!(create.raft_index > 0);

    let insert = InsertPlan {
        table: table.clone(),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![
            SqlValue::Int(1),
            SqlValue::Text("before-failover".to_string()),
        ]],
    };
    let insert_ack = gateway.insert(&insert).await.unwrap();
    assert_eq!(insert_ack.affected_rows, Some(1));

    wait_for_row_count(&nodes, 1, None).await;
    for node in &nodes {
        assert!(node.catalog.get_table("replicated_items").is_some());
    }

    // SQL success was already returned. Killing that leader must not make the
    // acknowledged row disappear from the surviving quorum.
    let old_leader_id = nodes[leader].id.clone();
    nodes[leader].stop().await;
    let new_leader = wait_for_leader(&nodes, Some(leader)).await;
    assert_ne!(nodes[new_leader].id, old_leader_id);
    wait_for_row_count(&nodes, 1, Some(leader)).await;

    // Continue writing through the newly elected leader. UPDATE materialization
    // must see the previously acknowledged replicated state.
    let update = UpdatePlan {
        table,
        assignments: vec![(
            "name".to_string(),
            SqlValue::Text("after-failover".to_string()),
        )],
        predicate: Some(DmlPredicate {
            column: "id".to_string(),
            op: DmlCmpOp::Eq,
            value: SqlValue::Int(1),
        }),
    };
    let update_ack = nodes[new_leader].gateway().update(&update).await.unwrap();
    assert_eq!(update_ack.affected_rows, Some(1));

    let tid = table_id_for("replicated_items");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let mut converged = true;
        for (index, node) in nodes.iter().enumerate() {
            if index == leader || node.handle.is_none() {
                continue;
            }
            let rows = node.engine.scan_table(tid, HlcTimestamp::MAX).unwrap();
            let row = rows
                .first()
                .and_then(|(_, bytes)| decode_row(bytes))
                .expect("replicated row must decode");
            converged &= row.get("name").map(String::as_str) == Some("after-failover");
        }
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "post-failover update did not converge"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for (index, node) in nodes.iter_mut().enumerate() {
        if index != leader {
            node.stop().await;
        }
    }
}

#[tokio::test]
async fn restart_recovers_acknowledged_sql_and_replays_without_duplicate_effects() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let bus = ChannelTransport::new_bus();

    let transport = Arc::new(
        ChannelTransport::register("restart-solo".to_string(), Arc::clone(&bus)).await,
    );
    let store: Arc<dyn RaftPersistenceStore> = Arc::new(FailClosedPersistenceStore::new(
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine))),
    ));
    let sm = Arc::new(
        ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap(),
    );
    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(8);
    let sm_task = Arc::clone(&sm);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            let result = sm_task
                .apply_log_entry(&committed.entry)
                .map(|_| ())
                .map_err(|error| error.to_string());
            let _ = committed.completion.send(result);
        }
    });
    let mut raft = RaftNode::new("restart-solo".to_string(), vec![], transport)
        .with_persistence(store)
        .with_confirmed_apply_tx(apply_tx);
    raft.set_election_timeout_ms(20);
    let (client_tx, shared, handle) = raft.spawn();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while shared.lock().await.role != RaftRole::Leader {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let gateway = ReplicatedSqlGateway::new(
        client_tx,
        shared,
        Arc::clone(&engine),
        Arc::clone(&clock),
    );
    let table = schema();
    gateway.create_table(table.clone()).await.unwrap();
    gateway
        .insert(&InsertPlan {
            table: table.clone(),
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![SqlValue::Int(7), SqlValue::Text("durable".to_string())]],
        })
        .await
        .unwrap();
    let tid = table_id_for("replicated_items");
    assert_eq!(engine.raw_scan_table_versions(tid).unwrap().len(), 1);
    let durable_index = sm.durable_state().unwrap().last_applied_index;
    assert!(durable_index >= 2);

    handle.shutdown().await;
    apply_task.abort();
    let _ = apply_task.await;
    drop(sm);
    drop(clock);

    // Reconstruct both consensus and SQL state from the same RocksDB directory.
    let recovered_clock = Arc::new(HlcClock::new(500));
    let recovered_sm = Arc::new(
        ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&recovered_clock),
        )
        .unwrap(),
    );
    assert_eq!(
        recovered_sm.durable_state().unwrap().last_applied_index,
        durable_index
    );
    assert_eq!(engine.raw_scan_table_versions(tid).unwrap().len(), 1);

    let transport2 = Arc::new(
        ChannelTransport::register("restart-solo".to_string(), Arc::clone(&bus)).await,
    );
    let store2: Arc<dyn RaftPersistenceStore> = Arc::new(FailClosedPersistenceStore::new(
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine))),
    ));
    let (apply_tx2, mut apply_rx2) = mpsc::channel::<CommittedEntry>(8);
    let sm2 = Arc::clone(&recovered_sm);
    let apply_task2 = tokio::spawn(async move {
        while let Some(committed) = apply_rx2.recv().await {
            let result = sm2
                .apply_log_entry(&committed.entry)
                .map(|_| ())
                .map_err(|error| error.to_string());
            let _ = committed.completion.send(result);
        }
    });
    let mut raft2 = RaftNode::new("restart-solo".to_string(), vec![], transport2)
        .with_persistence(store2)
        .with_confirmed_apply_tx(apply_tx2);
    raft2.set_election_timeout_ms(20);
    let (client_tx2, shared2, handle2) = raft2.spawn();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(1);
    while shared2.lock().await.role != RaftRole::Leader {
        assert!(tokio::time::Instant::now() < deadline2);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A new current-term command causes the recovered log prefix to be walked.
    // The durable apply index makes the old CREATE/INSERT idempotent: no second
    // MVCC version is produced for the acknowledged row.
    let gateway2 = ReplicatedSqlGateway::new(
        client_tx2,
        shared2,
        Arc::clone(&engine),
        Arc::clone(&recovered_clock),
    );
    let ack = gateway2
        .update(&UpdatePlan {
            table,
            assignments: vec![("name".to_string(), SqlValue::Text("after-restart".to_string()))],
            predicate: Some(DmlPredicate {
                column: "id".to_string(),
                op: DmlCmpOp::Eq,
                value: SqlValue::Int(7),
            }),
        })
        .await
        .unwrap();
    assert_eq!(ack.affected_rows, Some(1));
    assert_eq!(
        engine.raw_scan_table_versions(tid).unwrap().len(),
        2,
        "one original version plus one post-restart update; replay must add none"
    );

    handle2.shutdown().await;
    apply_task2.abort();
    let _ = apply_task2.await;
}
