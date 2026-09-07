// SPDX-License-Identifier: Apache-2.0
//! Independent validation that UPDATE/DELETE predicates are resolved on the
//! leader before the replicated command enters Raft.

use std::sync::Arc;

use neuralbase::binder::{DeletePlan, DmlCmpOp, DmlPredicate, SqlValue, UpdatePlan};
use neuralbase::catalog::{ColumnDef, TableSchema};
use neuralbase::consensus::{ClientCommand, RaftRole, RaftShared};
use neuralbase::hlc::{HlcClock, HlcTimestamp};
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_sql::ReplicatedMutation;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::{decode_row, encode_row, table_id_for};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

fn schema() -> TableSchema {
    TableSchema {
        name: "items".to_string(),
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

fn seed_rows(engine: &StorageEngine) {
    let table_id = table_id_for("items");
    let ts = HlcTimestamp {
        wall_ms: 1_000,
        logical: 1,
    };
    engine
        .write_version(
            table_id,
            b"pk-a",
            ts,
            &encode_row(&[("id", "1"), ("name", "alpha")]),
        )
        .unwrap();
    engine
        .write_version(
            table_id,
            b"pk-b",
            ts,
            &encode_row(&[("id", "2"), ("name", "beta")]),
        )
        .unwrap();
}

fn leader_shared() -> Arc<Mutex<RaftShared>> {
    Arc::new(Mutex::new(RaftShared {
        role: RaftRole::Leader,
        leader_id: Some("leader-a".to_string()),
        commit_index: 0,
        last_applied: 0,
    }))
}

/// Capture the readiness barrier followed by exactly one replicated SQL
/// mutation. The fake Raft endpoint acknowledges both so the gateway can
/// complete, but it never evaluates SQL or touches storage.
fn spawn_capture(
    mut rx: mpsc::Receiver<ClientCommand>,
) -> oneshot::Receiver<Vec<u8>> {
    let (captured_tx, captured_rx) = oneshot::channel();
    tokio::spawn(async move {
        let barrier = rx.recv().await.expect("readiness barrier command");
        assert_eq!(barrier.payload, b"NBRB\x01");
        barrier.reply.send(Ok(1)).expect("ack readiness barrier");

        let mutation = rx.recv().await.expect("replicated SQL mutation");
        captured_tx
            .send(mutation.payload.clone())
            .expect("capture mutation payload");
        mutation.reply.send(Ok(2)).expect("ack mutation");
    });
    captured_rx
}

#[tokio::test]
async fn update_replication_contains_only_leader_materialized_matching_row() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    seed_rows(&engine);

    let (client_tx, command_rx) = mpsc::channel(8);
    let captured = spawn_capture(command_rx);
    let gateway = ReplicatedSqlGateway::new(
        client_tx,
        leader_shared(),
        Arc::clone(&engine),
        Arc::new(HlcClock::new(500)),
    );

    let plan = UpdatePlan {
        table: schema(),
        assignments: vec![("name".to_string(), SqlValue::Text("changed".to_string()))],
        predicate: Some(DmlPredicate {
            column: "id".to_string(),
            op: DmlCmpOp::Eq,
            value: SqlValue::Int(1),
        }),
    };

    let ack = gateway.update(&plan).await.unwrap();
    assert_eq!(ack.affected_rows, Some(1));

    let payload = captured.await.expect("captured UPDATE payload");
    match ReplicatedMutation::decode(&payload).unwrap() {
        ReplicatedMutation::UpdateRows {
            table,
            table_id,
            commit_ts,
            rows,
        } => {
            assert_eq!(table, "items");
            assert_eq!(table_id, table_id_for("items"));
            assert_ne!(commit_ts, 0);
            assert_eq!(rows.len(), 1, "only id=1 may be replicated");
            assert_eq!(rows[0].primary_key, b"pk-a");
            let row = decode_row(&rows[0].value).expect("updated row must decode");
            assert_eq!(row.get("id").map(String::as_str), Some("1"));
            assert_eq!(row.get("name").map(String::as_str), Some("changed"));
        }
        other => panic!("expected materialized UpdateRows command, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_replication_contains_only_leader_materialized_matching_key() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    seed_rows(&engine);

    let (client_tx, command_rx) = mpsc::channel(8);
    let captured = spawn_capture(command_rx);
    let gateway = ReplicatedSqlGateway::new(
        client_tx,
        leader_shared(),
        Arc::clone(&engine),
        Arc::new(HlcClock::new(500)),
    );

    let plan = DeletePlan {
        table: schema(),
        predicate: Some(DmlPredicate {
            column: "id".to_string(),
            op: DmlCmpOp::Eq,
            value: SqlValue::Int(2),
        }),
    };

    let ack = gateway.delete(&plan).await.unwrap();
    assert_eq!(ack.affected_rows, Some(1));

    let payload = captured.await.expect("captured DELETE payload");
    match ReplicatedMutation::decode(&payload).unwrap() {
        ReplicatedMutation::DeleteRows {
            table,
            table_id,
            commit_ts,
            primary_keys,
        } => {
            assert_eq!(table, "items");
            assert_eq!(table_id, table_id_for("items"));
            assert_ne!(commit_ts, 0);
            assert_eq!(primary_keys, vec![b"pk-b".to_vec()]);
        }
        other => panic!("expected materialized DeleteRows command, got {other:?}"),
    }
}
