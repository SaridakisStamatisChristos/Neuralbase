// SPDX-License-Identifier: Apache-2.0
//! Leader-side materialization and Raft submission for persistent SQL mutations.
//!
//! The gateway serializes mutating SQL statements on the leader. Before a
//! mutation is bound/materialized, a current-term non-SQL barrier is committed
//! and confirmed applied. Because Raft applies log entries in order, successful
//! barrier acknowledgement proves this leader has applied every preceding
//! committed entry in its local SQL state machine. This prevents stale catalog
//! or row materialization immediately after failover.

use std::collections::BTreeMap;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::binder::{DeletePlan, InsertPlan, UpdatePlan};
use crate::catalog::TableSchema;
use crate::codec;
use crate::consensus::{ClientCommand, RaftRole, RaftShared};
use crate::hlc::{HlcClock, HlcTimestamp};
use crate::replicated_sql::{ReplicatedMutation, ReplicatedRowWrite};
use crate::storage::StorageEngine;
use crate::storage_executor::{decode_row, encode_row, table_id_for};
use crate::vectorized::{ColumnVector, RecordBatch};

const SQL_READINESS_BARRIER: &[u8] = b"NBRB\x01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedMutationAck {
    pub raft_index: u64,
    pub command_tag: &'static str,
    pub affected_rows: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ReplicatedGatewayError {
    #[error("not Raft leader; current leader is {leader:?}")]
    NotLeader { leader: Option<String> },
    #[error("Raft command channel closed")]
    CommandChannelClosed,
    #[error("Raft command acknowledgement channel closed")]
    ReplyChannelClosed,
    #[error("Raft rejected replicated SQL mutation: {0}")]
    Raft(String),
    #[error("storage failure while materializing replicated SQL: {0}")]
    Storage(String),
    #[error("cannot decode stored row while materializing replicated SQL")]
    CorruptStoredRow,
    #[error("replicated mutation encoding failed: {0}")]
    Encoding(String),
}

#[derive(Clone)]
pub struct ReplicatedSqlGateway {
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    mutation_serial: Arc<Mutex<()>>,
}

impl ReplicatedSqlGateway {
    pub fn new(
        client_tx: mpsc::Sender<ClientCommand>,
        shared: Arc<Mutex<RaftShared>>,
        engine: Arc<StorageEngine>,
        clock: Arc<HlcClock>,
    ) -> Self {
        Self {
            client_tx,
            shared,
            engine,
            clock,
            mutation_serial: Arc::new(Mutex::new(())),
        }
    }

    pub async fn current_leader(&self) -> Option<String> {
        self.shared.lock().await.leader_id.clone()
    }

    /// Establish a leader/apply barrier before the SQL binder reads catalog
    /// state for a persistent table mutation. This method intentionally does
    /// not take `mutation_serial`; the concrete mutation method takes that lock
    /// and commits another barrier before materialization, closing the race
    /// between pre-bind readiness and actual proposal.
    pub async fn prepare_mutation(&self) -> Result<(), ReplicatedGatewayError> {
        self.commit_readiness_barrier().await.map(|_| ())
    }

    pub async fn create_table(
        &self,
        schema: TableSchema,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        self.submit(ReplicatedMutation::CreateTable { schema })
            .await
    }

    pub async fn drop_table(
        &self,
        table: &str,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        self.submit(ReplicatedMutation::DropTable {
            table: table.to_string(),
            table_id: table_id_for(table),
        })
        .await
    }

    pub async fn insert(
        &self,
        plan: &InsertPlan,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;

        let commit_ts = self.clock.tick().to_u64();
        let rows = plan
            .rows
            .iter()
            .enumerate()
            .map(|(ordinal, values)| {
                let mut row = BTreeMap::new();
                for (column, value) in plan.columns.iter().zip(values) {
                    row.insert(
                        column.clone(),
                        value.to_storage_string().unwrap_or_default(),
                    );
                }
                let encoded = encode_row_for_schema(&plan.table, &row);
                let mut primary_key = Vec::with_capacity(12);
                primary_key.extend_from_slice(&commit_ts.to_be_bytes());
                primary_key.extend_from_slice(&(ordinal as u32).to_be_bytes());
                ReplicatedRowWrite {
                    primary_key,
                    value: encoded,
                }
            })
            .collect();

        self.submit(ReplicatedMutation::InsertRows {
            table: plan.table.name.clone(),
            table_id: table_id_for(&plan.table.name),
            commit_ts,
            rows,
        })
        .await
    }

    pub async fn update(
        &self,
        plan: &UpdatePlan,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;

        let table_id = table_id_for(&plan.table.name);
        let visible = self
            .engine
            .scan_table(table_id, HlcTimestamp::MAX)
            .map_err(|error| ReplicatedGatewayError::Storage(error.to_string()))?;
        let mut rows = Vec::new();
        for (primary_key, value) in visible {
            let Some(mut row) = decode_any_row(&value) else {
                if value.is_empty() {
                    continue;
                }
                return Err(ReplicatedGatewayError::CorruptStoredRow);
            };
            if plan
                .predicate
                .as_ref()
                .is_some_and(|predicate| !predicate.matches(&row))
            {
                continue;
            }
            for (column, value) in &plan.assignments {
                row.insert(
                    column.clone(),
                    value.to_storage_string().unwrap_or_default(),
                );
            }
            rows.push(ReplicatedRowWrite {
                primary_key,
                value: encode_row_for_schema(&plan.table, &row),
            });
        }

        let commit_ts = self.clock.tick().to_u64();
        self.submit(ReplicatedMutation::UpdateRows {
            table: plan.table.name.clone(),
            table_id,
            commit_ts,
            rows,
        })
        .await
    }

    pub async fn delete(
        &self,
        plan: &DeletePlan,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;

        let table_id = table_id_for(&plan.table.name);
        let visible = self
            .engine
            .scan_table(table_id, HlcTimestamp::MAX)
            .map_err(|error| ReplicatedGatewayError::Storage(error.to_string()))?;
        let mut primary_keys = Vec::new();
        for (primary_key, value) in visible {
            let Some(row) = decode_any_row(&value) else {
                if value.is_empty() {
                    continue;
                }
                return Err(ReplicatedGatewayError::CorruptStoredRow);
            };
            if plan
                .predicate
                .as_ref()
                .is_some_and(|predicate| !predicate.matches(&row))
            {
                continue;
            }
            primary_keys.push(primary_key);
        }

        let commit_ts = self.clock.tick().to_u64();
        self.submit(ReplicatedMutation::DeleteRows {
            table: plan.table.name.clone(),
            table_id,
            commit_ts,
            primary_keys,
        })
        .await
    }

    async fn ensure_leader(&self) -> Result<(), ReplicatedGatewayError> {
        let shared = self.shared.lock().await;
        if shared.role != RaftRole::Leader {
            return Err(ReplicatedGatewayError::NotLeader {
                leader: shared.leader_id.clone(),
            });
        }
        Ok(())
    }

    async fn commit_readiness_barrier(&self) -> Result<u64, ReplicatedGatewayError> {
        self.ensure_leader().await?;
        self.submit_payload(SQL_READINESS_BARRIER.to_vec()).await
    }

    async fn submit_payload(&self, payload: Vec<u8>) -> Result<u64, ReplicatedGatewayError> {
        let (reply, response) = oneshot::channel();
        self.client_tx
            .send(ClientCommand { payload, reply })
            .await
            .map_err(|_| ReplicatedGatewayError::CommandChannelClosed)?;
        response
            .await
            .map_err(|_| ReplicatedGatewayError::ReplyChannelClosed)?
            .map_err(ReplicatedGatewayError::Raft)
    }

    async fn submit(
        &self,
        mutation: ReplicatedMutation,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let command_tag = mutation.command_tag();
        let affected_rows = mutation.affected_rows();
        let payload = mutation
            .encode()
            .map_err(|error| ReplicatedGatewayError::Encoding(error.to_string()))?;
        let raft_index = self.submit_payload(payload).await?;
        Ok(ReplicatedMutationAck {
            raft_index,
            command_tag,
            affected_rows,
        })
    }
}

fn encode_row_for_schema(schema: &TableSchema, row: &BTreeMap<String, String>) -> Vec<u8> {
    let owned: Vec<(String, String)> = schema
        .columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                row.get(&column.name).cloned().unwrap_or_default(),
            )
        })
        .collect();
    let borrowed: Vec<(&str, &str)> = owned
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    encode_row(&borrowed)
}

fn decode_any_row(bytes: &[u8]) -> Option<BTreeMap<String, String>> {
    if bytes.is_empty() {
        return None;
    }
    if let Some(batch) = codec::decode_batch(bytes) {
        return Some(batch_row_to_map(&batch));
    }
    decode_row(bytes)
}

fn batch_row_to_map(batch: &RecordBatch) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if batch.row_count == 0 {
        return map;
    }
    for (name, column) in &batch.columns {
        let value = match column {
            ColumnVector::Int32(values) => values[0].map(|v| v.to_string()).unwrap_or_default(),
            ColumnVector::Int64(values) => values[0].map(|v| v.to_string()).unwrap_or_default(),
            ColumnVector::Float64(values) => values[0].map(|v| v.to_string()).unwrap_or_default(),
            ColumnVector::Date32(values) => values[0].map(|v| v.to_string()).unwrap_or_default(),
            ColumnVector::Utf8(values) => values.get(0).unwrap_or_default(),
        };
        map.insert(name.clone(), value);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, InMemoryCatalog, MutableCatalog};
    use crate::consensus::{ChannelTransport, CommittedEntry, RaftNode};
    use crate::replicated_state_machine::ReplicatedSqlStateMachine;
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn follower_rejects_without_local_mutation() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let clock = Arc::new(HlcClock::new(500));
        let bus = ChannelTransport::new_bus();
        let transport =
            Arc::new(ChannelTransport::register("follower".to_string(), Arc::clone(&bus)).await);
        let node = RaftNode::new(
            "follower".to_string(),
            vec!["missing".to_string()],
            transport,
        );
        let (client_tx, shared, _handle) = node.spawn();
        let gateway = ReplicatedSqlGateway::new(client_tx, shared, Arc::clone(&engine), clock);

        let error = gateway.create_table(schema()).await.unwrap_err();
        assert!(matches!(error, ReplicatedGatewayError::NotLeader { .. }));
        assert!(engine.read_catalog_entry("items").unwrap().is_none());
    }

    #[tokio::test]
    async fn single_node_insert_waits_for_barrier_and_state_machine_apply() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        catalog.create_table(schema());
        let clock = Arc::new(HlcClock::new(500));
        let state_machine = Arc::new(
            ReplicatedSqlStateMachine::new(
                Arc::clone(&engine),
                Arc::clone(&catalog),
                Arc::clone(&clock),
            )
            .unwrap(),
        );

        let bus = ChannelTransport::new_bus();
        let transport =
            Arc::new(ChannelTransport::register("solo".to_string(), Arc::clone(&bus)).await);
        let (apply_tx, mut apply_rx) = tokio::sync::mpsc::channel::<CommittedEntry>(8);
        let mut node =
            RaftNode::new("solo".to_string(), vec![], transport).with_confirmed_apply_tx(apply_tx);
        node.set_election_timeout_ms(20);
        let (client_tx, shared, _handle) = node.spawn();
        let sm = Arc::clone(&state_machine);
        tokio::spawn(async move {
            while let Some(committed) = apply_rx.recv().await {
                let result = sm
                    .apply_log_entry(&committed.entry)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let _ = committed.completion.send(result);
            }
        });

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            if shared.lock().await.role == RaftRole::Leader {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader election timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let gateway =
            ReplicatedSqlGateway::new(client_tx, shared, Arc::clone(&engine), Arc::clone(&clock));
        let plan = InsertPlan {
            table: schema(),
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![
                crate::binder::SqlValue::Int(1),
                crate::binder::SqlValue::Text("alpha".to_string()),
            ]],
        };
        let ack = gateway.insert(&plan).await.unwrap();
        assert_eq!(ack.affected_rows, Some(1));
        assert!(ack.raft_index >= 2, "barrier must precede SQL mutation");
        assert_eq!(
            engine
                .scan_table(table_id_for("items"), HlcTimestamp::MAX)
                .unwrap()
                .len(),
            1
        );
    }
}
