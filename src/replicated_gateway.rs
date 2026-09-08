// SPDX-License-Identifier: Apache-2.0
//! Leader-side materialization and Raft submission for persistent SQL and identity mutations.
//!
//! The gateway serializes mutations on the leader. Before materialization, a
//! current-term barrier is committed and confirmed applied. Identity passwords
//! are converted to SCRAM verifier material on the leader before proposal; the
//! plaintext password is never part of a replicated command.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::auth::{create_scram_user, UserRecord};
use crate::binder::{DeletePlan, InsertPlan, UpdatePlan};
use crate::catalog::TableSchema;
use crate::codec;
use crate::consensus::{ClientCommand, RaftRole, RaftShared};
use crate::hlc::{HlcClock, HlcTimestamp};
use crate::replicated_identity::{ReplicatedIdentityMutation, ReplicatedScramCredential};
use crate::replicated_identity_store::ReplicatedIdentityState;
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
    #[error("cluster node is catching up replicated SQL state")]
    CatchingUp,
    #[error("not Raft leader; current leader is {leader:?}")]
    NotLeader { leader: Option<String> },
    #[error("Raft command channel closed")]
    CommandChannelClosed,
    #[error("Raft command acknowledgement channel closed")]
    ReplyChannelClosed,
    #[error("Raft rejected replicated mutation: {0}")]
    Raft(String),
    #[error("storage failure while materializing replicated SQL: {0}")]
    Storage(String),
    #[error("cannot decode stored row while materializing replicated SQL")]
    CorruptStoredRow,
    #[error("replicated mutation encoding failed: {0}")]
    Encoding(String),
    #[error("replicated identity state is not initialized; explicit migration is required")]
    IdentityNotInitialized,
    #[error("replicated identity initialization conflicts with authoritative state")]
    IdentityInitializationConflict,
    #[error("identity user already exists: {0}")]
    UserAlreadyExists(String),
    #[error("identity user does not exist: {0}")]
    UserNotFound(String),
    #[error("replicated identity failure: {0}")]
    Identity(String),
}

#[derive(Clone)]
pub struct ReplicatedSqlGateway {
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    serving_ready: Arc<AtomicBool>,
    mutation_serial: Arc<Mutex<()>>,
}

impl ReplicatedSqlGateway {
    pub fn new(
        client_tx: mpsc::Sender<ClientCommand>,
        shared: Arc<Mutex<RaftShared>>,
        engine: Arc<StorageEngine>,
        clock: Arc<HlcClock>,
    ) -> Self {
        Self::new_with_readiness(
            client_tx,
            shared,
            engine,
            clock,
            Arc::new(AtomicBool::new(true)),
        )
    }

    pub fn new_with_readiness(
        client_tx: mpsc::Sender<ClientCommand>,
        shared: Arc<Mutex<RaftShared>>,
        engine: Arc<StorageEngine>,
        clock: Arc<HlcClock>,
        serving_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client_tx,
            shared,
            engine,
            clock,
            serving_ready,
            mutation_serial: Arc::new(Mutex::new(())),
        }
    }

    pub fn serving_ready(&self) -> bool {
        self.serving_ready.load(Ordering::Acquire)
    }

    pub async fn current_leader(&self) -> Option<String> {
        self.shared.lock().await.leader_id.clone()
    }

    pub async fn prepare_mutation(&self) -> Result<(), ReplicatedGatewayError> {
        self.commit_readiness_barrier().await.map(|_| ())
    }

    pub async fn create_table(
        &self,
        schema: TableSchema,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        self.submit_sql(ReplicatedMutation::CreateTable { schema })
            .await
    }

    pub async fn drop_table(
        &self,
        table: &str,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        self.submit_sql(ReplicatedMutation::DropTable {
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

        self.submit_sql(ReplicatedMutation::InsertRows {
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
        self.submit_sql(ReplicatedMutation::UpdateRows {
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
        self.submit_sql(ReplicatedMutation::DeleteRows {
            table: plan.table.name.clone(),
            table_id,
            commit_ts,
            primary_keys,
        })
        .await
    }

    pub async fn initialize_identity(
        &self,
        records: &[UserRecord],
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        let requested = ReplicatedIdentityState::from_user_records(records)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?;
        if let Some(existing) = ReplicatedIdentityState::load(&self.engine)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?
        {
            if existing != requested {
                return Err(ReplicatedGatewayError::IdentityInitializationConflict);
            }
        }
        self.submit_identity(ReplicatedIdentityMutation::Initialize {
            users: requested.users().to_vec(),
        })
        .await
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        initialize_if_empty: bool,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;

        let mut state = ReplicatedIdentityState::load(&self.engine)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?;
        if state.is_none() {
            if !initialize_if_empty {
                return Err(ReplicatedGatewayError::IdentityNotInitialized);
            }
            self.submit_identity(ReplicatedIdentityMutation::Initialize { users: vec![] })
                .await?;
            state = Some(ReplicatedIdentityState::default());
        }
        if state
            .as_ref()
            .is_some_and(|identity| identity.contains_user(username))
        {
            return Err(ReplicatedGatewayError::UserAlreadyExists(
                username.to_string(),
            ));
        }

        let record = create_scram_user(username, password);
        let credential = ReplicatedScramCredential::from_user_record(&record)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?;
        self.submit_identity(ReplicatedIdentityMutation::CreateUser {
            username: username.to_string(),
            credential,
        })
        .await
    }

    pub async fn alter_user(
        &self,
        username: &str,
        new_password: &str,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        let state = ReplicatedIdentityState::load(&self.engine)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?
            .ok_or(ReplicatedGatewayError::IdentityNotInitialized)?;
        if !state.contains_user(username) {
            return Err(ReplicatedGatewayError::UserNotFound(username.to_string()));
        }

        let record = create_scram_user(username, new_password);
        let credential = ReplicatedScramCredential::from_user_record(&record)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?;
        self.submit_identity(ReplicatedIdentityMutation::AlterUser {
            username: username.to_string(),
            credential,
        })
        .await
    }

    pub async fn drop_user(
        &self,
        username: &str,
        if_exists: bool,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let _guard = self.mutation_serial.lock().await;
        self.commit_readiness_barrier().await?;
        let state = ReplicatedIdentityState::load(&self.engine)
            .map_err(|error| ReplicatedGatewayError::Identity(error.to_string()))?
            .ok_or(ReplicatedGatewayError::IdentityNotInitialized)?;
        if !if_exists && !state.contains_user(username) {
            return Err(ReplicatedGatewayError::UserNotFound(username.to_string()));
        }
        self.submit_identity(ReplicatedIdentityMutation::DropUser {
            username: username.to_string(),
            if_exists,
        })
        .await
    }

    async fn ensure_leader(&self) -> Result<(), ReplicatedGatewayError> {
        if !self.serving_ready() {
            return Err(ReplicatedGatewayError::CatchingUp);
        }
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

    async fn submit_sql(
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

    async fn submit_identity(
        &self,
        mutation: ReplicatedIdentityMutation,
    ) -> Result<ReplicatedMutationAck, ReplicatedGatewayError> {
        let command_tag = mutation.command_tag();
        let payload = mutation
            .encode()
            .map_err(|error| ReplicatedGatewayError::Encoding(error.to_string()))?;
        let raft_index = self.submit_payload(payload).await?;
        Ok(ReplicatedMutationAck {
            raft_index,
            command_tag,
            affected_rows: None,
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
    async fn closed_serving_gate_rejects_before_leader_check() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let clock = Arc::new(HlcClock::new(500));
        let bus = ChannelTransport::new_bus();
        let transport =
            Arc::new(ChannelTransport::register("catching-up".to_string(), Arc::clone(&bus)).await);
        let node = RaftNode::new(
            "catching-up".to_string(),
            vec!["missing".to_string()],
            transport,
        );
        let (client_tx, shared, _handle) = node.spawn();
        let serving_ready = Arc::new(AtomicBool::new(false));
        let gateway = ReplicatedSqlGateway::new_with_readiness(
            client_tx,
            shared,
            Arc::clone(&engine),
            clock,
            serving_ready,
        );

        let error = gateway.prepare_mutation().await.unwrap_err();
        assert!(matches!(error, ReplicatedGatewayError::CatchingUp));
        assert!(engine.read_catalog_entry("items").unwrap().is_none());
    }

    async fn single_node_gateway() -> (
        ReplicatedSqlGateway,
        Arc<StorageEngine>,
        Arc<HlcClock>,
        Arc<tokio::sync::Mutex<RaftShared>>,
        TempDir,
        crate::consensus::RaftTaskHandle,
    ) {
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
        let (client_tx, shared, handle) = node.spawn();
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
        let gateway = ReplicatedSqlGateway::new(
            client_tx,
            Arc::clone(&shared),
            Arc::clone(&engine),
            Arc::clone(&clock),
        );
        (gateway, engine, clock, shared, dir, handle)
    }

    #[tokio::test]
    async fn single_node_insert_waits_for_barrier_and_state_machine_apply() {
        let (gateway, engine, _clock, _shared, _dir, _handle) = single_node_gateway().await;
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

    #[tokio::test]
    async fn identity_ddl_waits_for_confirmed_apply_and_converges_to_durable_state() {
        let (gateway, engine, _clock, _shared, _dir, _handle) = single_node_gateway().await;
        let create = gateway
            .create_user("alice", "secret-one", true)
            .await
            .unwrap();
        assert_eq!(create.command_tag, "CREATE USER");
        assert!(create.raft_index >= 3);
        let created = ReplicatedIdentityState::load(&engine).unwrap().unwrap();
        assert!(created.contains_user("alice"));

        let before = created
            .user_record("alice")
            .unwrap()
            .unwrap()
            .credential;
        gateway
            .alter_user("alice", "secret-two")
            .await
            .unwrap();
        let after = ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .user_record("alice")
            .unwrap()
            .unwrap()
            .credential;
        match (before, after) {
            (
                crate::auth::StoredCredential::ScramSha256(before),
                crate::auth::StoredCredential::ScramSha256(after),
            ) => assert_ne!(before.stored_key, after.stored_key),
            _ => panic!("replicated identity must use SCRAM credentials"),
        }

        gateway.drop_user("alice", false).await.unwrap();
        assert!(!ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .contains_user("alice"));
    }
}
