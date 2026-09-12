// SPDX-License-Identifier: Apache-2.0
//! Bounded Raft operator surface. Guards are checked in the serialized loop.
use super::{ClientCommand, ClusterMembership, MembershipChange};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
pub const GUARDED_MEMBERSHIP_TAG: &[u8] = b"NBOG\x01";
const TIMEOUT: Duration = Duration::from_secs(5);
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MembershipGuard {
    pub leader: String,
    pub term: u64,
    pub generation: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardedMembership {
    pub guard: MembershipGuard,
    pub change: MembershipChange,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorStatus {
    pub id: String,
    pub leader: Option<String>,
    pub is_leader: bool,
    pub term: u64,
    pub committed: ClusterMembership,
    pub transition_pending: bool,
    pub applied: u64,
    pub commit_index: u64,
    pub ready: bool,
    pub matched: BTreeMap<String, u64>,
    /// Nonzero only after the authoritative observation barrier succeeds.
    pub authority_index: u64,
}
pub(super) enum ControlRequest {
    Status(oneshot::Sender<OperatorStatus>),
    Transfer(MembershipGuard, String, oneshot::Sender<Result<(), String>>),
}
#[derive(Clone)]
pub struct OperatorHandle {
    pub(super) control_tx: mpsc::Sender<ControlRequest>,
    pub(super) client_tx: mpsc::Sender<ClientCommand>,
}
impl OperatorHandle {
    async fn submit(&self, payload: Vec<u8>) -> Result<u64, String> {
        tokio::time::timeout(TIMEOUT, async {
            let (reply, rx) = oneshot::channel();
            self.client_tx
                .send(ClientCommand { payload, reply })
                .await
                .map_err(|_| "Raft stopped".to_string())?;
            rx.await.map_err(|_| "Raft reply dropped".to_string())?
        })
        .await
        .map_err(|_| "operator command timed out; outcome uncertain".to_string())?
    }
    pub async fn status(&self) -> Result<OperatorStatus, String> {
        tokio::time::timeout(TIMEOUT, async {
            let (tx, rx) = oneshot::channel();
            self.control_tx
                .send(ControlRequest::Status(tx))
                .await
                .map_err(|_| "Raft stopped".to_string())?;
            rx.await.map_err(|_| "status dropped".to_string())
        })
        .await
        .map_err(|_| "operator status timeout".to_string())?
    }
    pub async fn observe_authoritative(&self) -> Result<OperatorStatus, String> {
        let before = self.status().await?;
        if !before.is_leader || !before.ready {
            return Err("not serving leader".into());
        }
        let index = self.submit(b"NBRB\x01".to_vec()).await?;
        let mut after = self.status().await?;
        if !after.is_leader || !after.ready || after.term != before.term || after.applied < index {
            return Err("leadership changed during operator authority barrier".into());
        }
        after.authority_index = index;
        Ok(after)
    }
    pub async fn change_membership(&self, request: GuardedMembership) -> Result<u64, String> {
        let mut bytes = GUARDED_MEMBERSHIP_TAG.to_vec();
        bytes.extend(serde_json::to_vec(&request).map_err(|e| e.to_string())?);
        if bytes.len() > 4096 {
            return Err("guarded membership request too large".into());
        }
        self.submit(bytes).await
    }
    pub async fn transfer(&self, guard: MembershipGuard, target: String) -> Result<(), String> {
        tokio::time::timeout(TIMEOUT, async {
            let (tx, rx) = oneshot::channel();
            self.control_tx
                .send(ControlRequest::Transfer(guard, target, tx))
                .await
                .map_err(|_| "Raft stopped".to_string())?;
            rx.await.map_err(|_| "transfer reply dropped".to_string())?
        })
        .await
        .map_err(|_| "transfer timeout; observe new leader before removal".to_string())?
    }
}
