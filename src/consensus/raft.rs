// SPDX-License-Identifier: Apache-2.0
//! Raft consensus state machine.
//!
//! Phase 3 replaces the historical process-local AddNode/RemoveNode behavior
//! with committed versioned membership, non-voting learners, and Raft joint
//! consensus. The newest membership entry in the local log is the *effective*
//! configuration for elections/commit decisions even before that entry is
//! committed, as required to avoid incompatible majority rules during a
//! transition. The last committed configuration is additionally persisted in
//! `PersistentState` so restart and compaction never fall back to environment
//! peer counts.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::consensus::log::{
    PersistentState, RaftPersistenceStore, StagedSnapshot, StagedSnapshotKind,
};
use crate::consensus::membership::ClusterMembership;
use crate::consensus::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
    MembershipChange, NodeId, RaftMessage, RequestVoteArgs, RequestVoteReply,
};
use crate::consensus::snapshot::StateMachineSnapshotStore;
use crate::consensus::snapshot_payload::{decode_snapshot_payload, encode_snapshot_payload};
use crate::consensus::transport::Transport;
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Instant};

// ── Admin payload tags ─────────────────────────────────────────────────────

pub const MEMBERSHIP_CHANGE_TAG: &[u8] = &[0xFC, 0xFD];
pub const COMPACT_LOG_TAG: &[u8] = &[0xFE, 0xFD];
pub const LEADER_TRANSFER_TAG: &[u8] = &[0xFA, 0xFD];

pub fn encode_membership_change(change: &MembershipChange) -> Vec<u8> {
    let mut v = MEMBERSHIP_CHANGE_TAG.to_vec();
    v.extend_from_slice(
        &serde_json::to_vec(change).expect("MembershipChange must be JSON-serializable"),
    );
    v
}

pub fn encode_compact_log(last_index: u64, data: &[u8]) -> Vec<u8> {
    let mut v = COMPACT_LOG_TAG.to_vec();
    v.extend_from_slice(&last_index.to_be_bytes());
    v.extend_from_slice(data);
    v
}

pub fn encode_leader_transfer(target: &str) -> Vec<u8> {
    let mut v = LEADER_TRANSFER_TAG.to_vec();
    v.extend_from_slice(target.as_bytes());
    v
}

const HEARTBEAT_MS: u64 = 50;
const ELECTION_TIMEOUT_BASE_MS: u64 = 150;
pub const APPLY_CHANNEL_CAPACITY: usize = 1024;
const LEADER_TRANSFER_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

struct LeaderState {
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
}

pub struct ClientCommand {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<u64, String>>,
}

pub struct CommittedEntry {
    pub entry: LogEntry,
    pub completion: oneshot::Sender<Result<(), String>>,
}

pub struct RaftTaskHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
    transfer_tx: mpsc::Sender<oneshot::Sender<Result<String, String>>>,
}

impl RaftTaskHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.join_handle).await;
    }

    /// Initiate transfer to an eligible, fully caught-up voter. The returned ID
    /// identifies the target; callers that need to remove the old leader must
    /// additionally observe/prove that target as the new leader before issuing
    /// the removal command.
    pub async fn request_leader_transfer(&self) -> Result<String, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.transfer_tx
            .send(reply_tx)
            .await
            .map_err(|_| "raft event loop closed".to_string())?;
        match tokio::time::timeout(Duration::from_millis(LEADER_TRANSFER_TIMEOUT_MS), reply_rx)
            .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("reply channel dropped".to_string()),
            Err(_) => Err("leader transfer timed out".to_string()),
        }
    }
}

impl Drop for RaftTaskHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.join_handle.abort();
    }
}

pub struct RaftNode<T: Transport> {
    id: NodeId,
    /// Fixed bootstrap/seed view. Once `ps.membership` exists it is no longer a
    /// source of quorum truth.
    bootstrap_membership: ClusterMembership,
    joining_learner: bool,
    transport: Arc<T>,
    ps: PersistentState,
    /// Latest configuration represented by committed membership plus any newer
    /// membership entries still present in the local log.
    effective_membership: ClusterMembership,

    commit_index: u64,
    last_applied: u64,
    role: RaftRole,
    leader_id: Option<NodeId>,

    votes_received: BTreeSet<NodeId>,
    leader: Option<LeaderState>,

    snapshot_data: Arc<Vec<u8>>,
    snapshot_store: Option<Arc<dyn StateMachineSnapshotStore>>,
    pending_staged_snapshot: Option<StagedSnapshot>,

    serving_ready: Arc<AtomicBool>,
    successful_append_seen: bool,
    recovery_readiness_pending: bool,

    transfer_in_progress: Option<(NodeId, Instant)>,
    persistence: Option<Arc<dyn RaftPersistenceStore>>,
    election_timeout_base_ms: u64,

    apply_tx: Option<mpsc::Sender<LogEntry>>,
    confirmed_apply_tx: Option<mpsc::Sender<CommittedEntry>>,
    pending_clients: HashMap<u64, oneshot::Sender<Result<u64, String>>>,
    /// Membership admin RPCs are acknowledged only after the corresponding
    /// transition reaches its durable apply point. The sender is retained here
    /// while the event loop remains leader.
    pending_membership_rpcs: HashMap<u64, NodeId>,
}

impl<T: Transport> RaftNode<T> {
    /// Initial fixed-membership constructor retained for backward compatibility.
    pub fn new(id: NodeId, peers: Vec<NodeId>, transport: Arc<T>) -> Self {
        let bootstrap = ClusterMembership::bootstrap(id.clone(), peers);
        let mut ps = PersistentState::new();
        ps.membership = Some(bootstrap.clone());
        Self::from_bootstrap(id, bootstrap, false, transport, ps)
    }

    /// Construct a brand-new joining process. Seed voters are routing/bootstrap
    /// knowledge only; this local ID cannot campaign or vote until the committed
    /// membership received from Raft says that it is a voter.
    pub fn new_learner(
        id: NodeId,
        seed_voters: Vec<NodeId>,
        transport: Arc<T>,
    ) -> Result<Self, String> {
        let bootstrap = ClusterMembership::bootstrap_learner(id.clone(), seed_voters)?;
        Ok(Self::from_bootstrap(
            id,
            bootstrap,
            true,
            transport,
            PersistentState::new(),
        ))
    }

    fn from_bootstrap(
        id: NodeId,
        bootstrap_membership: ClusterMembership,
        joining_learner: bool,
        transport: Arc<T>,
        ps: PersistentState,
    ) -> Self {
        Self {
            id,
            effective_membership: bootstrap_membership.clone(),
            bootstrap_membership,
            joining_learner,
            transport,
            ps,
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
            votes_received: BTreeSet::new(),
            leader: None,
            snapshot_data: Arc::new(vec![]),
            snapshot_store: None,
            pending_staged_snapshot: None,
            serving_ready: Arc::new(AtomicBool::new(!joining_learner)),
            successful_append_seen: false,
            recovery_readiness_pending: false,
            transfer_in_progress: None,
            persistence: None,
            election_timeout_base_ms: ELECTION_TIMEOUT_BASE_MS,
            apply_tx: None,
            confirmed_apply_tx: None,
            pending_clients: HashMap::new(),
            pending_membership_rpcs: HashMap::new(),
        }
    }

    pub fn serving_readiness(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.serving_ready)
    }

    pub fn with_apply_tx(mut self, tx: mpsc::Sender<LogEntry>) -> Self {
        self.apply_tx = Some(tx);
        self.confirmed_apply_tx = None;
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn StateMachineSnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        let recovered_install = self.recover_staged_snapshot_if_possible();
        if !recovered_install {
            self.restore_active_snapshot_if_possible();
        }
        self
    }

    pub fn with_confirmed_apply_tx(mut self, tx: mpsc::Sender<CommittedEntry>) -> Self {
        let has_unrecoverable_snapshot = (self.ps.snapshot_index != 0
            || !self.snapshot_data.is_empty())
            && self.snapshot_store.is_none();
        let has_unrecoverable_install = self
            .pending_staged_snapshot
            .as_ref()
            .is_some_and(|staged| staged.kind == StagedSnapshotKind::Installation)
            && self.snapshot_store.is_none();
        if has_unrecoverable_snapshot || has_unrecoverable_install {
            panic!(
                "replicated SQL confirmed apply cannot start from a legacy Raft snapshot; SQL state snapshot restore is not implemented"
            );
        }
        self.confirmed_apply_tx = Some(tx);
        self.apply_tx = None;
        self
    }

    pub fn with_persistence(mut self, store: Arc<dyn RaftPersistenceStore>) -> Self {
        let loaded = match store.load() {
            Ok(state) => state,
            Err(error) => panic!("fatal Raft persistence load failure: {error}"),
        };
        let fresh_persistent_state = loaded.is_none();
        let staged = match store.load_staged_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => panic!("fatal staged Raft snapshot load failure: {error}"),
        };
        let mut migrated_phase2_membership = false;

        if let Some((mut ps, snap)) = loaded {
            let has_boundary = ps.snapshot_index != 0;
            let has_bytes = !snap.is_empty();
            if has_boundary != has_bytes {
                panic!(
                    "fatal Raft snapshot recovery: snapshot boundary metadata and active snapshot bytes are inconsistent"
                );
            }
            if ps.membership.is_none() && !self.joining_learner {
                // One-time Phase-2 -> Phase-3 migration. The fixed configured
                // bootstrap is authoritative only because no dynamic membership
                // could have existed in Phase 2.
                ps.membership = Some(self.bootstrap_membership.clone());
                migrated_phase2_membership = true;
            }
            if let Some(membership) = &ps.membership {
                self.validate_persisted_membership(membership, &ps)
                    .unwrap_or_else(|error| panic!("fatal persisted membership state: {error}"));
            }
            self.snapshot_data = Arc::new(snap);
            let snap_idx = ps.snapshot_index;
            self.ps = ps;
            self.commit_index = snap_idx;
            self.last_applied = snap_idx;
        }

        self.persistence = Some(store);
        self.pending_staged_snapshot = staged;
        self.recompute_effective_membership()
            .unwrap_or_else(|error| panic!("fatal effective membership recovery: {error}"));

        let recovered_install = self.recover_staged_snapshot_if_possible();
        if !recovered_install {
            self.restore_active_snapshot_if_possible();
        }
        if migrated_phase2_membership && self.pending_staged_snapshot.is_none() {
            self.persist();
        }

        // A process that loaded any durable Raft state must re-prove a safe
        // serving frontier. `commit_index` is volatile, so blindly inheriting
        // constructor readiness can expose SQL/identity state before restart
        // recovery has established which durable tail entries are committed.
        if !fresh_persistent_state {
            self.recovery_readiness_pending = true;
            self.serving_ready.store(false, Ordering::Release);
        }
        if fresh_persistent_state
            && (self.joining_learner
                || self
                    .effective_membership
                    .replication_targets()
                    .iter()
                    .any(|id| id != &self.id))
        {
            self.serving_ready.store(false, Ordering::Release);
        }
        if self
            .ps
            .membership
            .as_ref()
            .is_some_and(|m| m.is_removed(&self.id))
        {
            self.serving_ready.store(false, Ordering::Release);
        }
        self
    }

    fn validate_persisted_membership(
        &self,
        membership: &ClusterMembership,
        ps: &PersistentState,
    ) -> Result<(), String> {
        membership.validate()?;
        if membership.config_index > ps.last_log_index() {
            return Err(format!(
                "membership config index {} is beyond local Raft end {}",
                membership.config_index,
                ps.last_log_index()
            ));
        }
        Ok(())
    }

    fn committed_membership(&self) -> &ClusterMembership {
        self.ps
            .membership
            .as_ref()
            .unwrap_or(&self.bootstrap_membership)
    }

    fn decode_membership_command(payload: &[u8]) -> Result<Option<MembershipChange>, String> {
        if !payload.starts_with(MEMBERSHIP_CHANGE_TAG) {
            return Ok(None);
        }
        let change =
            serde_json::from_slice::<MembershipChange>(&payload[MEMBERSHIP_CHANGE_TAG.len()..])
                .map_err(|error| format!("decode membership command: {error}"))?;
        Ok(Some(change))
    }

    fn transition_membership(
        base: &ClusterMembership,
        change: &MembershipChange,
        index: u64,
    ) -> Result<ClusterMembership, String> {
        match change {
            MembershipChange::AddNode(id) | MembershipChange::AddLearner(id) => {
                base.add_learner(id.clone(), index)
            }
            MembershipChange::PromoteLearner(id) => base.begin_promotion(id, index),
            MembershipChange::RemoveNode(id) => base.begin_removal(id, index),
            MembershipChange::FinalizeJoint => base.finalize_joint(index),
        }
    }

    /// Rebuild the latest effective configuration from the last committed
    /// durable membership and every newer membership command still present in
    /// the local log. Uncommitted configuration entries therefore immediately
    /// govern election/commit quorums, while log truncation naturally rolls an
    /// uncommitted transition back.
    fn recompute_effective_membership(&mut self) -> Result<(), String> {
        let mut membership = self.committed_membership().clone();
        let committed_config_index = membership.config_index;
        for entry in &self.ps.log {
            if entry.index <= committed_config_index {
                continue;
            }
            if let Some(change) = Self::decode_membership_command(&entry.command)? {
                membership = Self::transition_membership(&membership, &change, entry.index)?;
            }
        }
        membership.validate()?;
        self.effective_membership = membership;
        self.sync_leader_tracking();
        Ok(())
    }

    fn sync_leader_tracking(&mut self) {
        let Some(leader) = &mut self.leader else {
            return;
        };
        let mut targets = self.effective_membership.replication_targets();
        targets.remove(&self.id);
        leader.next_index.retain(|id, _| targets.contains(id));
        leader.match_index.retain(|id, _| targets.contains(id));
        let next = self.ps.last_log_index().saturating_add(1);
        for target in targets {
            leader.next_index.entry(target.clone()).or_insert(next);
            leader.match_index.entry(target).or_insert(0);
        }
    }

    fn membership_transition_active(&self) -> bool {
        let committed = self.committed_membership();
        if committed.is_joint() {
            return true;
        }
        self.ps.log.iter().any(|entry| {
            entry.index > committed.config_index && entry.command.starts_with(MEMBERSHIP_CHANGE_TAG)
        })
    }

    fn validate_snapshot_membership_boundary(
        membership: &ClusterMembership,
        boundary: u64,
    ) -> Result<(), String> {
        membership.validate()?;
        if membership.is_joint() {
            return Err("snapshot may not encode a joint membership transition".to_string());
        }
        if membership.config_index > boundary {
            return Err(format!(
                "snapshot membership config index {} exceeds snapshot boundary {boundary}",
                membership.config_index
            ));
        }
        Ok(())
    }

    fn validate_incoming_snapshot_membership(
        &self,
        incoming: &ClusterMembership,
        boundary: u64,
    ) -> Result<(), String> {
        Self::validate_snapshot_membership_boundary(incoming, boundary)?;
        if let Some(local) = &self.ps.membership {
            if local.config_index > boundary {
                return Err(format!(
                    "snapshot boundary {boundary} would regress committed membership at {}",
                    local.config_index
                ));
            }
            if local.generation > incoming.generation {
                return Err(format!(
                    "snapshot membership generation {} regresses local generation {}",
                    incoming.generation, local.generation
                ));
            }
            if local.generation == incoming.generation && local != incoming {
                return Err("conflicting membership metadata at the same generation".to_string());
            }
        }
        Ok(())
    }

    fn restore_active_snapshot_if_possible(&self) {
        let has_boundary = self.ps.snapshot_index != 0;
        let has_bytes = !self.snapshot_data.is_empty();
        if has_boundary != has_bytes {
            panic!(
                "fatal Raft snapshot recovery: snapshot boundary metadata and active snapshot bytes are inconsistent"
            );
        }
        if !has_boundary {
            return;
        }
        let Some(snapshot_store) = &self.snapshot_store else {
            return;
        };
        let sql_bytes = match decode_snapshot_payload(&self.snapshot_data) {
            Ok(Some((membership, sql_bytes))) => {
                Self::validate_snapshot_membership_boundary(&membership, self.ps.snapshot_index)
                    .unwrap_or_else(|error| {
                        panic!("fatal active snapshot membership validation failure: {error}")
                    });
                if let Some(current) = &self.ps.membership {
                    if current.config_index <= self.ps.snapshot_index && current != &membership {
                        panic!(
                            "fatal active snapshot recovery: persisted membership conflicts with snapshot membership"
                        );
                    }
                }
                sql_bytes
            }
            Ok(None) => self.snapshot_data.as_slice(),
            Err(error) => panic!("fatal Raft snapshot envelope recovery failure: {error}"),
        };
        if let Err(error) = snapshot_store.restore_snapshot(
            self.ps.snapshot_index,
            self.ps.snapshot_term,
            sql_bytes,
        ) {
            panic!("fatal state-machine snapshot recovery failure: {error}");
        }
    }

    fn recover_staged_snapshot_if_possible(&mut self) -> bool {
        let Some(staged) = self.pending_staged_snapshot.clone() else {
            return false;
        };
        let Some(persistence) = self.persistence.as_ref().cloned() else {
            return false;
        };

        match staged.kind {
            StagedSnapshotKind::Creation => {
                if let Err(error) = persistence.clear_staged_snapshot() {
                    panic!("fatal staged Raft snapshot clear failure: {error}");
                }
                self.pending_staged_snapshot = None;
                false
            }
            StagedSnapshotKind::Installation => {
                let Some(snapshot_store) = self.snapshot_store.as_ref().cloned() else {
                    return false;
                };

                if staged.last_included_index <= self.ps.snapshot_index {
                    if let Err(error) = persistence.clear_staged_snapshot() {
                        panic!("fatal staged Raft snapshot clear failure: {error}");
                    }
                    self.pending_staged_snapshot = None;
                    return false;
                }

                let (incoming_membership, sql_bytes) = match decode_snapshot_payload(&staged.data) {
                    Ok(Some((membership, sql_bytes))) => {
                        self.validate_incoming_snapshot_membership(
                            &membership,
                            staged.last_included_index,
                        )
                        .unwrap_or_else(|error| {
                            panic!("fatal staged snapshot membership validation failure: {error}")
                        });
                        (Some(membership), sql_bytes)
                    }
                    Ok(None) => {
                        if self
                            .ps
                            .membership
                            .as_ref()
                            .is_some_and(|m| m.config_index > 0)
                        {
                            panic!(
                                "fatal staged snapshot recovery: legacy snapshot cannot replace dynamic membership"
                            );
                        }
                        (None, staged.data.as_slice())
                    }
                    Err(error) => {
                        panic!("fatal staged snapshot envelope validation failure: {error}")
                    }
                };

                if let Err(error) = snapshot_store.validate_snapshot(
                    staged.last_included_index,
                    staged.last_included_term,
                    sql_bytes,
                ) {
                    panic!("fatal staged state-machine snapshot validation failure: {error}");
                }
                if let Err(error) = snapshot_store.restore_snapshot(
                    staged.last_included_index,
                    staged.last_included_term,
                    sql_bytes,
                ) {
                    panic!("fatal staged state-machine snapshot recovery failure: {error}");
                }

                if let Some(membership) = incoming_membership {
                    self.ps.membership = Some(membership);
                }
                self.ps
                    .install_snapshot(staged.last_included_index, staged.last_included_term);
                self.snapshot_data = Arc::clone(&staged.data);
                self.commit_index = self.commit_index.max(staged.last_included_index);
                self.last_applied = self.last_applied.max(staged.last_included_index);
                self.recompute_effective_membership()
                    .unwrap_or_else(|error| {
                        panic!("fatal effective membership after snapshot recovery: {error}")
                    });
                if let Err(error) = persistence.save(&self.ps, &self.snapshot_data) {
                    panic!("fatal Raft snapshot recovery publish failure: {error}");
                }
                self.pending_staged_snapshot = None;
                true
            }
        }
    }

    pub fn set_election_timeout_ms(&mut self, ms: u64) {
        self.election_timeout_base_ms = ms;
    }

    fn election_timeout(&self) -> Duration {
        let base = self.election_timeout_base_ms;
        let jitter_range = base.max(10);
        let extra = rand::thread_rng().gen_range(0..jitter_range);
        Duration::from_millis(base + extra)
    }

    fn persist(&self) {
        if let Some(store) = &self.persistence {
            if let Err(error) = store.save(&self.ps, &self.snapshot_data) {
                panic!("fatal Raft persistence save failure: {error}");
            }
        }
    }

    fn fail_uncommitted_clients(&mut self, reason: &str) {
        let committed = self.commit_index;
        let indexes: Vec<u64> = self
            .pending_clients
            .keys()
            .copied()
            .filter(|index| *index > committed)
            .collect();
        for index in indexes {
            if let Some(reply) = self.pending_clients.remove(&index) {
                let _ = reply.send(Err(reason.to_string()));
            }
            self.pending_membership_rpcs.remove(&index);
        }
    }

    fn fail_all_clients(&mut self, reason: &str) {
        let pending = std::mem::take(&mut self.pending_clients);
        for (_, reply) in pending {
            let _ = reply.send(Err(reason.to_string()));
        }
        self.pending_membership_rpcs.clear();
    }

    pub fn spawn(
        mut self,
    ) -> (
        mpsc::Sender<ClientCommand>,
        Arc<Mutex<RaftShared>>,
        RaftTaskHandle,
    ) {
        if self
            .pending_staged_snapshot
            .as_ref()
            .is_some_and(|staged| staged.kind == StagedSnapshotKind::Installation)
        {
            panic!(
                "fatal Raft snapshot recovery: staged installation requires a state-machine snapshot store"
            );
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ClientCommand>(64);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let (transfer_tx, mut transfer_rx) =
            mpsc::channel::<oneshot::Sender<Result<String, String>>>(1);
        let shared = Arc::new(Mutex::new(RaftShared {
            role: RaftRole::Follower,
            leader_id: None,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            membership: self.effective_membership.clone(),
        }));
        let shared_clone = shared.clone();

        let join_handle = tokio::spawn(async move {
            let mut election_deadline = Instant::now() + self.election_timeout();
            let mut hb_interval = time::interval(Duration::from_millis(HEARTBEAT_MS));

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        self.fail_all_clients("raft node shutting down before command apply");
                        break;
                    }
                    msg = self.transport.recv() => {
                        match msg {
                            Some((from, rmsg)) => {
                                election_deadline = self.handle_message(from, rmsg, election_deadline).await;
                            }
                            None => {
                                self.fail_all_clients("raft transport closed before command apply");
                                break;
                            }
                        }
                    }
                    _ = hb_interval.tick() => {
                        if self.role == RaftRole::Leader {
                            self.send_heartbeats().await;
                        }
                    }
                    _ = time::sleep_until(election_deadline) => {
                        if self.role != RaftRole::Leader {
                            self.start_election().await;
                        }
                        election_deadline = Instant::now() + self.election_timeout();
                    }
                    Some(cmd) = cmd_rx.recv() => {
                        // Compaction is a local admin operation and retains its
                        // direct acknowledgement. Membership is no longer a
                        // legacy append-only acknowledgement: it waits for the
                        // required committed durable apply/finalization.
                        let direct_admin_ack = cmd.payload.starts_with(COMPACT_LOG_TAG);
                        match self.handle_client_command(cmd.payload) {
                            Ok(index) if direct_admin_ack => {
                                let _ = cmd.reply.send(Ok(index));
                            }
                            Ok(index) => {
                                self.pending_clients.insert(index, cmd.reply);
                                self.try_advance_commit();
                                if self.role == RaftRole::Leader {
                                    self.send_heartbeats().await;
                                }
                            }
                            Err(error) => {
                                let _ = cmd.reply.send(Err(error));
                            }
                        }
                    }
                    Some(reply_tx) = transfer_rx.recv() => {
                        let result = match self.choose_transfer_target() {
                            Ok(target) => match self.initiate_leader_transfer(target.clone()) {
                                Ok(()) => {
                                    self.transport
                                        .send(
                                            &target,
                                            RaftMessage::TimeoutNow {
                                                term: self.ps.current_term,
                                            },
                                        )
                                        .await;
                                    Ok(target)
                                }
                                Err(error) => Err(error),
                            },
                            Err(error) => Err(error),
                        };
                        let _ = reply_tx.send(result);
                    }
                }

                while self.last_applied < self.commit_index {
                    let next_index = self.last_applied + 1;
                    let physical = (next_index - self.ps.snapshot_index) as usize;
                    let Some(entry) = self.ps.log.get(physical).cloned() else {
                        self.fail_all_clients("committed Raft entry missing from local log");
                        return;
                    };

                    let mut joint_started = false;
                    if entry.command.starts_with(MEMBERSHIP_CHANGE_TAG) {
                        match self.apply_committed_membership(&entry) {
                            Ok(started) => joint_started = started,
                            Err(error) => {
                                self.fail_all_clients(&format!(
                                    "committed membership apply failed at index {next_index}: {error}"
                                ));
                                return;
                            }
                        }
                    }

                    // All committed log positions, including Raft control
                    // entries, pass through the state-machine completion point.
                    // The production SQL state machine records the durable apply
                    // index for non-SQL entries without mutating SQL data.
                    if let Some(tx) = &self.confirmed_apply_tx {
                        let (completion_tx, completion_rx) = oneshot::channel();
                        let send_result = tokio::select! {
                            biased;
                            _ = &mut shutdown_rx => {
                                self.fail_all_clients("raft node shutting down during state-machine apply");
                                return;
                            },
                            result = tx.send(CommittedEntry {
                                entry: entry.clone(),
                                completion: completion_tx,
                            }) => result,
                        };
                        if send_result.is_err() {
                            self.fail_all_clients("confirmed state-machine apply channel closed");
                            return;
                        }

                        let completion = tokio::select! {
                            biased;
                            _ = &mut shutdown_rx => {
                                self.fail_all_clients("raft node shutting down while awaiting state-machine completion");
                                return;
                            },
                            result = completion_rx => result,
                        };
                        match completion {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                self.fail_all_clients(&format!(
                                    "committed state-machine apply failed at index {next_index}: {error}"
                                ));
                                return;
                            }
                            Err(_) => {
                                self.fail_all_clients(&format!(
                                    "state-machine completion channel dropped at index {next_index}"
                                ));
                                return;
                            }
                        }
                    } else if let Some(tx) = &self.apply_tx {
                        let send_result = tokio::select! {
                            biased;
                            _ = &mut shutdown_rx => {
                                self.fail_all_clients("raft node shutting down during apply handoff");
                                return;
                            },
                            result = tx.send(entry.clone()) => result,
                        };
                        if send_result.is_err() {
                            self.fail_all_clients("legacy state-machine apply channel closed");
                            return;
                        }
                    }

                    self.last_applied = next_index;

                    let client_reply = self.pending_clients.remove(&next_index);
                    let rpc_reply = self.pending_membership_rpcs.remove(&next_index);
                    if joint_started {
                        if self.role == RaftRole::Leader {
                            match self.ensure_joint_finalize_entry() {
                                Ok(final_index) => {
                                    if let Some(reply) = client_reply {
                                        self.pending_clients.insert(final_index, reply);
                                    }
                                    if let Some(from) = rpc_reply {
                                        self.pending_membership_rpcs.insert(final_index, from);
                                    }
                                    self.try_advance_commit();
                                    self.send_heartbeats().await;
                                }
                                Err(error) => {
                                    if let Some(reply) = client_reply {
                                        let _ = reply.send(Err(error.clone()));
                                    }
                                    if let Some(from) = rpc_reply {
                                        self.transport
                                            .send(
                                                &from,
                                                RaftMessage::MembershipChangeCmdReply {
                                                    success: false,
                                                    error: Some(error),
                                                },
                                            )
                                            .await;
                                    }
                                }
                            }
                        } else {
                            let error = "joint membership entry committed but leadership changed before finalization; outcome is uncertain and the valid leader must finish the transition".to_string();
                            if let Some(reply) = client_reply {
                                let _ = reply.send(Err(error.clone()));
                            }
                            if let Some(from) = rpc_reply {
                                self.transport
                                    .send(
                                        &from,
                                        RaftMessage::MembershipChangeCmdReply {
                                            success: false,
                                            error: Some(error),
                                        },
                                    )
                                    .await;
                            }
                        }
                    } else {
                        if let Some(reply) = client_reply {
                            let _ = reply.send(Ok(next_index));
                        }
                        if let Some(from) = rpc_reply {
                            self.transport
                                .send(
                                    &from,
                                    RaftMessage::MembershipChangeCmdReply {
                                        success: true,
                                        error: None,
                                    },
                                )
                                .await;
                        }
                    }
                }

                let removed = self
                    .ps
                    .membership
                    .as_ref()
                    .is_some_and(|membership| membership.is_removed(&self.id));
                if self.recovery_readiness_pending && !removed {
                    let current_term_commit_proven = self.commit_index > self.ps.snapshot_index
                        && self.ps.term_at(self.commit_index) == self.ps.current_term;
                    let follower_has_no_unresolved_tail = self.role != RaftRole::Leader
                        && self.successful_append_seen
                        && self.commit_index == self.ps.last_log_index();
                    if self.last_applied >= self.commit_index
                        && (current_term_commit_proven || follower_has_no_unresolved_tail)
                    {
                        self.recovery_readiness_pending = false;
                        self.serving_ready.store(true, Ordering::Release);
                    }
                } else if !self.serving_ready.load(Ordering::Acquire)
                    && self.successful_append_seen
                    && self.last_applied >= self.commit_index
                    && !removed
                {
                    self.serving_ready.store(true, Ordering::Release);
                }

                if let Some((_target, deadline)) = &self.transfer_in_progress {
                    if Instant::now() >= *deadline {
                        self.transfer_in_progress = None;
                    }
                }
                if self.transfer_in_progress.is_some() && self.role != RaftRole::Leader {
                    self.transfer_in_progress = None;
                }

                {
                    let mut s = shared_clone.lock().await;
                    s.role = self.role;
                    s.leader_id = self.leader_id.clone();
                    s.commit_index = self.commit_index;
                    s.last_applied = self.last_applied;
                    s.membership = self.effective_membership.clone();
                }
            }
        });

        (
            cmd_tx,
            shared,
            RaftTaskHandle {
                shutdown_tx: Some(shutdown_tx),
                join_handle,
                transfer_tx,
            },
        )
    }

    async fn handle_message(
        &mut self,
        from: NodeId,
        msg: RaftMessage,
        mut election_deadline: Instant,
    ) -> Instant {
        match msg {
            RaftMessage::RequestVote(args) => {
                let reply = self.on_request_vote(args);
                self.transport
                    .send(&from, RaftMessage::RequestVoteReply(reply))
                    .await;
            }
            RaftMessage::RequestVoteReply(reply) => {
                self.on_request_vote_reply(from, reply).await;
            }
            RaftMessage::AppendEntries(args) => {
                let reset = self.effective_membership.is_voter(&args.leader_id)
                    && args.term >= self.ps.current_term;
                let reply = self.on_append_entries(args);
                self.transport
                    .send(&from, RaftMessage::AppendEntriesReply(reply))
                    .await;
                if reset {
                    election_deadline = Instant::now() + self.election_timeout();
                }
            }
            RaftMessage::AppendEntriesReply(reply) => {
                self.on_append_entries_reply(from, reply).await;
            }
            RaftMessage::InstallSnapshot(args) => {
                let reset = self.effective_membership.is_voter(&args.leader_id)
                    && args.term >= self.ps.current_term;
                let reply = self.on_install_snapshot(args);
                self.transport
                    .send(&from, RaftMessage::InstallSnapshotReply(reply))
                    .await;
                if reset {
                    election_deadline = Instant::now() + self.election_timeout();
                }
            }
            RaftMessage::InstallSnapshotReply(reply) => {
                self.on_install_snapshot_reply(from, reply).await;
            }
            RaftMessage::MembershipChangeCmd(change) => {
                if self.role != RaftRole::Leader {
                    self.transport
                        .send(
                            &from,
                            RaftMessage::MembershipChangeCmdReply {
                                success: false,
                                error: Some("not leader".to_string()),
                            },
                        )
                        .await;
                } else {
                    let payload = encode_membership_change(&change);
                    match self.handle_client_command(payload) {
                        Ok(index) => {
                            self.pending_membership_rpcs.insert(index, from);
                            self.try_advance_commit();
                            self.send_heartbeats().await;
                        }
                        Err(error) => {
                            self.transport
                                .send(
                                    &from,
                                    RaftMessage::MembershipChangeCmdReply {
                                        success: false,
                                        error: Some(error),
                                    },
                                )
                                .await;
                        }
                    }
                }
            }
            RaftMessage::MembershipChangeCmdReply { .. } => {}
            RaftMessage::LeaderTransfer { target } => {
                self.on_leader_transfer(&from, target).await;
            }
            RaftMessage::LeaderTransferReply { .. } => {}
            RaftMessage::TimeoutNow { term } => {
                self.on_timeout_now(&from, term, &mut election_deadline)
                    .await;
            }
        }
        election_deadline
    }

    async fn start_election(&mut self) {
        if !self.effective_membership.is_voter(&self.id) {
            self.role = RaftRole::Follower;
            self.votes_received.clear();
            return;
        }
        self.role = RaftRole::Candidate;
        self.ps.current_term += 1;
        self.ps.voted_for = Some(self.id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.id.clone());
        self.leader_id = None;
        self.persist();

        let args = RequestVoteArgs {
            term: self.ps.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.ps.last_log_index(),
            last_log_term: self.ps.last_log_term(),
        };
        let targets = self.effective_membership.election_targets();
        for peer in targets.iter().filter(|id| *id != &self.id) {
            self.transport
                .send(peer, RaftMessage::RequestVote(args.clone()))
                .await;
        }
        if self
            .effective_membership
            .has_vote_quorum(&self.votes_received)
        {
            self.become_leader().await;
        }
    }

    fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // Membership rejection happens before term adoption. A stale removed
        // process therefore cannot poison a valid cluster merely by increasing
        // its local term and sending RequestVote.
        if !self.effective_membership.is_voter(&self.id)
            || !self.effective_membership.is_voter(&args.candidate_id)
            || self
                .ps
                .membership
                .as_ref()
                .is_some_and(|m| m.is_removed(&args.candidate_id))
        {
            return RequestVoteReply {
                term: self.ps.current_term,
                vote_granted: false,
            };
        }
        if args.term < self.ps.current_term {
            return RequestVoteReply {
                term: self.ps.current_term,
                vote_granted: false,
            };
        }
        if args.term > self.ps.current_term {
            self.become_follower(args.term);
        }
        let already_voted = self
            .ps
            .voted_for
            .as_ref()
            .map(|v| v != &args.candidate_id)
            .unwrap_or(false);
        let log_up_to_date = args.last_log_term > self.ps.last_log_term()
            || (args.last_log_term == self.ps.last_log_term()
                && args.last_log_index >= self.ps.last_log_index());
        let vote_granted = !already_voted && log_up_to_date;
        if vote_granted {
            self.ps.voted_for = Some(args.candidate_id);
            self.persist();
        }
        RequestVoteReply {
            term: self.ps.current_term,
            vote_granted,
        }
    }

    async fn on_request_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Candidate || !self.effective_membership.is_voter(&from) {
            return;
        }
        if reply.vote_granted {
            self.votes_received.insert(from);
            if self
                .effective_membership
                .has_vote_quorum(&self.votes_received)
            {
                self.become_leader().await;
            }
        }
    }

    async fn send_heartbeats(&self) {
        let mut targets = self.effective_membership.replication_targets();
        targets.remove(&self.id);
        for peer in targets {
            let next = self
                .leader
                .as_ref()
                .and_then(|l| l.next_index.get(&peer))
                .copied()
                .unwrap_or(1);

            if !self.snapshot_data.is_empty() && next <= self.ps.snapshot_index {
                let snap = InstallSnapshotArgs {
                    term: self.ps.current_term,
                    leader_id: self.id.clone(),
                    last_included_index: self.ps.snapshot_index,
                    last_included_term: self.ps.snapshot_term,
                    data: Arc::clone(&self.snapshot_data),
                    done: true,
                };
                self.transport
                    .send(&peer, RaftMessage::InstallSnapshot(snap))
                    .await;
                continue;
            }

            let prev_log_index = next.saturating_sub(1);
            let prev_log_term = self.ps.term_at(prev_log_index);
            let entries = self.ps.entries_from(next).to_vec();
            let args = AppendEntriesArgs {
                term: self.ps.current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };
            self.transport
                .send(&peer, RaftMessage::AppendEntries(args))
                .await;
        }
    }

    fn on_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if !self.effective_membership.is_voter(&args.leader_id)
            || self
                .ps
                .membership
                .as_ref()
                .is_some_and(|m| m.is_removed(&args.leader_id))
        {
            return AppendEntriesReply {
                term: self.ps.current_term,
                success: false,
                match_index: self.ps.last_log_index(),
            };
        }
        if args.term < self.ps.current_term {
            return AppendEntriesReply {
                term: self.ps.current_term,
                success: false,
                match_index: self.ps.last_log_index(),
            };
        }
        if args.term > self.ps.current_term || self.role == RaftRole::Candidate {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id.clone());

        if args.prev_log_index > 0 && self.ps.term_at(args.prev_log_index) != args.prev_log_term {
            return AppendEntriesReply {
                term: self.ps.current_term,
                success: false,
                match_index: self.ps.last_log_index(),
            };
        }

        self.successful_append_seen = true;
        if !args.entries.is_empty() {
            self.ps
                .truncate_and_append(args.prev_log_index, args.entries);
            self.recompute_effective_membership()
                .unwrap_or_else(|error| {
                    panic!("fatal replicated membership log validation failure: {error}")
                });
            self.persist();
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(self.ps.last_log_index());
        }

        AppendEntriesReply {
            term: self.ps.current_term,
            success: true,
            match_index: self.ps.last_log_index(),
        }
    }

    async fn on_append_entries_reply(&mut self, from: NodeId, reply: AppendEntriesReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Leader
            || !self
                .effective_membership
                .replication_targets()
                .contains(&from)
        {
            return;
        }
        if let Some(leader) = &mut self.leader {
            if reply.success {
                leader.match_index.insert(from.clone(), reply.match_index);
                leader
                    .next_index
                    .insert(from, reply.match_index.saturating_add(1));
                self.try_advance_commit();
            } else {
                let cur = leader.next_index.get(&from).copied().unwrap_or(1);
                let follower_next = reply.match_index.saturating_add(1);
                let retry = cur.saturating_sub(1).min(follower_next).max(1);
                leader.next_index.insert(from, retry);
            }
        }
    }

    fn try_advance_commit(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }
        let n = self.ps.last_log_index();
        for idx in (self.commit_index + 1..=n).rev() {
            if self.ps.term_at(idx) != self.ps.current_term {
                continue;
            }
            let matches = self
                .leader
                .as_ref()
                .map(|leader| &leader.match_index)
                .expect("leader state must exist while role is Leader");
            if self
                .effective_membership
                .has_match_quorum(&self.id, matches, idx)
            {
                self.commit_index = idx;
                break;
            }
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.fail_uncommitted_clients(
            "leadership lost before command reached required quorum commit",
        );
        self.ps.current_term = term;
        self.ps.voted_for = None;
        self.role = RaftRole::Follower;
        self.leader = None;
        self.votes_received.clear();
        self.persist();
    }

    async fn become_leader(&mut self) {
        if !self.effective_membership.is_voter(&self.id) {
            self.role = RaftRole::Follower;
            return;
        }
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.id.clone());
        if !self.recovery_readiness_pending {
            self.serving_ready.store(true, Ordering::Release);
        }

        // `commit_index` is volatile and restarts at the durable snapshot
        // boundary. A leader recovering persisted state always appends a
        // current-term no-op before becoming ready. An unresolved durable tail
        // requires the same barrier before Raft may safely advance the commit
        // index over prior-term entries (§5.4.2). Empty commands are intentional
        // Raft control entries; the replicated state machine advances only its
        // durable apply cursor for them.
        let recovery_barrier_needed =
            self.recovery_readiness_pending || self.commit_index < self.ps.last_log_index();
        let next = self.ps.last_log_index() + 1;
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let mut targets = self.effective_membership.replication_targets();
        targets.remove(&self.id);
        for peer in targets {
            next_index.insert(peer.clone(), next);
            match_index.insert(peer, 0);
        }
        self.leader = Some(LeaderState {
            next_index,
            match_index,
        });

        if recovery_barrier_needed {
            let barrier_index = self.ps.append(self.ps.current_term, Vec::new());
            debug_assert_eq!(barrier_index, next);
            self.persist();
        }

        // A leader elected while the committed state is joint must finish the
        // transition. If a FinalizeJoint entry already exists uncommitted, the
        // effective config is already stable and it is replicated as-is.
        if self.committed_membership().is_joint() && self.effective_membership.is_joint() {
            if let Err(error) = self.ensure_joint_finalize_entry() {
                panic!("fatal joint-membership recovery on leader election: {error}");
            }
        }
        // A single-node recovery barrier/finalizer has quorum immediately; in a
        // multi-node cluster follower replies will call this again as match
        // indexes advance.
        self.try_advance_commit();
        self.send_heartbeats().await;
    }

    fn validate_membership_request(&self, change: &MembershipChange) -> Result<(), String> {
        if self.membership_transition_active() {
            return Err("membership change already in progress".to_string());
        }
        let next_index = self.ps.last_log_index().saturating_add(1);
        match change {
            MembershipChange::AddNode(id) | MembershipChange::AddLearner(id) => self
                .effective_membership
                .add_learner(id.clone(), next_index)
                .map(|_| ()),
            MembershipChange::PromoteLearner(id) => {
                if !self.effective_membership.is_learner(id) {
                    return Err(format!("node id {id} is not a learner"));
                }
                let matched = self
                    .leader
                    .as_ref()
                    .and_then(|leader| leader.match_index.get(id))
                    .copied()
                    .unwrap_or(0);
                if matched < self.commit_index {
                    return Err(format!(
                        "learner {id} is not caught up: match_index={matched}, required_commit_index={}",
                        self.commit_index
                    ));
                }
                self.effective_membership
                    .begin_promotion(id, next_index)
                    .map(|_| ())
            }
            MembershipChange::RemoveNode(id) => {
                if id == &self.id && self.effective_membership.is_voter(id) {
                    return Err(
                        "cannot remove current leader; transfer leadership and prove the new leader first"
                            .to_string(),
                    );
                }
                self.effective_membership
                    .begin_removal(id, next_index)
                    .map(|_| ())
            }
            MembershipChange::FinalizeJoint => Err(
                "FinalizeJoint is an internal Raft transition and cannot be submitted directly"
                    .to_string(),
            ),
        }
    }

    fn handle_client_command(&mut self, payload: Vec<u8>) -> Result<u64, String> {
        if self.role != RaftRole::Leader {
            return Err(format!("not leader; redirect to {:?}", self.leader_id));
        }
        if self.transfer_in_progress.is_some() {
            return Err("leadership transfer in progress; retry later".to_string());
        }
        if payload.starts_with(COMPACT_LOG_TAG) {
            return self.handle_compact_log_cmd(payload);
        }
        if payload.starts_with(LEADER_TRANSFER_TAG) {
            return Err("use LeaderTransfer RPC, not client command".to_string());
        }
        if let Some(change) = Self::decode_membership_command(&payload)? {
            self.validate_membership_request(&change)?;
        }

        let idx = self.ps.append(self.ps.current_term, payload);
        self.recompute_effective_membership()?;
        self.persist();
        Ok(idx)
    }

    fn handle_compact_log_cmd(&mut self, payload: Vec<u8>) -> Result<u64, String> {
        if payload.len() < 2 + 8 {
            return Err("compact-log payload too short (need at least 10 bytes)".to_string());
        }
        let last_index = u64::from_be_bytes(
            payload[2..10]
                .try_into()
                .map_err(|_| "bad last_index bytes".to_string())?,
        );
        if self.membership_transition_active() {
            return Err("cannot compact Raft log during membership transition".to_string());
        }
        let membership = self
            .ps
            .membership
            .as_ref()
            .ok_or_else(|| "cannot compact before authoritative membership is known".to_string())?
            .clone();
        if membership.is_joint() {
            return Err("cannot compact a joint membership configuration".to_string());
        }

        if let Some(snapshot_store) = &self.snapshot_store {
            let safe_last = last_index.min(self.commit_index).min(self.last_applied);
            if safe_last <= self.ps.snapshot_index {
                return Ok(self.ps.snapshot_index);
            }
            if membership.config_index > safe_last {
                return Err(format!(
                    "snapshot boundary {safe_last} precedes committed membership index {}",
                    membership.config_index
                ));
            }
            let last_term = self.ps.term_at(safe_last);
            if last_term == 0 {
                return Err(format!(
                    "cannot create snapshot at Raft index {safe_last}: boundary term is unavailable"
                ));
            }

            let sql_data = snapshot_store
                .create_snapshot(safe_last, last_term)
                .map_err(|error| format!("state-machine snapshot creation failed: {error}"))?;
            snapshot_store
                .validate_snapshot(safe_last, last_term, &sql_data)
                .map_err(|error| {
                    format!("created state-machine snapshot failed validation: {error}")
                })?;
            let data = Arc::new(encode_snapshot_payload(&membership, &sql_data)?);

            let persistence = self.persistence.as_ref().ok_or_else(|| {
                "SQL-aware Raft compaction requires durable persistence".to_string()
            })?;
            let staged = StagedSnapshot {
                kind: StagedSnapshotKind::Creation,
                last_included_index: safe_last,
                last_included_term: last_term,
                data: Arc::clone(&data),
            };
            persistence
                .stage_snapshot(&staged)
                .map_err(|error| format!("stage state-machine snapshot: {error}"))?;

            self.ps.install_snapshot(safe_last, last_term);
            self.snapshot_data = data;
            self.recompute_effective_membership()?;
            self.persist();
            return Ok(safe_last);
        }

        if self.confirmed_apply_tx.is_some() {
            return Err(
                "Raft log compaction is disabled in replicated SQL mode until SQL state snapshots are implemented"
                    .to_string(),
            );
        }

        let raw_data = payload[10..].to_vec();
        let safe_last = last_index.min(self.commit_index);
        if safe_last == 0 {
            return Ok(0);
        }
        if safe_last <= self.ps.snapshot_index {
            return Ok(self.ps.snapshot_index);
        }
        if membership.config_index > safe_last {
            return Err(format!(
                "snapshot boundary {safe_last} precedes committed membership index {}",
                membership.config_index
            ));
        }
        let last_term = self.ps.term_at(safe_last);
        let data = encode_snapshot_payload(&membership, &raw_data)?;
        self.ps.install_snapshot(safe_last, last_term);
        self.snapshot_data = Arc::new(data);
        self.recompute_effective_membership()?;
        self.persist();
        Ok(safe_last)
    }

    /// Apply a membership log entry to the *committed* durable configuration.
    /// Returns true when this entry starts joint consensus and therefore needs a
    /// FinalizeJoint entry before the initiating admin request can succeed.
    fn apply_committed_membership(&mut self, entry: &LogEntry) -> Result<bool, String> {
        let Some(change) = Self::decode_membership_command(&entry.command)? else {
            return Ok(false);
        };
        if self
            .ps
            .membership
            .as_ref()
            .is_some_and(|membership| membership.config_index >= entry.index)
        {
            return Ok(matches!(
                change,
                MembershipChange::PromoteLearner(_) | MembershipChange::RemoveNode(_)
            ) && self.committed_membership().is_joint());
        }
        let base = self.committed_membership().clone();
        let next = Self::transition_membership(&base, &change, entry.index)?;
        let joint_started = !base.is_joint() && next.is_joint();
        self.ps.membership = Some(next);
        self.recompute_effective_membership()?;
        self.persist();
        if self
            .ps
            .membership
            .as_ref()
            .is_some_and(|membership| membership.is_removed(&self.id))
        {
            self.serving_ready.store(false, Ordering::Release);
        }
        Ok(joint_started)
    }

    fn ensure_joint_finalize_entry(&mut self) -> Result<u64, String> {
        if self.role != RaftRole::Leader {
            return Err("cannot finalize joint membership while not leader".to_string());
        }
        if !self.committed_membership().is_joint() {
            return Err("committed membership is not joint".to_string());
        }
        if !self.effective_membership.is_joint() {
            let committed_index = self.committed_membership().config_index;
            for entry in &self.ps.log {
                if entry.index <= committed_index {
                    continue;
                }
                if matches!(
                    Self::decode_membership_command(&entry.command)?,
                    Some(MembershipChange::FinalizeJoint)
                ) {
                    return Ok(entry.index);
                }
            }
            return Err(
                "effective membership is stable but FinalizeJoint entry is missing".to_string(),
            );
        }

        let idx = self.ps.append(
            self.ps.current_term,
            encode_membership_change(&MembershipChange::FinalizeJoint),
        );
        self.recompute_effective_membership()?;
        self.persist();
        Ok(idx)
    }

    fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if !self.effective_membership.is_voter(&args.leader_id)
            || self
                .ps
                .membership
                .as_ref()
                .is_some_and(|m| m.is_removed(&args.leader_id))
        {
            return InstallSnapshotReply {
                term: self.ps.current_term,
                success: false,
                last_included_index: args.last_included_index,
            };
        }
        if args.term < self.ps.current_term {
            return InstallSnapshotReply {
                term: self.ps.current_term,
                success: false,
                last_included_index: args.last_included_index,
            };
        }
        if args.term > self.ps.current_term || self.role == RaftRole::Candidate {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id.clone());

        if !args.done {
            return InstallSnapshotReply {
                term: self.ps.current_term,
                success: false,
                last_included_index: args.last_included_index,
            };
        }
        if args.last_included_index <= self.ps.snapshot_index {
            return InstallSnapshotReply {
                term: self.ps.current_term,
                success: true,
                last_included_index: args.last_included_index,
            };
        }

        let (incoming_membership, sql_bytes) = match decode_snapshot_payload(&args.data) {
            Ok(Some((membership, sql_bytes))) => {
                if self
                    .validate_incoming_snapshot_membership(&membership, args.last_included_index)
                    .is_err()
                {
                    return InstallSnapshotReply {
                        term: self.ps.current_term,
                        success: false,
                        last_included_index: args.last_included_index,
                    };
                }
                (Some(membership), sql_bytes)
            }
            Ok(None) => {
                if self
                    .ps
                    .membership
                    .as_ref()
                    .is_some_and(|m| m.config_index > 0)
                {
                    return InstallSnapshotReply {
                        term: self.ps.current_term,
                        success: false,
                        last_included_index: args.last_included_index,
                    };
                }
                (None, args.data.as_slice())
            }
            Err(_) => {
                return InstallSnapshotReply {
                    term: self.ps.current_term,
                    success: false,
                    last_included_index: args.last_included_index,
                }
            }
        };

        if let Some(snapshot_store) = &self.snapshot_store {
            if snapshot_store
                .validate_snapshot(args.last_included_index, args.last_included_term, sql_bytes)
                .is_err()
            {
                return InstallSnapshotReply {
                    term: self.ps.current_term,
                    success: false,
                    last_included_index: args.last_included_index,
                };
            }

            let Some(persistence) = &self.persistence else {
                return InstallSnapshotReply {
                    term: self.ps.current_term,
                    success: false,
                    last_included_index: args.last_included_index,
                };
            };
            let staged = StagedSnapshot {
                kind: StagedSnapshotKind::Installation,
                last_included_index: args.last_included_index,
                last_included_term: args.last_included_term,
                data: Arc::clone(&args.data),
            };
            if let Err(error) = persistence.stage_snapshot(&staged) {
                panic!("fatal Raft snapshot staging failure: {error}");
            }

            if snapshot_store
                .restore_snapshot(args.last_included_index, args.last_included_term, sql_bytes)
                .is_err()
            {
                if let Err(error) = persistence.clear_staged_snapshot() {
                    panic!("fatal staged Raft snapshot clear failure: {error}");
                }
                return InstallSnapshotReply {
                    term: self.ps.current_term,
                    success: false,
                    last_included_index: args.last_included_index,
                };
            }
        } else if self.confirmed_apply_tx.is_some() {
            panic!(
                "fatal replicated SQL snapshot install: SQL state snapshot restore is not implemented"
            );
        }

        if let Some(membership) = incoming_membership {
            self.ps.membership = Some(membership);
        }
        self.ps
            .install_snapshot(args.last_included_index, args.last_included_term);
        self.snapshot_data = args.data;
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = self.last_applied.max(args.last_included_index);
        self.recompute_effective_membership()
            .unwrap_or_else(|error| {
                panic!("fatal effective membership after InstallSnapshot: {error}")
            });
        self.persist();

        InstallSnapshotReply {
            term: self.ps.current_term,
            success: true,
            last_included_index: self.ps.snapshot_index,
        }
    }

    async fn on_install_snapshot_reply(&mut self, from: NodeId, reply: InstallSnapshotReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Leader || !reply.success {
            return;
        }
        if reply.last_included_index > self.ps.snapshot_index {
            return;
        }
        if !self
            .effective_membership
            .replication_targets()
            .contains(&from)
        {
            return;
        }
        if let Some(leader) = &mut self.leader {
            let acknowledged = reply.last_included_index;
            let match_index = leader.match_index.entry(from.clone()).or_insert(0);
            *match_index = (*match_index).max(acknowledged);
            let next_index = leader.next_index.entry(from).or_insert(1);
            *next_index = (*next_index).max(acknowledged.saturating_add(1));
        }
        self.try_advance_commit();
    }

    fn choose_transfer_target(&self) -> Result<NodeId, String> {
        if self.role != RaftRole::Leader {
            return Err("not leader".to_string());
        }
        if self.membership_transition_active() {
            return Err(
                "membership transition in progress; cannot transfer leadership".to_string(),
            );
        }
        let last = self.ps.last_log_index();
        let leader = self
            .leader
            .as_ref()
            .ok_or_else(|| "leader replication state unavailable".to_string())?;
        self.effective_membership
            .election_targets()
            .into_iter()
            .filter(|id| id != &self.id)
            .find(|id| leader.match_index.get(id).copied().unwrap_or(0) >= last)
            .ok_or_else(|| {
                "no eligible up-to-date voter available for leadership transfer".to_string()
            })
    }

    fn initiate_leader_transfer(&mut self, target: NodeId) -> Result<(), String> {
        if self.role != RaftRole::Leader {
            return Err("not leader".to_string());
        }
        if self.membership_transition_active() {
            return Err(
                "membership transition in progress; cannot transfer leadership".to_string(),
            );
        }
        if !self.effective_membership.is_stable_voter(&target) || target == self.id {
            return Err(format!("target {target} is not an eligible stable voter"));
        }
        let matched = self
            .leader
            .as_ref()
            .and_then(|leader| leader.match_index.get(&target))
            .copied()
            .unwrap_or(0);
        if matched < self.ps.last_log_index() {
            return Err(format!(
                "target {target} is not caught up: match_index={matched}, last_log_index={}",
                self.ps.last_log_index()
            ));
        }
        if self.transfer_in_progress.is_some() {
            return Err("transfer already in progress".to_string());
        }
        let deadline = Instant::now() + Duration::from_millis(LEADER_TRANSFER_TIMEOUT_MS);
        self.transfer_in_progress = Some((target.clone(), deadline));
        // Fire-and-forget is performed by the async caller; this helper only
        // validates and records the transfer state.
        Ok(())
    }

    async fn on_leader_transfer(&mut self, from: &NodeId, target: NodeId) {
        let result = self.initiate_leader_transfer(target.clone());
        match result {
            Ok(()) => {
                self.transport
                    .send(
                        &target,
                        RaftMessage::TimeoutNow {
                            term: self.ps.current_term,
                        },
                    )
                    .await;
                self.transport
                    .send(
                        from,
                        RaftMessage::LeaderTransferReply {
                            success: true,
                            error: None,
                        },
                    )
                    .await;
            }
            Err(error) => {
                self.transport
                    .send(
                        from,
                        RaftMessage::LeaderTransferReply {
                            success: false,
                            error: Some(error),
                        },
                    )
                    .await;
            }
        }
    }

    async fn on_timeout_now(&mut self, from: &NodeId, term: u64, election_deadline: &mut Instant) {
        if self.role == RaftRole::Leader
            || term < self.ps.current_term
            || !self.effective_membership.is_voter(&self.id)
            || !self.effective_membership.is_voter(from)
        {
            return;
        }
        self.start_election().await;
        *election_deadline = Instant::now() + self.election_timeout();
    }
}

#[derive(Debug, Clone)]
pub struct RaftShared {
    pub role: RaftRole,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub membership: ClusterMembership,
}
