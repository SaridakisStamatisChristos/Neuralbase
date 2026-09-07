// SPDX-License-Identifier: Apache-2.0
// Raft consensus state machine — Session 13: log compaction + membership changes + leader transfer.
//
// Implements the Raft algorithm (Ongaro & Ousterhout, 2014) as a tokio task.
// Session 13 additions:
//   - InstallSnapshot RPC (Raft §7): leader sends snapshot to laggard followers.
//   - Node restart recovery: load persistent state on startup via RaftPersistenceStore.
//   - Membership changes: single-step AddNode/RemoveNode via tagged ClientCommand.
//   - WAL simulation: persist() called before RPC replies on every state mutation.
//   - LeaderTransfer RPC (Raft §3.10): graceful leadership handoff.
//   - Bounded apply_tx channel: backpressure prevents unbounded memory growth.
//
// Replicated-SQL additions:
//   - Regular ClientCommand replies are deferred until quorum commit.
//   - A confirmed state-machine channel can defer success until durable apply.
//   - Single-node regular commands commit immediately (majority of one).
//   - Pending uncommitted clients fail if leadership is lost.
//   - Required stable-storage load/save failures fail-stop the Raft node.
//   - SQL-aware snapshots are created only at an applied/committed boundary,
//     durably staged before log truncation, and restored before install ACK.
//   - Interrupted follower snapshot installs are resumed from staged metadata on
//     restart, closing the SQL-restore/Raft-publication crash window.
//
// CONFIDENCE: raw=0.76 effective=0.68
// DEPENDS_ON: log, rpc, snapshot, transport
// RISK: Single-step membership changes are unsafe under certain network
//       partitions (see Raft §6 for joint-consensus alternative).
//       InstallSnapshot invariants MUST be human-reviewed before confidence cap
//       is lifted — see REVIEW_REQUIRED.md §Session13.
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Session13

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::consensus::log::{
    PersistentState, RaftPersistenceStore, StagedSnapshot, StagedSnapshotKind,
};
use crate::consensus::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
    MembershipChange, NodeId, RaftMessage, RequestVoteArgs, RequestVoteReply,
};
use crate::consensus::snapshot::StateMachineSnapshotStore;
use crate::consensus::transport::Transport;
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Instant};

// ── Admin payload tags ─────────────────────────────────────────────────────

/// First two bytes of a `ClientCommand::payload` that identify a membership-
/// change request rather than a regular data command.
pub const MEMBERSHIP_CHANGE_TAG: &[u8] = &[0xFC, 0xFD];

/// First two bytes of a `ClientCommand::payload` that trigger log compaction
/// at a given index.  Format: tag(2) + last_index(8 BE) + snapshot_data(*).
pub const COMPACT_LOG_TAG: &[u8] = &[0xFE, 0xFD];

/// First two bytes of a `ClientCommand::payload` that trigger a leadership
/// transfer to a specific follower.  Format: tag(2) + target_id_json(*).
pub const LEADER_TRANSFER_TAG: &[u8] = &[0xFA, 0xFD];

// ── Admin payload helpers (public for tests) ───────────────────────────────

/// Encode a membership-change request as a `ClientCommand::payload`.
pub fn encode_membership_change(change: &MembershipChange) -> Vec<u8> {
    let mut v = MEMBERSHIP_CHANGE_TAG.to_vec();
    v.extend_from_slice(
        &serde_json::to_vec(change).expect("MembershipChange must be JSON-serializable"),
    );
    v
}

/// Encode a compact-log (snapshot trigger) request as a `ClientCommand::payload`.
/// `last_index`: highest Raft index to include in the snapshot.
/// `data`:       opaque state-machine bytes for legacy/non-SQL callers. When a
///               `StateMachineSnapshotStore` is attached, Raft ignores these
///               bytes and asks the state machine to create the exact snapshot.
pub fn encode_compact_log(last_index: u64, data: &[u8]) -> Vec<u8> {
    let mut v = COMPACT_LOG_TAG.to_vec();
    v.extend_from_slice(&last_index.to_be_bytes());
    v.extend_from_slice(data);
    v
}

/// Encode a leader-transfer request as a `ClientCommand::payload`.
pub fn encode_leader_transfer(target: &str) -> Vec<u8> {
    let mut v = LEADER_TRANSFER_TAG.to_vec();
    v.extend_from_slice(target.as_bytes());
    v
}

// ── Constants ──────────────────────────────────────────────────────────────

const HEARTBEAT_MS: u64 = 50;
/// Default election timeout base (ms). Final timeout = base + rand(0..base).
const ELECTION_TIMEOUT_BASE_MS: u64 = 150;

/// Maximum capacity for the bounded apply channel.
pub const APPLY_CHANNEL_CAPACITY: usize = 1024;

/// Leadership transfer timeout (ms).
const LEADER_TRANSFER_TIMEOUT_MS: u64 = 5_000;

// ── RaftRole ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

// ── LeaderState ────────────────────────────────────────────────────────────

/// Volatile leader state.  Present only when role == Leader.
struct LeaderState {
    /// For each peer: index of the next log entry to send.
    next_index: HashMap<NodeId, u64>,
    /// For each peer: highest log entry known to be replicated.
    match_index: HashMap<NodeId, u64>,
}

// ── ClientCommand / committed state-machine handoff ────────────────────────

/// A client command submitted to the leader for replication.
///
/// For regular data commands, `reply` is resolved only after the entry is
/// quorum-committed and reaches the configured state-machine apply point. The
/// historical Session-13 membership/compaction admin commands retain their
/// legacy acknowledgement behavior until coordinated membership work is done.
pub struct ClientCommand {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<u64, String>>,
}

/// A committed log entry requiring state-machine application.
///
/// A consumer attached through `with_confirmed_apply_tx` MUST resolve
/// `completion` after its durable state-machine apply succeeds or fails. Raft
/// does not advance `last_applied`, and regular ClientCommand success is not
/// reported, until this acknowledgement arrives.
pub struct CommittedEntry {
    pub entry: LogEntry,
    pub completion: oneshot::Sender<Result<(), String>>,
}

/// Handle for a spawned Raft event-loop task.
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

    /// Request the Raft node to transfer leadership to any healthy peer.
    /// Returns `Ok(new_leader_id)` on success, `Err(reason)` on failure/timeout.
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

// ── RaftNode ───────────────────────────────────────────────────────────────

/// A single Raft cluster member.
///
/// Call `RaftNode::spawn` to start the event loop in a tokio task; then
/// communicate via `client_tx` and observe `role()` / `leader_id()`.
pub struct RaftNode<T: Transport> {
    id: NodeId,
    peers: Vec<NodeId>,
    transport: Arc<T>,
    ps: PersistentState,

    // Volatile state (all nodes).
    commit_index: u64,
    last_applied: u64,
    role: RaftRole,
    leader_id: Option<NodeId>,

    // Candidate state.
    votes_received: usize,

    // Leader state.
    leader: Option<LeaderState>,

    // Snapshot state (Session 13 / replicated SQL Phase 2).
    snapshot_data: Arc<Vec<u8>>,
    snapshot_store: Option<Arc<dyn StateMachineSnapshotStore>>,
    pending_staged_snapshot: Option<StagedSnapshot>,

    // Membership-change state (Session 13).
    membership_change_in_progress: bool,

    // Leadership transfer state (Session 13 — Raft §3.10).
    transfer_in_progress: Option<(NodeId, Instant)>,

    // Persistence (Session 13).
    persistence: Option<Arc<dyn RaftPersistenceStore>>,

    // Election timeout; overrideable for tests.
    election_timeout_base_ms: u64,

    // Legacy apply handoff retained for existing callers/tests.
    apply_tx: Option<mpsc::Sender<LogEntry>>,

    // Confirmed apply handoff used by replicated SQL.
    confirmed_apply_tx: Option<mpsc::Sender<CommittedEntry>>,

    // Regular client replies keyed by appended log index.
    pending_clients: HashMap<u64, oneshot::Sender<Result<u64, String>>>,
}

impl<T: Transport> RaftNode<T> {
    /// Create a node. Call `spawn` to start it.
    pub fn new(id: NodeId, peers: Vec<NodeId>, transport: Arc<T>) -> Self {
        Self {
            id,
            peers,
            transport,
            ps: PersistentState::new(),
            commit_index: 0,
            last_applied: 0,
            role: RaftRole::Follower,
            leader_id: None,
            votes_received: 0,
            leader: None,
            snapshot_data: Arc::new(vec![]),
            snapshot_store: None,
            pending_staged_snapshot: None,
            membership_change_in_progress: false,
            transfer_in_progress: None,
            persistence: None,
            election_timeout_base_ms: ELECTION_TIMEOUT_BASE_MS,
            apply_tx: None,
            confirmed_apply_tx: None,
            pending_clients: HashMap::new(),
        }
    }

    pub fn with_apply_tx(mut self, tx: mpsc::Sender<LogEntry>) -> Self {
        self.apply_tx = Some(tx);
        self.confirmed_apply_tx = None;
        self
    }

    /// Attach a complete state-machine snapshot creator/restorer.
    pub fn with_snapshot_store(mut self, store: Arc<dyn StateMachineSnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        let recovered_install = self.recover_staged_snapshot_if_possible();
        if !recovered_install {
            self.restore_active_snapshot_if_possible();
        }
        self
    }

    /// Attach a bounded state-machine channel with explicit completion.
    pub fn with_confirmed_apply_tx(mut self, tx: mpsc::Sender<CommittedEntry>) -> Self {
        let has_unrecoverable_snapshot =
            (self.ps.snapshot_index != 0 || !self.snapshot_data.is_empty())
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

    /// Attach a persistence store and recover any active/staged snapshot state.
    pub fn with_persistence(mut self, store: Arc<dyn RaftPersistenceStore>) -> Self {
        let loaded = match store.load() {
            Ok(state) => state,
            Err(error) => panic!("fatal Raft persistence load failure: {error}"),
        };
        let staged = match store.load_staged_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => panic!("fatal staged Raft snapshot load failure: {error}"),
        };

        if let Some((ps, snap)) = loaded {
            let has_boundary = ps.snapshot_index != 0;
            let has_bytes = !snap.is_empty();
            if has_boundary != has_bytes {
                panic!(
                    "fatal Raft snapshot recovery: snapshot boundary metadata and active snapshot bytes are inconsistent"
                );
            }
            self.snapshot_data = Arc::new(snap);
            let snap_idx = ps.snapshot_index;
            self.ps = ps;
            self.commit_index = snap_idx;
            self.last_applied = snap_idx;
        }

        self.persistence = Some(store);
        self.pending_staged_snapshot = staged;
        let recovered_install = self.recover_staged_snapshot_if_possible();
        if !recovered_install {
            self.restore_active_snapshot_if_possible();
        }
        self
    }

    /// Restore the currently active snapshot if both active state and a state-
    /// machine snapshot implementation are available.
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
        if let Err(error) = snapshot_store.restore_snapshot(
            self.ps.snapshot_index,
            self.ps.snapshot_term,
            &self.snapshot_data,
        ) {
            panic!("fatal state-machine snapshot recovery failure: {error}");
        }
    }

    /// Resolve a crash-left staged snapshot transition when enough components
    /// are attached. Returns true only when an interrupted installation was
    /// promoted to the active snapshot, in which case active restore is complete.
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

                if let Err(error) = snapshot_store.validate_snapshot(
                    staged.last_included_index,
                    staged.last_included_term,
                    staged.data.as_slice(),
                ) {
                    panic!("fatal staged state-machine snapshot validation failure: {error}");
                }
                if let Err(error) = snapshot_store.restore_snapshot(
                    staged.last_included_index,
                    staged.last_included_term,
                    staged.data.as_slice(),
                ) {
                    panic!("fatal staged state-machine snapshot recovery failure: {error}");
                }

                self.ps.install_snapshot(
                    staged.last_included_index,
                    staged.last_included_term,
                );
                self.snapshot_data = Arc::clone(&staged.data);
                self.commit_index = self.commit_index.max(staged.last_included_index);
                self.last_applied = self.last_applied.max(staged.last_included_index);
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
        }
    }

    fn fail_all_clients(&mut self, reason: &str) {
        let pending = std::mem::take(&mut self.pending_clients);
        for (_, reply) in pending {
            let _ = reply.send(Err(reason.to_string()));
        }
    }

    /// Spawn the Raft event loop.
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
                            election_deadline = Instant::now() + self.election_timeout();
                        } else {
                            election_deadline = Instant::now() + self.election_timeout();
                        }
                    }
                    Some(cmd) = cmd_rx.recv() => {
                        let legacy_admin_ack = cmd.payload.starts_with(MEMBERSHIP_CHANGE_TAG)
                            || cmd.payload.starts_with(COMPACT_LOG_TAG);
                        match self.handle_client_command(cmd.payload) {
                            Ok(index) if legacy_admin_ack => {
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
                        let result = if self.role != RaftRole::Leader {
                            Err("not leader".to_string())
                        } else if self.peers.is_empty() {
                            Err("no peers available".to_string())
                        } else {
                            let target = self.peers[0].clone();
                            self.on_leader_transfer(&self.id.clone(), target.clone()).await;
                            Ok(target)
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

                    if entry.command.starts_with(MEMBERSHIP_CHANGE_TAG) {
                        let payload = &entry.command[MEMBERSHIP_CHANGE_TAG.len()..];
                        if let Ok(change) = serde_json::from_slice::<MembershipChange>(payload) {
                            self.apply_membership_change(change);
                        }
                    }

                    if let Some(tx) = &self.confirmed_apply_tx {
                        let (completion_tx, completion_rx) = oneshot::channel();
                        let send_result = tokio::select! {
                            biased;
                            _ = &mut shutdown_rx => {
                                self.fail_all_clients("raft node shutting down during state-machine apply");
                                return;
                            },
                            result = tx.send(CommittedEntry {
                                entry,
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
                            result = tx.send(entry) => result,
                        };
                        if send_result.is_err() {
                            self.fail_all_clients("legacy state-machine apply channel closed");
                            return;
                        }
                    }

                    self.last_applied = next_index;
                    if let Some(reply) = self.pending_clients.remove(&next_index) {
                        let _ = reply.send(Ok(next_index));
                    }
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
                self.on_request_vote_reply(reply).await;
            }
            RaftMessage::AppendEntries(args) => {
                let reset = args.term >= self.ps.current_term;
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
                let reset = args.term >= self.ps.current_term;
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
                let success = if self.role == RaftRole::Leader {
                    let payload = encode_membership_change(&change);
                    self.handle_client_command(payload).is_ok()
                } else {
                    false
                };
                self.transport
                    .send(
                        &from,
                        RaftMessage::MembershipChangeCmdReply {
                            success,
                            error: if success {
                                None
                            } else {
                                Some("not leader or change in progress".to_string())
                            },
                        },
                    )
                    .await;
            }
            RaftMessage::MembershipChangeCmdReply {
                success: _,
                error: _,
            } => {}
            RaftMessage::LeaderTransfer { target } => {
                self.on_leader_transfer(&from, target).await;
            }
            RaftMessage::LeaderTransferReply {
                success: _,
                error: _,
            } => {}
            RaftMessage::TimeoutNow { term } => {
                self.on_timeout_now(term, &mut election_deadline).await;
            }
        }
        election_deadline
    }

    async fn start_election(&mut self) {
        self.role = RaftRole::Candidate;
        self.ps.current_term += 1;
        self.ps.voted_for = Some(self.id.clone());
        self.votes_received = 1;
        self.leader_id = None;
        self.persist();

        let args = RequestVoteArgs {
            term: self.ps.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.ps.last_log_index(),
            last_log_term: self.ps.last_log_term(),
        };
        for peer in &self.peers.clone() {
            self.transport
                .send(peer, RaftMessage::RequestVote(args.clone()))
                .await;
        }
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        if self.votes_received >= majority {
            self.become_leader().await;
        }
    }

    fn on_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
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

    async fn on_request_vote_reply(&mut self, reply: RequestVoteReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Candidate {
            return;
        }
        if reply.vote_granted {
            self.votes_received += 1;
            let total_nodes = self.peers.len() + 1;
            let majority = total_nodes / 2 + 1;
            if self.votes_received >= majority {
                self.become_leader().await;
            }
        }
    }

    async fn send_heartbeats(&self) {
        for peer in &self.peers {
            let next = self
                .leader
                .as_ref()
                .and_then(|l| l.next_index.get(peer))
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
                    .send(peer, RaftMessage::InstallSnapshot(snap))
                    .await;
                continue;
            }

            let prev_log_index = next - 1;
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
                .send(peer, RaftMessage::AppendEntries(args))
                .await;
        }
    }

    fn on_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
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

        if !args.entries.is_empty() {
            self.ps
                .truncate_and_append(args.prev_log_index, args.entries);
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
        if self.role != RaftRole::Leader {
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
                leader.next_index.insert(from, cur.saturating_sub(1).max(1));
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
            let replicated = self
                .leader
                .as_ref()
                .map(|l| l.match_index.values().filter(|&&m| m >= idx).count())
                .unwrap_or(0)
                + 1;
            let total_nodes = self.peers.len() + 1;
            let majority = total_nodes / 2 + 1;
            if replicated >= majority {
                self.commit_index = idx;
                break;
            }
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.fail_uncommitted_clients("leadership lost before command reached quorum commit");
        self.ps.current_term = term;
        self.ps.voted_for = None;
        self.role = RaftRole::Follower;
        self.leader = None;
        self.persist();
    }

    async fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.id.clone());
        let next = self.ps.last_log_index() + 1;
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer in &self.peers {
            next_index.insert(peer.clone(), next);
            match_index.insert(peer.clone(), 0);
        }
        self.leader = Some(LeaderState {
            next_index,
            match_index,
        });
        self.send_heartbeats().await;
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
        if payload.starts_with(MEMBERSHIP_CHANGE_TAG) {
            if self.membership_change_in_progress {
                return Err("membership change already in progress".to_string());
            }
            self.membership_change_in_progress = true;
        } else if self.membership_change_in_progress {
            return Err("membership change in progress; retry later".to_string());
        }

        let idx = self.ps.append(self.ps.current_term, payload);
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

        if let Some(snapshot_store) = &self.snapshot_store {
            let safe_last = last_index.min(self.commit_index).min(self.last_applied);
            if safe_last <= self.ps.snapshot_index {
                return Ok(self.ps.snapshot_index);
            }
            let last_term = self.ps.term_at(safe_last);
            if last_term == 0 {
                return Err(format!(
                    "cannot create snapshot at Raft index {safe_last}: boundary term is unavailable"
                ));
            }

            let data = Arc::new(
                snapshot_store
                    .create_snapshot(safe_last, last_term)
                    .map_err(|error| format!("state-machine snapshot creation failed: {error}"))?,
            );
            snapshot_store
                .validate_snapshot(safe_last, last_term, data.as_slice())
                .map_err(|error| format!("created state-machine snapshot failed validation: {error}"))?;

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
            self.persist();
            return Ok(safe_last);
        }

        if self.confirmed_apply_tx.is_some() {
            return Err(
                "Raft log compaction is disabled in replicated SQL mode until SQL state snapshots are implemented"
                    .to_string(),
            );
        }

        let data = payload[10..].to_vec();
        let safe_last = last_index.min(self.commit_index);
        if safe_last == 0 {
            return Ok(0);
        }
        if safe_last <= self.ps.snapshot_index {
            return Ok(self.ps.snapshot_index);
        }

        let last_term = self.ps.term_at(safe_last);
        self.ps.install_snapshot(safe_last, last_term);
        self.snapshot_data = Arc::new(data);
        self.persist();
        Ok(safe_last)
    }

    fn apply_membership_change(&mut self, change: MembershipChange) {
        match change {
            MembershipChange::AddNode(ref new_id) => {
                if !self.peers.contains(new_id) && new_id != &self.id {
                    self.peers.push(new_id.clone());
                    if let Some(leader) = &mut self.leader {
                        leader.next_index.entry(new_id.clone()).or_insert(1);
                        leader.match_index.entry(new_id.clone()).or_insert(0);
                    }
                }
            }
            MembershipChange::RemoveNode(ref gone_id) => {
                self.peers.retain(|p| p != gone_id);
                if let Some(leader) = &mut self.leader {
                    leader.next_index.remove(gone_id);
                    leader.match_index.remove(gone_id);
                }
            }
        }
        self.membership_change_in_progress = false;
    }

    fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
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

        if let Some(snapshot_store) = &self.snapshot_store {
            if snapshot_store
                .validate_snapshot(
                    args.last_included_index,
                    args.last_included_term,
                    args.data.as_slice(),
                )
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
                .restore_snapshot(
                    args.last_included_index,
                    args.last_included_term,
                    args.data.as_slice(),
                )
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

        self.ps
            .install_snapshot(args.last_included_index, args.last_included_term);
        self.snapshot_data = args.data;
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = self.last_applied.max(args.last_included_index);
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
        if let Some(leader) = &mut self.leader {
            let acknowledged = reply.last_included_index;
            let match_index = leader.match_index.entry(from.clone()).or_insert(0);
            *match_index = (*match_index).max(acknowledged);
            let next_index = leader.next_index.entry(from).or_insert(1);
            *next_index = (*next_index).max(acknowledged.saturating_add(1));
        }
    }

    async fn on_leader_transfer(&mut self, from: &NodeId, target: NodeId) {
        if self.role != RaftRole::Leader {
            self.transport
                .send(
                    from,
                    RaftMessage::LeaderTransferReply {
                        success: false,
                        error: Some("not leader".to_string()),
                    },
                )
                .await;
            return;
        }
        if !self.peers.contains(&target) {
            self.transport
                .send(
                    from,
                    RaftMessage::LeaderTransferReply {
                        success: false,
                        error: Some(format!("unknown target node: {target}")),
                    },
                )
                .await;
            return;
        }
        if self.transfer_in_progress.is_some() {
            self.transport
                .send(
                    from,
                    RaftMessage::LeaderTransferReply {
                        success: false,
                        error: Some("transfer already in progress".to_string()),
                    },
                )
                .await;
            return;
        }

        let deadline = Instant::now() + Duration::from_millis(LEADER_TRANSFER_TIMEOUT_MS);
        self.transfer_in_progress = Some((target.clone(), deadline));

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

    async fn on_timeout_now(&mut self, term: u64, election_deadline: &mut Instant) {
        if self.role == RaftRole::Leader {
            return;
        }
        if term < self.ps.current_term {
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
}
