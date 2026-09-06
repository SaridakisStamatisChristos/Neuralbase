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
//
// CONFIDENCE: raw=0.76 effective=0.68
// DEPENDS_ON: log, rpc, transport
// RISK: Single-step membership changes are unsafe under certain network
//       partitions (see Raft §6 for joint-consensus alternative).
//       InstallSnapshot invariants MUST be human-reviewed before confidence cap
//       is lifted — see REVIEW_REQUIRED.md §Session13.
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Session13

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::consensus::log::{PersistentState, RaftPersistenceStore};
use crate::consensus::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
    MembershipChange, NodeId, RaftMessage, RequestVoteArgs, RequestVoteReply,
};
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
/// `data`:       opaque state-machine bytes that the snapshot represents.
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

    // Snapshot state (Session 13).
    /// Opaque snapshot bytes held by the leader for forwarding to laggard
    /// followers via InstallSnapshot.  Empty = no snapshot held.
    snapshot_data: Arc<Vec<u8>>,

    // Membership-change state (Session 13).
    /// True while a single-step membership change is being committed.
    /// Client commands are rejected while this flag is set.
    membership_change_in_progress: bool,

    // Leadership transfer state (Session 13 — Raft §3.10).
    transfer_in_progress: Option<(NodeId, Instant)>,

    // Persistence (Session 13).
    persistence: Option<Arc<dyn RaftPersistenceStore>>,

    // Election timeout; overrideable for tests.
    election_timeout_base_ms: u64,

    // Legacy apply handoff retained for existing callers/tests. Delivery to
    // this channel is treated as the apply point because it has no completion
    // protocol. Replicated SQL uses confirmed_apply_tx instead.
    apply_tx: Option<mpsc::Sender<LogEntry>>,

    // Confirmed apply handoff used by replicated SQL. The Raft loop waits for
    // the consumer's completion acknowledgement before advancing last_applied.
    confirmed_apply_tx: Option<mpsc::Sender<CommittedEntry>>,

    // Regular client replies keyed by appended log index. They remain pending
    // through local append and quorum replication, and resolve only on apply.
    pending_clients: HashMap<u64, oneshot::Sender<Result<u64, String>>>,
}

impl<T: Transport> RaftNode<T> {
    /// Create a node.  Call `spawn` to start it.
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
            membership_change_in_progress: false,
            transfer_in_progress: None,
            persistence: None,
            election_timeout_base_ms: ELECTION_TIMEOUT_BASE_MS,
            apply_tx: None,
            confirmed_apply_tx: None,
            pending_clients: HashMap::new(),
        }
    }

    /// Attach the legacy bounded apply channel. Committed `LogEntry` values are
    /// sent in order. Delivery is the apply acknowledgement point for this mode.
    pub fn with_apply_tx(mut self, tx: mpsc::Sender<LogEntry>) -> Self {
        self.apply_tx = Some(tx);
        self.confirmed_apply_tx = None;
        self
    }

    /// Attach a bounded state-machine channel with explicit completion.
    ///
    /// This is the required mode for replicated SQL: a committed entry is sent
    /// to the consumer and ClientCommand success waits until `completion`
    /// reports that the durable state-machine apply finished successfully.
    pub fn with_confirmed_apply_tx(mut self, tx: mpsc::Sender<CommittedEntry>) -> Self {
        self.confirmed_apply_tx = Some(tx);
        self.apply_tx = None;
        self
    }

    /// Attach a persistence store.  If a previously-saved state exists it is
    /// loaded immediately (restoring term, votedFor, log, snapshot).
    ///
    /// MUST be called before `spawn`. Calling after spawn has no effect.
    pub fn with_persistence(mut self, store: Arc<dyn RaftPersistenceStore>) -> Self {
        if let Ok(Some((ps, snap))) = store.load() {
            self.snapshot_data = Arc::new(snap);
            let snap_idx = ps.snapshot_index;
            self.ps = ps;
            self.commit_index = snap_idx;
            self.last_applied = snap_idx;
        }
        self.persistence = Some(store);
        self
    }

    /// Override the election timeout (ms) — used by tests for speed.
    pub fn set_election_timeout_ms(&mut self, ms: u64) {
        self.election_timeout_base_ms = ms;
    }

    fn election_timeout(&self) -> Duration {
        let base = self.election_timeout_base_ms;
        let jitter_range = base.max(10);
        let extra = rand::thread_rng().gen_range(0..jitter_range);
        Duration::from_millis(base + extra)
    }

    // ── Persistence helper ─────────────────────────────────────────────────

    /// Flush current persistent state to stable storage.
    ///
    /// Per Raft safety: called BEFORE the node responds to any RPC that
    /// depends on the state just mutated (term, vote, log, snapshot).
    fn persist(&self) {
        if let Some(store) = &self.persistence {
            // Session-13 behavior retained in this commit; a following focused
            // change converts persistence failures to fail-stop semantics.
            if let Err(e) = store.save(&self.ps, &self.snapshot_data) {
                eprintln!("[raft] persist error: {e}");
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

    // ── Spawn ──────────────────────────────────────────────────────────────

    /// Spawn the Raft event loop.  Returns a handle to submit client commands.
    pub fn spawn(
        mut self,
    ) -> (
        mpsc::Sender<ClientCommand>,
        Arc<Mutex<RaftShared>>,
        RaftTaskHandle,
    ) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ClientCommand>(64);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let (transfer_tx, mut transfer_rx) =
            mpsc::channel::<oneshot::Sender<Result<String, String>>>(1);
        let shared = Arc::new(Mutex::new(RaftShared {
            role: RaftRole::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
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

                    // Incoming RPC.
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

                    // Heartbeat tick (only meaningful when leader).
                    _ = hb_interval.tick() => {
                        if self.role == RaftRole::Leader {
                            self.send_heartbeats().await;
                        }
                    }

                    // Election timeout.
                    _ = time::sleep_until(election_deadline) => {
                        if self.role != RaftRole::Leader {
                            self.start_election().await;
                            election_deadline = Instant::now() + self.election_timeout();
                        } else {
                            election_deadline = Instant::now() + self.election_timeout();
                        }
                    }

                    // Client command.
                    Some(cmd) = cmd_rx.recv() => {
                        // Session-13 membership and compaction admin commands keep
                        // their historical immediate acknowledgement semantics.
                        // They are intentionally outside replicated SQL guarantees.
                        let legacy_admin_ack = cmd.payload.starts_with(MEMBERSHIP_CHANGE_TAG)
                            || cmd.payload.starts_with(COMPACT_LOG_TAG);
                        match self.handle_client_command(cmd.payload) {
                            Ok(index) if legacy_admin_ack => {
                                let _ = cmd.reply.send(Ok(index));
                            }
                            Ok(index) => {
                                self.pending_clients.insert(index, cmd.reply);
                                // Majority-of-one must commit without waiting for an
                                // AppendEntriesReply that can never exist.
                                self.try_advance_commit();
                                // Replicate a newly appended entry immediately rather
                                // than waiting for the next 50 ms heartbeat tick.
                                if self.role == RaftRole::Leader {
                                    self.send_heartbeats().await;
                                }
                            }
                            Err(error) => {
                                let _ = cmd.reply.send(Err(error));
                            }
                        }
                    }

                    // Leader transfer request (from RaftTaskHandle).
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

                // Apply all newly committed entries in Raft order. `last_applied`
                // advances only after the configured state machine acknowledges
                // completion; regular client success is resolved at that point.
                while self.last_applied < self.commit_index {
                    let next_index = self.last_applied + 1;
                    let physical = (next_index - self.ps.snapshot_index) as usize;
                    let Some(entry) = self.ps.log.get(physical).cloned() else {
                        self.fail_all_clients("committed Raft entry missing from local log");
                        return;
                    };

                    // Membership changes remain an internal consensus state-machine
                    // concern. Replicated SQL commands do not use this tag.
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

                // ── Session 13: leadership transfer timeout ────────────────
                if let Some((_target, deadline)) = &self.transfer_in_progress {
                    if Instant::now() >= *deadline {
                        self.transfer_in_progress = None;
                    }
                }
                if self.transfer_in_progress.is_some() && self.role != RaftRole::Leader {
                    self.transfer_in_progress = None;
                }

                // Publish shared state.
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

    // ── Message dispatch ───────────────────────────────────────────────────

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

    // ── Election ───────────────────────────────────────────────────────────

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

    // ── Log replication ────────────────────────────────────────────────────

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

    // ── Role transitions ───────────────────────────────────────────────────

    fn become_follower(&mut self, term: u64) {
        // A client whose entry is already committed may still receive success
        // after this node applies it as a follower. Uncommitted proposals become
        // outcome-uncertain on leadership loss and must never be reported as success.
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

    // ── Client command ─────────────────────────────────────────────────────

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
        // The append must be durable before replication starts. The public
        // regular-client reply is intentionally NOT resolved here anymore.
        self.persist();
        Ok(idx)
    }

    /// Handle a compact-log admin command embedded in a ClientCommand payload.
    fn handle_compact_log_cmd(&mut self, payload: Vec<u8>) -> Result<u64, String> {
        if payload.len() < 2 + 8 {
            return Err("compact-log payload too short (need at least 10 bytes)".to_string());
        }
        let last_index = u64::from_be_bytes(
            payload[2..10]
                .try_into()
                .map_err(|_| "bad last_index bytes".to_string())?,
        );
        let data = payload[10..].to_vec();

        let safe_last = last_index.min(self.commit_index);
        if safe_last == 0 {
            return Ok(0);
        }

        let last_term = self.ps.term_at(safe_last);
        self.ps.install_snapshot(safe_last, last_term);
        self.snapshot_data = Arc::new(data);
        self.persist();
        Ok(safe_last)
    }

    // ── Session 13: membership change application ─────────────────────────

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

    // ── Session 13: InstallSnapshot handlers ──────────────────────────────

    fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if args.term < self.ps.current_term {
            return InstallSnapshotReply {
                term: self.ps.current_term,
            };
        }
        if args.term > self.ps.current_term || self.role == RaftRole::Candidate {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);

        if args.last_included_index <= self.ps.snapshot_index {
            return InstallSnapshotReply {
                term: self.ps.current_term,
            };
        }

        self.ps
            .install_snapshot(args.last_included_index, args.last_included_term);
        self.snapshot_data = args.data;
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = args.last_included_index;
        self.persist();

        InstallSnapshotReply {
            term: self.ps.current_term,
        }
    }

    async fn on_install_snapshot_reply(&mut self, from: NodeId, reply: InstallSnapshotReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Leader {
            return;
        }
        if let Some(leader) = &mut self.leader {
            let snap_idx = self.ps.snapshot_index;
            leader.match_index.insert(from.clone(), snap_idx);
            leader.next_index.insert(from, snap_idx + 1);
        }
    }

    // ── Session 13: leader transfer (Raft §3.10) ──────────────────────────

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

    /// Handle a TimeoutNow message: immediately start an election.
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

// ── RaftShared ─────────────────────────────────────────────────────────────

/// Publicly observable state snapshot published after each event loop tick.
#[derive(Debug, Clone)]
pub struct RaftShared {
    pub role: RaftRole,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub last_applied: u64,
}
