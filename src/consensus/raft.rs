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

use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Instant};
use crate::consensus::log::{PersistentState, RaftPersistenceStore};
use crate::consensus::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
    MembershipChange, NodeId, RaftMessage, RequestVoteArgs, RequestVoteReply,
};
use crate::consensus::transport::Transport;

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

// ── ClientCommand ──────────────────────────────────────────────────────────

/// A client command submitted to the leader for replication.
pub struct ClientCommand {
    pub payload: Vec<u8>,
    /// Channel on which the committed log index is returned (or an error string).
    pub reply: oneshot::Sender<Result<u64, String>>,
}

/// Handle for a spawned Raft event-loop task.
pub struct RaftTaskHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl RaftTaskHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.join_handle).await;
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
    ///
    /// Stored as Arc<Vec<u8>> so that InstallSnapshot messages sent to multiple
    /// laggard peers on every heartbeat tick share the same buffer rather than
    /// each getting an O(n) copy.  Replacing the snapshot data is still O(1)
    /// (just swap the Arc pointer).  The buffer is freed when the last holder
    /// (the node itself or in-flight messages) drops their reference.
    snapshot_data: Arc<Vec<u8>>,

    // Membership-change state (Session 13).
    /// True while a single-step membership change is being committed.
    /// Client commands are rejected while this flag is set.
    membership_change_in_progress: bool,

    // Leadership transfer state (Session 13 — Raft §3.10).
    /// Active leadership transfer: (target_node_id, deadline).
    /// Client commands are rejected while a transfer is in progress.
    /// If the deadline expires without the target becoming leader,
    /// the transfer aborts and normal operation resumes.
    transfer_in_progress: Option<(NodeId, Instant)>,

    // Persistence (Session 13).
    /// Stable storage — if Some, called on every state mutation that Raft
    /// requires to be durable before an RPC reply is sent.
    persistence: Option<Arc<dyn RaftPersistenceStore>>,

    // Election timeout; overrideable for tests (shrinks to tick_ms_override if Some).
    election_timeout_base_ms: u64,

    // Apply channel: receives committed LogEntry values after last_applied advances.
    // None means apply-loop advances last_applied but does not dispatch entries
    // (acceptable for nodes that are followers-only or in test mode).
    //
    // BOUNDED: capacity = APPLY_CHANNEL_CAPACITY (1024).  When full, the Raft
    // apply loop blocks (async await) until the consumer drains entries.  This
    // provides backpressure rather than unbounded memory growth.  Entries are
    // NEVER dropped — blocking is the correct behavior.
    apply_tx: Option<mpsc::Sender<LogEntry>>,
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
        }
    }

    /// Attach a bounded apply channel.  Committed `LogEntry` values are sent here
    /// in order after `last_applied` advances.  Call before `spawn`.
    ///
    /// The channel has a fixed capacity of `APPLY_CHANNEL_CAPACITY` entries.
    /// When full, the Raft apply loop blocks until the consumer drains entries.
    /// Entries are never dropped.
    pub fn with_apply_tx(mut self, tx: mpsc::Sender<LogEntry>) -> Self {
        self.apply_tx = Some(tx);
        self
    }

    /// Attach a persistence store.  If a previously-saved state exists it is
    /// loaded immediately (restoring term, votedFor, log, snapshot).
    ///
    /// MUST be called before `spawn`.  Calling after spawn has no effect.
    pub fn with_persistence(mut self, store: Arc<dyn RaftPersistenceStore>) -> Self {
        if let Ok(Some((ps, snap))) = store.load() {
            self.snapshot_data = Arc::new(snap);
            // Restore volatile derived state from the persistent snapshot index.
            let snap_idx = ps.snapshot_index;
            self.ps = ps;
            // After restart: commit_index and last_applied can at minimum be
            // advanced to snapshot_index (snapshot represents committed + applied).
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
        // Always use at least a range of 10 for the jitter so that even
        // base = 1 ms (used by adversarial tests) produces real staggering.
        // gen_range(0..1) is the integer range {0} only — every node would
        // get the same timeout and elections would livelock forever.
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
            // Best-effort: a storage error is non-fatal in tests but would
            // be fatal in production.  Log the error; do not panic.
            if let Err(e) = store.save(&self.ps, &self.snapshot_data) {
                // tracing is available but we don't import it here to keep
                // the consensus module independent.  Use eprintln as fallback.
                eprintln!("[raft] persist error: {e}");
            }
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
                        break;
                    }

                    // Incoming RPC.
                    msg = self.transport.recv() => {
                        match msg {
                            Some((from, rmsg)) => {
                                election_deadline = self.handle_message(from, rmsg, election_deadline).await;
                            }
                            None => break, // transport closed
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
                        let result = self.handle_client_command(cmd.payload);
                        let _ = cmd.reply.send(result);
                    }
                }

                // Advance state machine: apply all newly committed log entries.
                // Raft Invariant 4: last_applied only advances up to commit_index,
                // so no uncommitted entry is ever applied.
                // After a snapshot, log entries are addressed by their Raft index
                // (not physical position).  Physical = raft_index − snapshot_index.
                while self.last_applied < self.commit_index {
                    self.last_applied += 1;
                    // Physical position in the log vector.
                    let physical =
                        (self.last_applied - self.ps.snapshot_index) as usize;
                    if let Some(entry) = self.ps.log.get(physical).cloned() {
                        // Intercept membership-change entries before forwarding.
                        if entry.command.starts_with(MEMBERSHIP_CHANGE_TAG) {
                            let payload = &entry.command[MEMBERSHIP_CHANGE_TAG.len()..];
                            if let Ok(change) =
                                serde_json::from_slice::<MembershipChange>(payload)
                            {
                                self.apply_membership_change(change);
                            }
                        }
                        if let Some(tx) = &self.apply_tx {
                            // Bounded channel: .send().await blocks when full,
                            // providing backpressure.  Entries are never dropped.
                            // Channel closed means executor is shutting down.
                            if tx.send(entry).await.is_err() {
                                // Receiver dropped — no consumer, stop forwarding.
                                break;
                            }
                        }
                    }
                }

                // ── Session 13: leadership transfer timeout ────────────────
                // If a transfer is in progress and the deadline has expired,
                // abort the transfer and resume accepting client commands.
                if let Some((_target, deadline)) = &self.transfer_in_progress {
                    if Instant::now() >= *deadline {
                        self.transfer_in_progress = None;
                    }
                }
                // If we were the old leader and we detect a new term (meaning
                // the target won the election), clear the transfer state.
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
                    // Valid heartbeat/replication from current or newer leader.
                    election_deadline = Instant::now() + self.election_timeout();
                }
            }
            RaftMessage::AppendEntriesReply(reply) => {
                self.on_append_entries_reply(from, reply).await;
            }
            // ── Session 13: snapshot install ──────────────────────────────
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
            // ── Session 13: membership changes ───────────────────────────
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
            RaftMessage::MembershipChangeCmdReply { success: _, error: _ } => {
                // Acknowledgement of a membership-change command we sent.
                // No action needed in this implementation.
            }
            // ── Session 13: leader transfer (Raft §3.10) ────────────────
            RaftMessage::LeaderTransfer { target } => {
                self.on_leader_transfer(&from, target).await;
            }
            RaftMessage::LeaderTransferReply { success: _, error: _ } => {
                // Acknowledgement; no further action needed.
            }
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
        self.votes_received = 1; // vote for self
        self.leader_id = None;
        // Persist before sending RequestVote (Raft safety: term + votedFor durable).
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
        // Single-node cluster: self-vote already constitutes a majority.  
        // Check immediately so we don't wait for replies that will never come.
        let total_nodes = self.peers.len() + 1; // cluster size including self
        let majority = total_nodes / 2 + 1;    // floor(N/2) + 1
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
            // Persist before granting vote (Raft safety: votedFor durable).
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

            // ── Session 13: InstallSnapshot for laggard followers ──────────
            // If the next entry the follower needs has been compacted into the
            // snapshot, send the snapshot instead of AppendEntries.
            // CONFIDENCE: raw=0.80  [HUMAN REVIEW REQUIRED] §Session13 Inv-2
            if !self.snapshot_data.is_empty() && next <= self.ps.snapshot_index {
                let snap = InstallSnapshotArgs {
                    term: self.ps.current_term,
                    leader_id: self.id.clone(),
                    last_included_index: self.ps.snapshot_index,
                    last_included_term: self.ps.snapshot_term,
                    // Arc::clone is O(1) — all peers share the same buffer.
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

        // Consistency check.
        if args.prev_log_index > 0 && self.ps.term_at(args.prev_log_index) != args.prev_log_term {
            return AppendEntriesReply {
                term: self.ps.current_term,
                success: false,
                match_index: self.ps.last_log_index(),
            };
        }

        // Append entries.
        if !args.entries.is_empty() {
            self.ps
                .truncate_and_append(args.prev_log_index, args.entries);
            // Persist log changes before replying (Raft safety).
            self.persist();
        }

        // Advance commit index.
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
                // Advance commit index if a majority has replicated.
                self.try_advance_commit();
            } else {
                // Back off by 1.
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
                .map(|l| {
                    l.match_index
                        .values()
                        .filter(|&&m| m >= idx)
                        .count()
                })
                .unwrap_or(0)
                + 1; // +1 for self
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
        self.ps.current_term = term;
        self.ps.voted_for = None;
        self.role = RaftRole::Follower;
        self.leader = None;
        // Persist term change before any RPC interaction at the new term.
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
        // Immediately send heartbeats to assert leadership.
        self.send_heartbeats().await;
    }

    // ── Client command ─────────────────────────────────────────────────────

    fn handle_client_command(&mut self, payload: Vec<u8>) -> Result<u64, String> {
        if self.role != RaftRole::Leader {
            return Err(format!(
                "not leader; redirect to {:?}",
                self.leader_id
            ));
        }

        // ── Session 13: leadership transfer in progress ────────────────────
        // Reject client commands while a transfer is active (Raft §3.10).
        if self.transfer_in_progress.is_some() {
            return Err("leadership transfer in progress; retry later".to_string());
        }

        // ── Session 13: compact-log (snapshot trigger) ─────────────────────
        if payload.starts_with(COMPACT_LOG_TAG) {
            return self.handle_compact_log_cmd(payload);
        }

        // ── Session 13: leader transfer via admin command ──────────────────
        // This pathway is reached when a client sends a LEADER_TRANSFER_TAG
        // payload; the actual transfer RPC is handled in handle_message.
        // We return immediately — the transfer is async.
        if payload.starts_with(LEADER_TRANSFER_TAG) {
            return Err("use LeaderTransfer RPC, not client command".to_string());
        }

        // ── Session 13: membership change ──────────────────────────────────
        if payload.starts_with(MEMBERSHIP_CHANGE_TAG) {
            if self.membership_change_in_progress {
                return Err("membership change already in progress".to_string());
            }
            self.membership_change_in_progress = true;
            // Fall through: append as a regular log entry; apply loop
            // intercepts membership-change entries when committed.
        } else if self.membership_change_in_progress {
            // Reject regular data commands while a membership change is
            // being committed (single-step safety: no overlap).
            return Err("membership change in progress; retry later".to_string());
        }

        let idx = self.ps.append(self.ps.current_term, payload);
        // Persist log change before informing the client (Raft safety).
        self.persist();
        Ok(idx)
    }

    /// Handle a compact-log admin command embedded in a ClientCommand payload.
    ///
    /// Payload format: `COMPACT_LOG_TAG` (2B) + last_index (8B BE) + data (*).
    ///
    /// CONFIDENCE: raw=0.80  [HUMAN REVIEW REQUIRED] §Session13 Invariant 1.
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

        // Safety: only compact entries that are already committed.
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

    /// Apply a committed membership-change entry to the live peer list.
    ///
    /// Called from the apply loop AFTER the entry is committed (majority ack).
    /// This means both the leader and all followers execute this.
    ///
    /// CONFIDENCE: raw=0.74  [HUMAN REVIEW REQUIRED] §Session13 Invariant 3.
    fn apply_membership_change(&mut self, change: MembershipChange) {
        match change {
            MembershipChange::AddNode(ref new_id) => {
                if !self.peers.contains(new_id) && new_id != &self.id {
                    self.peers.push(new_id.clone());
                    // If we are the leader, initialise tracking state for the
                    // new peer so it starts receiving AppendEntries.
                    if let Some(leader) = &mut self.leader {
                        // Fresh node: start at index 1 so the consistency check
                        // either succeeds (node has log) or drives next_index down.
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

    /// Process an InstallSnapshot RPC from the leader.
    ///
    /// CONFIDENCE: raw=0.78  [HUMAN REVIEW REQUIRED] §Session13 Invariants 1–4.
    fn on_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if args.term < self.ps.current_term {
            return InstallSnapshotReply { term: self.ps.current_term };
        }
        if args.term > self.ps.current_term || self.role == RaftRole::Candidate {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);

        // Invariant 1: Accept only if the snapshot is strictly newer than our
        // current snapshot (i.e. contains more committed entries).
        if args.last_included_index <= self.ps.snapshot_index {
            return InstallSnapshotReply { term: self.ps.current_term };
        }

        // Install the snapshot.  install_snapshot() retains any log entries
        // that follow last_included_index (Raft §7 step 6).
        self.ps.install_snapshot(args.last_included_index, args.last_included_term);
        // Arc move — O(1), no buffer copy.
        self.snapshot_data = args.data;

        // Advance commit_index and last_applied to the snapshot boundary.
        // Invariant 4: last_applied must never exceed commit_index.
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = args.last_included_index;

        // Persist the new snapshot state before replying.
        self.persist();

        InstallSnapshotReply { term: self.ps.current_term }
    }

    /// Process an InstallSnapshotReply from a follower.
    ///
    /// On success: advance next_index and match_index for the follower so
    /// subsequent heartbeats send AppendEntries for entries after the snapshot.
    async fn on_install_snapshot_reply(&mut self, from: NodeId, reply: InstallSnapshotReply) {
        if reply.term > self.ps.current_term {
            self.become_follower(reply.term);
            return;
        }
        if self.role != RaftRole::Leader {
            return;
        }
        if let Some(leader) = &mut self.leader {
            // Follower now has the snapshot up to snapshot_index; advance
            // its next_index to snapshot_index + 1 so we send AppendEntries
            // (not another snapshot) on the next heartbeat.
            let snap_idx = self.ps.snapshot_index;
            leader.match_index.insert(from.clone(), snap_idx);
            leader.next_index.insert(from, snap_idx + 1);
        }
    }

    // ── Session 13: leader transfer (Raft §3.10) ──────────────────────────

    /// Handle a LeaderTransfer request (received by the leader from any node,
    /// typically via an admin command).
    ///
    /// Protocol:
    /// 1. Verify target is a known peer.
    /// 2. Set `transfer_in_progress` with a deadline.
    /// 3. Send `TimeoutNow` to the target so it immediately starts an election.
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

        // Send TimeoutNow to the transfer target so it starts an election
        // immediately (without waiting for random election timeout).
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
    ///
    /// Per Raft §3.10, the transfer target skips the randomized election
    /// timeout and calls an election right away.
    async fn on_timeout_now(&mut self, term: u64, election_deadline: &mut Instant) {
        // Only followers should honor TimeoutNow.
        if self.role == RaftRole::Leader {
            return;
        }
        if term < self.ps.current_term {
            return;
        }
        // Start election immediately (skip randomized timeout).
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
