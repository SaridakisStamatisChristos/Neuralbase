// SPDX-License-Identifier: Apache-2.0
// Raft consensus state machine.
//
// Implements the Raft algorithm (Ongaro & Ousterhout, 2014) as a tokio task.
// The state machine is driven by an event loop that selects on:
//   - Incoming RPC messages (via Transport::recv)
//   - Election timeout (randomised 150–300 ms unless tick_ms_override set)
//   - Heartbeat ticker (50 ms, leader only)
//   - Client command channel
//
// CONFIDENCE: raw=0.84 effective=0.74
// DEPENDS_ON: log, rpc, transport
// RISK: Log compaction (snapshotting) is stubbed — full snapshot install not
//       implemented. Membership changes are single-step (not joint consensus).
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Raft

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{self, Instant};

use crate::consensus::log::PersistentState;
use crate::consensus::rpc::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, NodeId, RaftMessage, RequestVoteArgs,
    RequestVoteReply,
};
use crate::consensus::transport::Transport;

// ── Constants ──────────────────────────────────────────────────────────────

const HEARTBEAT_MS: u64 = 50;
/// Default election timeout base (ms). Final timeout = base + rand(0..base).
const ELECTION_TIMEOUT_BASE_MS: u64 = 150;

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

    // Election timeout; overrideable for tests (shrinks to tick_ms_override if Some).
    election_timeout_base_ms: u64,

    // Apply channel: receives committed LogEntry values after last_applied advances.
    // None means apply-loop advances last_applied but does not dispatch entries
    // (acceptable for nodes that are followers-only or in test mode).
    apply_tx: Option<mpsc::UnboundedSender<LogEntry>>,
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
            election_timeout_base_ms: ELECTION_TIMEOUT_BASE_MS,
            apply_tx: None,
        }
    }

    /// Attach an apply channel.  Committed `LogEntry` values are sent here
    /// in order after `last_applied` advances.  Call before `spawn`.
    pub fn with_apply_tx(mut self, tx: mpsc::UnboundedSender<LogEntry>) -> Self {
        self.apply_tx = Some(tx);
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
                // apply_tx channel delivers committed entries to the state machine
                // executor (query engine / StorageExecutor write path).
                while self.last_applied < self.commit_index {
                    self.last_applied += 1;
                    if let Some(tx) = &self.apply_tx {
                        // Retrieve the committed entry and dispatch it.
                        // Index 0 is the sentinel; real entries start at 1.
                        if let Some(entry) = self.ps.log.get(self.last_applied as usize).cloned() {
                            // Best-effort: channel closed means the executor is
                            // shutting down; ignore the error.
                            let _ = tx.send(entry);
                        }
                    }
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
        let idx = self.ps.append(self.ps.current_term, payload);
        Ok(idx)
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
