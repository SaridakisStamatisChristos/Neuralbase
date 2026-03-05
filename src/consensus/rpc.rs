// SPDX-License-Identifier: Apache-2.0
// Raft RPC message types.
//
// Implements the two core RPCs from the Raft paper (§5):
//   - RequestVote  — used by candidates during leader election
//   - AppendEntries — used by leaders for log replication and heartbeats
//
// CONFIDENCE: raw=0.88 effective=0.80
// DEPENDS_ON: (none — pure data types)
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Raft

use serde::{Deserialize, Serialize};

/// A single entry in the Raft log.
/// `command` is an opaque byte payload (serialized client request).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    /// The term when the entry was received by the leader.
    pub term: u64,
    /// 1-based log index.
    pub index: u64,
    /// Opaque client command payload.
    pub command: Vec<u8>,
}

/// Arguments for the RequestVote RPC (§5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    /// Candidate's current term.
    pub term: u64,
    /// ID of the candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// Reply to a RequestVote RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    /// Current term of the receiver (so the caller can update itself).
    pub term: u64,
    /// True if the candidate received this node's vote.
    pub vote_granted: bool,
}

/// Arguments for the AppendEntries RPC (§5.3).
/// An empty `entries` vec is a heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    /// Leader's current term.
    pub term: u64,
    /// Leader's node ID (so followers can redirect clients).
    pub leader_id: NodeId,
    /// Index of the log entry immediately preceding the new ones.
    pub prev_log_index: u64,
    /// Term of the `prev_log_index` entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

/// Reply to an AppendEntries RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    /// Current term of the receiver.
    pub term: u64,
    /// True if the follower contained an entry matching prevLogIndex / prevLogTerm.
    pub success: bool,
    /// Optimistic hint: the follower's last log index (used to speed up nextIndex backtracking).
    pub match_index: u64,
}

/// Top-level message envelope sent over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
}

/// A node identifier — a string such as "node1:7000".
pub type NodeId = String;
