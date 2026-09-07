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

use std::sync::Arc;

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

/// Arguments for the InstallSnapshot RPC (Raft §7).
/// Single-chunk implementation: `done` is always true.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    /// Leader's current term.
    pub term: u64,
    /// Leader's node ID.
    pub leader_id: NodeId,
    /// Last log index included in the snapshot.
    pub last_included_index: u64,
    /// Term of `last_included_index`.
    pub last_included_term: u64,
    /// Opaque serialized state-machine data.
    /// Wrapped in Arc so that the leader can share the same buffer across
    /// multiple in-flight InstallSnapshot messages (one per laggard peer)
    /// without copying the snapshot bytes on every heartbeat tick.
    pub data: Arc<Vec<u8>>,
    /// True for the last (and only) chunk.
    pub done: bool,
}

/// Reply to an InstallSnapshot RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    /// Current term of the receiver (so the caller can update itself).
    pub term: u64,
    /// Exact snapshot boundary this reply acknowledges. Echoing the installed
    /// index prevents a delayed reply for snapshot N from being mistaken for a
    /// newer snapshot M that the leader created while the RPC was in flight.
    pub last_included_index: u64,
}

/// Single-step membership change command.
///
/// WARNING: Single-step membership changes are unsafe under certain
/// network partitions (see Raft §6 for the joint-consensus safe alternative).
/// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Session13
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MembershipChange {
    AddNode(NodeId),
    RemoveNode(NodeId),
}

/// Top-level message envelope sent over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    /// Leader → follower: install a snapshot (Raft §7).
    InstallSnapshot(InstallSnapshotArgs),
    /// Follower → leader: reply to InstallSnapshot.
    InstallSnapshotReply(InstallSnapshotReply),
    /// Admin → leader: single-step cluster membership change.
    MembershipChangeCmd(MembershipChange),
    /// Leader → admin: result of membership change.
    MembershipChangeCmdReply {
        success: bool,
        error: Option<String>,
    },
    /// Admin → leader: request leadership transfer to a specific follower (Raft §3.10).
    LeaderTransfer {
        target: NodeId,
    },
    /// Leader → admin: result of a leadership transfer request.
    LeaderTransferReply {
        success: bool,
        error: Option<String>,
    },
    /// Leader → target follower: skip election timeout and start election now.
    TimeoutNow {
        term: u64,
    },
}

/// A node identifier — a string such as "node1:7000".
pub type NodeId = String;
