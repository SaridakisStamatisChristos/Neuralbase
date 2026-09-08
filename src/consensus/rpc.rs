// SPDX-License-Identifier: Apache-2.0
// Raft RPC message types.
//
// Implements the core Raft RPCs plus snapshot, membership-administration, and
// leadership-transfer messages. Membership requests are log-replicated; they
// are not permission to mutate a process-local peer list.

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
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

/// Arguments for the AppendEntries RPC (§5.3).
/// An empty `entries` vec is a heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    /// Optimistic hint used to accelerate nextIndex backtracking.
    pub match_index: u64,
}

/// Arguments for the InstallSnapshot RPC (Raft §7).
/// Single-chunk implementation: `done` is always true.
///
/// Phase 3 binds committed membership to `data` through the versioned Raft
/// snapshot envelope. The SQL state machine still owns only the inner payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Arc<Vec<u8>>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub success: bool,
    /// Exact snapshot boundary this reply refers to.
    pub last_included_index: u64,
}

/// Administrative intent for a coordinated membership transition.
///
/// `AddNode` is retained for backward compatibility and is deliberately
/// redefined as learner addition. A node never becomes a voter merely because
/// it was added or because its process is alive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MembershipChange {
    /// Backward-compatible alias for adding a non-voting learner.
    AddNode(NodeId),
    /// Add a non-voting learner. This is a one-configuration change and does
    /// not alter voting quorum.
    AddLearner(NodeId),
    /// Promote an already-caught-up learner. This creates the joint old+new
    /// configuration; Raft appends `FinalizeJoint` only after the joint entry
    /// is durably committed/applied.
    PromoteLearner(NodeId),
    /// Remove a learner directly, or a voter through joint consensus.
    RemoveNode(NodeId),
    /// Internal second half of a joint-consensus transition. External callers
    /// must not submit this directly.
    FinalizeJoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotReply(InstallSnapshotReply),
    /// Admin → leader: coordinated membership intent.
    MembershipChangeCmd(MembershipChange),
    /// Leader → admin: result after the requested membership transition reaches
    /// its required durable commit/apply point. Timeout remains outcome-uncertain.
    MembershipChangeCmdReply {
        success: bool,
        error: Option<String>,
    },
    LeaderTransfer {
        target: NodeId,
    },
    LeaderTransferReply {
        success: bool,
        error: Option<String>,
    },
    TimeoutNow {
        term: u64,
    },
}

/// A logical node/incarnation identifier.
///
/// Phase 3 treats this string as an incarnation identity: once committed as
/// removed, the exact same NodeId is tombstoned and cannot be reused. A
/// replacement process must use a new NodeId.
pub type NodeId = String;
