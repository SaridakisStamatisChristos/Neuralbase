// SPDX-License-Identifier: Apache-2.0
// Consensus module — Raft consensus engine for NeuralBase.
//
// Sub-modules:
//   log       — persistent log (PersistentState, RaftPersistenceStore)
//   raft      — state machine (RaftNode, RaftRole, RaftShared)
//   rpc       — wire message types
//   transport — Transport trait + ChannelTransport + TcpTransport + optional TLS
//
// Session 13 additions:
//   - InstallSnapshot RPC support (rpc.rs + raft.rs)
//   - RaftPersistenceStore trait + MemPersistenceStore (log.rs)
//   - Membership changes via tagged ClientCommand (raft.rs)
//
// CONFIDENCE: raw=0.78 effective=0.70
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Session13

pub mod log;
pub mod raft;
pub mod rpc;
pub mod transport;

// Re-exports used by integration tests and production cluster wiring.
#[allow(unused_imports)]
pub use log::{MemPersistenceStore, PersistentState, RaftPersistenceStore};
#[allow(unused_imports)]
pub use raft::{
    encode_compact_log, encode_leader_transfer, encode_membership_change, ClientCommand,
    CommittedEntry, RaftNode, RaftRole, RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
    COMPACT_LOG_TAG, LEADER_TRANSFER_TAG, MEMBERSHIP_CHANGE_TAG,
};
#[allow(unused_imports)]
pub use rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
    MembershipChange, NodeId, RaftMessage, RequestVoteArgs, RequestVoteReply,
};
#[cfg(feature = "tls")]
#[allow(unused_imports)]
pub use transport::TlsTcpTransport;
#[allow(unused_imports)]
pub use transport::{ChannelBus, ChannelTransport, TcpTransport, Transport};