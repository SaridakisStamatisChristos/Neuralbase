// SPDX-License-Identifier: Apache-2.0
// Consensus module — Raft consensus engine for NeuralBase.
//
// Sub-modules:
//   log       — persistent log (PersistentState, LogEntry)
//   raft      — state machine (RaftNode, RaftRole, RaftShared)
//   rpc       — wire message types
//   transport — Transport trait + ChannelTransport + TcpTransport
//
// CONFIDENCE: raw=0.84 effective=0.72
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Raft

// Session 5 — not yet wired into query execution path. Suppress dead_code.

pub mod log;
pub mod raft;
pub mod rpc;
pub mod transport;

// Re-exports: used by integration tests and future query-execution wiring.
// The binary itself does not yet call these directly — suppress until wired up.
#[allow(unused_imports)]
pub use raft::{RaftNode, RaftRole, RaftShared};
#[allow(unused_imports)]
pub use raft::RaftTaskHandle;
#[allow(unused_imports)]
pub use rpc::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, NodeId, RaftMessage, RequestVoteArgs,
    RequestVoteReply,
};
#[allow(unused_imports)]
pub use transport::{ChannelBus, ChannelTransport, Transport};

