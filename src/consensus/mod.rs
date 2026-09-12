// SPDX-License-Identifier: Apache-2.0
// Consensus module — Raft consensus engine for NeuralBase.
//
// Sub-modules:
//   fail_closed      — strict persistence adapter for consensus-critical storage
//   log              — persistent log (PersistentState, RaftPersistenceStore)
//   membership       — durable voter/learner/joint-consensus configuration
//   raft             — state machine (RaftNode, RaftRole, RaftShared)
//   rpc              — wire message types
//   snapshot         — state-machine snapshot create/restore contract
//   snapshot_payload — membership-bound Raft snapshot envelope
//   transport        — Transport trait + ChannelTransport + TcpTransport + optional TLS
//
// Replicated-SQL / phased distributed-correctness additions:
//   - confirmed state-machine apply acknowledgement
//   - fail-closed persistence adapter for term/vote/log durability failures
//   - explicit SQL-aware state-machine snapshot contract
//   - resumable staged snapshot transitions across crashes
//   - Phase 3 versioned committed membership and joint-quorum primitives

pub mod fail_closed;
pub mod log;
pub mod membership;
pub mod operator_control;
pub mod raft;
pub mod rpc;
pub mod snapshot;
pub mod snapshot_payload;
pub mod transport;

// Re-exports used by integration tests and production cluster wiring.
#[allow(unused_imports)]
pub use fail_closed::FailClosedPersistenceStore;
#[allow(unused_imports)]
pub use log::{
    MemPersistenceStore, PersistentState, RaftPersistenceStore, StagedSnapshot, StagedSnapshotKind,
};
#[allow(unused_imports)]
pub use membership::{ClusterMembership, JointConfig, MEMBERSHIP_FORMAT_VERSION};
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
#[allow(unused_imports)]
pub use snapshot::StateMachineSnapshotStore;
#[allow(unused_imports)]
pub use snapshot_payload::{decode_snapshot_payload, encode_snapshot_payload};
#[cfg(feature = "tls")]
#[allow(unused_imports)]
pub use transport::TlsTcpTransport;
#[allow(unused_imports)]
pub use transport::{ChannelBus, ChannelTransport, TcpTransport, Transport};
