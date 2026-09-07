// SPDX-License-Identifier: Apache-2.0
//! State-machine snapshot boundary used by Raft.
//!
//! Raft owns the ordering and persistence rules around snapshot creation and
//! installation, but it must not know the state machine's wire format. Concrete
//! state machines implement this interface to create a complete snapshot for an
//! exact Raft boundary and to validate/restore received bytes before Raft can
//! acknowledge installation.

/// Complete state-machine snapshot provider/restorer for Raft.
///
/// Implementations must fail closed: `create_snapshot` must represent exactly
/// the requested `(last_included_index, last_included_term)` boundary, and
/// `restore_snapshot` must reject bytes whose embedded boundary differs from the
/// expected RPC/persistence metadata before mutating state.
pub trait StateMachineSnapshotStore: Send + Sync {
    fn create_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<Vec<u8>, String>;

    fn restore_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<(), String>;
}
