// SPDX-License-Identifier: Apache-2.0
//! State-machine snapshot boundary used by Raft.
//!
//! Raft owns the ordering and persistence rules around snapshot creation and
//! installation, but it must not know the state machine's wire format. Concrete
//! state machines implement this interface to create, validate, and restore a
//! complete snapshot for an exact Raft boundary.

/// Complete state-machine snapshot provider/restorer for Raft.
///
/// Implementations must fail closed: `create_snapshot` must represent exactly
/// the requested `(last_included_index, last_included_term)` boundary;
/// `validate_snapshot` must perform all non-mutating wire/boundary validation;
/// and `restore_snapshot` must remain safe to retry after an interrupted install.
pub trait StateMachineSnapshotStore: Send + Sync {
    fn create_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<Vec<u8>, String>;

    fn validate_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<(), String>;

    fn restore_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<(), String>;
}
