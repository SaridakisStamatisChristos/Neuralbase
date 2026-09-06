// SPDX-License-Identifier: Apache-2.0
// Raft persistent log.
//
// Stores log entries with optional snapshot support for log compaction.
// The PersistentState struct captures the fields Raft requires to be stable
// across crashes: currentTerm, votedFor, log[], snapshot_index, snapshot_term.
//
// After a snapshot at last_included_index=N:
//   - log[0] is a sentinel with term=snapshot_term, index=N
//   - Real entries start at log[1] (Raft index N+1)
//   - last_log_index = snapshot_index + log.len() - 1
//
// CONFIDENCE: raw=0.86 effective=0.78
// DEPENDS_ON: rpc
// RISK: Snapshot install changes index arithmetic — see invariants in
//       REVIEW_REQUIRED.md §Session13 before lifting confidence cap.
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Session13

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::consensus::rpc::{LogEntry, NodeId};

// ── RaftPersistenceStore ───────────────────────────────────────────────────

/// Stable-storage interface for Raft persistent state.
///
/// Implementations must be Send + Sync so they can be shared across the
/// Raft event-loop task and the test harness.
///
/// Invariant: `save` is called BEFORE the node replies to any RPC that
/// depends on the saved state (term, vote, log entry).
pub trait RaftPersistenceStore: Send + Sync {
    /// Persist the full state + snapshot data atomically.
    fn save(&self, state: &PersistentState, snapshot_data: &[u8]) -> Result<(), String>;
    /// Load previously saved state.  Returns `None` on a fresh node.
    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String>;
}

// ── MemPersistenceStore ────────────────────────────────────────────────────

/// In-memory persistence store — survives node restart within the same
/// process when the `Arc<MemPersistenceStore>` outlives the `RaftNode`.
///
/// Used by integration tests to verify restart-recovery behaviour.
pub struct MemPersistenceStore {
    inner: Mutex<Option<MemPersistedData>>,
}

struct MemPersistedData {
    state: PersistentState,
    snapshot_data: Vec<u8>,
}

impl MemPersistenceStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }
}

impl Default for MemPersistenceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftPersistenceStore for MemPersistenceStore {
    fn save(&self, state: &PersistentState, snapshot_data: &[u8]) -> Result<(), String> {
        let mut guard = self.inner.lock().map_err(|e| e.to_string())?;
        *guard = Some(MemPersistedData {
            state: state.clone(),
            snapshot_data: snapshot_data.to_vec(),
        });
        Ok(())
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        let guard = self.inner.lock().map_err(|e| e.to_string())?;
        Ok(guard
            .as_ref()
            .map(|d| (d.state.clone(), d.snapshot_data.clone())))
    }
}

// ── PersistentState ────────────────────────────────────────────────────────

/// The fields Raft requires to be persisted to stable storage before
/// responding to any RPC.
///
/// ## Log index arithmetic after snapshot
///
/// Physical position in `self.log` maps to Raft index as follows:
/// ```text
///   raft_index = snapshot_index + physical_position
/// ```
/// `log[0]` is always a sentinel entry representing the snapshot boundary
/// (term = snapshot_term, index = snapshot_index, command = []).
/// When `snapshot_index == 0`, this is the initial sentinel (term 0, index 0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term this node has seen (initialised to 0, increases monotonically).
    pub current_term: u64,
    /// CandidateId that received vote in current term, or None.
    pub voted_for: Option<NodeId>,
    /// The actual log.  `log[0]` is always the snapshot-boundary sentinel.
    pub log: Vec<LogEntry>,
    /// Raft index of the last entry included in the most recent snapshot.
    /// 0 = no snapshot has been taken.
    pub snapshot_index: u64,
    /// Term of `snapshot_index`.
    pub snapshot_term: u64,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentState {
    /// Create a new persistent state with the sentinel entry at index 0.
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                index: 0,
                command: vec![],
            }],
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }

    /// Return the Raft index of the last log entry.
    ///
    /// `snapshot_index + len - 1` because `log[0]` occupies the snapshot-
    /// boundary position (not a distinct extra entry).
    pub fn last_log_index(&self) -> u64 {
        self.snapshot_index + self.log.len() as u64 - 1
    }

    /// Return the term of the last log entry.
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Return the term of the entry at Raft `index`, or 0 if out of range.
    pub fn term_at(&self, index: u64) -> u64 {
        if index < self.snapshot_index {
            return 0;
        }
        let physical = (index - self.snapshot_index) as usize;
        self.log.get(physical).map(|e| e.term).unwrap_or(0)
    }

    /// Append a new entry and return its Raft index.
    pub fn append(&mut self, term: u64, command: Vec<u8>) -> u64 {
        let index = self.snapshot_index + self.log.len() as u64;
        self.log.push(LogEntry {
            term,
            index,
            command,
        });
        index
    }

    /// Truncate the log to `prev_log_index` (inclusive) and append `entries`.
    /// Implements the "delete conflicting entries then append" step of
    /// AppendEntries (§5.3).
    pub fn truncate_and_append(&mut self, prev_log_index: u64, entries: Vec<LogEntry>) {
        let physical_keep = if prev_log_index >= self.snapshot_index {
            (prev_log_index - self.snapshot_index) as usize + 1
        } else {
            // prev_log_index is before the snapshot boundary — keep only the
            // sentinel so the log remains in a valid state.
            1
        };
        self.log.truncate(physical_keep);
        for entry in entries {
            self.log.push(entry);
        }
    }

    /// Return a slice of entries from Raft index `from` (inclusive) to end.
    ///
    /// Never returns the sentinel (physical[0]).
    pub fn entries_from(&self, from: u64) -> &[LogEntry] {
        // Physical position of `from`.  If `from` <= snapshot_index, start
        // from the first real entry (physical 1) — the sentinel is not sent.
        let physical = if from > self.snapshot_index {
            (from - self.snapshot_index) as usize
        } else {
            1 // skip sentinel
        };
        if physical >= self.log.len() {
            &[]
        } else {
            &self.log[physical..]
        }
    }

    /// Discard all log entries ≤ `last_included_index` and replace the
    /// sentinel with the new snapshot boundary.
    ///
    /// If the log contains the entry at `last_included_index`, its entries
    /// after that point are retained.  Otherwise the log is reset to just
    /// the new sentinel.
    ///
    /// CONFIDENCE: raw=0.82  [HUMAN REVIEW REQUIRED] §Session13 Invariant 1.
    pub fn install_snapshot(&mut self, last_included_index: u64, last_included_term: u64) {
        // Build a new sentinel for the snapshot boundary.
        let new_sentinel = LogEntry {
            term: last_included_term,
            index: last_included_index,
            command: vec![],
        };

        // Retain any log entries that follow the snapshot.
        let retained: Vec<LogEntry> = if last_included_index >= self.last_log_index() {
            // Entire existing log is covered by the snapshot.
            vec![]
        } else {
            let physical_first_kept = (last_included_index - self.snapshot_index) as usize + 1;
            self.log[physical_first_kept..].to_vec()
        };

        self.log = std::iter::once(new_sentinel).chain(retained).collect();
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_has_sentinel() {
        let s = PersistentState::new();
        assert_eq!(s.last_log_index(), 0);
        assert_eq!(s.last_log_term(), 0);
    }

    #[test]
    fn append_advances_index() {
        let mut s = PersistentState::new();
        let idx = s.append(1, b"cmd1".to_vec());
        assert_eq!(idx, 1);
        assert_eq!(s.last_log_index(), 1);
        assert_eq!(s.last_log_term(), 1);
    }

    #[test]
    fn truncate_and_append_removes_conflict() {
        let mut s = PersistentState::new();
        s.append(1, b"a".to_vec()); // idx=1
        s.append(1, b"b".to_vec()); // idx=2
        s.append(2, b"c".to_vec()); // idx=3 — conflicting
                                    // Leader sends entries starting at index 2 with term 3.
        s.truncate_and_append(
            1,
            vec![LogEntry {
                term: 3,
                index: 2,
                command: b"x".to_vec(),
            }],
        );
        assert_eq!(s.last_log_index(), 2);
        assert_eq!(s.term_at(2), 3);
    }

    #[test]
    fn entries_from_empty_when_past_end() {
        let s = PersistentState::new();
        assert_eq!(s.entries_from(99).len(), 0);
    }

    // ── Snapshot arithmetic tests ──────────────────────────────────────────

    #[test]
    fn install_snapshot_resets_log_and_sentinel() {
        let mut s = PersistentState::new();
        for i in 1u64..=10 {
            s.append(1, format!("cmd{i}").into_bytes());
        }
        assert_eq!(s.last_log_index(), 10);

        s.install_snapshot(5, 1);
        // Sentinel is at physical[0] = Raft index 5.
        assert_eq!(s.snapshot_index, 5);
        assert_eq!(s.snapshot_term, 1);
        // Entries 6..=10 were retained.
        assert_eq!(s.last_log_index(), 10);
        // term_at the snapshot boundary.
        assert_eq!(s.term_at(5), 1);
        // Entries before snapshot are inaccessible.
        assert_eq!(s.term_at(3), 0);
    }

    #[test]
    fn install_snapshot_beyond_log_resets_to_sentinel_only() {
        let mut s = PersistentState::new();
        s.append(1, b"a".to_vec()); // idx=1
        s.install_snapshot(1, 1);
        // Log is just the sentinel.
        assert_eq!(s.last_log_index(), 1);
        assert_eq!(s.entries_from(2).len(), 0);
    }

    #[test]
    fn entries_from_after_snapshot() {
        let mut s = PersistentState::new();
        for i in 1u64..=5 {
            s.append(1, format!("c{i}").into_bytes());
        }
        s.install_snapshot(3, 1);
        // Entries 4 and 5 are still accessible.
        let e = s.entries_from(4);
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].index, 4);
        assert_eq!(e[1].index, 5);
    }

    #[test]
    fn append_after_snapshot_uses_correct_raft_index() {
        let mut s = PersistentState::new();
        for i in 1u64..=5 {
            s.append(1, format!("c{i}").into_bytes());
        }
        s.install_snapshot(5, 1);
        // Append after snapshot — next Raft index must be 6.
        let idx = s.append(2, b"new".to_vec());
        assert_eq!(idx, 6);
        assert_eq!(s.last_log_index(), 6);
    }

    #[test]
    fn mem_persistence_store_roundtrip() {
        let store = MemPersistenceStore::new();
        let mut ps = PersistentState::new();
        ps.current_term = 7;
        ps.append(7, b"hello".to_vec());
        store.save(&ps, b"snap").unwrap();
        let (loaded_ps, loaded_snap) = store.load().unwrap().unwrap();
        assert_eq!(loaded_ps.current_term, 7);
        assert_eq!(loaded_ps.last_log_index(), 1);
        assert_eq!(loaded_snap, b"snap");
    }
}
