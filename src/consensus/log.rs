// SPDX-License-Identifier: Apache-2.0
// Raft persistent log.
//
// Stores log entries in memory with an in-memory backing store
// (production path would persist to RocksDB — the interface is the same).
// The PersistentState struct captures the three fields that Raft requires to
// be stable across crashes: currentTerm, votedFor, and the log itself.
//
// CONFIDENCE: raw=0.86 effective=0.78
// DEPENDS_ON: rpc
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Raft

use crate::consensus::rpc::{LogEntry, NodeId};

/// The fields Raft requires to be persisted to stable storage before
/// responding to any RPC.  For session 5 this is in-memory; the interface
/// mirrors what a RocksDB-backed implementation would expose.
#[derive(Debug, Default)]
pub struct PersistentState {
    /// Latest term this node has seen (initialised to 0, increases monotonically).
    pub current_term: u64,
    /// CandidateId that received vote in current term, or None.
    pub voted_for: Option<NodeId>,
    /// The actual log.  Index 0 is a sentinel entry with term 0, index 0.
    pub log: Vec<LogEntry>,
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
        }
    }

    /// Return the 1-based index of the last log entry.
    pub fn last_log_index(&self) -> u64 {
        self.log.len() as u64 - 1
    }

    /// Return the term of the last log entry.
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Return the term of the entry at `index`, or 0 if out of range.
    pub fn term_at(&self, index: u64) -> u64 {
        self.log.get(index as usize).map(|e| e.term).unwrap_or(0)
    }

    /// Append a new entry.  `term` and `index` are assigned by the leader.
    pub fn append(&mut self, term: u64, command: Vec<u8>) -> u64 {
        let index = self.log.len() as u64;
        self.log.push(LogEntry {
            term,
            index,
            command,
        });
        index
    }

    /// Truncate the log to `keep_through` (inclusive) and append `entries`.
    /// This implements the "delete conflicting entries then append" step of
    /// AppendEntries (§5.3).
    pub fn truncate_and_append(&mut self, prev_log_index: u64, entries: Vec<LogEntry>) {
        // Truncate anything after prev_log_index.
        self.log.truncate((prev_log_index + 1) as usize);
        for entry in entries {
            self.log.push(entry);
        }
    }

    /// Return a slice of entries from `from` (inclusive) to end.
    pub fn entries_from(&self, from: u64) -> &[LogEntry] {
        let start = from as usize;
        if start >= self.log.len() {
            &[]
        } else {
            &self.log[start..]
        }
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
}
