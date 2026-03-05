// SPDX-License-Identifier: Apache-2.0
// MVCC Transaction Manager — snapshot isolation, write-write conflict detection.
//
// Protocol:
//   1. begin()        → allocates HLC timestamp as transaction ID (= snapshot_ts)
//   2. write(tx, ...) → buffers write in memory; not visible to other txns yet
//   3. commit(tx)     → conflict check, then flush write buffer atomically to RocksDB
//   4. rollback(tx)   → discard buffer; no persistent side-effects
//
// Snapshot Isolation invariant:
//   A transaction T reads data committed strictly before T.snapshot_ts.
//   A committed write W is visible to T iff W.commit_ts ≤ T.snapshot_ts.
//
// Write-write conflict (first-committer-wins):
//   If any key in T's write set has a committed version with ts > T.snapshot_ts,
//   T aborts.
//
// CONFIDENCE: raw=0.76 effective=0.67

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;
use thiserror::Error;

use crate::hlc::{HlcClock, HlcTimestamp};
use crate::storage::{encode_versioned_key, StorageEngine, StorageError};

#[derive(Debug, Error)]
pub enum TxnError {
    #[error("write-write conflict: key already updated after snapshot")]
    WriteConflict,
    #[error("transaction already committed or rolled back")]
    TxnDone,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// A pending transaction.  Must be consumed by `commit` or `rollback`.
pub struct Transaction {
    pub id: u64, // = snapshot_ts.to_u64()
    pub snapshot_ts: HlcTimestamp,
    /// Buffered writes: (table_id, pk_bytes, value_bytes).
    writes: Vec<(u32, Vec<u8>, Vec<u8>)>,
    done: bool,
}

impl Transaction {
    fn new(snapshot_ts: HlcTimestamp) -> Self {
        Self {
            id: snapshot_ts.to_u64(),
            snapshot_ts,
            writes: Vec::new(),
            done: false,
        }
    }

    /// Buffer a write for `(table_id, pk_bytes)` → `value`.
    /// Multiple writes to the same key within a transaction are allowed;
    /// only the last one is committed.
    pub fn write(&mut self, table_id: u32, pk_bytes: Vec<u8>, value: Vec<u8>) {
        // Remove prior write to same key (keep only latest intent).
        self.writes
            .retain(|(tid, pk, _)| !(*tid == table_id && pk == &pk_bytes));
        self.writes.push((table_id, pk_bytes, value));
    }

    /// Read from the transaction's own write buffer (read-your-writes).
    pub fn read_own_write(&self, table_id: u32, pk_bytes: &[u8]) -> Option<&[u8]> {
        self.writes
            .iter()
            .rev()
            .find(|(tid, pk, _)| *tid == table_id && pk == pk_bytes)
            .map(|(_, _, v)| v.as_slice())
    }
}

/// Shared active-snapshot registry used by GC to compute safe horizon.
pub type ActiveSnapshots = Arc<Mutex<BTreeSet<u64>>>;

/// Transaction manager — thread-safe, shareable via Arc.
pub struct TransactionManager {
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    pub active_snapshots: ActiveSnapshots,
    /// Serializes the critical section: conflict_check → clock.tick() → write_batch.
    /// Held only by commit(); begin(), rollback(), and read() never acquire it.
    /// Eliminates the TOCTOU window where two concurrent committers could both
    /// pass the conflict check before either writes to RocksDB.
    commit_serializer: Mutex<()>,
}

impl TransactionManager {
    pub fn new(engine: Arc<StorageEngine>, clock: Arc<HlcClock>) -> Self {
        Self {
            engine,
            clock,
            active_snapshots: Arc::new(Mutex::new(BTreeSet::new())),
            commit_serializer: Mutex::new(()),
        }
    }

    /// Begin a new snapshot-isolated transaction.
    /// The snapshot timestamp is the current HLC.
    pub fn begin(&self) -> Transaction {
        let ts = self.clock.tick();
        {
            let mut snapshots = self.active_snapshots.lock().unwrap();
            snapshots.insert(ts.to_u64());
        }
        Transaction::new(ts)
    }

    /// Advance the HLC clock and return the new timestamp as a u64.
    /// Used for generating unique, monotone PKs without opening a full transaction.
    pub fn next_timestamp(&self) -> u64 {
        self.clock.tick().to_u64()
    }

    /// Commit the transaction:
    ///  1. Acquire commit_serializer — serializes all concurrent commits.
    ///  2. Check write-write conflicts (first-committer-wins).
    ///  3. Obtain a commit timestamp (new HLC tick — always > snapshot_ts).
    ///  4. Flush all writes to RocksDB atomically via a WriteBatch.
    ///  5. Remove snapshot from active set.
    ///
    /// The commit_serializer lock is held across steps 2–4 as a single unit,
    /// eliminating the TOCTOU race between conflict detection and the write.
    pub fn commit(&self, mut tx: Transaction) -> Result<HlcTimestamp, TxnError> {
        if tx.done {
            return Err(TxnError::TxnDone);
        }
        tx.done = true;

        // Serialize: conflict_check → clock.tick() → write_batch are one atomic unit.
        // begin(), rollback(), and read() do NOT acquire this lock.
        let _commit_guard = self.commit_serializer.lock().unwrap();

        // 1. Conflict detection.
        for (table_id, pk_bytes, _) in &tx.writes {
            // Check whether any version with ts > snapshot_ts exists for this key.
            if self.has_conflicting_version(*table_id, pk_bytes, tx.snapshot_ts)? {
                self.remove_snapshot(tx.id);
                return Err(TxnError::WriteConflict);
            }
        }

        // 2. Commit timestamp (strict causal successor of snapshot).
        let commit_ts = self.clock.tick();

        // 3. Atomic write batch (commit_serializer still held).
        let mut batch = WriteBatch::default();
        let cf = self.engine.db.cf_handle(crate::storage::CF_DATA).unwrap();
        for (table_id, pk_bytes, value) in &tx.writes {
            let key = encode_versioned_key(*table_id, pk_bytes, commit_ts);
            batch.put_cf(&cf, &key, value);
        }
        self.engine.write_batch(batch)?;

        // 4. Remove snapshot, then release commit_serializer (_commit_guard drops here).
        self.remove_snapshot(tx.id);
        Ok(commit_ts)
        // _commit_guard dropped at end of scope — lock released.
    }

    /// Rollback the transaction — discard write buffer, remove snapshot.
    pub fn rollback(&self, mut tx: Transaction) {
        if !tx.done {
            tx.done = true;
            tx.writes.clear();
            self.remove_snapshot(tx.id);
        }
    }

    // ── Transactional read (delegates to storage) ───────────────────────

    /// Read the latest version of `pk` in `table_id` visible to `tx`.
    /// Respects read-your-writes: checks own write buffer first.
    pub fn read(
        &self,
        tx: &Transaction,
        table_id: u32,
        pk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, TxnError> {
        // Read-your-writes.
        if let Some(v) = tx.read_own_write(table_id, pk_bytes) {
            return Ok(Some(v.to_vec()));
        }
        Ok(self
            .engine
            .read_latest(table_id, pk_bytes, tx.snapshot_ts)?)
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn has_conflicting_version(
        &self,
        table_id: u32,
        pk_bytes: &[u8],
        snapshot_ts: HlcTimestamp,
    ) -> Result<bool, TxnError> {
        // A conflict exists if the highest committed version ts > snapshot_ts.
        // We use read_latest at MAX to find the most recent committed version.
        let latest = self
            .engine
            .read_latest(table_id, pk_bytes, HlcTimestamp::MAX)?;
        if latest.is_none() {
            return Ok(false);
        }
        // Re-scan to extract the timestamp of the latest version.
        // Use raw scan to find the key with the greatest ts.
        let rows = self.engine.raw_scan_table_versions(table_id)?;
        let pk_prefix = crate::storage::encode_key_prefix(table_id, pk_bytes);
        let max_ts = rows
            .iter()
            .filter(|(k, _)| k.starts_with(&pk_prefix))
            .filter_map(|(k, _)| crate::storage::decode_ts_from_key(k))
            .max();
        Ok(max_ts.is_some_and(|ts| ts > snapshot_ts))
    }

    fn remove_snapshot(&self, id: u64) {
        let mut snapshots = self.active_snapshots.lock().unwrap();
        snapshots.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageEngine;
    use tempfile::TempDir;

    fn setup() -> (Arc<TransactionManager>, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let clock = Arc::new(HlcClock::new(500));
        let tm = Arc::new(TransactionManager::new(engine, clock));
        (tm, dir)
    }

    #[test]
    fn commit_makes_write_visible_to_later_snapshot() {
        let (tm, _dir) = setup();
        let mut tx1 = tm.begin();
        tx1.write(1, b"k1".to_vec(), b"hello".to_vec());
        let _ct = tm.commit(tx1).unwrap();

        let tx2 = tm.begin();
        let val = tm.read(&tx2, 1, b"k1").unwrap();
        assert_eq!(val.as_deref(), Some(b"hello" as &[u8]));
        tm.rollback(tx2);
    }

    #[test]
    fn rollback_leaves_nothing_visible() {
        let (tm, _dir) = setup();
        let mut tx = tm.begin();
        tx.write(1, b"k2".to_vec(), b"ghost".to_vec());
        tm.rollback(tx);

        let tx2 = tm.begin();
        let val = tm.read(&tx2, 1, b"k2").unwrap();
        assert!(val.is_none());
        tm.rollback(tx2);
    }

    #[test]
    fn read_your_own_write() {
        let (tm, _dir) = setup();
        let mut tx = tm.begin();
        tx.write(1, b"k3".to_vec(), b"seen".to_vec());
        let val = tm.read(&tx, 1, b"k3").unwrap();
        assert_eq!(val.as_deref(), Some(b"seen" as &[u8]));
        tm.rollback(tx);
    }

    #[test]
    fn snapshot_does_not_see_concurrent_write() {
        let (tm, _dir) = setup();
        // T1 starts first (lower snapshot_ts).
        let tx1 = tm.begin();
        // T2 writes and commits.
        let mut tx2 = tm.begin();
        tx2.write(1, b"k4".to_vec(), b"late".to_vec());
        tm.commit(tx2).unwrap();
        // T1 should not see T2's write (T2 committed after T1 began).
        let val = tm.read(&tx1, 1, b"k4").unwrap();
        assert!(
            val.is_none(),
            "T1 must not see write committed after its snapshot"
        );
        tm.rollback(tx1);
    }

    /// Two threads race to commit writes to the same key.
    /// The commit_serializer guarantees exactly one succeeds and one gets
    /// WriteConflict — no lost updates, no phantom double-commits.
    #[test]
    fn concurrent_commits_to_same_key_exactly_one_wins() {
        use std::thread;
        let (tm, _dir) = setup();

        // Both transactions begin before either commits.
        // HLC ensures tx2.snapshot_ts > tx1.snapshot_ts (monotone ticks).
        // Neither has seen any committed version of "contested" yet.
        let mut tx1 = tm.begin();
        let mut tx2 = tm.begin();
        tx1.write(1, b"contested".to_vec(), b"v_from_tx1".to_vec());
        tx2.write(1, b"contested".to_vec(), b"v_from_tx2".to_vec());

        let tm1 = Arc::clone(&tm);
        let tm2 = Arc::clone(&tm);

        let h1 = thread::spawn(move || tm1.commit(tx1));
        let h2 = thread::spawn(move || tm2.commit(tx2));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(TxnError::WriteConflict)))
            .count();
        assert_eq!(
            successes, 1,
            "exactly one commit must succeed; got r1={r1:?} r2={r2:?}"
        );
        assert_eq!(
            conflicts, 1,
            "exactly one commit must conflict; got r1={r1:?} r2={r2:?}"
        );

        // No lost update: the key must be readable with the committed value.
        let tx3 = tm.begin();
        let val = tm.read(&tx3, 1, b"contested").unwrap();
        assert!(
            val.is_some(),
            "key must be readable after successful concurrent commit"
        );
        tm.rollback(tx3);
    }

    #[test]
    fn write_write_conflict_aborts_second_writer() {
        let (tm, _dir) = setup();
        // T1 commits first.
        let mut tx1 = tm.begin();
        tx1.write(1, b"k5".to_vec(), b"v1".to_vec());
        tm.commit(tx1).unwrap();

        // T2 was open before T1 committed — tries to write same key.
        // Because T2's snapshot < T1's commit_ts, write-write conflict fires.
        // (We open T2 after T1 commits intentionally here — HLC guarantees
        //  T2 begin ts > T1 commit ts, so conflict won't fire exactly. Test
        //  the conflict path by directly lower-setting snapshot_ts.)
        // Use a manually crafted low-snapshot transaction:
        let mut tx_low = Transaction::new(HlcTimestamp {
            wall_ms: 1,
            logical: 0,
        });
        {
            let mut snaps = tm.active_snapshots.lock().unwrap();
            snaps.insert(tx_low.id);
        }
        tx_low.write(1, b"k5".to_vec(), b"conflict".to_vec());
        let result = tm.commit(tx_low);
        assert!(
            matches!(result, Err(TxnError::WriteConflict)),
            "expected WriteConflict, got {result:?}"
        );
    }
}
