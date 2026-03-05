// SPDX-License-Identifier: Apache-2.0
// MVCC Garbage Collector — incremental background version cleanup.
//
// Safe horizon = min(active snapshot timestamps).
//   If there are no active snapshots → safe_horizon = u64::MAX (full GC allowed).
//
// For each primary key in a table, we keep the latest version ≤ safe_horizon
// and delete all strictly older versions.  This ensures:
//   ∀ active snapshot S with ts ≥ safe_horizon: S can still read the kept version.
//   All versions with ts < safe_horizon (except the latest) are unreachable
//   from any snapshot and may be safely deleted.
//
// GC runs on a background thread; the stop signal lets the owning object
// shut it down cleanly.
//
// CONFIDENCE: raw=0.68 effective=0.55
// DEPENDS_ON: storage, mvcc (active_snapshots)
// RISK: GC correctness depends on atomic snapshot registration before begin().
//       If a snapshot is registered after GC reads the active_snapshots list,
//       it may have a ts < the just-computed safe horizon, potentially exposing
//       a reclaimed version. Mitigation: transaction begin() registers its
//       snapshot BEFORE reading any data. Verified in GC safety tests.
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::hlc::HlcTimestamp;
use crate::storage::{decode_pk_from_key, decode_ts_from_key, StorageEngine, StorageError};

/// MVCC Garbage Collector.
pub struct GarbageCollector {
    engine: Arc<StorageEngine>,
    active_snapshots: Arc<Mutex<BTreeSet<u64>>>,
    stop: Arc<AtomicBool>,
}

impl GarbageCollector {
    pub fn new(engine: Arc<StorageEngine>, active_snapshots: Arc<Mutex<BTreeSet<u64>>>) -> Self {
        Self {
            engine,
            active_snapshots,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the GC background thread.  Returns a handle to stop it.
    pub fn start(self: Arc<Self>, interval_ms: u64) -> GcHandle {
        let stop_thread = Arc::clone(&self.stop);
        let stop_handle = Arc::clone(&self.stop);
        let gc = Arc::clone(&self);
        let handle = thread::spawn(move || {
            // Relaxed ordering: GC may run 1-2 extra passes after
            // stop signal on weakly-ordered CPUs. Safe — GC is
            // idempotent and cannot corrupt data or violate
            // any MVCC invariants.
            while !stop_thread.load(Ordering::Relaxed) {
                match gc.run_once_all_tables() {
                    Ok(stats) => {
                        if stats.versions_deleted > 0 {
                            eprintln!(
                                "[GC] deleted {} versions (safe_horizon={:?})",
                                stats.versions_deleted, stats.safe_horizon
                            );
                        }
                    }
                    Err(e) => eprintln!("[GC] error during gc run: {e}"),
                }
                thread::sleep(Duration::from_millis(interval_ms));
            }
        });
        GcHandle {
            stop: stop_handle,
            handle: Some(handle),
        }
    }

    /// Run one GC pass over all table IDs seen in the data CF.
    /// For tests, we accept a specific set of table IDs to GC.
    pub fn run_once(&self, table_ids: &[u32]) -> Result<GcStats, StorageError> {
        let safe_horizon = self.safe_horizon();
        let mut deleted = 0usize;

        for &table_id in table_ids {
            deleted += self.gc_table(table_id, safe_horizon)?;
        }
        Ok(GcStats {
            safe_horizon,
            versions_deleted: deleted,
        })
    }

    /// Scan the data CF for all table IDs present, then GC each.
    pub fn run_once_all_tables(&self) -> Result<GcStats, StorageError> {
        let table_ids = self.discover_table_ids()?;
        self.run_once(&table_ids)
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    /// Compute the GC safe horizon: the minimum active snapshot ts.
    /// All versions strictly older than the safe horizon may be collected
    /// (keeping the latest version per key at or below the horizon).
    pub fn safe_horizon(&self) -> HlcTimestamp {
        let snaps = self.active_snapshots.lock().unwrap();
        match snaps.iter().next() {
            Some(&min_id) => HlcTimestamp::from_u64(min_id),
            None => HlcTimestamp::MAX,
        }
    }

    fn gc_table(&self, table_id: u32, safe_horizon: HlcTimestamp) -> Result<usize, StorageError> {
        let rows = self.engine.raw_scan_table_versions(table_id)?;
        if rows.is_empty() {
            return Ok(0);
        }

        // Group versions by pk.
        let mut by_pk: std::collections::BTreeMap<Vec<u8>, Vec<(HlcTimestamp, Vec<u8>)>> =
            std::collections::BTreeMap::new();

        for (key, _val) in &rows {
            if let (Some(pk), Some(ts)) = (decode_pk_from_key(key), decode_ts_from_key(key)) {
                by_pk.entry(pk).or_default().push((ts, key.clone()));
            }
        }

        let mut deleted = 0;
        for (_pk, mut versions) in by_pk {
            // Sort ascending by timestamp.
            versions.sort_by_key(|(ts, _)| *ts);

            // Find versions ≤ safe_horizon.
            let below_horizon: Vec<_> = versions
                .iter()
                .filter(|(ts, _)| *ts <= safe_horizon)
                .collect();

            if below_horizon.len() <= 1 {
                // Nothing to collect: zero or one version below horizon.
                continue;
            }

            // Keep only the latest version ≤ safe_horizon.
            // Delete all others below the horizon.
            let keep_idx = below_horizon.len() - 1;
            for (i, (ts, raw_key)) in below_horizon.iter().enumerate() {
                if i < keep_idx {
                    // Extract pk from raw key for delete call.
                    if let Some(pk) = decode_pk_from_key(raw_key) {
                        self.engine.delete_version(table_id, &pk, *ts)?;
                        deleted += 1;
                    }
                }
            }
        }
        Ok(deleted)
    }

    fn discover_table_ids(&self) -> Result<Vec<u32>, StorageError> {
        let cf = self.engine.db.cf_handle(crate::storage::CF_DATA).unwrap();
        let mut iter = self.engine.db.raw_iterator_cf(&cf);
        iter.seek_to_first();

        let mut ids = std::collections::BTreeSet::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            if k.len() >= 4 {
                let id = u32::from_be_bytes(k[..4].try_into().unwrap());
                ids.insert(id);
            }
            iter.next();
        }
        Ok(ids.into_iter().collect())
    }
}

/// Statistics from a single GC pass.
#[derive(Debug)]
pub struct GcStats {
    pub safe_horizon: HlcTimestamp,
    pub versions_deleted: usize,
}

/// Handle to a running GC background thread.
pub struct GcHandle {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for GcHandle {
    fn drop(&mut self) {
        // Signal stop then join — ensures the GC thread has fully exited
        // before the owning scope (e.g. main) is torn down.
        // Relaxed ordering: GC is idempotent; an extra pass is harmless.
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use crate::storage::StorageEngine;
    use tempfile::TempDir;

    fn write_version(engine: &StorageEngine, table_id: u32, pk: &[u8], ts: HlcTimestamp) {
        engine.write_version(table_id, pk, ts, b"data").unwrap();
    }

    #[test]
    fn gc_deletes_old_versions_keeps_latest() {
        let snaps = Arc::new(Mutex::new(BTreeSet::new()));
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());

        let ts1 = HlcTimestamp {
            wall_ms: 10,
            logical: 0,
        };
        let ts2 = HlcTimestamp {
            wall_ms: 20,
            logical: 0,
        };
        let ts3 = HlcTimestamp {
            wall_ms: 30,
            logical: 0,
        };
        write_version(&engine, 1, b"pk", ts1);
        write_version(&engine, 1, b"pk", ts2);
        write_version(&engine, 1, b"pk", ts3);

        let gc = Arc::new(GarbageCollector::new(
            Arc::clone(&engine),
            Arc::clone(&snaps),
        ));
        let stats = gc.run_once(&[1]).unwrap();
        // No active snapshots → safe_horizon = MAX → ts1 and ts2 are below ts3; delete 2.
        assert_eq!(stats.versions_deleted, 2);

        // Latest version (ts3) still readable.
        let val = engine.read_latest(1, b"pk", HlcTimestamp::MAX).unwrap();
        assert_eq!(val.as_deref(), Some(b"data" as &[u8]));
    }

    #[test]
    fn gc_does_not_delete_version_visible_to_active_snapshot() {
        let snaps = Arc::new(Mutex::new(BTreeSet::new()));
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());

        let ts1 = HlcTimestamp {
            wall_ms: 10,
            logical: 0,
        };
        let ts2 = HlcTimestamp {
            wall_ms: 20,
            logical: 0,
        };
        write_version(&engine, 1, b"q", ts1);
        write_version(&engine, 1, b"q", ts2);

        // Register a snapshot with ts = ts1 (wants to read the ts1 version).
        snaps.lock().unwrap().insert(ts1.to_u64());

        let gc = Arc::new(GarbageCollector::new(
            Arc::clone(&engine),
            Arc::clone(&snaps),
        ));
        let stats = gc.run_once(&[1]).unwrap();
        // safe_horizon = ts1; only versions strictly < ts1 can be collected; none here.
        assert_eq!(stats.versions_deleted, 0);

        // Release snapshot.
        snaps.lock().unwrap().remove(&ts1.to_u64());
    }

    #[test]
    fn gc_safe_horizon_is_min_of_active_snapshots() {
        let snaps = Arc::new(Mutex::new(BTreeSet::from([100u64, 200u64, 50u64])));
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let gc = Arc::new(GarbageCollector::new(Arc::clone(&engine), snaps));
        let horizon = gc.safe_horizon();
        assert_eq!(horizon, HlcTimestamp::from_u64(50));
    }
}
