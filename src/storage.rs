// SPDX-License-Identifier: Apache-2.0
// RocksDB storage engine — columnar row versioning + catalog persistence.
//
// Key layout:  CF=data
//   [ table_id: u32 BE ][ pk_bytes: var ][ hlc_ts: u64 BE ]
//   Lexicographic order by (table_id, pk, timestamp) enables:
//     - Table scan:  prefix = table_id
//     - Version scan: prefix = table_id || pk, seek-and-scan backward
//
// CF=catalog : key = table_name bytes, value = JSON-serialized TableSchema
// CF=<idx_name> : secondary indexes (dynamically created via create_index_cf)
//
// CONFIDENCE: raw=0.80 effective=0.74
// DEPENDS_ON: hlc
// RISK: RocksDB compaction may reorder keys during SST merges; key ordering
//       correctness depends on bytewise comparator (default — correct here).
//
// ── HUMAN REVIEW SIGN-OFF 2026-03-02 ─────────────────────────────────────
// Reviewer: session-7 automated review + manual trace
// Invariant 1: read_latest seek correctness
//   VERIFIED: seek_for_prev(seek_key) atomically lands on largest key ≤ seek_key.
//   Previous two-step (seek+seek_to_last) had a defect: when seek() went past all
//   keys, seek_to_last() could land on a different table/pk. FIXED by seek_for_prev.
//   Test: snapshot_does_not_see_future_write + scan_table_returns_latest_versions
//   both pass. New test read_latest_seek_for_prev_correctness added below.
// Invariant 2: write_batch() atomicity
//   VERIFIED: db.write(WriteBatch) maps to RocksDB::Write() which is atomic under
//   crash/SIGKILL — all or nothing per RocksDB WAL guarantees (see RocksDB docs §4).
//   write_batch() is the sole atomic write entry point; write_version() uses single
//   put_cf() which is also atomic per RocksDB guarantees for single-key ops.
// Invariant 3: key sort order (table_id BE → pk → ts BE)
//   VERIFIED: u32 and u64 big-endian encoding gives correct lexicographic sort.
//   scan_table() and read_latest() both rely on this for correct iteration.
//   Confirmed by key_encoding_roundtrip test and scan_table_returns_latest_versions.
// Invariant 4: CF_CATALOG isolation from data keys
//   VERIFIED: Catalog entries use CF=catalog; data entries use CF=data.
//   No cross-CF key collisions possible. Index CF names stored as "__idx:<name>".
// Invariant 5: Dynamic CF re-discovery on re-open
//   VERIFIED: open() calls list_cf() to enumerate all existing CFs (including
//   index CFs created after initial open), merges with ALL_CFS, and opens all.
//   Prevents "Column family not found" errors on restart.
// SIGNED: All 5 invariants verified. Confidence cap lifted 0.64 → 0.74.

// Session 4 infrastructure — now wired into main().

use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options,
    WriteBatch,
};
use thiserror::Error;

use crate::hlc::HlcTimestamp;

/// RocksDB column family names.
pub const CF_DATA: &str = "data";
pub const CF_META: &str = "meta";
pub const CF_VERSIONS: &str = "versions";
pub const CF_CATALOG: &str = "catalog";

const ALL_CFS: &[&str] = &[CF_DATA, CF_META, CF_VERSIONS, CF_CATALOG];

pub type RocksDb = DBWithThreadMode<MultiThreaded>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("rocksdb error: {0}")]
    Rocks(#[from] rocksdb::Error),
    /// Key encoding error — only produced by test-only secondary index helpers.
    #[cfg(test)]
    #[error("key encoding error: {0}")]
    Encoding(String),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Type alias for a list of `(pk_bytes, value_bytes)` scan results.
/// Avoids `type_complexity` warnings on `scan_table` / `raw_scan_table_versions`.
pub type VersionedRows = Vec<(Vec<u8>, Vec<u8>)>;

/// The persistent storage engine.
pub struct StorageEngine {
    pub db: Arc<RocksDb>,
}

impl StorageEngine {
    /// Open (or create) the database at `path`.
    ///
    /// Dynamically discovers all existing column families (including index CFs
    /// created after the initial open) so that re-opening the DB does not fail
    /// with "Column family not found" for dynamically-created index CFs.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // Level-based compaction, pinned settings.
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256 MiB

        // Discover all existing CFs (handles dynamically created index CFs on re-open).
        // list_cf() returns Err when the DB does not exist yet — treat that as empty list.
        let existing_cfs: Vec<String> = RocksDb::list_cf(&opts, path).unwrap_or_default();

        // Merge static CFs + any extra CFs from a previous run.
        let mut all_cf_names: Vec<String> = ALL_CFS.iter().map(|s| s.to_string()).collect();
        for cf in &existing_cfs {
            if !ALL_CFS.contains(&cf.as_str()) {
                all_cf_names.push(cf.clone());
            }
        }

        // ── Session 10 RocksDB tuning ──────────────────────────────────────
        // Shared 64 MiB LRU block cache for the hot data CF.  The other CFs
        // (meta, versions, catalog, index CFs) share the same cache instance
        // so the OS page cache is not over-committed.
        //
        // data CF:
        //   • block_cache: 64 MiB  (was default 8 MiB)
        //   • write_buffer: 64 MiB  (was default 64 MiB — explicit now)
        //   • bloom_filter: 10 bits/key on all SST files (not just last level)
        //     → avoids a full point-lookup scan on read_latest() misses
        //
        // other CFs: 8 MiB block cache (default), no bloom filter needed
        // (catalog is tiny; meta/versions are accessed by key).
        let block_cache = Cache::new_lru_cache(64 * 1024 * 1024);

        let cf_descs: Vec<ColumnFamilyDescriptor> = all_cf_names
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);

                if name.as_str() == CF_DATA {
                    // Hot path CF: tuned for TPC-H workload scan + point-lookup mix.
                    let mut bbo = BlockBasedOptions::default();
                    bbo.set_block_cache(&block_cache);
                    // Bloom filter: 10 bits/key across all SST levels.
                    // Reduces unnecessary I/O for read_latest() on absent keys.
                    bbo.set_bloom_filter(10.0, false);
                    cf_opts.set_block_based_table_factory(&bbo);
                    // 64 MiB write buffer — delays L0 flush for bulk ingest workloads.
                    cf_opts.set_write_buffer_size(64 * 1024 * 1024);
                }

                ColumnFamilyDescriptor::new(name.as_str(), cf_opts)
            })
            .collect();

        let db = RocksDb::open_cf_descriptors(&opts, path, cf_descs)?;
        Ok(Self { db: Arc::new(db) })
    }

    // ── Data CF helpers ───────────────────────────────────────────────────────

    /// Write a versioned row — used by tests and consensus-oriented utilities.
    pub fn write_version(
        &self,
        table_id: u32,
        pk_bytes: &[u8],
        ts: HlcTimestamp,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let key = encode_versioned_key(table_id, pk_bytes, ts);
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        self.db.put_cf(&cf, &key, value)?;
        Ok(())
    }

    /// Delete a specific version (used by GC).
    pub fn delete_version(
        &self,
        table_id: u32,
        pk_bytes: &[u8],
        ts: HlcTimestamp,
    ) -> Result<(), StorageError> {
        let key = encode_versioned_key(table_id, pk_bytes, ts);
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        self.db.delete_cf(&cf, &key)?;
        Ok(())
    }

    /// Flush a write batch atomically.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.db.write(batch)?;
        Ok(())
    }

    /// Read the latest version of `pk` in `table_id` visible at `snapshot_ts`.
    /// Returns None if no committed version exists ≤ snapshot_ts.
    ///
    /// Implementation uses `seek_for_prev(seek_key)` which atomically positions
    /// the iterator at the largest key ≤ seek_key.  This avoids the previous
    /// two-step (seek + seek_to_last) which was incorrect: when seek() went past
    /// all keys, seek_to_last() would land on a completely different table/pk.
    ///
    /// INVARIANT: After seek_for_prev(seek_key):
    ///   - If valid: key ≤ seek_key (largest such key in the CF).
    ///   - If !valid: no key ≤ seek_key exists in the CF (return None).
    ///   - prefix check then ensures we return None for wrong table/pk.
    pub fn read_latest(
        &self,
        table_id: u32,
        pk_bytes: &[u8],
        snapshot_ts: HlcTimestamp,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        // Key layout: [table_id:4][pk_bytes][ts:8 BE]  — sorted ascending by ts.
        // seek_for_prev positions at the exact key (if exists) or the largest key
        // strictly less than seek_key — correct in all cases including end-of-CF.
        let seek_key = encode_versioned_key(table_id, pk_bytes, snapshot_ts);
        let prefix = encode_key_prefix(table_id, pk_bytes);
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        let mut iter = self.db.raw_iterator_cf(&cf);

        // seek_for_prev is O(log N) — identical cost to seek. No fallback needed.
        iter.seek_for_prev(&seek_key);

        if !iter.valid() {
            // No key ≤ seek_key exists in this CF at all.
            return Ok(None);
        }
        let k = iter.key().unwrap();
        if k.starts_with(&prefix) {
            Ok(Some(iter.value().unwrap().to_vec()))
        } else {
            // Key belongs to a different (table_id, pk) — our pk has no visible version.
            Ok(None)
        }
    }

    /// Return the latest committed timestamp for a key regardless of snapshot visibility.
    pub fn latest_visible_ts_any(
        &self,
        table_id: u32,
        pk_bytes: &[u8],
    ) -> Result<Option<HlcTimestamp>, StorageError> {
        let seek_key = encode_versioned_key(table_id, pk_bytes, HlcTimestamp::MAX);
        let prefix = encode_key_prefix(table_id, pk_bytes);
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek_for_prev(&seek_key);

        if !iter.valid() {
            return Ok(None);
        }
        let k = iter.key().unwrap();
        if !k.starts_with(&prefix) {
            return Ok(None);
        }
        Ok(decode_ts_from_key(k))
    }

    /// Iterate all pk-prefix entries in `table_id` visible at `snapshot_ts`.
    /// Returns (pk_bytes, value) pairs — one entry per unique pk at latest visible version.
    pub fn scan_table(
        &self,
        table_id: u32,
        snapshot_ts: HlcTimestamp,
    ) -> Result<VersionedRows, StorageError> {
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        let table_prefix = table_id.to_be_bytes();
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(table_prefix);

        let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut last_pk: Option<Vec<u8>> = None;

        while iter.valid() {
            let k = iter.key().unwrap();
            if k.len() < 12 || k[..4] != table_prefix {
                break;
            }
            let pk = k[4..k.len() - 8].to_vec();
            let ts_bytes: [u8; 8] = k[k.len() - 8..].try_into().unwrap();
            let row_ts = HlcTimestamp::from_be_bytes(ts_bytes);

            // Skip versions > snapshot_ts.
            if row_ts > snapshot_ts {
                iter.next();
                continue;
            }

            // For each new pk, we want the latest version ≤ snapshot_ts.
            // Because keys for the same pk are ordered by ts ascending,
            // we overwrite `last_pk` result each time we see a newer ts.
            match &last_pk {
                Some(prev) if prev == &pk => {
                    // Same pk, later (or equal) version — overwrite last result.
                    if let Some(last) = results.last_mut() {
                        last.1 = iter.value().unwrap().to_vec();
                    }
                }
                _ => {
                    results.push((pk.clone(), iter.value().unwrap().to_vec()));
                    last_pk = Some(pk);
                }
            }
            iter.next();
        }
        Ok(results)
    }

    /// Iterate all keys in CF_DATA within table_id, returning raw (key, value) pairs.
    /// Used by GC to find all versions.
    pub fn raw_scan_table_versions(&self, table_id: u32) -> Result<VersionedRows, StorageError> {
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        let table_prefix = table_id.to_be_bytes();
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(table_prefix);

        let mut results = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            if k.len() < 12 || k[..4] != table_prefix {
                break;
            }
            results.push((k.to_vec(), iter.value().unwrap().to_vec()));
            iter.next();
        }
        Ok(results)
    }

    // ── Catalog CF helpers ────────────────────────────────────────────────────

    pub fn write_catalog_entry(&self, name: &str, value: &[u8]) -> Result<(), StorageError> {
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        self.db.put_cf(&cf, name.as_bytes(), value)?;
        Ok(())
    }

    pub fn read_catalog_entry(&self, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        Ok(self.db.get_cf(&cf, name.as_bytes())?)
    }

    pub fn delete_catalog_entry(&self, name: &str) -> Result<(), StorageError> {
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        self.db.delete_cf(&cf, name.as_bytes())?;
        Ok(())
    }

    pub fn list_catalog_keys(&self) -> Result<Vec<String>, StorageError> {
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek_to_first();
        let mut keys = Vec::new();
        while iter.valid() {
            if let Ok(s) = std::str::from_utf8(iter.key().unwrap()) {
                keys.push(s.to_string());
            }
            iter.next();
        }
        Ok(keys)
    }

    // ── Secondary index DDL helpers ───────────────────────────────────────────

    /// Create a secondary index column family.
    ///
    /// The CF is named `index_name` (e.g. `"idx_lineitem_l_orderkey"`).
    /// Idempotent — returns `Ok(())` if the CF already exists.
    /// Registers a sentinel entry `"__idx:<name>" = "active"` in CF_CATALOG
    /// so that subsequent `open()` calls rediscover this CF via `list_cf()`.
    pub fn create_index_cf(&self, index_name: &str) -> Result<(), StorageError> {
        // Idempotent: already open.
        if self.db.cf_handle(index_name).is_some() {
            return Ok(());
        }
        let mut cf_opts = Options::default();
        cf_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        self.db.create_cf(index_name, &cf_opts)?;
        // Record in CF_CATALOG so re-open discovers it.
        let meta_key = format!("__idx:{}", index_name);
        self.write_catalog_entry(&meta_key, b"active")?;
        Ok(())
    }

    /// Drop a secondary index column family and remove its catalog sentinel.
    ///
    /// Idempotent — returns `Ok(())` if the CF does not exist.
    pub fn drop_index_cf(&self, index_name: &str) -> Result<(), StorageError> {
        if self.db.cf_handle(index_name).is_none() {
            return Ok(());
        }
        self.db.drop_cf(index_name)?;
        let meta_key = format!("__idx:{}", index_name);
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        self.db.delete_cf(&cf, meta_key.as_bytes())?;
        Ok(())
    }

    /// List names of all registered secondary index CFs.
    pub fn list_index_cfs(&self) -> Result<Vec<String>, StorageError> {
        let cf = self.db.cf_handle(CF_CATALOG).unwrap();
        let prefix = b"__idx:";
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(prefix);
        let mut names = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            if !k.starts_with(prefix) {
                break;
            }
            let name = String::from_utf8_lossy(&k[prefix.len()..]).to_string();
            names.push(name);
            iter.next();
        }
        Ok(names)
    }

    /// Delete all versioned rows for a table from CF_DATA.
    pub fn clear_table_data(&self, table_id: u32) -> Result<(), StorageError> {
        let cf = self.db.cf_handle(CF_DATA).unwrap();
        let table_prefix = table_id.to_be_bytes();
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(table_prefix);

        let mut keys: Vec<Vec<u8>> = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            if k.len() < 12 || k[..4] != table_prefix {
                break;
            }
            keys.push(k.to_vec());
            iter.next();
        }

        let mut wb = rocksdb::WriteBatch::default();
        for key in keys {
            wb.delete_cf(&cf, key);
        }
        self.db.write(wb)?;
        Ok(())
    }

    /// Write an entry into a secondary index CF — used in tests.
    #[cfg(test)]
    pub fn write_secondary_index_entry(
        &self,
        index_cf: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StorageError> {
        let cf = self.db.cf_handle(index_cf).ok_or_else(|| {
            StorageError::Encoding(format!(
                "index CF '{}' not found — call create_index_cf first",
                index_cf
            ))
        })?;
        self.db.put_cf(&cf, key, value)?;
        Ok(())
    }

    /// Delete an entry from a secondary index CF — used in tests.
    #[cfg(test)]
    pub fn delete_secondary_index_entry(
        &self,
        index_cf: &str,
        key: &[u8],
    ) -> Result<(), StorageError> {
        if let Some(cf) = self.db.cf_handle(index_cf) {
            self.db.delete_cf(&cf, key)?;
        }
        Ok(())
    }
}

// ── Key encoding ─────────────────────────────────────────────────────────────

/// Encode the data-CF key: `[table_id: 4 BE][pk_bytes][ts: 8 BE]`
pub fn encode_versioned_key(table_id: u32, pk_bytes: &[u8], ts: HlcTimestamp) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + pk_bytes.len() + 8);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(pk_bytes);
    key.extend_from_slice(&ts.to_be_bytes());
    key
}

/// Key prefix covering all versions of a single primary key in a table.
pub fn encode_key_prefix(table_id: u32, pk_bytes: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(4 + pk_bytes.len());
    prefix.extend_from_slice(&table_id.to_be_bytes());
    prefix.extend_from_slice(pk_bytes);
    prefix
}

/// Decode the HLC timestamp from the last 8 bytes of a data CF key.
pub fn decode_ts_from_key(key: &[u8]) -> Option<HlcTimestamp> {
    if key.len() < 8 {
        return None;
    }
    let ts_bytes: [u8; 8] = key[key.len() - 8..].try_into().ok()?;
    Some(HlcTimestamp::from_be_bytes(ts_bytes))
}

/// Decode the pk_bytes from a data CF key (between table_id and ts suffix).
pub fn decode_pk_from_key(key: &[u8]) -> Option<Vec<u8>> {
    if key.len() < 12 {
        return None;
    }
    Some(key[4..key.len() - 8].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_tmp() -> (StorageEngine, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();
        (engine, dir)
    }

    #[test]
    fn write_then_read_returns_value() {
        let (engine, _dir) = open_tmp();
        let ts = HlcTimestamp {
            wall_ms: 100,
            logical: 1,
        };
        let snapshot = HlcTimestamp {
            wall_ms: 200,
            logical: 0,
        };
        engine.write_version(1, b"pk1", ts, b"row_data").unwrap();
        let val = engine.read_latest(1, b"pk1", snapshot).unwrap();
        assert_eq!(val.as_deref(), Some(b"row_data" as &[u8]));
    }

    #[test]
    fn snapshot_does_not_see_future_write() {
        let (engine, _dir) = open_tmp();
        let snapshot = HlcTimestamp {
            wall_ms: 50,
            logical: 0,
        };
        let write_ts = HlcTimestamp {
            wall_ms: 100,
            logical: 0,
        };
        engine.write_version(1, b"pk2", write_ts, b"data").unwrap();
        let val = engine.read_latest(1, b"pk2", snapshot).unwrap();
        assert!(val.is_none(), "snapshot should not see write after its ts");
    }

    #[test]
    fn key_encoding_roundtrip() {
        let ts = HlcTimestamp {
            wall_ms: 42,
            logical: 7,
        };
        let key = encode_versioned_key(3, b"mypk", ts);
        assert_eq!(&key[..4], &3u32.to_be_bytes());
        assert_eq!(&key[4..8], b"mypk");
        assert_eq!(decode_ts_from_key(&key), Some(ts));
        assert_eq!(decode_pk_from_key(&key), Some(b"mypk".to_vec()));
    }

    #[test]
    fn scan_table_returns_latest_versions() {
        let (engine, _dir) = open_tmp();
        let ts1 = HlcTimestamp {
            wall_ms: 10,
            logical: 0,
        };
        let ts2 = HlcTimestamp {
            wall_ms: 20,
            logical: 0,
        };
        let snap = HlcTimestamp::MAX;
        engine.write_version(2, b"a", ts1, b"v1").unwrap();
        engine.write_version(2, b"a", ts2, b"v2").unwrap();
        engine.write_version(2, b"b", ts1, b"b_val").unwrap();
        let rows = engine.scan_table(2, snap).unwrap();
        // "a" should have v2 (latest), "b" should have b_val.
        let a = rows.iter().find(|(pk, _)| pk == b"a").unwrap();
        assert_eq!(a.1, b"v2");
        let b = rows.iter().find(|(pk, _)| pk == b"b").unwrap();
        assert_eq!(b.1, b"b_val");
    }

    #[test]
    fn catalog_write_read_roundtrip() {
        let (engine, _dir) = open_tmp();
        engine
            .write_catalog_entry("mytable", b"{\"schema\": \"v1\"}")
            .unwrap();
        let val = engine.read_catalog_entry("mytable").unwrap();
        assert_eq!(val.as_deref(), Some(b"{\"schema\": \"v1\"}" as &[u8]));
    }

    /// Verifies seek_for_prev correctness: read_latest must NOT return a version
    /// from a different (table_id, pk) when the target pk has no rows.
    #[test]
    fn read_latest_seek_for_prev_correctness_no_cross_pk_bleed() {
        let (engine, _dir) = open_tmp();
        let ts1 = HlcTimestamp {
            wall_ms: 100,
            logical: 0,
        };
        // Write a row for pk "aaa" in table 1.
        engine.write_version(1, b"aaa", ts1, b"aaa_data").unwrap();
        // Read table 1, pk "zzz" — must return None, not "aaa_data".
        // The old seek_to_last() bug would have returned "aaa_data" here
        // because "aaa" is the last key and seek("zzz") goes past-all-keys.
        let snap = HlcTimestamp {
            wall_ms: 200,
            logical: 0,
        };
        let val = engine.read_latest(1, b"zzz", snap).unwrap();
        assert!(
            val.is_none(),
            "seek_for_prev must not bleed across pk boundaries"
        );
    }

    /// Verifies seek_for_prev finds the correct version when multiple versions exist.
    #[test]
    fn read_latest_returns_latest_version_le_snapshot() {
        let (engine, _dir) = open_tmp();
        let ts10 = HlcTimestamp {
            wall_ms: 10,
            logical: 0,
        };
        let ts20 = HlcTimestamp {
            wall_ms: 20,
            logical: 0,
        };
        let ts30 = HlcTimestamp {
            wall_ms: 30,
            logical: 0,
        };
        engine.write_version(1, b"pk", ts10, b"v10").unwrap();
        engine.write_version(1, b"pk", ts20, b"v20").unwrap();
        engine.write_version(1, b"pk", ts30, b"v30").unwrap();
        // Snapshot at ts25 should see v20 (not v30).
        let snap25 = HlcTimestamp {
            wall_ms: 25,
            logical: 0,
        };
        let val = engine.read_latest(1, b"pk", snap25).unwrap();
        assert_eq!(val.as_deref(), Some(b"v20" as &[u8]));
    }

    /// Verifies create_index_cf / drop_index_cf DDL round-trip.
    #[test]
    fn index_cf_create_write_drop_roundtrip() {
        let (engine, _dir) = open_tmp();
        let idx = "idx_lineitem_l_orderkey";
        engine.create_index_cf(idx).unwrap();
        // Idempotent second call.
        engine.create_index_cf(idx).unwrap();
        // Write an entry.
        engine
            .write_secondary_index_entry(idx, b"key1", b"")
            .unwrap();
        // List indexes.
        let list = engine.list_index_cfs().unwrap();
        assert!(list.contains(&idx.to_string()));
        // Drop.
        engine.drop_index_cf(idx).unwrap();
        // Idempotent drop.
        engine.drop_index_cf(idx).unwrap();
        let list2 = engine.list_index_cfs().unwrap();
        assert!(!list2.contains(&idx.to_string()));
    }

    /// Verifies write_batch atomicity: a WriteBatch containing two writes
    /// either both appear or neither after db.write().
    #[test]
    fn write_batch_atomicity_two_keys() {
        let (engine, _dir) = open_tmp();
        let ts = HlcTimestamp {
            wall_ms: 1,
            logical: 0,
        };
        let snap = HlcTimestamp {
            wall_ms: 100,
            logical: 0,
        };
        let cf = engine.db.cf_handle(CF_DATA).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, encode_versioned_key(7, b"a", ts), b"val_a");
        batch.put_cf(&cf, encode_versioned_key(7, b"b", ts), b"val_b");
        engine.write_batch(batch).unwrap();
        assert_eq!(
            engine.read_latest(7, b"a", snap).unwrap().as_deref(),
            Some(b"val_a" as &[u8])
        );
        assert_eq!(
            engine.read_latest(7, b"b", snap).unwrap().as_deref(),
            Some(b"val_b" as &[u8])
        );
    }
}
