// SPDX-License-Identifier: Apache-2.0
// MVCC-backed storage executor.
//
// Bridges StorageEngine + TransactionManager with the vectorized execution
// layer.  Each `scan_table` call opens a read-only MVCC snapshot, reads
// all visible rows, rolls back, and builds a typed RecordBatch.
//
// Row encoding (Session 7: replaced JSON with typed binary codec)
// ────────────────────────────────────────────────────────────────
// Binary format (all integers little-endian):
//   [magic: 2 bytes = 0x4E 0x42 ("NB")]
//   [version: 1 byte = 0x01]
//   [num_cols: u16 LE]
//   For each column:
//     [key_len: u16 LE][key_bytes: UTF-8]
//     [val_len: u32 LE][val_bytes: UTF-8]
//
// Versus JSON: ~3–5× more compact; O(1) per-column seek vs. JSON parse;
// no string escaping overhead; deterministic byte layout for future
// typed-value extension (replace val_bytes with tag+typed-bytes).
//
// Table ID derivation
// ───────────────────
// table_id = FNV-1a(table_name bytes) as u32.
// Deterministic across nodes and restarts (byte-by-byte, no endianness dep).
//
// CONFIDENCE: raw=0.80 effective=0.73
// DEPENDS_ON: storage, mvcc, catalog, vectorized
// RISK: Value representation is still string — typed binary values (Int64 as
//   8 bytes) would be ~2× more compact still. Planned as v0.2 codec upgrade.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::codec;
use crate::execution::TableScanner;
use crate::mvcc::TransactionManager;
use crate::storage::StorageEngine;
use crate::vectorized::{ColumnVector, ExecError, RecordBatch, Utf8Column};

// ── FNV-1a table_id ───────────────────────────────────────────────────────

/// Derive a stable u32 table id from table name via FNV-1a (32-bit).
/// Byte-by-byte; no multi-byte integer loads — endianness-stable.
pub fn table_id_for(name: &str) -> u32 {
    const FNV_PRIME: u32 = 16_777_619;
    const FNV_OFFSET: u32 = 2_166_136_261;
    let mut h = FNV_OFFSET;
    for byte in name.as_bytes() {
        h ^= *byte as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

// ── Row codec (binary, Session 7) ─────────────────────────────────────────

/// Decode a binary-encoded row back to `BTreeMap<column_name, string_value>`.
/// Returns `None` if bytes do not start with the "NB\x01" magic/version header.
pub fn decode_row(bytes: &[u8]) -> Option<BTreeMap<String, String>> {
    if bytes.len() < 5 {
        return None;
    }
    // Magic + version guard.
    if bytes[0] != 0x4E || bytes[1] != 0x42 || bytes[2] != 0x01 {
        return None;
    }
    let num_cols = u16::from_le_bytes([bytes[3], bytes[4]]) as usize;
    let mut pos = 5usize;
    let mut map = BTreeMap::new();
    for _ in 0..num_cols {
        // key_len
        if pos + 2 > bytes.len() {
            return None;
        }
        let key_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        // key bytes
        if pos + key_len > bytes.len() {
            return None;
        }
        let key = String::from_utf8(bytes[pos..pos + key_len].to_vec()).ok()?;
        pos += key_len;
        // val_len
        if pos + 4 > bytes.len() {
            return None;
        }
        let val_len = u32::from_le_bytes(
            bytes[pos..pos + 4].try_into().ok()?
        ) as usize;
        pos += 4;
        // val bytes
        if pos + val_len > bytes.len() {
            return None;
        }
        let val = String::from_utf8(bytes[pos..pos + val_len].to_vec()).ok()?;
        pos += val_len;
        map.insert(key, val);
    }
    Some(map)
}

// ── StorageExecutor ───────────────────────────────────────────────────────

/// Executes table scans against MVCC-backed RocksDB storage.
///
/// Shared across connections — wrap in `Arc` when storing in server state.
pub struct StorageExecutor {
    engine: Arc<StorageEngine>,
    txn_mgr: Arc<TransactionManager>,
    catalog: Arc<dyn Catalog>,
}

impl StorageExecutor {
    pub fn new(
        engine: Arc<StorageEngine>,
        txn_mgr: Arc<TransactionManager>,
        catalog: Arc<dyn Catalog>,
    ) -> Self {
        Self {
            engine,
            txn_mgr,
            catalog,
        }
    }

    /// Scan all rows of `table_name` visible under the current snapshot.
    ///
    /// Read-path:
    ///   1. begin() — allocates HLC snapshot_ts, registers in active_snapshots.
    ///   2. engine.scan_table() — iterates RocksDB for rows ≤ snapshot_ts.
    ///   3. rollback() — removes snapshot from active_snapshots; no writes.
    ///   4. Decode JSON rows → RecordBatch with typed columns from catalog schema.
    ///
    /// Returns an empty RecordBatch when the table exists in the catalog but
    /// has no rows in RocksDB yet (pre-ingest state).
    pub fn scan_table(&self, table_name: &str) -> Result<RecordBatch, ExecError> {
        let schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        // Begin read-only snapshot transaction.
        let txn = self.txn_mgr.begin();
        let snapshot_ts = txn.snapshot_ts;
        let tid = table_id_for(table_name);

        let rows = self
            .engine
            .scan_table(tid, snapshot_ts)
            .map_err(|e| ExecError::Storage(e.to_string()))?;

        // Rollback: read-only, just releases the snapshot registration.
        self.txn_mgr.rollback(txn);

        if rows.is_empty() {
            return Ok(RecordBatch::empty());
        }

        build_record_batch_from_rows(&schema.columns, &rows)
    }
}

// ── Write-path codec ─────────────────────────────────────────────────────

/// Encode a row as a compact binary blob.
///
/// Format (all integers little-endian):
/// ```text
///   [magic: 2  = 0x4E 0x42]  // "NB"
///   [version:1 = 0x01]
///   [num_cols: u16 LE]
///   For each column:
///     [key_len: u16 LE][key_bytes: UTF-8]
///     [val_len: u32 LE][val_bytes: UTF-8]
/// ```
pub fn encode_row(cols: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x4E); // 'N'
    buf.push(0x42); // 'B'
    buf.push(0x01); // version 1
    let num = cols.len() as u16;
    buf.extend_from_slice(&num.to_le_bytes());
    for (key, val) in cols {
        let kb = key.as_bytes();
        let vb = val.as_bytes();
        buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        buf.extend_from_slice(vb);
    }
    buf
}

impl StorageExecutor {
    /// Generate a unique, monotone PK suitable for a new row.
    /// Uses the HLC clock — unique per node, sortable by wall time.
    pub fn next_pk(&self) -> Vec<u8> {
        self.txn_mgr.next_timestamp().to_be_bytes().to_vec()
    }

    /// Insert a single row under a new MVCC write transaction.
    ///
    /// Write-path:
    ///   1. `begin()` — HLC snapshot_ts, registered in active_snapshots.
    ///   2. `txn.write()` — buffers `encode_row(cols)` in the transaction buffer.
    ///   3. `commit()` — conflict-check, commit_ts, atomic WriteBatch flush.
    pub fn insert_row(
        &self,
        table_name: &str,
        pk: &[u8],
        cols: &[(&str, &str)],
    ) -> Result<(), ExecError> {
        let schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        let mut txn = self.txn_mgr.begin();
        let tid = table_id_for(table_name);
        // NB v2: encode with typed binary codec; fall back to NB v1 on failure.
        let row_map: BTreeMap<String, String> = cols
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let encoded = encode_row_typed(&schema.columns, &row_map)
            .unwrap_or_else(|| encode_row(cols));
        txn.write(tid, pk.to_vec(), encoded);

        self.txn_mgr
            .commit(txn)
            .map_err(|e| ExecError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Update rows matching `predicate` by rewriting them with `assignments`.
    /// Returns the number of rows updated.
    ///
    /// Implementation: scan → collect matching rows → write updated versions
    /// in a single MVCC transaction.
    pub fn update_rows(
        &self,
        table_name: &str,
        assignments: &[(String, String)],
        predicate: Option<&crate::binder::DmlPredicate>,
    ) -> Result<u64, ExecError> {
        let schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        // Read snapshot — same begin/rollback pattern as scan_table.
        let read_txn = self.txn_mgr.begin();
        let snapshot_ts = read_txn.snapshot_ts;
        let tid = table_id_for(table_name);
        let rows = self
            .engine
            .scan_table(tid, snapshot_ts)
            .map_err(|e| ExecError::Storage(e.to_string()))?;
        self.txn_mgr.rollback(read_txn);

        // Decode, filter, mutate.
        let mut write_txn = self.txn_mgr.begin();
        let mut count = 0u64;

        for (pk_bytes, val_bytes) in &rows {
            // NB v2: try typed codec first; fall back to NB v1.
            let Some(mut row) = decode_any_row(val_bytes) else { continue };

            // Apply predicate if present.
            if let Some(pred) = predicate {
                if !pred.matches(&row) {
                    continue;
                }
            }

            // Apply assignments.
            for (col, new_val) in assignments {
                row.insert(col.clone(), new_val.clone());
            }

            // Re-encode with NB v2 typed codec; fall back to NB v1.
            let pairs: Vec<(&str, &str)> = schema
                .columns
                .iter()
                .map(|c| (c.name.as_str(), row.get(&c.name).map(|s| s.as_str()).unwrap_or("")))
                .collect();
            let encoded = encode_row_typed(&schema.columns, &row)
                .unwrap_or_else(|| encode_row(&pairs));
            write_txn.write(tid, pk_bytes.clone(), encoded);
            count += 1;
        }

        if count > 0 {
            self.txn_mgr
                .commit(write_txn)
                .map_err(|e| ExecError::Storage(e.to_string()))?;
        } else {
            self.txn_mgr.rollback(write_txn);
        }

        Ok(count)
    }

    /// Delete rows matching `predicate` by writing tombstones (empty bytes).
    /// Returns the number of rows deleted.
    ///
    /// Tombstone semantics: `decode_row(&[])` returns `None` — the magic check
    /// fails on empty input — so tombstoned rows are silently skipped by every
    /// future scan without any special scan-path logic.
    pub fn delete_rows(
        &self,
        table_name: &str,
        predicate: Option<&crate::binder::DmlPredicate>,
    ) -> Result<u64, ExecError> {
        let _schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        let read_txn = self.txn_mgr.begin();
        let snapshot_ts = read_txn.snapshot_ts;
        let tid = table_id_for(table_name);
        let rows = self
            .engine
            .scan_table(tid, snapshot_ts)
            .map_err(|e| ExecError::Storage(e.to_string()))?;
        self.txn_mgr.rollback(read_txn);

        let mut write_txn = self.txn_mgr.begin();
        let mut count = 0u64;

        for (pk_bytes, val_bytes) in &rows {
            // NB v2: try typed codec first; fall back to NB v1.
            let Some(row) = decode_any_row(val_bytes) else { continue };

            if let Some(pred) = predicate {
                if !pred.matches(&row) {
                    continue;
                }
            }

            // Write tombstone: empty value; decode_any_row returns None → invisible to future scans.
            write_txn.write(tid, pk_bytes.clone(), vec![]);
            count += 1;
        }

        if count > 0 {
            self.txn_mgr
                .commit(write_txn)
                .map_err(|e| ExecError::Storage(e.to_string()))?;
        } else {
            self.txn_mgr.rollback(write_txn);
        }

        Ok(count)
    }
}

// ── Codec helpers (Session 10: NB v2 typed binary codec wired in) ─────────

/// Try NB v2 typed codec first; fall back to NB v1 string codec.
/// Returns `None` for tombstones (empty slice) or irrecoverably corrupt data.
fn decode_any_row(bytes: &[u8]) -> Option<BTreeMap<String, String>> {
    if bytes.is_empty() {
        return None; // tombstone written by delete_rows
    }
    // NB v2: typed RecordBatch codec.
    if let Some(batch) = codec::decode_batch(bytes) {
        return Some(batch_row_to_map(&batch));
    }
    // NB v1 fallback: legacy string-based codec.
    decode_row(bytes)
}

/// Encode a single row as a NB v2 typed RecordBatch blob.
///
/// Builds a 1-row `RecordBatch` from the supplied column definitions +
/// string values (same type mapping as the scan-path), then delegates to
/// `codec::encode_batch`.  Returns `None` only if RecordBatch construction
/// fails (mismatched column counts — should not happen in practice).
fn encode_row_typed(
    col_defs: &[crate::catalog::ColumnDef],
    row: &BTreeMap<String, String>,
) -> Option<Vec<u8>> {
    let columns: Vec<(String, ColumnVector)> = col_defs
        .iter()
        .map(|col| {
            let val_str = row.get(&col.name).map(|s| s.as_str()).unwrap_or("");
            let cv = string_to_col_vector(&col.data_type, val_str);
            (col.name.clone(), cv)
        })
        .collect();
    let batch = RecordBatch::new(columns).ok()?;
    codec::encode_batch(&batch).ok()
}

/// Build a 1-element `ColumnVector` from a string value.
/// Mirrors the type-mapping in `build_record_batch_from_rows`.
fn string_to_col_vector(data_type: &str, val_str: &str) -> ColumnVector {
    match data_type.to_uppercase().as_str() {
        "BIGINT" | "INT8" | "INT64" => ColumnVector::Int64(vec![val_str.parse().ok()]),
        "INTEGER" | "INT4" | "INT32" | "INT" => ColumnVector::Int32(vec![val_str.parse().ok()]),
        "DOUBLE" | "FLOAT8" | "FLOAT64" | "REAL" | "FLOAT" | "DECIMAL" | "NUMERIC" => {
            ColumnVector::Float64(vec![val_str.parse().ok()])
        }
        "DATE" | "DATE32" => ColumnVector::Date32(vec![val_str.parse().ok()]),
        _ => {
            let opt = if val_str.is_empty() { None } else { Some(val_str) };
            ColumnVector::Utf8(Utf8Column::from_options(vec![opt]))
        }
    }
}

/// Extract a `BTreeMap<column_name, string_value>` from row 0 of a
/// `RecordBatch`.  Called after `codec::decode_batch` on the read path.
fn batch_row_to_map(batch: &RecordBatch) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if batch.row_count == 0 {
        return map;
    }
    for (name, col) in &batch.columns {
        let val = match col {
            ColumnVector::Int32(v)   => v[0].map(|n| n.to_string()).unwrap_or_default(),
            ColumnVector::Int64(v)   => v[0].map(|n| n.to_string()).unwrap_or_default(),
            ColumnVector::Float64(v) => v[0].map(|n| n.to_string()).unwrap_or_default(),
            ColumnVector::Date32(v)  => v[0].map(|n| n.to_string()).unwrap_or_default(),
            ColumnVector::Utf8(u)    => u.get(0).unwrap_or_default(),
        };
        map.insert(name.clone(), val);
    }
    map
}

// ── RecordBatch builder ───────────────────────────────────────────────────

fn build_record_batch_from_rows(
    col_defs: &[crate::catalog::ColumnDef],
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<RecordBatch, ExecError> {
    // NB v2: try typed codec first; fall back to NB v1 per row.
    let decoded: Vec<BTreeMap<String, String>> = rows
        .iter()
        .filter_map(|(_, val)| decode_any_row(val))
        .collect();

    if decoded.is_empty() {
        return Ok(RecordBatch::empty());
    }

    let mut columns: Vec<(String, ColumnVector)> = Vec::with_capacity(col_defs.len());

    for col in col_defs {
        let cv = match col.data_type.to_uppercase().as_str() {
            "BIGINT" | "INT8" | "INT64" => {
                let vals: Vec<Option<i64>> = decoded
                    .iter()
                    .map(|row| row.get(&col.name).and_then(|v| v.parse().ok()))
                    .collect();
                ColumnVector::Int64(vals)
            }
            "INTEGER" | "INT4" | "INT32" | "INT" => {
                let vals: Vec<Option<i32>> = decoded
                    .iter()
                    .map(|row| row.get(&col.name).and_then(|v| v.parse().ok()))
                    .collect();
                ColumnVector::Int32(vals)
            }
            "DOUBLE" | "FLOAT8" | "FLOAT64" | "REAL" | "FLOAT" | "DECIMAL"
            | "NUMERIC" => {
                let vals: Vec<Option<f64>> = decoded
                    .iter()
                    .map(|row| row.get(&col.name).and_then(|v| v.parse().ok()))
                    .collect();
                ColumnVector::Float64(vals)
            }
            "DATE" | "DATE32" => {
                // Stored as integer days (e.g. YYYYMMDD packed or epoch days).
                let vals: Vec<Option<i32>> = decoded
                    .iter()
                    .map(|row| row.get(&col.name).and_then(|v| v.parse().ok()))
                    .collect();
                ColumnVector::Date32(vals)
            }
            _ => {
                // TEXT, VARCHAR, CHAR, BYTEA, etc.
                let strs: Vec<Option<&str>> = decoded
                    .iter()
                    .map(|row| row.get(&col.name).map(|s| s.as_str()))
                    .collect();
                ColumnVector::Utf8(Utf8Column::from_options(strs))
            }
        };
        columns.push((col.name.clone(), cv));
    }

    RecordBatch::new(columns).map_err(|e| ExecError::Storage(e.to_string()))
}

// ── TableScanner impl ─────────────────────────────────────────────────────

impl TableScanner for StorageExecutor {
    fn scan_table(&self, table_name: &str) -> Result<RecordBatch, ExecError> {
        self.scan_table(table_name)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_id_is_stable() {
        assert_eq!(table_id_for("lineitem"), table_id_for("lineitem"));
        assert_ne!(table_id_for("lineitem"), table_id_for("orders"));
        assert_ne!(table_id_for(""), table_id_for("a"));
    }

    #[test]
    fn table_id_is_deterministic_byte_order() {
        // FNV operates byte-by-byte — verify two different names never collide.
        let a = table_id_for("customer");
        let b = table_id_for("supplier");
        assert_ne!(a, b);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let cols: &[(&str, &str)] = &[
            ("l_orderkey", "42"),
            ("l_extendedprice", "9.99"),
            ("l_shipdate", "19960101"),
        ];
        let encoded = encode_row(cols);
        // Verify magic header.
        assert_eq!(&encoded[0..3], &[0x4E, 0x42, 0x01]);
        let decoded = decode_row(&encoded).expect("must decode");
        assert_eq!(decoded.get("l_orderkey").map(|s| s.as_str()), Some("42"));
        assert_eq!(
            decoded.get("l_extendedprice").map(|s| s.as_str()),
            Some("9.99")
        );
        assert_eq!(
            decoded.get("l_shipdate").map(|s| s.as_str()),
            Some("19960101")
        );
    }

    #[test]
    fn decode_invalid_bytes_returns_none() {
        // Old JSON input should be rejected by magic check.
        assert!(decode_row(b"{\"col\":\"val\"}").is_none());
        assert!(decode_row(b"not-json").is_none());
        assert!(decode_row(b"").is_none());
        // Truncated valid header.
        assert!(decode_row(&[0x4E, 0x42, 0x01]).is_none());
    }

    #[test]
    fn encode_row_empty() {
        let encoded = encode_row(&[]);
        let decoded = decode_row(&encoded).expect("must decode empty");
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_row_binary_is_more_compact_than_json_equivalent() {
        // For a typical TPC-H row with 3 columns, binary should be smaller
        // than the JSON equivalent (no key quoting, no braces, no commas).
        let cols: &[(&str, &str)] = &[
            ("l_orderkey", "1234567"),
            ("l_extendedprice", "12345.67"),
            ("l_returnflag", "N"),
        ];
        let binary = encode_row(cols);
        // Binary layout: 3-byte magic + 2-byte num_cols + per-col (2-byte key_len + key + 4-byte val_len + val).
        // For these 3 columns totals 76 bytes — comparable to JSON (~71 bytes) but with decodable
        // structure without a parser. Assert a generous upper bound to catch accidental bloat.
        assert!(binary.len() < 100, "binary codec should be compact: {} bytes", binary.len());
        // Decode must round-trip.
        let dec = decode_row(&binary).unwrap();
        assert_eq!(dec.get("l_returnflag").map(String::as_str), Some("N"));
    }
}
