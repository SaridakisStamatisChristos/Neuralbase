// SPDX-License-Identifier: Apache-2.0
//! Deterministic, versioned logical snapshots for replicated SQL state.
//!
//! This module intentionally defines only the snapshot artifact and its codec.
//! It does not wire snapshots into Raft yet. A snapshot captures one logical SQL
//! state-machine point: Raft inclusion metadata, the durable replicated-SQL
//! apply marker/HLC floor, canonical catalog schemas, and exact latest row bytes.
//! Restore code must consume these concrete bytes rather than re-plan SQL.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::catalog::{ColumnDef, TableSchema};
use crate::storage_executor::table_id_for;

const MAGIC: &[u8; 4] = b"NBSN";
pub const REPLICATED_SNAPSHOT_VERSION: u8 = 1;
pub const MAX_REPLICATED_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;
const CHECKSUM_BYTES: usize = 32;
const MAX_TABLES: usize = 100_000;
const MAX_COLUMNS_PER_TABLE: usize = 4_096;
const MAX_ROWS_PER_TABLE: usize = 10_000_000;
const MAX_FIELD_BYTES: usize = 64 * 1024 * 1024;
const MAX_EXTENSION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// Highest Raft log index represented by this snapshot.
    pub last_included_index: u64,
    /// Term of `last_included_index`.
    pub last_included_term: u64,
    /// Highest durable replicated-SQL apply marker represented by the snapshot.
    /// This may be lower than `last_included_index` because Raft can contain
    /// non-SQL entries such as readiness/barrier commands.
    pub latest_sql_apply_index: u64,
    /// Highest leader-selected replicated SQL commit timestamp represented by
    /// the snapshot. Zero is valid when no replicated DML has committed yet.
    pub latest_commit_ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub primary_key: Vec<u8>,
    /// Exact encoded row value from the logical latest-visible state.
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTable {
    pub schema: TableSchema,
    pub table_id: u32,
    pub rows: Vec<SnapshotRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedSqlSnapshot {
    pub metadata: SnapshotMetadata,
    pub tables: Vec<SnapshotTable>,
    /// Reserved deterministic payload for future metadata that does not alter
    /// v1 semantics. New semantic requirements still require a new version.
    pub metadata_extension: Vec<u8>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SnapshotCodecError {
    #[error("replicated SQL snapshot exceeds {MAX_REPLICATED_SNAPSHOT_BYTES} bytes")]
    TooLarge,
    #[error("replicated SQL snapshot is truncated")]
    UnexpectedEof,
    #[error("invalid replicated SQL snapshot magic")]
    InvalidMagic,
    #[error("unsupported replicated SQL snapshot version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported replicated SQL snapshot flags {0:#04x}")]
    UnsupportedFlags(u8),
    #[error("replicated SQL snapshot checksum mismatch")]
    ChecksumMismatch,
    #[error("replicated SQL snapshot contains invalid UTF-8")]
    InvalidUtf8,
    #[error("replicated SQL snapshot contains an empty table name")]
    EmptyTableName,
    #[error("replicated SQL snapshot contains an empty column name")]
    EmptyColumnName,
    #[error("replicated SQL snapshot contains an empty column type")]
    EmptyColumnType,
    #[error("replicated SQL snapshot contains too many {0}")]
    TooManyItems(&'static str),
    #[error("replicated SQL snapshot {0} exceeds the configured field-size limit")]
    FieldTooLarge(&'static str),
    #[error(
        "replicated SQL snapshot tables must be strictly ordered and unique by normalized name"
    )]
    NonCanonicalTableOrder,
    #[error("replicated SQL snapshot row keys must be strictly increasing and unique")]
    NonCanonicalRowOrder,
    #[error(
        "replicated SQL snapshot table id mismatch for {table}: snapshot={snapshot_table_id}, derived={derived_table_id}"
    )]
    TableIdMismatch {
        table: String,
        snapshot_table_id: u32,
        derived_table_id: u32,
    },
    #[error(
        "replicated SQL snapshot apply index {latest_sql_apply_index} exceeds last included Raft index {last_included_index}"
    )]
    ApplyIndexBeyondSnapshot {
        latest_sql_apply_index: u64,
        last_included_index: u64,
    },
    #[error("replicated SQL snapshot has a commit timestamp without an applied SQL index")]
    TimestampWithoutApplyIndex,
    #[error("replicated SQL snapshot has trailing bytes before its checksum")]
    TrailingBytes,
}

impl ReplicatedSqlSnapshot {
    /// Encode into the stable NeuralBase replicated-SQL snapshot format.
    ///
    /// Integer fields are big-endian. Variable fields are u32-length-prefixed.
    /// Tables are sorted by normalized table name and rows by raw primary-key
    /// bytes before serialization. Duplicate normalized table names and duplicate
    /// primary keys are rejected. The final 32 bytes are SHA-256 over all prior
    /// bytes, including magic/version/metadata.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotCodecError> {
        validate_metadata(self.metadata)?;
        validate_extension(&self.metadata_extension)?;

        let mut tables = self.tables.clone();
        for table in &mut tables {
            validate_table(table)?;
            table.rows.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
            ensure_row_order(&table.rows)?;
        }
        tables
            .sort_by(|a, b| normalized_name(&a.schema.name).cmp(&normalized_name(&b.schema.name)));
        ensure_table_order(&tables)?;

        let mut out = Vec::new();
        put_raw(&mut out, MAGIC)?;
        put_u8(&mut out, REPLICATED_SNAPSHOT_VERSION)?;
        put_u8(&mut out, 0)?; // v1 flags: none defined.
        put_u64(&mut out, self.metadata.last_included_index)?;
        put_u64(&mut out, self.metadata.last_included_term)?;
        put_u64(&mut out, self.metadata.latest_sql_apply_index)?;
        put_u64(&mut out, self.metadata.latest_commit_ts)?;
        put_count(&mut out, tables.len(), MAX_TABLES, "tables")?;

        for table in &tables {
            put_u32(&mut out, table.table_id)?;
            put_string(&mut out, &table.schema.name, "table name")?;
            put_count(
                &mut out,
                table.schema.columns.len(),
                MAX_COLUMNS_PER_TABLE,
                "columns",
            )?;
            for column in &table.schema.columns {
                put_string(&mut out, &column.name, "column name")?;
                put_string(&mut out, &column.data_type, "column type")?;
            }
            put_count(&mut out, table.rows.len(), MAX_ROWS_PER_TABLE, "rows")?;
            for row in &table.rows {
                put_bytes(&mut out, &row.primary_key, "primary key")?;
                put_bytes(&mut out, &row.value, "row value")?;
            }
        }

        put_bytes_with_limit(
            &mut out,
            &self.metadata_extension,
            MAX_EXTENSION_BYTES,
            "metadata extension",
        )?;

        if out.len() > MAX_REPLICATED_SNAPSHOT_BYTES.saturating_sub(CHECKSUM_BYTES) {
            return Err(SnapshotCodecError::TooLarge);
        }
        let digest = Sha256::digest(&out);
        put_raw(&mut out, &digest)?;
        Ok(out)
    }

    /// Decode, checksum, and fully validate a v1 logical SQL snapshot.
    /// Non-canonical ordering is rejected rather than silently normalized.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotCodecError> {
        if bytes.len() > MAX_REPLICATED_SNAPSHOT_BYTES {
            return Err(SnapshotCodecError::TooLarge);
        }
        if bytes.len() < CHECKSUM_BYTES {
            return Err(SnapshotCodecError::UnexpectedEof);
        }
        let payload_len = bytes.len() - CHECKSUM_BYTES;
        let (payload, checksum) = bytes.split_at(payload_len);
        let expected = Sha256::digest(payload);
        if expected[..] != checksum[..] {
            return Err(SnapshotCodecError::ChecksumMismatch);
        }

        let mut reader = Reader::new(payload);
        if reader.take(4)? != MAGIC {
            return Err(SnapshotCodecError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != REPLICATED_SNAPSHOT_VERSION {
            return Err(SnapshotCodecError::UnsupportedVersion(version));
        }
        let flags = reader.u8()?;
        if flags != 0 {
            return Err(SnapshotCodecError::UnsupportedFlags(flags));
        }

        let metadata = SnapshotMetadata {
            last_included_index: reader.u64()?,
            last_included_term: reader.u64()?,
            latest_sql_apply_index: reader.u64()?,
            latest_commit_ts: reader.u64()?,
        };
        validate_metadata(metadata)?;

        let table_count = reader.count(MAX_TABLES, "tables")?;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            let table_id = reader.u32()?;
            let name = reader.string("table name")?;
            if name.is_empty() {
                return Err(SnapshotCodecError::EmptyTableName);
            }
            let column_count = reader.count(MAX_COLUMNS_PER_TABLE, "columns")?;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let column_name = reader.string("column name")?;
                if column_name.is_empty() {
                    return Err(SnapshotCodecError::EmptyColumnName);
                }
                let data_type = reader.string("column type")?;
                if data_type.is_empty() {
                    return Err(SnapshotCodecError::EmptyColumnType);
                }
                columns.push(ColumnDef {
                    name: column_name,
                    data_type,
                });
            }

            let row_count = reader.count(MAX_ROWS_PER_TABLE, "rows")?;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                rows.push(SnapshotRow {
                    primary_key: reader.bytes("primary key")?,
                    value: reader.bytes("row value")?,
                });
            }
            ensure_row_order(&rows)?;

            let table = SnapshotTable {
                schema: TableSchema { name, columns },
                table_id,
                rows,
            };
            validate_table(&table)?;
            tables.push(table);
        }
        ensure_table_order(&tables)?;

        let metadata_extension =
            reader.bytes_with_limit(MAX_EXTENSION_BYTES, "metadata extension")?;
        if !reader.is_finished() {
            return Err(SnapshotCodecError::TrailingBytes);
        }

        Ok(Self {
            metadata,
            tables,
            metadata_extension,
        })
    }
}

fn validate_metadata(metadata: SnapshotMetadata) -> Result<(), SnapshotCodecError> {
    if metadata.latest_sql_apply_index > metadata.last_included_index {
        return Err(SnapshotCodecError::ApplyIndexBeyondSnapshot {
            latest_sql_apply_index: metadata.latest_sql_apply_index,
            last_included_index: metadata.last_included_index,
        });
    }
    if metadata.latest_sql_apply_index == 0 && metadata.latest_commit_ts != 0 {
        return Err(SnapshotCodecError::TimestampWithoutApplyIndex);
    }
    Ok(())
}

fn validate_extension(extension: &[u8]) -> Result<(), SnapshotCodecError> {
    if extension.len() > MAX_EXTENSION_BYTES {
        Err(SnapshotCodecError::FieldTooLarge("metadata extension"))
    } else {
        Ok(())
    }
}

fn validate_table(table: &SnapshotTable) -> Result<(), SnapshotCodecError> {
    if table.schema.name.is_empty() {
        return Err(SnapshotCodecError::EmptyTableName);
    }
    if table.schema.columns.len() > MAX_COLUMNS_PER_TABLE {
        return Err(SnapshotCodecError::TooManyItems("columns"));
    }
    if table.rows.len() > MAX_ROWS_PER_TABLE {
        return Err(SnapshotCodecError::TooManyItems("rows"));
    }
    for column in &table.schema.columns {
        if column.name.is_empty() {
            return Err(SnapshotCodecError::EmptyColumnName);
        }
        if column.data_type.is_empty() {
            return Err(SnapshotCodecError::EmptyColumnType);
        }
        validate_field(column.name.as_bytes(), "column name")?;
        validate_field(column.data_type.as_bytes(), "column type")?;
    }
    validate_field(table.schema.name.as_bytes(), "table name")?;
    for row in &table.rows {
        validate_field(&row.primary_key, "primary key")?;
        validate_field(&row.value, "row value")?;
    }

    let derived_table_id = table_id_for(&table.schema.name);
    if table.table_id != derived_table_id {
        return Err(SnapshotCodecError::TableIdMismatch {
            table: table.schema.name.clone(),
            snapshot_table_id: table.table_id,
            derived_table_id,
        });
    }
    Ok(())
}

fn validate_field(value: &[u8], field: &'static str) -> Result<(), SnapshotCodecError> {
    if value.len() > MAX_FIELD_BYTES || u32::try_from(value.len()).is_err() {
        Err(SnapshotCodecError::FieldTooLarge(field))
    } else {
        Ok(())
    }
}

fn normalized_name(name: &str) -> String {
    name.to_lowercase()
}

fn ensure_table_order(tables: &[SnapshotTable]) -> Result<(), SnapshotCodecError> {
    for pair in tables.windows(2) {
        if normalized_name(&pair[0].schema.name) >= normalized_name(&pair[1].schema.name) {
            return Err(SnapshotCodecError::NonCanonicalTableOrder);
        }
    }
    Ok(())
}

fn ensure_row_order(rows: &[SnapshotRow]) -> Result<(), SnapshotCodecError> {
    for pair in rows.windows(2) {
        if pair[0].primary_key >= pair[1].primary_key {
            return Err(SnapshotCodecError::NonCanonicalRowOrder);
        }
    }
    Ok(())
}

fn ensure_growth(out: &[u8], additional: usize) -> Result<(), SnapshotCodecError> {
    let new_len = out
        .len()
        .checked_add(additional)
        .ok_or(SnapshotCodecError::TooLarge)?;
    if new_len > MAX_REPLICATED_SNAPSHOT_BYTES {
        Err(SnapshotCodecError::TooLarge)
    } else {
        Ok(())
    }
}

fn put_raw(out: &mut Vec<u8>, value: &[u8]) -> Result<(), SnapshotCodecError> {
    ensure_growth(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn put_u8(out: &mut Vec<u8>, value: u8) -> Result<(), SnapshotCodecError> {
    put_raw(out, &[value])
}

fn put_u32(out: &mut Vec<u8>, value: u32) -> Result<(), SnapshotCodecError> {
    put_raw(out, &value.to_be_bytes())
}

fn put_u64(out: &mut Vec<u8>, value: u64) -> Result<(), SnapshotCodecError> {
    put_raw(out, &value.to_be_bytes())
}

fn put_count(
    out: &mut Vec<u8>,
    value: usize,
    max: usize,
    item: &'static str,
) -> Result<(), SnapshotCodecError> {
    if value > max {
        return Err(SnapshotCodecError::TooManyItems(item));
    }
    let encoded = u32::try_from(value).map_err(|_| SnapshotCodecError::TooManyItems(item))?;
    put_u32(out, encoded)
}

fn put_string(
    out: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), SnapshotCodecError> {
    put_bytes(out, value.as_bytes(), field)
}

fn put_bytes(
    out: &mut Vec<u8>,
    value: &[u8],
    field: &'static str,
) -> Result<(), SnapshotCodecError> {
    put_bytes_with_limit(out, value, MAX_FIELD_BYTES, field)
}

fn put_bytes_with_limit(
    out: &mut Vec<u8>,
    value: &[u8],
    max: usize,
    field: &'static str,
) -> Result<(), SnapshotCodecError> {
    if value.len() > max {
        return Err(SnapshotCodecError::FieldTooLarge(field));
    }
    let len = u32::try_from(value.len()).map_err(|_| SnapshotCodecError::FieldTooLarge(field))?;
    put_u32(out, len)?;
    put_raw(out, value)
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SnapshotCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(SnapshotCodecError::UnexpectedEof)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(SnapshotCodecError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, SnapshotCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, SnapshotCodecError> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| SnapshotCodecError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn u64(&mut self) -> Result<u64, SnapshotCodecError> {
        let raw: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| SnapshotCodecError::UnexpectedEof)?;
        Ok(u64::from_be_bytes(raw))
    }

    fn count(&mut self, max: usize, item: &'static str) -> Result<usize, SnapshotCodecError> {
        let count = self.u32()? as usize;
        if count > max {
            return Err(SnapshotCodecError::TooManyItems(item));
        }
        Ok(count)
    }

    fn bytes(&mut self, field: &'static str) -> Result<Vec<u8>, SnapshotCodecError> {
        self.bytes_with_limit(MAX_FIELD_BYTES, field)
    }

    fn bytes_with_limit(
        &mut self,
        max: usize,
        field: &'static str,
    ) -> Result<Vec<u8>, SnapshotCodecError> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(SnapshotCodecError::FieldTooLarge(field));
        }
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self, field: &'static str) -> Result<String, SnapshotCodecError> {
        String::from_utf8(self.bytes(field)?).map_err(|_| SnapshotCodecError::InvalidUtf8)
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, rows: &[(&[u8], &[u8])]) -> SnapshotTable {
        SnapshotTable {
            schema: TableSchema {
                name: name.to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: "BIGINT".to_string(),
                    },
                    ColumnDef {
                        name: "note".to_string(),
                        data_type: "TEXT".to_string(),
                    },
                ],
            },
            table_id: table_id_for(name),
            rows: rows
                .iter()
                .map(|(primary_key, value)| SnapshotRow {
                    primary_key: primary_key.to_vec(),
                    value: value.to_vec(),
                })
                .collect(),
        }
    }

    fn sample_snapshot() -> ReplicatedSqlSnapshot {
        ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 42,
                last_included_term: 7,
                latest_sql_apply_index: 41,
                latest_commit_ts: 9_001,
            },
            tables: vec![table("orders", &[(b"1", b"v1"), (b"2", b"v2")])],
            metadata_extension: b"meta".to_vec(),
        }
    }

    fn recompute_checksum(bytes: &mut [u8]) {
        let payload_len = bytes.len() - CHECKSUM_BYTES;
        let digest = Sha256::digest(&bytes[..payload_len]);
        bytes[payload_len..].copy_from_slice(&digest);
    }

    #[test]
    fn deterministic_roundtrip_and_golden_digest() {
        let snapshot = sample_snapshot();
        let encoded = snapshot.encode().unwrap();
        assert_eq!(encoded.len(), 158);
        assert_eq!(
            hex::encode(Sha256::digest(&encoded)),
            "5da6868f65ad2329e8e2a98de19145468ac9ef240ff927b464176285a39438bc"
        );
        assert_eq!(ReplicatedSqlSnapshot::decode(&encoded).unwrap(), snapshot);
    }

    #[test]
    fn encode_canonicalizes_table_and_row_order() {
        let mut first = table("zeta", &[(b"9", b"z"), (b"1", b"a")]);
        let second = table("alpha", &[(b"2", b"b")]);
        let snapshot = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 2,
                last_included_term: 1,
                latest_sql_apply_index: 2,
                latest_commit_ts: 1,
            },
            tables: vec![first.clone(), second.clone()],
            metadata_extension: Vec::new(),
        };
        first.rows.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
        let canonical = ReplicatedSqlSnapshot {
            tables: vec![second, first],
            ..snapshot.clone()
        };
        assert_eq!(snapshot.encode().unwrap(), canonical.encode().unwrap());
        assert_eq!(
            ReplicatedSqlSnapshot::decode(&snapshot.encode().unwrap()).unwrap(),
            canonical
        );
    }

    #[test]
    fn rejects_bad_checksum_before_parsing_payload() {
        let mut encoded = sample_snapshot().encode().unwrap();
        encoded[10] ^= 0x55;
        assert_eq!(
            ReplicatedSqlSnapshot::decode(&encoded),
            Err(SnapshotCodecError::ChecksumMismatch)
        );
    }

    #[test]
    fn rejects_invalid_magic_with_valid_checksum() {
        let mut encoded = sample_snapshot().encode().unwrap();
        encoded[0] ^= 0x01;
        recompute_checksum(&mut encoded);
        assert_eq!(
            ReplicatedSqlSnapshot::decode(&encoded),
            Err(SnapshotCodecError::InvalidMagic)
        );
    }

    #[test]
    fn rejects_truncation() {
        let encoded = sample_snapshot().encode().unwrap();
        assert_eq!(
            ReplicatedSqlSnapshot::decode(&encoded[..16]),
            Err(SnapshotCodecError::UnexpectedEof)
        );
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut encoded = sample_snapshot().encode().unwrap();
        encoded[4] = REPLICATED_SNAPSHOT_VERSION + 1;
        recompute_checksum(&mut encoded);
        assert_eq!(
            ReplicatedSqlSnapshot::decode(&encoded),
            Err(SnapshotCodecError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn rejects_duplicate_or_noncanonical_tables() {
        let mut encoded = sample_snapshot().encode().unwrap();
        // Turn the single table count into two by duplicating the encoded table
        // would require rebuilding the payload; exercising the same invariant at
        // encode time proves duplicate normalized names cannot be serialized.
        let snapshot = ReplicatedSqlSnapshot {
            tables: vec![table("Orders", &[]), table("orders", &[])],
            ..sample_snapshot()
        };
        assert_eq!(
            snapshot.encode(),
            Err(SnapshotCodecError::NonCanonicalTableOrder)
        );

        // Keep `encoded` used so this test also guards the canonical baseline.
        assert!(ReplicatedSqlSnapshot::decode(&encoded).is_ok());
        encoded.clear();
    }

    #[test]
    fn rejects_duplicate_primary_keys() {
        let snapshot = ReplicatedSqlSnapshot {
            tables: vec![table("orders", &[(b"1", b"a"), (b"1", b"b")])],
            ..sample_snapshot()
        };
        assert_eq!(
            snapshot.encode(),
            Err(SnapshotCodecError::NonCanonicalRowOrder)
        );
    }

    #[test]
    fn rejects_table_id_mismatch() {
        let mut snapshot = sample_snapshot();
        snapshot.tables[0].table_id ^= 1;
        assert!(matches!(
            snapshot.encode(),
            Err(SnapshotCodecError::TableIdMismatch { .. })
        ));
    }

    #[test]
    fn rejects_apply_index_beyond_snapshot_index() {
        let mut snapshot = sample_snapshot();
        snapshot.metadata.latest_sql_apply_index = snapshot.metadata.last_included_index + 1;
        assert_eq!(
            snapshot.encode(),
            Err(SnapshotCodecError::ApplyIndexBeyondSnapshot {
                latest_sql_apply_index: 43,
                last_included_index: 42,
            })
        );
    }

    #[test]
    fn rejects_commit_timestamp_without_sql_apply_index() {
        let mut snapshot = sample_snapshot();
        snapshot.metadata.latest_sql_apply_index = 0;
        assert_eq!(
            snapshot.encode(),
            Err(SnapshotCodecError::TimestampWithoutApplyIndex)
        );
    }

    #[test]
    fn noncanonical_wire_row_order_is_rejected_even_with_valid_checksum() {
        let canonical = sample_snapshot().encode().unwrap();
        let mut snapshot = sample_snapshot();
        snapshot.tables[0].rows.reverse();

        // `encode` normalizes, so construct a valid-checksum wire payload by
        // swapping the two fixed-size row records in the canonical bytes.
        let mut malformed = canonical.clone();
        let first = malformed
            .windows(11)
            .position(|w| w == b"\0\0\0\x011\0\0\0\x02v1")
            .unwrap();
        let second = first + 11;
        let row1 = malformed[first..first + 11].to_vec();
        let row2 = malformed[second..second + 11].to_vec();
        malformed[first..first + 11].copy_from_slice(&row2);
        malformed[second..second + 11].copy_from_slice(&row1);
        recompute_checksum(&mut malformed);

        assert_eq!(
            ReplicatedSqlSnapshot::decode(&malformed),
            Err(SnapshotCodecError::NonCanonicalRowOrder)
        );
        assert_eq!(snapshot.encode().unwrap(), canonical);
    }
}
