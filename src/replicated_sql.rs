// SPDX-License-Identifier: Apache-2.0
//! Deterministic, versioned commands for Raft-replicated SQL mutations.
//!
//! The state-machine apply path must never re-plan SQL or consult a local wall
//! clock. Leaders therefore materialize SQL mutations into this representation
//! before proposing them to Raft. DML commands carry concrete primary keys and
//! already-encoded row values; followers only apply those bytes in Raft order.

use crate::catalog::{ColumnDef, TableSchema};
use thiserror::Error;

const MAGIC: &[u8; 4] = b"NBRM";
pub const REPLICATED_MUTATION_VERSION: u8 = 1;
pub const MAX_REPLICATED_MUTATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_ITEMS: usize = 1_000_000;

const OP_CREATE_TABLE: u8 = 1;
const OP_DROP_TABLE: u8 = 2;
const OP_INSERT_ROWS: u8 = 3;
const OP_UPDATE_ROWS: u8 = 4;
const OP_DELETE_ROWS: u8 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedRowWrite {
    pub primary_key: Vec<u8>,
    /// Exact storage value bytes. Encoding happens once on the leader before
    /// proposal, so apply does not depend on a node-local codec decision.
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatedMutation {
    CreateTable { schema: TableSchema },
    DropTable { table: String, table_id: u32 },
    InsertRows {
        table: String,
        table_id: u32,
        rows: Vec<ReplicatedRowWrite>,
    },
    UpdateRows {
        table: String,
        table_id: u32,
        rows: Vec<ReplicatedRowWrite>,
    },
    DeleteRows {
        table: String,
        table_id: u32,
        primary_keys: Vec<Vec<u8>>,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MutationCodecError {
    #[error("replicated mutation exceeds {MAX_REPLICATED_MUTATION_BYTES} bytes")]
    TooLarge,
    #[error("replicated mutation is truncated")]
    UnexpectedEof,
    #[error("invalid replicated mutation magic")]
    InvalidMagic,
    #[error("unsupported replicated mutation version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown replicated mutation opcode {0}")]
    UnknownOpcode(u8),
    #[error("replicated mutation contains invalid UTF-8")]
    InvalidUtf8,
    #[error("replicated mutation contains too many items")]
    TooManyItems,
    #[error("replicated mutation contains an empty table name")]
    EmptyTableName,
    #[error("replicated row keys must be strictly increasing and unique")]
    NonCanonicalRowOrder,
    #[error("replicated mutation has trailing bytes")]
    TrailingBytes,
    #[error("field length exceeds u32 encoding range")]
    FieldTooLarge,
}

impl ReplicatedMutation {
    /// Encode the command into the stable NeuralBase replicated-mutation wire
    /// format. All integer fields are big-endian and all variable fields are
    /// u32-length-prefixed. DML row keys are sorted before encoding so callers
    /// cannot accidentally create two byte representations of the same write set.
    pub fn encode(&self) -> Result<Vec<u8>, MutationCodecError> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(REPLICATED_MUTATION_VERSION);

        match self {
            Self::CreateTable { schema } => {
                validate_table_name(&schema.name)?;
                out.push(OP_CREATE_TABLE);
                put_string(&mut out, &schema.name)?;
                put_count(&mut out, schema.columns.len())?;
                for column in &schema.columns {
                    put_string(&mut out, &column.name)?;
                    put_string(&mut out, &column.data_type)?;
                }
            }
            Self::DropTable { table, table_id } => {
                validate_table_name(table)?;
                out.push(OP_DROP_TABLE);
                put_string(&mut out, table)?;
                out.extend_from_slice(&table_id.to_be_bytes());
            }
            Self::InsertRows {
                table,
                table_id,
                rows,
            } => {
                encode_row_writes(&mut out, OP_INSERT_ROWS, table, *table_id, rows)?;
            }
            Self::UpdateRows {
                table,
                table_id,
                rows,
            } => {
                encode_row_writes(&mut out, OP_UPDATE_ROWS, table, *table_id, rows)?;
            }
            Self::DeleteRows {
                table,
                table_id,
                primary_keys,
            } => {
                validate_table_name(table)?;
                out.push(OP_DELETE_ROWS);
                put_string(&mut out, table)?;
                out.extend_from_slice(&table_id.to_be_bytes());
                let mut keys = primary_keys.clone();
                keys.sort();
                ensure_strict_key_order(&keys)?;
                put_count(&mut out, keys.len())?;
                for key in &keys {
                    put_bytes(&mut out, key)?;
                }
            }
        }

        if out.len() > MAX_REPLICATED_MUTATION_BYTES {
            return Err(MutationCodecError::TooLarge);
        }
        Ok(out)
    }

    /// Decode and validate a replicated mutation. Non-canonical DML ordering is
    /// rejected so every accepted command has exactly one serialized form.
    pub fn decode(bytes: &[u8]) -> Result<Self, MutationCodecError> {
        if bytes.len() > MAX_REPLICATED_MUTATION_BYTES {
            return Err(MutationCodecError::TooLarge);
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(MutationCodecError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != REPLICATED_MUTATION_VERSION {
            return Err(MutationCodecError::UnsupportedVersion(version));
        }
        let opcode = reader.u8()?;
        let mutation = match opcode {
            OP_CREATE_TABLE => {
                let name = reader.string()?;
                validate_table_name(&name)?;
                let count = reader.count()?;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(ColumnDef {
                        name: reader.string()?,
                        data_type: reader.string()?,
                    });
                }
                Self::CreateTable {
                    schema: TableSchema { name, columns },
                }
            }
            OP_DROP_TABLE => {
                let table = reader.string()?;
                validate_table_name(&table)?;
                let table_id = reader.u32()?;
                Self::DropTable { table, table_id }
            }
            OP_INSERT_ROWS | OP_UPDATE_ROWS => {
                let table = reader.string()?;
                validate_table_name(&table)?;
                let table_id = reader.u32()?;
                let count = reader.count()?;
                let mut rows = Vec::with_capacity(count);
                for _ in 0..count {
                    rows.push(ReplicatedRowWrite {
                        primary_key: reader.bytes()?,
                        value: reader.bytes()?,
                    });
                }
                ensure_row_write_order(&rows)?;
                if opcode == OP_INSERT_ROWS {
                    Self::InsertRows {
                        table,
                        table_id,
                        rows,
                    }
                } else {
                    Self::UpdateRows {
                        table,
                        table_id,
                        rows,
                    }
                }
            }
            OP_DELETE_ROWS => {
                let table = reader.string()?;
                validate_table_name(&table)?;
                let table_id = reader.u32()?;
                let count = reader.count()?;
                let mut primary_keys = Vec::with_capacity(count);
                for _ in 0..count {
                    primary_keys.push(reader.bytes()?);
                }
                ensure_strict_key_order(&primary_keys)?;
                Self::DeleteRows {
                    table,
                    table_id,
                    primary_keys,
                }
            }
            other => return Err(MutationCodecError::UnknownOpcode(other)),
        };

        if !reader.is_finished() {
            return Err(MutationCodecError::TrailingBytes);
        }
        Ok(mutation)
    }

    pub fn command_tag(&self) -> &'static str {
        match self {
            Self::CreateTable { .. } => "CREATE TABLE",
            Self::DropTable { .. } => "DROP TABLE",
            Self::InsertRows { .. } => "INSERT",
            Self::UpdateRows { .. } => "UPDATE",
            Self::DeleteRows { .. } => "DELETE",
        }
    }

    pub fn affected_rows(&self) -> Option<u64> {
        match self {
            Self::InsertRows { rows, .. } | Self::UpdateRows { rows, .. } => {
                Some(rows.len() as u64)
            }
            Self::DeleteRows { primary_keys, .. } => Some(primary_keys.len() as u64),
            Self::CreateTable { .. } | Self::DropTable { .. } => None,
        }
    }
}

fn encode_row_writes(
    out: &mut Vec<u8>,
    opcode: u8,
    table: &str,
    table_id: u32,
    rows: &[ReplicatedRowWrite],
) -> Result<(), MutationCodecError> {
    validate_table_name(table)?;
    out.push(opcode);
    put_string(out, table)?;
    out.extend_from_slice(&table_id.to_be_bytes());

    let mut canonical = rows.to_vec();
    canonical.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
    ensure_row_write_order(&canonical)?;
    put_count(out, canonical.len())?;
    for row in &canonical {
        put_bytes(out, &row.primary_key)?;
        put_bytes(out, &row.value)?;
    }
    Ok(())
}

fn ensure_row_write_order(rows: &[ReplicatedRowWrite]) -> Result<(), MutationCodecError> {
    for pair in rows.windows(2) {
        if pair[0].primary_key >= pair[1].primary_key {
            return Err(MutationCodecError::NonCanonicalRowOrder);
        }
    }
    Ok(())
}

fn ensure_strict_key_order(keys: &[Vec<u8>]) -> Result<(), MutationCodecError> {
    for pair in keys.windows(2) {
        if pair[0] >= pair[1] {
            return Err(MutationCodecError::NonCanonicalRowOrder);
        }
    }
    Ok(())
}

fn validate_table_name(table: &str) -> Result<(), MutationCodecError> {
    if table.is_empty() {
        Err(MutationCodecError::EmptyTableName)
    } else {
        Ok(())
    }
}

fn put_count(out: &mut Vec<u8>, value: usize) -> Result<(), MutationCodecError> {
    if value > MAX_ITEMS {
        return Err(MutationCodecError::TooManyItems);
    }
    let encoded = u32::try_from(value).map_err(|_| MutationCodecError::FieldTooLarge)?;
    out.extend_from_slice(&encoded.to_be_bytes());
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), MutationCodecError> {
    put_bytes(out, value.as_bytes())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), MutationCodecError> {
    let len = u32::try_from(value.len()).map_err(|_| MutationCodecError::FieldTooLarge)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MutationCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(MutationCodecError::UnexpectedEof)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(MutationCodecError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, MutationCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MutationCodecError> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| MutationCodecError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn count(&mut self) -> Result<usize, MutationCodecError> {
        let count = self.u32()? as usize;
        if count > MAX_ITEMS {
            return Err(MutationCodecError::TooManyItems);
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, MutationCodecError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String, MutationCodecError> {
        String::from_utf8(self.bytes()?).map_err(|_| MutationCodecError::InvalidUtf8)
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &[u8], value: &[u8]) -> ReplicatedRowWrite {
        ReplicatedRowWrite {
            primary_key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn roundtrip_all_mutation_classes() {
        let mutations = vec![
            ReplicatedMutation::CreateTable {
                schema: TableSchema {
                    name: "orders".to_string(),
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
            },
            ReplicatedMutation::DropTable {
                table: "orders".to_string(),
                table_id: 7,
            },
            ReplicatedMutation::InsertRows {
                table: "orders".to_string(),
                table_id: 7,
                rows: vec![row(b"b", b"two"), row(b"a", b"one")],
            },
            ReplicatedMutation::UpdateRows {
                table: "orders".to_string(),
                table_id: 7,
                rows: vec![row(b"a", b"changed")],
            },
            ReplicatedMutation::DeleteRows {
                table: "orders".to_string(),
                table_id: 7,
                primary_keys: vec![b"b".to_vec(), b"a".to_vec()],
            },
        ];

        for mutation in mutations {
            let encoded = mutation.encode().unwrap();
            let decoded = ReplicatedMutation::decode(&encoded).unwrap();
            // Encoding canonicalizes DML row order; compare encoded bytes instead
            // of source object ordering.
            assert_eq!(decoded.encode().unwrap(), encoded);
        }
    }

    #[test]
    fn dml_encoding_is_independent_of_input_row_order() {
        let a = ReplicatedMutation::InsertRows {
            table: "t".to_string(),
            table_id: 1,
            rows: vec![row(b"b", b"2"), row(b"a", b"1")],
        };
        let b = ReplicatedMutation::InsertRows {
            table: "t".to_string(),
            table_id: 1,
            rows: vec![row(b"a", b"1"), row(b"b", b"2")],
        };
        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn duplicate_row_keys_are_rejected() {
        let mutation = ReplicatedMutation::UpdateRows {
            table: "t".to_string(),
            table_id: 1,
            rows: vec![row(b"same", b"1"), row(b"same", b"2")],
        };
        assert_eq!(
            mutation.encode().unwrap_err(),
            MutationCodecError::NonCanonicalRowOrder
        );
    }

    #[test]
    fn create_table_preserves_semantic_column_order() {
        let mutation = ReplicatedMutation::CreateTable {
            schema: TableSchema {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "z".to_string(),
                        data_type: "TEXT".to_string(),
                    },
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: "BIGINT".to_string(),
                    },
                ],
            },
        };
        let encoded = mutation.encode().unwrap();
        let decoded = ReplicatedMutation::decode(&encoded).unwrap();
        let ReplicatedMutation::CreateTable { schema } = decoded else {
            panic!("expected create table");
        };
        assert_eq!(schema.columns[0].name, "z");
        assert_eq!(schema.columns[1].name, "a");
    }

    #[test]
    fn drop_table_has_stable_golden_encoding() {
        let mutation = ReplicatedMutation::DropTable {
            table: "t".to_string(),
            table_id: 0x0102_0304,
        };
        assert_eq!(
            hex::encode(mutation.encode().unwrap()),
            "4e42524d0102000000017401020304"
        );
    }

    #[test]
    fn unknown_version_is_rejected() {
        let mut bytes = ReplicatedMutation::DropTable {
            table: "t".to_string(),
            table_id: 1,
        }
        .encode()
        .unwrap();
        bytes[4] = REPLICATED_MUTATION_VERSION + 1;
        assert_eq!(
            ReplicatedMutation::decode(&bytes).unwrap_err(),
            MutationCodecError::UnsupportedVersion(REPLICATED_MUTATION_VERSION + 1)
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = ReplicatedMutation::DropTable {
            table: "t".to_string(),
            table_id: 1,
        }
        .encode()
        .unwrap();
        bytes.push(0);
        assert_eq!(
            ReplicatedMutation::decode(&bytes).unwrap_err(),
            MutationCodecError::TrailingBytes
        );
    }

    #[test]
    fn decoder_rejects_noncanonical_row_order() {
        let canonical = ReplicatedMutation::InsertRows {
            table: "t".to_string(),
            table_id: 1,
            rows: vec![row(b"a", b"1"), row(b"b", b"2")],
        }
        .encode()
        .unwrap();

        // Locate the one-byte key payloads and swap only their values, leaving
        // lengths untouched. The resulting byte stream is structurally valid
        // but no longer canonical.
        let first_a = canonical.iter().position(|b| *b == b'a').unwrap();
        let last_b = canonical.iter().rposition(|b| *b == b'b').unwrap();
        let mut noncanonical = canonical.clone();
        noncanonical[first_a] = b'b';
        noncanonical[last_b] = b'a';
        assert_eq!(
            ReplicatedMutation::decode(&noncanonical).unwrap_err(),
            MutationCodecError::NonCanonicalRowOrder
        );
    }
}
