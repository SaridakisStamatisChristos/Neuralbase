// SPDX-License-Identifier: Apache-2.0
// RocksDB-backed catalog — persists schemas in the `catalog` column family.
//
// Kept separate from catalog.rs so test binaries that only need InMemoryCatalog
// can include catalog.rs without depending on the storage module.
//
// CONFIDENCE: raw=0.74 effective=0.62
// DEPENDS_ON: catalog, storage

// Session 4 infrastructure — now wired into main() for startup schema loading.

use std::sync::Arc;

use crate::catalog::{Catalog, InMemoryCatalog, TableSchema};
use crate::storage::{StorageEngine, StorageError};

/// A catalog backed by the RocksDB `catalog` column family.
/// Schema registrations are durable — they survive process restart.
/// Thread-safe: `StorageEngine` uses `DBWithThreadMode<MultiThreaded>`.
pub struct RocksDbCatalog {
    engine: Arc<StorageEngine>,
}

impl RocksDbCatalog {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Persist a table schema (idempotent — overwrites on duplicate name).
    pub fn register_table(&self, schema: &TableSchema) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(schema).map_err(StorageError::Serde)?;
        self.engine
            .write_catalog_entry(&schema.name.to_lowercase(), &bytes)?;
        Ok(())
    }

    /// Remove persisted table schema entry.
    pub fn unregister_table(&self, table_name: &str) -> Result<(), StorageError> {
        self.engine
            .delete_catalog_entry(&table_name.to_lowercase())?;
        Ok(())
    }

    /// List all persisted table names (available in test builds).
    #[cfg(test)]
    pub fn list_tables(&self) -> Result<Vec<String>, StorageError> {
        self.engine.list_catalog_keys()
    }

    /// Load the full catalog into an in-memory snapshot (for startup warm-up).
    ///
    /// Corrupt persisted schema bytes fail the whole hydration instead of
    /// silently returning a partial catalog. Clustered startup relies on this
    /// fail-closed behavior before it begins serving SQL.
    pub fn load_all(&self) -> Result<InMemoryCatalog, StorageError> {
        let mem = InMemoryCatalog::default();
        for key in self.engine.list_catalog_keys()? {
            if let Some(bytes) = self.engine.read_catalog_entry(&key)? {
                let schema =
                    serde_json::from_slice::<TableSchema>(&bytes).map_err(StorageError::Serde)?;
                mem.register_table(schema);
            }
        }
        Ok(mem)
    }
}

impl Catalog for RocksDbCatalog {
    fn get_table(&self, table_name: &str) -> Option<TableSchema> {
        let bytes = self
            .engine
            .read_catalog_entry(&table_name.to_lowercase())
            .ok()??;
        serde_json::from_slice(&bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use crate::storage::StorageEngine;
    use tempfile::TempDir;

    fn setup() -> (RocksDbCatalog, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        (RocksDbCatalog::new(engine), dir)
    }

    #[test]
    fn register_and_lookup_roundtrip() {
        let (cat, _dir) = setup();
        let schema = TableSchema {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "o_orderkey".to_string(),
                data_type: "BIGINT".to_string(),
            }],
        };
        cat.register_table(&schema).unwrap();
        let found = cat.get_table("orders").unwrap();
        assert_eq!(found, schema);
    }

    #[test]
    fn missing_table_returns_none() {
        let (cat, _dir) = setup();
        assert!(cat.get_table("nonexistent").is_none());
    }

    #[test]
    fn register_duplicate_overwrites_previous() {
        let (cat, _dir) = setup();
        let v1 = TableSchema {
            name: "tbl".to_string(),
            columns: vec![],
        };
        let v2 = TableSchema {
            name: "tbl".to_string(),
            columns: vec![ColumnDef {
                name: "col1".to_string(),
                data_type: "INT".to_string(),
            }],
        };
        cat.register_table(&v1).unwrap();
        cat.register_table(&v2).unwrap();
        assert_eq!(cat.get_table("tbl").unwrap(), v2);
    }

    #[test]
    fn load_all_restores_all_schemas() {
        let (cat, _dir) = setup();
        for i in 0u32..5 {
            cat.register_table(&TableSchema {
                name: format!("table_{i}"),
                columns: vec![],
            })
            .unwrap();
        }
        let mem = cat.load_all().unwrap();
        for i in 0..5u32 {
            assert!(mem.get_table(&format!("table_{i}")).is_some());
        }
    }

    #[test]
    fn load_all_rejects_corrupt_schema_instead_of_partial_hydration() {
        let (cat, _dir) = setup();
        cat.register_table(&TableSchema {
            name: "valid".to_string(),
            columns: vec![],
        })
        .unwrap();
        cat.engine
            .write_catalog_entry("broken", b"{not-valid-json")
            .unwrap();

        assert!(matches!(cat.load_all(), Err(StorageError::Serde(_))));
    }

    #[test]
    fn case_insensitive_lookup() {
        let (cat, _dir) = setup();
        let schema = TableSchema {
            name: "Lineitem".to_string(),
            columns: vec![],
        };
        cat.register_table(&schema).unwrap();
        assert!(cat.get_table("LINEITEM").is_some());
        assert!(cat.get_table("lineitem").is_some());
    }
}
