// SPDX-License-Identifier: Apache-2.0
use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Read-only catalog access — implemented by all catalog types.
pub trait Catalog: Send + Sync {
    fn get_table(&self, table_name: &str) -> Option<TableSchema>;
}

/// Writable catalog — adds DDL operations on top of read-only `Catalog`.
pub trait MutableCatalog: Catalog {
    /// Register or replace a table schema.
    fn create_table(&self, schema: TableSchema);
}

/// In-memory catalog with interior mutability for thread-safe DDL.
#[derive(Debug, Default)]
pub struct InMemoryCatalog {
    tables: RwLock<HashMap<String, TableSchema>>,
}

impl InMemoryCatalog {
    /// Pre-populate with TPC-H lineitem schema.
    pub fn with_tpch_lineitem() -> Self {
        let catalog = Self::default();
        catalog.register_table(TableSchema {
            name: "lineitem".to_string(),
            columns: vec![
                ColumnDef {
                    name: "l_orderkey".to_string(),
                    data_type: "BIGINT".to_string(),
                },
                ColumnDef {
                    name: "l_partkey".to_string(),
                    data_type: "BIGINT".to_string(),
                },
                ColumnDef {
                    name: "l_quantity".to_string(),
                    data_type: "DOUBLE".to_string(),
                },
                ColumnDef {
                    name: "l_extendedprice".to_string(),
                    data_type: "DOUBLE".to_string(),
                },
                ColumnDef {
                    name: "l_discount".to_string(),
                    data_type: "DOUBLE".to_string(),
                },
                ColumnDef {
                    name: "l_shipdate".to_string(),
                    data_type: "DATE".to_string(),
                },
                ColumnDef {
                    name: "l_returnflag".to_string(),
                    data_type: "TEXT".to_string(),
                },
            ],
        });
        catalog
    }

    /// Register or replace a table schema (takes `&self` — interior RwLock).
    pub fn register_table(&self, table: TableSchema) {
        self.tables
            .write()
            .expect("catalog RwLock poisoned")
            .insert(table.name.to_lowercase(), table);
    }

    /// Return all registered schemas (used for startup catalog sync from RocksDB).
    pub fn all_tables(&self) -> Vec<TableSchema> {
        self.tables
            .read()
            .expect("catalog RwLock poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Atomically replace the complete in-memory catalog snapshot.
    ///
    /// SQL-aware snapshot restore uses this only after the corresponding durable
    /// RocksDB replacement succeeds, so readers never observe a partially rebuilt
    /// in-memory catalog.
    pub fn replace_all(&self, schemas: Vec<TableSchema>) {
        let mut replacement = HashMap::with_capacity(schemas.len());
        for schema in schemas {
            replacement.insert(schema.name.to_lowercase(), schema);
        }
        *self.tables.write().expect("catalog RwLock poisoned") = replacement;
    }

    /// Remove a table schema by name.  A no-op if the table does not exist.
    pub fn drop_table(&self, name: &str) {
        self.tables
            .write()
            .expect("catalog RwLock poisoned")
            .remove(&name.to_lowercase());
    }

    /// Pre-populate with all TPC-H table schemas (8 tables).
    pub fn with_tpch_all_tables() -> Self {
        let catalog = Self::with_tpch_lineitem();
        let extra: &[(&str, &[(&str, &str)])] = &[
            (
                "orders",
                &[
                    ("o_orderkey", "BIGINT"),
                    ("o_custkey", "BIGINT"),
                    ("o_orderstatus", "TEXT"),
                    ("o_totalprice", "DOUBLE"),
                    ("o_orderdate", "DATE"),
                    ("o_orderpriority", "TEXT"),
                    ("o_shippriority", "INT"),
                    ("o_comment", "TEXT"),
                ],
            ),
            (
                "customer",
                &[
                    ("c_custkey", "BIGINT"),
                    ("c_name", "TEXT"),
                    ("c_nationkey", "BIGINT"),
                    ("c_mktsegment", "TEXT"),
                    ("c_acctbal", "DOUBLE"),
                    ("c_phone", "TEXT"),
                    ("c_address", "TEXT"),
                    ("c_comment", "TEXT"),
                ],
            ),
            (
                "nation",
                &[
                    ("n_nationkey", "BIGINT"),
                    ("n_name", "TEXT"),
                    ("n_regionkey", "BIGINT"),
                ],
            ),
            ("region", &[("r_regionkey", "BIGINT"), ("r_name", "TEXT")]),
            (
                "part",
                &[
                    ("p_partkey", "BIGINT"),
                    ("p_name", "TEXT"),
                    ("p_mfgr", "TEXT"),
                    ("p_brand", "TEXT"),
                    ("p_type", "TEXT"),
                    ("p_size", "INT"),
                    ("p_container", "TEXT"),
                    ("p_retailprice", "DOUBLE"),
                ],
            ),
            (
                "supplier",
                &[
                    ("s_suppkey", "BIGINT"),
                    ("s_name", "TEXT"),
                    ("s_nationkey", "BIGINT"),
                    ("s_acctbal", "DOUBLE"),
                    ("s_address", "TEXT"),
                    ("s_phone", "TEXT"),
                    ("s_comment", "TEXT"),
                ],
            ),
            (
                "partsupp",
                &[
                    ("ps_partkey", "BIGINT"),
                    ("ps_suppkey", "BIGINT"),
                    ("ps_availqty", "INT"),
                    ("ps_supplycost", "DOUBLE"),
                ],
            ),
        ];
        for (tname, cols) in extra {
            catalog.register_table(TableSchema {
                name: tname.to_string(),
                columns: cols
                    .iter()
                    .map(|(n, t)| ColumnDef {
                        name: n.to_string(),
                        data_type: t.to_string(),
                    })
                    .collect(),
            });
        }
        catalog
    }

    /// List all registered table names (available in test builds).
    #[cfg(test)]
    pub fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .expect("catalog RwLock poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

impl Catalog for InMemoryCatalog {
    fn get_table(&self, table_name: &str) -> Option<TableSchema> {
        self.tables
            .read()
            .expect("catalog RwLock poisoned")
            .get(&table_name.to_lowercase())
            .cloned()
    }
}

impl MutableCatalog for InMemoryCatalog {
    fn create_table(&self, schema: TableSchema) {
        self.register_table(schema);
    }
}
