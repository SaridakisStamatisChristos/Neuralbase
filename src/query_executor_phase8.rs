// SPDX-License-Identifier: Apache-2.0
// Phase 8 semantic front-end for the row-oriented query executor.

pub use crate::query_executor_legacy::{
    query_result_to_batch, QueryCatalog, QueryError, QueryResult, ScalarVal,
};

pub fn execute_select_query(
    query: &sqlparser::ast::Query,
    catalog: &QueryCatalog,
) -> Result<QueryResult, QueryError> {
    crate::query_executor_legacy::execute_select_query(query, catalog)
}
