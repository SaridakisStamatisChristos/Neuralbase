// Regression guard for PostgreSQL query-error synchronization semantics.
//
// Query execution is nested beneath the frontend protocol state machine: Simple Query
// owns the single ReadyForQuery frame, while extended protocol reaches ReadyForQuery
// only after Sync. This source-level guard prevents execution helpers from regressing
// back to emitting an inner ReadyForQuery and corrupting client framing during transient
// Raft authority errors.

#[test]
fn query_execution_errors_do_not_emit_ready_for_query() {
    let query = include_str!("../src/server_parts/query.rs");
    let tail = include_str!("../src/server_parts/tail.rs");

    assert!(
        query.contains("write_query_error"),
        "query execution must use the ErrorResponse-only helper"
    );
    assert!(
        !query.contains("write_error_and_ready("),
        "process_query must not emit ReadyForQuery from inside execution"
    );

    let replicated_start = tail
        .find("async fn write_replicated_error")
        .expect("replicated error helper");
    let batch_start = tail
        .find("async fn write_batch")
        .expect("write_batch helper");
    let replicated = &tail[replicated_start..batch_start];
    assert!(
        !replicated.contains("write_error_and_ready("),
        "replicated query errors must not enqueue ReadyForQuery"
    );
}
