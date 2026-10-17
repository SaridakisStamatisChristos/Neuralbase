// SPDX-License-Identifier: Apache-2.0
// Session 14: Connection Pooling, Advanced SQL, Prepared Statements & Plan Cache.
//
// Tests:
//   Phase 1A — Per-user connection limit
//   Phase 1B — CTEs, UNION/INTERSECT/EXCEPT, Window functions, EXPLAIN
//   Phase 2  — Plan cache (unit), Extended query protocol (P/B/E)

use neuralbase::binder::{bind_nb_statement, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::protocol::STARTUP_PROTOCOL_V3;
use neuralbase::query_executor::{execute_select_query, QueryCatalog, ScalarVal};
use neuralbase::server;
use neuralbase::server::PlanCache;
use neuralbase::sql::parse_nb_statement;
use neuralbase::storage;
use neuralbase::storage_executor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};

// ── Helpers ────────────────────────────────────────────────────────────────────

async fn write_startup_message(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&STARTUP_PROTOCOL_V3.to_be_bytes());
    body.extend_from_slice(b"user\0neuralbase\0database\0neuralbase\0\0");

    let mut packet = Vec::new();
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream.write_all(&packet).await.expect("send startup");
}

async fn write_simple_query(stream: &mut TcpStream, query: &str) {
    let mut body = query.as_bytes().to_vec();
    body.push(0);
    let mut packet = Vec::new();
    packet.push(b'Q');
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream.write_all(&packet).await.expect("send query");
}

async fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let (tag, _) = read_one_message(stream).await.expect("read_until_ready");
        if tag == b'Z' {
            break;
        }
    }
}

async fn read_one_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).await?;
    let mut len_bytes = [0_u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let raw = i32::from_be_bytes(len_bytes);
    let payload_len = raw
        .checked_sub(4)
        .and_then(|n| if n <= 65536 { Some(n as usize) } else { None })
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid message length: {raw}"),
            )
        })?;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload).await?;
    Ok((tag[0], payload))
}

async fn read_messages_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut msgs = Vec::new();
    loop {
        let m = read_one_message(stream)
            .await
            .expect("read_messages_until_ready");
        let done = m.0 == b'Z';
        msgs.push(m);
        if done {
            break;
        }
    }
    msgs
}

/// Non-panicking message reader — returns None on EOF or any IO error.
async fn try_read_one_message(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut tag = [0_u8; 1];
    if stream.read_exact(&mut tag).await.is_err() {
        return None;
    }
    let mut len_bytes = [0_u8; 4];
    if stream.read_exact(&mut len_bytes).await.is_err() {
        return None;
    }
    let len = i32::from_be_bytes(len_bytes);
    if len < 4 {
        return None;
    }
    let mut payload = vec![0_u8; (len - 4) as usize];
    if stream.read_exact(&mut payload).await.is_err() {
        return None;
    }
    Some((tag[0], payload))
}

/// Reads messages until EOF (server closed) or ReadyForQuery ('Z').
async fn read_msgs_until_close_or_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut msgs = Vec::new();
    while let Some(m) = try_read_one_message(stream).await {
        let done = m.0 == b'Z';
        msgs.push(m);
        if done {
            break;
        }
    }
    msgs
}

// ── Phase 1A: Per-user connection limit ───────────────────────────────────────

#[tokio::test]
async fn per_user_connection_limit_rejects_excess() {
    // Set limit to 2 connections per user.
    std::env::set_var("NEURALBASE_MAX_CONNECTIONS_PER_USER", "2");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    // First two connections should be accepted.
    let mut c1 = TcpStream::connect(addr).await.expect("connect 1");
    write_startup_message(&mut c1).await;
    read_until_ready(&mut c1).await;

    let mut c2 = TcpStream::connect(addr).await.expect("connect 2");
    write_startup_message(&mut c2).await;
    read_until_ready(&mut c2).await;

    // Third connection should be rejected with 53300.
    let mut c3 = TcpStream::connect(addr).await.expect("connect 3");
    write_startup_message(&mut c3).await;
    // After auth succeeds the per-user guard is checked; the rejection
    // message arrives before ReadyForQuery.
    let msgs = timeout(
        Duration::from_secs(5),
        read_msgs_until_close_or_ready(&mut c3),
    )
    .await
    .unwrap_or_default();
    let rejected = msgs
        .iter()
        .any(|(tag, payload)| *tag == b'E' && String::from_utf8_lossy(payload).contains("53300"));
    assert!(
        rejected,
        "third connection should be rejected with SQLSTATE 53300; got: {:?}",
        msgs.iter().map(|(t, _)| *t).collect::<Vec<_>>()
    );

    let _ = c1.shutdown().await;
    let _ = c2.shutdown().await;
    let _ = c3.shutdown().await;
    server_task.abort();

    // Clean up env var so other tests are not affected.
    std::env::remove_var("NEURALBASE_MAX_CONNECTIONS_PER_USER");
}

// ── Phase 1B: CTEs ────────────────────────────────────────────────────────────

#[test]
fn cte_basic_select_returns_value() {
    let cat = QueryCatalog::new();
    let sql = "WITH t AS (SELECT 1 AS x) SELECT x FROM t";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 1, "expect one row from CTE");
    assert_eq!(result.rows[0][0], ScalarVal::Int(1));
}

#[test]
fn cte_multi_step_is_resolved_in_order() {
    let cat = QueryCatalog::new();
    // Two CTEs: second references first.
    let sql = "WITH a AS (SELECT 1 AS v), b AS (SELECT v FROM a) SELECT v FROM b";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarVal::Int(1));
}

// ── Phase 1B: UNION / INTERSECT / EXCEPT ─────────────────────────────────────

#[test]
fn union_all_produces_two_rows() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 UNION ALL SELECT 2").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 2, "UNION ALL should keep both rows");
}

#[test]
fn union_distinct_deduplicates_rows() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 UNION SELECT 1").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 1, "UNION DISTINCT should deduplicate");
}

#[test]
fn union_all_with_different_values() {
    let cat = QueryCatalog::new();
    let stmt =
        neuralbase::sql::parse_statement("SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30")
            .expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn intersect_returns_common_rows() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 INTERSECT SELECT 1").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(
        result.rows.len(),
        1,
        "INTERSECT should return the common row"
    );
}

#[test]
fn intersect_empty_when_no_common_rows() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 INTERSECT SELECT 2").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(
        result.rows.len(),
        0,
        "INTERSECT should be empty when no common rows"
    );
}

#[test]
fn except_returns_difference() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 EXCEPT SELECT 2").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(
        result.rows.len(),
        1,
        "EXCEPT should return rows from left not in right"
    );
    assert_eq!(result.rows[0][0], ScalarVal::Int(1));
}

#[test]
fn except_removes_matching_row() {
    let cat = QueryCatalog::new();
    let stmt = neuralbase::sql::parse_statement("SELECT 1 EXCEPT SELECT 1").expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert_eq!(result.rows.len(), 0, "EXCEPT of equal sets should be empty");
}

// ── Phase 1B: Window functions ─────────────────────────────────────────────────

#[test]
fn row_number_window_function_assigns_sequential_ranks() {
    use neuralbase::tpch::generate_tpch_data;
    let dataset = generate_tpch_data(0.001);
    let cat = QueryCatalog::from_tpch(&dataset);
    let sql = "SELECT l_orderkey, ROW_NUMBER() OVER (ORDER BY l_orderkey) AS rn \
               FROM lineitem ORDER BY l_orderkey LIMIT 5";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert!(!result.rows.is_empty(), "should return rows");
    // Row numbers should be 1, 2, 3, 4, 5 in ascending order
    for (i, row) in result.rows.iter().enumerate() {
        // rn column is the second one
        assert_eq!(
            row[1],
            ScalarVal::Int(i as i64 + 1),
            "ROW_NUMBER expected {}, got {:?}",
            i + 1,
            row[1]
        );
    }
}

#[test]
fn rank_window_function_with_ties_same_rank() {
    use neuralbase::tpch::generate_tpch_data;
    let dataset = generate_tpch_data(0.001);
    let cat = QueryCatalog::from_tpch(&dataset);
    // Use a query where we know there'll be ties in a constant column (all ties → same rank).
    let sql = "SELECT 1 AS x, RANK() OVER (ORDER BY 1) AS rnk FROM lineitem LIMIT 3";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert!(!result.rows.is_empty(), "should return rows");
    // All rows have the same ORDER BY value (1), so all should have rank 1.
    for row in &result.rows {
        assert_eq!(
            row[1],
            ScalarVal::Int(1),
            "all ties should have RANK=1, got {:?}",
            row[1]
        );
    }
}

#[test]
fn lag_window_function_returns_previous_row_value() {
    use neuralbase::tpch::generate_tpch_data;
    let dataset = generate_tpch_data(0.001);
    let cat = QueryCatalog::from_tpch(&dataset);
    let sql = "SELECT l_orderkey, LAG(l_orderkey) OVER (ORDER BY l_orderkey) AS prev_key \
               FROM lineitem ORDER BY l_orderkey LIMIT 3";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert!(!result.rows.is_empty(), "should return rows");
    // First row has no previous → NULL
    assert_eq!(
        result.rows[0][1],
        ScalarVal::Null,
        "first LAG should be NULL"
    );
    // Second row's prev should equal first row's key
    if result.rows.len() >= 2 {
        assert_eq!(
            result.rows[1][1], result.rows[0][0],
            "second row LAG should equal first row value"
        );
    }
}

#[test]
fn lead_window_function_returns_next_row_value() {
    use neuralbase::tpch::generate_tpch_data;
    let dataset = generate_tpch_data(0.001);
    let cat = QueryCatalog::from_tpch(&dataset);
    let sql = "SELECT l_orderkey, LEAD(l_orderkey) OVER (ORDER BY l_orderkey) AS next_key \
               FROM lineitem ORDER BY l_orderkey LIMIT 3";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert!(!result.rows.is_empty(), "should return rows");
    // Last returned row has no next (or LIMIT cuts it off) → NULL
    if result.rows.len() >= 2 {
        // First row's LEAD should equal second row's key
        assert_eq!(
            result.rows[0][1], result.rows[1][0],
            "first row LEAD should equal second row value"
        );
    }
}

#[test]
fn row_number_with_partition_by() {
    use neuralbase::tpch::generate_tpch_data;
    let dataset = generate_tpch_data(0.001);
    let cat = QueryCatalog::from_tpch(&dataset);
    // Partition by a constant (all rows in same partition), order by key.
    let sql = "SELECT l_orderkey, ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY l_orderkey) AS rn \
               FROM lineitem ORDER BY l_orderkey LIMIT 5";
    let stmt = neuralbase::sql::parse_statement(sql).expect("parse");
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("expected query"),
    };
    let result = execute_select_query(&query, &cat).expect("execute");
    assert!(!result.rows.is_empty());
    // Row numbers should be 1, 2, 3, ...
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(
            row[1],
            ScalarVal::Int(i as i64 + 1),
            "ROW_NUMBER with PARTITION BY 1 expected {}",
            i + 1
        );
    }
}

// ── Phase 1B: EXPLAIN / EXPLAIN ANALYZE ────────────────────────────────────────

#[tokio::test]
async fn explain_select_returns_plan_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    write_startup_message(&mut client).await;
    read_until_ready(&mut client).await;

    write_simple_query(&mut client, "EXPLAIN SELECT 1").await;
    let msgs = read_messages_until_ready(&mut client).await;

    // Should include RowDescription ('T') and at least one DataRow ('D')
    let has_row = msgs.iter().any(|(tag, _)| *tag == b'D');
    assert!(
        has_row,
        "EXPLAIN should return at least one data row; got: {:?}",
        msgs.iter().map(|(t, _)| *t as char).collect::<Vec<_>>()
    );

    // Should NOT include error response
    let has_error = msgs.iter().any(|(tag, _)| *tag == b'E');
    assert!(!has_error, "EXPLAIN should not return an error");

    let _ = client.shutdown().await;
    server_task.abort();
}

#[tokio::test]
async fn explain_analyze_select_returns_timing_info() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    write_startup_message(&mut client).await;
    read_until_ready(&mut client).await;

    write_simple_query(&mut client, "EXPLAIN ANALYZE SELECT 1").await;
    let msgs = read_messages_until_ready(&mut client).await;

    // Should succeed without error
    let has_error = msgs.iter().any(|(tag, _)| *tag == b'E');
    assert!(!has_error, "EXPLAIN ANALYZE should not return an error");

    // At least one data row containing 'Actual time'
    let timing_found = msgs.iter().any(|(tag, payload)| {
        *tag == b'D' && String::from_utf8_lossy(payload).contains("Actual time")
    });
    assert!(
        timing_found,
        "EXPLAIN ANALYZE should include 'Actual time' in output"
    );

    let _ = client.shutdown().await;
    server_task.abort();
}

// ── Phase 1B: Binder produces Explain plan ────────────────────────────────────

#[test]
fn binder_produces_explain_plan_without_analyze() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_nb_statement("EXPLAIN SELECT 1").expect("parse");
    let plan = bind_nb_statement(&stmt, &catalog).expect("bind");
    match plan {
        BoundPlan::Explain { analyze, .. } => {
            assert!(!analyze, "plain EXPLAIN should have analyze=false");
        }
        other => panic!("expected Explain plan, got {other:?}"),
    }
}

#[test]
fn binder_produces_explain_analyze_plan() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_nb_statement("EXPLAIN ANALYZE SELECT 1").expect("parse");
    let plan = bind_nb_statement(&stmt, &catalog).expect("bind");
    match plan {
        BoundPlan::Explain { analyze, .. } => {
            assert!(analyze, "EXPLAIN ANALYZE should have analyze=true");
        }
        other => panic!("expected Explain plan, got {other:?}"),
    }
}

// ── Phase 2: Plan cache unit tests ────────────────────────────────────────────

#[test]
fn plan_cache_hit_rate_above_80_percent() {
    let mut cache = PlanCache::new(500);

    // Cold start: cache miss.
    let plan = {
        let catalog = InMemoryCatalog::with_tpch_lineitem();
        let stmt = parse_nb_statement("SELECT 1").expect("parse");
        bind_nb_statement(&stmt, &catalog).expect("bind")
    };
    let key = "select 1".to_string();
    assert!(cache.get(&key).is_none(), "should be a miss before insert");
    cache.insert(key.clone(), plan);

    // 9 cache hits.
    for _ in 0..9 {
        assert!(cache.get(&key).is_some(), "should be a cache hit");
    }

    let rate = cache.hit_rate();
    assert!(
        rate > 0.80,
        "expected hit rate > 80%, got {:.2}%",
        rate * 100.0
    );
}

#[test]
fn plan_cache_lru_evicts_least_recently_used() {
    let mut cache = PlanCache::new(2);
    let catalog = InMemoryCatalog::with_tpch_lineitem();

    let plan1 = {
        let stmt = parse_nb_statement("SELECT 1").expect("parse");
        bind_nb_statement(&stmt, &catalog).expect("bind")
    };
    let plan2 = {
        let stmt = parse_nb_statement("SELECT 2").expect("parse");
        bind_nb_statement(&stmt, &catalog).expect("bind")
    };
    let plan3 = {
        let stmt = parse_nb_statement("SELECT 3").expect("parse");
        bind_nb_statement(&stmt, &catalog).expect("bind")
    };

    cache.insert("select 1".to_string(), plan1);
    cache.insert("select 2".to_string(), plan2);
    // Access select 2 to make select 1 the LRU.
    cache.get("select 2");
    // Insert select 3: should evict select 1 (LRU).
    cache.insert("select 3".to_string(), plan3);

    assert!(
        cache.get("select 1").is_none(),
        "LRU entry should be evicted"
    );
    assert!(
        cache.get("select 2").is_some(),
        "MRU entry should be retained"
    );
    assert!(
        cache.get("select 3").is_some(),
        "newest entry should be present"
    );
}

#[test]
fn plan_cache_invalidate_all_clears_entries() {
    let mut cache = PlanCache::new(500);
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let plan = {
        let stmt = parse_nb_statement("SELECT 1").expect("parse");
        bind_nb_statement(&stmt, &catalog).expect("bind")
    };
    cache.insert("select 1".to_string(), plan);
    assert!(cache.get("select 1").is_some());

    cache.invalidate_all();
    // Reset hit/miss stats by checking again
    assert!(
        cache.get("select 1").is_none(),
        "cache should be empty after invalidate_all"
    );
}

// ── Phase 2: Extended query protocol (P/B/E/S) ──────────────────────────────

/// Build a 'P' (Parse) message: name\0 sql\0 num_params:i16
fn build_parse_message(name: &str, sql: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i16.to_be_bytes()); // 0 param types
    let mut msg = Vec::new();
    msg.push(b'P');
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Build a 'B' (Bind) message: portal\0 stmt\0 0:i16 0:i16 0:i16
fn build_bind_message(portal: &str, stmt: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(stmt.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i16.to_be_bytes()); // num format codes
    body.extend_from_slice(&0_i16.to_be_bytes()); // num param values
    body.extend_from_slice(&0_i16.to_be_bytes()); // num result format codes
    let mut msg = Vec::new();
    msg.push(b'B');
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Build an 'E' (Execute) message: portal\0 max_rows:i32
fn build_execute_message(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());
    let mut msg = Vec::new();
    msg.push(b'E');
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Build an 'S' (Sync) message.
fn build_sync_message() -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'S');
    msg.extend_from_slice(&4_i32.to_be_bytes());
    msg
}

#[tokio::test]
async fn extended_protocol_parse_bind_execute_select_const() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    write_startup_message(&mut client).await;
    read_until_ready(&mut client).await;

    // Prepare "SELECT 42"
    client
        .write_all(&build_parse_message("s1", "SELECT 42"))
        .await
        .expect("parse");
    // Bind to unnamed portal
    client
        .write_all(&build_bind_message("p1", "s1"))
        .await
        .expect("bind");
    // Execute portal
    client
        .write_all(&build_execute_message("p1", 0))
        .await
        .expect("execute");
    // Sync
    client.write_all(&build_sync_message()).await.expect("sync");

    // Collect messages until ReadyForQuery
    let msgs = timeout(
        Duration::from_secs(5),
        read_messages_until_ready(&mut client),
    )
    .await
    .expect("timeout");

    // Should have ParseComplete ('1'), BindComplete ('2'), then data + ReadyForQuery
    let has_parse_complete = msgs.iter().any(|(t, _)| *t == b'1');
    let has_bind_complete = msgs.iter().any(|(t, _)| *t == b'2');
    let has_no_error = !msgs.iter().any(|(t, _)| *t == b'E');
    let has_ready = msgs.iter().any(|(t, _)| *t == b'Z');

    assert!(
        has_parse_complete,
        "should get ParseComplete; got {:?}",
        msgs.iter().map(|(t, _)| *t as char).collect::<Vec<_>>()
    );
    assert!(has_bind_complete, "should get BindComplete");
    assert!(has_no_error, "should not get error");
    assert!(has_ready, "should get ReadyForQuery");

    let _ = client.shutdown().await;
    server_task.abort();
}

#[tokio::test]
async fn extended_protocol_execute_cached_plan_multiple_times() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    write_startup_message(&mut client).await;
    read_until_ready(&mut client).await;

    // Prepare once, execute 3 times (exercising plan reuse via portal re-bind).
    client
        .write_all(&build_parse_message("s1", "SELECT 1"))
        .await
        .expect("parse");
    let (parse_tag, _) = read_one_message(&mut client).await.expect("ParseComplete");
    assert_eq!(parse_tag, b'1', "expected ParseComplete");

    for i in 0..3 {
        let portal = format!("p{i}");
        client
            .write_all(&build_bind_message(&portal, "s1"))
            .await
            .expect("bind");
        let (_bind_tag, _) = read_one_message(&mut client).await.expect("BindComplete");

        client
            .write_all(&build_execute_message(&portal, 0))
            .await
            .expect("execute");
        // Collect until we get a CommandComplete ('C')
        let mut got_command = false;
        for _ in 0..10 {
            let (tag, _) = read_one_message(&mut client)
                .await
                .expect("read during execute");
            if tag == b'C' {
                got_command = true;
                break;
            }
            if tag == b'E' {
                panic!("got error on iteration {i}");
            }
        }
        assert!(got_command, "expected CommandComplete on iteration {i}");

        client.write_all(&build_sync_message()).await.expect("sync");
        let (sync_tag, _) = read_one_message(&mut client)
            .await
            .expect("ReadyForQuery after Sync");
        assert_eq!(
            sync_tag, b'Z',
            "expected ReadyForQuery after Sync on iteration {i}"
        );
    }

    let _ = client.shutdown().await;
    server_task.abort();
}
