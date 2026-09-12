async fn prepare_replicated_identity_mutation<S>(
    socket: &mut S,
    gateway: &ReplicatedSqlGateway,
    storage_engine: Option<&Arc<StorageEngine>>,
    users_file: &str,
) -> std::io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let Some(engine) = storage_engine else {
        write_error_and_ready(
            socket,
            "replicated identity storage is unavailable",
            "55000",
        )
        .await?;
        return Ok(false);
    };

    match migrate_legacy_identity_if_configured(gateway, engine, users_file).await {
        Ok(_) => Ok(true),
        Err(ReplicatedIdentityRuntimeError::Gateway(error)) => {
            write_replicated_error(socket, &error).await?;
            Ok(false)
        }
        Err(error) => {
            write_error_and_ready(
                socket,
                &format!("replicated identity migration failed: {error}"),
                "58030",
            )
            .await?;
            Ok(false)
        }
    }
}

async fn write_read_barrier_error<S>(
    socket: &mut S,
    error: &crate::read_barrier::ReadBarrierError,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    use crate::read_barrier::ReadBarrierError;

    match error {
        ReadBarrierError::ClusterRequired(mode) => {
            write_error_and_ready(
                socket,
                &format!("{mode} read consistency requires clustered Raft mode"),
                "0A000",
            )
            .await
        }
        ReadBarrierError::Timeout => {
            write_error_and_ready(
                socket,
                "strong-read authority barrier timed out before quorum/apply confirmation",
                "57014",
            )
            .await
        }
        ReadBarrierError::Gateway(ReplicatedGatewayError::NotLeader { leader }) => {
            let message = match leader {
                Some(leader) => {
                    format!("strong read requires current Raft leader; retry on leader {leader}")
                }
                None => "strong read requires current Raft leader; leader currently unknown"
                    .to_string(),
            };
            write_error_and_ready(socket, &message, "25006").await
        }
        ReadBarrierError::Gateway(ReplicatedGatewayError::CatchingUp) => {
            write_error_and_ready(socket, &error.to_string(), "57P03").await
        }
        ReadBarrierError::Gateway(_) => {
            write_error_and_ready(
                socket,
                &format!("strong-read consensus barrier failed: {error}"),
                "58030",
            )
            .await
        }
    }
}

async fn write_replicated_error<S>(
    socket: &mut S,
    error: &ReplicatedGatewayError,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    match error {
        ReplicatedGatewayError::NotLeader { leader } => {
            let message = match leader {
                Some(leader) => format!("not Raft leader; retry write on leader {leader}"),
                None => "not Raft leader; leader currently unknown".to_string(),
            };
            write_error_and_ready(socket, &message, "25006").await
        }
        ReplicatedGatewayError::CatchingUp => {
            write_error_and_ready(socket, &error.to_string(), "57P03").await
        }
        ReplicatedGatewayError::UserAlreadyExists(_) => {
            write_error_and_ready(socket, &error.to_string(), "42710").await
        }
        ReplicatedGatewayError::UserNotFound(_) => {
            write_error_and_ready(socket, &error.to_string(), "42704").await
        }
        ReplicatedGatewayError::IdentityNotInitialized
        | ReplicatedGatewayError::IdentityInitializationConflict => {
            write_error_and_ready(socket, &error.to_string(), "55000").await
        }
        _ => {
            write_error_and_ready(
                socket,
                &format!("replicated mutation failed: {error}"),
                "58030",
            )
            .await
        }
    }
}

async fn write_batch<S>(
    socket: &mut S,
    batch: &crate::vectorized::RecordBatch,
) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let (columns, rows) = batch_to_pg_rows(batch);
    let row_count = rows.len();
    let cols = columns
        .iter()
        .map(|(name, oid, size)| (name.as_str(), *oid, *size))
        .collect::<Vec<_>>();
    socket.write_all(&build_row_description(&cols)).await?;
    for row in rows {
        socket.write_all(&build_data_row(&row)).await?;
    }
    socket
        .write_all(&build_command_complete(&format!("SELECT {row_count}")))
        .await?;
    Ok(())
}

async fn write_error_and_ready<S>(socket: &mut S, message: &str, code: &str) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    socket
        .write_all(&build_error_response(message, code))
        .await?;
    socket.write_all(&build_ready_for_query()).await?;
    Ok(())
}

fn extract_query_pattern(sql: &str, elapsed_us: u64) -> Option<QueryPattern> {
    let stmt = parse_statement(sql).ok()?;
    let Statement::Query(q) = &stmt else {
        return None;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };

    let tables: Vec<String> = sel
        .from
        .iter()
        .filter_map(|tw| {
            if let TableFactor::Table { name, .. } = &tw.relation {
                Some(name.to_string().to_lowercase())
            } else {
                None
            }
        })
        .collect();

    if tables.is_empty() {
        return None;
    }

    let primary_table = &tables[0];
    let predicate_columns = extract_where_columns(&sel.selection, primary_table);

    Some(QueryPattern {
        tables,
        predicate_columns,
        join_columns: vec![],
        rows_scanned: 0,
        rows_returned: 0,
        elapsed_us,
        recorded_at: std::time::Instant::now(),
    })
}

fn extract_where_columns(expr: &Option<Expr>, table: &str) -> Vec<(String, String)> {
    let Some(e) = expr else {
        return vec![];
    };
    let mut cols = vec![];
    collect_col_refs(e, table, &mut cols);
    cols
}

fn collect_col_refs(expr: &Expr, table: &str, out: &mut Vec<(String, String)>) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_col_refs(left, table, out);
            collect_col_refs(right, table, out);
        }
        Expr::Identifier(ident) => {
            out.push((table.to_string(), ident.value.to_lowercase()));
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(col) = parts.last() {
                out.push((table.to_string(), col.value.to_lowercase()));
            }
        }
        Expr::Nested(inner) => collect_col_refs(inner, table, out),
        _ => {}
    }
}
