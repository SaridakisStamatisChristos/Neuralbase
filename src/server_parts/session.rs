async fn handle_client_stream<S>(mut socket: S, ctx: ClientSessionContext) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let username = match startup_and_auth(
        &mut socket,
        &ctx.registry,
        ctx.storage_engine.as_ref(),
        &ctx.users_file,
    )
    .await
    {
        Ok(u) => u,
        Err(_) => return Ok(()),
    };

    if !ctx.user_tracker.try_acquire(&username) {
        counter!("rejected_connections_per_user_total").increment(1);
        tracing::warn!(%username, "rejecting connection: per-user limit exceeded");
        let _ = socket
            .write_all(&build_error_response(
                "too many connections for this user",
                "53300",
            ))
            .await;
        let _ = socket.shutdown().await;
        return Ok(());
    }
    let _user_guard = UserConnectionGuard {
        tracker: ctx.user_tracker.clone(),
        username: username.clone(),
    };

    socket
        .write_all(&build_parameter_status("server_version", "16.0"))
        .await?;
    socket
        .write_all(&build_parameter_status("client_encoding", "UTF8"))
        .await?;
    socket.write_all(&build_backend_key_data(42, 7)).await?;
    socket.write_all(&build_ready_for_query()).await?;

    let mut stmt_cache: HashMap<String, PreparedStatement> = HashMap::new();
    let mut portal_cache: HashMap<String, Portal> = HashMap::new();
    // Phase 6 keeps the historical local/stale behavior as the per-connection
    // default. SET neuralbase_read_consistency changes only this connection.
    let mut read_consistency = crate::read_consistency::ReadConsistency::Local;

    loop {
        let mut message_type = [0_u8; 1];
        match socket.read_exact(&mut message_type).await {
            Ok(_) => {}
            Err(_) => return Ok(()),
        }

        let mut len_bytes = [0_u8; 4];
        if socket.read_exact(&mut len_bytes).await.is_err() {
            return Ok(());
        }
        let len = i32::from_be_bytes(len_bytes);
        let payload_len = match parse_message_length(len) {
            Ok(len) => len,
            Err(err) => {
                write_error_and_ready(&mut socket, &err.to_string(), "08P01").await?;
                continue;
            }
        };

        let mut payload = vec![0_u8; payload_len];
        if socket.read_exact(&mut payload).await.is_err() {
            return Ok(());
        }

        match message_type[0] {
            b'Q' => {
                let sql_text = String::from_utf8_lossy(&payload)
                    .trim_end_matches('\0')
                    .to_string();
                let start = std::time::Instant::now();
                let scanner: Option<&dyn TableScanner> =
                    ctx.dml_exec.as_deref().map(|s| s as &dyn TableScanner);
                process_query(
                    &mut socket,
                    &sql_text,
                    &ctx.catalog,
                    scanner,
                    ctx.dml_exec.as_deref(),
                    ctx.storage_engine.as_ref(),
                    &ctx.registry,
                    &ctx.users_file,
                    &ctx.plan_cache,
                    &mut read_consistency,
                )
                .await?;
                let elapsed_us = start.elapsed().as_micros() as u64;

                if let Some(pattern) = extract_query_pattern(&sql_text, elapsed_us) {
                    ctx.advisor.record_query(pattern);
                    let n = ctx.query_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(100) {
                        let adv = Arc::clone(&ctx.advisor);
                        let exec = Arc::clone(&ctx.executor);
                        let eng = ctx.storage_engine.clone();
                        if ctx
                            .advisor_inflight
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            let inflight_task = Arc::clone(&ctx.advisor_inflight);
                            tokio::spawn(async move {
                                let decisions = adv.advise();
                                if let Some(engine) = eng {
                                    if let Ok(cfs) = engine.list_index_cfs() {
                                        for cf in cfs {
                                            adv.register_index(cf);
                                        }
                                    }
                                    let results = exec.apply(&decisions, &engine);
                                    for result in &results {
                                        match result {
                                            DdlResult::Created { index_name } => {
                                                adv.touch_index(index_name);
                                                tracing::info!(index_name, "index created");
                                            }
                                            DdlResult::Dropped { index_name } => {
                                                tracing::info!(index_name, "index dropped");
                                            }
                                            DdlResult::Skipped { index_name, reason } => {
                                                tracing::debug!(
                                                    index_name,
                                                    reason,
                                                    "index ddl skipped"
                                                );
                                            }
                                            DdlResult::Failed { index_name, error } => {
                                                tracing::warn!(
                                                    index_name,
                                                    error,
                                                    "index ddl failed"
                                                );
                                            }
                                        }
                                    }
                                    tracing::debug!(
                                        active_indexes = exec.applied_indexes().len(),
                                        "advisor DDL cycle complete"
                                    );
                                }

                                for d in &decisions {
                                    match d {
                                        IndexDecision::Create {
                                            candidate,
                                            estimated_benefit,
                                            estimated_cost_bytes,
                                            reason,
                                        } => {
                                            tracing::debug!(
                                                index = candidate.index_name(),
                                                benefit = estimated_benefit,
                                                cost_bytes = estimated_cost_bytes,
                                                %reason,
                                                "advisor: CREATE INDEX"
                                            );
                                        }
                                        IndexDecision::Drop {
                                            index_name,
                                            unused_for,
                                            reason,
                                        } => {
                                            tracing::debug!(
                                                %index_name,
                                                unused_secs = unused_for.as_secs(),
                                                %reason,
                                                "advisor: DROP INDEX"
                                            );
                                        }
                                    }
                                }
                                inflight_task.store(false, Ordering::Release);
                            });
                        }
                    }
                }

                socket.write_all(&build_ready_for_query()).await?;
            }
            b'P' => {
                let mut cursor = 0usize;
                let name = read_cstring(&payload, &mut cursor);
                let sql = read_cstring(&payload, &mut cursor);
                let nparams = if cursor + 2 <= payload.len() {
                    let n = i16::from_be_bytes([payload[cursor], payload[cursor + 1]]) as usize;
                    cursor += 2;
                    n
                } else {
                    0
                };
                let mut ptypes: Vec<i32> = Vec::with_capacity(nparams);
                for _ in 0..nparams {
                    if cursor + 4 <= payload.len() {
                        let oid = i32::from_be_bytes([
                            payload[cursor],
                            payload[cursor + 1],
                            payload[cursor + 2],
                            payload[cursor + 3],
                        ]);
                        ptypes.push(oid);
                        cursor += 4;
                    }
                }
                if stmt_cache.len() >= STMT_CACHE_MAX_SIZE {
                    stmt_cache.clear();
                }
                let _ = ptypes;
                stmt_cache.insert(name, PreparedStatement { sql });
                socket.write_all(&build_parse_complete()).await?;
            }
            b'B' => {
                let mut cursor = 0usize;
                let portal_name = read_cstring(&payload, &mut cursor);
                let stmt_name = read_cstring(&payload, &mut cursor);
                let sql = stmt_cache
                    .get(&stmt_name)
                    .map(|s| s.sql.clone())
                    .unwrap_or_default();
                portal_cache.insert(portal_name, Portal { sql });
                socket.write_all(&build_bind_complete()).await?;
            }
            b'D' => {
                socket.write_all(&build_no_data()).await?;
            }
            b'E' => {
                let mut cursor = 0usize;
                let portal_name = read_cstring(&payload, &mut cursor);
                let sql = portal_cache
                    .get(&portal_name)
                    .map(|p| p.sql.clone())
                    .unwrap_or_default();
                if !sql.is_empty() {
                    let scanner: Option<&dyn TableScanner> =
                        ctx.dml_exec.as_deref().map(|s| s as &dyn TableScanner);
                    process_query(
                        &mut socket,
                        &sql,
                        &ctx.catalog,
                        scanner,
                        ctx.dml_exec.as_deref(),
                        ctx.storage_engine.as_ref(),
                        &ctx.registry,
                        &ctx.users_file,
                        &ctx.plan_cache,
                        &mut read_consistency,
                    )
                    .await?;
                } else {
                    socket
                        .write_all(&build_command_complete("EXECUTE 0"))
                        .await?;
                }
            }
            b'S' => {
                socket.write_all(&build_ready_for_query()).await?;
            }
            b'C' => {
                socket.write_all(&build_close_complete()).await?;
            }
            b'X' => return Ok(()),
            _ => {
                write_error_and_ready(&mut socket, "unsupported frontend message", "0A000").await?;
            }
        }
    }
}
