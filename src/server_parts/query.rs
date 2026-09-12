fn nb_statement_is_read(statement: &crate::sql::NbStatement) -> bool {
    matches!(
        statement,
        crate::sql::NbStatement::Sql(statement)
            if matches!(statement.as_ref(), Statement::Query(_) | Statement::Explain { .. })
    )
}

#[allow(clippy::too_many_arguments)]
async fn process_query<S>(
    socket: &mut S,
    sql: &str,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
    dml_exec: Option<&StorageExecutor>,
    storage_engine: Option<&Arc<StorageEngine>>,
    registry: &RwLock<UserRegistry>,
    users_file: &str,
    plan_cache: &Mutex<PlanCache>,
    read_consistency: &mut crate::read_consistency::ReadConsistency,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::read_consistency::{
        parse_read_consistency_setting, ReadConsistencySetting,
    };

    match parse_read_consistency_setting(sql) {
        ReadConsistencySetting::Set(mode) => {
            *read_consistency = mode;
            socket.write_all(&build_command_complete("SET")).await?;
            return Ok(());
        }
        ReadConsistencySetting::Invalid(error) => {
            write_error_and_ready(socket, &error, "22023").await?;
            return Ok(());
        }
        ReadConsistencySetting::NotSetting => {}
    }

    // Parse once before binding so a strong read fences catalog access as well
    // as table-data execution. The legacy inner path deliberately parses again;
    // that keeps all historical binding/error behavior unchanged.
    let classification = match parse_nb_statement(sql) {
        Ok(statement) => statement,
        Err(_) => {
            return process_query_inner(
                socket,
                sql,
                catalog,
                storage,
                dml_exec,
                storage_engine,
                registry,
                users_file,
                plan_cache,
            )
            .await;
        }
    };

    if nb_statement_is_read(&classification) {
        let gateway = replicated_sql_gateway();
        if let Err(error) = crate::read_barrier::prepare_read(gateway.as_deref(), *read_consistency).await {
            write_read_barrier_error(socket, &error).await?;
            return Ok(());
        }
    }

    process_query_inner(
        socket,
        sql,
        catalog,
        storage,
        dml_exec,
        storage_engine,
        registry,
        users_file,
        plan_cache,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_query_inner<S>(
    socket: &mut S,
    sql: &str,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
    dml_exec: Option<&StorageExecutor>,
    storage_engine: Option<&Arc<StorageEngine>>,
    registry: &RwLock<UserRegistry>,
    users_file: &str,
    plan_cache: &Mutex<PlanCache>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let nb_stmt = match parse_nb_statement(sql) {
        Ok(s) => s,
        Err(err) => {
            write_error_and_ready(socket, &err.to_string(), "42601").await?;
            return Ok(());
        }
    };

    // A newly elected leader may have the committed schema/data in its Raft
    // log before its local state machine has applied it. Commit a current-term
    // barrier before the binder reads catalog state for table mutations.
    if is_persistent_table_mutation_sql(sql) {
        if let Some(gateway) = replicated_sql_gateway() {
            if let Err(error) = gateway.prepare_mutation().await {
                write_replicated_error(socket, &error).await?;
                return Ok(());
            }
        }
    }

    let norm = normalize_sql(sql);
    let cached_plan = {
        let mut cache = plan_cache.lock().unwrap();
        cache.get(&norm).cloned()
    };
    let plan = if let Some(p) = cached_plan {
        p
    } else {
        let p = match bind_nb_statement(&nb_stmt, catalog) {
            Ok(plan) => plan,
            Err(err) => {
                write_error_and_ready(socket, &err.to_string(), "42P01").await?;
                return Ok(());
            }
        };
        {
            let mut cache = plan_cache.lock().unwrap();
            match &p {
                BoundPlan::SelectConstI64(_)
                | BoundPlan::SelectFromTable { .. }
                | BoundPlan::SelectQuery(_)
                | BoundPlan::Explain { .. } => {
                    cache.insert(norm, p.clone());
                }
                _ => cache.invalidate_all(),
            }
        }
        p
    };

    match plan {
        BoundPlan::SelectConstI64(value) => {
            let batch = mock_const_batch(value);
            write_batch(socket, &batch).await?;
        }
        BoundPlan::SelectFromTable { .. } => {
            let physical_plan = build_physical_plan(&plan);
            let dataset = generate_tpch_data(0.1);
            let scheduler = MorselScheduler::new(16_384);
            match execute_physical_plan(&physical_plan, &dataset, &scheduler, storage) {
                Ok(batch) => write_batch(socket, &batch).await?,
                Err(err) => write_error_and_ready(socket, &err.to_string(), "22000").await?,
            }
        }
        BoundPlan::SelectQuery(query) => {
            let dataset = generate_tpch_data(0.1);
            let mut qcat = QueryCatalog::from_tpch(&dataset);
            for schema in catalog.all_tables() {
                if !qcat.tables.contains_key(&schema.name.to_lowercase()) {
                    if let Some(scanner) = storage {
                        if let Ok(batch) = scanner.scan_table(&schema.name) {
                            qcat.add_batch(&schema.name, &batch);
                        }
                    }
                }
            }
            match crate::query_executor::execute_select_query(&query, &qcat) {
                Ok(result) => write_batch(socket, &query_result_to_batch(result)).await?,
                Err(err) => write_error_and_ready(socket, &err.to_string(), "22000").await?,
            }
        }
        BoundPlan::DropTable { name } => {
            if let Some(gateway) = replicated_sql_gateway() {
                match gateway.drop_table(&name).await {
                    Ok(_) => {
                        socket
                            .write_all(&build_command_complete("DROP TABLE"))
                            .await?
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                catalog.drop_table(&name);
                if let Some(engine) = storage_engine {
                    let rdb = RocksDbCatalog::new(engine.clone());
                    if let Err(e) = rdb.unregister_table(&name) {
                        tracing::warn!(error = %e, table = %name, "failed to remove persisted schema entry");
                    }
                    if let Err(e) = engine.clear_table_data(table_id_for(&name)) {
                        tracing::warn!(error = %e, table = %name, "failed to clear dropped table data");
                    }
                }
                socket
                    .write_all(&build_command_complete("DROP TABLE"))
                    .await?;
            }
        }
        BoundPlan::CreateTable(create_plan) => {
            let schema = create_plan.to_table_schema();
            if let Some(gateway) = replicated_sql_gateway() {
                match gateway.create_table(schema).await {
                    Ok(_) => {
                        socket
                            .write_all(&build_command_complete("CREATE TABLE"))
                            .await?
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                catalog.create_table(schema.clone());
                if let Some(engine) = storage_engine {
                    let rdb = RocksDbCatalog::new(engine.clone());
                    if let Err(e) = rdb.register_table(&schema) {
                        tracing::warn!(error = %e, "Failed to persist schema entry");
                    }
                }
                socket
                    .write_all(&build_command_complete("CREATE TABLE"))
                    .await?;
            }
        }
        BoundPlan::Insert(insert_plan) => {
            if let Some(gateway) = replicated_sql_gateway() {
                match gateway.insert(&insert_plan).await {
                    Ok(ack) => {
                        let count = ack.affected_rows.unwrap_or(0);
                        socket
                            .write_all(&build_command_complete(&format!("INSERT 0 {count}")))
                            .await?;
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let Some(exec) = dml_exec else {
                    write_error_and_ready(
                        socket,
                        "storage not available (DB_PATH not set)",
                        "55000",
                    )
                    .await?;
                    return Ok(());
                };
                let mut count = 0u64;
                for row_values in &insert_plan.rows {
                    let pairs: Vec<(String, String)> = insert_plan
                        .columns
                        .iter()
                        .zip(row_values)
                        .map(|(col, val)| {
                            (col.clone(), val.to_storage_string().unwrap_or_default())
                        })
                        .collect();
                    let str_pairs: Vec<(&str, &str)> = pairs
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    let pk = exec.next_pk();
                    match exec.insert_row(&insert_plan.table.name, &pk, &str_pairs) {
                        Ok(()) => count += 1,
                        Err(e) => {
                            write_error_and_ready(socket, &e.to_string(), "22000").await?;
                            return Ok(());
                        }
                    }
                }
                socket
                    .write_all(&build_command_complete(&format!("INSERT 0 {count}")))
                    .await?;
            }
        }
        BoundPlan::Update(update_plan) => {
            if let Some(gateway) = replicated_sql_gateway() {
                match gateway.update(&update_plan).await {
                    Ok(ack) => {
                        let count = ack.affected_rows.unwrap_or(0);
                        socket
                            .write_all(&build_command_complete(&format!("UPDATE {count}")))
                            .await?;
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let Some(exec) = dml_exec else {
                    write_error_and_ready(
                        socket,
                        "storage not available (DB_PATH not set)",
                        "55000",
                    )
                    .await?;
                    return Ok(());
                };
                let assignments: Vec<(String, String)> = update_plan
                    .assignments
                    .iter()
                    .map(|(col, val)| (col.clone(), val.to_storage_string().unwrap_or_default()))
                    .collect();
                match exec.update_rows(
                    &update_plan.table.name,
                    &assignments,
                    update_plan.predicate.as_ref(),
                ) {
                    Ok(n) => {
                        socket
                            .write_all(&build_command_complete(&format!("UPDATE {n}")))
                            .await?
                    }
                    Err(e) => write_error_and_ready(socket, &e.to_string(), "22000").await?,
                }
            }
        }
        BoundPlan::Delete(delete_plan) => {
            if let Some(gateway) = replicated_sql_gateway() {
                match gateway.delete(&delete_plan).await {
                    Ok(ack) => {
                        let count = ack.affected_rows.unwrap_or(0);
                        socket
                            .write_all(&build_command_complete(&format!("DELETE {count}")))
                            .await?;
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let Some(exec) = dml_exec else {
                    write_error_and_ready(
                        socket,
                        "storage not available (DB_PATH not set)",
                        "55000",
                    )
                    .await?;
                    return Ok(());
                };
                match exec.delete_rows(&delete_plan.table.name, delete_plan.predicate.as_ref()) {
                    Ok(n) => {
                        socket
                            .write_all(&build_command_complete(&format!("DELETE {n}")))
                            .await?
                    }
                    Err(e) => write_error_and_ready(socket, &e.to_string(), "22000").await?,
                }
            }
        }
        BoundPlan::CreateUser { username, password } => {
            if let Some(gateway) = replicated_sql_gateway() {
                if !prepare_replicated_identity_mutation(
                    socket,
                    &gateway,
                    storage_engine,
                    users_file,
                )
                .await?
                {
                    return Ok(());
                }
                let initialize_if_empty = allow_empty_identity_bootstrap(users_file);
                match gateway
                    .create_user(&username, &password, initialize_if_empty)
                    .await
                {
                    Ok(_) => {
                        socket
                            .write_all(&build_command_complete("CREATE USER"))
                            .await?
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let new_record = create_scram_user(&username, &password);
                {
                    let mut reg = registry.write().await;
                    reg.add_user(new_record);
                    if let Err(e) = reg.save_to_file(users_file) {
                        write_error_and_ready(
                            socket,
                            &format!("failed to persist user registry: {e}"),
                            "58030",
                        )
                        .await?;
                        return Ok(());
                    }
                }
                socket
                    .write_all(&build_command_complete("CREATE USER"))
                    .await?;
            }
        }
        BoundPlan::AlterUser {
            username,
            new_password,
        } => {
            if let Some(gateway) = replicated_sql_gateway() {
                if !prepare_replicated_identity_mutation(
                    socket,
                    &gateway,
                    storage_engine,
                    users_file,
                )
                .await?
                {
                    return Ok(());
                }
                match gateway.alter_user(&username, &new_password).await {
                    Ok(_) => {
                        socket
                            .write_all(&build_command_complete("ALTER USER"))
                            .await?
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let updated = create_scram_user(&username, &new_password);
                let updated_ok = {
                    let mut reg = registry.write().await;
                    if reg.update_user(updated) {
                        if let Err(e) = reg.save_to_file(users_file) {
                            write_error_and_ready(
                                socket,
                                &format!("failed to persist user registry: {e}"),
                                "58030",
                            )
                            .await?;
                            return Ok(());
                        }
                        true
                    } else {
                        false
                    }
                };
                if updated_ok {
                    socket
                        .write_all(&build_command_complete("ALTER USER"))
                        .await?;
                } else {
                    write_error_and_ready(socket, &format!("user not found: {username}"), "42704")
                        .await?;
                }
            }
        }
        BoundPlan::DropUser {
            username,
            if_exists,
        } => {
            if let Some(gateway) = replicated_sql_gateway() {
                if !prepare_replicated_identity_mutation(
                    socket,
                    &gateway,
                    storage_engine,
                    users_file,
                )
                .await?
                {
                    return Ok(());
                }
                match gateway.drop_user(&username, if_exists).await {
                    Ok(_) => {
                        socket
                            .write_all(&build_command_complete("DROP USER"))
                            .await?
                    }
                    Err(error) => write_replicated_error(socket, &error).await?,
                }
            } else {
                let removed = {
                    let mut reg = registry.write().await;
                    let removed = reg.remove_user(&username);
                    if removed || if_exists {
                        if let Err(e) = reg.save_to_file(users_file) {
                            write_error_and_ready(
                                socket,
                                &format!("failed to persist user registry: {e}"),
                                "58030",
                            )
                            .await?;
                            return Ok(());
                        }
                    }
                    removed
                };
                if !removed && !if_exists {
                    write_error_and_ready(socket, &format!("user not found: {username}"), "42704")
                        .await?;
                } else {
                    socket
                        .write_all(&build_command_complete("DROP USER"))
                        .await?;
                }
            }
        }
        BoundPlan::Explain { query, analyze } => {
            let plan_text = "PhysicalPlan: SeqScan -> Project".to_string();
            let explain_text = if analyze {
                let start = std::time::Instant::now();
                let dataset = generate_tpch_data(0.1);
                let mut qcat = QueryCatalog::from_tpch(&dataset);
                for schema in catalog.all_tables() {
                    if !qcat.tables.contains_key(&schema.name.to_lowercase()) {
                        if let Some(sc) = storage {
                            if let Ok(batch) = sc.scan_table(&schema.name) {
                                qcat.add_batch(&schema.name, &batch);
                            }
                        }
                    }
                }
                let elapsed_ms = match crate::query_executor::execute_select_query(&query, &qcat) {
                    Ok(_) | Err(_) => start.elapsed().as_millis(),
                };
                format!("{plan_text}\nActual time: {elapsed_ms}ms")
            } else {
                plan_text
            };
            write_batch(socket, &explain_text_to_batch(&explain_text)).await?;
        }
    }

    Ok(())
}
