async fn startup_and_auth<S>(
    socket: &mut S,
    registry: &RwLock<UserRegistry>,
    storage_engine: Option<&Arc<StorageEngine>>,
    users_file: &str,
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (username, payload) = read_startup_message(socket).await?;
    let cluster_gateway = replicated_sql_gateway();

    if let Some(gateway) = cluster_gateway.as_ref() {
        if !gateway.serving_ready() {
            let _ = socket
                .write_all(&build_error_response(
                    "cluster node is catching up replicated state",
                    "57P03",
                ))
                .await;
            return Err(ProtocolError::InvalidLength(0));
        }
        let Some(engine) = storage_engine else {
            tracing::error!("replicated identity configured without durable storage engine");
            let _ = socket
                .write_all(&build_error_response(
                    "replicated identity storage is unavailable",
                    "55000",
                ))
                .await;
            return Err(ProtocolError::InvalidLength(0));
        };

        match migrate_legacy_identity_if_configured(gateway, engine, users_file).await {
            Ok(migrated) => {
                if migrated {
                    tracing::info!(users_file, "legacy identity registry migrated through Raft");
                }
            }
            Err(ReplicatedIdentityRuntimeError::Gateway(ReplicatedGatewayError::NotLeader {
                leader,
            })) => {
                let message = match leader {
                    Some(leader) => format!(
                        "replicated identity migration is pending; connect once to leader {leader}"
                    ),
                    None => "replicated identity migration is pending; Raft leader is unknown"
                        .to_string(),
                };
                let _ = socket
                    .write_all(&build_error_response(&message, "57P03"))
                    .await;
                return Err(ProtocolError::InvalidLength(0));
            }
            Err(error) => {
                tracing::error!(%error, users_file, "replicated identity migration failed closed");
                let _ = socket
                    .write_all(&build_error_response(
                        &format!("replicated identity migration failed: {error}"),
                        "58030",
                    ))
                    .await;
                return Err(ProtocolError::InvalidLength(0));
            }
        }
    }

    let require_auth = registry.read().await.require_auth;
    if !require_auth {
        socket
            .write_all(&build_auth_ok())
            .await
            .map_err(|_| ProtocolError::InvalidLength(0))?;
        return Ok(username);
    }

    let cred = if cluster_gateway.is_some() {
        let Some(engine) = storage_engine else {
            return Err(ProtocolError::InvalidLength(0));
        };
        match replicated_identity_initialized(engine) {
            Ok(true) => {}
            Ok(false) => {
                let message = format!(
                    "cluster identity is not initialized; configure {IDENTITY_MIGRATION_SHA256_ENV} with the selected legacy users.json digest"
                );
                let _ = socket
                    .write_all(&build_error_response(&message, "28000"))
                    .await;
                return Err(ProtocolError::InvalidLength(0));
            }
            Err(error) => {
                tracing::error!(%error, "cannot read authoritative replicated identity state");
                let _ = socket
                    .write_all(&build_error_response(
                        "replicated identity state is unavailable",
                        "58030",
                    ))
                    .await;
                return Err(ProtocolError::InvalidLength(0));
            }
        }

        match replicated_user_record(engine, &username) {
            Ok(Some(user)) => user.credential,
            Ok(None) => {
                let err = build_error_response(
                    &format!("password authentication failed for user \"{username}\""),
                    "28P01",
                );
                let _ = socket.write_all(&err).await;
                return Err(ProtocolError::InvalidLength(0));
            }
            Err(error) => {
                tracing::error!(%error, %username, "cannot read replicated identity user");
                let _ = socket
                    .write_all(&build_error_response(
                        "replicated identity state is unavailable",
                        "58030",
                    ))
                    .await;
                return Err(ProtocolError::InvalidLength(0));
            }
        }
    } else {
        let reg = registry.read().await;
        match reg.get_user(&username) {
            Some(user) => user.credential.clone(),
            None => {
                drop(reg);
                let err = build_error_response(
                    &format!("password authentication failed for user \"{username}\""),
                    "28P01",
                );
                let _ = socket.write_all(&err).await;
                return Err(ProtocolError::InvalidLength(0));
            }
        }
    };

    match cred {
        StoredCredential::ScramSha256(keys) => {
            perform_scram_auth(socket, username, keys, &payload).await
        }
        StoredCredential::Md5 { password_hash } => {
            // MD5 remains available only to historical non-Raft deployments.
            // The replicated identity representation cannot encode this form.
            perform_md5_auth(socket, username, password_hash).await
        }
    }
}

async fn read_startup_message<S>(socket: &mut S) -> Result<(String, Vec<u8>), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let mut len_bytes = [0_u8; 4];
        socket
            .read_exact(&mut len_bytes)
            .await
            .map_err(|_| ProtocolError::InvalidLength(0))?;
        let len = i32::from_be_bytes(len_bytes);
        let payload_len = parse_message_length(len)?;
        let mut payload = vec![0_u8; payload_len];
        socket
            .read_exact(&mut payload)
            .await
            .map_err(|_| ProtocolError::InvalidLength(len))?;
        let code = parse_startup_body(&payload)?;
        if code == SSL_REQUEST_CODE {
            socket
                .write_all(b"N")
                .await
                .map_err(|_| ProtocolError::InvalidLength(0))?;
            continue;
        }
        if code != STARTUP_PROTOCOL_V3 {
            return Err(ProtocolError::InvalidLength(code));
        }
        let username = parse_startup_username(&payload);
        return Ok((username, payload));
    }
}

async fn perform_scram_auth<S>(
    socket: &mut S,
    username: String,
    keys: crate::auth::ScramKeys,
    _startup_payload: &[u8],
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut scram = ScramServer::new(keys);
    socket
        .write_all(&build_auth_sasl_request(&["SCRAM-SHA-256"]))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    let client_first = read_frontend_message_payload(socket).await?;
    let (_, initial_data) =
        parse_sasl_initial_response(&client_first).map_err(|_| ProtocolError::InvalidLength(0))?;
    let client_first_str =
        String::from_utf8(initial_data).map_err(|_| ProtocolError::InvalidLength(0))?;
    let server_first = scram
        .process_client_first(&client_first_str)
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    socket
        .write_all(&build_auth_sasl_continue(server_first.as_bytes()))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let client_final_payload = read_frontend_message_payload(socket).await?;
    let client_final =
        String::from_utf8(client_final_payload).map_err(|_| ProtocolError::InvalidLength(0))?;
    let server_sig = scram
        .process_client_final(&client_final)
        .map_err(|_| ProtocolError::InvalidLength(0))?;

    let final_msg = format!("v={server_sig}");
    socket
        .write_all(&build_auth_sasl_final(final_msg.as_bytes()))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    socket
        .write_all(&build_auth_ok())
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    Ok(username)
}

async fn perform_md5_auth<S>(
    socket: &mut S,
    username: String,
    password_hash: String,
) -> Result<String, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let md5_state = Md5State::new(password_hash);
    socket
        .write_all(&build_auth_md5_request(&md5_state.salt))
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let pw_payload = read_frontend_message_payload(socket).await?;
    let response = String::from_utf8(
        pw_payload
            .strip_suffix(b"\0")
            .unwrap_or(&pw_payload)
            .to_vec(),
    )
    .map_err(|_| ProtocolError::InvalidLength(0))?;

    if !md5_state.verify(&response) {
        let err = build_error_response(
            &format!("password authentication failed for user \"{username}\""),
            "28P01",
        );
        let _ = socket.write_all(&err).await;
        return Err(ProtocolError::InvalidLength(0));
    }

    socket
        .write_all(&build_auth_ok())
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    Ok(username)
}

async fn read_frontend_message_payload<S>(socket: &mut S) -> Result<Vec<u8>, ProtocolError>
where
    S: AsyncRead + Unpin,
{
    let mut type_byte = [0u8; 1];
    socket
        .read_exact(&mut type_byte)
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let mut len_bytes = [0u8; 4];
    socket
        .read_exact(&mut len_bytes)
        .await
        .map_err(|_| ProtocolError::InvalidLength(0))?;
    let len = i32::from_be_bytes(len_bytes);
    let payload_len = parse_message_length(len)?;
    let mut payload = vec![0u8; payload_len];
    socket
        .read_exact(&mut payload)
        .await
        .map_err(|_| ProtocolError::InvalidLength(len))?;
    Ok(payload)
}

fn read_cstring(buf: &[u8], cursor: &mut usize) -> String {
    let start = *cursor;
    while *cursor < buf.len() && buf[*cursor] != 0 {
        *cursor += 1;
    }
    let s = String::from_utf8_lossy(&buf[start..*cursor]).to_string();
    if *cursor < buf.len() {
        *cursor += 1;
    }
    s
}

fn explain_text_to_batch(text: &str) -> crate::vectorized::RecordBatch {
    use crate::vectorized::{ColumnVector, RecordBatch, Utf8Column};
    let lines: Vec<Option<String>> = text.lines().map(|l| Some(l.to_string())).collect();
    let row_count = lines.len();
    RecordBatch {
        columns: vec![(
            "QUERY PLAN".to_string(),
            ColumnVector::Utf8(Utf8Column::from_owned_options(lines)),
        )],
        row_count,
    }
}
