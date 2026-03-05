use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("message too large: {0}")]
    MessageTooLarge(i32),
    #[error("invalid message length: {0}")]
    InvalidLength(i32),
}

pub const SSL_REQUEST_CODE: i32 = 80877103;
pub const STARTUP_PROTOCOL_V3: i32 = 196608;

pub fn build_auth_ok() -> Vec<u8> {
    let mut body = Vec::with_capacity(4);
    body.extend_from_slice(&0_i32.to_be_bytes());
    build_typed_message(b'R', &body)
}

/// AuthenticationMD5Password — PostgreSQL auth type 5.
/// `salt` is the 4-byte random challenge sent to the client.
pub fn build_auth_md5_request(salt: &[u8; 4]) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&5_i32.to_be_bytes()); // auth type = MD5
    body.extend_from_slice(salt);
    build_typed_message(b'R', &body)
}

/// AuthenticationSASL — PostgreSQL auth type 10.
/// `mechanisms` is a list of SASL mechanism names (e.g. ["SCRAM-SHA-256"]).
pub fn build_auth_sasl_request(mechanisms: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&10_i32.to_be_bytes()); // auth type = SASL
    for m in mechanisms {
        body.extend_from_slice(m.as_bytes());
        body.push(0);
    }
    body.push(0); // list terminator
    build_typed_message(b'R', &body)
}

/// AuthenticationSASLContinue — PostgreSQL auth type 11.
pub fn build_auth_sasl_continue(data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + data.len());
    body.extend_from_slice(&11_i32.to_be_bytes());
    body.extend_from_slice(data);
    build_typed_message(b'R', &body)
}

/// AuthenticationSASLFinal — PostgreSQL auth type 12.
pub fn build_auth_sasl_final(data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + data.len());
    body.extend_from_slice(&12_i32.to_be_bytes());
    body.extend_from_slice(data);
    build_typed_message(b'R', &body)
}

pub fn build_parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    build_typed_message(b'S', &body)
}

pub fn build_backend_key_data(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret.to_be_bytes());
    build_typed_message(b'K', &body)
}

pub fn build_ready_for_query() -> Vec<u8> {
    build_typed_message(b'Z', b"I")
}

pub fn build_row_description(columns: &[(&str, i32, i16)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for (name, type_oid, type_size) in columns {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        body.extend_from_slice(&type_oid.to_be_bytes());
        body.extend_from_slice(&type_size.to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
    }
    build_typed_message(b'T', &body)
}

pub fn build_data_row(values: &[Option<String>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());

    for value in values {
        match value {
            Some(text) => {
                let bytes = text.as_bytes();
                body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                body.extend_from_slice(bytes);
            }
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }

    build_typed_message(b'D', &body)
}

pub fn build_command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    build_typed_message(b'C', &body)
}

pub fn build_error_response(message: &str, code: &str) -> Vec<u8> {
    let mut body = Vec::new();

    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);

    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);

    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);

    body.push(0);

    build_typed_message(b'E', &body)
}

pub fn parse_startup_body(buf: &[u8]) -> Result<i32, ProtocolError> {
    if buf.len() < 4 {
        return Err(ProtocolError::InvalidLength(buf.len() as i32));
    }

    Ok(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

pub fn parse_message_length(len: i32) -> Result<usize, ProtocolError> {
    if len < 4 {
        return Err(ProtocolError::InvalidLength(len));
    }
    if len > 16 * 1024 * 1024 {
        return Err(ProtocolError::MessageTooLarge(len));
    }
    Ok((len - 4) as usize)
}

/// Extract the value of the `user` parameter from a PostgreSQL startup message body.
/// The body is the full payload (4-byte protocol version + null-terminated key=value pairs).
/// Returns "postgres" as a safe default if the field is absent or malformed.
pub fn parse_startup_username(startup_body: &[u8]) -> String {
    if startup_body.len() <= 4 {
        return "postgres".to_string();
    }
    let params = &startup_body[4..];
    let mut iter = params.split(|&b| b == 0);
    loop {
        let key = match iter.next() {
            Some(k) if !k.is_empty() => k,
            _ => break,
        };
        let val = match iter.next() {
            Some(v) => v,
            None => break,
        };
        if key == b"user" {
            if let Ok(username) = std::str::from_utf8(val) {
                return username.to_string();
            }
        }
    }
    "postgres".to_string()
}

/// Parse a SASLInitialResponse ('p') payload.
/// Format: mechanism\0 + data_length(i32) + data
/// Returns (mechanism_name, initial_data).
pub fn parse_sasl_initial_response(
    payload: &[u8],
) -> Result<(String, Vec<u8>), ProtocolError> {
    let null_pos = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or(ProtocolError::InvalidLength(payload.len() as i32))?;
    let mechanism = std::str::from_utf8(&payload[..null_pos])
        .map_err(|_| ProtocolError::InvalidLength(null_pos as i32))?
        .to_string();
    let rest = &payload[null_pos + 1..];
    if rest.len() < 4 {
        return Ok((mechanism, vec![]));
    }
    let data_len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    if data_len < 0 {
        return Ok((mechanism, vec![]));
    }
    let data_len = data_len as usize;
    if rest.len() < 4 + data_len {
        return Err(ProtocolError::InvalidLength(data_len as i32));
    }
    Ok((mechanism, rest[4..4 + data_len].to_vec()))
}

pub fn build_typed_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}
