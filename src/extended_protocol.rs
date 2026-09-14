// SPDX-License-Identifier: Apache-2.0
// Phase 8: bounded PostgreSQL extended-protocol Bind parsing and parameter
// materialization. This module is deliberately narrower than PostgreSQL: it
// supports deterministic scalar parameter types and text row results. Binary
// result-format requests are accepted only when the command produces no rows;
// unsupported formats/types otherwise fail closed.

use thiserror::Error;

pub const BOOLOID: i32 = 16;
pub const INT8OID: i32 = 20;
pub const INT2OID: i32 = 21;
pub const INT4OID: i32 = 23;
pub const TEXTOID: i32 = 25;
pub const FLOAT4OID: i32 = 700;
pub const FLOAT8OID: i32 = 701;
pub const BPCHAROID: i32 = 1042;
pub const VARCHAROID: i32 = 1043;
pub const DATEOID: i32 = 1082;

const MAX_BIND_PARAMETERS: usize = 1024;
const MAX_PARAMETER_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMessage {
    pub portal_name: String,
    pub statement_name: String,
    pub parameter_formats: Vec<i16>,
    pub parameters: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExtendedProtocolError {
    #[error("malformed extended-protocol message: {0}")]
    Protocol(String),
    #[error("unsupported extended-protocol feature: {0}")]
    Unsupported(String),
    #[error("invalid parameter value: {0}")]
    InvalidParameter(String),
}

impl ExtendedProtocolError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Protocol(_) => "08P01",
            Self::Unsupported(_) => "0A000",
            Self::InvalidParameter(_) => "22P02",
        }
    }
}

pub fn parse_parse_parameter_types(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<Vec<i32>, ExtendedProtocolError> {
    let count = read_i16(payload, cursor)?;
    if count < 0 {
        return Err(ExtendedProtocolError::Protocol(
            "negative Parse parameter count".into(),
        ));
    }
    let count = count as usize;
    if count > MAX_BIND_PARAMETERS {
        return Err(ExtendedProtocolError::Protocol(
            "too many Parse parameters".into(),
        ));
    }
    let mut types = Vec::with_capacity(count);
    for _ in 0..count {
        types.push(read_i32(payload, cursor)?);
    }
    if *cursor != payload.len() {
        return Err(ExtendedProtocolError::Protocol(
            "trailing bytes in Parse message".into(),
        ));
    }
    Ok(types)
}

pub fn parse_bind_message(payload: &[u8]) -> Result<BindMessage, ExtendedProtocolError> {
    let mut cursor = 0;
    let portal_name = read_cstring_strict(payload, &mut cursor)?;
    let statement_name = read_cstring_strict(payload, &mut cursor)?;

    let format_count = read_i16(payload, &mut cursor)?;
    if format_count < 0 {
        return Err(ExtendedProtocolError::Protocol(
            "negative parameter format count".into(),
        ));
    }
    let format_count = format_count as usize;
    if format_count > MAX_BIND_PARAMETERS {
        return Err(ExtendedProtocolError::Protocol(
            "too many parameter formats".into(),
        ));
    }
    let mut parameter_formats = Vec::with_capacity(format_count);
    for _ in 0..format_count {
        let format = read_i16(payload, &mut cursor)?;
        if !matches!(format, 0 | 1) {
            return Err(ExtendedProtocolError::Protocol(format!(
                "invalid parameter format code {format}"
            )));
        }
        parameter_formats.push(format);
    }

    let parameter_count = read_i16(payload, &mut cursor)?;
    if parameter_count < 0 {
        return Err(ExtendedProtocolError::Protocol(
            "negative Bind parameter count".into(),
        ));
    }
    let parameter_count = parameter_count as usize;
    if parameter_count > MAX_BIND_PARAMETERS {
        return Err(ExtendedProtocolError::Protocol(
            "too many Bind parameters".into(),
        ));
    }
    if parameter_formats.len() > 1 && parameter_formats.len() != parameter_count {
        return Err(ExtendedProtocolError::Protocol(
            "parameter format count must be 0, 1, or equal to parameter count".into(),
        ));
    }

    let mut parameters = Vec::with_capacity(parameter_count);
    for _ in 0..parameter_count {
        let len = read_i32(payload, &mut cursor)?;
        if len == -1 {
            parameters.push(None);
            continue;
        }
        if len < 0 {
            return Err(ExtendedProtocolError::Protocol(
                "invalid negative parameter length".into(),
            ));
        }
        let len = len as usize;
        if len > MAX_PARAMETER_BYTES || cursor.saturating_add(len) > payload.len() {
            return Err(ExtendedProtocolError::Protocol(
                "parameter length exceeds message bounds".into(),
            ));
        }
        parameters.push(Some(payload[cursor..cursor + len].to_vec()));
        cursor += len;
    }

    let result_count = read_i16(payload, &mut cursor)?;
    if result_count < 0 {
        return Err(ExtendedProtocolError::Protocol(
            "negative result format count".into(),
        ));
    }
    let result_count = result_count as usize;
    if result_count > MAX_BIND_PARAMETERS {
        return Err(ExtendedProtocolError::Protocol(
            "too many result formats".into(),
        ));
    }
    let mut result_formats = Vec::with_capacity(result_count);
    for _ in 0..result_count {
        let format = read_i16(payload, &mut cursor)?;
        if !matches!(format, 0 | 1) {
            return Err(ExtendedProtocolError::Protocol(format!(
                "invalid result format code {format}"
            )));
        }
        result_formats.push(format);
    }
    if cursor != payload.len() {
        return Err(ExtendedProtocolError::Protocol(
            "trailing bytes in Bind message".into(),
        ));
    }

    Ok(BindMessage {
        portal_name,
        statement_name,
        parameter_formats,
        parameters,
        result_formats,
    })
}

/// Materialize bound parameters into SQL for the existing parser/binder path.
/// Only deterministic scalar encodings are accepted. Untyped OID 0 is rejected
/// because PostgreSQL inference is context-sensitive and substituting a guessed
/// type would create a false compatibility contract.
pub fn materialize_bound_sql(
    sql: &str,
    parameter_types: &[i32],
    bind: &BindMessage,
) -> Result<String, ExtendedProtocolError> {
    if bind.parameters.len() != parameter_types.len() {
        return Err(ExtendedProtocolError::Protocol(format!(
            "Bind supplies {} parameters but Parse declared {}",
            bind.parameters.len(),
            parameter_types.len()
        )));
    }

    let rendered: Result<Vec<String>, ExtendedProtocolError> = bind
        .parameters
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let format = effective_format(&bind.parameter_formats, index)?;
            render_parameter(parameter_types[index], format, value.as_deref())
        })
        .collect();
    let materialized = substitute_placeholders(sql, &rendered?)?;
    validate_result_formats(&materialized, &bind.result_formats)?;
    Ok(materialized)
}

fn validate_result_formats(sql: &str, formats: &[i16]) -> Result<(), ExtendedProtocolError> {
    if formats.iter().all(|format| *format == 0) {
        return Ok(());
    }

    // PostgreSQL clients commonly request binary row results for every Bind,
    // including commands that never emit RowDescription/DataRow messages. In
    // that no-row case the requested result format is semantically irrelevant.
    // Keep the exception deliberately narrow: Phase 8 still rejects binary
    // formats for row-producing statements because binary result encoding is
    // not implemented.
    if matches!(
        crate::read_consistency::parse_read_consistency_setting(sql),
        crate::read_consistency::ReadConsistencySetting::Set(_)
    ) {
        return Ok(());
    }

    Err(ExtendedProtocolError::Unsupported(
        "binary result formats are not implemented for row-producing statements".into(),
    ))
}

pub fn build_parameter_description(parameter_types: &[i32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(2 + parameter_types.len() * 4);
    body.extend_from_slice(&(parameter_types.len() as i16).to_be_bytes());
    for oid in parameter_types {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    build_message(b't', &body)
}

fn effective_format(formats: &[i16], index: usize) -> Result<i16, ExtendedProtocolError> {
    match formats.len() {
        0 => Ok(0),
        1 => Ok(formats[0]),
        _ => formats
            .get(index)
            .copied()
            .ok_or_else(|| ExtendedProtocolError::Protocol("missing parameter format".into())),
    }
}

fn render_parameter(
    oid: i32,
    format: i16,
    value: Option<&[u8]>,
) -> Result<String, ExtendedProtocolError> {
    let Some(value) = value else {
        return Ok("NULL".to_string());
    };
    if oid == 0 {
        return Err(ExtendedProtocolError::Unsupported(
            "OID 0 parameter inference is not implemented; Parse must declare parameter types"
                .into(),
        ));
    }
    match format {
        0 => render_text_parameter(oid, value),
        1 => render_binary_parameter(oid, value),
        other => Err(ExtendedProtocolError::Protocol(format!(
            "invalid parameter format {other}"
        ))),
    }
}

fn render_text_parameter(oid: i32, bytes: &[u8]) -> Result<String, ExtendedProtocolError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        ExtendedProtocolError::InvalidParameter("parameter text is not UTF-8".into())
    })?;
    match oid {
        INT2OID | INT4OID | INT8OID => text
            .parse::<i64>()
            .map(|value| value.to_string())
            .map_err(|_| ExtendedProtocolError::InvalidParameter("invalid integer".into())),
        FLOAT4OID | FLOAT8OID => {
            let value = text.parse::<f64>().map_err(|_| {
                ExtendedProtocolError::InvalidParameter("invalid floating-point value".into())
            })?;
            if !value.is_finite() {
                return Err(ExtendedProtocolError::Unsupported(
                    "non-finite floating-point parameters are outside the compatibility profile"
                        .into(),
                ));
            }
            Ok(value.to_string())
        }
        BOOLOID => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => Ok("TRUE".into()),
            "f" | "false" | "0" => Ok("FALSE".into()),
            _ => Err(ExtendedProtocolError::InvalidParameter(
                "invalid boolean".into(),
            )),
        },
        TEXTOID | VARCHAROID | BPCHAROID => Ok(quote_sql_string(text)),
        DATEOID => {
            crate::binder::date_str_to_epoch_days(text)
                .ok_or_else(|| ExtendedProtocolError::InvalidParameter("invalid DATE".into()))?;
            Ok(format!("DATE {}", quote_sql_string(text)))
        }
        other => Err(ExtendedProtocolError::Unsupported(format!(
            "parameter type OID {other} is not implemented"
        ))),
    }
}

fn render_binary_parameter(oid: i32, bytes: &[u8]) -> Result<String, ExtendedProtocolError> {
    match oid {
        BOOLOID if bytes.len() == 1 => match bytes[0] {
            0 => Ok("FALSE".into()),
            1 => Ok("TRUE".into()),
            _ => Err(ExtendedProtocolError::InvalidParameter(
                "invalid binary boolean".into(),
            )),
        },
        INT2OID if bytes.len() == 2 => Ok(i16::from_be_bytes([bytes[0], bytes[1]]).to_string()),
        INT4OID if bytes.len() == 4 => {
            Ok(i32::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        INT8OID if bytes.len() == 8 => {
            Ok(i64::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        FLOAT4OID if bytes.len() == 4 => {
            let value = f32::from_bits(u32::from_be_bytes(bytes.try_into().unwrap()));
            if !value.is_finite() {
                return Err(ExtendedProtocolError::Unsupported(
                    "non-finite FLOAT4".into(),
                ));
            }
            Ok(value.to_string())
        }
        FLOAT8OID if bytes.len() == 8 => {
            let value = f64::from_bits(u64::from_be_bytes(bytes.try_into().unwrap()));
            if !value.is_finite() {
                return Err(ExtendedProtocolError::Unsupported(
                    "non-finite FLOAT8".into(),
                ));
            }
            Ok(value.to_string())
        }
        DATEOID if bytes.len() == 4 => {
            // PostgreSQL binary DATE is days since 2000-01-01. NeuralBase's
            // durable DATE representation is days since 1970-01-01.
            let pg_days = i32::from_be_bytes(bytes.try_into().unwrap());
            let epoch_days = pg_days
                .checked_add(10_957)
                .ok_or_else(|| ExtendedProtocolError::InvalidParameter("DATE overflow".into()))?;
            let iso = epoch_days_to_iso(epoch_days);
            Ok(format!("DATE {}", quote_sql_string(&iso)))
        }
        TEXTOID | VARCHAROID | BPCHAROID => {
            let text = std::str::from_utf8(bytes).map_err(|_| {
                ExtendedProtocolError::InvalidParameter("binary text is not UTF-8".into())
            })?;
            Ok(quote_sql_string(text))
        }
        _ => Err(ExtendedProtocolError::InvalidParameter(format!(
            "invalid binary length for parameter type OID {oid}"
        ))),
    }
}

fn substitute_placeholders(
    sql: &str,
    rendered: &[String],
) -> Result<String, ExtendedProtocolError> {
    let bytes = sql.as_bytes();
    let mut out =
        String::with_capacity(sql.len() + rendered.iter().map(String::len).sum::<usize>());
    let mut index = 0;
    let mut single_quote = false;
    let mut double_quote = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if single_quote {
            out.push(byte as char);
            if byte == b'\\' && index + 1 < bytes.len() {
                index += 1;
                out.push(bytes[index] as char);
            } else if byte == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 1;
                    out.push('\'');
                } else {
                    single_quote = false;
                }
            }
            index += 1;
            continue;
        }
        if double_quote {
            out.push(byte as char);
            if byte == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    index += 1;
                    out.push('"');
                } else {
                    double_quote = false;
                }
            }
            index += 1;
            continue;
        }
        if byte == b'\'' {
            single_quote = true;
            out.push('\'');
            index += 1;
            continue;
        }
        if byte == b'"' {
            double_quote = true;
            out.push('"');
            index += 1;
            continue;
        }
        if byte == b'$' && index + 1 < bytes.len() && bytes[index + 1].is_ascii_digit() {
            let mut end = index + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let number = std::str::from_utf8(&bytes[index + 1..end])
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| {
                    ExtendedProtocolError::Protocol("invalid parameter placeholder".into())
                })?;
            if number == 0 || number > rendered.len() {
                return Err(ExtendedProtocolError::Protocol(format!(
                    "parameter ${number} has no bound value"
                )));
            }
            out.push_str(&rendered[number - 1]);
            index = end;
            continue;
        }
        out.push(byte as char);
        index += 1;
    }

    for number in 1..=rendered.len() {
        let needle = format!("${number}");
        if contains_placeholder(&out, &needle) {
            return Err(ExtendedProtocolError::Protocol(format!(
                "parameter {needle} was not materialized"
            )));
        }
    }
    Ok(out)
}

fn contains_placeholder(sql: &str, needle: &str) -> bool {
    sql.split_whitespace().any(|part| part == needle)
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn read_cstring_strict(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<String, ExtendedProtocolError> {
    if *cursor >= payload.len() {
        return Err(ExtendedProtocolError::Protocol("missing cstring".into()));
    }
    let rest = &payload[*cursor..];
    let Some(position) = rest.iter().position(|byte| *byte == 0) else {
        return Err(ExtendedProtocolError::Protocol(
            "unterminated cstring".into(),
        ));
    };
    let value = std::str::from_utf8(&rest[..position])
        .map_err(|_| ExtendedProtocolError::Protocol("cstring is not UTF-8".into()))?
        .to_string();
    *cursor += position + 1;
    Ok(value)
}

fn read_i16(payload: &[u8], cursor: &mut usize) -> Result<i16, ExtendedProtocolError> {
    if cursor.saturating_add(2) > payload.len() {
        return Err(ExtendedProtocolError::Protocol("truncated i16".into()));
    }
    let value = i16::from_be_bytes([payload[*cursor], payload[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_i32(payload: &[u8], cursor: &mut usize) -> Result<i32, ExtendedProtocolError> {
    if cursor.saturating_add(4) > payload.len() {
        return Err(ExtendedProtocolError::Protocol("truncated i32".into()));
    }
    let value = i32::from_be_bytes([
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

fn build_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(body.len() + 5);
    message.push(tag);
    message.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    message.extend_from_slice(body);
    message
}

fn epoch_days_to_iso(days: i32) -> String {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(parameters: Vec<Option<Vec<u8>>>, formats: Vec<i16>) -> BindMessage {
        BindMessage {
            portal_name: "p".into(),
            statement_name: "s".into(),
            parameter_formats: formats,
            parameters,
            result_formats: vec![],
        }
    }

    #[test]
    fn text_integer_and_text_parameters_materialize_safely() {
        let message = bind(
            vec![Some(b"42".to_vec()), Some(b"O'Reilly".to_vec())],
            vec![],
        );
        let sql = materialize_bound_sql(
            "SELECT $1, $2, '$1 stays literal'",
            &[INT4OID, TEXTOID],
            &message,
        )
        .unwrap();
        assert_eq!(sql, "SELECT 42, 'O''Reilly', '$1 stays literal'");
    }

    #[test]
    fn null_parameter_materializes_as_null() {
        let message = bind(vec![None], vec![]);
        assert_eq!(
            materialize_bound_sql("SELECT $1", &[INT4OID], &message).unwrap(),
            "SELECT NULL"
        );
    }

    #[test]
    fn binary_int4_is_big_endian() {
        let message = bind(vec![Some(42_i32.to_be_bytes().to_vec())], vec![1]);
        assert_eq!(
            materialize_bound_sql("SELECT $1", &[INT4OID], &message).unwrap(),
            "SELECT 42"
        );
    }

    #[test]
    fn binary_text_is_utf8_and_sql_quoted() {
        let message = bind(vec![Some(b"O'Reilly".to_vec())], vec![1]);
        assert_eq!(
            materialize_bound_sql("SELECT $1", &[TEXTOID], &message).unwrap(),
            "SELECT 'O''Reilly'"
        );
    }

    #[test]
    fn unknown_oid_fails_closed() {
        let message = bind(vec![Some(b"42".to_vec())], vec![]);
        assert!(matches!(
            materialize_bound_sql("SELECT $1", &[0], &message),
            Err(ExtendedProtocolError::Unsupported(_))
        ));
    }

    #[test]
    fn binary_results_fail_closed_for_row_producing_query() {
        let mut message = bind(vec![], vec![]);
        message.result_formats = vec![1];
        assert!(matches!(
            materialize_bound_sql("SELECT 1", &[], &message),
            Err(ExtendedProtocolError::Unsupported(_))
        ));
    }

    #[test]
    fn binary_result_preference_is_irrelevant_for_no_row_read_consistency_set() {
        let mut message = bind(vec![Some(b"local".to_vec())], vec![1]);
        message.result_formats = vec![1];
        assert_eq!(
            materialize_bound_sql("SET neuralbase_read_consistency = $1", &[TEXTOID], &message,)
                .unwrap(),
            "SET neuralbase_read_consistency = 'local'"
        );
    }
}
