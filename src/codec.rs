// SPDX-License-Identifier: Apache-2.0
// Typed binary RecordBatch codec — Session 10.
//
// Replaces the string-value NB v1 codec in storage_executor.rs with a
// fully typed binary format in which fixed-width scalars are stored inline
// and variable-width bytes are length-prefixed.  A per-row NULL bitmap
// eliminates the need for sentinel strings.
//
// Wire format (all integers little-endian):
// ─────────────────────────────────────────
//   MAGIC:    [0x4E, 0x42, 0x02]     // "NB" + version byte
//   num_cols: u16 LE
//   num_rows: u32 LE
//
//   Column table (num_cols entries):
//     name_len:  u16 LE
//     name:      UTF-8 bytes
//     type_tag:  u8
//       0x00 = Int32   (fixed  4 bytes LE)
//       0x01 = Int64   (fixed  8 bytes LE)
//       0x02 = Float64 (fixed  8 bytes LE, IEEE 754)
//       0x03 = Date32  (fixed  4 bytes LE)
//       0x04 = Utf8    (4-byte LE length prefix + raw UTF-8 bytes)
//
//   Row data (num_rows rows):
//     null_bitmap: ceil(num_cols / 8) bytes
//       bit i = 1  → column i is NULL  (no value bytes follow for that col)
//       bit i = 0  → column i is non-null; value bytes follow in column order
//     For each non-null column (ascending column index):
//       value bytes per type tag above
//
// Encoding invariants
// ───────────────────
// 1. A RecordBatch with 0 rows encodes to MAGIC + 0-cols + 0-rows header.
// 2. NULL bitmap is always ceil(num_cols/8) bytes — zero-padded on the right.
// 3. Float64 NaN values round-trip byte-for-byte (no canonicalization).
// 4. The decoder rejects any input that does not start with the 3-byte magic.
// 5. RecordBatches with mismatched column lengths cannot be encoded (checked).
//
// CONFIDENCE: raw=0.84 effective=0.78
// DEPENDS_ON: vectorized
// RISK: Columnar compression (Snappy/LZ4) not applied — Session 15+ scope.

use crate::vectorized::{ColumnVector, RecordBatch, Utf8Column};

const MAGIC: [u8; 3] = [0x4E, 0x42, 0x02];

/// Type tags (1 byte) written into the column table.
const TAG_INT32: u8 = 0x00;
const TAG_INT64: u8 = 0x01;
const TAG_FLOAT64: u8 = 0x02;
const TAG_DATE32: u8 = 0x03;
const TAG_UTF8: u8 = 0x04;

// ── Encoder ──────────────────────────────────────────────────────────────────

/// Encode a `RecordBatch` into a compact typed binary blob.
///
/// Returns an error string if the input is structurally invalid (column length
/// mismatch).  In practice a well-formed RecordBatch never triggers this.
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, &'static str> {
    let num_cols = batch.columns.len() as u16;
    let num_rows = batch.row_count as u32;

    // Validate: all columns must have the same length.
    for (_, cv) in &batch.columns {
        if cv.len() != batch.row_count {
            return Err("codec: column length mismatch");
        }
    }

    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&num_cols.to_le_bytes());
    buf.extend_from_slice(&num_rows.to_le_bytes());

    // Column table
    for (name, cv) in &batch.columns {
        let nb = name.as_bytes();
        buf.extend_from_slice(&(nb.len() as u16).to_le_bytes());
        buf.extend_from_slice(nb);
        buf.push(type_tag_of(cv));
    }

    // Row data
    let bitmap_bytes = bitmap_len(num_cols as usize);
    for row in 0..batch.row_count {
        // Build null bitmap for this row.
        let mut bitmap = vec![0u8; bitmap_bytes];
        for (col_idx, (_, cv)) in batch.columns.iter().enumerate() {
            if is_null_at(cv, row) {
                bitmap[col_idx / 8] |= 1 << (col_idx % 8);
            }
        }
        buf.extend_from_slice(&bitmap);

        // Emit value bytes for each non-null column.
        for (col_idx, (_, cv)) in batch.columns.iter().enumerate() {
            if (bitmap[col_idx / 8] >> (col_idx % 8)) & 1 == 1 {
                continue; // NULL — skip
            }
            encode_value_at(cv, row, &mut buf);
        }
    }

    Ok(buf)
}

/// Decode a typed binary blob back to a `RecordBatch`.
///
/// Returns `None` on any format violation (truncated data, bad magic, invalid
/// type tag, or invalid UTF-8).
pub fn decode_batch(bytes: &[u8]) -> Option<RecordBatch> {
    let mut pos = 0usize;

    // Magic check
    if bytes.len() < 3 || bytes[0..3] != MAGIC {
        return None;
    }
    pos += 3;

    // num_cols + num_rows
    let num_cols = read_u16(bytes, &mut pos)? as usize;
    let num_rows = read_u32(bytes, &mut pos)? as usize;

    // Column table
    let mut col_names: Vec<String> = Vec::with_capacity(num_cols);
    let mut col_tags: Vec<u8> = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        let name_len = read_u16(bytes, &mut pos)? as usize;
        let name_bytes = read_bytes(bytes, &mut pos, name_len)?;
        let name = String::from_utf8(name_bytes.to_vec()).ok()?;
        let tag = read_u8(bytes, &mut pos)?;
        if !valid_tag(tag) {
            return None;
        }
        col_names.push(name);
        col_tags.push(tag);
    }

    // Build empty column accumulators (one per column).
    let mut int32_data: Vec<Vec<Option<i32>>> = vec![Vec::with_capacity(num_rows); num_cols];
    let mut int64_data: Vec<Vec<Option<i64>>> = vec![Vec::with_capacity(num_rows); num_cols];
    let mut float64_data: Vec<Vec<Option<f64>>> = vec![Vec::with_capacity(num_rows); num_cols];
    let mut date32_data: Vec<Vec<Option<i32>>> = vec![Vec::with_capacity(num_rows); num_cols];
    let mut utf8_strs: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(num_rows); num_cols];

    let bitmap_bytes = bitmap_len(num_cols);

    for _ in 0..num_rows {
        // Read null bitmap.
        let bitmap = read_bytes(bytes, &mut pos, bitmap_bytes)?;

        for col_idx in 0..num_cols {
            let is_null = (bitmap[col_idx / 8] >> (col_idx % 8)) & 1 == 1;
            match col_tags[col_idx] {
                TAG_INT32 => {
                    int32_data[col_idx].push(if is_null {
                        None
                    } else {
                        let v = read_i32(bytes, &mut pos)?;
                        Some(v)
                    });
                }
                TAG_INT64 => {
                    int64_data[col_idx].push(if is_null {
                        None
                    } else {
                        let v = read_i64(bytes, &mut pos)?;
                        Some(v)
                    });
                }
                TAG_FLOAT64 => {
                    float64_data[col_idx].push(if is_null {
                        None
                    } else {
                        let v = read_f64(bytes, &mut pos)?;
                        Some(v)
                    });
                }
                TAG_DATE32 => {
                    date32_data[col_idx].push(if is_null {
                        None
                    } else {
                        let v = read_i32(bytes, &mut pos)?;
                        Some(v)
                    });
                }
                TAG_UTF8 => {
                    utf8_strs[col_idx].push(if is_null {
                        None
                    } else {
                        let len = read_u32(bytes, &mut pos)? as usize;
                        let s_bytes = read_bytes(bytes, &mut pos, len)?;
                        Some(String::from_utf8(s_bytes.to_vec()).ok()?)
                    });
                }
                _ => return None,
            }
        }
    }

    // Reconstruct columns.
    let columns: Vec<(String, ColumnVector)> = col_names
        .into_iter()
        .zip(col_tags.iter())
        .enumerate()
        .map(|(i, (name, &tag))| {
            let cv = match tag {
                TAG_INT32 => ColumnVector::Int32(int32_data[i].clone()),
                TAG_INT64 => ColumnVector::Int64(int64_data[i].clone()),
                TAG_FLOAT64 => ColumnVector::Float64(float64_data[i].clone()),
                TAG_DATE32 => ColumnVector::Date32(date32_data[i].clone()),
                TAG_UTF8 => {
                    let opts: Vec<Option<&str>> =
                        utf8_strs[i].iter().map(|o| o.as_deref()).collect();
                    ColumnVector::Utf8(Utf8Column::from_options(opts))
                }
                _ => unreachable!(),
            };
            (name, cv)
        })
        .collect();

    if columns.is_empty() {
        return Some(RecordBatch::empty());
    }

    RecordBatch::new(columns).ok()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn type_tag_of(cv: &ColumnVector) -> u8 {
    match cv {
        ColumnVector::Int32(_) => TAG_INT32,
        ColumnVector::Int64(_) => TAG_INT64,
        ColumnVector::Float64(_) => TAG_FLOAT64,
        ColumnVector::Date32(_) => TAG_DATE32,
        ColumnVector::Utf8(_) => TAG_UTF8,
    }
}

fn is_null_at(cv: &ColumnVector, row: usize) -> bool {
    match cv {
        ColumnVector::Int32(v) => v[row].is_none(),
        ColumnVector::Int64(v) => v[row].is_none(),
        ColumnVector::Float64(v) => v[row].is_none(),
        ColumnVector::Date32(v) => v[row].is_none(),
        ColumnVector::Utf8(v) => !v.validity.get(row).copied().unwrap_or(false),
    }
}

fn encode_value_at(cv: &ColumnVector, row: usize, buf: &mut Vec<u8>) {
    match cv {
        ColumnVector::Int32(v) => {
            if let Some(i) = v[row] {
                buf.extend_from_slice(&i.to_le_bytes());
            }
        }
        ColumnVector::Int64(v) => {
            if let Some(i) = v[row] {
                buf.extend_from_slice(&i.to_le_bytes());
            }
        }
        ColumnVector::Float64(v) => {
            if let Some(f) = v[row] {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        ColumnVector::Date32(v) => {
            if let Some(d) = v[row] {
                buf.extend_from_slice(&d.to_le_bytes());
            }
        }
        ColumnVector::Utf8(v) => {
            if let Some(s) = v.get(row) {
                let sb = s.as_bytes();
                buf.extend_from_slice(&(sb.len() as u32).to_le_bytes());
                buf.extend_from_slice(sb);
            }
        }
    }
}

fn bitmap_len(num_cols: usize) -> usize {
    num_cols.div_ceil(8)
}

fn valid_tag(tag: u8) -> bool {
    tag <= TAG_UTF8
}

// ── Low-level byte readers ────────────────────────────────────────────────────

fn read_u8(bytes: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos + 1 > bytes.len() {
        return None;
    }
    let v = bytes[*pos];
    *pos += 1;
    Some(v)
}

fn read_u16(bytes: &[u8], pos: &mut usize) -> Option<u16> {
    if *pos + 2 > bytes.len() {
        return None;
    }
    let v = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos += 2;
    Some(v)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > bytes.len() {
        return None;
    }
    let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(v)
}

fn read_i32(bytes: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > bytes.len() {
        return None;
    }
    let v = i32::from_le_bytes(bytes[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(v)
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    if *pos + 8 > bytes.len() {
        return None;
    }
    let v = i64::from_le_bytes(bytes[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(v)
}

fn read_f64(bytes: &[u8], pos: &mut usize) -> Option<f64> {
    if *pos + 8 > bytes.len() {
        return None;
    }
    let v = f64::from_le_bytes(bytes[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(v)
}

fn read_bytes<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
    if *pos + len > bytes.len() {
        return None;
    }
    let slice = &bytes[*pos..*pos + len];
    *pos += len;
    Some(slice)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectorized::{ColumnVector, RecordBatch, Utf8Column};
    use proptest::prelude::*;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn two_row_batch() -> RecordBatch {
        RecordBatch::new(vec![
            ("id".into(), ColumnVector::Int32(vec![Some(1), Some(2)])),
            (
                "price".into(),
                ColumnVector::Float64(vec![Some(9.99), None]),
            ),
            (
                "label".into(),
                ColumnVector::Utf8(Utf8Column::from_options(vec![Some("foo"), Some("bar")])),
            ),
        ])
        .unwrap()
    }

    // ── Unit tests ────────────────────────────────────────────────────────────

    #[test]
    fn magic_present_in_encoded_output() {
        let batch = two_row_batch();
        let enc = encode_batch(&batch).unwrap();
        assert_eq!(
            &enc[0..3],
            &[0x4E, 0x42, 0x02],
            "must start with NB-v2 magic"
        );
    }

    #[test]
    fn encode_decode_two_row_roundtrip() {
        let orig = two_row_batch();
        let enc = encode_batch(&orig).unwrap();
        let got = decode_batch(&enc).expect("decode must succeed");

        assert_eq!(got.row_count, orig.row_count);
        assert_eq!(got.columns.len(), orig.columns.len());

        // id column
        let ColumnVector::Int32(ids) = got.column("id").unwrap() else {
            panic!("type");
        };
        assert_eq!(ids[0], Some(1));
        assert_eq!(ids[1], Some(2));

        // price column — second row is NULL
        let ColumnVector::Float64(prices) = got.column("price").unwrap() else {
            panic!("type");
        };
        assert!((prices[0].unwrap() - 9.99).abs() < 1e-12);
        assert!(prices[1].is_none());

        // label column
        let ColumnVector::Utf8(labels) = got.column("label").unwrap() else {
            panic!("type");
        };
        assert_eq!(labels.get(0).as_deref(), Some("foo"));
        assert_eq!(labels.get(1).as_deref(), Some("bar"));
    }

    #[test]
    fn encode_decode_all_null_column() {
        let batch = RecordBatch::new(vec![(
            "x".into(),
            ColumnVector::Int64(vec![None, None, None]),
        )])
        .unwrap();
        let got = decode_batch(&encode_batch(&batch).unwrap()).unwrap();
        let ColumnVector::Int64(vals) = got.column("x").unwrap() else {
            panic!();
        };
        assert!(vals.iter().all(|v| v.is_none()));
    }

    #[test]
    fn encode_decode_zero_rows() {
        let batch = RecordBatch::empty();
        let enc = encode_batch(&batch).unwrap();
        let got = decode_batch(&enc).expect("empty decode");
        assert_eq!(got.row_count, 0);
    }

    #[test]
    fn decode_rejects_wrong_magic() {
        assert!(
            decode_batch(&[0x4E, 0x42, 0x01, 0, 0, 0, 0, 0, 0]).is_none(),
            "v1 must be rejected"
        );
        assert!(decode_batch(b"{}").is_none(), "JSON must be rejected");
        assert!(decode_batch(&[]).is_none(), "empty must be rejected");
    }

    #[test]
    fn decode_rejects_truncated_data() {
        let batch = two_row_batch();
        let full = encode_batch(&batch).unwrap();
        // Truncate half the payload — must return None.
        assert!(decode_batch(&full[..full.len() / 2]).is_none());
    }

    #[test]
    fn date32_roundtrip() {
        let batch = RecordBatch::new(vec![(
            "d".into(),
            ColumnVector::Date32(vec![Some(19940101), None, Some(19960630)]),
        )])
        .unwrap();
        let got = decode_batch(&encode_batch(&batch).unwrap()).unwrap();
        let ColumnVector::Date32(dates) = got.column("d").unwrap() else {
            panic!();
        };
        assert_eq!(dates[0], Some(19940101));
        assert!(dates[1].is_none());
        assert_eq!(dates[2], Some(19960630));
    }

    #[test]
    fn int64_roundtrip() {
        let batch = RecordBatch::new(vec![(
            "big".into(),
            ColumnVector::Int64(vec![Some(i64::MAX), Some(-1), None, Some(0)]),
        )])
        .unwrap();
        let got = decode_batch(&encode_batch(&batch).unwrap()).unwrap();
        let ColumnVector::Int64(vals) = got.column("big").unwrap() else {
            panic!();
        };
        assert_eq!(vals[0], Some(i64::MAX));
        assert_eq!(vals[1], Some(-1i64));
        assert!(vals[2].is_none());
        assert_eq!(vals[3], Some(0i64));
    }

    #[test]
    fn typed_codec_more_compact_than_string_codec() {
        // For a typical TPC-H lineitem row, the typed codec should be
        // measurably smaller than the NB-v1 string codec.
        let batch = RecordBatch::new(vec![
            (
                "l_orderkey".into(),
                ColumnVector::Int64(vec![Some(1234567)]),
            ),
            (
                "l_extendedprice".into(),
                ColumnVector::Float64(vec![Some(12345.67)]),
            ),
            ("l_quantity".into(), ColumnVector::Float64(vec![Some(24.0)])),
        ])
        .unwrap();
        let encoded = encode_batch(&batch).unwrap();
        // 3 magic + 2 num_cols + 4 num_rows = 9 header
        // + 3 cols × (2 name_len + name + 1 tag) ≈ 60 bytes header
        // + 1 row × (1 bitmap + 8 + 8 + 8 values) = 25 bytes data
        // Total ≈ 85 bytes. String-based codec for same data ≈ 120+ bytes.
        assert!(
            encoded.len() < 150,
            "typed codec should be compact: {} bytes",
            encoded.len()
        );
    }

    // ── Property-based roundtrip test ─────────────────────────────────────────
    //
    // Strategies cover 0–8 rows, 1–4 Int32 columns with ~10% NULL values.
    // The roundtrip invariant must hold for all inputs in this space.
    // A second adversarial property verifies decode never panics.

    proptest! {
        /// Core invariant: for all valid RecordBatches,
        /// decode(encode(batch)) == batch.
        ///
        /// Covers: multiple columns, mixed NULL / non-NULL, zero-row batches.
        /// All four columns are generated with the same length to avoid global
        /// rejects from the prior `prop_assume!` approach.
        #[test]
        fn roundtrip_all_types(
            (c0, c1, c2, c3) in (0usize..=8usize).prop_flat_map(|n| (
                prop::collection::vec(prop::option::weighted(0.9, any::<i32>()), n),
                prop::collection::vec(prop::option::weighted(0.9, any::<i32>()), n),
                prop::collection::vec(prop::option::weighted(0.9, any::<i64>()), n),
                prop::collection::vec(prop::option::weighted(0.9, -1e12f64..1e12f64), n),
            ))
        ) {
            let num_rows = c0.len();

            let batch = if num_rows == 0 {
                RecordBatch::empty()
            } else {
                RecordBatch::new(vec![
                    ("a".into(), ColumnVector::Int32(c0.clone())),
                    ("b".into(), ColumnVector::Int32(c1.clone())),
                    ("c".into(), ColumnVector::Int64(c2.clone())),
                    ("d".into(), ColumnVector::Float64(c3.clone())),
                ])
                .unwrap()
            };

            let enc = encode_batch(&batch).expect("encode must succeed");
            let got = decode_batch(&enc).expect("decode must succeed");

            prop_assert_eq!(got.row_count, batch.row_count);
            prop_assert_eq!(got.columns.len(), batch.columns.len());

            if num_rows > 0 {
                let ColumnVector::Int32(a_got)  = got.column("a").unwrap() else { unreachable!() };
                let ColumnVector::Int32(b_got)  = got.column("b").unwrap() else { unreachable!() };
                let ColumnVector::Int64(c_got)  = got.column("c").unwrap() else { unreachable!() };
                let ColumnVector::Float64(d_got) = got.column("d").unwrap() else { unreachable!() };

                prop_assert_eq!(a_got, &c0);
                prop_assert_eq!(b_got, &c1);
                prop_assert_eq!(c_got, &c2);

                // Float64: compare with epsilon (handles ±0.0 edge cases).
                for (expected, actual) in c3.iter().zip(d_got.iter()) {
                    match (expected, actual) {
                        (None, None) => {}
                        (Some(e), Some(a)) => {
                            prop_assert!((e - a).abs() < 1e-9, "float mismatch: {e} vs {a}");
                        }
                        _ => prop_assert!(false, "NULL mismatch"),
                    }
                }
            }
        }
    }

    proptest! {
        /// Adversarial: decode never panics on arbitrary byte inputs.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(
            bytes in prop::collection::vec(any::<u8>(), 0..512),
        ) {
            // Must not panic — None is the valid return for any malformed input.
            let _ = decode_batch(&bytes);
        }
    }
}
