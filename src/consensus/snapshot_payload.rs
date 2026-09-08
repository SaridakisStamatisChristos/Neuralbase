// SPDX-License-Identifier: Apache-2.0
//! Phase-3 Raft snapshot envelope.
//!
//! The existing state-machine snapshot bytes remain owned by the SQL state
//! machine. Raft wraps those bytes with the committed membership configuration
//! before staging/publishing/sending a snapshot. This keeps membership and SQL
//! bootstrap under the same crash-safe Phase-2 lifecycle without introducing a
//! second bootstrap path.

use sha2::{Digest, Sha256};

use crate::consensus::membership::ClusterMembership;

const MAGIC: &[u8; 4] = b"NBR3";
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 4 + 1 + 3 + 4 + 8 + 32;

pub fn encode_snapshot_payload(
    membership: &ClusterMembership,
    sql_snapshot: &[u8],
) -> Result<Vec<u8>, String> {
    membership.validate()?;
    let membership_bytes = serde_json::to_vec(membership)
        .map_err(|error| format!("serialize snapshot membership: {error}"))?;
    let membership_len = u32::try_from(membership_bytes.len())
        .map_err(|_| "snapshot membership metadata is too large".to_string())?;
    let sql_len =
        u64::try_from(sql_snapshot.len()).map_err(|_| "SQL snapshot is too large".to_string())?;

    let mut digest = Sha256::new();
    digest.update(&membership_bytes);
    digest.update(sql_snapshot);
    let checksum = digest.finalize();

    let mut out = Vec::with_capacity(HEADER_BYTES + membership_bytes.len() + sql_snapshot.len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&[0u8; 3]);
    out.extend_from_slice(&membership_len.to_be_bytes());
    out.extend_from_slice(&sql_len.to_be_bytes());
    out.extend_from_slice(&checksum);
    out.extend_from_slice(&membership_bytes);
    out.extend_from_slice(sql_snapshot);
    Ok(out)
}

/// Decode a Phase-3 snapshot payload. `Ok(None)` means the bytes are a legacy
/// Phase-2 raw SQL snapshot and should be handled using the already-durable
/// bootstrap membership migration rules.
pub fn decode_snapshot_payload(bytes: &[u8]) -> Result<Option<(ClusterMembership, &[u8])>, String> {
    if bytes.len() < 4 || &bytes[..4] != MAGIC {
        return Ok(None);
    }
    if bytes.len() < HEADER_BYTES {
        return Err(format!(
            "truncated Phase-3 Raft snapshot envelope: {} bytes",
            bytes.len()
        ));
    }
    if bytes[4] != VERSION {
        return Err(format!(
            "unsupported Phase-3 Raft snapshot envelope version {}",
            bytes[4]
        ));
    }
    if bytes[5..8] != [0u8; 3] {
        return Err("non-zero reserved Phase-3 snapshot header bytes".to_string());
    }
    let membership_len = u32::from_be_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| "decode membership length".to_string())?,
    ) as usize;
    let sql_len = u64::from_be_bytes(
        bytes[12..20]
            .try_into()
            .map_err(|_| "decode SQL snapshot length".to_string())?,
    );
    let sql_len = usize::try_from(sql_len)
        .map_err(|_| "SQL snapshot length does not fit this platform".to_string())?;
    let expected = HEADER_BYTES
        .checked_add(membership_len)
        .and_then(|value| value.checked_add(sql_len))
        .ok_or_else(|| "Phase-3 snapshot envelope length overflow".to_string())?;
    if bytes.len() != expected {
        return Err(format!(
            "Phase-3 snapshot envelope length mismatch: header declares {expected}, got {}",
            bytes.len()
        ));
    }

    let membership_start = HEADER_BYTES;
    let membership_end = membership_start + membership_len;
    let membership_bytes = &bytes[membership_start..membership_end];
    let sql_snapshot = &bytes[membership_end..];

    let mut digest = Sha256::new();
    digest.update(membership_bytes);
    digest.update(sql_snapshot);
    let actual = digest.finalize();
    if actual.as_slice() != &bytes[20..52] {
        return Err("Phase-3 Raft snapshot envelope checksum mismatch".to_string());
    }

    let membership: ClusterMembership = serde_json::from_slice(membership_bytes)
        .map_err(|error| format!("decode snapshot membership: {error}"))?;
    membership.validate()?;
    Ok(Some((membership, sql_snapshot)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip_is_exact() {
        let membership =
            ClusterMembership::bootstrap("n1".to_string(), ["n2".to_string(), "n3".to_string()]);
        let sql = b"opaque-sql-snapshot\0\xff";
        let encoded = encode_snapshot_payload(&membership, sql).unwrap();
        let (decoded_membership, decoded_sql) = decode_snapshot_payload(&encoded)
            .unwrap()
            .expect("Phase-3 envelope");
        assert_eq!(decoded_membership, membership);
        assert_eq!(decoded_sql, sql);
    }

    #[test]
    fn corruption_is_rejected() {
        let membership = ClusterMembership::bootstrap("n1".to_string(), Vec::<String>::new());
        let mut encoded = encode_snapshot_payload(&membership, b"sql").unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x80;
        assert!(decode_snapshot_payload(&encoded)
            .unwrap_err()
            .contains("checksum mismatch"));
    }

    #[test]
    fn legacy_bytes_are_not_misclassified() {
        assert!(decode_snapshot_payload(b"legacy-sql-snapshot")
            .unwrap()
            .is_none());
    }
}
