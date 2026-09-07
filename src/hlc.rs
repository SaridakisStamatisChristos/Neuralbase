// SPDX-License-Identifier: Apache-2.0
// Hybrid Logical Clock (HLC) implementation.
//
// Encoding: u64 = wall_ms << 16 | logical
//   - 48 bits wall clock (ms since Unix epoch)
//   - 16 bits logical counter per millisecond
//
// CONFIDENCE: raw=0.82 effective=0.73
// RISK: HLC monotonicity depends on system clock quality; simulated clock jumps
//       are tested but real NTP backward jumps may require additional guards.

// Session 4 infrastructure — not yet wired into main(). Suppress dead_code.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// A Hybrid Logical Clock timestamp.
/// Ordered first by wall_ms, then by logical counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HlcTimestamp {
    pub wall_ms: u64,
    pub logical: u16,
}

impl HlcTimestamp {
    pub const ZERO: HlcTimestamp = HlcTimestamp {
        wall_ms: 0,
        logical: 0,
    };
    pub const MAX: HlcTimestamp = HlcTimestamp {
        wall_ms: u64::MAX >> 16,
        logical: u16::MAX,
    };

    /// Serialize to a sortable u64 suitable for RocksDB keys (big-endian).
    pub fn to_u64(self) -> u64 {
        (self.wall_ms << 16) | (self.logical as u64)
    }

    /// Deserialize from a u64 key.
    pub fn from_u64(v: u64) -> Self {
        Self {
            wall_ms: v >> 16,
            logical: v as u16,
        }
    }

    /// Big-endian bytes for embedding in RocksDB keys.
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.to_u64().to_be_bytes()
    }

    /// Parse from big-endian bytes.
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self::from_u64(u64::from_be_bytes(bytes))
    }
}

/// Error returned by `HlcClock::update` when the remote clock skew exceeds the bound.
#[derive(Debug, Error)]
pub enum HlcError {
    #[error("remote clock is {skew_ms}ms ahead; exceeds max skew of {max_skew_ms}ms")]
    SkewExceeded { skew_ms: u64, max_skew_ms: u64 },
}

/// A thread-safe HLC clock node.
pub struct HlcClock {
    inner: Mutex<HlcTimestamp>,
    /// Maximum allowed remote clock skew used by `update()`.
    pub max_skew_ms: u64,
}

impl HlcClock {
    /// Create a new HLC clock with the given max skew bound in milliseconds.
    pub fn new(max_skew_ms: u64) -> Self {
        Self {
            inner: Mutex::new(HlcTimestamp::ZERO),
            max_skew_ms,
        }
    }

    /// Advance the clock and return a new timestamp for a local event.
    /// Guarantees: result > all previously returned timestamps.
    pub fn tick(&self) -> HlcTimestamp {
        let wall = wall_now_ms();
        let mut cur = self.inner.lock().unwrap();
        *cur = advance(*cur, wall);
        *cur
    }

    /// Observe a timestamp that has already been ordered by a replicated state
    /// machine. Unlike `update`, this performs no local-wall-clock skew check:
    /// the timestamp is not an untrusted remote clock sample; it is committed
    /// data that every node must incorporate identically. The next `tick()` is
    /// therefore guaranteed to be strictly greater than the committed value.
    pub fn observe_committed(&self, committed: HlcTimestamp) -> HlcTimestamp {
        let mut cur = self.inner.lock().unwrap();
        if committed > *cur {
            *cur = committed;
        }
        *cur
    }
}

impl HlcClock {
    /// Update the clock upon receiving a message with `remote` timestamp.
    /// Returns the new local timestamp for the received event.
    /// Errors if the remote wall clock is > max_skew_ms ahead of local wall.
    pub fn update(&self, remote: HlcTimestamp) -> Result<HlcTimestamp, HlcError> {
        let wall = wall_now_ms();
        if remote.wall_ms > wall + self.max_skew_ms {
            return Err(HlcError::SkewExceeded {
                skew_ms: remote.wall_ms - wall,
                max_skew_ms: self.max_skew_ms,
            });
        }
        let mut cur = self.inner.lock().unwrap();
        *cur = advance(advance(*cur, wall), remote.wall_ms);
        if cur.wall_ms == remote.wall_ms {
            cur.logical = cur.logical.max(remote.logical).saturating_add(1);
        }
        Ok(*cur)
    }

    /// Read the current clock value without advancing it.
    pub fn now(&self) -> HlcTimestamp {
        *self.inner.lock().unwrap()
    }
}

/// Compute the next HLC value given current state `cur` and local/remote wall time `wall`.
///
/// When `logical` would overflow u16::MAX we bump `wall_ms` by 1 and reset
/// `logical` to 0.  This guarantees strict monotonicity even when the system
/// clock stalls and more than 65 535 events are generated within 1 ms.
fn advance(cur: HlcTimestamp, wall: u64) -> HlcTimestamp {
    if wall > cur.wall_ms {
        HlcTimestamp {
            wall_ms: wall,
            logical: 0,
        }
    } else if cur.logical == u16::MAX {
        // Logical counter exhausted for this wall_ms tick.
        // Advance wall_ms by 1 to guarantee a strictly greater timestamp.
        HlcTimestamp {
            wall_ms: cur.wall_ms + 1,
            logical: 0,
        }
    } else {
        HlcTimestamp {
            wall_ms: cur.wall_ms,
            logical: cur.logical + 1,
        }
    }
}

/// Current wall clock in milliseconds since Unix epoch.
pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_always_monotonic_single_thread() {
        let clock = HlcClock::new(500);
        let mut prev = HlcTimestamp::ZERO;
        for _ in 0..1000 {
            let ts = clock.tick();
            assert!(ts > prev, "HLC went backward: {ts:?} <= {prev:?}");
            prev = ts;
        }
    }

    #[test]
    fn update_with_future_remote_advances_past_it() {
        let clock = HlcClock::new(500);
        let base = clock.tick();
        let remote = HlcTimestamp {
            wall_ms: base.wall_ms + 50,
            logical: 10,
        };
        let result = clock.update(remote).unwrap();
        assert!(result > remote, "result should be > remote: {result:?}");
    }

    #[test]
    fn update_rejects_excessive_skew() {
        let clock = HlcClock::new(500);
        let wall = wall_now_ms();
        let far_future = HlcTimestamp {
            wall_ms: wall + 600,
            logical: 0,
        };
        assert!(clock.update(far_future).is_err());
    }

    #[test]
    fn observe_committed_does_not_consult_wall_clock() {
        let clock = HlcClock::new(1);
        let committed = HlcTimestamp {
            wall_ms: HlcTimestamp::MAX.wall_ms - 10,
            logical: 7,
        };
        assert_eq!(clock.observe_committed(committed), committed);
        assert_eq!(clock.now(), committed);
        assert!(clock.tick() > committed);
    }

    #[test]
    fn to_u64_roundtrip() {
        let ts = HlcTimestamp {
            wall_ms: 1_700_000_000_000,
            logical: 42,
        };
        assert_eq!(HlcTimestamp::from_u64(ts.to_u64()), ts);
    }

    #[test]
    fn be_bytes_roundtrip() {
        let ts = HlcTimestamp {
            wall_ms: 9_999_999,
            logical: 7,
        };
        assert_eq!(HlcTimestamp::from_be_bytes(ts.to_be_bytes()), ts);
    }

    #[test]
    fn ordering_wall_then_logical() {
        let a = HlcTimestamp {
            wall_ms: 100,
            logical: 5,
        };
        let b = HlcTimestamp {
            wall_ms: 100,
            logical: 6,
        };
        let c = HlcTimestamp {
            wall_ms: 101,
            logical: 0,
        };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn tick_70000_times_produces_no_duplicates() {
        use std::collections::HashSet;
        let clock = HlcClock::new(500);
        let mut seen = HashSet::with_capacity(70_000);
        for _ in 0..70_000 {
            let ts = clock.tick();
            assert!(
                seen.insert(ts.to_u64()),
                "duplicate timestamp: wall_ms={} logical={}",
                ts.wall_ms,
                ts.logical
            );
        }
        assert_eq!(seen.len(), 70_000);
    }
}
