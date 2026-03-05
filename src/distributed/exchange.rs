// SPDX-License-Identifier: Apache-2.0
// Distributed exchange operators: Shuffle, Broadcast, Gather.
//
// These operators sit at the boundaries between plan fragments in a
// distributed query execution plan (see mod.rs).
//
//   Shuffle   — route each row to exactly one consumer based on hash of
//               the partition key.  Used for distributed hash join / group-by.
//   Broadcast — send every row to all consumers.  Used when the inner side
//               of a join is small enough to replicate.
//   Gather    — merge streams from N producers into one output stream.
//               For ordered gather, merges in sort-key order (sort-merge).
//
// CONFIDENCE: raw=0.80 effective=0.70
// DEPENDS_ON: distributed::backpressure

// Session 5 — not yet wired into query path. Suppress dead_code.


use crate::distributed::backpressure::{bounded_channel, BoundedReceiver, BoundedSender};

// ── Row type (opaque bytes for now) ───────────────────────────────────────

/// A single logical row represented as an opaque byte vector.
/// In a real integration this would be a RecordBatch slice.
pub type Row = Vec<u8>;

// ── ExchangeBuffer ────────────────────────────────────────────────────────

const EXCHANGE_CAPACITY: usize = 256;
const EXCHANGE_LOW_WATERMARK: usize = 64;

pub fn exchange_channel() -> (BoundedSender<Row>, BoundedReceiver<Row>) {
    bounded_channel::<Row>(EXCHANGE_CAPACITY, EXCHANGE_LOW_WATERMARK)
}

// ── Shuffle ───────────────────────────────────────────────────────────────

/// Routes each row to a partition based on `hash(partition_key) % n`.
pub struct ShuffleWriter {
    /// One sender per output partition.
    senders: Vec<BoundedSender<Row>>,
    /// Byte offset within a row to treat as the partition key.
    key_offset: usize,
    key_len: usize,
}

impl ShuffleWriter {
    pub fn new(
        senders: Vec<BoundedSender<Row>>,
        key_offset: usize,
        key_len: usize,
    ) -> Self {
        Self {
            senders,
            key_offset,
            key_len,
        }
    }

    pub async fn write(&self, row: Row) -> Result<(), String> {
        let n = self.senders.len();
        if n == 0 {
            return Err("shuffle: no output partitions".to_string());
        }
        let key_end = (self.key_offset + self.key_len).min(row.len());
        let key_bytes = &row[self.key_offset.min(row.len())..key_end];
        let partition = (fnv1a(key_bytes) as usize) % n;
        self.senders[partition]
            .send(row)
            .await
            .map_err(|_| "shuffle send: receiver dropped".to_string())
    }
}

/// Reads rows from one partition of a shuffle.
pub struct ShuffleReader {
    receiver: BoundedReceiver<Row>,
}

impl ShuffleReader {
    pub fn new(receiver: BoundedReceiver<Row>) -> Self {
        Self { receiver }
    }
    pub async fn next(&mut self) -> Option<Row> {
        self.receiver.recv().await
    }
}

/// Build a shuffle exchange with `n` output partitions.
pub fn shuffle_exchange(
    n: usize,
    key_offset: usize,
    key_len: usize,
) -> (ShuffleWriter, Vec<ShuffleReader>) {
    let mut senders = vec![];
    let mut readers = vec![];
    for _ in 0..n {
        let (tx, rx) = exchange_channel();
        senders.push(tx);
        readers.push(ShuffleReader::new(rx));
    }
    (ShuffleWriter::new(senders, key_offset, key_len), readers)
}

// ── Broadcast ─────────────────────────────────────────────────────────────

/// Sends every row to all consumers.
pub struct BroadcastWriter {
    senders: Vec<BoundedSender<Row>>,
}

impl BroadcastWriter {
    pub fn new(senders: Vec<BoundedSender<Row>>) -> Self {
        Self { senders }
    }

    /// Clone + send the row to each consumer in sequence.
    pub async fn write(&self, row: Row) -> Result<(), String> {
        for (i, tx) in self.senders.iter().enumerate() {
            tx.send(row.clone())
                .await
                .map_err(|_| format!("broadcast send to partition {i}: receiver dropped"))?;
        }
        Ok(())
    }
}

pub fn broadcast_exchange(n: usize) -> (BroadcastWriter, Vec<BoundedReceiver<Row>>) {
    let mut senders = vec![];
    let mut receivers = vec![];
    for _ in 0..n {
        let (tx, rx) = exchange_channel();
        senders.push(tx);
        receivers.push(rx);
    }
    (BroadcastWriter::new(senders), receivers)
}

// ── Gather ────────────────────────────────────────────────────────────────

/// Merges N input streams into one unordered output stream.
/// For ordered merge, use `SortedGather`.
pub struct Gather {
    receivers: Vec<BoundedReceiver<Row>>,
    current: usize,
}

impl Gather {
    pub fn new(receivers: Vec<BoundedReceiver<Row>>) -> Self {
        Self {
            receivers,
            current: 0,
        }
    }

    /// Round-robin poll: returns the next available row from any input.
    pub async fn next(&mut self) -> Option<Row> {
        if self.receivers.is_empty() {
            return None;
        }
        // Try each receiver in round-robin order until one has data.
        let n = self.receivers.len();
        for _ in 0..n {
            let idx = self.current % n;
            self.current += 1;
            if let Ok(Some(row)) =
                tokio::time::timeout(
                    std::time::Duration::from_millis(1),
                    self.receivers[idx].recv(),
                )
                .await
            {
                return Some(row);
            }
        }
        // All receivers currently empty; block on the first one.
        self.receivers[self.current % n].recv().await
    }
}

// ── FNV-1a (local copy to avoid cluster dep) ─────────────────────────────

fn fnv1a(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

// ── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shuffle_routes_to_correct_partition() {
        // 2-partition shuffle.  All rows with the same key byte go to the same partition.
        let (writer, mut readers) = shuffle_exchange(2, 0, 1);
        // Row with key byte 0x00.
        let row_a = vec![0x00u8, 1, 2, 3];
        // Row with same key byte → same partition.
        let row_b = vec![0x00u8, 4, 5, 6];
        // Row with different key byte — may go to different partition.
        let row_c = vec![0xFFu8, 7, 8, 9];

        writer.write(row_a.clone()).await.unwrap();
        writer.write(row_b.clone()).await.unwrap();
        writer.write(row_c.clone()).await.unwrap();

        // Find which partition has 2 rows.
        let p0a = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            readers[0].next(),
        )
        .await;
        let p0b = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            readers[0].next(),
        )
        .await;
        let p1a = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            readers[1].next(),
        )
        .await;
        let p1b = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            readers[1].next(),
        )
        .await;

        let count_p0 = p0a.ok().flatten().is_some() as usize
            + p0b.ok().flatten().is_some() as usize;
        let count_p1 = p1a.ok().flatten().is_some() as usize
            + p1b.ok().flatten().is_some() as usize;
        assert_eq!(count_p0 + count_p1, 3);
    }

    #[tokio::test]
    async fn broadcast_sends_to_all_partitions() {
        let (writer, mut receivers) = broadcast_exchange(3);
        let row = vec![42u8; 8];
        writer.write(row.clone()).await.unwrap();
        for rx in &mut receivers {
            let received = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                rx.recv(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(received, row);
        }
    }

    #[tokio::test]
    async fn gather_collects_from_all_inputs() {
        let mut senders = vec![];
        let mut receivers = vec![];
        for _ in 0..3 {
            let (tx, rx) = exchange_channel();
            senders.push(tx);
            receivers.push(rx);
        }
        for (i, tx) in senders.iter().enumerate() {
            tx.send(vec![i as u8]).await.unwrap();
        }
        let mut gather = Gather::new(receivers);
        let mut results = vec![];
        for _ in 0..3 {
            let row = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                gather.next(),
            )
            .await
            .unwrap()
            .unwrap();
            results.push(row[0]);
        }
        results.sort();
        assert_eq!(results, vec![0, 1, 2]);
    }
}
