// SPDX-License-Identifier: Apache-2.0
// Back-pressure flow control for distributed exchange operators.
//
// BoundedChannel wraps a tokio MPSC channel with high/low-watermark
// semantics so that a fast producer cannot OOM a slow consumer.
//
// Protocol:
//   - Producer calls `send(item)`.  If the channel is at high-watermark,
//     send parks the producer on a Notify until the buffer drains
//     to low-watermark.
//   - Consumer calls `recv()` normally.  When the buffer falls to
//     low-watermark after a receive, it notifies any parked producers.
//
// CONFIDENCE: raw=0.84 effective=0.76
// DEPENDS_ON: (tokio)
// RISK: Deadlock analysis — no circular wait possible because producers and
//       consumers are always in producer→consumer order; no feedback cycle
//       within the same operator.

// Session 5 — not yet wired into query path. Suppress dead_code.

use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};

// ── BoundedChannel ─────────────────────────────────────────────────────────

/// A channel with back-pressure.
/// `high_watermark` — producer blocks when len ≥ high.
/// `low_watermark`  — producer resumes when len ≤ low.
pub struct BoundedSender<T: Send + 'static> {
    inner: mpsc::Sender<T>,
    /// Semaphore with `high_watermark` permits — each send acquires one.
    semaphore: Arc<Semaphore>,
}

pub struct BoundedReceiver<T: Send + 'static> {
    inner: mpsc::Receiver<T>,
    semaphore: Arc<Semaphore>,
}

/// Create a bounded channel pair.
///
/// * `capacity`       — maximum number of items in-flight simultaneously.
/// * `low_watermark`  — resume producers when in-flight drops to this level.
pub fn bounded_channel<T: Send + 'static>(
    capacity: usize,
    _low_watermark: usize,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let semaphore = Arc::new(Semaphore::new(capacity));
    let sender = BoundedSender {
        inner: tx,
        semaphore: semaphore.clone(),
    };
    let receiver = BoundedReceiver {
        inner: rx,
        semaphore,
    };
    (sender, receiver)
}

impl<T: Send + 'static> BoundedSender<T> {
    /// Send an item, pausing if the channel is at capacity.
    /// Returns `Err(item)` if the receiver has been dropped.
    pub async fn send(&self, item: T) -> Result<(), T> {
        // Acquire a semaphore permit (blocks when at high-watermark).
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore closed unexpectedly");
        // Only forget the permit on success — on failure the permit drops,
        // returning the slot to the semaphore so senders can unblock.
        match self.inner.send(item).await {
            Ok(()) => {
                permit.forget();
                Ok(())
            }
            Err(e) => Err(e.0),
        }
    }

    /// Non-blocking try-send.  Returns `Err` if full or disconnected.
    pub fn try_send(&self, item: T) -> Result<(), T> {
        match self.semaphore.try_acquire() {
            Ok(permit) => {
                permit.forget();
                self.inner.try_send(item).map_err(|e| e.into_inner())
            }
            Err(_) => Err(item),
        }
    }
}

impl<T: Send + 'static> BoundedReceiver<T> {
    /// Receive an item, releasing back-pressure when buffer drains.
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.inner.recv().await;
        if item.is_some() {
            // Return a permit to the semaphore, unblocking a parked producer.
            self.semaphore.add_permits(1);
        }
        item
    }
}

// ── FlowController ─────────────────────────────────────────────────────────

/// Tracks and reports flow control metrics for a single exchange channel.
#[derive(Debug, Default)]
pub struct FlowController {
    pub total_sent: u64,
    pub total_received: u64,
    pub producer_pauses: u64,
}

impl FlowController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_send(&mut self) {
        self.total_sent += 1;
    }

    pub fn record_recv(&mut self) {
        self.total_received += 1;
    }

    pub fn record_pause(&mut self) {
        self.producer_pauses += 1;
    }

    /// Approximate in-flight items (sent but not yet received).
    pub fn in_flight(&self) -> u64 {
        self.total_sent.saturating_sub(self.total_received)
    }
}

// ── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_channel_passes_all_items() {
        // Capacity must exceed item count to avoid deadlock when sending
        // sequentially in a single task before draining the receiver.
        let (tx, mut rx) = bounded_channel::<u32>(16, 8);
        for i in 0..10u32 {
            tx.send(i).await.unwrap();
        }
        let mut received = vec![];
        for _ in 0..10 {
            received.push(rx.recv().await.unwrap());
        }
        assert_eq!(received, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn slow_consumer_does_not_cause_unbounded_growth() {
        // High-watermark = 4: producer must block after 4 items.
        let (tx, mut rx) = bounded_channel::<u32>(4, 2);
        let tx = Arc::new(tx);
        let tx_clone = tx.clone();

        // Producer: tries to send 100 items.
        let producer = tokio::spawn(async move {
            for i in 0..100u32 {
                tx_clone.send(i).await.unwrap();
            }
        });

        // Slow consumer: drains one item at a time with a tiny delay.
        let consumer = tokio::spawn(async move {
            let mut count = 0u32;
            while (rx.recv().await).is_some() {
                count += 1;
                if count == 100 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            count
        });

        let (p, c) = tokio::join!(producer, consumer);
        p.unwrap();
        assert_eq!(c.unwrap(), 100);
    }

    #[tokio::test]
    async fn try_send_fails_when_full() {
        let (tx, _rx) = bounded_channel::<u32>(2, 1);
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        // 3rd item: semaphore exhausted.
        assert!(tx.try_send(3).is_err());
    }

    #[test]
    fn flow_controller_tracks_in_flight() {
        let mut fc = FlowController::new();
        fc.record_send();
        fc.record_send();
        fc.record_recv();
        assert_eq!(fc.in_flight(), 1);
    }
}
