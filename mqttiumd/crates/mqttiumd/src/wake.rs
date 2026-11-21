//! One wake-up signal for the core thread.
//!
//! Producers on other threads publish their value first and call
//! [`Wake::wake`] after. The core thread performs a pass over its sources
//! and calls [`Wake::wait`] only after a pass that made no progress. A
//! signal raised at any time after the previous `wait` returned is still
//! pending when the next `wait` is entered, so a value published during a
//! pass is never left waiting for a timer.
//!
//! The pending flag makes repeated signals cheap: only the transition from
//! clear to pending takes the lock and notifies.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SendError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub struct Wake(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    pending: AtomicBool,
    lock: Mutex<()>,
    ready: Condvar,
}

impl Wake {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Signals the waiter. Idempotent until the waiter consumes the signal.
    pub fn wake(&self) {
        if !self.0.pending.swap(true, Ordering::SeqCst) {
            // The waiter checks `pending` under this lock and releases it
            // only by waiting, so the notification cannot fall between its
            // check and its wait.
            let guard = self.0.lock.lock();
            self.0.ready.notify_one();
            drop(guard);
        }
    }

    /// Consumes a pending signal without waiting.
    pub fn take(&self) -> bool {
        self.0.pending.swap(false, Ordering::SeqCst)
    }

    /// Waits until signalled or until `timeout` elapsed. Returns whether a
    /// signal was consumed. A poisoned lock is treated as a signal: the
    /// caller makes another pass and observes the failure at its source.
    pub fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let Ok(mut guard) = self.0.lock.lock() else {
            return true;
        };
        loop {
            if self.0.pending.swap(false, Ordering::SeqCst) {
                return true;
            }
            let remaining = match deadline {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()),
                None => timeout,
            };
            if remaining.is_zero() {
                return false;
            }
            guard = match self.0.ready.wait_timeout(guard, remaining) {
                Ok((guard, _)) => guard,
                Err(_) => return true,
            };
        }
    }
}

/// The sending half of a bounded lane whose consumer is signalled after
/// each publication, so it never polls the lane on a timer.
#[derive(Debug)]
pub struct Signalled<T> {
    sender: SyncSender<T>,
    wake: Wake,
}

impl<T> Signalled<T> {
    /// Publishes without blocking. A refused value comes back intact and
    /// raises no signal.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let result = self.sender.try_send(value);
        if result.is_ok() {
            self.wake.wake();
        }
        result
    }

    /// Publishes, waiting for room.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let result = self.sender.send(value);
        if result.is_ok() {
            self.wake.wake();
        }
        result
    }
}

/// A bounded lane whose consumer waits on `wake`.
#[must_use]
pub fn signalled<T>(slots: usize, wake: &Wake) -> (Signalled<T>, Receiver<T>) {
    let (sender, receiver) = mpsc::sync_channel(slots);
    (
        Signalled {
            sender,
            wake: wake.clone(),
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn a_signal_raised_before_the_wait_is_consumed_without_waiting() {
        let wake = Wake::new();
        wake.wake();
        wake.wake();
        let started = Instant::now();
        assert!(wake.wait(Duration::from_secs(5)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!wake.take());
    }

    #[test]
    fn an_unsignalled_wait_ends_at_its_timeout() {
        let wake = Wake::new();
        let started = Instant::now();
        assert!(!wake.wait(Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn a_signal_from_another_thread_ends_the_wait_early() {
        let wake = Wake::new();
        let remote = wake.clone();
        let producer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            remote.wake();
        });
        let started = Instant::now();
        assert!(wake.wait(Duration::from_secs(5)));
        assert!(started.elapsed() < Duration::from_secs(2));
        producer.join().unwrap();
    }

    /// Every published item is consumed although the consumer waits with a
    /// timeout far longer than the test: a lost signal would stall it.
    #[test]
    fn no_published_item_waits_for_the_timeout() {
        const ITEMS: usize = 20_000;
        let wake = Wake::new();
        let published = Arc::new(AtomicUsize::new(0));
        let producers: Vec<_> = (0..2)
            .map(|_| {
                let wake = wake.clone();
                let published = published.clone();
                thread::spawn(move || {
                    for _ in 0..ITEMS / 2 {
                        published.fetch_add(1, Ordering::SeqCst);
                        wake.wake();
                    }
                })
            })
            .collect();
        let started = Instant::now();
        let mut consumed = 0;
        while consumed < ITEMS {
            let seen = published.load(Ordering::SeqCst);
            if seen == consumed {
                assert!(wake.wait(Duration::from_secs(30)), "a signal was lost");
            } else {
                consumed = seen;
            }
            assert!(started.elapsed() < Duration::from_secs(20));
        }
        for producer in producers {
            producer.join().unwrap();
        }
    }
}
