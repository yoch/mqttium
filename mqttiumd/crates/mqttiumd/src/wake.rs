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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SendError, TryRecvError, TrySendError};
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
    #[inline]
    pub fn wake(&self) {
        if !self.0.pending.load(Ordering::SeqCst) && !self.0.pending.swap(true, Ordering::SeqCst) {
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

#[derive(Debug)]
struct LaneInner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    senders: usize,
    receiver_alive: bool,
    send_waiters: usize,
    recv_waiters: usize,
}

#[derive(Debug)]
struct LaneShared<T> {
    /// Set whenever `queue` is non-empty or all senders have dropped.
    /// Allows `try_recv` to return `Empty` without touching the mutex.
    has_items: AtomicBool,
    inner: Mutex<LaneInner<T>>,
    not_full: Condvar,
    not_empty: Condvar,
}

/// Bounded sender without `std::sync::mpmc`'s per-message `SyncWaker` overhead.
#[derive(Debug)]
pub struct LaneSender<T> {
    shared: Arc<LaneShared<T>>,
}

impl<T> Clone for LaneSender<T> {
    fn clone(&self) -> Self {
        if let Ok(mut inner) = self.shared.inner.lock() {
            inner.senders += 1;
        }
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for LaneSender<T> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.shared.inner.lock() {
            inner.senders = inner.senders.saturating_sub(1);
            if inner.senders == 0 {
                self.shared.has_items.store(true, Ordering::Release);
                if inner.recv_waiters > 0 {
                    self.shared.not_empty.notify_all();
                }
            }
        }
    }
}

impl<T> LaneSender<T> {
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let Ok(mut inner) = self.shared.inner.lock() else {
            return Err(TrySendError::Disconnected(value));
        };
        if !inner.receiver_alive {
            return Err(TrySendError::Disconnected(value));
        }
        if inner.queue.len() >= inner.capacity {
            return Err(TrySendError::Full(value));
        }
        inner.queue.push_back(value);
        self.shared.has_items.store(true, Ordering::Release);
        let notify = inner.recv_waiters > 0;
        drop(inner);
        if notify {
            self.shared.not_empty.notify_one();
        }
        Ok(())
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let Ok(mut inner) = self.shared.inner.lock() else {
            return Err(SendError(value));
        };
        while inner.receiver_alive && inner.queue.len() >= inner.capacity {
            inner.send_waiters += 1;
            inner = match self.shared.not_full.wait(inner) {
                Ok(guard) => guard,
                Err(_) => return Err(SendError(value)),
            };
            inner.send_waiters -= 1;
        }
        if !inner.receiver_alive {
            return Err(SendError(value));
        }
        inner.queue.push_back(value);
        self.shared.has_items.store(true, Ordering::Release);
        let notify = inner.recv_waiters > 0;
        drop(inner);
        if notify {
            self.shared.not_empty.notify_one();
        }
        Ok(())
    }

    /// Moves as many items as fit from the front of `pending` into the lane
    /// under a single mutex acquisition. Returns `Some(moved)` or `None` if
    /// the receiver has dropped.
    #[inline]
    pub fn try_send_drain(&self, pending: &mut VecDeque<T>) -> Option<usize> {
        if pending.is_empty() {
            return Some(0);
        }
        let Ok(mut inner) = self.shared.inner.lock() else {
            return None;
        };
        if !inner.receiver_alive {
            return None;
        }
        let room = inner.capacity.saturating_sub(inner.queue.len());
        let take = room.min(pending.len());
        if take == 0 {
            return Some(0);
        }
        for _ in 0..take {
            if let Some(item) = pending.pop_front() {
                inner.queue.push_back(item);
            }
        }
        self.shared.has_items.store(true, Ordering::Release);
        let notify = inner.recv_waiters > 0;
        drop(inner);
        if notify {
            self.shared.not_empty.notify_one();
        }
        Some(take)
    }
}

/// Receiving half paired with [`LaneSender`] or [`Signalled`].
#[derive(Debug)]
pub struct LaneReceiver<T> {
    shared: Arc<LaneShared<T>>,
}

impl<T> Drop for LaneReceiver<T> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.shared.inner.lock() {
            inner.receiver_alive = false;
            inner.queue.clear();
            if inner.send_waiters > 0 {
                self.shared.not_full.notify_all();
            }
        }
    }
}

impl<T> LaneReceiver<T> {
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if !self.shared.has_items.load(Ordering::Acquire) {
            return Err(TryRecvError::Empty);
        }
        let Ok(mut inner) = self.shared.inner.lock() else {
            return Err(TryRecvError::Disconnected);
        };
        if let Some(value) = inner.queue.pop_front() {
            let more = !inner.queue.is_empty() || inner.senders == 0;
            self.shared.has_items.store(more, Ordering::Release);
            let notify = inner.send_waiters > 0;
            drop(inner);
            if notify {
                self.shared.not_full.notify_one();
            }
            Ok(value)
        } else if inner.senders == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            self.shared.has_items.store(false, Ordering::Release);
            Err(TryRecvError::Empty)
        }
    }

    /// Drains up to `limit` items into `out` under one mutex acquisition.
    /// Returns `(taken, disconnected_and_empty)`.
    #[inline]
    pub fn drain_into(&self, out: &mut Vec<T>, limit: usize) -> (usize, bool) {
        if limit == 0 || !self.shared.has_items.load(Ordering::Acquire) {
            return (0, false);
        }
        let Ok(mut inner) = self.shared.inner.lock() else {
            return (0, true);
        };
        let take = limit.min(inner.queue.len());
        if take == 0 {
            let disconnected = inner.senders == 0;
            if !disconnected {
                self.shared.has_items.store(false, Ordering::Release);
            }
            return (0, disconnected);
        }
        out.reserve(take);
        for _ in 0..take {
            if let Some(item) = inner.queue.pop_front() {
                out.push(item);
            }
        }
        let more = !inner.queue.is_empty() || inner.senders == 0;
        self.shared.has_items.store(more, Ordering::Release);
        let notify = inner.send_waiters > 0;
        drop(inner);
        if notify {
            self.shared.not_full.notify_all();
        }
        (take, false)
    }

    /// Drains up to `limit` items into `out` under one mutex acquisition.
    /// Returns `(taken, disconnected_and_empty)`.
    #[inline]
    pub fn drain_into_deque(&self, out: &mut VecDeque<T>, limit: usize) -> (usize, bool) {
        if limit == 0 || !self.shared.has_items.load(Ordering::Acquire) {
            return (0, false);
        }
        let Ok(mut inner) = self.shared.inner.lock() else {
            return (0, true);
        };
        let take = limit.min(inner.queue.len());
        if take == 0 {
            let disconnected = inner.senders == 0;
            if !disconnected {
                self.shared.has_items.store(false, Ordering::Release);
            }
            return (0, disconnected);
        }
        out.reserve(take);
        for _ in 0..take {
            if let Some(item) = inner.queue.pop_front() {
                out.push_back(item);
            }
        }
        let more = !inner.queue.is_empty() || inner.senders == 0;
        self.shared.has_items.store(more, Ordering::Release);
        let notify = inner.send_waiters > 0;
        drop(inner);
        if notify {
            self.shared.not_full.notify_all();
        }
        (take, false)
    }

    #[cfg(test)]
    pub fn try_iter(&self) -> impl Iterator<Item = T> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }

    /// Blocks up to `timeout` waiting for the next item (used by tests).
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let Ok(mut inner) = self.shared.inner.lock() else {
            return Err(RecvTimeoutError::Disconnected);
        };
        loop {
            if let Some(value) = inner.queue.pop_front() {
                let more = !inner.queue.is_empty() || inner.senders == 0;
                self.shared.has_items.store(more, Ordering::Release);
                let notify = inner.send_waiters > 0;
                drop(inner);
                if notify {
                    self.shared.not_full.notify_one();
                }
                return Ok(value);
            }
            if inner.senders == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let remaining = match deadline {
                Some(d) => d.saturating_duration_since(Instant::now()),
                None => timeout,
            };
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            inner.recv_waiters += 1;
            let (guard, timed_out) = match self.shared.not_empty.wait_timeout(inner, remaining) {
                Ok((g, res)) => (g, res.timed_out()),
                Err(_) => return Err(RecvTimeoutError::Disconnected),
            };
            inner = guard;
            inner.recv_waiters -= 1;
            if timed_out && inner.queue.is_empty() && inner.senders > 0 {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }
}

/// A bounded FIFO lane without per-message `mpsc` waker overhead.
#[must_use]
pub fn bounded<T>(slots: usize) -> (LaneSender<T>, LaneReceiver<T>) {
    let shared = Arc::new(LaneShared {
        has_items: AtomicBool::new(false),
        inner: Mutex::new(LaneInner {
            queue: VecDeque::new(),
            capacity: slots,
            senders: 1,
            receiver_alive: true,
            send_waiters: 0,
            recv_waiters: 0,
        }),
        not_full: Condvar::new(),
        not_empty: Condvar::new(),
    });
    (
        LaneSender {
            shared: Arc::clone(&shared),
        },
        LaneReceiver { shared },
    )
}

/// The sending half of a bounded lane whose consumer is signalled after
/// each publication, so it never polls the lane on a timer.
#[derive(Debug)]
pub struct Signalled<T> {
    sender: LaneSender<T>,
    wake: Wake,
}

impl<T> Signalled<T> {
    /// Publishes without blocking. A refused value comes back intact and
    /// raises no signal.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let result = self.sender.try_send(value);
        if result.is_ok() {
            self.wake.wake();
        }
        result
    }

    /// Publishes without blocking and without signalling `wake`; the caller
    /// must invoke [`Self::wake`] once the batch is enqueued.
    #[inline]
    pub fn try_send_quiet(&self, value: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)
    }

    /// Signals the consumer after one or more [`Self::try_send_quiet`] calls.
    #[inline]
    pub fn wake(&self) {
        self.wake.wake();
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
pub fn signalled<T>(slots: usize, wake: &Wake) -> (Signalled<T>, LaneReceiver<T>) {
    let (sender, receiver) = bounded(slots);
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
