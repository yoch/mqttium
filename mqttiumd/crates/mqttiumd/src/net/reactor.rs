//! Readiness-driven I/O threads (ADR 0044).
//!
//! A reactor thread owns the transport tasks registered with it. A task is
//! a non-blocking state machine: [`Driven::turn`] attempts everything the
//! task could do and reports whether it advanced. A turn never depends on
//! which event caused it, so a spurious or stale readiness event is
//! harmless. A reactor runs a task when
//!
//! - its socket became readable or writable;
//! - the core published a frame for it, consumed from a lane the task
//!   found full, or cancelled it ([`Link::notify`]);
//! - it holds a time-bound state (a handshake, a partial packet, a value
//!   a full lane refused, an unfinished write): such a task reports
//!   `waiting` and is run again every [`TICK`] so that its own deadline
//!   checks fire.
//!
//! An idle connection costs nothing between events. Nothing here knows
//! MQTT, TLS or the store.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::wake::Wake;

/// How often a task holding a time-bound state is run for its deadlines.
const TICK: Duration = Duration::from_millis(5);
/// Upper bound of a wait with no such task: nothing depends on it.
const REST: Duration = Duration::from_secs(1);
/// Consecutive turns one task may take before the others are served.
const TURNS: usize = 64;
const WAKER: Token = Token(usize::MAX);
const UNREGISTERED_GEN: u64 = u64::MAX;
static CONFIGURED_THREADS: AtomicUsize = AtomicUsize::new(0);

/// Sets the reactor thread count used when the shared pool initializes.
pub fn configure_threads(threads: usize) {
    CONFIGURED_THREADS.store(threads.clamp(1, 32), Ordering::Relaxed);
}

/// Default reactor count: matches available CPU parallelism within `[2, 4]`.
#[must_use]
pub fn default_threads() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2)
        .clamp(2, 4)
}

/// What one turn of a task did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Turn {
    /// Bytes or values moved. A task is run again while it advances.
    pub progress: bool,
    /// A deadline is pending inside the task.
    pub waiting: bool,
}

/// A transport task a reactor drives.
pub(crate) trait Driven: Send {
    /// The descriptor whose readiness runs the task.
    fn descriptor(&self) -> RawFd;
    /// One bounded, non-blocking step using the reactor thread's reusable
    /// scratch buffers. An error is terminal.
    fn turn(&mut self, scratch: &mut super::ReactorScratch) -> Result<Turn, &'static str>;
    /// Publishes the terminal facts of the task. Called once, last.
    fn finish(self: Box<Self>, code: &'static str);
}

/// The core's handle on one registered task.
pub(crate) struct Link {
    shared: Arc<Shared>,
    wake: Wake,
    /// Packed `(generation << 32) | slot`, or `UNREGISTERED_GEN` while not
    /// registered. Enables zero-`Arc` wake messages on the hot path.
    slot_gen: AtomicU64,
    dirty: AtomicBool,
    owed: AtomicBool,
    finished: AtomicBool,
}

impl Link {
    /// Asks the reactor to run the task. Idempotent until it runs.
    #[inline]
    pub fn notify(self: &Arc<Self>) {
        if !self.dirty.load(Ordering::SeqCst) && !self.dirty.swap(true, Ordering::SeqCst) {
            let packed = self.slot_gen.load(Ordering::Acquire);
            if packed != UNREGISTERED_GEN {
                self.shared.send(Message::RunSlot(packed));
            } else {
                self.shared.send(Message::Run(self.clone()));
            }
        }
    }

    /// The task found a lane towards the core full. The consumer of any
    /// such lane calls [`Link::consumed`] after it takes a value.
    pub fn owe(&self) {
        self.owed.store(true, Ordering::SeqCst);
    }

    /// The core took a value from a lane of this task.
    pub fn consumed(self: &Arc<Self>) {
        if self.owed.load(Ordering::SeqCst) && self.owed.swap(false, Ordering::SeqCst) {
            self.notify();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

enum Message {
    Register(Arc<Link>, Box<dyn Driven>),
    Run(Arc<Link>),
    RunSlot(u64),
    Stop,
    /// Leaves the loop as an unrecoverable poll error does.
    #[cfg(test)]
    Fail,
}

struct Shared {
    queue: Mutex<Vec<Message>>,
    /// Set by the reactor before it blocks and checked by a producer after
    /// it published: one of the two always sees the other.
    sleeping: AtomicBool,
    waker: Waker,
    /// A producer serves many lanes in one pass and will wake once.
    held: AtomicBool,
    /// A message was queued and its wake-up not given yet.
    owing: AtomicBool,
    /// The thread left its loop: nothing queued from now on is served.
    dead: AtomicBool,
}

impl Shared {
    fn new(waker: Waker) -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            sleeping: AtomicBool::new(false),
            waker,
            held: AtomicBool::new(false),
            owing: AtomicBool::new(false),
            dead: AtomicBool::new(false),
        }
    }

    fn send(&self, message: Message) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.push(message);
        }
        // The debt is recorded before the hold is read, and the release
        // clears the hold before it reads the debt: one of them pays it.
        self.owing.store(true, Ordering::SeqCst);
        // Checked after the push, as the thread sets it before its last
        // sweep: one of the two finds the message.
        if self.dead.load(Ordering::SeqCst) {
            self.abandon();
        } else if !self.held.load(Ordering::SeqCst) {
            self.kick();
        }
    }

    /// Ends every task still waiting to be registered. A thread that left
    /// its loop serves nothing: a task handed to it is finished at once,
    /// so that its connection is refused instead of left without a driver.
    fn abandon(&self) {
        let messages = match self.queue.lock() {
            Ok(mut queue) => std::mem::take(&mut *queue),
            Err(_) => return,
        };
        for message in messages {
            if let Message::Register(link, task) = message {
                let _ = catch_unwind(AssertUnwindSafe(move || task.finish("reactor_unavailable")));
                link.finished.store(true, Ordering::SeqCst);
                link.wake.wake();
            }
        }
    }

    fn kick(&self) {
        if self.owing.swap(false, Ordering::SeqCst) && self.sleeping.load(Ordering::SeqCst) {
            let _ = self.waker.wake();
        }
    }

    /// The reactor takes every message queued (`ReactorTake`) into `out`,
    /// reusing the backing allocation across passes. `false` when the lock is
    /// poisoned: the reactor then leaves its loop.
    fn take(&self, out: &mut Vec<Message>) -> bool {
        out.clear();
        match self.queue.lock() {
            Ok(mut queue) => {
                std::mem::swap(&mut *queue, out);
                true
            }
            Err(_) => false,
        }
    }

    /// The reactor blocks until an event, a wake-up or `timeout`
    /// (`ReactorSleep`, `ReactorRecheck`, `ReactorPoll`, `ReactorAwake`).
    fn sleep(&self, poll: &mut Poll, events: &mut Events, timeout: Duration) -> io::Result<()> {
        self.sleeping.store(true, Ordering::SeqCst);
        let pending = self.queue.lock().map_or(true, |queue| !queue.is_empty());
        let timeout = if pending { Duration::ZERO } else { timeout };
        let polled = poll.poll(events, Some(timeout));
        self.sleeping.store(false, Ordering::SeqCst);
        polled
    }

    /// A producer will serve many lanes in one pass (`ProducerHold`).
    fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// The pass is over (`ProducerRelease`): clear the hold, then kick.
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.kick();
    }

    /// The thread left its loop: mark dead, then abandon remaining registrations.
    fn leave(&self) {
        self.dead.store(true, Ordering::SeqCst);
        self.abandon();
    }

    /// The pool is dropped: tell the thread to stop and wake it unconditionally.
    fn stop(&self) {
        self.send(Message::Stop);
        let _ = self.waker.wake();
    }
}

/// While it lives, messages to the reactors are queued without a wake-up.
/// Dropping it wakes each reactor that was sent something meanwhile. A
/// reactor that is awake takes its messages as usual, and one that sleeps
/// takes them at its next timeout at the latest.
pub(crate) struct Held<'a>(&'a Reactors);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        for (shared, _) in &self.0.threads {
            shared.release();
        }
    }
}

struct Entry {
    link: Arc<Link>,
    task: Box<dyn Driven>,
    descriptor: RawFd,
    generation: u32,
    queued: bool,
    waiting: bool,
}

/// A fixed set of reactor threads. Tasks are spread over them in
/// registration order and stay on their thread for life.
pub struct Reactors {
    threads: Vec<(Arc<Shared>, Option<JoinHandle<()>>)>,
    next: AtomicUsize,
}

impl Reactors {
    pub fn new(threads: usize) -> io::Result<Self> {
        let mut started = Vec::new();
        for index in 0..threads.max(1) {
            let poll = Poll::new()?;
            let shared = Arc::new(Shared::new(Waker::new(poll.registry(), WAKER)?));
            let worker = shared.clone();
            let handle = thread::Builder::new()
                .name(format!("mqttiumd-io-{index}"))
                .spawn(move || {
                    run(poll, &worker);
                    worker.leave();
                })?;
            started.push((shared, Some(handle)));
        }
        Ok(Self {
            threads: started,
            next: AtomicUsize::new(0),
        })
    }

    /// The pool of the process. A pool that could not be started is an
    /// error every caller sees, never a panic.
    pub fn shared() -> io::Result<&'static Self> {
        static SHARED: OnceLock<Result<Reactors, io::ErrorKind>> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                let threads = match CONFIGURED_THREADS.load(Ordering::Relaxed) {
                    0 => default_threads(),
                    n => n,
                };
                Self::new(threads).map_err(|error| error.kind())
            })
            .as_ref()
            .map_err(|kind| io::Error::from(*kind))
    }

    /// Creates the link of a task before the task itself exists, so that
    /// the task's lanes can carry it.
    pub(crate) fn link(&self, wake: Wake) -> Arc<Link> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.threads.len();
        Arc::new(Link {
            shared: self.threads[index].0.clone(),
            wake,
            slot_gen: AtomicU64::new(UNREGISTERED_GEN),
            dirty: AtomicBool::new(false),
            owed: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        })
    }

    pub(crate) fn hold(&self) -> Held<'_> {
        for (shared, _) in &self.threads {
            shared.hold();
        }
        Held(self)
    }

    pub(crate) fn register(&self, link: &Arc<Link>, task: Box<dyn Driven>) {
        link.shared.send(Message::Register(link.clone(), task));
    }
}

impl Drop for Reactors {
    fn drop(&mut self) {
        for (shared, _) in &self.threads {
            shared.stop();
        }
        for (_, handle) in &mut self.threads {
            if let Some(handle) = handle.take() {
                let _ = handle.join();
            }
        }
    }
}

fn retire(poll: &Poll, entry: Entry, code: &'static str) {
    let _ = poll.registry().deregister(&mut SourceFd(&entry.descriptor));
    entry
        .link
        .slot_gen
        .store(UNREGISTERED_GEN, Ordering::SeqCst);
    let task = entry.task;
    let _ = catch_unwind(AssertUnwindSafe(move || task.finish(code)));
    entry.link.finished.store(true, Ordering::SeqCst);
    // The terminal facts were published before the flag; the core reads
    // the flag outside any lane.
    entry.link.wake.wake();
}

fn run(mut poll: Poll, shared: &Shared) {
    let mut events = Events::with_capacity(1024);
    let mut slots: Vec<Option<Entry>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut ready: VecDeque<usize> = VecDeque::new();
    let mut messages: Vec<Message> = Vec::with_capacity(64);
    let mut next_gen: u32 = 1;
    // Reusable scratch read/batch buffers shared across all tasks on this
    // reactor thread so idle connections hold zero I/O buffer RAM.
    let mut scratch = super::ReactorScratch::new();
    let mut waiting = 0usize;
    let mut last_tick = Instant::now();
    loop {
        if !shared.take(&mut messages) {
            return;
        }
        for message in messages.drain(..) {
            match message {
                Message::Register(link, task) => {
                    let descriptor = task.descriptor();
                    let slot = free.pop().unwrap_or_else(|| {
                        slots.push(None);
                        slots.len() - 1
                    });
                    let generation = next_gen;
                    next_gen = next_gen.wrapping_add(1).max(1);
                    let mut entry = Entry {
                        link,
                        task,
                        descriptor,
                        generation,
                        queued: true,
                        waiting: false,
                    };
                    if poll
                        .registry()
                        .register(
                            &mut SourceFd(&descriptor),
                            Token(slot),
                            Interest::READABLE | Interest::WRITABLE,
                        )
                        .is_err()
                    {
                        free.push(slot);
                        entry.descriptor = descriptor;
                        retire(&poll, entry, "reactor_registration");
                        continue;
                    }
                    let packed = (u64::from(generation) << 32) | (slot as u64);
                    entry.link.slot_gen.store(packed, Ordering::SeqCst);
                    slots[slot] = Some(entry);
                    ready.push_back(slot);
                }
                Message::Run(link) => {
                    link.dirty.store(false, Ordering::SeqCst);
                    let packed = link.slot_gen.load(Ordering::SeqCst);
                    if packed != UNREGISTERED_GEN {
                        let raw_slot = (packed as u32) as usize;
                        if let Some(Some(entry)) = slots.get_mut(raw_slot)
                            && Arc::ptr_eq(&entry.link, &link)
                            && !entry.queued
                        {
                            entry.queued = true;
                            ready.push_back(raw_slot);
                        }
                    }
                }
                Message::RunSlot(packed) => {
                    let generation = (packed >> 32) as u32;
                    let raw_slot = (packed as u32) as usize;
                    if let Some(Some(entry)) = slots.get_mut(raw_slot)
                        && entry.generation == generation
                    {
                        entry.link.dirty.store(false, Ordering::SeqCst);
                        if !entry.queued {
                            entry.queued = true;
                            ready.push_back(raw_slot);
                        }
                    }
                }
                #[cfg(test)]
                Message::Fail => {
                    for entry in slots.drain(..).flatten() {
                        retire(&poll, entry, "reactor_poll");
                    }
                    return;
                }
                Message::Stop => {
                    for entry in slots.drain(..).flatten() {
                        retire(&poll, entry, "cancelled");
                    }
                    return;
                }
            }
        }
        while let Some(slot) = ready.pop_front() {
            let Some(entry) = slots.get_mut(slot).and_then(Option::as_mut) else {
                continue;
            };
            entry.queued = false;
            let was_waiting = entry.waiting;
            let mut ended = None;
            for turn in 0..TURNS {
                // A panic ends one connection, not every task of the thread.
                let outcome = catch_unwind(AssertUnwindSafe(|| entry.task.turn(&mut scratch)))
                    .unwrap_or(Err("task_panicked"));
                match outcome {
                    Ok(done) => {
                        entry.waiting = done.waiting;
                        if !done.progress {
                            break;
                        }
                        if turn + 1 == TURNS {
                            entry.queued = true;
                        }
                    }
                    Err(code) => {
                        ended = Some(code);
                        break;
                    }
                }
            }
            if let Some(code) = ended {
                if was_waiting {
                    waiting -= 1;
                }
                if let Some(entry) = slots[slot].take() {
                    retire(&poll, entry, code);
                }
                free.push(slot);
                continue;
            }
            match (was_waiting, entry.waiting) {
                (false, true) => waiting += 1,
                (true, false) => waiting -= 1,
                _ => {}
            }
            if entry.queued {
                ready.push_back(slot);
                // Messages and readiness of other tasks are taken between
                // the bounded bursts of a busy one.
                break;
            }
        }
        if ready.is_empty() {
            ready.clear();
        }
        let timeout = if !ready.is_empty() {
            Duration::ZERO
        } else if waiting != 0 {
            TICK.saturating_sub(last_tick.elapsed())
        } else {
            REST
        };
        let polled = shared.sleep(&mut poll, &mut events, timeout);
        if let Err(error) = polled
            && error.kind() != io::ErrorKind::Interrupted
        {
            for entry in slots.drain(..).flatten() {
                retire(&poll, entry, "reactor_poll");
            }
            return;
        }
        for event in &events {
            let slot = event.token().0;
            if let Some(Some(entry)) = slots.get_mut(slot)
                && !entry.queued
            {
                entry.queued = true;
                ready.push_back(slot);
            }
        }
        if waiting != 0 && last_tick.elapsed() >= TICK {
            last_tick = Instant::now();
            for (slot, entry) in slots.iter_mut().enumerate() {
                if let Some(entry) = entry
                    && entry.waiting
                    && !entry.queued
                {
                    entry.queued = true;
                    ready.push_back(slot);
                }
            }
        } else if waiting == 0 {
            last_tick = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::sync::mpsc;

    /// Echoes what it reads, counts its turns, ends on end of file.
    struct Echo {
        stream: TcpStream,
        turns: Arc<AtomicUsize>,
        ended: mpsc::Sender<&'static str>,
        hold: Arc<AtomicBool>,
    }

    impl Driven for Echo {
        fn descriptor(&self) -> RawFd {
            self.stream.as_raw_fd()
        }
        fn turn(
            &mut self,
            _scratch: &mut super::super::ReactorScratch,
        ) -> Result<Turn, &'static str> {
            self.turns.fetch_add(1, Ordering::SeqCst);
            if self.hold.load(Ordering::SeqCst) {
                return Ok(Turn {
                    progress: false,
                    waiting: true,
                });
            }
            let mut bytes = [0u8; 64];
            match self.stream.read(&mut bytes) {
                Ok(0) => Err("eof"),
                Ok(count) => {
                    self.stream
                        .write_all(&bytes[..count])
                        .map_err(|_| "write")?;
                    Ok(Turn {
                        progress: true,
                        waiting: false,
                    })
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(Turn::default()),
                Err(_) => Err("read"),
            }
        }
        fn finish(self: Box<Self>, code: &'static str) {
            let _ = self.ended.send(code);
        }
    }

    struct Fixture {
        peer: TcpStream,
        link: Arc<Link>,
        turns: Arc<AtomicUsize>,
        ended: mpsc::Receiver<&'static str>,
        hold: Arc<AtomicBool>,
    }

    fn echo(pool: &Reactors) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(true).unwrap();
        let turns = Arc::new(AtomicUsize::new(0));
        let hold = Arc::new(AtomicBool::new(false));
        let (sender, ended) = mpsc::channel();
        let link = pool.link(Wake::new());
        pool.register(
            &link,
            Box::new(Echo {
                stream,
                turns: turns.clone(),
                ended: sender,
                hold: hold.clone(),
            }),
        );
        Fixture {
            peer,
            link,
            turns,
            ended,
            hold,
        }
    }

    #[test]
    fn a_task_runs_on_readiness_and_rests_between_events() {
        let pool = Reactors::new(1).unwrap();
        let mut fixture = echo(&pool);
        fixture.peer.write_all(b"ping").unwrap();
        let mut reply = [0u8; 4];
        fixture.peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"ping");
        thread::sleep(Duration::from_millis(50));
        let rested = fixture.turns.load(Ordering::SeqCst);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            fixture.turns.load(Ordering::SeqCst),
            rested,
            "an idle task was run without an event"
        );
        assert!(!fixture.link.is_finished());
    }

    #[test]
    fn a_notification_runs_the_task_once_and_a_waiting_task_is_ticked() {
        let pool = Reactors::new(1).unwrap();
        let fixture = echo(&pool);
        thread::sleep(Duration::from_millis(50));
        let before = fixture.turns.load(Ordering::SeqCst);
        fixture.link.notify();
        fixture.link.notify();
        thread::sleep(Duration::from_millis(100));
        let after = fixture.turns.load(Ordering::SeqCst);
        assert!(after > before, "a notification did not run the task");
        assert!(
            after <= before + 2,
            "idempotent notifications ran it {after} times"
        );
        // A task with a pending deadline is run again without any event.
        fixture.hold.store(true, Ordering::SeqCst);
        fixture.link.notify();
        thread::sleep(Duration::from_millis(100));
        let ticked = fixture.turns.load(Ordering::SeqCst);
        assert!(
            ticked >= after + 5,
            "a waiting task was run {} times",
            ticked - after
        );
        // A value the core consumed from a full lane runs the task as well.
        fixture.hold.store(false, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(50));
        let settled = fixture.turns.load(Ordering::SeqCst);
        fixture.link.consumed();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(fixture.turns.load(Ordering::SeqCst), settled);
        fixture.link.owe();
        fixture.link.consumed();
        thread::sleep(Duration::from_millis(50));
        assert!(fixture.turns.load(Ordering::SeqCst) > settled);
    }

    /// A reactor that left its loop on an unrecoverable error finishes the
    /// tasks it had and refuses the ones registered afterwards: none is
    /// left without a driver.
    #[test]
    fn a_dead_reactor_finishes_what_is_registered_with_it() {
        let pool = Reactors::new(1).unwrap();
        let before = echo(&pool);
        pool.threads[0].0.send(Message::Fail);
        assert_eq!(
            before.ended.recv_timeout(Duration::from_secs(5)).unwrap(),
            "reactor_poll"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.threads[0].0.dead.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let after = echo(&pool);
        assert_eq!(
            after.ended.recv_timeout(Duration::from_secs(5)).unwrap(),
            "reactor_unavailable"
        );
        assert!(after.link.is_finished());
    }

    #[test]
    fn a_held_wake_up_is_given_when_the_hold_ends() {
        let pool = Reactors::new(1).unwrap();
        let fixture = echo(&pool);
        thread::sleep(Duration::from_millis(50));
        let before = fixture.turns.load(Ordering::SeqCst);
        let held = pool.hold();
        fixture.link.notify();
        drop(held);
        let deadline = Instant::now() + Duration::from_millis(500);
        while fixture.turns.load(Ordering::SeqCst) == before {
            assert!(
                Instant::now() < deadline,
                "the wake-up owed at the end of the hold was not given"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_terminal_turn_publishes_its_facts_before_the_link_reads_finished() {
        let pool = Reactors::new(1).unwrap();
        let fixture = echo(&pool);
        drop(fixture.peer);
        assert_eq!(
            fixture.ended.recv_timeout(Duration::from_secs(5)).unwrap(),
            "eof"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !fixture.link.is_finished() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        // A notification for a retired task is ignored.
        fixture.link.notify();
    }

    /// Ends at its first turn. Its `finish` tells the test it began and
    /// returns when the test lets it: the terminal facts are being published.
    struct SlowEnd {
        stream: TcpStream,
        began: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Driven for SlowEnd {
        fn descriptor(&self) -> RawFd {
            self.stream.as_raw_fd()
        }
        fn turn(
            &mut self,
            _scratch: &mut super::super::ReactorScratch,
        ) -> Result<Turn, &'static str> {
            Err("eof")
        }
        fn finish(self: Box<Self>, _code: &'static str) {
            let _ = self.began.send(());
            let _ = self.release.recv();
        }
    }

    #[test]
    fn a_link_does_not_read_finished_while_its_task_publishes_its_terminal_facts() {
        let pool = Reactors::new(1).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(true).unwrap();
        let (began, in_finish) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let link = pool.link(Wake::new());
        pool.register(
            &link,
            Box::new(SlowEnd {
                stream,
                began,
                release: released,
            }),
        );
        in_finish.recv_timeout(Duration::from_secs(5)).unwrap();
        // `finish` has not returned: a core that read the flag now would
        // take the lanes as complete before the terminal facts are in them.
        assert!(
            !link.is_finished(),
            "finished was set before the terminal facts were published"
        );
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !link.is_finished() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn dropping_the_pool_finishes_every_task_as_cancelled() {
        let pool = Reactors::new(2).unwrap();
        let first = echo(&pool);
        let second = echo(&pool);
        drop(pool);
        assert_eq!(first.ended.try_recv().unwrap(), "cancelled");
        assert_eq!(second.ended.try_recv().unwrap(), "cancelled");
        assert!(first.link.is_finished() && second.link.is_finished());
    }
}
