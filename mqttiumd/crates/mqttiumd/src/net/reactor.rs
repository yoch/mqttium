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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
const TURNS: usize = 32;
const WAKER: Token = Token(usize::MAX);
const UNREGISTERED: usize = usize::MAX;

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
    /// One bounded, non-blocking step. An error is terminal.
    fn turn(&mut self) -> Result<Turn, &'static str>;
    /// Publishes the terminal facts of the task. Called once, last.
    fn finish(self: Box<Self>, code: &'static str);
}

/// The core's handle on one registered task.
pub(crate) struct Link {
    shared: Arc<Shared>,
    slot: AtomicUsize,
    dirty: AtomicBool,
    owed: AtomicBool,
    finished: AtomicBool,
    wake: Wake,
}

impl Link {
    /// Asks the reactor to run the task. Idempotent until it runs.
    pub fn notify(self: &Arc<Self>) {
        if !self.dirty.swap(true, Ordering::SeqCst) {
            self.shared.send(Message::Run(self.clone()));
        }
    }

    /// The task found a lane towards the core full. The consumer of any
    /// such lane calls [`Link::consumed`] after it takes a value.
    pub fn owe(&self) {
        self.owed.store(true, Ordering::SeqCst);
    }

    /// The core took a value from a lane of this task.
    pub fn consumed(self: &Arc<Self>) {
        if self.owed.swap(false, Ordering::SeqCst) {
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
    Stop,
}

struct Shared {
    queue: Mutex<Vec<Message>>,
    /// Set by the reactor before it blocks and checked by a producer after
    /// it published: one of the two always sees the other.
    sleeping: AtomicBool,
    waker: Waker,
}

impl Shared {
    fn send(&self, message: Message) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.push(message);
        }
        if self.sleeping.load(Ordering::SeqCst) {
            let _ = self.waker.wake();
        }
    }
}

struct Entry {
    link: Arc<Link>,
    task: Box<dyn Driven>,
    descriptor: RawFd,
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
            let shared = Arc::new(Shared {
                queue: Mutex::new(Vec::new()),
                sleeping: AtomicBool::new(false),
                waker: Waker::new(poll.registry(), WAKER)?,
            });
            let worker = shared.clone();
            let handle = thread::Builder::new()
                .name(format!("mqttiumd-io-{index}"))
                .spawn(move || run(poll, &worker))?;
            started.push((shared, Some(handle)));
        }
        Ok(Self {
            threads: started,
            next: AtomicUsize::new(0),
        })
    }

    /// The pool of a process that did not configure one: the unit tests.
    pub fn shared() -> &'static Self {
        static SHARED: OnceLock<Reactors> = OnceLock::new();
        SHARED.get_or_init(|| Self::new(2).expect("reactor threads"))
    }

    /// Creates the link of a task before the task itself exists, so that
    /// the task's lanes can carry it.
    pub(crate) fn link(&self, wake: Wake) -> Arc<Link> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.threads.len();
        Arc::new(Link {
            shared: self.threads[index].0.clone(),
            slot: AtomicUsize::new(UNREGISTERED),
            dirty: AtomicBool::new(false),
            owed: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            wake,
        })
    }

    pub(crate) fn register(&self, link: &Arc<Link>, task: Box<dyn Driven>) {
        link.shared.send(Message::Register(link.clone(), task));
    }
}

impl Drop for Reactors {
    fn drop(&mut self) {
        for (shared, _) in &self.threads {
            shared.send(Message::Stop);
            let _ = shared.waker.wake();
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
    entry.link.slot.store(UNREGISTERED, Ordering::SeqCst);
    let task = entry.task;
    let _ = catch_unwind(AssertUnwindSafe(move || task.finish(code)));
    entry.link.finished.store(true, Ordering::SeqCst);
    // The terminal facts were published before the flag; the core reads
    // the flag outside any lane.
    entry.link.wake.wake();
}

fn run(mut poll: Poll, shared: &Shared) {
    let mut events = Events::with_capacity(256);
    let mut slots: Vec<Option<Entry>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut ready: VecDeque<usize> = VecDeque::new();
    let mut waiting = 0usize;
    let mut last_tick = Instant::now();
    loop {
        let messages = match shared.queue.lock() {
            Ok(mut queue) => std::mem::take(&mut *queue),
            Err(_) => return,
        };
        for message in messages {
            match message {
                Message::Register(link, task) => {
                    let descriptor = task.descriptor();
                    let slot = free.pop().unwrap_or_else(|| {
                        slots.push(None);
                        slots.len() - 1
                    });
                    let mut entry = Entry {
                        link,
                        task,
                        descriptor,
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
                    entry.link.slot.store(slot, Ordering::SeqCst);
                    slots[slot] = Some(entry);
                    ready.push_back(slot);
                }
                Message::Run(link) => {
                    link.dirty.store(false, Ordering::SeqCst);
                    let slot = link.slot.load(Ordering::SeqCst);
                    if let Some(Some(entry)) = slots.get_mut(slot)
                        && Arc::ptr_eq(&entry.link, &link)
                        && !entry.queued
                    {
                        entry.queued = true;
                        ready.push_back(slot);
                    }
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
                let outcome = catch_unwind(AssertUnwindSafe(|| entry.task.turn()))
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
        let timeout = if !ready.is_empty() {
            Duration::ZERO
        } else if waiting != 0 {
            TICK.saturating_sub(last_tick.elapsed())
        } else {
            REST
        };
        shared.sleeping.store(true, Ordering::SeqCst);
        let pending = shared.queue.lock().map_or(true, |queue| !queue.is_empty());
        let timeout = if pending { Duration::ZERO } else { timeout };
        let polled = poll.poll(&mut events, Some(timeout));
        shared.sleeping.store(false, Ordering::SeqCst);
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
        fn turn(&mut self) -> Result<Turn, &'static str> {
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
