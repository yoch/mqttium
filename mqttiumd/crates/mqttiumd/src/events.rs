//! Bounded product observations, independent of fixture-based campaign verdicts.
use mqttiumd_contract::ids::OperationId;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Same Linux monotonic clock as the independent client measurements.
pub(crate) fn monotonic_ns() -> Option<u64> {
    let now = nix::time::clock_gettime(nix::time::ClockId::CLOCK_MONOTONIC).ok()?;
    u64::try_from(now.tv_sec())
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(now.tv_nsec()).ok()?)
}

const MAX_RECORD: usize = 2048;
const MAX_LOG: usize = 16 * 1024 * 1024;
/// What the event file records.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Level {
    /// Every observation, including one record per frame and per command.
    #[default]
    Audit,
    /// Lifecycle, refusals and faults. Observations made per message are
    /// counted and reported as one `counters` record per interval, so a
    /// drop or a refusal on the data path stays visible without a write
    /// per message.
    Ops,
}

impl std::str::FromStr for Level {
    type Err = ();
    fn from_str(name: &str) -> Result<Self, ()> {
        match name {
            "audit" => Ok(Self::Audit),
            "ops" => Ok(Self::Ops),
            _ => Err(()),
        }
    }
}

/// The kinds written at [`Level::Ops`], sorted. Any other kind is counted.
const OPS_KINDS: &[&str] = &[
    "authorization_refused",
    "barrier_hit",
    "barrier_released",
    "closed_receipts_collected",
    "connect_validated",
    "connection_closed",
    "connection_opened",
    "connection_refused",
    "core_fault",
    "counters",
    "expiry_inventory",
    "input_capacity_refused",
    "log_checkpoint_failed",
    "log_checkpoint_saved",
    "log_rotated",
    "message_expiry_clock",
    "message_expiry_retry_exhausted",
    "packet_refused",
    "ready",
    "receipt_policy",
    "receipt_window_pressure",
    "session_changed",
    "session_clock_anchor",
    "session_clock_checkpoint",
    "session_clock_degraded",
    "session_established",
    "session_refused",
    "session_shutdown_completed",
    "shutdown_incomplete",
    "sqlite_configuration",
    "starting",
    "startup_or_runtime_refused",
    "stopped",
    "tls_authenticated",
    "tls_profile",
    "volatile_snapshot_failed",
    "volatile_snapshot_saved",
    "will_capabilities",
    "will_deferred",
];

/// Distinct suppressed kinds a `counters` record can carry.
const MAX_TALLIES: usize = 96;

/// Hot suppressed event kinds counted lock-free in `Level::Ops`.
const FAST_TALLIES: [&str; 16] = [
    "command_submitted",
    "core_finish_observed",
    "delivery_discarded",
    "effect_retained",
    "frame_failed",
    "frame_queued",
    "frame_written",
    "puback_ignored",
    "puback_observed",
    "publish_validated",
    "qos0_dropped",
    "receive_credit",
    "store_group",
    "store_result",
    "wire_receive_credit",
    "work_page_ignored",
];

#[inline]
fn fast_tally_index(kind: &str) -> Option<usize> {
    match kind {
        "command_submitted" => Some(0),
        "core_finish_observed" => Some(1),
        "delivery_discarded" => Some(2),
        "effect_retained" => Some(3),
        "frame_failed" => Some(4),
        "frame_queued" => Some(5),
        "frame_written" => Some(6),
        "puback_ignored" => Some(7),
        "puback_observed" => Some(8),
        "publish_validated" => Some(9),
        "qos0_dropped" => Some(10),
        "receive_credit" => Some(11),
        "store_group" => Some(12),
        "store_result" => Some(13),
        "wire_receive_credit" => Some(14),
        "work_page_ignored" => Some(15),
        _ => None,
    }
}

#[derive(Clone)]
pub struct Events {
    log: Arc<Mutex<Log>>,
    fast_tallies: Arc<[AtomicU64; FAST_TALLIES.len()]>,
    level: Level,
}
struct Log {
    file: File,
    path: PathBuf,
    previous: PathBuf,
    seq: u64,
    bytes: usize,
    first_seq: u64,
    tallies: BTreeMap<String, u64>,
}
impl Events {
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_at(path, Level::Audit)
    }

    /// Whether a record of this kind is written at the current level. A
    /// caller on the data path asks before it builds the fields.
    #[inline]
    #[must_use]
    pub fn records(&self, kind: &str) -> bool {
        if self.level == Level::Audit {
            return true;
        }
        if kind == "session_cardinality" || fast_tally_index(kind).is_some() {
            return false;
        }
        OPS_KINDS.binary_search(&kind).is_ok()
    }

    /// Counts one observation that the current level does not write.
    #[inline]
    pub fn tally(&self, kind: &str) {
        if self.level == Level::Ops
            && let Some(index) = fast_tally_index(kind)
        {
            self.fast_tallies[index].fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.records(kind) {
            return;
        }
        self.tally_slow(kind);
    }

    #[cold]
    fn tally_slow(&self, kind: &str) {
        if let Some(index) = fast_tally_index(kind) {
            self.fast_tallies[index].fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Ok(mut log) = self.log.lock() {
            if let Some(count) = log.tallies.get_mut(kind) {
                *count = count.saturating_add(1);
            } else if log.tallies.len() < MAX_TALLIES {
                log.tallies.insert(kind.to_owned(), 1);
            } else {
                let other = log.tallies.entry("other".to_owned()).or_insert(0);
                *other = other.saturating_add(1);
            }
        }
    }

    /// Writes the counts gathered since the previous call as one `counters`
    /// record, or nothing when no suppressed observation was made.
    pub fn flush_tallies(&self) -> io::Result<()> {
        let mut counts = {
            let mut log = self
                .log
                .lock()
                .map_err(|_| io::Error::other("event lock"))?;
            std::mem::take(&mut log.tallies)
        };
        for (index, kind) in FAST_TALLIES.iter().enumerate() {
            let delta = self.fast_tallies[index].swap(0, Ordering::Relaxed);
            if delta != 0 {
                let entry = counts.entry((*kind).to_owned()).or_insert(0);
                *entry = entry.saturating_add(delta);
            }
        }
        if counts.is_empty() {
            return Ok(());
        }
        let counts: serde_json::Map<String, Value> = counts
            .into_iter()
            .map(|(kind, count)| (kind, Value::String(count.to_string())))
            .collect();
        self.write("counters", json!({"details":{"counts":counts}}))
    }

    pub fn open_at(path: &Path, level: Level) -> io::Result<Self> {
        let mut name = path.as_os_str().to_owned();
        name.push(".previous");
        let previous = PathBuf::from(name);
        match std::fs::symlink_metadata(&previous) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "previous event path is not a regular file",
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        // If a prior run left an event journal at `path`, acquire an exclusive
        // non-blocking lock on it first (so a live broker's journal is never
        // displaced), then rotate it atomically to `.previous` before opening
        // a fresh segment with sequence 1.
        let held_prior = match std::fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "event path is not a regular file",
                    ));
                }
                let existing = OpenOptions::new().write(true).open(path)?;
                existing.try_lock().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "event file is locked by another process",
                    )
                })?;
                std::fs::rename(path, &previous)?;
                Some(existing)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "event file is locked by another process",
            )
        })?;
        drop(held_prior);
        Ok(Self {
            log: Arc::new(Mutex::new(Log {
                file,
                path: path.to_owned(),
                previous,
                seq: 0,
                bytes: 0,
                first_seq: 1,
                tallies: BTreeMap::new(),
            })),
            fast_tallies: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            level,
        })
    }
    #[inline]
    pub fn emit(&self, kind: &str, fields: Value) -> io::Result<()> {
        if self.level == Level::Ops {
            if let Some(index) = fast_tally_index(kind) {
                self.fast_tallies[index].fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if OPS_KINDS.binary_search(&kind).is_err() {
                self.tally_slow(kind);
                return Ok(());
            }
        }
        self.write(kind, fields)
    }
    /// Lazily builds the JSON fields only when `kind` is actually recorded.
    #[inline]
    pub fn emit_with(&self, kind: &str, fields: impl FnOnce() -> Value) -> io::Result<()> {
        if self.level == Level::Ops {
            if let Some(index) = fast_tally_index(kind) {
                self.fast_tallies[index].fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if OPS_KINDS.binary_search(&kind).is_err() {
                self.tally_slow(kind);
                return Ok(());
            }
        }
        self.write(kind, fields())
    }
    fn write(&self, kind: &str, fields: Value) -> io::Result<()> {
        let mut log = self
            .log
            .lock()
            .map_err(|_| io::Error::other("event lock"))?;
        let mut seq = log
            .seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("event counter"))?;
        let mut bytes = event_bytes(seq, kind, &fields)?;
        if log
            .bytes
            .checked_add(bytes.len())
            .is_none_or(|total| total > MAX_LOG)
        {
            // Reserve both sequence values before changing files. A rotation
            // marker is a real observation with its own continuous sequence.
            let next = seq
                .checked_add(1)
                .ok_or_else(|| io::Error::other("event counter"))?;
            let marker = event_bytes(
                seq,
                "log_rotated",
                &json!({"details":{"retained_from":log.first_seq.to_string(),"previous_last_seq":log.seq.to_string(),"discarded_through":(log.first_seq-1).to_string()}}),
            )?;
            std::fs::rename(&log.path, &log.previous)?;
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&log.path)?;
            file.try_lock().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "event file is locked by another process",
                )
            })?;
            log.file = file;
            log.file.write_all(&marker)?;
            log.file.sync_data()?;
            log.seq = seq;
            log.first_seq = seq;
            log.bytes = marker.len();
            seq = next;
            bytes = event_bytes(seq, kind, &fields)?;
        }
        log.file.write_all(&bytes)?;
        // A barrier is acknowledged only after its observation is outside the broker.
        if kind == "barrier_hit" {
            log.file.sync_data()?;
        }
        log.seq = seq;
        log.bytes = log
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("event budget"))?;
        Ok(())
    }
}
fn event_bytes(seq: u64, kind: &str, fields: &Value) -> io::Result<Vec<u8>> {
    let mut event = json!({"format":"mqttiumd.product-event","schema_version":1,"seq":seq.to_string(),"kind":kind,"pid":std::process::id()});
    for (k, v) in fields
        .as_object()
        .ok_or_else(|| io::Error::other("event fields"))?
    {
        event[k] = v.clone();
    }
    let mut bytes = serde_json::to_vec(&event)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_RECORD {
        return Err(io::Error::other("event record budget"));
    }
    Ok(bytes)
}
pub fn operation(id: OperationId) -> Value {
    json!({"incarnation":id.incarnation().get().to_string(),"epoch":id.epoch().get().to_string(),"owner":id.owner().get().to_string(),"sequence":id.sequence().get().to_string()})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "mqttiumd-log-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn read(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn fast_tallies_and_session_cardinality_are_disjoint_from_ops_kinds() {
        assert!(OPS_KINDS.windows(2).all(|w| w[0] < w[1]));
        assert!(OPS_KINDS.binary_search(&"session_cardinality").is_err());
        for kind in FAST_TALLIES {
            assert!(
                OPS_KINDS.binary_search(&kind).is_err(),
                "{kind} must not appear in OPS_KINDS"
            );
            assert!(fast_tally_index(kind).is_some());
        }
    }
    fn sequence(value: &Value) -> u64 {
        value["seq"].as_str().unwrap().parse().unwrap()
    }

    #[test]
    fn observations_rotate_bounded_files_without_stopping_a_long_lived_product() {
        let directory = Directory::new();
        let path = directory.0.join("events.jsonl");
        let events = Events::open(&path).unwrap();
        let text = "x".repeat(240);
        for index in 0..34_000 {
            events.emit("frame_queued",json!({"details":{"packet_kind":"PUBLISH","effect":index.to_string(),"a":text,"b":text,"c":text,"d":text}})).expect("bounded observations rotate instead of exhausting the product log budget");
        }
        let previous = directory.0.join("events.jsonl.previous");
        assert!(
            previous.is_file(),
            "a previous bounded file preserves rotation evidence"
        );
        assert!(std::fs::metadata(&path).unwrap().len() <= MAX_LOG as u64);
        assert!(std::fs::metadata(&previous).unwrap().len() <= MAX_LOG as u64);
        assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 2);
        let before = read(&previous);
        let current = read(&path);
        assert_eq!(current[0]["kind"], "log_rotated");
        assert_eq!(
            current[0]["details"]["retained_from"],
            sequence(&before[0]).to_string()
        );
        assert_eq!(
            current[0]["details"]["previous_last_seq"],
            sequence(before.last().unwrap()).to_string()
        );
        assert_eq!(
            current[0]["details"]["discarded_through"],
            (sequence(&before[0]) - 1).to_string()
        );
        let all: Vec<_> = before.iter().chain(current.iter()).collect();
        assert!(
            all.windows(2)
                .all(|pair| sequence(pair[1]) == sequence(pair[0]) + 1)
        );
        assert!(
            sequence(before.first().unwrap()) > 1,
            "this witness crosses more than one real 16MiB boundary"
        );
    }

    #[test]
    fn an_oversized_record_neither_rotates_nor_advances_the_event_sequence() {
        let directory = Directory::new();
        let path = directory.0.join("events.jsonl");
        let events = Events::open(&path).unwrap();
        events.emit("starting", json!({})).unwrap();
        assert!(
            events
                .emit(
                    "starting",
                    json!({"details":{"oversized":"x".repeat(MAX_RECORD)}})
                )
                .is_err()
        );
        events.emit("stopped", json!({})).unwrap();
        assert!(!directory.0.join("events.jsonl.previous").exists());
        assert_eq!(read(&path).iter().map(sequence).collect::<Vec<_>>(), [1, 2]);
    }

    #[test]
    fn the_operations_level_keeps_lifecycle_records_and_counts_the_rest() {
        let directory = Directory::new();
        let path = directory.0.join("events.jsonl");
        let events = Events::open_at(&path, Level::Ops).unwrap();
        assert!(events.records("ready"));
        assert!(!events.records("frame_queued"));
        events.emit("ready", json!({})).unwrap();
        for _ in 0..3 {
            events
                .emit("frame_queued", json!({"details":{"packet_kind":"PUBLISH"}}))
                .unwrap();
        }
        events.tally("qos0_dropped");
        events.tally("qos0_dropped");
        events.emit("connection_closed", json!({})).unwrap();
        // Nothing per message was written, and nothing was lost either.
        assert_eq!(
            read(&path)
                .iter()
                .map(|row| row["kind"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>(),
            ["ready", "connection_closed"]
        );
        events.flush_tallies().unwrap();
        let rows = read(&path);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["kind"], "counters");
        assert_eq!(rows[2]["details"]["counts"]["frame_queued"], "3");
        assert_eq!(rows[2]["details"]["counts"]["qos0_dropped"], "2");
        assert!(
            rows.windows(2)
                .all(|pair| sequence(&pair[1]) == sequence(&pair[0]) + 1)
        );
        // An interval without a suppressed record writes nothing.
        events.flush_tallies().unwrap();
        assert_eq!(read(&path).len(), 3);
    }

    #[test]
    fn the_audit_level_writes_every_record() {
        let directory = Directory::new();
        let path = directory.0.join("events.jsonl");
        let events = Events::open(&path).unwrap();
        assert!(events.records("frame_queued"));
        events.emit("frame_queued", json!({})).unwrap();
        events.tally("qos0_dropped");
        events.flush_tallies().unwrap();
        assert_eq!(read(&path).len(), 1);
    }

    #[test]
    fn the_operations_kinds_are_sorted_for_their_lookup() {
        assert!(OPS_KINDS.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn a_level_is_parsed_from_its_name_only() {
        assert_eq!("audit".parse(), Ok(Level::Audit));
        assert_eq!("ops".parse(), Ok(Level::Ops));
        assert!("AUDIT".parse::<Level>().is_err());
    }

    #[test]
    fn consecutive_startups_rotate_prior_journal_while_concurrent_open_is_refused() {
        let directory = Directory::new();
        let path = directory.0.join("events.jsonl");
        let previous = directory.0.join("events.jsonl.previous");

        // First run opens fresh journal and holds an exclusive lock.
        let first = Events::open(&path).unwrap();
        first
            .emit("starting", json!({"details":{"run":1}}))
            .unwrap();

        // A live concurrent broker must not displace the locked journal.
        let concurrent = Events::open(&path);
        assert!(
            concurrent
                .as_ref()
                .is_err_and(|err| err.kind() == io::ErrorKind::WouldBlock),
            "concurrent open on active journal must fail with WouldBlock"
        );
        drop(first);

        // Second consecutive startup on the same journal rotates run 1 to `.previous`.
        let second = Events::open(&path).unwrap();
        second
            .emit("starting", json!({"details":{"run":2}}))
            .unwrap();
        assert!(previous.is_file());
        let prev_rows = read(&previous);
        assert_eq!(prev_rows.len(), 1);
        assert_eq!(prev_rows[0]["details"]["run"], 1);
        let cur_rows = read(&path);
        assert_eq!(cur_rows.len(), 1);
        assert_eq!(cur_rows[0]["details"]["run"], 2);
        assert_eq!(sequence(&cur_rows[0]), 1);
        drop(second);

        // Third consecutive startup replaces `.previous` with run 2 cleanly.
        let third = Events::open(&path).unwrap();
        third
            .emit("starting", json!({"details":{"run":3}}))
            .unwrap();
        let prev_rows = read(&previous);
        assert_eq!(prev_rows[0]["details"]["run"], 2);
        let cur_rows = read(&path);
        assert_eq!(cur_rows[0]["details"]["run"], 3);
    }
}
