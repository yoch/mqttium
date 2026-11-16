//! Bounded product observations, independent of fixture-based campaign verdicts.
use mqttiumd_contract::ids::OperationId;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAX_RECORD: usize = 2048;
const MAX_LOG: usize = 16 * 1024 * 1024;
#[derive(Clone)]
pub struct Events(Arc<Mutex<Log>>);
struct Log {
    file: File,
    path: PathBuf,
    previous: PathBuf,
    seq: u64,
    bytes: usize,
    first_seq: u64,
}
impl Events {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut name = path.as_os_str().to_owned();
        name.push(".previous");
        let previous = PathBuf::from(name);
        if previous.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous event file exists",
            ));
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self(Arc::new(Mutex::new(Log {
            file,
            path: path.to_owned(),
            previous,
            seq: 0,
            bytes: 0,
            first_seq: 1,
        }))))
    }
    pub fn emit(&self, kind: &str, fields: Value) -> io::Result<()> {
        let mut log = self.0.lock().map_err(|_| io::Error::other("event lock"))?;
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
}
