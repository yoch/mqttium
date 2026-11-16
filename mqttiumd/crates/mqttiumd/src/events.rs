//! Bounded product observations, independent of fixture-based campaign verdicts.
use mqttiumd_contract::ids::OperationId;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

const MAX_RECORD: usize = 2048;
const MAX_LOG: usize = 16 * 1024 * 1024;
#[derive(Clone)]
pub struct Events(Arc<Mutex<Log>>);
struct Log {
    file: File,
    seq: u64,
    bytes: usize,
}
impl Events {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self(Arc::new(Mutex::new(Log {
            file,
            seq: 0,
            bytes: 0,
        }))))
    }
    pub fn emit(&self, kind: &str, fields: Value) -> io::Result<()> {
        let mut log = self.0.lock().map_err(|_| io::Error::other("event lock"))?;
        let seq = log
            .seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("event counter"))?;
        let mut event = json!({"format":"mqttiumd.product-event","schema_version":1,"seq":seq.to_string(),"kind":kind,"pid":std::process::id()});
        for (k, v) in fields
            .as_object()
            .ok_or_else(|| io::Error::other("event fields"))?
        {
            event[k] = v.clone();
        }
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.push(b'\n');
        let total = log
            .bytes
            .checked_add(bytes.len())
            .filter(|v| *v <= MAX_LOG)
            .ok_or_else(|| io::Error::other("event budget"))?;
        if bytes.len() > MAX_RECORD {
            return Err(io::Error::other("event record budget"));
        }
        log.file.write_all(&bytes)?;
        // A barrier is acknowledged only after its observation is outside the broker.
        if kind == "barrier_hit" {
            log.file.sync_data()?;
        }
        log.seq = seq;
        log.bytes = total;
        Ok(())
    }
}
pub fn operation(id: OperationId) -> Value {
    json!({"incarnation":id.incarnation().get().to_string(),"epoch":id.epoch().get().to_string(),"owner":id.owner().get().to_string(),"sequence":id.sequence().get().to_string()})
}
