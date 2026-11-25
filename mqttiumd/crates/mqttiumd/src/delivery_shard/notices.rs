//! A close is a terminal decision for one live connection generation. Reserving
//! its slot at attachment lets any shard report a close while the coordinator
//! is busy sending to another bounded lane. A fatal error has its own slot.
use super::ShardToCoord;
use crate::wake::Wake;
use crate::writer::Failure;
use mqttiumd_contract::ids::ConnectionGeneration;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

#[derive(Default)]
struct State {
    /// True once this generation has requested its close. Keep the first
    /// reason, including while the coordinator flushes the DISCONNECT.
    requested: BTreeMap<ConnectionGeneration, bool>,
    closes: Vec<ShardToCoord>,
    fatal: Option<Failure>,
}

pub(super) struct Notices {
    state: Mutex<State>,
    pending: AtomicBool,
    wake: Wake,
}

impl Notices {
    pub(super) fn new(wake: Wake) -> Self {
        Self {
            state: Mutex::default(),
            pending: AtomicBool::new(false),
            wake,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn attach(&self, generation: ConnectionGeneration) {
        self.lock().requested.entry(generation).or_insert(false);
    }

    pub(super) fn detach(&self, generation: ConnectionGeneration) {
        let mut state = self.lock();
        if state.requested.remove(&generation) == Some(true) {
            state.closes.retain(|notice| {
                !matches!(notice,
                    ShardToCoord::CloseConnection { generation: g, .. } if *g == generation
                )
            });
        }
    }

    pub(super) fn publish(&self, notice: ShardToCoord) {
        let mut state = self.lock();
        match notice {
            ShardToCoord::CloseConnection { generation, .. } => {
                let Some(requested) = state.requested.get_mut(&generation) else {
                    return; // The peer has already closed.
                };
                if *requested {
                    return;
                }
                *requested = true;
                state.closes.push(notice);
            }
            ShardToCoord::Fatal(failure) => {
                state.fatal.get_or_insert(failure);
            }
        }
        self.pending.store(true, Ordering::Release);
        drop(state);
        self.wake.wake();
    }

    pub(super) fn drain_into(&self, out: &mut Vec<ShardToCoord>) -> Option<Failure> {
        if !self.pending.load(Ordering::Acquire) {
            return None;
        }
        let mut state = self.lock();
        out.append(&mut state.closes);
        self.pending.store(false, Ordering::Release);
        state.fatal.take()
    }
}
