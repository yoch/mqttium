//! Exclusive SQLite worker. A closed socket never cancels an accepted command.
use crate::events::{Events, operation};
use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::{Recipe, StartupNonce};
use mqttiumd_contract::effects::{Effect, OwnedEffect};
use mqttiumd_contract::ids::{EffectId, OperationId, Revision, SessionId, StoreIncarnation};
use mqttiumd_contract::outcome::{
    EpochGrant, EpochOpening, OpeningReply, OpeningStage, StoreReply,
};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_store_api::{
    AdminCommand, AdminDone, DurableStore, Limits, ReadError, ReadPhase, RecoveryCursor,
    RecoveryPage, RecoveryRefused, SessionState,
};
use mqttiumd_store_sqlite::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender},
};
use std::thread::{self, JoinHandle};
#[cfg(feature = "test-hooks")]
use std::time::{Duration, Instant};

pub const QUEUE: usize = 32;
pub const SESSIONS: usize = 128;
pub const MAX_CLIENT: usize = 1024;
pub const MAX_TOPIC: usize = 1024;
pub const MAX_PAYLOAD: usize = 32768;
pub fn limits() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
        references: References::new(128),
        exchange_slots: ExchangeSlots::new(128),
        receipt_slots: ReceiptSlots::new(8192),
        max_targets_per_admission: 1,
        max_topic_bytes: MAX_TOPIC,
        max_payload_bytes: MAX_PAYLOAD,
        max_sessions: SESSIONS,
        max_route_targets: 1,
    }
}
#[derive(Clone, Debug)]
pub struct Failure {
    pub code: &'static str,
    pub read: Option<(ReadError, ReadPhase)>,
}
impl Failure {
    pub fn plain(code: &'static str) -> Self {
        Self { code, read: None }
    }
    pub fn read(error: ReadError, phase: ReadPhase) -> Self {
        Self {
            code: "durable_read",
            read: Some((error, phase)),
        }
    }
    pub fn fields(&self) -> serde_json::Value {
        match self.read {
            Some((cause, phase)) => {
                json!({"cause":cause.code(),"phase":phase.code(),"details":{"code":self.code}})
            }
            None => json!({"details":{"code":self.code}}),
        }
    }
}
#[derive(Clone, Debug)]
pub struct Session {
    pub id: SessionId,
    pub revision: Revision,
    pub client: SharedBytes,
    pub present: bool,
}
pub struct Boot {
    pub grant: EpochGrant,
    pub sessions: BTreeMap<Vec<u8>, Session>,
}
pub enum Request {
    Recovery(Option<RecoveryCursor>),
    Session { request: u64, client: SharedBytes },
    Effect(OwnedEffect),
    Stop,
}
pub enum Response {
    Recovery(Result<RecoveryPage, RecoveryRefused>),
    Session {
        request: u64,
        result: Result<Session, Failure>,
    },
    Store {
        effect: EffectId,
        operation: OperationId,
        reply: StoreReply,
    },
    Stopped,
}
#[derive(Clone, Default)]
pub struct Hooks {
    pub directory: Option<PathBuf>,
    pub pause: Vec<String>,
}
impl Hooks {
    #[cfg(feature = "test-hooks")]
    fn barrier(
        &self,
        point: &'static str,
        kind: &'static str,
        id: OperationId,
        events: &Events,
        cancel: &AtomicBool,
    ) -> Result<(), Failure> {
        if !self.pause.iter().any(|p| p == point) {
            return Ok(());
        }
        let dir = self
            .directory
            .as_ref()
            .ok_or_else(|| Failure::plain("hook_directory"))?;
        events
            .emit(
                "barrier_hit",
                json!({"point":point,"command_kind":kind,"operation":operation(id)}),
            )
            .map_err(|_| Failure::plain("events_unavailable"))?;
        let release = dir.join(format!("release-{point}"));
        let start = Instant::now();
        while !cancel.load(Ordering::Relaxed) {
            if release.exists() {
                std::fs::remove_file(release).map_err(|_| Failure::plain("hook_release"))?;
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(30) {
                return Err(Failure::plain("hook_deadline"));
            }
            thread::sleep(Duration::from_millis(2));
        }
        // Cancellation releases a submitted command. It never means certain rejection.
        Ok(())
    }
}
pub struct Writer {
    pub sender: SyncSender<Request>,
    pub receiver: Receiver<Response>,
    pub boot: Receiver<Result<Boot, Failure>>,
    handle: Option<JoinHandle<()>>,
}
impl Writer {
    pub fn spawn(
        path: PathBuf,
        events: Events,
        hooks: Hooks,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, Failure> {
        let _ = &hooks;
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        let (rtx, rrx) = mpsc::sync_channel(64);
        let (btx, brx) = mpsc::sync_channel(1);
        let handle=thread::Builder::new().name("sqlite-writer".into()).spawn(move||{
            let (mut store,boot)=match bootstrap(&path) {Ok(v)=>v,Err(e)=>{let _=btx.send(Err(e));return;}};
            if btx.send(Ok(boot)).is_err(){return;}
            while let Ok(request)=rx.recv(){
                let response=match request {
                    Request::Stop=>{drop(store);let _=rtx.send(Response::Stopped);return;}
                    Request::Recovery(cursor)=>Response::Recovery(store.recover(cursor,64)),
                    Request::Session{request,client}=>Response::Session{request,result:session(&mut store,client)},
                    Request::Effect(effect)=>{
                        let (effect,work)=effect.into_parts();
                        match work {
                            Effect::Persist{command}=>{
                                let id=command.recipe.operation();
                                let kind=match command.recipe.get(){Recipe::AdmitPublication(_)=>"Admission",Recipe::ReleaseInbound(_)=>"Release"};
                                #[cfg(feature="test-hooks")]
                                {
                                    let hc=hooks.clone();let ev=events.clone();let stop=cancel.clone();
                                    let point=if kind=="Admission"{"admission.prepared"}else{"release.prepared"};
                                    // The hook is inside the actual transaction, after all writes and before COMMIT.
                                    let hook=Box::new(move|p|{if p==mqttiumd_store_sqlite::kill_points::KillPoint::CommitPrepared && hc.barrier(point,kind,id,&ev,&stop).is_err(){stop.store(true,Ordering::Relaxed);}});
                                    if store.set_kill_hook(Some(hook)).is_err(){cancel.store(true,Ordering::Relaxed);}
                                }
                                let applied=store.apply(&command);
                                #[cfg(feature="test-hooks")]
                                {
                                    let _=store.set_kill_hook(None);
                                    if applied.committed_now && kind=="Admission" && hooks.barrier("admission.committed",kind,id,&events,&cancel).is_err(){cancel.store(true,Ordering::Relaxed);}
                                }
                                if events.emit("store_result",json!({"operation":operation(id),"command_kind":kind,"details":{"committed_now":applied.committed_now,"wrote":applied.wrote,"outcome":outcome_name(applied.reply)}})).is_err(){cancel.store(true,Ordering::Relaxed);}
                                Response::Store{effect,operation:id,reply:applied.reply}
                            }
                            Effect::ResolveAndFence{operation}=>Response::Store{effect,operation,reply:StoreReply::Outcome(store.resolve_and_fence(operation).outcome)},
                            _=>{cancel.store(true,Ordering::Relaxed);return;}
                        }
                    }
                };
                // At most QUEUE + max in-flight results. The receiver outlives orderly drain.
                if rtx.send(response).is_err(){return;}
            }
        }).map_err(|_|Failure::plain("writer_spawn"))?;
        Ok(Self {
            sender: tx,
            receiver: rrx,
            boot: brx,
            handle: Some(handle),
        })
    }
    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(|h| h.is_finished())
    }
    pub fn join_finished(&mut self) -> Result<(), Failure> {
        if self.is_finished() {
            if let Some(h) = self.handle.take() {
                h.join().map_err(|_| Failure::plain("writer_panicked"))?;
            }
            Ok(())
        } else {
            Err(Failure::plain("writer_not_finished"))
        }
    }
}
fn bootstrap(path: &Path) -> Result<(SqliteStore, Boot), Failure> {
    // Existing files, including empty/truncated files, are never silently initialised.
    let mut store = if path
        .try_exists()
        .map_err(|e| Failure::read(io_cause(e.kind()), ReadPhase::Open))?
    {
        SqliteStore::open(path)
    } else {
        let mut random = [0u8; 8];
        getrandom::fill(&mut random).map_err(|_| Failure::plain("random_unavailable"))?;
        let raw = u64::from_be_bytes(random).max(1);
        SqliteStore::create(path, StoreIncarnation::new(raw), limits())
    }
    .map_err(|e| Failure::read(e.read_error(), ReadPhase::Open))?;
    let state = store
        .snapshot()
        .map_err(|e| Failure::read(e, ReadPhase::Snapshot))?;
    if state.limits != limits() || !state.routes.is_empty() {
        return Err(Failure::plain("durable_profile_mismatch"));
    }
    let mut sessions = BTreeMap::new();
    for (id, s) in &state.sessions {
        if s.state != SessionState::Active {
            continue;
        }
        let client = SharedBytes::copy_from(&s.client_id, MAX_CLIENT)
            .map_err(|_| Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot))?;
        if sessions.len() >= SESSIONS {
            return Err(Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot));
        }
        sessions.insert(
            s.client_id.clone(),
            Session {
                id: *id,
                revision: s.revision,
                client,
                present: true,
            },
        );
    }
    let ledger = store
        .ledger()
        .map_err(|e| Failure::read(e, ReadPhase::Ledger))?;
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(|_| Failure::plain("random_unavailable"))?;
    let mut opening = EpochOpening::new(ledger.incarnation(), StartupNonce::new(nonce));
    let command = opening
        .submit(ledger.last_opened())
        .map_err(|_| Failure::plain("opening_submit"))?;
    let outcome = store
        .open_epoch(&command)
        .map_err(|e| Failure::read(e, ReadPhase::Opening))?;
    let _ = opening
        .observe(OpeningReply { command, outcome })
        .map_err(|_| Failure::plain("opening_reply"))?;
    if !matches!(opening.stage(), OpeningStage::Committed(_)) {
        return Err(Failure::plain("opening_unknown"));
    }
    let grant = opening
        .hand_over()
        .map_err(|_| Failure::plain("opening_grant"))?;
    Ok((store, Boot { grant, sessions }))
}
fn session(store: &mut SqliteStore, client: SharedBytes) -> Result<Session, Failure> {
    let state = store
        .snapshot()
        .map_err(|e| Failure::read(e, ReadPhase::Lookup))?;
    if let Some((id, s)) = state
        .sessions
        .iter()
        .find(|(_, s)| s.state == SessionState::Active && s.client_id == client.as_slice())
    {
        return Ok(Session {
            id: *id,
            revision: s.revision,
            client,
            present: true,
        });
    }
    match store.admin(AdminCommand::CreateSession {
        client_id: client.as_slice().to_vec(),
    }) {
        Ok(AdminDone::SessionCreated { session, revision }) => Ok(Session {
            id: session,
            revision,
            client,
            present: false,
        }),
        // WriteFailed has an unknown outcome. No retry and no success acknowledgement.
        Err(mqttiumd_store_api::AdminRefused::WriteFailed) => {
            Err(Failure::plain("session_creation_unknown"))
        }
        _ => Err(Failure::plain("session_creation_refused")),
    }
}
fn outcome_name(reply: StoreReply) -> &'static str {
    use mqttiumd_contract::outcome::StoreOutcome::*;
    match reply {
        StoreReply::Outcome(Committed(_)) => "Committed",
        StoreReply::Outcome(RejectedCertain(_)) => "RejectedCertain",
        StoreReply::Outcome(PendingOrUnknown) => "PendingOrUnknown",
        StoreReply::Outcome(RetiredOutcomeUnavailable) => "RetiredOutcomeUnavailable",
        StoreReply::Violation(_) => "Violation",
    }
}

fn io_cause(kind: std::io::ErrorKind) -> ReadError {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::PermissionDenied | ErrorKind::WouldBlock | ErrorKind::NotFound => {
            ReadError::Unavailable
        }
        ErrorKind::OutOfMemory => ReadError::AllocationFailed,
        _ => ReadError::Io,
    }
}
