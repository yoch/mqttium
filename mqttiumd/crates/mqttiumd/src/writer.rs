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
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptCollection, ReceiptCollectionReply,
};
use mqttiumd_store_api::{
    AdminCommand, AdminDone, DurableStore, Limits, ReadError, ReadPhase, RecoveryCursor,
    RecoveryPage, RecoveryRefused, SessionState, WorkPage,
};
use mqttiumd_store_sqlite::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
#[cfg(feature = "test-hooks")]
use std::time::Instant;

pub const QUEUE: usize = 32;
pub const CONTROL_QUEUE: usize = 4;
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
        max_targets_per_admission: 16,
        max_topic_bytes: MAX_TOPIC,
        max_payload_bytes: MAX_PAYLOAD,
        max_sessions: SESSIONS,
        max_route_targets: 16,
    }
}
pub fn recipe_kind(recipe: &Recipe) -> &'static str {
    match recipe {
        Recipe::AdmitPublication(_) => "Admission",
        Recipe::ReleaseInbound(_) => "Release",
        Recipe::ChangeSubscriptions(_) => "Subscriptions",
        Recipe::ClaimDelivery(_) => "Claim",
        Recipe::FinishDelivery(_) => "Finish",
        Recipe::ConsumeDelivery(_) => "Consume",
        Recipe::UpdateSubscriptions(_) => "Subscriptions",
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
    pub collect_closed: bool,
    pub sqlite: mqttiumd_store_sqlite::Settings,
}
pub enum Request {
    Recovery(Option<RecoveryCursor>),
    Session { request: u64, client: SharedBytes },
    Effect(OwnedEffect),
    Stop,
}
/// A reserved lane: maintenance never waits for a network/business slot.
pub enum ControlRequest {
    Collect {
        effect: EffectId,
        collection: ReceiptCollection,
    },
    Closed(ClosedReceiptCollection),
}
#[derive(Clone)]
pub enum ControlResponse {
    Collected {
        effect: EffectId,
        collection: ReceiptCollection,
        result: Result<ReceiptCollectionReply, Failure>,
    },
    Closed {
        collection: ClosedReceiptCollection,
        result: Result<ClosedReceiptReply, Failure>,
    },
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
        targets: Vec<SessionId>,
    },
    Work {
        effect: EffectId,
        session: SessionId,
        result: Result<WorkPage, ReadError>,
    },
    Stopped,
}
pub fn collection_details(collection: ReceiptCollection) -> serde_json::Value {
    let closure = collection.closure;
    json!({"incarnation":closure.origin.incarnation.get().to_string(),"epoch":closure.origin.epoch.get().to_string(),"owner":closure.origin.owner.get().to_string(),"through":closure.through.get().to_string(),"publication_through":closure.publication_through.get().to_string(),"payload_through":closure.payload_through.get().to_string(),"limit":collection.limit})
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
        self.barrier_fields(
            point,
            json!({"point":point,"command_kind":kind,"operation":operation(id)}),
            events,
            cancel,
        )
    }
    #[cfg(feature = "test-hooks")]
    fn barrier_fields(
        &self,
        point: &'static str,
        fields: serde_json::Value,
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
            .emit("barrier_hit", fields)
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
    pub control_sender: SyncSender<ControlRequest>,
    pub control_receiver: Receiver<ControlResponse>,
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
        let (ctx, crx) = mpsc::sync_channel(CONTROL_QUEUE);
        let (cctx, ccrx) = mpsc::sync_channel(CONTROL_QUEUE);
        let handle = thread::Builder::new()
            .name("sqlite-writer".into())
            .spawn(move || {
                let (mut store, boot) = match bootstrap(&path) {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = btx.send(Err(error));
                        return;
                    }
                };
                if btx.send(Ok(boot)).is_err() {
                    return;
                }
                let mut pending = None;
                let mut pending_control = None;
                let mut stopping = false;
                let mut state = BusinessState::default();
                #[cfg(feature="test-hooks")]
                let mut duplicate_control: Option<ControlResponse> = None;
                loop {
                    if let Some(reply) = pending_control.take() {
                        match cctx.try_send(reply) {
                            Ok(()) => {}
                            Err(TrySendError::Full(reply)) => pending_control = Some(reply),
                            Err(TrySendError::Disconnected(_)) => return,
                        }
                    }
                    if let Some(reply) = pending.take() {
                        match rtx.try_send(reply) {
                            Ok(()) if stopping => return,
                            Ok(()) => {}
                            Err(TrySendError::Full(reply)) => pending = Some(reply),
                            Err(TrySendError::Disconnected(_)) => return,
                        }
                    }
                    // Even a completely unread ordinary result lane cannot prevent
                    // a reserved receipt closure from committing and replying.
                    #[cfg(feature="test-hooks")]
                    if pending_control.is_none() { pending_control = duplicate_control.take(); }
                    if pending_control.is_none() {
                        match crx.try_recv() {
                            Ok(request) => {
                                let reply = control_response(&mut store, request, &events, &hooks, &cancel);
                                #[cfg(feature="test-hooks")]
                                if let ControlResponse::Collected { collection, result:Ok(ReceiptCollectionReply::Applied { .. }),.. } = &reply
                                    && hooks.directory.as_ref().is_some_and(|d| consume_file(&d.join("duplicate-close-result"))) {
                                    let _ = events.emit("receipt_collection_result_duplicated",json!({"command_kind":"ReceiptCollection","details":collection_details(*collection)}));
                                    duplicate_control = Some(reply.clone());
                                }
                                pending_control = Some(reply);
                            }
                            Err(TryRecvError::Empty) => {}
                            Err(TryRecvError::Disconnected) => return,
                        }
                    }
                    if pending.is_none() && !stopping {
                        match rx.recv_timeout(Duration::from_millis(1)) {
                            Ok(Request::Stop) => {
                                pending = Some(Response::Stopped);
                                stopping = true;
                            }
                            Ok(request) => {
                                pending = Some(business_response(
                                    &mut store, request, &events, &hooks, &cancel, &mut state,
                                ))
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => {}
                            Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        }
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .map_err(|_| Failure::plain("writer_spawn"))?;
        Ok(Self {
            sender: tx,
            receiver: rrx,
            control_sender: ctx,
            control_receiver: ccrx,
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
#[derive(Default)]
struct BusinessState {
    #[cfg(feature = "test-hooks")]
    held_admission: Option<OperationId>,
    #[cfg(feature = "test-hooks")]
    captured: Option<mqttiumd_contract::command::Command>,
}
fn business_response(
    store: &mut SqliteStore,
    request: Request,
    events: &Events,
    hooks: &Hooks,
    cancel: &Arc<AtomicBool>,
    state: &mut BusinessState,
) -> Response {
    let _ = (hooks, cancel, &state);
    match request {
        Request::Stop => Response::Stopped,
        Request::Recovery(cursor) => Response::Recovery(store.recover(cursor, 64)),
        Request::Session { request, client } => Response::Session {
            request,
            result: session(store, client),
        },
        Request::Effect(effect) => {
            let (effect, work) = effect.into_parts();
            match work {
                Effect::Persist { command } => {
                    let id = command.recipe.operation();
                    let kind = recipe_kind(command.recipe.get());
                    let targets = match command.recipe.get() {
                        Recipe::AdmitPublication(a) => {
                            a.targets.as_slice().iter().map(|t| t.session).collect()
                        }
                        _ => Vec::new(),
                    };
                    #[cfg(feature = "test-hooks")]
                    {
                        let hc = hooks.clone();
                        let ev = events.clone();
                        let stop = cancel.clone();
                        let point = match kind {
                            "Admission" => "admission.prepared",
                            "Release" => "release.prepared",
                            "Subscriptions" => "subscription.prepared",
                            "Claim" => "claim.prepared",
                            "Consume" => "consume.prepared",
                            _ => "finish.prepared",
                        };
                        // The hook is inside the actual transaction, after all writes and before COMMIT.
                        let hook = Box::new(move |p| {
                            if p == mqttiumd_store_sqlite::kill_points::KillPoint::CommitPrepared
                                && hc.barrier(point, kind, id, &ev, &stop).is_err()
                            {
                                stop.store(true, Ordering::Relaxed);
                            }
                        });
                        if store.set_kill_hook(Some(hook)).is_err() {
                            cancel.store(true, Ordering::Relaxed);
                        }
                    }
                    #[cfg(feature = "test-hooks")]
                    if kind == "Admission"
                        && hooks
                            .directory
                            .as_ref()
                            .is_some_and(|d| consume_file(&d.join("capture-next-admission")))
                    {
                        state.captured = Some(command.clone());
                        if let Recipe::AdmitPublication(a) = command.recipe.get() {
                            let _ = events.emit("receipt_replay_captured",json!({"command_kind":"Admission","operation":operation(id),"details":{"publication":identity_text(a.publication.incarnation().get(),a.publication.epoch().get(),a.publication.owner().get(),a.publication.sequence().get()),"payload":identity_text(a.payload.id.incarnation().get(),a.payload.id.epoch().get(),a.payload.id.owner().get(),a.payload.id.sequence().get())}}));
                        }
                    }
                    let applied = store.apply(&command);
                    #[cfg(feature = "test-hooks")]
                    {
                        let _ = store.set_kill_hook(None);
                        let point = match kind {
                            "Admission" => Some("admission.committed"),
                            "Claim" => Some("claim.committed"),
                            "Finish" => Some("finish.committed"),
                            "Consume" => Some("consume.committed"),
                            _ => None,
                        };
                        if applied.committed_now
                            && let Some(point) = point
                            && hooks.barrier(point, kind, id, &events, &cancel).is_err()
                        {
                            cancel.store(true, Ordering::Relaxed);
                        }
                    }
                    if events.emit("store_result",json!({"operation":operation(id),"command_kind":kind,"details":{"committed_now":applied.committed_now,"wrote":applied.wrote,"outcome":outcome_name(applied.reply)}})).is_err(){cancel.store(true,Ordering::Relaxed);}
                    #[cfg(not(feature = "test-hooks"))]
                    let reply = applied.reply;
                    #[cfg(feature = "test-hooks")]
                    let mut reply = applied.reply;
                    #[cfg(feature = "test-hooks")]
                    if applied.committed_now
                        && matches!(kind, "Claim" | "Finish" | "Consume")
                        && let Some(dir) = &hooks.directory
                    {
                        let file =
                            dir.join(format!("drop-next-{}-result", kind.to_ascii_lowercase()));
                        if file.exists() && std::fs::remove_file(file).is_ok() {
                            let _ = events.emit(
                                "store_result_dropped",
                                json!({"operation":operation(id),"command_kind":kind}),
                            );
                            reply = StoreReply::Outcome(
                                mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown,
                            );
                        }
                    }
                    #[cfg(feature = "test-hooks")]
                    let reply = hold_admission_reply(state, hooks, id, kind, reply, events);
                    Response::Store {
                        effect,
                        operation: id,
                        reply,
                        targets,
                    }
                }
                Effect::ResolveAndFence { operation: id } => {
                    let reply = StoreReply::Outcome(store.resolve_and_fence(id).outcome);
                    #[cfg(feature = "test-hooks")]
                    let reply = hold_admission_reply(state, hooks, id, "Resolution", reply, events);
                    if events.emit("store_resolved",json!({"operation":operation(id),"details":{"outcome":outcome_name(reply).to_ascii_lowercase()}})).is_err(){cancel.store(true,Ordering::Relaxed);}
                    Response::Store {
                        effect,
                        operation: id,
                        reply,
                        targets: Vec::new(),
                    }
                }
                Effect::LoadWork {
                    session,
                    after,
                    limit,
                } => Response::Work {
                    effect,
                    session,
                    result: store.read_work(session, after, limit as usize),
                },
                _ => {
                    cancel.store(true, Ordering::Relaxed);
                    Response::Stopped
                }
            }
        }
    }
}

#[cfg(feature = "test-hooks")]
fn hold_admission_reply(
    state: &mut BusinessState,
    hooks: &Hooks,
    id: OperationId,
    kind: &str,
    reply: StoreReply,
    events: &Events,
) -> StoreReply {
    let hold = hooks
        .directory
        .as_ref()
        .is_some_and(|d| d.join("hold-admission-result").exists());
    if !hold {
        state.held_admission = None;
        return reply;
    }
    if kind == "Admission"
        && state.held_admission.is_none()
        && matches!(
            reply,
            StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::Committed(_))
        )
    {
        state.held_admission = Some(id);
    }
    if state.held_admission == Some(id) {
        let _ = events.emit("store_result_held",json!({"operation":operation(id),"command_kind":"Admission","details":{"outcome":outcome_name(reply),"source":kind}}));
        StoreReply::Outcome(mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown)
    } else {
        reply
    }
}
#[cfg(feature = "test-hooks")]
fn identity_text(incarnation: u64, epoch: u64, owner: u64, sequence: u64) -> String {
    format!("{incarnation}/{epoch}/{owner}/{sequence}")
}

fn control_response(
    store: &mut SqliteStore,
    request: ControlRequest,
    events: &Events,
    hooks: &Hooks,
    cancel: &Arc<AtomicBool>,
) -> ControlResponse {
    let _ = hooks;
    match request {
        ControlRequest::Closed(collection) => ControlResponse::Closed {
            collection,
            result: store
                .collect_closed_receipts(collection)
                .map_err(|e| Failure::read(e, ReadPhase::Maintenance)),
        },
        ControlRequest::Collect { effect, collection } => {
            #[cfg(not(feature = "test-hooks"))]
            let effective = collection;
            #[cfg(feature = "test-hooks")]
            let effective = ReceiptCollection {
                limit: if hooks
                    .directory
                    .as_ref()
                    .is_some_and(|d| d.join("receipt-collection-limit-one").exists())
                {
                    1
                } else {
                    collection.limit
                },
                ..collection
            };
            #[cfg(feature = "test-hooks")]
            {
                let hc = hooks.clone();
                let ev = events.clone();
                let stop = cancel.clone();
                store.set_receipt_hook(Some(Box::new(move |point, current| {
                    let mut details = collection_details(current);
                    details["requested_limit"] = json!(collection.limit);
                    if current.closure.through.get()!=0 && hc.barrier_fields(point.name(), json!({"point":point.name(),"command_kind":"ReceiptCollection","details":details}), &ev, &stop).is_err() { stop.store(true, Ordering::Relaxed); }
                })));
            }
            let result = store
                .collect_receipts(effective)
                .map_err(|e| Failure::read(e, ReadPhase::Maintenance));
            #[cfg(feature = "test-hooks")]
            store.set_receipt_hook(None);
            let mut details = collection_details(effective);
            details["requested_limit"] = json!(collection.limit);
            details["effect"] = json!(format!(
                "{}/{}/{}",
                effect.epoch.get(),
                effect.owner.get(),
                effect.sequence.get()
            ));
            match result {
                Ok(ReceiptCollectionReply::Applied {
                    removed,
                    retained,
                    more,
                    ..
                }) => {
                    details["outcome"] = json!("Applied");
                    details["removed"] = json!(removed);
                    details["retained"] = json!(retained.to_string());
                    details["more"] = json!(more);
                }
                Ok(ReceiptCollectionReply::PendingOrUnknown) => {
                    details["outcome"] = json!("PendingOrUnknown")
                }
                Ok(ReceiptCollectionReply::Refused(reason)) => {
                    details["outcome"] = json!("Refused");
                    details["reason"] = json!(format!("{reason:?}"));
                }
                Err(ref failure) => {
                    details["outcome"] = json!("ReadError");
                    if let Some((cause, _)) = failure.read {
                        details["cause"] = json!(cause.code());
                    }
                }
            }
            if events
                .emit(
                    "receipt_collection_result",
                    json!({"command_kind":"ReceiptCollection","details":details}),
                )
                .is_err()
            {
                cancel.store(true, Ordering::Relaxed);
            }
            #[cfg(feature = "test-hooks")]
            let result = if matches!(result, Ok(ReceiptCollectionReply::Applied { .. }))
                && hooks
                    .directory
                    .as_ref()
                    .is_some_and(|d| consume_file(&d.join("drop-next-close-result")))
            {
                let _ = events.emit(
                    "receipt_collection_result_dropped",
                    json!({"command_kind":"ReceiptCollection","details":details}),
                );
                Ok(ReceiptCollectionReply::PendingOrUnknown)
            } else {
                result
            };
            ControlResponse::Collected {
                effect,
                collection,
                result,
            }
        }
    }
}
#[cfg(feature = "test-hooks")]
fn consume_file(path: &Path) -> bool {
    path.exists() && std::fs::remove_file(path).is_ok()
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
    if state.limits != limits() {
        return Err(Failure::plain("durable_profile_mismatch"));
    }
    let route_count = state
        .routes
        .values()
        .try_fold(0usize, |n, targets| n.checked_add(targets.len()))
        .ok_or_else(|| Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot))?;
    if route_count > 128 {
        return Err(Failure::read(ReadError::LimitExceeded, ReadPhase::Snapshot));
    }
    if state
        .routes
        .values()
        .flatten()
        .any(|t| t.qos == mqttiumd_contract::packet::Qos::ExactlyOnce)
        || state
            .obligations
            .values()
            .any(|o| o.qos == mqttiumd_contract::packet::Qos::ExactlyOnce)
    {
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
    let collect_closed =
        state.receipt_frontiers.len() >= 32 || state.accounts.receipt_slots.get() >= 4096;
    let sqlite = store.effective_settings().clone();
    Ok((
        store,
        Boot {
            grant,
            sessions,
            collect_closed,
            sqlite,
        },
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use mqttiumd_contract::ids::{EffectSeq, LogicalOwnerId};
    use mqttiumd_contract::receipts::{ReceiptClosure, ReceiptOrigin};
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn reserved_collection_progresses_while_ordinary_replies_are_unread() {
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-control-lane-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let db = path.join("store.sqlite");
        let events = Events::open(&path.join("events.jsonl")).unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut writer =
            Writer::spawn(db.clone(), events, Hooks::default(), cancel.clone()).unwrap();
        let boot = writer
            .boot
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        let origin = ReceiptOrigin {
            incarnation: boot.grant.incarnation(),
            epoch: boot.grant.epoch(),
            owner: LogicalOwnerId::new(0),
        };
        // Exactly 64 ordinary reply slots are left unread. The 65th command
        // still has a real durable session effect, so an independent snapshot
        // establishes the worker reached the blocked reply, rather than merely
        // assuming it did from elapsed time.
        for request in 1..=65 {
            writer
                .sender
                .send(Request::Session {
                    request,
                    client: SharedBytes::copy_from(
                        format!("lane-{request}").as_bytes(),
                        MAX_CLIENT,
                    )
                    .unwrap(),
                })
                .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while SqliteStore::inspect(&db).unwrap().sessions.len() != 65 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let established = SqliteStore::inspect(&db).unwrap().sessions.len() == 65;
        let collection = ReceiptCollection {
            closure: ReceiptClosure::empty(origin),
            limit: 128,
        };
        let effect = EffectId {
            epoch: origin.epoch,
            owner: origin.owner,
            sequence: EffectSeq::new(1),
        };
        writer
            .control_sender
            .send(ControlRequest::Collect { effect, collection })
            .unwrap();
        let reply = writer.control_receiver.recv_timeout(Duration::from_secs(2));
        // Cleanup before asserting: even the original blocked implementation
        // must release its writer and terminate when the semantic test fails.
        cancel.store(true, Ordering::Relaxed);
        while writer.receiver.try_recv().is_ok() {}
        writer.sender.send(Request::Stop).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while !writer.is_finished() && Instant::now() < deadline {
            while writer.receiver.try_recv().is_ok() {}
            while writer.control_receiver.try_recv().is_ok() {}
            thread::sleep(Duration::from_millis(1));
        }
        writer.join_finished().unwrap();
        let _ = std::fs::remove_dir_all(path);
        assert!(
            established,
            "all65 ordinary commands actually completed while their replies stayed unread"
        );
        assert!(
            matches!(reply,Ok(ControlResponse::Collected { effect:e,collection:c,result:Ok(ReceiptCollectionReply::Applied { removed:0,.. }) }) if e==effect && c==collection),
            "reserved maintenance must reply even when the ordinary result lane is demonstrably full"
        );
    }
}
