//! The store adapter of the bench: what is volatile around a writer.
//!
//! `SimStore` wraps any `DurableStore` and keeps apart the four places a
//! command can be (DEM-013):
//!
//! | Place | Held in |
//! | - | - |
//! | Durable state | the wrapped store |
//! | Transaction in preparation | inside one call of the wrapped store |
//! | Command in transit | `transit`: accepted, not in the writer yet |
//! | Result that is ready | `ready`: produced, not delivered yet |
//!
//! Nothing moves by itself. The scheduler picks one elementary action at
//! a time among `eligible`, each with a stable identity (ADR 0002). The
//! named injection points of `contracts/v1/fault-points.json` sit on the
//! passages between those places, and every fault that is reached
//! records a `FaultHit`.
//!
//! The adapter is also the dispatcher of ADR 0003: it remembers the work
//! it accepted and hands a result over only when it answers one of them.
//! A result that answers nothing in flight is refused and recorded.
//!
//! A crash keeps the wrapped store and the evidence already recorded,
//! and loses everything else.

use std::collections::{BTreeMap, VecDeque};

use mqttiumd_contract::command::{Command, OpenEpoch, OpeningAttempt, Recipe, StartupNonce};
use mqttiumd_contract::effects::{
    Effect, EffectSink, Input, OwnedEffect, RecoveredFact, SubmitBlock, SubmitResult,
};
use mqttiumd_contract::ids::{EffectId, InboundPacketId, OperationId, SessionId};
use mqttiumd_contract::outcome::{EpochOpening, OpeningOutcome, OpeningReply, StoreReply};

use super::durable::{DurableStore, RecoveryCursor, RecoveryPage, RecoveryRefused};

/// A named injection point of the fault catalogue.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum FaultPoint {
    /// The adapter accepted a command that has not entered the writer.
    CommandTransit,
    /// The transaction is committed and no result exists yet.
    CommitApplied,
    /// The result is ready on the store side and not delivered.
    ResultReady,
    /// The result reached the core, which has not finished consuming it.
    ResultConsume,
}

impl FaultPoint {
    /// The name of the point in `contracts/v1/fault-points.json`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CommandTransit => "store.command.transit",
            Self::CommitApplied => "store.commit.applied",
            Self::ResultReady => "store.result.ready",
            Self::ResultConsume => "store.result.consume",
        }
    }

    /// The actions the catalogue allows at this point.
    #[must_use]
    pub const fn actions(self) -> &'static [FaultAction] {
        match self {
            Self::CommandTransit => &[FaultAction::HoldCommand],
            Self::CommitApplied | Self::ResultConsume => &[FaultAction::Barrier],
            Self::ResultReady => &[
                FaultAction::DropResult,
                FaultAction::DelayResult,
                FaultAction::DuplicateResult,
            ],
        }
    }
}

/// What a fault does when it is reached.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FaultAction {
    /// The command stays in transit until it is released.
    HoldCommand,
    /// The adapter stops at the point until it is resumed or crashed.
    Barrier,
    /// The result is lost on its way to the core.
    DropResult,
    /// The result stays ready until it is released.
    DelayResult,
    /// The result is made ready twice.
    DuplicateResult,
}

impl FaultAction {
    /// The name of the action in `contracts/v1/fault-points.json`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::HoldCommand => "hold_command",
            Self::Barrier => "barrier",
            Self::DropResult => "drop_result",
            Self::DelayResult => "delay_result",
            Self::DuplicateResult => "duplicate_result",
        }
    }
}

/// The kind of work the adapter carries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WorkKind {
    AdmitPublication,
    ReleaseInbound,
    ResolveAndFence,
    OpenEpoch,
}

impl WorkKind {
    /// The type name used in traces and in the fault catalogue.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AdmitPublication => "AdmitPublication",
            Self::ReleaseInbound => "ReleaseInbound",
            Self::ResolveAndFence => "ResolveAndFence",
            Self::OpenEpoch => "OpenEpoch",
        }
    }
}

/// The stable identity of one piece of work: the effect that carried
/// it, or the attempt of an opening.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum WorkId {
    Effect(EffectId),
    Opening(OpeningAttempt),
}

/// The nonce of one start-up of the process under test, drawn from the
/// seed of the run and the number of restarts before it. The bench is
/// the trusted code that draws it: two start-ups of one run never share
/// a nonce, and the same run draws the same ones again.
#[must_use]
pub fn startup_nonce(seed: u64, restart: u64) -> StartupNonce {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&seed.to_be_bytes());
    bytes[8..].copy_from_slice(&restart.to_be_bytes());
    StartupNonce::new(bytes)
}

/// What the store answered to one piece of work.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Answer {
    Operation {
        operation: OperationId,
        reply: StoreReply,
    },
    Opening {
        command: OpenEpoch,
        outcome: OpeningOutcome,
    },
}

/// Which passages a fault watches. A passage that every set field
/// matches is counted; the fault is reached at its `occurrence`-th one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FaultSpec {
    pub point: FaultPoint,
    pub action: FaultAction,
    pub kind: Option<WorkKind>,
    /// The inbound exchange slot a command comes from. The bench knows a
    /// client's packet, never an `OperationId` (ADR 0003).
    pub source: Option<(SessionId, InboundPacketId)>,
    /// One-based.
    pub occurrence: u64,
}

/// Index of an armed fault, in arming order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct FaultId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FaultRefused {
    /// The catalogue does not allow this action at this point.
    ActionNotAllowed,
    /// Occurrences are counted from one.
    ZeroOccurrence,
    TableFull,
}

/// Evidence recorded by the adapter, in order. Lot L3 owns the trace
/// records these are written as.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StoreEvent {
    /// The adapter took the work; it is in transit.
    CommandSubmitted {
        id: WorkId,
        kind: WorkKind,
        operation: Option<OperationId>,
    },
    /// The writer committed the effect of this work, durably.
    CommitStable {
        id: WorkId,
        operation: Option<OperationId>,
    },
    /// The writer made a fence durable for this operation.
    FenceStable { id: WorkId, operation: OperationId },
    /// A result exists on the store side.
    ResultReady { id: WorkId, answer: Answer },
    /// The result was handed to the core.
    ResultDelivered { id: WorkId, answer: Answer },
    /// An armed fault was reached and its action applied.
    FaultHit {
        fault: FaultId,
        point: FaultPoint,
        action: FaultAction,
        id: WorkId,
        operation: Option<OperationId>,
    },
    /// A result answered no work in flight. It was refused (ADR 0003).
    UncorrelatedResult {
        id: WorkId,
        operation: Option<OperationId>,
    },
    /// The adapter read one page of the recovery inventory. `page`
    /// counts from zero within one inventory.
    RecoveryPageRead {
        page: u64,
        last: bool,
        facts: Vec<RecoveredFact>,
    },
}

/// One elementary action of the scheduler on the store.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreAction {
    /// Move a command in transit into the writer.
    EnterWriter(WorkId),
    /// Release a command a fault holds in transit.
    ReleaseCommand(WorkId),
    /// Hand a ready result to the dispatcher.
    DeliverResult(WorkId),
    /// Release a result a fault delays.
    ReleaseResult(WorkId),
}

/// A result handed over by the dispatcher.
#[derive(PartialEq, Eq, Debug)]
pub enum Delivery {
    /// The answer to a store effect, as the input the core consumes.
    Store(Input),
    /// The answer to an opening, for `EpochOpening::observe`.
    Opening(OpeningReply),
}

/// What one action did.
#[derive(PartialEq, Eq, Debug)]
pub enum Performed {
    /// The command ran in the writer; its result is ready, or was
    /// dropped by a fault.
    Executed,
    /// A barrier was reached: the adapter is stopped at this point.
    Halted(FaultPoint),
    /// A held command or a delayed result was released.
    Released,
    /// The result was handed over. `barrier` tells that the adapter
    /// stopped right after: the core has not finished consuming it.
    Delivered { delivery: Delivery, barrier: bool },
    /// The result answered nothing in flight; it was refused and
    /// recorded.
    Uncorrelated,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActionRefused {
    /// The adapter is stopped at a barrier.
    Halted,
    /// The action is not among the eligible ones.
    NotEligible,
}

/// The bounds of the adapter.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AdapterLimits {
    /// Commands accepted and not in the writer yet.
    pub operations_in_flight: usize,
    /// Results owed: commands in transit plus results not delivered.
    pub result_slots: usize,
    pub faults: usize,
    /// Events kept until they are taken. Past it, evidence is lost and
    /// the run is inconclusive.
    pub events: usize,
}

enum Work {
    Persist(Command),
    Resolve(OperationId),
    Open(OpenEpoch),
}

struct Transit {
    id: WorkId,
    work: Work,
    held: bool,
}

/// The inbound exchange slot a command comes from.
type Source = Option<(SessionId, InboundPacketId)>;

struct Ready {
    id: WorkId,
    kind: WorkKind,
    source: Source,
    answer: Answer,
    delayed: bool,
}

struct ArmedFault {
    spec: FaultSpec,
    seen: u64,
    hits: u64,
}

/// Where a barrier stopped the adapter, and what is left to do there.
struct Halt {
    point: FaultPoint,
    pending: Option<(WorkId, WorkKind, Source, Answer)>,
}

/// The deterministic store adapter. See the module documentation.
pub struct SimStore<S> {
    store: S,
    limits: AdapterLimits,
    transit: VecDeque<Transit>,
    ready: VecDeque<Ready>,
    /// The work in flight whose result the dispatcher still expects.
    awaited: BTreeMap<WorkId, Option<OperationId>>,
    faults: Vec<ArmedFault>,
    halt: Option<Halt>,
    events: Vec<StoreEvent>,
    evidence_lost: bool,
    /// The number the next page of the current inventory will carry.
    recovery_page: u64,
}

/// What a crash leaves: the store and the evidence already recorded.
pub struct Crashed<S> {
    pub store: S,
    pub events: Vec<StoreEvent>,
    pub evidence_lost: bool,
}

fn describe(work: &Work) -> (WorkKind, Option<OperationId>, Source) {
    match work {
        Work::Persist(command) => match command.recipe.get() {
            Recipe::AdmitPublication(admit) => (
                WorkKind::AdmitPublication,
                Some(admit.operation),
                Some((admit.source.session, admit.source.packet_id)),
            ),
            Recipe::ReleaseInbound(release) => (
                WorkKind::ReleaseInbound,
                Some(release.operation),
                Some((release.source_session, release.packet_id)),
            ),
        },
        Work::Resolve(operation) => (WorkKind::ResolveAndFence, Some(*operation), None),
        Work::Open(_) => (WorkKind::OpenEpoch, None, None),
    }
}

fn operation_of(answer: Answer) -> Option<OperationId> {
    match answer {
        Answer::Operation { operation, .. } => Some(operation),
        Answer::Opening { .. } => None,
    }
}

impl<S: DurableStore> SimStore<S> {
    /// Wraps a store. Nothing is in flight.
    #[must_use]
    pub fn new(store: S, limits: AdapterLimits) -> Self {
        Self {
            store,
            limits,
            transit: VecDeque::new(),
            ready: VecDeque::new(),
            awaited: BTreeMap::new(),
            faults: Vec::new(),
            halt: None,
            events: Vec::new(),
            evidence_lost: false,
            recovery_page: 0,
        }
    }

    /// The wrapped store, for inspection and for the commands that do
    /// not travel as effects (administration, recovery pages).
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The wrapped store, mutably. Fixture set-up and availability
    /// switches go through it; they take their place in the writer's
    /// order like any other step.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Arms a fault. Faults are tried in arming order.
    pub fn arm(&mut self, spec: FaultSpec) -> Result<FaultId, FaultRefused> {
        if !spec.point.actions().contains(&spec.action) {
            return Err(FaultRefused::ActionNotAllowed);
        }
        if spec.occurrence == 0 {
            return Err(FaultRefused::ZeroOccurrence);
        }
        if self.faults.len() >= self.limits.faults {
            return Err(FaultRefused::TableFull);
        }
        self.faults.push(ArmedFault {
            spec,
            seen: 0,
            hits: 0,
        });
        Ok(FaultId(self.faults.len() - 1))
    }

    /// How many times a fault was reached.
    #[must_use]
    pub fn hits(&self, fault: FaultId) -> u64 {
        self.faults.get(fault.0).map_or(0, |armed| armed.hits)
    }

    /// Takes the evidence recorded so far.
    pub fn take_events(&mut self) -> Vec<StoreEvent> {
        core::mem::take(&mut self.events)
    }

    /// Whether an event could not be kept. The run is then inconclusive.
    #[must_use]
    pub fn evidence_lost(&self) -> bool {
        self.evidence_lost
    }

    /// The barrier the adapter is stopped at, if any.
    #[must_use]
    pub fn halted_at(&self) -> Option<FaultPoint> {
        self.halt.as_ref().map(|halt| halt.point)
    }

    fn record(&mut self, event: StoreEvent) {
        if self.events.len() >= self.limits.events {
            self.evidence_lost = true;
        } else {
            self.events.push(event);
        }
    }

    /// Counts one passage at `point` and returns the action of the first
    /// fault reached there. A counter that cannot advance never reaches
    /// its occurrence again; nothing wraps.
    fn pass(
        &mut self,
        point: FaultPoint,
        id: WorkId,
        kind: WorkKind,
        operation: Option<OperationId>,
        source: Source,
    ) -> Option<FaultAction> {
        let mut reached = None;
        for index in 0..self.faults.len() {
            let armed = &mut self.faults[index];
            let spec = armed.spec;
            let matches = spec.point == point
                && spec.kind.is_none_or(|wanted| wanted == kind)
                && spec.source.is_none_or(|wanted| Some(wanted) == source);
            if !matches {
                continue;
            }
            let Some(seen) = armed.seen.checked_add(1) else {
                continue;
            };
            armed.seen = seen;
            if seen == spec.occurrence && reached.is_none() {
                armed.hits = armed.hits.saturating_add(1);
                reached = Some(spec.action);
                self.record(StoreEvent::FaultHit {
                    fault: FaultId(index),
                    point,
                    action: spec.action,
                    id,
                    operation,
                });
            }
        }
        reached
    }

    fn room(&self) -> Result<(), SubmitBlock> {
        if self.halt.is_some() {
            return Err(SubmitBlock::Closed);
        }
        if self.transit.len() >= self.limits.operations_in_flight {
            return Err(SubmitBlock::InFlightLimit);
        }
        let owed = self.transit.len().saturating_add(self.ready.len());
        if owed >= self.limits.result_slots {
            return Err(SubmitBlock::QueueFull);
        }
        Ok(())
    }

    fn accept(&mut self, id: WorkId, work: Work) {
        let (kind, operation, source) = describe(&work);
        self.awaited.insert(id, operation);
        self.record(StoreEvent::CommandSubmitted {
            id,
            kind,
            operation,
        });
        let held = self.pass(FaultPoint::CommandTransit, id, kind, operation, source)
            == Some(FaultAction::HoldCommand);
        self.transit.push_back(Transit { id, work, held });
    }

    /// The opening of this start-up: for the data set behind the
    /// adapter, which start-up verified by reading its ledger, and with
    /// the nonce this start-up drew.
    #[must_use]
    pub fn begin_opening(&self, startup: StartupNonce) -> EpochOpening {
        EpochOpening::new(self.store.ledger().incarnation(), startup)
    }

    /// Submits the opening of an epoch. It travels like a command: in
    /// transit, through the writer, then as a ready result, which
    /// carries the whole command back.
    pub fn submit_opening(&mut self, command: OpenEpoch) -> Result<(), SubmitBlock> {
        self.room()?;
        let id = WorkId::Opening(command.attempt);
        if self.awaited.contains_key(&id) {
            return Err(SubmitBlock::Closed);
        }
        self.accept(id, Work::Open(command));
        Ok(())
    }

    /// The actions the scheduler may choose now, in a fixed order:
    /// commands in transit in their order of acceptance, then results in
    /// their order of readiness. Empty while a barrier holds.
    #[must_use]
    pub fn eligible(&self) -> Vec<StoreAction> {
        if self.halt.is_some() {
            return Vec::new();
        }
        let mut actions = Vec::new();
        for transit in &self.transit {
            actions.push(if transit.held {
                StoreAction::ReleaseCommand(transit.id)
            } else {
                StoreAction::EnterWriter(transit.id)
            });
        }
        let mut seen = Vec::new();
        for ready in &self.ready {
            // Of two copies of one result only the first is eligible.
            if seen.contains(&ready.id) {
                continue;
            }
            seen.push(ready.id);
            actions.push(if ready.delayed {
                StoreAction::ReleaseResult(ready.id)
            } else {
                StoreAction::DeliverResult(ready.id)
            });
        }
        actions
    }

    /// Performs one eligible action.
    pub fn perform(&mut self, action: StoreAction) -> Result<Performed, ActionRefused> {
        if self.halt.is_some() {
            return Err(ActionRefused::Halted);
        }
        if !self.eligible().contains(&action) {
            return Err(ActionRefused::NotEligible);
        }
        match action {
            StoreAction::ReleaseCommand(id) => {
                for transit in &mut self.transit {
                    if transit.id == id {
                        transit.held = false;
                    }
                }
                Ok(Performed::Released)
            }
            StoreAction::ReleaseResult(id) => {
                if let Some(ready) = self.ready.iter_mut().find(|ready| ready.id == id) {
                    ready.delayed = false;
                }
                Ok(Performed::Released)
            }
            StoreAction::EnterWriter(id) => {
                let index = self
                    .transit
                    .iter()
                    .position(|transit| transit.id == id)
                    .ok_or(ActionRefused::NotEligible)?;
                let transit = self
                    .transit
                    .remove(index)
                    .ok_or(ActionRefused::NotEligible)?;
                Ok(self.run(transit))
            }
            StoreAction::DeliverResult(id) => {
                let index = self
                    .ready
                    .iter()
                    .position(|ready| ready.id == id)
                    .ok_or(ActionRefused::NotEligible)?;
                let ready = self.ready.remove(index).ok_or(ActionRefused::NotEligible)?;
                Ok(self.deliver(ready.id, ready.answer, Some((ready.kind, ready.source))))
            }
        }
    }

    /// Runs one command in the writer and makes its result ready.
    fn run(&mut self, transit: Transit) -> Performed {
        let id = transit.id;
        let (kind, operation, source) = describe(&transit.work);
        let (answer, committed_now) = match &transit.work {
            Work::Persist(command) => {
                let applied = self.store.apply(command);
                let operation = command.recipe.operation();
                (
                    Answer::Operation {
                        operation,
                        reply: applied.reply,
                    },
                    applied.committed_now,
                )
            }
            Work::Resolve(operation) => {
                let resolved = self.store.resolve_and_fence(*operation);
                if resolved.fenced_now {
                    self.record(StoreEvent::FenceStable {
                        id,
                        operation: *operation,
                    });
                }
                (
                    Answer::Operation {
                        operation: *operation,
                        reply: StoreReply::Outcome(resolved.outcome),
                    },
                    false,
                )
            }
            Work::Open(command) => {
                let outcome = self.store.open_epoch(command);
                (
                    Answer::Opening {
                        command: *command,
                        outcome,
                    },
                    matches!(outcome, OpeningOutcome::Committed(_)),
                )
            }
        };
        if committed_now {
            self.record(StoreEvent::CommitStable { id, operation });
            if self.pass(FaultPoint::CommitApplied, id, kind, operation, source)
                == Some(FaultAction::Barrier)
            {
                self.halt = Some(Halt {
                    point: FaultPoint::CommitApplied,
                    pending: Some((id, kind, source, answer)),
                });
                return Performed::Halted(FaultPoint::CommitApplied);
            }
        }
        self.make_ready(id, kind, source, answer);
        Performed::Executed
    }

    fn make_ready(&mut self, id: WorkId, kind: WorkKind, source: Source, answer: Answer) {
        self.record(StoreEvent::ResultReady { id, answer });
        let operation = operation_of(answer);
        let ready = |delayed| Ready {
            id,
            kind,
            source,
            answer,
            delayed,
        };
        match self.pass(FaultPoint::ResultReady, id, kind, operation, source) {
            Some(FaultAction::DropResult) => {
                // The result is gone: nothing will ever answer this work.
                self.awaited.remove(&id);
            }
            Some(FaultAction::DelayResult) => self.ready.push_back(ready(true)),
            Some(FaultAction::DuplicateResult) => {
                self.ready.push_back(ready(false));
                self.ready.push_back(ready(false));
            }
            Some(FaultAction::HoldCommand | FaultAction::Barrier) | None => {
                self.ready.push_back(ready(false));
            }
        }
    }

    /// The dispatcher: hands a result over when it answers work in
    /// flight, for the operation that work named. Otherwise refuses it
    /// and records the refusal. `origin` is known for a result of this
    /// adapter's own pipeline.
    fn deliver(
        &mut self,
        id: WorkId,
        answer: Answer,
        origin: Option<(WorkKind, Source)>,
    ) -> Performed {
        let operation = operation_of(answer);
        let delivery = match (id, answer) {
            (WorkId::Effect(effect), Answer::Operation { operation, reply }) => {
                Some(Delivery::Store(Input::StoreResult {
                    effect,
                    operation,
                    reply,
                }))
            }
            (WorkId::Opening(attempt), Answer::Opening { command, outcome })
                if command.attempt == attempt =>
            {
                Some(Delivery::Opening(OpeningReply { command, outcome }))
            }
            (WorkId::Effect(_) | WorkId::Opening(_), Answer::Opening { .. })
            | (WorkId::Opening(_), Answer::Operation { .. }) => None,
        };
        let (Some(delivery), true) = (delivery, self.awaited.get(&id) == Some(&operation)) else {
            self.record(StoreEvent::UncorrelatedResult { id, operation });
            return Performed::Uncorrelated;
        };
        self.awaited.remove(&id);
        self.record(StoreEvent::ResultDelivered { id, answer });
        let barrier = origin.is_some_and(|(kind, source)| {
            self.pass(FaultPoint::ResultConsume, id, kind, operation, source)
                == Some(FaultAction::Barrier)
        });
        if barrier {
            self.halt = Some(Halt {
                point: FaultPoint::ResultConsume,
                pending: None,
            });
        }
        Performed::Delivered { delivery, barrier }
    }

    /// Presents a result that did not come out of this adapter's own
    /// pipeline. It goes through the same dispatcher: it is handed over
    /// only when it answers work in flight (ADR 0003). An identity the
    /// bench remembered answers nothing and is refused here.
    pub fn present_result(
        &mut self,
        effect: EffectId,
        operation: OperationId,
        reply: StoreReply,
    ) -> Performed {
        self.deliver(
            WorkId::Effect(effect),
            Answer::Operation { operation, reply },
            None,
        )
    }

    /// Reads one page of the recovery inventory through the adapter,
    /// which is the only builder of recovery facts (ADR 0003).
    pub fn recover(
        &mut self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        let page = self.store.recover(cursor, limit)?;
        let number = match cursor {
            None => 0,
            Some(_) => self.recovery_page,
        };
        self.recovery_page = number.saturating_add(1);
        self.record(StoreEvent::RecoveryPageRead {
            page: number,
            last: page.next.is_none(),
            facts: page.facts.as_slice().to_vec(),
        });
        Ok(page)
    }

    /// Leaves a barrier and goes on from the point it stopped at.
    pub fn resume(&mut self) -> Option<FaultPoint> {
        let halt = self.halt.take()?;
        if let Some((id, kind, source, answer)) = halt.pending {
            self.make_ready(id, kind, source, answer);
        }
        Some(halt.point)
    }

    /// Crashes the process around the store. Commands in transit, ready
    /// results, correlations, armed faults and the barrier are lost; the
    /// store and the evidence already recorded remain.
    #[must_use]
    pub fn crash(self) -> Crashed<S> {
        Crashed {
            store: self.store,
            events: self.events,
            evidence_lost: self.evidence_lost,
        }
    }
}

impl<S: DurableStore> EffectSink for SimStore<S> {
    /// Takes `Persist` and `ResolveAndFence`. Any other effect belongs
    /// to another adapter and comes back untouched, as from a closed
    /// sink; so does an effect whose identity is already in flight.
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let store_effect = matches!(
            effect.effect(),
            Effect::Persist { .. } | Effect::ResolveAndFence { .. }
        );
        let id = WorkId::Effect(effect.id());
        if !store_effect || self.awaited.contains_key(&id) {
            return SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::Closed,
            };
        }
        if let Err(reason) = self.room() {
            return SubmitResult::NotSubmitted { effect, reason };
        }
        let (effect_id, body) = effect.into_parts();
        let work = match body {
            Effect::Persist { command } => Work::Persist(command),
            Effect::ResolveAndFence { operation } => Work::Resolve(operation),
            // Excluded by `store_effect` above; nothing was accepted.
            other => {
                return SubmitResult::NotSubmitted {
                    effect: OwnedEffect::new(effect_id, other),
                    reason: SubmitBlock::Closed,
                };
            }
        };
        self.accept(id, work);
        SubmitResult::Accepted
    }
}
