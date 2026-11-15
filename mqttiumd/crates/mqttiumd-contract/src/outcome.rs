//! The four store outcomes and monotone knowledge (DEM-010).
//!
//! An observed absence is not a certain rejection: a command may still sit in
//! a queue while a read finds no receipt. Only a durable fence ordered with
//! the writer turns "absent" into `RejectedCertain`.
//!
//! # Opening an epoch and handing over the creation authority (ADR 0009)
//!
//! Start-up verifies the data set, reads the last epoch opened in it,
//! submits `OpenEpoch` and the store answers with an `OpeningReply`. That
//! an epoch was opened is a durable fact, and a fact can be read any
//! number of times: reading it authorises nothing. What authorises the
//! creation of owners is the `EpochGrant`, and a grant comes out of
//! exactly one transition of the volatile `EpochOpening`:
//!
//! ```text
//! Idle --submit--> Submitted --Committed--> Committed --hand_over--> HandedOver
//!                   |  |  \--RejectedCertain--> Rejected --submit (unless definitive)--> Submitted
//!                   |  \--Unknown (store)--\
//!                   \--abandon (deadline)--+--> Unknown --submit--> Submitted (new ticket)
//!
//! any stage with a terminal result --a contradicting terminal result--> Faulted
//! ```
//!
//! - A reply is honoured only when it echoes the ticket of the attempt in
//!   flight, and a committed reply only when it names the data set this
//!   opening was created for and the epoch right after the one its
//!   command expected. A reply delivered twice changes nothing.
//! - `hand_over` succeeds once, from `Committed`. The grant is moved to
//!   the creator of owners and is neither `Clone` nor `Copy`.
//! - An unknown outcome gives no grant, whether the store reported it or
//!   start-up stopped waiting (`abandon`). Start-up does not proceed on
//!   an opening it assumes: it reads the last opened epoch again and
//!   submits a new attempt on that condition. Reading the value to form a
//!   condition adopts nothing: the grant still comes from the committed
//!   reply to the new attempt.
//! - An older attempt can never undo a newer one. The store applies an
//!   `OpenEpoch` only while the last opened epoch is the one the command
//!   expected (`EpochLedger::open`), so an attempt that was held on its
//!   way and arrives after a later opening committed is certainly
//!   rejected. Without that condition the epoch of a grant already handed
//!   over could be closed by a command the process had given up on.
//! - Two terminal results that disagree for one attempt stop start-up:
//!   the opening is `Faulted`, for good, and neither `hand_over` nor
//!   `submit` passes any more.
//! - No function takes a recovered fact or a recovery page and returns a
//!   grant; `check_recovered` compares a fact and borrows immutably.
//! - When the process dies, this state and the grant die with it. The
//!   next process opens the next epoch (DEM-012); it never rebuilds an
//!   authority in the older one.
//!
//! Trusted code, named as ADR 0009 requires. `EpochOpening::hand_over`
//! is the one function that constructs an `EpochGrant`, and consuming a
//! grant is the one way lot L1's `OwnerAuthority` is constructed.
//! `EpochOpening::observe` is the entry point between the store adapter
//! and this crate: a reply is plain data, and the type system does not
//! prevent code from building one. What is established here is that a
//! given `EpochOpening` yields at most one grant, for the epoch that
//! follows its own condition in its own data set, after a committed
//! reply to its own attempt. That each committed opening names an epoch
//! no earlier opening named is the store's part (`EpochLedger`). That a
//! process drives one `EpochOpening` per store it opened, and that a
//! reply reaches the `EpochOpening` that submitted its command, is the
//! wiring of lot L4; this type cannot tell two instances apart.
//!
//! A limit, stated: a grant that was handed over cannot be taken back.
//! When a contradiction arrives afterwards the opening becomes `Faulted`
//! and says that a grant is out; stopping its holder is start-up's.

use crate::capacity::Resource;
use crate::command::{OpenEpoch, OpeningTicket};
use crate::ids::{ExchangeId, Exhausted, PublicationId, RunEpoch, StoreIncarnation};

/// The durable facts established by a committed command.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommitFacts {
    /// Publication, obligations, inbound QoS 2 state, accounts and receipt
    /// were committed together.
    PublicationAdmitted {
        publication: PublicationId,
        exchange: ExchangeId,
    },
    /// The inbound QoS 2 exchange was durably terminated after PUBREL.
    InboundReleased { exchange: ExchangeId },
}

/// Why a command was certainly not executed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RejectReason {
    /// A guard on identities or revisions failed; nothing was admitted.
    PreconditionFailed,
    /// A durable capacity was insufficient; nothing was admitted.
    CapacityExceeded(Resource),
    /// A `ResolveAndFence` closed the identity before the command arrived.
    Fenced,
    /// The identity belongs to a run epoch that is closed for execution.
    EpochClosed,
}

/// What the store reports about one `OperationId`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreOutcome {
    /// The durable effect and its result are established for this recipe.
    Committed(CommitFacts),
    /// No effect of this operation was committed and any later execution of
    /// this identity is forbidden. It stays rejected after quota is freed.
    RejectedCertain(RejectReason),
    /// The observations do not allow a conclusion.
    PendingOrUnknown,
    /// The identity can never execute; the detail of its historical result
    /// is no longer available. This is not a rejection of what it once did.
    RetiredOutcomeUnavailable,
}

/// An internal violation reported instead of an outcome. It has no effect.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreViolation {
    /// The same `OperationId` was presented with a different recipe.
    RecipeMismatch,
}

/// The store's answer to one effect.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreReply {
    Outcome(StoreOutcome),
    Violation(StoreViolation),
}

/// What an owner knows about one operation. Knowledge only grows:
/// `Unknown`, then `Retired`, then exactly one of the two terminal results.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Knowledge {
    /// Nothing conclusive was observed yet.
    Unknown,
    /// The identity can no longer execute; its result is not known here.
    Retired,
    /// The operation committed these facts.
    Committed(CommitFacts),
    /// The operation certainly did not execute.
    RejectedCertain(RejectReason),
}

/// Two terminal results disagree for one recipe: an internal fault that
/// stops the owner with a diagnostic.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ContradictoryOutcome {
    pub established: Knowledge,
    pub reported: StoreOutcome,
}

impl Knowledge {
    /// Whether a commit or a certain rejection is established.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed(_) | Self::RejectedCertain(_))
    }

    /// Whether this identity may no longer be executed.
    ///
    /// Only `Unknown` leaves execution possible. `Retired` and
    /// `RejectedCertain` both forbid it, and a committed operation is never
    /// executed a second time. `Retired` forbids execution without saying
    /// what the operation once did: it is not a rejection.
    #[must_use]
    pub const fn forbids_execution(self) -> bool {
        !matches!(self, Self::Unknown)
    }

    /// Merges a newly delivered outcome into what is already known.
    ///
    /// A late `PendingOrUnknown` never replaces a terminal result, an answer
    /// after collection reopens nothing, a repeated terminal result changes
    /// nothing, and two different terminal results are a fault.
    pub fn observe(self, reported: StoreOutcome) -> Result<Self, ContradictoryOutcome> {
        let conflict = ContradictoryOutcome {
            established: self,
            reported,
        };
        match (self, reported) {
            (known, StoreOutcome::PendingOrUnknown) => Ok(known),
            (Self::Unknown, StoreOutcome::RetiredOutcomeUnavailable) => Ok(Self::Retired),
            (known, StoreOutcome::RetiredOutcomeUnavailable) => Ok(known),
            (Self::Unknown | Self::Retired, StoreOutcome::Committed(facts)) => {
                Ok(Self::Committed(facts))
            }
            (Self::Unknown | Self::Retired, StoreOutcome::RejectedCertain(reason)) => {
                Ok(Self::RejectedCertain(reason))
            }
            (Self::Committed(known), StoreOutcome::Committed(facts)) if known == facts => Ok(self),
            // Both say "never executed"; the first recorded reason is kept.
            (Self::RejectedCertain(_), StoreOutcome::RejectedCertain(_)) => Ok(self),
            (Self::Committed(_), StoreOutcome::Committed(_))
            | (Self::Committed(_), StoreOutcome::RejectedCertain(_))
            | (Self::RejectedCertain(_), StoreOutcome::Committed(_)) => Err(conflict),
        }
    }
}

/// The durable fact established by a committed opening: the data set and
/// the epoch now open in it. Every older epoch is closed for execution.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EpochOpened {
    pub incarnation: StoreIncarnation,
    pub epoch: RunEpoch,
}

/// Why an opening was certainly not committed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningRejection {
    /// The epoch counter of the data set reached its end. Nothing wraps,
    /// and no later attempt can succeed.
    EpochsExhausted,
    /// The command names another data set than the one the store holds.
    ForeignIncarnation,
    /// The last opened epoch is not the one the command expected: an
    /// opening was committed since start-up read it. A new attempt on
    /// the value read again may succeed.
    PredecessorMoved,
}

impl OpeningRejection {
    /// Whether no later attempt of the same opening can succeed. It is a
    /// property of the reason, not of having been rejected.
    #[must_use]
    pub const fn is_definitive(self) -> bool {
        match self {
            Self::EpochsExhausted | Self::ForeignIncarnation => true,
            Self::PredecessorMoved => false,
        }
    }
}

/// What the store reports about one attempt to open an epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningOutcome {
    /// The opening is durable: this epoch is open, the older ones closed.
    Committed(EpochOpened),
    /// No epoch was opened by this attempt, and none ever will be.
    RejectedCertain(OpeningRejection),
    /// The observations do not allow a conclusion: the attempt may have
    /// opened an epoch. This is a result of its own, not a rejection.
    Unknown,
}

/// The store's answer to one `OpenEpoch`, echoing its ticket.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpeningReply {
    pub ticket: OpeningTicket,
    pub outcome: OpeningOutcome,
}

/// Where an `EpochOpening` stands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningStage {
    /// Nothing was submitted.
    Idle,
    /// An attempt is in flight; its reply is awaited.
    Submitted,
    /// The outcome of the last attempt is unknown: the store said so, or
    /// start-up stopped waiting. No grant exists.
    Unknown,
    /// The last attempt was certainly not committed. The first reason
    /// reported is kept.
    Rejected(OpeningRejection),
    /// The committed result arrived; the grant was not handed over yet.
    Committed(EpochOpened),
    /// The grant of this opening was handed over. It never is again.
    HandedOver(EpochOpened),
    /// Two terminal results disagreed for one attempt. Terminal: no
    /// grant, no new attempt. `handed_over` names the opening whose grant
    /// had already left when the contradiction arrived, if any.
    Faulted { handed_over: Option<EpochOpened> },
}

/// Why `submit` produced no command. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubmitRefused {
    /// An attempt is already in flight; one is outstanding at a time.
    /// `abandon` it first to stop waiting for it.
    AwaitingReply,
    /// This process already holds a committed opening. Opening again
    /// would close the epoch its own owners live in.
    AlreadyOpened,
    /// The store answered with a reason that no new attempt can change.
    Rejected(OpeningRejection),
    /// The opening is faulted.
    Faulted,
    /// The value read is lower than one this opening already read. The
    /// last opened epoch of a data set never decreases: this is not the
    /// same data set, or not a read of it.
    LedgerRegressed,
    /// The ticket counter reached its end; nothing wraps.
    TicketsExhausted,
}

/// Why a reply was refused. The caller records it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplyRefused {
    /// The reply answers no attempt in flight here: nothing was
    /// submitted, or its ticket is not the one of the current attempt
    /// (ADR 0003). Nothing changed.
    Uncorrelated { ticket: OpeningTicket },
    /// The reply carries the current ticket and a committed opening that
    /// cannot answer the current command: another data set, or another
    /// epoch than the one after the command's condition. It belongs to
    /// some other opening. Nothing changed.
    Mismatched { opened: EpochOpened },
    /// Two terminal results disagree for one attempt: an internal fault.
    /// The opening is now `Faulted`.
    Contradictory {
        established: OpeningStage,
        reported: OpeningOutcome,
    },
    /// The opening is faulted and takes no reply. Nothing changed.
    Faulted,
}

/// Why no grant was handed over. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HandOverRefused {
    /// No committed result is held by this opening.
    NotCommitted(OpeningStage),
    /// The one grant of this opening was already handed over.
    AlreadyHandedOver,
}

/// `abandon` was called while no attempt was awaiting its reply.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NotAwaiting(pub OpeningStage);

/// What a correlated reply did to the opening.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Observed {
    /// The reply taught something: the opening is now at this stage.
    Advanced(OpeningStage),
    /// The reply repeats what is known, or is a late `Unknown` that
    /// lowers nothing. The stage did not move.
    Unchanged,
}

/// How a recovered `Epoch` fact relates to an opening. Reading a fact
/// never moves the opening and never yields a grant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveredEpochCheck {
    /// The fact describes the epoch this process opened.
    Confirms,
    /// The fact describes another data set or another epoch than the one
    /// this process opened: the inventory is not the one of this opening.
    Foreign,
    /// This process holds no committed opening. Whatever the fact says,
    /// it establishes nothing here.
    NotEstablished,
}

/// The permit to create the owners of one opened epoch.
///
/// It is neither `Clone` nor `Copy` and its field is private: the only
/// function that constructs one is `EpochOpening::hand_over`. It is moved
/// to the creator of owners, never lent, and it is consumed by the
/// construction of the creation authority (ADR 0009). Dropping it loses
/// the authority of this opening for good.
#[must_use]
#[derive(PartialEq, Eq, Debug)]
pub struct EpochGrant {
    opened: EpochOpened,
}

impl EpochGrant {
    /// The opening this grant belongs to.
    #[must_use]
    pub const fn opened(&self) -> EpochOpened {
        self.opened
    }

    /// The data set of the opened epoch.
    #[must_use]
    pub const fn incarnation(&self) -> StoreIncarnation {
        self.opened.incarnation
    }

    /// The opened epoch.
    #[must_use]
    pub const fn epoch(&self) -> RunEpoch {
        self.opened.epoch
    }
}

/// The volatile state that controls one opening of an epoch and the
/// hand-over of its grant. One per store opened by a process; it is not
/// `Clone`, since a copy taken after the commit could hand over twice.
#[derive(Debug)]
pub struct EpochOpening {
    /// The data set start-up verified before it opened anything.
    incarnation: StoreIncarnation,
    /// The ticket of the last attempt; zero before the first one.
    ticket: OpeningTicket,
    /// The condition of the last attempt: the last opened epoch that
    /// start-up read. It never decreases.
    after: RunEpoch,
    stage: OpeningStage,
}

impl EpochOpening {
    /// An opening on which nothing was submitted, for the data set
    /// start-up verified. A committed reply that names another data set
    /// is never honoured.
    #[must_use]
    pub const fn new(incarnation: StoreIncarnation) -> Self {
        Self {
            incarnation,
            ticket: OpeningTicket::new(0),
            after: RunEpoch::new(0),
            stage: OpeningStage::Idle,
        }
    }

    /// Where this opening stands.
    #[must_use]
    pub const fn stage(&self) -> OpeningStage {
        self.stage
    }

    /// The data set this opening was created for.
    #[must_use]
    pub const fn incarnation(&self) -> StoreIncarnation {
        self.incarnation
    }

    /// Starts an attempt and returns its command: open the epoch after
    /// `last_opened`, the value start-up just read from the store, under
    /// a ticket no earlier attempt of this opening used.
    ///
    /// After an unknown outcome, or a rejection that is not definitive,
    /// this is the way forward, with the value read again. The command
    /// commits only while `last_opened` is still the last opened epoch,
    /// so the abandoned attempt and the new one cannot both commit, and
    /// whichever arrives after a later opening is rejected.
    pub fn submit(&mut self, last_opened: RunEpoch) -> Result<OpenEpoch, SubmitRefused> {
        match self.stage {
            OpeningStage::Idle | OpeningStage::Unknown => {}
            OpeningStage::Rejected(reason) if !reason.is_definitive() => {}
            OpeningStage::Rejected(reason) => return Err(SubmitRefused::Rejected(reason)),
            OpeningStage::Submitted => return Err(SubmitRefused::AwaitingReply),
            OpeningStage::Committed(_) | OpeningStage::HandedOver(_) => {
                return Err(SubmitRefused::AlreadyOpened);
            }
            OpeningStage::Faulted { .. } => return Err(SubmitRefused::Faulted),
        }
        if last_opened < self.after {
            return Err(SubmitRefused::LedgerRegressed);
        }
        let ticket = self
            .ticket
            .checked_next()
            .map_err(|Exhausted| SubmitRefused::TicketsExhausted)?;
        self.ticket = ticket;
        self.after = last_opened;
        self.stage = OpeningStage::Submitted;
        Ok(OpenEpoch {
            ticket,
            incarnation: self.incarnation,
            after: last_opened,
        })
    }

    /// Stops waiting for the reply of the attempt in flight: start-up's
    /// deadline passed. This is the process giving up, not the store
    /// reporting; the outcome is unknown either way, and what may follow
    /// is the same. The reply of that attempt is still honoured if it
    /// arrives before a new attempt is submitted.
    pub fn abandon(&mut self) -> Result<(), NotAwaiting> {
        if self.stage != OpeningStage::Submitted {
            return Err(NotAwaiting(self.stage));
        }
        self.stage = OpeningStage::Unknown;
        Ok(())
    }

    /// Whether a committed opening can be the answer to the attempt in
    /// flight: this data set, and the epoch right after its condition.
    /// Epoch 0 never is, and no epoch below or beyond the next one.
    fn answers_the_attempt(&self, opened: EpochOpened) -> bool {
        opened.incarnation == self.incarnation && Ok(opened.epoch) == self.after.checked_next()
    }

    /// Takes the store's reply to the attempt in flight.
    ///
    /// Knowledge only grows: `Submitted`, then `Unknown`, then one
    /// terminal result. A repeated reply and a late `Unknown` change
    /// nothing, and a rejection repeated with another reason keeps the
    /// first one, as `Knowledge::observe` does. A reply to another
    /// ticket, or a committed reply that cannot answer the command, is
    /// refused. A commit after a rejection, or a rejection after a
    /// commit, is a fault that stops the opening for good.
    pub fn observe(&mut self, reply: OpeningReply) -> Result<Observed, ReplyRefused> {
        if let OpeningStage::Faulted { .. } = self.stage {
            return Err(ReplyRefused::Faulted);
        }
        if self.stage == OpeningStage::Idle || reply.ticket != self.ticket {
            return Err(ReplyRefused::Uncorrelated {
                ticket: reply.ticket,
            });
        }
        if let OpeningOutcome::Committed(opened) = reply.outcome
            && !self.answers_the_attempt(opened)
        {
            return Err(ReplyRefused::Mismatched { opened });
        }
        let handed_over = match (self.stage, reply.outcome) {
            (OpeningStage::Submitted, OpeningOutcome::Unknown) => {
                return Ok(self.advance(OpeningStage::Unknown));
            }
            (_, OpeningOutcome::Unknown) => return Ok(Observed::Unchanged),
            (
                OpeningStage::Submitted | OpeningStage::Unknown,
                OpeningOutcome::Committed(opened),
            ) => return Ok(self.advance(OpeningStage::Committed(opened))),
            (
                OpeningStage::Submitted | OpeningStage::Unknown,
                OpeningOutcome::RejectedCertain(reason),
            ) => return Ok(self.advance(OpeningStage::Rejected(reason))),
            (
                OpeningStage::Committed(known) | OpeningStage::HandedOver(known),
                OpeningOutcome::Committed(opened),
            ) if known == opened => return Ok(Observed::Unchanged),
            // Both say that nothing was opened; the first reason stays.
            (OpeningStage::Rejected(_), OpeningOutcome::RejectedCertain(_)) => {
                return Ok(Observed::Unchanged);
            }
            (OpeningStage::HandedOver(known), _) => Some(known),
            (
                OpeningStage::Idle
                | OpeningStage::Committed(_)
                | OpeningStage::Rejected(_)
                | OpeningStage::Faulted { .. },
                OpeningOutcome::Committed(_) | OpeningOutcome::RejectedCertain(_),
            ) => None,
        };
        let contradiction = ReplyRefused::Contradictory {
            established: self.stage,
            reported: reply.outcome,
        };
        self.stage = OpeningStage::Faulted { handed_over };
        Err(contradiction)
    }

    fn advance(&mut self, stage: OpeningStage) -> Observed {
        self.stage = stage;
        Observed::Advanced(stage)
    }

    /// Hands over the grant of the committed opening. This transition is
    /// taken once: the stage moves to `HandedOver` and no later call, no
    /// repeated reply and no recovered fact produces another grant.
    pub fn hand_over(&mut self) -> Result<EpochGrant, HandOverRefused> {
        match self.stage {
            OpeningStage::Committed(opened) => {
                self.stage = OpeningStage::HandedOver(opened);
                Ok(EpochGrant { opened })
            }
            OpeningStage::HandedOver(_) => Err(HandOverRefused::AlreadyHandedOver),
            stage => Err(HandOverRefused::NotCommitted(stage)),
        }
    }

    /// Compares a recovered `Epoch` fact with this opening. It borrows
    /// immutably: a fact read in a recovery page, however often, moves no
    /// stage and authorises nothing.
    #[must_use]
    pub fn check_recovered(
        &self,
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    ) -> RecoveredEpochCheck {
        match self.stage {
            OpeningStage::Committed(opened) | OpeningStage::HandedOver(opened) => {
                if opened == (EpochOpened { incarnation, epoch }) {
                    RecoveredEpochCheck::Confirms
                } else {
                    RecoveredEpochCheck::Foreign
                }
            }
            OpeningStage::Idle
            | OpeningStage::Submitted
            | OpeningStage::Unknown
            | OpeningStage::Rejected(_)
            | OpeningStage::Faulted { .. } => RecoveredEpochCheck::NotEstablished,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{
        ExchangeGeneration, LogicalOwnerId, PublicationSeq, RunEpoch, SessionId, StoreIncarnation,
    };

    fn facts(sequence: u64) -> CommitFacts {
        CommitFacts::PublicationAdmitted {
            publication: PublicationId::new(
                StoreIncarnation::new(1),
                RunEpoch::new(1),
                LogicalOwnerId::new(0),
                PublicationSeq::new(sequence),
            ),
            exchange: ExchangeId {
                session: SessionId::new(1),
                generation: ExchangeGeneration::new(1),
            },
        }
    }

    const REJECTED: StoreOutcome = StoreOutcome::RejectedCertain(RejectReason::Fenced);

    fn rank(knowledge: Knowledge) -> u8 {
        match knowledge {
            Knowledge::Unknown => 0,
            Knowledge::Retired => 1,
            Knowledge::Committed(_) | Knowledge::RejectedCertain(_) => 2,
        }
    }

    fn all_knowledge() -> [Knowledge; 5] {
        [
            Knowledge::Unknown,
            Knowledge::Retired,
            Knowledge::Committed(facts(1)),
            Knowledge::Committed(facts(2)),
            Knowledge::RejectedCertain(RejectReason::Fenced),
        ]
    }

    fn all_outcomes() -> [StoreOutcome; 6] {
        [
            StoreOutcome::PendingOrUnknown,
            StoreOutcome::RetiredOutcomeUnavailable,
            StoreOutcome::Committed(facts(1)),
            StoreOutcome::Committed(facts(2)),
            REJECTED,
            StoreOutcome::RejectedCertain(RejectReason::PreconditionFailed),
        ]
    }

    #[test]
    fn absence_of_a_conclusion_is_not_a_rejection() {
        let knowledge = Knowledge::Unknown
            .observe(StoreOutcome::PendingOrUnknown)
            .expect("no conflict");
        assert_eq!(knowledge, Knowledge::Unknown);
        assert!(!knowledge.is_terminal());
    }

    #[test]
    fn late_unknown_does_not_replace_a_terminal_result() {
        let committed = Knowledge::Committed(facts(1));
        assert_eq!(
            committed.observe(StoreOutcome::PendingOrUnknown),
            Ok(committed)
        );
        let rejected = Knowledge::RejectedCertain(RejectReason::Fenced);
        assert_eq!(
            rejected.observe(StoreOutcome::PendingOrUnknown),
            Ok(rejected)
        );
    }

    #[test]
    fn an_answer_after_collection_reopens_nothing() {
        let committed = Knowledge::Committed(facts(1));
        assert_eq!(
            committed.observe(StoreOutcome::RetiredOutcomeUnavailable),
            Ok(committed)
        );
        assert_eq!(
            Knowledge::Unknown.observe(StoreOutcome::RetiredOutcomeUnavailable),
            Ok(Knowledge::Retired)
        );
        assert!(!Knowledge::Retired.is_terminal());
    }

    #[test]
    fn every_knowledge_but_unknown_forbids_execution() {
        assert!(!Knowledge::Unknown.forbids_execution());
        assert!(Knowledge::Retired.forbids_execution());
        assert!(Knowledge::RejectedCertain(RejectReason::Fenced).forbids_execution());
        assert!(Knowledge::Committed(facts(1)).forbids_execution());
        // Retired forbids execution and still is not a terminal result.
        assert!(!Knowledge::Retired.is_terminal());
        // Once execution is forbidden, no later outcome allows it again.
        for knowledge in all_knowledge() {
            for outcome in all_outcomes() {
                if let Ok(next) = knowledge.observe(outcome) {
                    assert!(!knowledge.forbids_execution() || next.forbids_execution());
                }
            }
        }
    }

    #[test]
    fn contradictory_terminal_results_are_a_fault() {
        let committed = Knowledge::Committed(facts(1));
        assert_eq!(
            committed.observe(REJECTED),
            Err(ContradictoryOutcome {
                established: committed,
                reported: REJECTED
            })
        );
        let rejected = Knowledge::RejectedCertain(RejectReason::Fenced);
        assert!(rejected.observe(StoreOutcome::Committed(facts(1))).is_err());
        assert!(
            committed
                .observe(StoreOutcome::Committed(facts(2)))
                .is_err()
        );
    }

    #[test]
    fn a_rejection_stays_rejected_whatever_reason_is_repeated() {
        let rejected =
            Knowledge::RejectedCertain(RejectReason::CapacityExceeded(Resource::PayloadBytes));
        assert_eq!(
            rejected.observe(StoreOutcome::RejectedCertain(RejectReason::EpochClosed)),
            Ok(rejected)
        );
    }

    #[test]
    fn observation_is_monotone_and_idempotent_over_the_whole_table() {
        for knowledge in all_knowledge() {
            for outcome in all_outcomes() {
                let Ok(next) = knowledge.observe(outcome) else {
                    assert!(knowledge.is_terminal(), "only terminal knowledge conflicts");
                    continue;
                };
                assert!(rank(next) >= rank(knowledge), "knowledge regressed");
                if knowledge.is_terminal() {
                    assert_eq!(next, knowledge, "terminal knowledge changed");
                }
                assert_eq!(next.observe(outcome), Ok(next), "not idempotent");
            }
        }
    }

    const DATA_SET: StoreIncarnation = StoreIncarnation::new(1);

    fn opened(epoch: u64) -> EpochOpened {
        EpochOpened {
            incarnation: DATA_SET,
            epoch: RunEpoch::new(epoch),
        }
    }

    fn reply(command: OpenEpoch, outcome: OpeningOutcome) -> OpeningReply {
        OpeningReply {
            ticket: command.ticket,
            outcome,
        }
    }

    /// An opening whose first attempt, made after reading epoch 3, is in
    /// flight. Its committed answer is epoch 4.
    fn submitted() -> (EpochOpening, OpenEpoch) {
        let mut opening = EpochOpening::new(DATA_SET);
        let command = opening.submit(RunEpoch::new(3)).expect("idle");
        (opening, command)
    }

    /// An opening that holds the committed result of epoch 4.
    fn committed() -> (EpochOpening, OpenEpoch) {
        let (mut opening, command) = submitted();
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("the answer");
        (opening, command)
    }

    const EXHAUSTED: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::EpochsExhausted);
    const MOVED: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::PredecessorMoved);
    const NO_GRANT_OUT: OpeningStage = OpeningStage::Faulted { handed_over: None };

    #[test]
    fn a_command_carries_the_data_set_and_the_condition_start_up_read() {
        let mut opening = EpochOpening::new(DATA_SET);
        assert_eq!(opening.stage(), OpeningStage::Idle);
        assert_eq!(opening.incarnation(), DATA_SET);
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Ok(OpenEpoch {
                ticket: OpeningTicket::new(1),
                incarnation: DATA_SET,
                after: RunEpoch::new(3),
            })
        );
        assert_eq!(opening.stage(), OpeningStage::Submitted);
    }

    #[test]
    fn a_committed_opening_hands_over_exactly_one_grant() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
            Ok(Observed::Advanced(OpeningStage::Committed(opened(4))))
        );
        let grant = opening.hand_over().expect("committed");
        assert_eq!(grant.opened(), opened(4));
        assert_eq!(grant.incarnation(), DATA_SET);
        assert_eq!(grant.epoch(), RunEpoch::new(4));
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
    }

    #[test]
    fn a_duplicated_reply_gives_no_second_grant() {
        let answer = OpeningOutcome::Committed(opened(4));

        // Delivered twice before the hand-over.
        let (mut opening, command) = submitted();
        assert!(matches!(
            opening.observe(reply(command, answer)),
            Ok(Observed::Advanced(_))
        ));
        assert_eq!(
            opening.observe(reply(command, answer)),
            Ok(Observed::Unchanged)
        );
        let _grant = opening.hand_over().expect("committed");
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

        // Delivered again after the hand-over.
        assert_eq!(
            opening.observe(reply(command, answer)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
    }

    #[test]
    fn an_unknown_outcome_gives_no_grant() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Unknown)),
            Ok(Observed::Advanced(OpeningStage::Unknown))
        );
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Unknown))
        );
        assert_eq!(opening.stage(), OpeningStage::Unknown);
    }

    #[test]
    fn no_grant_exists_before_a_committed_result() {
        let mut opening = EpochOpening::new(DATA_SET);
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Idle))
        );
        let _command = opening.submit(RunEpoch::new(3)).expect("idle");
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Submitted))
        );
    }

    /// Giving up on a reply is a transition of its own: the process
    /// stopped waiting, the store reported nothing.
    #[test]
    fn start_up_abandons_an_attempt_whose_reply_does_not_come() {
        let mut opening = EpochOpening::new(DATA_SET);
        assert_eq!(opening.abandon(), Err(NotAwaiting(OpeningStage::Idle)));

        let first = opening.submit(RunEpoch::new(3)).expect("idle");
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Err(SubmitRefused::AwaitingReply)
        );
        assert_eq!(opening.abandon(), Ok(()));
        assert_eq!(opening.stage(), OpeningStage::Unknown);
        assert_eq!(opening.abandon(), Err(NotAwaiting(OpeningStage::Unknown)));
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Unknown))
        );

        // No reply had to be made up to get here, and a new attempt may
        // start, on the value read again.
        let second = opening.submit(RunEpoch::new(3)).expect("abandoned");
        assert_ne!(second.ticket, first.ticket);

        let (mut held, _command) = committed();
        assert_eq!(
            held.abandon(),
            Err(NotAwaiting(OpeningStage::Committed(opened(4))))
        );
    }

    /// The older attempt committed epoch 4 and its reply was lost.
    /// Start-up reads 4, and opens the epoch after it.
    #[test]
    fn after_an_unknown_outcome_start_up_reads_again_and_opens_the_next_epoch() {
        let (mut opening, first) = submitted();
        opening.abandon().expect("awaiting");
        let second = opening.submit(RunEpoch::new(4)).expect("a new attempt");
        assert_ne!(second.ticket, first.ticket);
        assert_eq!(second.after, RunEpoch::new(4));

        // The reply of the older attempt shows up late. It answers no
        // attempt in flight: no grant for epoch 4.
        assert_eq!(
            opening.observe(reply(first, OpeningOutcome::Committed(opened(4)))),
            Err(ReplyRefused::Uncorrelated {
                ticket: first.ticket
            })
        );
        assert_eq!(opening.stage(), OpeningStage::Submitted);
        assert!(opening.hand_over().is_err());

        opening
            .observe(reply(second, OpeningOutcome::Committed(opened(5))))
            .expect("the answer");
        assert_eq!(
            opening.hand_over().expect("committed").epoch(),
            RunEpoch::new(5)
        );
    }

    /// The older attempt is still on its way when the newer one is
    /// submitted on the same condition. The store commits at most one of
    /// them; whichever reply comes, the grant is for the epoch after the
    /// condition and for the attempt in flight.
    #[test]
    fn an_older_attempt_on_the_same_condition_never_yields_a_grant() {
        // The newer attempt commits; the older one is rejected late.
        let (mut opening, older) = submitted();
        opening.abandon().expect("awaiting");
        let newer = opening.submit(RunEpoch::new(3)).expect("a new attempt");
        assert_eq!(newer.after, older.after);
        opening
            .observe(reply(newer, OpeningOutcome::Committed(opened(4))))
            .expect("the answer");
        let grant = opening.hand_over().expect("committed");
        assert_eq!(
            opening.observe(reply(older, MOVED)),
            Err(ReplyRefused::Uncorrelated {
                ticket: older.ticket
            })
        );
        assert_eq!(opening.stage(), OpeningStage::HandedOver(grant.opened()));

        // The older attempt commits; the newer one is rejected, and the
        // older reply, in either order, answers nothing in flight.
        for older_reply_first in [true, false] {
            let (mut opening, older) = submitted();
            opening.abandon().expect("awaiting");
            let newer = opening.submit(RunEpoch::new(3)).expect("a new attempt");
            let late = reply(older, OpeningOutcome::Committed(opened(4)));
            let refused = Err(ReplyRefused::Uncorrelated {
                ticket: older.ticket,
            });
            if older_reply_first {
                assert_eq!(opening.observe(late), refused);
            }
            assert_eq!(
                opening.observe(reply(newer, MOVED)),
                Ok(Observed::Advanced(OpeningStage::Rejected(
                    OpeningRejection::PredecessorMoved
                )))
            );
            if !older_reply_first {
                assert_eq!(opening.observe(late), refused);
            }
            assert!(opening.hand_over().is_err());

            // Start-up reads 4 and opens after it.
            let third = opening.submit(RunEpoch::new(4)).expect("not definitive");
            opening
                .observe(reply(third, OpeningOutcome::Committed(opened(5))))
                .expect("the answer");
            assert_eq!(
                opening.hand_over().expect("committed").epoch(),
                RunEpoch::new(5)
            );
        }
    }

    #[test]
    fn the_committed_result_of_the_attempt_in_flight_may_follow_an_unknown() {
        for by_the_store in [true, false] {
            let (mut opening, command) = submitted();
            if by_the_store {
                opening
                    .observe(reply(command, OpeningOutcome::Unknown))
                    .expect("correlated");
            } else {
                opening.abandon().expect("awaiting");
            }
            assert_eq!(
                opening.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
                Ok(Observed::Advanced(OpeningStage::Committed(opened(4))))
            );
            assert_eq!(
                opening.hand_over().expect("committed").epoch(),
                RunEpoch::new(4)
            );
        }
    }

    #[test]
    fn a_late_unknown_never_lowers_an_opening() {
        let (mut opening, command) = committed();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Unknown)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(opening.stage(), OpeningStage::Committed(opened(4)));
        let _grant = opening.hand_over().expect("still committed");
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Unknown)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));

        let (mut rejected, command) = submitted();
        rejected
            .observe(reply(command, EXHAUSTED))
            .expect("correlated");
        assert_eq!(
            rejected.observe(reply(command, OpeningOutcome::Unknown)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(
            rejected.stage(),
            OpeningStage::Rejected(OpeningRejection::EpochsExhausted)
        );
    }

    #[test]
    fn a_reply_that_answers_no_attempt_in_flight_is_refused() {
        // Nothing was submitted: even ticket zero correlates to nothing.
        let mut idle = EpochOpening::new(DATA_SET);
        for ticket in [0, 1] {
            let stray = OpeningReply {
                ticket: OpeningTicket::new(ticket),
                outcome: OpeningOutcome::Committed(opened(1)),
            };
            assert_eq!(
                idle.observe(stray),
                Err(ReplyRefused::Uncorrelated {
                    ticket: OpeningTicket::new(ticket)
                })
            );
        }
        assert_eq!(idle.stage(), OpeningStage::Idle);
        assert!(idle.hand_over().is_err());

        // Another ticket than the one in flight.
        let (mut opening, command) = submitted();
        let foreign = OpeningTicket::new(command.ticket.get() + 7);
        assert_eq!(
            opening.observe(OpeningReply {
                ticket: foreign,
                outcome: OpeningOutcome::Committed(opened(4)),
            }),
            Err(ReplyRefused::Uncorrelated { ticket: foreign })
        );
        assert_eq!(opening.stage(), OpeningStage::Submitted);
        assert!(opening.hand_over().is_err());
    }

    /// Every opening starts at ticket 1, so the ticket alone does not
    /// tell whose reply this is. A committed reply is honoured only for
    /// the data set this opening was created for.
    #[test]
    fn a_committed_reply_of_another_data_set_is_refused() {
        let (mut opening, command) = submitted();
        let elsewhere = EpochOpened {
            incarnation: StoreIncarnation::new(2),
            epoch: RunEpoch::new(4),
        };
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Committed(elsewhere))),
            Err(ReplyRefused::Mismatched { opened: elsewhere })
        );
        assert_eq!(opening.stage(), OpeningStage::Submitted);
        assert!(opening.hand_over().is_err());

        // Its own answer is still expected, and still honoured.
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("the answer");
        assert_eq!(opening.hand_over().expect("committed").opened(), opened(4));
    }

    /// The command asked for the epoch after 3. Epoch 0 is never opened,
    /// and no other epoch than 4 answers that command.
    #[test]
    fn a_committed_reply_names_the_epoch_after_the_condition_or_is_refused() {
        for epoch in [0, 1, 3, 5, u64::MAX] {
            let (mut opening, command) = submitted();
            assert_eq!(
                opening.observe(reply(command, OpeningOutcome::Committed(opened(epoch)))),
                Err(ReplyRefused::Mismatched {
                    opened: opened(epoch)
                }),
                "epoch {epoch}"
            );
            assert_eq!(opening.stage(), OpeningStage::Submitted);
            assert!(opening.hand_over().is_err());
        }
        // From a data set that had no epoch, the answer is epoch 1.
        let mut first = EpochOpening::new(DATA_SET);
        let command = first.submit(RunEpoch::new(0)).expect("idle");
        assert!(matches!(
            first.observe(reply(command, OpeningOutcome::Committed(opened(0)))),
            Err(ReplyRefused::Mismatched { .. })
        ));
        first
            .observe(reply(command, OpeningOutcome::Committed(opened(1))))
            .expect("the answer");
        // At the end of the counter no epoch follows the condition.
        let mut last = EpochOpening::new(DATA_SET);
        let command = last.submit(RunEpoch::new(u64::MAX)).expect("idle");
        for epoch in [0, u64::MAX] {
            assert!(matches!(
                last.observe(reply(command, OpeningOutcome::Committed(opened(epoch)))),
                Err(ReplyRefused::Mismatched { .. })
            ));
        }
    }

    /// Two terminal results that disagree stop start-up: the opening is
    /// faulted for good, and the commit that was contradicted yields no
    /// grant.
    #[test]
    fn contradictory_results_fault_the_opening_for_good() {
        let (mut opening, command) = committed();
        assert_eq!(
            opening.observe(reply(command, EXHAUSTED)),
            Err(ReplyRefused::Contradictory {
                established: OpeningStage::Committed(opened(4)),
                reported: EXHAUSTED,
            })
        );
        assert_eq!(opening.stage(), NO_GRANT_OUT);
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(NO_GRANT_OUT))
        );
        assert_eq!(
            opening.submit(RunEpoch::new(4)),
            Err(SubmitRefused::Faulted)
        );
        assert_eq!(opening.abandon(), Err(NotAwaiting(NO_GRANT_OUT)));
        // Nothing brings it back: not the commit again, not an unknown.
        for outcome in [
            OpeningOutcome::Committed(opened(4)),
            OpeningOutcome::Unknown,
            EXHAUSTED,
        ] {
            assert_eq!(
                opening.observe(reply(command, outcome)),
                Err(ReplyRefused::Faulted)
            );
        }
        assert_eq!(opening.stage(), NO_GRANT_OUT);
        assert!(opening.hand_over().is_err());
        assert_eq!(
            opening.check_recovered(DATA_SET, RunEpoch::new(4)),
            RecoveredEpochCheck::NotEstablished
        );

        // A commit after a rejection is the same fault.
        let (mut rejected, command) = submitted();
        rejected.observe(reply(command, MOVED)).expect("correlated");
        assert!(matches!(
            rejected.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
            Err(ReplyRefused::Contradictory { .. })
        ));
        assert_eq!(rejected.stage(), NO_GRANT_OUT);
        assert!(rejected.hand_over().is_err());
        assert_eq!(
            rejected.submit(RunEpoch::new(4)),
            Err(SubmitRefused::Faulted)
        );
    }

    /// The limit of the contract: a grant that already left cannot be
    /// taken back. The fault says that one is out, and no other follows.
    #[test]
    fn a_contradiction_after_the_hand_over_records_the_grant_that_is_out() {
        let (mut opening, command) = committed();
        let grant = opening.hand_over().expect("committed");
        assert!(matches!(
            opening.observe(reply(command, MOVED)),
            Err(ReplyRefused::Contradictory { .. })
        ));
        assert_eq!(
            opening.stage(),
            OpeningStage::Faulted {
                handed_over: Some(grant.opened())
            }
        );
        assert!(opening.hand_over().is_err());
        assert_eq!(
            opening.submit(RunEpoch::new(4)),
            Err(SubmitRefused::Faulted)
        );
    }

    /// As for `Knowledge::observe`: two rejections both say that nothing
    /// was opened, and the first reason is kept.
    #[test]
    fn a_rejection_repeated_with_another_reason_keeps_the_first() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.observe(reply(command, MOVED)),
            Ok(Observed::Advanced(OpeningStage::Rejected(
                OpeningRejection::PredecessorMoved
            )))
        );
        for again in [MOVED, EXHAUSTED] {
            assert_eq!(
                opening.observe(reply(command, again)),
                Ok(Observed::Unchanged)
            );
            assert_eq!(
                opening.stage(),
                OpeningStage::Rejected(OpeningRejection::PredecessorMoved)
            );
        }
        assert!(opening.hand_over().is_err());
    }

    /// Whether a new attempt may follow a rejection belongs to its
    /// reason.
    #[test]
    fn only_a_definitive_rejection_ends_the_attempts() {
        assert!(OpeningRejection::EpochsExhausted.is_definitive());
        assert!(OpeningRejection::ForeignIncarnation.is_definitive());
        assert!(!OpeningRejection::PredecessorMoved.is_definitive());

        for reason in [
            OpeningRejection::EpochsExhausted,
            OpeningRejection::ForeignIncarnation,
        ] {
            let (mut opening, command) = submitted();
            opening
                .observe(reply(command, OpeningOutcome::RejectedCertain(reason)))
                .expect("correlated");
            assert_eq!(
                opening.hand_over(),
                Err(HandOverRefused::NotCommitted(OpeningStage::Rejected(
                    reason
                )))
            );
            assert_eq!(
                opening.submit(RunEpoch::new(3)),
                Err(SubmitRefused::Rejected(reason))
            );
            assert_eq!(opening.stage(), OpeningStage::Rejected(reason));
        }

        // The predecessor moved: start-up reads again and tries again.
        let (mut opening, command) = submitted();
        opening.observe(reply(command, MOVED)).expect("correlated");
        assert!(opening.hand_over().is_err());
        let again = opening.submit(RunEpoch::new(4)).expect("not definitive");
        assert_ne!(again.ticket, command.ticket);
        assert_eq!(again.after, RunEpoch::new(4));
    }

    #[test]
    fn one_attempt_is_outstanding_and_a_live_opening_is_not_opened_again() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Err(SubmitRefused::AwaitingReply)
        );
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("the answer");
        assert_eq!(
            opening.submit(RunEpoch::new(4)),
            Err(SubmitRefused::AlreadyOpened)
        );
        let _grant = opening.hand_over().expect("committed");
        assert_eq!(
            opening.submit(RunEpoch::new(4)),
            Err(SubmitRefused::AlreadyOpened)
        );
        // The refusals changed nothing.
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
    }

    /// The last opened epoch of a data set never decreases. A lower
    /// value than one already read is not a read of this data set.
    #[test]
    fn a_condition_lower_than_one_already_read_is_refused() {
        let (mut opening, _first) = submitted();
        opening.abandon().expect("awaiting");
        assert_eq!(
            opening.submit(RunEpoch::new(2)),
            Err(SubmitRefused::LedgerRegressed)
        );
        assert_eq!(opening.stage(), OpeningStage::Unknown);
        assert!(opening.submit(RunEpoch::new(3)).is_ok());
    }

    #[test]
    fn a_recovered_fact_gives_no_grant() {
        // The attempt did commit epoch 4, but its outcome is unknown
        // here. A recovery page then shows epoch 4, once and again.
        let (mut opening, _command) = submitted();
        opening.abandon().expect("awaiting");
        for _page in 0..3 {
            assert_eq!(
                opening.check_recovered(DATA_SET, RunEpoch::new(4)),
                RecoveredEpochCheck::NotEstablished
            );
            assert_eq!(opening.stage(), OpeningStage::Unknown);
            assert_eq!(
                opening.hand_over(),
                Err(HandOverRefused::NotCommitted(OpeningStage::Unknown))
            );
        }

        // After the hand-over, the fact read again confirms the opening
        // and still yields nothing.
        let (mut opening, _command) = committed();
        assert_eq!(
            opening.check_recovered(DATA_SET, RunEpoch::new(4)),
            RecoveredEpochCheck::Confirms
        );
        let _grant = opening.hand_over().expect("committed");
        for _page in 0..3 {
            assert_eq!(
                opening.check_recovered(DATA_SET, RunEpoch::new(4)),
                RecoveredEpochCheck::Confirms
            );
            assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
        }
        assert_eq!(
            opening.check_recovered(DATA_SET, RunEpoch::new(5)),
            RecoveredEpochCheck::Foreign
        );
        assert_eq!(
            opening.check_recovered(StoreIncarnation::new(2), RunEpoch::new(4)),
            RecoveredEpochCheck::Foreign
        );
    }

    #[test]
    fn tickets_are_never_reused_and_never_wrap() {
        let mut opening = EpochOpening::new(DATA_SET);
        let mut seen = Vec::new();
        for _ in 0..5 {
            let command = opening.submit(RunEpoch::new(3)).expect("idle or unknown");
            assert!(!seen.contains(&command.ticket));
            assert_ne!(command.ticket, OpeningTicket::new(0));
            seen.push(command.ticket);
            opening.abandon().expect("awaiting");
        }

        let mut last = EpochOpening {
            incarnation: DATA_SET,
            ticket: OpeningTicket::new(u64::MAX - 1),
            after: RunEpoch::new(3),
            stage: OpeningStage::Unknown,
        };
        let command = last
            .submit(RunEpoch::new(3))
            .expect("the last ticket is usable");
        assert_eq!(command.ticket, OpeningTicket::new(u64::MAX));
        last.abandon().expect("awaiting");
        assert_eq!(
            last.submit(RunEpoch::new(3)),
            Err(SubmitRefused::TicketsExhausted)
        );
        assert_eq!(last.stage(), OpeningStage::Unknown);
    }

    /// Compiles only while `EpochGrant` and `EpochOpening` are not
    /// `Clone` (hence not `Copy`): with a `Clone` impl both blanket impls
    /// below would apply and the inference would be ambiguous. A build
    /// that breaks here is a guard, not a detection by a test.
    #[test]
    fn the_grant_and_the_opening_cannot_be_duplicated() {
        trait AmbiguousIfClone<Marker> {
            fn check() {}
        }
        impl<T> AmbiguousIfClone<()> for T {}
        impl<T: Clone> AmbiguousIfClone<u8> for T {}
        <EpochGrant as AmbiguousIfClone<_>>::check();
        <EpochOpening as AmbiguousIfClone<_>>::check();
    }

    #[derive(Clone, Copy, Debug)]
    enum Action {
        Submit(u64),
        Abandon,
        HandOver,
        Reply(u64, OpeningOutcome),
    }

    /// Every sequence of five actions over two tickets, two conditions
    /// and every kind of reply. An opening hands over at most one grant;
    /// only for its own data set and for the epoch after the condition
    /// of the attempt in flight, once a committed reply echoed its
    /// ticket; and never once a contradiction was reported.
    #[test]
    fn no_sequence_of_actions_yields_two_grants_or_an_unfounded_one() {
        let elsewhere = EpochOpened {
            incarnation: StoreIncarnation::new(2),
            epoch: RunEpoch::new(4),
        };
        let mut actions = vec![
            Action::Submit(3),
            Action::Submit(4),
            Action::Abandon,
            Action::HandOver,
        ];
        for ticket in [1, 2] {
            for outcome in [
                OpeningOutcome::Committed(opened(4)),
                OpeningOutcome::Committed(opened(5)),
                OpeningOutcome::Committed(opened(0)),
                OpeningOutcome::Committed(elsewhere),
                EXHAUSTED,
                MOVED,
                OpeningOutcome::Unknown,
            ] {
                actions.push(Action::Reply(ticket, outcome));
            }
        }
        const DEPTH: u32 = 5;
        let base = actions.len();
        let mut granted_runs = 0u32;
        let mut faulted_runs = 0u32;
        for run in 0..base.pow(DEPTH) {
            let mut code = run;
            let mut opening = EpochOpening::new(DATA_SET);
            // The ticket in flight and the epoch that answers it.
            let mut in_flight: Option<(OpeningTicket, RunEpoch)> = None;
            let mut founded = false;
            let mut faulted = false;
            let mut grants = Vec::new();
            for _ in 0..DEPTH {
                let action = actions[code % base];
                code /= base;
                match action {
                    Action::Submit(last_opened) => {
                        if let Ok(command) = opening.submit(RunEpoch::new(last_opened)) {
                            assert!(!faulted, "an attempt after a fault");
                            in_flight = Some((command.ticket, RunEpoch::new(last_opened + 1)));
                            // A new attempt abandons what the old one
                            // might still report.
                            founded = false;
                        }
                    }
                    Action::Abandon => {
                        let _ = opening.abandon();
                    }
                    Action::Reply(ticket, outcome) => {
                        let ticket = OpeningTicket::new(ticket);
                        match opening.observe(OpeningReply { ticket, outcome }) {
                            Ok(_) => {
                                assert!(!faulted, "a reply taken after a fault");
                                let (awaited, answer) = in_flight.expect("an attempt in flight");
                                assert_eq!(ticket, awaited, "an uncorrelated reply");
                                if let OpeningOutcome::Committed(opened) = outcome {
                                    assert_eq!(opened.incarnation, DATA_SET);
                                    assert_eq!(opened.epoch, answer);
                                    founded = true;
                                }
                            }
                            Err(ReplyRefused::Contradictory { .. }) => faulted = true,
                            Err(_) => {}
                        }
                    }
                    Action::HandOver => {
                        if let Ok(grant) = opening.hand_over() {
                            assert!(!faulted, "a grant after a contradiction");
                            assert!(founded, "a grant without a committed reply in flight");
                            let (_, answer) = in_flight.expect("an attempt in flight");
                            assert_eq!(grant.incarnation(), DATA_SET);
                            assert_eq!(grant.epoch(), answer);
                            grants.push(grant);
                        }
                    }
                }
            }
            assert!(grants.len() <= 1, "two grants from one opening");
            granted_runs += u32::from(!grants.is_empty());
            faulted_runs += u32::from(faulted);
        }
        assert!(granted_runs > 0, "the search never reached a hand-over");
        assert!(faulted_runs > 0, "the search never reached a fault");
    }
}
