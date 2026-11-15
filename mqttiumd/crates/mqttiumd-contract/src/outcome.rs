//! The four store outcomes and monotone knowledge (DEM-010).
//!
//! An observed absence is not a certain rejection: a command may still sit in
//! a queue while a read finds no receipt. Only a durable fence ordered with
//! the writer turns "absent" into `RejectedCertain`.
//!
//! # Opening an epoch and handing over the creation authority (ADR 0009)
//!
//! Start-up submits `OpenEpoch` and the store answers with an
//! `OpeningReply`. That an epoch was opened is a durable fact, and a fact
//! can be read any number of times: reading it authorises nothing. What
//! authorises the creation of owners is the `EpochGrant`, and a grant
//! comes out of exactly one transition of the volatile `EpochOpening`:
//!
//! ```text
//! Idle --submit--> Submitted --Committed--> Committed --hand_over--> HandedOver
//!                     |  \--RejectedCertain--> Rejected
//!                     \--Unknown--> Unknown --submit--> Submitted (new ticket)
//! ```
//!
//! - A reply is honoured only when it echoes the ticket of the attempt in
//!   flight. A reply delivered twice changes nothing the second time.
//! - `hand_over` succeeds once, from `Committed`. The grant is moved to
//!   the creator of owners and is neither `Clone` nor `Copy`.
//! - An unknown outcome gives no grant. Start-up does not proceed on an
//!   opening it assumes: the way forward is a new attempt, which opens a
//!   newer epoch and closes the one the lost attempt may have opened.
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
//! given `EpochOpening` yields at most one grant, for the epoch of a
//! committed reply to its own attempt. That each committed opening names
//! an epoch no earlier opening named is the store's part (`EpochLedger`).
//! That a process drives one `EpochOpening` per store it opened, and that
//! a reply reaches the `EpochOpening` that submitted its command, is the
//! wiring of lot L4; this type cannot tell two instances apart.

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
    /// The epoch counter of the data set reached its end. Definitive:
    /// nothing wraps, and no later attempt can succeed.
    EpochsExhausted,
}

/// What the store reports about one attempt to open an epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningOutcome {
    /// The opening is durable: this epoch is open, the older ones closed.
    Committed(EpochOpened),
    /// No epoch was opened by this attempt.
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
    /// The outcome of the last attempt is unknown. No grant exists.
    Unknown,
    /// The last attempt was certainly not committed, definitively.
    Rejected(OpeningRejection),
    /// The committed result arrived; the grant was not handed over yet.
    Committed(EpochOpened),
    /// The grant of this opening was handed over. It never is again.
    HandedOver(EpochOpened),
}

/// Why `submit` produced no command. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubmitRefused {
    /// An attempt is already in flight; one is outstanding at a time.
    AwaitingReply,
    /// This process already holds a committed opening. Opening again
    /// would close the epoch its own owners live in.
    AlreadyOpened,
    /// The store answered that no opening can succeed any more.
    Rejected(OpeningRejection),
    /// The ticket counter reached its end; nothing wraps.
    TicketsExhausted,
}

/// Why a reply was refused. Nothing changed; the caller records it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplyRefused {
    /// The reply answers no attempt in flight here: nothing was
    /// submitted, or its ticket is not the one of the current attempt
    /// (ADR 0003).
    Uncorrelated { ticket: OpeningTicket },
    /// Two terminal results disagree for one attempt: an internal fault
    /// that stops start-up with a diagnostic.
    Contradictory {
        established: OpeningStage,
        reported: OpeningOutcome,
    },
}

/// Why no grant was handed over. Nothing changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HandOverRefused {
    /// No committed result was received by this opening.
    NotCommitted(OpeningStage),
    /// The one grant of this opening was already handed over.
    AlreadyHandedOver,
}

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
    /// The ticket of the last attempt; zero before the first one.
    ticket: OpeningTicket,
    stage: OpeningStage,
}

impl Default for EpochOpening {
    fn default() -> Self {
        Self::new()
    }
}

impl EpochOpening {
    /// An opening on which nothing was submitted.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ticket: OpeningTicket::new(0),
            stage: OpeningStage::Idle,
        }
    }

    /// Where this opening stands.
    #[must_use]
    pub const fn stage(&self) -> OpeningStage {
        self.stage
    }

    /// Starts an attempt and returns its command, under a ticket no
    /// earlier attempt of this opening used.
    ///
    /// After an unknown outcome this is the only way forward. The new
    /// attempt abandons the old one: a late reply to the old ticket is
    /// then refused, and the epoch it may have opened is closed by the
    /// opening the new attempt commits.
    pub fn submit(&mut self) -> Result<OpenEpoch, SubmitRefused> {
        match self.stage {
            OpeningStage::Idle | OpeningStage::Unknown => {}
            OpeningStage::Submitted => return Err(SubmitRefused::AwaitingReply),
            OpeningStage::Rejected(reason) => return Err(SubmitRefused::Rejected(reason)),
            OpeningStage::Committed(_) | OpeningStage::HandedOver(_) => {
                return Err(SubmitRefused::AlreadyOpened);
            }
        }
        let ticket = self
            .ticket
            .checked_next()
            .map_err(|Exhausted| SubmitRefused::TicketsExhausted)?;
        self.ticket = ticket;
        self.stage = OpeningStage::Submitted;
        Ok(OpenEpoch { ticket })
    }

    /// Takes the store's reply to the attempt in flight.
    ///
    /// Knowledge only grows: `Submitted`, then `Unknown`, then one
    /// terminal result. A repeated reply and a late `Unknown` change
    /// nothing; a reply to another ticket is refused; two different
    /// terminal results for one attempt are a fault.
    pub fn observe(&mut self, reply: OpeningReply) -> Result<Observed, ReplyRefused> {
        if self.stage == OpeningStage::Idle || reply.ticket != self.ticket {
            return Err(ReplyRefused::Uncorrelated {
                ticket: reply.ticket,
            });
        }
        let contradictory = ReplyRefused::Contradictory {
            established: self.stage,
            reported: reply.outcome,
        };
        let next = match (self.stage, reply.outcome) {
            (OpeningStage::Submitted, OpeningOutcome::Unknown) => OpeningStage::Unknown,
            (_, OpeningOutcome::Unknown) => return Ok(Observed::Unchanged),
            (
                OpeningStage::Submitted | OpeningStage::Unknown,
                OpeningOutcome::Committed(opened),
            ) => OpeningStage::Committed(opened),
            (
                OpeningStage::Submitted | OpeningStage::Unknown,
                OpeningOutcome::RejectedCertain(reason),
            ) => OpeningStage::Rejected(reason),
            (
                OpeningStage::Committed(known) | OpeningStage::HandedOver(known),
                OpeningOutcome::Committed(opened),
            ) if known == opened => return Ok(Observed::Unchanged),
            (OpeningStage::Rejected(known), OpeningOutcome::RejectedCertain(reason))
                if known == reason =>
            {
                return Ok(Observed::Unchanged);
            }
            (
                OpeningStage::Idle
                | OpeningStage::Committed(_)
                | OpeningStage::HandedOver(_)
                | OpeningStage::Rejected(_),
                OpeningOutcome::Committed(_) | OpeningOutcome::RejectedCertain(_),
            ) => return Err(contradictory),
        };
        self.stage = next;
        Ok(Observed::Advanced(next))
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
            | OpeningStage::Rejected(_) => RecoveredEpochCheck::NotEstablished,
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

    fn opened(epoch: u64) -> EpochOpened {
        EpochOpened {
            incarnation: StoreIncarnation::new(1),
            epoch: RunEpoch::new(epoch),
        }
    }

    fn reply(command: OpenEpoch, outcome: OpeningOutcome) -> OpeningReply {
        OpeningReply {
            ticket: command.ticket,
            outcome,
        }
    }

    const OPENING_REJECTED: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::EpochsExhausted);

    #[test]
    fn a_committed_opening_hands_over_exactly_one_grant() {
        let mut opening = EpochOpening::new();
        assert_eq!(opening.stage(), OpeningStage::Idle);
        let command = opening.submit().expect("idle");
        assert_eq!(opening.stage(), OpeningStage::Submitted);
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
            Ok(Observed::Advanced(OpeningStage::Committed(opened(4))))
        );
        let grant = opening.hand_over().expect("committed");
        assert_eq!(grant.opened(), opened(4));
        assert_eq!(grant.incarnation(), StoreIncarnation::new(1));
        assert_eq!(grant.epoch(), RunEpoch::new(4));
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
    }

    #[test]
    fn a_duplicated_reply_gives_no_second_grant() {
        let committed = OpeningOutcome::Committed(opened(4));

        // Delivered twice before the hand-over.
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        assert!(matches!(
            opening.observe(reply(command, committed)),
            Ok(Observed::Advanced(_))
        ));
        assert_eq!(
            opening.observe(reply(command, committed)),
            Ok(Observed::Unchanged)
        );
        let _grant = opening.hand_over().expect("committed");
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

        // Delivered again after the hand-over.
        assert_eq!(
            opening.observe(reply(command, committed)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
    }

    #[test]
    fn an_unknown_outcome_gives_no_grant() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
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
        let mut opening = EpochOpening::new();
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Idle))
        );
        let _command = opening.submit().expect("idle");
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Submitted))
        );
    }

    #[test]
    fn after_an_unknown_outcome_the_way_forward_is_a_new_attempt() {
        let mut opening = EpochOpening::new();
        let first = opening.submit().expect("idle");
        opening
            .observe(reply(first, OpeningOutcome::Unknown))
            .expect("correlated");
        let second = opening.submit().expect("unknown allows a new attempt");
        assert_ne!(second.ticket, first.ticket);

        // The lost attempt had committed epoch 4 and its reply shows up
        // late. It answers no attempt in flight: no grant for epoch 4.
        assert_eq!(
            opening.observe(reply(first, OpeningOutcome::Committed(opened(4)))),
            Err(ReplyRefused::Uncorrelated {
                ticket: first.ticket
            })
        );
        assert_eq!(opening.stage(), OpeningStage::Submitted);
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Submitted))
        );

        // The new attempt opened epoch 5, which closed epoch 4.
        opening
            .observe(reply(second, OpeningOutcome::Committed(opened(5))))
            .expect("correlated");
        assert_eq!(
            opening.hand_over().expect("committed").epoch(),
            RunEpoch::new(5)
        );
    }

    #[test]
    fn the_committed_result_of_the_attempt_in_flight_may_follow_an_unknown() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        opening
            .observe(reply(command, OpeningOutcome::Unknown))
            .expect("correlated");
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
            Ok(Observed::Advanced(OpeningStage::Committed(opened(4))))
        );
        assert_eq!(
            opening.hand_over().expect("committed").epoch(),
            RunEpoch::new(4)
        );
    }

    #[test]
    fn a_late_unknown_never_lowers_an_opening() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("correlated");
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

        let mut rejected = EpochOpening::new();
        let command = rejected.submit().expect("idle");
        rejected
            .observe(reply(command, OPENING_REJECTED))
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
        let mut idle = EpochOpening::new();
        for ticket in [0, 1] {
            let stray = OpeningReply {
                ticket: OpeningTicket::new(ticket),
                outcome: OpeningOutcome::Committed(opened(4)),
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
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
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

    #[test]
    fn contradictory_results_of_one_opening_are_a_fault() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("correlated");
        for reported in [OpeningOutcome::Committed(opened(5)), OPENING_REJECTED] {
            assert_eq!(
                opening.observe(reply(command, reported)),
                Err(ReplyRefused::Contradictory {
                    established: OpeningStage::Committed(opened(4)),
                    reported,
                })
            );
            assert_eq!(opening.stage(), OpeningStage::Committed(opened(4)));
        }
        let grant = opening.hand_over().expect("committed");
        assert_eq!(grant.epoch(), RunEpoch::new(4));
        // Still a fault after the hand-over, and still no second grant.
        assert!(matches!(
            opening.observe(reply(command, OpeningOutcome::Committed(opened(5)))),
            Err(ReplyRefused::Contradictory { .. })
        ));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

        let mut rejected = EpochOpening::new();
        let command = rejected.submit().expect("idle");
        rejected
            .observe(reply(command, OPENING_REJECTED))
            .expect("correlated");
        assert!(matches!(
            rejected.observe(reply(command, OpeningOutcome::Committed(opened(4)))),
            Err(ReplyRefused::Contradictory { .. })
        ));
        assert!(rejected.hand_over().is_err());
    }

    #[test]
    fn a_rejected_opening_gives_no_grant_and_no_new_attempt() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        assert_eq!(
            opening.observe(reply(command, OPENING_REJECTED)),
            Ok(Observed::Advanced(OpeningStage::Rejected(
                OpeningRejection::EpochsExhausted
            )))
        );
        assert_eq!(
            opening.observe(reply(command, OPENING_REJECTED)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Rejected(
                OpeningRejection::EpochsExhausted
            )))
        );
        assert_eq!(
            opening.submit(),
            Err(SubmitRefused::Rejected(OpeningRejection::EpochsExhausted))
        );
    }

    #[test]
    fn one_attempt_is_outstanding_and_a_live_opening_is_not_opened_again() {
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        assert_eq!(opening.submit(), Err(SubmitRefused::AwaitingReply));
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("correlated");
        assert_eq!(opening.submit(), Err(SubmitRefused::AlreadyOpened));
        let _grant = opening.hand_over().expect("committed");
        assert_eq!(opening.submit(), Err(SubmitRefused::AlreadyOpened));
        // The refusals changed nothing.
        assert_eq!(opening.stage(), OpeningStage::HandedOver(opened(4)));
    }

    #[test]
    fn a_recovered_fact_gives_no_grant() {
        let incarnation = StoreIncarnation::new(1);

        // The attempt did commit epoch 4, but its outcome is unknown
        // here. A recovery page then shows epoch 4, once and again.
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        opening
            .observe(reply(command, OpeningOutcome::Unknown))
            .expect("correlated");
        for _page in 0..3 {
            assert_eq!(
                opening.check_recovered(incarnation, RunEpoch::new(4)),
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
        let mut opening = EpochOpening::new();
        let command = opening.submit().expect("idle");
        opening
            .observe(reply(command, OpeningOutcome::Committed(opened(4))))
            .expect("correlated");
        assert_eq!(
            opening.check_recovered(incarnation, RunEpoch::new(4)),
            RecoveredEpochCheck::Confirms
        );
        let _grant = opening.hand_over().expect("committed");
        for _page in 0..3 {
            assert_eq!(
                opening.check_recovered(incarnation, RunEpoch::new(4)),
                RecoveredEpochCheck::Confirms
            );
            assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
        }
        assert_eq!(
            opening.check_recovered(incarnation, RunEpoch::new(5)),
            RecoveredEpochCheck::Foreign
        );
        assert_eq!(
            opening.check_recovered(StoreIncarnation::new(2), RunEpoch::new(4)),
            RecoveredEpochCheck::Foreign
        );
    }

    #[test]
    fn tickets_are_never_reused_and_never_wrap() {
        let mut opening = EpochOpening::new();
        let mut seen = Vec::new();
        for _ in 0..5 {
            let command = opening.submit().expect("idle or unknown");
            assert!(!seen.contains(&command.ticket));
            assert_ne!(command.ticket, OpeningTicket::new(0));
            seen.push(command.ticket);
            opening
                .observe(reply(command, OpeningOutcome::Unknown))
                .expect("correlated");
        }

        let mut last = EpochOpening {
            ticket: OpeningTicket::new(u64::MAX - 1),
            stage: OpeningStage::Unknown,
        };
        let command = last.submit().expect("the last ticket is usable");
        assert_eq!(command.ticket, OpeningTicket::new(u64::MAX));
        last.observe(reply(command, OpeningOutcome::Unknown))
            .expect("correlated");
        assert_eq!(last.submit(), Err(SubmitRefused::TicketsExhausted));
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
        Submit,
        HandOver,
        Reply(u64, OpeningOutcome),
    }

    /// Every sequence of five actions over two tickets and every outcome:
    /// an opening hands over at most one grant, and only for the epoch of
    /// a committed reply that echoed the ticket then in flight.
    #[test]
    fn no_sequence_of_actions_yields_two_grants_or_an_unfounded_one() {
        let mut actions = vec![Action::Submit, Action::HandOver];
        for ticket in [1, 2] {
            for outcome in [
                OpeningOutcome::Committed(opened(4)),
                OpeningOutcome::Committed(opened(5)),
                OPENING_REJECTED,
                OpeningOutcome::Unknown,
            ] {
                actions.push(Action::Reply(ticket, outcome));
            }
        }
        const DEPTH: u32 = 5;
        let base = actions.len();
        let mut granted_runs = 0u32;
        for mut code in 0..base.pow(DEPTH) {
            let mut opening = EpochOpening::new();
            let mut in_flight = None;
            let mut founded = Vec::new();
            let mut grants = Vec::new();
            for _ in 0..DEPTH {
                let action = actions[code % base];
                code /= base;
                match action {
                    Action::Submit => {
                        if let Ok(command) = opening.submit() {
                            in_flight = Some(command.ticket);
                            // A new attempt abandons what the old one
                            // might still report.
                            founded.clear();
                        }
                    }
                    Action::Reply(ticket, outcome) => {
                        let ticket = OpeningTicket::new(ticket);
                        let accepted = opening.observe(OpeningReply { ticket, outcome });
                        if accepted.is_ok() {
                            assert_eq!(Some(ticket), in_flight, "an uncorrelated reply");
                            if let OpeningOutcome::Committed(opened) = outcome {
                                founded.push(opened);
                            }
                        }
                    }
                    Action::HandOver => {
                        if let Ok(grant) = opening.hand_over() {
                            assert!(
                                founded.contains(&grant.opened()),
                                "a grant without a committed reply to the attempt in flight"
                            );
                            grants.push(grant);
                        }
                    }
                }
            }
            assert!(grants.len() <= 1, "two grants from one opening");
            granted_runs += u32::from(!grants.is_empty());
        }
        assert!(granted_runs > 0, "the search never reached a hand-over");
    }
}
