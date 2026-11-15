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
//!                   |  |  |  \--RejectedCertain--> Rejected --submit (unless definitive)--\
//!                   |  |  \--Superseded--> Superseded --submit, on a higher value read--+--> Submitted
//!                   |  \--Unknown (store)--\                                           |
//!                   \--abandon (deadline)--+--> Unknown --submit----------------------/
//!
//! a commit that cannot answer the command, or a commit and a rejection
//! of one attempt --> Faulted
//! ```
//!
//! What a reply to the command in flight does, stage by reply. "same"
//! leaves the stage as it is; "fault" moves to `Faulted`.
//!
//! | Stage \\ reply | `Unknown` | `Superseded` | `Committed`, the answer | `Committed`, any other | `RejectedCertain` |
//! | - | - | - | - | - | - |
//! | `Submitted` | `Unknown` | `Superseded` | `Committed` | fault | `Rejected` |
//! | `Unknown` | same | `Superseded` | `Committed` | fault | `Rejected` |
//! | `Superseded` | same | same | same, no grant | fault | `Rejected` |
//! | `Rejected` | same | same | fault | fault | same, first reason kept |
//! | `Committed` | same | `Superseded` | same | fault | fault |
//! | `HandedOver` | same | same, reported to the caller | same | fault | fault |
//! | `Faulted` | refused | refused | refused | refused | refused |
//!
//! "The answer" is the commit for this data set and the epoch right
//! after the command's condition. A reply to any other command is
//! refused at every stage and changes nothing.
//!
//! - An attempt has an identity: the nonce its start-up drew and its
//!   number there. The store records the attempt that opened the last
//!   epoch, and every reply carries the whole command it answers. A
//!   reply is honoured only when that command is exactly the one in
//!   flight, whatever its outcome: the reply of another opening, or of
//!   a predecessor that died with the same counters, changes nothing.
//! - Once the command is recognised, a commit can only be for this data
//!   set and the epoch right after the command's condition. Any other
//!   commit, and a commit together with a rejection, is a fault that
//!   stops start-up: the opening is `Faulted`, for good.
//! - `hand_over` succeeds once, from `Committed`. The grant is moved to
//!   the creator of owners and is neither `Clone` nor `Copy`.
//! - An unknown outcome gives no grant, whether the store reported it or
//!   start-up stopped waiting (`abandon`). Start-up does not proceed on
//!   an opening it assumes: it reads the last opened epoch again and
//!   submits a new attempt on that condition. Reading the value to form a
//!   condition adopts nothing: the grant still comes from the committed
//!   reply to the new attempt.
//! - An older attempt can never undo a newer one. The store opens an
//!   epoch only while the last opened epoch is the one the command
//!   expects (`EpochLedger::open`), so an attempt that was held on its
//!   way and arrives after a later opening committed opens nothing.
//! - Only `RejectedCertain` says that an attempt opened nothing. An
//!   attempt whose epoch is already closed is `Superseded`: no grant,
//!   and nothing is claimed about what it did.
//! - No function takes a recovered fact or a recovery page and returns a
//!   grant; `check_recovered` compares a fact and borrows immutably.
//! - When the process dies, this state and the grant die with it. The
//!   next process opens the next epoch (DEM-012); it never rebuilds an
//!   authority in the older one.
//!
//! Trusted code, named as ADR 0009 requires. `EpochOpening::hand_over`
//! is the one function that constructs an `EpochGrant`. The rule lot L1
//! realises is that the creation authority is constructed by consuming a
//! grant and in no other way; nothing in this crate enforces it yet.
//! `EpochOpening::observe` is the entry point between the store adapter
//! and this crate: a reply is plain data, and the type system does not
//! prevent code from building one. The `StartupNonce` is drawn by the
//! adapter, and that no two start-ups share one is its precondition.
//! What is established here is that a given `EpochOpening` yields at
//! most one grant, for the epoch that follows its own condition in its
//! own data set, after a committed reply to its own command. That each
//! committed opening names an epoch no earlier opening named is the
//! store's part (`EpochLedger`). That a process drives one
//! `EpochOpening` per store it opened is the wiring of lot L4.
//!
//! A limit, stated: a grant that was handed over cannot be taken back.
//! When a contradiction arrives afterwards the opening becomes `Faulted`
//! and says that a grant is out; when the store reports the epoch of a
//! grant superseded, `observe` says so. Stopping the holder is
//! start-up's.

use crate::capacity::Resource;
use crate::command::{
    LedgerOpening, OpenEpoch, OpeningAttempt, OpeningRejection, OpeningTicket, StartupNonce,
};
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

/// What the store reports about one attempt to open an epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningOutcome {
    /// The opening is durable: this attempt opened this epoch.
    Committed(EpochOpened),
    /// No epoch was opened by this attempt, and none ever will be.
    RejectedCertain(OpeningRejection),
    /// The epoch this attempt asked for is closed. The attempt can never
    /// open one; whether it had opened that one is not known any more.
    /// This is not a rejection of what it once did.
    Superseded,
    /// The observations do not allow a conclusion: the attempt may have
    /// opened an epoch, or may still. A result of its own.
    Unknown,
}

impl OpeningOutcome {
    /// The outcome a store reports for a decision of its ledger, once
    /// the ledger of `Opens` is durable. A store that could not make it
    /// durable reports `Unknown` instead.
    #[must_use]
    pub const fn of(incarnation: StoreIncarnation, decision: LedgerOpening) -> Self {
        match decision {
            LedgerOpening::Opens { epoch, .. } | LedgerOpening::AlreadyOpened { epoch } => {
                Self::Committed(EpochOpened { incarnation, epoch })
            }
            LedgerOpening::Rejected(reason) => Self::RejectedCertain(reason),
            LedgerOpening::Superseded => Self::Superseded,
            LedgerOpening::Undecided => Self::Unknown,
        }
    }
}

/// The store's answer to one `OpenEpoch`. It carries the whole command
/// it answers, whatever the outcome.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpeningReply {
    pub command: OpenEpoch,
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
    /// The epoch the last attempt asked for is closed. No grant exists,
    /// and none will for this attempt, even if its commit shows up.
    Superseded,
    /// The last attempt was certainly not committed. The first reason
    /// reported is kept.
    Rejected(OpeningRejection),
    /// The committed result arrived; the grant was not handed over yet.
    Committed(EpochOpened),
    /// The grant of this opening was handed over. It never is again.
    HandedOver(EpochOpened),
    /// The replies to one attempt cannot all be true. Terminal: no
    /// grant, no new attempt. `handed_over` names the opening whose grant
    /// had already left when the fault was found, if any.
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
    /// The store answered that an epoch was opened after the value this
    /// opening last read, and the value read now is still that one: the
    /// read is stale, and a command on it could only be refused again.
    ConditionUnchanged,
    /// The ticket counter reached its end; nothing wraps.
    TicketsExhausted,
}

/// Why a reply was refused. The caller records it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplyRefused {
    /// The reply answers no attempt in flight here: nothing was
    /// submitted, or its command is not the one of the current attempt
    /// (ADR 0003). Nothing changed.
    Uncorrelated { attempt: OpeningAttempt },
    /// The replies to the command in flight cannot all be true: a commit
    /// for another data set or another epoch than the one after its
    /// condition, or a commit and a rejection. An internal fault; the
    /// opening is now `Faulted`.
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
    /// The reply repeats what is known, or is a late answer that lowers
    /// nothing. The stage did not move.
    Unchanged,
    /// The store reports that the epoch of the grant already handed over
    /// is closed. The stage did not move: the grant cannot be taken
    /// back, and its holder is to be stopped by the caller.
    GrantedEpochSuperseded(EpochOpened),
}

/// How a recovered `Epoch` fact relates to an opening. Reading a fact
/// never moves the opening and never yields a grant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecoveredEpochCheck {
    /// The fact describes the epoch this process opened.
    Confirms,
    /// The fact describes another data set or another epoch than the one
    /// this process opened, or holds a grant for: the inventory is not
    /// the one of this opening.
    Foreign,
    /// This process holds no committed opening and no grant. Whatever
    /// the fact says, it establishes nothing here.
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
    /// What makes the attempts of this start-up its own.
    startup: StartupNonce,
    /// The ticket of the last attempt; zero before the first one.
    ticket: OpeningTicket,
    /// The command of the last attempt, the only one a reply may answer.
    in_flight: Option<OpenEpoch>,
    /// The highest value of the last opened epoch this opening read.
    read: RunEpoch,
    /// The store answered that an epoch was opened after `read`: the
    /// next value read must be higher.
    read_is_stale: bool,
    stage: OpeningStage,
}

impl EpochOpening {
    /// An opening on which nothing was submitted, for the data set
    /// start-up verified and with the nonce this start-up drew.
    #[must_use]
    pub const fn new(incarnation: StoreIncarnation, startup: StartupNonce) -> Self {
        Self {
            incarnation,
            startup,
            ticket: OpeningTicket::new(0),
            in_flight: None,
            read: RunEpoch::new(0),
            read_is_stale: false,
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
    /// an identity no other attempt has.
    ///
    /// After an unknown outcome, a superseded attempt or a rejection
    /// that is not definitive, this is the way forward, with the value
    /// read again. Once the store answered that an epoch was opened
    /// after the value last read, the value read again must be higher.
    pub fn submit(&mut self, last_opened: RunEpoch) -> Result<OpenEpoch, SubmitRefused> {
        match self.stage {
            OpeningStage::Idle | OpeningStage::Unknown | OpeningStage::Superseded => {}
            OpeningStage::Rejected(reason) if !reason.is_definitive() => {}
            OpeningStage::Rejected(reason) => return Err(SubmitRefused::Rejected(reason)),
            OpeningStage::Submitted => return Err(SubmitRefused::AwaitingReply),
            OpeningStage::Committed(_) | OpeningStage::HandedOver(_) => {
                return Err(SubmitRefused::AlreadyOpened);
            }
            OpeningStage::Faulted { .. } => return Err(SubmitRefused::Faulted),
        }
        if last_opened < self.read {
            return Err(SubmitRefused::LedgerRegressed);
        }
        if self.read_is_stale && last_opened == self.read {
            return Err(SubmitRefused::ConditionUnchanged);
        }
        let ticket = self
            .ticket
            .checked_next()
            .map_err(|Exhausted| SubmitRefused::TicketsExhausted)?;
        let command = OpenEpoch {
            attempt: OpeningAttempt {
                startup: self.startup,
                ticket,
            },
            incarnation: self.incarnation,
            after: last_opened,
        };
        self.ticket = ticket;
        self.read = last_opened;
        self.read_is_stale = false;
        self.in_flight = Some(command);
        self.stage = OpeningStage::Submitted;
        Ok(command)
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

    /// Takes the store's reply to the attempt in flight.
    ///
    /// A reply whose command is not the one in flight is refused and
    /// changes nothing. Otherwise knowledge only grows: `Submitted`,
    /// `Unknown`, `Superseded`, then one terminal result. A repeated
    /// reply and a late `Unknown` change nothing, and a rejection
    /// repeated with another reason keeps the first one, as
    /// `Knowledge::observe` does. Two things differ from it: once the
    /// attempt is known superseded its commit yields no grant, and a
    /// commit not yet handed over gives way to `Superseded`, because a
    /// grant for a closed epoch is never handed over.
    pub fn observe(&mut self, reply: OpeningReply) -> Result<Observed, ReplyRefused> {
        if let OpeningStage::Faulted { .. } = self.stage {
            return Err(ReplyRefused::Faulted);
        }
        if self.in_flight != Some(reply.command) {
            return Err(ReplyRefused::Uncorrelated {
                attempt: reply.command.attempt,
            });
        }
        let stage = self.stage;
        let next = match reply.outcome {
            OpeningOutcome::Unknown => match stage {
                OpeningStage::Submitted => Some(OpeningStage::Unknown),
                _ => None,
            },
            OpeningOutcome::Superseded => match stage {
                OpeningStage::Submitted | OpeningStage::Unknown | OpeningStage::Committed(_) => {
                    Some(OpeningStage::Superseded)
                }
                OpeningStage::HandedOver(opened) => {
                    return Ok(Observed::GrantedEpochSuperseded(opened));
                }
                _ => None,
            },
            OpeningOutcome::Committed(opened) => {
                // The command is ours: the only commit that answers it
                // is this data set and the epoch after its condition.
                let answers = opened.incarnation == self.incarnation
                    && Ok(opened.epoch) == reply.command.after.checked_next();
                match stage {
                    OpeningStage::Submitted | OpeningStage::Unknown if answers => {
                        Some(OpeningStage::Committed(opened))
                    }
                    OpeningStage::Committed(_)
                    | OpeningStage::HandedOver(_)
                    | OpeningStage::Superseded
                        if answers =>
                    {
                        None
                    }
                    _ => return Err(self.fault(reply.outcome)),
                }
            }
            OpeningOutcome::RejectedCertain(reason) => match stage {
                OpeningStage::Submitted | OpeningStage::Unknown | OpeningStage::Superseded => {
                    Some(OpeningStage::Rejected(reason))
                }
                // Both say that nothing was opened; the first reason stays.
                OpeningStage::Rejected(_) => None,
                _ => return Err(self.fault(reply.outcome)),
            },
        };
        let Some(next) = next else {
            return Ok(Observed::Unchanged);
        };
        self.read_is_stale = matches!(
            next,
            OpeningStage::Superseded | OpeningStage::Rejected(OpeningRejection::PredecessorMoved)
        );
        self.stage = next;
        Ok(Observed::Advanced(next))
    }

    /// Stops the opening for good and reports the contradiction.
    fn fault(&mut self, reported: OpeningOutcome) -> ReplyRefused {
        let established = self.stage;
        let handed_over = match established {
            OpeningStage::HandedOver(opened) => Some(opened),
            _ => None,
        };
        self.stage = OpeningStage::Faulted { handed_over };
        ReplyRefused::Contradictory {
            established,
            reported,
        }
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
    /// stage and authorises nothing. A grant that is out is still
    /// compared after a fault: its holder reads these pages.
    #[must_use]
    pub fn check_recovered(
        &self,
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    ) -> RecoveredEpochCheck {
        match self.stage {
            OpeningStage::Committed(opened)
            | OpeningStage::HandedOver(opened)
            | OpeningStage::Faulted {
                handed_over: Some(opened),
            } => {
                if opened == (EpochOpened { incarnation, epoch }) {
                    RecoveredEpochCheck::Confirms
                } else {
                    RecoveredEpochCheck::Foreign
                }
            }
            OpeningStage::Idle
            | OpeningStage::Submitted
            | OpeningStage::Unknown
            | OpeningStage::Superseded
            | OpeningStage::Rejected(_)
            | OpeningStage::Faulted { handed_over: None } => RecoveredEpochCheck::NotEstablished,
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
    const THIS_START: StartupNonce = StartupNonce::new([1; 16]);
    const OTHER_START: StartupNonce = StartupNonce::new([2; 16]);

    fn opened(epoch: u64) -> EpochOpened {
        EpochOpened {
            incarnation: DATA_SET,
            epoch: RunEpoch::new(epoch),
        }
    }

    fn reply(command: OpenEpoch, outcome: OpeningOutcome) -> OpeningReply {
        OpeningReply { command, outcome }
    }

    fn fresh() -> EpochOpening {
        EpochOpening::new(DATA_SET, THIS_START)
    }

    /// An opening whose first attempt, made after reading epoch 3, is in
    /// flight. Its committed answer is epoch 4.
    fn submitted() -> (EpochOpening, OpenEpoch) {
        let mut opening = fresh();
        let command = opening.submit(RunEpoch::new(3)).expect("idle");
        (opening, command)
    }

    /// An opening that holds the committed result of epoch 4.
    fn committed() -> (EpochOpening, OpenEpoch) {
        let (mut opening, command) = submitted();
        opening
            .observe(reply(command, COMMITTED_4))
            .expect("the answer");
        (opening, command)
    }

    const COMMITTED_4: OpeningOutcome = OpeningOutcome::Committed(EpochOpened {
        incarnation: DATA_SET,
        epoch: RunEpoch::new(4),
    });
    const EXHAUSTED: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::EpochsExhausted);
    const MOVED: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::PredecessorMoved);
    const NO_GRANT_OUT: OpeningStage = OpeningStage::Faulted { handed_over: None };

    fn uncorrelated(command: OpenEpoch) -> Result<Observed, ReplyRefused> {
        Err(ReplyRefused::Uncorrelated {
            attempt: command.attempt,
        })
    }

    #[test]
    fn a_command_carries_its_identity_the_data_set_and_the_condition_read() {
        let mut opening = fresh();
        assert_eq!(opening.stage(), OpeningStage::Idle);
        assert_eq!(opening.incarnation(), DATA_SET);
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Ok(OpenEpoch {
                attempt: OpeningAttempt {
                    startup: THIS_START,
                    ticket: OpeningTicket::new(1),
                },
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
            opening.observe(reply(command, COMMITTED_4)),
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
        // Delivered twice before the hand-over.
        let (mut opening, command) = submitted();
        assert!(matches!(
            opening.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Advanced(_))
        ));
        assert_eq!(
            opening.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Unchanged)
        );
        let _grant = opening.hand_over().expect("committed");
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

        // Delivered again after the hand-over.
        assert_eq!(
            opening.observe(reply(command, COMMITTED_4)),
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
        let mut opening = fresh();
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
        let mut opening = fresh();
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
        assert_ne!(second.attempt, first.attempt);

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
        assert_ne!(second.attempt, first.attempt);
        assert_eq!(second.after, RunEpoch::new(4));

        // The reply of the older attempt shows up late. It answers no
        // attempt in flight: no grant for epoch 4.
        assert_eq!(
            opening.observe(reply(first, COMMITTED_4)),
            uncorrelated(first)
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
    /// them; whichever reply comes, the grant is for the attempt in
    /// flight.
    #[test]
    fn an_older_attempt_on_the_same_condition_never_yields_a_grant() {
        // The newer attempt commits; the older one is rejected late.
        let (mut opening, older) = submitted();
        opening.abandon().expect("awaiting");
        let newer = opening.submit(RunEpoch::new(3)).expect("a new attempt");
        assert_eq!(newer.after, older.after);
        opening
            .observe(reply(newer, COMMITTED_4))
            .expect("the answer");
        let grant = opening.hand_over().expect("committed");
        assert_eq!(opening.observe(reply(older, MOVED)), uncorrelated(older));
        assert_eq!(opening.stage(), OpeningStage::HandedOver(grant.opened()));

        // The older attempt commits; the newer one is rejected, and the
        // older reply, in either order, answers nothing in flight.
        for older_reply_first in [true, false] {
            let (mut opening, older) = submitted();
            opening.abandon().expect("awaiting");
            let newer = opening.submit(RunEpoch::new(3)).expect("a new attempt");
            let late = reply(older, COMMITTED_4);
            if older_reply_first {
                assert_eq!(opening.observe(late), uncorrelated(older));
            }
            assert_eq!(
                opening.observe(reply(newer, MOVED)),
                Ok(Observed::Advanced(OpeningStage::Rejected(
                    OpeningRejection::PredecessorMoved
                )))
            );
            if !older_reply_first {
                assert_eq!(opening.observe(late), uncorrelated(older));
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
                opening.observe(reply(command, COMMITTED_4)),
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

        for (outcome, stage) in [
            (
                EXHAUSTED,
                OpeningStage::Rejected(OpeningRejection::EpochsExhausted),
            ),
            (OpeningOutcome::Superseded, OpeningStage::Superseded),
        ] {
            let (mut settled, command) = submitted();
            settled
                .observe(reply(command, outcome))
                .expect("correlated");
            assert_eq!(
                settled.observe(reply(command, OpeningOutcome::Unknown)),
                Ok(Observed::Unchanged)
            );
            assert_eq!(settled.stage(), stage);
        }
    }

    /// A reply is honoured only for the command in flight. Every field
    /// of the command counts, and every outcome is refused alike.
    #[test]
    fn a_reply_to_any_other_command_is_refused_whatever_it_reports() {
        let outcomes = [
            COMMITTED_4,
            EXHAUSTED,
            MOVED,
            OpeningOutcome::Superseded,
            OpeningOutcome::Unknown,
        ];
        // Nothing was submitted.
        let mut idle = fresh();
        let (_, never_ours) = submitted();
        for outcome in outcomes {
            assert_eq!(
                idle.observe(reply(never_ours, outcome)),
                uncorrelated(never_ours)
            );
        }
        assert_eq!(idle.stage(), OpeningStage::Idle);
        assert!(idle.hand_over().is_err());

        let (mut opening, command) = submitted();
        let others = [
            // Another start-up, with the same ticket and condition: a
            // dead predecessor, or another opening.
            OpenEpoch {
                attempt: OpeningAttempt {
                    startup: OTHER_START,
                    ..command.attempt
                },
                ..command
            },
            OpenEpoch {
                attempt: OpeningAttempt {
                    ticket: OpeningTicket::new(2),
                    ..command.attempt
                },
                ..command
            },
            OpenEpoch {
                incarnation: StoreIncarnation::new(2),
                ..command
            },
            OpenEpoch {
                after: RunEpoch::new(2),
                ..command
            },
        ];
        for other in others {
            for outcome in outcomes {
                assert_eq!(
                    opening.observe(reply(other, outcome)),
                    uncorrelated(other),
                    "{other:?} {outcome:?}"
                );
                assert_eq!(opening.stage(), OpeningStage::Submitted);
            }
        }
        assert!(opening.hand_over().is_err());
        // Its own answer is still expected, and still honoured.
        opening
            .observe(reply(command, COMMITTED_4))
            .expect("the answer");
        assert_eq!(opening.hand_over().expect("committed").opened(), opened(4));
    }

    /// The scenario of the review: opening A and opening B both issue
    /// ticket 1. B's rejection, delivered to A, must not make A's own
    /// commit a contradiction.
    #[test]
    fn the_rejection_of_another_opening_does_not_fault_this_one() {
        let (mut ours, command) = submitted();
        let mut theirs = EpochOpening::new(StoreIncarnation::new(2), OTHER_START);
        let their_command = theirs.submit(RunEpoch::new(3)).expect("idle");
        assert_eq!(their_command.attempt.ticket, command.attempt.ticket);

        assert_eq!(
            ours.observe(reply(their_command, EXHAUSTED)),
            uncorrelated(their_command)
        );
        assert_eq!(ours.stage(), OpeningStage::Submitted);
        assert_eq!(
            ours.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Advanced(OpeningStage::Committed(opened(4))))
        );
        assert!(ours.hand_over().is_ok());
    }

    /// The scenario of the review: a process dies with its first command
    /// held; its successor reads the same value and issues the same
    /// ticket. The store commits one and rejects the other. In both
    /// orders the successor takes only the reply to its own command.
    #[test]
    fn a_restarted_process_is_not_answered_by_its_predecessor() {
        let (_dead, predecessor) = submitted();
        for successor_commits in [true, false] {
            for own_reply_first in [true, false] {
                let mut opening = EpochOpening::new(DATA_SET, OTHER_START);
                let command = opening.submit(RunEpoch::new(3)).expect("idle");
                assert_eq!(command.attempt.ticket, predecessor.attempt.ticket);
                assert_eq!(command.after, predecessor.after);
                assert_ne!(command, predecessor);

                let (own, foreign) = if successor_commits {
                    (COMMITTED_4, MOVED)
                } else {
                    (MOVED, COMMITTED_4)
                };
                if !own_reply_first {
                    assert_eq!(
                        opening.observe(reply(predecessor, foreign)),
                        uncorrelated(predecessor)
                    );
                }
                assert!(matches!(
                    opening.observe(reply(command, own)),
                    Ok(Observed::Advanced(_))
                ));
                let grant = opening.hand_over();
                assert_eq!(grant.is_ok(), successor_commits);
                if own_reply_first {
                    assert_eq!(
                        opening.observe(reply(predecessor, foreign)),
                        uncorrelated(predecessor)
                    );
                }
                assert!(
                    !matches!(opening.stage(), OpeningStage::Faulted { .. }),
                    "the predecessor's reply faulted its successor"
                );
            }
        }
    }

    /// Once the command is recognised as ours, a commit can only be for
    /// this data set and the epoch after the condition. Anything else
    /// cannot be true, and stops the opening.
    #[test]
    fn a_commit_that_cannot_answer_the_command_faults_the_opening() {
        let elsewhere = EpochOpened {
            incarnation: StoreIncarnation::new(2),
            epoch: RunEpoch::new(4),
        };
        let impossible = [
            opened(0),
            opened(1),
            opened(3),
            opened(5),
            opened(u64::MAX),
            elsewhere,
        ];
        for wrong in impossible {
            let (mut opening, command) = submitted();
            let reported = OpeningOutcome::Committed(wrong);
            assert_eq!(
                opening.observe(reply(command, reported)),
                Err(ReplyRefused::Contradictory {
                    established: OpeningStage::Submitted,
                    reported,
                }),
                "{wrong:?}"
            );
            assert_eq!(opening.stage(), NO_GRANT_OUT);
            assert!(opening.hand_over().is_err());
        }
        // At the end of the counter no epoch follows the condition.
        let mut last = fresh();
        let command = last.submit(RunEpoch::new(u64::MAX)).expect("idle");
        assert!(matches!(
            last.observe(reply(command, OpeningOutcome::Committed(opened(0)))),
            Err(ReplyRefused::Contradictory { .. })
        ));
        // From a data set that had no epoch, the answer is epoch 1.
        let mut first = fresh();
        let command = first.submit(RunEpoch::new(0)).expect("idle");
        first
            .observe(reply(command, OpeningOutcome::Committed(opened(1))))
            .expect("the answer");
    }

    /// Two terminal results that disagree stop start-up: the opening is
    /// faulted for good, and the commit that was contradicted yields no
    /// grant. Two different commits are such a pair.
    #[test]
    fn contradictory_results_fault_the_opening_for_good() {
        for reported in [OpeningOutcome::Committed(opened(5)), EXHAUSTED, MOVED] {
            let (mut opening, command) = committed();
            assert_eq!(
                opening.observe(reply(command, reported)),
                Err(ReplyRefused::Contradictory {
                    established: OpeningStage::Committed(opened(4)),
                    reported,
                }),
                "{reported:?}"
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
                COMMITTED_4,
                OpeningOutcome::Unknown,
                OpeningOutcome::Superseded,
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
        }

        // A commit after a rejection is the same fault.
        let (mut rejected, command) = submitted();
        rejected.observe(reply(command, MOVED)).expect("correlated");
        assert!(matches!(
            rejected.observe(reply(command, COMMITTED_4)),
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
    /// taken back. The fault says that one is out, no other follows, and
    /// the pages its holder reads are still compared with it.
    #[test]
    fn a_contradiction_after_the_hand_over_records_the_grant_that_is_out() {
        for reported in [OpeningOutcome::Committed(opened(5)), MOVED] {
            let (mut opening, command) = committed();
            let grant = opening.hand_over().expect("committed");
            assert!(matches!(
                opening.observe(reply(command, reported)),
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
            assert_eq!(
                opening.check_recovered(DATA_SET, RunEpoch::new(4)),
                RecoveredEpochCheck::Confirms
            );
            assert_eq!(
                opening.check_recovered(DATA_SET, RunEpoch::new(5)),
                RecoveredEpochCheck::Foreign
            );
            assert_eq!(
                opening.check_recovered(StoreIncarnation::new(2), RunEpoch::new(4)),
                RecoveredEpochCheck::Foreign
            );
        }
    }

    /// An attempt whose epoch is closed gives no grant: not when the
    /// store says so first, not when its commit was already here.
    #[test]
    fn a_superseded_attempt_gives_no_grant() {
        // Superseded, then the late commit of the same attempt.
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Advanced(OpeningStage::Superseded))
        );
        assert_eq!(
            opening.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Unchanged)
        );
        assert_eq!(opening.stage(), OpeningStage::Superseded);
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Superseded))
        );

        // The commit, then superseded before the hand-over.
        let (mut opening, command) = committed();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Advanced(OpeningStage::Superseded))
        );
        assert!(opening.hand_over().is_err());

        // After the hand-over the grant cannot be taken back: the
        // caller is told, and the stage stays.
        let (mut opening, command) = committed();
        let grant = opening.hand_over().expect("committed");
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::GrantedEpochSuperseded(grant.opened()))
        );
        assert_eq!(opening.stage(), OpeningStage::HandedOver(grant.opened()));
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));

        // It is not a rejection, and a rejection may still follow it.
        let (mut opening, command) = submitted();
        opening
            .observe(reply(command, OpeningOutcome::Superseded))
            .expect("correlated");
        assert_eq!(
            opening.observe(reply(command, MOVED)),
            Ok(Observed::Advanced(OpeningStage::Rejected(
                OpeningRejection::PredecessorMoved
            )))
        );
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Unchanged)
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
                opening.submit(RunEpoch::new(4)),
                Err(SubmitRefused::Rejected(reason))
            );
            assert_eq!(opening.stage(), OpeningStage::Rejected(reason));
        }

        // The predecessor moved: start-up reads again and tries again.
        let (mut opening, command) = submitted();
        opening.observe(reply(command, MOVED)).expect("correlated");
        assert!(opening.hand_over().is_err());
        let again = opening.submit(RunEpoch::new(4)).expect("not definitive");
        assert_ne!(again.attempt, command.attempt);
        assert_eq!(again.after, RunEpoch::new(4));
    }

    /// The store answered that an epoch was opened after the value read.
    /// A value read again that is not higher is a stale read: a command
    /// on it could only be refused again, and start-up would loop.
    #[test]
    fn after_the_store_says_the_condition_moved_the_next_read_must_be_higher() {
        for outcome in [MOVED, OpeningOutcome::Superseded] {
            let (mut opening, command) = submitted();
            opening
                .observe(reply(command, outcome))
                .expect("correlated");
            let stage = opening.stage();
            assert_eq!(
                opening.submit(RunEpoch::new(3)),
                Err(SubmitRefused::ConditionUnchanged),
                "{outcome:?}"
            );
            assert_eq!(
                opening.submit(RunEpoch::new(2)),
                Err(SubmitRefused::LedgerRegressed)
            );
            // The refusals consumed no ticket and changed nothing.
            assert_eq!(opening.stage(), stage);
            let next = opening.submit(RunEpoch::new(4)).expect("a higher read");
            assert_eq!(next.attempt.ticket.get(), command.attempt.ticket.get() + 1);
        }
        // After an unknown outcome the same value may be read again: the
        // attempt may simply not have been applied.
        let (mut opening, _command) = submitted();
        opening.abandon().expect("awaiting");
        assert_eq!(
            opening.submit(RunEpoch::new(2)),
            Err(SubmitRefused::LedgerRegressed)
        );
        assert_eq!(opening.stage(), OpeningStage::Unknown);
        assert!(opening.submit(RunEpoch::new(3)).is_ok());
    }

    #[test]
    fn one_attempt_is_outstanding_and_a_live_opening_is_not_opened_again() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Err(SubmitRefused::AwaitingReply)
        );
        opening
            .observe(reply(command, COMMITTED_4))
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

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Expect {
        Advance(OpeningStage),
        Same,
        Fault,
        GrantSuperseded,
    }

    /// The table of the module documentation, cell by cell.
    #[test]
    fn every_stage_takes_every_reply_as_the_table_says() {
        use Expect::{Advance, Fault, GrantSuperseded, Same};
        let moved = OpeningStage::Rejected(OpeningRejection::PredecessorMoved);
        let exhausted = OpeningStage::Rejected(OpeningRejection::EpochsExhausted);
        let held = OpeningStage::Committed(opened(4));
        let replies = [
            OpeningOutcome::Unknown,
            OpeningOutcome::Superseded,
            COMMITTED_4,
            OpeningOutcome::Committed(opened(5)),
            EXHAUSTED,
        ];
        let table = [
            (
                OpeningStage::Submitted,
                [
                    Advance(OpeningStage::Unknown),
                    Advance(OpeningStage::Superseded),
                    Advance(held),
                    Fault,
                    Advance(exhausted),
                ],
            ),
            (
                OpeningStage::Unknown,
                [
                    Same,
                    Advance(OpeningStage::Superseded),
                    Advance(held),
                    Fault,
                    Advance(exhausted),
                ],
            ),
            (
                OpeningStage::Superseded,
                [Same, Same, Same, Fault, Advance(exhausted)],
            ),
            (moved, [Same, Same, Fault, Fault, Same]),
            (
                held,
                [Same, Advance(OpeningStage::Superseded), Same, Fault, Fault],
            ),
            (
                OpeningStage::HandedOver(opened(4)),
                [Same, GrantSuperseded, Same, Fault, Fault],
            ),
        ];
        for (stage, row) in table {
            for (outcome, expected) in replies.into_iter().zip(row) {
                // Reach the stage by the replies that lead to it.
                let (mut opening, command) = submitted();
                match stage {
                    OpeningStage::Submitted => {}
                    OpeningStage::Unknown => opening.abandon().expect("awaiting"),
                    OpeningStage::Superseded => {
                        opening
                            .observe(reply(command, OpeningOutcome::Superseded))
                            .expect("correlated");
                    }
                    OpeningStage::Rejected(_) => {
                        opening.observe(reply(command, MOVED)).expect("correlated");
                    }
                    OpeningStage::Committed(_) | OpeningStage::HandedOver(_) => {
                        opening
                            .observe(reply(command, COMMITTED_4))
                            .expect("the answer");
                        if let OpeningStage::HandedOver(_) = stage {
                            let _grant = opening.hand_over().expect("committed");
                        }
                    }
                    OpeningStage::Idle | OpeningStage::Faulted { .. } => {
                        panic!("not a row of the table")
                    }
                }
                assert_eq!(opening.stage(), stage);

                let result = opening.observe(reply(command, outcome));
                match expected {
                    Advance(next) => {
                        assert_eq!(
                            result,
                            Ok(Observed::Advanced(next)),
                            "{stage:?} x {outcome:?}"
                        );
                        assert_eq!(opening.stage(), next, "{stage:?} x {outcome:?}");
                    }
                    Same => {
                        assert_eq!(result, Ok(Observed::Unchanged), "{stage:?} x {outcome:?}");
                        assert_eq!(opening.stage(), stage, "{stage:?} x {outcome:?}");
                    }
                    GrantSuperseded => {
                        assert_eq!(
                            result,
                            Ok(Observed::GrantedEpochSuperseded(opened(4))),
                            "{stage:?} x {outcome:?}"
                        );
                        assert_eq!(opening.stage(), stage, "{stage:?} x {outcome:?}");
                    }
                    Fault => {
                        assert_eq!(
                            result,
                            Err(ReplyRefused::Contradictory {
                                established: stage,
                                reported: outcome,
                            }),
                            "{stage:?} x {outcome:?}"
                        );
                        assert!(
                            matches!(opening.stage(), OpeningStage::Faulted { .. }),
                            "{stage:?} x {outcome:?}"
                        );
                        // The last row: a faulted opening refuses all.
                        for again in replies {
                            assert_eq!(
                                opening.observe(reply(command, again)),
                                Err(ReplyRefused::Faulted),
                                "{stage:?} x {outcome:?}"
                            );
                        }
                    }
                }
                // A grant exists exactly when a commit is held.
                let grantable = matches!(opening.stage(), OpeningStage::Committed(_));
                assert_eq!(
                    opening.hand_over().is_ok(),
                    grantable,
                    "{stage:?} x {outcome:?}"
                );
            }
        }
    }

    /// Why the nonce is needed. Two processes are alive on one data set:
    /// the older one was only presumed dead. Both read epoch 3 and both
    /// issue ticket 1. The ledger commits one and rejects the other, in
    /// either order and however often each command is delivered:
    /// exactly one of them obtains a grant.
    #[test]
    fn two_live_openings_on_one_condition_yield_one_grant() {
        use crate::command::EpochLedger;
        for ours_first in [true, false] {
            let read = EpochLedger::from_durable(DATA_SET, RunEpoch::new(3), None);
            let mut openings = [fresh(), EpochOpening::new(DATA_SET, OTHER_START)];
            let commands = openings
                .each_mut()
                .map(|opening| opening.submit(read.last_opened()).expect("idle"));
            assert_eq!(commands[0].attempt.ticket, commands[1].attempt.ticket);
            assert_ne!(commands[0], commands[1]);

            let order = if ours_first { [0, 1] } else { [1, 0] };
            let LedgerOpening::Opens { ledger, .. } = read.open(&commands[order[0]]) else {
                panic!("the first command to arrive does not open");
            };
            let mut grants = Vec::new();
            // Each command is delivered twice, the loser first.
            for index in [order[1], order[0], order[1], order[0]] {
                let outcome = OpeningOutcome::of(DATA_SET, ledger.open(&commands[index]));
                let _ = openings[index].observe(reply(commands[index], outcome));
                if let Ok(grant) = openings[index].hand_over() {
                    grants.push((index, grant));
                }
            }
            assert_eq!(grants.len(), 1, "one epoch, one authority");
            assert_eq!(grants[0].0, order[0]);
            assert_eq!(grants[0].1.opened(), opened(4));
            assert_eq!(
                openings[order[1]].stage(),
                OpeningStage::Rejected(OpeningRejection::PredecessorMoved)
            );
        }
    }

    /// The precondition, made visible: that no two start-ups share a
    /// nonce is what the adapter owes. If two do, their first commands
    /// are one command, the ledger answers both the same commit, and
    /// two grants exist for one epoch. Nothing here can detect it.
    #[test]
    fn two_start_ups_with_one_nonce_obtain_two_grants() {
        use crate::command::EpochLedger;
        let read = EpochLedger::from_durable(DATA_SET, RunEpoch::new(3), None);
        let mut openings = [fresh(), fresh()];
        let commands = openings
            .each_mut()
            .map(|opening| opening.submit(read.last_opened()).expect("idle"));
        assert_eq!(commands[0], commands[1], "the commands are told apart");

        let LedgerOpening::Opens { ledger, .. } = read.open(&commands[0]) else {
            panic!("not opened");
        };
        let mut grants = Vec::new();
        for (opening, command) in openings.iter_mut().zip(commands) {
            let outcome = OpeningOutcome::of(DATA_SET, ledger.open(&command));
            opening
                .observe(reply(command, outcome))
                .expect("correlated");
            grants.push(opening.hand_over().expect("committed"));
        }
        assert_eq!(grants[0].opened(), grants[1].opened());
    }

    #[test]
    fn the_outcome_of_a_ledger_decision_never_calls_a_past_opening_a_rejection() {
        use crate::command::EpochLedger;
        let ledger = EpochLedger::fresh(DATA_SET);
        let (_, command) = submitted();
        let first = OpenEpoch {
            after: RunEpoch::new(0),
            ..command
        };
        let LedgerOpening::Opens { ledger: one, epoch } = ledger.open(&first) else {
            panic!("not opened");
        };
        assert_eq!(
            OpeningOutcome::of(DATA_SET, ledger.open(&first)),
            OpeningOutcome::Committed(EpochOpened {
                incarnation: DATA_SET,
                epoch
            })
        );
        // Presented again: the same commit, not a rejection.
        assert_eq!(
            OpeningOutcome::of(DATA_SET, one.open(&first)),
            OpeningOutcome::Committed(opened(1))
        );
        let second = OpenEpoch {
            attempt: OpeningAttempt {
                startup: OTHER_START,
                ..first.attempt
            },
            after: RunEpoch::new(1),
            ..first
        };
        let LedgerOpening::Opens { ledger: two, .. } = one.open(&second) else {
            panic!("not opened");
        };
        // Its epoch is closed now: superseded, still not a rejection.
        assert_eq!(
            OpeningOutcome::of(DATA_SET, two.open(&first)),
            OpeningOutcome::Superseded
        );
        assert_eq!(
            OpeningOutcome::of(DATA_SET, ledger.open(&second)),
            OpeningOutcome::Unknown
        );
        assert_eq!(
            OpeningOutcome::of(
                DATA_SET,
                LedgerOpening::Rejected(OpeningRejection::PredecessorMoved)
            ),
            MOVED
        );
    }

    #[test]
    fn tickets_are_never_reused_and_never_wrap() {
        let mut opening = fresh();
        let mut seen = Vec::new();
        for _ in 0..5 {
            let command = opening.submit(RunEpoch::new(3)).expect("idle or unknown");
            assert!(!seen.contains(&command.attempt));
            assert_ne!(command.attempt.ticket, OpeningTicket::new(0));
            assert_eq!(command.attempt.startup, THIS_START);
            seen.push(command.attempt);
            opening.abandon().expect("awaiting");
        }

        let mut last = EpochOpening {
            ticket: OpeningTicket::new(u64::MAX - 1),
            stage: OpeningStage::Unknown,
            ..fresh()
        };
        let command = last
            .submit(RunEpoch::new(3))
            .expect("the last ticket is usable");
        assert_eq!(command.attempt.ticket, OpeningTicket::new(u64::MAX));
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
        /// A reply to the attempt with this ticket, made by this
        /// start-up when `ours`, by another one otherwise.
        Reply {
            ticket: u64,
            ours: bool,
            outcome: OpeningOutcome,
        },
    }

    /// Every sequence of five actions: two conditions, two tickets, the
    /// replies of this start-up and of another one with the same
    /// tickets, and every kind of outcome.
    ///
    /// Each refusal is told apart. A reply is taken only when it is to
    /// the command in flight; a commit that is taken is the one answer
    /// to that command; a fault is reported exactly when the replies to
    /// one command cannot all be true; an opening hands over at most one
    /// grant, founded on such a commit, and none once it is faulted or
    /// its attempt is known superseded.
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
        for outcome in [
            COMMITTED_4,
            OpeningOutcome::Committed(opened(5)),
            OpeningOutcome::Committed(opened(0)),
            OpeningOutcome::Committed(elsewhere),
            EXHAUSTED,
            MOVED,
            OpeningOutcome::Superseded,
            OpeningOutcome::Unknown,
        ] {
            for (ticket, ours) in [(1, true), (2, true), (1, false)] {
                actions.push(Action::Reply {
                    ticket,
                    ours,
                    outcome,
                });
            }
        }
        const DEPTH: u32 = 4;
        let base = actions.len();
        let (mut granted_runs, mut faulted_runs, mut foreign_refusals) = (0u32, 0u32, 0u32);
        for run in 0..base.pow(DEPTH) {
            let mut code = run;
            let mut opening = fresh();
            let mut commands: Vec<OpenEpoch> = Vec::new();
            // What the replies taken for the command in flight said.
            let mut commit: Option<EpochOpened> = None;
            let mut rejected = false;
            let mut superseded = false;
            let mut faulted = false;
            let mut grants = Vec::new();
            for _ in 0..DEPTH {
                let action = actions[code % base];
                code /= base;
                match action {
                    Action::Submit(last_opened) => {
                        if let Ok(command) = opening.submit(RunEpoch::new(last_opened)) {
                            assert!(!faulted, "an attempt after a fault");
                            assert!(grants.is_empty(), "an attempt after a grant");
                            commands.push(command);
                            (commit, rejected, superseded) = (None, false, false);
                        }
                    }
                    Action::Abandon => {
                        let _ = opening.abandon();
                    }
                    Action::Reply {
                        ticket,
                        ours,
                        outcome,
                    } => {
                        // The command this reply claims to answer: one
                        // this opening made, or the same one made by
                        // another start-up.
                        let Some(made) = commands
                            .iter()
                            .find(|command| command.attempt.ticket.get() == ticket)
                        else {
                            continue;
                        };
                        let command = if ours {
                            *made
                        } else {
                            OpenEpoch {
                                attempt: OpeningAttempt {
                                    startup: OTHER_START,
                                    ..made.attempt
                                },
                                ..*made
                            }
                        };
                        let in_flight = commands.last() == Some(&command);
                        let answer = EpochOpened {
                            incarnation: DATA_SET,
                            epoch: RunEpoch::new(command.after.get() + 1),
                        };
                        match opening.observe(OpeningReply { command, outcome }) {
                            Ok(_) => {
                                assert!(in_flight, "a reply to another command was taken");
                                assert!(!faulted, "a reply taken after a fault");
                                match outcome {
                                    OpeningOutcome::Committed(opened) => {
                                        assert_eq!(opened, answer, "an impossible commit");
                                        assert!(!rejected, "a commit taken after a rejection");
                                        commit = Some(opened);
                                    }
                                    OpeningOutcome::RejectedCertain(_) => {
                                        assert!(
                                            commit.is_none() || superseded,
                                            "a rejection taken after a commit"
                                        );
                                        rejected = true;
                                    }
                                    OpeningOutcome::Superseded => superseded = true,
                                    OpeningOutcome::Unknown => {}
                                }
                            }
                            Err(ReplyRefused::Uncorrelated { attempt }) => {
                                assert_eq!(attempt, command.attempt);
                                assert!(!in_flight, "the reply in flight was refused");
                                foreign_refusals += u32::from(!ours);
                            }
                            Err(ReplyRefused::Contradictory { .. }) => {
                                assert!(in_flight && !faulted);
                                let impossible = matches!(
                                    outcome,
                                    OpeningOutcome::Committed(opened) if opened != answer
                                );
                                let disagrees = match outcome {
                                    OpeningOutcome::Committed(_) => rejected,
                                    OpeningOutcome::RejectedCertain(_) => {
                                        commit.is_some() && !superseded
                                    }
                                    _ => false,
                                };
                                assert!(impossible || disagrees, "a fault without a cause");
                                faulted = true;
                            }
                            Err(ReplyRefused::Faulted) => assert!(faulted),
                        }
                    }
                    Action::HandOver => {
                        if let Ok(grant) = opening.hand_over() {
                            assert!(!faulted, "a grant after a fault");
                            assert!(!superseded, "a grant for an epoch known closed");
                            assert!(!rejected, "a grant after a rejection");
                            assert_eq!(Some(grant.opened()), commit, "an unfounded grant");
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
        assert!(
            foreign_refusals > 0,
            "no reply of another start-up was tried"
        );
    }
}
