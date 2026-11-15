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
//!                   |  |  \--Superseded--> Superseded --submit, on a read two higher----+--> Submitted
//!                   |  \--Unknown (store)--\                                           |
//!                   \--abandon (deadline)--+--> Unknown --submit----------------------/
//!
//! HandedOver --Superseded--> GrantSuperseded
//!
//! a commit that cannot answer the command, or a commit and a rejection
//! of one attempt --> Faulted
//! ```
//!
//! What a reply to the command in flight does, stage by reply. "same"
//! leaves the stage as it is; "fault" moves to `Faulted`. `Superseded`
//! keeps whether the commit of the attempt was reported.
//!
//! | Stage \\ reply | `Unknown` | `Superseded` | `Committed`, the answer | `Committed`, any other | `RejectedCertain` |
//! | - | - | - | - | - | - |
//! | `Submitted` | `Unknown` | `Superseded` | `Committed` | fault | `Rejected` |
//! | `Unknown` | same | `Superseded` | `Committed` | fault | `Rejected` |
//! | `Superseded`, no commit seen | same | same | `Superseded`, commit seen; no grant | fault | `Rejected` |
//! | `Superseded`, commit seen | same | same | same | fault | fault |
//! | `Rejected` | same | same | fault | fault | same, first reason kept |
//! | `Committed` | same | `Superseded`, commit seen | same | fault | fault |
//! | `HandedOver` | same | `GrantSuperseded` | same | fault | fault |
//! | `GrantSuperseded` | same | same | same | fault | fault |
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
//!   stops start-up: the opening is `Faulted`, for good. `Superseded`
//!   between the two hides nothing: the stage keeps that the commit
//!   was reported.
//! - `hand_over` succeeds once, from `Committed`. The grant is moved to
//!   the creator of owners and is neither `Clone` nor `Copy`.
//! - An unknown outcome gives no grant, whether the store reported it or
//!   start-up stopped waiting (`abandon`). Start-up does not proceed on
//!   an opening it assumes: it reads the last opened epoch again and
//!   submits a new attempt on that condition. Reading the value to form a
//!   condition adopts nothing: the grant still comes from the committed
//!   reply to the new attempt. A value below what the store's replies
//!   established is a stale read and is refused: one more than the
//!   condition after `PredecessorMoved`, two more after `Superseded`.
//! - An older attempt can never undo a newer one. The store opens an
//!   epoch only while the last opened epoch is the one the command
//!   expects (`EpochLedger::open`), so an attempt that was held on its
//!   way and arrives after a later opening committed opens nothing.
//! - Only `RejectedCertain` says that a command opened nothing. An
//!   attempt whose epoch is already closed is `Superseded`: no grant,
//!   and nothing is claimed about what it did.
//! - A store reports a commit only from the ledger that holds it
//!   (`OpeningOutcome::read`): the decision to open is not a commit.
//! - An attempt designates one command. The opener of the last epoch
//!   that asks for the next one under the same identity is rejected
//!   with `AttemptReused` and its start-up stops.
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
//! adapter, and that no two start-ups share one is its precondition:
//! two start-ups with one nonce that read the same value form one
//! command, both are answered with its commit, and nothing here can
//! tell them apart.
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
//! grant superseded, the opening becomes `GrantSuperseded` and stays
//! there. Stopping the holder is start-up's.

use crate::capacity::Resource;
use crate::command::{
    EpochLedger, LedgerOpening, OpenEpoch, OpeningAttempt, OpeningRejection, OpeningTicket,
    StartupNonce,
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
    /// No epoch was opened by this command, and none ever will be.
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
    /// What `ledger` establishes about `command`. Nothing is written.
    ///
    /// A store answers a command with this, read from the ledger that
    /// is durable when it is done with the command: the one it wrote
    /// after `EpochLedger::open` said `Opens`, or the one it left as it
    /// was. `Committed` is built only from the data set and the epoch a
    /// ledger holds, for the command that ledger records as the opener.
    /// A ledger in which the command would open an epoch, and has not,
    /// establishes nothing: a store that could not write, or did not,
    /// has no commit to report.
    #[must_use]
    pub fn read(ledger: &EpochLedger, command: &OpenEpoch) -> Self {
        match ledger.open(command) {
            LedgerOpening::AlreadyOpened { epoch } => Self::Committed(EpochOpened {
                incarnation: ledger.incarnation(),
                epoch,
            }),
            LedgerOpening::Opens { .. } | LedgerOpening::Undecided => Self::Unknown,
            LedgerOpening::Rejected(reason) => Self::RejectedCertain(reason),
            LedgerOpening::Superseded => Self::Superseded,
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
    /// and none will for this attempt. `commit_seen` keeps that the
    /// commit of this attempt was reported, before or after: a rejection
    /// of the same attempt is then a contradiction.
    Superseded { commit_seen: bool },
    /// The last attempt was certainly not committed. The first reason
    /// reported is kept.
    Rejected(OpeningRejection),
    /// The committed result arrived; the grant was not handed over yet.
    Committed(EpochOpened),
    /// The grant of this opening was handed over. It never is again.
    HandedOver(EpochOpened),
    /// The grant was handed over and the store then reported its epoch
    /// closed. Terminal: no grant, no new attempt. The grant cannot be
    /// taken back; its holder is to be stopped by start-up.
    GrantSuperseded(EpochOpened),
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
    /// This process already holds a committed opening, or handed over
    /// its grant. Opening again would close the epoch its own owners
    /// live in; when that epoch is already closed, the process stops
    /// instead of opening another one around them.
    AlreadyOpened,
    /// The store answered with a reason that no new attempt can change.
    Rejected(OpeningRejection),
    /// The opening is faulted.
    Faulted,
    /// The value read is lower than the one this opening read before.
    /// The last opened epoch of a data set never decreases: this is not
    /// the same data set, or not a read of it.
    LedgerRegressed,
    /// The value read is lower than what the store's replies to the last
    /// attempt establish: the read is stale, and a command on it could
    /// only be refused again. After `PredecessorMoved` the epoch after
    /// the condition was opened; after `Superseded` it was also closed,
    /// so the one after it was opened too.
    StaleRead,
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

/// What a correlated reply did to the opening. To be looked at: an
/// advance to `GrantSuperseded` is the only notice that the holder of
/// the grant must be stopped, and the stage keeps it.
#[must_use]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Observed {
    /// The reply taught something: the opening is now at this stage.
    Advanced(OpeningStage),
    /// The reply repeats what is known, or is a late answer that lowers
    /// nothing. The stage did not move.
    Unchanged,
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
    /// The fact describes the epoch of the grant this process handed
    /// over, and the store reported that epoch closed since. The fact
    /// confirms nothing: the holder of the grant is to be stopped.
    Closed,
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
    /// The command of the last attempt, the only one a reply may answer.
    /// The last ticket issued and the last value read are its own.
    in_flight: Option<OpenEpoch>,
    /// The lowest value a read of the last opened epoch can return now:
    /// the value last read, raised by what the store's replies to the
    /// last attempt say was opened since. It never decreases, whatever
    /// the stage does. `Err` when it is past the last epoch: no read
    /// can be accepted any more.
    floor: Result<RunEpoch, Exhausted>,
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
            in_flight: None,
            floor: Ok(RunEpoch::new(0)),
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
    /// read again. That value is never lower than the one read before,
    /// and never lower than what the store's replies established: one
    /// more than the condition after `PredecessorMoved`, two more after
    /// `Superseded`.
    pub fn submit(&mut self, last_opened: RunEpoch) -> Result<OpenEpoch, SubmitRefused> {
        match self.stage {
            OpeningStage::Idle | OpeningStage::Unknown | OpeningStage::Superseded { .. } => {}
            OpeningStage::Rejected(reason) if !reason.is_definitive() => {}
            OpeningStage::Rejected(reason) => return Err(SubmitRefused::Rejected(reason)),
            OpeningStage::Submitted => return Err(SubmitRefused::AwaitingReply),
            OpeningStage::Committed(_)
            | OpeningStage::HandedOver(_)
            | OpeningStage::GrantSuperseded(_) => {
                return Err(SubmitRefused::AlreadyOpened);
            }
            OpeningStage::Faulted { .. } => return Err(SubmitRefused::Faulted),
        }
        let (read, last_ticket) = match self.in_flight {
            Some(last) => (last.after, last.attempt.ticket),
            None => (RunEpoch::new(0), OpeningTicket::new(0)),
        };
        if last_opened < read {
            return Err(SubmitRefused::LedgerRegressed);
        }
        if self.floor.map_or(true, |floor| last_opened < floor) {
            return Err(SubmitRefused::StaleRead);
        }
        let ticket = last_ticket
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
        self.in_flight = Some(command);
        self.floor = Ok(last_opened);
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
    /// commit gives way to `Superseded`, because a grant for a closed
    /// epoch is never handed over. Neither forgets the commit: a
    /// rejection that follows is a contradiction, as it is without the
    /// `Superseded` in between.
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
        let after = reply.command.after;
        // What the reply says the last opened epoch is, at least.
        let mut said = Ok(after);
        let next = match reply.outcome {
            OpeningOutcome::Unknown => match stage {
                OpeningStage::Submitted => Some(OpeningStage::Unknown),
                _ => None,
            },
            OpeningOutcome::Superseded => {
                said = after.checked_next().and_then(RunEpoch::checked_next);
                match stage {
                    OpeningStage::Submitted | OpeningStage::Unknown => {
                        Some(OpeningStage::Superseded { commit_seen: false })
                    }
                    OpeningStage::Committed(_) => {
                        Some(OpeningStage::Superseded { commit_seen: true })
                    }
                    OpeningStage::HandedOver(opened) => Some(OpeningStage::GrantSuperseded(opened)),
                    _ => None,
                }
            }
            OpeningOutcome::Committed(opened) => {
                // The command is ours: the only commit that answers it
                // is this data set and the epoch after its condition.
                let answers = opened.incarnation == self.incarnation
                    && Ok(opened.epoch) == after.checked_next();
                match stage {
                    OpeningStage::Submitted | OpeningStage::Unknown if answers => {
                        Some(OpeningStage::Committed(opened))
                    }
                    OpeningStage::Superseded { commit_seen: false } if answers => {
                        Some(OpeningStage::Superseded { commit_seen: true })
                    }
                    OpeningStage::Superseded { commit_seen: true }
                    | OpeningStage::Committed(_)
                    | OpeningStage::HandedOver(_)
                    | OpeningStage::GrantSuperseded(_)
                        if answers =>
                    {
                        None
                    }
                    _ => return Err(self.fault(reply.outcome)),
                }
            }
            OpeningOutcome::RejectedCertain(reason) => {
                let next = match stage {
                    OpeningStage::Submitted
                    | OpeningStage::Unknown
                    | OpeningStage::Superseded { commit_seen: false } => {
                        Some(OpeningStage::Rejected(reason))
                    }
                    // Both say that nothing was opened; the first reason stays.
                    OpeningStage::Rejected(_) => None,
                    _ => return Err(self.fault(reply.outcome)),
                };
                if reason == OpeningRejection::PredecessorMoved {
                    said = after.checked_next();
                }
                next
            }
        };
        self.floor = match (self.floor, said) {
            (Ok(held), Ok(said)) => Ok(held.max(said)),
            _ => Err(Exhausted),
        };
        let Some(next) = next else {
            return Ok(Observed::Unchanged);
        };
        self.stage = next;
        Ok(Observed::Advanced(next))
    }

    /// Stops the opening for good and reports the contradiction.
    fn fault(&mut self, reported: OpeningOutcome) -> ReplyRefused {
        let established = self.stage;
        let handed_over = match established {
            OpeningStage::HandedOver(opened) | OpeningStage::GrantSuperseded(opened) => {
                Some(opened)
            }
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
            OpeningStage::HandedOver(_) | OpeningStage::GrantSuperseded(_) => {
                Err(HandOverRefused::AlreadyHandedOver)
            }
            stage => Err(HandOverRefused::NotCommitted(stage)),
        }
    }

    /// Compares a recovered `Epoch` fact with this opening. It borrows
    /// immutably: a fact read in a recovery page, however often, moves no
    /// stage and authorises nothing. A grant that is out is still
    /// compared after a fault: its holder reads these pages. Once its
    /// epoch was reported closed, no fact confirms it.
    #[must_use]
    pub fn check_recovered(
        &self,
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    ) -> RecoveredEpochCheck {
        let (opened, matching) = match self.stage {
            OpeningStage::Committed(opened)
            | OpeningStage::HandedOver(opened)
            | OpeningStage::Faulted {
                handed_over: Some(opened),
            } => (opened, RecoveredEpochCheck::Confirms),
            OpeningStage::GrantSuperseded(opened) => (opened, RecoveredEpochCheck::Closed),
            OpeningStage::Idle
            | OpeningStage::Submitted
            | OpeningStage::Unknown
            | OpeningStage::Superseded { .. }
            | OpeningStage::Rejected(_)
            | OpeningStage::Faulted { handed_over: None } => {
                return RecoveredEpochCheck::NotEstablished;
            }
        };
        if opened == (EpochOpened { incarnation, epoch }) {
            matching
        } else {
            RecoveredEpochCheck::Foreign
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

    /// Takes a reply to the command in flight and checks that what
    /// `observe` returns is what happened to the stage.
    fn take(
        opening: &mut EpochOpening,
        command: OpenEpoch,
        outcome: OpeningOutcome,
    ) -> OpeningStage {
        let before = opening.stage();
        let observed = opening
            .observe(reply(command, outcome))
            .expect("a reply to the command in flight, and no contradiction");
        let after = opening.stage();
        match observed {
            Observed::Advanced(stage) => {
                assert_eq!(stage, after);
                assert_ne!(before, after);
            }
            Observed::Unchanged => assert_eq!(before, after),
        }
        after
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
        take(&mut opening, command, COMMITTED_4);
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
    const SUPERSEDED: OpeningStage = OpeningStage::Superseded { commit_seen: false };
    const SUPERSEDED_AFTER_COMMIT: OpeningStage = OpeningStage::Superseded { commit_seen: true };

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

        take(&mut opening, second, OpeningOutcome::Committed(opened(5)));
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
        take(&mut opening, newer, COMMITTED_4);
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
            take(&mut opening, third, OpeningOutcome::Committed(opened(5)));
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
                take(&mut opening, command, OpeningOutcome::Unknown);
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
            (OpeningOutcome::Superseded, SUPERSEDED),
        ] {
            let (mut settled, command) = submitted();
            take(&mut settled, command, outcome);
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
        take(&mut opening, command, COMMITTED_4);
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
        take(&mut first, command, OpeningOutcome::Committed(opened(1)));
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
        take(&mut rejected, command, MOVED);
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
        // Superseded, then the late commit of the same attempt: no
        // grant, and the commit is remembered.
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Advanced(SUPERSEDED))
        );
        assert_eq!(
            opening.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Advanced(SUPERSEDED_AFTER_COMMIT))
        );
        assert_eq!(
            opening.hand_over(),
            Err(HandOverRefused::NotCommitted(SUPERSEDED_AFTER_COMMIT))
        );

        // The commit, then superseded before the hand-over.
        let (mut opening, command) = committed();
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Advanced(SUPERSEDED_AFTER_COMMIT))
        );
        assert!(opening.hand_over().is_err());

        // It is not a rejection, and while no commit was seen a
        // rejection may still follow it.
        let (mut opening, command) = submitted();
        take(&mut opening, command, OpeningOutcome::Superseded);
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

    /// A commit and a certain rejection of one attempt cannot both be
    /// true. `Superseded` between them, or before them, changes nothing
    /// to that: it says the epoch is closed, not that the commit was not
    /// reported.
    #[test]
    fn a_rejection_after_a_commit_is_a_fault_whatever_came_between() {
        let superseded = OpeningOutcome::Superseded;
        for seen in [[COMMITTED_4, superseded], [superseded, COMMITTED_4]] {
            for reported in [MOVED, EXHAUSTED] {
                let (mut opening, command) = submitted();
                for outcome in seen {
                    take(&mut opening, command, outcome);
                }
                assert_eq!(opening.stage(), SUPERSEDED_AFTER_COMMIT, "{seen:?}");
                assert_eq!(
                    opening.observe(reply(command, reported)),
                    Err(ReplyRefused::Contradictory {
                        established: SUPERSEDED_AFTER_COMMIT,
                        reported,
                    }),
                    "{seen:?} then {reported:?}"
                );
                assert_eq!(opening.stage(), NO_GRANT_OUT);
                assert_eq!(
                    opening.submit(RunEpoch::new(5)),
                    Err(SubmitRefused::Faulted)
                );
            }
        }
        // The commit repeated after it was seen teaches nothing.
        let (mut opening, command) = committed();
        take(&mut opening, command, superseded);
        assert_eq!(
            opening.observe(reply(command, COMMITTED_4)),
            Ok(Observed::Unchanged)
        );
    }

    /// The store reports the epoch of a grant that already left closed.
    /// The grant cannot be taken back, so the opening keeps what was
    /// said: a caller that drops the return value still finds it in the
    /// stage, and nothing confirms the grant any more.
    #[test]
    fn a_grant_whose_epoch_was_reported_closed_stays_marked() {
        let (mut opening, command) = committed();
        let grant = opening.hand_over().expect("committed");
        let closed = OpeningStage::GrantSuperseded(grant.opened());
        assert_eq!(
            opening.observe(reply(command, OpeningOutcome::Superseded)),
            Ok(Observed::Advanced(closed))
        );
        for again in [
            OpeningOutcome::Superseded,
            COMMITTED_4,
            OpeningOutcome::Unknown,
        ] {
            assert_eq!(
                opening.observe(reply(command, again)),
                Ok(Observed::Unchanged)
            );
            assert_eq!(opening.stage(), closed);
        }
        assert_eq!(opening.hand_over(), Err(HandOverRefused::AlreadyHandedOver));
        assert_eq!(opening.abandon(), Err(NotAwaiting(closed)));
        // No other epoch is opened around the owners of the closed one.
        for read in [4, 5, 6] {
            assert_eq!(
                opening.submit(RunEpoch::new(read)),
                Err(SubmitRefused::AlreadyOpened)
            );
        }
        assert_eq!(
            opening.check_recovered(DATA_SET, RunEpoch::new(4)),
            RecoveredEpochCheck::Closed
        );
        assert_eq!(
            opening.check_recovered(DATA_SET, RunEpoch::new(5)),
            RecoveredEpochCheck::Foreign
        );
        assert_eq!(
            opening.check_recovered(StoreIncarnation::new(2), RunEpoch::new(4)),
            RecoveredEpochCheck::Foreign
        );
        assert_eq!(opening.stage(), closed);

        // A contradiction afterwards still records the grant that is out.
        assert_eq!(
            opening.observe(reply(command, MOVED)),
            Err(ReplyRefused::Contradictory {
                established: closed,
                reported: MOVED,
            })
        );
        assert_eq!(
            opening.stage(),
            OpeningStage::Faulted {
                handed_over: Some(grant.opened())
            }
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
            take(
                &mut opening,
                command,
                OpeningOutcome::RejectedCertain(reason),
            );
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
        take(&mut opening, command, MOVED);
        assert!(opening.hand_over().is_err());
        let again = opening.submit(RunEpoch::new(4)).expect("not definitive");
        assert_ne!(again.attempt, command.attempt);
        assert_eq!(again.after, RunEpoch::new(4));
    }

    /// The store's replies say what was opened after the value read: one
    /// epoch when the predecessor moved, two when the attempt is
    /// superseded, since the epoch it asked for was opened and closed. A
    /// value read again below that is a stale read: a command on it
    /// could only be refused again, and start-up would loop. The bound
    /// is the ledger's and never comes down, whatever reply follows.
    #[test]
    fn after_the_store_says_the_condition_moved_the_next_read_must_be_higher() {
        let superseded = OpeningOutcome::Superseded;
        // The replies to a command on epoch 3, and the lowest value
        // that may be read afterwards.
        let cases: [(&[OpeningOutcome], u64); 7] = [
            (&[MOVED], 4),
            (&[superseded], 5),
            (&[COMMITTED_4, superseded], 5),
            (&[superseded, COMMITTED_4], 5),
            (&[superseded, MOVED], 5),
            (&[MOVED, superseded], 5),
            // An unknown outcome: the attempt may simply not have been
            // applied, and the same value may be read again.
            (&[OpeningOutcome::Unknown], 3),
        ];
        for (replies, lowest) in cases {
            let (mut opening, command) = submitted();
            for outcome in replies {
                take(&mut opening, command, *outcome);
            }
            let stage = opening.stage();
            assert_eq!(
                opening.submit(RunEpoch::new(2)),
                Err(SubmitRefused::LedgerRegressed),
                "{replies:?}"
            );
            for stale in 3..lowest {
                assert_eq!(
                    opening.submit(RunEpoch::new(stale)),
                    Err(SubmitRefused::StaleRead),
                    "{replies:?}, read {stale}"
                );
            }
            // The refusals consumed no ticket and changed nothing.
            assert_eq!(opening.stage(), stage);
            let next = opening
                .submit(RunEpoch::new(lowest))
                .expect("a read that is high enough");
            assert_eq!(next.after, RunEpoch::new(lowest), "{replies:?}");
            assert_eq!(next.attempt.ticket.get(), command.attempt.ticket.get() + 1);
        }

        // At the end of the epochs nothing wraps: a bound past the last
        // epoch refuses every read.
        for (outcome, accepted) in [(MOVED, Some(u64::MAX)), (superseded, None)] {
            let mut opening = fresh();
            let command = opening.submit(RunEpoch::new(u64::MAX - 1)).expect("idle");
            take(&mut opening, command, outcome);
            for read in [u64::MAX - 1, u64::MAX] {
                let result = opening.submit(RunEpoch::new(read));
                if accepted == Some(read) {
                    assert!(result.is_ok(), "{outcome:?}, read {read}");
                } else {
                    assert_eq!(result, Err(SubmitRefused::StaleRead), "{outcome:?}");
                }
            }
        }
    }

    #[test]
    fn one_attempt_is_outstanding_and_a_live_opening_is_not_opened_again() {
        let (mut opening, command) = submitted();
        assert_eq!(
            opening.submit(RunEpoch::new(3)),
            Err(SubmitRefused::AwaitingReply)
        );
        take(&mut opening, command, COMMITTED_4);
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
    }

    /// The table of the module documentation, cell by cell.
    #[test]
    fn every_stage_takes_every_reply_as_the_table_says() {
        use Expect::{Advance, Fault, Same};
        let moved = OpeningStage::Rejected(OpeningRejection::PredecessorMoved);
        let exhausted = OpeningStage::Rejected(OpeningRejection::EpochsExhausted);
        let held = OpeningStage::Committed(opened(4));
        let out = OpeningStage::HandedOver(opened(4));
        let closed = OpeningStage::GrantSuperseded(opened(4));
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
                    Advance(SUPERSEDED),
                    Advance(held),
                    Fault,
                    Advance(exhausted),
                ],
            ),
            (
                OpeningStage::Unknown,
                [
                    Same,
                    Advance(SUPERSEDED),
                    Advance(held),
                    Fault,
                    Advance(exhausted),
                ],
            ),
            (
                SUPERSEDED,
                [
                    Same,
                    Same,
                    Advance(SUPERSEDED_AFTER_COMMIT),
                    Fault,
                    Advance(exhausted),
                ],
            ),
            (SUPERSEDED_AFTER_COMMIT, [Same, Same, Same, Fault, Fault]),
            (moved, [Same, Same, Fault, Fault, Same]),
            (
                held,
                [Same, Advance(SUPERSEDED_AFTER_COMMIT), Same, Fault, Fault],
            ),
            (out, [Same, Advance(closed), Same, Fault, Fault]),
            (closed, [Same, Same, Same, Fault, Fault]),
        ];
        for (stage, row) in table {
            for (outcome, expected) in replies.into_iter().zip(row) {
                // Reach the stage by the replies that lead to it.
                let (mut opening, command) = submitted();
                let lead: &[OpeningOutcome] = match stage {
                    OpeningStage::Submitted => &[],
                    OpeningStage::Unknown => &[OpeningOutcome::Unknown],
                    OpeningStage::Superseded { commit_seen: false } => {
                        &[OpeningOutcome::Superseded]
                    }
                    OpeningStage::Superseded { commit_seen: true } => {
                        &[COMMITTED_4, OpeningOutcome::Superseded]
                    }
                    OpeningStage::Rejected(_) => &[MOVED],
                    OpeningStage::Committed(_) | OpeningStage::HandedOver(_) => &[COMMITTED_4],
                    OpeningStage::GrantSuperseded(_) => &[COMMITTED_4],
                    OpeningStage::Idle | OpeningStage::Faulted { .. } => {
                        panic!("not a row of the table")
                    }
                };
                for outcome in lead {
                    take(&mut opening, command, *outcome);
                }
                let grant_out = match stage {
                    OpeningStage::HandedOver(_) | OpeningStage::GrantSuperseded(_) => {
                        Some(opening.hand_over().expect("committed").opened())
                    }
                    _ => None,
                };
                if stage == closed {
                    take(&mut opening, command, OpeningOutcome::Superseded);
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
                    Fault => {
                        assert_eq!(
                            result,
                            Err(ReplyRefused::Contradictory {
                                established: stage,
                                reported: outcome,
                            }),
                            "{stage:?} x {outcome:?}"
                        );
                        assert_eq!(
                            opening.stage(),
                            OpeningStage::Faulted {
                                handed_over: grant_out
                            },
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

    /// A ledger whose last opened epoch is `epoch`, opened by an
    /// attempt of some earlier start-up.
    fn ledger_at(epoch: u64) -> EpochLedger {
        let opener = OpeningAttempt {
            startup: StartupNonce::new([9; 16]),
            ticket: OpeningTicket::new(1),
        };
        EpochLedger::from_durable(DATA_SET, RunEpoch::new(epoch), Some(opener))
            .expect("an epoch and its opener")
    }

    /// A store reduced to its ledger: it decides, writes what the
    /// decision gives it, and answers from what is then durable.
    fn apply(ledger: &mut EpochLedger, command: &OpenEpoch) -> OpeningOutcome {
        if let LedgerOpening::Opens { ledger: next } = ledger.open(command) {
            *ledger = next;
        }
        OpeningOutcome::read(ledger, command)
    }

    /// Why the nonce is needed. Two processes are alive on one data set:
    /// the older one was only presumed dead. Both read epoch 3 and both
    /// issue ticket 1. The ledger commits one and rejects the other, in
    /// either order and however often each command is delivered:
    /// exactly one of them obtains a grant.
    #[test]
    fn two_live_openings_on_one_condition_yield_one_grant() {
        for ours_first in [true, false] {
            let mut ledger = ledger_at(3);
            let mut openings = [fresh(), EpochOpening::new(DATA_SET, OTHER_START)];
            let commands = openings
                .each_mut()
                .map(|opening| opening.submit(ledger.last_opened()).expect("idle"));
            assert_eq!(commands[0].attempt.ticket, commands[1].attempt.ticket);
            assert_ne!(commands[0], commands[1]);

            let order = if ours_first { [0, 1] } else { [1, 0] };
            let mut grants = Vec::new();
            // Each command is delivered three times.
            for index in [order[0], order[1], order[1], order[0], order[1], order[0]] {
                let outcome = apply(&mut ledger, &commands[index]);
                take(&mut openings[index], commands[index], outcome);
                if let Ok(grant) = openings[index].hand_over() {
                    grants.push((index, grant));
                }
            }
            assert_eq!(ledger.open_epoch(), Some(RunEpoch::new(4)));
            assert_eq!(grants.len(), 1, "one epoch, one authority");
            assert_eq!(grants[0].0, order[0]);
            assert_eq!(grants[0].1.opened(), opened(4));
            assert_eq!(
                openings[order[1]].stage(),
                OpeningStage::Rejected(OpeningRejection::PredecessorMoved)
            );
        }
    }

    /// The precondition, where it is seen broken. A start-up drew the
    /// nonce of the one that opened the open epoch. It reads that epoch
    /// and asks for the next under an identity that already designates
    /// another command. The store opens nothing, the epoch of the first
    /// grant stays open, and the second start-up stops for good.
    #[test]
    fn a_start_up_that_reused_the_nonce_of_the_opener_opens_nothing() {
        let mut ledger = ledger_at(3);
        let mut first = fresh();
        let opener = first.submit(ledger.last_opened()).expect("idle");
        let outcome = apply(&mut ledger, &opener);
        take(&mut first, opener, outcome);
        let grant = first.hand_over().expect("committed");
        assert_eq!(grant.opened(), opened(4));

        let mut second = fresh();
        let reused = second.submit(ledger.last_opened()).expect("idle");
        assert_eq!(reused.attempt, opener.attempt);
        assert_ne!(reused, opener);
        let outcome = apply(&mut ledger, &reused);
        let stopped = OpeningRejection::AttemptReused;
        assert_eq!(outcome, OpeningOutcome::RejectedCertain(stopped));
        assert_eq!(ledger.open_epoch(), Some(grant.epoch()));
        assert_eq!(
            second.observe(reply(reused, outcome)),
            Ok(Observed::Advanced(OpeningStage::Rejected(stopped)))
        );
        assert_eq!(
            second.hand_over(),
            Err(HandOverRefused::NotCommitted(OpeningStage::Rejected(
                stopped
            )))
        );
        for read in [4, 5] {
            assert_eq!(
                second.submit(RunEpoch::new(read)),
                Err(SubmitRefused::Rejected(stopped))
            );
        }
        // The first start-up is still answered as the opener.
        assert_eq!(apply(&mut ledger, &opener), COMMITTED_4);
    }

    /// The limit of that detection, named. Two start-ups with one nonce
    /// that read the same value form one and the same command. The
    /// ledger answers a command presented again as the one that opened
    /// the epoch, so both are told they committed and two grants exist
    /// for one epoch. Nothing here can tell them apart: that no two
    /// start-ups share a nonce is what the adapter owes.
    #[test]
    fn two_start_ups_with_one_nonce_obtain_two_grants() {
        let mut ledger = ledger_at(3);
        let mut openings = [fresh(), fresh()];
        let commands = openings
            .each_mut()
            .map(|opening| opening.submit(ledger.last_opened()).expect("idle"));
        assert_eq!(
            commands[0], commands[1],
            "one nonce and one condition make one command"
        );

        let mut grants = Vec::new();
        for (opening, command) in openings.iter_mut().zip(commands) {
            let outcome = apply(&mut ledger, &command);
            take(opening, command, outcome);
            grants.push(opening.hand_over().expect("committed"));
        }
        assert_eq!(grants[0].opened(), grants[1].opened());
    }

    /// A commit is reported from a ledger that holds it, and from
    /// nothing else: the data set and the epoch are the ledger's, and
    /// the ledger names the command as its opener.
    #[test]
    fn a_commit_is_read_only_from_a_ledger_that_holds_it() {
        let read = ledger_at(3);
        let (_, command) = submitted();
        // The command would open epoch 4 and has not: no commit.
        let LedgerOpening::Opens { ledger: written } = read.open(&command) else {
            panic!("not opened");
        };
        assert_eq!(
            OpeningOutcome::read(&read, &command),
            OpeningOutcome::Unknown
        );
        assert_eq!(OpeningOutcome::read(&written, &command), COMMITTED_4);

        // A store that could not write answers from what is durable,
        // and its start-up gets no grant.
        let (mut opening, command) = submitted();
        let durable = read;
        take(
            &mut opening,
            command,
            OpeningOutcome::read(&durable, &command),
        );
        assert_eq!(opening.stage(), OpeningStage::Unknown);
        assert!(opening.hand_over().is_err());

        // The data set of a commit is the ledger's own.
        let elsewhere = StoreIncarnation::new(2);
        let other_set = EpochLedger::from_durable(elsewhere, read.last_opened(), read.opened_by())
            .expect("an epoch and its opener");
        let there = OpenEpoch {
            incarnation: elsewhere,
            ..command
        };
        let LedgerOpening::Opens { ledger: written } = other_set.open(&there) else {
            panic!("not opened");
        };
        assert_eq!(
            OpeningOutcome::read(&written, &there),
            OpeningOutcome::Committed(EpochOpened {
                incarnation: elsewhere,
                epoch: RunEpoch::new(4),
            })
        );
        assert_eq!(
            OpeningOutcome::read(&written, &command),
            OpeningOutcome::RejectedCertain(OpeningRejection::ForeignIncarnation)
        );
    }

    #[test]
    fn the_outcome_read_from_a_ledger_never_calls_a_past_opening_a_rejection() {
        let mut ledger = EpochLedger::fresh(DATA_SET);
        let (_, command) = submitted();
        let first = OpenEpoch {
            after: RunEpoch::new(0),
            ..command
        };
        let second = OpenEpoch {
            attempt: OpeningAttempt {
                startup: OTHER_START,
                ..first.attempt
            },
            after: RunEpoch::new(1),
            ..first
        };
        // A condition ahead of the ledger: nothing certain.
        assert_eq!(apply(&mut ledger, &second), OpeningOutcome::Unknown);
        assert_eq!(ledger, EpochLedger::fresh(DATA_SET));

        assert_eq!(
            apply(&mut ledger, &first),
            OpeningOutcome::Committed(opened(1))
        );
        // Presented again: the same commit, not a rejection.
        assert_eq!(
            apply(&mut ledger, &first),
            OpeningOutcome::Committed(opened(1))
        );
        // A rival on the same condition is the one certainly rejected.
        let rival = OpenEpoch {
            attempt: OpeningAttempt {
                ticket: OpeningTicket::new(2),
                ..second.attempt
            },
            ..first
        };
        assert_eq!(apply(&mut ledger, &rival), MOVED);

        assert_eq!(
            apply(&mut ledger, &second),
            OpeningOutcome::Committed(opened(2))
        );
        // Their epoch is closed now: superseded, still not a rejection,
        // for the one that opened it and for the one that did not.
        for command in [first, rival] {
            assert_eq!(apply(&mut ledger, &command), OpeningOutcome::Superseded);
        }
        assert_eq!(ledger.open_epoch(), Some(RunEpoch::new(2)));
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
            in_flight: Some(OpenEpoch {
                attempt: OpeningAttempt {
                    startup: THIS_START,
                    ticket: OpeningTicket::new(u64::MAX - 1),
                },
                incarnation: DATA_SET,
                after: RunEpoch::new(3),
            }),
            floor: Ok(RunEpoch::new(3)),
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

    /// One state of the search: the opening, field by field, and what
    /// an observer of the replies it took knows about the attempt in
    /// flight. The observer never looks at the stage.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Explored {
        in_flight: Option<OpenEpoch>,
        floor: Result<RunEpoch, Exhausted>,
        stage: OpeningStage,
        /// Every command this opening made, oldest first.
        commands: Vec<OpenEpoch>,
        /// No reply was taken for the attempt and it was not abandoned.
        awaiting: bool,
        commit: Option<EpochOpened>,
        /// The first rejection taken.
        rejected: Option<OpeningRejection>,
        moved: bool,
        superseded: bool,
        faulted: bool,
        granted: Option<EpochOpened>,
    }

    impl Explored {
        /// The lowest value the next read may have, as the replies
        /// taken establish it.
        fn lowest_read(&self) -> u64 {
            let Some(last) = self.commands.last() else {
                return 0;
            };
            last.after.get()
                + if self.superseded {
                    2
                } else {
                    u64::from(self.moved)
                }
        }

        /// Applies one action to a copy of the opening, checks what it
        /// did against the observer, and returns the state it leads to.
        fn step(&self, action: Action) -> Self {
            let mut opening = EpochOpening {
                incarnation: DATA_SET,
                startup: THIS_START,
                in_flight: self.in_flight,
                floor: self.floor,
                stage: self.stage,
            };
            let mut next = self.clone();
            let definitive = self.rejected.is_some_and(OpeningRejection::is_definitive);
            // A grant is due: a commit, and nothing that takes it back.
            let due = self.commit.is_some()
                && self.rejected.is_none()
                && !self.superseded
                && !self.faulted
                && self.granted.is_none();
            match action {
                Action::Submit(read) => {
                    let regressed = self
                        .commands
                        .last()
                        .is_some_and(|last| read < last.after.get());
                    let expected = if self.faulted {
                        Err(SubmitRefused::Faulted)
                    } else if self.awaiting {
                        Err(SubmitRefused::AwaitingReply)
                    } else if self.granted.is_some() || due {
                        Err(SubmitRefused::AlreadyOpened)
                    } else if let (Some(reason), true) = (self.rejected, definitive) {
                        Err(SubmitRefused::Rejected(reason))
                    } else if regressed {
                        Err(SubmitRefused::LedgerRegressed)
                    } else if read < self.lowest_read() {
                        Err(SubmitRefused::StaleRead)
                    } else {
                        Ok(())
                    };
                    let result = opening.submit(RunEpoch::new(read));
                    assert_eq!(result.map(|_| ()), expected, "{self:?} then {action:?}");
                    if let Ok(command) = result {
                        assert!(!self.commands.contains(&command), "a command made twice");
                        assert_eq!(command.after, RunEpoch::new(read));
                        assert_eq!(command.incarnation, DATA_SET);
                        next.commands.push(command);
                        next.awaiting = true;
                        next.commit = None;
                        next.rejected = None;
                        next.moved = false;
                        next.superseded = false;
                    }
                }
                Action::Abandon => {
                    let result = opening.abandon();
                    assert_eq!(result.is_ok(), self.awaiting && !self.faulted);
                    next.awaiting &= result.is_err();
                }
                Action::HandOver => match opening.hand_over() {
                    Ok(grant) => {
                        assert!(due, "an unfounded grant: {self:?}");
                        assert_eq!(Some(grant.opened()), self.commit);
                        next.granted = Some(grant.opened());
                    }
                    Err(_) => assert!(!due, "a grant that is due was refused: {self:?}"),
                },
                Action::Reply {
                    ticket,
                    ours,
                    outcome,
                } => {
                    // The command this reply claims to answer: one this
                    // opening made, or the same one made by another
                    // start-up.
                    let Some(made) = self
                        .commands
                        .iter()
                        .find(|command| command.attempt.ticket.get() == ticket)
                    else {
                        return next;
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
                    let in_flight = self.commands.last() == Some(&command);
                    let answer = EpochOpened {
                        incarnation: DATA_SET,
                        epoch: RunEpoch::new(command.after.get() + 1),
                    };
                    // The replies to one command cannot all be true.
                    let contradicts = match outcome {
                        OpeningOutcome::Committed(opened) => {
                            opened != answer || self.rejected.is_some()
                        }
                        OpeningOutcome::RejectedCertain(_) => self.commit.is_some(),
                        OpeningOutcome::Superseded | OpeningOutcome::Unknown => false,
                    };
                    match opening.observe(OpeningReply { command, outcome }) {
                        Ok(_) => {
                            assert!(in_flight, "a reply to another command was taken");
                            assert!(!self.faulted, "a reply taken after a fault");
                            assert!(!contradicts, "{self:?} took {outcome:?}");
                            next.awaiting = false;
                            match outcome {
                                OpeningOutcome::Committed(opened) => next.commit = Some(opened),
                                OpeningOutcome::RejectedCertain(reason) => {
                                    next.rejected = self.rejected.or(Some(reason));
                                    next.moved |= reason == OpeningRejection::PredecessorMoved;
                                }
                                OpeningOutcome::Superseded => next.superseded = true,
                                OpeningOutcome::Unknown => {}
                            }
                        }
                        Err(ReplyRefused::Uncorrelated { attempt }) => {
                            assert_eq!(attempt, command.attempt);
                            assert!(!self.faulted && !in_flight, "{self:?} refused {outcome:?}");
                        }
                        Err(ReplyRefused::Contradictory { .. }) => {
                            assert!(in_flight && !self.faulted);
                            assert!(contradicts, "a fault without a cause: {self:?}");
                            next.faulted = true;
                        }
                        Err(ReplyRefused::Faulted) => assert!(self.faulted),
                    }
                }
            }
            next.in_flight = opening.in_flight;
            next.floor = opening.floor;
            next.stage = opening.stage;
            // What the stage says is what the observer saw.
            assert_eq!(
                matches!(next.stage, OpeningStage::Faulted { .. }),
                next.faulted
            );
            if next.superseded && !next.faulted {
                assert!(
                    !matches!(
                        next.stage,
                        OpeningStage::Committed(_) | OpeningStage::HandedOver(_)
                    ),
                    "an epoch known closed is still presented as held: {next:?}"
                );
            }
            next
        }
    }

    /// Every state an opening can reach within four attempts, by any
    /// sequence of actions of any length: four conditions, the replies
    /// of this start-up and of another one with the same tickets, and
    /// every kind of outcome. The search stops when no action leads to
    /// a state not yet seen; the bound is the number of attempts, not
    /// the number of actions.
    ///
    /// Each refusal of `submit` is the one the replies call for. A
    /// reply is taken only when it is to the command in flight; a fault
    /// is reported exactly when the replies to one command cannot all
    /// be true, whatever came between them; an opening hands over
    /// exactly the grant that is due, once; and a read below what the
    /// store established is refused.
    #[test]
    fn no_reachable_state_within_four_attempts_breaks_the_opening() {
        const ATTEMPTS: usize = 4;
        let elsewhere = EpochOpened {
            incarnation: StoreIncarnation::new(2),
            epoch: RunEpoch::new(4),
        };
        let mut actions = vec![
            Action::Submit(3),
            Action::Submit(4),
            Action::Submit(5),
            Action::Submit(6),
            Action::Abandon,
            Action::HandOver,
        ];
        for outcome in [
            COMMITTED_4,
            OpeningOutcome::Committed(opened(5)),
            OpeningOutcome::Committed(opened(6)),
            OpeningOutcome::Committed(opened(7)),
            OpeningOutcome::Committed(elsewhere),
            EXHAUSTED,
            MOVED,
            OpeningOutcome::Superseded,
            OpeningOutcome::Unknown,
        ] {
            for ticket in 1..=4 {
                for ours in [true, false] {
                    actions.push(Action::Reply {
                        ticket,
                        ours,
                        outcome,
                    });
                }
            }
        }
        let start = fresh();
        let mut seen = vec![Explored {
            in_flight: start.in_flight,
            floor: start.floor,
            stage: start.stage,
            commands: Vec::new(),
            awaiting: false,
            commit: None,
            rejected: None,
            moved: false,
            superseded: false,
            faulted: false,
            granted: None,
        }];
        // Breadth first: `seen[from..]` is the frontier of one depth.
        let (mut from, mut frontiers, mut transitions) = (0, 0u32, 0u32);
        while from < seen.len() {
            let until = seen.len();
            for index in from..until {
                let state = seen[index].clone();
                for action in &actions {
                    if matches!(action, Action::Submit(_)) && state.commands.len() == ATTEMPTS {
                        continue;
                    }
                    let next = state.step(*action);
                    transitions += 1;
                    if !seen.contains(&next) {
                        seen.push(next);
                    }
                }
            }
            from = until;
            frontiers += 1;
        }
        let reached = |wanted: fn(&Explored) -> bool| seen.iter().any(wanted);
        assert!(reached(|state| state.granted.is_some() && state.superseded));
        assert!(reached(|state| state.faulted && state.granted.is_some()));
        assert!(reached(|state| state.commit.is_some()
            && state.superseded
            && state.faulted));
        assert!(reached(
            |state| state.commands.len() == ATTEMPTS && state.granted.is_some()
        ));
        // The size of what was searched, pinned so that a change of
        // the machine or of the search shows: the states, the actions
        // applied to them, and the frontiers. The farthest state is
        // eleven actions away from the start.
        assert_eq!((seen.len(), transitions, frontiers), (1795, 136_370, 12));
    }
}
