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
//! The opening keeps no stage. It keeps what the replies taken for
//! the command in flight establish about the ledger, and the stage, the
//! bound on the next read and every refusal are read from that. Each
//! reply establishes one durable fact, so the result does not depend on
//! the order in which replies arrive:
//!
//! | Reply | What it establishes | Needs |
//! | - | - | - |
//! | `Unknown` | nothing | |
//! | `Committed`, the answer | this command opened the epoch after its condition | that epoch exists |
//! | `PredecessorMoved` | another attempt opened that epoch | that epoch exists |
//! | `Superseded` | that epoch is closed: the one after it was opened | two epochs after the condition exist |
//! | `AttemptReused` | the epoch of the condition was opened under this identity | |
//! | `EpochsExhausted` | the condition is the last epoch, opened by another attempt | the condition is the last epoch |
//! | `ForeignIncarnation` | the store holds another data set | |
//!
//! "The answer" is the commit for this data set and the epoch right
//! after the command's condition; any other commit establishes nothing
//! a ledger could hold.
//!
//! Two domains of replies are kept apart.
//!
//! - *What a ledger produces.* A store that follows `EpochLedger`
//!   reports, for one command and over one history, only facts that
//!   hold together. With such replies, late, repeated or out of order,
//!   the opening never faults, hands over a grant only for an epoch its
//!   command opened, and at every point start-up can read the ledger
//!   and submit again or has a definitive reason not to.
//! - *What no ledger produces.* One rule: a recognised reply that
//!   cannot be true of any ledger of which the replies already taken
//!   are true faults the opening. Another data set answers nothing
//!   else; the last epoch opened by another attempt is the last state
//!   of a ledger; a command that opened the epoch after its condition
//!   was not preceded there (`PredecessorMoved`) and did not follow its
//!   own identity (`AttemptReused`). Nothing else is a contradiction.
//!   What was established before the fault stays established.
//!
//! The stage reads the facts in this order: a fault; a grant that left
//! (`GrantSuperseded` once its epoch is closed, `HandedOver` before); a
//! definitive rejection; `PredecessorMoved`; `Superseded`, which keeps
//! whether the commit was reported; `Committed`; then `Submitted` or
//! `Unknown`. A reply to any other command is refused and changes
//! nothing.
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
//!   between the two hides nothing, and a fault takes nothing back: a
//!   grant whose epoch was reported closed stays unconfirmed.
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
//! is the one function that constructs an `EpochGrant`. That the
//! creation authority is constructed by consuming a grant, and in no
//! other way, is the rule lot L1 realises in `effects.rs` with
//! `OwnerAuthority::from_grant`, its only constructor. This module
//! issues the grant; what consumes it is not in this file.
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

/// What the replies taken for one command establish about the ledger.
/// Each is a durable fact: once true of a data set it stays true, so the
/// set only grows, in whatever order the replies arrive. `Unknown`
/// establishes nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Established {
    /// `Committed`: this command opened the epoch after its condition.
    committed: Option<EpochOpened>,
    /// `PredecessorMoved`: another attempt opened that epoch.
    moved: bool,
    /// `Superseded`: that epoch was opened and is closed, so the one
    /// after it was opened too.
    closed: bool,
    /// `AttemptReused`: the epoch of the condition was opened under the
    /// identity of this command.
    reused: bool,
    /// `EpochsExhausted`: the condition is the last epoch there is, and
    /// another attempt opened it.
    exhausted: bool,
    /// `ForeignIncarnation`: the store holds another data set.
    foreign: bool,
}

impl Established {
    const NOTHING: Self = Self {
        committed: None,
        moved: false,
        closed: false,
        reused: false,
        exhausted: false,
        foreign: false,
    };

    /// These facts with what `outcome` says of `command`, or `None`
    /// when no ledger can have produced them all: the one rule by which
    /// a recognised reply faults an opening.
    fn taking(
        self,
        incarnation: StoreIncarnation,
        command: &OpenEpoch,
        outcome: OpeningOutcome,
    ) -> Option<Self> {
        let next = command.after.checked_next();
        let mut seen = self;
        match outcome {
            OpeningOutcome::Unknown => {}
            OpeningOutcome::Committed(opened) => {
                // The one commit that answers the command: this data
                // set and the epoch after its condition.
                if opened.incarnation != incarnation || Ok(opened.epoch) != next {
                    return None;
                }
                seen.committed = Some(opened);
            }
            OpeningOutcome::Superseded => {
                // Two epochs past the condition must exist.
                if next.and_then(RunEpoch::checked_next).is_err() {
                    return None;
                }
                seen.closed = true;
            }
            OpeningOutcome::RejectedCertain(reason) => match reason {
                OpeningRejection::PredecessorMoved => {
                    if next.is_err() {
                        return None;
                    }
                    seen.moved = true;
                }
                OpeningRejection::EpochsExhausted => {
                    if next.is_ok() {
                        return None;
                    }
                    seen.exhausted = true;
                }
                OpeningRejection::AttemptReused => seen.reused = true,
                OpeningRejection::ForeignIncarnation => seen.foreign = true,
            },
        }
        seen.coexist().then_some(seen)
    }

    /// Whether one history of one ledger can make all these facts true.
    ///
    /// Another data set answers nothing else, ever. The last epoch,
    /// once opened by another attempt, is the last state of a ledger. A
    /// command that opened the epoch after its condition was not
    /// preceded there by another attempt, and the opener of its
    /// condition was not its own identity, or it would have been
    /// refused.
    fn coexist(self) -> bool {
        let rejected_there = self.moved || self.reused;
        let any_opening = self.committed.is_some() || rejected_there || self.closed;
        !(self.foreign && (any_opening || self.exhausted))
            && !(self.exhausted && any_opening)
            && !(self.committed.is_some() && rejected_there)
    }

    /// How many epochs were opened after the condition, at least.
    fn opened_since(self) -> u8 {
        if self.closed {
            2
        } else {
            u8::from(self.committed.is_some() || self.moved)
        }
    }
}

/// The attempt in flight: its command, whether its reply is still
/// awaited, and what the replies taken for it establish.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Attempt {
    command: OpenEpoch,
    awaiting: bool,
    seen: Established,
}

impl Attempt {
    /// The lowest value a read of the last opened epoch can return now:
    /// the condition, raised by what the replies say was opened since.
    /// `taking` checked that those epochs exist.
    fn floor(&self) -> RunEpoch {
        let mut floor = self.command.after;
        for _ in 0..self.seen.opened_since() {
            if let Ok(next) = floor.checked_next() {
                floor = next;
            }
        }
        floor
    }
}

/// The volatile state that controls one opening of an epoch and the
/// hand-over of its grant. One per store opened by a process; it is not
/// `Clone`, since a copy taken after the commit could hand over twice.
///
/// It holds no stage: it holds what the replies established, and the
/// stage, the bound on the next read and every refusal are read from
/// that.
#[derive(Debug)]
pub struct EpochOpening {
    /// The data set start-up verified before it opened anything.
    incarnation: StoreIncarnation,
    /// What makes the attempts of this start-up its own.
    startup: StartupNonce,
    /// The last attempt, the only one a reply may answer.
    attempt: Option<Attempt>,
    /// The opening whose grant left, once it did.
    handed_over: Option<EpochOpened>,
    /// A recognised reply could not be true together with the others.
    /// What was established before it stays.
    faulted: bool,
}

impl EpochOpening {
    /// An opening on which nothing was submitted, for the data set
    /// start-up verified and with the nonce this start-up drew.
    #[must_use]
    pub const fn new(incarnation: StoreIncarnation, startup: StartupNonce) -> Self {
        Self {
            incarnation,
            startup,
            attempt: None,
            handed_over: None,
            faulted: false,
        }
    }

    /// Where this opening stands. A definitive rejection comes first,
    /// whatever else was established and in whatever order.
    #[must_use]
    pub fn stage(&self) -> OpeningStage {
        if self.faulted {
            return OpeningStage::Faulted {
                handed_over: self.handed_over,
            };
        }
        let Some(attempt) = self.attempt else {
            return OpeningStage::Idle;
        };
        let seen = attempt.seen;
        if let Some(opened) = self.handed_over {
            return if seen.closed {
                OpeningStage::GrantSuperseded(opened)
            } else {
                OpeningStage::HandedOver(opened)
            };
        }
        if seen.foreign {
            OpeningStage::Rejected(OpeningRejection::ForeignIncarnation)
        } else if seen.exhausted {
            OpeningStage::Rejected(OpeningRejection::EpochsExhausted)
        } else if seen.reused {
            OpeningStage::Rejected(OpeningRejection::AttemptReused)
        } else if seen.moved {
            OpeningStage::Rejected(OpeningRejection::PredecessorMoved)
        } else if seen.closed {
            OpeningStage::Superseded {
                commit_seen: seen.committed.is_some(),
            }
        } else if let Some(opened) = seen.committed {
            OpeningStage::Committed(opened)
        } else if attempt.awaiting {
            OpeningStage::Submitted
        } else {
            OpeningStage::Unknown
        }
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
    /// more than the condition after a commit or `PredecessorMoved`,
    /// two more after `Superseded`.
    pub fn submit(&mut self, last_opened: RunEpoch) -> Result<OpenEpoch, SubmitRefused> {
        match self.stage() {
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
        let mut last_ticket = OpeningTicket::new(0);
        if let Some(last) = self.attempt {
            if last_opened < last.command.after {
                return Err(SubmitRefused::LedgerRegressed);
            }
            if last_opened < last.floor() {
                return Err(SubmitRefused::StaleRead);
            }
            last_ticket = last.command.attempt.ticket;
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
        self.attempt = Some(Attempt {
            command,
            awaiting: true,
            seen: Established::NOTHING,
        });
        Ok(command)
    }

    /// Stops waiting for the reply of the attempt in flight: start-up's
    /// deadline passed. This is the process giving up, not the store
    /// reporting; the outcome is unknown either way, and what may follow
    /// is the same. The reply of that attempt is still honoured if it
    /// arrives before a new attempt is submitted.
    pub fn abandon(&mut self) -> Result<(), NotAwaiting> {
        match (self.stage(), &mut self.attempt) {
            (OpeningStage::Submitted, Some(attempt)) => {
                attempt.awaiting = false;
                Ok(())
            }
            (stage, _) => Err(NotAwaiting(stage)),
        }
    }

    /// Takes the store's reply to the attempt in flight.
    ///
    /// A reply whose command is not the one in flight is refused and
    /// changes nothing. Otherwise the reply adds what it establishes
    /// about the ledger to what the earlier replies established. The
    /// result does not depend on the order in which replies arrive, and
    /// a repeated reply or a late `Unknown` changes nothing.
    ///
    /// One rule decides a fault: the reply cannot be true of any ledger
    /// of which the replies already taken are true. A commit that is
    /// not the answer to the command, a commit and a rejection that
    /// says nothing was opened, an epoch closed past the last one: no
    /// store that follows `EpochLedger` produces them. What was
    /// established before the fault stays established.
    pub fn observe(&mut self, reply: OpeningReply) -> Result<Observed, ReplyRefused> {
        if self.faulted {
            return Err(ReplyRefused::Faulted);
        }
        let uncorrelated = ReplyRefused::Uncorrelated {
            attempt: reply.command.attempt,
        };
        let attempt = self.attempt.ok_or(uncorrelated)?;
        if attempt.command != reply.command {
            return Err(uncorrelated);
        }
        let established = self.stage();
        let Some(seen) = attempt
            .seen
            .taking(self.incarnation, &attempt.command, reply.outcome)
        else {
            self.faulted = true;
            return Err(ReplyRefused::Contradictory {
                established,
                reported: reply.outcome,
            });
        };
        self.attempt = Some(Attempt {
            command: attempt.command,
            awaiting: false,
            seen,
        });
        let stage = self.stage();
        if stage == established {
            Ok(Observed::Unchanged)
        } else {
            Ok(Observed::Advanced(stage))
        }
    }

    /// Hands over the grant of the committed opening. This transition is
    /// taken once: the stage moves to `HandedOver` and no later call, no
    /// repeated reply and no recovered fact produces another grant.
    pub fn hand_over(&mut self) -> Result<EpochGrant, HandOverRefused> {
        match self.stage() {
            OpeningStage::Committed(opened) => {
                self.handed_over = Some(opened);
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
    /// epoch was reported closed no fact confirms it, and a fault that
    /// comes later does not bring the confirmation back.
    #[must_use]
    pub fn check_recovered(
        &self,
        incarnation: StoreIncarnation,
        epoch: RunEpoch,
    ) -> RecoveredEpochCheck {
        let (opened, matching) = match (self.handed_over, self.stage()) {
            (Some(opened), _) => {
                let closed = self.attempt.is_some_and(|attempt| attempt.seen.closed);
                let matching = if closed {
                    RecoveredEpochCheck::Closed
                } else {
                    RecoveredEpochCheck::Confirms
                };
                (opened, matching)
            }
            (None, OpeningStage::Committed(opened)) => (opened, RecoveredEpochCheck::Confirms),
            (None, _) => return RecoveredEpochCheck::NotEstablished,
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
                FOREIGN,
                OpeningStage::Rejected(OpeningRejection::ForeignIncarnation),
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

    /// Two rejections of one command are weighed like any two replies.
    /// The same one again changes nothing. A definitive one prevails
    /// over one that is not, in whichever order they arrive: a ledger
    /// answers `AttemptReused` and, once another start-up opened the
    /// epoch, `PredecessorMoved` to the same command. Two that no
    /// ledger reports of one command are a fault.
    #[test]
    fn of_two_rejections_the_definitive_one_prevails_in_either_order() {
        let stopped = OpeningRejection::AttemptReused;
        for replies in [[MOVED, REUSED], [REUSED, MOVED]] {
            let (mut opening, command) = submitted();
            for outcome in replies {
                take(&mut opening, command, outcome);
            }
            assert_eq!(
                opening.stage(),
                OpeningStage::Rejected(stopped),
                "{replies:?}"
            );
            for again in replies {
                assert_eq!(
                    opening.observe(reply(command, again)),
                    Ok(Observed::Unchanged)
                );
            }
            assert_eq!(
                opening.submit(RunEpoch::new(4)),
                Err(SubmitRefused::Rejected(stopped)),
                "{replies:?}"
            );
            assert!(opening.hand_over().is_err());
        }
        for (first, second) in [
            (MOVED, EXHAUSTED),
            (MOVED, FOREIGN),
            (FOREIGN, MOVED),
            (FOREIGN, REUSED),
            (REUSED, FOREIGN),
        ] {
            let (mut opening, command) = submitted();
            let established = take(&mut opening, command, first);
            assert_eq!(
                opening.observe(reply(command, second)),
                Err(ReplyRefused::Contradictory {
                    established,
                    reported: second,
                }),
                "{first:?} then {second:?}"
            );
            assert_eq!(opening.stage(), NO_GRANT_OUT);
        }
    }

    /// A grant left, and its epoch was reported closed. A contradiction
    /// that comes afterwards faults the opening and takes nothing back:
    /// no recovered fact confirms that grant again.
    #[test]
    fn a_fault_does_not_bring_back_the_confirmation_of_a_closed_grant() {
        for reported in [
            MOVED,
            REUSED,
            FOREIGN,
            EXHAUSTED,
            OpeningOutcome::Committed(opened(5)),
        ] {
            let (mut opening, command) = committed();
            let grant = opening.hand_over().expect("committed");
            let closed = take(&mut opening, command, OpeningOutcome::Superseded);
            assert_eq!(closed, OpeningStage::GrantSuperseded(grant.opened()));
            assert_eq!(
                opening.check_recovered(DATA_SET, grant.epoch()),
                RecoveredEpochCheck::Closed
            );
            assert_eq!(
                opening.observe(reply(command, reported)),
                Err(ReplyRefused::Contradictory {
                    established: closed,
                    reported,
                })
            );
            assert_eq!(
                opening.stage(),
                OpeningStage::Faulted {
                    handed_over: Some(grant.opened())
                }
            );
            assert_eq!(
                opening.check_recovered(DATA_SET, grant.epoch()),
                RecoveredEpochCheck::Closed,
                "{reported:?}"
            );
            assert_eq!(
                opening.check_recovered(DATA_SET, RunEpoch::new(5)),
                RecoveredEpochCheck::Foreign
            );
        }
    }

    /// Replies that name an epoch past the last one, or that cannot be
    /// true of the data set the earlier ones describe, come from no
    /// ledger. They fault the opening; none leaves it in a stage from
    /// which every read would be refused.
    #[test]
    fn a_reply_no_ledger_can_give_faults_the_opening() {
        let superseded = OpeningOutcome::Superseded;
        let last = OpeningOutcome::Committed(opened(LAST));
        // The condition, what was taken first, and the impossible reply.
        let cases: [(u64, &[OpeningOutcome], OpeningOutcome); 9] = [
            // Nothing is opened after the last epoch.
            (LAST, &[], MOVED),
            (LAST, &[], superseded),
            (LAST, &[], OpeningOutcome::Committed(opened(0))),
            // The last epoch is never closed, grant or not.
            (LAST - 1, &[], superseded),
            (LAST - 1, &[last], superseded),
            // The epochs are not exhausted before the last one.
            (3, &[], EXHAUSTED),
            (3, &[superseded], EXHAUSTED),
            // Another data set has none of these epochs.
            (3, &[FOREIGN], superseded),
            (3, &[superseded], FOREIGN),
        ];
        for (read, taken, impossible) in cases {
            for hand_over in [false, true] {
                let mut opening = fresh();
                let command = opening.submit(RunEpoch::new(read)).expect("idle");
                for outcome in taken {
                    take(&mut opening, command, *outcome);
                }
                let grant = if hand_over {
                    opening.hand_over().ok()
                } else {
                    None
                };
                let established = opening.stage();
                assert_eq!(
                    opening.observe(reply(command, impossible)),
                    Err(ReplyRefused::Contradictory {
                        established,
                        reported: impossible,
                    }),
                    "{read}: {taken:?} then {impossible:?}"
                );
                let out = grant.as_ref().map(EpochGrant::opened);
                assert_eq!(opening.stage(), OpeningStage::Faulted { handed_over: out });
                if let Some(grant) = grant {
                    // The last epoch was not closed: the grant stands.
                    assert_eq!(
                        opening.check_recovered(DATA_SET, grant.epoch()),
                        RecoveredEpochCheck::Confirms
                    );
                }
            }
        }
        // What a ledger does give at the end of the epochs is taken.
        let mut opening = fresh();
        let command = opening.submit(RunEpoch::new(LAST)).expect("idle");
        assert_eq!(
            take(&mut opening, command, EXHAUSTED),
            OpeningStage::Rejected(OpeningRejection::EpochsExhausted)
        );
    }

    /// Whether a new attempt may follow a rejection belongs to its
    /// reason.
    #[test]
    fn only_a_definitive_rejection_ends_the_attempts() {
        for (reason, read) in [
            (OpeningRejection::EpochsExhausted, LAST),
            (OpeningRejection::ForeignIncarnation, 3),
            (OpeningRejection::AttemptReused, 3),
        ] {
            let mut opening = fresh();
            let command = opening.submit(RunEpoch::new(read)).expect("idle");
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
                opening.submit(RunEpoch::new(read)),
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

        // At the end of the epochs the last one may still be read once
        // the predecessor moved. An epoch closed past the last one is
        // the reply of no ledger: it leaves no bound that every read
        // would fail.
        let mut opening = fresh();
        let command = opening.submit(RunEpoch::new(LAST - 1)).expect("idle");
        take(&mut opening, command, MOVED);
        assert_eq!(
            opening.submit(RunEpoch::new(LAST - 1)),
            Err(SubmitRefused::StaleRead)
        );
        assert!(opening.submit(RunEpoch::new(LAST)).is_ok());

        let mut opening = fresh();
        let command = opening.submit(RunEpoch::new(LAST - 1)).expect("idle");
        assert_eq!(
            opening.observe(reply(command, superseded)),
            Err(ReplyRefused::Contradictory {
                established: OpeningStage::Submitted,
                reported: superseded,
            })
        );
        assert_eq!(
            opening.submit(RunEpoch::new(LAST)),
            Err(SubmitRefused::Faulted)
        );
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
            attempt: Some(Attempt {
                command: OpenEpoch {
                    attempt: OpeningAttempt {
                        startup: THIS_START,
                        ticket: OpeningTicket::new(u64::MAX - 1),
                    },
                    incarnation: DATA_SET,
                    after: RunEpoch::new(3),
                },
                awaiting: false,
                seen: Established::NOTHING,
            }),
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

    const REUSED: OpeningOutcome = OpeningOutcome::RejectedCertain(OpeningRejection::AttemptReused);
    const FOREIGN: OpeningOutcome =
        OpeningOutcome::RejectedCertain(OpeningRejection::ForeignIncarnation);
    const LAST: u64 = u64::MAX;

    /// An identity no opening of these tests has: a start-up of its own
    /// for each rank.
    fn stranger(rank: u64) -> OpeningAttempt {
        let mut nonce = [0x77; 16];
        nonce[8..].copy_from_slice(&rank.to_be_bytes());
        OpeningAttempt {
            startup: StartupNonce::new(nonce),
            ticket: OpeningTicket::new(1),
        }
    }

    /// Every set of outcomes one ledger can report for `command` in the
    /// course of one history, found by running `EpochLedger` itself: the
    /// command applied or only read, other start-ups opening epochs, and
    /// a start-up that opens under the identity of the command with
    /// another condition. Nothing here knows how an opening weighs a
    /// reply.
    fn producible(command: OpenEpoch) -> Vec<Vec<OpeningOutcome>> {
        fn visit(
            ledger: EpochLedger,
            command: OpenEpoch,
            depth: u64,
            reported: &[OpeningOutcome],
            sets: &mut Vec<Vec<OpeningOutcome>>,
        ) {
            let mut reported = reported.to_vec();
            let outcome = OpeningOutcome::read(&ledger, &command);
            if outcome != OpeningOutcome::Unknown && !reported.contains(&outcome) {
                reported.push(outcome);
            }
            if !sets.contains(&reported) {
                sets.push(reported.clone());
            }
            if depth == 5 {
                return;
            }
            let twin = OpenEpoch {
                after: ledger.last_opened(),
                ..command
            };
            let rival = OpenEpoch {
                attempt: stranger(depth),
                ..twin
            };
            for opener in [command, twin, rival] {
                if let LedgerOpening::Opens { ledger: next } = ledger.open(&opener) {
                    visit(next, command, depth + 1, &reported, sets);
                }
            }
        }
        let mut sets = Vec::new();
        let before = command.after.get() - 1;
        let start = EpochLedger::from_durable(DATA_SET, RunEpoch::new(before), Some(stranger(9)))
            .expect("an epoch and its opener");
        visit(start, command, 0, &[], &mut sets);
        // Another data set, whatever its epochs, answers one thing.
        let elsewhere = EpochLedger::fresh(StoreIncarnation::new(2));
        visit(elsewhere, command, 4, &[], &mut sets);
        sets
    }

    /// The second domain: replies no ledger produces. One rule covers
    /// them, and this test states it without the opening's own tables.
    /// For every sequence of four replies, with a condition in the
    /// middle of the epochs and at their end, a recognised reply faults
    /// the opening exactly when no history of `EpochLedger` reports it
    /// together with the replies already taken. Until then the stage
    /// does not depend on the order of the replies, and what a recovered
    /// fact is told about a grant never goes back.
    #[test]
    fn a_reply_faults_exactly_when_no_ledger_reports_it_with_those_taken() {
        let (mut faults, mut clean, mut closed_then_faulted) = (0u32, 0u32, 0u32);
        for after in [3, LAST - 1, LAST] {
            let condition = RunEpoch::new(after);
            let command = fresh().submit(condition).expect("idle");
            let sets = producible(command);
            let possible = |taken: &[OpeningOutcome]| {
                sets.iter().any(|set| {
                    taken
                        .iter()
                        .all(|outcome| *outcome == OpeningOutcome::Unknown || set.contains(outcome))
                })
            };
            let answer = condition.checked_next().ok();
            let commit = |epoch: Option<RunEpoch>| {
                OpeningOutcome::Committed(EpochOpened {
                    incarnation: DATA_SET,
                    epoch: epoch.unwrap_or(RunEpoch::new(0)),
                })
            };
            let alphabet = [
                OpeningOutcome::Unknown,
                OpeningOutcome::Superseded,
                commit(answer),
                commit(answer.and_then(|epoch| epoch.checked_next().ok())),
                OpeningOutcome::Committed(EpochOpened {
                    incarnation: StoreIncarnation::new(2),
                    epoch: answer.unwrap_or(condition),
                }),
                MOVED,
                EXHAUSTED,
                REUSED,
                FOREIGN,
            ];
            let base = alphabet.len();
            // The stage a set of replies leads to, whatever their order.
            let settled = |replies: &[usize]| {
                let mut sorted = replies.to_vec();
                sorted.sort_unstable();
                let mut opening = fresh();
                let command = opening.submit(condition).expect("idle");
                for index in sorted {
                    take(&mut opening, command, alphabet[index]);
                }
                opening.stage()
            };
            for hand_over in [false, true] {
                for code in 0..base.pow(4) {
                    let replies: [usize; 4] =
                        core::array::from_fn(|place| code / base.pow(place as u32) % base);
                    let mut opening = fresh();
                    let command = opening.submit(condition).expect("idle");
                    let mut taken = Vec::new();
                    let mut faulted = false;
                    let mut granted = None;
                    let mut closed = false;
                    for (place, index) in replies.into_iter().enumerate() {
                        let outcome = alphabet[index];
                        let before = opening.stage();
                        taken.push(outcome);
                        let result = opening.observe(reply(command, outcome));
                        if faulted {
                            assert_eq!(result, Err(ReplyRefused::Faulted));
                        } else if possible(&taken) {
                            assert!(result.is_ok(), "{after}: {taken:?} gave {result:?}");
                            closed |= granted.is_some() && outcome == OpeningOutcome::Superseded;
                            if !hand_over {
                                assert_eq!(
                                    opening.stage(),
                                    settled(&replies[..=place]),
                                    "{after}: the order of {taken:?} matters"
                                );
                            }
                        } else {
                            assert_eq!(
                                result,
                                Err(ReplyRefused::Contradictory {
                                    established: before,
                                    reported: outcome,
                                }),
                                "{after}: {taken:?}"
                            );
                            faulted = true;
                            closed_then_faulted += u32::from(closed);
                        }
                        if hand_over && let Ok(grant) = opening.hand_over() {
                            assert!(!faulted && granted.is_none());
                            granted = Some(grant.opened());
                        }
                        assert_eq!(
                            matches!(opening.stage(), OpeningStage::Faulted { .. }),
                            faulted
                        );
                        if let Some(opened) = granted {
                            // The grant is out: what a recovered fact
                            // is told only moves from confirmed to
                            // closed, fault or not.
                            let told = opening.check_recovered(opened.incarnation, opened.epoch);
                            let expected = if closed {
                                RecoveredEpochCheck::Closed
                            } else {
                                RecoveredEpochCheck::Confirms
                            };
                            assert_eq!(told, expected, "{after}: {taken:?}");
                        }
                    }
                    faults += u32::from(faulted);
                    clean += u32::from(!faulted);
                }
            }
        }
        assert!(faults > 0 && clean > 0);
        assert!(
            closed_then_faulted > 0,
            "no grant was reported closed before a fault"
        );
    }

    /// One step of a history: what start-up, the store, other start-ups
    /// and the path between them may do.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Step {
        /// Start-up reads the ledger and submits an attempt.
        Submit,
        /// The writer applies the command made at this rank, however
        /// late and however often, and a reply to it exists.
        Apply(usize),
        /// The same, on a medium that cannot write.
        ReadOnly(usize),
        /// Another start-up opens the next epoch.
        Rival,
        /// The precondition of the nonce, broken: a start-up opens the
        /// next epoch under the identity our next attempt will have.
        TwinAhead,
        /// The same under the identity of our last attempt, with
        /// another condition.
        TwinBehind,
        /// The reply produced at this rank reaches start-up: in any
        /// order, any number of times.
        Deliver(usize),
        Abandon,
        HandOver,
    }

    /// A history replayed from its steps, with what really happened.
    struct History {
        ledger: EpochLedger,
        opening: EpochOpening,
        made: Vec<OpenEpoch>,
        replies: Vec<OpeningReply>,
        /// Which command opened which epoch.
        truth: Vec<(RunEpoch, OpenEpoch)>,
        /// The commands the ledger said opened nothing and never will.
        rejected: Vec<OpenEpoch>,
        granted: Option<EpochOpened>,
        /// The replies delivered for the attempt in flight, and whether
        /// it was abandoned.
        delivered: Vec<usize>,
        abandoned: bool,
        told_closed: bool,
        rivals: u64,
    }

    const FIRST_READ: u64 = 3;
    const EPOCHS: u64 = 3;
    const ATTEMPTS: usize = 2;

    impl History {
        fn start() -> Self {
            Self {
                ledger: ledger_at(FIRST_READ),
                opening: fresh(),
                made: Vec::new(),
                replies: Vec::new(),
                truth: Vec::new(),
                rejected: Vec::new(),
                granted: None,
                delivered: Vec::new(),
                abandoned: false,
                told_closed: false,
                rivals: 0,
            }
        }

        /// The store handles `command`: it writes when it may, and
        /// answers from what is durable.
        fn handle(&mut self, command: OpenEpoch, writes: bool) -> OpeningOutcome {
            if writes
                && self.ledger.last_opened().get() < FIRST_READ + EPOCHS
                && let LedgerOpening::Opens { ledger } = self.ledger.open(&command)
            {
                assert!(
                    !self.rejected.contains(&command),
                    "{command:?} was rejected for certain, and opens an epoch"
                );
                self.ledger = ledger;
                self.truth.push((ledger.last_opened(), command));
            }
            let outcome = OpeningOutcome::read(&self.ledger, &command);
            match outcome {
                OpeningOutcome::Committed(opened) => {
                    assert!(self.truth.contains(&(opened.epoch, command)));
                }
                OpeningOutcome::RejectedCertain(_) => {
                    assert!(
                        self.truth.iter().all(|(_, opener)| *opener != command),
                        "{command:?} opened an epoch and is called rejected"
                    );
                    if !self.rejected.contains(&command) {
                        self.rejected.push(command);
                    }
                }
                OpeningOutcome::Superseded | OpeningOutcome::Unknown => {}
            }
            outcome
        }

        /// Takes one step, or says that it does not apply here.
        fn step(&mut self, step: Step) -> bool {
            let last = self.ledger.last_opened();
            let twin = |attempt| OpenEpoch {
                attempt,
                incarnation: DATA_SET,
                after: last,
            };
            match step {
                Step::Submit => {
                    if self.made.len() == ATTEMPTS {
                        return false;
                    }
                    let Ok(command) = self.opening.submit(last) else {
                        return false;
                    };
                    assert_eq!(command.after, last);
                    assert!(self.made.iter().all(|made| made.attempt != command.attempt));
                    self.made.push(command);
                    self.delivered.clear();
                    self.abandoned = false;
                }
                Step::Apply(rank) | Step::ReadOnly(rank) => {
                    let Some(command) = self.made.get(rank).copied() else {
                        return false;
                    };
                    let outcome = self.handle(command, matches!(step, Step::Apply(_)));
                    let reply = OpeningReply { command, outcome };
                    if !self.replies.contains(&reply) {
                        self.replies.push(reply);
                    }
                }
                Step::Rival => {
                    self.rivals += 1;
                    let _outcome = self.handle(twin(stranger(self.rivals)), true);
                }
                Step::TwinAhead => {
                    if self.made.len() == ATTEMPTS {
                        return false;
                    }
                    let ahead = OpeningAttempt {
                        startup: THIS_START,
                        ticket: OpeningTicket::new(self.made.len() as u64 + 1),
                    };
                    let _outcome = self.handle(twin(ahead), true);
                }
                Step::TwinBehind => {
                    let Some(ours) = self.made.last() else {
                        return false;
                    };
                    let behind = twin(ours.attempt);
                    if behind == *ours {
                        return false;
                    }
                    let _outcome = self.handle(behind, true);
                }
                Step::Deliver(rank) => {
                    let Some(reply) = self.replies.get(rank).copied() else {
                        return false;
                    };
                    let in_flight = self.made.last() == Some(&reply.command);
                    match self.opening.observe(reply) {
                        Ok(_) => {
                            assert!(in_flight, "a reply to another command was taken");
                            if !self.delivered.contains(&rank) {
                                self.delivered.push(rank);
                                self.delivered.sort_unstable();
                            }
                        }
                        Err(ReplyRefused::Uncorrelated { attempt }) => {
                            assert!(!in_flight && attempt == reply.command.attempt);
                            return false;
                        }
                        Err(refused) => panic!("a ledger's own reply was refused: {refused:?}"),
                    }
                }
                Step::Abandon => {
                    if self.opening.abandon().is_err() {
                        return false;
                    }
                    self.abandoned = true;
                }
                Step::HandOver => {
                    let Ok(grant) = self.opening.hand_over() else {
                        return false;
                    };
                    assert_eq!(self.granted, None, "a second grant");
                    self.granted = Some(grant.opened());
                }
            }
            self.check();
            true
        }

        /// What the opening holds is what happened.
        fn check(&mut self) {
            let stage = self.opening.stage();
            let opened_by_ours = |opened: EpochOpened| {
                opened.incarnation == DATA_SET
                    && self
                        .made
                        .last()
                        .is_some_and(|ours| self.truth.contains(&(opened.epoch, *ours)))
            };
            let ours_opened_nothing = self
                .made
                .last()
                .is_some_and(|ours| self.truth.iter().all(|(_, opener)| opener != ours));
            match stage {
                OpeningStage::Committed(opened) | OpeningStage::HandedOver(opened) => {
                    assert!(opened_by_ours(opened), "an unfounded commit: {stage:?}");
                }
                OpeningStage::GrantSuperseded(opened) => {
                    assert!(opened_by_ours(opened));
                    assert!(self.ledger.last_opened() > opened.epoch);
                }
                OpeningStage::Superseded { commit_seen: true } => {
                    assert!(!ours_opened_nothing);
                }
                OpeningStage::Rejected(_) => {
                    assert!(ours_opened_nothing, "a commit called a rejection");
                }
                OpeningStage::Faulted { .. } => panic!("a fault with an honest ledger"),
                OpeningStage::Idle
                | OpeningStage::Submitted
                | OpeningStage::Unknown
                | OpeningStage::Superseded { commit_seen: false } => {}
            }
            if let Some(opened) = self.granted {
                assert!(opened_by_ours(opened), "an unfounded grant");
                let told = self
                    .opening
                    .check_recovered(opened.incarnation, opened.epoch);
                let closed = told == RecoveredEpochCheck::Closed;
                assert!(closed || told == RecoveredEpochCheck::Confirms);
                assert!(
                    closed || !self.told_closed,
                    "a closed grant confirmed again"
                );
                assert!(!closed || self.ledger.last_opened() > opened.epoch);
                self.told_closed = closed;
            }
        }

        /// Start-up reads the ledger as it is and tries to go on. It
        /// either can, or has a reason that no read would change.
        fn go_on(mut self) {
            let stage = self.opening.stage();
            let read = self.ledger.last_opened();
            let mut result = self.opening.submit(read);
            if result == Err(SubmitRefused::AwaitingReply) {
                self.opening.abandon().expect("awaiting");
                result = self.opening.submit(read);
            }
            match result {
                Ok(_) => {}
                Err(SubmitRefused::AlreadyOpened) => assert!(matches!(
                    stage,
                    OpeningStage::Committed(_)
                        | OpeningStage::HandedOver(_)
                        | OpeningStage::GrantSuperseded(_)
                )),
                Err(SubmitRefused::Rejected(reason)) => {
                    assert!(reason.is_definitive());
                    assert_eq!(stage, OpeningStage::Rejected(reason));
                }
                Err(stuck) => panic!("start-up cannot go on from {stage:?}: {stuck:?}"),
            }
        }

        /// What tells two histories apart for everything that follows.
        fn key(&self) -> Vec<u64> {
            let rank_of = |attempt: Option<OpeningAttempt>| match attempt {
                None => 0,
                Some(attempt) if attempt.startup == THIS_START => attempt.ticket.get(),
                Some(_) => u64::MAX,
            };
            let outcome_code = |outcome: OpeningOutcome| match outcome {
                OpeningOutcome::Unknown => 0,
                OpeningOutcome::Superseded => 1,
                OpeningOutcome::Committed(opened) => 10 + opened.epoch.get(),
                OpeningOutcome::RejectedCertain(reason) => match reason {
                    OpeningRejection::PredecessorMoved => 2,
                    OpeningRejection::AttemptReused => 3,
                    OpeningRejection::EpochsExhausted => 4,
                    OpeningRejection::ForeignIncarnation => 5,
                },
            };
            let mut key = vec![
                self.ledger.last_opened().get(),
                rank_of(self.ledger.opened_by()),
                u64::from(self.abandoned),
                u64::from(self.granted.is_some()),
                u64::from(self.told_closed),
                self.made.len() as u64,
            ];
            key.extend(self.made.iter().map(|command| command.after.get()));
            for (epoch, opener) in &self.truth {
                key.extend([u64::MAX - 1, epoch.get(), rank_of(Some(opener.attempt))]);
            }
            for reply in &self.replies {
                let rank = reply.command.attempt.ticket.get();
                key.extend([u64::MAX - 2, rank, outcome_code(reply.outcome)]);
            }
            key.push(u64::MAX - 3);
            key.extend(self.delivered.iter().map(|rank| *rank as u64));
            key
        }
    }

    /// The first domain: the replies a ledger produces. Every history
    /// within two attempts and three openings, by steps of any number:
    /// the writer applies a command late or twice or cannot write,
    /// other start-ups open epochs, a start-up shares our identity, and
    /// the replies reach us in any order, any number of times.
    ///
    /// No reply of the ledger faults the opening. A grant is handed
    /// over only for an epoch our command opened, once. A stage never
    /// says more than what happened. A command the ledger rejected for
    /// certain never opens an epoch afterwards. What a recovered fact
    /// is told about a grant never goes back from closed. And at every
    /// point start-up, reading the ledger as it is, can submit again or
    /// has a reason no read would change.
    #[test]
    fn with_the_replies_of_a_ledger_the_opening_never_faults_and_can_go_on() {
        let mut steps = vec![
            Step::Submit,
            Step::Rival,
            Step::TwinAhead,
            Step::TwinBehind,
            Step::Abandon,
            Step::HandOver,
        ];
        for rank in 0..ATTEMPTS {
            steps.extend([Step::Apply(rank), Step::ReadOnly(rank)]);
        }
        // Each command gets at most five different replies.
        steps.extend((0..ATTEMPTS * 5).map(Step::Deliver));

        let replay = |path: &[Step]| {
            let mut history = History::start();
            for step in path {
                assert!(history.step(*step), "a path that was taken once");
            }
            history
        };
        let mut seen = alloc::collections::BTreeSet::new();
        seen.insert(History::start().key());
        let mut frontier: Vec<Vec<Step>> = vec![Vec::new()];
        let (mut granted, mut closed, mut reused, mut commit_closed) = (0u32, 0u32, 0u32, 0u32);
        let mut longest = 0;
        while let Some(path) = frontier.pop() {
            longest = longest.max(path.len());
            // What exists at this point, read once: a step on a command
            // or a reply that does not exist is not replayed for.
            let here = replay(&path);
            let (made, replies) = (here.made.len(), here.replies.len());
            for step in &steps {
                let exists = match step {
                    Step::Apply(rank) | Step::ReadOnly(rank) => *rank < made,
                    Step::Deliver(rank) => *rank < replies,
                    _ => true,
                };
                if !exists {
                    continue;
                }
                let mut history = replay(&path);
                if !history.step(*step) {
                    continue;
                }
                let key = history.key();
                let stage = history.opening.stage();
                history.go_on();
                if seen.insert(key) {
                    granted += u32::from(matches!(stage, OpeningStage::HandedOver(_)));
                    closed += u32::from(matches!(stage, OpeningStage::GrantSuperseded(_)));
                    reused +=
                        u32::from(stage == OpeningStage::Rejected(OpeningRejection::AttemptReused));
                    commit_closed += u32::from(stage == SUPERSEDED_AFTER_COMMIT);
                    let mut next = path.clone();
                    next.push(*step);
                    frontier.push(next);
                }
            }
        }
        // What the search went through, so that a search that shrank
        // says where.
        assert!(granted > 0, "no history handed a grant over");
        assert!(closed > 0, "no history closed the epoch of a grant");
        assert!(reused > 0, "no history met a reused identity");
        assert!(
            commit_closed > 0,
            "no history closed a commit not handed over"
        );
        assert!(longest >= 10, "the longest history has {longest} steps");
        assert!(seen.len() >= 10_000, "{} histories", seen.len());
    }
}
