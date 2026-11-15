//! The life of one immutable command, as the registry of transitions
//! states it (machine `command` of `contracts/v1/p1.transitions.json`).
//!
//! The machine is a pure function of a phase and an event. The owner asks
//! it before it changes anything: the verdict says whether the event moves
//! the command, is absorbed, is refused, or stops the owner.

use crate::contract::outcome::StoreOutcome;

/// Where a command stands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandPhase {
    /// The recipe is computed; nothing was handed to the effect box.
    Prepared,
    /// The frozen command was handed to the effect box, once.
    Submitted,
    /// A result said the outcome is unknown, or the deadline of the result
    /// passed: a resolution is under way.
    Indeterminate,
    /// The store reported the commit; the owner has not published what
    /// depends on it yet.
    Committed,
    /// The store reported that the command certainly did not execute.
    RejectedCertain,
    /// A committed result whose dependent effects the owner has published.
    Consumed,
    /// The identity can never execute and the detail of its result is no
    /// longer available.
    ReceiptClosed,
}

/// What a store reports, without its detail.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OutcomeClass {
    Committed,
    RejectedCertain,
    Unknown,
    Retired,
}

impl OutcomeClass {
    /// The class of a reported outcome.
    #[must_use]
    pub const fn of(outcome: &StoreOutcome) -> Self {
        match outcome {
            StoreOutcome::Committed(_) => Self::Committed,
            StoreOutcome::RejectedCertain(_) => Self::RejectedCertain,
            StoreOutcome::PendingOrUnknown => Self::Unknown,
            StoreOutcome::RetiredOutcomeUnavailable => Self::Retired,
        }
    }
}

/// What happens to a command.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandEvent {
    /// The owner hands the frozen command to its effect box.
    Submit,
    /// A result is delivered for the operation.
    StoreResult(OutcomeClass),
    /// The deadline armed for the result of the operation is delivered.
    ResultTimeout,
    /// The same operation is presented to the store again, with the same
    /// canonical recipe or with another.
    Replay { same_recipe: bool },
}

/// What an event does to a command.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// The command moves to this phase, which may be the one it is in.
    Transition(CommandPhase),
    /// The event is late or repeated: nothing changes, nothing is emitted.
    IgnoreStale,
    /// The event is refused: nothing changes, and the command is not
    /// submitted or executed again.
    Reject,
    /// The event contradicts what is established: the owner stops.
    Fault,
    /// No adapter can produce this event in this phase.
    Unreachable,
}

impl CommandPhase {
    /// The verdict of the registry for `event` in this phase.
    #[must_use]
    pub const fn on(self, event: CommandEvent) -> Verdict {
        use CommandEvent::{Replay, ResultTimeout, StoreResult, Submit};
        use OutcomeClass as Class;
        use Verdict::{Fault, IgnoreStale, Reject, Transition, Unreachable};
        match (self, event) {
            // Nothing was presented to the store yet: only a submission
            // can follow.
            (Self::Prepared, Submit) => Transition(Self::Submitted),
            (Self::Prepared, StoreResult(_) | ResultTimeout | Replay { .. }) => Unreachable,

            // A submission freezes the recipe and the identity, once.
            (_, Submit) => Reject,

            // The receipt is closed: nothing can be compared any more, so
            // nothing is a contradiction and nothing is executed again.
            (Self::ReceiptClosed, StoreResult(_) | ResultTimeout) => IgnoreStale,
            (Self::ReceiptClosed, Replay { .. }) => Reject,

            // One identity, one recipe.
            (_, Replay { same_recipe: false }) => Fault,
            (Self::RejectedCertain, Replay { same_recipe: true }) => Reject,
            (_, Replay { same_recipe: true }) => IgnoreStale,

            // While the outcome is open, a result settles it or leaves it
            // open, and a deadline starts a resolution.
            (Self::Submitted | Self::Indeterminate, StoreResult(class)) => match class {
                Class::Committed => Transition(Self::Committed),
                Class::RejectedCertain => Transition(Self::RejectedCertain),
                Class::Unknown => Transition(Self::Indeterminate),
                Class::Retired => Transition(Self::ReceiptClosed),
            },
            (Self::Submitted | Self::Indeterminate, ResultTimeout) => {
                Transition(Self::Indeterminate)
            }

            // Knowledge is monotonic: a terminal result is never replaced
            // by a later unknown one, and two terminal results that
            // disagree are a fault.
            (Self::Committed | Self::Consumed, StoreResult(class)) => match class {
                Class::RejectedCertain => Fault,
                Class::Committed | Class::Unknown | Class::Retired => IgnoreStale,
            },
            (Self::RejectedCertain, StoreResult(class)) => match class {
                Class::Committed => Fault,
                Class::RejectedCertain | Class::Unknown | Class::Retired => IgnoreStale,
            },
            (Self::Committed | Self::RejectedCertain | Self::Consumed, ResultTimeout) => {
                IgnoreStale
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CommandEvent::{Replay, ResultTimeout, StoreResult, Submit};
    use super::CommandPhase::{
        Committed, Consumed, Indeterminate, Prepared, ReceiptClosed, RejectedCertain, Submitted,
    };
    use super::OutcomeClass as Class;
    use super::Verdict::{Fault, IgnoreStale, Reject, Transition, Unreachable};
    use super::*;

    const RESULTS: [OutcomeClass; 4] = [
        Class::Committed,
        Class::RejectedCertain,
        Class::Unknown,
        Class::Retired,
    ];

    // One test per rule of the machine `command`, named after the rule.
    // command.prepared.submit
    #[test]
    fn rule_command_prepared_submit() {
        assert_eq!(Prepared.on(Submit), Transition(Submitted));
    }

    // command.prepared.submit.defer
    #[test]
    fn rule_command_prepared_submit_defer() {
        // A lack of effect slots hands the input back before the command
        // moves: the verdict is the one of the event, and the owner applies
        // it only once the slots are reserved.
        assert_eq!(Prepared.on(Submit), Transition(Submitted));
    }

    // command.submitted.result.committed
    #[test]
    fn rule_command_submitted_result_committed() {
        assert_eq!(
            Submitted.on(StoreResult(Class::Committed)),
            Transition(Committed)
        );
    }

    // command.submitted.result.rejected
    #[test]
    fn rule_command_submitted_result_rejected() {
        assert_eq!(
            Submitted.on(StoreResult(Class::RejectedCertain)),
            Transition(RejectedCertain)
        );
    }

    // command.submitted.result.unknown
    #[test]
    fn rule_command_submitted_result_unknown() {
        assert_eq!(
            Submitted.on(StoreResult(Class::Unknown)),
            Transition(Indeterminate)
        );
    }

    // command.submitted.timeout
    #[test]
    fn rule_command_submitted_timeout() {
        assert_eq!(Submitted.on(ResultTimeout), Transition(Indeterminate));
    }

    // command.submitted.timeout.defer
    #[test]
    fn rule_command_submitted_timeout_defer() {
        // A lack of effect slots hands the input back before the command
        // moves: the verdict is the one of the event, and the owner applies
        // it only once the slots are reserved.
        assert_eq!(Submitted.on(ResultTimeout), Transition(Indeterminate));
    }

    // command.indeterminate.result.committed
    #[test]
    fn rule_command_indeterminate_result_committed() {
        assert_eq!(
            Indeterminate.on(StoreResult(Class::Committed)),
            Transition(Committed)
        );
    }

    // command.indeterminate.result.rejected
    #[test]
    fn rule_command_indeterminate_result_rejected() {
        assert_eq!(
            Indeterminate.on(StoreResult(Class::RejectedCertain)),
            Transition(RejectedCertain)
        );
    }

    // command.indeterminate.result.unknown
    #[test]
    fn rule_command_indeterminate_result_unknown() {
        assert_eq!(
            Indeterminate.on(StoreResult(Class::Unknown)),
            Transition(Indeterminate)
        );
    }

    // command.indeterminate.timeout
    #[test]
    fn rule_command_indeterminate_timeout() {
        assert_eq!(Indeterminate.on(ResultTimeout), Transition(Indeterminate));
    }

    // command.indeterminate.timeout.defer
    #[test]
    fn rule_command_indeterminate_timeout_defer() {
        // A lack of effect slots hands the input back before the command
        // moves: the verdict is the one of the event, and the owner applies
        // it only once the slots are reserved.
        assert_eq!(Indeterminate.on(ResultTimeout), Transition(Indeterminate));
    }

    // command.committed.result.unknown
    #[test]
    fn rule_command_committed_result_unknown() {
        assert_eq!(Committed.on(StoreResult(Class::Unknown)), IgnoreStale);
    }

    // command.committed.result.duplicate
    #[test]
    fn rule_command_committed_result_duplicate() {
        assert_eq!(Committed.on(StoreResult(Class::Committed)), IgnoreStale);
    }

    // command.committed.result.contradiction
    #[test]
    fn rule_command_committed_result_contradiction() {
        assert_eq!(Committed.on(StoreResult(Class::RejectedCertain)), Fault);
    }

    // command.rejected.result.unknown
    #[test]
    fn rule_command_rejected_result_unknown() {
        assert_eq!(RejectedCertain.on(StoreResult(Class::Unknown)), IgnoreStale);
    }

    // command.rejected.result.contradiction
    #[test]
    fn rule_command_rejected_result_contradiction() {
        assert_eq!(RejectedCertain.on(StoreResult(Class::Committed)), Fault);
    }

    // command.committed.timeout.stale
    #[test]
    fn rule_command_committed_timeout_stale() {
        assert_eq!(Committed.on(ResultTimeout), IgnoreStale);
    }

    // command.rejected.timeout.stale
    #[test]
    fn rule_command_rejected_timeout_stale() {
        assert_eq!(RejectedCertain.on(ResultTimeout), IgnoreStale);
    }

    // command.consumed.timeout.stale
    #[test]
    fn rule_command_consumed_timeout_stale() {
        assert_eq!(Consumed.on(ResultTimeout), IgnoreStale);
    }

    // command.receipt_closed.timeout.stale
    #[test]
    fn rule_command_receipt_closed_timeout_stale() {
        assert_eq!(ReceiptClosed.on(ResultTimeout), IgnoreStale);
    }

    // command.prepared.result.unreachable
    #[test]
    fn rule_command_prepared_result_unreachable() {
        for class in RESULTS {
            assert_eq!(Prepared.on(StoreResult(class)), Unreachable, "{class:?}");
        }
    }

    // command.prepared.timeout.unreachable
    #[test]
    fn rule_command_prepared_timeout_unreachable() {
        assert_eq!(Prepared.on(ResultTimeout), Unreachable);
    }

    // command.prepared.replay.unreachable
    #[test]
    fn rule_command_prepared_replay_unreachable() {
        assert_eq!(Prepared.on(Replay { same_recipe: true }), Unreachable);
        assert_eq!(Prepared.on(Replay { same_recipe: false }), Unreachable);
    }

    // command.submitted.submit.refuse
    #[test]
    fn rule_command_submitted_submit_refuse() {
        assert_eq!(Submitted.on(Submit), Reject);
    }

    // command.indeterminate.submit.refuse
    #[test]
    fn rule_command_indeterminate_submit_refuse() {
        assert_eq!(Indeterminate.on(Submit), Reject);
    }

    // command.committed.submit.refuse
    #[test]
    fn rule_command_committed_submit_refuse() {
        assert_eq!(Committed.on(Submit), Reject);
    }

    // command.rejected.submit.refuse
    #[test]
    fn rule_command_rejected_submit_refuse() {
        assert_eq!(RejectedCertain.on(Submit), Reject);
    }

    // command.consumed.submit.refuse
    #[test]
    fn rule_command_consumed_submit_refuse() {
        assert_eq!(Consumed.on(Submit), Reject);
    }

    // command.receipt_closed.submit.refuse
    #[test]
    fn rule_command_receipt_closed_submit_refuse() {
        assert_eq!(ReceiptClosed.on(Submit), Reject);
    }

    // command.submitted.result.retired
    #[test]
    fn rule_command_submitted_result_retired() {
        assert_eq!(
            Submitted.on(StoreResult(Class::Retired)),
            Transition(ReceiptClosed)
        );
    }

    // command.indeterminate.result.retired
    #[test]
    fn rule_command_indeterminate_result_retired() {
        assert_eq!(
            Indeterminate.on(StoreResult(Class::Retired)),
            Transition(ReceiptClosed)
        );
    }

    // command.submitted.replay.same
    #[test]
    fn rule_command_submitted_replay_same() {
        assert_eq!(Submitted.on(Replay { same_recipe: true }), IgnoreStale);
    }

    // command.submitted.replay.mismatch
    #[test]
    fn rule_command_submitted_replay_mismatch() {
        assert_eq!(Submitted.on(Replay { same_recipe: false }), Fault);
    }

    // command.indeterminate.replay.same
    #[test]
    fn rule_command_indeterminate_replay_same() {
        assert_eq!(Indeterminate.on(Replay { same_recipe: true }), IgnoreStale);
    }

    // command.indeterminate.replay.mismatch
    #[test]
    fn rule_command_indeterminate_replay_mismatch() {
        assert_eq!(Indeterminate.on(Replay { same_recipe: false }), Fault);
    }

    // command.committed.replay.same
    #[test]
    fn rule_command_committed_replay_same() {
        assert_eq!(Committed.on(Replay { same_recipe: true }), IgnoreStale);
    }

    // command.committed.replay.mismatch
    #[test]
    fn rule_command_committed_replay_mismatch() {
        assert_eq!(Committed.on(Replay { same_recipe: false }), Fault);
    }

    // command.rejected.replay.same
    #[test]
    fn rule_command_rejected_replay_same() {
        assert_eq!(RejectedCertain.on(Replay { same_recipe: true }), Reject);
    }

    // command.rejected.replay.mismatch
    #[test]
    fn rule_command_rejected_replay_mismatch() {
        assert_eq!(RejectedCertain.on(Replay { same_recipe: false }), Fault);
    }

    // command.consumed.replay.same
    #[test]
    fn rule_command_consumed_replay_same() {
        assert_eq!(Consumed.on(Replay { same_recipe: true }), IgnoreStale);
    }

    // command.consumed.replay.mismatch
    #[test]
    fn rule_command_consumed_replay_mismatch() {
        assert_eq!(Consumed.on(Replay { same_recipe: false }), Fault);
    }

    // command.committed.result.retired
    #[test]
    fn rule_command_committed_result_retired() {
        assert_eq!(Committed.on(StoreResult(Class::Retired)), IgnoreStale);
    }

    // command.rejected.result.repeat
    #[test]
    fn rule_command_rejected_result_repeat() {
        assert_eq!(
            RejectedCertain.on(StoreResult(Class::RejectedCertain)),
            IgnoreStale
        );
        assert_eq!(RejectedCertain.on(StoreResult(Class::Retired)), IgnoreStale);
    }

    // command.consumed.result.repeat
    #[test]
    fn rule_command_consumed_result_repeat() {
        assert_eq!(Consumed.on(StoreResult(Class::Committed)), IgnoreStale);
        assert_eq!(Consumed.on(StoreResult(Class::Unknown)), IgnoreStale);
        assert_eq!(Consumed.on(StoreResult(Class::Retired)), IgnoreStale);
    }

    // command.consumed.result.contradiction
    #[test]
    fn rule_command_consumed_result_contradiction() {
        assert_eq!(Consumed.on(StoreResult(Class::RejectedCertain)), Fault);
    }

    // command.receipt_closed.result.late
    #[test]
    fn rule_command_receipt_closed_result_late() {
        for class in RESULTS {
            assert_eq!(
                ReceiptClosed.on(StoreResult(class)),
                IgnoreStale,
                "{class:?}"
            );
        }
    }

    // command.receipt_closed.replay.refuse
    #[test]
    fn rule_command_receipt_closed_replay_refuse() {
        assert_eq!(ReceiptClosed.on(Replay { same_recipe: true }), Reject);
        assert_eq!(ReceiptClosed.on(Replay { same_recipe: false }), Reject);
    }
}
