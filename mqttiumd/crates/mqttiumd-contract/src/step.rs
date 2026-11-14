//! The synchronous step interface of the core (DEM-006).
//!
//! An input is either applied or handed back intact. `NeedCapacity` keeps
//! the input, the business state, the identity counters, the accounts, the
//! reservations already held and the content of the effect box unchanged;
//! provisional reservations are returned. Planning work already spent is
//! declared in `work_used`: it is not presented as free.
//!
//! A transition follows five phases with no re-entrancy and no adapter
//! callback in between: read and prepare without external effect; compute
//! the resources needed; reserve them; apply the mutations without fallible
//! allocation; publish the effects into the reserved slots. ADR 0006 fixes
//! the shape that makes this checkable: a pure plan, then an infallible
//! application.

use crate::capacity::{CapacityNeed, WorkUnits};
use crate::effects::Input;
use crate::ids::OperationId;

/// An input owned by the caller of a step.
pub type OwnedInput = Input;

/// The work a turn is allowed to spend. It is not a reservation of slots,
/// memory or result capacity (this refines ARC-004).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TurnGrant {
    pub work_limit: WorkUnits,
}

/// How far an applied input went.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Progress {
    /// The input is fully handled.
    Complete,
    /// The transition published effects and waits for their results.
    WaitingForEffects,
    /// A counted continuation holds the remaining work. A transition that
    /// keeps a cursor or a partial manifest is applied, never `NeedCapacity`.
    ContinuationScheduled,
}

/// An internal fault: the owner stops with a diagnostic.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CoreFault {
    /// Two terminal results disagree for one recipe.
    ContradictoryOutcome { operation: OperationId },
    /// The store saw one `OperationId` with two different recipes.
    RecipeMismatch { operation: OperationId },
    /// A transition published more effects than it reserved.
    ReservationOverrun,
    /// An input that the adapters can never produce in this state.
    ImpossibleInput,
}

/// The structured result of one step.
#[must_use]
#[derive(PartialEq, Eq, Debug)]
pub enum StepOutcome {
    /// The transition was applied entirely.
    Applied {
        work_used: WorkUnits,
        progress: Progress,
    },
    /// Nothing was applied; the same input comes back with what it needs.
    NeedCapacity {
        input: OwnedInput,
        need: CapacityNeed,
        work_used: WorkUnits,
    },
    /// The owner cannot continue.
    Fatal(CoreFault),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{Resource, WakeCondition};
    use crate::ids::SessionId;

    #[test]
    fn need_capacity_hands_the_same_input_back() {
        let input = || Input::WorkAvailable {
            session: SessionId::new(7),
        };
        let outcome = StepOutcome::NeedCapacity {
            input: input(),
            need: CapacityNeed {
                resource: Resource::EffectSlots,
                amount: 1,
                wake: WakeCondition::EffectAccepted,
            },
            work_used: WorkUnits::new(3),
        };
        let StepOutcome::NeedCapacity {
            input: returned,
            work_used,
            ..
        } = outcome
        else {
            panic!("variant changed");
        };
        assert_eq!(returned, input());
        assert_eq!(work_used, WorkUnits::new(3));
    }
}
