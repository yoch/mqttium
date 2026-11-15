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
use crate::effects::{Input, ReserveRefused};
use crate::ids::{LogicalOwnerId, OperationId, Revision};

/// An input owned by the caller of a step.
pub type OwnedInput = Input;

/// The work a turn is allowed to spend. It is not a reservation of slots,
/// memory or result capacity (this refines ARC-004).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TurnGrant {
    pub work_limit: WorkUnits,
}

/// What a plan was computed against: the owner and the revision of its
/// state. A plan is consumed by its application, which refuses it when the
/// state moved in between (ADR 0006).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PlanStamp {
    pub owner: LogicalOwnerId,
    pub state_revision: Revision,
}

impl PlanStamp {
    /// Checks that the plan still applies to this owner at this revision.
    pub fn check(self, owner: LogicalOwnerId, state_revision: Revision) -> Result<(), CoreFault> {
        if self.owner == owner && self.state_revision == state_revision {
            Ok(())
        } else {
            Err(CoreFault::StalePlan)
        }
    }
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
    /// A plan was applied to another owner, or after the state it was
    /// computed against had changed.
    StalePlan,
    /// An input that the adapters can never produce in this state.
    ImpossibleInput,
    /// A transition asked for more effect slots than its whole box has.
    /// The constructor of an owner refuses such a box, so this is a fault
    /// of the core and never a wait (ADR 0006, amendment).
    BoxTooSmall,
}

/// What is exhausted when a step answers `Exhausted`. No wake-up ends it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Exhaustion {
    /// The reservation table of the owner's effect box. The holder of the
    /// creation authority may renew the box: dispatch it until it is
    /// empty, retire it, create a box for the same owner and present the
    /// input again. No counter of the owner is reset. Otherwise the owner
    /// stops.
    ReservationTable,
    /// An identity counter of the owner reached its end. It is not
    /// renewable within a run epoch: the owner stops, and the next epoch
    /// is a new identity space.
    IdentityCounter,
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
    /// Nothing was applied; the same input comes back. It keeps everything
    /// `NeedCapacity` keeps, and unlike it no wake-up ends the refusal: the
    /// owner is renewed as its `Exhaustion` states, or it stops. It is
    /// never turned into a `CapacityNeed`.
    Exhausted {
        input: OwnedInput,
        exhaustion: Exhaustion,
        work_used: WorkUnits,
    },
    /// The owner cannot continue.
    Fatal(CoreFault),
}

impl StepOutcome {
    /// The outcome of a step whose reservation of effect slots was
    /// refused. This is the one place where a refusal of the box crosses
    /// the boundary of a step.
    pub fn reservation_refused(
        input: OwnedInput,
        refusal: ReserveRefused,
        work_used: WorkUnits,
    ) -> Self {
        match refusal {
            ReserveRefused::Need(need) => Self::NeedCapacity {
                input,
                need,
                work_used,
            },
            ReserveRefused::TableExhausted => Self::Exhausted {
                input,
                exhaustion: Exhaustion::ReservationTable,
                work_used,
            },
            ReserveRefused::LargerThanBox { .. } => Self::Fatal(CoreFault::BoxTooSmall),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{Resource, WakeCondition};
    use crate::ids::SessionId;

    fn input() -> Input {
        Input::WorkAvailable {
            session: SessionId::new(7),
        }
    }

    // ADR 0006, amendment: a definitive refusal of the box is never turned
    // into a wait at the boundary of a step.
    #[test]
    fn an_exhausted_reservation_table_hands_the_input_back_without_a_wake_up() {
        let outcome = StepOutcome::reservation_refused(
            input(),
            ReserveRefused::TableExhausted,
            WorkUnits::new(2),
        );
        assert_eq!(
            outcome,
            StepOutcome::Exhausted {
                input: input(),
                exhaustion: Exhaustion::ReservationTable,
                work_used: WorkUnits::new(2),
            }
        );
    }

    #[test]
    fn a_request_larger_than_the_box_is_a_core_fault() {
        let outcome = StepOutcome::reservation_refused(
            input(),
            ReserveRefused::LargerThanBox {
                requested: 4,
                capacity: 3,
            },
            WorkUnits::new(1),
        );
        assert_eq!(outcome, StepOutcome::Fatal(CoreFault::BoxTooSmall));
    }

    #[test]
    fn a_temporary_lack_keeps_its_need_and_its_wake_up() {
        let wakes = [
            (Resource::EffectSlots, WakeCondition::EffectAccepted),
            (
                Resource::EffectSlots,
                WakeCondition::EffectAcceptedOrResourceReleased,
            ),
            (Resource::Reservations, WakeCondition::ResourceReleased),
        ];
        for (resource, wake) in wakes {
            let need = CapacityNeed {
                resource,
                amount: 2,
                wake,
            };
            assert_eq!(
                StepOutcome::reservation_refused(
                    input(),
                    ReserveRefused::Need(need),
                    WorkUnits::new(5)
                ),
                StepOutcome::NeedCapacity {
                    input: input(),
                    need,
                    work_used: WorkUnits::new(5),
                }
            );
        }
    }

    #[test]
    fn a_plan_applies_only_to_the_state_it_was_computed_against() {
        let stamp = PlanStamp {
            owner: LogicalOwnerId::new(1),
            state_revision: Revision::new(7),
        };
        assert_eq!(
            stamp.check(LogicalOwnerId::new(1), Revision::new(7)),
            Ok(())
        );
        assert_eq!(
            stamp.check(LogicalOwnerId::new(1), Revision::new(8)),
            Err(CoreFault::StalePlan),
            "the state moved between plan and application"
        );
        assert_eq!(
            stamp.check(LogicalOwnerId::new(2), Revision::new(7)),
            Err(CoreFault::StalePlan),
            "another owner"
        );
    }

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
