//! Recipes of a message with a lifetime (ADR 0033): a publication admitted
//! with its expiry, a retained replay with the time it was observed, and
//! the marking and cancellation of a claimed copy. The lifetime itself is
//! in `message_time`, re-exported here.

use alloc::vec::Vec;

use crate::canonical::{Reader, RecipeError};
use crate::ids::{DeliveryId, OperationId, OutboundPacketId, Revision, SessionId};
pub use crate::message_time::{
    ExpiryState, MessageExpiry, MessageExpiryBlock, MessageTime, MessageTimeError, NANOS_PER_SECOND,
};
use crate::pubsub::FinishDelivery;
use crate::retained::{ReplaySubscriptions, RetainedPublication, RetainedQos0};

/// Legacy-shaped admission within tag 13. QoS0 is retained and has no exchange.
#[derive(PartialEq, Eq, Debug)]
pub enum ExpiringAdmission {
    Qos12(RetainedPublication),
    Qos0(RetainedQos0),
}

#[derive(PartialEq, Eq, Debug)]
pub struct ExpiringPublication {
    pub admission: ExpiringAdmission,
    pub expiry: MessageExpiry,
}

#[derive(PartialEq, Eq, Debug)]
pub struct TimedReplaySubscriptions {
    pub replay: ReplaySubscriptions,
    pub observed: MessageTime,
}

/// Records only a positive-byte socket observation, guarded by the exact claim.
/// An unmarked recovered claim is uncertain, not proof that it never started.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MarkDeliveryStarted {
    pub operation: OperationId,
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub packet_id: OutboundPacketId,
    pub claim: OperationId,
}

/// Cancels a claimed copy only with a correlated certain-never-started frame.
/// The store must additionally guard the claim and refuse a started delivery.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ExpireDelivery {
    pub finish: FinishDelivery,
    pub expiry: MessageExpiry,
    pub observed: MessageTime,
}

/// One absolute durable cancellation; the owner releases its matching cache.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ExpiredDelivery {
    pub delivery: DeliveryId,
    pub target: SessionId,
}

impl ExpiringPublication {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        self.expiry.check()?;
        match &self.admission {
            ExpiringAdmission::Qos12(value) => {
                if value.admission.retain {
                    value.check_retained()
                } else {
                    value.admission.check_retained()?;
                    if value.admission.qos == crate::message::Qos::AtMostOnce {
                        return Err(RecipeError::InvalidField("expiring_qos"));
                    }
                    if !value.retain_targets.is_zero() {
                        return Err(RecipeError::InvalidField("retain_targets"));
                    }
                    Ok(())
                }
            }
            ExpiringAdmission::Qos0(value) => value.check_retained(),
        }
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        let mut nested = Vec::new();
        match &self.admission {
            ExpiringAdmission::Qos12(value) if !value.admission.retain => {
                value.admission.encode_retained(&mut nested)?;
            }
            ExpiringAdmission::Qos12(value) => {
                nested.extend_from_slice(b"MQDR\0\x01");
                value.encode_retained(&mut nested)?;
            }
            ExpiringAdmission::Qos0(value) => {
                nested.extend_from_slice(b"MQDR\0\x01");
                value.encode_retained(&mut nested)?;
            }
        }
        out.push(13);
        encode_nested(&nested, out)?;
        self.expiry.encode(out);
        Ok(())
    }
}

impl TimedReplaySubscriptions {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        self.replay.check_retained()
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        let mut nested = Vec::new();
        nested.extend_from_slice(b"MQDR\0\x01");
        self.replay.encode_retained(&mut nested)?;
        out.push(14);
        encode_nested(&nested, out)?;
        self.observed.encode(out);
        Ok(())
    }
}

impl ExpireDelivery {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        match self.expiry.state(self.observed) {
            Ok(ExpiryState::Expired) => Ok(()),
            Ok(ExpiryState::TemporalUncertain) => {
                Err(RecipeError::InvalidField("message_temporal_uncertain"))
            }
            Ok(ExpiryState::Fresh { .. }) => Err(RecipeError::InvalidField("message_still_fresh")),
            Err(MessageTimeError::ClockRegressed) => {
                Err(RecipeError::InvalidField("message_clock_regressed"))
            }
            Err(MessageTimeError::ArithmeticOverflow) => {
                Err(RecipeError::InvalidField("message_expiry_overflow"))
            }
        }
    }
}

fn encode_nested(nested: &[u8], out: &mut Vec<u8>) -> Result<(), RecipeError> {
    let len = u32::try_from(nested.len())
        .map_err(|_| RecipeError::InvalidField("message_expiry_nested_length"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(nested);
    Ok(())
}

pub(crate) fn read_nested<'a>(
    reader: &mut Reader<'a>,
    allowed: &[u8],
) -> Result<&'a [u8], RecipeError> {
    let len = usize::try_from(reader.u32()?)
        .map_err(|_| RecipeError::InvalidField("message_expiry_nested_length"))?;
    let nested = reader.take(len)?;
    if nested.len() < 7 || !allowed.contains(&nested[6]) {
        return Err(RecipeError::InvalidField("message_expiry_nested_kind"));
    }
    Ok(nested)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(tick: u64) -> MessageTime {
        MessageTime {
            domain: [1; 32],
            tick,
        }
    }

    #[test]
    fn exact_deadline_and_ceil_remaining_preserve_original_lifetime() {
        let expiry = MessageExpiry {
            observed: at(100),
            interval: 2,
        };
        for (tick, remaining) in [
            (100, 2),
            (101, 2),
            (1_000_000_099, 2),
            (1_000_000_100, 1),
            (2_000_000_099, 1),
        ] {
            assert_eq!(expiry.state(at(tick)), Ok(ExpiryState::Fresh { remaining }));
        }
        assert_eq!(expiry.state(at(2_000_000_100)), Ok(ExpiryState::Expired));
        assert_eq!(expiry.state(at(u64::MAX)), Ok(ExpiryState::Expired));
    }

    #[test]
    fn absent_continuity_and_bad_arithmetic_never_prove_expiration() {
        let expiry = MessageExpiry {
            observed: at(100),
            interval: 1,
        };
        assert_eq!(
            expiry.state(MessageTime {
                domain: [2; 32],
                tick: u64::MAX
            }),
            Ok(ExpiryState::TemporalUncertain)
        );
        assert_eq!(expiry.state(at(99)), Err(MessageTimeError::ClockRegressed));
        let overflow = MessageExpiry {
            observed: at(u64::MAX),
            interval: 1,
        };
        assert_eq!(
            overflow.deadline(),
            Err(MessageTimeError::ArithmeticOverflow)
        );
        assert_eq!(
            overflow.state(at(u64::MAX)),
            Err(MessageTimeError::ArithmeticOverflow)
        );
    }

    #[test]
    fn zero_and_maximum_intervals_are_finite() {
        let zero = MessageExpiry {
            observed: at(0),
            interval: 0,
        };
        assert_eq!(zero.state(at(0)), Ok(ExpiryState::Expired));
        let largest = MessageExpiry {
            interval: u32::MAX,
            ..zero
        };
        assert_eq!(
            largest.state(at(0)),
            Ok(ExpiryState::Fresh {
                remaining: u32::MAX
            })
        );
        let end = largest.deadline().unwrap();
        assert_eq!(
            largest.state(at(end - 1)),
            Ok(ExpiryState::Fresh { remaining: 1 })
        );
        assert_eq!(largest.state(at(end)), Ok(ExpiryState::Expired));
    }
}
