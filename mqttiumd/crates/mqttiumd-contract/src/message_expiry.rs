//! Message lifetime in an explicitly identified monotonic clock domain.
//!
//! An observation is frozen at receipt and copied unchanged into retained
//! replay. Unlike session retention, an unknown clock origin cannot pause or
//! restart a message's lifetime: such a message is temporally uncertain.

use alloc::vec::Vec;

use crate::command::{FinishDelivery, Reader, Recipe, RecipeError};
use crate::ids::{DeliveryId, OperationId, OutboundPacketId, Revision, SessionId};
use crate::retained::{ReplaySubscriptions, RetainedPublication, RetainedQos0};

pub const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// One observation supplied by the clock adapter, never UTC or an Instant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MessageTime {
    pub domain: [u8; 32],
    pub tick: u64,
}

/// The original MQTT lifetime; every u32 value, including its maximum, is finite.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MessageExpiry {
    pub observed: MessageTime,
    pub interval: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExpiryState {
    Fresh { remaining: u32 },
    Expired,
    TemporalUncertain,
}

/// A bad reading or deadline is a refusal, never evidence of expiration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MessageTimeError {
    ClockRegressed,
    ArithmeticOverflow,
}

/// A correlated adapter refusal proves this frame wrote no bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MessageExpiryBlock {
    Expired,
    TemporalUncertain,
}

impl MessageExpiry {
    pub fn deadline(self) -> Result<u64, MessageTimeError> {
        u64::from(self.interval)
            .checked_mul(NANOS_PER_SECOND)
            .and_then(|lifetime| self.observed.tick.checked_add(lifetime))
            .ok_or(MessageTimeError::ArithmeticOverflow)
    }

    /// Expiry is exact at the deadline. Before it, the remaining wire seconds
    /// round up, equivalently subtracting only complete elapsed seconds.
    pub fn state(self, now: MessageTime) -> Result<ExpiryState, MessageTimeError> {
        let deadline = self.deadline()?;
        if now.domain != self.observed.domain {
            return Ok(ExpiryState::TemporalUncertain);
        }
        let elapsed = now
            .tick
            .checked_sub(self.observed.tick)
            .ok_or(MessageTimeError::ClockRegressed)?;
        if now.tick >= deadline {
            return Ok(ExpiryState::Expired);
        }
        let waited = u32::try_from(elapsed / NANOS_PER_SECOND)
            .map_err(|_| MessageTimeError::ArithmeticOverflow)?;
        let remaining = self
            .interval
            .checked_sub(waited)
            .ok_or(MessageTimeError::ArithmeticOverflow)?;
        Ok(ExpiryState::Fresh { remaining })
    }

    pub(crate) fn check(self) -> Result<(), RecipeError> {
        self.deadline()
            .map(|_| ())
            .map_err(|_| RecipeError::InvalidField("message_expiry_overflow"))
    }

    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        self.observed.encode(out);
        out.extend_from_slice(&self.interval.to_be_bytes());
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, RecipeError> {
        let value = Self {
            observed: MessageTime::decode(reader)?,
            interval: reader.u32()?,
        };
        value.check()?;
        Ok(value)
    }
}

impl MessageTime {
    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.domain);
        out.extend_from_slice(&self.tick.to_be_bytes());
    }

    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, RecipeError> {
        let mut domain = [0; 32];
        domain.copy_from_slice(reader.take(32)?);
        Ok(Self {
            domain,
            tick: reader.u64()?,
        })
    }
}

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
                    if value.admission.qos == crate::packet::Qos::AtMostOnce {
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

    pub(crate) fn decode(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic: usize,
    ) -> Result<Self, RecipeError> {
        let nested = read_nested(reader, &[1, 10, 11, 20, 21])?;
        // No temporal extension can appear within a temporal wrapper. Existing
        // tag 10 retains its single legacy tag-1 envelope.
        let admission = match Recipe::decode_canonical(nested, max_targets, max_topic)? {
            Recipe::AdmitPublication(admission) if !admission.retain => {
                ExpiringAdmission::Qos12(RetainedPublication {
                    admission,
                    retain_targets: crate::retained::RetainMask::EMPTY,
                })
            }
            Recipe::RetainedPublication(value) => ExpiringAdmission::Qos12(value),
            Recipe::RetainedQos0(value) => ExpiringAdmission::Qos0(value),
            _ => return Err(RecipeError::InvalidField("message_expiry_nested_kind")),
        };
        let value = Self {
            admission,
            expiry: MessageExpiry::decode(reader)?,
        };
        value.check()?;
        Ok(value)
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

    pub(crate) fn decode(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic: usize,
    ) -> Result<Self, RecipeError> {
        let nested = read_nested(reader, &[12])?;
        let Recipe::ReplaySubscriptions(replay) =
            Recipe::decode_canonical(nested, max_targets, max_topic)?
        else {
            return Err(RecipeError::InvalidField("message_expiry_nested_kind"));
        };
        Ok(Self {
            replay,
            observed: MessageTime::decode(reader)?,
        })
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

fn read_nested<'a>(reader: &mut Reader<'a>, allowed: &[u8]) -> Result<&'a [u8], RecipeError> {
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
