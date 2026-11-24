//! Message lifetime in an explicitly identified monotonic clock domain.
//!
//! An observation is frozen at receipt and copied unchanged into retained
//! replay. Unlike session retention, an unknown clock origin cannot pause or
//! restart a message's lifetime: such a message is temporally uncertain.

use alloc::vec::Vec;

use crate::canonical::{Reader, RecipeError};

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
