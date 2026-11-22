//! Durable zero-delay QoS1 Will intentions, independent of their source session.
//! The installing Open OperationId is the never-reused occurrence identity.
use crate::capacity::BoundedVec;
use crate::command::{Costs, DeliveryCause, PayloadRef, Reader, RecipeError, Target};
use crate::ids::{DeliveryId, OperationId, PayloadId, PublicationId, Revision, SessionId};
use crate::packet::{Qos, SharedBytes};
use crate::retained::{LEGACY_MAX_TARGETS, MAX_RETAIN_MASK_TARGETS, RetainMask};
use alloc::vec::Vec;
use core::fmt;

pub const MAX_WILL_TOPIC_BYTES: usize = 1024;
pub const MAX_WILL_PAYLOAD_BYTES: usize = 32_768;
/// A separate bounded reserve; existing message payload/receipt quotas do not change.
pub const MAX_WILL_INTENTS: usize = 1_024;
pub const MAX_WILL_BYTES: u64 = 32 * 1024 * 1024;

/// The supported profile has QoS1, delay=0 and no other properties.
#[derive(Clone, PartialEq, Eq)]
pub struct WillSpec {
    pub retain: bool,
    pub topic: SharedBytes,
    pub payload: SharedBytes,
}
impl fmt::Debug for WillSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WillSpec")
            .field("retain", &self.retain)
            .field("topic_bytes", &self.topic.len())
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}
impl WillSpec {
    pub fn check(&self) -> Result<(), RecipeError> {
        if self.topic.is_empty()
            || self.topic.len() > MAX_WILL_TOPIC_BYTES
            || self
                .topic
                .as_slice()
                .iter()
                .any(|b| matches!(*b, 0 | b'+' | b'#'))
            || core::str::from_utf8(self.topic.as_slice()).is_err()
        {
            return Err(RecipeError::InvalidField("will_topic"));
        }
        if self.payload.len() > MAX_WILL_PAYLOAD_BYTES {
            return Err(RecipeError::InvalidField("will_payload"));
        }
        Ok(())
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.topic.len() as u16).to_be_bytes());
        out.extend_from_slice(self.topic.as_slice());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(self.payload.as_slice());
    }
    pub(crate) fn decode(r: &mut Reader<'_>, retain: bool) -> Result<Self, RecipeError> {
        let n = usize::from(r.u16()?);
        if n > MAX_WILL_TOPIC_BYTES {
            return Err(RecipeError::InvalidField("will_topic"));
        }
        let topic = SharedBytes::copy_from(r.take(n)?, MAX_WILL_TOPIC_BYTES)
            .map_err(|_| RecipeError::AllocationFailed)?;
        let n = usize::try_from(r.u32()?).map_err(|_| RecipeError::InvalidField("will_payload"))?;
        if n > MAX_WILL_PAYLOAD_BYTES {
            return Err(RecipeError::InvalidField("will_payload"));
        }
        let payload = SharedBytes::copy_from(r.take(n)?, MAX_WILL_PAYLOAD_BYTES)
            .map_err(|_| RecipeError::AllocationFailed)?;
        let value = Self {
            retain,
            topic,
            payload,
        };
        value.check()?;
        Ok(value)
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WillState {
    Armed,
    Pending,
}
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WillIntent {
    pub id: OperationId,
    pub session: SessionId,
    /// MQTT No Local compares this frozen ClientID even after source cleanup.
    pub client_id: SharedBytes,
    pub spec: WillSpec,
    pub state: WillState,
}

/// Kind17 (nonretained) or kind18 (retained) for up to 16 targets, or kind22
/// for 17..=1024 targets: conditional consumption of one Pending intent and
/// atomic publication admission.
/// No MQTT inbound Packet Identifier or fabricated exchange is required.
#[derive(PartialEq, Eq, Debug)]
pub struct WillPublication {
    pub operation: OperationId,
    pub will: OperationId,
    pub publication: PublicationId,
    pub route_revision: Revision,
    pub topic: SharedBytes,
    pub payload: PayloadRef,
    pub costs: Costs,
    pub targets: BoundedVec<Target>,
    /// Present for a retained Will. Bit i preserves RETAIN on live target i.
    pub live_retain: Option<RetainMask>,
}
impl WillPublication {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        if self.topic.is_empty()
            || self.topic.len() > MAX_WILL_TOPIC_BYTES
            || self
                .topic
                .as_slice()
                .iter()
                .any(|b| matches!(*b, 0 | b'+' | b'#'))
            || core::str::from_utf8(self.topic.as_slice()).is_err()
            || self.payload.length.get() > MAX_WILL_PAYLOAD_BYTES as u64
        {
            return Err(RecipeError::InvalidField("will_publication"));
        }
        if self.targets.len() > MAX_RETAIN_MASK_TARGETS {
            return Err(RecipeError::InvalidField("will_targets"));
        }
        if let Some(mask) = &self.live_retain {
            mask.check(self.targets.len(), "will_live_retain")?;
        }
        let mut last = None;
        for (index, target) in self.targets.as_slice().iter().enumerate() {
            if target.delivery
                != (DeliveryId {
                    publication: self.publication,
                    index: index as u32,
                })
                || target.cause != DeliveryCause::LiveExact
                || target.effective_qos > Qos::AtLeastOnce
                || last.is_some_and(|session| session >= target.session)
            {
                return Err(RecipeError::InvalidField("will_targets"));
            }
            last = Some(target.session);
        }
        Ok(())
    }
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        let wide = self.targets.len() > LEGACY_MAX_TARGETS;
        out.push(if wide {
            22
        } else if self.live_retain.is_some() {
            18
        } else {
            17
        });
        out.extend_from_slice(&self.operation.to_be_bytes());
        out.extend_from_slice(&self.will.to_be_bytes());
        out.extend_from_slice(&self.publication.to_be_bytes());
        out.extend_from_slice(&self.route_revision.to_be_bytes());
        out.extend_from_slice(&(self.topic.len() as u16).to_be_bytes());
        out.extend_from_slice(self.topic.as_slice());
        out.extend_from_slice(&self.payload.id.to_be_bytes());
        out.extend_from_slice(&self.payload.length.get().to_be_bytes());
        for n in [
            self.costs.payload_bytes.get(),
            self.costs.references.get(),
            self.costs.exchange_slots.get(),
            self.costs.receipt_slots.get(),
        ] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        if wide {
            out.extend_from_slice(&(self.targets.len() as u16).to_be_bytes());
        } else {
            out.push(self.targets.len() as u8);
        }
        for target in self.targets.as_slice() {
            out.extend_from_slice(&target.session.to_be_bytes());
            out.extend_from_slice(&target.session_revision.to_be_bytes());
            out.extend_from_slice(&target.delivery.index.to_be_bytes());
            out.push(target.effective_qos.to_wire());
            out.push(target.cause as u8);
        }
        if wide {
            out.push(u8::from(self.live_retain.is_some()));
            if let Some(mask) = &self.live_retain {
                mask.encode_wide(self.targets.len(), out);
            }
        } else if let Some(mask) = &self.live_retain {
            let bits = mask.as_u16().unwrap_or(0);
            out.extend_from_slice(&bits.to_be_bytes());
        }
    }
    pub(crate) fn decode(
        r: &mut Reader<'_>,
        max_targets: usize,
        max_topic: usize,
        tag: u8,
    ) -> Result<Self, RecipeError> {
        use crate::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
        let wide = tag == 22;
        let operation = r.operation()?;
        let will = r.operation()?;
        let publication =
            PublicationId::from_be_slice(r.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let route_revision = Revision::new(r.u64()?);
        let n = usize::from(r.u16()?);
        if n > max_topic.min(MAX_WILL_TOPIC_BYTES) {
            return Err(RecipeError::TopicTooLong(n));
        }
        let topic = SharedBytes::copy_from(r.take(n)?, max_topic)
            .map_err(|_| RecipeError::AllocationFailed)?;
        let payload = PayloadRef {
            id: PayloadId::from_be_slice(r.take(32)?).map_err(|_| RecipeError::Truncated)?,
            length: PayloadBytes::new(r.u64()?),
        };
        let costs = Costs {
            payload_bytes: PayloadBytes::new(r.u64()?),
            references: References::new(r.u64()?),
            exchange_slots: ExchangeSlots::new(r.u64()?),
            receipt_slots: ReceiptSlots::new(r.u64()?),
        };
        let count = if wide {
            usize::from(r.u16()?)
        } else {
            usize::from(r.u8()?)
        };
        let ceiling = if wide {
            max_targets.min(MAX_RETAIN_MASK_TARGETS)
        } else {
            max_targets.min(LEGACY_MAX_TARGETS)
        };
        if wide && count <= LEGACY_MAX_TARGETS {
            return Err(RecipeError::InvalidField("noncanonical_wide_targets"));
        }
        if count > ceiling {
            return Err(RecipeError::TooManyTargets {
                count,
                max: ceiling,
            });
        }
        let mut targets =
            BoundedVec::with_capacity(count).map_err(|_| RecipeError::AllocationFailed)?;
        for _ in 0..count {
            let target = Target {
                session: SessionId::new(r.u64()?),
                session_revision: Revision::new(r.u64()?),
                delivery: DeliveryId {
                    publication,
                    index: r.u32()?,
                },
                effective_qos: r.qos()?,
                cause: match r.u8()? {
                    1 => DeliveryCause::LiveExact,
                    _ => return Err(RecipeError::InvalidField("will_cause")),
                },
            };
            targets
                .try_push(target)
                .map_err(|_| RecipeError::AllocationFailed)?;
        }
        let live_retain = if wide {
            if r.boolean("will_live_retain")? {
                Some(RetainMask::decode_wide(r, count, "will_live_retain")?)
            } else {
                None
            }
        } else if tag == 18 {
            Some(RetainMask::from_u16(r.u16()?))
        } else {
            None
        };
        Ok(Self {
            operation,
            will,
            publication,
            route_revision,
            topic,
            payload,
            costs,
            targets,
            live_retain,
        })
    }
}
