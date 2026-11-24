//! Bounded retained values and transactional subscription replay (ADR 0032).
use crate::canonical::RecipeError;
use crate::capacity::BoundedVec;
use crate::ids::{
    ConnectionGeneration, OperationId, PayloadId, PublicationId, RequestId, Revision, SessionId,
};
use crate::message::SharedBytes;
use crate::publication::{AdmitPublication, Costs, PayloadRef, Target};
use crate::pubsub::UpdateSubscriptions;
use alloc::sync::Arc;
use alloc::vec::Vec;

pub const MAX_RETAINED_TOPICS: usize = 4_096;
pub const MAX_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum destinations represented in a per-target RETAIN bitset (ADR 0051).
pub const MAX_RETAIN_MASK_TARGETS: usize = 1_024;
/// Historical target count threshold for legacy 16-bit RETAIN masks.
pub const LEGACY_MAX_TARGETS: usize = 16;
/// Eight ordered filter occurrences, each selecting at most the inventory.
pub const MAX_REPLAYS: usize = crate::pubsub::MAX_FILTERS * MAX_RETAINED_TOPICS;

/// Compact canonical per-target RETAIN bitset (RAP).
///
/// Up to 64 targets stay inline with no heap allocation; higher target indices
/// spill into a trimmed byte slice so equality depends solely on the set of
/// marked indices.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RetainMask {
    Inline(u64),
    Wide(Arc<[u8]>),
}

impl Default for RetainMask {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl From<u16> for RetainMask {
    fn from(mask: u16) -> Self {
        Self::from_u16(mask)
    }
}

impl RetainMask {
    pub const EMPTY: Self = Self::Inline(0);

    #[must_use]
    pub const fn from_u16(mask: u16) -> Self {
        Self::Inline(mask as u64)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut end = bytes.len();
        while end > 0 && bytes[end - 1] == 0 {
            end -= 1;
        }
        if end <= 8 {
            let mut raw = [0u8; 8];
            raw[..end].copy_from_slice(&bytes[..end]);
            Self::Inline(u64::from_le_bytes(raw))
        } else {
            Self::Wide(Arc::from(&bytes[..end]))
        }
    }

    pub fn from_predicate(
        len: usize,
        mut pred: impl FnMut(usize) -> bool,
    ) -> Result<Self, RecipeError> {
        if len > MAX_RETAIN_MASK_TARGETS {
            return Err(RecipeError::InvalidField("retain_targets"));
        }
        let mut inline = 0u64;
        let mut wide: Option<Vec<u8>> = None;
        for index in 0..len {
            if !pred(index) {
                continue;
            }
            if index < 64 && wide.is_none() {
                inline |= 1u64 << index;
            } else {
                let buf = wide.get_or_insert_with(|| {
                    let mut v = Vec::new();
                    v.extend_from_slice(&inline.to_le_bytes());
                    v
                });
                let byte_idx = index / 8;
                if buf.len() <= byte_idx {
                    buf.resize(byte_idx + 1, 0);
                }
                buf[byte_idx] |= 1u8 << (index % 8);
            }
        }
        Ok(match wide {
            None => Self::Inline(inline),
            Some(buf) => Self::from_bytes(&buf),
        })
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        matches!(self, Self::Inline(0))
    }

    #[must_use]
    pub fn is_set(&self, index: usize) -> bool {
        match self {
            Self::Inline(bits) => index < 64 && (*bits & (1u64 << index)) != 0,
            Self::Wide(bytes) => bytes
                .get(index / 8)
                .is_some_and(|byte| (*byte & (1u8 << (index % 8))) != 0),
        }
    }

    #[must_use]
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Self::Inline(bits) => u16::try_from(*bits).ok(),
            Self::Wide(_) => None,
        }
    }

    pub(crate) fn check(
        &self,
        target_count: usize,
        field: &'static str,
    ) -> Result<(), RecipeError> {
        if target_count > MAX_RETAIN_MASK_TARGETS {
            return Err(RecipeError::InvalidField(field));
        }
        match self {
            Self::Inline(bits) => {
                if target_count == 0 {
                    if *bits != 0 {
                        return Err(RecipeError::InvalidField(field));
                    }
                } else if target_count < 64 && (*bits >> target_count) != 0 {
                    return Err(RecipeError::InvalidField(field));
                }
            }
            Self::Wide(bytes) => {
                let Some(last) = bytes.last().copied() else {
                    return Ok(());
                };
                if last == 0 {
                    return Err(RecipeError::InvalidField(field));
                }
                let highest_bit = (bytes.len() - 1) * 8 + (7 - last.leading_zeros() as usize);
                if highest_bit >= target_count {
                    return Err(RecipeError::InvalidField(field));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn encode_wide(&self, target_count: usize, out: &mut Vec<u8>) {
        let byte_len = target_count.div_ceil(8);
        out.extend_from_slice(&(byte_len as u16).to_be_bytes());
        match self {
            Self::Inline(bits) => {
                let raw = bits.to_le_bytes();
                let copy = byte_len.min(8);
                out.extend_from_slice(&raw[..copy]);
                if byte_len > copy {
                    out.resize(out.len() + (byte_len - copy), 0);
                }
            }
            Self::Wide(bytes) => {
                let copy = byte_len.min(bytes.len());
                out.extend_from_slice(&bytes[..copy]);
                if byte_len > copy {
                    out.resize(out.len() + (byte_len - copy), 0);
                }
            }
        }
    }

    pub(crate) fn decode_wide(
        r: &mut crate::canonical::Reader<'_>,
        target_count: usize,
        field: &'static str,
    ) -> Result<Self, RecipeError> {
        let expected = target_count.div_ceil(8);
        let len = usize::from(r.u16()?);
        if len != expected {
            return Err(RecipeError::InvalidField(field));
        }
        let slice = r.take(len)?;
        let rem = target_count % 8;
        if rem != 0 && slice.last().is_some_and(|last| (*last >> rem) != 0) {
            return Err(RecipeError::InvalidField(field));
        }
        Ok(Self::from_bytes(slice))
    }
}

/// Tag 10 (up to 16 targets, legacy bytes) or tag 20 (17..=1024 targets).
#[derive(PartialEq, Eq, Debug)]
pub struct RetainedPublication {
    pub admission: AdmitPublication,
    /// Bit i preserves the publication RETAIN flag for live target i (RAP).
    pub retain_targets: RetainMask,
}

/// QoS0 has an occurrence, not an invented MQTT Packet Identifier/exchange.
/// Tag 11 (up to 16 targets, legacy bytes) or tag 21 (17..=1024 targets).
#[derive(Debug)]
pub struct RetainedQos0 {
    pub operation: OperationId,
    pub request: RequestId,
    pub publication: PublicationId,
    pub session: SessionId,
    pub session_revision: Revision,
    pub connection: ConnectionGeneration,
    pub ingress_order: u64,
    pub route_revision: Revision,
    pub topic: SharedBytes,
    pub payload: PayloadRef,
    pub costs: Costs,
    pub targets: BoundedVec<Target>,
    pub retain_targets: RetainMask,
}

/// New tag 12. All replay publication identities are reserved before submission.
/// The store consumes only a prefix; unused identities are burned, never reused.
#[derive(PartialEq, Eq, Debug)]
pub struct ReplaySubscriptions {
    pub update: UpdateSubscriptions,
    pub first_publication: PublicationId,
}

impl RetainedQos0 {
    /// Kinds 11 and 21: a retained QoS 0 publication, its retain mask
    /// narrow (11) or wide (21).
    pub(crate) fn decode(
        kind: u8,
        bytes: &mut &[u8],
        max_targets: usize,
        max_topic: usize,
    ) -> Result<Self, RecipeError> {
        use crate::canonical::Reader;
        use crate::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
        use crate::ids::DeliveryId;
        use crate::pubsub::DeliveryCause;
        if !matches!(kind, 11 | 21) {
            return Err(RecipeError::UnknownKind(kind));
        }
        let mut r = Reader { bytes };
        let wide = kind == 21;
        let operation = r.operation()?;
        let request = RequestId::from_be_slice(r.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let publication =
            PublicationId::from_be_slice(r.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let session = SessionId::new(r.u64()?);
        let session_revision = Revision::new(r.u64()?);
        let connection = ConnectionGeneration::new(r.u64()?);
        let ingress_order = r.u64()?;
        let route_revision = Revision::new(r.u64()?);
        let len = usize::from(r.u16()?);
        if len > max_topic {
            return Err(RecipeError::TopicTooLong(len));
        }
        let topic = SharedBytes::copy_from(r.take(len)?, max_topic)
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
        if wide && count <= LEGACY_MAX_TARGETS {
            return Err(RecipeError::InvalidField("noncanonical_wide_targets"));
        }
        if !wide && count > LEGACY_MAX_TARGETS {
            return Err(RecipeError::InvalidField("retain_targets"));
        }
        if count > max_targets {
            return Err(RecipeError::TooManyTargets {
                count,
                max: max_targets,
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
                    _ => return Err(RecipeError::InvalidField("cause")),
                },
            };
            targets
                .try_push(target)
                .map_err(|_| RecipeError::AllocationFailed)?;
        }
        let retain_targets = if wide {
            RetainMask::decode_wide(&mut r, count, "retain_targets")?
        } else {
            RetainMask::from_u16(r.u16()?)
        };
        let value = RetainedQos0 {
            operation,
            request,
            publication,
            session,
            session_revision,
            connection,
            ingress_order,
            route_revision,
            topic,
            payload,
            costs,
            targets,
            retain_targets,
        };
        *bytes = r.bytes;
        Ok(value)
    }
}

impl PartialEq for RetainedQos0 {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.request == other.request
            && self.publication == other.publication
            && self.session == other.session
            && self.session_revision == other.session_revision
            && self.connection == other.connection
            && self.ingress_order == other.ingress_order
            && self.route_revision == other.route_revision
            && self.topic == other.topic
            && self.payload == other.payload
            && self.costs == other.costs
            && self.targets.as_slice() == other.targets.as_slice()
            && self.retain_targets == other.retain_targets
    }
}
impl Eq for RetainedQos0 {}

impl RetainedPublication {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        // Check the legacy-shaped payload without changing its encoding.
        self.admission.check_retained()?;
        if self.admission.qos == crate::message::Qos::AtMostOnce {
            return Err(RecipeError::InvalidField("retained_qos"));
        }
        if !self.admission.retain {
            return Err(RecipeError::InvalidField("retained_flag"));
        }
        self.retain_targets
            .check(self.admission.targets.len(), "retain_targets")
    }
}

impl RetainedQos0 {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        if self
            .targets
            .as_slice()
            .iter()
            .any(|target| target.cause != crate::pubsub::DeliveryCause::LiveExact)
        {
            return Err(RecipeError::InvalidField("admission_cause"));
        }
        if ((self.topic.is_empty()
            || self.topic.len() > usize::from(u16::MAX)
            || self.topic.as_slice().contains(&0)
            || core::str::from_utf8(self.topic.as_slice()).is_err())
            || self
                .topic
                .as_slice()
                .iter()
                .any(|b| matches!(*b, b'+' | b'#')))
            || self.ingress_order == 0
        {
            return Err(RecipeError::InvalidField("retained_qos0_source"));
        }
        if self
            .targets
            .as_slice()
            .iter()
            .any(|t| t.delivery.publication != self.publication)
        {
            return Err(RecipeError::ForeignDelivery);
        }
        self.retain_targets
            .check(self.targets.len(), "retain_targets")
    }
}

impl ReplaySubscriptions {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        self.update.check_retained()?;
        let op = self.update.operation;
        if self.first_publication.sequence().get() == 0
            || self.first_publication.incarnation() != op.incarnation()
            || self.first_publication.epoch() != op.epoch()
            || self.first_publication.owner() != op.owner()
            || self
                .first_publication
                .sequence()
                .get()
                .checked_add(MAX_REPLAYS as u64 - 1)
                .is_none()
        {
            return Err(RecipeError::InvalidField("replay_identity_range"));
        }
        Ok(())
    }
}

impl RetainedPublication {
    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        let wide = self.admission.targets.len() > LEGACY_MAX_TARGETS;
        out.push(if wide { 20 } else { 10 });
        let mut nested = Vec::new();
        self.admission.encode_retained(&mut nested)?;
        out.extend_from_slice(&(nested.len() as u32).to_be_bytes());
        out.extend_from_slice(&nested);
        if wide {
            self.retain_targets
                .encode_wide(self.admission.targets.len(), out);
        } else {
            let mask = self
                .retain_targets
                .as_u16()
                .ok_or(RecipeError::InvalidField("retain_targets"))?;
            out.extend_from_slice(&mask.to_be_bytes());
        }

        Ok(())
    }
}

impl RetainedQos0 {
    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        let wide = self.targets.len() > LEGACY_MAX_TARGETS;
        out.push(if wide { 21 } else { 11 });
        out.extend_from_slice(&self.operation.to_be_bytes());
        out.extend_from_slice(&self.request.to_be_bytes());
        out.extend_from_slice(&self.publication.to_be_bytes());
        out.extend_from_slice(&self.session.to_be_bytes());
        out.extend_from_slice(&self.session_revision.to_be_bytes());
        out.extend_from_slice(&self.connection.to_be_bytes());
        out.extend_from_slice(&self.ingress_order.to_be_bytes());
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
            self.retain_targets.encode_wide(self.targets.len(), out);
        } else {
            let mask = self
                .retain_targets
                .as_u16()
                .ok_or(RecipeError::InvalidField("retain_targets"))?;
            out.extend_from_slice(&mask.to_be_bytes());
        }

        Ok(())
    }
}

impl ReplaySubscriptions {
    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        out.push(12);
        let mut nested = Vec::new();
        self.update.encode_retained(&mut nested)?;
        out.extend_from_slice(&(nested.len() as u32).to_be_bytes());
        out.extend_from_slice(&nested);
        out.extend_from_slice(&self.first_publication.to_be_bytes());

        Ok(())
    }
}
