//! Bounded retained values and transactional subscription replay (ADR 0032).
use crate::capacity::BoundedVec;
use crate::command::{
    AdmitPublication, Costs, PayloadRef, Recipe, RecipeError, Target, UpdateSubscriptions,
};
use crate::ids::{
    ConnectionGeneration, OperationId, PayloadId, PublicationId, RequestId, Revision, SessionId,
};
use crate::packet::SharedBytes;
use alloc::vec::Vec;

pub const MAX_RETAINED_TOPICS: usize = 32;
pub const MAX_RETAINED_BYTES: u64 = 1024 * 1024;
/// Eight ordered filter occurrences, each selecting at most the inventory.
pub const MAX_REPLAYS: usize = crate::pubsub::MAX_FILTERS * MAX_RETAINED_TOPICS;

/// New tag 10: legacy admission bytes retain their historical meaning.
#[derive(PartialEq, Eq, Debug)]
pub struct RetainedPublication {
    pub admission: AdmitPublication,
    /// Bit i preserves the publication RETAIN flag for live target i (RAP).
    pub retain_targets: u16,
}

/// QoS0 has an occurrence, not an invented MQTT Packet Identifier/exchange.
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
    pub retain_targets: u16,
}

/// New tag 12. All replay publication identities are reserved before submission.
/// The store consumes only a prefix; unused identities are burned, never reused.
#[derive(PartialEq, Eq, Debug)]
pub struct ReplaySubscriptions {
    pub update: UpdateSubscriptions,
    pub first_publication: PublicationId,
}

pub(crate) fn check(recipe: &Recipe) -> Result<(), RecipeError> {
    match recipe {
        Recipe::RetainedPublication(value) => value.check_retained(),
        Recipe::RetainedQos0(value) => value.check_retained(),
        Recipe::ReplaySubscriptions(value) => value.check_retained(),
        _ => Err(RecipeError::InvalidField("retained_recipe")),
    }
}
fn check_mask(mask: u16, len: usize) -> Result<(), RecipeError> {
    if len > 16 || (len < 16 && (u32::from(mask) >> len) != 0) {
        return Err(RecipeError::InvalidField("retain_targets"));
    }
    Ok(())
}

pub(crate) fn encode(recipe: &Recipe, out: &mut Vec<u8>) -> Result<(), RecipeError> {
    match recipe {
        Recipe::RetainedPublication(value) => value.encode_retained(out)?,
        Recipe::ReplaySubscriptions(value) => value.encode_retained(out)?,
        Recipe::RetainedQos0(value) => value.encode_retained(out)?,
        _ => return Err(RecipeError::InvalidField("retained_recipe")),
    }
    Ok(())
}

pub(crate) fn decode(
    kind: u8,
    bytes: &mut &[u8],
    max_targets: usize,
    max_topic: usize,
) -> Result<Recipe, RecipeError> {
    use crate::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
    use crate::command::{DeliveryCause, Reader};
    use crate::ids::DeliveryId;
    let mut r = Reader { bytes };
    let recipe = if kind == 10 || kind == 12 {
        let len = r.u32()? as usize;
        let nested = r.take(len)?;
        // Reject nested extensions before decoding: depth is exactly one.
        if nested.len() < 7
            || (kind == 10 && nested[6] != 1)
            || (kind == 12 && !matches!(nested[6], 7 | 9))
        {
            return Err(RecipeError::InvalidField("retained_nested_kind"));
        }
        match Recipe::decode_canonical(nested, max_targets, max_topic)? {
            Recipe::AdmitPublication(admission) if kind == 10 => {
                Recipe::RetainedPublication(RetainedPublication {
                    admission,
                    retain_targets: r.u16()?,
                })
            }
            Recipe::UpdateSubscriptions(update) if kind == 12 => {
                Recipe::ReplaySubscriptions(ReplaySubscriptions {
                    update,
                    first_publication: PublicationId::from_be_slice(r.take(32)?)
                        .map_err(|_| RecipeError::Truncated)?,
                })
            }
            _ => return Err(RecipeError::InvalidField("retained_nested_kind")),
        }
    } else {
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
        let count = usize::from(r.u8()?);
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
        Recipe::RetainedQos0(RetainedQos0 {
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
            retain_targets: r.u16()?,
        })
    };
    *bytes = r.bytes;
    Ok(recipe)
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
        if self.admission.qos == crate::packet::Qos::AtMostOnce {
            return Err(RecipeError::InvalidField("retained_qos"));
        }
        if !self.admission.retain {
            return Err(RecipeError::InvalidField("retained_flag"));
        }
        check_mask(self.retain_targets, self.admission.targets.len())
    }
}

impl RetainedQos0 {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        if self
            .targets
            .as_slice()
            .iter()
            .any(|target| target.cause != crate::command::DeliveryCause::LiveExact)
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
        check_mask(self.retain_targets, self.targets.len())
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
        out.push(10);
        let mut nested = Vec::new();
        self.admission.encode_retained(&mut nested)?;
        out.extend_from_slice(&(nested.len() as u32).to_be_bytes());
        out.extend_from_slice(&nested);
        out.extend_from_slice(&self.retain_targets.to_be_bytes());

        Ok(())
    }
}

impl RetainedQos0 {
    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        out.push(11);
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
        out.push(self.targets.len() as u8);
        for target in self.targets.as_slice() {
            out.extend_from_slice(&target.session.to_be_bytes());
            out.extend_from_slice(&target.session_revision.to_be_bytes());
            out.extend_from_slice(&target.delivery.index.to_be_bytes());
            out.push(target.effective_qos.to_wire());
            out.push(target.cause as u8);
        }
        out.extend_from_slice(&self.retain_targets.to_be_bytes());

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
