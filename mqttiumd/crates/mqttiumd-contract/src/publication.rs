//! An admitted publication: the exchange it came from, its payload, what it
//! costs and who receives it (recipe kind 1). Retained, expiring and will
//! publications are built on it; `command` frames it as a recipe.

use alloc::vec::Vec;

use crate::canonical::{
    KIND_ADMIT_PUBLICATION, MAGIC, RECIPE_FORMAT, Reader, RecipeError, put_exchange_generation,
};
use crate::capacity::{BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use crate::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId, OperationId, PayloadId,
    PublicationId, RequestId, Revision, SessionId,
};
use crate::message::{Qos, SharedBytes};
use crate::pubsub::DeliveryCause;

/// The inbound exchange a publication came from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Source {
    pub session: SessionId,
    pub session_revision: Revision,
    pub packet_id: InboundPacketId,
    pub exchange: ExchangeId,
}

/// A durable immutable payload object and its length.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PayloadRef {
    pub id: PayloadId,
    pub length: PayloadBytes,
}

/// One destination of an admission. The target is a `SessionId`, never a
/// ClientID: a recreated session is a different target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Target {
    pub session: SessionId,
    pub session_revision: Revision,
    pub delivery: DeliveryId,
    pub effective_qos: Qos,
    pub cause: DeliveryCause,
}

/// Durable costs charged by an admission, checked and committed with it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Costs {
    pub payload_bytes: PayloadBytes,
    pub references: References,
    pub exchange_slots: ExchangeSlots,
    pub receipt_slots: ReceiptSlots,
}

/// Commits publication, payload, obligations, inbound QoS 2 state, accounts
/// and receipt together, or nothing.
///
/// Equality is the equality of recipes, not of their storage: two
/// admissions are equal exactly when their canonical bytes are equal. The
/// bound of the container that holds the targets is not part of the
/// recipe and takes no part in the comparison.
#[derive(Debug)]
pub struct AdmitPublication {
    pub operation: OperationId,
    pub request: RequestId,
    pub publication: PublicationId,
    pub source: Source,
    pub route_revision: Revision,
    pub qos: Qos,
    pub retain: bool,
    pub topic: SharedBytes,
    pub payload: PayloadRef,
    pub costs: Costs,
    pub targets: BoundedVec<Target>,
}

impl PartialEq for AdmitPublication {
    fn eq(&self, other: &Self) -> bool {
        // Destructured so that a new field cannot be left out silently.
        let Self {
            operation,
            request,
            publication,
            source,
            route_revision,
            qos,
            retain,
            topic,
            payload,
            costs,
            targets,
        } = self;
        *operation == other.operation
            && *request == other.request
            && *publication == other.publication
            && *source == other.source
            && *route_revision == other.route_revision
            && *qos == other.qos
            && *retain == other.retain
            && *topic == other.topic
            && *payload == other.payload
            && *costs == other.costs
            && targets.as_slice() == other.targets.as_slice()
    }
}

impl Eq for AdmitPublication {}

impl AdmitPublication {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        if self
            .targets
            .as_slice()
            .iter()
            .any(|target| target.cause != DeliveryCause::LiveExact)
        {
            return Err(RecipeError::InvalidField("admission_cause"));
        }
        if self.source.exchange.session != self.source.session {
            return Err(RecipeError::ForeignExchange);
        }
        if self.topic.len() > usize::from(u16::MAX) {
            return Err(RecipeError::TopicTooLong(self.topic.len()));
        }
        if self.targets.len() > usize::from(u16::MAX) {
            return Err(RecipeError::TooManyTargets {
                count: self.targets.len(),
                max: usize::from(u16::MAX),
            });
        }
        if self
            .targets
            .as_slice()
            .iter()
            .any(|target| target.delivery.publication != self.publication)
        {
            return Err(RecipeError::ForeignDelivery);
        }
        Ok(())
    }
}
impl AdmitPublication {
    pub(crate) fn encode_body(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.operation.to_be_bytes());
        out.extend_from_slice(&self.request.to_be_bytes());
        out.extend_from_slice(&self.publication.to_be_bytes());
        out.extend_from_slice(&self.source.session.to_be_bytes());
        out.extend_from_slice(&self.source.session_revision.to_be_bytes());
        out.extend_from_slice(&self.source.packet_id.get().to_be_bytes());
        put_exchange_generation(out, self.source.exchange);
        out.extend_from_slice(&self.route_revision.to_be_bytes());
        out.push(self.qos.to_wire());
        out.push(u8::from(self.retain));
        // Both lengths were bounded by `check` above.
        out.extend_from_slice(&(self.topic.len() as u16).to_be_bytes());
        out.extend_from_slice(self.topic.as_slice());
        out.extend_from_slice(&self.payload.id.to_be_bytes());
        out.extend_from_slice(&self.payload.length.get().to_be_bytes());
        out.extend_from_slice(&self.costs.payload_bytes.get().to_be_bytes());
        out.extend_from_slice(&self.costs.references.get().to_be_bytes());
        out.extend_from_slice(&self.costs.exchange_slots.get().to_be_bytes());
        out.extend_from_slice(&self.costs.receipt_slots.get().to_be_bytes());
        out.extend_from_slice(&(self.targets.len() as u16).to_be_bytes());
        for target in self.targets.as_slice() {
            out.extend_from_slice(&target.session.to_be_bytes());
            out.extend_from_slice(&target.session_revision.to_be_bytes());
            out.extend_from_slice(&target.delivery.index.to_be_bytes());
            out.push(target.effective_qos.to_wire());
            out.push(target.cause as u8);
        }
    }

    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&RECIPE_FORMAT.to_be_bytes());
        out.push(KIND_ADMIT_PUBLICATION);
        self.encode_body(out);
        Ok(())
    }

    pub(crate) fn decode_body(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic_len: usize,
    ) -> Result<Self, RecipeError> {
        let operation = reader.operation()?;
        let request =
            RequestId::from_be_slice(reader.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let publication =
            PublicationId::from_be_slice(reader.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let session = SessionId::new(reader.u64()?);
        let session_revision = Revision::new(reader.u64()?);
        let packet_id = reader.packet_id()?;
        let exchange = ExchangeId {
            session,
            generation: ExchangeGeneration::new(reader.u64()?),
        };
        let route_revision = Revision::new(reader.u64()?);
        let qos = reader.qos()?;
        let retain = reader.boolean("retain")?;
        let topic_len = usize::from(reader.u16()?);
        if topic_len > max_topic_len {
            return Err(RecipeError::TopicTooLong(topic_len));
        }
        let topic = SharedBytes::copy_from(reader.take(topic_len)?, max_topic_len)
            .map_err(|error| RecipeError::TopicTooLong(error.len))?;
        let payload = PayloadRef {
            id: PayloadId::from_be_slice(reader.take(32)?).map_err(|_| RecipeError::Truncated)?,
            length: PayloadBytes::new(reader.u64()?),
        };
        let costs = Costs {
            payload_bytes: PayloadBytes::new(reader.u64()?),
            references: References::new(reader.u64()?),
            exchange_slots: ExchangeSlots::new(reader.u64()?),
            receipt_slots: ReceiptSlots::new(reader.u64()?),
        };
        let count = usize::from(reader.u16()?);
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
                session: SessionId::new(reader.u64()?),
                session_revision: Revision::new(reader.u64()?),
                delivery: DeliveryId {
                    publication,
                    index: reader.u32()?,
                },
                effective_qos: reader.qos()?,
                cause: match reader.u8()? {
                    1 => DeliveryCause::LiveExact,
                    _ => return Err(RecipeError::InvalidField("cause")),
                },
            };
            targets
                .try_push(target)
                .map_err(|_| RecipeError::AllocationFailed)?;
        }
        Ok(Self {
            operation,
            request,
            publication,
            source: Source {
                session,
                session_revision,
                packet_id,
                exchange,
            },
            route_revision,
            qos,
            retain,
            topic,
            payload,
            costs,
            targets,
        })
    }
}
