//! Frozen commands and their exact canonical representation (DEM-009).
//!
//! An `OperationId` designates exactly one recipe. The store keeps the
//! canonical bytes of the recipe and compares them byte for byte when a
//! command is presented again; a digest may speed up a lookup but never
//! replaces that comparison.
//!
//! `OpenEpoch` is the one command that is not a recipe: it opens the run
//! epoch every `OperationId` is scoped by. It lives in `opening` with
//! its durable state, the `EpochLedger`, and with what start-up does
//! with the reply (ADR 0009); its types are re-exported here.
//!
//! Canonical layout, format 1. All integers are big-endian.
//!
//! ```text
//! "MQDR"  format:u16  kind:u8
//! kind 1, AdmitPublication:
//!   operation:32 request:32 publication:32
//!   source.session:8 source.session_revision:8 source.packet_id:2
//!   source.exchange_generation:8
//!   route_revision:8  qos:u8  retain:u8
//!   topic_len:u16 topic
//!   payload.id:32 payload.length:8
//!   costs: payload_bytes:8 references:8 exchange_slots:8 receipt_slots:8
//!   target_count:u16, then per target:
//!     session:8 session_revision:8 delivery_index:u32 qos:u8 cause:u8
//! kind 2, ReleaseInbound:
//!   operation:32 session:8 packet_id:2 exchange_generation:8
//! kind 3, ChangeSubscriptions:
//!   operation:32 session:8 session_revision:8 route_revision:8 count:u8
//!   per change: subscribe:u8 topic_len:u16 topic
//! kind 4, ClaimDelivery:
//!   operation:32 publication:32 delivery_index:u32 target:8 target_revision:8 packet_id:2
//! kind 5, FinishDelivery:
//!   the kind-4 fields, then claim_operation:32
//! kind 6, ConsumeDelivery:
//!   operation:32 publication:32 delivery_index:u32 target:8 target_revision:8
//! kind 7, UpdateSubscriptions:
//!   kind-3 header; each action is 0(remove), 1(grant0), 2(grant1), then topic
//! kind 13, ExpiringPublication:
//!   nested_len:u32 nested_legacy_recipe (kind 1, 10 or 11)
//!   received.domain:32 received.tick:8 interval_seconds:u32
//! kind 14, TimedReplaySubscriptions:
//!   nested_len:u32 nested_legacy_recipe (kind 12) observed.domain:32 observed.tick:8
//! kind 15, MarkDeliveryStarted: the kind-5 fields
//! kind 16, ExpireDelivery:
//!   the kind-5 fields, received.domain:32 received.tick:8 interval_seconds:u32
//!   observed.domain:32 observed.tick:8
//! ```

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::capacity::{BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use crate::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId, OperationId, OutboundPacketId,
    PayloadId, PublicationId, RequestId, Revision, SessionId,
};
pub use crate::message_expiry::{
    ExpireDelivery, ExpiringAdmission, ExpiringPublication, MarkDeliveryStarted,
    TimedReplaySubscriptions,
};
use crate::packet::{Qos, SharedBytes};
use crate::pubsub::{GrantedSubscriptionChange, MAX_FILTERS, SubscriptionChange};

const MAGIC: &[u8; 4] = b"MQDR";

/// The only recipe format of contract revision 1.
pub const RECIPE_FORMAT: u16 = 1;

const KIND_ADMIT_PUBLICATION: u8 = 1;
const KIND_RELEASE_INBOUND: u8 = 2;
const KIND_CHANGE_SUBSCRIPTIONS: u8 = 3;
const KIND_CLAIM_DELIVERY: u8 = 4;
const KIND_FINISH_DELIVERY: u8 = 5;
const KIND_CONSUME_DELIVERY: u8 = 6;
const KIND_UPDATE_SUBSCRIPTIONS: u8 = 7;
const KIND_SUBSCRIPTION_OPTIONS: u8 = 9;
const KIND_ANNOTATED_PUBLICATION: u8 = 23;
const KIND_CLAIMED_PUBLICATION: u8 = 24;

/// Why an obligation exists. The cause stays inspectable by the oracle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeliveryCause {
    /// A live publication selected the committed routing cut. The historical
    /// name and byte value also cover aggregated hierarchical subscriptions.
    LiveExact = 1,
    RetainedReplay = 2,
}

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

#[derive(Debug)]
enum TargetStorage {
    Empty,
    One(Target),
    Many(BoundedVec<Target>),
}

/// Bounded target list storing 0 or 1 destinations inline (zero heap
/// allocation on the dominant 1:1 pub/sub path) and spilling to a
/// [`BoundedVec`] for multi-subscriber fan-out.
#[derive(Debug)]
pub struct TargetVec {
    storage: TargetStorage,
    capacity: usize,
}

impl TargetVec {
    /// Reserves capacity for up to `capacity` targets without allocating when
    /// `capacity <= 1`.
    pub fn with_capacity(capacity: usize) -> Result<Self, crate::capacity::AllocationFailed> {
        let storage = if capacity <= 1 {
            TargetStorage::Empty
        } else {
            TargetStorage::Many(BoundedVec::with_capacity(capacity)?)
        };
        Ok(Self { storage, capacity })
    }

    /// Builds a bounded target list holding a copy of `items`, with bound
    /// `capacity`. Returns `None` when `items.len() > capacity` or allocation
    /// fails.
    #[must_use]
    pub fn from_slice(items: &[Target], capacity: usize) -> Option<Self> {
        if items.len() > capacity {
            return None;
        }
        let storage = match items {
            [] => TargetStorage::Empty,
            [one] => TargetStorage::One(*one),
            many => TargetStorage::Many(BoundedVec::from_slice(many, capacity)?),
        };
        Some(Self { storage, capacity })
    }

    /// Appends `target` if the bound has not been reached.
    pub fn try_push(&mut self, target: Target) -> Result<(), Target> {
        match &mut self.storage {
            TargetStorage::Empty => {
                if self.capacity == 0 {
                    return Err(target);
                }
                self.storage = TargetStorage::One(target);
                Ok(())
            }
            TargetStorage::One(first) => {
                if self.capacity <= 1 {
                    return Err(target);
                }
                let Ok(mut vec) = BoundedVec::with_capacity(self.capacity) else {
                    return Err(target);
                };
                let _ = vec.try_push(*first);
                let _ = vec.try_push(target);
                self.storage = TargetStorage::Many(vec);
                Ok(())
            }
            TargetStorage::Many(vec) => vec.try_push(target),
        }
    }

    /// Removes and returns the last target.
    pub fn pop(&mut self) -> Option<Target> {
        match &mut self.storage {
            TargetStorage::Empty => None,
            TargetStorage::One(_) => {
                let TargetStorage::One(target) =
                    core::mem::replace(&mut self.storage, TargetStorage::Empty)
                else {
                    panic!("target storage was matched as One");
                };
                Some(target)
            }
            TargetStorage::Many(vec) => vec.pop(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.storage {
            TargetStorage::Empty => 0,
            TargetStorage::One(_) => 1,
            TargetStorage::Many(vec) => vec.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn as_slice(&self) -> &[Target] {
        match &self.storage {
            TargetStorage::Empty => &[],
            TargetStorage::One(target) => core::slice::from_ref(target),
            TargetStorage::Many(vec) => vec.as_slice(),
        }
    }

    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [Target] {
        match &mut self.storage {
            TargetStorage::Empty => &mut [],
            TargetStorage::One(target) => core::slice::from_mut(target),
            TargetStorage::Many(vec) => vec.as_mut_slice(),
        }
    }

    /// Converts into a [`BoundedVec<Target>`] with the same bound.
    #[must_use]
    pub fn into_bounded(self) -> BoundedVec<Target> {
        match self.storage {
            TargetStorage::Empty => BoundedVec::with_capacity(self.capacity).expect("empty fits"),
            TargetStorage::One(target) => {
                BoundedVec::from_slice(&[target], self.capacity).expect("one fits")
            }
            TargetStorage::Many(vec) => vec,
        }
    }
}

impl From<BoundedVec<Target>> for TargetVec {
    fn from(vec: BoundedVec<Target>) -> Self {
        let capacity = vec.capacity();
        match vec.as_slice() {
            [] => Self {
                storage: TargetStorage::Empty,
                capacity,
            },
            [one] => Self {
                storage: TargetStorage::One(*one),
                capacity,
            },
            _ => Self {
                storage: TargetStorage::Many(vec),
                capacity,
            },
        }
    }
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
    pub targets: TargetVec,
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

/// Durably terminates an inbound QoS 2 exchange after PUBREL, before PUBCOMP
/// becomes eligible. It never creates a distribution.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReleaseInbound {
    pub operation: OperationId,
    pub source_session: SessionId,
    pub packet_id: InboundPacketId,
    pub exchange: ExchangeId,
}

#[derive(Debug)]
pub struct ChangeSubscriptions {
    pub operation: OperationId,
    pub session: SessionId,
    pub session_revision: Revision,
    pub route_revision: Revision,
    pub changes: BoundedVec<SubscriptionChange>,
}
impl PartialEq for ChangeSubscriptions {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            operation,
            session,
            session_revision,
            route_revision,
            changes,
        } = self;
        *operation == other.operation
            && *session == other.session
            && *session_revision == other.session_revision
            && *route_revision == other.route_revision
            && changes.as_slice() == other.changes.as_slice()
    }
}
impl Eq for ChangeSubscriptions {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ClaimDelivery {
    pub operation: OperationId,
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub packet_id: OutboundPacketId,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FinishDelivery {
    pub operation: OperationId,
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub packet_id: OutboundPacketId,
    pub claim: OperationId,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ConsumeDelivery {
    pub operation: OperationId,
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
}
#[derive(Debug)]
pub struct UpdateSubscriptions {
    pub operation: OperationId,
    pub session: SessionId,
    pub session_revision: Revision,
    pub route_revision: Revision,
    pub changes: BoundedVec<GrantedSubscriptionChange>,
}
impl PartialEq for UpdateSubscriptions {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            operation,
            session,
            session_revision,
            route_revision,
            changes,
        } = self;
        *operation == other.operation
            && *session == other.session
            && *session_revision == other.session_revision
            && *route_revision == other.route_revision
            && changes.as_slice() == other.changes.as_slice()
    }
}
impl Eq for UpdateSubscriptions {}

/// Inline claim of one QoS 1/2 target created atomically by an admission.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct InlineClaim {
    pub target_index: u16,
    pub packet_id: OutboundPacketId,
}

/// Commits an `AdmitPublication` and claims one of its QoS 1/2 targets in the
/// same durable transaction (recipe kind 24).
#[derive(PartialEq, Eq, Debug)]
pub struct ClaimedPublication {
    pub admission: AdmitPublication,
    pub claim: InlineClaim,
}

impl ClaimedPublication {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        self.admission.check_retained()?;
        if self.admission.retain {
            return Err(RecipeError::InvalidField("inline_claim_retain"));
        }
        let Some(target) = self
            .admission
            .targets
            .as_slice()
            .get(usize::from(self.claim.target_index))
        else {
            return Err(RecipeError::InvalidField("inline_claim_target"));
        };
        if target.effective_qos == Qos::AtMostOnce {
            return Err(RecipeError::InvalidField("inline_claim_qos"));
        }
        Ok(())
    }
}

/// Wraps a publication admission recipe with non-empty MQTT 5 PUBLISH
/// application properties (ADR 0053, recipe kind 23).
#[derive(PartialEq, Eq, Debug)]
pub struct AnnotatedPublication {
    pub inner: Box<Recipe>,
    pub properties: Arc<crate::properties::PublishProperties>,
}

impl AnnotatedPublication {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        if self.properties.is_empty() {
            return Err(RecipeError::InvalidField("empty_publish_properties"));
        }
        self.properties.check()?;
        match self.inner.as_ref() {
            Recipe::AdmitPublication(_)
            | Recipe::ClaimedPublication(_)
            | Recipe::RetainedPublication(_)
            | Recipe::RetainedQos0(_)
            | Recipe::ExpiringPublication(_) => self.inner.check(),
            _ => Err(RecipeError::InvalidField(
                "annotated_publication_inner_kind",
            )),
        }
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        let mut props = Vec::new();
        self.properties.encode_canonical(&mut props)?;
        let props_len = u32::try_from(props.len())
            .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?;
        let mut nested = Vec::new();
        self.inner.encode_canonical(&mut nested)?;
        let nested_len = u32::try_from(nested.len())
            .map_err(|_| RecipeError::InvalidField("annotated_publication_nested_length"))?;
        out.push(KIND_ANNOTATED_PUBLICATION);
        out.extend_from_slice(&props_len.to_be_bytes());
        out.extend_from_slice(&props);
        out.extend_from_slice(&nested_len.to_be_bytes());
        out.extend_from_slice(&nested);
        Ok(())
    }

    pub(crate) fn decode(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic_len: usize,
    ) -> Result<Self, RecipeError> {
        let props_len = usize::try_from(reader.u32()?)
            .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?;
        // Bounded canonical properties size: logical bytes + fixed framing overhead.
        if props_len == 0 || props_len > crate::properties::MAX_PROPERTY_BYTES + 1024 {
            return Err(RecipeError::InvalidField("publish_properties_size"));
        }
        let props_bytes = reader.take(props_len)?;
        let properties = Arc::new(crate::properties::PublishProperties::decode_canonical(
            props_bytes,
        )?);
        let nested_len = usize::try_from(reader.u32()?)
            .map_err(|_| RecipeError::InvalidField("annotated_publication_nested_length"))?;
        let nested = reader.take(nested_len)?;
        if nested.len() < 7 || !matches!(nested[6], 1 | 10 | 11 | 13 | 20 | 21 | 24) {
            return Err(RecipeError::InvalidField(
                "annotated_publication_inner_kind",
            ));
        }
        let inner = Box::new(Recipe::decode_canonical(
            nested,
            max_targets,
            max_topic_len,
        )?);
        let value = Self { inner, properties };
        value.check()?;
        Ok(value)
    }
}

/// The closed set of durable recipes of the received perimeter.
#[derive(PartialEq, Eq, Debug)]
pub enum Recipe {
    AnnotatedPublication(AnnotatedPublication),
    WillPublication(crate::will::WillPublication),
    ExpiringPublication(ExpiringPublication),
    TimedReplaySubscriptions(TimedReplaySubscriptions),
    MarkDeliveryStarted(MarkDeliveryStarted),
    ExpireDelivery(ExpireDelivery),
    RetainedPublication(crate::retained::RetainedPublication),
    RetainedQos0(crate::retained::RetainedQos0),
    ReplaySubscriptions(crate::retained::ReplaySubscriptions),
    SessionLifecycle(crate::session::SessionLifecycle),
    AdmitPublication(AdmitPublication),
    ClaimedPublication(ClaimedPublication),
    ReleaseInbound(ReleaseInbound),
    ChangeSubscriptions(ChangeSubscriptions),
    ClaimDelivery(ClaimDelivery),
    FinishDelivery(FinishDelivery),
    /// Record a positive PUBREC against the exact claim before sending PUBREL.
    PreparePubrel(FinishDelivery),
    ConsumeDelivery(ConsumeDelivery),
    UpdateSubscriptions(UpdateSubscriptions),
}

/// A recipe is not well formed, or cannot be decoded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecipeError {
    /// The bytes end before the layout does.
    Truncated,
    /// Bytes remain after the layout ends.
    TrailingBytes,
    /// The first four bytes are not the recipe magic.
    BadMagic,
    /// The format number is not one this revision reads.
    UnknownFormat(u16),
    /// The kind tag is not in the closed set.
    UnknownKind(u8),
    /// A field holds a value outside its closed domain.
    InvalidField(&'static str),
    /// More targets than the profile admits.
    TooManyTargets { count: usize, max: usize },
    /// The topic does not fit its two-byte length.
    TopicTooLong(usize),
    /// A delivery does not belong to the recipe's publication.
    ForeignDelivery,
    /// The source exchange does not belong to the source session.
    ForeignExchange,
    /// Storage for the decoded recipe could not be allocated.
    AllocationFailed,
}

fn put_exchange_generation(out: &mut Vec<u8>, exchange: ExchangeId) {
    out.extend_from_slice(&exchange.generation.to_be_bytes());
}

fn encode_delivery(
    out: &mut Vec<u8>,
    operation: OperationId,
    delivery: DeliveryId,
    target: SessionId,
    revision: Revision,
    packet_id: OutboundPacketId,
) {
    out.extend_from_slice(&operation.to_be_bytes());
    out.extend_from_slice(&delivery.publication.to_be_bytes());
    out.extend_from_slice(&delivery.index.to_be_bytes());
    out.extend_from_slice(&target.to_be_bytes());
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&packet_id.get().to_be_bytes());
}

impl Recipe {
    /// The acknowledged admission, including the explicitly versioned retained
    /// variant. QoS0 deliberately has no exchange-shaped admission.
    #[must_use]
    pub fn admission(&self) -> Option<&AdmitPublication> {
        match self {
            Self::AnnotatedPublication(value) => value.inner.admission(),
            Self::AdmitPublication(admission) => Some(admission),
            Self::ClaimedPublication(claimed) => Some(&claimed.admission),
            Self::RetainedPublication(value) => Some(&value.admission),
            Self::ExpiringPublication(ExpiringPublication {
                admission: ExpiringAdmission::Qos12(value),
                ..
            }) => Some(&value.admission),
            _ => None,
        }
    }
    /// A QoS0 retained admission, including its timed envelope.
    #[must_use]
    pub fn retained_qos0(&self) -> Option<&crate::retained::RetainedQos0> {
        match self {
            Self::AnnotatedPublication(value) => value.inner.retained_qos0(),
            Self::RetainedQos0(value) => Some(value),
            Self::ExpiringPublication(ExpiringPublication {
                admission: ExpiringAdmission::Qos0(value),
                ..
            }) => Some(value),
            _ => None,
        }
    }
    /// Optional MQTT 5 PUBLISH application properties carried by this recipe.
    #[must_use]
    pub fn properties(&self) -> Option<&Arc<crate::properties::PublishProperties>> {
        match self {
            Self::AnnotatedPublication(value) => Some(&value.properties),
            _ => None,
        }
    }
    /// Unwraps an `AnnotatedPublication` envelope to its inner publication recipe.
    #[must_use]
    pub fn inner_publication(&self) -> &Self {
        match self {
            Self::AnnotatedPublication(value) => value.inner.as_ref(),
            other => other,
        }
    }
    /// Subscription plus frozen replay, including its timed envelope.
    #[must_use]
    pub fn replay_subscriptions(&self) -> Option<&crate::retained::ReplaySubscriptions> {
        match self {
            Self::ReplaySubscriptions(value) => Some(value),
            Self::TimedReplaySubscriptions(value) => Some(&value.replay),
            _ => None,
        }
    }
    /// The operation this recipe belongs to.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        match self {
            Self::AnnotatedPublication(value) => value.inner.operation(),
            Self::WillPublication(value) => value.operation,
            Self::ExpiringPublication(value) => match &value.admission {
                ExpiringAdmission::Qos12(value) => value.admission.operation,
                ExpiringAdmission::Qos0(value) => value.operation,
            },
            Self::TimedReplaySubscriptions(value) => value.replay.update.operation,
            Self::MarkDeliveryStarted(value) => value.operation,
            Self::ExpireDelivery(value) => value.finish.operation,
            Self::RetainedPublication(value) => value.admission.operation,
            Self::RetainedQos0(value) => value.operation,
            Self::ReplaySubscriptions(value) => value.update.operation,
            Self::SessionLifecycle(change) => change.operation,
            Self::AdmitPublication(admit) => admit.operation,
            Self::ClaimedPublication(claimed) => claimed.admission.operation,
            Self::ReleaseInbound(release) => release.operation,
            Self::ChangeSubscriptions(change) => change.operation,
            Self::ClaimDelivery(claim) => claim.operation,
            Self::FinishDelivery(finish) | Self::PreparePubrel(finish) => finish.operation,
            Self::ConsumeDelivery(consume) => consume.operation,
            Self::UpdateSubscriptions(change) => change.operation,
        }
    }

    /// Checks the internal consistency the canonical layout relies on.
    pub fn check(&self) -> Result<(), RecipeError> {
        match self {
            Self::AnnotatedPublication(value) => value.check(),
            Self::WillPublication(value) => value.check(),
            Self::ExpiringPublication(value) => value.check(),
            Self::TimedReplaySubscriptions(value) => value.check(),
            Self::MarkDeliveryStarted(_) => Ok(()),
            Self::ExpireDelivery(value) => value.check(),
            Self::RetainedPublication(_) | Self::RetainedQos0(_) | Self::ReplaySubscriptions(_) => {
                crate::retained::check(self)
            }
            Self::SessionLifecycle(change) => change.check(),
            Self::UpdateSubscriptions(change) => change.check_retained(),
            Self::ChangeSubscriptions(change) => {
                if change.changes.is_empty() || change.changes.len() > MAX_FILTERS {
                    return Err(RecipeError::InvalidField("subscription_count"));
                }
                for entry in change.changes.as_slice() {
                    if entry.topic.is_empty()
                        || entry.topic.len() > usize::from(u16::MAX)
                        || entry
                            .topic
                            .as_slice()
                            .iter()
                            .any(|b| matches!(*b, 0 | b'+' | b'#'))
                        || core::str::from_utf8(entry.topic.as_slice()).is_err()
                    {
                        return Err(RecipeError::InvalidField("exact_filter"));
                    }
                }
                Ok(())
            }
            Self::ClaimDelivery(_)
            | Self::FinishDelivery(_)
            | Self::PreparePubrel(_)
            | Self::ConsumeDelivery(_) => Ok(()),
            Self::AdmitPublication(admit) => admit.check_retained(),
            Self::ClaimedPublication(claimed) => claimed.check(),
            Self::ReleaseInbound(release) => {
                if release.exchange.session != release.source_session {
                    return Err(RecipeError::ForeignExchange);
                }
                Ok(())
            }
        }
    }

    /// Appends the canonical representation to `out`.
    ///
    /// Two recipes are the same recipe exactly when these bytes are equal.
    pub fn encode_canonical(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        self.check()?;
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&RECIPE_FORMAT.to_be_bytes());
        match self {
            Self::AnnotatedPublication(value) => value.encode(out)?,
            Self::WillPublication(value) => value.encode(out),
            Self::ExpiringPublication(value) => value.encode(out)?,
            Self::TimedReplaySubscriptions(value) => value.encode(out)?,
            Self::MarkDeliveryStarted(value) => {
                out.push(15);
                encode_delivery(
                    out,
                    value.operation,
                    value.delivery,
                    value.target,
                    value.target_revision,
                    value.packet_id,
                );
                out.extend_from_slice(&value.claim.to_be_bytes());
            }
            Self::ExpireDelivery(value) => {
                out.push(16);
                let finish = value.finish;
                encode_delivery(
                    out,
                    finish.operation,
                    finish.delivery,
                    finish.target,
                    finish.target_revision,
                    finish.packet_id,
                );
                out.extend_from_slice(&finish.claim.to_be_bytes());
                value.expiry.encode(out);
                value.observed.encode(out);
            }
            Self::RetainedPublication(_) | Self::RetainedQos0(_) | Self::ReplaySubscriptions(_) => {
                crate::retained::encode(self, out)?
            }
            Self::SessionLifecycle(change) => {
                out.push(8);
                change.encode(out);
            }
            Self::ConsumeDelivery(consume) => {
                out.push(KIND_CONSUME_DELIVERY);
                out.extend_from_slice(&consume.operation.to_be_bytes());
                out.extend_from_slice(&consume.delivery.publication.to_be_bytes());
                out.extend_from_slice(&consume.delivery.index.to_be_bytes());
                out.extend_from_slice(&consume.target.to_be_bytes());
                out.extend_from_slice(&consume.target_revision.to_be_bytes());
            }
            Self::UpdateSubscriptions(change) => {
                let mut nested = Vec::new();
                change.encode_retained(&mut nested)?;
                out.extend_from_slice(&nested[6..]);
            }
            Self::ChangeSubscriptions(change) => {
                out.push(KIND_CHANGE_SUBSCRIPTIONS);
                out.extend_from_slice(&change.operation.to_be_bytes());
                out.extend_from_slice(&change.session.to_be_bytes());
                out.extend_from_slice(&change.session_revision.to_be_bytes());
                out.extend_from_slice(&change.route_revision.to_be_bytes());
                out.push(change.changes.len() as u8);
                for entry in change.changes.as_slice() {
                    out.push(u8::from(entry.subscribe));
                    out.extend_from_slice(&(entry.topic.len() as u16).to_be_bytes());
                    out.extend_from_slice(entry.topic.as_slice());
                }
            }
            Self::ClaimDelivery(claim) => {
                out.push(KIND_CLAIM_DELIVERY);
                encode_delivery(
                    out,
                    claim.operation,
                    claim.delivery,
                    claim.target,
                    claim.target_revision,
                    claim.packet_id,
                );
            }
            Self::FinishDelivery(finish) | Self::PreparePubrel(finish) => {
                out.push(if matches!(self, Self::PreparePubrel(_)) {
                    19
                } else {
                    KIND_FINISH_DELIVERY
                });
                encode_delivery(
                    out,
                    finish.operation,
                    finish.delivery,
                    finish.target,
                    finish.target_revision,
                    finish.packet_id,
                );
                out.extend_from_slice(&finish.claim.to_be_bytes());
            }
            Self::AdmitPublication(admit) => {
                out.push(KIND_ADMIT_PUBLICATION);
                admit.encode_body(out);
            }
            Self::ClaimedPublication(claimed) => {
                out.push(KIND_CLAIMED_PUBLICATION);
                claimed.admission.encode_body(out);
                out.extend_from_slice(&claimed.claim.target_index.to_be_bytes());
                out.extend_from_slice(&claimed.claim.packet_id.get().to_be_bytes());
            }
            Self::ReleaseInbound(release) => {
                out.push(KIND_RELEASE_INBOUND);
                out.extend_from_slice(&release.operation.to_be_bytes());
                out.extend_from_slice(&release.source_session.to_be_bytes());
                out.extend_from_slice(&release.packet_id.get().to_be_bytes());
                put_exchange_generation(out, release.exchange);
            }
        }
        Ok(())
    }

    /// Decodes a canonical representation, refusing anything that is not the
    /// unique encoding of a well-formed recipe with at most `max_targets`
    /// targets and a topic of at most `max_topic_len` bytes.
    pub fn decode_canonical(
        bytes: &[u8],
        max_targets: usize,
        max_topic_len: usize,
    ) -> Result<Self, RecipeError> {
        let mut reader = Reader { bytes };
        if reader.take(4)? != MAGIC {
            return Err(RecipeError::BadMagic);
        }
        let format = reader.u16()?;
        if format != RECIPE_FORMAT {
            return Err(RecipeError::UnknownFormat(format));
        }
        let recipe = match reader.u8()? {
            KIND_ANNOTATED_PUBLICATION => Self::AnnotatedPublication(AnnotatedPublication::decode(
                &mut reader,
                max_targets,
                max_topic_len,
            )?),
            tag @ (17 | 18 | 22) => Self::WillPublication(crate::will::WillPublication::decode(
                &mut reader,
                max_targets,
                max_topic_len,
                tag,
            )?),
            13 => Self::ExpiringPublication(ExpiringPublication::decode(
                &mut reader,
                max_targets,
                max_topic_len,
            )?),
            14 => Self::TimedReplaySubscriptions(TimedReplaySubscriptions::decode(
                &mut reader,
                max_targets,
                max_topic_len,
            )?),
            15 => {
                let (operation, delivery, target, target_revision, packet_id) =
                    reader.delivery()?;
                Self::MarkDeliveryStarted(MarkDeliveryStarted {
                    operation,
                    delivery,
                    target,
                    target_revision,
                    packet_id,
                    claim: reader.operation()?,
                })
            }
            16 => {
                let (operation, delivery, target, target_revision, packet_id) =
                    reader.delivery()?;
                let finish = FinishDelivery {
                    operation,
                    delivery,
                    target,
                    target_revision,
                    packet_id,
                    claim: reader.operation()?,
                };
                Self::ExpireDelivery(ExpireDelivery {
                    finish,
                    expiry: crate::message_expiry::MessageExpiry::decode(&mut reader)?,
                    observed: crate::message_expiry::MessageTime::decode(&mut reader)?,
                })
            }
            kind @ (10..=12 | 20 | 21) => {
                crate::retained::decode(kind, &mut reader.bytes, max_targets, max_topic_len)?
            }
            8 => {
                Self::SessionLifecycle(crate::session::SessionLifecycle::decode(&mut reader.bytes)?)
            }
            KIND_CONSUME_DELIVERY => {
                let operation = reader.operation()?;
                let publication = PublicationId::from_be_slice(reader.take(32)?)
                    .map_err(|_| RecipeError::Truncated)?;
                let delivery = DeliveryId {
                    publication,
                    index: reader.u32()?,
                };
                let target = SessionId::new(reader.u64()?);
                let target_revision = Revision::new(reader.u64()?);
                Self::ConsumeDelivery(ConsumeDelivery {
                    operation,
                    delivery,
                    target,
                    target_revision,
                })
            }
            kind @ (KIND_UPDATE_SUBSCRIPTIONS | KIND_SUBSCRIPTION_OPTIONS) => {
                let operation = reader.operation()?;
                let session = SessionId::new(reader.u64()?);
                let session_revision = Revision::new(reader.u64()?);
                let route_revision = Revision::new(reader.u64()?);
                let count = usize::from(reader.u8()?);
                if count == 0 || count > MAX_FILTERS {
                    return Err(RecipeError::InvalidField("subscription_count"));
                }
                let mut changes =
                    BoundedVec::with_capacity(count).map_err(|_| RecipeError::AllocationFailed)?;
                for _ in 0..count {
                    let granted = match reader.u8()? {
                        0 => None,
                        1 => Some(Qos::AtMostOnce),
                        2 => Some(Qos::AtLeastOnce),
                        3 => Some(Qos::ExactlyOnce),
                        _ => return Err(RecipeError::InvalidField("granted_qos")),
                    };
                    let options = if kind == KIND_SUBSCRIPTION_OPTIONS {
                        crate::pubsub::SubscriptionOptions::from_bits(reader.u8()?)
                            .ok_or(RecipeError::InvalidField("subscription_options"))?
                    } else {
                        crate::pubsub::SubscriptionOptions::DEFAULT
                    };
                    let len = usize::from(reader.u16()?);
                    if len > max_topic_len {
                        return Err(RecipeError::TopicTooLong(len));
                    }
                    let topic = SharedBytes::copy_from(reader.take(len)?, max_topic_len)
                        .map_err(|_| RecipeError::TopicTooLong(len))?;
                    changes
                        .try_push(GrantedSubscriptionChange {
                            topic,
                            granted,
                            options,
                        })
                        .map_err(|_| RecipeError::AllocationFailed)?;
                }
                if kind == KIND_SUBSCRIPTION_OPTIONS
                    && changes
                        .as_slice()
                        .iter()
                        .all(|c| c.options == crate::pubsub::SubscriptionOptions::DEFAULT)
                {
                    return Err(RecipeError::InvalidField(
                        "noncanonical_subscription_options",
                    ));
                }
                Self::UpdateSubscriptions(UpdateSubscriptions {
                    operation,
                    session,
                    session_revision,
                    route_revision,
                    changes,
                })
            }
            KIND_CHANGE_SUBSCRIPTIONS => {
                let operation = reader.operation()?;
                let session = SessionId::new(reader.u64()?);
                let session_revision = Revision::new(reader.u64()?);
                let route_revision = Revision::new(reader.u64()?);
                let count = usize::from(reader.u8()?);
                if count == 0 || count > MAX_FILTERS {
                    return Err(RecipeError::InvalidField("subscription_count"));
                }
                let mut changes =
                    BoundedVec::with_capacity(count).map_err(|_| RecipeError::AllocationFailed)?;
                for _ in 0..count {
                    let subscribe = reader.boolean("subscribe")?;
                    let len = usize::from(reader.u16()?);
                    if len > max_topic_len {
                        return Err(RecipeError::TopicTooLong(len));
                    }
                    let topic = SharedBytes::copy_from(reader.take(len)?, max_topic_len)
                        .map_err(|_| RecipeError::TopicTooLong(len))?;
                    changes
                        .try_push(SubscriptionChange { topic, subscribe })
                        .map_err(|_| RecipeError::AllocationFailed)?;
                }
                Self::ChangeSubscriptions(ChangeSubscriptions {
                    operation,
                    session,
                    session_revision,
                    route_revision,
                    changes,
                })
            }
            KIND_CLAIM_DELIVERY => {
                let (operation, delivery, target, target_revision, packet_id) =
                    reader.delivery()?;
                Self::ClaimDelivery(ClaimDelivery {
                    operation,
                    delivery,
                    target,
                    target_revision,
                    packet_id,
                })
            }
            kind @ (KIND_FINISH_DELIVERY | 19) => {
                let (operation, delivery, target, target_revision, packet_id) =
                    reader.delivery()?;
                let claim = reader.operation()?;
                let delivery = FinishDelivery {
                    operation,
                    delivery,
                    target,
                    target_revision,
                    packet_id,
                    claim,
                };
                if kind == 19 {
                    Self::PreparePubrel(delivery)
                } else {
                    Self::FinishDelivery(delivery)
                }
            }
            KIND_ADMIT_PUBLICATION => Self::AdmitPublication(AdmitPublication::decode_body(
                &mut reader,
                max_targets,
                max_topic_len,
            )?),
            KIND_CLAIMED_PUBLICATION => {
                let admission =
                    AdmitPublication::decode_body(&mut reader, max_targets, max_topic_len)?;
                let target_index = reader.u16()?;
                let packet_id = OutboundPacketId::new(reader.u16()?)
                    .ok_or(RecipeError::InvalidField("outbound_packet_id"))?;
                Self::ClaimedPublication(ClaimedPublication {
                    admission,
                    claim: InlineClaim {
                        target_index,
                        packet_id,
                    },
                })
            }
            KIND_RELEASE_INBOUND => {
                let operation = reader.operation()?;
                let source_session = SessionId::new(reader.u64()?);
                let packet_id = reader.packet_id()?;
                let exchange = ExchangeId {
                    session: source_session,
                    generation: ExchangeGeneration::new(reader.u64()?),
                };
                Self::ReleaseInbound(ReleaseInbound {
                    operation,
                    source_session,
                    packet_id,
                    exchange,
                })
            }
            kind => return Err(RecipeError::UnknownKind(kind)),
        };
        if !reader.bytes.is_empty() {
            return Err(RecipeError::TrailingBytes);
        }
        recipe.check()?;
        Ok(recipe)
    }
}

pub(crate) struct Reader<'a> {
    pub(crate) bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn delivery(
        &mut self,
    ) -> Result<
        (
            OperationId,
            DeliveryId,
            SessionId,
            Revision,
            OutboundPacketId,
        ),
        RecipeError,
    > {
        let operation = self.operation()?;
        let publication =
            PublicationId::from_be_slice(self.take(32)?).map_err(|_| RecipeError::Truncated)?;
        let delivery = DeliveryId {
            publication,
            index: self.u32()?,
        };
        let target = SessionId::new(self.u64()?);
        let revision = Revision::new(self.u64()?);
        let packet = OutboundPacketId::new(self.u16()?)
            .ok_or(RecipeError::InvalidField("outbound_packet_id"))?;
        Ok((operation, delivery, target, revision, packet))
    }
    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], RecipeError> {
        if self.bytes.len() < len {
            return Err(RecipeError::Truncated);
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, RecipeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, RecipeError> {
        let mut array = [0u8; 2];
        array.copy_from_slice(self.take(2)?);
        Ok(u16::from_be_bytes(array))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, RecipeError> {
        let mut array = [0u8; 4];
        array.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(array))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, RecipeError> {
        let mut array = [0u8; 8];
        array.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(array))
    }

    pub(crate) fn operation(&mut self) -> Result<OperationId, RecipeError> {
        OperationId::from_be_slice(self.take(32)?).map_err(|_| RecipeError::Truncated)
    }

    pub(crate) fn packet_id(&mut self) -> Result<InboundPacketId, RecipeError> {
        InboundPacketId::new(self.u16()?).ok_or(RecipeError::InvalidField("packet_id"))
    }

    pub(crate) fn qos(&mut self) -> Result<Qos, RecipeError> {
        Qos::from_wire(self.u8()?).ok_or(RecipeError::InvalidField("qos"))
    }

    pub(crate) fn boolean(&mut self, field: &'static str) -> Result<bool, RecipeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(RecipeError::InvalidField(field)),
        }
    }
}

/// A recipe that can no longer change.
///
/// The `Arc` is private and no method hands out a mutable reference, so
/// neither interior mutation nor copy-on-write is reachable: immutability is
/// the contract of this wrapper (DEM-009). The owner keeps one clone until
/// the operation is resolved. Validation runs eagerly in [`Self::freeze`],
/// while canonical wire encoding is computed only when a durable medium or
/// snapshot actually needs the serialized bytes.
#[derive(Clone, Debug)]
pub struct FrozenRecipe(Arc<Recipe>);

impl PartialEq for FrozenRecipe {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl Eq for FrozenRecipe {}

struct StackBuf<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> StackBuf<N> {
    fn new() -> Self {
        Self {
            buf: [0u8; N],
            len: 0,
        }
    }

    fn push(&mut self, byte: u8) {
        self.buf[self.len] = byte;
        self.len += 1;
    }

    fn extend(&mut self, slice: &[u8]) {
        let end = self.len + slice.len();
        self.buf[self.len..end].copy_from_slice(slice);
        self.len = end;
    }

    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

fn encode_delivery_stack<const N: usize>(
    out: &mut StackBuf<N>,
    operation: OperationId,
    delivery: DeliveryId,
    target: SessionId,
    revision: Revision,
    packet_id: OutboundPacketId,
) {
    out.extend(&operation.to_be_bytes());
    out.extend(&delivery.publication.to_be_bytes());
    out.extend(&delivery.index.to_be_bytes());
    out.extend(&target.to_be_bytes());
    out.extend(&revision.to_be_bytes());
    out.extend(&packet_id.get().to_be_bytes());
}

fn encode_admit_body_stack<const N: usize>(out: &mut StackBuf<N>, admit: &AdmitPublication) {
    out.extend(&admit.operation.to_be_bytes());
    out.extend(&admit.request.to_be_bytes());
    out.extend(&admit.publication.to_be_bytes());
    out.extend(&admit.source.session.to_be_bytes());
    out.extend(&admit.source.session_revision.to_be_bytes());
    out.extend(&admit.source.packet_id.get().to_be_bytes());
    out.extend(&admit.source.exchange.generation.to_be_bytes());
    out.extend(&admit.route_revision.to_be_bytes());
    out.push(admit.qos.to_wire());
    out.push(u8::from(admit.retain));
    out.extend(&(admit.topic.len() as u16).to_be_bytes());
    out.extend(admit.topic.as_slice());
    out.extend(&admit.payload.id.to_be_bytes());
    out.extend(&admit.payload.length.get().to_be_bytes());
    out.extend(&admit.costs.payload_bytes.get().to_be_bytes());
    out.extend(&admit.costs.references.get().to_be_bytes());
    out.extend(&admit.costs.exchange_slots.get().to_be_bytes());
    out.extend(&admit.costs.receipt_slots.get().to_be_bytes());
    out.extend(&(admit.targets.len() as u16).to_be_bytes());
    for target in admit.targets.as_slice() {
        out.extend(&target.session.to_be_bytes());
        out.extend(&target.session_revision.to_be_bytes());
        out.extend(&target.delivery.index.to_be_bytes());
        out.push(target.effective_qos.to_wire());
        out.push(target.cause as u8);
    }
}

impl FrozenRecipe {
    /// Freezes a well-formed recipe after validating its canonical invariants.
    pub fn freeze(recipe: Recipe) -> Result<Self, RecipeError> {
        recipe.check()?;
        Ok(Self(Arc::new(recipe)))
    }

    /// The frozen recipe.
    #[must_use]
    pub fn get(&self) -> &Recipe {
        &self.0
    }

    /// Appends the exact canonical bytes of this already-checked recipe to `out`.
    pub fn encode_canonical_into(&self, out: &mut Vec<u8>) {
        encode_checked_into(&self.0, out);
    }

    /// Encodes the exact canonical bytes of this frozen recipe into a fresh `Vec<u8>`.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.canonical_len());
        encode_checked_into(&self.0, &mut out);
        out
    }

    /// Encodes the exact canonical bytes of this frozen recipe into an `Arc<[u8]>`.
    #[must_use]
    pub fn canonical_arc(&self) -> Arc<[u8]> {
        encode_checked_arc(&self.0)
    }

    /// Exact canonical byte length without allocating or serializing hot variants.
    #[must_use]
    pub fn canonical_len(&self) -> usize {
        const ADMIT_FIXED: usize = 215;
        const TARGET_BYTES: usize = 22;
        match self.0.as_ref() {
            Recipe::FinishDelivery(_) | Recipe::PreparePubrel(_) => 125,
            Recipe::ClaimDelivery(_) => 93,
            Recipe::ConsumeDelivery(_) => 91,
            Recipe::ReleaseInbound(_) => 57,
            Recipe::AdmitPublication(admit) => ADMIT_FIXED
                .saturating_add(admit.topic.len())
                .saturating_add(admit.targets.len().saturating_mul(TARGET_BYTES)),
            Recipe::ClaimedPublication(claimed) => (ADMIT_FIXED + 4)
                .saturating_add(claimed.admission.topic.len())
                .saturating_add(claimed.admission.targets.len().saturating_mul(TARGET_BYTES)),
            _ => {
                let mut out = Vec::with_capacity(128);
                encode_checked_into(&self.0, &mut out);
                out.len()
            }
        }
    }

    /// The operation this recipe belongs to.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        self.0.operation()
    }
}

fn encode_checked_into(recipe: &Recipe, out: &mut Vec<u8>) {
    const STACK_CAP: usize = 384;
    const ADMIT_FIXED: usize = 215;
    const TARGET_BYTES: usize = 22;

    match recipe {
        Recipe::FinishDelivery(finish) | Recipe::PreparePubrel(finish) => {
            let mut buf = StackBuf::<128>::new();
            buf.extend(MAGIC);
            buf.extend(&RECIPE_FORMAT.to_be_bytes());
            buf.push(if matches!(recipe, Recipe::PreparePubrel(_)) {
                19
            } else {
                KIND_FINISH_DELIVERY
            });
            encode_delivery_stack(
                &mut buf,
                finish.operation,
                finish.delivery,
                finish.target,
                finish.target_revision,
                finish.packet_id,
            );
            buf.extend(&finish.claim.to_be_bytes());
            out.extend_from_slice(buf.as_slice());
            return;
        }
        Recipe::ClaimDelivery(claim) => {
            let mut buf = StackBuf::<96>::new();
            buf.extend(MAGIC);
            buf.extend(&RECIPE_FORMAT.to_be_bytes());
            buf.push(KIND_CLAIM_DELIVERY);
            encode_delivery_stack(
                &mut buf,
                claim.operation,
                claim.delivery,
                claim.target,
                claim.target_revision,
                claim.packet_id,
            );
            out.extend_from_slice(buf.as_slice());
            return;
        }
        Recipe::ConsumeDelivery(consume) => {
            let mut buf = StackBuf::<96>::new();
            buf.extend(MAGIC);
            buf.extend(&RECIPE_FORMAT.to_be_bytes());
            buf.push(KIND_CONSUME_DELIVERY);
            buf.extend(&consume.operation.to_be_bytes());
            buf.extend(&consume.delivery.publication.to_be_bytes());
            buf.extend(&consume.delivery.index.to_be_bytes());
            buf.extend(&consume.target.to_be_bytes());
            buf.extend(&consume.target_revision.to_be_bytes());
            out.extend_from_slice(buf.as_slice());
            return;
        }
        Recipe::ReleaseInbound(release) => {
            let mut buf = StackBuf::<64>::new();
            buf.extend(MAGIC);
            buf.extend(&RECIPE_FORMAT.to_be_bytes());
            buf.push(KIND_RELEASE_INBOUND);
            buf.extend(&release.operation.to_be_bytes());
            buf.extend(&release.source_session.to_be_bytes());
            buf.extend(&release.packet_id.get().to_be_bytes());
            buf.extend(&release.exchange.generation.to_be_bytes());
            out.extend_from_slice(buf.as_slice());
            return;
        }
        Recipe::AdmitPublication(admit) => {
            let needed = ADMIT_FIXED
                .saturating_add(admit.topic.len())
                .saturating_add(admit.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                let mut buf = StackBuf::<STACK_CAP>::new();
                buf.extend(MAGIC);
                buf.extend(&RECIPE_FORMAT.to_be_bytes());
                buf.push(KIND_ADMIT_PUBLICATION);
                encode_admit_body_stack(&mut buf, admit);
                out.extend_from_slice(buf.as_slice());
                return;
            }
        }
        Recipe::ClaimedPublication(claimed) => {
            let needed = ADMIT_FIXED
                .saturating_add(4)
                .saturating_add(claimed.admission.topic.len())
                .saturating_add(claimed.admission.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                let mut buf = StackBuf::<STACK_CAP>::new();
                buf.extend(MAGIC);
                buf.extend(&RECIPE_FORMAT.to_be_bytes());
                buf.push(KIND_CLAIMED_PUBLICATION);
                encode_admit_body_stack(&mut buf, &claimed.admission);
                buf.extend(&claimed.claim.target_index.to_be_bytes());
                buf.extend(&claimed.claim.packet_id.get().to_be_bytes());
                out.extend_from_slice(buf.as_slice());
                return;
            }
        }
        _ => {}
    }
    let _ = recipe.encode_canonical(out);
}

fn encode_checked_arc(recipe: &Recipe) -> Arc<[u8]> {
    const STACK_CAP: usize = 384;
    const ADMIT_FIXED: usize = 215;
    const TARGET_BYTES: usize = 22;

    match recipe {
        Recipe::FinishDelivery(finish) | Recipe::PreparePubrel(finish) => {
            let mut out = StackBuf::<128>::new();
            out.extend(MAGIC);
            out.extend(&RECIPE_FORMAT.to_be_bytes());
            out.push(if matches!(recipe, Recipe::PreparePubrel(_)) {
                19
            } else {
                KIND_FINISH_DELIVERY
            });
            encode_delivery_stack(
                &mut out,
                finish.operation,
                finish.delivery,
                finish.target,
                finish.target_revision,
                finish.packet_id,
            );
            out.extend(&finish.claim.to_be_bytes());
            return Arc::from(out.as_slice());
        }
        Recipe::ClaimDelivery(claim) => {
            let mut out = StackBuf::<96>::new();
            out.extend(MAGIC);
            out.extend(&RECIPE_FORMAT.to_be_bytes());
            out.push(KIND_CLAIM_DELIVERY);
            encode_delivery_stack(
                &mut out,
                claim.operation,
                claim.delivery,
                claim.target,
                claim.target_revision,
                claim.packet_id,
            );
            return Arc::from(out.as_slice());
        }
        Recipe::ConsumeDelivery(consume) => {
            let mut out = StackBuf::<96>::new();
            out.extend(MAGIC);
            out.extend(&RECIPE_FORMAT.to_be_bytes());
            out.push(KIND_CONSUME_DELIVERY);
            out.extend(&consume.operation.to_be_bytes());
            out.extend(&consume.delivery.publication.to_be_bytes());
            out.extend(&consume.delivery.index.to_be_bytes());
            out.extend(&consume.target.to_be_bytes());
            out.extend(&consume.target_revision.to_be_bytes());
            return Arc::from(out.as_slice());
        }
        Recipe::ReleaseInbound(release) => {
            let mut out = StackBuf::<64>::new();
            out.extend(MAGIC);
            out.extend(&RECIPE_FORMAT.to_be_bytes());
            out.push(KIND_RELEASE_INBOUND);
            out.extend(&release.operation.to_be_bytes());
            out.extend(&release.source_session.to_be_bytes());
            out.extend(&release.packet_id.get().to_be_bytes());
            out.extend(&release.exchange.generation.to_be_bytes());
            return Arc::from(out.as_slice());
        }
        Recipe::AdmitPublication(admit) => {
            let needed = ADMIT_FIXED
                .saturating_add(admit.topic.len())
                .saturating_add(admit.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                let mut out = StackBuf::<STACK_CAP>::new();
                out.extend(MAGIC);
                out.extend(&RECIPE_FORMAT.to_be_bytes());
                out.push(KIND_ADMIT_PUBLICATION);
                encode_admit_body_stack(&mut out, admit);
                return Arc::from(out.as_slice());
            }
        }
        Recipe::ClaimedPublication(claimed) => {
            let needed = ADMIT_FIXED
                .saturating_add(4)
                .saturating_add(claimed.admission.topic.len())
                .saturating_add(claimed.admission.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                let mut out = StackBuf::<STACK_CAP>::new();
                out.extend(MAGIC);
                out.extend(&RECIPE_FORMAT.to_be_bytes());
                out.push(KIND_CLAIMED_PUBLICATION);
                encode_admit_body_stack(&mut out, &claimed.admission);
                out.extend(&claimed.claim.target_index.to_be_bytes());
                out.extend(&claimed.claim.packet_id.get().to_be_bytes());
                return Arc::from(out.as_slice());
            }
        }
        _ => {}
    }
    let mut canonical = Vec::with_capacity(256);
    let _ = recipe.encode_canonical(&mut canonical);
    Arc::from(canonical)
}

/// A frozen command proposed to the store. The payload bytes travel with an
/// admission; the recipe refers to them by durable identity and length.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Command {
    pub recipe: FrozenRecipe,
    pub payload: Option<SharedBytes>,
}

pub use crate::opening::{
    EpochLedger, EpochPhase, LedgerIncoherent, LedgerOpening, OpenEpoch, OpeningAttempt,
    OpeningRejection, OpeningTicket, StartupNonce,
};

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

    fn decode_body(
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
            TargetVec::with_capacity(count).map_err(|_| RecipeError::AllocationFailed)?;
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
impl UpdateSubscriptions {
    pub(crate) fn check_retained(&self) -> Result<(), RecipeError> {
        if self.changes.is_empty() || self.changes.len() > MAX_FILTERS {
            return Err(RecipeError::InvalidField("subscription_count"));
        }
        for entry in self.changes.as_slice() {
            if !crate::topic::valid_filter(entry.topic.as_slice()) {
                return Err(RecipeError::InvalidField("topic_filter"));
            }
            if entry.granted.is_none()
                && entry.options != crate::pubsub::SubscriptionOptions::DEFAULT
            {
                return Err(RecipeError::InvalidField("unsubscribe_options"));
            }
        }
        Ok(())
    }
}
impl UpdateSubscriptions {
    pub(crate) fn encode_retained(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&RECIPE_FORMAT.to_be_bytes());
        let with_options = self
            .changes
            .as_slice()
            .iter()
            .any(|c| c.options != crate::pubsub::SubscriptionOptions::DEFAULT);
        out.push(if with_options {
            KIND_SUBSCRIPTION_OPTIONS
        } else {
            KIND_UPDATE_SUBSCRIPTIONS
        });
        out.extend_from_slice(&self.operation.to_be_bytes());
        out.extend_from_slice(&self.session.to_be_bytes());
        out.extend_from_slice(&self.session_revision.to_be_bytes());
        out.extend_from_slice(&self.route_revision.to_be_bytes());
        out.push(self.changes.len() as u8);
        for entry in self.changes.as_slice() {
            out.push(match entry.granted {
                None => 0,
                Some(Qos::AtMostOnce) => 1,
                Some(Qos::AtLeastOnce) => 2,
                Some(Qos::ExactlyOnce) => 3,
            });
            if with_options {
                out.push(entry.options.bits());
            }
            out.extend_from_slice(&(entry.topic.len() as u16).to_be_bytes());
            out.extend_from_slice(entry.topic.as_slice());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{
        LogicalOwnerId, OperationSeq, PayloadSeq, PublicationSeq, RequestSeq, RunEpoch,
        StoreIncarnation,
    };

    fn scope() -> (StoreIncarnation, RunEpoch, LogicalOwnerId) {
        (
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(0),
        )
    }

    fn publication() -> PublicationId {
        let (incarnation, epoch, owner) = scope();
        PublicationId::new(incarnation, epoch, owner, PublicationSeq::new(5))
    }

    fn target(session: u64, index: u32) -> Target {
        Target {
            session: SessionId::new(session),
            session_revision: Revision::new(1),
            delivery: DeliveryId {
                publication: publication(),
                index,
            },
            effective_qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
        }
    }

    fn admission(targets: &[Target]) -> Recipe {
        let (incarnation, epoch, owner) = scope();
        let session = SessionId::new(10);
        Recipe::AdmitPublication(AdmitPublication {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(3)),
            request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(4)),
            publication: publication(),
            source: Source {
                session,
                session_revision: Revision::new(1),
                packet_id: InboundPacketId::new(7).expect("non-zero"),
                exchange: ExchangeId {
                    session,
                    generation: ExchangeGeneration::new(1),
                },
            },
            route_revision: Revision::new(1),
            qos: Qos::ExactlyOnce,
            retain: false,
            topic: SharedBytes::copy_from(b"p1/example", 64).expect("fits"),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(6)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(2),
                exchange_slots: ExchangeSlots::new(1),
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: TargetVec::from_slice(targets, 4).expect("fits"),
        })
    }

    fn encode(recipe: &Recipe) -> Vec<u8> {
        let mut bytes = Vec::new();
        recipe.encode_canonical(&mut bytes).expect("well formed");
        bytes
    }

    #[test]
    fn pubsub_recipes_round_trip_and_reject_every_incomplete_encoding() {
        let (incarnation, epoch, owner) = scope();
        let operation = OperationId::new(incarnation, epoch, owner, OperationSeq::new(7));
        let claim = OperationId::new(incarnation, epoch, owner, OperationSeq::new(8));
        let delivery = target(11, 0).delivery;
        let packet_id = OutboundPacketId::new(12).unwrap();
        let changes = BoundedVec::from_slice(
            &[SubscriptionChange {
                topic: SharedBytes::copy_from(b"exact/topic", 64).unwrap(),
                subscribe: true,
            }],
            MAX_FILTERS,
        )
        .unwrap();
        let recipes = [
            Recipe::ChangeSubscriptions(ChangeSubscriptions {
                operation,
                session: SessionId::new(11),
                session_revision: Revision::new(1),
                route_revision: Revision::new(2),
                changes,
            }),
            Recipe::ClaimDelivery(ClaimDelivery {
                operation,
                delivery,
                target: SessionId::new(11),
                target_revision: Revision::new(1),
                packet_id,
            }),
            Recipe::FinishDelivery(FinishDelivery {
                operation,
                delivery,
                target: SessionId::new(11),
                target_revision: Revision::new(1),
                packet_id,
                claim,
            }),
        ];
        for recipe in recipes {
            let bytes = encode(&recipe);
            assert_eq!(Recipe::decode_canonical(&bytes, 16, 64).unwrap(), recipe);
            for cut in 0..bytes.len() {
                assert!(
                    Recipe::decode_canonical(&bytes[..cut], 16, 64).is_err(),
                    "accepted cut {cut}"
                );
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert_eq!(
                Recipe::decode_canonical(&trailing, 16, 64),
                Err(RecipeError::TrailingBytes)
            );
        }
    }

    #[test]
    fn replay_cause_cannot_change_legacy_admission_encoding() {
        let mut destination = target(2, 0);
        destination.cause = DeliveryCause::RetainedReplay;
        assert_eq!(
            admission(&[destination]).check(),
            Err(RecipeError::InvalidField("admission_cause"))
        );
    }

    #[test]
    fn retained_qos0_accepts_literal_dollar_share_topic_names() {
        let Recipe::AdmitPublication(a) = admission(&[]) else {
            panic!()
        };
        let recipe = Recipe::RetainedQos0(crate::retained::RetainedQos0 {
            operation: a.operation,
            request: a.request,
            publication: a.publication,
            session: a.source.session,
            session_revision: a.source.session_revision,
            connection: crate::ids::ConnectionGeneration::new(1),
            ingress_order: 1,
            route_revision: a.route_revision,
            topic: SharedBytes::copy_from(b"$share/literal", 1024).unwrap(),
            payload: a.payload,
            costs: a.costs,
            targets: a.targets.into_bounded(),
            retain_targets: crate::retained::RetainMask::EMPTY,
        });
        assert_eq!(
            recipe.check(),
            Ok(()),
            "topic_name_misclassified_as_subscription"
        );
    }

    #[test]
    fn retained_acknowledged_recipe_rejects_qos0() {
        let Recipe::AdmitPublication(mut admit) = admission(&[]) else {
            panic!()
        };
        admit.retain = true;
        admit.qos = Qos::AtMostOnce;
        let recipe = Recipe::RetainedPublication(crate::retained::RetainedPublication {
            admission: admit,
            retain_targets: crate::retained::RetainMask::EMPTY,
        });
        assert_eq!(
            recipe.check(),
            Err(RecipeError::InvalidField("retained_qos"))
        );
    }

    #[test]
    fn retained_extension_preserves_legacy_bytes_and_bounds_nesting() {
        let Recipe::AdmitPublication(mut admit) = admission(&[target(2, 0)]) else {
            panic!()
        };
        admit.retain = true;
        let recipe = Recipe::RetainedPublication(crate::retained::RetainedPublication {
            admission: admit,
            retain_targets: crate::retained::RetainMask::from_u16(1),
        });
        let bytes = encode(&recipe);
        assert_eq!(bytes[6], 10);
        assert_eq!(Recipe::decode_canonical(&bytes, 16, 1024).unwrap(), recipe);
        for length in 0..bytes.len() {
            assert!(Recipe::decode_canonical(&bytes[..length], 16, 1024).is_err());
        }
        let mut nested = bytes.clone();
        nested[17] = 10;
        assert!(Recipe::decode_canonical(&nested, 16, 1024).is_err());
        let Recipe::RetainedPublication(mut invalid) = recipe else {
            panic!()
        };
        invalid.retain_targets = crate::retained::RetainMask::from_u16(2);
        assert!(Recipe::RetainedPublication(invalid).check().is_err());
    }

    #[test]
    fn wide_fanout_retained_and_will_recipes_round_trip_canonically() {
        let count = 200usize;
        let list: Vec<Target> = (0..count)
            .map(|i| target(100 + i as u64, i as u32))
            .collect();
        let mask = crate::retained::RetainMask::from_predicate(count, |i| i % 3 == 0).unwrap();
        assert!(mask.is_set(0));
        assert!(!mask.is_set(1));
        assert!(mask.is_set(198));
        let (incarnation, epoch, owner) = scope();
        let session = SessionId::new(10);
        let mut targets = TargetVec::with_capacity(count).unwrap();
        for t in &list {
            targets.try_push(*t).unwrap();
        }
        let admit = AdmitPublication {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(3)),
            request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(4)),
            publication: publication(),
            source: Source {
                session,
                session_revision: Revision::new(1),
                packet_id: InboundPacketId::new(7).unwrap(),
                exchange: ExchangeId {
                    session,
                    generation: ExchangeGeneration::new(1),
                },
            },
            route_revision: Revision::new(1),
            qos: Qos::AtLeastOnce,
            retain: true,
            topic: SharedBytes::copy_from(b"p1/wide", 64).unwrap(),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(6)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(count as u64 + 1),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets,
        };
        let retained = Recipe::RetainedPublication(crate::retained::RetainedPublication {
            admission: admit,
            retain_targets: mask.clone(),
        });
        let bytes = encode(&retained);
        assert_eq!(bytes[6], 20);
        assert_eq!(
            Recipe::decode_canonical(&bytes, 1024, 64).unwrap(),
            retained
        );
        // Wide tag with <= 16 targets is noncanonical and must be rejected.
        let mut small_targets = BoundedVec::with_capacity(1).unwrap();
        small_targets.try_push(target(2, 0)).unwrap();
        let small = Recipe::RetainedQos0(crate::retained::RetainedQos0 {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(3)),
            request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(4)),
            publication: publication(),
            session,
            session_revision: Revision::new(1),
            connection: crate::ids::ConnectionGeneration::new(1),
            ingress_order: 1,
            route_revision: Revision::new(1),
            topic: SharedBytes::copy_from(b"p1/small", 64).unwrap(),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(6)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(2),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: small_targets,
            retain_targets: crate::retained::RetainMask::from_u16(1),
        });
        assert_eq!(encode(&small)[6], 11);
        let mut wide_q0_targets = BoundedVec::with_capacity(count).unwrap();
        for t in &list {
            wide_q0_targets.try_push(*t).unwrap();
        }
        let wide_q0 = Recipe::RetainedQos0(crate::retained::RetainedQos0 {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(3)),
            request: RequestId::new(incarnation, epoch, owner, RequestSeq::new(4)),
            publication: publication(),
            session,
            session_revision: Revision::new(1),
            connection: crate::ids::ConnectionGeneration::new(1),
            ingress_order: 1,
            route_revision: Revision::new(1),
            topic: SharedBytes::copy_from(b"p1/wideq0", 64).unwrap(),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(6)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(count as u64 + 1),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: wide_q0_targets,
            retain_targets: mask.clone(),
        });
        let q0_bytes = encode(&wide_q0);
        assert_eq!(q0_bytes[6], 21);
        assert_eq!(
            Recipe::decode_canonical(&q0_bytes, 1024, 64).unwrap(),
            wide_q0
        );
        let mut will_targets = BoundedVec::with_capacity(count).unwrap();
        for t in &list {
            will_targets.try_push(*t).unwrap();
        }
        let wide_will = Recipe::WillPublication(crate::will::WillPublication {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(3)),
            will: OperationId::new(incarnation, epoch, owner, OperationSeq::new(1)),
            publication: publication(),
            route_revision: Revision::new(1),
            topic: SharedBytes::copy_from(b"p1/will", 64).unwrap(),
            payload: PayloadRef {
                id: PayloadId::new(incarnation, epoch, owner, PayloadSeq::new(6)),
                length: PayloadBytes::new(7),
            },
            costs: Costs {
                payload_bytes: PayloadBytes::new(7),
                references: References::new(count as u64 + 1),
                exchange_slots: ExchangeSlots::ZERO,
                receipt_slots: ReceiptSlots::new(1),
            },
            targets: will_targets,
            live_retain: Some(mask),
        });
        let will_bytes = encode(&wide_will);
        assert_eq!(will_bytes[6], 22);
        assert_eq!(
            Recipe::decode_canonical(&will_bytes, 1024, 64).unwrap(),
            wide_will
        );
    }

    #[test]
    fn admission_round_trips_exactly() {
        let recipe = admission(&[target(11, 0), target(12, 1)]);
        let bytes = encode(&recipe);
        let decoded = Recipe::decode_canonical(&bytes, 4, 64).expect("canonical");
        assert_eq!(encode(&decoded), bytes);
        assert_eq!(decoded.operation(), recipe.operation());
        let Recipe::AdmitPublication(admit) = decoded else {
            panic!("kind changed");
        };
        assert_eq!(admit.targets.as_slice(), &[target(11, 0), target(12, 1)]);
        assert_eq!(admit.topic.as_slice(), b"p1/example");
    }
    #[test]
    fn qos0_canonical_consumption_has_no_identifier_and_grants_are_distinct() {
        let (incarnation, epoch, owner) = scope();
        let operation = OperationId::new(incarnation, epoch, owner, OperationSeq::new(3));
        let consume = Recipe::ConsumeDelivery(ConsumeDelivery {
            operation,
            delivery: target(11, 0).delivery,
            target: SessionId::new(11),
            target_revision: Revision::new(1),
        });
        let bytes = encode(&consume);
        assert_eq!(bytes.len(), 91);
        assert_eq!(bytes[6], 6);
        assert_eq!(Recipe::decode_canonical(&bytes, 16, 64).unwrap(), consume);
        for len in 0..bytes.len() {
            assert!(Recipe::decode_canonical(&bytes[..len], 16, 64).is_err());
        }
        let mut encodings = Vec::new();
        for granted in [None, Some(Qos::AtMostOnce), Some(Qos::AtLeastOnce)] {
            let recipe = Recipe::UpdateSubscriptions(UpdateSubscriptions {
                operation,
                session: SessionId::new(11),
                session_revision: Revision::new(1),
                route_revision: Revision::new(1),
                changes: BoundedVec::from_slice(
                    &[GrantedSubscriptionChange {
                        options: crate::pubsub::SubscriptionOptions::DEFAULT,
                        topic: SharedBytes::copy_from(b"t", 64).unwrap(),
                        granted,
                    }],
                    8,
                )
                .unwrap(),
            });
            let bytes = encode(&recipe);
            assert_eq!(bytes[6], 7);
            assert_eq!(Recipe::decode_canonical(&bytes, 16, 64).unwrap(), recipe);
            encodings.push(bytes);
        }
        assert_ne!(encodings[0], encodings[1]);
        assert_ne!(encodings[1], encodings[2]);
    }

    #[test]
    fn a_decoded_recipe_equals_the_recipe_that_was_encoded() {
        // Two targets held in a container bounded at four.
        let recipe = admission(&[target(11, 0), target(12, 1)]);
        let decoded = Recipe::decode_canonical(&encode(&recipe), 4, 64).expect("canonical");
        assert_eq!(encode(&decoded), encode(&recipe));
        assert_eq!(decoded, recipe, "equal canonical bytes, unequal recipes");
    }

    fn admission_bounded_at(targets: &[Target], bound: usize) -> Recipe {
        let Recipe::AdmitPublication(mut admit) = admission(targets) else {
            panic!("kind changed");
        };
        admit.targets = TargetVec::from_slice(targets, bound).expect("fits");
        Recipe::AdmitPublication(admit)
    }

    #[test]
    fn the_bound_of_the_target_container_is_not_part_of_the_recipe() {
        let targets = [target(11, 0), target(12, 1)];
        let tight = admission_bounded_at(&targets, 2);
        let loose = admission_bounded_at(&targets, 64);
        assert_eq!(tight, loose);
        assert_eq!(encode(&tight), encode(&loose));
        let frozen = FrozenRecipe::freeze(tight).expect("well formed");
        assert_eq!(frozen, FrozenRecipe::freeze(loose).expect("well formed"));
    }

    #[test]
    fn recipes_are_equal_exactly_when_their_canonical_bytes_are() {
        let mut other_cause = target(12, 1);
        other_cause.effective_qos = Qos::AtMostOnce;
        let mut variants = vec![
            admission(&[target(11, 0), target(12, 1)]),
            admission_bounded_at(&[target(11, 0), target(12, 1)], 9),
            admission(&[target(12, 1), target(11, 0)]),
            admission(&[target(11, 0), target(13, 1)]),
            admission(&[target(11, 0), other_cause]),
            admission(&[target(11, 0)]),
            admission(&[]),
        ];
        for edit in 0..5 {
            let Recipe::AdmitPublication(mut admit) = admission(&[target(11, 0), target(12, 1)])
            else {
                panic!("kind changed");
            };
            match edit {
                0 => admit.costs.references = References::new(3),
                1 => admit.retain = true,
                2 => admit.topic = SharedBytes::copy_from(b"p1/other", 64).expect("fits"),
                3 => admit.payload.length = PayloadBytes::new(8),
                _ => admit.route_revision = Revision::new(2),
            }
            variants.push(Recipe::AdmitPublication(admit));
        }
        let mut equal_pairs = 0;
        for left in &variants {
            for right in &variants {
                let same_bytes = encode(left) == encode(right);
                assert_eq!(left == right, same_bytes, "{left:?}\n{right:?}");
                equal_pairs += usize::from(same_bytes);
            }
        }
        // Every recipe equals itself, and the two bounds of the first one.
        assert_eq!(equal_pairs, variants.len() + 2);
    }

    #[test]
    fn layout_is_pinned() {
        let bytes = encode(&admission(&[target(11, 0)]));
        assert_eq!(&bytes[0..4], b"MQDR");
        assert_eq!(&bytes[4..6], &[0, 1]);
        assert_eq!(bytes[6], 1);
        // header 7, three scoped ids 96, source 26, route 8, qos and retain 2,
        // topic 2 + 10, payload 40, costs 32, count 2, one target 22.
        assert_eq!(bytes.len(), 7 + 96 + 26 + 8 + 2 + 12 + 40 + 32 + 2 + 22);
        // operation sequence is the last component of the first identity.
        assert_eq!(&bytes[31..39], &3u64.to_be_bytes());
    }

    #[test]
    fn any_change_of_targets_cost_or_payload_changes_the_bytes() {
        let reference = encode(&admission(&[target(11, 0), target(12, 1)]));
        assert_ne!(
            encode(&admission(&[target(11, 0), target(13, 1)])),
            reference
        );
        assert_ne!(
            encode(&admission(&[target(12, 1), target(11, 0)])),
            reference
        );
        assert_ne!(encode(&admission(&[target(11, 0)])), reference);
        let mut other_cause = target(12, 1);
        other_cause.effective_qos = Qos::AtMostOnce;
        assert_ne!(encode(&admission(&[target(11, 0), other_cause])), reference);
        let Recipe::AdmitPublication(mut costlier) = admission(&[target(11, 0), target(12, 1)])
        else {
            panic!("kind changed");
        };
        costlier.costs.references = References::new(3);
        assert_ne!(encode(&Recipe::AdmitPublication(costlier)), reference);
    }

    #[test]
    fn release_round_trips_exactly() {
        let (incarnation, epoch, owner) = scope();
        let session = SessionId::new(10);
        let recipe = Recipe::ReleaseInbound(ReleaseInbound {
            operation: OperationId::new(incarnation, epoch, owner, OperationSeq::new(9)),
            source_session: session,
            packet_id: InboundPacketId::new(65_535).expect("non-zero"),
            exchange: ExchangeId {
                session,
                generation: ExchangeGeneration::new(u64::MAX),
            },
        });
        let bytes = encode(&recipe);
        assert_eq!(bytes.len(), 7 + 32 + 8 + 2 + 8);
        assert_eq!(Recipe::decode_canonical(&bytes, 0, 0), Ok(recipe));
    }

    #[test]
    fn decoding_refuses_everything_but_the_unique_encoding() {
        let bytes = encode(&admission(&[target(11, 0), target(12, 1)]));
        for cut in 0..bytes.len() {
            assert_eq!(
                Recipe::decode_canonical(&bytes[..cut], 4, 64),
                Err(RecipeError::Truncated),
                "prefix of {cut} bytes"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(
            Recipe::decode_canonical(&longer, 4, 64),
            Err(RecipeError::TrailingBytes)
        );
        let mut magic = bytes.clone();
        magic[0] = b'X';
        assert_eq!(
            Recipe::decode_canonical(&magic, 4, 64),
            Err(RecipeError::BadMagic)
        );
        let mut format = bytes.clone();
        format[5] = 2;
        assert_eq!(
            Recipe::decode_canonical(&format, 4, 64),
            Err(RecipeError::UnknownFormat(2))
        );
        let mut kind = bytes.clone();
        kind[6] = 255;
        assert_eq!(
            Recipe::decode_canonical(&kind, 4, 64),
            Err(RecipeError::UnknownKind(255))
        );
    }

    #[test]
    fn decoding_checks_closed_domains_and_bounds() {
        let bytes = encode(&admission(&[target(11, 0), target(12, 1)]));
        // source.packet_id sits after the header, three ids and two u64.
        let packet_id = 7 + 96 + 16;
        let mut zero_id = bytes.clone();
        zero_id[packet_id] = 0;
        zero_id[packet_id + 1] = 0;
        assert_eq!(
            Recipe::decode_canonical(&zero_id, 4, 64),
            Err(RecipeError::InvalidField("packet_id"))
        );
        let qos = 7 + 96 + 26 + 8;
        let mut bad_qos = bytes.clone();
        bad_qos[qos] = 3;
        assert_eq!(
            Recipe::decode_canonical(&bad_qos, 4, 64),
            Err(RecipeError::InvalidField("qos"))
        );
        let mut bad_retain = bytes.clone();
        bad_retain[qos + 1] = 2;
        assert_eq!(
            Recipe::decode_canonical(&bad_retain, 4, 64),
            Err(RecipeError::InvalidField("retain"))
        );
        assert_eq!(
            Recipe::decode_canonical(&bytes, 1, 64),
            Err(RecipeError::TooManyTargets { count: 2, max: 1 })
        );
        assert_eq!(
            Recipe::decode_canonical(&bytes, 4, 9),
            Err(RecipeError::TopicTooLong(10))
        );
        let last = bytes.len() - 1;
        let mut bad_cause = bytes.clone();
        bad_cause[last] = 9;
        assert_eq!(
            Recipe::decode_canonical(&bad_cause, 4, 64),
            Err(RecipeError::InvalidField("cause"))
        );
    }

    #[test]
    fn ill_formed_recipes_cannot_be_frozen() {
        let (incarnation, epoch, owner) = scope();
        let foreign = Target {
            delivery: DeliveryId {
                publication: PublicationId::new(incarnation, epoch, owner, PublicationSeq::new(99)),
                index: 0,
            },
            ..target(11, 0)
        };
        assert_eq!(
            FrozenRecipe::freeze(admission(&[foreign])),
            Err(RecipeError::ForeignDelivery)
        );
        let Recipe::AdmitPublication(mut admit) = admission(&[target(11, 0)]) else {
            panic!("kind changed");
        };
        admit.source.exchange.session = SessionId::new(999);
        assert_eq!(
            FrozenRecipe::freeze(Recipe::AdmitPublication(admit)),
            Err(RecipeError::ForeignExchange)
        );
    }

    #[test]
    fn frozen_recipes_share_one_immutable_value() {
        let frozen = FrozenRecipe::freeze(admission(&[target(11, 0)])).expect("well formed");
        let kept = frozen.clone();
        assert_eq!(kept, frozen);
        assert_eq!(kept.operation(), frozen.get().operation());
        assert_eq!(encode(kept.get()), encode(frozen.get()));
    }

    #[test]
    fn claimed_publication_canonical_roundtrip_and_guards() {
        let Recipe::AdmitPublication(admit) = admission(&[target(11, 0)]) else {
            panic!("expected AdmitPublication");
        };
        let claimed = Recipe::ClaimedPublication(ClaimedPublication {
            admission: admit,
            claim: InlineClaim {
                target_index: 0,
                packet_id: OutboundPacketId::new(17).expect("non-zero"),
            },
        });
        assert_eq!(claimed.operation(), claimed.admission().unwrap().operation);
        let bytes = encode(&claimed);
        assert_eq!(bytes[6], KIND_CLAIMED_PUBLICATION);
        let decoded = Recipe::decode_canonical(&bytes, 4, 64).expect("decode claimed");
        assert_eq!(decoded, claimed);

        // AnnotatedPublication wrapping ClaimedPublication also round-trips cleanly.
        let props = crate::properties::PublishProperties {
            content_type: Some(SharedBytes::copy_from(b"text/plain", 64).unwrap()),
            ..Default::default()
        };
        let annotated = Recipe::AnnotatedPublication(AnnotatedPublication {
            inner: Box::new(decoded),
            properties: Arc::new(props),
        });
        let ann_bytes = encode(&annotated);
        assert_eq!(
            Recipe::decode_canonical(&ann_bytes, 4, 64).expect("decode annotated claimed"),
            annotated
        );

        // Out-of-range target index is refused.
        let Recipe::AdmitPublication(admit) = admission(&[target(11, 0)]) else {
            panic!("expected AdmitPublication");
        };
        assert_eq!(
            FrozenRecipe::freeze(Recipe::ClaimedPublication(ClaimedPublication {
                admission: admit,
                claim: InlineClaim {
                    target_index: 1,
                    packet_id: OutboundPacketId::new(1).unwrap(),
                },
            })),
            Err(RecipeError::InvalidField("inline_claim_target"))
        );

        // QoS 0 target is refused.
        let mut qos0_target = target(11, 0);
        qos0_target.effective_qos = Qos::AtMostOnce;
        let Recipe::AdmitPublication(admit) = admission(&[qos0_target]) else {
            panic!("expected AdmitPublication");
        };
        assert_eq!(
            FrozenRecipe::freeze(Recipe::ClaimedPublication(ClaimedPublication {
                admission: admit,
                claim: InlineClaim {
                    target_index: 0,
                    packet_id: OutboundPacketId::new(1).unwrap(),
                },
            })),
            Err(RecipeError::InvalidField("inline_claim_qos"))
        );

        // Retain=true is refused for ClaimedPublication.
        let Recipe::AdmitPublication(mut admit) = admission(&[target(11, 0)]) else {
            panic!("expected AdmitPublication");
        };
        admit.retain = true;
        assert_eq!(
            FrozenRecipe::freeze(Recipe::ClaimedPublication(ClaimedPublication {
                admission: admit,
                claim: InlineClaim {
                    target_index: 0,
                    packet_id: OutboundPacketId::new(1).unwrap(),
                },
            })),
            Err(RecipeError::InvalidField("inline_claim_retain"))
        );
    }

    #[test]
    fn stack_fast_path_matches_encode_canonical_byte_for_byte() {
        let (incarnation, epoch, owner) = scope();
        let op = |seq: u64| OperationId::new(incarnation, epoch, owner, OperationSeq::new(seq));
        let assert_parity = |recipe: Recipe| {
            let mut expected = Vec::new();
            recipe.encode_canonical(&mut expected).unwrap();
            let frozen = FrozenRecipe::freeze(recipe).unwrap();
            assert_eq!(frozen.to_canonical_bytes(), expected);
            assert_eq!(frozen.canonical_len(), expected.len());
        };

        let claim = ClaimDelivery {
            operation: op(10),
            delivery: DeliveryId {
                publication: publication(),
                index: 2,
            },
            target: SessionId::new(9),
            target_revision: Revision::new(3),
            packet_id: OutboundPacketId::new(42).unwrap(),
        };
        assert_parity(Recipe::ClaimDelivery(claim));

        let finish = FinishDelivery {
            operation: op(11),
            delivery: DeliveryId {
                publication: publication(),
                index: 2,
            },
            target: SessionId::new(9),
            target_revision: Revision::new(3),
            packet_id: OutboundPacketId::new(42).unwrap(),
            claim: op(10),
        };
        assert_parity(Recipe::FinishDelivery(finish));
        assert_parity(Recipe::PreparePubrel(finish));

        assert_parity(Recipe::ConsumeDelivery(ConsumeDelivery {
            operation: op(12),
            delivery: DeliveryId {
                publication: publication(),
                index: 0,
            },
            target: SessionId::new(9),
            target_revision: Revision::new(3),
        }));

        assert_parity(Recipe::ReleaseInbound(ReleaseInbound {
            operation: op(13),
            source_session: SessionId::new(7),
            packet_id: InboundPacketId::new(17).unwrap(),
            exchange: ExchangeId {
                session: SessionId::new(7),
                generation: ExchangeGeneration::new(99),
            },
        }));

        assert_parity(admission(&[target(11, 0)]));
        let Recipe::AdmitPublication(admit) = admission(&[target(11, 0), target(12, 1)]) else {
            panic!("expected AdmitPublication");
        };
        assert_parity(Recipe::ClaimedPublication(ClaimedPublication {
            admission: admit,
            claim: InlineClaim {
                target_index: 1,
                packet_id: OutboundPacketId::new(77).unwrap(),
            },
        }));
    }
}
