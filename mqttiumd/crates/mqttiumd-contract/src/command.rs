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

use crate::canonical::{
    KIND_ADMIT_PUBLICATION, KIND_ANNOTATED_PUBLICATION, KIND_CHANGE_SUBSCRIPTIONS,
    KIND_CLAIM_DELIVERY, KIND_CLAIMED_PUBLICATION, KIND_CONSUME_DELIVERY, KIND_FINISH_DELIVERY,
    KIND_RELEASE_INBOUND, KIND_SUBSCRIPTION_OPTIONS, KIND_UPDATE_SUBSCRIPTIONS, MAGIC, Reader,
    put_exchange_generation,
};
pub use crate::canonical::{RECIPE_FORMAT, RecipeError};
use crate::capacity::BoundedVec;
use crate::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId, OperationId, OutboundPacketId,
    PublicationId, Revision, SessionId,
};
use crate::message::{Qos, SharedBytes};
pub use crate::message_expiry::{
    ExpireDelivery, ExpiringAdmission, ExpiringPublication, MarkDeliveryStarted,
    TimedReplaySubscriptions,
};
pub use crate::publication::{AdmitPublication, Costs, PayloadRef, Source, Target};
pub use crate::pubsub::{
    ChangeSubscriptions, ClaimDelivery, ConsumeDelivery, DeliveryCause, FinishDelivery,
    UpdateSubscriptions,
};
use crate::pubsub::{GrantedSubscriptionChange, MAX_FILTERS, SubscriptionChange};

/// Durably terminates an inbound QoS 2 exchange after PUBREL, before PUBCOMP
/// becomes eligible. It never creates a distribution.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReleaseInbound {
    pub operation: OperationId,
    pub source_session: SessionId,
    pub packet_id: InboundPacketId,
    pub exchange: ExchangeId,
}

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
            Self::RetainedPublication(value) => value.check_retained(),
            Self::RetainedQos0(value) => value.check_retained(),
            Self::ReplaySubscriptions(value) => value.check_retained(),
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
            Self::RetainedPublication(value) => value.encode_retained(out)?,
            Self::ReplaySubscriptions(value) => value.encode_retained(out)?,
            Self::RetainedQos0(value) => value.encode_retained(out)?,
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

    /// Kind 13: a publication (kind 1, 10, 11, 20 or 21, nested) with its
    /// message expiry.
    fn decode_expiring(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic: usize,
    ) -> Result<ExpiringPublication, RecipeError> {
        let nested = crate::message_expiry::read_nested(reader, &[1, 10, 11, 20, 21])?;
        // No temporal extension can appear within a temporal wrapper. Existing
        // tag 10 retains its single legacy tag-1 envelope.
        let admission = match Recipe::decode_canonical(nested, max_targets, max_topic)? {
            Recipe::AdmitPublication(admission) if !admission.retain => {
                ExpiringAdmission::Qos12(crate::retained::RetainedPublication {
                    admission,
                    retain_targets: crate::retained::RetainMask::EMPTY,
                })
            }
            Recipe::RetainedPublication(value) => ExpiringAdmission::Qos12(value),
            Recipe::RetainedQos0(value) => ExpiringAdmission::Qos0(value),
            _ => return Err(RecipeError::InvalidField("message_expiry_nested_kind")),
        };
        let value = ExpiringPublication {
            admission,
            expiry: crate::message_time::MessageExpiry::decode(reader)?,
        };
        value.check()?;
        Ok(value)
    }

    /// Kinds 10, 12 and 20: a legacy recipe (kind 1, 7 or 9), nested once,
    /// with what makes it retained.
    fn decode_retained_nested(
        kind: u8,
        bytes: &mut &[u8],
        max_targets: usize,
        max_topic: usize,
    ) -> Result<Self, RecipeError> {
        use crate::retained::{
            LEGACY_MAX_TARGETS, ReplaySubscriptions, RetainMask, RetainedPublication,
        };
        let mut r = Reader { bytes };
        let recipe = {
            let len = r.u32()? as usize;
            let nested = r.take(len)?;
            // Reject nested extensions before decoding: depth is exactly one.
            if nested.len() < 7
                || (matches!(kind, 10 | 20) && nested[6] != 1)
                || (kind == 12 && !matches!(nested[6], 7 | 9))
            {
                return Err(RecipeError::InvalidField("retained_nested_kind"));
            }
            match Recipe::decode_canonical(nested, max_targets, max_topic)? {
                Recipe::AdmitPublication(admission) if kind == 10 => {
                    if admission.targets.len() > LEGACY_MAX_TARGETS {
                        return Err(RecipeError::InvalidField("retain_targets"));
                    }
                    Recipe::RetainedPublication(RetainedPublication {
                        admission,
                        retain_targets: RetainMask::from_u16(r.u16()?),
                    })
                }
                Recipe::AdmitPublication(admission) if kind == 20 => {
                    if admission.targets.len() <= LEGACY_MAX_TARGETS {
                        return Err(RecipeError::InvalidField("noncanonical_wide_targets"));
                    }
                    let retain_targets =
                        RetainMask::decode_wide(&mut r, admission.targets.len(), "retain_targets")?;
                    Recipe::RetainedPublication(RetainedPublication {
                        admission,
                        retain_targets,
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
        };
        *bytes = r.bytes;
        Ok(recipe)
    }

    /// Kind 14: a replay of retained messages (kind 12, nested) with the
    /// time it was observed.
    fn decode_timed_replay(
        reader: &mut Reader<'_>,
        max_targets: usize,
        max_topic: usize,
    ) -> Result<TimedReplaySubscriptions, RecipeError> {
        let nested = crate::message_expiry::read_nested(reader, &[12])?;
        let Recipe::ReplaySubscriptions(replay) =
            Recipe::decode_canonical(nested, max_targets, max_topic)?
        else {
            return Err(RecipeError::InvalidField("message_expiry_nested_kind"));
        };
        Ok(TimedReplaySubscriptions {
            replay,
            observed: crate::message_time::MessageTime::decode(reader)?,
        })
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
            13 => Self::ExpiringPublication(Self::decode_expiring(
                &mut reader,
                max_targets,
                max_topic_len,
            )?),
            14 => Self::TimedReplaySubscriptions(Self::decode_timed_replay(
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
                    expiry: crate::message_time::MessageExpiry::decode(&mut reader)?,
                    observed: crate::message_time::MessageTime::decode(&mut reader)?,
                })
            }
            kind @ (10 | 12 | 20) => {
                Self::decode_retained_nested(kind, &mut reader.bytes, max_targets, max_topic_len)?
            }
            kind @ (11 | 21) => Self::RetainedQos0(crate::retained::RetainedQos0::decode(
                kind,
                &mut reader.bytes,
                max_targets,
                max_topic_len,
            )?),
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

#[derive(Debug)]
struct FrozenInner {
    recipe: Recipe,
    canonical: Arc<[u8]>,
}

/// A recipe that can no longer change.
///
/// The `Arc` is private and no method hands out a mutable reference, so
/// neither interior mutation nor copy-on-write is reachable: immutability is
/// the contract of this wrapper (DEM-009). The owner keeps one clone until
/// the operation is resolved. Its exact canonical bytes are frozen once at
/// construction so stores never re-encode the same command.
#[derive(Clone, Debug)]
pub struct FrozenRecipe(Arc<FrozenInner>);

impl PartialEq for FrozenRecipe {
    fn eq(&self, other: &Self) -> bool {
        self.0.canonical == other.0.canonical
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
    /// Freezes a well-formed recipe together with its canonical encoding.
    pub fn freeze(recipe: Recipe) -> Result<Self, RecipeError> {
        let canonical = encode_canonical_arc(&recipe)?;
        Ok(Self(Arc::new(FrozenInner { recipe, canonical })))
    }

    /// The frozen recipe.
    #[must_use]
    pub fn get(&self) -> &Recipe {
        &self.0.recipe
    }

    /// The exact canonical bytes of this frozen recipe (DEM-009).
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.0.canonical
    }

    /// Zero-copy shared handle to the canonical bytes of this frozen recipe.
    #[must_use]
    pub fn canonical_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.0.canonical)
    }

    /// The operation this recipe belongs to.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        self.0.recipe.operation()
    }
}

fn encode_canonical_arc(recipe: &Recipe) -> Result<Arc<[u8]>, RecipeError> {
    const STACK_CAP: usize = 384;
    // Fixed bytes of AdmitPublication header + body excluding topic and targets.
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
            return Ok(Arc::from(out.as_slice()));
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
            return Ok(Arc::from(out.as_slice()));
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
            return Ok(Arc::from(out.as_slice()));
        }
        Recipe::ReleaseInbound(release) => {
            recipe.check()?;
            let mut out = StackBuf::<64>::new();
            out.extend(MAGIC);
            out.extend(&RECIPE_FORMAT.to_be_bytes());
            out.push(KIND_RELEASE_INBOUND);
            out.extend(&release.operation.to_be_bytes());
            out.extend(&release.source_session.to_be_bytes());
            out.extend(&release.packet_id.get().to_be_bytes());
            out.extend(&release.exchange.generation.to_be_bytes());
            return Ok(Arc::from(out.as_slice()));
        }
        Recipe::AdmitPublication(admit) => {
            let needed = ADMIT_FIXED
                .saturating_add(admit.topic.len())
                .saturating_add(admit.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                admit.check_retained()?;
                let mut out = StackBuf::<STACK_CAP>::new();
                out.extend(MAGIC);
                out.extend(&RECIPE_FORMAT.to_be_bytes());
                out.push(KIND_ADMIT_PUBLICATION);
                encode_admit_body_stack(&mut out, admit);
                return Ok(Arc::from(out.as_slice()));
            }
        }
        Recipe::ClaimedPublication(claimed) => {
            let needed = ADMIT_FIXED
                .saturating_add(4)
                .saturating_add(claimed.admission.topic.len())
                .saturating_add(claimed.admission.targets.len().saturating_mul(TARGET_BYTES));
            if needed <= STACK_CAP {
                claimed.check()?;
                let mut out = StackBuf::<STACK_CAP>::new();
                out.extend(MAGIC);
                out.extend(&RECIPE_FORMAT.to_be_bytes());
                out.push(KIND_CLAIMED_PUBLICATION);
                encode_admit_body_stack(&mut out, &claimed.admission);
                out.extend(&claimed.claim.target_index.to_be_bytes());
                out.extend(&claimed.claim.packet_id.get().to_be_bytes());
                return Ok(Arc::from(out.as_slice()));
            }
        }
        _ => {}
    }
    let mut canonical = Vec::with_capacity(256);
    recipe.encode_canonical(&mut canonical)?;
    Ok(Arc::from(canonical))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
    use crate::ids::{
        LogicalOwnerId, OperationSeq, PayloadSeq, PublicationSeq, RequestSeq, RunEpoch,
        StoreIncarnation,
    };
    use crate::ids::{PayloadId, RequestId};

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
            targets: BoundedVec::from_slice(targets, 4).expect("fits"),
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
            targets: a.targets,
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
        let mut targets = BoundedVec::with_capacity(count).unwrap();
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
        admit.targets = BoundedVec::from_slice(targets, bound).expect("fits");
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
            assert_eq!(frozen.canonical_bytes(), expected.as_slice());
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
