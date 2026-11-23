//! Bounded subscription and outgoing QoS 1 data (ADR 0025).
use crate::canonical::{
    KIND_SUBSCRIPTION_OPTIONS, KIND_UPDATE_SUBSCRIPTIONS, MAGIC, RECIPE_FORMAT, RecipeError,
};
use crate::capacity::BoundedVec;
use crate::ids::{
    ConnectionKey, DeliveryId, EffectId, OperationId, OutboundPacketId, Revision, SessionId,
};
use crate::message::{Qos, SharedBytes, TooLarge};
use alloc::sync::Arc;
use alloc::vec::Vec;

pub const MAX_FILTERS: usize = 8;
pub const MAX_SUBSCRIPTIONS: usize = 16_384;
pub const WORK_PAGE_LIMIT: u32 = 8;

/// Immutable bounded filters, cheaply cloned with a validated packet. The
/// capacity primitive is unchanged; this owns at most eight shared entries.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FilterList<T>(Arc<[T]>);

impl<T: Clone> FilterList<T> {
    pub fn copy_from(items: &[T]) -> Result<Self, TooLarge> {
        if items.len() > MAX_FILTERS {
            return Err(TooLarge {
                len: items.len(),
                max: MAX_FILTERS,
            });
        }
        Ok(Self(Arc::from(items)))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SubscriptionFilter {
    pub filter: SharedBytes,
    pub qos: Qos,
    pub options: SubscriptionOptions,
}

/// Ordered packet-specific reason codes, with a fixed bounded representation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FilterResults {
    pub codes: [u8; MAX_FILTERS],
    pub len: u8,
}

impl FilterResults {
    pub fn new(codes: &[u8]) -> Result<Self, TooLarge> {
        if codes.len() > MAX_FILTERS {
            return Err(TooLarge {
                len: codes.len(),
                max: MAX_FILTERS,
            });
        }
        let mut result = Self {
            codes: [0; MAX_FILTERS],
            len: codes.len() as u8,
        };
        result.codes[..codes.len()].copy_from_slice(codes);
        Ok(result)
    }

    #[must_use]
    pub fn as_slice(&self) -> Option<&[u8]> {
        self.codes.get(..usize::from(self.len))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SubscriptionChange {
    pub topic: SharedBytes,
    pub subscribe: bool,
}
/// New subscription semantics; legacy kind 3 retains its implicit QoS 1.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GrantedSubscriptionChange {
    pub topic: SharedBytes,
    pub granted: Option<Qos>,
    pub options: SubscriptionOptions,
}
/// A publication occurrence in one live connection, with no MQTT identifier.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VolatilePublicationId {
    pub source: ConnectionKey,
    pub ingress_order: u64,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Qos0Origin {
    Volatile {
        publication: VolatilePublicationId,
    },
    Consumed {
        delivery: DeliveryId,
        operation: OperationId,
    },
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Qos0DropReason {
    MessageExpired,
    TemporalUncertain,
    NoSubscribers,
    DestinationAbsent,
    CapacityExceeded,
    PacketTooLarge,
    ConnectionClosed,
    OutputSaturated,
    WriteFailed,
}
impl Qos0DropReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::MessageExpired => "message_expired",
            Self::TemporalUncertain => "temporal_uncertain",
            Self::NoSubscribers => "no_subscribers",
            Self::DestinationAbsent => "destination_absent",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::PacketTooLarge => "packet_too_large",
            Self::ConnectionClosed => "connection_closed",
            Self::OutputSaturated => "output_saturated",
            Self::WriteFailed => "write_failed",
        }
    }
}

/// Why the owner ended a claimed QoS 1 or 2 delivery without sending it
/// (ADR 0025, amendment of 21 November 2026). The obligation ends through
/// its guarded `FinishDelivery`; no PUBLISH and no acknowledgement exist.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeliveryDiscardReason {
    /// The only encoding of its PUBLISH exceeds the Maximum Packet Size of
    /// the receiving connection (MQTT-3.1.2-25).
    PacketTooLarge,
}
impl DeliveryDiscardReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PacketTooLarge => "packet_too_large",
        }
    }
}

/// Durable intent that a QoS 1 or 2 delivery may have begun. It is not proof of
/// socket write or peer receipt. Its operation is also its slot generation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliveryClaim {
    pub operation: OperationId,
    pub packet_id: OutboundPacketId,
    /// A durable positive-byte observation; false after recovery is uncertain.
    pub started: bool,
    /// Positive PUBREC committed. The next transmission is PUBREL.
    pub await_pubcomp: bool,
}

impl DeliveryClaim {
    #[must_use]
    pub const fn valid_for(self, qos: Qos, timed: bool) -> bool {
        !matches!(qos, Qos::AtMostOnce)
            && (!self.await_pubcomp || matches!(qos, Qos::ExactlyOnce))
            && (!self.started || timed)
    }
}

/// An immutable association of a validated PUBACK observation with one
/// outgoing occurrence (ADR 0028). The Packet Identifier is recyclable;
/// none of these causal identities is. The connection and packet number
/// travel in `Input::ObservedPuback` alongside this target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PubackTarget {
    pub delivery: DeliveryId,
    pub claim: OperationId,
    pub frame: EffectId,
}

/// A coherent bounded work read; payload is loaded only for online work,
/// never repeated in the startup metadata inventory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WorkItem {
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub qos: Qos,
    pub cause: DeliveryCause,
    pub retain: bool,
    pub expiry: Option<crate::message_time::MessageExpiry>,
    pub properties: Option<Arc<crate::properties::PublishProperties>>,
    pub topic: SharedBytes,
    pub payload: SharedBytes,
    pub claim: Option<DeliveryClaim>,
}

/// MQTT 5 subscription options excluding QoS.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SubscriptionOptions(u8);
impl SubscriptionOptions {
    pub const DEFAULT: Self = Self(0);
    #[must_use]
    pub const fn from_bits(bits: u8) -> Option<Self> {
        if bits & 0xc3 != 0 || (bits >> 4) & 3 == 3 {
            None
        } else {
            Some(Self(bits))
        }
    }
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
    #[must_use]
    pub const fn retain_as_published(self) -> bool {
        self.0 & 8 != 0
    }
    #[must_use]
    pub const fn retain_handling(self) -> u8 {
        (self.0 >> 4) & 3
    }
    #[must_use]
    pub const fn no_local(self) -> bool {
        self.0 & 4 != 0
    }
}

/// Why an obligation exists. The cause stays inspectable by the oracle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeliveryCause {
    /// A live publication selected the committed routing cut. The historical
    /// name and byte value also cover aggregated hierarchical subscriptions.
    LiveExact = 1,
    RetainedReplay = 2,
}

// The recipes of subscriptions (kinds 3, 7 and 9) and of outgoing
// deliveries (kinds 4 to 6); `command` frames them.
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
