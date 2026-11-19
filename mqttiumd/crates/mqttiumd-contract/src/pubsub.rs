//! Bounded subscription and outgoing QoS 1 data (ADR 0025).
use crate::command::DeliveryCause;
use crate::ids::{
    ConnectionKey, DeliveryId, EffectId, OperationId, OutboundPacketId, Revision, SessionId,
};
use crate::packet::{Qos, SharedBytes, TooLarge};
use alloc::sync::Arc;

pub const MAX_FILTERS: usize = 8;
pub const MAX_SUBSCRIPTIONS: usize = 128;
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

/// Durable intent that a QoS 1 delivery may have begun. It is not proof of
/// socket write or peer receipt. Its operation is also its slot generation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliveryClaim {
    pub operation: OperationId,
    pub packet_id: OutboundPacketId,
    /// A durable positive-byte observation; false after recovery is uncertain.
    pub started: bool,
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
    pub expiry: Option<crate::message_expiry::MessageExpiry>,
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
