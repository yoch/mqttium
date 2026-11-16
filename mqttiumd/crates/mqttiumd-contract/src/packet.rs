//! Validated packets and frames of the S0 subset (DEM-008, DEM-022).
//!
//! The decoder turns bytes into these values only after validation; a
//! malformed scenario supplies bytes and never builds an impossible value
//! here. The subset includes durable incoming QoS 1 and QoS 2.

use alloc::sync::Arc;

use crate::ids::{InboundPacketId, OutboundPacketId};
use crate::outcome::RejectReason;
use crate::pubsub::{FilterList, FilterResults, SubscriptionFilter};

/// MQTT quality of service.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl Qos {
    /// Decodes the two-bit wire value; `3` is not a QoS.
    #[must_use]
    pub const fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }

    /// The wire value.
    #[must_use]
    pub const fn to_wire(self) -> u8 {
        self as u8
    }
}

/// The slice exceeds the bound given by the caller.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TooLarge {
    pub len: usize,
    pub max: usize,
}

/// Immutable shared bytes: a payload or a topic name.
///
/// Cloning shares the same allocation. No method mutates the content, and
/// the inner `Arc` is private, so immutability is a property of this wrapper
/// rather than a convention (DEM-009). The physical cost stays charged until
/// the last clone is dropped; each clone has its own reference cost.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SharedBytes(Arc<[u8]>);

impl SharedBytes {
    /// Copies `bytes` once into shared storage, refusing more than `max`.
    pub fn copy_from(bytes: &[u8], max: usize) -> Result<Self, TooLarge> {
        if bytes.len() > max {
            return Err(TooLarge {
                len: bytes.len(),
                max,
            });
        }
        Ok(Self(Arc::from(bytes)))
    }

    /// The content.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the content is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether both values share one allocation.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// A validated packet received from a client.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum InboundPacket {
    /// CONNECT resuming a session prepared by the fixture.
    ConnectResume { client_id: SharedBytes },
    /// PUBLISH. `packet_id` is present exactly when `qos` is above zero.
    Publish {
        topic: SharedBytes,
        qos: Qos,
        retain: bool,
        dup: bool,
        packet_id: Option<InboundPacketId>,
        payload: SharedBytes,
    },
    /// PUBREL of an inbound QoS 2 exchange.
    Pubrel { packet_id: InboundPacketId },
    /// A validated MQTT PINGREQ, without a body.
    Pingreq,
    Subscribe {
        packet_id: InboundPacketId,
        filters: FilterList<SubscriptionFilter>,
    },
    Unsubscribe {
        packet_id: InboundPacketId,
        filters: FilterList<SharedBytes>,
    },
    /// The decoder accepts only the MQTT 5 PUBACK reason-code domain.
    Puback {
        packet_id: OutboundPacketId,
        reason: u8,
    },
}

/// Outcome carried by a PUBREC or PUBCOMP of the subset.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AckReason {
    Success,
    /// PUBCOMP for a PUBREL whose identifier is not in use.
    PacketIdentifierNotFound,
    /// PUBREC refusing the admission for lack of capacity (MQTT 5
    /// section 3.5.2.1, reason code 0x97). It answers a certain rejection
    /// for `CapacityExceeded` and no other: it tells the client that a
    /// quota is the cause.
    QuotaExceeded,
    /// PUBREC refusing an admission that is valid and that the broker
    /// does not accept (MQTT 5 section 3.5.2.1, reason code 0x83,
    /// "Implementation specific error: the PUBLISH is valid but the
    /// receiver does not accept it"). It answers the certain rejections
    /// in which no quota is involved: `Fenced`, `PreconditionFailed` and
    /// `EpochClosed`.
    ImplementationSpecificError,
}

impl AckReason {
    /// The reason of the PUBREC that refuses an admission the store
    /// certainly rejected. Nothing was admitted, the exchange ends and the
    /// Packet Identifier is free again (MQTT 5 section 4.3.3).
    #[must_use]
    pub const fn refusing(rejection: RejectReason) -> Self {
        match rejection {
            RejectReason::CapacityExceeded(_) => Self::QuotaExceeded,
            RejectReason::Fenced | RejectReason::PreconditionFailed | RejectReason::EpochClosed => {
                Self::ImplementationSpecificError
            }
        }
    }
}

/// PUBACK reasons implemented by the incoming QoS 1 receiver. Kept separate
/// from PUBCOMP: Packet Identifier not found (0x92) is not valid in PUBACK.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PubackReason {
    Success,
    QuotaExceeded,
    ImplementationSpecificError,
}

impl PubackReason {
    #[must_use]
    pub const fn refusing(rejection: RejectReason) -> Self {
        match rejection {
            RejectReason::CapacityExceeded(_) => Self::QuotaExceeded,
            RejectReason::Fenced | RejectReason::PreconditionFailed | RejectReason::EpochClosed => {
                Self::ImplementationSpecificError
            }
        }
    }

    /// MQTT 5 section 3.4.2.1. No reason belonging only to another ACK can
    /// be represented by this type.
    #[must_use]
    pub const fn to_wire(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::QuotaExceeded => 0x97,
            Self::ImplementationSpecificError => 0x83,
        }
    }
}

/// A frame the core may make eligible on a connection.
///
/// PUBREC and PUBCOMP answer the client's exchange, so they carry the
/// client's Packet Identifier space.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OutboundFrame {
    Suback {
        packet_id: InboundPacketId,
        results: FilterResults,
    },
    Unsuback {
        packet_id: InboundPacketId,
        results: FilterResults,
    },
    /// Response to a validated PINGREQ on a connected session.
    Pingresp,
    Connack {
        session_present: bool,
    },
    Puback {
        packet_id: InboundPacketId,
        reason: PubackReason,
    },
    Pubrec {
        packet_id: InboundPacketId,
        reason: AckReason,
    },
    Pubcomp {
        packet_id: InboundPacketId,
        reason: AckReason,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Resource;

    // MQTT 5 section 3.5.2.1: a refusal says that a quota is exceeded only
    // when one is.
    #[test]
    fn a_refusal_names_a_quota_only_for_a_lack_of_capacity() {
        for resource in [
            Resource::PayloadBytes,
            Resource::References,
            Resource::ExchangeSlots,
            Resource::ReceiptSlots,
        ] {
            assert_eq!(
                AckReason::refusing(RejectReason::CapacityExceeded(resource)),
                AckReason::QuotaExceeded,
                "{resource:?}"
            );
        }
        for rejection in [
            RejectReason::Fenced,
            RejectReason::PreconditionFailed,
            RejectReason::EpochClosed,
        ] {
            assert_eq!(
                AckReason::refusing(rejection),
                AckReason::ImplementationSpecificError,
                "{rejection:?}: no quota is involved"
            );
        }
    }

    #[test]
    fn qos_wire_values_are_closed() {
        for value in 0..=2 {
            assert_eq!(Qos::from_wire(value).map(Qos::to_wire), Some(value));
        }
        assert_eq!(Qos::from_wire(3), None);
    }

    #[test]
    fn shared_bytes_respect_the_bound_and_share_storage() {
        assert_eq!(
            SharedBytes::copy_from(b"abc", 2),
            Err(TooLarge { len: 3, max: 2 })
        );
        let payload = SharedBytes::copy_from(b"abc", 3).expect("fits");
        let reference = payload.clone();
        assert!(payload.shares_storage_with(&reference));
        assert_eq!(reference.as_slice(), b"abc");
        let copy = SharedBytes::copy_from(b"abc", 3).expect("fits");
        assert_eq!(copy, payload);
        assert!(!copy.shares_storage_with(&payload));
    }
}
