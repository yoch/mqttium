//! Bounded exact-subscription and outgoing QoS 1 data (ADR 0025).
use crate::ids::{DeliveryId, OperationId, OutboundPacketId, Revision, SessionId};
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

/// Durable intent that a QoS 1 delivery may have begun. It is not proof of
/// socket write or peer receipt. Its operation is also its slot generation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliveryClaim {
    pub operation: OperationId,
    pub packet_id: OutboundPacketId,
}

/// A coherent bounded work read; payload is loaded only for online work,
/// never repeated in the startup metadata inventory.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WorkItem {
    pub delivery: DeliveryId,
    pub target: SessionId,
    pub target_revision: Revision,
    pub topic: SharedBytes,
    pub payload: SharedBytes,
    pub claim: Option<DeliveryClaim>,
}
