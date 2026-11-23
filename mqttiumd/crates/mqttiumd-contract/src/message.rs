//! What every message is made of: its bytes, shared and immutable, and its
//! quality of service. Every other module of the contract builds on these;
//! `packet` re-exports them where the decoder hands them out.

use alloc::sync::Arc;

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
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SharedBytes(Arc<[u8]>);

impl core::borrow::Borrow<[u8]> for SharedBytes {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

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
