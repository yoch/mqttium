//! The reading of canonical bytes, and what refuses them (DEM-009).
//!
//! Every recipe and every value nested in one is read through a [`Reader`]
//! over its exact bytes, big-endian, and every refusal is a
//! [`RecipeError`]. `command` frames a whole recipe with these; the modules
//! whose values a recipe carries read their own fields with them.

use crate::ids::{
    DeliveryId, ExchangeId, InboundPacketId, OperationId, OutboundPacketId, PublicationId,
    Revision, SessionId,
};
use crate::message::Qos;
use alloc::vec::Vec;

/// The magic every canonical recipe starts with, before its format and its
/// kind.
pub(crate) const MAGIC: &[u8; 4] = b"MQDR";

/// The only recipe format of contract revision 1.
pub const RECIPE_FORMAT: u16 = 1;

// The kinds of format 1 that have a name here; the others are written as
// numbers where their recipe is encoded and decoded.
pub(crate) const KIND_ADMIT_PUBLICATION: u8 = 1;
pub(crate) const KIND_RELEASE_INBOUND: u8 = 2;
pub(crate) const KIND_CHANGE_SUBSCRIPTIONS: u8 = 3;
pub(crate) const KIND_CLAIM_DELIVERY: u8 = 4;
pub(crate) const KIND_FINISH_DELIVERY: u8 = 5;
pub(crate) const KIND_CONSUME_DELIVERY: u8 = 6;
pub(crate) const KIND_UPDATE_SUBSCRIPTIONS: u8 = 7;
pub(crate) const KIND_SUBSCRIPTION_OPTIONS: u8 = 9;
pub(crate) const KIND_ANNOTATED_PUBLICATION: u8 = 23;
pub(crate) const KIND_CLAIMED_PUBLICATION: u8 = 24;

/// The generation of an exchange, as every recipe that names one writes it.
pub(crate) fn put_exchange_generation(out: &mut Vec<u8>, exchange: ExchangeId) {
    out.extend_from_slice(&exchange.generation.to_be_bytes());
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
