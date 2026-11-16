//! Bounded live receipt retirement (proposed ADR 0027).
//!
//! Confirmation consumes a command's result, not its business responsibility.
//! This control has no OperationId and never creates a receipt of its own.

use crate::command::Recipe;
use crate::ids::{
    LogicalOwnerId, OperationId, OperationSeq, PayloadSeq, PublicationSeq, RunEpoch,
    StoreIncarnation,
};

pub const RECEIPT_WINDOW: usize = 1024;
pub const RECEIPT_DATA_WINDOW: usize = 768;
pub const RECEIPT_CONTROL_RESERVE: u64 = 512;
pub const MAX_RECEIPT_ORIGINS: usize = 64;
pub const MAX_COLLECTION_ROWS: u16 = 128;
pub const RECEIPT_COLLECTION_TRIGGER: usize = 64;

/// One producer's never-reused sequence spaces.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ReceiptOrigin {
    pub incarnation: StoreIncarnation,
    pub epoch: RunEpoch,
    pub owner: LogicalOwnerId,
}

impl ReceiptOrigin {
    #[must_use]
    pub const fn of(operation: OperationId) -> Self {
        Self {
            incarnation: operation.incarnation(),
            epoch: operation.epoch(),
            owner: operation.owner(),
        }
    }
}

/// Durable non-execution boundaries. Zero is the initial empty prefix.
/// Identity boundaries come from issuance at the consumed operation prefix,
/// never from the largest committed admission seen by a reader.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReceiptClosure {
    pub origin: ReceiptOrigin,
    pub through: OperationSeq,
    pub publication_through: PublicationSeq,
    pub payload_through: PayloadSeq,
}

impl ReceiptClosure {
    #[must_use]
    pub const fn empty(origin: ReceiptOrigin) -> Self {
        Self {
            origin,
            through: OperationSeq::new(0),
            publication_through: PublicationSeq::new(0),
            payload_through: PayloadSeq::new(0),
        }
    }
}

/// Idempotent confirmation and one bounded physical sweep in one transaction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReceiptCollection {
    pub closure: ReceiptClosure,
    pub limit: u16,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReceiptCollectionRefusal {
    OriginMismatch,
    FutureEpoch,
    InvalidFrontier,
    Gap,
    LimitExceeded,
    OriginCapacity,
    Inconsistent,
    Exhausted,
}

/// Counts describe this pass; they are never credits that a caller may add
/// again when an answer is repeated. Issuance uses the absolute known closure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReceiptCollectionReply {
    Applied {
        closure: ReceiptClosure,
        removed: u16,
        retained: u64,
        more: bool,
    },
    PendingOrUnknown,
    Refused(ReceiptCollectionRefusal),
}

/// Startup-only bounded sweeping behind the existing closed-epoch authority.
/// It does not rotate an active producer or manufacture result consumption.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ClosedReceiptCollection {
    pub through: RunEpoch,
    pub limit: u16,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClosedReceiptReply {
    Applied {
        through: RunEpoch,
        removed: u16,
        removed_origins: u16,
        retained: u64,
        more: bool,
    },
    PendingOrUnknown,
    Refused(ReceiptCollectionRefusal),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReceiptClass {
    Business,
    Control,
}

impl ReceiptClass {
    #[must_use]
    pub const fn of(recipe: &Recipe) -> Self {
        match recipe {
            Recipe::AdmitPublication(_)
            | Recipe::ChangeSubscriptions(_)
            | Recipe::UpdateSubscriptions(_) => Self::Business,
            Recipe::ReleaseInbound(_)
            | Recipe::ClaimDelivery(_)
            | Recipe::FinishDelivery(_)
            | Recipe::ConsumeDelivery(_) => Self::Control,
        }
    }
}
