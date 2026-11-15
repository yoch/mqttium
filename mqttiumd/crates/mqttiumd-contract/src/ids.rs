//! Distinct identity types (DEM-004, ARC-005).
//!
//! Each identity is its own type: there is no conversion between them and no
//! blanket `From<u64>`. Counters never wrap: `checked_next` reports
//! exhaustion and the owner suspends the admissions concerned.
//!
//! Durable encoding (DEM-005, DEM-011): every 64-bit component is eight
//! big-endian bytes, so SQLite stores it as a BLOB whose byte order is the
//! numeric order. Increments are decoded, `checked_next`, re-encoded; never
//! arithmetic on the encoded form.

use core::fmt;
use core::num::NonZeroU16;

use crate::decimal::{DecimalError, parse_u64};

/// A counter reached `u64::MAX`; the identity space must be renewed or closed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Exhausted;

/// A durable identity encoding has the wrong shape.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdDecodeError {
    /// The byte string does not have the exact encoded length.
    WrongLength { expected: usize, actual: usize },
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(array)
}

macro_rules! scalar_id {
    ($(#[doc = $doc:literal])* $name:ident) => {
        $(#[doc = $doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(u64);

        impl $name {
            /// Length of the durable big-endian encoding.
            pub const ENCODED_LEN: usize = 8;

            /// Builds the identity from its numeric value.
            #[must_use]
            pub const fn new(raw: u64) -> Self {
                Self(raw)
            }

            /// The numeric value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// The durable big-endian encoding.
            #[must_use]
            pub const fn to_be_bytes(self) -> [u8; 8] {
                self.0.to_be_bytes()
            }

            /// Decodes the durable encoding, checking its exact length.
            pub fn from_be_slice(bytes: &[u8]) -> Result<Self, IdDecodeError> {
                if bytes.len() != Self::ENCODED_LEN {
                    return Err(IdDecodeError::WrongLength {
                        expected: Self::ENCODED_LEN,
                        actual: bytes.len(),
                    });
                }
                Ok(Self(read_u64(bytes)))
            }

            /// Parses the canonical decimal string used in JSON files.
            pub fn parse_decimal(text: &str) -> Result<Self, DecimalError> {
                parse_u64(text).map(Self)
            }

            /// The next value, or `Exhausted`; never wraps.
            pub fn checked_next(self) -> Result<Self, Exhausted> {
                self.0.checked_add(1).map(Self).ok_or(Exhausted)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }
    };
}

scalar_id!(
    /// Identity space of one data set; renewed by restore and duplication.
    StoreIncarnation
);
scalar_id!(
    /// Durable epoch opened before any new admission of a run.
    RunEpoch
);
scalar_id!(
    /// Logical owner, independent of the number of workers.
    LogicalOwnerId
);
scalar_id!(
    /// Incarnation of a persistent session: stable on resume, new on recreation.
    SessionId
);
scalar_id!(
    /// One accepted connection; never reused within a run epoch.
    ConnectionGeneration
);
scalar_id!(
    /// Per-session generation of a QoS exchange slot.
    ExchangeGeneration
);
scalar_id!(
    /// Per-owner sequence of immutable commands.
    OperationSeq
);
scalar_id!(
    /// Per-owner sequence of logical requests.
    RequestSeq
);
scalar_id!(
    /// Per-owner sequence of internal publications.
    PublicationSeq
);
scalar_id!(
    /// Per-owner sequence of durable payload objects.
    PayloadSeq
);
scalar_id!(
    /// Per-owner sequence of effects handed to adapters.
    EffectSeq
);
scalar_id!(
    /// Serial of one effect box under one creation authority. It comes from
    /// a checked counter and is never used again, even after the box was
    /// retired (ADR 0009).
    OutboxSerial
);
scalar_id!(
    /// A capacity account.
    AccountId
);
scalar_id!(
    /// Reuse generation of a lease slot.
    LeaseGeneration
);
scalar_id!(
    /// Revision of one sub-state: session lifecycle, route table, slot.
    Revision
);
scalar_id!(
    /// Token of one armed deadline; never reused.
    DeadlineToken
);
scalar_id!(
    /// Identity of one counted continuation.
    ContinuationId
);
scalar_id!(
    /// Explicit time supplied to the core as data.
    LogicalTime
);
scalar_id!(
    /// A delay in the unit of `LogicalTime`, counted by the timer adapter
    /// from the moment it accepts the deadline.
    LogicalDuration
);

macro_rules! scoped_id {
    ($(#[doc = $doc:literal])* $name:ident, $sequence:ident) => {
        $(#[doc = $doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name {
            incarnation: StoreIncarnation,
            epoch: RunEpoch,
            owner: LogicalOwnerId,
            sequence: $sequence,
        }

        impl $name {
            /// Length of the durable encoding: four big-endian components.
            pub const ENCODED_LEN: usize = 32;

            /// Builds the identity from its four components.
            #[must_use]
            pub const fn new(
                incarnation: StoreIncarnation,
                epoch: RunEpoch,
                owner: LogicalOwnerId,
                sequence: $sequence,
            ) -> Self {
                Self { incarnation, epoch, owner, sequence }
            }

            /// The data set this identity belongs to.
            #[must_use]
            pub const fn incarnation(self) -> StoreIncarnation {
                self.incarnation
            }

            /// The run epoch that issued this identity.
            #[must_use]
            pub const fn epoch(self) -> RunEpoch {
                self.epoch
            }

            /// The logical owner that issued this identity.
            #[must_use]
            pub const fn owner(self) -> LogicalOwnerId {
                self.owner
            }

            /// The owner's sequence number.
            #[must_use]
            pub const fn sequence(self) -> $sequence {
                self.sequence
            }

            /// The durable encoding: incarnation, epoch, owner, sequence.
            #[must_use]
            pub fn to_be_bytes(self) -> [u8; 32] {
                let mut bytes = [0u8; 32];
                bytes[0..8].copy_from_slice(&self.incarnation.to_be_bytes());
                bytes[8..16].copy_from_slice(&self.epoch.to_be_bytes());
                bytes[16..24].copy_from_slice(&self.owner.to_be_bytes());
                bytes[24..32].copy_from_slice(&self.sequence.to_be_bytes());
                bytes
            }

            /// Decodes the durable encoding, checking its exact length.
            pub fn from_be_slice(bytes: &[u8]) -> Result<Self, IdDecodeError> {
                if bytes.len() != Self::ENCODED_LEN {
                    return Err(IdDecodeError::WrongLength {
                        expected: Self::ENCODED_LEN,
                        actual: bytes.len(),
                    });
                }
                Ok(Self {
                    incarnation: StoreIncarnation::new(read_u64(&bytes[0..8])),
                    epoch: RunEpoch::new(read_u64(&bytes[8..16])),
                    owner: LogicalOwnerId::new(read_u64(&bytes[16..24])),
                    sequence: $sequence::new(read_u64(&bytes[24..32])),
                })
            }
        }
    };
}

scoped_id!(
    /// One immutable command. An `OperationId` designates exactly one recipe
    /// (DEM-009); a new preparation after a certain rejection gets a new one.
    OperationId,
    OperationSeq
);
scoped_id!(
    /// One logical request, kept across re-preparations after certain rejection.
    RequestId,
    RequestSeq
);
scoped_id!(
    /// One internal publication, independent of any Packet Identifier.
    PublicationId,
    PublicationSeq
);
scoped_id!(
    /// One durable immutable payload object.
    PayloadId,
    PayloadSeq
);

/// A precise connection of a session. Guards network replies only: a store
/// result for an old connection may still carry a commit that must be
/// reconciled (DEM-004, ID-004).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ConnectionKey {
    pub session: SessionId,
    pub generation: ConnectionGeneration,
}

/// One MQTT QoS exchange of a session. The generation distinguishes it from
/// any later exchange that reuses the same Packet Identifier.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ExchangeId {
    pub session: SessionId,
    pub generation: ExchangeGeneration,
}

/// One submission to an adapter and its result. Volatile: lost in a crash.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EffectId {
    pub epoch: RunEpoch,
    pub owner: LogicalOwnerId,
    pub sequence: EffectSeq,
}

/// One obligation towards one destination of a publication.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct DeliveryId {
    pub publication: PublicationId,
    pub index: u32,
}

/// A reservation: the account charged, the slot holding it and the reuse
/// generation of that slot. A stale key never releases a newer reservation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LeaseKey {
    pub account: AccountId,
    pub slot: u32,
    pub generation: LeaseGeneration,
}

macro_rules! packet_id {
    ($(#[doc = $doc:literal])* $name:ident) => {
        $(#[doc = $doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(NonZeroU16);

        impl $name {
            /// Builds a Packet Identifier; zero is not one.
            #[must_use]
            pub const fn new(raw: u16) -> Option<Self> {
                match NonZeroU16::new(raw) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }

            /// The MQTT Packet Identifier, `1..=65535`.
            #[must_use]
            pub const fn get(self) -> u16 {
                self.0.get()
            }
        }
    };
}

packet_id!(
    /// Packet Identifier chosen by the client for its own QoS exchanges.
    InboundPacketId
);
packet_id!(
    /// Packet Identifier chosen by the broker for its outgoing exchanges.
    /// The same number may be live in both directions at once.
    OutboundPacketId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_report_exhaustion_instead_of_wrapping() {
        assert_eq!(
            OperationSeq::new(0).checked_next(),
            Ok(OperationSeq::new(1))
        );
        assert_eq!(
            OperationSeq::new(u64::MAX - 1).checked_next(),
            Ok(OperationSeq::new(u64::MAX))
        );
        assert_eq!(OperationSeq::new(u64::MAX).checked_next(), Err(Exhausted));
    }

    #[test]
    fn scalar_encoding_is_eight_big_endian_bytes() {
        let id = SessionId::new(0x0102_0304_0506_0708);
        assert_eq!(id.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(SessionId::from_be_slice(&id.to_be_bytes()), Ok(id));
        for raw in [0, 1, u64::MAX - 1, u64::MAX] {
            let id = RunEpoch::new(raw);
            assert_eq!(RunEpoch::from_be_slice(&id.to_be_bytes()), Ok(id));
        }
    }

    #[test]
    fn encoded_order_is_numeric_order() {
        let values = [0u64, 1, 255, 256, 65_536, u64::MAX - 1, u64::MAX];
        for pair in values.windows(2) {
            let low = SessionId::new(pair[0]).to_be_bytes();
            let high = SessionId::new(pair[1]).to_be_bytes();
            assert!(low < high);
        }
    }

    #[test]
    fn decoding_checks_the_exact_length() {
        assert_eq!(
            SessionId::from_be_slice(&[0; 7]),
            Err(IdDecodeError::WrongLength {
                expected: 8,
                actual: 7
            })
        );
        assert_eq!(
            SessionId::from_be_slice(&[0; 9]),
            Err(IdDecodeError::WrongLength {
                expected: 8,
                actual: 9
            })
        );
        assert_eq!(
            OperationId::from_be_slice(&[0; 31]),
            Err(IdDecodeError::WrongLength {
                expected: 32,
                actual: 31
            })
        );
    }

    #[test]
    fn scoped_identity_round_trips_and_keeps_component_order() {
        let id = OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(3),
            OperationSeq::new(u64::MAX),
        );
        let bytes = id.to_be_bytes();
        assert_eq!(&bytes[0..8], &1u64.to_be_bytes());
        assert_eq!(&bytes[8..16], &2u64.to_be_bytes());
        assert_eq!(&bytes[16..24], &3u64.to_be_bytes());
        assert_eq!(&bytes[24..32], &u64::MAX.to_be_bytes());
        assert_eq!(OperationId::from_be_slice(&bytes), Ok(id));
    }

    #[test]
    fn decimal_text_is_canonical() {
        assert_eq!(SessionId::parse_decimal("42"), Ok(SessionId::new(42)));
        assert_eq!(SessionId::new(42).to_string(), "42");
        assert_eq!(
            SessionId::parse_decimal("042"),
            Err(DecimalError::LeadingZero)
        );
        assert_eq!(
            SessionId::parse_decimal("18446744073709551616"),
            Err(DecimalError::Overflow)
        );
    }

    #[test]
    fn packet_identifiers_exclude_zero_and_keep_their_direction() {
        assert!(InboundPacketId::new(0).is_none());
        assert!(OutboundPacketId::new(0).is_none());
        assert_eq!(InboundPacketId::new(1).map(InboundPacketId::get), Some(1));
        assert_eq!(
            InboundPacketId::new(65_535).map(InboundPacketId::get),
            Some(65_535)
        );
        assert_eq!(OutboundPacketId::new(7).map(OutboundPacketId::get), Some(7));
    }
}
