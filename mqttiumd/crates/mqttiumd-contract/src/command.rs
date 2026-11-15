//! Frozen commands and their exact canonical representation (DEM-009).
//!
//! An `OperationId` designates exactly one recipe. The store keeps the
//! canonical bytes of the recipe and compares them byte for byte when a
//! command is presented again; a digest may speed up a lookup but never
//! replaces that comparison.
//!
//! `OpenEpoch` is the one command that is not a recipe: it opens the run
//! epoch every `OperationId` is scoped by. Its durable state is the
//! `EpochLedger`; its results and the hand-over of the creation authority
//! are in `outcome` (ADR 0009).
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
//! ```

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::capacity::{BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use crate::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, Exhausted, InboundPacketId, OperationId, PayloadId,
    PublicationId, RequestId, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use crate::packet::{Qos, SharedBytes};

const MAGIC: &[u8; 4] = b"MQDR";

/// The only recipe format of contract revision 1.
pub const RECIPE_FORMAT: u16 = 1;

const KIND_ADMIT_PUBLICATION: u8 = 1;
const KIND_RELEASE_INBOUND: u8 = 2;

/// Why an obligation exists. The cause stays inspectable by the oracle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeliveryCause {
    /// A live publication matched an exact-topic route.
    LiveExact = 1,
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
    pub targets: BoundedVec<Target>,
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

/// The closed set of recipes of the S0 perimeter.
#[expect(
    clippy::large_enum_variant,
    reason = "a recipe lives behind the Arc of FrozenRecipe; boxing would add an allocation"
)]
#[derive(PartialEq, Eq, Debug)]
pub enum Recipe {
    AdmitPublication(AdmitPublication),
    ReleaseInbound(ReleaseInbound),
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

impl Recipe {
    /// The operation this recipe belongs to.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        match self {
            Self::AdmitPublication(admit) => admit.operation,
            Self::ReleaseInbound(release) => release.operation,
        }
    }

    /// Checks the internal consistency the canonical layout relies on.
    pub fn check(&self) -> Result<(), RecipeError> {
        match self {
            Self::AdmitPublication(admit) => {
                if admit.source.exchange.session != admit.source.session {
                    return Err(RecipeError::ForeignExchange);
                }
                if admit.topic.len() > usize::from(u16::MAX) {
                    return Err(RecipeError::TopicTooLong(admit.topic.len()));
                }
                if admit.targets.len() > usize::from(u16::MAX) {
                    return Err(RecipeError::TooManyTargets {
                        count: admit.targets.len(),
                        max: usize::from(u16::MAX),
                    });
                }
                if admit
                    .targets
                    .as_slice()
                    .iter()
                    .any(|target| target.delivery.publication != admit.publication)
                {
                    return Err(RecipeError::ForeignDelivery);
                }
                Ok(())
            }
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
            Self::AdmitPublication(admit) => {
                out.push(KIND_ADMIT_PUBLICATION);
                out.extend_from_slice(&admit.operation.to_be_bytes());
                out.extend_from_slice(&admit.request.to_be_bytes());
                out.extend_from_slice(&admit.publication.to_be_bytes());
                out.extend_from_slice(&admit.source.session.to_be_bytes());
                out.extend_from_slice(&admit.source.session_revision.to_be_bytes());
                out.extend_from_slice(&admit.source.packet_id.get().to_be_bytes());
                put_exchange_generation(out, admit.source.exchange);
                out.extend_from_slice(&admit.route_revision.to_be_bytes());
                out.push(admit.qos.to_wire());
                out.push(u8::from(admit.retain));
                // Both lengths were bounded by `check` above.
                out.extend_from_slice(&(admit.topic.len() as u16).to_be_bytes());
                out.extend_from_slice(admit.topic.as_slice());
                out.extend_from_slice(&admit.payload.id.to_be_bytes());
                out.extend_from_slice(&admit.payload.length.get().to_be_bytes());
                out.extend_from_slice(&admit.costs.payload_bytes.get().to_be_bytes());
                out.extend_from_slice(&admit.costs.references.get().to_be_bytes());
                out.extend_from_slice(&admit.costs.exchange_slots.get().to_be_bytes());
                out.extend_from_slice(&admit.costs.receipt_slots.get().to_be_bytes());
                out.extend_from_slice(&(admit.targets.len() as u16).to_be_bytes());
                for target in admit.targets.as_slice() {
                    out.extend_from_slice(&target.session.to_be_bytes());
                    out.extend_from_slice(&target.session_revision.to_be_bytes());
                    out.extend_from_slice(&target.delivery.index.to_be_bytes());
                    out.push(target.effective_qos.to_wire());
                    out.push(target.cause as u8);
                }
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
            KIND_ADMIT_PUBLICATION => {
                let operation = reader.operation()?;
                let request = RequestId::from_be_slice(reader.take(32)?)
                    .map_err(|_| RecipeError::Truncated)?;
                let publication = PublicationId::from_be_slice(reader.take(32)?)
                    .map_err(|_| RecipeError::Truncated)?;
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
                    id: PayloadId::from_be_slice(reader.take(32)?)
                        .map_err(|_| RecipeError::Truncated)?,
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
                    BoundedVec::with_capacity(count).map_err(|_| RecipeError::AllocationFailed)?;
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
                Self::AdmitPublication(AdmitPublication {
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
        Ok(recipe)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], RecipeError> {
        if self.bytes.len() < len {
            return Err(RecipeError::Truncated);
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, RecipeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, RecipeError> {
        let mut array = [0u8; 2];
        array.copy_from_slice(self.take(2)?);
        Ok(u16::from_be_bytes(array))
    }

    fn u32(&mut self) -> Result<u32, RecipeError> {
        let mut array = [0u8; 4];
        array.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(array))
    }

    fn u64(&mut self) -> Result<u64, RecipeError> {
        let mut array = [0u8; 8];
        array.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(array))
    }

    fn operation(&mut self) -> Result<OperationId, RecipeError> {
        OperationId::from_be_slice(self.take(32)?).map_err(|_| RecipeError::Truncated)
    }

    fn packet_id(&mut self) -> Result<InboundPacketId, RecipeError> {
        InboundPacketId::new(self.u16()?).ok_or(RecipeError::InvalidField("packet_id"))
    }

    fn qos(&mut self) -> Result<Qos, RecipeError> {
        Qos::from_wire(self.u8()?).ok_or(RecipeError::InvalidField("qos"))
    }

    fn boolean(&mut self, field: &'static str) -> Result<bool, RecipeError> {
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
/// the operation is resolved.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FrozenRecipe(Arc<Recipe>);

impl FrozenRecipe {
    /// Freezes a well-formed recipe.
    pub fn freeze(recipe: Recipe) -> Result<Self, RecipeError> {
        recipe.check()?;
        Ok(Self(Arc::new(recipe)))
    }

    /// The frozen recipe.
    #[must_use]
    pub fn get(&self) -> &Recipe {
        &self.0
    }

    /// The operation this recipe belongs to.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        self.0.operation()
    }
}

/// A frozen command proposed to the store. The payload bytes travel with an
/// admission; the recipe refers to them by durable identity and length.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Command {
    pub recipe: FrozenRecipe,
    pub payload: Option<SharedBytes>,
}

/// Volatile correlation of one attempt to open a run epoch.
///
/// A ticket is minted by the `EpochOpening` of the process that submits
/// the attempt and is echoed by the store in its reply. It is never
/// durable: a crash loses it with the attempt it named. Like every reply
/// correlation it is plain data and carries no authority (ADR 0003).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct OpeningTicket(u64);

impl OpeningTicket {
    /// Builds the ticket from its numeric value.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next ticket, or `Exhausted`; never wraps.
    pub fn checked_next(self) -> Result<Self, Exhausted> {
        self.0.checked_add(1).map(Self).ok_or(Exhausted)
    }
}

/// The start-up command that opens a run epoch (DEM-012, ADR 0009).
///
/// It is not a `Recipe`: it precedes every `OperationId`, which needs the
/// epoch this command establishes. The store applies it in the order of
/// authority of every other command, as one transaction that closes every
/// older epoch for execution and opens the next one, or changes nothing.
/// There is no instant at which two epochs are open, and none at which an
/// older epoch could be entered again.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpenEpoch {
    pub ticket: OpeningTicket,
}

/// Durable phase of one run epoch of a data set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EpochPhase {
    /// No committed opening established this epoch yet.
    Unopened,
    /// The one epoch in which commands may execute.
    Open,
    /// Definitively closed for execution. Closing forbids execution; it
    /// does not turn the unknown results of the epoch into rejections
    /// (V3-006), and its durable effects are still reconciled (DEM-012).
    Closed,
}

/// The durable state of the epochs of one data set: its incarnation and
/// the last epoch a committed opening established.
///
/// Epochs open in increasing order, one at a time. The last opened epoch
/// is the only open one and every epoch before it is closed; epoch 0 is
/// never opened and encodes "no epoch yet". The same pure transition is
/// used by the deterministic store and by SQLite, which reads the two
/// values, calls `open_next` and writes the result in the transaction
/// that applies `OpenEpoch`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EpochLedger {
    incarnation: StoreIncarnation,
    last_opened: RunEpoch,
}

impl EpochLedger {
    /// The ledger of a data set in which no epoch was opened yet.
    #[must_use]
    pub const fn fresh(incarnation: StoreIncarnation) -> Self {
        Self {
            incarnation,
            last_opened: RunEpoch::new(0),
        }
    }

    /// Rebuilds the ledger from its two durable values. `last_opened` is
    /// zero when no epoch was opened.
    #[must_use]
    pub const fn from_durable(incarnation: StoreIncarnation, last_opened: RunEpoch) -> Self {
        Self {
            incarnation,
            last_opened,
        }
    }

    /// The data set these epochs belong to.
    #[must_use]
    pub const fn incarnation(&self) -> StoreIncarnation {
        self.incarnation
    }

    /// The durable value: the last opened epoch, zero when there is none.
    #[must_use]
    pub const fn last_opened(&self) -> RunEpoch {
        self.last_opened
    }

    /// The one epoch open for execution, if an opening was committed.
    #[must_use]
    pub fn open_epoch(&self) -> Option<RunEpoch> {
        (self.last_opened.get() != 0).then_some(self.last_opened)
    }

    /// The durable phase of `epoch`.
    #[must_use]
    pub fn phase_of(&self, epoch: RunEpoch) -> EpochPhase {
        if epoch <= self.last_opened {
            EpochPhase::Open
        } else {
            EpochPhase::Unopened
        }
    }

    /// The ledger after one committed opening. Skeleton: the rules are
    /// not written yet.
    pub fn open_next(&self) -> Result<Self, Exhausted> {
        Ok(Self {
            incarnation: self.incarnation,
            last_opened: self.last_opened.checked_next().unwrap_or(self.last_opened),
        })
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
            targets: BoundedVec::from_slice(targets, 4).expect("fits"),
        })
    }

    fn encode(recipe: &Recipe) -> Vec<u8> {
        let mut bytes = Vec::new();
        recipe.encode_canonical(&mut bytes).expect("well formed");
        bytes
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
        kind[6] = 3;
        assert_eq!(
            Recipe::decode_canonical(&kind, 4, 64),
            Err(RecipeError::UnknownKind(3))
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
    fn a_fresh_ledger_has_no_open_epoch() {
        let ledger = EpochLedger::fresh(StoreIncarnation::new(7));
        assert_eq!(ledger.incarnation(), StoreIncarnation::new(7));
        assert_eq!(ledger.open_epoch(), None);
        assert_eq!(ledger.last_opened(), RunEpoch::new(0));
        for epoch in [0, 1, 2, u64::MAX] {
            assert_eq!(ledger.phase_of(RunEpoch::new(epoch)), EpochPhase::Unopened);
        }
    }

    #[test]
    fn an_opening_opens_the_next_epoch_and_closes_every_older_one() {
        let fresh = EpochLedger::fresh(StoreIncarnation::new(7));
        let first = fresh.open_next().expect("room");
        assert_eq!(first.open_epoch(), Some(RunEpoch::new(1)));
        assert_eq!(first.incarnation(), StoreIncarnation::new(7));
        // Planning is pure: the ledger it was asked of did not move.
        assert_eq!(fresh.open_epoch(), None);

        let third = first
            .open_next()
            .and_then(|second| second.open_next())
            .expect("room");
        assert_eq!(third.open_epoch(), Some(RunEpoch::new(3)));
        assert_eq!(third.phase_of(RunEpoch::new(0)), EpochPhase::Closed);
        assert_eq!(third.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
        assert_eq!(third.phase_of(RunEpoch::new(2)), EpochPhase::Closed);
        assert_eq!(third.phase_of(RunEpoch::new(3)), EpochPhase::Open);
        assert_eq!(third.phase_of(RunEpoch::new(4)), EpochPhase::Unopened);
    }

    #[test]
    fn exactly_one_epoch_is_open_and_none_opens_twice() {
        let mut ledger = EpochLedger::fresh(StoreIncarnation::new(1));
        let mut opened = Vec::new();
        for _ in 0..6 {
            ledger = ledger.open_next().expect("room");
            let open = ledger.open_epoch().expect("an opening was committed");
            assert!(!opened.contains(&open), "epoch {open} opened twice");
            opened.push(open);
            let open_now = (0..10)
                .filter(|epoch| ledger.phase_of(RunEpoch::new(*epoch)) == EpochPhase::Open)
                .count();
            assert_eq!(open_now, 1);
            // An epoch that was open before is closed and stays closed.
            for older in &opened[..opened.len() - 1] {
                assert_eq!(ledger.phase_of(*older), EpochPhase::Closed);
            }
        }
    }

    #[test]
    fn the_last_epoch_opens_and_the_next_opening_is_exhausted() {
        let before_last =
            EpochLedger::from_durable(StoreIncarnation::new(1), RunEpoch::new(u64::MAX - 1));
        let last = before_last.open_next().expect("the last epoch is usable");
        assert_eq!(last.open_epoch(), Some(RunEpoch::new(u64::MAX)));
        assert_eq!(last.open_next(), Err(Exhausted));
        // The refusal changed nothing and nothing wrapped.
        assert_eq!(last.open_epoch(), Some(RunEpoch::new(u64::MAX)));
        assert_eq!(last.phase_of(RunEpoch::new(0)), EpochPhase::Closed);
        assert_eq!(last.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
    }

    #[test]
    fn the_ledger_round_trips_through_its_durable_values() {
        let ledger = EpochLedger::fresh(StoreIncarnation::new(9))
            .open_next()
            .and_then(|first| first.open_next())
            .expect("room");
        let restored = EpochLedger::from_durable(ledger.incarnation(), ledger.last_opened());
        assert_eq!(restored, ledger);
        let empty = EpochLedger::fresh(StoreIncarnation::new(9));
        assert_eq!(
            EpochLedger::from_durable(empty.incarnation(), empty.last_opened()),
            empty
        );
    }

    #[test]
    fn opening_tickets_never_wrap() {
        assert_eq!(
            OpeningTicket::new(1).checked_next(),
            Ok(OpeningTicket::new(2))
        );
        assert_eq!(OpeningTicket::new(u64::MAX).checked_next(), Err(Exhausted));
    }
}
