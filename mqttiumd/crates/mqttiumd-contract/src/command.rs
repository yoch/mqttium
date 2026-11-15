//! Frozen commands and their exact canonical representation (DEM-009).
//!
//! An `OperationId` designates exactly one recipe. The store keeps the
//! canonical bytes of the recipe and compares them byte for byte when a
//! command is presented again; a digest may speed up a lookup but never
//! replaces that comparison.
//!
//! `OpenEpoch` is the one command that is not a recipe: it opens the run
//! epoch every `OperationId` is scoped by. Its durable state and the
//! decision the writer takes on it are the `EpochLedger`; what start-up
//! does with the reply, and the hand-over of the creation authority, are
//! in `outcome` (ADR 0009).
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
    DeliveryId, ExchangeGeneration, ExchangeId, Exhausted, IdDecodeError, InboundPacketId,
    OperationId, PayloadId, PublicationId, RequestId, Revision, RunEpoch, SessionId,
    StoreIncarnation,
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

/// A value drawn once by each start-up of a process, outside this crate.
///
/// It makes the attempts of one start-up different from those of every
/// other, including a predecessor that died with the same counters. The
/// store adapter draws it (in the deterministic store, from the seed) and
/// that it is not reused is a precondition of that trusted code: this
/// crate draws nothing.
///
/// What the ledger can see of a broken precondition and assert of the
/// command in front of it, it refuses: the opener of the last epoch
/// asking for the next one under the same identity is
/// `OpeningRejection::AttemptReused`, because that command opened
/// nothing and never will. The limits, named:
///
/// - two start-ups with one nonce that read the same value form the
///   very same command, and a command presented again is answered as
///   the one that opened the epoch;
/// - once a later epoch was opened the ledger no longer holds the
///   identity to compare with;
/// - a command under the identity of the opener whose condition is
///   older than the one that opened shows that one identity designates
///   two commands, but not which of the two it is: the command may be
///   the one that opened an epoch since closed. It is `Superseded`, not
///   rejected;
/// - the same with a condition ahead of the ledger is `Undecided`: once
///   another start-up opens an epoch, that command would open the next
///   one, so no ledger can say that it never will.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StartupNonce([u8; 16]);

impl StartupNonce {
    /// Length of the durable encoding.
    pub const ENCODED_LEN: usize = 16;

    /// Wraps the sixteen bytes that were drawn.
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The durable encoding.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// The number of one attempt within one start-up: a checked counter.
/// It is durable only as part of an `OpeningAttempt`, which has the
/// encoding; alone it identifies nothing, which is why it is not one of
/// the identities of `ids`.
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

/// The identity of one attempt to open an epoch: the start-up that made
/// it and its number there. No two attempts share one, across processes
/// and restarts, so the store can tell its own committed command from
/// any other and a reply can name the attempt it answers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct OpeningAttempt {
    pub startup: StartupNonce,
    pub ticket: OpeningTicket,
}

impl OpeningAttempt {
    /// Length of the durable encoding: the sixteen bytes of the nonce,
    /// then the ticket as eight big-endian bytes.
    pub const ENCODED_LEN: usize = 24;

    /// The durable encoding, as the ledger keeps the attempt that
    /// opened the last epoch.
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[..16].copy_from_slice(&self.startup.to_bytes());
        bytes[16..].copy_from_slice(&self.ticket.get().to_be_bytes());
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
        let mut nonce = [0u8; 16];
        nonce.copy_from_slice(&bytes[..16]);
        let mut ticket = [0u8; 8];
        ticket.copy_from_slice(&bytes[16..]);
        Ok(Self {
            startup: StartupNonce::new(nonce),
            ticket: OpeningTicket::new(u64::from_be_bytes(ticket)),
        })
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
///
/// The opening is conditional. Start-up reads the data set it verified
/// and the last epoch opened in it, and the command carries both: the
/// store opens `after + 1` only while `after` is still the last opened
/// epoch. Of two commands that carry the same condition at most one
/// commits, and a command that arrives after any later opening was
/// committed opens nothing, however long it was held on its way.
///
/// Like a recipe, the command is idempotent: presented again while the
/// epoch it opened is still the last one, it gets the same result. And
/// as an `OperationId` designates one recipe, an attempt designates one
/// command: the attempt that opened the last epoch cannot ask for the
/// next one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpenEpoch {
    pub attempt: OpeningAttempt,
    /// The data set start-up verified before it asked.
    pub incarnation: StoreIncarnation,
    /// The last opened epoch start-up read before it asked; zero when
    /// the data set had none.
    pub after: RunEpoch,
}

/// Why an opening was certainly not committed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpeningRejection {
    /// The epoch counter of the data set reached its end. Nothing wraps,
    /// and no later attempt can succeed.
    EpochsExhausted,
    /// The command names another data set than the one the store holds.
    ForeignIncarnation,
    /// Another attempt opened the epoch this one asked for. This attempt
    /// never commits; a new one on the value read again may.
    PredecessorMoved,
    /// The attempt named by this command opened the last epoch with
    /// another command, and this one asks for the next: one identity,
    /// two commands. The precondition of `StartupNonce` is broken. This
    /// command opened nothing and never will, and the start-up that sent
    /// it shares its identity with the opener of the open epoch: it
    /// must stop.
    AttemptReused,
}

impl OpeningRejection {
    /// Whether no later attempt of the same opening can succeed. It is a
    /// property of the reason, not of having been rejected.
    #[must_use]
    pub const fn is_definitive(self) -> bool {
        match self {
            Self::EpochsExhausted | Self::ForeignIncarnation | Self::AttemptReused => true,
            Self::PredecessorMoved => false,
        }
    }
}

/// What the ledger establishes about one `OpenEpoch`. Only `Rejected`
/// says that the command opened nothing and never will.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LedgerOpening {
    /// The command opens the next epoch once this ledger is durable.
    /// Until then nothing is opened: the epoch a store reports is read
    /// from the ledger it wrote (`OpeningOutcome::read`), never from
    /// this decision.
    Opens { ledger: EpochLedger },
    /// The command already opened this epoch, which is still the last
    /// one: the same command presented again. Nothing is to be written.
    AlreadyOpened { epoch: RunEpoch },
    /// The command opened nothing and never will.
    Rejected(OpeningRejection),
    /// The epoch this attempt asked for was opened and is closed. The
    /// ledger keeps the opener of the last epoch only, so it does not
    /// know whether this attempt was the one: it does not call that a
    /// rejection. The attempt can never open an epoch any more.
    Superseded,
    /// The condition is ahead of the ledger: nothing is opened, and the
    /// condition could still come true, so nothing certain is said. A
    /// certain rejection would be contradicted if a held copy of the
    /// command were applied later. No start-up that read this data set
    /// forms such a condition; it arises when a data set was restored to
    /// an earlier state without a new incarnation, which is a failure to
    /// renew the identity spaces and not a case this contract repairs.
    Undecided,
}

/// Durable phase of one run epoch of a data set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EpochPhase {
    /// No committed opening established this epoch. Epoch 0 is never
    /// opened: it stays here whatever the data set went through.
    Unopened,
    /// The one epoch in which commands may execute.
    Open,
    /// It was open and is definitively closed for execution. Closing
    /// forbids execution; it does not turn the unknown results of the
    /// epoch into rejections (V3-006), and its durable effects are still
    /// reconciled (DEM-012).
    Closed,
}

/// Why three durable values are not the ledger of a data set. A store
/// that reads them is corrupt or is not this format: it opens nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LedgerIncoherent {
    /// An epoch is recorded as opened and no attempt as its opener. The
    /// command that did open it could not be recognised: it would be
    /// told that it opened nothing.
    EpochWithoutOpener,
    /// An opener is recorded and no epoch was opened.
    OpenerWithoutEpoch,
}

/// The durable state of the epochs of one data set: its incarnation, the
/// last epoch a committed opening established, and the attempt that
/// opened it.
///
/// Epochs open in increasing order, one at a time. The last opened epoch
/// is the only open one and every epoch opened before it is closed;
/// epoch 0 is never opened and encodes "no epoch yet". An opened epoch
/// always has its opener: the two are one value here, and durable values
/// that separate them are refused by `from_durable`. The same pure
/// decision is used by the deterministic store and by SQLite, which
/// reads the three values, calls `open` with the command and, when it
/// opens, writes the ledger it is given in the same transaction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EpochLedger {
    incarnation: StoreIncarnation,
    /// The last opened epoch, never zero, and the attempt that opened it.
    opened: Option<(RunEpoch, OpeningAttempt)>,
}

impl EpochLedger {
    /// The ledger of a data set in which no epoch was opened yet.
    #[must_use]
    pub const fn fresh(incarnation: StoreIncarnation) -> Self {
        Self {
            incarnation,
            opened: None,
        }
    }

    /// Rebuilds the ledger from its durable values: `last_opened` is
    /// zero and `opened_by` absent when no epoch was opened, and both
    /// are present otherwise. Any other pair is refused.
    pub fn from_durable(
        incarnation: StoreIncarnation,
        last_opened: RunEpoch,
        opened_by: Option<OpeningAttempt>,
    ) -> Result<Self, LedgerIncoherent> {
        let opened = match (last_opened.get(), opened_by) {
            (0, None) => None,
            (0, Some(_)) => return Err(LedgerIncoherent::OpenerWithoutEpoch),
            (_, None) => return Err(LedgerIncoherent::EpochWithoutOpener),
            (_, Some(attempt)) => Some((last_opened, attempt)),
        };
        Ok(Self {
            incarnation,
            opened,
        })
    }

    /// The data set these epochs belong to.
    #[must_use]
    pub const fn incarnation(&self) -> StoreIncarnation {
        self.incarnation
    }

    /// The durable value: the last opened epoch, zero when there is none.
    #[must_use]
    pub const fn last_opened(&self) -> RunEpoch {
        match self.opened {
            Some((epoch, _)) => epoch,
            None => RunEpoch::new(0),
        }
    }

    /// The durable value: the attempt that opened the last epoch.
    #[must_use]
    pub const fn opened_by(&self) -> Option<OpeningAttempt> {
        match self.opened {
            Some((_, attempt)) => Some(attempt),
            None => None,
        }
    }

    /// The one epoch open for execution, if an opening was committed.
    #[must_use]
    pub const fn open_epoch(&self) -> Option<RunEpoch> {
        match self.opened {
            Some((epoch, _)) => Some(epoch),
            None => None,
        }
    }

    /// The durable phase of `epoch`.
    #[must_use]
    pub fn phase_of(&self, epoch: RunEpoch) -> EpochPhase {
        let last_opened = self.last_opened();
        if epoch.get() == 0 || epoch > last_opened {
            EpochPhase::Unopened
        } else if epoch == last_opened {
            EpochPhase::Open
        } else {
            EpochPhase::Closed
        }
    }

    /// Decides what `command` does to this ledger. Pure: the ledger it
    /// is asked of does not move, and nothing wraps.
    #[must_use]
    pub fn open(&self, command: &OpenEpoch) -> LedgerOpening {
        if command.incarnation != self.incarnation {
            return LedgerOpening::Rejected(OpeningRejection::ForeignIncarnation);
        }
        let last_opened = self.last_opened();
        if command.after > last_opened {
            return LedgerOpening::Undecided;
        }
        let by_the_last_opener = self.opened_by() == Some(command.attempt);
        if command.after == last_opened {
            if by_the_last_opener {
                // One identity, two commands: it opened the last epoch
                // and now asks for the next.
                return LedgerOpening::Rejected(OpeningRejection::AttemptReused);
            }
            return match last_opened.checked_next() {
                Ok(epoch) => LedgerOpening::Opens {
                    ledger: Self {
                        incarnation: self.incarnation,
                        opened: Some((epoch, command.attempt)),
                    },
                },
                Err(Exhausted) => LedgerOpening::Rejected(OpeningRejection::EpochsExhausted),
            };
        }
        // The epoch right after the condition was opened. Is it still
        // the last one, and by whom?
        if command.after.checked_next() != Ok(last_opened) {
            return LedgerOpening::Superseded;
        }
        if by_the_last_opener {
            LedgerOpening::AlreadyOpened { epoch: last_opened }
        } else {
            LedgerOpening::Rejected(OpeningRejection::PredecessorMoved)
        }
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

    const FIRST_START: StartupNonce = StartupNonce::new([1; 16]);
    const RESTART: StartupNonce = StartupNonce::new([2; 16]);

    fn attempt(startup: StartupNonce, ticket: u64) -> OpeningAttempt {
        OpeningAttempt {
            startup,
            ticket: OpeningTicket::new(ticket),
        }
    }

    /// The command a start-up that just read `ledger` would send.
    fn opening(ledger: &EpochLedger, ticket: u64) -> OpenEpoch {
        OpenEpoch {
            attempt: attempt(FIRST_START, ticket),
            incarnation: ledger.incarnation(),
            after: ledger.last_opened(),
        }
    }

    fn opened_by(ledger: &EpochLedger, command: &OpenEpoch) -> EpochLedger {
        match ledger.open(command) {
            LedgerOpening::Opens { ledger: next } => {
                assert_eq!(Ok(next.last_opened()), command.after.checked_next());
                next
            }
            other => panic!("not opened: {other:?}"),
        }
    }

    /// One more opening, by a start-up of its own: each start-up of
    /// these tests opens one epoch, as a real one does.
    fn opened(ledger: &EpochLedger) -> EpochLedger {
        let rank = ledger.last_opened().get() as u8;
        let command = OpenEpoch {
            attempt: attempt(StartupNonce::new([0x80 | rank; 16]), 1),
            ..opening(ledger, 1)
        };
        opened_by(ledger, &command)
    }

    /// A ledger whose last opened epoch is `epoch`, as a store holds it.
    fn at(epoch: u64) -> EpochLedger {
        EpochLedger::from_durable(
            StoreIncarnation::new(1),
            RunEpoch::new(epoch),
            Some(attempt(StartupNonce::new([0x70; 16]), 1)),
        )
        .expect("an epoch and its opener")
    }

    const MOVED: LedgerOpening = LedgerOpening::Rejected(OpeningRejection::PredecessorMoved);
    const ALREADY_2: LedgerOpening = LedgerOpening::AlreadyOpened {
        epoch: RunEpoch::new(2),
    };

    #[test]
    fn a_fresh_ledger_has_no_open_epoch() {
        let ledger = EpochLedger::fresh(StoreIncarnation::new(7));
        assert_eq!(ledger.incarnation(), StoreIncarnation::new(7));
        assert_eq!(ledger.open_epoch(), None);
        assert_eq!(ledger.last_opened(), RunEpoch::new(0));
        assert_eq!(ledger.opened_by(), None);
        for epoch in [0, 1, 2, u64::MAX] {
            assert_eq!(ledger.phase_of(RunEpoch::new(epoch)), EpochPhase::Unopened);
        }
    }

    #[test]
    fn an_opening_opens_the_next_epoch_and_closes_every_older_one() {
        let fresh = EpochLedger::fresh(StoreIncarnation::new(7));
        let command = opening(&fresh, 1);
        let first = opened_by(&fresh, &command);
        assert_eq!(first.open_epoch(), Some(RunEpoch::new(1)));
        assert_eq!(first.incarnation(), StoreIncarnation::new(7));
        assert_eq!(first.opened_by(), Some(command.attempt));
        // The decision is pure: the ledger it was asked of did not move.
        assert_eq!(fresh.open_epoch(), None);

        let third = opened(&opened(&first));
        assert_eq!(third.open_epoch(), Some(RunEpoch::new(3)));
        assert_eq!(third.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
        assert_eq!(third.phase_of(RunEpoch::new(2)), EpochPhase::Closed);
        assert_eq!(third.phase_of(RunEpoch::new(3)), EpochPhase::Open);
        assert_eq!(third.phase_of(RunEpoch::new(4)), EpochPhase::Unopened);
    }

    #[test]
    fn epoch_zero_is_never_open_and_never_closed() {
        let mut ledger = EpochLedger::fresh(StoreIncarnation::new(7));
        for _ in 0..4 {
            assert_eq!(ledger.phase_of(RunEpoch::new(0)), EpochPhase::Unopened);
            ledger = opened(&ledger);
        }
        assert_eq!(ledger.phase_of(RunEpoch::new(0)), EpochPhase::Unopened);
        assert_eq!(
            at(u64::MAX).phase_of(RunEpoch::new(0)),
            EpochPhase::Unopened
        );
    }

    #[test]
    fn exactly_one_epoch_is_open_and_none_opens_twice() {
        let mut ledger = EpochLedger::fresh(StoreIncarnation::new(1));
        let mut seen = Vec::new();
        for _ in 0..6 {
            ledger = opened(&ledger);
            let open = ledger.open_epoch().expect("an opening was committed");
            assert!(!seen.contains(&open), "epoch {open} opened twice");
            seen.push(open);
            let open_now = (0..10)
                .filter(|epoch| ledger.phase_of(RunEpoch::new(*epoch)) == EpochPhase::Open)
                .count();
            assert_eq!(open_now, 1);
            // An epoch that was open before is closed and stays closed.
            for older in &seen[..seen.len() - 1] {
                assert_eq!(ledger.phase_of(*older), EpochPhase::Closed);
            }
        }
    }

    /// An epoch that is opened has an opener. Durable values that say
    /// otherwise are not a ledger: with no opener recorded, the command
    /// that did open the last epoch would be told it opened nothing.
    #[test]
    fn durable_values_without_an_opener_for_their_epoch_are_not_a_ledger() {
        let incarnation = StoreIncarnation::new(1);
        let opener = attempt(FIRST_START, 1);
        assert_eq!(
            EpochLedger::from_durable(incarnation, RunEpoch::new(0), None),
            Ok(EpochLedger::fresh(incarnation))
        );
        for epoch in [1, 5, u64::MAX] {
            assert_eq!(
                EpochLedger::from_durable(incarnation, RunEpoch::new(epoch), None),
                Err(LedgerIncoherent::EpochWithoutOpener),
                "epoch {epoch}"
            );
            let ledger = EpochLedger::from_durable(incarnation, RunEpoch::new(epoch), Some(opener))
                .expect("an epoch and its opener");
            assert_eq!(ledger.last_opened(), RunEpoch::new(epoch));
            assert_eq!(ledger.opened_by(), Some(opener));
        }
        assert_eq!(
            EpochLedger::from_durable(incarnation, RunEpoch::new(0), Some(opener)),
            Err(LedgerIncoherent::OpenerWithoutEpoch)
        );
        // Every ledger an opening produces survives its durable values.
        let mut ledger = EpochLedger::fresh(incarnation);
        for _ in 0..3 {
            ledger = opened(&ledger);
            assert_eq!(
                EpochLedger::from_durable(
                    ledger.incarnation(),
                    ledger.last_opened(),
                    ledger.opened_by()
                ),
                Ok(ledger)
            );
        }
    }

    /// Two attempts read the same ledger. Whichever the writer applies
    /// first commits; the other one is certainly rejected, in both
    /// orders, and so never closes the epoch the first one opened.
    #[test]
    fn of_two_openings_with_one_condition_at_most_one_commits() {
        let read = at(1);
        let older = opening(&read, 2);
        let newer = opening(&read, 3);
        for (first, second) in [(older, newer), (newer, older)] {
            let committed = opened_by(&read, &first);
            assert_eq!(committed.open_epoch(), Some(RunEpoch::new(2)));
            assert_eq!(committed.open(&second), MOVED);
        }
    }

    /// The command that opened the last epoch, presented again, gets
    /// its own result and changes nothing: "rejected" would say that it
    /// opened nothing.
    #[test]
    fn the_command_that_opened_the_last_epoch_is_recognised_again() {
        let read = at(1);
        let command = opening(&read, 2);
        let committed = opened_by(&read, &command);
        for _delivery in 0..3 {
            assert_eq!(committed.open(&command), ALREADY_2);
        }
        assert_eq!(committed.last_opened(), RunEpoch::new(2));
    }

    /// A restarted process starts its tickets again at one and reads the
    /// same condition as its dead predecessor. Their commands differ by
    /// the start-up that made them, and the ledger tells them apart.
    #[test]
    fn the_same_ticket_of_another_start_up_is_another_attempt() {
        let read = at(1);
        let predecessor = opening(&read, 1);
        let successor = OpenEpoch {
            attempt: attempt(RESTART, 1),
            ..predecessor
        };
        assert_ne!(predecessor, successor);
        for (first, second) in [(predecessor, successor), (successor, predecessor)] {
            let committed = opened_by(&read, &first);
            assert_eq!(committed.opened_by(), Some(first.attempt));
            assert_eq!(committed.open(&second), MOVED);
            assert_eq!(committed.open(&first), ALREADY_2);
        }
    }

    /// The precondition of the nonce, where the ledger can see it
    /// broken. A start-up that drew the nonce of the one before reads
    /// the epoch that one opened and asks for the next under the same
    /// identity: one attempt, two commands. Nothing is opened.
    #[test]
    fn the_opener_of_the_last_epoch_cannot_open_the_next_under_the_same_identity() {
        let read = at(3);
        let predecessor = opening(&read, 1);
        let committed = opened_by(&read, &predecessor);
        let successor = opening(&committed, 1);
        assert_eq!(successor.attempt, predecessor.attempt);
        assert_ne!(successor, predecessor);
        assert_eq!(
            committed.open(&successor),
            LedgerOpening::Rejected(OpeningRejection::AttemptReused)
        );
        // Nothing moved, and no other reason hides this one: at the end
        // of the epochs the reused identity is still what is reported.
        assert_eq!(committed.last_opened(), RunEpoch::new(4));
        let last = opened_by(&at(u64::MAX - 1), &opening(&at(u64::MAX - 1), 1));
        assert_eq!(
            last.open(&opening(&last, 1)),
            LedgerOpening::Rejected(OpeningRejection::AttemptReused)
        );
        // Any other identity opens it, and the first command is still
        // answered as the one that opened epoch 4.
        assert_eq!(
            committed.open(&predecessor),
            LedgerOpening::AlreadyOpened {
                epoch: RunEpoch::new(4)
            }
        );
        let next = opening(&committed, 2);
        assert_eq!(
            opened_by(&committed, &next).open_epoch(),
            Some(RunEpoch::new(5))
        );
    }

    /// Where the ledger sees one identity under two commands and cannot
    /// reject. A command under the identity of the opener, with a
    /// condition ahead of the ledger, would open an epoch once another
    /// start-up opened the one before: a rejection now would be followed
    /// by its commit. With a condition older than the one that opened,
    /// it may be the command that opened an epoch since closed: a
    /// rejection would deny what it did.
    #[test]
    fn a_reused_identity_is_rejected_only_where_the_command_can_never_have_opened() {
        let read = at(4);
        let opener = opening(&read, 1);
        let at_five = opened_by(&read, &opener);
        let ahead = OpenEpoch {
            after: RunEpoch::new(6),
            ..opener
        };
        assert_eq!(at_five.open(&ahead), LedgerOpening::Undecided);
        // Another start-up opens epoch 6: the same command opens 7.
        let at_six = opened(&at_five);
        let at_seven = opened_by(&at_six, &ahead);
        assert_eq!(at_seven.open_epoch(), Some(RunEpoch::new(7)));

        // `opener` did open epoch 5. Now the ledger holds its identity
        // as the opener of epoch 7, with another command.
        assert_eq!(at_seven.opened_by(), Some(opener.attempt));
        assert_eq!(at_seven.open(&opener), LedgerOpening::Superseded);
    }

    /// Once a later opening was committed the ledger no longer knows
    /// who opened the older epoch. It says that the attempt is
    /// superseded, never that it opened nothing.
    #[test]
    fn an_attempt_for_an_epoch_already_closed_is_superseded_not_rejected() {
        let read = at(1);
        let opener = opening(&read, 2);
        let rival = opening(&read, 3);
        let later = opened(&opened_by(&read, &opener));
        assert_eq!(later.last_opened(), RunEpoch::new(3));
        for command in [opener, rival] {
            assert_eq!(later.open(&command), LedgerOpening::Superseded);
        }
        assert_eq!(later.last_opened(), RunEpoch::new(3));
    }

    #[test]
    fn a_command_for_another_data_set_or_ahead_of_the_ledger_opens_nothing() {
        let at_three = at(3);
        let other_data_set = OpenEpoch {
            incarnation: StoreIncarnation::new(2),
            ..opening(&at_three, 1)
        };
        assert_eq!(
            at_three.open(&other_data_set),
            LedgerOpening::Rejected(OpeningRejection::ForeignIncarnation)
        );
        // A condition the ledger has not reached could still come true:
        // nothing certain is said about it.
        for ahead in [4, u64::MAX] {
            let command = OpenEpoch {
                after: RunEpoch::new(ahead),
                ..opening(&at_three, 1)
            };
            assert_eq!(at_three.open(&command), LedgerOpening::Undecided);
        }
        assert_eq!(opened(&at_three).open_epoch(), Some(RunEpoch::new(4)));
    }

    #[test]
    fn the_last_epoch_opens_and_the_next_opening_is_exhausted() {
        let last = opened(&at(u64::MAX - 1));
        assert_eq!(last.open_epoch(), Some(RunEpoch::new(u64::MAX)));
        assert_eq!(
            last.open(&opening(&last, 2)),
            LedgerOpening::Rejected(OpeningRejection::EpochsExhausted)
        );
        // The refusal changed nothing and nothing wrapped.
        assert_eq!(last.open_epoch(), Some(RunEpoch::new(u64::MAX)));
        assert_eq!(last.phase_of(RunEpoch::new(1)), EpochPhase::Closed);
    }

    #[test]
    fn only_a_moved_predecessor_leaves_room_for_a_new_attempt() {
        assert!(OpeningRejection::EpochsExhausted.is_definitive());
        assert!(OpeningRejection::ForeignIncarnation.is_definitive());
        assert!(OpeningRejection::AttemptReused.is_definitive());
        assert!(!OpeningRejection::PredecessorMoved.is_definitive());
    }

    #[test]
    fn an_attempt_round_trips_through_its_durable_encoding() {
        let nonce: [u8; 16] = core::array::from_fn(|index| index as u8 + 1);
        let attempts = [
            OpeningAttempt {
                startup: StartupNonce::new(nonce),
                ticket: OpeningTicket::new(0x0102_0304_0506_0708),
            },
            attempt(FIRST_START, 1),
            attempt(RESTART, u64::MAX),
            attempt(StartupNonce::new([0; 16]), 0),
        ];
        for attempt in attempts {
            let bytes = attempt.to_be_bytes();
            assert_eq!(bytes.len(), OpeningAttempt::ENCODED_LEN);
            assert_eq!(OpeningAttempt::from_be_slice(&bytes), Ok(attempt));
        }
        // The layout is pinned: the nonce as drawn, then the ticket.
        let bytes = attempts[0].to_be_bytes();
        assert_eq!(&bytes[..16], &nonce);
        assert_eq!(&bytes[16..], &[1, 2, 3, 4, 5, 6, 7, 8]);
        // Two attempts differ exactly when their encodings do.
        for left in attempts {
            for right in attempts {
                assert_eq!(left == right, left.to_be_bytes() == right.to_be_bytes());
            }
        }
        for length in [0, 23, 25] {
            assert_eq!(
                OpeningAttempt::from_be_slice(&vec![0; length]),
                Err(IdDecodeError::WrongLength {
                    expected: 24,
                    actual: length
                })
            );
        }
    }

    /// The ledger is three durable values, and the attempt among them
    /// survives its encoding: the command that opened the last epoch is
    /// still recognised after the store was closed and reopened.
    #[test]
    fn the_opener_is_recognised_from_its_encoded_form() {
        let read = at(1);
        let command = opening(&read, 2);
        let committed = opened_by(&read, &command);
        let stored = committed.opened_by().map(OpeningAttempt::to_be_bytes);
        let decoded = stored.map(|bytes| OpeningAttempt::from_be_slice(&bytes).expect("stored"));
        let reopened =
            EpochLedger::from_durable(committed.incarnation(), committed.last_opened(), decoded);
        assert_eq!(reopened, Ok(committed));
        assert_eq!(reopened.map(|ledger| ledger.open(&command)), Ok(ALREADY_2));
    }

    #[test]
    fn opening_tickets_never_wrap() {
        assert_eq!(
            OpeningTicket::new(1).checked_next(),
            Ok(OpeningTicket::new(2))
        );
        assert_eq!(OpeningTicket::new(u64::MAX).checked_next(), Err(Exhausted));
        assert_eq!(StartupNonce::new([9; 16]).to_bytes(), [9; 16]);
    }
}
