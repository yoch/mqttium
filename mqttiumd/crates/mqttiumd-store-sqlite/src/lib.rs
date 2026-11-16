//! The SQLite store of mqttiumd (lot L7).
//!
//! One file, one connection that is the only writer, WAL and
//! `synchronous=FULL`, the values read back after they are set and the
//! opening refused when they differ (DEM-011). The engine is the SQLite
//! amalgamation compiled into the binding; its version and compile
//! options are read from the engine itself and kept with the settings.
//!
//! Every command is one `BEGIN IMMEDIATE` transaction on the real tables:
//! each guard is a read in it, each row a write in it, and a guard that
//! fails rolls everything back, so no subset of an admission is ever
//! admitted. The decisions are those of the contract and of the
//! deterministic store: `EpochLedger::open` decides an opening and
//! `OpeningOutcome::read` answers it from the ledger that is durable
//! after the write; a rejection is recorded on a receipt that holds a
//! receipt slot, and without one nothing certain is announced.
//!
//! A commit that fails is reported as not written. SQLite rolls back a
//! transaction whose commit fails; the answer is then unknown, never a
//! certain rejection, and the next command reads the durable state again.
//!
//! # The writer's lock
//!
//! `BEGIN IMMEDIATE` with no busy timeout serialises transactions; it does
//! not make one process the owner of the store for its whole life. That is
//! the writer's lock: an exclusive, non-blocking advisory lock
//! (`File::try_lock`, `flock` on Linux) on a file beside the database,
//! `<database>.writer-lock`. `SqliteStore::create` and `SqliteStore::open`
//! take it before they touch the database and hold it for the whole life
//! of the `SqliteStore`; it is released when the store is dropped, after
//! its connection is closed, and by the kernel when the process dies, a
//! `SIGKILL` included. A second writer while the first lives is refused at
//! once with `OpenError::WriterAlive`, from this process or another.
//! `SqliteStore::inspect` does not take it, and the database itself is not
//! locked beyond what SQLite does: `locking_mode` stays `NORMAL`, so an
//! inspector reads while the writer lives.
//!
//! The lock file is never removed by the store: a removal would let two
//! writers lock two different files of the same name. Its existence alone
//! means nothing; only the lock held on it does. What the lock does not
//! guarantee: an advisory lock does not stop a program that ignores it
//! (another SQLite client opening the database directly), and file
//! systems over a network, where `flock` may not be shared between hosts,
//! are outside its perimeter.

#![forbid(unsafe_code)]

mod codec;
mod pubsub;
mod reader;
mod receipts;
mod schema;
mod sessions;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, Command, EpochLedger, EpochPhase, LedgerOpening, OpenEpoch, Recipe,
    RecipeError, ReleaseInbound,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, InboundPacketId, OperationId, OperationSeq, OutboundPacketId, PayloadId,
    PublicationId, Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_store_api::{
    Accounts, AdminCommand, AdminDone, AdminRefused, Applied, Decision, Durability, DurableState,
    DurableStore, InboundRecord, Limits, MAX_CLIENT_ID_BYTES, MAX_RECOVERY_INVENTORY_ROWS,
    ObligationRecord, OperationRecord, OutboundRecord, PayloadRecord, PublicationRecord,
    ReadBudget, ReadError, RecoveryCursor, RecoveryPage, RecoveryRefused, Resolved, RouteTarget,
    SessionRecord, SessionState, WorkPage,
};

use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptClass, ReceiptClosure, ReceiptCollection,
    ReceiptCollectionReply, ReceiptOrigin,
};

use codec::{
    Corrupt, attempt_blob, blob_attempt, blob_exchange, blob_u64, blob_usize, cause_code,
    code_cause, code_inbound_phase, code_obligation_phase, code_qos, code_session_state,
    codes_reason, exchange_blob, inbound_phase_code, obligation_phase_code, qos_code, reason_codes,
    session_state_code, u64_blob, usize_blob,
};

pub use schema::{FORMAT, SCHEMA_VERSION};

/// Whether this compiled store carries test-only transaction kill points.
/// The bench checks this before writing a campaign, including when Cargo
/// unified a transitive request for the store feature into its normal build.
pub const KILL_POINTS_ENABLED: bool = cfg!(feature = "kill-points");

/// The points of the store at which the bench may kill its process, in
/// a build with the feature `kill-points` only (record 0002, plan
/// `docs/missions/L7-kill-inside-a-transaction.md`). The product is never
/// built with it: `effective_settings().kill_points` says which build
/// runs, and `tools/check_kill_points.sh` refuses a product build that
/// carries it.
#[cfg(feature = "kill-points")]
pub mod kill_points {
    /// A point inside the write path of a command.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum KillPoint {
        /// `store.commit.prepared`: every guard read and every row written,
        /// the meta row one revision later, `COMMIT` not called yet. The
        /// transaction is open and holds the engine's write lock.
        CommitPrepared,
        /// `store.commit.logged`: the engine's WAL hook after the commit
        /// and after release of SQLite's write lock. With WAL and FULL,
        /// the commit is durable; `COMMIT` has not returned to the adapter.
        CommitLogged,
    }

    /// Called at each point the store passes. It may block, for the bench
    /// to kill the process there; it must not call the store.
    pub type Hook = Box<dyn FnMut(KillPoint) + Send>;

    /// The hook, shared with the engine's WAL hook.
    pub(crate) type Shared = std::sync::Arc<std::sync::Mutex<Option<Hook>>>;

    /// Calls the hook, if one is set, at `point`.
    pub(crate) fn pass(shared: &Shared, point: KillPoint) {
        if let Ok(mut hook) = shared.lock()
            && let Some(hook) = hook.as_mut()
        {
            hook(point);
        }
    }
}

/// Test-only receipt-control barriers, distinct from command kill points.
#[cfg(feature = "kill-points")]
pub mod receipt_points {
    use mqttiumd_contract::receipts::ReceiptCollection;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum ReceiptPoint {
        ClosePrepared,
        CloseCommitted,
    }

    impl ReceiptPoint {
        #[must_use]
        pub const fn name(self) -> &'static str {
            match self {
                Self::ClosePrepared => "receipt.close.prepared",
                Self::CloseCommitted => "receipt.close.committed",
            }
        }
    }

    pub type Hook = Box<dyn FnMut(ReceiptPoint, ReceiptCollection) + Send>;
    pub(crate) type Shared = std::sync::Arc<std::sync::Mutex<Option<Hook>>>;
    pub(crate) fn pass(shared: &Shared, point: ReceiptPoint, collection: ReceiptCollection) {
        if let Ok(mut hook) = shared.lock()
            && let Some(hook) = hook.as_mut()
        {
            hook(point, collection);
        }
    }
}

/// What the engine says about itself and about the settings in force,
/// read back after they were set (DEM-011).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Settings {
    /// `sqlite_version()`.
    pub sqlite_version: String,
    /// `PRAGMA compile_options`, in the engine's order.
    pub compile_options: Vec<String>,
    /// `PRAGMA journal_mode`, read back: `wal`.
    pub journal_mode: String,
    /// `PRAGMA synchronous`, read back: 2, which is FULL.
    pub synchronous: i64,
    /// `PRAGMA user_version`: the schema version of the file.
    pub user_version: i64,
    /// `PRAGMA page_size`, in bytes.
    pub page_size: i64,
    /// `PRAGMA wal_autocheckpoint`, in pages: the engine's checkpoint of
    /// the WAL into the database. Commits are not grouped otherwise.
    pub wal_autocheckpoint: i64,
    /// Who, if anyone, checkpoints the WAL on this connection. Normal
    /// builds use `sqlite-autocheckpoint`; an armed kill hook uses
    /// `disabled-while-kill-hook-armed`, with no checkpoint in its callback.
    pub checkpoint_policy: &'static str,
    /// Whether this build carries the kill points of the bench (feature
    /// `kill-points`). A build of the product must say false.
    pub kill_points: bool,
}

/// Why a file was not opened as a store.
#[derive(Debug)]
pub enum OpenError {
    /// `create` was given a file that already holds a database.
    NotEmpty,
    /// The schema version of the file is not this store's: an older or a
    /// future format, refused before anything is written.
    SchemaVersion(i64),
    /// The file names another format, or a durable value does not decode.
    Corrupt,
    /// A bounded semantic read refused the file without exposing its value.
    Read(ReadError),
    /// `PRAGMA quick_check` found the file damaged; its first lines.
    Integrity(String),
    /// A setting read back is not the one that was set.
    Setting {
        name: &'static str,
        read_back: String,
    },
    Sql(rusqlite::Error),
    /// Another `SqliteStore` holds the writer's lock of this file, in this
    /// process or another: refused at once, without waiting.
    WriterAlive,
    /// The lock file could not be opened, or the lock could not be asked
    /// for (not that it is held: that is `WriterAlive`).
    WriterLock(std::io::Error),
}

impl OpenError {
    /// Stable behaviour code, without parsing driver text or copying a value.
    #[must_use]
    pub fn read_error(&self) -> ReadError {
        match self {
            Self::Read(error) => *error,
            Self::Sql(error) => reader::sql_error(error),
            Self::WriterAlive => ReadError::Unavailable,
            Self::WriterLock(error) => reader::io_error(error),
            _ => ReadError::InvalidData,
        }
    }
}

impl From<rusqlite::Error> for OpenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<Corrupt> for OpenError {
    fn from(_: Corrupt) -> Self {
        Self::Corrupt
    }
}

/// A transaction could not go on: an SQL error or a value that does not
/// decode. Nothing it prepared is written.
#[derive(Debug)]
enum Fault {
    Sql(rusqlite::Error),
    Corrupt,
    Read(ReadError),
}

impl Fault {
    fn read_error(&self) -> ReadError {
        match self {
            Self::Sql(error) => reader::sql_error(error),
            Self::Corrupt => ReadError::InvalidData,
            Self::Read(error) => *error,
        }
    }
}

impl From<ReadError> for Fault {
    fn from(error: ReadError) -> Self {
        Self::Read(error)
    }
}

impl From<rusqlite::Error> for Fault {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<Corrupt> for Fault {
    fn from(_: Corrupt) -> Self {
        Self::Corrupt
    }
}

/// Why a transaction stopped before its commit: a guard refused it, or it
/// could not go on.
enum Abort<E> {
    Refused(E),
    /// Preserve durable-read causes through controls such as epoch opening.
    Read(ReadError),
    Fault,
}

impl<E> From<rusqlite::Error> for Abort<E> {
    fn from(error: rusqlite::Error) -> Self {
        Self::Read(reader::sql_error(&error))
    }
}

impl<E> From<Corrupt> for Abort<E> {
    fn from(_: Corrupt) -> Self {
        Self::Read(ReadError::InvalidData)
    }
}

impl<E> From<ReadError> for Abort<E> {
    fn from(error: ReadError) -> Self {
        Self::Read(error)
    }
}

impl<E> From<Fault> for Abort<E> {
    fn from(error: Fault) -> Self {
        Self::Read(error.read_error())
    }
}

/// What a transaction did.
enum Txn<T, E> {
    Committed(T),
    /// A guard refused it; it was rolled back.
    Refused(E),
    /// A metadata read failed before the transaction could be prepared.
    ReadFailed(ReadError),
    /// It was not written: rolled back, or its commit failed.
    NotWritten,
}

/// The single row of `meta`, decoded.
#[derive(Clone, Copy, Debug)]
struct Meta {
    ledger: EpochLedger,
    collected_through: RunEpoch,
    revision: u64,
    limits: Limits,
    accounts: Accounts,
    next_session: SessionId,
    route_revision: Revision,
}

type Guard<T> = Result<T, Abort<RejectReason>>;

/// Builds one recovered fact from a row; an inner error is a fact that
/// cannot be built within its budget, in its place.
type BuildFact<'a> =
    dyn FnMut(&rusqlite::Row<'_>) -> Result<Result<RecoveredFact, RecoveryRefused>, Fault> + 'a;

fn refused<T>(reason: RejectReason) -> Guard<T> {
    Err(Abort::Refused(reason))
}

fn blob(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Vec<u8>> {
    reader::copy_blob(row, index, ReadBudget::SNAPSHOT.max_field_bytes)
}

fn read_meta(conn: &Connection) -> Result<Meta, Fault> {
    #[cfg(test)]
    if let Some(error) = transaction_read_tests::take_if_transaction(conn) {
        return Err(Fault::Read(error));
    }
    let mut statement = conn.prepare_cached(
        "SELECT format, incarnation, last_opened, opened_by, collected_through, revision,
                next_session, route_revision,
                limit_payload_bytes, limit_references, limit_exchange_slots,
                limit_receipt_slots, max_targets_per_admission, max_topic_bytes,
                max_payload_bytes, max_sessions, max_route_targets,
                used_payload_bytes, used_references, used_exchange_slots, used_receipt_slots
           FROM meta WHERE one = 1",
    )?;
    let mut rows = statement.query([])?;
    let row = rows.next()?.ok_or(Fault::Corrupt)?;
    let format = match row.get_ref(0)? {
        rusqlite::types::ValueRef::Text(bytes) if bytes == FORMAT.as_bytes() => FORMAT,
        _ => return Err(Fault::Corrupt),
    };
    let mut b: [&[u8]; 20] = [&[]; 20];
    for index in 1..21 {
        if index == 3 && matches!(row.get_ref(index)?, rusqlite::types::ValueRef::Null) {
            continue;
        }
        b[index - 1] = reader::borrow_blob(row, index, if index == 3 { 24 } else { 8 })?;
    }
    if format != FORMAT {
        return Err(Fault::Corrupt);
    }
    let opened_by = if b[2].is_empty() {
        None
    } else {
        Some(blob_attempt(b[2])?)
    };
    let ledger = EpochLedger::from_durable(
        StoreIncarnation::new(blob_u64(b[0])?),
        RunEpoch::new(blob_u64(b[1])?),
        opened_by,
    )
    .map_err(|_| Fault::Corrupt)?;
    Ok(Meta {
        ledger,
        collected_through: RunEpoch::new(blob_u64(b[3])?),
        revision: blob_u64(b[4])?,
        next_session: SessionId::new(blob_u64(b[5])?),
        route_revision: Revision::new(blob_u64(b[6])?),
        limits: Limits {
            payload_bytes: PayloadBytes::new(blob_u64(b[7])?),
            references: References::new(blob_u64(b[8])?),
            exchange_slots: ExchangeSlots::new(blob_u64(b[9])?),
            receipt_slots: ReceiptSlots::new(blob_u64(b[10])?),
            max_targets_per_admission: blob_usize(b[11])?,
            max_topic_bytes: blob_usize(b[12])?,
            max_payload_bytes: blob_usize(b[13])?,
            max_sessions: blob_usize(b[14])?,
            max_route_targets: blob_usize(b[15])?,
        },
        accounts: Accounts {
            payload_bytes: PayloadBytes::new(blob_u64(b[16])?),
            references: References::new(blob_u64(b[17])?),
            exchange_slots: ExchangeSlots::new(blob_u64(b[18])?),
            receipt_slots: ReceiptSlots::new(blob_u64(b[19])?),
        },
    })
}

fn write_meta(conn: &Connection, meta: &Meta) -> Result<(), Fault> {
    let limits = meta.limits;
    let accounts = meta.accounts;
    let opened_by = meta.ledger.opened_by().map(attempt_blob);
    let changed = conn
        .prepare_cached(
            "UPDATE meta SET incarnation = ?1, last_opened = ?2, opened_by = ?3,
                collected_through = ?4, revision = ?5, next_session = ?6, route_revision = ?7,
                limit_payload_bytes = ?8, limit_references = ?9, limit_exchange_slots = ?10,
                limit_receipt_slots = ?11, max_targets_per_admission = ?12,
                max_topic_bytes = ?13, max_payload_bytes = ?14, max_sessions = ?15,
                max_route_targets = ?16, used_payload_bytes = ?17, used_references = ?18,
                used_exchange_slots = ?19, used_receipt_slots = ?20
          WHERE one = 1",
        )?
        .execute(params![
            u64_blob(meta.ledger.incarnation().get()),
            u64_blob(meta.ledger.last_opened().get()),
            opened_by.as_ref().map(<[u8; 24]>::as_slice),
            u64_blob(meta.collected_through.get()),
            u64_blob(meta.revision),
            u64_blob(meta.next_session.get()),
            u64_blob(meta.route_revision.get()),
            u64_blob(limits.payload_bytes.get()),
            u64_blob(limits.references.get()),
            u64_blob(limits.exchange_slots.get()),
            u64_blob(limits.receipt_slots.get()),
            usize_blob(limits.max_targets_per_admission)?,
            usize_blob(limits.max_topic_bytes)?,
            usize_blob(limits.max_payload_bytes)?,
            usize_blob(limits.max_sessions)?,
            usize_blob(limits.max_route_targets)?,
            u64_blob(accounts.payload_bytes.get()),
            u64_blob(accounts.references.get()),
            u64_blob(accounts.exchange_slots.get()),
            u64_blob(accounts.receipt_slots.get()),
        ])?;
    if changed == 1 {
        Ok(())
    } else {
        Err(Fault::Corrupt)
    }
}

/// Charges one account against its limit, or names the resource that is
/// short. The same order as the deterministic store.
fn charge(meta: &mut Meta, costs: Accounts) -> Guard<()> {
    let limits = meta.limits;
    let used = meta.accounts;
    let short = |resource| Abort::Refused(RejectReason::CapacityExceeded(resource));
    let payload_bytes = used
        .payload_bytes
        .checked_add(costs.payload_bytes)
        .filter(|total| *total <= limits.payload_bytes)
        .ok_or_else(|| short(Resource::PayloadBytes))?;
    let references = used
        .references
        .checked_add(costs.references)
        .filter(|total| *total <= limits.references)
        .ok_or_else(|| short(Resource::References))?;
    let exchange_slots = used
        .exchange_slots
        .checked_add(costs.exchange_slots)
        .filter(|total| *total <= limits.exchange_slots)
        .ok_or_else(|| short(Resource::ExchangeSlots))?;
    let receipt_slots = used
        .receipt_slots
        .checked_add(costs.receipt_slots)
        .filter(|total| *total <= limits.receipt_slots)
        .ok_or_else(|| short(Resource::ReceiptSlots))?;
    meta.accounts = Accounts {
        payload_bytes,
        references,
        exchange_slots,
        receipt_slots,
    };
    Ok(())
}

/// What is already established about an operation, before any guard.
enum Established {
    Outcome(StoreOutcome),
    Absent(EpochPhase),
}

fn decision_of(row: &rusqlite::Row<'_>, first: usize) -> rusqlite::Result<DecisionColumns> {
    Ok(DecisionColumns {
        decision: row.get(first)?,
        publication: reader::optional_blob(row, first + 1, 32)?,
        exchange: reader::optional_blob(row, first + 2, 16)?,
        reason: row.get(first + 3)?,
        resource: row.get(first + 4)?,
        facts: reader::optional_blob(row, first + 5, 96)?,
    })
}

struct DecisionColumns {
    decision: i64,
    publication: Option<Vec<u8>>,
    exchange: Option<Vec<u8>>,
    reason: Option<i64>,
    resource: Option<i64>,
    facts: Option<Vec<u8>>,
}

const DECISION_ADMITTED: i64 = 1;
const DECISION_RELEASED: i64 = 2;
const DECISION_REJECTED: i64 = 3;

impl DecisionColumns {
    fn decode(&self) -> Result<Decision, Corrupt> {
        let exchange = || blob_exchange(self.exchange.as_deref().ok_or(Corrupt)?);
        match self.decision {
            DECISION_ADMITTED => Ok(Decision::Committed(CommitFacts::PublicationAdmitted {
                publication: PublicationId::from_be_slice(
                    self.publication.as_deref().ok_or(Corrupt)?,
                )
                .map_err(|_| Corrupt)?,
                exchange: exchange()?,
            })),
            DECISION_RELEASED => Ok(Decision::Committed(CommitFacts::InboundReleased {
                exchange: exchange()?,
            })),
            DECISION_REJECTED => Ok(Decision::Rejected(codes_reason(
                self.reason.ok_or(Corrupt)?,
                self.resource,
            )?)),
            4..=8 => Ok(Decision::Committed(pubsub::decode_facts(
                self.decision,
                self.facts.as_deref().ok_or(Corrupt)?,
            )?)),
            _ => Err(Corrupt),
        }
    }
}

fn insert_operation(
    conn: &Connection,
    operation: OperationId,
    recipe: &[u8],
    decision: Decision,
) -> Result<(), Fault> {
    let (code, publication, exchange, reason, resource, facts) = match decision {
        Decision::Committed(CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        }) => (
            DECISION_ADMITTED,
            Some(publication.to_be_bytes().to_vec()),
            Some(exchange_blob(exchange).to_vec()),
            None,
            None,
            None,
        ),
        Decision::Committed(CommitFacts::InboundReleased { exchange }) => (
            DECISION_RELEASED,
            None,
            Some(exchange_blob(exchange).to_vec()),
            None,
            None,
            None,
        ),
        Decision::Committed(facts) => {
            let (code, bytes) = pubsub::facts_blob(facts).ok_or(Fault::Corrupt)?;
            (code, None, None, None, None, Some(bytes))
        }
        Decision::Rejected(rejected) => {
            let (reason, resource) = reason_codes(rejected);
            (DECISION_REJECTED, None, None, Some(reason), resource, None)
        }
    };
    conn.prepare_cached("INSERT INTO operations(id,decision,publication,exchange,reason,resource,recipe,facts) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)")?.execute(params![operation.to_be_bytes().as_slice(),code,publication,exchange,reason,resource,recipe,facts])?;
    Ok(())
}

fn read_operation(
    conn: &Connection,
    operation: OperationId,
) -> Result<Option<OperationRecord>, Fault> {
    let row = conn
        .prepare_cached(
            "SELECT recipe, decision, publication, exchange, reason, resource, facts
               FROM operations WHERE id = ?1",
        )?
        .query_row([operation.to_be_bytes().as_slice()], |row| {
            Ok((blob(row, 0)?, decision_of(row, 1)?))
        })
        .optional()?;
    match row {
        None => Ok(None),
        Some((recipe, columns)) => Ok(Some(OperationRecord {
            recipe,
            decision: columns.decode()?,
        })),
    }
}

fn fenced(conn: &Connection, operation: OperationId) -> Result<bool, Fault> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM fences WHERE id = ?1")?
        .query_row([operation.to_be_bytes().as_slice()], |_| Ok(()))
        .optional()?
        .is_some())
}

fn established(
    conn: &Connection,
    meta: &Meta,
    operation: OperationId,
) -> Result<Established, Fault> {
    if operation.incarnation() != meta.ledger.incarnation() {
        return Ok(Established::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed,
        )));
    }
    if operation.epoch() <= meta.collected_through && operation.epoch().get() != 0 {
        return Ok(Established::Outcome(
            StoreOutcome::RetiredOutcomeUnavailable,
        ));
    }
    if receipts::frontier(conn, meta, ReceiptOrigin::of(operation))?
        .is_some_and(|closure| operation.sequence() <= closure.through)
    {
        return Ok(Established::Outcome(
            StoreOutcome::RetiredOutcomeUnavailable,
        ));
    }
    if let Some(record) = read_operation(conn, operation)? {
        return Ok(Established::Outcome(match record.decision {
            Decision::Committed(facts) => StoreOutcome::Committed(facts),
            Decision::Rejected(reason) => StoreOutcome::RejectedCertain(reason),
        }));
    }
    if fenced(conn, operation)? {
        return Ok(Established::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::Fenced,
        )));
    }
    Ok(match meta.ledger.phase_of(operation.epoch()) {
        EpochPhase::Closed => {
            Established::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
        }
        phase => Established::Absent(phase),
    })
}

/// The revision and state of a session, when it exists.
fn session_row(
    conn: &Connection,
    session: SessionId,
) -> Result<Option<(Revision, SessionState)>, Fault> {
    let row = conn
        .prepare_cached("SELECT revision, state, expiry, opened_by FROM sessions WHERE id = ?1")?
        .query_row([session.to_be_bytes().as_slice()], |row| {
            let binding = reader::optional_blob(row, 3, 32)?;
            Ok((
                blob(row, 0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                binding,
            ))
        })
        .optional()?;
    match row {
        None => Ok(None),
        Some((revision, state, expiry, binding)) => {
            sessions::decode_expiry(expiry)?;
            if let Some(bytes) = binding {
                OperationId::from_be_slice(&bytes).map_err(|_| Corrupt)?;
            }
            Ok(Some((
                Revision::new(blob_u64(&revision)?),
                code_session_state(state)?,
            )))
        }
    }
}

fn active_session(conn: &Connection, session: SessionId, revision: Revision) -> Guard<()> {
    match session_row(conn, session)? {
        Some((current, SessionState::Active)) if current == revision => Ok(()),
        _ => refused(RejectReason::PreconditionFailed),
    }
}

fn exists(conn: &Connection, sql: &str, key: &[u8]) -> Result<bool, Fault> {
    Ok(conn
        .prepare_cached(sql)?
        .query_row([key], |_| Ok(()))
        .optional()?
        .is_some())
}

/// A completed publication can lose its metadata while its admission receipt
/// remains an exact identity witness. Only this operation's scope can contain
/// either incoming identity; collected scopes can never execute again.
fn admission_identity_was_used(
    conn: &Connection,
    meta: &Meta,
    admit: &AdmitPublication,
) -> Result<bool, Fault> {
    let operation = admit.operation;
    if let Some(closure) = receipts::frontier(conn, meta, ReceiptOrigin::of(operation))?
        && ((!receipts::MUTANT_ALLOW_OLD_PUBLICATION
            && admit.publication.sequence() <= closure.publication_through)
            || (!receipts::MUTANT_ALLOW_OLD_PAYLOAD
                && admit.payload.id.sequence() <= closure.payload_through))
    {
        return Ok(true);
    }
    let bound = |sequence| {
        OperationId::new(
            operation.incarnation(),
            operation.epoch(),
            operation.owner(),
            OperationSeq::new(sequence),
        )
        .to_be_bytes()
    };
    let mut statement = conn.prepare_cached(
        "SELECT id, recipe, decision, publication, exchange, reason, resource, facts
           FROM operations WHERE id >= ?1 AND id <= ?2 ORDER BY id",
    )?;
    let mut rows = statement.query(params![bound(0).as_slice(), bound(u64::MAX).as_slice()])?;
    let mut meter = reader::Meter::new(ReadBudget::SNAPSHOT);
    while let Some(row) = rows.next()? {
        let recipe = reader::borrow_blob(row, 1, ReadBudget::SNAPSHOT.max_field_bytes)?;
        meter.charge(
            256usize
                .checked_add(recipe.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        let id =
            OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?).map_err(|_| Corrupt)?;
        if let Decision::Committed(CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        }) = decision_of(row, 2)?.decode()?
        {
            let stored = Recipe::decode_canonical(
                recipe,
                meta.limits.max_targets_per_admission,
                meta.limits.max_topic_bytes,
            )
            .map_err(|error| {
                Fault::Read(match error {
                    RecipeError::AllocationFailed => ReadError::AllocationFailed,
                    RecipeError::TooManyTargets { .. } | RecipeError::TopicTooLong(_) => {
                        ReadError::LimitExceeded
                    }
                    _ => ReadError::InvalidData,
                })
            })?;
            let Recipe::AdmitPublication(stored) = stored else {
                return Err(Fault::Corrupt);
            };
            if stored.operation != id
                || stored.publication != publication
                || stored.source.exchange != exchange
            {
                return Err(Fault::Corrupt);
            }
            if stored.publication == admit.publication || stored.payload.id == admit.payload.id {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Metadata is live while either the inbound exchange or an obligation needs
/// it. Payload bytes are released separately with their final obligation.
fn discard_completed_publication(conn: &Connection, publication: &[u8]) -> Result<(), Fault> {
    conn.prepare_cached(
        "DELETE FROM publications WHERE id = ?1
           AND NOT EXISTS (SELECT 1 FROM inbound WHERE publication = ?1)
           AND NOT EXISTS (SELECT 1 FROM obligations WHERE publication = ?1)",
    )?
    .execute([publication])?;
    Ok(())
}

fn stage_admission(
    conn: &Connection,
    meta: &mut Meta,
    admit: &AdmitPublication,
    payload: Option<&SharedBytes>,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let operation = admit.operation;
    let same_scope = |incarnation, epoch, owner| {
        incarnation == operation.incarnation()
            && epoch == operation.epoch()
            && owner == operation.owner()
    };
    if !same_scope(
        admit.publication.incarnation(),
        admit.publication.epoch(),
        admit.publication.owner(),
    ) || !same_scope(
        admit.payload.id.incarnation(),
        admit.payload.id.epoch(),
        admit.payload.id.owner(),
    ) || !same_scope(
        admit.request.incarnation(),
        admit.request.epoch(),
        admit.request.owner(),
    ) {
        return refused(failed);
    }
    let targets = admit.targets.as_slice();
    if targets.len() > meta.limits.max_targets_per_admission
        || admit.topic.len() > meta.limits.max_topic_bytes
        || admit.route_revision != meta.route_revision
    {
        return refused(failed);
    }
    let Some(bytes) = payload else {
        return refused(failed);
    };
    let length = u64::try_from(bytes.len()).map_err(|_| Abort::Refused(failed))?;
    if length != admit.payload.length.get() || bytes.len() > meta.limits.max_payload_bytes {
        return refused(failed);
    }
    // The costs are part of the frozen recipe and must be the costs of
    // what it admits: the store releases exactly what it charged.
    let exactly_once = admit.qos == Qos::ExactlyOnce;
    let reference_count = u64::try_from(targets.len()).map_err(|_| Abort::Refused(failed))?;
    if admit.costs.payload_bytes != admit.payload.length
        || admit.costs.references.get() != reference_count
        || admit.costs.exchange_slots.get() != u64::from(exactly_once)
        || admit.costs.receipt_slots.get() != 1
    {
        return refused(failed);
    }
    active_session(conn, admit.source.session, admit.source.session_revision)?;

    if admission_identity_was_used(conn, meta, admit)? {
        return refused(failed);
    }

    // A payload identity names one payload, kept or not.
    let publication = admit.publication.to_be_bytes();
    let payload_id = admit.payload.id.to_be_bytes();
    if exists(
        conn,
        "SELECT 1 FROM publications WHERE id = ?1",
        &publication,
    )? || exists(conn, "SELECT 1 FROM payloads WHERE id = ?1", &payload_id)?
        || exists(
            conn,
            "SELECT 1 FROM publications WHERE payload = ?1",
            &payload_id,
        )?
    {
        return refused(failed);
    }
    conn.prepare_cached(
        "INSERT INTO publications (id, operation, source, qos, retain, payload, topic)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?
    .execute(params![
        publication.as_slice(),
        operation.to_be_bytes().as_slice(),
        admit.source.session.to_be_bytes().as_slice(),
        qos_code(admit.qos),
        i64::from(admit.retain),
        payload_id.as_slice(),
        admit.topic.as_slice(),
    ])?;
    // Its bytes are kept while an obligation refers to them, and only then
    // charged. Without a target they are charged and released in this same
    // transaction.
    if reference_count > 0 {
        conn.prepare_cached(
            "INSERT INTO payloads (id, refs, length, bytes) VALUES (?1, ?2, ?3, ?4)",
        )?
        .execute(params![
            payload_id.as_slice(),
            u64_blob(reference_count).as_slice(),
            u64_blob(length).as_slice(),
            bytes.as_slice(),
        ])?;
    }
    for target in targets {
        active_session(conn, target.session, target.session_revision)?;
        let delivery = target.delivery;
        let key = (
            delivery.publication.to_be_bytes(),
            i64::from(delivery.index),
        );
        let taken = conn
            .prepare_cached("SELECT 1 FROM obligations WHERE publication = ?1 AND idx = ?2")?
            .query_row(params![key.0.as_slice(), key.1], |_| Ok(()))
            .optional()?
            .is_some();
        if taken {
            return refused(failed);
        }
        conn.prepare_cached(
            "INSERT INTO obligations (publication, idx, target, target_revision, qos, cause, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?
        .execute(params![
            key.0.as_slice(),
            key.1,
            target.session.to_be_bytes().as_slice(),
            target.session_revision.to_be_bytes().as_slice(),
            qos_code(target.effective_qos),
            cause_code(target.cause),
            obligation_phase_code(mqttiumd_store_api::ObligationPhase::Pending),
        ])?;
    }
    if exactly_once {
        // One admission per inbound exchange: an occupied slot is never
        // taken over, whatever generation the newcomer claims.
        let session = admit.source.session.to_be_bytes();
        let packet = i64::from(admit.source.packet_id.get());
        let taken = conn
            .prepare_cached("SELECT 1 FROM inbound WHERE session = ?1 AND packet_id = ?2")?
            .query_row(params![session.as_slice(), packet], |_| Ok(()))
            .optional()?
            .is_some();
        if taken {
            return refused(failed);
        }
        conn.prepare_cached(
            "INSERT INTO inbound (session, packet_id, exchange, publication, phase)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?
        .execute(params![
            session.as_slice(),
            packet,
            exchange_blob(admit.source.exchange).as_slice(),
            publication.as_slice(),
            inbound_phase_code(InboundPhase::AwaitPubrel),
        ])?;
    }
    charge(
        meta,
        Accounts {
            payload_bytes: admit.costs.payload_bytes,
            references: admit.costs.references,
            exchange_slots: admit.costs.exchange_slots,
            receipt_slots: admit.costs.receipt_slots,
        },
    )?;
    if reference_count == 0 {
        meta.accounts.payload_bytes = meta
            .accounts
            .payload_bytes
            .checked_sub(admit.costs.payload_bytes)
            .ok_or(Abort::Refused(failed))?;
    }
    discard_completed_publication(conn, &publication)?;
    Ok(CommitFacts::PublicationAdmitted {
        publication: admit.publication,
        exchange: admit.source.exchange,
    })
}

fn stage_release(
    conn: &Connection,
    meta: &mut Meta,
    release: &ReleaseInbound,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let session = release.source_session.to_be_bytes();
    let packet = i64::from(release.packet_id.get());
    let marker = conn
        .prepare_cached(
            "SELECT exchange, phase, publication FROM inbound WHERE session = ?1 AND packet_id = ?2",
        )?
        .query_row(params![session.as_slice(), packet], |row| {
            Ok((blob(row, 0)?, row.get::<_, i64>(1)?, reader::copy_blob(row, 2, 32)?))
        })
        .optional()?;
    let publication = match marker {
        Some((exchange, phase, publication))
            if blob_exchange(&exchange)? == release.exchange
                && code_inbound_phase(phase)? == InboundPhase::AwaitPubrel =>
        {
            publication
        }
        _ => return refused(failed),
    };
    conn.prepare_cached("DELETE FROM inbound WHERE session = ?1 AND packet_id = ?2")?
        .execute(params![session.as_slice(), packet])?;
    meta.accounts.exchange_slots = meta
        .accounts
        .exchange_slots
        .checked_sub(ExchangeSlots::new(1))
        .ok_or(Abort::Refused(failed))?;
    // The receipt of the release holds a slot like every receipt.
    charge(meta, Accounts::RECEIPT)?;
    discard_completed_publication(conn, &publication)?;
    Ok(CommitFacts::InboundReleased {
        exchange: release.exchange,
    })
}

/// Ends one obligation and releases exactly what it held.
fn end_obligation(
    conn: &Connection,
    meta: &mut Meta,
    publication: &[u8],
    index: i64,
) -> Result<(), Abort<AdminRefused>> {
    let broken = Abort::Refused(AdminRefused::Inconsistent);
    let removed = conn
        .prepare_cached("DELETE FROM outbound WHERE publication=?1 AND idx=?2")?
        .execute(params![publication, index])?;
    if removed == 1 {
        meta.accounts.exchange_slots = meta
            .accounts
            .exchange_slots
            .checked_sub(ExchangeSlots::new(1))
            .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    }
    conn.prepare_cached("DELETE FROM obligations WHERE publication = ?1 AND idx = ?2")?
        .execute(params![publication, index])?;
    meta.accounts.references = meta
        .accounts
        .references
        .checked_sub(References::new(1))
        .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    let Some(payload) = conn
        .prepare_cached("SELECT payload FROM publications WHERE id = ?1")?
        .query_row([publication], |row| blob(row, 0))
        .optional()?
    else {
        return Err(broken);
    };
    let Some((refs, length)) = conn
        .prepare_cached("SELECT refs, length FROM payloads WHERE id = ?1")?
        .query_row([payload.as_slice()], |row| {
            Ok((blob(row, 0)?, blob(row, 1)?))
        })
        .optional()?
    else {
        return Err(broken);
    };
    let refs = blob_u64(&refs)?
        .checked_sub(1)
        .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    if refs == 0 {
        conn.prepare_cached("DELETE FROM payloads WHERE id = ?1")?
            .execute([payload.as_slice()])?;
        meta.accounts.payload_bytes = meta
            .accounts
            .payload_bytes
            .checked_sub(PayloadBytes::new(blob_u64(&length)?))
            .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    } else {
        conn.prepare_cached("UPDATE payloads SET refs = ?2 WHERE id = ?1")?
            .execute(params![payload.as_slice(), u64_blob(refs).as_slice()])?;
    }
    discard_completed_publication(conn, publication)?;
    Ok(())
}

fn admin_refused<T>(refusal: AdminRefused) -> Result<T, Abort<AdminRefused>> {
    Err(Abort::Refused(refusal))
}

fn stage_admin(
    conn: &Connection,
    meta: &mut Meta,
    command: AdminCommand,
) -> Result<AdminDone, Abort<AdminRefused>> {
    let active = session_state_code(SessionState::Active);
    match command {
        AdminCommand::CreateSession { client_id } => {
            if client_id.len() > MAX_CLIENT_ID_BYTES {
                return admin_refused(AdminRefused::ClientIdTooLong);
            }
            let duplicate = conn
                .prepare_cached("SELECT 1 FROM sessions WHERE state = ?1 AND client_id = ?2")?
                .query_row(params![active, client_id.as_slice()], |_| Ok(()))
                .optional()?
                .is_some();
            if duplicate {
                return admin_refused(AdminRefused::DuplicateClientId);
            }
            let count: i64 = conn
                .prepare_cached("SELECT count(*) FROM sessions WHERE state = ?1")?
                .query_row([active], |row| row.get(0))?;
            let count = usize::try_from(count).map_err(|_| Abort::Fault)?;
            if count >= meta.limits.max_sessions {
                return admin_refused(AdminRefused::TableFull);
            }
            let session = meta.next_session;
            meta.next_session = session
                .checked_next()
                .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            let revision = Revision::new(1);
            conn.prepare_cached(
                "INSERT INTO sessions (id, revision, state, client_id, expiry, opened_by) VALUES (?1, ?2, ?3, ?4, 1, NULL)",
            )?
            .execute(params![
                session.to_be_bytes().as_slice(),
                revision.to_be_bytes().as_slice(),
                active,
                client_id.as_slice()
            ])?;
            Ok(AdminDone::SessionCreated { session, revision })
        }
        AdminCommand::AddRoute {
            topic,
            session,
            qos,
        } => {
            if topic.len() > meta.limits.max_topic_bytes {
                return admin_refused(AdminRefused::TopicTooLong);
            }
            match session_row(conn, session)? {
                Some((_, SessionState::Active)) => {}
                _ => return admin_refused(AdminRefused::UnknownSession),
            }
            let mut targets = Vec::new();
            {
                let mut statement = conn.prepare_cached(
                    "SELECT session, position FROM routes WHERE topic = ?1 ORDER BY position",
                )?;
                let mut rows = statement.query([topic.as_slice()])?;
                while let Some(row) = rows.next()? {
                    if targets.len() > meta.limits.max_route_targets {
                        return Err(Abort::Fault);
                    }
                    targets.push((blob(row, 0)?, blob_u64(reader::borrow_blob(row, 1, 8)?)?));
                }
            }
            let target = session.to_be_bytes();
            if targets
                .iter()
                .any(|(existing, _)| existing.as_slice() == target)
            {
                return admin_refused(AdminRefused::DuplicateTarget);
            }
            if targets.len() >= meta.limits.max_route_targets {
                return admin_refused(AdminRefused::TableFull);
            }
            let position = match targets.last() {
                Some((_, last)) => last
                    .checked_add(1)
                    .ok_or(Abort::Refused(AdminRefused::Exhausted))?,
                None => 0,
            };
            conn.prepare_cached(
                "INSERT INTO routes (topic, position, session, qos) VALUES (?1, ?2, ?3, ?4)",
            )?
            .execute(params![
                topic.as_slice(),
                u64_blob(position).as_slice(),
                target.as_slice(),
                qos_code(qos)
            ])?;
            meta.route_revision = meta
                .route_revision
                .checked_next()
                .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            Ok(AdminDone::RouteAdded {
                route_revision: meta.route_revision,
            })
        }
        AdminCommand::DeleteSession { session } => {
            sessions::erase(conn, meta, session)?;
            Ok(AdminDone::SessionDeleted)
        }
        AdminCommand::CollectClosedEpochs { through } => {
            if meta.ledger.phase_of(through) != EpochPhase::Closed {
                return admin_refused(AdminRefused::EpochNotClosed);
            }
            if through > meta.collected_through {
                meta.collected_through = through;
            }
            let collected = meta.collected_through;
            // Epoch 0 is never opened, so never closed: its receipts and
            // fences are never retired.
            let retired = |operation: OperationId| {
                operation.epoch() <= collected && operation.epoch().get() != 0
            };
            let mut freed = 0u64;
            for table in ["operations", "fences"] {
                let mut doomed = Vec::new();
                {
                    let mut statement = conn.prepare_cached(&format!("SELECT id FROM {table}"))?;
                    let mut rows = statement.query([])?;
                    while let Some(row) = rows.next()? {
                        let id = blob(row, 0)?;
                        let operation =
                            OperationId::from_be_slice(&id).map_err(|_| Abort::Fault)?;
                        if retired(operation) {
                            doomed.push(id);
                        }
                    }
                }
                let mut delete =
                    conn.prepare_cached(&format!("DELETE FROM {table} WHERE id = ?1"))?;
                for id in doomed {
                    delete.execute([id.as_slice()])?;
                    freed = freed
                        .checked_add(1)
                        .ok_or(Abort::Refused(AdminRefused::Exhausted))?;
                }
            }
            // Every receipt and every fence holds one receipt slot.
            meta.accounts.receipt_slots = meta
                .accounts
                .receipt_slots
                .checked_sub(ReceiptSlots::new(freed))
                .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
            Ok(AdminDone::Collected)
        }
    }
}

/// Opens a connection to `path` and checks, before anything is written,
/// that its schema version is `expected` (0 for a file to create).
/// The path of the writer's lock of the database at `path`.
#[must_use]
pub fn writer_lock_path(path: &Path) -> PathBuf {
    let mut file = path.as_os_str().to_os_string();
    file.push(".writer-lock");
    PathBuf::from(file)
}

/// Takes the writer's lock of the database at `path`, without waiting.
/// The lock file is created if it is missing and never truncated or
/// removed.
fn writer_lock(path: &Path) -> Result<WriterLease, OpenError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(writer_lock_path(path))
        .map_err(OpenError::WriterLock)?;
    match file.try_lock() {
        Ok(()) => Ok(WriterLease { file }),
        Err(TryLockError::WouldBlock) => Err(OpenError::WriterAlive),
        Err(TryLockError::Error(error)) => Err(OpenError::WriterLock(error)),
    }
}

/// Closing one descriptor does not release a lock while a descriptor inherited
/// across fork still refers to the same open file description. The logical
/// owner explicitly releases its lease; a transient child descriptor carries
/// no authority after that point. This guard always outlives the connection.
struct WriterLease {
    file: File,
}

impl Drop for WriterLease {
    fn drop(&mut self) {
        // If unlocking fails, closing the descriptor remains conservative:
        // another opener still has to acquire the exclusive kernel lock.
        let _ = self.file.unlock();
    }
}

fn connect(path: &Path) -> Result<Connection, OpenError> {
    let conn = Connection::open(path)?;
    // One writer: a busy database is an error, never a wait.
    conn.busy_timeout(std::time::Duration::ZERO)?;
    Ok(conn)
}

/// Sets WAL and `synchronous=FULL`, then reads back what is in force, the
/// engine version and its compile options.
fn settle(conn: &Connection) -> Result<Settings, OpenError> {
    let journal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(OpenError::Setting {
            name: "journal_mode",
            read_back: journal_mode,
        });
    }
    conn.execute_batch("PRAGMA synchronous = FULL")?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(OpenError::Setting {
            name: "journal_mode",
            read_back: journal_mode,
        });
    }
    if synchronous != 2 {
        return Err(OpenError::Setting {
            name: "synchronous",
            read_back: synchronous.to_string(),
        });
    }
    let sqlite_version: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let mut compile_options = Vec::new();
    {
        let mut statement = conn.prepare("PRAGMA compile_options")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            compile_options.push(row.get::<_, String>(0)?);
        }
    }
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let wal_autocheckpoint: i64 =
        conn.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
    Ok(Settings {
        sqlite_version,
        compile_options,
        journal_mode,
        synchronous,
        user_version,
        page_size,
        wal_autocheckpoint,
        checkpoint_policy: "sqlite-autocheckpoint",
        kill_points: KILL_POINTS_ENABLED,
    })
}

/// `PRAGMA quick_check`: its first lines, `["ok"]` for a sound file. A
/// damage the check itself trips over is a report too, not an SQL error.
fn quick_check(conn: &Connection) -> Result<Vec<String>, OpenError> {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut report = Vec::new();
        let mut statement = conn.prepare("PRAGMA quick_check")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            if report.len() < 8 {
                let value = row.get_ref(0)?;
                let ok = matches!(value, rusqlite::types::ValueRef::Text(bytes) if bytes == b"ok");
                report.push(
                    if ok {
                        "ok"
                    } else {
                        "SQLite integrity check rejected the image"
                    }
                    .to_owned(),
                );
            }
        }
        Ok(report)
    };
    match run() {
        Ok(report) => Ok(report),
        Err(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
            ) =>
        {
            Ok(vec!["SQLite integrity check rejected the image".to_owned()])
        }
        Err(error) => Err(OpenError::Sql(error)),
    }
}

/// The SQLite store. The connection is the only writer of its file.
pub struct SqliteStore {
    conn: Connection,
    settings: Settings,
    durability: Durability,
    /// The writer's lock, held while the store lives. Declared last: the
    /// fields drop in order, so the connection is closed, and its last
    /// checkpoint done, before another writer can take the lock.
    #[cfg(feature = "kill-points")]
    kill_hook: kill_points::Shared,
    #[cfg(feature = "kill-points")]
    receipt_hook: receipt_points::Shared,
    #[cfg(feature = "kill-points")]
    autocheckpoint_before_hook: Option<i64>,
    _writer_lock: WriterLease,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore")
            .field("settings", &self.settings)
            .field("durability", &self.durability)
            .field("ledger", &"decoded on each read")
            .finish_non_exhaustive()
    }
}

impl SqliteStore {
    /// Creates an empty data set in a file that holds no database yet. No
    /// epoch is open in it.
    pub fn create(
        path: &Path,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<Self, OpenError> {
        let lock = writer_lock(path)?;
        let mut conn = connect(path)?;
        let tables: i64 =
            conn.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if tables != 0 || version != 0 {
            return Err(OpenError::NotEmpty);
        }
        settle(&conn)?;
        let meta = Meta {
            ledger: EpochLedger::fresh(incarnation),
            collected_through: RunEpoch::new(0),
            revision: 0,
            limits,
            accounts: Accounts::default(),
            next_session: SessionId::new(1),
            route_revision: Revision::new(0),
        };
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(schema::CREATE)?;
        tx.execute(
            "INSERT INTO meta (one, format, incarnation, last_opened, collected_through, revision,
                 next_session, route_revision, limit_payload_bytes, limit_references,
                 limit_exchange_slots, limit_receipt_slots, max_targets_per_admission,
                 max_topic_bytes, max_payload_bytes, max_sessions, max_route_targets,
                 used_payload_bytes, used_references, used_exchange_slots, used_receipt_slots)
             VALUES (1, ?1, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2)",
            params![FORMAT, u64_blob(0).as_slice()],
        )?;
        write_meta(&tx, &meta).map_err(|_| OpenError::Corrupt)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        Self::verified(lock, conn)
    }

    /// Opens an existing data set: its schema version is checked before
    /// anything is written, then the settings are set and read back, and
    /// its durable values are decoded.
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        let lock = writer_lock(path)?;
        {
            let probe = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            let version: i64 = probe.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if version != SCHEMA_VERSION {
                return Err(OpenError::SchemaVersion(version));
            }
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::ZERO)?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersion(version));
        }
        Self::verified(lock, conn)
    }

    /// Reads the durable state of the file at `path` without the writer,
    /// for an inspector outside the process that writes it.
    ///
    /// The file is opened read-only (`SQLITE_OPEN_READ_ONLY`, the flag of
    /// `mode=ro`; never `immutable`, which would read without locks while
    /// the WAL changes). No pragma that writes is set and no checkpoint is
    /// run. The whole state is read in one read transaction, which ends,
    /// and the connection is closed, before this returns: the inspector
    /// holds nothing that would keep the writer's checkpoint back. It does
    /// not take the writer's lock, and reads while a writer lives.
    pub fn inspect(path: &Path) -> Result<DurableState, ReadError> {
        Self::inspect_with_budget(path, ReadBudget::SNAPSHOT)
    }

    /// Bounded inspection uses the same frozen read and budget as the writer.
    pub fn inspect_with_budget(path: &Path, budget: ReadBudget) -> Result<DurableState, ReadError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| reader::sql_error(&error))?;
        conn.busy_timeout(std::time::Duration::from_secs(1))
            .map_err(|error| reader::sql_error(&error))?;
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|error| reader::sql_error(&error))?;
        if version != SCHEMA_VERSION {
            return Err(ReadError::InvalidData);
        }
        let read = conn
            .unchecked_transaction()
            .map_err(|error| reader::sql_error(&error))?;
        let state = read_snapshot(&read, budget).map_err(|fault| fault.read_error())?;
        read.commit().map_err(|error| reader::sql_error(&error))?;
        Ok(state)
    }

    // Parameters drop in reverse order on an early error: close/checkpoint
    // SQLite before releasing the lease, as the fields of a live store do.
    fn verified(lock: WriterLease, conn: Connection) -> Result<Self, OpenError> {
        let settings = settle(&conn)?;
        // Before an epoch is opened: every page of the file is checked,
        // not only those opening reads.
        let report = quick_check(&conn)?;
        if report != ["ok"] {
            return Err(OpenError::Integrity(report.join("\n")));
        }
        if settings.user_version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersion(settings.user_version));
        }
        read_meta(&conn).map_err(|fault| OpenError::Read(fault.read_error()))?;
        Ok(Self {
            conn,
            settings,
            durability: Durability::Available,
            #[cfg(feature = "kill-points")]
            kill_hook: kill_points::Shared::default(),
            #[cfg(feature = "kill-points")]
            receipt_hook: receipt_points::Shared::default(),
            #[cfg(feature = "kill-points")]
            autocheckpoint_before_hook: None,
            _writer_lock: lock,
        })
    }

    /// The durable state, with a stable read cause rather than a panic.
    pub fn try_snapshot(&self) -> Result<DurableState, ReadError> {
        self.snapshot()
    }

    /// Sets the function the store calls at each kill point, or removes it.
    ///
    /// Installation order is explicit: disable SQLite's autocheckpoint,
    /// read back zero, then install the WAL hook. The hook reports the
    /// committed transaction and performs no checkpoint. Removing it first
    /// removes the WAL callback, then restores the saved autocheckpoint
    /// threshold. A PRAGMA alone cannot prove the callback is installed;
    /// the kill-point tests observe actual callback invocations.
    #[cfg(feature = "kill-points")]
    pub fn set_kill_hook(&mut self, hook: Option<kill_points::Hook>) -> Result<(), OpenError> {
        use kill_points::{KillPoint, pass};
        let armed = hook.is_some();
        if armed {
            let threshold = self
                .autocheckpoint_before_hook
                .unwrap_or(self.settings.wal_autocheckpoint);
            self.conn.pragma_update(None, "wal_autocheckpoint", 0)?;
            let read_back: i64 = self
                .conn
                .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
            if read_back != 0 {
                return Err(OpenError::Setting {
                    name: "wal_autocheckpoint",
                    read_back: read_back.to_string(),
                });
            }
            *self
                .kill_hook
                .lock()
                .expect("kill callback is not poisoned") = hook;
            let shared = std::sync::Arc::clone(&self.kill_hook);
            self.conn.wal_hook(Some(
                move |_wal: &mut rusqlite::hooks::Wal, _frames: std::ffi::c_int| {
                    pass(&shared, KillPoint::CommitLogged);
                    Ok(())
                },
            ))?;
            self.autocheckpoint_before_hook = Some(threshold);
            self.settings.wal_autocheckpoint = read_back;
            self.settings.checkpoint_policy = "disabled-while-kill-hook-armed";
        } else {
            self.conn.wal_hook(
                None::<fn(&mut rusqlite::hooks::Wal, std::ffi::c_int) -> rusqlite::Result<()>>,
            )?;
            *self
                .kill_hook
                .lock()
                .expect("kill callback is not poisoned") = None;
            let threshold = self
                .autocheckpoint_before_hook
                .unwrap_or(self.settings.wal_autocheckpoint);
            self.conn
                .pragma_update(None, "wal_autocheckpoint", threshold)?;
            self.settings.wal_autocheckpoint =
                self.conn
                    .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
            self.autocheckpoint_before_hook = None;
            self.settings.checkpoint_policy = "sqlite-autocheckpoint";
        }
        Ok(())
    }

    /// Installs barriers carrying the real control, never a fabricated command.
    #[cfg(feature = "kill-points")]
    pub fn set_receipt_hook(&mut self, hook: Option<receipt_points::Hook>) {
        if let Ok(mut installed) = self.receipt_hook.lock() {
            *installed = hook;
        }
    }

    fn collect_receipts_transaction(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        let failing = self.durability == Durability::Failing;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| reader::sql_error(&error))?;
        let mut meta = read_meta(&tx).map_err(|fault| fault.read_error())?;
        let (reply, changed) =
            receipts::prepare(&tx, &mut meta, collection).map_err(|fault| fault.read_error())?;
        if !changed {
            return Ok(reply);
        }
        let Some(revision) = meta.revision.checked_add(1) else {
            return Ok(ReceiptCollectionReply::Refused(
                mqttiumd_contract::receipts::ReceiptCollectionRefusal::Exhausted,
            ));
        };
        meta.revision = revision;
        write_meta(&tx, &meta).map_err(|fault| fault.read_error())?;
        #[cfg(feature = "kill-points")]
        receipt_points::pass(
            &self.receipt_hook,
            receipt_points::ReceiptPoint::ClosePrepared,
            collection,
        );
        if failing {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        }
        if tx.commit().is_err() {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        }
        #[cfg(feature = "kill-points")]
        receipt_points::pass(
            &self.receipt_hook,
            receipt_points::ReceiptPoint::CloseCommitted,
            collection,
        );
        Ok(reply)
    }

    fn collect_closed_receipts_transaction(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        let failing = self.durability == Durability::Failing;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| reader::sql_error(&error))?;
        let mut meta = read_meta(&tx).map_err(|fault| fault.read_error())?;
        let (reply, changed) = receipts::prepare_closed(&tx, &mut meta, collection)
            .map_err(|fault| fault.read_error())?;
        if !changed {
            return Ok(reply);
        }
        let Some(revision) = meta.revision.checked_add(1) else {
            return Ok(ClosedReceiptReply::Refused(
                mqttiumd_contract::receipts::ReceiptCollectionRefusal::Exhausted,
            ));
        };
        meta.revision = revision;
        write_meta(&tx, &meta).map_err(|fault| fault.read_error())?;
        if failing || tx.commit().is_err() {
            return Ok(ClosedReceiptReply::PendingOrUnknown);
        }
        Ok(reply)
    }

    /// The engine and the settings in force, read back at opening.
    #[must_use]
    pub fn effective_settings(&self) -> &Settings {
        &self.settings
    }

    /// For a measurement only: sets `PRAGMA wal_autocheckpoint` on the
    /// writer's connection and reads it back into the settings. A store
    /// keeps the engine's default otherwise; nothing in the product calls
    /// this. Refused while a kill hook is armed: setting the PRAGMA would
    /// replace that hook in SQLite, even if its value stayed zero.
    pub fn set_wal_autocheckpoint_for_measurement(&mut self, pages: i64) -> Result<i64, OpenError> {
        #[cfg(feature = "kill-points")]
        if self.autocheckpoint_before_hook.is_some() {
            return Err(OpenError::Setting {
                name: "wal_autocheckpoint_while_kill_hook_armed",
                read_back: self.settings.wal_autocheckpoint.to_string(),
            });
        }
        self.conn.pragma_update(None, "wal_autocheckpoint", pages)?;
        let read_back: i64 = self
            .conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
        self.settings.wal_autocheckpoint = read_back;
        Ok(read_back)
    }

    /// Sets whether the medium accepts writes. While it fails, every
    /// transaction is rolled back instead of committed.
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Runs one write transaction. `prepare` reads and writes rows and the
    /// decoded `meta`; on success `meta` is written one revision later and
    /// the transaction committed, unless the medium fails.
    fn transact<T, E>(
        &mut self,
        prepare: impl FnOnce(&Connection, &mut Meta) -> Result<T, Abort<E>>,
    ) -> Txn<T, E> {
        let failing = self.durability == Durability::Failing;
        let result = (|| {
            let tx: Transaction<'_> = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| None)?;
            let mut meta = read_meta(&tx).map_err(|fault| Some(fault.read_error()))?;
            let value = match prepare(&tx, &mut meta) {
                Ok(value) => value,
                // Dropping the transaction rolls it back.
                Err(Abort::Refused(refusal)) => return Ok(Err(refusal)),
                Err(Abort::Read(error)) => return Err(Some(error)),
                Err(Abort::Fault) => return Err(None),
            };
            meta.revision = meta.revision.checked_add(1).ok_or(None)?;
            write_meta(&tx, &meta).map_err(|_| None)?;
            // K1 `store.commit.prepared`: every row written, `COMMIT` not
            // called; the transaction is open.
            #[cfg(feature = "kill-points")]
            kill_points::pass(&self.kill_hook, kill_points::KillPoint::CommitPrepared);
            if failing {
                return Err(None);
            }
            tx.commit().map_err(|_| None)?;
            Ok(Ok(value))
        })();
        match result {
            Ok(Ok(value)) => Txn::Committed(value),
            Ok(Err(refusal)) => Txn::Refused(refusal),
            Err(Some(error)) => Txn::ReadFailed(error),
            Err(None) => Txn::NotWritten,
        }
    }

    /// Records a certain rejection on a receipt that holds a receipt slot.
    /// Without a slot nothing is written and nothing certain announced.
    fn commit_rejection(
        &mut self,
        operation: OperationId,
        recipe: &[u8],
        reason: RejectReason,
    ) -> (StoreOutcome, bool) {
        let written = self.transact(|conn, meta| {
            charge(meta, Accounts::RECEIPT).map_err(|_| Abort::Refused(()))?;
            insert_operation(conn, operation, recipe, Decision::Rejected(reason))?;
            Ok::<(), Abort<()>>(())
        });
        match written {
            Txn::Committed(()) => (StoreOutcome::RejectedCertain(reason), true),
            Txn::Refused(()) | Txn::ReadFailed(_) | Txn::NotWritten => {
                (StoreOutcome::PendingOrUnknown, false)
            }
        }
    }

    fn read_ledger(&self) -> Result<EpochLedger, ReadError> {
        read_meta(&self.conn)
            .map(|meta| meta.ledger)
            .map_err(|fault| fault.read_error())
    }

    /// The recovery inventory from `start`, at most `count` facts, in the
    /// order of record 0014, each section read with its own bounded query.
    fn inventory(
        conn: &Connection,
        meta: &Meta,
        start: usize,
        count: usize,
        budget: ReadBudget,
    ) -> Result<(Vec<Result<RecoveredFact, RecoveryRefused>>, bool), Fault> {
        let mut total_rows = 3usize;
        for table in [
            "routes",
            "sessions",
            "inbound",
            "obligations",
            "operations",
            "fences",
            "receipt_origins",
        ] {
            let count: i64 = conn.query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT {})",
                    MAX_RECOVERY_INVENTORY_ROWS + 1
                ),
                [],
                |row| row.get(0),
            )?;
            total_rows = total_rows
                .checked_add(usize::try_from(count).map_err(|_| Fault::Corrupt)?)
                .ok_or(ReadError::LimitExceeded)?;
            if total_rows > MAX_RECOVERY_INVENTORY_ROWS {
                return Err(Fault::Read(ReadError::LimitExceeded));
            }
        }
        let claims: i64 = conn.query_row(
            "SELECT count(*) FROM (SELECT 1 FROM outbound LIMIT ?1)",
            [i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?],
            |row| row.get(0),
        )?;
        if usize::try_from(claims).map_err(|_| Fault::Corrupt)? > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(Fault::Read(ReadError::LimitExceeded));
        }
        let orphan: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM outbound c LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx WHERE o.publication IS NULL LIMIT 1)",
            [], |row| row.get(0),
        )?;
        if orphan {
            return Err(Fault::Corrupt);
        }
        let mut out = Vec::new();
        out.try_reserve_exact(count)
            .map_err(|_| ReadError::AllocationFailed)?;
        let mut meter = reader::Meter::new(budget);
        let mut position = 0usize;
        let wanted = |out: &mut Vec<_>, position: &mut usize| {
            let take = *position >= start && out.len() < count;
            *position = position.checked_add(1).ok_or(ReadError::LimitExceeded)?;
            Ok::<bool, ReadError>(take)
        };
        let refused = |_| RecoveryRefused::Read(ReadError::LimitExceeded);
        let incarnation = meta.ledger.incarnation();
        let open = meta.ledger.open_epoch();
        if let Some(epoch) = open
            && wanted(&mut out, &mut position)?
        {
            meter.charge(128)?;
            out.push(Ok(RecoveredFact::Epoch { incarnation, epoch }));
        }
        if wanted(&mut out, &mut position)? {
            meter.charge(128)?;
            let bound = |value: usize| u32::try_from(value).map_err(refused);
            out.push((|| {
                Ok(RecoveredFact::Limits {
                    max_targets_per_admission: bound(meta.limits.max_targets_per_admission)?,
                    max_topic_bytes: bound(meta.limits.max_topic_bytes)?,
                    max_payload_bytes: PayloadBytes::new(
                        u64::try_from(meta.limits.max_payload_bytes).map_err(refused)?,
                    ),
                })
            })());
        }
        if wanted(&mut out, &mut position)? {
            meter.charge(128)?;
            out.push(Ok(RecoveredFact::RouteTable {
                revision: meta.route_revision,
            }));
        }
        let max_topic_bytes = meta.limits.max_topic_bytes;
        let active = session_state_code(SessionState::Active);
        // Each section: its rows from `skip`, at most what is still wanted.
        let section = |sql: &str,
                       out: &mut Vec<_>,
                       position: &mut usize,
                       build: &mut BuildFact<'_>|
         -> Result<(), Fault> {
            let skip = start.saturating_sub(*position);
            let room = count.saturating_sub(out.len());
            let total: i64 = conn
                .prepare_cached(&format!(
                    "SELECT count(*) FROM ({sql} LIMIT {})",
                    MAX_RECOVERY_INVENTORY_ROWS + 1
                ))?
                .query_row(
                    params![
                        active,
                        open.map(|epoch| epoch.to_be_bytes().to_vec())
                            .unwrap_or_default()
                    ],
                    |row| row.get(0),
                )?;
            let total = usize::try_from(total).map_err(|_| Fault::Corrupt)?;
            if total > MAX_RECOVERY_INVENTORY_ROWS {
                return Err(Fault::Read(ReadError::LimitExceeded));
            }
            if room > 0 && skip < total {
                let mut statement = conn.prepare_cached(&format!("{sql} LIMIT ?3 OFFSET ?4"))?;
                let limit = i64::try_from(room).map_err(|_| Fault::Corrupt)?;
                let offset = i64::try_from(skip).map_err(|_| Fault::Corrupt)?;
                let mut rows = statement.query(params![
                    active,
                    open.map(|epoch| epoch.to_be_bytes().to_vec())
                        .unwrap_or_default(),
                    limit,
                    offset
                ])?;
                while let Some(row) = rows.next()? {
                    out.push(build(row)?);
                }
            }
            *position = position
                .checked_add(total)
                .ok_or(ReadError::LimitExceeded)?;
            if *position > MAX_RECOVERY_INVENTORY_ROWS {
                return Err(Fault::Read(ReadError::LimitExceeded));
            }
            Ok(())
        };
        // ?1 is the code of an active session and ?2 the open epoch, in
        // every query, used or not.
        section(
            "SELECT r.topic, r.session, r.qos, s.revision, s.state FROM routes r
               LEFT JOIN sessions s ON s.id = r.session
              WHERE ?1 = ?1 AND ?2 = ?2 ORDER BY r.topic, r.position",
            &mut out,
            &mut position,
            &mut |row| {
                let topic =
                    reader::borrow_blob(row, 0, max_topic_bytes.min(budget.max_field_bytes))?;
                budget.check_field(topic.len())?;
                meter.charge(
                    128usize
                        .checked_add(topic.len())
                        .ok_or(ReadError::LimitExceeded)?,
                )?;
                let target = SessionId::from_be_slice(reader::borrow_blob(row, 1, 8)?)
                    .map_err(|_| Fault::Corrupt)?;
                let max_qos = code_qos(row.get(2)?)?;
                let revision = reader::optional_blob(row, 3, 8)?;
                let state: Option<i64> = row.get(4)?;
                let Some(revision) = revision.filter(|_| state == Some(active)) else {
                    return Ok(Err(RecoveryRefused::Read(ReadError::InvalidData)));
                };
                let Ok(topic) = SharedBytes::copy_from(topic, max_topic_bytes) else {
                    return Ok(Err(RecoveryRefused::Read(ReadError::LimitExceeded)));
                };
                Ok(Ok(RecoveredFact::Route {
                    topic,
                    target,
                    target_revision: Revision::new(blob_u64(&revision)?),
                    max_qos,
                }))
            },
        )?;
        section(
            "SELECT id, client_id, revision, state, expiry, opened_by FROM sessions
              WHERE (state = ?1 OR state NOT IN (1, 2)) AND ?2 = ?2 ORDER BY id",
            &mut out,
            &mut position,
            &mut |row| {
                if code_session_state(row.get(3)?)? != SessionState::Active {
                    return Err(Fault::Corrupt);
                }
                let session = SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?)
                    .map_err(|_| Fault::Corrupt)?;
                let bytes =
                    reader::borrow_blob(row, 1, MAX_CLIENT_ID_BYTES.min(budget.max_field_bytes))?;
                budget.check_field(bytes.len())?;
                meter.charge(
                    128usize
                        .checked_add(bytes.len())
                        .ok_or(ReadError::LimitExceeded)?,
                )?;
                let Ok(client_id) = SharedBytes::copy_from(bytes, MAX_CLIENT_ID_BYTES) else {
                    return Ok(Err(RecoveryRefused::Read(ReadError::LimitExceeded)));
                };
                Ok(Ok(RecoveredFact::Session {
                    session,
                    client_id,
                    revision: Revision::new(blob_u64(reader::borrow_blob(row, 2, 8)?)?),
                    expiry: sessions::decode_expiry(row.get(4)?)?,
                    opened_by: sessions::attachment(
                        reader::optional_blob(row, 5, 32)?.as_deref(),
                        meta,
                    )?,
                }))
            },
        )?;
        section(
            "SELECT packet_id, exchange, publication, phase FROM inbound
              WHERE ?1 = ?1 AND ?2 = ?2 ORDER BY session, packet_id",
            &mut out,
            &mut position,
            &mut |row| {
                budget.check_field(32)?;
                meter.charge(128)?;
                let packet: i64 = row.get(0)?;
                let packet_id = u16::try_from(packet)
                    .ok()
                    .and_then(InboundPacketId::new)
                    .ok_or(Fault::Corrupt)?;
                Ok(Ok(RecoveredFact::InboundExchange {
                    exchange: blob_exchange(reader::borrow_blob(row, 1, 16)?)?,
                    packet_id,
                    publication: PublicationId::from_be_slice(reader::borrow_blob(row, 2, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    phase: code_inbound_phase(row.get(3)?)?,
                }))
            },
        )?;
        section(
            "SELECT o.publication,o.idx,o.target,o.target_revision,c.packet_id,c.claim,c.target,c.target_revision,o.qos,s.revision,s.state FROM obligations o LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx LEFT JOIN sessions s ON s.id=o.target WHERE ?1=?1 AND ?2=?2 ORDER BY o.publication,o.idx",
            &mut out,
            &mut position,
            &mut |row| {
                budget.check_field(32)?;
                meter.charge(160)?;
                let delivery = DeliveryId {
                    publication: PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    index: u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Fault::Corrupt)?,
                };
                let target = SessionId::from_be_slice(reader::borrow_blob(row, 2, 8)?)
                    .map_err(|_| Fault::Corrupt)?;
                let target_revision = Revision::new(blob_u64(reader::borrow_blob(row, 3, 8)?)?);
                if code_session_state(row.get(10)?)? != SessionState::Active
                    || target_revision.get() != blob_u64(reader::borrow_blob(row, 9, 8)?)?
                {
                    return Err(Fault::Corrupt);
                }
                let qos = code_qos(row.get(8)?)?;
                let claim = match row.get_ref(4)? {
                    rusqlite::types::ValueRef::Null => None,
                    _ => {
                        let operation =
                            OperationId::from_be_slice(reader::borrow_blob(row, 5, 32)?)
                                .map_err(|_| Fault::Corrupt)?;
                        if operation.incarnation() != incarnation
                            || qos != Qos::AtLeastOnce
                            || reader::borrow_blob(row, 6, 8)? != target.to_be_bytes()
                            || blob_u64(reader::borrow_blob(row, 7, 8)?)? != target_revision.get()
                        {
                            return Err(Fault::Corrupt);
                        }
                        Some(mqttiumd_contract::pubsub::DeliveryClaim {
                            operation,
                            packet_id: u16::try_from(row.get::<_, i64>(4)?)
                                .ok()
                                .and_then(OutboundPacketId::new)
                                .ok_or(Fault::Corrupt)?,
                        })
                    }
                };
                Ok(Ok(RecoveredFact::OutboundDelivery {
                    delivery,
                    target,
                    target_revision,
                    qos,
                    claim,
                }))
            },
        )?;
        section(
            "SELECT epoch,owner,consumed_through,publication_through,payload_through FROM receipt_origins WHERE ?1=?1 AND ?2=?2 ORDER BY epoch,owner",
            &mut out,
            &mut position,
            &mut |row| {
                budget.check_field(8)?;
                meter.charge(128)?;
                Ok(Ok(RecoveredFact::ReceiptFrontier(
                    receipts::decode_frontier(row, meta)?,
                )))
            },
        )?;
        // What is established about the operations of older epochs: the
        // epoch is bytes 9 to 16 of the identity, compared as bytes.
        section(
            "SELECT o.id,o.decision,o.publication,o.exchange,o.reason,o.resource,o.facts FROM operations o
              WHERE ?1=?1 AND substr(o.id,9,8)!=?2
                AND (substr(o.id,9,8)=zeroblob(8) OR substr(o.id,9,8)>(SELECT collected_through FROM meta WHERE one=1))
                AND NOT EXISTS(SELECT 1 FROM receipt_origins r WHERE r.epoch=substr(o.id,9,8) AND r.owner=substr(o.id,17,8) AND substr(o.id,25,8)<=r.consumed_through)
              ORDER BY o.id",
            &mut out,
            &mut position,
            &mut |row| {
                budget.check_field(32)?;
                meter.charge(128)?;
                let operation = OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                    .map_err(|_| Fault::Corrupt)?;
                let knowledge = match decision_of(row, 1)?.decode()? {
                    Decision::Committed(facts) => Knowledge::Committed(facts),
                    Decision::Rejected(reason) => Knowledge::RejectedCertain(reason),
                };
                Ok(Ok(RecoveredFact::Operation {
                    operation,
                    knowledge,
                }))
            },
        )?;
        section(
            "SELECT o.id FROM fences o WHERE ?1=?1 AND substr(o.id,9,8)!=?2
                AND (substr(o.id,9,8)=zeroblob(8) OR substr(o.id,9,8)>(SELECT collected_through FROM meta WHERE one=1))
                AND NOT EXISTS(SELECT 1 FROM receipt_origins r WHERE r.epoch=substr(o.id,9,8) AND r.owner=substr(o.id,17,8) AND substr(o.id,25,8)<=r.consumed_through)
              ORDER BY o.id",
            &mut out,
            &mut position,
            &mut |row| {
                budget.check_field(32)?;
                meter.charge(128)?;
                let operation = OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                    .map_err(|_| Fault::Corrupt)?;
                Ok(Ok(RecoveredFact::Operation {
                    operation,
                    knowledge: Knowledge::RejectedCertain(RejectReason::Fenced),
                }))
            },
        )?;
        if start > position {
            return Err(Fault::Corrupt);
        }
        let consumed = start
            .checked_add(out.len())
            .ok_or(ReadError::LimitExceeded)?;
        Ok((out, position > consumed))
    }

    fn read_snapshot(&self, budget: ReadBudget) -> Result<DurableState, Fault> {
        // One read transaction for the whole state, even on the writer's
        // own connection: a state read in several snapshots is a trap.
        let read = self.conn.unchecked_transaction()?;
        let state = read_snapshot(&read, budget)?;
        read.commit()?;
        Ok(state)
    }
}

/// Every table of the file, decoded, as one `DurableState`. The caller
/// holds the read transaction.
fn read_snapshot(conn: &Connection, budget: ReadBudget) -> Result<DurableState, Fault> {
    {
        let meta = read_meta(conn)?;
        reader::snapshot(conn, &meta, budget)?;
        let mut sessions = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT id, client_id, revision, state, expiry, opened_by FROM sessions",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                sessions.insert(
                    SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?)
                        .map_err(|_| Fault::Corrupt)?,
                    SessionRecord {
                        client_id: reader::copy_blob(row, 1, budget.max_field_bytes)?,
                        revision: Revision::new(blob_u64(reader::borrow_blob(row, 2, 8)?)?),
                        state: code_session_state(row.get(3)?)?,
                        expiry: sessions::decode_expiry(row.get(4)?)?,
                        opened_by: sessions::attachment(
                            reader::optional_blob(row, 5, 32)?.as_deref(),
                            &meta,
                        )?,
                    },
                );
            }
        }
        let mut routes: BTreeMap<Vec<u8>, Vec<RouteTarget>> = BTreeMap::new();
        {
            let mut statement =
                conn.prepare("SELECT topic, session, qos FROM routes ORDER BY topic, position")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                routes
                    .entry(reader::copy_blob(row, 0, budget.max_field_bytes)?)
                    .or_default()
                    .push(RouteTarget {
                        session: SessionId::from_be_slice(reader::borrow_blob(row, 1, 8)?)
                            .map_err(|_| Fault::Corrupt)?,
                        qos: code_qos(row.get(2)?)?,
                    });
            }
        }
        let mut operations = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT id, recipe, decision, publication, exchange, reason, resource, facts FROM operations",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                operations.insert(
                    OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    OperationRecord {
                        recipe: reader::copy_blob(row, 1, budget.max_field_bytes)?,
                        decision: decision_of(row, 2)?.decode()?,
                    },
                );
            }
        }
        let mut fences = BTreeSet::new();
        {
            let mut statement = conn.prepare("SELECT id FROM fences")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                fences.insert(
                    OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                );
            }
        }
        let mut publications = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT id, operation, source, topic, qos, retain, payload FROM publications",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                publications.insert(
                    PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    PublicationRecord {
                        operation: OperationId::from_be_slice(reader::borrow_blob(row, 1, 32)?)
                            .map_err(|_| Fault::Corrupt)?,
                        source: SessionId::from_be_slice(reader::borrow_blob(row, 2, 8)?)
                            .map_err(|_| Fault::Corrupt)?,
                        topic: reader::copy_blob(row, 3, budget.max_field_bytes)?,
                        qos: code_qos(row.get(4)?)?,
                        retain: row.get::<_, i64>(5)? == 1,
                        payload: PayloadId::from_be_slice(reader::borrow_blob(row, 6, 32)?)
                            .map_err(|_| Fault::Corrupt)?,
                    },
                );
            }
        }
        let mut payloads = BTreeMap::new();
        {
            let mut statement = conn.prepare("SELECT id, bytes, refs FROM payloads")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                payloads.insert(
                    PayloadId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    PayloadRecord {
                        bytes: reader::copy_blob(row, 1, budget.max_field_bytes)?,
                        references: blob_u64(reader::borrow_blob(row, 2, 8)?)?,
                    },
                );
            }
        }
        let mut obligations = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT publication, idx, target, target_revision, qos, cause, phase FROM obligations",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let index: i64 = row.get(1)?;
                obligations.insert(
                    DeliveryId {
                        publication: PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                            .map_err(|_| Fault::Corrupt)?,
                        index: u32::try_from(index).map_err(|_| Fault::Corrupt)?,
                    },
                    ObligationRecord {
                        target: SessionId::from_be_slice(reader::borrow_blob(row, 2, 8)?)
                            .map_err(|_| Fault::Corrupt)?,
                        target_revision: Revision::new(blob_u64(reader::borrow_blob(row, 3, 8)?)?),
                        qos: code_qos(row.get(4)?)?,
                        cause: code_cause(row.get(5)?)?,
                        phase: code_obligation_phase(row.get(6)?)?,
                    },
                );
            }
        }
        let mut outbound = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT publication,idx,target,target_revision,packet_id,claim FROM outbound",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let delivery = DeliveryId {
                    publication: PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                        .map_err(|_| Fault::Corrupt)?,
                    index: u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Fault::Corrupt)?,
                };
                let target = SessionId::from_be_slice(reader::borrow_blob(row, 2, 8)?)
                    .map_err(|_| Fault::Corrupt)?;
                let target_revision = Revision::new(blob_u64(reader::borrow_blob(row, 3, 8)?)?);
                let packet_id = u16::try_from(row.get::<_, i64>(4)?)
                    .ok()
                    .and_then(OutboundPacketId::new)
                    .ok_or(Fault::Corrupt)?;
                let claim = OperationId::from_be_slice(reader::borrow_blob(row, 5, 32)?)
                    .map_err(|_| Fault::Corrupt)?;
                let record = OutboundRecord {
                    target,
                    target_revision,
                    packet_id,
                    claim,
                };
                if !obligations
                    .get(&delivery)
                    .is_some_and(|obligation: &ObligationRecord| {
                        obligation.target == target
                            && obligation.target_revision == target_revision
                            && obligation.qos == Qos::AtLeastOnce
                    })
                    || claim.incarnation() != meta.ledger.incarnation()
                {
                    return Err(Fault::Corrupt);
                }
                outbound.insert(delivery, record);
            }
        }
        let mut inbound = BTreeMap::new();
        {
            let mut statement = conn
                .prepare("SELECT session, packet_id, exchange, publication, phase FROM inbound")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let packet: i64 = row.get(1)?;
                inbound.insert(
                    (
                        SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?)
                            .map_err(|_| Fault::Corrupt)?,
                        u16::try_from(packet)
                            .ok()
                            .and_then(InboundPacketId::new)
                            .ok_or(Fault::Corrupt)?,
                    ),
                    InboundRecord {
                        exchange: blob_exchange(reader::borrow_blob(row, 2, 16)?)?,
                        publication: PublicationId::from_be_slice(reader::borrow_blob(row, 3, 32)?)
                            .map_err(|_| Fault::Corrupt)?,
                        phase: code_inbound_phase(row.get(4)?)?,
                    },
                );
            }
        }
        Ok(DurableState {
            ledger: meta.ledger,
            collected_through: meta.collected_through,
            revision: meta.revision,
            limits: meta.limits,
            accounts: meta.accounts,
            next_session: meta.next_session,
            route_revision: meta.route_revision,
            sessions,
            routes,
            operations,
            fences,
            receipt_frontiers: receipts::all_frontiers(conn, &meta, budget)?,
            publications,
            payloads,
            obligations,
            inbound,
            outbound,
        })
    }
}

impl DurableStore for SqliteStore {
    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        self.collect_receipts_transaction(collection)
    }

    fn receipt_frontier(&self, origin: ReceiptOrigin) -> Result<Option<ReceiptClosure>, ReadError> {
        let meta = read_meta(&self.conn).map_err(|fault| fault.read_error())?;
        receipts::frontier(&self.conn, &meta, origin).map_err(|fault| fault.read_error())
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        self.collect_closed_receipts_transaction(collection)
    }

    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.read_ledger()
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        // The decision is the contract's; only an opening writes.
        if let LedgerOpening::Opens { ledger } = self.read_ledger()?.open(command)
            && let Txn::ReadFailed(error) = self.transact(|conn, meta| {
                meta.ledger = ledger;
                sessions::expire(conn, meta)?;
                Ok::<(), Abort<()>>(())
            })
        {
            return Err(error);
        }
        // The answer comes from what is durable now.
        Ok(OpeningOutcome::read(&self.read_ledger()?, command))
    }

    fn apply(&mut self, command: &Command) -> Applied {
        let settled = |outcome| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote: false,
        };
        let rejected = |(outcome, wrote)| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote,
        };
        let unknown = settled(StoreOutcome::PendingOrUnknown);
        let recipe = command.recipe.get();
        let operation = recipe.operation();
        let mut canonical = Vec::new();
        if recipe.encode_canonical(&mut canonical).is_err() {
            return settled(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            ));
        }
        let Ok(meta) = read_meta(&self.conn) else {
            return unknown;
        };

        // Logical retirement wins even while a bounded sweep leaves physical rows.
        match established(&self.conn, &meta, operation) {
            Ok(Established::Outcome(StoreOutcome::RetiredOutcomeUnavailable)) => {
                return settled(StoreOutcome::RetiredOutcomeUnavailable);
            }
            Err(_) => return unknown,
            _ => {}
        }
        // 1. Recognise an operation that was already decided.
        let Ok(record) = read_operation(&self.conn, operation) else {
            return unknown;
        };
        if let Some(record) = record {
            let same_payload = match (recipe, &command.payload, record.decision) {
                (Recipe::AdmitPublication(admit), Some(bytes), Decision::Committed(_)) => {
                    let stored: Result<Option<Vec<u8>>, _> = self
                        .conn
                        .prepare_cached("SELECT bytes FROM payloads WHERE id = ?1")
                        .and_then(|mut statement| {
                            statement
                                .query_row([admit.payload.id.to_be_bytes().as_slice()], |row| {
                                    row.get(0)
                                })
                                .optional()
                        });
                    match stored {
                        Ok(stored) => stored.is_none_or(|stored| stored == bytes.as_slice()),
                        Err(_) => return unknown,
                    }
                }
                _ => true,
            };
            if record.recipe != canonical || !same_payload {
                return Applied {
                    reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
                    committed_now: false,
                    wrote: false,
                };
            }
        }
        let phase = match established(&self.conn, &meta, operation) {
            Ok(Established::Outcome(outcome)) => return settled(outcome),
            Ok(Established::Absent(phase)) => phase,
            Err(_) => return unknown,
        };
        if !matches!(
            receipts::capacity(&self.conn, &meta, operation, Some(ReceiptClass::of(recipe))),
            Ok(true)
        ) {
            return unknown;
        }
        if phase != EpochPhase::Open {
            return rejected(self.commit_rejection(
                operation,
                &canonical,
                RejectReason::PreconditionFailed,
            ));
        }

        // 2 to 5. Guards, capacities and every row, in one transaction.
        let payload = command.payload.as_ref();
        let written = self.transact(|conn, meta| {
            let facts = match recipe {
                Recipe::AdmitPublication(admit) => stage_admission(conn, meta, admit, payload)?,
                Recipe::ReleaseInbound(release) => stage_release(conn, meta, release)?,
                Recipe::ChangeSubscriptions(command) => pubsub::subscriptions(conn, meta, command)?,
                Recipe::ClaimDelivery(command) => pubsub::claim(conn, meta, command)?,
                Recipe::FinishDelivery(command) => pubsub::finish(conn, meta, command)?,
                Recipe::ConsumeDelivery(command) => pubsub::consume(conn, meta, command)?,
                Recipe::UpdateSubscriptions(command) => pubsub::updates(conn, meta, command)?,
                Recipe::SessionLifecycle(command) => sessions::stage(conn, meta, command)?,
            };
            insert_operation(conn, operation, &canonical, Decision::Committed(facts))?;
            Ok(facts)
        });
        match written {
            Txn::Committed(facts) => Applied {
                reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
                committed_now: true,
                wrote: true,
            },
            Txn::Refused(reason) => rejected(self.commit_rejection(operation, &canonical, reason)),
            Txn::ReadFailed(_) | Txn::NotWritten => unknown,
        }
    }

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        let meta = read_meta(&self.conn).map_err(|fault| fault.read_error())?;
        match established(&self.conn, &meta, operation).map_err(|fault| fault.read_error())? {
            Established::Outcome(outcome) => Ok(Some(outcome)),
            Established::Absent(_) => Ok(None),
        }
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        let settled = |outcome| Resolved {
            outcome,
            fenced_now: false,
        };
        let written = self.transact(|conn, meta| {
            // Read again inside the transaction: the fence is written only
            // on an absence established in it.
            match established(conn, meta, operation)? {
                Established::Outcome(outcome) => return Err(Abort::Refused(Some(outcome))),
                Established::Absent(_) => {}
            }
            if !receipts::capacity(conn, meta, operation, None)? {
                return Err(Abort::Refused(None));
            }
            // A fence holds a receipt slot. Without one no fence is
            // written, and absence proves nothing: unknown.
            charge(meta, Accounts::RECEIPT).map_err(|_| Abort::Refused(None))?;
            conn.prepare_cached("INSERT INTO fences (id) VALUES (?1)")?
                .execute([operation.to_be_bytes().as_slice()])?;
            Ok(())
        });
        match written {
            Txn::Committed(()) => Resolved {
                outcome: StoreOutcome::RejectedCertain(RejectReason::Fenced),
                fenced_now: true,
            },
            Txn::Refused(Some(outcome)) => settled(outcome),
            Txn::Refused(None) | Txn::ReadFailed(_) | Txn::NotWritten => {
                settled(StoreOutcome::PendingOrUnknown)
            }
        }
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        match self.transact(|conn, meta| stage_admin(conn, meta, command)) {
            Txn::Committed(done) => Ok(done),
            Txn::Refused(refusal) => Err(refusal),
            Txn::ReadFailed(_) | Txn::NotWritten => Err(AdminRefused::WriteFailed),
        }
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        let refused = RecoveryRefused::Read;
        if limit == 0 {
            return Err(RecoveryRefused::EmptyPage);
        }
        budget.check_field(8).map_err(refused)?;
        let minimum = limit
            .checked_mul(128)
            .ok_or_else(|| refused(ReadError::LimitExceeded))?;
        budget.check_totals(limit, minimum).map_err(refused)?;
        let read = self
            .conn
            .unchecked_transaction()
            .map_err(|error| refused(reader::sql_error(&error)))?;
        let meta = read_meta(&read).map_err(|fault| refused(fault.read_error()))?;
        let revision = meta.revision;
        let start = match cursor {
            Some(cursor) if cursor.revision() != revision => {
                return Err(RecoveryRefused::FrontierMoved);
            }
            Some(cursor) => cursor.position(),
            None => 0,
        };
        if start > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(refused(ReadError::LimitExceeded));
        }
        let mut facts =
            BoundedVec::with_capacity(limit).map_err(|_| refused(ReadError::AllocationFailed))?;
        let (inventory, more) = Self::inventory(&read, &meta, start, limit, budget)
            .map_err(|fault| refused(fault.read_error()))?;
        let count = inventory.len();
        for fact in inventory {
            facts
                .try_push(fact?)
                .map_err(|_| refused(ReadError::LimitExceeded))?;
        }
        let position = start
            .checked_add(count)
            .ok_or_else(|| refused(ReadError::LimitExceeded))?;
        let next = more.then(|| RecoveryCursor::new(revision, position));
        read.commit()
            .map_err(|error| refused(reader::sql_error(&error)))?;
        Ok(RecoveryPage { facts, next })
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        pubsub::work(&self.conn, session, after, limit, budget).map_err(|fault| fault.read_error())
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        self.read_snapshot(budget)
            .map_err(|fault| fault.read_error())
    }
}

#[cfg(test)]
mod writer_lease_tests {
    use super::*;

    #[test]
    fn dropping_the_store_releases_a_lease_even_if_its_descriptor_was_duplicated() {
        let directory = std::env::temp_dir().join(format!(
            "mqttiumd-duplicated-writer-lease-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("isolated directory");
        let path = directory.join("store.sqlite");
        let store = SqliteStore::create(
            &path,
            StoreIncarnation::new(1),
            Limits {
                payload_bytes: PayloadBytes::new(4096),
                references: References::new(16),
                exchange_slots: ExchangeSlots::new(4),
                receipt_slots: ReceiptSlots::new(8),
                max_targets_per_admission: 4,
                max_topic_bytes: 256,
                max_payload_bytes: 1024,
                max_sessions: 8,
                max_route_targets: 4,
            },
        )
        .expect("healthy store");
        let before = store.snapshot().expect("healthy snapshot");
        // A descriptor inherited across fork refers to this same kernel open
        // file description. Keep a safe duplicate beyond the owner's lifetime
        // to make that otherwise narrow scheduling window deterministic.
        let inherited = store
            ._writer_lock
            .file
            .try_clone()
            .expect("duplicated descriptor");
        assert!(matches!(
            SqliteStore::open(&path),
            Err(OpenError::WriterAlive)
        ));
        drop(store);
        let reopened = SqliteStore::open(&path);
        assert!(
            reopened.is_ok(),
            "the store dropped, but an inherited descriptor kept its lease: {reopened:?}"
        );
        let reopened = reopened.expect("the owner released its lease");
        assert_eq!(reopened.snapshot().expect("healthy snapshot"), before);
        drop(inherited);
        assert!(
            matches!(SqliteStore::open(&path), Err(OpenError::WriterAlive)),
            "closing the old duplicate must not release the new owner's lease"
        );
        drop(reopened);
        drop(SqliteStore::open(&path).expect("the next owner also releases its lease"));
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}

#[cfg(test)]
mod transaction_read_tests {
    use super::*;
    use mqttiumd_contract::command::{OpeningAttempt, OpeningTicket, StartupNonce};
    use std::cell::Cell;

    std::thread_local! {
        // One-shot fault at the first metadata read *inside* a transaction.
        // Initial and final ledger reads are deliberately healthy.
        static FAILURE: Cell<Option<ReadError>> = const { Cell::new(None) };
    }

    pub(super) fn take_if_transaction(conn: &Connection) -> Option<ReadError> {
        if conn.is_autocommit() {
            None
        } else {
            FAILURE.with(|failure| failure.take())
        }
    }

    #[test]
    fn an_opening_keeps_the_read_cause_when_only_its_transactional_read_fails() {
        let directory =
            std::env::temp_dir().join(format!("mqttiumd-opening-read-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("test directory");
        let path = directory.join("store.sqlite");
        let mut store = SqliteStore::create(
            &path,
            StoreIncarnation::new(1),
            Limits {
                payload_bytes: PayloadBytes::new(4096),
                references: References::new(16),
                exchange_slots: ExchangeSlots::new(4),
                receipt_slots: ReceiptSlots::new(8),
                max_targets_per_admission: 4,
                max_topic_bytes: 256,
                max_payload_bytes: 1024,
                max_sessions: 8,
                max_route_targets: 4,
            },
        )
        .expect("healthy store");
        let before = store.snapshot().expect("healthy initial snapshot");
        let command = OpenEpoch {
            attempt: OpeningAttempt {
                startup: StartupNonce::new([3; 16]),
                ticket: OpeningTicket::new(1),
            },
            incarnation: before.ledger.incarnation(),
            after: before.ledger.last_opened(),
        };
        for cause in [
            ReadError::InvalidData,
            ReadError::LimitExceeded,
            ReadError::Unavailable,
            ReadError::Io,
            ReadError::AllocationFailed,
        ] {
            FAILURE.with(|failure| failure.set(Some(cause)));
            assert_eq!(store.open_epoch(&command), Err(cause));
            assert_eq!(store.snapshot().expect("fault is one-shot"), before);
        }
        assert!(matches!(
            store.open_epoch(&command),
            Ok(OpeningOutcome::Committed(_))
        ));
        drop(store);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
