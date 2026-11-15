//! Bounded application copies and stable classification of driver failures.
use rusqlite::types::{Type, ValueRef};
use rusqlite::{Connection, Row};

use mqttiumd_store_api::{ReadBudget, ReadError};

use super::{Fault, MAX_CLIENT_ID_BYTES, Meta};

/// A driver closure can carry a read cause without copying the bad value.
#[derive(Debug)]
struct CopyRefused(ReadError);

impl std::fmt::Display for CopyRefused {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(self.0.code())
    }
}

impl std::error::Error for CopyRefused {}

pub(crate) fn sql_error(error: &rusqlite::Error) -> ReadError {
    use rusqlite::ErrorCode;
    match error {
        rusqlite::Error::FromSqlConversionFailure(_, _, cause) => cause
            .downcast_ref::<CopyRefused>()
            .map_or(ReadError::InvalidData, |cause| cause.0),
        rusqlite::Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::OutOfMemory => ReadError::AllocationFailed,
            ErrorCode::TooBig => ReadError::LimitExceeded,
            ErrorCode::SystemIoFailure | ErrorCode::DiskFull | ErrorCode::NoLargeFileSupport => {
                ReadError::Io
            }
            ErrorCode::PermissionDenied
            | ErrorCode::DatabaseBusy
            | ErrorCode::DatabaseLocked
            | ErrorCode::ReadOnly
            | ErrorCode::CannotOpen
            | ErrorCode::FileLockingProtocolFailed
            | ErrorCode::AuthorizationForStatementDenied
            | ErrorCode::OperationAborted
            | ErrorCode::OperationInterrupted => ReadError::Unavailable,
            // A missing table/column, wrong type or damaged image is data/schema
            // invalidity. No classification depends on the engine's text.
            _ => ReadError::InvalidData,
        },
        _ => ReadError::InvalidData,
    }
}

fn refused(index: usize, error: ReadError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Blob, Box::new(CopyRefused(error)))
}

/// Borrow before checking a field; this never builds an application Vec.
pub(crate) fn borrow_blob<'a>(
    row: &'a Row<'_>,
    index: usize,
    max: usize,
) -> Result<&'a [u8], Fault> {
    let ValueRef::Blob(bytes) = row.get_ref(index)? else {
        return Err(Fault::Corrupt);
    };
    if bytes.len() > max {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    Ok(bytes)
}

pub(crate) fn copy_blob(row: &Row<'_>, index: usize, max: usize) -> rusqlite::Result<Vec<u8>> {
    let ValueRef::Blob(bytes) = row.get_ref(index)? else {
        return Err(refused(index, ReadError::InvalidData));
    };
    if bytes.len() > max {
        return Err(refused(index, ReadError::LimitExceeded));
    }
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(bytes.len())
        .map_err(|_| refused(index, ReadError::AllocationFailed))?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

pub(crate) fn optional_blob(
    row: &Row<'_>,
    index: usize,
    max: usize,
) -> rusqlite::Result<Option<Vec<u8>>> {
    match row.get_ref(index)? {
        ValueRef::Null => Ok(None),
        _ => copy_blob(row, index, max).map(Some),
    }
}

/// Charges virtual rows, with deterministic overhead and logical bytes.
pub(crate) struct Meter {
    budget: ReadBudget,
    rows: usize,
    bytes: usize,
}

impl Meter {
    pub(crate) fn new(budget: ReadBudget) -> Self {
        Self {
            budget,
            rows: 0,
            bytes: 0,
        }
    }

    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), ReadError> {
        self.rows = self.rows.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(ReadError::LimitExceeded)?;
        self.budget.check_totals(self.rows, self.bytes)
    }

    /// Count every selected column without copying it. SQL scalar cells cost
    /// eight bytes; nullable cells cost zero when absent.
    fn row(&mut self, row: &Row<'_>, fields: &[(usize, usize)]) -> Result<(), Fault> {
        let mut bytes = 128usize;
        for index in 0..row.as_ref().column_count() {
            let size = match row.get_ref(index)? {
                ValueRef::Null => 0,
                ValueRef::Integer(_) | ValueRef::Real(_) => 8,
                ValueRef::Blob(value) | ValueRef::Text(value) => {
                    self.budget.check_field(value.len())?;
                    value.len()
                }
            };
            bytes = bytes.checked_add(size).ok_or(ReadError::LimitExceeded)?;
        }
        for &(index, max) in fields {
            match row.get_ref(index)? {
                ValueRef::Null => (),
                ValueRef::Blob(value) if value.len() <= max => (),
                ValueRef::Blob(_) => return Err(Fault::Read(ReadError::LimitExceeded)),
                _ => return Err(Fault::Corrupt),
            }
        }
        self.charge(bytes)?;
        Ok(())
    }
}

/// Preflight the frozen read snapshot, with a stop at the first row or field
/// beyond budget. Materialisation happens only after the whole preflight.
pub(crate) fn snapshot(conn: &Connection, meta: &Meta, budget: ReadBudget) -> Result<(), Fault> {
    let mut meter = Meter::new(budget);
    budget.check_field(8)?;
    meter.charge(256)?;
    let tables: [(&str, &[(usize, usize)]); 8] = [
        (
            "SELECT id, client_id, revision, state FROM sessions",
            &[(0, 8), (1, MAX_CLIENT_ID_BYTES), (2, 8)],
        ),
        (
            "SELECT topic, session, qos FROM routes",
            &[(0, meta.limits.max_topic_bytes), (1, 8)],
        ),
        (
            "SELECT id, recipe, decision, publication, exchange, reason, resource FROM operations",
            &[(0, 32), (1, budget.max_field_bytes), (3, 32), (4, 16)],
        ),
        ("SELECT id FROM fences", &[(0, 32)]),
        (
            "SELECT id, operation, source, topic, qos, retain, payload FROM publications",
            &[
                (0, 32),
                (1, 32),
                (2, 8),
                (3, meta.limits.max_topic_bytes),
                (6, 32),
            ],
        ),
        (
            "SELECT id, bytes, refs FROM payloads",
            &[(0, 32), (1, meta.limits.max_payload_bytes), (2, 8)],
        ),
        (
            "SELECT publication, idx, target, target_revision, qos, cause, phase FROM obligations",
            &[(0, 32), (2, 8), (3, 8)],
        ),
        (
            "SELECT session, packet_id, exchange, publication, phase FROM inbound",
            &[(0, 8), (2, 16), (3, 32)],
        ),
    ];
    for (sql, fields) in tables {
        let mut statement = conn.prepare(sql)?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            meter.row(row, fields)?;
        }
    }
    Ok(())
}
