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

/// A system failure never asserts that the durable contents were invalid.
pub(crate) fn io_error(error: &std::io::Error) -> ReadError {
    match error.kind() {
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::PermissionDenied
        | std::io::ErrorKind::WouldBlock
        | std::io::ErrorKind::TimedOut
        | std::io::ErrorKind::Interrupted => ReadError::Unavailable,
        std::io::ErrorKind::OutOfMemory => ReadError::AllocationFailed,
        _ => ReadError::Io,
    }
}

fn conversion_error(cause: &(dyn std::error::Error + Send + Sync + 'static)) -> ReadError {
    conversion_cause(cause, 0)
}

fn conversion_cause(
    cause: &(dyn std::error::Error + Send + Sync + 'static),
    depth: usize,
) -> ReadError {
    if depth >= 8 {
        return ReadError::Unavailable;
    }
    if let Some(cause) = cause.downcast_ref::<CopyRefused>() {
        cause.0
    } else if let Some(cause) = cause.downcast_ref::<std::io::Error>() {
        io_error(cause)
    } else if let Some(cause) = cause.downcast_ref::<rusqlite::types::FromSqlError>() {
        match cause {
            rusqlite::types::FromSqlError::Other(cause) => {
                conversion_cause(cause.as_ref(), depth + 1)
            }
            _ => ReadError::InvalidData,
        }
    } else if cause.is::<std::str::Utf8Error>() || cause.is::<std::num::TryFromIntError>() {
        ReadError::InvalidData
    } else {
        ReadError::Unavailable
    }
}

fn sqlite_error(failure: &rusqlite::ffi::Error) -> ReadError {
    use rusqlite::ErrorCode;
    match failure.code {
        ErrorCode::OutOfMemory => ReadError::AllocationFailed,
        ErrorCode::TooBig => ReadError::LimitExceeded,
        ErrorCode::SystemIoFailure | ErrorCode::DiskFull | ErrorCode::NoLargeFileSupport => {
            ReadError::Io
        }
        ErrorCode::DatabaseCorrupt
        | ErrorCode::NotADatabase
        | ErrorCode::TypeMismatch
        | ErrorCode::ConstraintViolation => ReadError::InvalidData,
        // An internal/configuration/query failure, SQLITE_ERROR included,
        // cannot prove that durable contents were invalid. SQLITE_ERROR alone
        // cannot distinguish a missing schema object from a broken query.
        _ => ReadError::Unavailable,
    }
}

pub(crate) fn sql_error(error: &rusqlite::Error) -> ReadError {
    match error {
        rusqlite::Error::FromSqlConversionFailure(_, _, cause) => conversion_error(cause.as_ref()),
        rusqlite::Error::SqliteFailure(failure, _)
        | rusqlite::Error::SqlInputError { error: failure, .. } => sqlite_error(failure),
        rusqlite::Error::IntegralValueOutOfRange(_, _)
        | rusqlite::Error::Utf8Error(_, _)
        | rusqlite::Error::InvalidColumnType(_, _, _)
        | rusqlite::Error::QueryReturnedNoRows
        | rusqlite::Error::QueryReturnedMoreThanOneRow => ReadError::InvalidData,
        rusqlite::Error::ToSqlConversionFailure(cause) => conversion_error(cause.as_ref()),
        _ => ReadError::Unavailable,
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
    if meta.ledger.opened_by().is_some() {
        budget.check_field(24)?;
    }
    meter.charge(256)?;
    let tables: [(&str, &[(usize, usize)]); 10] = [
        (
            "SELECT id, client_id, revision, state FROM sessions",
            &[(0, 8), (1, MAX_CLIENT_ID_BYTES), (2, 8)],
        ),
        (
            "SELECT topic, session, qos FROM routes",
            &[(0, meta.limits.max_topic_bytes), (1, 8)],
        ),
        (
            "SELECT id, recipe, decision, publication, exchange, reason, resource, facts FROM operations",
            &[
                (0, 32),
                (1, budget.max_field_bytes),
                (3, 32),
                (4, 16),
                (7, 96),
            ],
        ),
        ("SELECT id FROM fences", &[(0, 32)]),
        (
            "SELECT epoch,owner,consumed_through,publication_through,payload_through FROM receipt_origins",
            &[(0, 8), (1, 8), (2, 8), (3, 8), (4, 8)],
        ),
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
            "SELECT publication,idx,target,target_revision,packet_id,claim FROM outbound",
            &[(0, 32), (2, 8), (3, 8), (5, 32)],
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

#[cfg(test)]
mod tests {
    use super::*;

    fn driver(code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            Some("display text is not evidence of InvalidData".to_owned()),
        )
    }

    #[test]
    fn a_driver_or_query_failure_does_not_invent_invalid_durable_data() {
        for error in [
            driver(rusqlite::ffi::SQLITE_INTERNAL),
            driver(rusqlite::ffi::SQLITE_MISUSE),
            driver(rusqlite::ffi::SQLITE_ERROR),
            driver(rusqlite::ffi::SQLITE_SCHEMA),
            driver(rusqlite::ffi::SQLITE_RANGE),
            driver(rusqlite::ffi::SQLITE_NOTFOUND),
            rusqlite::Error::SqliteSingleThreadedMode,
            rusqlite::Error::InvalidQuery,
            rusqlite::Error::InvalidParameterCount(2, 1),
            rusqlite::Error::InvalidColumnIndex(12),
            rusqlite::Error::InvalidColumnName("missing".to_owned()),
        ] {
            assert_eq!(sql_error(&error), ReadError::Unavailable, "{error:?}");
        }
    }

    #[test]
    fn structured_driver_codes_preserve_data_limits_io_and_allocation_causes() {
        for (code, expected) in [
            (rusqlite::ffi::SQLITE_CORRUPT, ReadError::InvalidData),
            (rusqlite::ffi::SQLITE_NOTADB, ReadError::InvalidData),
            (rusqlite::ffi::SQLITE_MISMATCH, ReadError::InvalidData),
            (rusqlite::ffi::SQLITE_CONSTRAINT, ReadError::InvalidData),
            (rusqlite::ffi::SQLITE_TOOBIG, ReadError::LimitExceeded),
            (rusqlite::ffi::SQLITE_NOMEM, ReadError::AllocationFailed),
            (rusqlite::ffi::SQLITE_IOERR_READ, ReadError::Io),
            (rusqlite::ffi::SQLITE_FULL, ReadError::Io),
            (rusqlite::ffi::SQLITE_CANTOPEN, ReadError::Unavailable),
            (rusqlite::ffi::SQLITE_BUSY, ReadError::Unavailable),
            (rusqlite::ffi::SQLITE_AUTH, ReadError::Unavailable),
        ] {
            assert_eq!(sql_error(&driver(code)), expected);
        }
    }

    #[test]
    fn a_conversion_failure_retains_a_structured_io_cause() {
        let error = rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Blob,
            Box::new(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
        );
        assert_eq!(sql_error(&error), ReadError::Io);
        let error = rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Blob,
            Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        );
        assert_eq!(sql_error(&error), ReadError::Unavailable);
        let nested = rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Blob,
            Box::new(rusqlite::types::FromSqlError::Other(Box::new(
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            ))),
        );
        assert_eq!(sql_error(&nested), ReadError::Io);
    }
}
