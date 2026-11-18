//! Storage authority for a static, authenticated product data set.
//! Insertion and cleanup run inside the existing session transactions.

use super::{Connection, OpenError, SCHEMA_VERSION, params, reader};
use std::collections::BTreeMap;

pub const SECURE_SCHEMA_VERSION: i64 = 11;
const MAX_OWNERS: usize = 128;
const MAX_CLIENT_BYTES: usize = 1024;
const MAX_PRINCIPAL_BYTES: usize = 64;

#[derive(Clone)]
pub struct StoragePolicy {
    pub fingerprint: [u8; 32],
    /// Each MQTT ClientID has exactly one owner in this data set.
    pub owners: BTreeMap<Vec<u8>, String>,
}

pub(super) fn version(policy: Option<&StoragePolicy>) -> i64 {
    if policy.is_some() {
        SECURE_SCHEMA_VERSION
    } else {
        SCHEMA_VERSION
    }
}

const CREATE: &str = "
CREATE TABLE security_policy (
    one INTEGER PRIMARY KEY CHECK (one = 1),
    fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32)
) STRICT;
CREATE TABLE security_client_owners (
    client_id BLOB PRIMARY KEY CHECK (length(client_id) BETWEEN 1 AND 1024),
    principal TEXT NOT NULL CHECK (length(principal) BETWEEN 1 AND 64)
) STRICT, WITHOUT ROWID;
CREATE TABLE security_session_owners (
    session BLOB PRIMARY KEY CHECK (length(session) = 8),
    principal TEXT NOT NULL CHECK (length(principal) BETWEEN 1 AND 64)
) STRICT, WITHOUT ROWID;
CREATE TRIGGER secure_session_insert AFTER INSERT ON sessions BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM security_client_owners WHERE client_id = NEW.client_id
    ) THEN RAISE(ABORT, 'unowned ClientID') END;
    INSERT INTO security_session_owners (session, principal)
        SELECT NEW.id, principal FROM security_client_owners WHERE client_id = NEW.client_id;
END;
CREATE TRIGGER secure_session_delete AFTER DELETE ON sessions BEGIN
    DELETE FROM security_session_owners WHERE session = OLD.id;
END;
CREATE TRIGGER secure_session_identity BEFORE UPDATE OF id, client_id ON sessions BEGIN
    SELECT RAISE(ABORT, 'immutable session identity');
END;
";

impl StoragePolicy {
    pub(super) fn validate(&self) -> Result<(), OpenError> {
        if self.owners.is_empty()
            || self.owners.len() > MAX_OWNERS
            || self.owners.iter().any(|(id, principal)| {
                id.is_empty()
                    || id.len() > MAX_CLIENT_BYTES
                    || principal.is_empty()
                    || principal.len() > MAX_PRINCIPAL_BYTES
            })
        {
            return Err(OpenError::SecurityPolicyMismatch);
        }
        Ok(())
    }

    pub(super) fn install(&self, conn: &Connection) -> Result<(), OpenError> {
        conn.execute_batch(CREATE)?;
        conn.execute(
            "INSERT INTO security_policy VALUES (1, ?1)",
            [self.fingerprint.as_slice()],
        )?;
        let mut insert = conn.prepare("INSERT INTO security_client_owners VALUES (?1, ?2)")?;
        for (id, owner) in &self.owners {
            insert.execute(params![id, owner])?;
        }
        Ok(())
    }

    pub(super) fn verify(&self, conn: &Connection) -> Result<(), OpenError> {
        let read = conn.unchecked_transaction()?;
        self.verify_in(&read)?;
        read.commit()?;
        Ok(())
    }

    pub(super) fn recorded_fingerprint(conn: &Connection) -> Result<[u8; 32], OpenError> {
        let mut stmt = conn.prepare("SELECT one, fingerprint FROM security_policy LIMIT 2")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.ok_or(OpenError::Corrupt)?;
        let fingerprint = reader::borrow_blob(row, 1, 32)
            .map_err(|fault| OpenError::Read(fault.read_error()))?
            .try_into()
            .map_err(|_| OpenError::Corrupt)?;
        if row.get::<_, i64>(0)? != 1 || rows.next()?.is_some() {
            return Err(OpenError::Corrupt);
        }
        Ok(fingerprint)
    }

    /// Verify the complete authority inside the caller's frozen snapshot.
    pub(super) fn verify_in(&self, conn: &Connection) -> Result<(), OpenError> {
        self.validate()?;
        if Self::recorded_fingerprint(conn)? != self.fingerprint {
            return Err(OpenError::SecurityPolicyMismatch);
        }
        // Delimit the exact installed trigger definitions. The SQLite schema
        // catalog strips the trailing semicolon from each CREATE statement.
        for (name, ddl) in [
            (
                "secure_session_insert",
                "secure_session_insert AFTER INSERT",
            ),
            (
                "secure_session_delete",
                "secure_session_delete AFTER DELETE",
            ),
            (
                "secure_session_identity",
                "secure_session_identity BEFORE UPDATE",
            ),
        ] {
            let prefix = format!("CREATE TRIGGER {ddl}");
            let begin = CREATE.find(&prefix).ok_or(OpenError::Corrupt)?;
            let end = CREATE[begin..].find("\nEND;").ok_or(OpenError::Corrupt)? + 4;
            let trigger_exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' AND name=?1 AND sql=?2)",
                params![name, &CREATE[begin..begin + end]], |row| row.get(0))?;
            if !trigger_exists {
                return Err(OpenError::Corrupt);
            }
        }
        {
            let mut statement = conn.prepare(
                "SELECT client_id, principal FROM security_client_owners ORDER BY client_id LIMIT 129",
            )?;
            let mut rows = statement.query([])?;
            for (badged_id, badged_owner) in &self.owners {
                let row = rows.next()?.ok_or(OpenError::Corrupt)?;
                let id = reader::borrow_blob(row, 0, MAX_CLIENT_BYTES)
                    .map_err(|e| OpenError::Read(e.read_error()))?;
                let owner = match row.get_ref(1)? {
                    rusqlite::types::ValueRef::Text(owner)
                        if owner.len() <= MAX_PRINCIPAL_BYTES =>
                    {
                        owner
                    }
                    _ => return Err(OpenError::Corrupt),
                };
                if id != badged_id.as_slice() || owner != badged_owner.as_bytes() {
                    return Err(OpenError::Corrupt);
                }
            }
            if rows.next()?.is_some() {
                return Err(OpenError::Corrupt);
            }
        }
        // Limit the inventory before joining. Every live session has exactly
        // one durable owner and termination reclaims that row atomically.
        for table in ["sessions", "security_session_owners"] {
            let count: i64 = conn.query_row(
                &format!("SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT 129)"),
                [],
                |row| row.get(0),
            )?;
            if count > MAX_OWNERS as i64 {
                return Err(OpenError::Read(
                    mqttiumd_store_api::ReadError::LimitExceeded,
                ));
            }
        }
        let inconsistent: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions s LEFT JOIN security_session_owners o ON s.id=o.session
               LEFT JOIN security_client_owners c ON s.client_id=c.client_id
               WHERE c.principal IS NULL OR o.principal IS NULL OR c.principal != o.principal)
             OR EXISTS(SELECT 1 FROM security_session_owners o LEFT JOIN sessions s ON s.id=o.session WHERE s.id IS NULL)",
            [], |row| row.get(0))?;
        if inconsistent {
            return Err(OpenError::Corrupt);
        }
        Ok(())
    }
}
