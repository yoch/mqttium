//! Schema-5 explicit retention, attachment guards and bounded atomic cleanup.
use super::*;
use mqttiumd_contract::session::{
    SessionAction, SessionCancellations, SessionChange, SessionEndReason, SessionExpiry,
    SessionGuard, SessionLifecycle,
};

pub(super) fn expiry_code(expiry: SessionExpiry) -> i64 {
    match expiry {
        SessionExpiry::OnDisconnect => 0,
        SessionExpiry::Never => 1,
    }
}
pub(super) fn decode_expiry(value: i64) -> Result<SessionExpiry, Corrupt> {
    match value {
        0 => Ok(SessionExpiry::OnDisconnect),
        1 => Ok(SessionExpiry::Never),
        _ => Err(Corrupt),
    }
}
pub(super) fn attachment(bytes: Option<&[u8]>, meta: &Meta) -> Result<Option<OperationId>, Fault> {
    bytes
        .map(|bytes| {
            let id = OperationId::from_be_slice(bytes).map_err(|_| Corrupt)?;
            if id.incarnation() != meta.ledger.incarnation()
                || id.epoch().get() == 0
                || id.epoch() > meta.ledger.last_opened()
            {
                return Err(Fault::Corrupt);
            }
            Ok(id)
        })
        .transpose()
}

type CurrentSession = (SessionId, Revision, SessionExpiry, Option<OperationId>);

pub(super) fn validate_metadata(
    conn: &Connection,
    meta: &Meta,
    only: Option<SessionId>,
) -> Result<(), Fault> {
    let mut statement = conn.prepare_cached(
        "SELECT state,expiry,opened_by FROM sessions WHERE (?1 IS NULL OR id=?1) LIMIT ?2",
    )?;
    let mut rows = statement.query(params![
        only.map(|id| id.to_be_bytes().to_vec()),
        i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Corrupt)?
    ])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count = count.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if count > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(Fault::Read(ReadError::LimitExceeded));
        }
        code_session_state(row.get(0)?)?;
        decode_expiry(row.get(1)?)?;
        let binding = reader::optional_blob(row, 2, 32)?;
        attachment(binding.as_deref(), meta)?;
    }
    Ok(())
}

fn active_for_client(
    conn: &Connection,
    meta: &Meta,
    client: &[u8],
) -> Result<Option<CurrentSession>, Fault> {
    let mut stmt = conn.prepare_cached(
        "SELECT id,revision,state,expiry,opened_by FROM sessions WHERE client_id=?1 LIMIT 2",
    )?;
    let mut rows = stmt.query([client])?;
    let mut found = None;
    while let Some(row) = rows.next()? {
        let state = code_session_state(row.get(2)?)?;
        let expiry = decode_expiry(row.get(3)?)?;
        let binding = reader::optional_blob(row, 4, 32)?;
        let binding = attachment(binding.as_deref(), meta)?;
        if state == SessionState::Active {
            if found.is_some() {
                return Err(Fault::Corrupt);
            }
            found = Some((
                SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?).map_err(|_| Corrupt)?,
                Revision::new(blob_u64(reader::borrow_blob(row, 1, 8)?)?),
                expiry,
                binding,
            ));
        }
    }
    Ok(found)
}

fn count(conn: &Connection, sql: &str, key: &[u8], maximum: u64) -> Result<usize, Fault> {
    let maximum = maximum.min(MAX_RECOVERY_INVENTORY_ROWS as u64);
    let number: i64 = conn.prepare_cached(sql)?.query_row(
        params![key, i64::try_from(maximum + 1).map_err(|_| Corrupt)?],
        |row| row.get(0),
    )?;
    let number = usize::try_from(number).map_err(|_| Corrupt)?;
    if number as u64 > maximum {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    Ok(number)
}

/// Preflight metadata only. Ordinary receipt recipes and payload bytes do not
/// consume this control's memory or require spare business capacity.
pub(super) fn erase(
    conn: &Connection,
    meta: &mut Meta,
    session: SessionId,
) -> Result<SessionCancellations, Abort<AdminRefused>> {
    let broken = Abort::Refused(AdminRefused::Inconsistent);
    let revision = match session_row(conn, session)? {
        Some((revision, SessionState::Active)) => revision,
        _ => return Err(Abort::Refused(AdminRefused::UnknownSession)),
    };
    revision
        .checked_next()
        .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
    let key = session.to_be_bytes();
    let markers = count(
        conn,
        "SELECT count(*) FROM (SELECT 1 FROM inbound WHERE session=?1 LIMIT ?2)",
        &key,
        meta.limits.exchange_slots.get(),
    )?;
    let deliveries = count(
        conn,
        "SELECT count(*) FROM (SELECT 1 FROM obligations WHERE target=?1 LIMIT ?2)",
        &key,
        meta.limits.references.get(),
    )?;
    let routes = count(
        conn,
        "SELECT count(*) FROM (SELECT 1 FROM routes WHERE session=?1 LIMIT ?2)",
        &key,
        MAX_RECOVERY_INVENTORY_ROWS as u64,
    )?;
    let total = markers
        .checked_add(deliveries)
        .and_then(|n| n.checked_add(routes))
        .ok_or(Fault::Read(ReadError::LimitExceeded))?;
    ReadBudget::SNAPSHOT
        .check_totals(
            total,
            total.checked_mul(256).ok_or(ReadError::LimitExceeded)?,
        )
        .map_err(Fault::Read)?;
    let mut meter = reader::Meter::new(ReadBudget::SNAPSHOT);
    {
        let mut stmt = conn.prepare_cached(
            "SELECT publication,exchange,packet_id,phase FROM inbound WHERE session=?1",
        )?;
        let mut rows = stmt.query([key.as_slice()])?;
        while let Some(row) = rows.next()? {
            meter.charge(160)?;
            PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?).map_err(|_| Corrupt)?;
            if blob_exchange(reader::borrow_blob(row, 1, 16)?)?.session != session {
                return Err(broken);
            }
            let raw: i64 = row.get(2)?;
            if u16::try_from(raw)
                .ok()
                .and_then(InboundPacketId::new)
                .is_none()
            {
                return Err(broken);
            }
            code_inbound_phase(row.get(3)?)?;
        }
    }
    let mut ended = Vec::new();
    ended
        .try_reserve_exact(deliveries)
        .map_err(|_| Abort::Refused(AdminRefused::WriteFailed))?;
    {
        let mut stmt = conn.prepare_cached("SELECT o.publication,o.idx,o.target_revision,o.qos,o.cause,o.phase,c.packet_id,c.claim,c.target,c.target_revision FROM obligations o LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx WHERE o.target=?1 ORDER BY o.publication,o.idx")?;
        let mut rows = stmt.query([key.as_slice()])?;
        while let Some(row) = rows.next()? {
            meter.charge(256)?;
            let publication = PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                .map_err(|_| Corrupt)?;
            let index = u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Corrupt)?;
            let target_revision = Revision::new(blob_u64(reader::borrow_blob(row, 2, 8)?)?);
            let qos = code_qos(row.get(3)?)?;
            code_cause(row.get(4)?)?;
            code_obligation_phase(row.get(5)?)?;
            if target_revision != revision {
                return Err(broken);
            }
            let packet: Option<i64> = row.get(6)?;
            if let Some(packet) = packet {
                if qos != Qos::AtLeastOnce
                    || u16::try_from(packet)
                        .ok()
                        .and_then(OutboundPacketId::new)
                        .is_none()
                {
                    return Err(broken);
                }
                let operation = OperationId::from_be_slice(reader::borrow_blob(row, 7, 32)?)
                    .map_err(|_| Corrupt)?;
                if operation.incarnation() != meta.ledger.incarnation()
                    || reader::borrow_blob(row, 8, 8)? != key
                    || blob_u64(reader::borrow_blob(row, 9, 8)?)? != revision.get()
                {
                    return Err(broken);
                }
            }
            ended.push((publication, index));
        }
    }
    {
        let mut stmt = conn.prepare_cached("SELECT topic,qos FROM routes WHERE session=?1")?;
        let mut rows = stmt.query([key.as_slice()])?;
        while let Some(row) = rows.next()? {
            let topic = reader::borrow_blob(
                row,
                0,
                meta.limits
                    .max_topic_bytes
                    .min(ReadBudget::SNAPSHOT.max_field_bytes),
            )?;
            meter.charge(
                128usize
                    .checked_add(topic.len())
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            code_qos(row.get(1)?)?;
        }
    }
    conn.prepare_cached("DELETE FROM publications WHERE id IN (SELECT publication FROM inbound WHERE session=?1) AND NOT EXISTS (SELECT 1 FROM obligations WHERE publication=publications.id) AND NOT EXISTS (SELECT 1 FROM inbound WHERE publication=publications.id AND session!=?1)")?.execute([key.as_slice()])?;
    let removed = conn
        .prepare_cached("DELETE FROM inbound WHERE session=?1")?
        .execute([key.as_slice()])?;
    if removed != markers {
        return Err(broken);
    }
    meta.accounts.exchange_slots = meta
        .accounts
        .exchange_slots
        .checked_sub(ExchangeSlots::new(markers as u64))
        .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    for (publication, index) in ended {
        end_obligation(conn, meta, &publication.to_be_bytes(), i64::from(index))?;
    }
    let removed = conn
        .prepare_cached("DELETE FROM routes WHERE session=?1")?
        .execute([key.as_slice()])?;
    if removed != routes {
        return Err(broken);
    }
    if routes != 0 {
        meta.route_revision = meta
            .route_revision
            .checked_next()
            .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
    }
    if conn
        .prepare_cached("DELETE FROM sessions WHERE id=?1")?
        .execute([key.as_slice()])?
        != 1
    {
        return Err(broken);
    }
    Ok(SessionCancellations {
        inbound: u32::try_from(markers).map_err(|_| Corrupt)?,
        deliveries: u32::try_from(deliveries).map_err(|_| Corrupt)?,
        routes: u32::try_from(routes).map_err(|_| Corrupt)?,
    })
}

fn lifecycle_erase(
    conn: &Connection,
    meta: &mut Meta,
    session: SessionId,
) -> Guard<SessionCancellations> {
    erase(conn, meta, session).map_err(|error| match error {
        Abort::Read(error) => Abort::Read(error),
        Abort::Fault => Abort::Fault,
        Abort::Refused(_) => Abort::Refused(RejectReason::PreconditionFailed),
    })
}

pub(super) fn stage(
    conn: &Connection,
    meta: &mut Meta,
    command: &SessionLifecycle,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let change = match &command.action {
        SessionAction::Open {
            client_id,
            expected,
            clean_start,
            expiry,
        } => {
            let current = active_for_client(conn, meta, client_id.as_slice())?;
            if current.map(|(session, _, _, opened_by)| SessionGuard { session, opened_by })
                != *expected
            {
                return refused(failed);
            }
            let replace = current.is_some_and(|(_, _, previous, _)| {
                *clean_start || previous == SessionExpiry::OnDisconnect
            });
            let mut cancelled = SessionCancellations::default();
            let retired = if replace {
                let old = current.ok_or(Abort::Refused(failed))?.0;
                cancelled = lifecycle_erase(conn, meta, old)?;
                Some(old)
            } else {
                None
            };
            let (session, revision, session_present) = if let Some((session, revision, _, _)) =
                current.filter(|_| !replace)
            {
                conn.prepare_cached("UPDATE sessions SET expiry=?2,opened_by=?3 WHERE id=?1")?
                    .execute(params![
                        session.to_be_bytes().as_slice(),
                        expiry_code(*expiry),
                        command.operation.to_be_bytes().as_slice()
                    ])?;
                (session, revision, true)
            } else {
                let active: i64 = conn.query_row(
                    "SELECT count(*) FROM (SELECT 1 FROM sessions WHERE state=1 LIMIT ?1)",
                    [
                        i64::try_from(meta.limits.max_sessions.checked_add(1).ok_or(Corrupt)?)
                            .map_err(|_| Corrupt)?,
                    ],
                    |row| row.get(0),
                )?;
                if usize::try_from(active).map_err(|_| Corrupt)? >= meta.limits.max_sessions {
                    return refused(RejectReason::SessionCapacityExceeded);
                }
                let session = meta.next_session;
                meta.next_session = session.checked_next().map_err(|_| Abort::Refused(failed))?;
                let revision = Revision::new(1);
                conn.prepare_cached("INSERT INTO sessions(id,revision,state,client_id,expiry,opened_by) VALUES(?1,?2,1,?3,?4,?5)")?.execute(params![session.to_be_bytes().as_slice(),revision.to_be_bytes().as_slice(),client_id.as_slice(),expiry_code(*expiry),command.operation.to_be_bytes().as_slice()])?;
                (session, revision, false)
            };
            SessionChange::Opened {
                session,
                revision,
                session_present,
                expiry: *expiry,
                retired,
                route_revision: meta.route_revision,
                cancelled,
            }
        }
        SessionAction::Close { guard, reason } => {
            let row = conn
                .prepare_cached("SELECT state,expiry,opened_by FROM sessions WHERE id=?1")?
                .query_row([guard.session.to_be_bytes().as_slice()], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        reader::optional_blob(row, 2, 32)?,
                    ))
                })
                .optional()?;
            let Some((state, expiry, opened_by)) = row else {
                return refused(failed);
            };
            let expiry = decode_expiry(expiry)?;
            if code_session_state(state)? != SessionState::Active
                || attachment(opened_by.as_deref(), meta)? != guard.opened_by
                || (matches!(
                    reason,
                    SessionEndReason::TransportLost | SessionEndReason::Startup
                ) && expiry != SessionExpiry::OnDisconnect)
            {
                return refused(failed);
            }
            let cancelled = lifecycle_erase(conn, meta, guard.session)?;
            SessionChange::Closed {
                session: guard.session,
                route_revision: meta.route_revision,
                cancelled,
            }
        }
    };
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::SessionLifecycle(change))
}

/// The actual epoch opening has fenced the previous process. A repeated opener
/// never reaches this function and cannot expire a current-process attachment.
pub(super) fn expire(conn: &Connection, meta: &mut Meta) -> Result<(), Abort<()>> {
    let mut stmt =
        conn.prepare_cached("SELECT id,expiry,state,opened_by FROM sessions ORDER BY id LIMIT ?1")?;
    let mut rows = stmt.query([i64::try_from(
        meta.limits
            .max_sessions
            .min(MAX_RECOVERY_INVENTORY_ROWS)
            .checked_add(1)
            .ok_or(Corrupt)?,
    )
    .map_err(|_| Corrupt)?])?;
    let mut expired = Vec::new();
    expired
        .try_reserve_exact(meta.limits.max_sessions.min(MAX_RECOVERY_INVENTORY_ROWS))
        .map_err(|_| ReadError::AllocationFailed)?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count = count.checked_add(1).ok_or(Corrupt)?;
        if count > meta.limits.max_sessions || count > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(ReadError::LimitExceeded.into());
        }
        let expiry = decode_expiry(row.get(1)?)?;
        code_session_state(row.get(2)?)?;
        let opened_by = reader::optional_blob(row, 3, 32)?;
        attachment(opened_by.as_deref(), meta)?;
        if expiry == SessionExpiry::OnDisconnect {
            expired.push(
                SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?).map_err(|_| Corrupt)?,
            );
        }
    }
    drop(rows);
    drop(stmt);
    for session in expired {
        erase(conn, meta, session).map_err(|error| match error {
            Abort::Read(error) => Abort::Read(error),
            Abort::Fault => Abort::Fault,
            Abort::Refused(AdminRefused::WriteFailed) => Abort::Read(ReadError::AllocationFailed),
            Abort::Refused(_) => Abort::Read(ReadError::InvalidData),
        })?;
    }
    Ok(())
}

pub(super) fn encode(change: SessionChange) -> Vec<u8> {
    let mut out = Vec::new();
    let (session, route, cancelled) = match change {
        SessionChange::Opened {
            session,
            revision,
            session_present,
            expiry,
            retired,
            route_revision,
            cancelled,
        } => {
            out.push(0);
            out.extend_from_slice(&session.to_be_bytes());
            out.extend_from_slice(&revision.to_be_bytes());
            out.push(u8::from(session_present));
            out.push(expiry_code(expiry) as u8);
            out.push(u8::from(retired.is_some()));
            out.extend_from_slice(&retired.map_or(0, SessionId::get).to_be_bytes());
            (session, route_revision, cancelled)
        }
        SessionChange::Closed {
            session,
            route_revision,
            cancelled,
        } => {
            out.push(1);
            out.extend_from_slice(&session.to_be_bytes());
            (session, route_revision, cancelled)
        }
    };
    let _ = session;
    out.extend_from_slice(&route.to_be_bytes());
    out.extend_from_slice(&cancelled.inbound.to_be_bytes());
    out.extend_from_slice(&cancelled.deliveries.to_be_bytes());
    out.extend_from_slice(&cancelled.routes.to_be_bytes());
    out
}
pub(super) fn decode(bytes: &[u8]) -> Result<CommitFacts, Corrupt> {
    fn session(bytes: &[u8]) -> Result<SessionId, Corrupt> {
        SessionId::from_be_slice(bytes).map_err(|_| Corrupt)
    }
    fn u32_at(bytes: &[u8]) -> Result<u32, Corrupt> {
        Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| Corrupt)?))
    }
    let (offset, id, opened) = match bytes.first() {
        Some(0) if bytes.len() == 48 => (28, session(&bytes[1..9])?, true),
        Some(1) if bytes.len() == 29 => (9, session(&bytes[1..9])?, false),
        _ => return Err(Corrupt),
    };
    let route_revision = Revision::new(blob_u64(&bytes[offset..offset + 8])?);
    let cancelled = SessionCancellations {
        inbound: u32_at(&bytes[offset + 8..offset + 12])?,
        deliveries: u32_at(&bytes[offset + 12..offset + 16])?,
        routes: u32_at(&bytes[offset + 16..offset + 20])?,
    };
    Ok(CommitFacts::SessionLifecycle(if opened {
        let session_present = match bytes[17] {
            0 => false,
            1 => true,
            _ => return Err(Corrupt),
        };
        let retired = match bytes[19] {
            0 if blob_u64(&bytes[20..28])? == 0 => None,
            1 => Some(session(&bytes[20..28])?),
            _ => return Err(Corrupt),
        };
        SessionChange::Opened {
            session: id,
            revision: Revision::new(blob_u64(&bytes[9..17])?),
            session_present,
            expiry: decode_expiry(i64::from(bytes[18]))?,
            retired,
            route_revision,
            cancelled,
        }
    } else {
        SessionChange::Closed {
            session: id,
            route_revision,
            cancelled,
        }
    }))
}
