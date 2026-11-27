//! Schema-6 explicit retention, attachment guards and bounded atomic cleanup.
use super::*;
use mqttiumd_contract::session::{
    SessionAction, SessionCancellations, SessionChange, SessionDetachment, SessionEndReason,
    SessionExpiry, SessionGuard, SessionLifecycle,
};

pub(crate) fn expiry_code(expiry: SessionExpiry) -> i64 {
    match expiry {
        SessionExpiry::OnDisconnect => 0,
        SessionExpiry::Never => 1,
        SessionExpiry::After(seconds) => 2 + i64::from(seconds),
    }
}
pub(super) fn decode_expiry(value: i64) -> Result<SessionExpiry, Corrupt> {
    match value {
        0 => Ok(SessionExpiry::OnDisconnect),
        1 => Ok(SessionExpiry::Never),
        value if value >= 3 && value <= i64::from(u32::MAX) + 1 => Ok(SessionExpiry::After(
            u32::try_from(value - 2).map_err(|_| Corrupt)?,
        )),
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

type CurrentSession = (
    SessionId,
    Revision,
    SessionExpiry,
    Option<OperationId>,
    Option<SessionDetachment>,
);

pub(super) fn detachment(
    row: &rusqlite::Row<'_>,
    token_index: usize,
    deadline_index: usize,
    expiry: SessionExpiry,
    opened_by: Option<OperationId>,
    meta: &Meta,
) -> Result<Option<SessionDetachment>, Fault> {
    let token = reader::optional_blob(row, token_index, 32)?;
    let deadline = reader::optional_blob(row, deadline_index, 8)?;
    match (token, deadline) {
        (None, None) => Ok(None),
        (Some(token), deadline) if opened_by.is_some() => {
            let operation = attachment(Some(&token), meta)?.ok_or(Corrupt)?;
            let deadline = deadline.as_deref().map(blob_u64).transpose()?;
            let detached = SessionDetachment {
                operation,
                deadline,
            };
            if !detached.valid_for(expiry) {
                return Err(Fault::Corrupt);
            }
            Ok(Some(detached))
        }
        _ => Err(Fault::Corrupt),
    }
}

pub(super) fn clock(conn: &Connection) -> Result<Option<SessionClockAnchor>, Fault> {
    let mut statement =
        conn.prepare_cached("SELECT domain,tick,logical FROM session_clock ORDER BY one LIMIT 2")?;
    let mut rows = statement.query([])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let domain: [u8; 32] = reader::borrow_blob(row, 0, 32)?
        .try_into()
        .map_err(|_| Corrupt)?;
    let tick = blob_u64(reader::borrow_blob(row, 1, 8)?)?;
    let logical = blob_u64(reader::borrow_blob(row, 2, 8)?)?;
    if rows.next()?.is_some() {
        return Err(Fault::Corrupt);
    }
    Ok(Some(SessionClockAnchor {
        reading: SessionClockReading { domain, tick },
        logical,
    }))
}

pub(super) fn validate_metadata(
    conn: &Connection,
    meta: &Meta,
    only: Option<SessionId>,
) -> Result<(), Fault> {
    let mut statement = conn.prepare_cached(
        "SELECT state,expiry,opened_by,detached_by,deadline \
         FROM sessions WHERE (?1 IS NULL OR id=?1) \
         LIMIT ?2",
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
        let expiry = decode_expiry(row.get(1)?)?;
        if matches!(expiry, SessionExpiry::After(_)) && clock(conn)?.is_none() {
            return Err(Fault::Corrupt);
        }
        let binding = reader::optional_blob(row, 2, 32)?;
        let opened_by = attachment(binding.as_deref(), meta)?;
        detachment(row, 3, 4, expiry, opened_by, meta)?;
    }
    Ok(())
}

fn active_for_client(
    conn: &Connection,
    meta: &Meta,
    client: &[u8],
) -> Result<Option<CurrentSession>, Fault> {
    let mut stmt = conn.prepare_cached(
        "SELECT id,revision,state,expiry,opened_by,detached_by,deadline \
         FROM sessions WHERE client_id=?1 \
         LIMIT 2",
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
                detachment(row, 5, 6, expiry, binding, meta)?,
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
    conn.prepare_cached("UPDATE wills SET state=2 WHERE session=?1 AND state=1")?
        .execute([session.to_be_bytes().as_slice()])?;
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
        let mut stmt = conn.prepare_cached("SELECT \
                                            o.publication,o.idx,o.target_revision,o.qos,o.cause,o.phase,c.packet_id,c.claim,c.target,c.target_revision \
                                            FROM obligations o \
                                            LEFT JOIN outbound c ON c.publication=o.publication \
                                            AND c.idx=o.idx WHERE o.target=?1 \
                                            ORDER BY o.publication,o.idx")?;
        let mut rows = stmt.query([key.as_slice()])?;
        while let Some(row) = rows.next()? {
            meter.charge(256)?;
            let publication = PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                .map_err(|_| Corrupt)?;
            let index = u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Corrupt)?;
            let target_revision = Revision::new(blob_u64(reader::borrow_blob(row, 2, 8)?)?);
            let qos = code_qos(row.get(3)?)?;
            code_cause(row.get(4)?)?;
            let await_pubcomp =
                code_obligation_phase(row.get(5)?)? == ObligationPhase::AwaitPubcomp;
            if target_revision != revision {
                return Err(broken);
            }
            let packet: Option<i64> = row.get(6)?;
            if let Some(packet) = packet {
                if qos == Qos::AtMostOnce
                    || (await_pubcomp && qos != Qos::ExactlyOnce)
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
            } else if await_pubcomp {
                return Err(broken);
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
    conn.prepare_cached(
        "DELETE FROM publications WHERE id IN (SELECT publication \
         FROM inbound WHERE session=?1) AND NOT EXISTS (SELECT 1 \
         FROM obligations WHERE publication=publications.id) AND NOT EXISTS (SELECT 1 \
         FROM retained WHERE publication=publications.id) AND NOT EXISTS (SELECT 1 \
         FROM inbound WHERE publication=publications.id AND session!=?1)",
    )?
    .execute([key.as_slice()])?;
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
    if matches!(command.action, SessionAction::OpenWillAt { .. }) {
        return will::open(conn, meta, command);
    }
    if matches!(command.action, SessionAction::EndWill { .. }) {
        return will::end(conn, meta, command);
    }
    let change = match &command.action {
        SessionAction::OpenWillAt { .. } | SessionAction::EndWill { .. } => return refused(failed),
        SessionAction::Open {
            client_id,
            expected,
            clean_start,
            expiry,
        }
        | SessionAction::OpenAt {
            client_id,
            expected,
            clean_start,
            expiry,
            ..
        } => {
            if matches!(expiry, SessionExpiry::After(_)) && clock(conn)?.is_none() {
                return refused(failed);
            }
            if will::has_client(conn, client_id.as_slice())? {
                return refused(failed);
            }
            let current = active_for_client(conn, meta, client_id.as_slice())?;
            if current.map(|(session, _, _, opened_by, _)| SessionGuard { session, opened_by })
                != *expected
            {
                return refused(failed);
            }
            let now = match command.action {
                SessionAction::OpenAt { now, .. } => Some(now),
                _ => None,
            };
            if now.is_none()
                && current.is_some_and(|(_, _, _, _, detached)| {
                    detached.and_then(|d| d.deadline).is_some()
                })
            {
                return refused(failed);
            }
            let replace = current.is_some_and(|(_, _, previous, _, detached)| {
                *clean_start
                    || previous == SessionExpiry::OnDisconnect
                    || detached
                        .and_then(|d| d.deadline)
                        .zip(now)
                        .is_some_and(|(deadline, now)| deadline <= now)
            });
            let mut cancelled = SessionCancellations::default();
            let retired = if replace {
                let old = current.ok_or(Abort::Refused(failed))?.0;
                cancelled = lifecycle_erase(conn, meta, old)?;
                Some(old)
            } else {
                None
            };
            let (session, revision, session_present) = if let Some((session, revision, _, _, _)) =
                current.filter(|_| !replace)
            {
                conn.prepare_cached(
                    "UPDATE sessions SET expiry=?2,opened_by=?3,detached_by=NULL,deadline=NULL \
                     WHERE id=?1",
                )?
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
                conn.prepare_cached(
                    "INSERT INTO sessions(id,revision,state,client_id,expiry,opened_by) \
                     VALUES(?1,?2,1,?3,?4,?5)",
                )?
                .execute(params![
                    session.to_be_bytes().as_slice(),
                    revision.to_be_bytes().as_slice(),
                    client_id.as_slice(),
                    expiry_code(*expiry),
                    command.operation.to_be_bytes().as_slice()
                ])?;
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
            let Some((expiry, opened_by, _)) = metadata_for_session(conn, meta, guard.session)?
            else {
                return refused(failed);
            };
            if opened_by != guard.opened_by
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
        SessionAction::Detach {
            guard,
            expiry,
            observed_at,
        } => {
            if matches!(expiry, SessionExpiry::After(_)) && clock(conn)?.is_none() {
                return refused(failed);
            }
            let Some((_, opened_by, previous)) = metadata_for_session(conn, meta, guard.session)?
            else {
                return refused(failed);
            };
            if opened_by.is_none() || opened_by != guard.opened_by || previous.is_some() {
                return refused(failed);
            }
            let deadline = expiry
                .deadline(*observed_at)
                .ok_or(Abort::Refused(failed))?;
            if *expiry == SessionExpiry::OnDisconnect {
                let cancelled = lifecycle_erase(conn, meta, guard.session)?;
                SessionChange::Closed {
                    session: guard.session,
                    route_revision: meta.route_revision,
                    cancelled,
                }
            } else {
                let detached = SessionDetachment {
                    operation: command.operation,
                    deadline,
                };
                conn.prepare_cached(
                    "UPDATE sessions SET expiry=?2,detached_by=?3,deadline=?4 WHERE id=?1",
                )?
                .execute(params![
                    guard.session.to_be_bytes().as_slice(),
                    expiry_code(*expiry),
                    command.operation.to_be_bytes().as_slice(),
                    deadline.map(u64_blob).as_ref().map(<[u8; 8]>::as_slice)
                ])?;
                SessionChange::Detached {
                    session: guard.session,
                    expiry: *expiry,
                    detached,
                }
            }
        }
        SessionAction::Expire {
            guard,
            detached,
            now,
        } => {
            let Some((expiry, opened_by, current)) =
                metadata_for_session(conn, meta, guard.session)?
            else {
                return refused(failed);
            };
            if opened_by != guard.opened_by
                || current != Some(*detached)
                || !matches!(expiry, SessionExpiry::After(_))
                || !detached.deadline.is_some_and(|deadline| *now >= deadline)
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
    let observed = match command.action {
        SessionAction::OpenAt { now, .. } | SessionAction::Expire { now, .. } => Some(now),
        SessionAction::Detach { observed_at, .. } => Some(observed_at),
        _ => None,
    };
    if let Some(now) = observed {
        coalesce_time(conn, now)?;
    }
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::SessionLifecycle(change))
}

// A business/lifecycle commit retaining a temporal decision also retains its
// observed progress. The runtime captures logical time in this process's one
// anchored domain; advancing both coordinates preserves its affine mapping.
fn coalesce_time(conn: &Connection, now: u64) -> Guard<()> {
    let Some(anchor) = clock(conn)? else {
        return Ok(());
    };
    if now <= anchor.logical {
        return Ok(());
    }
    let delta = now
        .checked_sub(anchor.logical)
        .ok_or(Abort::Refused(RejectReason::PreconditionFailed))?;
    let tick = anchor
        .reading
        .tick
        .checked_add(delta)
        .ok_or(Abort::Refused(RejectReason::PreconditionFailed))?;
    conn.prepare_cached("UPDATE session_clock SET tick=?1,logical=?2 WHERE one=1")?
        .execute(params![u64_blob(tick).as_slice(), u64_blob(now).as_slice()])?;
    Ok(())
}

type SessionMetadata = (
    SessionExpiry,
    Option<OperationId>,
    Option<SessionDetachment>,
);

fn metadata_for_session(
    conn: &Connection,
    meta: &Meta,
    session: SessionId,
) -> Result<Option<SessionMetadata>, Fault> {
    let mut stmt = conn.prepare_cached(
        "SELECT state,expiry,opened_by,detached_by,deadline FROM sessions WHERE id=?1",
    )?;
    let mut rows = stmt.query([session.to_be_bytes().as_slice()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    if code_session_state(row.get(0)?)? != SessionState::Active {
        return Ok(None);
    }
    let expiry = decode_expiry(row.get(1)?)?;
    let opened_by = attachment(reader::optional_blob(row, 2, 32)?.as_deref(), meta)?;
    let detached = detachment(row, 3, 4, expiry, opened_by, meta)?;
    Ok(Some((expiry, opened_by, detached)))
}

pub(super) fn expire(conn: &Connection, meta: &mut Meta) -> Result<(), Abort<()>> {
    let anchor = clock(conn)?;
    let mut stmt = conn.prepare_cached(
        "SELECT id,state,expiry,opened_by,detached_by,deadline FROM sessions ORDER BY id LIMIT ?1",
    )?;
    let mut rows =
        stmt.query([
            i64::try_from(meta.limits.max_sessions.checked_add(1).ok_or(Corrupt)?)
                .map_err(|_| Corrupt)?,
        ])?;
    let mut expired = Vec::new();
    let mut detach = Vec::new();
    let bound = meta.limits.max_sessions.min(MAX_RECOVERY_INVENTORY_ROWS);
    expired
        .try_reserve_exact(bound)
        .map_err(|_| ReadError::AllocationFailed)?;
    detach
        .try_reserve_exact(bound)
        .map_err(|_| ReadError::AllocationFailed)?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count = count.checked_add(1).ok_or(Corrupt)?;
        if count > bound {
            return Err(ReadError::LimitExceeded.into());
        }
        let id = SessionId::from_be_slice(reader::borrow_blob(row, 0, 8)?).map_err(|_| Corrupt)?;
        code_session_state(row.get(1)?)?;
        let expiry = decode_expiry(row.get(2)?)?;
        let opened_by = attachment(reader::optional_blob(row, 3, 32)?.as_deref(), meta)?;
        let detached = detachment(row, 4, 5, expiry, opened_by, meta)?;
        match expiry {
            SessionExpiry::OnDisconnect => expired.push(id),
            SessionExpiry::Never => {}
            SessionExpiry::After(_) => {
                let now = anchor.ok_or(ReadError::InvalidData)?.logical;
                if let Some(detached) = detached {
                    if detached.deadline.is_some_and(|deadline| deadline <= now) {
                        expired.push(id)
                    }
                } else {
                    let token = opened_by.ok_or(ReadError::InvalidData)?;
                    let deadline = expiry
                        .deadline(now)
                        .flatten()
                        .ok_or(ReadError::InvalidData)?;
                    detach.push((id, token, deadline));
                }
            }
        }
    }
    drop(rows);
    drop(stmt);
    for (id, token, deadline) in detach {
        conn.prepare_cached("UPDATE sessions SET detached_by=?2,deadline=?3 WHERE id=?1")?
            .execute(params![
                id.to_be_bytes().as_slice(),
                token.to_be_bytes().as_slice(),
                u64_blob(deadline).as_slice()
            ])?;
    }
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
    match change {
        SessionChange::Opened {
            session,
            revision,
            session_present,
            expiry,
            retired,
            route_revision,
            cancelled,
        } => {
            let finite = matches!(expiry, SessionExpiry::After(_));
            out.push(if finite { 2 } else { 0 });
            out.extend_from_slice(&session.to_be_bytes());
            out.extend_from_slice(&revision.to_be_bytes());
            out.push(u8::from(session_present));
            if finite {
                out.extend_from_slice(&expiry.to_wire().to_be_bytes())
            } else {
                out.push(expiry_code(expiry) as u8)
            }
            out.push(u8::from(retired.is_some()));
            out.extend_from_slice(&retired.map_or(0, SessionId::get).to_be_bytes());
            append_cancel(&mut out, route_revision, cancelled);
        }
        SessionChange::Closed {
            session,
            route_revision,
            cancelled,
        } => {
            out.push(1);
            out.extend_from_slice(&session.to_be_bytes());
            append_cancel(&mut out, route_revision, cancelled);
        }
        SessionChange::Detached {
            session,
            expiry,
            detached,
        } => {
            out.push(3);
            out.extend_from_slice(&session.to_be_bytes());
            out.extend_from_slice(&expiry.to_wire().to_be_bytes());
            out.extend_from_slice(&detached.operation.to_be_bytes());
            out.push(u8::from(detached.deadline.is_some()));
            out.extend_from_slice(&detached.deadline.unwrap_or(0).to_be_bytes());
        }
    }
    out
}
fn append_cancel(out: &mut Vec<u8>, revision: Revision, cancelled: SessionCancellations) {
    out.extend_from_slice(&revision.to_be_bytes());
    out.extend_from_slice(&cancelled.inbound.to_be_bytes());
    out.extend_from_slice(&cancelled.deliveries.to_be_bytes());
    out.extend_from_slice(&cancelled.routes.to_be_bytes());
}
pub(super) fn decode(bytes: &[u8]) -> Result<CommitFacts, Corrupt> {
    fn id(bytes: &[u8]) -> Result<SessionId, Corrupt> {
        SessionId::from_be_slice(bytes).map_err(|_| Corrupt)
    }
    fn number(bytes: &[u8]) -> Result<u32, Corrupt> {
        Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| Corrupt)?))
    }
    fn boolean(byte: u8) -> Result<bool, Corrupt> {
        match byte {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Corrupt),
        }
    }
    if bytes.first() == Some(&3) && bytes.len() == 54 {
        let expiry = SessionExpiry::from_wire(number(&bytes[9..13])?).ok_or(Corrupt)?;
        let operation = OperationId::from_be_slice(&bytes[13..45]).map_err(|_| Corrupt)?;
        let deadline = match bytes[45] {
            0 if blob_u64(&bytes[46..54])? == 0 => None,
            1 => Some(blob_u64(&bytes[46..54])?),
            _ => return Err(Corrupt),
        };
        let detached = SessionDetachment {
            operation,
            deadline,
        };
        if !detached.valid_for(expiry) {
            return Err(Corrupt);
        }
        return Ok(CommitFacts::SessionLifecycle(SessionChange::Detached {
            session: id(&bytes[1..9])?,
            expiry,
            detached,
        }));
    }
    let (offset, session, opened, finite) = match bytes.first() {
        Some(0) if bytes.len() == 48 => (28, id(&bytes[1..9])?, true, false),
        Some(1) if bytes.len() == 29 => (9, id(&bytes[1..9])?, false, false),
        Some(2) if bytes.len() == 51 => (31, id(&bytes[1..9])?, true, true),
        _ => return Err(Corrupt),
    };
    let route_revision = Revision::new(blob_u64(&bytes[offset..offset + 8])?);
    let cancelled = SessionCancellations {
        inbound: number(&bytes[offset + 8..offset + 12])?,
        deliveries: number(&bytes[offset + 12..offset + 16])?,
        routes: number(&bytes[offset + 16..offset + 20])?,
    };
    let change = if opened {
        let (expiry, retired_index) = if finite {
            let expiry = SessionExpiry::from_wire(number(&bytes[18..22])?).ok_or(Corrupt)?;
            if !matches!(expiry, SessionExpiry::After(_)) {
                return Err(Corrupt);
            }
            (expiry, 22)
        } else {
            (decode_expiry(i64::from(bytes[18]))?, 19)
        };
        let retired = match bytes[retired_index] {
            0 if blob_u64(&bytes[retired_index + 1..retired_index + 9])? == 0 => None,
            1 => Some(id(&bytes[retired_index + 1..retired_index + 9])?),
            _ => return Err(Corrupt),
        };
        SessionChange::Opened {
            session,
            revision: Revision::new(blob_u64(&bytes[9..17])?),
            session_present: boolean(bytes[17])?,
            expiry,
            retired,
            route_revision,
            cancelled,
        }
    } else {
        SessionChange::Closed {
            session,
            route_revision,
            cancelled,
        }
    };
    Ok(CommitFacts::SessionLifecycle(change))
}
