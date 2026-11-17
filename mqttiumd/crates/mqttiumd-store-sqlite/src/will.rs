//! Bounded durable Will intentions; the installing Open is their causal identity.
use super::*;
use mqttiumd_contract::session::{SessionAction, SessionChange, SessionLifecycle};
use mqttiumd_contract::will::{
    MAX_WILL_BYTES, MAX_WILL_INTENTS, MAX_WILL_PAYLOAD_BYTES, MAX_WILL_TOPIC_BYTES, WillIntent,
    WillPublication, WillSpec, WillState,
};
use mqttiumd_store_api::WillPage;

fn state(raw: i64) -> Result<WillState, Fault> {
    match raw {
        1 => Ok(WillState::Armed),
        2 => Ok(WillState::Pending),
        _ => Err(Fault::Corrupt),
    }
}
fn metadata(
    conn: &Connection,
    row: &rusqlite::Row<'_>,
    meta: &Meta,
) -> Result<(OperationId, SessionId, WillState, usize), Fault> {
    let id = OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?).map_err(|_| Corrupt)?;
    if id.incarnation() != meta.ledger.incarnation()
        || id.sequence().get() == 0
        || id.epoch().get() == 0
        || id.epoch() > meta.ledger.last_opened()
    {
        return Err(Fault::Corrupt);
    }
    let session = SessionId::from_be_slice(reader::borrow_blob(row, 1, 8)?).map_err(|_| Corrupt)?;
    if session.get() == 0 || session >= meta.next_session {
        return Err(Fault::Corrupt);
    }
    let state = state(row.get(2)?)?;
    let client = reader::borrow_blob(row, 3, mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES)?;
    let topic = reader::borrow_blob(row, 4, MAX_WILL_TOPIC_BYTES)?;
    let payload = reader::borrow_blob(row, 5, MAX_WILL_PAYLOAD_BYTES)?;
    if client.is_empty()
        || client.contains(&0)
        || std::str::from_utf8(client).is_err()
        || topic.is_empty()
        || topic.iter().any(|b| matches!(*b, 0 | b'+' | b'#'))
        || std::str::from_utf8(topic).is_err()
    {
        return Err(Fault::Corrupt);
    }
    if state == WillState::Armed {
        let mut statement=conn.prepare_cached("SELECT opened_by,client_id FROM sessions WHERE id=?1 AND state=1 AND detached_by IS NULL")?;
        let mut rows = statement.query([session.to_be_bytes().as_slice()])?;
        let Some(binding) = rows.next()? else {
            return Err(Fault::Corrupt);
        };
        if reader::borrow_blob(binding, 0, 32)? != id.to_be_bytes()
            || reader::borrow_blob(
                binding,
                1,
                mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES,
            )? != client
        {
            return Err(Fault::Corrupt);
        }
    }
    let bytes = 128usize
        .checked_add(client.len())
        .and_then(|n| n.checked_add(topic.len()))
        .and_then(|n| n.checked_add(payload.len()))
        .ok_or(ReadError::LimitExceeded)?;
    Ok((id, session, state, bytes))
}
fn build(
    conn: &Connection,
    row: &rusqlite::Row<'_>,
    meta: &Meta,
    budget: ReadBudget,
) -> Result<WillIntent, Fault> {
    let (id, session, state, bytes) = metadata(conn, row, meta)?;
    budget.check_totals(1, bytes)?;
    for (index, maximum) in [
        (3, mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES),
        (4, MAX_WILL_TOPIC_BYTES),
        (5, MAX_WILL_PAYLOAD_BYTES),
    ] {
        budget.check_field(reader::borrow_blob(row, index, maximum)?.len())?;
    }
    let copy = |index, maximum| -> Result<SharedBytes, Fault> {
        SharedBytes::copy_from(reader::borrow_blob(row, index, maximum)?, maximum)
            .map_err(|_| Fault::Read(ReadError::AllocationFailed))
    };
    Ok(WillIntent {
        id,
        session,
        state,
        client_id: copy(3, mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES)?,
        spec: WillSpec {
            topic: copy(4, MAX_WILL_TOPIC_BYTES)?,
            payload: copy(5, MAX_WILL_PAYLOAD_BYTES)?,
        },
    })
}
const SELECT: &str =
    "SELECT id,session,state,client_id,topic,payload FROM wills ORDER BY id LIMIT 257";

pub(super) fn snapshot(
    conn: &Connection,
    meta: &Meta,
    budget: ReadBudget,
) -> Result<BTreeMap<OperationId, WillIntent>, Fault> {
    let mut statement = conn.prepare_cached(SELECT)?;
    let mut rows = statement.query([])?;
    let mut out = BTreeMap::new();
    let mut meter = reader::Meter::new(budget);
    let mut payload_bytes = 0u64;
    while let Some(row) = rows.next()? {
        if out.len() >= MAX_WILL_INTENTS {
            return Err(ReadError::LimitExceeded.into());
        }
        let (id, _, _, bytes) = metadata(conn, row, meta)?;
        meter.charge(bytes)?;
        payload_bytes = payload_bytes
            .checked_add(reader::borrow_blob(row, 5, MAX_WILL_PAYLOAD_BYTES)?.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if payload_bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
        let intent = build(conn, row, meta, budget)?;
        out.insert(id, intent);
    }
    Ok(out)
}

pub(super) fn page(
    conn: &Connection,
    meta: &Meta,
    after: Option<OperationId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WillPage, Fault> {
    if limit == 0 || limit > MAX_WILL_INTENTS || limit > budget.max_rows {
        return Err(ReadError::LimitExceeded.into());
    }
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut statement = conn.prepare_cached(SELECT)?;
    let mut rows = statement.query([])?;
    let mut meter = reader::Meter::new(budget);
    let mut count = 0usize;
    let mut payload_bytes = 0u64;
    let mut more = false;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > MAX_WILL_INTENTS {
            return Err(ReadError::LimitExceeded.into());
        }
        // Validate before filtering: corrupt state is never interpreted as no work.
        let (id, _, state, bytes) = metadata(conn, row, meta)?;
        payload_bytes = payload_bytes
            .checked_add(reader::borrow_blob(row, 5, MAX_WILL_PAYLOAD_BYTES)?.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if payload_bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
        if state != WillState::Pending || after.is_some_and(|a| id <= a) {
            continue;
        }
        if items.len() == limit {
            more = true;
            continue;
        }
        meter.charge(bytes)?;
        items
            .try_push(build(conn, row, meta, budget)?)
            .map_err(|_| ReadError::AllocationFailed)?;
    }
    let next = more
        .then(|| items.as_slice().last().map(|i| i.id))
        .flatten();
    Ok(WillPage { items, next })
}

pub(super) fn has_client(conn: &Connection, client: &[u8]) -> Result<bool, Fault> {
    let meta = read_meta(conn)?;
    let mut statement=conn.prepare_cached("SELECT w.state,w.client_id,length(w.topic),length(w.payload),w.id,w.session,s.opened_by,s.client_id,s.detached_by FROM wills w LEFT JOIN sessions s ON s.id=w.session LIMIT 257")?;
    let mut rows = statement.query([])?;
    let mut count = 0usize;
    let mut found = false;
    let mut payload_bytes = 0u64;
    while let Some(row) = rows.next()? {
        count += 1;
        if count > MAX_WILL_INTENTS {
            return Err(ReadError::LimitExceeded.into());
        }
        let phase = state(row.get(0)?)?;
        let id =
            OperationId::from_be_slice(reader::borrow_blob(row, 4, 32)?).map_err(|_| Corrupt)?;
        let session =
            SessionId::from_be_slice(reader::borrow_blob(row, 5, 8)?).map_err(|_| Corrupt)?;
        if id.incarnation() != meta.ledger.incarnation()
            || id.epoch().get() == 0
            || id.epoch() > meta.ledger.last_opened()
            || id.sequence().get() == 0
            || session.get() == 0
            || session >= meta.next_session
        {
            return Err(Fault::Corrupt);
        }
        let stored =
            reader::borrow_blob(row, 1, mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES)?;
        if stored.is_empty() || stored.contains(&0) || std::str::from_utf8(stored).is_err() {
            return Err(Fault::Corrupt);
        }
        if phase == WillState::Armed
            && (reader::borrow_blob(row, 6, 32)? != id.to_be_bytes()
                || reader::borrow_blob(
                    row,
                    7,
                    mqttiumd_contract::session::MAX_SESSION_CLIENT_BYTES,
                )? != stored
                || !matches!(row.get_ref(8)?, rusqlite::types::ValueRef::Null))
        {
            return Err(Fault::Corrupt);
        }
        let topic: i64 = row.get(2)?;
        let payload: i64 = row.get(3)?;
        if topic <= 0
            || topic > MAX_WILL_TOPIC_BYTES as i64
            || payload < 0
            || payload > MAX_WILL_PAYLOAD_BYTES as i64
        {
            return Err(ReadError::LimitExceeded.into());
        }
        payload_bytes = payload_bytes
            .checked_add(payload as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if payload_bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
        found |= stored == client;
    }
    Ok(found)
}

pub(super) fn recover(conn: &Connection, meta: &Meta) -> Result<(), Fault> {
    let mut statement = conn.prepare_cached(SELECT)?;
    let mut rows = statement.query([])?;
    let mut count = 0usize;
    let mut bytes = 0u64;
    while let Some(row) = rows.next()? {
        count = count.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if count > MAX_WILL_INTENTS {
            return Err(ReadError::LimitExceeded.into());
        }
        metadata(conn, row, meta)?;
        bytes = bytes
            .checked_add(reader::borrow_blob(row, 5, MAX_WILL_PAYLOAD_BYTES)?.len() as u64)
            .ok_or(ReadError::LimitExceeded)?;
        if bytes > MAX_WILL_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
    }
    drop(rows);
    drop(statement);
    conn.prepare_cached("UPDATE wills SET state=2 WHERE state=1")?
        .execute([])?;
    Ok(())
}

pub(super) fn open(
    conn: &Connection,
    meta: &mut Meta,
    command: &SessionLifecycle,
) -> Guard<CommitFacts> {
    let SessionAction::OpenWillAt {
        client_id,
        expected,
        clean_start,
        expiry,
        now,
        will,
    } = &command.action
    else {
        return refused(RejectReason::PreconditionFailed);
    };
    if has_client(conn, client_id.as_slice())? {
        return refused(RejectReason::PreconditionFailed);
    }
    if let Some(spec) = will {
        if spec.topic.len() > meta.limits.max_topic_bytes
            || spec.payload.len() > meta.limits.max_payload_bytes
        {
            return refused(RejectReason::PreconditionFailed);
        }
        let (count, bytes): (i64, i64) = conn.query_row(
            "SELECT count(*),coalesce(sum(length(payload)),0) FROM wills",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if count < 0
            || count as usize >= MAX_WILL_INTENTS
            || bytes < 0
            || (bytes as u64)
                .checked_add(spec.payload.len() as u64)
                .is_none_or(|n| n > MAX_WILL_BYTES)
        {
            return refused(RejectReason::CapacityExceeded(Resource::PayloadBytes));
        }
    }
    let legacy = SessionLifecycle {
        operation: command.operation,
        action: SessionAction::OpenAt {
            client_id: client_id.clone(),
            expected: *expected,
            clean_start: *clean_start,
            expiry: *expiry,
            now: *now,
        },
    };
    let facts = sessions::stage(conn, meta, &legacy)?;
    if let (Some(spec), CommitFacts::SessionLifecycle(SessionChange::Opened { session, .. })) =
        (will, facts)
    {
        conn.prepare_cached(
            "INSERT INTO wills(id,session,state,client_id,topic,payload) VALUES(?1,?2,1,?3,?4,?5)",
        )?
        .execute(params![
            command.operation.to_be_bytes().as_slice(),
            session.to_be_bytes().as_slice(),
            client_id.as_slice(),
            spec.topic.as_slice(),
            spec.payload.as_slice()
        ])?;
    }
    Ok(facts)
}

pub(super) fn end(
    conn: &Connection,
    meta: &mut Meta,
    command: &SessionLifecycle,
) -> Guard<CommitFacts> {
    let SessionAction::EndWill {
        guard,
        expiry,
        observed_at,
        publish,
    } = command.action
    else {
        return refused(RejectReason::PreconditionFailed);
    };
    let binding: Option<Vec<u8>> = conn
        .query_row(
            "SELECT opened_by FROM sessions WHERE id=?1 AND state=1 AND detached_by IS NULL",
            [guard.session.to_be_bytes().as_slice()],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if binding.as_deref()
        != guard
            .opened_by
            .as_ref()
            .map(|id| id.to_be_bytes())
            .as_ref()
            .map(<[u8; 32]>::as_slice)
        || guard.opened_by.is_none()
    {
        return refused(RejectReason::PreconditionFailed);
    }
    let key = guard
        .opened_by
        .ok_or(Abort::Refused(RejectReason::PreconditionFailed))?
        .to_be_bytes();
    if publish {
        conn.prepare_cached("UPDATE wills SET state=2 WHERE id=?1 AND session=?2 AND state=1")?
            .execute(params![
                key.as_slice(),
                guard.session.to_be_bytes().as_slice()
            ])?;
    } else {
        conn.prepare_cached("DELETE FROM wills WHERE id=?1 AND session=?2 AND state=1")?
            .execute(params![
                key.as_slice(),
                guard.session.to_be_bytes().as_slice()
            ])?;
    }
    let detach = SessionLifecycle {
        operation: command.operation,
        action: SessionAction::Detach {
            guard,
            expiry,
            observed_at,
        },
    };
    sessions::stage(conn, meta, &detach)
}

pub(super) fn publish(
    conn: &Connection,
    meta: &mut Meta,
    value: &WillPublication,
    payload: Option<&SharedBytes>,
) -> Guard<CommitFacts> {
    let mut statement = conn
        .prepare_cached("SELECT id,session,state,client_id,topic,payload FROM wills WHERE id=?1")?;
    let mut rows = statement.query([value.will.to_be_bytes().as_slice()])?;
    let Some(row) = rows.next()? else {
        return refused(RejectReason::PreconditionFailed);
    };
    let intent = build(conn, row, meta, ReadBudget::PAGE)?;
    if intent.state != WillState::Pending
        || value.topic != intent.spec.topic
        || payload != Some(&intent.spec.payload)
        || value.payload.length.get() != intent.spec.payload.len() as u64
    {
        return refused(RejectReason::PreconditionFailed);
    }
    drop(rows);
    drop(statement);
    let admit = Admission {
        expiry: None,
        operation: value.operation,
        request: None,
        will: Some(value.will),
        publication: value.publication,
        source_session: intent.session,
        source_revision: Revision::new(0),
        inbound: None,
        route_revision: value.route_revision,
        qos: Qos::AtLeastOnce,
        retain: false,
        topic: &value.topic,
        payload: value.payload,
        costs: value.costs,
        targets: value.targets.as_slice(),
    };
    stage_publication(conn, meta, admit, None, payload)?;
    if conn
        .prepare_cached("DELETE FROM wills WHERE id=?1 AND state=2")?
        .execute([value.will.to_be_bytes().as_slice()])?
        != 1
    {
        return refused(RejectReason::PreconditionFailed);
    }
    Ok(CommitFacts::WillPublished {
        will: value.will,
        publication: value.publication,
    })
}

/// Large Open receipts are separately bounded before a new receipt is created.
/// Exhaustion returns unknown, leaving the small reserved fence path available.
pub(super) fn receipt_capacity(conn: &Connection, canonical: &[u8]) -> Result<bool, Fault> {
    const MAX_BYTES: u64 = 8 * 1024 * 1024;
    let mut bytes = 0u64;
    let mut rows_count = 0usize;
    let mut statement = conn.prepare_cached("SELECT length(recipe) FROM operations WHERE substr(recipe,7,1)=x'08' AND substr(recipe,40,1)=x'05' LIMIT 8193")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        rows_count = rows_count.checked_add(1).ok_or(ReadError::LimitExceeded)?;
        if rows_count > 8192 {
            return Err(ReadError::LimitExceeded.into());
        }
        let length: i64 = row.get(0)?;
        let length = u64::try_from(length).map_err(|_| Corrupt)?;
        bytes = bytes.checked_add(length).ok_or(ReadError::LimitExceeded)?;
        if bytes > MAX_BYTES {
            return Err(ReadError::LimitExceeded.into());
        }
    }
    let is_open = canonical.get(6) == Some(&8) && canonical.get(39) == Some(&5);
    Ok(!is_open
        || bytes
            .checked_add(canonical.len() as u64)
            .is_some_and(|n| n <= MAX_BYTES))
}
