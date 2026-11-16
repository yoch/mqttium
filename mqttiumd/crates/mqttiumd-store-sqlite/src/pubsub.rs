//! Schema-2 subscription and outgoing delivery transactions and bounded reads.
use super::*;
use mqttiumd_contract::command::{ChangeSubscriptions, ClaimDelivery, FinishDelivery};
use mqttiumd_contract::ids::OutboundPacketId;
use mqttiumd_contract::pubsub::{DeliveryClaim, MAX_SUBSCRIPTIONS, WORK_PAGE_LIMIT, WorkItem};

pub(super) fn subscriptions(
    conn: &Connection,
    meta: &mut Meta,
    command: &ChangeSubscriptions,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.session, command.session_revision)?;
    if meta.route_revision != command.route_revision {
        return refused(failed);
    }
    for change in command.changes.as_slice() {
        if change.topic.len() > meta.limits.max_topic_bytes {
            return refused(failed);
        }
        let topic = change.topic.as_slice();
        let target = command.session.to_be_bytes();
        if change.subscribe {
            let present: bool = conn
                .prepare_cached("SELECT 1 FROM routes WHERE topic=?1 AND session=?2 LIMIT 1")?
                .query_row(params![topic, target.as_slice()], |_| Ok(()))
                .optional()?
                .is_some();
            if present {
                conn.prepare_cached("UPDATE routes SET qos=1 WHERE topic=?1 AND session=?2")?
                    .execute(params![topic, target.as_slice()])?;
            } else {
                let count: i64 = conn
                    .prepare_cached(
                        "SELECT count(*) FROM (SELECT 1 FROM routes WHERE topic=?1 LIMIT ?2)",
                    )?
                    .query_row(
                        params![
                            topic,
                            i64::try_from(meta.limits.max_route_targets.saturating_add(1))
                                .map_err(|_| Abort::Fault)?
                        ],
                        |row| row.get(0),
                    )?;
                let count = usize::try_from(count).map_err(|_| Abort::Fault)?;
                if count >= meta.limits.max_route_targets
                    || count >= meta.limits.max_targets_per_admission
                {
                    return refused(RejectReason::CapacityExceeded(Resource::References));
                }
                let previous: Option<Vec<u8>> = conn
                    .prepare_cached(
                        "SELECT position FROM routes WHERE topic=?1 ORDER BY position DESC LIMIT 1",
                    )?
                    .query_row([topic], |row| reader::copy_blob(row, 0, 8))
                    .optional()?;
                let position = match previous {
                    Some(value) => blob_u64(&value)?
                        .checked_add(1)
                        .ok_or(Abort::Refused(failed))?,
                    None => 0,
                };
                conn.prepare_cached(
                    "INSERT INTO routes(topic,position,session,qos) VALUES(?1,?2,?3,1)",
                )?
                .execute(params![
                    topic,
                    u64_blob(position).as_slice(),
                    target.as_slice()
                ])?;
            }
        } else {
            conn.prepare_cached("DELETE FROM routes WHERE topic=?1 AND session=?2")?
                .execute(params![topic, target.as_slice()])?;
        }
    }
    let total: i64 = conn
        .prepare_cached("SELECT count(*) FROM (SELECT 1 FROM routes LIMIT ?1)")?
        .query_row(
            [i64::try_from(MAX_SUBSCRIPTIONS + 1).map_err(|_| Abort::Fault)?],
            |row| row.get(0),
        )?;
    if usize::try_from(total).map_err(|_| Abort::Fault)? > MAX_SUBSCRIPTIONS {
        return refused(RejectReason::CapacityExceeded(Resource::References));
    }
    meta.route_revision = meta
        .route_revision
        .checked_next()
        .map_err(|_| Abort::Refused(failed))?;
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::SubscriptionsChanged {
        session: command.session,
        route_revision: meta.route_revision,
    })
}

pub(super) fn claim(
    conn: &Connection,
    meta: &mut Meta,
    command: &ClaimDelivery,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.target, command.target_revision)?;
    let publication = command.delivery.publication.to_be_bytes();
    let index = i64::from(command.delivery.index);
    let row = conn
        .prepare_cached(
            "SELECT target,target_revision,qos FROM obligations WHERE publication=?1 AND idx=?2",
        )?
        .query_row(params![publication.as_slice(), index], |row| {
            Ok((
                reader::copy_blob(row, 0, 8)?,
                reader::copy_blob(row, 1, 8)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .optional()?;
    let Some((target, revision, qos)) = row else {
        return refused(failed);
    };
    if target != command.target.to_be_bytes()
        || blob_u64(&revision)? != command.target_revision.get()
        || qos != 1
    {
        return refused(failed);
    }
    if conn.prepare_cached("SELECT 1 FROM outbound WHERE (publication=?1 AND idx=?2) OR (target=?3 AND packet_id=?4) LIMIT 1")?.query_row(params![publication.as_slice(),index,target,i64::from(command.packet_id.get())],|_|Ok(())).optional()?.is_some() {return refused(failed)}
    charge(
        meta,
        Accounts {
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
            ..Accounts::default()
        },
    )?;
    conn.prepare_cached("INSERT INTO outbound(publication,idx,target,target_revision,packet_id,claim) VALUES(?1,?2,?3,?4,?5,?6)")?.execute(params![publication.as_slice(),index,command.target.to_be_bytes().as_slice(),command.target_revision.to_be_bytes().as_slice(),i64::from(command.packet_id.get()),command.operation.to_be_bytes().as_slice()])?;
    Ok(CommitFacts::DeliveryClaimed {
        delivery: command.delivery,
        target: command.target,
        packet_id: command.packet_id,
        claim: command.operation,
    })
}

pub(super) fn finish(
    conn: &Connection,
    meta: &mut Meta,
    command: &FinishDelivery,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.target, command.target_revision)?;
    let publication = command.delivery.publication.to_be_bytes();
    let index = i64::from(command.delivery.index);
    let row=conn.prepare_cached("SELECT target,target_revision,packet_id,claim FROM outbound WHERE publication=?1 AND idx=?2")?.query_row(params![publication.as_slice(),index],|row|Ok((reader::copy_blob(row,0,8)?,reader::copy_blob(row,1,8)?,row.get::<_,i64>(2)?,reader::copy_blob(row,3,32)?))).optional()?;
    let Some((target, revision, packet, claim)) = row else {
        return refused(failed);
    };
    if target != command.target.to_be_bytes()
        || blob_u64(&revision)? != command.target_revision.get()
        || packet != i64::from(command.packet_id.get())
        || claim != command.claim.to_be_bytes()
    {
        return refused(failed);
    }
    end_obligation(conn, meta, &publication, index).map_err(|error| match error {
        Abort::Fault => Abort::Fault,
        Abort::Refused(_) => Abort::Refused(failed),
    })?;
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::DeliveryFinished {
        delivery: command.delivery,
        target: command.target,
        packet_id: command.packet_id,
        claim: command.claim,
    })
}

/// Decision4: session8 + routeRevision8. Decisions5/6: publication32 +
/// index4 + target8 + packet2 + claim32. Every integer is big-endian.
pub(super) fn facts_blob(facts: CommitFacts) -> Option<(i64, Vec<u8>)> {
    let mut bytes = Vec::new();
    let code = match facts {
        CommitFacts::SubscriptionsChanged {
            session,
            route_revision,
        } => {
            bytes.extend_from_slice(&session.to_be_bytes());
            bytes.extend_from_slice(&route_revision.to_be_bytes());
            4
        }
        CommitFacts::DeliveryClaimed {
            delivery,
            target,
            packet_id,
            claim,
        }
        | CommitFacts::DeliveryFinished {
            delivery,
            target,
            packet_id,
            claim,
        } => {
            bytes.extend_from_slice(&delivery.publication.to_be_bytes());
            bytes.extend_from_slice(&delivery.index.to_be_bytes());
            bytes.extend_from_slice(&target.to_be_bytes());
            bytes.extend_from_slice(&packet_id.get().to_be_bytes());
            bytes.extend_from_slice(&claim.to_be_bytes());
            if matches!(facts, CommitFacts::DeliveryClaimed { .. }) {
                5
            } else {
                6
            }
        }
        _ => return None,
    };
    Some((code, bytes))
}

pub(super) fn decode_facts(code: i64, bytes: &[u8]) -> Result<CommitFacts, Corrupt> {
    if code == 4 && bytes.len() == 16 {
        return Ok(CommitFacts::SubscriptionsChanged {
            session: SessionId::from_be_slice(&bytes[..8]).map_err(|_| Corrupt)?,
            route_revision: Revision::new(blob_u64(&bytes[8..])?),
        });
    }
    if !matches!(code, 5 | 6) || bytes.len() != 78 {
        return Err(Corrupt);
    }
    let delivery = DeliveryId {
        publication: PublicationId::from_be_slice(&bytes[..32]).map_err(|_| Corrupt)?,
        index: u32::from_be_bytes(bytes[32..36].try_into().map_err(|_| Corrupt)?),
    };
    let target = SessionId::from_be_slice(&bytes[36..44]).map_err(|_| Corrupt)?;
    let packet_id = OutboundPacketId::new(u16::from_be_bytes(
        bytes[44..46].try_into().map_err(|_| Corrupt)?,
    ))
    .ok_or(Corrupt)?;
    let claim = OperationId::from_be_slice(&bytes[46..78]).map_err(|_| Corrupt)?;
    Ok(if code == 5 {
        CommitFacts::DeliveryClaimed {
            delivery,
            target,
            packet_id,
            claim,
        }
    } else {
        CommitFacts::DeliveryFinished {
            delivery,
            target,
            packet_id,
            claim,
        }
    })
}

pub(super) fn work(
    conn: &Connection,
    session: SessionId,
    after: Option<DeliveryId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WorkPage, Fault> {
    if limit == 0 || limit > WORK_PAGE_LIMIT as usize {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    let read = conn.unchecked_transaction()?;
    let meta = read_meta(&read)?;
    // Check the target's finite metadata domain before either the QoS or
    // cursor selection can turn an invalid obligation into apparent absence.
    // SQL inspects scalars only; no offending value or payload is copied.
    let obligations: i64 = read.query_row(
        "SELECT count(*) FROM (SELECT 1 FROM obligations WHERE target=?1 LIMIT ?2)",
        params![
            session.to_be_bytes().as_slice(),
            i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?
        ],
        |row| row.get(0),
    )?;
    if usize::try_from(obligations).map_err(|_| Fault::Corrupt)? > MAX_RECOVERY_INVENTORY_ROWS {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    let invalid: bool = read.query_row(
        "SELECT EXISTS(SELECT 1 FROM obligations WHERE target=?1 AND (typeof(qos)!='integer' OR qos NOT IN (0,1,2) OR typeof(cause)!='integer' OR cause!=1 OR typeof(phase)!='integer' OR phase!=1) LIMIT 1)",
        [session.to_be_bytes().as_slice()],
        |row| row.get(0),
    )?;
    if invalid {
        return Err(Fault::Corrupt);
    }
    // A missing obligation must not turn an already claimed delivery into a
    // successful empty scan. Bound the target's claim set before checking it.
    let claims: i64 = read.query_row(
        "SELECT count(*) FROM (SELECT 1 FROM outbound WHERE target=?1 LIMIT ?2)",
        params![
            session.to_be_bytes().as_slice(),
            i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?
        ],
        |row| row.get(0),
    )?;
    if usize::try_from(claims).map_err(|_| Fault::Corrupt)? > MAX_RECOVERY_INVENTORY_ROWS {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    let broken: bool = read.query_row(
        "SELECT EXISTS(SELECT 1 FROM outbound c LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx WHERE c.target=?1 AND (o.publication IS NULL OR o.target!=c.target OR o.target_revision!=c.target_revision OR o.qos!=1) LIMIT 1)",
        [session.to_be_bytes().as_slice()], |row| row.get(0),
    )?;
    if broken {
        return Err(Fault::Corrupt);
    }
    let sql = "SELECT o.publication,o.idx,o.target_revision,p.topic,y.bytes,c.packet_id,c.claim,c.target,c.target_revision,s.revision,s.state FROM obligations o LEFT JOIN publications p ON p.id=o.publication LEFT JOIN payloads y ON y.id=p.payload LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx LEFT JOIN sessions s ON s.id=o.target WHERE o.target=?1 AND o.qos=1 AND (?2 IS NULL OR o.publication>?2 OR (o.publication=?2 AND o.idx>?3)) ORDER BY o.publication,o.idx LIMIT ?4";
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut meter = reader::Meter::new(budget);
    let mut more = false;
    {
        let mut statement = read.prepare(sql)?;
        let mut rows = statement.query(params![
            session.to_be_bytes().as_slice(),
            after.map(|id| id.publication.to_be_bytes().to_vec()),
            after.map(|id| i64::from(id.index)).unwrap_or(0),
            i64::try_from(limit + 1).map_err(|_| Fault::Corrupt)?
        ])?;
        while let Some(row) = rows.next()? {
            if items.len() == limit {
                more = true;
                break;
            }
            budget.check_field(32)?;
            let topic = reader::borrow_blob(row, 3, meta.limits.max_topic_bytes)?;
            let payload = reader::borrow_blob(row, 4, meta.limits.max_payload_bytes)?;
            budget.check_field(topic.len())?;
            budget.check_field(payload.len())?;
            meter.charge(
                256usize
                    .checked_add(topic.len())
                    .and_then(|n| n.checked_add(payload.len()))
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            let delivery = DeliveryId {
                publication: PublicationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                    .map_err(|_| Fault::Corrupt)?,
                index: u32::try_from(row.get::<_, i64>(1)?).map_err(|_| Fault::Corrupt)?,
            };
            let revision = Revision::new(blob_u64(reader::borrow_blob(row, 2, 8)?)?);
            if code_session_state(row.get(10)?)? != SessionState::Active
                || revision.get() != blob_u64(reader::borrow_blob(row, 9, 8)?)?
            {
                return Err(Fault::Corrupt);
            }
            let claim = match row.get_ref(5)? {
                rusqlite::types::ValueRef::Null => {
                    if !matches!(row.get_ref(6)?, rusqlite::types::ValueRef::Null) {
                        return Err(Fault::Corrupt);
                    }
                    None
                }
                _ => {
                    let packet = u16::try_from(row.get::<_, i64>(5)?)
                        .ok()
                        .and_then(OutboundPacketId::new)
                        .ok_or(Fault::Corrupt)?;
                    let operation = OperationId::from_be_slice(reader::borrow_blob(row, 6, 32)?)
                        .map_err(|_| Fault::Corrupt)?;
                    if operation.incarnation() != meta.ledger.incarnation()
                        || reader::borrow_blob(row, 7, 8)? != session.to_be_bytes()
                        || blob_u64(reader::borrow_blob(row, 8, 8)?)? != revision.get()
                    {
                        return Err(Fault::Corrupt);
                    }
                    Some(DeliveryClaim {
                        operation,
                        packet_id: packet,
                    })
                }
            };
            items
                .try_push(WorkItem {
                    delivery,
                    target: session,
                    target_revision: revision,
                    qos: Qos::AtLeastOnce,
                    topic: SharedBytes::copy_from(topic, meta.limits.max_topic_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    payload: SharedBytes::copy_from(payload, meta.limits.max_payload_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    claim,
                })
                .map_err(|_| ReadError::LimitExceeded)?;
        }
    }
    let next = if more {
        items.as_slice().last().map(|item| item.delivery)
    } else {
        None
    };
    read.commit()?;
    Ok(WorkPage { items, next })
}
