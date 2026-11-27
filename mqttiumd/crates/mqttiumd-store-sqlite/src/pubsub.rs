//! Schema-7 subscription and outgoing delivery transactions and bounded reads.
use super::*;
use mqttiumd_contract::command::{
    ChangeSubscriptions, ClaimDelivery, ConsumeDelivery, FinishDelivery, UpdateSubscriptions,
};
use mqttiumd_contract::ids::OutboundPacketId;
use mqttiumd_contract::message_expiry::{ExpireDelivery, MarkDeliveryStarted};
use mqttiumd_contract::pubsub::{
    DeliveryClaim, MAX_SUBSCRIPTIONS, SubscriptionOptions, WORK_PAGE_LIMIT, WorkItem,
};

/// Validate discriminants before a route can be selected or copied.
pub(super) fn route_options(value: i64) -> Result<SubscriptionOptions, Fault> {
    u8::try_from(value)
        .ok()
        .and_then(SubscriptionOptions::from_bits)
        .ok_or(Fault::Corrupt)
}
pub(super) fn route_filter(bytes: &[u8]) -> Result<(), Fault> {
    if !mqttiumd_contract::topic::valid_filter(bytes) {
        return Err(Fault::Corrupt);
    }
    Ok(())
}

pub(super) fn subscriptions(
    conn: &Connection,
    meta: &mut Meta,
    command: &ChangeSubscriptions,
) -> Guard<CommitFacts> {
    route_updates(
        conn,
        meta,
        command.session,
        command.session_revision,
        command.route_revision,
        command.changes.as_slice().iter().map(|c| {
            (
                &c.topic,
                c.subscribe.then_some(Qos::AtLeastOnce),
                SubscriptionOptions::DEFAULT,
            )
        }),
    )
}
pub(super) fn updates(
    conn: &Connection,
    meta: &mut Meta,
    command: &UpdateSubscriptions,
) -> Guard<CommitFacts> {
    route_updates(
        conn,
        meta,
        command.session,
        command.session_revision,
        command.route_revision,
        command
            .changes
            .as_slice()
            .iter()
            .map(|c| (&c.topic, c.granted, c.options)),
    )
}
fn route_updates<'a>(
    conn: &Connection,
    meta: &mut Meta,
    session: SessionId,
    session_revision: Revision,
    route_revision: Revision,
    changes: impl Iterator<Item = (&'a SharedBytes, Option<Qos>, SubscriptionOptions)>,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, session, session_revision)?;
    if meta.route_revision != route_revision {
        return refused(failed);
    }
    for (topic, granted, options) in changes {
        if topic.len() > meta.limits.max_topic_bytes
            || !mqttiumd_contract::topic::valid_filter(topic.as_slice())
            || SubscriptionOptions::from_bits(options.bits()) != Some(options)
            || (granted.is_none() && options != SubscriptionOptions::DEFAULT)
        {
            return refused(failed);
        }
        let topic = topic.as_slice();
        let target = session.to_be_bytes();
        if let Some(qos) = granted {
            let present: bool = conn
                .prepare_cached("SELECT 1 FROM routes WHERE topic=?1 AND session=?2 LIMIT 1")?
                .query_row(params![topic, target.as_slice()], |_| Ok(()))
                .optional()?
                .is_some();
            if present {
                conn.prepare_cached(
                    "UPDATE routes SET qos=?3,options=?4 WHERE topic=?1 AND session=?2",
                )?
                .execute(params![
                    topic,
                    target.as_slice(),
                    qos_code(qos),
                    i64::from(options.bits())
                ])?;
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
                    "INSERT INTO routes(topic,position,session,qos,options) VALUES(?1,?2,?3,?4,?5)",
                )?
                .execute(params![
                    topic,
                    u64_blob(position).as_slice(),
                    target.as_slice(),
                    qos_code(qos),
                    i64::from(options.bits())
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
        session,
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
            "SELECT target,target_revision,qos \
             FROM obligations WHERE publication=?1 AND idx=?2 AND phase=1",
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
        || !matches!(qos, 1 | 2)
    {
        return refused(failed);
    }
    if conn
        .prepare_cached(
            "SELECT 1 FROM outbound WHERE (publication=?1 AND idx=?2) OR (target=?3 AND \
             packet_id=?4) LIMIT 1",
        )?
        .query_row(
            params![
                publication.as_slice(),
                index,
                target,
                i64::from(command.packet_id.get())
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some()
    {
        return refused(failed);
    }
    charge(
        meta,
        Accounts {
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
            ..Accounts::default()
        },
    )?;
    conn.prepare_cached(
        "INSERT INTO outbound(publication,idx,target,target_revision,packet_id,claim) \
         VALUES(?1,?2,?3,?4,?5,?6)",
    )?
    .execute(params![
        publication.as_slice(),
        index,
        command.target.to_be_bytes().as_slice(),
        command.target_revision.to_be_bytes().as_slice(),
        i64::from(command.packet_id.get()),
        command.operation.to_be_bytes().as_slice()
    ])?;
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
    let row = conn
        .prepare_cached(
            "SELECT target,target_revision,packet_id,claim \
             FROM outbound WHERE publication=?1 AND idx=?2",
        )?
        .query_row(params![publication.as_slice(), index], |row| {
            Ok((
                reader::copy_blob(row, 0, 8)?,
                reader::copy_blob(row, 1, 8)?,
                row.get::<_, i64>(2)?,
                reader::copy_blob(row, 3, 32)?,
            ))
        })
        .optional()?;
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
        Abort::Read(error) => Abort::Read(error),
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

pub(super) fn prepare_pubrel(
    conn: &Connection,
    meta: &mut Meta,
    command: &FinishDelivery,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.target, command.target_revision)?;
    let changed = conn
        .prepare_cached(
            "UPDATE obligations SET phase=2
             WHERE publication=?1 AND idx=?2 AND target=?3 AND target_revision=?4
               AND qos=2 AND phase=1
               AND EXISTS(SELECT 1 FROM outbound WHERE publication=?1 AND idx=?2
                           AND target=?3 AND target_revision=?4 AND packet_id=?5 AND claim=?6)",
        )?
        .execute(params![
            command.delivery.publication.to_be_bytes().as_slice(),
            i64::from(command.delivery.index),
            command.target.to_be_bytes().as_slice(),
            command.target_revision.to_be_bytes().as_slice(),
            i64::from(command.packet_id.get()),
            command.claim.to_be_bytes().as_slice(),
        ])?;
    if changed != 1 {
        return refused(failed);
    }
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::DeliveryPubrelReady {
        delivery: command.delivery,
        target: command.target,
        packet_id: command.packet_id,
        claim: command.claim,
    })
}

pub(super) fn mark_started(
    conn: &Connection,
    meta: &mut Meta,
    command: &MarkDeliveryStarted,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.target, command.target_revision)?;
    let changed = conn
        .prepare_cached(
            "UPDATE outbound SET started=1 \
             WHERE publication=?1 AND idx=?2 AND target=?3 AND target_revision=?4 AND \
             packet_id=?5 AND claim=?6 AND EXISTS(SELECT 1 \
             FROM obligations o JOIN publications p ON p.id=o.publication \
             WHERE o.publication=outbound.publication AND o.idx=outbound.idx AND \
             o.target=outbound.target AND o.target_revision=outbound.target_revision AND o.qos IN \
             (1,2) AND o.phase=1 AND p.expiry_domain IS NOT NULL)",
        )?
        .execute(params![
            command.delivery.publication.to_be_bytes().as_slice(),
            i64::from(command.delivery.index),
            command.target.to_be_bytes().as_slice(),
            command.target_revision.to_be_bytes().as_slice(),
            i64::from(command.packet_id.get()),
            command.claim.to_be_bytes().as_slice()
        ])?;
    if changed != 1 {
        return refused(failed);
    }
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::DeliveryStarted {
        delivery: command.delivery,
        target: command.target,
        packet_id: command.packet_id,
        claim: command.claim,
    })
}

pub(super) fn expire(
    conn: &Connection,
    meta: &mut Meta,
    command: &ExpireDelivery,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    message_expiry::guard_observation(conn, command.observed)?;
    let started: Option<i64> = conn
        .prepare_cached(
            "SELECT c.started FROM outbound c \
             JOIN obligations o USING(publication,idx) \
             WHERE c.publication=?1 AND c.idx=?2 AND o.phase=1",
        )?
        .query_row(
            params![
                command.finish.delivery.publication.to_be_bytes().as_slice(),
                i64::from(command.finish.delivery.index)
            ],
            |row| row.get(0),
        )
        .optional()?;
    if started != Some(0) {
        return refused(failed);
    }
    if message_expiry::publication(conn, command.finish.delivery.publication)?
        != Some(command.expiry)
        || command.expiry.state(command.observed) != Ok(ExpiryState::Expired)
    {
        return refused(failed);
    }
    finish(conn, meta, &command.finish)?;
    Ok(CommitFacts::DeliveryExpired {
        delivery: command.finish.delivery,
        target: command.finish.target,
        packet_id: command.finish.packet_id,
        claim: command.finish.claim,
    })
}

pub(super) fn consume(
    conn: &Connection,
    meta: &mut Meta,
    command: &ConsumeDelivery,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    active_session(conn, command.target, command.target_revision)?;
    let publication = command.delivery.publication.to_be_bytes();
    let index = i64::from(command.delivery.index);
    let row = conn
        .prepare_cached(
            "SELECT target,target_revision,qos \
             FROM obligations WHERE publication=?1 AND idx=?2 AND phase=1",
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
        || qos != 0
    {
        return refused(failed);
    }
    if conn
        .prepare_cached("SELECT 1 FROM outbound WHERE publication=?1 AND idx=?2 LIMIT 1")?
        .query_row(params![publication.as_slice(), index], |_| Ok(()))
        .optional()?
        .is_some()
    {
        return refused(failed);
    }
    end_obligation(conn, meta, &publication, index).map_err(|error| match error {
        Abort::Read(error) => Abort::Read(error),
        Abort::Fault => Abort::Fault,
        Abort::Refused(_) => Abort::Refused(failed),
    })?;
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::DeliveryConsumed {
        delivery: command.delivery,
        target: command.target,
    })
}

/// Decision4: session8 + routeRevision8. Decisions5/6: publication32 +
/// index4 + target8 + packet2 + claim32. Every integer is big-endian.
pub(super) fn facts_blob(facts: CommitFacts) -> Option<(i64, Vec<u8>)> {
    let mut bytes = Vec::new();
    let code = match facts {
        CommitFacts::WillPublished { will, publication } => {
            bytes.extend_from_slice(&will.to_be_bytes());
            bytes.extend_from_slice(&publication.to_be_bytes());
            12
        }
        CommitFacts::SessionLifecycle(change) => return Some((8, sessions::encode(change))),
        CommitFacts::RetainedQos0Stored { publication } => {
            bytes.extend_from_slice(&publication.to_be_bytes());
            9
        }
        CommitFacts::DeliveryConsumed { delivery, target } => {
            bytes.extend_from_slice(&delivery.publication.to_be_bytes());
            bytes.extend_from_slice(&delivery.index.to_be_bytes());
            bytes.extend_from_slice(&target.to_be_bytes());
            7
        }
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
        }
        | CommitFacts::DeliveryStarted {
            delivery,
            target,
            packet_id,
            claim,
        }
        | CommitFacts::DeliveryPubrelReady {
            delivery,
            target,
            packet_id,
            claim,
        }
        | CommitFacts::DeliveryExpired {
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
            match facts {
                CommitFacts::DeliveryClaimed { .. } => 5,
                CommitFacts::DeliveryFinished { .. } => 6,
                CommitFacts::DeliveryStarted { .. } => 10,
                CommitFacts::DeliveryExpired { .. } => 11,
                CommitFacts::DeliveryPubrelReady { .. } => 13,
                _ => return None,
            }
        }
        _ => return None,
    };
    Some((code, bytes))
}

pub(super) fn decode_facts(code: i64, bytes: &[u8]) -> Result<CommitFacts, Corrupt> {
    if code == 12 && bytes.len() == 64 {
        return Ok(CommitFacts::WillPublished {
            will: OperationId::from_be_slice(&bytes[..32]).map_err(|_| Corrupt)?,
            publication: PublicationId::from_be_slice(&bytes[32..]).map_err(|_| Corrupt)?,
        });
    }
    if code == 9 {
        return Ok(CommitFacts::RetainedQos0Stored {
            publication: PublicationId::from_be_slice(bytes).map_err(|_| Corrupt)?,
        });
    }
    if code == 8 {
        return sessions::decode(bytes);
    }
    if code == 7 && bytes.len() == 44 {
        return Ok(CommitFacts::DeliveryConsumed {
            delivery: DeliveryId {
                publication: PublicationId::from_be_slice(&bytes[..32]).map_err(|_| Corrupt)?,
                index: u32::from_be_bytes(bytes[32..36].try_into().map_err(|_| Corrupt)?),
            },
            target: SessionId::from_be_slice(&bytes[36..44]).map_err(|_| Corrupt)?,
        });
    }
    if code == 4 && bytes.len() == 16 {
        return Ok(CommitFacts::SubscriptionsChanged {
            session: SessionId::from_be_slice(&bytes[..8]).map_err(|_| Corrupt)?,
            route_revision: Revision::new(blob_u64(&bytes[8..])?),
        });
    }
    if !matches!(code, 5 | 6 | 10 | 11 | 13) || bytes.len() != 78 {
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
    } else if code == 6 {
        CommitFacts::DeliveryFinished {
            delivery,
            target,
            packet_id,
            claim,
        }
    } else if code == 10 {
        CommitFacts::DeliveryStarted {
            delivery,
            target,
            packet_id,
            claim,
        }
    } else if code == 13 {
        CommitFacts::DeliveryPubrelReady {
            delivery,
            target,
            packet_id,
            claim,
        }
    } else {
        CommitFacts::DeliveryExpired {
            delivery,
            target,
            packet_id,
            claim,
        }
    })
}

pub(super) fn work(
    conn: &Connection,
    caches: &mut StoreCaches,
    session: SessionId,
    after: Option<DeliveryId>,
    limit: usize,
    budget: ReadBudget,
) -> Result<WorkPage, Fault> {
    if limit == 0 || limit > WORK_PAGE_LIMIT as usize {
        return Err(Fault::Read(ReadError::LimitExceeded));
    }
    let read = conn.unchecked_transaction()?;
    caches.sync(&read)?;
    let meta = read_meta(&read)?;
    let counts_bounded = meta.accounts.references.get() <= MAX_RECOVERY_INVENTORY_ROWS as u64
        && meta.accounts.exchange_slots.get() <= MAX_RECOVERY_INVENTORY_ROWS as u64;
    if !(counts_bounded && caches.work_verified.contains(&session)) {
        sessions::validate_metadata(&read, &meta, Some(session))?;
        message_expiry::validate(&read, Some(session))?;
        // Check the target's finite metadata domain before either the QoS or
        // cursor selection can turn an invalid obligation into apparent absence.
        // SQL inspects scalars only; no offending value or payload is copied.
        let obligations: i64 = read
            .prepare_cached(
                "SELECT count(*) FROM (SELECT 1 FROM obligations WHERE target=?1 LIMIT ?2)",
            )?
            .query_row(
                params![
                    session.to_be_bytes().as_slice(),
                    i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?
                ],
                |row| row.get(0),
            )?;
        if usize::try_from(obligations).map_err(|_| Fault::Corrupt)? > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(Fault::Read(ReadError::LimitExceeded));
        }
        let invalid: bool = read
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM obligations o \
                 LEFT JOIN publications p ON p.id=o.publication \
                 WHERE o.target=?1 AND (typeof(o.qos)!='integer' OR o.qos NOT IN (0,1,2) OR \
                 typeof(o.cause)!='integer' OR o.cause NOT IN (1,2) OR \
                 typeof(o.retain)!='integer' OR o.retain NOT IN (0,1) OR \
                 typeof(o.phase)!='integer' OR o.phase NOT IN (1,2) OR (o.phase=2 AND o.qos!=2) \
                 OR p.id IS NULL OR typeof(p.qos)!='integer' OR p.qos NOT IN (0,1,2) OR \
                 (o.retain=1 AND p.retain!=1) OR (o.cause=2 AND (o.retain!=1 OR \
                 p.retained_version IS NULL OR p.retained_version=p.id)) OR (o.cause=1 AND \
                 p.retained_version IS NOT NULL)) \
                 LIMIT 1)",
            )?
            .query_row([session.to_be_bytes().as_slice()], |row| row.get(0))?;
        if invalid {
            return Err(Fault::Corrupt);
        }
        // A missing obligation must not turn an already claimed delivery into a
        // successful empty scan. Bound the target's claim set before checking it.
        let claims: i64 = read
            .prepare_cached(
                "SELECT count(*) FROM (SELECT 1 FROM outbound WHERE target=?1 LIMIT ?2)",
            )?
            .query_row(
                params![
                    session.to_be_bytes().as_slice(),
                    i64::try_from(MAX_RECOVERY_INVENTORY_ROWS + 1).map_err(|_| Fault::Corrupt)?
                ],
                |row| row.get(0),
            )?;
        if usize::try_from(claims).map_err(|_| Fault::Corrupt)? > MAX_RECOVERY_INVENTORY_ROWS {
            return Err(Fault::Read(ReadError::LimitExceeded));
        }
        let broken: bool = read
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM outbound c \
                 LEFT JOIN obligations o ON o.publication=c.publication AND o.idx=c.idx \
                 WHERE c.target=?1 AND (o.publication IS NULL OR o.target!=c.target OR \
                 o.target_revision!=c.target_revision OR o.qos NOT IN (1,2)) \
                 LIMIT 1)",
            )?
            .query_row([session.to_be_bytes().as_slice()], |row| row.get(0))?;
        if broken {
            return Err(Fault::Corrupt);
        }
        if counts_bounded {
            caches.work_verified.insert(session);
        }
    }
    const SQL_FIRST: &str = "SELECT \
                             o.publication,o.idx,o.target_revision,p.topic,y.bytes,c.packet_id,c.claim,c.target,c.target_revision,s.revision,s.state,o.qos,o.retain,p.expiry_domain,p.expiry_tick,p.expiry_interval,c.started,o.cause,o.phase,p.properties \
                             FROM obligations o LEFT JOIN publications p ON p.id=o.publication \
                             LEFT JOIN payloads y ON y.id=p.payload \
                             LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx \
                             LEFT JOIN sessions s ON s.id=o.target \
                             WHERE o.target=?1 ORDER BY o.publication,o.idx \
                             LIMIT ?2";
    const SQL_AFTER: &str = "SELECT \
                             o.publication,o.idx,o.target_revision,p.topic,y.bytes,c.packet_id,c.claim,c.target,c.target_revision,s.revision,s.state,o.qos,o.retain,p.expiry_domain,p.expiry_tick,p.expiry_interval,c.started,o.cause,o.phase,p.properties \
                             FROM obligations o LEFT JOIN publications p ON p.id=o.publication \
                             LEFT JOIN payloads y ON y.id=p.payload \
                             LEFT JOIN outbound c ON c.publication=o.publication AND c.idx=o.idx \
                             LEFT JOIN sessions s ON s.id=o.target \
                             WHERE o.target=?1 AND (o.publication,o.idx)>(?2,?3) \
                             ORDER BY o.publication,o.idx \
                             LIMIT ?4";
    let mut items = BoundedVec::with_capacity(limit).map_err(|_| ReadError::AllocationFailed)?;
    let mut meter = reader::Meter::new(budget);
    let mut more = false;
    let mut decode_rows = |mut rows: rusqlite::Rows<'_>| -> Result<(), Fault> {
        while let Some(row) = rows.next()? {
            if items.len() == limit {
                more = true;
                break;
            }
            budget.check_field(32)?;
            let topic = reader::borrow_blob(row, 3, meta.limits.max_topic_bytes)?;
            let payload = reader::borrow_blob(row, 4, meta.limits.max_payload_bytes)?;
            let max_props = mqttiumd_contract::properties::MAX_PROPERTY_BYTES + 1024;
            let props_raw = match row.get_ref(19)? {
                rusqlite::types::ValueRef::Null => None,
                rusqlite::types::ValueRef::Blob(bytes) => {
                    if bytes.len() > max_props {
                        return Err(ReadError::LimitExceeded.into());
                    }
                    Some(bytes)
                }
                _ => return Err(Fault::Corrupt),
            };
            budget.check_field(topic.len())?;
            budget.check_field(payload.len())?;
            if let Some(raw) = props_raw {
                budget.check_field(raw.len())?;
            }
            meter.charge(
                256usize
                    .checked_add(topic.len())
                    .and_then(|n| n.checked_add(payload.len()))
                    .and_then(|n| n.checked_add(props_raw.map_or(0, <[u8]>::len)))
                    .ok_or(ReadError::LimitExceeded)?,
            )?;
            let properties = match props_raw {
                None => None,
                Some(raw) => {
                    let parsed =
                        mqttiumd_contract::properties::PublishProperties::decode_canonical(raw)
                            .map_err(|_| Fault::Corrupt)?;
                    if parsed.is_empty() {
                        return Err(Fault::Corrupt);
                    }
                    Some(std::sync::Arc::new(parsed))
                }
            };
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
            let qos = code_qos(row.get(11)?)?;
            let expiry = message_expiry::decode(row, 13)?;
            let await_pubcomp =
                code_obligation_phase(row.get(18)?)? == ObligationPhase::AwaitPubcomp;
            let claim = match row.get_ref(5)? {
                rusqlite::types::ValueRef::Null => {
                    if await_pubcomp || !matches!(row.get_ref(6)?, rusqlite::types::ValueRef::Null)
                    {
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
                        started: message_expiry::started(row, 16)?,
                        await_pubcomp,
                    })
                }
            };
            if claim.is_some_and(|claim| !claim.valid_for(qos, expiry.is_some())) {
                return Err(Fault::Corrupt);
            }
            items
                .try_push(WorkItem {
                    delivery,
                    target: session,
                    target_revision: revision,
                    qos,
                    cause: crate::codec::code_cause(row.get(17)?)?,
                    expiry,
                    properties,
                    retain: match row.get::<_, i64>(12)? {
                        0 => false,
                        1 => true,
                        _ => return Err(Fault::Corrupt),
                    },
                    topic: SharedBytes::copy_from(topic, meta.limits.max_topic_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    payload: SharedBytes::copy_from(payload, meta.limits.max_payload_bytes)
                        .map_err(|_| ReadError::LimitExceeded)?,
                    claim,
                })
                .map_err(|_| ReadError::LimitExceeded)?;
        }
        Ok(())
    };
    let fetch_limit = i64::try_from(limit + 1).map_err(|_| Fault::Corrupt)?;
    if let Some(after) = after {
        let mut statement = read.prepare_cached(SQL_AFTER)?;
        let rows = statement.query(params![
            session.to_be_bytes().as_slice(),
            after.publication.to_be_bytes().as_slice(),
            i64::from(after.index),
            fetch_limit
        ])?;
        decode_rows(rows)?;
    } else {
        let mut statement = read.prepare_cached(SQL_FIRST)?;
        let rows = statement.query(params![session.to_be_bytes().as_slice(), fetch_limit])?;
        decode_rows(rows)?;
    }
    let next = if more {
        items.as_slice().last().map(|item| item.delivery)
    } else {
        None
    };
    read.commit()?;
    Ok(WorkPage { items, next })
}
