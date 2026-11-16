//! Bounded retirement controls. These have no OperationId or recursive receipt.
use super::*;
use mqttiumd_contract::ids::{LogicalOwnerId, PayloadSeq, PublicationSeq};
use mqttiumd_contract::receipts::{
    MAX_COLLECTION_ROWS, MAX_RECEIPT_ORIGINS, RECEIPT_CONTROL_RESERVE, RECEIPT_DATA_WINDOW,
    RECEIPT_WINDOW, ReceiptCollectionRefusal,
};

// Compiled, narrow system-reception mutations. Ordinary builds keep all guards.
pub(crate) const MUTANT_DELETE_WITHOUT_CLOSURE: bool = false;
pub(crate) const MUTANT_ALLOW_OLD_PUBLICATION: bool = false;
pub(crate) const MUTANT_ALLOW_OLD_PAYLOAD: bool = false;
pub(crate) const MUTANT_DOUBLE_RECEIPT_CREDIT: bool = false;

fn origin_bounds(origin: ReceiptOrigin, through: OperationSeq) -> ([u8; 32], [u8; 32]) {
    (
        OperationId::new(
            origin.incarnation,
            origin.epoch,
            origin.owner,
            OperationSeq::new(0),
        )
        .to_be_bytes(),
        OperationId::new(origin.incarnation, origin.epoch, origin.owner, through).to_be_bytes(),
    )
}

pub(crate) fn decode_frontier(
    row: &rusqlite::Row<'_>,
    meta: &Meta,
) -> Result<ReceiptClosure, Fault> {
    let value =
        |index| -> Result<u64, Fault> { Ok(blob_u64(reader::borrow_blob(row, index, 8)?)?) };
    let origin = ReceiptOrigin {
        incarnation: meta.ledger.incarnation(),
        epoch: RunEpoch::new(value(0)?),
        owner: LogicalOwnerId::new(value(1)?),
    };
    if origin.epoch.get() == 0 || origin.epoch > meta.ledger.last_opened() {
        return Err(Fault::Corrupt);
    }
    Ok(ReceiptClosure {
        origin,
        through: OperationSeq::new(value(2)?),
        publication_through: PublicationSeq::new(value(3)?),
        payload_through: PayloadSeq::new(value(4)?),
    })
}

pub(crate) fn frontier(
    conn: &Connection,
    meta: &Meta,
    origin: ReceiptOrigin,
) -> Result<Option<ReceiptClosure>, Fault> {
    if origin.incarnation != meta.ledger.incarnation() {
        return Err(Fault::Corrupt);
    }
    let mut statement = conn.prepare_cached("SELECT epoch,owner,consumed_through,publication_through,payload_through FROM receipt_origins WHERE epoch=?1 AND owner=?2")?;
    let mut rows = statement.query(params![
        origin.epoch.to_be_bytes().as_slice(),
        origin.owner.to_be_bytes().as_slice()
    ])?;
    rows.next()?
        .map(|row| decode_frontier(row, meta))
        .transpose()
}

pub(crate) fn all_frontiers(
    conn: &Connection,
    meta: &Meta,
    budget: ReadBudget,
) -> Result<BTreeMap<ReceiptOrigin, ReceiptClosure>, Fault> {
    let mut out = BTreeMap::new();
    let mut meter = reader::Meter::new(budget);
    let mut statement = conn.prepare_cached("SELECT epoch,owner,consumed_through,publication_through,payload_through FROM receipt_origins ORDER BY epoch,owner LIMIT ?1")?;
    let mut rows =
        statement.query([i64::try_from(MAX_RECEIPT_ORIGINS + 1).map_err(|_| Fault::Corrupt)?])?;
    while let Some(row) = rows.next()? {
        meter.charge(128)?;
        if out.len() >= MAX_RECEIPT_ORIGINS {
            return Err(Fault::Read(ReadError::LimitExceeded));
        }
        let closure = decode_frontier(row, meta)?;
        out.insert(closure.origin, closure);
    }
    Ok(out)
}

pub(crate) fn capacity(
    conn: &Connection,
    meta: &Meta,
    operation: OperationId,
    class: Option<ReceiptClass>,
) -> Result<bool, Fault> {
    if let Some(closure) = frontier(conn, meta, ReceiptOrigin::of(operation))? {
        let span = operation
            .sequence()
            .get()
            .checked_sub(closure.through.get());
        let window = if class == Some(ReceiptClass::Business) {
            RECEIPT_DATA_WINDOW
        } else {
            RECEIPT_WINDOW
        };
        if span.is_none_or(|span| span > window as u64) {
            return Ok(false);
        }
    }
    if class == Some(ReceiptClass::Business) {
        let enabled: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM receipt_origins LIMIT 1)",
            [],
            |row| row.get(0),
        )?;
        if enabled
            && meta.accounts.receipt_slots.get()
                >= meta
                    .limits
                    .receipt_slots
                    .get()
                    .saturating_sub(RECEIPT_CONTROL_RESERVE)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn admission_receipt(
    meta: &Meta,
    operation: OperationId,
    record: &OperationRecord,
) -> Result<Option<AdmitPublication>, Fault> {
    let Decision::Committed(CommitFacts::PublicationAdmitted {
        publication,
        exchange,
    }) = record.decision
    else {
        return Ok(None);
    };
    let recipe = Recipe::decode_canonical(
        &record.recipe,
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
    let Recipe::AdmitPublication(admit) = recipe else {
        return Err(Fault::Corrupt);
    };
    if admit.operation != operation
        || admit.publication != publication
        || admit.source.exchange != exchange
        || (
            admit.publication.incarnation(),
            admit.publication.epoch(),
            admit.publication.owner(),
        ) != (
            operation.incarnation(),
            operation.epoch(),
            operation.owner(),
        )
        || (
            admit.payload.id.incarnation(),
            admit.payload.id.epoch(),
            admit.payload.id.owner(),
        ) != (
            operation.incarnation(),
            operation.epoch(),
            operation.owner(),
        )
    {
        return Err(Fault::Corrupt);
    }
    Ok(Some(admit))
}

fn initial_compatible(
    conn: &Connection,
    meta: &Meta,
    origin: ReceiptOrigin,
) -> Result<bool, Fault> {
    let (first, last) = origin_bounds(origin, OperationSeq::new(u64::MAX));
    let zero = OperationId::new(
        origin.incarnation,
        origin.epoch,
        origin.owner,
        OperationSeq::new(0),
    );
    if exists(
        conn,
        "SELECT 1 FROM operations WHERE id=?1",
        &zero.to_be_bytes(),
    )? || fenced(conn, zero)?
        || exists(conn, "SELECT 1 FROM publications WHERE id=?1", &first)?
        || exists(conn, "SELECT 1 FROM payloads WHERE id=?1", &first)?
    {
        return Ok(false);
    }
    let mut meter = reader::Meter::new(ReadBudget::SNAPSHOT);
    let mut statement = conn.prepare_cached("SELECT id,recipe,decision,publication,exchange,reason,resource,facts FROM operations WHERE id>=?1 AND id<=?2 ORDER BY id")?;
    let mut rows = statement.query(params![first.as_slice(), last.as_slice()])?;
    while let Some(row) = rows.next()? {
        let bytes = reader::borrow_blob(row, 1, ReadBudget::SNAPSHOT.max_field_bytes)?;
        meter.charge(
            128usize
                .checked_add(bytes.len())
                .ok_or(ReadError::LimitExceeded)?,
        )?;
        let operation = OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
            .map_err(|_| Fault::Corrupt)?;
        let record = OperationRecord {
            recipe: reader::copy_blob(row, 1, ReadBudget::SNAPSHOT.max_field_bytes)?,
            decision: decision_of(row, 2)?.decode()?,
        };
        if let Some(admit) = admission_receipt(meta, operation, &record)?
            && (admit.publication.sequence().get() == 0 || admit.payload.id.sequence().get() == 0)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

// At most 2*(limit+1) fixed-size keys are copied, then a combined limit is applied.
fn receipt_keys(
    conn: &Connection,
    first: &[u8],
    last: &[u8],
    limit: u16,
) -> Result<Vec<(OperationId, bool)>, Fault> {
    let ceiling = usize::from(limit) + 1;
    let mut keys = Vec::new();
    keys.try_reserve_exact(ceiling * 2)
        .map_err(|_| ReadError::AllocationFailed)?;
    for (table, fence) in [("operations", false), ("fences", true)] {
        let mut statement = conn.prepare_cached(&format!(
            "SELECT id FROM {table} WHERE id>=?1 AND id<=?2 ORDER BY id LIMIT ?3"
        ))?;
        let mut rows = statement.query(params![
            first,
            last,
            i64::try_from(ceiling).map_err(|_| Fault::Corrupt)?
        ])?;
        while let Some(row) = rows.next()? {
            keys.push((
                OperationId::from_be_slice(reader::borrow_blob(row, 0, 32)?)
                    .map_err(|_| Fault::Corrupt)?,
                fence,
            ));
        }
    }
    keys.sort_unstable();
    if keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Fault::Corrupt);
    }
    Ok(keys)
}

fn remove_keys(
    conn: &Connection,
    meta: &mut Meta,
    keys: &[(OperationId, bool)],
) -> Result<u64, Fault> {
    for &(operation, fence) in keys {
        let table = if fence { "fences" } else { "operations" };
        let count = conn
            .prepare_cached(&format!("DELETE FROM {table} WHERE id=?1"))?
            .execute([operation.to_be_bytes().as_slice()])?;
        if count != 1 {
            return Err(Fault::Corrupt);
        }
    }
    let removed = u64::try_from(keys.len()).map_err(|_| Fault::Corrupt)?;
    let mut retained = meta
        .accounts
        .receipt_slots
        .get()
        .checked_sub(removed)
        .ok_or(Fault::Corrupt)?;
    if MUTANT_DOUBLE_RECEIPT_CREDIT {
        retained = retained.saturating_sub(removed);
    }
    meta.accounts.receipt_slots = ReceiptSlots::new(retained);
    Ok(retained)
}

pub(crate) fn prepare(
    conn: &Connection,
    meta: &mut Meta,
    collection: ReceiptCollection,
) -> Result<(ReceiptCollectionReply, bool), Fault> {
    let refused = |why| Ok((ReceiptCollectionReply::Refused(why), false));
    let request = collection.closure;
    let origin = request.origin;
    if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
        return refused(ReceiptCollectionRefusal::LimitExceeded);
    }
    if origin.incarnation != meta.ledger.incarnation() {
        return refused(ReceiptCollectionRefusal::OriginMismatch);
    }
    if origin.epoch > meta.ledger.last_opened() {
        return refused(ReceiptCollectionRefusal::FutureEpoch);
    }
    if origin.epoch.get() == 0 || origin.epoch <= meta.collected_through {
        return refused(ReceiptCollectionRefusal::InvalidFrontier);
    }
    let stored = frontier(conn, meta, origin)?;
    if stored.is_none() {
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM (SELECT 1 FROM receipt_origins LIMIT ?1)",
            [i64::try_from(MAX_RECEIPT_ORIGINS + 1).map_err(|_| Fault::Corrupt)?],
            |row| row.get(0),
        )?;
        if count >= MAX_RECEIPT_ORIGINS as i64 {
            return refused(ReceiptCollectionRefusal::OriginCapacity);
        }
        if !initial_compatible(conn, meta, origin)? {
            return refused(ReceiptCollectionRefusal::InvalidFrontier);
        }
    }
    let prior = stored.unwrap_or(ReceiptClosure::empty(origin));
    let closure = if request.through <= prior.through {
        if request.publication_through > prior.publication_through
            || request.payload_through > prior.payload_through
        {
            return refused(ReceiptCollectionRefusal::InvalidFrontier);
        }
        prior
    } else {
        let span = request
            .through
            .get()
            .checked_sub(prior.through.get())
            .ok_or(Fault::Corrupt)?;
        if span > u64::from(MAX_COLLECTION_ROWS) {
            return refused(ReceiptCollectionRefusal::LimitExceeded);
        }
        if request.publication_through < prior.publication_through
            || request.payload_through < prior.payload_through
        {
            return refused(ReceiptCollectionRefusal::InvalidFrontier);
        }
        let mut meter = reader::Meter::new(ReadBudget::COLLECTION);
        for seq in prior.through.get() + 1..=request.through.get() {
            let operation = OperationId::new(
                origin.incarnation,
                origin.epoch,
                origin.owner,
                OperationSeq::new(seq),
            );
            let record = read_operation_bounded(conn, operation, &mut meter)?;
            let fence = fenced(conn, operation)?;
            if record.is_some() && fence {
                return Err(Fault::Corrupt);
            }
            let Some(record) = record else {
                if !fence {
                    return refused(ReceiptCollectionRefusal::Gap);
                }
                meter.charge(32)?;
                continue;
            };
            if let Some(admit) = admission_receipt(meta, operation, &record)?
                && (admit.publication.sequence() > request.publication_through
                    || admit.payload.id.sequence() > request.payload_through)
            {
                return refused(ReceiptCollectionRefusal::InvalidFrontier);
            }
        }
        request
    };
    let (first, last) = origin_bounds(origin, closure.through);
    let mut keys = receipt_keys(conn, &first, &last, collection.limit)?;
    let more = keys.len() > usize::from(collection.limit);
    keys.truncate(usize::from(collection.limit));
    let removed = u16::try_from(keys.len()).map_err(|_| Fault::Corrupt)?;
    let changed = stored != Some(closure) || removed != 0;
    if changed && (!MUTANT_DELETE_WITHOUT_CLOSURE || stored.is_none()) {
        conn.prepare_cached("INSERT INTO receipt_origins(epoch,owner,consumed_through,publication_through,payload_through) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(epoch,owner) DO UPDATE SET consumed_through=excluded.consumed_through,publication_through=excluded.publication_through,payload_through=excluded.payload_through")?
            .execute(params![origin.epoch.to_be_bytes().as_slice(),origin.owner.to_be_bytes().as_slice(),closure.through.to_be_bytes().as_slice(),closure.publication_through.to_be_bytes().as_slice(),closure.payload_through.to_be_bytes().as_slice()])?;
    }
    let retained = remove_keys(conn, meta, &keys)?;
    Ok((
        ReceiptCollectionReply::Applied {
            closure,
            removed,
            retained,
            more,
        },
        changed,
    ))
}

fn read_operation_bounded(
    conn: &Connection,
    operation: OperationId,
    meter: &mut reader::Meter,
) -> Result<Option<OperationRecord>, Fault> {
    let mut statement=conn.prepare_cached("SELECT recipe,decision,publication,exchange,reason,resource,facts FROM operations WHERE id=?1")?;
    let mut rows = statement.query([operation.to_be_bytes().as_slice()])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let recipe = reader::borrow_blob(row, 0, ReadBudget::COLLECTION.max_field_bytes)?;
    meter.charge(
        128usize
            .checked_add(recipe.len())
            .ok_or(ReadError::LimitExceeded)?,
    )?;
    Ok(Some(OperationRecord {
        recipe: reader::copy_blob(row, 0, ReadBudget::COLLECTION.max_field_bytes)?,
        decision: decision_of(row, 1)?.decode()?,
    }))
}

pub(crate) fn prepare_closed(
    conn: &Connection,
    meta: &mut Meta,
    collection: ClosedReceiptCollection,
) -> Result<(ClosedReceiptReply, bool), Fault> {
    let refused = |why| Ok((ClosedReceiptReply::Refused(why), false));
    if collection.limit == 0 || collection.limit > MAX_COLLECTION_ROWS {
        return refused(ReceiptCollectionRefusal::LimitExceeded);
    }
    if meta.ledger.phase_of(collection.through) != EpochPhase::Closed {
        return refused(ReceiptCollectionRefusal::FutureEpoch);
    }
    let through = collection.through.max(meta.collected_through);
    let incarnation = meta.ledger.incarnation();
    let first = OperationId::new(
        incarnation,
        RunEpoch::new(1),
        LogicalOwnerId::new(0),
        OperationSeq::new(0),
    )
    .to_be_bytes();
    let last = OperationId::new(
        incarnation,
        through,
        LogicalOwnerId::new(u64::MAX),
        OperationSeq::new(u64::MAX),
    )
    .to_be_bytes();
    let mut keys = receipt_keys(conn, &first, &last, collection.limit)?;
    let mut origins = Vec::new();
    origins
        .try_reserve_exact(usize::from(collection.limit) + 1)
        .map_err(|_| ReadError::AllocationFailed)?;
    let mut statement=conn.prepare_cached("SELECT epoch,owner FROM receipt_origins WHERE epoch>=?1 AND epoch<=?2 ORDER BY epoch,owner LIMIT ?3")?;
    let mut rows = statement.query(params![
        RunEpoch::new(1).to_be_bytes().as_slice(),
        through.to_be_bytes().as_slice(),
        i64::from(collection.limit) + 1
    ])?;
    while let Some(row) = rows.next()? {
        origins.push((reader::copy_blob(row, 0, 8)?, reader::copy_blob(row, 1, 8)?));
    }
    let more = keys
        .len()
        .checked_add(origins.len())
        .ok_or(ReadError::LimitExceeded)?
        > usize::from(collection.limit);
    keys.truncate(usize::from(collection.limit));
    origins.truncate(usize::from(collection.limit) - keys.len());
    let removed = u16::try_from(keys.len()).map_err(|_| Fault::Corrupt)?;
    let removed_origins = u16::try_from(origins.len()).map_err(|_| Fault::Corrupt)?;
    let changed = through != meta.collected_through || removed != 0 || removed_origins != 0;
    let retained = remove_keys(conn, meta, &keys)?;
    for (epoch, owner) in origins {
        if conn
            .prepare_cached("DELETE FROM receipt_origins WHERE epoch=?1 AND owner=?2")?
            .execute(params![epoch, owner])?
            != 1
        {
            return Err(Fault::Corrupt);
        }
    }
    meta.collected_through = through;
    Ok((
        ClosedReceiptReply::Applied {
            through,
            removed,
            removed_origins,
            retained,
            more,
        },
        changed,
    ))
}
