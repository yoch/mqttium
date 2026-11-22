//! Group commit (record 0045, ARC-020): several commands in one
//! transaction of the engine.
//!
//! `SqliteStore::apply_group` opens one `BEGIN IMMEDIATE` transaction and
//! runs in it, for each command in order, what `apply` runs: the same
//! reads recognise an operation that is already decided, the same function
//! stages its guards and rows, and the same function stages the receipt of
//! a rejection. Where `apply` begins a transaction of its own, a member
//! opens a savepoint; where `apply` rolls its transaction back, the member
//! rolls back to its savepoint. One `COMMIT` then makes every member
//! durable together.
//!
//! The answers are those of `apply` called on each command in turn, and
//! they are given only once that `COMMIT` has returned. A group that is
//! not committed, because the medium fails, the commit fails or the
//! engine dropped the transaction, leaves nothing: every member is then
//! answered `PendingOrUnknown`, whatever was computed for it, the member
//! that would have been rejected included. Absence is not rejection.
//!
//! What changes is the crash granularity: a kill before the commit loses
//! the whole group, a kill after it keeps the whole group, and no state in
//! between is ever durable. `apply` itself is unchanged: one command, one
//! transaction.

use rusqlite::{Connection, TransactionBehavior};

use mqttiumd_contract::command::{Command, EpochPhase};
use mqttiumd_contract::outcome::{RejectReason, StoreOutcome};
use mqttiumd_store_api::{Applied, Durability, DurableStore};

use crate::{
    Abort, Meta, Recognised, SqliteStore, StoreCaches, TxDelta, Txn, UNKNOWN, answered, committed,
    read_meta, recognise_with_meta, stage_command, stage_rejection, write_meta,
};

/// The largest number of commands one group holds (ARC-020: batches are
/// bounded).
pub const MAX_GROUP: usize = 64;

/// Why a group was not started. Nothing was read and nothing written; the
/// commands stay with the caller, untouched.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GroupRefused {
    /// More commands than `MAX_GROUP`.
    TooLarge { commands: usize, max: usize },
}

/// The transaction of the group is gone, or a savepoint of it could not be
/// closed: nothing of the group may be committed or answered.
struct Lost;

impl SqliteStore {
    /// Applies `commands` in order in one transaction and returns, in the
    /// same order, what `apply` would have answered for each of them had
    /// they been applied one after the other on the same state.
    ///
    /// A member sees the members before it: the same `OperationId`
    /// presented twice is decided once and recognised the second time. A
    /// member that is refused is rolled back alone and its rejection is
    /// recorded on a receipt in the same group; the members around it are
    /// not affected. A member that cannot be read or written is answered
    /// `PendingOrUnknown` and leaves nothing, as after a failed `apply`.
    ///
    /// The answers hold only when the commit of the group succeeded. When
    /// the group had something to write and was not committed, nothing of
    /// it is durable and every member is answered `PendingOrUnknown`. A
    /// group that had nothing to write commits nothing: its answers were
    /// read from what was already durable, as `apply` reads them.
    ///
    /// At most `MAX_GROUP` commands. A larger group is refused before
    /// anything is read. An empty group does nothing, and a group of one
    /// is `apply`.
    ///
    /// The kill point `store.commit.prepared` is passed once, before the
    /// commit of the group; `store.commit.logged` once, by that commit.
    pub fn apply_group(&mut self, commands: &[&Command]) -> Result<Vec<Applied>, GroupRefused> {
        if commands.len() > MAX_GROUP {
            return Err(GroupRefused::TooLarge {
                commands: commands.len(),
                max: MAX_GROUP,
            });
        }
        match commands {
            [] => return Ok(Vec::new()),
            [only] => return Ok(vec![self.apply(only)]),
            _ => {}
        }
        // Bounded by `MAX_GROUP`, checked above.
        let unknown = || vec![UNKNOWN; commands.len()];
        let failing = self.durability == Durability::Failing;
        let Ok(tx) = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
        else {
            return Ok(unknown());
        };
        let caches = self.caches.get_mut();
        if caches.sync(&tx).is_err() {
            caches.clear();
            return Ok(unknown());
        }
        let mut cached_meta: Option<Meta> = None;
        let mut group_delta = TxDelta::default();
        let mut replies = Vec::with_capacity(commands.len());
        let mut any_wrote = false;
        for command in commands {
            match member(&tx, &mut cached_meta, command, caches, &mut group_delta) {
                // The engine drops a whole transaction on some errors; a
                // statement after that would be committed on its own.
                Ok(applied) if !tx.is_autocommit() => {
                    any_wrote |= applied.wrote;
                    replies.push(applied);
                }
                // Dropping the transaction rolls back what is left of it.
                Ok(_) | Err(Lost) => {
                    caches.clear_receipt_caches();
                    return Ok(unknown());
                }
            }
        }
        if !any_wrote {
            // No savepoint was kept: every answer was read from the state
            // that was durable before the group, and there is nothing to
            // commit. Dropping the transaction ends it.
            return Ok(replies);
        }
        let Some(meta) = cached_meta.as_ref() else {
            caches.clear_receipt_caches();
            return Ok(unknown());
        };
        if write_meta(&tx, meta).is_err() {
            caches.clear_receipt_caches();
            return Ok(unknown());
        }
        // K1 `store.commit.prepared`: every row of every member written,
        // `COMMIT` not called; the transaction is open.
        #[cfg(feature = "kill-points")]
        crate::kill_points::pass(
            &self.kill_hook,
            crate::kill_points::KillPoint::CommitPrepared,
        );
        if failing || tx.commit().is_err() {
            caches.clear_receipt_caches();
            return Ok(unknown());
        }
        group_delta.commit_into(caches);
        Ok(replies)
    }
}

/// One member of a group, on the transaction of the group: the steps of
/// `apply`, with a savepoint where `apply` has a transaction.
fn member(
    conn: &Connection,
    cached_meta: &mut Option<Meta>,
    command: &Command,
    caches: &mut StoreCaches,
    group_delta: &mut TxDelta,
) -> Result<Applied, Lost> {
    let meta = match cached_meta {
        Some(meta) => {
            #[cfg(test)]
            if crate::transaction_read_tests::take_if_transaction(conn).is_some() {
                return Ok(UNKNOWN);
            }
            meta
        }
        None => match read_meta(conn) {
            Ok(loaded) => cached_meta.insert(loaded),
            Err(_) => return Ok(UNKNOWN),
        },
    };
    let recipe = command.recipe.get();
    let operation = recipe.operation();
    let canonical = command.recipe.canonical_bytes();
    let phase = match recognise_with_meta(conn, meta, command, canonical, caches, group_delta) {
        Recognised::Settled(applied) => return Ok(applied),
        Recognised::Absent(phase) => phase,
    };
    let reason = if phase == EpochPhase::Open {
        let staged = savepoint(conn, meta, |conn, member_meta| {
            let mut member_delta = TxDelta::default();
            let facts = stage_command(
                conn,
                member_meta,
                caches,
                group_delta,
                &mut member_delta,
                command,
                canonical,
            )?;
            Ok((facts, member_delta))
        })?;
        match staged {
            Txn::Committed((facts, member_delta)) => {
                group_delta.extend_from(member_delta);
                return Ok(committed(facts));
            }
            Txn::Refused(reason) => reason,
            Txn::ReadFailed(_) | Txn::NotWritten => return Ok(UNKNOWN),
        }
    } else {
        RejectReason::PreconditionFailed
    };
    // The rejection is recorded as `commit_rejection` records it, in a
    // savepoint of its own: without a receipt slot nothing certain is said.
    let recorded = savepoint(conn, meta, |conn, member_meta| {
        let mut member_delta = TxDelta::default();
        stage_rejection(
            conn,
            member_meta,
            recipe,
            operation,
            canonical,
            reason,
            &mut member_delta,
        )?;
        Ok(member_delta)
    })?;
    Ok(match recorded {
        Txn::Committed(member_delta) => {
            group_delta.extend_from(member_delta);
            answered(StoreOutcome::RejectedCertain(reason), true)
        }
        Txn::Refused(()) | Txn::ReadFailed(_) | Txn::NotWritten => UNKNOWN,
    })
}

/// Runs `prepare` in a savepoint of the open transaction, as `transact`
/// runs it in a transaction: on success `meta` advances one revision in
/// memory (written once before `COMMIT`) and the savepoint is released into
/// the group; otherwise everything `prepare` wrote is rolled back and `meta`
/// stays unchanged.
/// `Txn::Committed` means staged here: nothing is durable before the
/// commit of the group.
fn savepoint<T, E>(
    conn: &Connection,
    meta: &mut Meta,
    prepare: impl FnOnce(&Connection, &mut Meta) -> Result<T, Abort<E>>,
) -> Result<Txn<T, E>, Lost> {
    let run = |sql: &str| conn.execute_batch(sql).map_err(|_| Lost);
    run("SAVEPOINT member")?;
    let mut member_meta = *meta;
    let staged = (|| {
        #[cfg(test)]
        if let Some(error) = crate::transaction_read_tests::take_if_transaction(conn) {
            return Err(Some(error));
        }
        let value = match prepare(conn, &mut member_meta) {
            Ok(value) => value,
            Err(Abort::Refused(refusal)) => return Ok(Err(refusal)),
            Err(Abort::Read(error)) => return Err(Some(error)),
            Err(Abort::Fault) => return Err(None),
        };
        member_meta.revision = member_meta.revision.checked_add(1).ok_or(None)?;
        Ok(Ok(value))
    })();
    let undone = match staged {
        Ok(Ok(value)) => {
            run("RELEASE member")?;
            *meta = member_meta;
            return Ok(Txn::Committed(value));
        }
        Ok(Err(refusal)) => Txn::Refused(refusal),
        Err(Some(error)) => Txn::ReadFailed(error),
        Err(None) => Txn::NotWritten,
    };
    // Fails when the engine already dropped the transaction: the group is
    // lost, and no statement may follow outside it.
    run("ROLLBACK TO member")?;
    run("RELEASE member")?;
    Ok(undone)
}
