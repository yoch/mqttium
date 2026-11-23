//! Per-origin receipt identity witness (`IntervalSet`) and Will receipt byte accounting.

use std::collections::BTreeMap;

use mqttiumd_contract::command::{Recipe, RecipeError};
use mqttiumd_contract::ids::{OperationId, PayloadId, PublicationId, PublicationSeq};
use mqttiumd_contract::outcome::CommitFacts;
use mqttiumd_contract::receipts::ReceiptOrigin;
use mqttiumd_contract::retained::MAX_REPLAYS;
use mqttiumd_store_api::{Decision, Limits, ReadError};

/// Bounded identity span reserved by a receipted command.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IdentityBounds {
    pub first: PublicationId,
    pub last: PublicationId,
    pub payload: Option<PayloadId>,
}

/// Disjoint, non-adjacent closed intervals `[start, end]`, keyed by `start`.
/// Monotonic sequences collapse into a single `(1, N)` interval in O(1) space.
#[derive(Default, Clone, Debug)]
pub(crate) struct IntervalSet {
    ranges: BTreeMap<u64, u64>,
}

impl IntervalSet {
    pub(crate) fn insert(&mut self, mut start: u64, mut end: u64) {
        if start > end {
            return;
        }
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..=start).next_back()
            && prev_end.saturating_add(1) >= start
        {
            if prev_end >= end {
                return;
            }
            start = prev_start;
            let upper = end.saturating_add(1);
            if self
                .ranges
                .range(start.saturating_add(1)..=upper)
                .next()
                .is_none()
            {
                if let Some(slot) = self.ranges.get_mut(&prev_start) {
                    *slot = end;
                }
                return;
            }
        }
        let upper = end.saturating_add(1);
        let mut to_remove = Vec::new();
        for (&s, &e) in self.ranges.range(start..=upper) {
            if e > end {
                end = e;
            }
            to_remove.push(s);
        }
        for s in to_remove {
            self.ranges.remove(&s);
        }
        self.ranges.insert(start, end);
    }

    pub(crate) fn overlaps(&self, start: u64, end: u64) -> bool {
        if start > end {
            return false;
        }
        self.ranges
            .range(..=end)
            .next_back()
            .is_some_and(|(_, &r_end)| r_end >= start)
    }

    /// Drops intervals fully covered by `<= through` (already guarded by the compact frontier).
    pub(crate) fn trim_through(&mut self, through: u64) {
        let mut covered = Vec::new();
        let mut partial: Option<(u64, u64)> = None;
        for (&s, &e) in self.ranges.range(..=through) {
            if e <= through {
                covered.push(s);
            } else {
                covered.push(s);
                partial = Some((through.saturating_add(1), e));
            }
        }
        for s in covered {
            self.ranges.remove(&s);
        }
        if let Some((ns, ne)) = partial
            && ns <= ne
        {
            self.ranges.insert(ns, ne);
        }
    }
}

/// Verified per-origin receipt witness intervals maintained incrementally.
#[derive(Default, Clone, Debug)]
pub(crate) struct OriginWitness {
    publications: IntervalSet,
    payloads: IntervalSet,
}

impl OriginWitness {
    pub(crate) fn record(&mut self, bounds: IdentityBounds) {
        self.publications
            .insert(bounds.first.sequence().get(), bounds.last.sequence().get());
        if let Some(payload) = bounds.payload {
            let seq = payload.sequence().get();
            self.payloads.insert(seq, seq);
        }
    }

    pub(crate) fn matches(
        &self,
        origin: ReceiptOrigin,
        first: PublicationId,
        last: PublicationId,
        payload: Option<PayloadId>,
    ) -> bool {
        let same_pub = (first.incarnation(), first.epoch(), first.owner())
            == (origin.incarnation, origin.epoch, origin.owner)
            && (last.incarnation(), last.epoch(), last.owner())
                == (origin.incarnation, origin.epoch, origin.owner);
        if same_pub
            && self
                .publications
                .overlaps(first.sequence().get(), last.sequence().get())
        {
            return true;
        }
        if let Some(p) = payload
            && (p.incarnation(), p.epoch(), p.owner())
                == (origin.incarnation, origin.epoch, origin.owner)
        {
            let seq = p.sequence().get();
            if self.payloads.overlaps(seq, seq) {
                return true;
            }
        }
        false
    }

    pub(crate) fn trim_through(&mut self, publication_through: u64, payload_through: u64) {
        self.publications.trim_through(publication_through);
        self.payloads.trim_through(payload_through);
    }
}

pub(crate) fn is_open_will_recipe(canonical: &[u8]) -> bool {
    canonical.get(6) == Some(&8) && matches!(canonical.get(39), Some(5 | 7))
}

pub(crate) fn bounds(recipe: &Recipe) -> Result<Option<IdentityBounds>, ReadError> {
    let one = |p, payload| {
        Some(IdentityBounds {
            first: p,
            last: p,
            payload: Some(payload),
        })
    };
    if let Recipe::WillPublication(a) = recipe {
        return Ok(one(a.publication, a.payload.id));
    }
    if let Some(a) = recipe.admission() {
        return Ok(one(a.publication, a.payload.id));
    }
    if let Some(a) = recipe.retained_qos0() {
        return Ok(one(a.publication, a.payload.id));
    }
    Ok(if let Some(a) = recipe.replay_subscriptions() {
        let first = a.first_publication;
        Some(IdentityBounds {
            first,
            last: PublicationId::new(
                first.incarnation(),
                first.epoch(),
                first.owner(),
                PublicationSeq::new(
                    first
                        .sequence()
                        .get()
                        .checked_add(MAX_REPLAYS as u64 - 1)
                        .ok_or(ReadError::InvalidData)?,
                ),
            ),
            payload: None,
        })
    } else {
        None
    })
}

pub(crate) fn receipt_bounds(
    limits: &Limits,
    operation: OperationId,
    recipe_bytes: &[u8],
    decision: Decision,
) -> Result<Option<IdentityBounds>, ReadError> {
    if matches!(decision, Decision::Rejected(_)) && !matches!(recipe_bytes.get(6), Some(12 | 14)) {
        return Ok(None);
    }
    let recipe = Recipe::decode_canonical(
        recipe_bytes,
        limits.max_targets_per_admission,
        limits.max_topic_bytes,
    )
    .map_err(|error| match error {
        RecipeError::AllocationFailed => ReadError::AllocationFailed,
        RecipeError::TooManyTargets { .. } | RecipeError::TopicTooLong(_) => {
            ReadError::LimitExceeded
        }
        _ => ReadError::InvalidData,
    })?;
    if recipe.operation() != operation {
        return Err(ReadError::InvalidData);
    }
    let Some(bounds) = bounds(&recipe)? else {
        return Ok(None);
    };
    let valid = match decision {
        Decision::Rejected(_) => recipe.replay_subscriptions().is_some(),
        Decision::Committed(CommitFacts::WillPublished { will, publication }) => {
            matches!(&recipe, Recipe::WillPublication(a) if a.will == will && a.publication == publication)
        }
        Decision::Committed(CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        }) => recipe
            .admission()
            .is_some_and(|a| a.publication == publication && a.source.exchange == exchange),
        Decision::Committed(CommitFacts::RetainedQos0Stored { publication }) => recipe
            .retained_qos0()
            .is_some_and(|a| a.publication == publication),
        Decision::Committed(CommitFacts::SubscriptionsChanged { session, .. }) => recipe
            .replay_subscriptions()
            .is_some_and(|a| a.update.session == session),
        _ => false,
    };
    let scope = |p: PublicationId| {
        (p.incarnation(), p.epoch(), p.owner())
            == (
                operation.incarnation(),
                operation.epoch(),
                operation.owner(),
            )
    };
    if !valid
        || !scope(bounds.first)
        || !scope(bounds.last)
        || bounds.payload.is_some_and(|p| {
            (p.incarnation(), p.epoch(), p.owner())
                != (
                    operation.incarnation(),
                    operation.epoch(),
                    operation.owner(),
                )
        })
    {
        return Err(ReadError::InvalidData);
    }
    Ok(Some(bounds))
}
