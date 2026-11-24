//! Per-origin receipt identity witness (`IntervalSet`) and Will receipt byte accounting.

use std::collections::BTreeMap;

use mqttiumd_contract::command::{Recipe, RecipeError};
use mqttiumd_contract::ids::{
    LogicalOwnerId, OperationId, PayloadId, PayloadSeq, PublicationId, PublicationSeq, RunEpoch,
    StoreIncarnation,
};
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

/// Prefix-stripped compact representation of `Option<Option<IdentityBounds>>`
/// stored inside `VOperation` (32 B vs 104 B), with zero heap allocations on
/// same-scope live commands and an exact boxed fallback for foreign scopes.
#[derive(Clone, Debug)]
pub(crate) enum CachedBounds {
    Unknown,
    None,
    SameScope {
        first_seq: u64,
        last_seq: u64,
        payload_seq: u64,
        has_payload: bool,
    },
    Foreign(Box<IdentityBounds>),
}

impl CachedBounds {
    #[inline]
    pub(crate) fn from_known(
        scope: (StoreIncarnation, RunEpoch, LogicalOwnerId),
        bounds: Option<IdentityBounds>,
    ) -> Self {
        let Some(b) = bounds else {
            return Self::None;
        };
        let same_first = (b.first.incarnation(), b.first.epoch(), b.first.owner()) == scope;
        let same_last = (b.last.incarnation(), b.last.epoch(), b.last.owner()) == scope;
        let same_payload = b
            .payload
            .is_none_or(|p| (p.incarnation(), p.epoch(), p.owner()) == scope);
        if same_first && same_last && same_payload {
            Self::SameScope {
                first_seq: b.first.sequence().get(),
                last_seq: b.last.sequence().get(),
                payload_seq: b.payload.map_or(0, |p| p.sequence().get()),
                has_payload: b.payload.is_some(),
            }
        } else {
            Self::Foreign(Box::new(b))
        }
    }

    #[inline]
    pub(crate) fn resolve(
        &self,
        scope: (StoreIncarnation, RunEpoch, LogicalOwnerId),
    ) -> Option<Option<IdentityBounds>> {
        match self {
            Self::Unknown => None,
            Self::None => Some(None),
            Self::SameScope {
                first_seq,
                last_seq,
                payload_seq,
                has_payload,
            } => Some(Some(IdentityBounds {
                first: PublicationId::new(
                    scope.0,
                    scope.1,
                    scope.2,
                    PublicationSeq::new(*first_seq),
                ),
                last: PublicationId::new(scope.0, scope.1, scope.2, PublicationSeq::new(*last_seq)),
                payload: (*has_payload).then(|| {
                    PayloadId::new(scope.0, scope.1, scope.2, PayloadSeq::new(*payload_seq))
                }),
            })),
            Self::Foreign(b) => Some(Some(**b)),
        }
    }
}

/// Disjoint, non-adjacent closed intervals `[start, end]`.
/// Monotonic sequences collapse into a single inline `(start, end)` pair in
/// O(1) time and zero heap allocations; non-contiguous sequences spill to `tail`.
#[derive(Default, Clone, Debug)]
pub(crate) struct IntervalSet {
    head: Option<(u64, u64)>,
    tail: BTreeMap<u64, u64>,
}

impl IntervalSet {
    #[inline]
    pub(crate) fn insert(&mut self, mut start: u64, mut end: u64) {
        if start > end {
            return;
        }
        if self.tail.is_empty() {
            match self.head {
                None => {
                    self.head = Some((start, end));
                    return;
                }
                Some((hs, he)) if start <= he.saturating_add(1) && hs <= end.saturating_add(1) => {
                    self.head = Some((hs.min(start), he.max(end)));
                    return;
                }
                Some((hs, he)) => {
                    self.tail.insert(hs, he);
                    self.head = None;
                }
            }
        }
        if let Some((&prev_start, &prev_end)) = self.tail.range(..=start).next_back()
            && prev_end.saturating_add(1) >= start
        {
            if prev_end >= end {
                return;
            }
            start = prev_start;
            let upper = end.saturating_add(1);
            if self
                .tail
                .range(start.saturating_add(1)..=upper)
                .next()
                .is_none()
            {
                if let Some(slot) = self.tail.get_mut(&prev_start) {
                    *slot = end;
                }
                return;
            }
        }
        let upper = end.saturating_add(1);
        let mut to_remove = Vec::new();
        for (&s, &e) in self.tail.range(start..=upper) {
            if e > end {
                end = e;
            }
            to_remove.push(s);
        }
        for s in to_remove {
            self.tail.remove(&s);
        }
        self.tail.insert(start, end);
        if self.tail.len() == 1
            && let Some((s, e)) = self.tail.pop_first()
        {
            self.head = Some((s, e));
        }
    }

    #[inline]
    pub(crate) fn overlaps(&self, start: u64, end: u64) -> bool {
        if start > end {
            return false;
        }
        if self.tail.is_empty() {
            return self.head.is_some_and(|(hs, he)| start <= he && hs <= end);
        }
        self.tail
            .range(..=end)
            .next_back()
            .is_some_and(|(_, &r_end)| r_end >= start)
    }

    /// Drops intervals fully covered by `<= through` (already guarded by the compact frontier).
    pub(crate) fn trim_through(&mut self, through: u64) {
        if self.tail.is_empty() {
            if let Some((hs, he)) = self.head {
                if he <= through {
                    self.head = None;
                } else if hs <= through {
                    self.head = Some((through.saturating_add(1), he));
                }
            }
            return;
        }
        let mut covered = Vec::new();
        let mut partial: Option<(u64, u64)> = None;
        for (&s, &e) in self.tail.range(..=through) {
            if e <= through {
                covered.push(s);
            } else {
                covered.push(s);
                partial = Some((through.saturating_add(1), e));
            }
        }
        for s in covered {
            self.tail.remove(&s);
        }
        if let Some((ns, ne)) = partial
            && ns <= ne
        {
            self.tail.insert(ns, ne);
        }
        if self.tail.len() == 1
            && let Some((s, e)) = self.tail.pop_first()
        {
            self.head = Some((s, e));
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

#[inline]
pub(crate) fn is_open_will_recipe(canonical: &[u8]) -> bool {
    canonical.get(6) == Some(&8) && matches!(canonical.get(39), Some(5 | 7))
}

#[inline]
pub(crate) fn is_open_will_recipe_obj(recipe: &Recipe) -> bool {
    matches!(
        recipe,
        Recipe::SessionLifecycle(lifecycle)
            if matches!(
                lifecycle.action,
                mqttiumd_contract::session::SessionAction::OpenWillAt { .. }
            )
    )
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
