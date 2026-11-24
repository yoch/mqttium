//! Indexed per-destination routing projection, shared by durable and volatile
//! admission (ADR 0051).
use super::*;
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;

pub(super) const MAX_TARGETS: usize = 1_024;
const INLINE_TARGETS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Selected {
    pub(super) session: SessionId,
    pub(super) revision: Revision,
    pub(super) qos: Qos,
    pub(super) rap: bool,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Refusal {
    Capacity,
    Revision,
}
#[derive(Debug)]
pub(super) struct Selection {
    inline: [Option<Selected>; INLINE_TARGETS],
    wide: Option<Vec<Selected>>,
    count: usize,
}
impl Selection {
    const fn empty() -> Self {
        Self {
            inline: [None; INLINE_TARGETS],
            wide: None,
            count: 0,
        }
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn iter(&self) -> SelectionIter<'_> {
        match &self.wide {
            Some(items) => SelectionIter::Wide(items.iter()),
            None => SelectionIter::Inline(self.inline[..self.count].iter()),
        }
    }
    fn push(&mut self, item: Selected) {
        if let Some(items) = &mut self.wide {
            items.push(item);
            self.count = items.len();
        } else if self.count < INLINE_TARGETS {
            self.inline[self.count] = Some(item);
            self.count += 1;
        } else {
            let mut items = Vec::with_capacity(INLINE_TARGETS * 2);
            for existing in self.inline.into_iter().flatten() {
                items.push(existing);
            }
            items.push(item);
            self.count = items.len();
            self.wide = Some(items);
        }
    }
    fn sort_canonical(&mut self) {
        if self.count <= 1 {
            return;
        }
        if let Some(items) = &mut self.wide {
            items.sort_unstable_by_key(|item| item.session);
        } else {
            self.inline[..self.count]
                .sort_unstable_by_key(|item| item.map(|target| target.session));
        }
    }
}

pub(super) enum SelectionIter<'a> {
    Inline(core::slice::Iter<'a, Option<Selected>>),
    Wide(core::slice::Iter<'a, Selected>),
}

impl<'a> Iterator for SelectionIter<'a> {
    type Item = &'a Selected;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(iter) => loop {
                match iter.next()? {
                    Some(item) => return Some(item),
                    None => continue,
                }
            },
            Self::Wide(iter) => iter.next(),
        }
    }
}

#[derive(Debug, Default)]
struct TrieNode {
    children: BTreeMap<Box<[u8]>, TrieNode>,
    plus: Option<Box<TrieNode>>,
    terminal: Vec<usize>,
    multi_wildcard: Vec<usize>,
}

impl TrieNode {
    fn insert(&mut self, filter: &[u8], route_idx: usize) {
        let mut node = self;
        for level in filter.split(|&b| b == b'/') {
            if level == b"#" {
                node.multi_wildcard.push(route_idx);
                return;
            } else if level == b"+" {
                node = node
                    .plus
                    .get_or_insert_with(|| Box::new(TrieNode::default()));
            } else {
                if !node.children.contains_key(level) {
                    node.children.insert(Box::from(level), TrieNode::default());
                }
                node = node.children.get_mut(level).expect("inserted");
            }
        }
        node.terminal.push(route_idx);
    }

    fn visit<'a, F>(&'a self, levels: &[&[u8]], depth: usize, dollar_topic: bool, f: &mut F)
    where
        F: FnMut(&'a [usize]),
    {
        if !(depth == 0 && dollar_topic) && !self.multi_wildcard.is_empty() {
            f(&self.multi_wildcard);
        }
        if depth == levels.len() {
            if !self.terminal.is_empty() {
                f(&self.terminal);
            }
            return;
        }
        let level = levels[depth];
        if let Some(child) = self.children.get(level) {
            child.visit(levels, depth + 1, dollar_topic, f);
        }
        if !(depth == 0 && dollar_topic)
            && let Some(plus) = &self.plus
        {
            plus.visit(levels, depth + 1, dollar_topic, f);
        }
    }
}

/// Deterministic 64-bit FNV-1a hash for exact-topic bucket lookup.
/// Buckets always verify full byte equality so collisions are handled exactly
/// with deterministic ordering (FlashMQ/Redis-inspired fast exact routing).
fn topic_hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Debug)]
enum ExactBucket {
    Single(SharedBytes, Vec<usize>),
    Collision(Vec<(SharedBytes, Vec<usize>)>),
}

impl ExactBucket {
    fn get(&self, topic: &[u8]) -> Option<&[usize]> {
        match self {
            Self::Single(key, indices) => (key.as_slice() == topic).then_some(indices.as_slice()),
            Self::Collision(entries) => entries
                .iter()
                .find(|(key, _)| key.as_slice() == topic)
                .map(|(_, indices)| indices.as_slice()),
        }
    }

    fn insert(&mut self, topic: SharedBytes, route_idx: usize) {
        match self {
            Self::Single(key, indices) => {
                if key.as_slice() == topic.as_slice() {
                    indices.push(route_idx);
                } else {
                    let prev_key = key.clone();
                    let prev_indices = core::mem::take(indices);
                    *self = Self::Collision(alloc::vec![
                        (prev_key, prev_indices),
                        (topic, alloc::vec![route_idx]),
                    ]);
                }
            }
            Self::Collision(entries) => {
                if let Some((_, indices)) = entries
                    .iter_mut()
                    .find(|(key, _)| key.as_slice() == topic.as_slice())
                {
                    indices.push(route_idx);
                } else {
                    entries.push((topic, alloc::vec![route_idx]));
                }
            }
        }
    }
}

/// Deterministic hash-indexed exact-map + wildcard trie pointing at indices in
/// `Owner::routes`.
#[derive(Debug, Default)]
pub(super) struct RouteIndex {
    exact: FlatMap<u64, ExactBucket>,
    wildcard: TrieNode,
    wildcard_count: usize,
    total: usize,
}

impl RouteIndex {
    pub(super) fn clear(&mut self) {
        self.exact.clear();
        self.wildcard = TrieNode::default();
        self.wildcard_count = 0;
        self.total = 0;
    }

    pub(super) fn has_wildcards(&self) -> bool {
        self.wildcard_count > 0
    }

    pub(super) fn wildcard_count(&self) -> usize {
        self.wildcard_count
    }

    pub(super) fn insert(&mut self, index: usize, route: &Route) {
        let bytes = route.topic.as_slice();
        if bytes.contains(&b'+') || bytes.contains(&b'#') {
            self.wildcard.insert(bytes, index);
            self.wildcard_count += 1;
        } else {
            let hash = topic_hash64(bytes);
            if let Some(bucket) = self.exact.get_mut(&hash) {
                bucket.insert(route.topic.clone(), index);
            } else {
                self.exact.insert(
                    hash,
                    ExactBucket::Single(route.topic.clone(), alloc::vec![index]),
                );
            }
        }
        self.total += 1;
    }

    pub(super) fn rebuild(&mut self, routes: &[Route]) {
        self.clear();
        for (index, route) in routes.iter().enumerate() {
            self.insert(index, route);
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "shared indexed selector accepts read-only owner tables without cloning"
    )]
    pub(super) fn select(
        &self,
        routes: &[Route],
        sessions: &[Session],
        session_by_id: &FlatMap<SessionId, usize>,
        source: SessionId,
        topic: &[u8],
        qos: Qos,
        maximum: u32,
    ) -> Result<Selection, Refusal> {
        let client = session_by_id
            .get(&source)
            .and_then(|&idx| sessions.get(idx))
            .map(|session| session.client_id.as_slice());
        self.select_client(routes, sessions, session_by_id, client, topic, qos, maximum)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "shared indexed selector accepts read-only owner tables without cloning"
    )]
    pub(super) fn select_client(
        &self,
        routes: &[Route],
        sessions: &[Session],
        session_by_id: &FlatMap<SessionId, usize>,
        source: Option<&[u8]>,
        topic: &[u8],
        qos: Qos,
        maximum: u32,
    ) -> Result<Selection, Refusal> {
        if routes.len() > crate::contract::pubsub::MAX_SUBSCRIPTIONS {
            return Err(Refusal::Capacity);
        }
        let limit = usize::try_from(maximum)
            .unwrap_or(usize::MAX)
            .min(MAX_TARGETS);
        let mut selected = Selection::empty();
        let mut wide_positions: Option<BTreeMap<SessionId, usize>> = None;
        let mut refusal: Option<Refusal> = None;

        let mut apply_bucket = |indices: &[usize]| {
            if refusal.is_some() {
                return;
            }
            for &route_idx in indices {
                let Some(route) = routes.get(route_idx) else {
                    refusal = Some(Refusal::Revision);
                    return;
                };
                let Some(target_session) = session_by_id
                    .get(&route.target)
                    .and_then(|&idx| sessions.get(idx))
                    .filter(|s| s.id == route.target && s.revision == route.target_revision)
                else {
                    refusal = Some(Refusal::Revision);
                    return;
                };
                if route.options.no_local() && source == Some(target_session.client_id.as_slice()) {
                    continue;
                }
                let effective = route.max_qos.min(qos);
                let existing = if let Some(positions) = &wide_positions {
                    positions.get(&route.target).copied()
                } else {
                    selected.inline[..selected.count]
                        .iter()
                        .position(|item| item.is_some_and(|h| h.session == route.target))
                };
                if let Some(pos) = existing {
                    let held = match &mut selected.wide {
                        Some(items) => &mut items[pos],
                        None => selected.inline[pos].as_mut().expect("present"),
                    };
                    if held.revision != route.target_revision {
                        refusal = Some(Refusal::Revision);
                        return;
                    }
                    held.qos = held.qos.max(effective);
                    held.rap |= route.options.retain_as_published();
                } else {
                    if selected.count == limit {
                        refusal = Some(Refusal::Capacity);
                        return;
                    }
                    let pos = selected.count;
                    if pos == INLINE_TARGETS && wide_positions.is_none() {
                        let mut map = BTreeMap::new();
                        for (i, item) in selected.inline.iter().enumerate() {
                            if let Some(entry) = item {
                                map.insert(entry.session, i);
                            }
                        }
                        wide_positions = Some(map);
                    }
                    selected.push(Selected {
                        session: route.target,
                        revision: route.target_revision,
                        qos: effective,
                        rap: route.options.retain_as_published(),
                    });
                    if let Some(map) = &mut wide_positions {
                        map.insert(route.target, pos);
                    }
                }
            }
        };

        if let Some(exact_indices) = self
            .exact
            .get(&topic_hash64(topic))
            .and_then(|bucket| bucket.get(topic))
        {
            apply_bucket(exact_indices);
        }
        if self.wildcard_count > 0 {
            let mut stack_levels: [&[u8]; 32] = [&[]; 32];
            let mut level_count = 0usize;
            let mut overflow_levels: Option<Vec<&[u8]>> = None;
            for seg in topic.split(|&b| b == b'/') {
                if let Some(v) = &mut overflow_levels {
                    v.push(seg);
                } else if level_count < stack_levels.len() {
                    stack_levels[level_count] = seg;
                    level_count += 1;
                } else {
                    let mut v = Vec::with_capacity(64);
                    v.extend_from_slice(&stack_levels);
                    v.push(seg);
                    overflow_levels = Some(v);
                }
            }
            let levels = match &overflow_levels {
                Some(v) => v.as_slice(),
                None => &stack_levels[..level_count],
            };
            let dollar_topic = topic.starts_with(b"$");
            self.wildcard
                .visit(levels, 0, dollar_topic, &mut apply_bucket);
        }
        if let Some(err) = refusal {
            return Err(err);
        }
        selected.sort_canonical();
        Ok(selected)
    }
}

#[cfg(test)]
mod reference {
    use super::*;

    /// Linear reference selector kept for tests and differential verification.
    pub(super) fn select(
        routes: &[Route],
        sessions: &[Session],
        source: SessionId,
        topic: &[u8],
        qos: Qos,
        maximum: u32,
    ) -> Result<Selection, Refusal> {
        let source = sessions
            .iter()
            .find(|session| session.id == source)
            .map(|session| session.client_id.as_slice());
        select_client(routes, sessions, source, topic, qos, maximum)
    }

    /// Linear reference selector kept for tests and differential verification.
    pub(super) fn select_client(
        routes: &[Route],
        sessions: &[Session],
        source: Option<&[u8]>,
        topic: &[u8],
        qos: Qos,
        maximum: u32,
    ) -> Result<Selection, Refusal> {
        let mut selected = Selection::empty();
        let mut wide_positions: Option<BTreeMap<SessionId, usize>> = None;
        if routes.len() > crate::contract::pubsub::MAX_SUBSCRIPTIONS {
            return Err(Refusal::Capacity);
        }
        let limit = usize::try_from(maximum)
            .unwrap_or(usize::MAX)
            .min(MAX_TARGETS);
        for route in routes {
            if !crate::contract::topic::matches(route.topic.as_slice(), topic) {
                continue;
            }
            let Some(target_session) = sessions
                .iter()
                .find(|s| s.id == route.target && s.revision == route.target_revision)
            else {
                return Err(Refusal::Revision);
            };
            if route.options.no_local() && source == Some(target_session.client_id.as_slice()) {
                continue;
            }
            let effective = route.max_qos.min(qos);
            let existing = if let Some(positions) = &wide_positions {
                positions.get(&route.target).copied()
            } else {
                selected.inline[..selected.count]
                    .iter()
                    .position(|item| item.is_some_and(|h| h.session == route.target))
            };
            if let Some(pos) = existing {
                let held = match &mut selected.wide {
                    Some(items) => &mut items[pos],
                    None => selected.inline[pos].as_mut().expect("present"),
                };
                if held.revision != route.target_revision {
                    return Err(Refusal::Revision);
                }
                held.qos = held.qos.max(effective);
                held.rap |= route.options.retain_as_published();
            } else {
                if selected.count == limit {
                    return Err(Refusal::Capacity);
                }
                let pos = selected.count;
                if pos == INLINE_TARGETS && wide_positions.is_none() {
                    let mut map = BTreeMap::new();
                    for (i, item) in selected.inline.iter().enumerate() {
                        if let Some(entry) = item {
                            map.insert(entry.session, i);
                        }
                    }
                    wide_positions = Some(map);
                }
                selected.push(Selected {
                    session: route.target,
                    revision: route.target_revision,
                    qos: effective,
                    rap: route.options.retain_as_published(),
                });
                if let Some(map) = &mut wide_positions {
                    map.insert(route.target, pos);
                }
            }
        }
        selected.sort_canonical();
        Ok(selected)
    }
}

impl Owner {
    pub(super) fn routing_work_bound(&self, input: &Input) -> u64 {
        if !matches!(
            input,
            Input::Packet {
                packet: InboundPacket::Publish { .. },
                ..
            }
        ) {
            return 0;
        }
        // Historical S0 exact/default fixtures retain their one-transition
        // work abstraction. Product registration or hierarchical options opt
        // into the additional bounded selection work, as for pub/sub work.
        if !self.lifecycle_enabled
            && self.pubsub.connections.is_empty()
            && !self.route_index.has_wildcards()
            && self
                .routes
                .as_slice()
                .iter()
                .all(|route| route.options == crate::contract::pubsub::SubscriptionOptions::DEFAULT)
        {
            return 0;
        }
        // Indexed exact lookup plus bounded wildcard trie walk (which may visit
        // up to `wildcard_count` overlapping filters that deduplicate onto a
        // single subscriber), per-target O(log S) session verification, and
        // O(T log T) canonical sort (ADR 0051).
        let targets = self
            .limits
            .map_or(0, |limits| {
                u64::from(limits.max_targets).min(MAX_TARGETS as u64)
            })
            .min(capacity_of(self.routes.capacity()));
        let wildcard_work = capacity_of(self.route_index.wildcard_count()).saturating_mul(2);
        let per_target = if self.route_index.has_wildcards() {
            16
        } else {
            8
        };
        targets
            .saturating_mul(per_target)
            .saturating_add(wildcard_work)
            .saturating_add(capacity_of(self.routes.capacity()).min(16))
    }
}

#[cfg(test)]
mod tests {
    use super::reference::select;
    use super::*;
    use crate::contract::effects::SubmitResult;
    use crate::contract::pubsub::SubscriptionOptions;
    use alloc::vec;
    fn bytes(value: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(value, 128).unwrap()
    }
    fn route(filter: &[u8], target: u64, qos: Qos, no_local: bool) -> Route {
        Route {
            topic: bytes(filter),
            target: SessionId::new(target),
            target_revision: Revision::new(1),
            max_qos: qos,
            options: SubscriptionOptions::from_bits(if no_local { 4 } else { 0 }).unwrap(),
        }
    }
    fn session(id: u64) -> Session {
        Session {
            id: SessionId::new(id),
            client_id: bytes(alloc::string::ToString::to_string(&id).as_bytes()),
            revision: Revision::new(1),
            connection: None,
            expiry: crate::contract::session::SessionExpiry::Never,
            opened_by: None,
            detached: None,
            will_managed: false,
        }
    }
    #[test]
    fn wildcard_contributions_aggregate_before_the_destination_budget() {
        let routes = [
            route(b"a/#", 1, Qos::AtLeastOnce, false),
            route(b"a/+", 1, Qos::AtMostOnce, false),
        ];
        let sessions = [session(1)];
        let selected = select(
            &routes,
            &sessions,
            SessionId::new(2),
            b"a/x",
            Qos::ExactlyOnce,
            1,
        )
        .unwrap_or_else(|reason| panic!("wildcard_routes_not_aggregated: {reason:?}"));
        assert_eq!(selected.len(), 1, "wildcard_routes_not_aggregated");
        assert_eq!(selected.iter().next().unwrap().qos, Qos::AtLeastOnce);
    }
    #[test]
    fn independent_filter_table_reaches_the_shared_routing_selector() {
        for (filter, topic, expected) in [
            ("a/#", "a", true),
            ("a/#", "a/", true),
            ("a/+", "a", false),
            ("a/+", "a/", true),
            ("+/+", "/", true),
            ("a/+/", "a//", true),
            ("a/+/#", "a/x", true),
            ("#", "$SYS/x", false),
            ("+/x", "$SYS/x", false),
            ("$SYS/#", "$SYS/x", true),
            ("温度/+/é", "温度//é", true),
            ("é/#", "e/x", false),
            ("a/+", "a/x/y", false),
            ("a", "A", false),
        ] {
            let routes = [route(filter.as_bytes(), 1, Qos::AtLeastOnce, false)];
            let sessions = [session(1)];
            let selected = select(
                &routes,
                &sessions,
                SessionId::new(2),
                topic.as_bytes(),
                Qos::AtLeastOnce,
                16,
            )
            .unwrap();
            assert_eq!(
                selected.len(),
                usize::from(expected),
                "selector_filter_table_disagrees: {filter} -> {topic}"
            );
        }
    }
    #[test]
    fn eligible_overlap_uses_maximum_qos_before_publish_cap() {
        let sessions = [session(1)];
        for order in [false, true] {
            let mut routes = [
                route(b"a/#", 1, Qos::AtLeastOnce, false),
                route(b"a/+", 1, Qos::AtMostOnce, false),
            ];
            if order {
                routes.reverse();
            }
            for published in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
                let selected =
                    select(&routes, &sessions, SessionId::new(2), b"a/x", published, 16).unwrap();
                assert_eq!(selected.len(), 1, "wildcard_routes_not_aggregated");
                assert_eq!(
                    selected.iter().next().unwrap().qos,
                    published.min(Qos::AtLeastOnce),
                    "wildcard_maximum_qos_lost"
                );
            }
        }
    }
    #[test]
    fn no_local_excludes_only_its_matching_contribution() {
        let sessions = [session(1), session(2)];
        let mut routes = [
            route(b"a/#", 1, Qos::AtLeastOnce, true),
            route(b"a/+", 1, Qos::AtMostOnce, false),
            route(b"a/#", 2, Qos::AtLeastOnce, true),
        ];
        for _ in 0..routes.len() {
            let selected = select(
                &routes,
                &sessions,
                SessionId::new(1),
                b"a/x",
                Qos::ExactlyOnce,
                16,
            )
            .unwrap();
            let actual: Vec<_> = selected.iter().map(|s| (s.session.get(), s.qos)).collect();
            assert_eq!(
                actual,
                [(1, Qos::AtMostOnce), (2, Qos::AtLeastOnce)],
                "no_local_applied_after_aggregation"
            );
            routes.rotate_left(1);
        }
    }
    #[test]
    fn target_order_is_canonical_for_every_filter_insertion_permutation() {
        fn permute(routes: &mut [Route], index: usize, sessions: &[Session]) {
            if index == routes.len() {
                let selected = select(
                    routes,
                    sessions,
                    SessionId::new(1),
                    b"a/x",
                    Qos::ExactlyOnce,
                    16,
                )
                .unwrap();
                let actual: Vec<_> = selected.iter().map(|s| (s.session.get(), s.qos)).collect();
                assert_eq!(
                    actual,
                    [
                        (1, Qos::AtMostOnce),
                        (2, Qos::AtLeastOnce),
                        (3, Qos::AtLeastOnce)
                    ],
                    "routing_depended_on_filter_insertion_order"
                );
                return;
            }
            for other in index..routes.len() {
                routes.swap(index, other);
                permute(routes, index + 1, sessions);
                routes.swap(index, other);
            }
        }
        let sessions = [session(1), session(2), session(3)];
        let mut routes = [
            route(b"a/+", 3, Qos::AtLeastOnce, false),
            route(b"a/#", 1, Qos::AtLeastOnce, true),
            route(b"a/x", 1, Qos::AtMostOnce, false),
            route(b"a/+", 2, Qos::AtMostOnce, false),
            route(b"a/#", 2, Qos::AtLeastOnce, false),
        ];
        permute(&mut routes, 0, &sessions);
    }
    #[test]
    fn a_revision_conflict_is_not_a_second_destination_or_a_different_incarnation() {
        let sessions = [session(1)];
        let mut stale = route(b"a/+", 1, Qos::AtLeastOnce, false);
        stale.target_revision = Revision::new(2);
        for routes in [
            [route(b"a/#", 1, Qos::AtMostOnce, false), stale.clone()],
            [stale.clone(), route(b"a/#", 1, Qos::AtMostOnce, false)],
        ] {
            assert!(
                matches!(
                    select(
                        &routes,
                        &sessions,
                        SessionId::new(2),
                        b"a/x",
                        Qos::ExactlyOnce,
                        16
                    ),
                    Err(Refusal::Revision)
                ),
                "routing_revision_conflict_ignored"
            );
        }
    }

    const TURN: TurnGrant = TurnGrant {
        work_limit: WorkUnits::new(100_000),
    };
    struct Taking(Vec<OwnedEffect>);
    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
            self.0.push(effect);
            SubmitResult::Accepted
        }
    }
    fn drain(owner: &mut Owner) -> Vec<(EffectId, Effect)> {
        let mut taking = Taking(Vec::new());
        owner.dispatch(&mut taking);
        taking.0.into_iter().map(OwnedEffect::into_parts).collect()
    }
    fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
        let outcome = owner.step(input, TURN);
        assert!(
            matches!(outcome, StepOutcome::Applied { .. }),
            "{outcome:?}"
        );
        drain(owner)
    }
    fn bounded_owner(routes: &[Route], session_count: u32) -> Owner {
        use crate::contract::opening::{
            EpochOpened, EpochOpening, OpeningOutcome, OpeningReply, StartupNonce,
        };
        let bound = |n| NonZeroU32::new(n).unwrap();
        let incarnation = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
        let command = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening
            .observe(OpeningReply {
                command,
                outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
            })
            .unwrap();
        let mut authority =
            OwnerAuthority::from_grant(opening.hand_over().unwrap(), bound(1)).unwrap();
        let outbox = authority
            .create_outbox(
                LogicalOwnerId::new(0),
                NonZeroUsize::new(128).unwrap(),
                bound(128),
            )
            .unwrap();
        let mut owner = Owner::new(
            OwnerConfig {
                max_sessions: bound(128),
                max_routes: bound(128),
                max_exchanges: bound(128),
                session_qos2_exchanges: NonZeroU32::MAX,
                account: AccountId::new(1),
                payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
                resolution_delay: LogicalDuration::new(10000),
                retained_operations: bound(128),
                remembered_connections: bound(8),
            },
            outbox,
        )
        .unwrap();
        let mut facts = vec![
            RecoveredFact::Epoch { incarnation, epoch },
            RecoveredFact::Limits {
                max_targets_per_admission: 16,
                max_topic_bytes: 128,
                max_payload_bytes: PayloadBytes::new(32768),
            },
            RecoveredFact::RouteTable {
                revision: Revision::new(0),
            },
        ];
        let mut routes = routes.to_vec();
        routes.sort_by(|a, b| a.topic.as_slice().cmp(b.topic.as_slice()));
        for r in routes {
            facts.push(RecoveredFact::Route {
                topic: r.topic,
                target: r.target,
                target_revision: r.target_revision,
                max_qos: r.max_qos,
                options: r.options,
            });
        }
        for id in 1..=session_count {
            let s = session(u64::from(id));
            facts.push(RecoveredFact::Session {
                session: s.id,
                client_id: s.client_id,
                revision: s.revision,
                expiry: s.expiry,
                opened_by: None,
                detached: None,
            });
        }
        assert!(
            apply(
                &mut owner,
                Input::Recovery {
                    facts: BoundedVec::from_slice(&facts, facts.len()).unwrap(),
                    last: true
                }
            )
            .is_empty()
        );
        owner
    }
    fn connect_source(owner: &mut Owner, id: u64, generation: u64) {
        apply(
            owner,
            Input::Packet {
                connection: ConnectionGeneration::new(generation),
                ingress_order: 0,
                packet: InboundPacket::ConnectResume {
                    client_id: bytes(alloc::string::ToString::to_string(&id).as_bytes()),
                },
            },
        );
        apply(
            owner,
            Input::ConnectionSettings {
                connection: ConnectionGeneration::new(generation),
                receive_maximum: 8,
                maximum_packet_size: 65536,
            },
        );
    }
    fn publish(qos: Qos, generation: u64, order: u64) -> Input {
        Input::Packet {
            connection: ConnectionGeneration::new(generation),
            ingress_order: order,
            packet: InboundPacket::Publish {
                expiry: None,
                properties: None,
                qos,
                topic: bytes(b"a/x"),
                payload: bytes(b"payload"),
                packet_id: (qos != Qos::AtMostOnce).then(|| InboundPacketId::new(1).unwrap()),
                dup: false,
                retain: false,
            },
        }
    }
    #[test]
    fn sixteen_destinations_fit_even_when_more_filters_match() {
        let mut routes: Vec<_> = (1..=16)
            .map(|id| route(b"a/#", id, Qos::AtLeastOnce, false))
            .collect();
        routes.push(route(b"a/+", 1, Qos::AtMostOnce, false));
        routes.push(route(b"a/x", 1, Qos::AtLeastOnce, false));
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
            let mut owner = bounded_owner(&routes, 17);
            connect_source(&mut owner, 17, 1);
            let effects = apply(&mut owner, publish(qos, 1, 1));
            if qos == Qos::AtMostOnce {
                assert_eq!(
                    owner.pubsub.volatile.len(),
                    16,
                    "routing_budget_counted_filters_instead_of_destinations"
                );
                assert!(
                    effects
                        .iter()
                        .any(|(_, e)| matches!(e, Effect::Continue { .. }))
                );
                assert!(!effects.iter().any(|(_, e)| matches!(
                    e,
                    Effect::Qos0Dropped { .. } | Effect::Persist { .. }
                )));
            } else {
                let admission = effects
                    .iter()
                    .find_map(|(_, e)| match e {
                        Effect::Persist { command } => match command.recipe.get() {
                            Recipe::AdmitPublication(admission) => Some(admission),
                            _ => None,
                        },
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(
                    admission.targets.len(),
                    16,
                    "routing_budget_counted_filters_instead_of_destinations"
                );
                assert_eq!(admission.costs.references, References::new(16));
                assert_eq!(
                    admission
                        .targets
                        .as_slice()
                        .iter()
                        .map(|t| t.session.get())
                        .collect::<Vec<_>>(),
                    (1..=16).collect::<Vec<_>>()
                );
                for (index, target) in admission.targets.as_slice().iter().enumerate() {
                    assert_eq!(target.delivery.index, index as u32);
                    assert_eq!(target.effective_qos, Qos::AtLeastOnce);
                }
            }
        }
    }
    #[test]
    fn seventeenth_destination_refuses_the_complete_durable_and_volatile_fanout() {
        let routes: Vec<_> = (1..=17)
            .map(|id| route(b"a/#", id, Qos::AtLeastOnce, false))
            .collect();
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
            let mut owner = bounded_owner(&routes, 18);
            connect_source(&mut owner, 18, 1);
            let effects = apply(&mut owner, publish(qos, 1, 1));
            if qos == Qos::AtMostOnce {
                assert!(
                    matches!(
                        effects.as_slice(),
                        [(
                            _,
                            Effect::Qos0Dropped {
                                target: None,
                                reason: crate::contract::pubsub::Qos0DropReason::CapacityExceeded,
                                ..
                            }
                        )]
                    ),
                    "wildcard_fanout_truncated"
                );
            } else {
                assert!(
                    matches!(
                        effects.as_slice(),
                        [(
                            _,
                            Effect::CloseConnection {
                                reason: CloseReason::CapacityExceeded,
                                ..
                            }
                        )]
                    ),
                    "wildcard_fanout_truncated"
                );
            }
            assert!(
                owner.pubsub.volatile.is_empty(),
                "wildcard_fanout_truncated"
            );
            assert!(owner.slots.is_empty());
            assert_eq!(owner.leases.held(), 0);
        }
    }
    fn subscription(
        generation: u64,
        packet_id: u16,
        filters: &[(Qos, SubscriptionOptions)],
    ) -> Input {
        let filters: Vec<_> = filters
            .iter()
            .map(
                |(qos, options)| crate::contract::pubsub::SubscriptionFilter {
                    filter: bytes(b"a/#"),
                    qos: *qos,
                    options: *options,
                },
            )
            .collect();
        Input::Packet {
            connection: ConnectionGeneration::new(generation),
            ingress_order: 1,
            packet: InboundPacket::Subscribe {
                denied: 0,
                packet_id: InboundPacketId::new(packet_id).unwrap(),
                filters: crate::contract::pubsub::FilterList::copy_from(&filters).unwrap(),
            },
        }
    }
    fn answer_control(owner: &mut Owner, reply: StoreOutcome) -> Vec<(EffectId, Effect)> {
        let (effect, operation) = {
            let flight = &owner.pubsub.control.as_ref().unwrap().in_flight;
            (flight.answers.persist_id(), flight.operation)
        };
        apply(
            owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(reply),
            },
        )
    }
    #[test]
    fn duplicate_filter_last_options_win_only_after_known_commit_and_before_suback() {
        let initial = route(b"a/#", 1, Qos::AtLeastOnce, false);
        let mut owner = bounded_owner(core::slice::from_ref(&initial), 2);
        connect_source(&mut owner, 1, 1);
        let options = SubscriptionOptions::from_bits(0x2c).unwrap();
        let effects = apply(
            &mut owner,
            subscription(
                1,
                2,
                &[
                    (Qos::ExactlyOnce, SubscriptionOptions::DEFAULT),
                    (Qos::AtMostOnce, options),
                ],
            ),
        );
        let command = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::Persist { command } => Some(command),
                _ => None,
            })
            .unwrap();
        let Recipe::UpdateSubscriptions(recipe) = command.recipe.get() else {
            panic!("subscription recipe")
        };
        assert_eq!(recipe.changes.len(), 2);
        assert_eq!(recipe.changes.as_slice()[0].granted, Some(Qos::ExactlyOnce));
        assert_eq!(recipe.changes.as_slice()[1].granted, Some(Qos::AtMostOnce));
        assert_eq!(recipe.changes.as_slice()[1].options, options);
        assert_eq!(owner.routes.as_slice(), core::slice::from_ref(&initial));
        assert!(answer_control(&mut owner, StoreOutcome::PendingOrUnknown).is_empty());
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
            assert!(
                matches!(
                    owner.step(publish(qos, 1, 2), TURN),
                    StepOutcome::NeedCapacity {
                        need: CapacityNeed {
                            resource: Resource::ResultSlots,
                            ..
                        },
                        ..
                    }
                ),
                "publication_crossed_unresolved_subscription_cut"
            );
        }
        assert_eq!(owner.routes.as_slice(), [initial]);
        let effects = answer_control(
            &mut owner,
            StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                session: SessionId::new(1),
                route_revision: Revision::new(1),
            }),
        );
        assert!(
            matches!(effects.as_slice(),[(_,Effect::EmitFrame {frame:OutboundFrame::Suback {results,..},..})] if results.as_slice()==Some(&[2,0][..]))
        );
        assert_eq!(
            owner.routes.len(),
            1,
            "duplicate_filter_installed_two_routes"
        );
        assert_eq!(
            owner.routes.as_slice()[0].options,
            options,
            "last_filter_options_lost"
        );
        assert_eq!(owner.routes.as_slice()[0].max_qos, Qos::AtMostOnce);
        // The committed projection is in force when the positive SUBACK is
        // available. No Local now excludes this source's sole contribution.
        let effects = apply(&mut owner, publish(Qos::AtMostOnce, 1, 3));
        assert!(
            matches!(
                effects.as_slice(),
                [(
                    _,
                    Effect::Qos0Dropped {
                        reason: crate::contract::pubsub::Qos0DropReason::NoSubscribers,
                        ..
                    }
                )]
            ),
            "suback_preceded_routing_projection"
        );
        let remove = Input::Packet {
            connection: ConnectionGeneration::new(1),
            ingress_order: 4,
            packet: InboundPacket::Unsubscribe {
                packet_id: InboundPacketId::new(3).unwrap(),
                filters: crate::contract::pubsub::FilterList::copy_from(&[bytes(b"a/+")]).unwrap(),
            },
        };
        apply(&mut owner, remove);
        let effects = answer_control(
            &mut owner,
            StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                session: SessionId::new(1),
                route_revision: Revision::new(2),
            }),
        );
        assert!(
            matches!(effects.as_slice(),[(_,Effect::EmitFrame {frame:OutboundFrame::Unsuback {results,..},..})] if results.as_slice()==Some(&[0x11][..]))
        );
        assert_eq!(
            owner.routes.len(),
            1,
            "unsubscribe_removed_a_matching_instead_of_literal_filter"
        );
        apply(
            &mut owner,
            Input::Packet {
                connection: ConnectionGeneration::new(1),
                ingress_order: 5,
                packet: InboundPacket::Unsubscribe {
                    packet_id: InboundPacketId::new(4).unwrap(),
                    filters: crate::contract::pubsub::FilterList::copy_from(&[bytes(b"a/#")])
                        .unwrap(),
                },
            },
        );
        answer_control(
            &mut owner,
            StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                session: SessionId::new(1),
                route_revision: Revision::new(3),
            }),
        );
        assert!(owner.routes.is_empty());
    }
    #[test]
    fn an_unknown_admission_keeps_its_original_destinations_across_a_route_cut() {
        let mut owner = bounded_owner(&[route(b"a/#", 1, Qos::AtLeastOnce, false)], 3);
        connect_source(&mut owner, 3, 1);
        let effects = apply(&mut owner, publish(Qos::AtLeastOnce, 1, 1));
        let command = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::Persist { command } => Some(command.clone()),
                _ => None,
            })
            .unwrap();
        let (effect, operation) = effects
            .iter()
            .find_map(|(effect, e)| match e {
                Effect::Persist { command } => Some((*effect, command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        let deadline = effects
            .iter()
            .find_map(|(_, e)| match e {
                Effect::ArmDeadline { token, .. } => Some(*token),
                _ => None,
            })
            .unwrap();
        apply(
            &mut owner,
            Input::StoreResult {
                effect,
                operation,
                reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
            },
        );
        apply(
            &mut owner,
            subscription(1, 2, &[(Qos::AtLeastOnce, SubscriptionOptions::DEFAULT)]),
        );
        answer_control(
            &mut owner,
            StoreOutcome::Committed(CommitFacts::SubscriptionsChanged {
                session: SessionId::new(3),
                route_revision: Revision::new(1),
            }),
        );
        assert_eq!(owner.routes.len(), 2);
        let resolved = apply(
            &mut owner,
            Input::Deadline {
                token: deadline,
                now: crate::contract::ids::LogicalTime::new(10000),
            },
        );
        assert!(resolved.iter().any(
            |(_, e)| matches!(e,Effect::ResolveAndFence {operation:actual} if *actual==operation)
        ));
        assert!(
            !resolved
                .iter()
                .any(|(_, e)| matches!(e, Effect::Persist { .. })),
            "unknown_admission_rerouted_under_a_new_operation"
        );
        let Recipe::AdmitPublication(admission) = command.recipe.get() else {
            panic!("admission recipe")
        };
        assert_eq!(
            admission.targets.len(),
            1,
            "unknown_admission_recomputed_targets"
        );
        assert_eq!(admission.targets.as_slice()[0].session, SessionId::new(1));
        assert_eq!(admission.route_revision, Revision::new(0));
        assert_eq!(
            select(
                owner.routes.as_slice(),
                owner.sessions.as_slice(),
                SessionId::new(3),
                b"a/x",
                Qos::AtLeastOnce,
                16
            )
            .unwrap()
            .len(),
            2
        );
    }
    #[test]
    fn no_local_eliminates_a_destination_before_the_sixteen_target_limit() {
        let sessions: Vec<_> = (1..=17).map(session).collect();
        let mut routes: Vec<_> = (1..=16)
            .map(|id| route(b"a/#", id, Qos::AtLeastOnce, false))
            .collect();
        routes.push(route(b"a/+", 17, Qos::AtLeastOnce, true));
        for _ in 0..routes.len() {
            let selected = select(
                &routes,
                &sessions,
                SessionId::new(17),
                b"a/x",
                Qos::AtLeastOnce,
                16,
            )
            .unwrap();
            assert_eq!(
                selected.len(),
                16,
                "routing_capacity_preceded_no_local_filtering"
            );
            assert!(selected.iter().all(|s| s.session != SessionId::new(17)));
            routes.rotate_left(1);
        }
    }
    #[test]
    fn live_rap_is_or_of_eligible_contributions_only() {
        let sessions = [session(1)];
        let mut excluded = route(b"a/#", 1, Qos::AtLeastOnce, true);
        excluded.options = SubscriptionOptions::from_bits(12).unwrap();
        let eligible = route(b"a/+", 1, Qos::AtMostOnce, false);
        for routes in [
            [excluded.clone(), eligible.clone()],
            [eligible.clone(), excluded],
        ] {
            let selected = select(
                &routes,
                &sessions,
                SessionId::new(1),
                b"a/x",
                Qos::ExactlyOnce,
                16,
            )
            .unwrap();
            assert!(
                !selected.iter().next().unwrap().rap,
                "excluded_no_local_contribution_preserved_retain_flag"
            );
        }
        let mut rap = eligible.clone();
        rap.options = SubscriptionOptions::from_bits(8).unwrap();
        let ordinary = route(b"a/#", 1, Qos::AtLeastOnce, false);
        for routes in [[rap.clone(), ordinary.clone()], [ordinary, rap]] {
            let selected = select(
                &routes,
                &sessions,
                SessionId::new(2),
                b"a/x",
                Qos::ExactlyOnce,
                16,
            )
            .unwrap();
            let selected = selected.iter().next().unwrap();
            assert!(selected.rap, "live_overlap_lost_retain_as_published");
            assert_eq!(selected.qos, Qos::AtLeastOnce);
        }
    }

    #[test]
    fn route_index_matches_linear_reference_across_wildcards_options_and_revisions() {
        let filters: &[&[u8]] = &[
            b"a",
            b"a/b",
            b"a/b/c",
            b"a/#",
            b"a/+",
            b"a/+/c",
            b"+/+",
            b"a/+/",
            b"a/+/#",
            b"#",
            b"+",
            b"/#",
            b"+/x",
            b"$SYS/#",
            b"$SYS/broker/+",
            b"$SYS/broker/load",
            "温度/+/é".as_bytes(),
            "é/#".as_bytes(),
        ];
        let topics: &[&[u8]] = &[
            b"a",
            b"a/",
            b"a/b",
            b"a/b/c",
            b"a//",
            b"a/x",
            b"a/x/c",
            b"a/x/y",
            b"/",
            b"/a",
            b"x/y",
            b"$SYS",
            b"$SYS/",
            b"$SYS/x",
            b"$SYS/broker/load",
            b"$SYS/broker/other",
            "温度//é".as_bytes(),
            "e/x".as_bytes(),
            b"A",
        ];
        let mut sessions = Vec::new();
        let mut session_by_id = FlatMap::new();
        for id in 1..=20u64 {
            session_by_id.insert(SessionId::new(id), sessions.len());
            sessions.push(session(id));
        }
        let mut routes = Vec::new();
        for (i, filter) in filters.iter().enumerate() {
            let sid = (i as u64 % 16) + 1;
            let qos = match i % 3 {
                0 => Qos::AtMostOnce,
                1 => Qos::AtLeastOnce,
                _ => Qos::ExactlyOnce,
            };
            let bits = match i % 4 {
                0 => 0,
                1 => 4,
                2 => 8,
                _ => 12,
            };
            let mut r = route(filter, sid, qos, false);
            r.options = SubscriptionOptions::from_bits(bits).unwrap();
            routes.push(r);
            // Add overlapping route on same session with different filter/options
            let mut overlap = route(b"a/#", sid, Qos::AtLeastOnce, i % 2 == 0);
            overlap.options =
                SubscriptionOptions::from_bits(if i % 3 == 0 { 8 } else { 0 }).unwrap();
            routes.push(overlap);
        }
        let mut index = RouteIndex::default();
        index.rebuild(&routes);
        for &topic in topics {
            for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
                for source in [SessionId::new(1), SessionId::new(2), SessionId::new(19)] {
                    for limit in [1u32, 4, 8, 16, 64] {
                        let expected = select(&routes, &sessions, source, topic, qos, limit)
                            .map(|sel| sel.iter().copied().collect::<Vec<_>>());
                        let actual = index
                            .select(
                                &routes,
                                &sessions,
                                &session_by_id,
                                source,
                                topic,
                                qos,
                                limit,
                            )
                            .map(|sel| sel.iter().copied().collect::<Vec<_>>());
                        assert_eq!(
                            actual,
                            expected,
                            "route_index_mismatch topic={:?} qos={qos:?} source={source:?} limit={limit}",
                            core::str::from_utf8(topic)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn wide_fanout_1024_targets_selects_in_canonical_order() {
        let count = MAX_TARGETS;
        let mut sessions = Vec::with_capacity(count);
        let mut session_by_id = FlatMap::new();
        let mut routes = Vec::with_capacity(count);
        for i in (1..=count as u64).rev() {
            session_by_id.insert(SessionId::new(i), sessions.len());
            sessions.push(session(i));
            let filter: &[u8] = if i % 2 == 0 {
                b"fanout/topic"
            } else {
                b"fanout/+"
            };
            let mut r = route(filter, i, Qos::AtLeastOnce, false);
            if i % 3 == 0 {
                r.options = SubscriptionOptions::from_bits(8).unwrap();
            }
            routes.push(r);
        }
        let mut index = RouteIndex::default();
        index.rebuild(&routes);
        let selected = index
            .select(
                &routes,
                &sessions,
                &session_by_id,
                SessionId::new(9999),
                b"fanout/topic",
                Qos::AtLeastOnce,
                MAX_TARGETS as u32,
            )
            .unwrap();
        assert_eq!(selected.len(), MAX_TARGETS);
        let ids: Vec<u64> = selected.iter().map(|s| s.session.get()).collect();
        let expected: Vec<u64> = (1..=count as u64).collect();
        assert_eq!(ids, expected);
        // Exceeding limit by 1 refuses cleanly without truncation.
        assert_eq!(
            index
                .select(
                    &routes,
                    &sessions,
                    &session_by_id,
                    SessionId::new(9999),
                    b"fanout/topic",
                    Qos::AtLeastOnce,
                    (MAX_TARGETS - 1) as u32,
                )
                .err(),
            Some(Refusal::Capacity)
        );
    }

    #[test]
    fn routing_work_bound_accounts_for_many_wildcard_filters_matching_one_subscriber() {
        // Generate 64 distinct 6-level wildcard filters matching `a/b/c/d/e/f`
        // (every non-exact combination of exact level vs `+` plus `#` prefixes),
        // all belonging to a SINGLE subscriber (session 2) while `max_targets = 1`.
        let levels = ["a", "b", "c", "d", "e", "f"];
        let mut wildcard_routes = Vec::new();
        for mask in 1u32..64 {
            let filter: alloc::string::String = levels
                .iter()
                .enumerate()
                .map(|(i, lvl)| if (mask & (1 << i)) != 0 { "+" } else { *lvl })
                .collect::<Vec<_>>()
                .join("/");
            wildcard_routes.push(route(
                filter.as_bytes(),
                2,
                if mask == 63 {
                    Qos::AtLeastOnce
                } else {
                    Qos::AtMostOnce
                },
                false,
            ));
        }
        wildcard_routes.push(route(b"#", 2, Qos::AtMostOnce, false));
        assert_eq!(wildcard_routes.len(), 64);

        let exact_only = [route(b"a/b/c/d/e/f", 2, Qos::AtLeastOnce, false)];

        let mut owner_exact = bounded_owner(&exact_only, 2);
        let mut owner_wild = bounded_owner(&wildcard_routes, 2);
        // Set max_targets = 1 on both owners so destination count alone is 1.
        owner_exact.limits.as_mut().unwrap().max_targets = 1;
        owner_wild.limits.as_mut().unwrap().max_targets = 1;

        let make_publish = |ingress_order: u64| Input::Packet {
            connection: ConnectionGeneration::new(1),
            ingress_order,
            packet: InboundPacket::Publish {
                topic: bytes(b"a/b/c/d/e/f"),
                qos: Qos::AtLeastOnce,
                retain: false,
                dup: false,
                packet_id: Some(InboundPacketId::new(1).unwrap()),
                expiry: None,
                properties: None,
                payload: bytes(b"ok"),
            },
        };

        // Connect session 1 (publisher) on both owners.
        for owner in [&mut owner_exact, &mut owner_wild] {
            let _ = apply(
                owner,
                Input::Packet {
                    connection: ConnectionGeneration::new(1),
                    ingress_order: 1,
                    packet: InboundPacket::ConnectResume {
                        client_id: bytes(b"1"),
                    },
                },
            );
        }

        let publish_probe = make_publish(2);
        let exact_bound = owner_exact.routing_work_bound(&publish_probe);
        let wild_bound = owner_wild.routing_work_bound(&publish_probe);
        assert!(
            wild_bound >= exact_bound + 64 * 2,
            "routing_work_bound must charge for all 64 matching wildcard filters on 1 subscriber (exact={exact_bound}, wild={wild_bound})"
        );

        // Verify select deduplicates all 64 matching filters onto 1 target.
        let selected = owner_wild
            .route_index
            .select(
                owner_wild.routes.as_slice(),
                owner_wild.sessions.as_slice(),
                &owner_wild.session_by_id,
                SessionId::new(1),
                b"a/b/c/d/e/f",
                Qos::AtLeastOnce,
                1,
            )
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected.iter().next().unwrap().session, SessionId::new(2));
        assert_eq!(selected.iter().next().unwrap().qos, Qos::AtLeastOnce);

        // Step with work_limit strictly below the wildcard bound MUST return
        // NeedCapacity(Work) without mutating state; stepping with sufficient grant succeeds.
        let too_small = TurnGrant {
            work_limit: WorkUnits::new(wild_bound),
        };
        assert!(
            matches!(
                owner_wild.step(publish_probe, too_small),
                StepOutcome::NeedCapacity {
                    need: crate::contract::capacity::CapacityNeed {
                        resource: crate::contract::capacity::Resource::Work,
                        ..
                    },
                    ..
                }
            ),
            "insufficient work budget must block before scanning 64 overlapping wildcard filters"
        );
        let sufficient = TurnGrant {
            work_limit: WorkUnits::new(wild_bound + 1_000),
        };
        assert!(matches!(
            owner_wild.step(make_publish(2), sufficient),
            StepOutcome::Applied { .. }
        ));
    }
}
