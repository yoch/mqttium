//! A bounded per-destination projection, shared by durable and volatile admission.
use super::*;

pub(super) const MAX_TARGETS: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Selected {
    pub(super) session: SessionId,
    pub(super) revision: Revision,
    pub(super) qos: Qos,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Refusal {
    Capacity,
    Revision,
}
#[derive(Debug)]
pub(super) struct Selection {
    items: [Option<Selected>; MAX_TARGETS],
    count: usize,
}
impl Selection {
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &Selected> {
        self.items[..self.count].iter().flatten()
    }
}

/// The fixed workspace holds one row per configured output destination, never
/// one row per matching filter. No effect, lease or publication identity has
/// been reserved when this function refuses the complete selection.
pub(super) fn select(
    routes: &[Route],
    sessions: &[Session],
    source: SessionId,
    topic: &[u8],
    qos: Qos,
    maximum: u32,
) -> Result<Selection, Refusal> {
    let mut selected = Selection {
        items: [None; MAX_TARGETS],
        count: 0,
    };
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
        if !sessions
            .iter()
            .any(|s| s.id == route.target && s.revision == route.target_revision)
        {
            return Err(Refusal::Revision);
        }
        // No Local removes this contribution before QoS aggregation; another
        // eligible filter on the same session can still contribute.
        if route.options.no_local() && route.target == source {
            continue;
        }
        let effective = route.max_qos.min(qos).min(Qos::AtLeastOnce);
        if let Some(held) = selected.items[..selected.count]
            .iter_mut()
            .flatten()
            .find(|held| held.session == route.target)
        {
            if held.revision != route.target_revision {
                return Err(Refusal::Revision);
            }
            held.qos = held.qos.max(effective);
        } else {
            if selected.count == limit {
                return Err(Refusal::Capacity);
            }
            selected.items[selected.count] = Some(Selected {
                session: route.target,
                revision: route.target_revision,
                qos: effective,
            });
            selected.count += 1;
        }
    }
    selected.items[..selected.count].sort_unstable_by_key(|item| item.map(|target| target.session));
    Ok(selected)
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
            && self.routes.as_slice().iter().all(|route| {
                !route.topic.as_slice().contains(&b'+')
                    && !route.topic.as_slice().contains(&b'#')
                    && route.options == crate::contract::pubsub::SubscriptionOptions::DEFAULT
            })
        {
            return 0;
        }
        // Per route: matcher invocation, at most one bounded session scan and
        // one 16-entry aggregate scan. The constant square also bounds sorting
        // the fixed output table. Topic byte lengths have separate read limits.
        let targets = self
            .limits
            .map_or(0, |limits| {
                u64::from(limits.max_targets).min(MAX_TARGETS as u64)
            })
            .min(capacity_of(self.routes.capacity()));
        capacity_of(self.routes.capacity())
            .saturating_mul(capacity_of(self.sessions.capacity()).saturating_add(targets + 1))
            .saturating_add(targets.saturating_mul(targets))
    }
}

#[cfg(test)]
mod tests {
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
                packet_id: InboundPacketId::new(packet_id).unwrap(),
                filters: crate::contract::pubsub::FilterList::copy_from(&filters).unwrap(),
            },
        }
    }
    fn answer_control(owner: &mut Owner, reply: StoreOutcome) -> Vec<(EffectId, Effect)> {
        let flight = owner.pubsub.control.as_ref().unwrap().in_flight;
        apply(
            owner,
            Input::StoreResult {
                effect: flight.answers.persist,
                operation: flight.operation,
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
        assert_eq!(recipe.changes.as_slice()[0].granted, Some(Qos::AtLeastOnce));
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
            matches!(effects.as_slice(),[(_,Effect::EmitFrame {frame:OutboundFrame::Suback {results,..},..})] if results.as_slice()==Some(&[1,0][..]))
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
}
