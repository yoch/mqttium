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
    use crate::contract::pubsub::SubscriptionOptions;
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
        .unwrap();
        assert_eq!(selected.len(), 1, "wildcard_routes_not_aggregated");
        assert_eq!(selected.iter().next().unwrap().qos, Qos::AtLeastOnce);
    }
}
