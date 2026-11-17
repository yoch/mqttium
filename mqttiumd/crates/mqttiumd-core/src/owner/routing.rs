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
    let limit = usize::try_from(maximum)
        .unwrap_or(usize::MAX)
        .min(MAX_TARGETS);
    let _ = (sessions, source);
    for route in routes
        .iter()
        .filter(|route| route.topic.as_slice() == topic)
    {
        if selected.count == limit {
            return Err(Refusal::Capacity);
        }
        selected.items[selected.count] = Some(Selected {
            session: route.target,
            revision: route.target_revision,
            qos: route.max_qos.min(qos),
        });
        selected.count += 1;
    }
    Ok(selected)
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
