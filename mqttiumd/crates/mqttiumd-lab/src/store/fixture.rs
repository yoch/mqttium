//! Materialises a fixture through store commands (DEM-002).
//!
//! A fixture is declarative. `plan` reads it into the limits the data
//! set is created with and the list of commands to send; `install`
//! sends them to the writer, one `AdminCommand` at a time. Nothing here
//! writes a row, and nothing opens an epoch: opening belongs to start-up
//! (ADR 0009).
//!
//! The document is expected to have passed the schema and the semantic
//! validation of the bench. This reader still refuses what it cannot
//! use, and never trusts a size it did not check.

use std::collections::BTreeMap;

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::decimal::parse_u64;
use mqttiumd_contract::ids::{Revision, SessionId, StoreIncarnation};
use mqttiumd_contract::packet::Qos;
use serde_json::Value;

use super::durable::{AdminCommand, AdminDone, AdminRefused, DurableStore, Limits, ReadError};

/// One session of a fixture: the name scenarios use, and its ClientID.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FixtureSession {
    pub client: String,
    pub client_id: Vec<u8>,
}

/// One exact-topic route of a fixture, towards clients by name.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FixtureRoute {
    pub topic: Vec<u8>,
    pub targets: Vec<(String, Qos)>,
}

/// What a fixture asks for, before any command is sent.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FixturePlan {
    pub incarnation: StoreIncarnation,
    /// The limits to create the data set with.
    pub limits: Limits,
    /// Bounds of the store adapter.
    pub operations_in_flight: usize,
    pub result_slots: usize,
    pub sessions: Vec<FixtureSession>,
    pub routes: Vec<FixtureRoute>,
}

/// What the commands of a fixture established.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Installed {
    /// The session the store created for each client name.
    pub sessions: BTreeMap<String, SessionId>,
    pub route_revision: Revision,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FixtureError {
    /// A member is missing or does not have the shape the schema gives.
    Malformed(&'static str),
    /// A route names a client the fixture does not declare.
    UnknownClient(String),
    /// The store refused a command.
    Refused(AdminRefused),
    /// A durable read failed; this says nothing about absence or rejection.
    Read(ReadError),
}

/// The most sessions, routes and targets a fixture may declare: the
/// bounds of the fixture schema.
const MAX_ITEMS: usize = 64;

fn member<'a>(value: &'a Value, name: &'static str) -> Result<&'a Value, FixtureError> {
    value.get(name).ok_or(FixtureError::Malformed(name))
}

fn text<'a>(value: &'a Value, name: &'static str) -> Result<&'a str, FixtureError> {
    member(value, name)?
        .as_str()
        .ok_or(FixtureError::Malformed(name))
}

/// A 64-bit value: a canonical decimal string, never a JSON number.
fn decimal(value: &Value, name: &'static str) -> Result<u64, FixtureError> {
    parse_u64(text(value, name)?).map_err(|_| FixtureError::Malformed(name))
}

fn size(value: &Value, name: &'static str) -> Result<usize, FixtureError> {
    usize::try_from(decimal(value, name)?).map_err(|_| FixtureError::Malformed(name))
}

fn items<'a>(value: &'a Value, name: &'static str) -> Result<&'a [Value], FixtureError> {
    let items = member(value, name)?
        .as_array()
        .ok_or(FixtureError::Malformed(name))?;
    if items.len() > MAX_ITEMS {
        return Err(FixtureError::Malformed(name));
    }
    Ok(items)
}

/// Reads a fixture document into a plan. Nothing is sent to a store.
pub fn plan(fixture: &Value) -> Result<FixturePlan, FixtureError> {
    let incarnation = StoreIncarnation::new(decimal(member(fixture, "store")?, "incarnation")?);
    let capacities = member(fixture, "capacities")?;

    let mut sessions = Vec::new();
    for session in items(fixture, "sessions")? {
        sessions.push(FixtureSession {
            client: text(session, "client")?.to_owned(),
            client_id: text(session, "client_id")?.as_bytes().to_vec(),
        });
    }
    let mut routes = Vec::new();
    for route in items(fixture, "routes")? {
        let mut targets = Vec::new();
        for target in items(route, "targets")? {
            let qos = member(target, "qos")?
                .as_u64()
                .and_then(|raw| u8::try_from(raw).ok())
                .and_then(Qos::from_wire)
                .ok_or(FixtureError::Malformed("qos"))?;
            targets.push((text(target, "client")?.to_owned(), qos));
        }
        routes.push(FixtureRoute {
            topic: text(route, "topic")?.as_bytes().to_vec(),
            targets,
        });
    }

    let max_targets_per_admission = size(capacities, "max_targets_per_admission")?;
    Ok(FixturePlan {
        incarnation,
        limits: Limits {
            payload_bytes: PayloadBytes::new(decimal(capacities, "payload_bytes")?),
            references: References::new(decimal(capacities, "references")?),
            exchange_slots: ExchangeSlots::new(decimal(capacities, "exchange_slots")?),
            receipt_slots: ReceiptSlots::new(decimal(capacities, "receipt_slots")?),
            max_targets_per_admission,
            max_topic_bytes: size(capacities, "max_topic_bytes")?,
            max_payload_bytes: size(capacities, "max_payload_bytes")?,
            // A fixture declares no bound for these two tables. The
            // data set holds the sessions it sets up, and a route has
            // at most the targets one admission may carry.
            max_sessions: sessions.len(),
            max_route_targets: max_targets_per_admission,
        },
        operations_in_flight: size(capacities, "store_operations_in_flight")?,
        result_slots: size(capacities, "result_slots")?,
        sessions,
        routes,
    })
}

/// Sends the commands of a plan to a store created with its limits:
/// the sessions in the order of the fixture, then each route target in
/// its order. A refused command stops the installation; the commands
/// before it stay committed, each as a whole.
pub fn install<S: DurableStore>(
    store: &mut S,
    plan: &FixturePlan,
) -> Result<Installed, FixtureError> {
    let mut sessions = BTreeMap::new();
    for session in &plan.sessions {
        let created = store
            .admin(AdminCommand::CreateSession {
                client_id: session.client_id.clone(),
            })
            .map_err(FixtureError::Refused)?;
        let AdminDone::SessionCreated { session: id, .. } = created else {
            return Err(FixtureError::Refused(AdminRefused::Inconsistent));
        };
        if sessions.insert(session.client.clone(), id).is_some() {
            return Err(FixtureError::Malformed("client"));
        }
    }
    let mut route_revision = store.snapshot().map_err(FixtureError::Read)?.route_revision;
    for route in &plan.routes {
        for (client, qos) in &route.targets {
            let session = *sessions
                .get(client)
                .ok_or_else(|| FixtureError::UnknownClient(client.clone()))?;
            let added = store
                .admin(AdminCommand::AddRoute {
                    topic: route.topic.clone(),
                    session,
                    qos: *qos,
                })
                .map_err(FixtureError::Refused)?;
            let AdminDone::RouteAdded {
                route_revision: now,
            } = added
            else {
                return Err(FixtureError::Refused(AdminRefused::Inconsistent));
            };
            route_revision = now;
        }
    }
    Ok(Installed {
        sessions,
        route_revision,
    })
}
