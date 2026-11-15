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

use mqttiumd_contract::ids::{Revision, SessionId, StoreIncarnation};
use mqttiumd_contract::packet::Qos;
use serde_json::Value;

use super::durable::{AdminRefused, DurableStore, Limits};

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
}

/// Reads a fixture document into a plan. Skeleton.
pub fn plan(fixture: &Value) -> Result<FixturePlan, FixtureError> {
    let _ = fixture;
    Err(FixtureError::Malformed("not written yet"))
}

/// Sends the commands of a plan to a store created with its limits.
/// Skeleton.
pub fn install<S: DurableStore>(
    store: &mut S,
    plan: &FixturePlan,
) -> Result<Installed, FixtureError> {
    let _ = (store, plan);
    Err(FixtureError::Malformed("not written yet"))
}
