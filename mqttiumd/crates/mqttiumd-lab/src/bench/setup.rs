//! The process under test as a fixture sets it up: its settings, the data
//! set installed through store commands, and the clients. The supervisor
//! and a child process read the same fixture to the same settings.

use std::num::{NonZeroU32, NonZeroUsize};

use mqttiumd_contract::ids::SessionId;
use mqttiumd_store_api::ReadPhase;
use serde_json::Value;

use super::backend::Backend;
use super::mqtt::transport::Bounds;
use super::process::{Settings, Stop, owner_config};
use crate::store::adapter::AdapterLimits;
use crate::store::durable::MAX_CLIENT_ID_BYTES;
use crate::store::fixture;

/// The resolution delay of the owner, in logical time units. The
/// specification fixes none for S0; the corpus uses 30.
pub const RESOLUTION_DELAY: u64 = 30;

/// Ordinary campaigns request this many facts per bounded recovery page.
pub const RECOVERY_PAGE_FACTS: usize = 64;

/// The fixture as the runner needs it: clients and their sessions.
#[derive(Clone, Debug)]
pub struct Population {
    /// Client name, ClientID and the session the store created.
    pub clients: Vec<(String, Vec<u8>, SessionId)>,
}

fn capacity(fixture: &Value, name: &str) -> Result<u64, String> {
    fixture
        .pointer(&format!("/capacities/{name}"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the fixture has no capacity {name}"))
        .and_then(|text| {
            mqttiumd_contract::decimal::parse_u64(text)
                .map_err(|error| format!("{name}: {error:?}"))
        })
}

fn narrow<T: TryFrom<u64>>(value: u64, name: &str) -> Result<T, String> {
    T::try_from(value).map_err(|_| format!("{name} does not fit"))
}

/// The settings of the process a fixture fixes, the same at every
/// start-up, in the supervisor and in a child process alike.
pub fn settings_of(
    fixture_document: &Value,
    run_id: &str,
    seed: u64,
    faults: usize,
) -> Result<Settings, String> {
    let plan = fixture::plan(fixture_document).map_err(|error| format!("{error:?}"))?;
    let limits = plan.limits;
    let owner = owner_config(
        narrow(limits.max_sessions as u64, "max_sessions")?,
        narrow(limits.max_route_targets as u64, "max_route_targets")?,
        narrow(limits.exchange_slots.get(), "exchange_slots")?,
        limits.payload_bytes.get(),
        RESOLUTION_DELAY,
    )
    .map_err(|error| error.to_string())?;
    let effect_slots: usize = narrow(capacity(fixture_document, "effect_slots")?, "effect_slots")?;
    let effect_slots = NonZeroUsize::new(effect_slots).ok_or("no effect slot")?;
    let reservations = NonZeroU32::new(narrow(effect_slots.get() as u64, "reservations")?)
        .ok_or("no reservation")?;
    let leaked: &'static str = Box::leak(run_id.to_owned().into_boxed_str());
    let settings = Settings {
        run_id: leaked,
        seed,
        resolution_delay: RESOLUTION_DELAY,
        work_limit: 4096,
        recovery_page: RECOVERY_PAGE_FACTS,
        effect_slots,
        reservations,
        adapter: AdapterLimits {
            operations_in_flight: plan.operations_in_flight,
            result_slots: plan.result_slots,
            faults: faults.max(1),
            events: 4096,
        },
        owner,
        bounds: Bounds {
            max_client_id: MAX_CLIENT_ID_BYTES,
            max_topic: limits.max_topic_bytes,
            max_payload: limits.max_payload_bytes,
        },
        retained_enabled: false,
    };
    Ok(settings)
}

/// The process under test as a fixture sets it up: its settings, the
/// data set installed through store commands, and the clients.
pub struct Setup<S> {
    pub settings: Settings,
    pub store: S,
    pub population: Population,
}

impl<S: crate::store::durable::DurableStore> Setup<S> {
    /// Reads a fixture document (already validated) and installs it in a
    /// new data set of `backend`.
    pub fn from_fixture<B: Backend<Store = S>>(
        backend: &mut B,
        fixture_document: &Value,
        run_id: &str,
        seed: u64,
        faults: usize,
    ) -> Result<Self, Stop> {
        let plan =
            fixture::plan(fixture_document).map_err(|error| Stop::new(format!("{error:?}")))?;
        let mut store = backend
            .create(plan.incarnation, plan.limits)
            .map_err(|error| Stop::read(error, ReadPhase::Open, 1, None))?;
        let installed = fixture::install(&mut store, &plan).map_err(|error| match error {
            fixture::FixtureError::Read(error) => Stop::read(error, ReadPhase::Snapshot, 1, None),
            other => Stop::new(format!("{other:?}")),
        })?;
        let settings = settings_of(fixture_document, run_id, seed, faults).map_err(Stop::new)?;
        let population = Population {
            clients: plan
                .sessions
                .iter()
                .map(|session| {
                    installed
                        .sessions
                        .get(&session.client)
                        .map(|id| (session.client.clone(), session.client_id.clone(), *id))
                        .ok_or_else(|| format!("{} has no session", session.client))
                })
                .collect::<Result<_, _>>()
                .map_err(Stop::new)?,
        };
        Ok(Self {
            settings,
            store,
            population,
        })
    }
}
