//! A scenario as the runner plays it.
//!
//! The document has passed the validation of the workspace before it is
//! read here (schema and semantic pass); this reader still refuses what
//! it cannot play, and never guesses a member. Bindings such as `pub_A`
//! and `op_A` stay names of the bench: they select faults and exchanges
//! and never reach the broker (ADR 0003).

use mqttiumd_contract::decimal::parse_u64;
use serde_json::Value;

use crate::store::adapter::{FaultAction, FaultPoint, WorkKind};

/// One fault the scenario declares.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Fault {
    pub id: String,
    pub point: FaultPoint,
    pub action: FaultAction,
    pub command_kind: Option<WorkKind>,
    pub publication_binding: Option<String>,
    /// `Persist` or `ResolveAndFence`, on a point that carries a result.
    pub result_of: Option<String>,
    pub occurrence: u64,
    pub expected_hits: u64,
}

/// What a `DriveUntil` step waits for.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Event {
    FaultHit {
        fault: String,
    },
    PeerPacketObserved {
        client: String,
        packet_kind: String,
        packet_id: Option<u16>,
    },
}

/// One step of a scenario.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Step {
    ClientResumeSession {
        client: String,
        expect_session_present: bool,
        max_steps: u64,
    },
    ClientPublish {
        client: String,
        topic: Vec<u8>,
        qos: u8,
        retain: bool,
        packet_id: Option<u16>,
        payload: Vec<u8>,
        bind_exchange_as: Option<String>,
    },
    ClientResumeExchange {
        client: String,
        exchange: String,
    },
    ClientSendPubrel {
        client: String,
        packet_id: u16,
    },
    DriveUntil {
        event: Event,
        max_steps: u64,
        /// While the event has not come and nothing else is eligible, fire
        /// the armed deadline due first (decision 0018).
        fire_deadlines: bool,
    },
    FireDeadline {
        max_steps: u64,
    },
    ReleaseHeldCommand {
        fault: String,
    },
    DeliverHeldResult {
        fault: String,
        max_steps: u64,
    },
    LeaveBarrier {
        fault: String,
    },
    InspectDurableState {
        checkpoint: String,
    },
    CrashBroker,
    RestartBroker,
}

impl Step {
    /// The name of the step kind in the scenario format.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ClientResumeSession { .. } => "ClientResumeSession",
            Self::ClientPublish { .. } => "ClientPublish",
            Self::ClientResumeExchange { .. } => "ClientResumeExchange",
            Self::ClientSendPubrel { .. } => "ClientSendPubrel",
            Self::DriveUntil { .. } => "DriveUntil",
            Self::FireDeadline { .. } => "FireDeadline",
            Self::ReleaseHeldCommand { .. } => "ReleaseHeldCommand",
            Self::DeliverHeldResult { .. } => "DeliverHeldResult",
            Self::LeaveBarrier { .. } => "LeaveBarrier",
            Self::InspectDurableState { .. } => "InspectDurableState",
            Self::CrashBroker => "CrashBroker",
            Self::RestartBroker => "RestartBroker",
        }
    }
}

/// A scenario the runner can play.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Scenario {
    pub id: String,
    pub perimeter: String,
    pub initial_fixture: String,
    pub max_steps: u64,
    pub max_trace_bytes: u64,
    pub faults: Vec<Fault>,
    pub steps: Vec<Step>,
    pub assertions: Vec<String>,
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("no member {name}"))
}

fn decimal(value: &Value, name: &str) -> Result<u64, String> {
    parse_u64(text(value, name)?).map_err(|error| format!("{name}: {error:?}"))
}

fn small(value: &Value, name: &str) -> Result<Option<u64>, String> {
    match value.get(name) {
        None => Ok(None),
        Some(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{name} is not a small integer")),
    }
}

fn hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("odd hexadecimal".to_owned());
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).map_err(|error| error.to_string()))
        .collect()
}

fn packet_id(value: &Value, name: &str) -> Result<Option<u16>, String> {
    small(value, name)?
        .map(|raw| u16::try_from(raw).map_err(|_| format!("{name} beyond 65535")))
        .transpose()
}

fn point(name: &str) -> Result<FaultPoint, String> {
    [
        FaultPoint::CommandTransit,
        FaultPoint::FenceTransit,
        FaultPoint::CommitPrepared,
        FaultPoint::CommitLogged,
        FaultPoint::CommitApplied,
        FaultPoint::ResultReady,
        FaultPoint::ResultConsume,
    ]
    .into_iter()
    .find(|point| point.name() == name)
    .ok_or_else(|| format!("the point {name} is not one the store adapter offers"))
}

fn action(point: FaultPoint, name: &str) -> Result<FaultAction, String> {
    point
        .actions()
        .iter()
        .copied()
        .find(|action| action.name() == name)
        .ok_or_else(|| format!("{name} is not an action of {}", point.name()))
}

fn work_kind(name: &str) -> Result<WorkKind, String> {
    [
        WorkKind::AdmitPublication,
        WorkKind::ReleaseInbound,
        WorkKind::ChangeSubscriptions,
        WorkKind::ClaimDelivery,
        WorkKind::FinishDelivery,
        WorkKind::ConsumeDelivery,
        WorkKind::UpdateSubscriptions,
        WorkKind::ResolveAndFence,
        WorkKind::OpenEpoch,
    ]
    .into_iter()
    .find(|kind| kind.name() == name)
    .ok_or_else(|| format!("the command kind {name} is unknown to the store adapter"))
}

/// Members of the scenario format the bench does not play yet: a fault
/// repeated from an occurrence on (decision 0018), which the injector of
/// the store adapter cannot arm yet. A scenario that holds one is refused,
/// never played by ignoring it.
const UNPLAYED_FAULT_MEMBERS: [&str; 3] = ["from_occurrence", "expected_hits_at_least", "lag"];
const UNPLAYED_STEP_MEMBERS: [&str; 0] = [];

/// Why the bench cannot play `document`, if it cannot: the first member it
/// does not play, and where.
#[must_use]
pub fn unplayed(document: &Value) -> Option<String> {
    let list = |name: &str| {
        document
            .get(name)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    for (index, fault) in list("faults").iter().enumerate() {
        for member in UNPLAYED_FAULT_MEMBERS {
            if fault.get(member).is_some() {
                let id = fault.get("id").and_then(Value::as_str).unwrap_or("?");
                return Some(format!(
                    "fault {index} ({id}) has {member}: a repeated fault of decision 0018 is not played by the bench yet"
                ));
            }
        }
    }
    for (index, step) in list("steps").iter().enumerate() {
        for member in UNPLAYED_STEP_MEMBERS {
            if step.get(member).is_some() {
                let kind = step.get("kind").and_then(Value::as_str).unwrap_or("?");
                return Some(format!(
                    "step {index} ({kind}) has {member}: a wait that fires the deadlines itself is not played by the bench yet"
                ));
            }
        }
    }
    None
}

fn fault(value: &Value) -> Result<Fault, String> {
    let point = point(text(value, "point")?)?;
    let matching = value.get("match").ok_or("a fault without match")?;
    Ok(Fault {
        id: text(value, "id")?.to_owned(),
        point,
        action: action(point, text(value, "action")?)?,
        command_kind: matching
            .get("command_kind")
            .and_then(Value::as_str)
            .map(work_kind)
            .transpose()?,
        publication_binding: matching
            .get("publication_binding")
            .and_then(Value::as_str)
            .map(str::to_owned),
        result_of: matching
            .get("result_of")
            .and_then(Value::as_str)
            .map(str::to_owned),
        occurrence: decimal(value, "occurrence")?,
        expected_hits: decimal(value, "expected_hits")?,
    })
}

fn step(value: &Value) -> Result<Step, String> {
    let kind = text(value, "kind")?;
    let client = || text(value, "client").map(str::to_owned);
    Ok(match kind {
        "ClientResumeSession" => Step::ClientResumeSession {
            client: client()?,
            expect_session_present: value
                .get("expect_session_present")
                .and_then(Value::as_bool)
                .ok_or("no expect_session_present")?,
            max_steps: decimal(value, "max_steps")?,
        },
        "ClientPublish" => Step::ClientPublish {
            client: client()?,
            topic: text(value, "topic")?.as_bytes().to_vec(),
            qos: u8::try_from(small(value, "qos")?.ok_or("no qos")?)
                .map_err(|_| "qos beyond a byte")?,
            retain: value
                .get("retain")
                .and_then(Value::as_bool)
                .ok_or("no retain")?,
            packet_id: packet_id(value, "packet_id")?,
            payload: hex(text(value, "payload_hex")?)?,
            bind_exchange_as: value
                .get("bind_exchange_as")
                .and_then(Value::as_str)
                .map(str::to_owned),
        },
        "ClientResumeExchange" => Step::ClientResumeExchange {
            client: client()?,
            exchange: text(value, "exchange")?.to_owned(),
        },
        "ClientSendPubrel" => Step::ClientSendPubrel {
            client: client()?,
            packet_id: packet_id(value, "packet_id")?.ok_or("no packet_id")?,
        },
        "DriveUntil" => {
            let event = match text(value, "event")? {
                "FaultHit" => Event::FaultHit {
                    fault: text(value, "binding")?.to_owned(),
                },
                "PeerPacketObserved" => Event::PeerPacketObserved {
                    client: client()?,
                    packet_kind: text(value, "packet_kind")?.to_owned(),
                    packet_id: packet_id(value, "packet_id")?,
                },
                other => return Err(format!("the runner cannot wait for {other}")),
            };
            Step::DriveUntil {
                event,
                max_steps: decimal(value, "max_steps")?,
                fire_deadlines: match value.get("fire_deadlines") {
                    None => false,
                    Some(flag) => flag.as_bool().ok_or("fire_deadlines is not a boolean")?,
                },
            }
        }
        "FireDeadline" => Step::FireDeadline {
            max_steps: decimal(value, "max_steps")?,
        },
        "ReleaseHeldCommand" => Step::ReleaseHeldCommand {
            fault: text(value, "fault")?.to_owned(),
        },
        "DeliverHeldResult" => Step::DeliverHeldResult {
            fault: text(value, "fault")?.to_owned(),
            max_steps: decimal(value, "max_steps")?,
        },
        "LeaveBarrier" => Step::LeaveBarrier {
            fault: text(value, "fault")?.to_owned(),
        },
        "InspectDurableState" => Step::InspectDurableState {
            checkpoint: text(value, "checkpoint")?.to_owned(),
        },
        "CrashBroker" => {
            if text(value, "failure_model")? != "process" {
                return Err("the runner simulates the process failure model only".to_owned());
            }
            Step::CrashBroker
        }
        "RestartBroker" => {
            if small(value, "workers")? != Some(1) {
                return Err("the runner restarts one worker".to_owned());
            }
            Step::RestartBroker
        }
        other => return Err(format!("the runner cannot play the step {other}")),
    })
}

impl Scenario {
    /// Reads a scenario document.
    pub fn read(document: &Value) -> Result<Self, String> {
        let budgets = document.get("budgets").ok_or("no budgets")?;
        let list = |name: &str| {
            document
                .get(name)
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[])
        };
        Ok(Self {
            id: text(document, "id")?.to_owned(),
            perimeter: document
                .get("perimeter")
                .and_then(Value::as_str)
                .unwrap_or("full")
                .to_owned(),
            initial_fixture: text(document, "initial_fixture")?.to_owned(),
            max_steps: decimal(budgets, "max_steps")?,
            max_trace_bytes: decimal(budgets, "max_trace_bytes")?,
            faults: list("faults").iter().map(fault).collect::<Result<_, _>>()?,
            steps: list("steps")
                .iter()
                .enumerate()
                .map(|(index, value)| step(value).map_err(|error| format!("step {index}: {error}")))
                .collect::<Result<_, _>>()?,
            assertions: list("assertions")
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        })
    }

    /// The failure model the scenario exercises.
    #[must_use]
    pub fn failure_model(&self) -> &'static str {
        if self.steps.contains(&Step::CrashBroker) {
            "process"
        } else {
            "none"
        }
    }
}
