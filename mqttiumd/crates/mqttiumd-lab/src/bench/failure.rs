//! Bounded, typed causes that cross the child/supervisor boundary.

use mqttiumd_store_api::{ReadError, ReadPhase, RecoveryRefused};
use serde_json::{Value, json};

pub const MAX_FAILURE_MESSAGE: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadCause {
    Error(ReadError),
    FrontierMoved,
    EmptyPage,
}

impl ReadCause {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Error(error) => error.code(),
            Self::FrontierMoved => "FrontierMoved",
            Self::EmptyPage => "EmptyPage",
        }
    }
}

impl From<ReadError> for ReadCause {
    fn from(error: ReadError) -> Self {
        Self::Error(error)
    }
}

impl From<RecoveryRefused> for ReadCause {
    fn from(error: RecoveryRefused) -> Self {
        match error {
            RecoveryRefused::Read(error) => Self::Error(error),
            RecoveryRefused::FrontierMoved => Self::FrontierMoved,
            RecoveryRefused::EmptyPage => Self::EmptyPage,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ControlCode {
    TruncatedFrame,
    FrameTooLarge,
    InvalidFrame,
    Io,
    ClosedChannel,
    DuplicateTermination,
    ReportBudgetExceeded,
    ActionDeadlineExceeded,
    DrainDeadlineExceeded,
    LateEvidence,
    LateReport,
    InvalidOrdering,
    InvalidParked,
}

impl ControlCode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::TruncatedFrame => "TruncatedFrame",
            Self::FrameTooLarge => "FrameTooLarge",
            Self::InvalidFrame => "InvalidFrame",
            Self::Io => "Io",
            Self::ClosedChannel => "ClosedChannel",
            Self::DuplicateTermination => "DuplicateTermination",
            Self::ReportBudgetExceeded => "ReportBudgetExceeded",
            Self::ActionDeadlineExceeded => "ActionDeadlineExceeded",
            Self::DrainDeadlineExceeded => "DrainDeadlineExceeded",
            Self::LateEvidence => "LateEvidence",
            Self::LateReport => "LateReport",
            Self::InvalidOrdering => "InvalidOrdering",
            Self::InvalidParked => "InvalidParked",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ControlPhase {
    Start,
    Action,
    KillDrain,
    Stop,
}

impl ControlPhase {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Start => "Start",
            Self::Action => "Action",
            Self::KillDrain => "KillDrain",
            Self::Stop => "Stop",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Failure {
    Read {
        cause: ReadCause,
        phase: ReadPhase,
        process_incarnation: u64,
        page: Option<u64>,
    },
    Control {
        code: ControlCode,
        phase: ControlPhase,
        process_incarnation: u64,
    },
}

impl Failure {
    pub const fn record_kind(self) -> &'static str {
        match self {
            Self::Read { .. } => "DurableReadFailed",
            Self::Control { .. } => "ProcessControlFailed",
        }
    }

    pub fn payload(self) -> Value {
        match self {
            Self::Read {
                cause,
                phase,
                process_incarnation,
                page,
            } => {
                let mut result = json!({"code": cause.code(), "phase": phase.code(), "process_incarnation": process_incarnation.to_string()});
                if let Some(page) = page {
                    result["page"] = json!(page.to_string());
                }
                result
            }
            Self::Control {
                code,
                phase,
                process_incarnation,
            } => {
                json!({"code": code.code(), "phase": phase.code(), "process_incarnation": process_incarnation.to_string()})
            }
        }
    }

    pub fn to_wire(self) -> Value {
        let mut result = self.payload();
        result["kind"] = json!(self.record_kind());
        result
    }

    /// Decode stable structured codes, never a human diagnostic.
    pub fn from_wire(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "kind" | "code" | "phase" | "process_incarnation" | "page"
            )
        }) {
            return None;
        }
        let incarnation = object.get("process_incarnation")?.as_str()?.parse().ok()?;
        let code = object.get("code")?.as_str()?;
        let phase = object.get("phase")?.as_str()?;
        match object.get("kind")?.as_str()? {
            "DurableReadFailed" => {
                let cause = match code {
                    "InvalidData" => ReadCause::Error(ReadError::InvalidData),
                    "LimitExceeded" => ReadCause::Error(ReadError::LimitExceeded),
                    "Unavailable" => ReadCause::Error(ReadError::Unavailable),
                    "Io" => ReadCause::Error(ReadError::Io),
                    "AllocationFailed" => ReadCause::Error(ReadError::AllocationFailed),
                    "FrontierMoved" => ReadCause::FrontierMoved,
                    "EmptyPage" => ReadCause::EmptyPage,
                    _ => return None,
                };
                let phase = match phase {
                    "Open" => ReadPhase::Open,
                    "Ledger" => ReadPhase::Ledger,
                    "Opening" => ReadPhase::Opening,
                    "Snapshot" => ReadPhase::Snapshot,
                    "Lookup" => ReadPhase::Lookup,
                    "Inspect" => ReadPhase::Inspect,
                    "Recovery" => ReadPhase::Recovery,
                    _ => return None,
                };
                let page = match object.get("page") {
                    Some(page) => Some(page.as_str()?.parse().ok()?),
                    None => None,
                };
                Some(Self::Read {
                    cause,
                    phase,
                    process_incarnation: incarnation,
                    page,
                })
            }
            "ProcessControlFailed" => {
                if object.contains_key("page") {
                    return None;
                }
                let code = match code {
                    "TruncatedFrame" => ControlCode::TruncatedFrame,
                    "FrameTooLarge" => ControlCode::FrameTooLarge,
                    "InvalidFrame" => ControlCode::InvalidFrame,
                    "Io" => ControlCode::Io,
                    "ClosedChannel" => ControlCode::ClosedChannel,
                    "DuplicateTermination" => ControlCode::DuplicateTermination,
                    "ReportBudgetExceeded" => ControlCode::ReportBudgetExceeded,
                    "ActionDeadlineExceeded" => ControlCode::ActionDeadlineExceeded,
                    "DrainDeadlineExceeded" => ControlCode::DrainDeadlineExceeded,
                    "LateEvidence" => ControlCode::LateEvidence,
                    "LateReport" => ControlCode::LateReport,
                    "InvalidOrdering" => ControlCode::InvalidOrdering,
                    "InvalidParked" => ControlCode::InvalidParked,
                    _ => return None,
                };
                let phase = match phase {
                    "Start" => ControlPhase::Start,
                    "Action" => ControlPhase::Action,
                    "KillDrain" => ControlPhase::KillDrain,
                    "Stop" => ControlPhase::Stop,
                    _ => return None,
                };
                Some(Self::Control {
                    code,
                    phase,
                    process_incarnation: incarnation,
                })
            }
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Stop {
    pub message: String,
    pub failure: Option<Failure>,
}

impl Stop {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: bounded(message.into()),
            failure: None,
        }
    }

    pub fn failed(failure: Failure) -> Self {
        Self {
            message: format!(
                "{}: {}",
                failure.record_kind(),
                failure.payload()["code"].as_str().unwrap_or("unknown")
            ),
            failure: Some(failure),
        }
    }

    pub fn read(
        error: impl Into<ReadCause>,
        phase: ReadPhase,
        process_incarnation: u64,
        page: Option<u64>,
    ) -> Self {
        Self::failed(Failure::Read {
            cause: error.into(),
            phase,
            process_incarnation,
            page,
        })
    }

    pub fn control(
        code: ControlCode,
        phase: ControlPhase,
        process_incarnation: u64,
        message: impl Into<String>,
    ) -> Self {
        Self {
            message: bounded(message.into()),
            failure: Some(Failure::Control {
                code,
                phase,
                process_incarnation,
            }),
        }
    }

    pub fn context(mut self, context: impl AsRef<str>) -> Self {
        self.message = bounded(format!("{}: {}", context.as_ref(), self.message));
        self
    }
}

fn bounded(mut value: String) -> String {
    if value.len() > MAX_FAILURE_MESSAGE {
        let mut end = MAX_FAILURE_MESSAGE;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    value
}

impl std::fmt::Display for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Stop {}
