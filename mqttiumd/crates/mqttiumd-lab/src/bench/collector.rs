//! The collector of a run: the one writer of the trace (DEM-016, ADR 0003).
//!
//! Each component hands its records over on a channel of its own, and the
//! collector assigns the origin of a record from that channel. A record
//! is accepted only when:
//!
//! - its kind is stated by the origin of the channel, as the trace-record
//!   schema of the contracts the campaign runs with says, kind by kind;
//! - a record already built by a component (the store adapter's writer)
//!   claims the origin of the channel it arrived on, the current
//!   incarnation of that origin, and the next sequence of that origin.
//!
//! A refused record is not written. The refusal is kept, and the trace
//! is then declared incomplete in its footer: a missing record may be
//! the one an assertion needs. The collector numbers every record it
//! writes, keeps one gapless sequence per origin and incarnation, and
//! bounds the trace by the budget of the scenario.

use std::collections::BTreeMap;

use serde_json::{Value, json};

/// The components of a run, as the trace format names them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Origin {
    BenchSupervisor,
    BrokerCore,
    Dispatcher,
    FaultInjector,
    Peer,
    StoreAdapter,
    StoreEvidence,
    Timer,
    Transport,
}

impl Origin {
    pub const ALL: [Self; 9] = [
        Self::BenchSupervisor,
        Self::BrokerCore,
        Self::Dispatcher,
        Self::FaultInjector,
        Self::Peer,
        Self::StoreAdapter,
        Self::StoreEvidence,
        Self::Timer,
        Self::Transport,
    ];

    /// The name of the origin in the trace format.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::BenchSupervisor => "bench-supervisor",
            Self::BrokerCore => "broker-core",
            Self::Dispatcher => "dispatcher",
            Self::FaultInjector => "fault-injector",
            Self::Peer => "peer",
            Self::StoreAdapter => "store-adapter",
            Self::StoreEvidence => "store-evidence",
            Self::Timer => "timer",
            Self::Transport => "transport",
        }
    }

    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|origin| origin.name() == name)
    }

    /// Whether the component lives in the process under test, and so
    /// starts a new incarnation when that process restarts. The
    /// supervisor and the peers live outside it (ADR 0002).
    #[must_use]
    pub const fn in_process(self) -> bool {
        !matches!(self, Self::BenchSupervisor | Self::Peer)
    }
}

/// A record already written, as a causal parent names it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cause {
    pub origin: Origin,
    pub incarnation: u64,
    pub sequence: u64,
}

impl Cause {
    fn to_json(self) -> Value {
        json!({
            "origin_component": self.origin.name(),
            "origin_incarnation": self.incarnation.to_string(),
            "origin_sequence": self.sequence.to_string(),
        })
    }
}

/// Why a record was not written.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Refusal {
    /// The record claims an origin other than its channel.
    ClaimsAnotherOrigin { channel: Origin, claimed: String },
    /// The schema states this kind by another origin.
    KindOfAnotherOrigin { channel: Origin, kind: String },
    /// The record claims an incarnation or a sequence the collector did
    /// not assign next on its channel.
    OutOfSequence { channel: Origin, kind: String },
    /// The record is not an object with the members of a record.
    Malformed(String),
    /// The trace reached the budget of the scenario.
    BudgetExceeded,
    /// The footer was written; nothing follows it.
    Closed,
}

impl core::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ClaimsAnotherOrigin { channel, claimed } => write!(
                formatter,
                "a record on the channel {} claims the origin {claimed}",
                channel.name()
            ),
            Self::KindOfAnotherOrigin { channel, kind } => {
                write!(formatter, "{kind} is not a record of {}", channel.name())
            }
            Self::OutOfSequence { channel, kind } => write!(
                formatter,
                "{kind} on the channel {} is out of its sequence",
                channel.name()
            ),
            Self::Malformed(why) => write!(formatter, "not a record: {why}"),
            Self::BudgetExceeded => write!(formatter, "the trace budget is exhausted"),
            Self::Closed => write!(formatter, "the trace is closed"),
        }
    }
}

/// Which origin states each kind, read from the trace-record schema.
#[derive(Clone, Debug, Default)]
pub struct KindTable(BTreeMap<String, Vec<Origin>>);

impl KindTable {
    /// Reads the `allOf` of the trace-record schema: one `if kind` /
    /// `then origin_component` pair per kind.
    pub fn from_schema(schema: &Value) -> Result<Self, String> {
        let mut table = BTreeMap::new();
        let rules = schema
            .get("allOf")
            .and_then(Value::as_array)
            .ok_or("the trace-record schema has no allOf")?;
        for rule in rules {
            let Some(kind) = rule
                .pointer("/if/properties/kind/const")
                .and_then(Value::as_str)
            else {
                continue;
            };
            let Some(origins) = rule
                .pointer("/then/properties/origin_component/enum")
                .and_then(Value::as_array)
            else {
                continue;
            };
            let origins = origins
                .iter()
                .map(|name| {
                    name.as_str()
                        .and_then(Origin::from_name)
                        .ok_or_else(|| format!("{kind}: unknown origin {name}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            table.insert(kind.to_owned(), origins);
        }
        if table.is_empty() {
            return Err("the trace-record schema states no origin".to_owned());
        }
        Ok(Self(table))
    }

    fn allows(&self, kind: &str, origin: Origin) -> bool {
        self.0
            .get(kind)
            .is_some_and(|origins| origins.contains(&origin))
    }
}

/// The trace of one run, as it is collected.
pub struct Collector {
    run_id: String,
    kinds: KindTable,
    budget: u64,
    lines: Vec<u8>,
    count: u64,
    incarnations: BTreeMap<Origin, u64>,
    sequences: BTreeMap<(Origin, u64), u64>,
    refusals: Vec<Refusal>,
    closed: bool,
}

impl Collector {
    /// An empty trace for `run_id`, bounded to `budget` bytes. Every
    /// origin starts at incarnation 1.
    #[must_use]
    pub fn new(run_id: &str, kinds: KindTable, budget: u64) -> Self {
        Self {
            run_id: run_id.to_owned(),
            kinds,
            budget,
            lines: Vec::new(),
            count: 0,
            incarnations: Origin::ALL.into_iter().map(|origin| (origin, 1)).collect(),
            sequences: BTreeMap::new(),
            refusals: Vec::new(),
            closed: false,
        }
    }

    /// The current incarnation of an origin.
    #[must_use]
    pub fn incarnation(&self, origin: Origin) -> u64 {
        self.incarnations.get(&origin).copied().unwrap_or(1)
    }

    /// The process under test restarted: each of its components starts
    /// a new incarnation, with its sequence from zero.
    pub fn restart_process(&mut self) {
        for (origin, incarnation) in &mut self.incarnations {
            if origin.in_process() {
                *incarnation = incarnation.saturating_add(1);
            }
        }
    }

    /// The next sequence of `origin` in its current incarnation.
    #[must_use]
    pub fn next_sequence(&self, origin: Origin) -> u64 {
        let incarnation = self.incarnation(origin);
        self.sequences
            .get(&(origin, incarnation))
            .copied()
            .unwrap_or(0)
    }

    /// Records refused so far.
    #[must_use]
    pub fn refusals(&self) -> &[Refusal] {
        &self.refusals
    }

    /// Records written so far.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    fn refuse(&mut self, refusal: Refusal) -> Refusal {
        self.refusals.push(refusal.clone());
        refusal
    }

    fn write(
        &mut self,
        channel: Origin,
        kind: &str,
        parents: &[Cause],
        payload: Value,
    ) -> Result<Cause, Refusal> {
        if self.closed {
            return Err(self.refuse(Refusal::Closed));
        }
        if !self.kinds.allows(kind, channel) {
            return Err(self.refuse(Refusal::KindOfAnotherOrigin {
                channel,
                kind: kind.to_owned(),
            }));
        }
        let incarnation = self.incarnation(channel);
        let sequence = self.next_sequence(channel);
        let record_sequence = self.count;
        let mut unique: Vec<Value> = Vec::new();
        for parent in parents {
            let parent = parent.to_json();
            if !unique.contains(&parent) {
                unique.push(parent);
            }
        }
        let record = json!({
            "format": "mqttiumd.trace-record",
            "schema_version": 1,
            "run_id": self.run_id,
            "record_sequence": record_sequence.to_string(),
            "origin_component": channel.name(),
            "origin_incarnation": incarnation.to_string(),
            "origin_sequence": sequence.to_string(),
            "causal_parents": unique,
            "time_domain": "logical-step",
            "time_value": record_sequence.to_string(),
            "kind": kind,
            "payload": payload,
        });
        let mut line =
            serde_json::to_vec(&record).map_err(|error| Refusal::Malformed(error.to_string()))?;
        line.push(b'\n');
        let total = (self.lines.len() as u64).saturating_add(line.len() as u64);
        if total > self.budget {
            self.closed = true;
            return Err(self.refuse(Refusal::BudgetExceeded));
        }
        self.lines.extend_from_slice(&line);
        self.count += 1;
        self.sequences.insert((channel, incarnation), sequence + 1);
        Ok(Cause {
            origin: channel,
            incarnation,
            sequence,
        })
    }

    /// Writes a record that the component of `channel` states.
    pub fn emit(
        &mut self,
        channel: Origin,
        kind: &str,
        parents: &[Cause],
        payload: Value,
    ) -> Result<Cause, Refusal> {
        self.write(channel, kind, parents, payload)
    }

    /// Takes a record a component built itself. Its claims are checked
    /// against the channel; the collector numbers it.
    pub fn accept(&mut self, channel: Origin, record: &Value) -> Result<Cause, Refusal> {
        let member = |name: &str| record.get(name).and_then(Value::as_str);
        let kind = member("kind").ok_or(Refusal::Malformed("no kind".to_owned()));
        let kind = match kind {
            Ok(kind) => kind.to_owned(),
            Err(refusal) => return Err(self.refuse(refusal)),
        };
        let claimed = member("origin_component").unwrap_or("").to_owned();
        if claimed != channel.name() {
            return Err(self.refuse(Refusal::ClaimsAnotherOrigin { channel, claimed }));
        }
        let incarnation = self.incarnation(channel).to_string();
        let sequence = self.next_sequence(channel).to_string();
        if member("origin_incarnation") != Some(incarnation.as_str())
            || member("origin_sequence") != Some(sequence.as_str())
            || member("run_id") != Some(self.run_id.as_str())
        {
            return Err(self.refuse(Refusal::OutOfSequence { channel, kind }));
        }
        let mut parents = Vec::new();
        for parent in record
            .get("causal_parents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let origin = parent
                .get("origin_component")
                .and_then(Value::as_str)
                .and_then(Origin::from_name);
            let number = |name: &str| {
                parent
                    .get(name)
                    .and_then(Value::as_str)
                    .and_then(|text| text.parse::<u64>().ok())
            };
            match (
                origin,
                number("origin_incarnation"),
                number("origin_sequence"),
            ) {
                (Some(origin), Some(incarnation), Some(sequence)) => parents.push(Cause {
                    origin,
                    incarnation,
                    sequence,
                }),
                _ => return Err(self.refuse(Refusal::Malformed("a causal parent".to_owned()))),
            }
        }
        let payload = record.get("payload").cloned().unwrap_or(Value::Null);
        self.write(channel, &kind, &parents, payload)
    }

    /// Writes the footer and returns the trace. `complete` is what the
    /// caller knows of the run; a refused record makes it false.
    pub fn finish(mut self, complete: bool) -> (Vec<u8>, Vec<Refusal>) {
        let complete = complete && self.refusals.is_empty() && !self.closed;
        let count = self.count.saturating_add(1);
        if !self.closed {
            let footer = json!({"record_count": count.to_string(), "complete": complete});
            // A footer that does not fit the budget closes nothing: the
            // trace stays without one, which every reader takes as cut.
            let _ = self.write(Origin::BenchSupervisor, "TraceFooter", &[], footer);
        }
        (self.lines, self.refusals)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Collector, KindTable, Origin, Refusal};

    fn kinds() -> KindTable {
        KindTable::from_schema(&json!({"allOf": [
            {"if": {"properties": {"kind": {"const": "FaultHit"}}},
             "then": {"properties": {"origin_component": {"enum": ["fault-injector"]}}}},
            {"if": {"properties": {"kind": {"const": "TraceFooter"}}},
             "then": {"properties": {"origin_component": {"enum": ["bench-supervisor"]}}}},
        ]}))
        .expect("a table")
    }

    #[test]
    fn a_record_that_claims_another_origin_is_refused_and_cuts_the_trace() {
        let mut collector = Collector::new("run", kinds(), 1 << 20);
        let forged = json!({
            "kind": "FaultHit",
            "origin_component": "fault-injector",
            "origin_incarnation": "1",
            "origin_sequence": "0",
            "run_id": "run",
            "causal_parents": [],
            "payload": {},
        });
        assert_eq!(
            collector.accept(Origin::StoreAdapter, &forged),
            Err(Refusal::ClaimsAnotherOrigin {
                channel: Origin::StoreAdapter,
                claimed: "fault-injector".to_owned()
            })
        );
        assert!(collector.accept(Origin::FaultInjector, &forged).is_ok());
        let (trace, refusals) = collector.finish(true);
        assert_eq!(refusals.len(), 1);
        let text = String::from_utf8(trace).unwrap();
        assert!(text.contains("\"complete\":false"), "{text}");
    }

    #[test]
    fn a_kind_is_written_only_on_the_channel_of_its_origin() {
        let mut collector = Collector::new("run", kinds(), 1 << 20);
        assert!(matches!(
            collector.emit(Origin::StoreAdapter, "FaultHit", &[], json!({})),
            Err(Refusal::KindOfAnotherOrigin { .. })
        ));
        let cause = collector
            .emit(Origin::FaultInjector, "FaultHit", &[], json!({}))
            .unwrap();
        assert_eq!((cause.incarnation, cause.sequence), (1, 0));
        collector.restart_process();
        let cause = collector
            .emit(Origin::FaultInjector, "FaultHit", &[], json!({}))
            .unwrap();
        assert_eq!((cause.incarnation, cause.sequence), (2, 0));
    }

    #[test]
    fn a_record_out_of_its_sequence_is_refused() {
        let mut collector = Collector::new("run", kinds(), 1 << 20);
        let ahead = json!({
            "kind": "FaultHit",
            "origin_component": "fault-injector",
            "origin_incarnation": "1",
            "origin_sequence": "3",
            "run_id": "run",
            "causal_parents": [],
            "payload": {},
        });
        assert!(matches!(
            collector.accept(Origin::FaultInjector, &ahead),
            Err(Refusal::OutOfSequence { .. })
        ));
    }

    #[test]
    fn the_budget_bounds_the_trace() {
        let mut collector = Collector::new("run", kinds(), 64);
        assert_eq!(
            collector.emit(Origin::FaultInjector, "FaultHit", &[], json!({})),
            Err(Refusal::BudgetExceeded)
        );
        let (trace, _) = collector.finish(true);
        assert!(trace.is_empty());
    }
}
