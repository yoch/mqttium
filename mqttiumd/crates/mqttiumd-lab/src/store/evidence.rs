//! The evidence of the store in the formats of lot L3 (record 0016):
//! the store snapshot (`contracts/schema/v1/store-snapshot.schema.json`)
//! and the trace records of the adapter
//! (`contracts/schema/v1/trace-record.schema.json`).
//!
//! What has no record in those formats is not invented here; it is
//! returned to the caller, named. The opening of an epoch has no trace
//! record yet, a violation has no store outcome, and the delivery of a
//! result to the core is the core's own record (`StoreResultDelivered`,
//! origin `broker-core`).

use std::collections::BTreeMap;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use mqttiumd_contract::capacity::Resource;
use mqttiumd_contract::command::{DeliveryCause, OpeningAttempt};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, EffectId, ExchangeId, OperationId, PayloadId, PublicationId, SessionId,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::Qos;
use mqttiumd_contract::pubsub::SubscriptionOptions;
use mqttiumd_contract::receipts::ReceiptClosure;
use mqttiumd_contract::session::{SessionChange, SessionDetachment, SessionExpiry};

use super::adapter::{Answer, FaultId, FaultPoint, StoreEvent, Uncorrelated, WorkId, WorkKind};
use super::durable::{Decision, DurableState, ObligationPhase, SessionState};

/// A 64-bit value, as every JSON format of the bench writes it.
fn decimal(value: u64) -> Value {
    Value::String(value.to_string())
}

fn receipt_closure(closure: ReceiptClosure) -> Value {
    json!({
        "origin": {
            "incarnation": decimal(closure.origin.incarnation.get()),
            "epoch": decimal(closure.origin.epoch.get()),
            "owner": decimal(closure.origin.owner.get()),
        },
        "through": decimal(closure.through.get()),
        "publication_through": decimal(closure.publication_through.get()),
        "payload_through": decimal(closure.payload_through.get()),
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

fn qos(qos: Qos) -> Value {
    json!(qos as u8)
}

fn session_expiry(expiry: SessionExpiry) -> Value {
    match expiry {
        SessionExpiry::OnDisconnect => json!("OnDisconnect"),
        SessionExpiry::Never => json!("Never"),
        SessionExpiry::After(seconds) => json!({"After": seconds}),
    }
}

fn detachment(detached: SessionDetachment) -> Value {
    json!({
        "operation": operation_id(detached.operation),
        "deadline": detached.deadline.map(decimal),
    })
}

fn operation_id(operation: OperationId) -> Value {
    json!({
        "incarnation": decimal(operation.incarnation().get()),
        "epoch": decimal(operation.epoch().get()),
        "owner": decimal(operation.owner().get()),
        "sequence": decimal(operation.sequence().get()),
    })
}

fn publication_id(publication: PublicationId) -> Value {
    json!({
        "incarnation": decimal(publication.incarnation().get()),
        "epoch": decimal(publication.epoch().get()),
        "owner": decimal(publication.owner().get()),
        "sequence": decimal(publication.sequence().get()),
    })
}

fn payload_id(payload: PayloadId) -> Value {
    json!({
        "incarnation": decimal(payload.incarnation().get()),
        "epoch": decimal(payload.epoch().get()),
        "owner": decimal(payload.owner().get()),
        "sequence": decimal(payload.sequence().get()),
    })
}

fn delivery_id(delivery: DeliveryId) -> Value {
    json!({
        "publication_id": publication_id(delivery.publication),
        "index": delivery.index,
    })
}

fn exchange_id(exchange: ExchangeId) -> Value {
    json!({
        "session": decimal(exchange.session.get()),
        "generation": decimal(exchange.generation.get()),
    })
}

fn effect_id(effect: EffectId) -> Value {
    json!({
        "epoch": decimal(effect.epoch.get()),
        "owner": decimal(effect.owner.get()),
        "sequence": decimal(effect.sequence.get()),
    })
}

fn session(session: SessionId) -> Value {
    decimal(session.get())
}

fn resource(resource: Resource) -> &'static str {
    match resource {
        Resource::PayloadBytes => "payload_bytes",
        Resource::References => "references",
        Resource::ExchangeSlots => "exchange_slots",
        Resource::ReceiptSlots => "receipt_slots",
        Resource::EffectSlots => "effect_slots",
        Resource::ResultSlots => "result_slots",
        Resource::Continuations => "continuations",
        Resource::Reservations => "reservations",
        Resource::Work => "work",
    }
}

fn reason(reason: RejectReason) -> &'static str {
    match reason {
        RejectReason::PreconditionFailed => "PreconditionFailed",
        RejectReason::CapacityExceeded(_) => "CapacityExceeded",
        RejectReason::Fenced => "Fenced",
        RejectReason::EpochClosed => "EpochClosed",
        RejectReason::SessionCapacityExceeded => "SessionCapacityExceeded",
    }
}

fn facts(facts: CommitFacts) -> Value {
    match facts {
        CommitFacts::SessionLifecycle(change) => {
            let change = match change {
                SessionChange::Opened {
                    session: id,
                    revision,
                    session_present,
                    expiry,
                    retired,
                    route_revision,
                    cancelled,
                } => json!({
                    "kind": "Opened", "session": session(id), "revision": decimal(revision.get()),
                    "session_present": session_present, "expiry": session_expiry(expiry),
                    "retired": retired.map(session), "route_revision": decimal(route_revision.get()),
                    "cancelled": { "inbound": cancelled.inbound, "deliveries": cancelled.deliveries, "routes": cancelled.routes },
                }),
                SessionChange::Detached {
                    session: id,
                    expiry,
                    detached,
                } => json!({
                    "kind": "Detached", "session": session(id),
                    "expiry": session_expiry(expiry), "detached": detachment(detached),
                }),
                SessionChange::Closed {
                    session: id,
                    route_revision,
                    cancelled,
                } => json!({
                    "kind": "Closed", "session": session(id), "route_revision": decimal(route_revision.get()),
                    "cancelled": { "inbound": cancelled.inbound, "deliveries": cancelled.deliveries, "routes": cancelled.routes },
                }),
            };
            json!({"kind": "SessionLifecycle", "change": change})
        }
        CommitFacts::RetainedQos0Stored { publication } => {
            json!({"kind":"RetainedQos0Stored","publication_id":publication_id(publication)})
        }
        CommitFacts::DeliveryConsumed { delivery, target } => json!({
            "kind": "DeliveryConsumed", "delivery_id": delivery_id(delivery),
            "target_session": session(target),
        }),
        CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        } => json!({
            "kind": "PublicationAdmitted",
            "publication_id": publication_id(publication),
            "exchange": exchange_id(exchange),
        }),
        CommitFacts::InboundReleased { exchange } => json!({
            "kind": "InboundReleased",
            "exchange": exchange_id(exchange),
        }),
        CommitFacts::SubscriptionsChanged {
            session: id,
            route_revision,
        } => json!({
            "kind": "SubscriptionsChanged",
            "session": session(id),
            "route_revision": decimal(route_revision.get()),
        }),
        CommitFacts::DeliveryClaimed {
            delivery,
            target,
            packet_id,
            claim,
        }
        | CommitFacts::DeliveryFinished {
            delivery,
            target,
            packet_id,
            claim,
        } => json!({
            "kind": if matches!(facts, CommitFacts::DeliveryClaimed { .. }) {
                "DeliveryClaimed"
            } else {
                "DeliveryFinished"
            },
            "delivery_id": delivery_id(delivery),
            "target_session": session(target),
            "packet_id": packet_id.get(),
            "claim_operation_id": operation_id(claim),
        }),
    }
}

fn decision(decision: Decision) -> Value {
    match decision {
        Decision::Committed(committed) => json!({
            "kind": "Committed",
            "facts": facts(committed),
        }),
        Decision::Rejected(rejected) => {
            let mut value = json!({
                "kind": "RejectedCertain",
                "reason": reason(rejected),
            });
            if let RejectReason::CapacityExceeded(short) = rejected {
                value["resource"] = json!(resource(short));
            }
            value
        }
    }
}

fn opened_by(attempt: Option<OpeningAttempt>) -> Value {
    attempt.map_or(Value::Null, |attempt| {
        json!({
            "startup_hex": hex(&attempt.startup.to_bytes()),
            "ticket": decimal(attempt.ticket.get()),
        })
    })
}

fn usize_decimal(value: usize) -> Value {
    // A `usize` of this crate's targets fits 64 bits.
    decimal(u64::try_from(value).unwrap_or(u64::MAX))
}

/// Where a snapshot was taken, and of what.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotContext<'a> {
    pub run_id: &'a str,
    pub checkpoint: &'a str,
    /// `sim-store` for this store.
    pub backend: &'a str,
}

/// The durable state of a store as a store snapshot. Rows are in the
/// order of their keys, as the collections of `DurableState` hold them,
/// so that two stores in the same state write the same document.
#[must_use]
pub fn snapshot(state: &DurableState, context: SnapshotContext<'_>) -> Value {
    let sessions: Vec<Value> = state
        .sessions
        .iter()
        .map(|(id, record)| {
            let mut row = json!({
                "session": session(*id),
                "client_id_hex": hex(&record.client_id),
                "revision": decimal(record.revision.get()),
                "expiry": session_expiry(record.expiry),
                "opened_by": record.opened_by.map(operation_id),
                "state": match record.state {
                    SessionState::Active => "Active",
                    SessionState::Terminal => "Terminal",
                },
            });
            if let Some(detached) = record.detached {
                row["detached"] = detachment(detached);
            }
            row
        })
        .collect();
    let routes: Vec<Value> = state
        .routes
        .iter()
        .map(|(topic, targets)| {
            let targets: Vec<Value> = targets
                .iter()
                .map(|target| {
                    let mut row =
                        json!({"session": session(target.session), "qos": qos(target.qos)});
                    if target.options != SubscriptionOptions::DEFAULT {
                        row["options"] = json!(target.options.bits());
                    }
                    row
                })
                .collect();
            json!({"topic_hex": hex(topic), "targets": targets})
        })
        .collect();
    let operations: Vec<Value> = state
        .operations
        .iter()
        .map(|(id, record)| {
            json!({
                "operation_id": operation_id(*id),
                "recipe_hex": hex(&record.recipe),
                "decision": decision(record.decision),
            })
        })
        .collect();
    let fences: Vec<Value> = state
        .fences
        .iter()
        .map(|id| json!({"operation_id": operation_id(*id)}))
        .collect();
    let payloads: Vec<Value> = state
        .payloads
        .iter()
        .map(|(id, record)| {
            json!({
                "payload_id": payload_id(*id),
                "length": usize_decimal(record.bytes.len()),
                "sha256": hex(&Sha256::digest(&record.bytes)),
                "references": decimal(record.references),
            })
        })
        .collect();
    let publications: Vec<Value> = state
        .publications
        .iter()
        .map(|(id, record)| {
            json!({
                "publication_id": publication_id(*id),
                "operation_id": operation_id(record.operation),
                "source_session": session(record.source),
                "topic_hex": hex(&record.topic),
                "qos": qos(record.qos),
                "retain": record.retain,
                "payload_id": payload_id(record.payload),
                "retained_version": record.retained_version.map(publication_id),
            })
        })
        .collect();
    let obligations: Vec<Value> = state
        .obligations
        .iter()
        .map(|(delivery, record)| {
            json!({
                "publication_id": publication_id(delivery.publication),
                "index": delivery.index,
                "session": session(record.target),
                "target_revision": decimal(record.target_revision.get()),
                "cause": match record.cause {
                    DeliveryCause::LiveExact => "LiveExact",
                    DeliveryCause::RetainedReplay=>"RetainedReplay",
                },
                "effective_qos": qos(record.qos),
                "retain":record.retain,
                "phase": match record.phase {
                    ObligationPhase::Pending => "Queued",
                },
            })
        })
        .collect();
    let inbound: Vec<Value> = state
        .inbound
        .iter()
        .map(|((_, packet_id), marker)| {
            json!({
                "exchange": exchange_id(marker.exchange),
                "packet_id": packet_id.get(),
                "publication_id": publication_id(marker.publication),
                "phase": match marker.phase {
                    InboundPhase::AwaitPubrel => "AwaitPubrel",
                },
            })
        })
        .collect();
    let outbound: Vec<Value> = state
        .outbound
        .iter()
        .map(|(delivery, record)| {
            json!({
                "delivery_id": delivery_id(*delivery),
                "target_session": session(record.target),
                "target_revision": decimal(record.target_revision.get()),
                "packet_id": record.packet_id.get(),
                "claim_operation_id": operation_id(record.claim),
                "phase": "AwaitPuback",
                "emission": "MayHaveStarted",
            })
        })
        .collect();
    let limits = state.limits;
    let used = state.accounts;
    let mut document = json!({
        "format": "mqttiumd.store-snapshot",
        "schema_version": 1,
        "run_id": context.run_id,
        "checkpoint": context.checkpoint,
        "backend": context.backend,
        "meta": {
            "incarnation": decimal(state.ledger.incarnation().get()),
            "last_opened_epoch": decimal(state.ledger.last_opened().get()),
            "opened_by": opened_by(state.ledger.opened_by()),
            "collected_through": decimal(state.collected_through.get()),
            "state_revision": decimal(state.revision),
            "next_session": session(state.next_session),
        },
        "sessions": sessions,
        "route_table": {
            "revision": decimal(state.route_revision.get()),
            "routes": routes,
        },
        "operations": operations,
        "receipt_frontiers": state.receipt_frontiers.values().copied()
            .map(receipt_closure).collect::<Vec<_>>(),
        "fences": fences,
        "payloads": payloads,
        "publications": publications,
        "retained":state.retained.iter().map(|(topic,record)|json!({"topic_hex":hex(topic),"publication_id":publication_id(record.publication),"producer_client_id_hex":hex(&record.producer_client_id)})).collect::<Vec<_>>(),
        "obligations": obligations,
        "inbound_exchanges": inbound,
        "outbound": outbound,
        "account": {
            "used": {
                "payload_bytes": decimal(used.payload_bytes.get()),
                "references": decimal(used.references.get()),
                "exchange_slots": decimal(used.exchange_slots.get()),
                "receipt_slots": decimal(used.receipt_slots.get()),
            },
            "limits": {
                "payload_bytes": decimal(limits.payload_bytes.get()),
                "references": decimal(limits.references.get()),
                "exchange_slots": decimal(limits.exchange_slots.get()),
                "receipt_slots": decimal(limits.receipt_slots.get()),
                "max_targets_per_admission": usize_decimal(limits.max_targets_per_admission),
                "max_topic_bytes": usize_decimal(limits.max_topic_bytes),
                "max_payload_bytes": usize_decimal(limits.max_payload_bytes),
                "max_sessions": usize_decimal(limits.max_sessions),
                "max_route_targets": usize_decimal(limits.max_route_targets),
            },
        },
    });
    if let Some(anchor) = state.session_clock {
        document["meta"]["session_clock"] = json!({
            "reading": {
                "domain": hex(&anchor.reading.domain),
                "tick": decimal(anchor.reading.tick),
            },
            "logical": decimal(anchor.logical),
        });
    }
    document
}

/// One recovered fact as a `RecoveryPageRead` record writes it. The
/// record of the core that delivers a page (`RecoveryPageDelivered`)
/// writes its facts with this same function.
#[must_use]
pub fn recovered_fact(fact: &RecoveredFact) -> Value {
    match fact {
        RecoveredFact::ReceiptFrontier(closure) => {
            let mut fact = receipt_closure(*closure);
            fact["kind"] = json!("ReceiptFrontier");
            fact
        }
        RecoveredFact::Epoch { incarnation, epoch } => json!({
            "kind": "Epoch",
            "incarnation": decimal(incarnation.get()),
            "epoch": decimal(epoch.get()),
        }),
        RecoveredFact::Limits {
            max_targets_per_admission,
            max_topic_bytes,
            max_payload_bytes,
        } => json!({
            "kind": "Limits",
            "max_targets_per_admission": decimal(u64::from(*max_targets_per_admission)),
            "max_topic_bytes": decimal(u64::from(*max_topic_bytes)),
            "max_payload_bytes": decimal(max_payload_bytes.get()),
        }),
        RecoveredFact::RouteTable { revision } => json!({
            "kind": "RouteTable",
            "revision": decimal(revision.get()),
        }),
        RecoveredFact::Route {
            topic,
            target,
            target_revision,
            max_qos,
            options,
        } => {
            let mut row = json!({
                "kind": "Route",
                "topic_hex": hex(topic.as_slice()),
                "target": session(*target),
                "target_revision": decimal(target_revision.get()),
                "max_qos": qos(*max_qos),
            });
            // Preserve exact legacy trace bytes; zero is the announced default.
            if *options != SubscriptionOptions::DEFAULT {
                row["options"] = json!(options.bits());
            }
            row
        }
        RecoveredFact::Session {
            session: id,
            client_id,
            revision,
            expiry,
            opened_by,
            detached,
        } => {
            let mut row = json!({
                "kind": "Session",
                "session": session(*id),
                "client_id_hex": hex(client_id.as_slice()),
                "revision": decimal(revision.get()),
                "expiry": session_expiry(*expiry),
                "opened_by": opened_by.map(operation_id),
            });
            if let Some(detached) = detached {
                row["detached"] = detachment(*detached);
            }
            row
        }
        RecoveredFact::InboundExchange {
            exchange,
            packet_id,
            publication,
            phase,
        } => json!({
            "kind": "InboundExchange",
            "exchange": exchange_id(*exchange),
            "packet_id": packet_id.get(),
            "publication": publication_id(*publication),
            "phase": match phase {
                InboundPhase::AwaitPubrel => "AwaitPubrel",
            },
        }),
        RecoveredFact::OutboundDelivery {
            delivery,
            target,
            target_revision,
            qos,
            claim,
        } => json!({
            "kind": "OutboundDelivery",
            "delivery_id": delivery_id(*delivery),
            "target_session": session(*target),
            "target_revision": decimal(target_revision.get()),
            "qos": qos.to_wire(),
            "claim": claim.map(|claim| json!({
                "operation_id": operation_id(claim.operation),
                "packet_id": claim.packet_id.get(),
            })),
        }),
        RecoveredFact::Operation {
            operation,
            knowledge,
        } => json!({
            "kind": "Operation",
            "operation_id": operation_id(*operation),
            "knowledge": match knowledge {
                Knowledge::Unknown => "Unknown",
                Knowledge::Retired => "Retired",
                Knowledge::Committed(_) => "Committed",
                Knowledge::RejectedCertain(_) => "RejectedCertain",
            },
        }),
    }
}

/// The component a record is written for, and its position among the
/// records of that component.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Origin {
    component: &'static str,
    sequence: u64,
}

/// A record of another component that caused a submission: the record
/// of the core that consumed the input the effect comes from. The bench
/// knows it; the adapter does not, and the writer never guesses it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cause {
    pub component: &'static str,
    pub incarnation: u64,
    pub sequence: u64,
}

/// Why `TraceWriter::write_caused` wrote nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CauseRefused {
    /// Causes are given only for a submission: `CommandSubmitted`,
    /// `ResolveSubmitted` or `OpeningSubmitted`.
    NotASubmission,
}

/// Writes the evidence of one adapter as trace records. The collector
/// of lot L4 owns the trace; this writer gives it the records of the
/// store adapter, the store evidence, the fault injector and the
/// dispatcher, numbered from `first_record` and in the order the adapter
/// recorded them.
#[derive(Clone)]
pub struct TraceWriter<'a> {
    run_id: &'a str,
    origin_incarnation: u64,
    next_record: u64,
    /// The scenario's name of each armed fault, by `FaultId`.
    fault_names: &'a [&'a str],
    /// The next sequence of each component.
    sequences: BTreeMap<&'static str, u64>,
    /// The record that took each piece of work in, for its parents, and
    /// the kind of command it was submitted as.
    submitted: BTreeMap<WorkId, (Origin, &'static str)>,
    /// The actual stable write of work that reached CommitApplied. An
    /// applied FaultHit must name that write across the two origins.
    stable: BTreeMap<WorkId, Origin>,
    /// The record of each fault that was reached.
    hits: BTreeMap<FaultId, Origin>,
    /// The `StoreResultReady` of each piece of work, the parent of its
    /// handover.
    ready: BTreeMap<WorkId, Origin>,
    /// The causes the caller gave for the submission being written.
    causes: Vec<Cause>,
}

/// What a writer produced from a list of events.
#[derive(Debug, Default)]
pub struct Written {
    pub records: Vec<Value>,
    /// The events no record of the formats can carry, in their order.
    pub unwritten: Vec<StoreEvent>,
}

fn kind_name(kind: WorkKind) -> &'static str {
    kind.name()
}

/// The outcome of a store result as a record carries it: `outcome`, the
/// `reject_reason` of a certain rejection, the `violation` of a
/// violation, and nothing else.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OutcomeFields {
    pub outcome: &'static str,
    pub reject_reason: Option<&'static str>,
    pub violation: Option<&'static str>,
}

impl OutcomeFields {
    /// Writes the fields into the payload of a record.
    pub fn write_into(&self, payload: &mut Value) {
        payload["outcome"] = json!(self.outcome);
        if let Some(reason) = self.reject_reason {
            payload["reject_reason"] = json!(reason);
        }
        if let Some(violation) = self.violation {
            payload["violation"] = json!(violation);
        }
    }
}

/// The outcome of `reply` as `StoreResultReady` and
/// `StoreResultHandedOver` write it; the record of the core that consumes
/// the result (`StoreResultDelivered`) is to write it with this same
/// function.
#[must_use]
pub fn outcome_fields(reply: StoreReply) -> OutcomeFields {
    let plain = |outcome| OutcomeFields {
        outcome,
        reject_reason: None,
        violation: None,
    };
    match reply {
        StoreReply::Outcome(StoreOutcome::Committed(_)) => plain("Committed"),
        StoreReply::Outcome(StoreOutcome::RejectedCertain(rejected)) => OutcomeFields {
            reject_reason: Some(reason(rejected)),
            ..plain("RejectedCertain")
        },
        StoreReply::Outcome(StoreOutcome::PendingOrUnknown) => plain("PendingOrUnknown"),
        StoreReply::Outcome(StoreOutcome::RetiredOutcomeUnavailable) => {
            plain("RetiredOutcomeUnavailable")
        }
        StoreReply::Violation(StoreViolation::RecipeMismatch) => OutcomeFields {
            violation: Some("RecipeMismatch"),
            ..plain("Violation")
        },
    }
}

impl<'a> TraceWriter<'a> {
    /// A writer whose first record is numbered `first_record`, for the
    /// process incarnation `origin_incarnation` of run `run_id`.
    #[must_use]
    pub fn new(
        run_id: &'a str,
        origin_incarnation: u64,
        first_record: u64,
        fault_names: &'a [&'a str],
    ) -> Self {
        Self {
            run_id,
            origin_incarnation,
            next_record: first_record,
            fault_names,
            sequences: BTreeMap::new(),
            submitted: BTreeMap::new(),
            stable: BTreeMap::new(),
            hits: BTreeMap::new(),
            ready: BTreeMap::new(),
            causes: Vec::new(),
        }
    }

    fn record(
        &mut self,
        component: &'static str,
        kind: &str,
        parents: &[Origin],
        payload: Value,
    ) -> (Value, Origin) {
        let sequence = self.sequences.entry(component).or_insert(0);
        let origin = Origin {
            component,
            sequence: *sequence,
        };
        *sequence = sequence.saturating_add(1);
        let record_sequence = self.next_record;
        self.next_record = record_sequence.saturating_add(1);
        // The causes the caller gave come first, then the writer's own.
        let given = core::mem::take(&mut self.causes);
        let parents: Vec<Value> = given
            .iter()
            .map(|cause| {
                json!({
                    "origin_component": cause.component,
                    "origin_incarnation": decimal(cause.incarnation),
                    "origin_sequence": decimal(cause.sequence),
                })
            })
            .chain(parents.iter().map(|parent| {
                json!({
                    "origin_component": parent.component,
                    "origin_incarnation": decimal(self.origin_incarnation),
                    "origin_sequence": decimal(parent.sequence),
                })
            }))
            .collect();
        let value = json!({
            "format": "mqttiumd.trace-record",
            "schema_version": 1,
            "run_id": self.run_id,
            "record_sequence": decimal(record_sequence),
            "origin_component": component,
            "origin_incarnation": decimal(self.origin_incarnation),
            "origin_sequence": decimal(origin.sequence),
            "causal_parents": parents,
            "time_domain": "logical-step",
            "time_value": decimal(record_sequence),
            "kind": kind,
            "payload": payload,
        });
        (value, origin)
    }

    fn parent_of(&self, id: WorkId) -> Vec<Origin> {
        self.submitted
            .get(&id)
            .map(|(origin, _)| *origin)
            .into_iter()
            .collect()
    }

    /// Writes one submission with the causes the caller gives, in their
    /// order, as its causal parents. The event is written as `write`
    /// writes it; `None` when no record of the formats carries it.
    pub fn write_caused(
        &mut self,
        event: &StoreEvent,
        causes: &[Cause],
    ) -> Result<Option<Value>, CauseRefused> {
        let submission = matches!(event, StoreEvent::CommandSubmitted { .. });
        if !submission && !causes.is_empty() {
            return Err(CauseRefused::NotASubmission);
        }
        self.causes = causes.to_vec();
        let record = self.one(event);
        self.causes.clear();
        Ok(record)
    }

    /// Writes `events`, in order.
    pub fn write(&mut self, events: &[StoreEvent]) -> Written {
        let mut written = Written::default();
        for event in events {
            match self.one(event) {
                Some(record) => written.records.push(record),
                None => written.unwritten.push(event.clone()),
            }
        }
        written
    }

    fn one(&mut self, event: &StoreEvent) -> Option<Value> {
        match event {
            StoreEvent::CommandSubmitted {
                id: id @ WorkId::Opening(_),
                kind: WorkKind::OpenEpoch,
                opening: Some(command),
                ..
            } => {
                let (record, origin) = self.record(
                    "store-adapter",
                    "OpeningSubmitted",
                    &[],
                    json!({
                        "attempt": opened_by(Some(command.attempt)),
                        "incarnation": decimal(command.incarnation.get()),
                        "after": decimal(command.after.get()),
                    }),
                );
                self.submitted
                    .insert(*id, (origin, kind_name(WorkKind::OpenEpoch)));
                Some(record)
            }
            StoreEvent::CommitStable {
                id: id @ WorkId::Opening(attempt),
                opened: Some(opened),
                ..
            } => {
                let parents = self.parent_of(*id);
                Some(
                    self.record(
                        "store-evidence",
                        "StoreEpochOpened",
                        &parents,
                        json!({
                            "incarnation": decimal(opened.incarnation.get()),
                            "epoch": decimal(opened.epoch.get()),
                            "attempt": opened_by(Some(*attempt)),
                        }),
                    )
                    .0,
                )
            }
            StoreEvent::CommandSubmitted {
                id: id @ WorkId::Effect(effect),
                kind,
                operation: Some(operation),
                ..
            } => {
                let (record, origin) = match kind {
                    WorkKind::ResolveAndFence => self.record(
                        "store-adapter",
                        "ResolveSubmitted",
                        &[],
                        json!({
                            "effect_id": effect_id(*effect),
                            "operation_id": operation_id(*operation),
                        }),
                    ),
                    WorkKind::RetainedPublication
                    | WorkKind::RetainedQos0
                    | WorkKind::ReplaySubscriptions
                    | WorkKind::AdmitPublication
                    | WorkKind::ReleaseInbound
                    | WorkKind::ChangeSubscriptions
                    | WorkKind::ClaimDelivery
                    | WorkKind::FinishDelivery
                    | WorkKind::ConsumeDelivery
                    | WorkKind::UpdateSubscriptions
                    | WorkKind::SessionLifecycle => self.record(
                        "store-adapter",
                        "CommandSubmitted",
                        &[],
                        json!({
                            "effect_id": effect_id(*effect),
                            "operation_id": operation_id(*operation),
                            "command_kind": kind_name(*kind),
                        }),
                    ),
                    WorkKind::OpenEpoch => return None,
                };
                self.submitted.insert(*id, (origin, kind_name(*kind)));
                Some(record)
            }
            StoreEvent::CommitStable {
                id,
                operation: Some(operation),
                ..
            } => {
                // The kind is the one the work was submitted with; only
                // effects carry an operation.
                let parents = self.parent_of(*id);
                let (_, kind) = *self.submitted.get(id)?;
                let (record, origin) = self.record(
                    "store-evidence",
                    "StoreCommitStable",
                    &parents,
                    json!({
                        "operation_id": operation_id(*operation),
                        "command_kind": kind,
                    }),
                );
                self.stable.insert(*id, origin);
                Some(record)
            }
            StoreEvent::FenceStable { id, operation } => {
                let parents = self.parent_of(*id);
                Some(
                    self.record(
                        "store-evidence",
                        "StoreFenceStable",
                        &parents,
                        json!({"operation_id": operation_id(*operation)}),
                    )
                    .0,
                )
            }
            StoreEvent::FenceRefused {
                id: id @ WorkId::Effect(effect),
                operation,
            } => {
                let parents = self.parent_of(*id);
                Some(
                    self.record(
                        "store-evidence",
                        "StoreFenceRefused",
                        &parents,
                        json!({
                            "effect_id": effect_id(*effect),
                            "operation_id": operation_id(*operation),
                        }),
                    )
                    .0,
                )
            }
            StoreEvent::ResultReady {
                id: id @ WorkId::Effect(effect),
                answer: Answer::Operation { operation, reply },
            } => {
                let mut payload = json!({
                    "effect_id": effect_id(*effect),
                    "operation_id": operation_id(*operation),
                });
                outcome_fields(*reply).write_into(&mut payload);
                let parents = self.parent_of(*id);
                let (record, origin) =
                    self.record("store-evidence", "StoreResultReady", &parents, payload);
                self.ready.insert(*id, origin);
                Some(record)
            }
            StoreEvent::ResultDelivered {
                id: id @ WorkId::Effect(effect),
                answer: Answer::Operation { operation, reply },
                produced,
            } => {
                // What the adapter hands over has the content of what the
                // store produced; a result presented from outside follows
                // no `StoreResultReady`.
                let mut payload = json!({
                    "effect_id": effect_id(*effect),
                    "operation_id": operation_id(*operation),
                });
                outcome_fields(*reply).write_into(&mut payload);
                let parents: Vec<Origin> = if *produced {
                    self.ready.get(id).copied().into_iter().collect()
                } else {
                    Vec::new()
                };
                Some(
                    self.record("store-adapter", "StoreResultHandedOver", &parents, payload)
                        .0,
                )
            }
            StoreEvent::FaultHit {
                fault,
                point,
                id,
                operation,
                occurrence,
                ..
            } => {
                let name = *self.fault_names.get(fault.0)?;
                let mut payload = json!({
                    "fault_id": name,
                    "point": point.name(),
                    "occurrence": decimal(*occurrence),
                });
                if let Some(operation) = operation {
                    payload["operation_id"] = operation_id(*operation);
                }
                if let WorkId::Effect(effect) = id {
                    payload["effect_id"] = effect_id(*effect);
                }
                let mut parents = self.parent_of(*id);
                if *point == FaultPoint::CommitApplied
                    && let Some(stable) = self.stable.get(id)
                {
                    parents.push(*stable);
                }
                let (record, origin) = self.record("fault-injector", "FaultHit", &parents, payload);
                self.hits.insert(*fault, origin);
                Some(record)
            }
            StoreEvent::HeldItemReleased { fault } => {
                let name = *self.fault_names.get(fault.0)?;
                let parents: Vec<Origin> = self.hits.get(fault).copied().into_iter().collect();
                Some(
                    self.record(
                        "fault-injector",
                        "HeldItemReleased",
                        &parents,
                        json!({"fault_id": name}),
                    )
                    .0,
                )
            }
            StoreEvent::UncorrelatedResult {
                id: WorkId::Effect(effect),
                operation: Some(operation),
                reason,
            } => Some(
                self.record(
                    "dispatcher",
                    "InputRefused",
                    &[],
                    json!({
                        "input": "StoreResult",
                        "effect_id": effect_id(*effect),
                        "operation_id": operation_id(*operation),
                        "reason": match reason {
                            Uncorrelated::NoWorkInFlight => "no-effect-in-flight",
                            Uncorrelated::OperationMismatch => "operation-mismatch",
                        },
                    }),
                )
                .0,
            ),
            StoreEvent::RecoveryPageRead {
                run_epoch,
                page,
                last,
                facts,
            } => {
                let facts: Vec<Value> = facts.iter().map(recovered_fact).collect();
                Some(
                    self.record(
                        "store-adapter",
                        "RecoveryPageRead",
                        &[],
                        json!({
                            "run_epoch": decimal(run_epoch.get()),
                            "page": decimal(*page),
                            "last": last,
                            "facts": facts,
                        }),
                    )
                    .0,
                )
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod expiry_tests {
    use super::*;
    use mqttiumd_contract::ids::{
        LogicalOwnerId, OperationSeq, Revision, RunEpoch, StoreIncarnation,
    };
    use mqttiumd_contract::packet::SharedBytes;

    fn operation() -> OperationId {
        OperationId::new(
            StoreIncarnation::new(1),
            RunEpoch::new(2),
            LogicalOwnerId::new(3),
            OperationSeq::new(4),
        )
    }

    #[test]
    fn finite_recovery_and_detach_results_keep_the_occurrence_and_exact_deadline() {
        let detached = SessionDetachment {
            operation: operation(),
            deadline: Some(u64::MAX),
        };
        let fact = RecoveredFact::Session {
            session: SessionId::new(5),
            client_id: SharedBytes::copy_from(b"client", 1024).unwrap(),
            revision: Revision::new(1),
            expiry: SessionExpiry::After(7),
            opened_by: Some(operation()),
            detached: Some(detached),
        };
        let row = recovered_fact(&fact);
        assert_eq!(row["expiry"], json!({"After": 7}));
        assert_eq!(row["detached"]["deadline"], json!("18446744073709551615"));
        assert_eq!(row["detached"]["operation"], operation_id(operation()));
        let result = facts(CommitFacts::SessionLifecycle(SessionChange::Detached {
            session: SessionId::new(5),
            expiry: SessionExpiry::After(7),
            detached,
        }));
        assert_eq!(result["change"]["kind"], "Detached");
        assert_eq!(result["change"]["detached"], row["detached"]);
        assert_eq!(result["change"]["expiry"], row["expiry"]);
    }

    #[test]
    fn legacy_recovery_keeps_its_shape_without_inventing_a_detachment() {
        let fact = RecoveredFact::Session {
            session: SessionId::new(5),
            client_id: SharedBytes::copy_from(b"client", 1024).unwrap(),
            revision: Revision::new(1),
            expiry: SessionExpiry::Never,
            opened_by: None,
            detached: None,
        };
        let row = recovered_fact(&fact);
        assert_eq!(row["expiry"], "Never");
        assert!(row.get("detached").is_none());
        assert_eq!(session_expiry(SessionExpiry::OnDisconnect), "OnDisconnect");
    }
}
