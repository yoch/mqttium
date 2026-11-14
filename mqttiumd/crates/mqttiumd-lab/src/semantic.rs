//! Semantic validation: what a JSON Schema cannot state (DEM-014, DEM-015).
//!
//! It runs only on documents that already satisfy their schema, and checks
//! numeric bounds, references between documents and catalogues, causal
//! order of steps, and the coverage of a transition registry.

use std::collections::{BTreeMap, BTreeSet};

use mqttiumd_contract::decimal::{DecimalError, parse_u64};
use serde_json::Value;

use crate::diagnostic::{Diagnostic, Report, code};
use crate::workspace::Workspace;

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn items<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn names<'a>(value: &'a Value, key: &str) -> Vec<&'a str> {
    items(value, key).iter().filter_map(Value::as_str).collect()
}

/// Parses the decimal string at `pointer`, if the member is present.
fn u64_at(document: &Value, pointer: &str, out: &mut Vec<Diagnostic>) -> Option<u64> {
    let raw = document.pointer(pointer)?.as_str()?;
    match parse_u64(raw) {
        Ok(value) => Some(value),
        Err(DecimalError::Overflow | DecimalError::TooLong) => {
            out.push(Diagnostic::new(
                code::U64_OUT_OF_RANGE,
                pointer,
                format!("{raw:?} exceeds 18446744073709551615"),
            ));
            None
        }
        Err(error) => {
            out.push(Diagnostic::new(
                code::U64_NOT_CANONICAL,
                pointer,
                format!("{raw:?} is not a canonical decimal string ({error:?})"),
            ));
            None
        }
    }
}

/// Reports every name that appears twice in `names`.
fn unique(names: &[(String, &str)], what: &str, code: &'static str, out: &mut Vec<Diagnostic>) {
    let mut seen = BTreeSet::new();
    for (pointer, name) in names {
        if !seen.insert(*name) {
            out.push(Diagnostic::new(
                code,
                pointer.clone(),
                format!("{what} {name:?} is defined twice"),
            ));
        }
    }
}

fn requirements_known(
    workspace: &Workspace,
    holder: &Value,
    base: &str,
    keys: &[&str],
    out: &mut Vec<Diagnostic>,
) {
    for key in keys {
        for (index, id) in names(holder, key).into_iter().enumerate() {
            if !workspace.catalogs().requirements.contains(id) {
                out.push(Diagnostic::new(
                    code::REQUIREMENT_UNKNOWN,
                    format!("{base}/{key}/{index}"),
                    format!("{id:?} is not in the requirement register"),
                ));
            }
        }
    }
}

/// Runs the semantic pass that matches the format of `document`.
pub fn check(workspace: &Workspace, document: &Value, report: &mut Report) {
    match text(document, "format") {
        "mqttiumd.scenario" => scenario(workspace, document, &mut report.diagnostics),
        "mqttiumd.fixture" => fixture(workspace, document, &mut report.diagnostics),
        "mqttiumd.transition-registry" => registry(workspace, document, report),
        "mqttiumd.transition-catalog" => transition_catalog(document, &mut report.diagnostics),
        "mqttiumd.run-result" => run_result(document, &mut report.diagnostics),
        "mqttiumd.run-manifest" => run_manifest(document, &mut report.diagnostics),
        "mqttiumd.trace-record" => trace_record(document, &mut report.diagnostics),
        "mqttiumd.requirement-register" => {
            let entries = entry_names(document, "entries", "id");
            unique(
                &entries,
                "requirement",
                code::DUPLICATE_NAME,
                &mut report.diagnostics,
            );
        }
        "mqttiumd.assertion-catalog" => {
            let entries = entry_names(document, "assertions", "name");
            unique(
                &entries,
                "assertion",
                code::DUPLICATE_NAME,
                &mut report.diagnostics,
            );
            for (index, entry) in items(document, "assertions").iter().enumerate() {
                requirements_known(
                    workspace,
                    entry,
                    &format!("/assertions/{index}"),
                    &["requirements"],
                    &mut report.diagnostics,
                );
            }
        }
        "mqttiumd.fault-catalog" => {
            let entries = entry_names(document, "points", "point");
            unique(
                &entries,
                "injection point",
                code::DUPLICATE_NAME,
                &mut report.diagnostics,
            );
        }
        "mqttiumd.profile-catalog" => {
            let entries = entry_names(document, "profiles", "id");
            unique(
                &entries,
                "profile",
                code::DUPLICATE_NAME,
                &mut report.diagnostics,
            );
        }
        _ => {}
    }
}

fn entry_names<'a>(document: &'a Value, list: &str, key: &str) -> Vec<(String, &'a str)> {
    items(document, list)
        .iter()
        .enumerate()
        .map(|(index, entry)| (format!("/{list}/{index}/{key}"), text(entry, key)))
        .collect()
}

// ------------------------------------------------------------------ scenario

fn scenario(workspace: &Workspace, document: &Value, out: &mut Vec<Diagnostic>) {
    let catalogs = workspace.catalogs();
    let budget = u64_at(document, "/budgets/max_steps", out);
    u64_at(document, "/budgets/max_trace_bytes", out);

    let profile = text(document, "profile");
    if !catalogs.profiles.contains(profile) {
        out.push(Diagnostic::new(
            code::PROFILE_UNKNOWN,
            "/profile",
            format!("{profile:?} is not in the profile catalogue"),
        ));
    }

    let clients: Option<BTreeSet<String>> = match workspace.load_reference(
        text(document, "initial_fixture"),
        "/initial_fixture",
        "mqttiumd.fixture",
    ) {
        Ok(fixture) => Some(
            items(&fixture, "sessions")
                .iter()
                .map(|session| text(session, "client").to_owned())
                .collect(),
        ),
        Err(diagnostic) => {
            out.push(diagnostic);
            None
        }
    };

    let faults = items(document, "faults");
    let steps = items(document, "steps");

    // One namespace for everything a scenario binds.
    let mut definitions: Vec<(String, &str)> = Vec::new();
    let mut fault_ids = BTreeSet::new();
    for (index, fault) in faults.iter().enumerate() {
        let base = format!("/faults/{index}");
        u64_at(document, &format!("{base}/occurrence"), out);
        u64_at(document, &format!("{base}/expected_hits"), out);
        definitions.push((format!("{base}/id"), text(fault, "id")));
        fault_ids.insert(text(fault, "id"));
        if let Some(binding) = fault.get("bind_operation_as").and_then(Value::as_str) {
            definitions.push((format!("{base}/bind_operation_as"), binding));
        }
        let point = text(fault, "point");
        match catalogs.fault_points.get(point) {
            None => out.push(Diagnostic::new(
                code::FAULT_POINT_UNKNOWN,
                format!("{base}/point"),
                format!("{point:?} is not a contractual injection point"),
            )),
            Some(actions) => {
                let action = text(fault, "action");
                if !actions.contains(action) {
                    out.push(Diagnostic::new(
                        code::FAULT_ACTION_UNKNOWN,
                        format!("{base}/action"),
                        format!("{point:?} does not offer the action {action:?}"),
                    ));
                }
            }
        }
        let kind = fault.pointer("/match/command_kind").and_then(Value::as_str);
        if let Some(kind) = kind.filter(|kind| !catalogs.command_kinds.contains(*kind)) {
            out.push(Diagnostic::new(
                code::COMMAND_KIND_UNKNOWN,
                format!("{base}/match/command_kind"),
                format!("{kind:?} is not a command kind"),
            ));
        }
    }
    let exchanges: BTreeSet<&str> = steps
        .iter()
        .filter_map(|step| step.get("bind_exchange_as").and_then(Value::as_str))
        .collect();
    for (index, fault) in faults.iter().enumerate() {
        let binding = fault
            .pointer("/match/publication_binding")
            .and_then(Value::as_str);
        if let Some(binding) = binding.filter(|binding| !exchanges.contains(binding)) {
            out.push(Diagnostic::new(
                code::BINDING_UNDEFINED,
                format!("/faults/{index}/match/publication_binding"),
                format!("no step binds an exchange as {binding:?}"),
            ));
        }
    }

    let mut bound_so_far = BTreeSet::new();
    let mut checkpoints: Vec<(String, &str)> = Vec::new();
    let mut broker_up = true;
    for (index, step) in steps.iter().enumerate() {
        let base = format!("/steps/{index}");
        let kind = text(step, "kind");

        let order_error = match (kind, broker_up) {
            ("RestartBroker", true) => Some("the broker is already running"),
            ("RestartBroker", false) => None,
            (_, false) => Some("the broker is crashed; only RestartBroker may follow"),
            _ => None,
        };
        if let Some(reason) = order_error {
            out.push(Diagnostic::new(
                code::STEP_ORDER_INVALID,
                base.clone(),
                reason,
            ));
        }
        match kind {
            "CrashBroker" => broker_up = false,
            "RestartBroker" => broker_up = true,
            _ => {}
        }

        if let Some(limit) = u64_at(document, &format!("{base}/max_steps"), out)
            && budget.is_some_and(|budget| limit > budget)
        {
            out.push(Diagnostic::new(
                code::BUDGET_EXCEEDED,
                format!("{base}/max_steps"),
                format!("{limit} steps exceed the scenario budget"),
            ));
        }
        if let (Some(client), Some(clients)) =
            (step.get("client").and_then(Value::as_str), clients.as_ref())
            && !clients.contains(client)
        {
            out.push(Diagnostic::new(
                code::CLIENT_UNKNOWN,
                format!("{base}/client"),
                format!("{client:?} is not a session of the fixture"),
            ));
        }
        if let Some(binding) = step.get("bind_exchange_as").and_then(Value::as_str) {
            definitions.push((format!("{base}/bind_exchange_as"), binding));
            bound_so_far.insert(binding);
        }
        if let Some(exchange) = step.get("exchange").and_then(Value::as_str)
            && !bound_so_far.contains(exchange)
        {
            out.push(Diagnostic::new(
                code::BINDING_UNDEFINED,
                format!("{base}/exchange"),
                format!("no earlier step binds an exchange as {exchange:?}"),
            ));
        }
        if kind == "DriveUntil"
            && let Some(binding) = step.get("binding").and_then(Value::as_str)
            && !fault_ids.contains(binding)
        {
            out.push(Diagnostic::new(
                code::BINDING_UNDEFINED,
                format!("{base}/binding"),
                format!("no fault is declared as {binding:?}"),
            ));
        }
        if let Some(checkpoint) = step.get("checkpoint").and_then(Value::as_str) {
            checkpoints.push((format!("{base}/checkpoint"), checkpoint));
        }
    }
    unique(&definitions, "binding", code::BINDING_DUPLICATE, out);
    unique(&checkpoints, "checkpoint", code::BINDING_DUPLICATE, out);

    for (index, assertion) in names(document, "assertions").into_iter().enumerate() {
        if !catalogs.assertions.contains(assertion) {
            out.push(Diagnostic::new(
                code::ASSERTION_UNKNOWN,
                format!("/assertions/{index}"),
                format!("{assertion:?} is not in the assertion catalogue"),
            ));
        }
    }
}

// ------------------------------------------------------------------- fixture

fn fixture(workspace: &Workspace, document: &Value, out: &mut Vec<Diagnostic>) {
    u64_at(document, "/store/incarnation", out);
    if let Some(capacities) = document.get("capacities").and_then(Value::as_object) {
        for name in capacities.keys() {
            u64_at(document, &format!("/capacities/{name}"), out);
        }
    }
    let profile = text(document, "profile");
    if !workspace.catalogs().profiles.contains(profile) {
        out.push(Diagnostic::new(
            code::PROFILE_UNKNOWN,
            "/profile",
            format!("{profile:?} is not in the profile catalogue"),
        ));
    }
    unique(
        &entry_names(document, "sessions", "client"),
        "client",
        code::DUPLICATE_NAME,
        out,
    );
    unique(
        &entry_names(document, "sessions", "client_id"),
        "ClientID",
        code::DUPLICATE_NAME,
        out,
    );
    unique(
        &entry_names(document, "routes", "topic"),
        "route",
        code::DUPLICATE_NAME,
        out,
    );
    let clients: BTreeSet<&str> = items(document, "sessions")
        .iter()
        .map(|session| text(session, "client"))
        .collect();
    for (route_index, route) in items(document, "routes").iter().enumerate() {
        for (target_index, target) in items(route, "targets").iter().enumerate() {
            let client = text(target, "client");
            if !clients.contains(client) {
                out.push(Diagnostic::new(
                    code::CLIENT_UNKNOWN,
                    format!("/routes/{route_index}/targets/{target_index}/client"),
                    format!("{client:?} is not a session of this fixture"),
                ));
            }
        }
    }
}

// ------------------------------------------------------- transition catalog

fn transition_catalog(document: &Value, out: &mut Vec<Diagnostic>) {
    unique(
        &entry_names(document, "guard_dimensions", "name"),
        "guard dimension",
        code::DUPLICATE_NAME,
        out,
    );
    unique(
        &entry_names(document, "event_classes", "name"),
        "event class",
        code::DUPLICATE_NAME,
        out,
    );
    unique(
        &entry_names(document, "machines", "name"),
        "machine",
        code::DUPLICATE_NAME,
        out,
    );
    let dimensions: BTreeSet<&str> = items(document, "guard_dimensions")
        .iter()
        .map(|dimension| text(dimension, "name"))
        .collect();
    for (index, dimension) in items(document, "guard_dimensions").iter().enumerate() {
        let values: Vec<(String, &str)> = items(dimension, "values")
            .iter()
            .enumerate()
            .map(|(value_index, value)| {
                (
                    format!("/guard_dimensions/{index}/values/{value_index}/value"),
                    text(value, "value"),
                )
            })
            .collect();
        unique(&values, "guard value", code::DUPLICATE_NAME, out);
    }
    let events: BTreeSet<&str> = items(document, "event_classes")
        .iter()
        .map(|event| text(event, "name"))
        .collect();
    for (index, event) in items(document, "event_classes").iter().enumerate() {
        for (dimension_index, dimension) in names(event, "guard_dimensions").into_iter().enumerate()
        {
            if !dimensions.contains(dimension) {
                out.push(Diagnostic::new(
                    code::CATALOG_REFERENCE_UNKNOWN,
                    format!("/event_classes/{index}/guard_dimensions/{dimension_index}"),
                    format!("{dimension:?} is not a guard dimension"),
                ));
            }
        }
    }
    for (index, machine) in items(document, "machines").iter().enumerate() {
        for (event_index, event) in names(machine, "event_classes").into_iter().enumerate() {
            if !events.contains(event) {
                out.push(Diagnostic::new(
                    code::CATALOG_REFERENCE_UNKNOWN,
                    format!("/machines/{index}/event_classes/{event_index}"),
                    format!("{event:?} is not an event class"),
                ));
            }
        }
    }
}

// ------------------------------------------------------- transition registry

struct Vocabulary<'a> {
    dimensions: BTreeMap<&'a str, Vec<&'a str>>,
    events: BTreeMap<&'a str, Vec<&'a str>>,
    effects: BTreeSet<&'a str>,
    machines: BTreeMap<&'a str, (Vec<&'a str>, Vec<&'a str>)>,
}

impl<'a> Vocabulary<'a> {
    fn read(catalog: &'a Value) -> Self {
        Self {
            dimensions: items(catalog, "guard_dimensions")
                .iter()
                .map(|dimension| {
                    let values = items(dimension, "values")
                        .iter()
                        .map(|value| text(value, "value"))
                        .collect();
                    (text(dimension, "name"), values)
                })
                .collect(),
            events: items(catalog, "event_classes")
                .iter()
                .map(|event| (text(event, "name"), names(event, "guard_dimensions")))
                .collect(),
            effects: names(catalog, "effects").into_iter().collect(),
            machines: items(catalog, "machines")
                .iter()
                .map(|machine| {
                    (
                        text(machine, "name"),
                        (
                            names(machine, "state_classes"),
                            names(machine, "event_classes"),
                        ),
                    )
                })
                .collect(),
        }
    }

    /// Every point of the guard domain of `event`: one value per dimension.
    fn guard_points(&self, event: &str) -> Vec<Vec<(&'a str, &'a str)>> {
        let mut points = vec![Vec::new()];
        for dimension in self.events.get(event).map_or(&[][..], Vec::as_slice) {
            let values = self
                .dimensions
                .get(dimension)
                .map_or(&[][..], Vec::as_slice);
            points = points
                .into_iter()
                .flat_map(|point| {
                    values.iter().map(move |value| {
                        let mut extended = point.clone();
                        extended.push((*dimension, *value));
                        extended
                    })
                })
                .collect();
        }
        points
    }
}

/// Guard points listed per uncovered cell; the count is always exact.
const MAX_HOLES_SHOWN: usize = 3;

/// Whether `rule` covers `point`. A dimension the rule does not name is
/// covered entirely.
fn rule_covers(rule: &Value, point: &[(&str, &str)]) -> bool {
    point.iter().all(|(dimension, value)| {
        match rule.pointer(&format!("/guard_partition/{dimension}")) {
            None => true,
            Some(values) => values
                .as_array()
                .is_some_and(|values| values.iter().any(|item| item.as_str() == Some(*value))),
        }
    })
}

fn describe(point: &[(&str, &str)]) -> String {
    let parts: Vec<String> = point
        .iter()
        .map(|(dimension, value)| format!("{dimension}={value}"))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

fn registry(workspace: &Workspace, document: &Value, report: &mut Report) {
    let catalog = match workspace.load_reference(
        text(document, "catalog"),
        "/catalog",
        "mqttiumd.transition-catalog",
    ) {
        Ok(catalog) => catalog,
        Err(diagnostic) => {
            report.diagnostics.push(diagnostic);
            return;
        }
    };
    let vocabulary = Vocabulary::read(&catalog);
    let rules = items(document, "rules");
    let out = &mut report.diagnostics;
    unique(
        &entry_names(document, "rules", "id"),
        "rule",
        code::DUPLICATE_NAME,
        out,
    );

    let unknown = |pointer: String, what: &str, name: &str| {
        Diagnostic::new(
            code::CATALOG_REFERENCE_UNKNOWN,
            pointer,
            format!("{name:?} is not a known {what}"),
        )
    };
    let mut well_formed = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        let base = format!("/rules/{index}");
        let before = out.len();
        requirements_known(
            workspace,
            rule,
            &base,
            &["requirements", "witnesses", "mutations"],
            out,
        );
        let machine = text(rule, "machine");
        let Some((states, events)) = vocabulary.machines.get(machine) else {
            out.push(unknown(format!("{base}/machine"), "machine", machine));
            continue;
        };
        for key in ["state_class", "next_state_class"] {
            if !states.contains(&text(rule, key)) {
                out.push(unknown(
                    format!("{base}/{key}"),
                    "state class",
                    text(rule, key),
                ));
            }
        }
        let event = text(rule, "event_class");
        if !events.contains(&event) {
            out.push(unknown(format!("{base}/event_class"), "event class", event));
        }
        let allowed = vocabulary.events.get(event).map_or(&[][..], Vec::as_slice);
        if let Some(partition) = rule.get("guard_partition").and_then(Value::as_object) {
            for (dimension, values) in partition {
                if !allowed.contains(&dimension.as_str()) {
                    out.push(unknown(
                        format!("{base}/guard_partition/{dimension}"),
                        "guard dimension of this event class",
                        dimension,
                    ));
                    continue;
                }
                let known = vocabulary
                    .dimensions
                    .get(dimension.as_str())
                    .map_or(&[][..], Vec::as_slice);
                for (value_index, value) in values.as_array().into_iter().flatten().enumerate() {
                    let value = value.as_str().unwrap_or_default();
                    if !known.contains(&value) {
                        out.push(unknown(
                            format!("{base}/guard_partition/{dimension}/{value_index}"),
                            "guard value",
                            value,
                        ));
                    }
                }
            }
        }
        let required = names(rule, "required_effects");
        for key in ["required_effects", "forbidden_effects"] {
            for (effect_index, effect) in names(rule, key).into_iter().enumerate() {
                if !vocabulary.effects.contains(effect) {
                    out.push(unknown(
                        format!("{base}/{key}/{effect_index}"),
                        "effect",
                        effect,
                    ));
                } else if key == "forbidden_effects" && required.contains(&effect) {
                    out.push(Diagnostic::new(
                        code::REGISTRY_EFFECT_CONFLICT,
                        format!("{base}/{key}/{effect_index}"),
                        format!("{effect:?} is both required and forbidden"),
                    ));
                }
            }
        }
        if out.len() == before {
            well_formed.push((index, rule));
        }
    }

    // Coverage: each point of each cell is covered by exactly one rule.
    let complete = text(document, "status") == "complete";
    let mut uncovered_cells = 0usize;
    for (machine, (states, events)) in &vocabulary.machines {
        for state in states {
            for event in events {
                let cell: Vec<&(usize, &Value)> = well_formed
                    .iter()
                    .filter(|(_, rule)| {
                        text(rule, "machine") == *machine
                            && text(rule, "state_class") == *state
                            && text(rule, "event_class") == *event
                    })
                    .collect();
                let points = vocabulary.guard_points(event);
                let mut holes = Vec::new();
                for point in &points {
                    let covering: Vec<&&(usize, &Value)> = cell
                        .iter()
                        .filter(|(_, rule)| rule_covers(rule, point))
                        .collect();
                    match covering.as_slice() {
                        [] => holes.push(describe(point)),
                        [_] => {}
                        [first, second, ..] => report.diagnostics.push(Diagnostic::new(
                            code::REGISTRY_OVERLAP,
                            format!("/rules/{}", second.0),
                            format!(
                                "{:?} and {:?} both cover {machine} {state} x {event} {}",
                                text(first.1, "id"),
                                text(second.1, "id"),
                                describe(point)
                            ),
                        )),
                    }
                }
                if holes.is_empty() {
                    continue;
                }
                uncovered_cells += 1;
                let shown = holes.len().min(MAX_HOLES_SHOWN);
                let more = match holes.len() - shown {
                    0 => String::new(),
                    hidden => format!(" and {hidden} more"),
                };
                let summary = format!(
                    "{machine} {state} x {event}: {} of {} guard points uncovered: {}{more}",
                    holes.len(),
                    points.len(),
                    holes[..shown].join(" ")
                );
                if complete {
                    report.diagnostics.push(Diagnostic::new(
                        code::REGISTRY_HOLE,
                        "/rules",
                        summary,
                    ));
                } else {
                    report.notes.push(format!("registry hole: {summary}"));
                }
            }
        }
    }
    if !complete {
        report.notes.push(format!(
            "registry {:?} is a seed with {uncovered_cells} uncovered cells; it cannot support a PASS",
            text(document, "id")
        ));
    }
}

// ---------------------------------------------------------------- run result

fn run_result(document: &Value, out: &mut Vec<Diagnostic>) {
    let mut failed = false;
    let mut inconclusive = false;
    for assertion in items(document, "assertions") {
        match text(assertion, "status") {
            "FAIL" => failed = true,
            "INCONCLUSIVE" => inconclusive = true,
            _ => {}
        }
    }
    for index in 0..items(document, "faults").len() {
        let expected = u64_at(document, &format!("/faults/{index}/expected_hits"), out);
        let hits = u64_at(document, &format!("/faults/{index}/hits"), out);
        // An injection that was not reached as announced proves nothing.
        inconclusive |= expected != hits;
    }
    for index in 0..items(document, "artifacts").len() {
        u64_at(document, &format!("/artifacts/{index}/bytes"), out);
    }
    let verdict = text(document, "verdict");
    let floor = if failed {
        Some("FAIL")
    } else if inconclusive {
        Some("INCONCLUSIVE")
    } else {
        None
    };
    // An established FAIL stands; a mandatory inconclusive assertion blocks.
    let consistent = match floor {
        Some("FAIL") => verdict == "FAIL",
        Some(_) => verdict == "INCONCLUSIVE" || verdict == "FAIL",
        None => true,
    };
    if !consistent {
        out.push(Diagnostic::new(
            code::VERDICT_INCONSISTENT,
            "/verdict",
            format!(
                "{verdict} is better than the evidence allows ({})",
                floor.unwrap_or("none")
            ),
        ));
    }
}

// ------------------------------------------------------- manifest and trace

fn run_manifest(document: &Value, out: &mut Vec<Diagnostic>) {
    for index in 0..items(document, "contracts").len() {
        u64_at(document, &format!("/contracts/{index}/bytes"), out);
    }
    for pointer in [
        "/scenario/bytes",
        "/fixture/bytes",
        "/scheduler/version",
        "/scheduler/seed",
    ] {
        u64_at(document, pointer, out);
    }
}

fn trace_record(document: &Value, out: &mut Vec<Diagnostic>) {
    for pointer in [
        "/record_sequence",
        "/origin_incarnation",
        "/origin_sequence",
        "/time_value",
    ] {
        u64_at(document, pointer, out);
    }
    for index in 0..items(document, "causal_parents").len() {
        u64_at(
            document,
            &format!("/causal_parents/{index}/origin_incarnation"),
            out,
        );
        u64_at(
            document,
            &format!("/causal_parents/{index}/origin_sequence"),
            out,
        );
    }
    decimal_strings(document.get("payload"), "/payload", out);
}

/// Checks every decimal-looking identity component below `value`. Payload
/// members that hold 64-bit components are objects of decimal strings.
fn decimal_strings(value: Option<&Value>, pointer: &str, out: &mut Vec<Diagnostic>) {
    const COMPONENTS: [&str; 12] = [
        "incarnation",
        "epoch",
        "owner",
        "sequence",
        "session",
        "generation",
        "record_count",
        "bytes_confirmed",
        "bytes_total",
        "occurrence",
        "run_epoch",
        "time_value",
    ];
    let Some(Value::Object(members)) = value else {
        return;
    };
    for (key, member) in members {
        let child = format!("{pointer}/{key}");
        match member {
            Value::String(raw) if COMPONENTS.contains(&key.as_str()) => match parse_u64(raw) {
                Ok(_) => {}
                Err(DecimalError::Overflow | DecimalError::TooLong) => out.push(Diagnostic::new(
                    code::U64_OUT_OF_RANGE,
                    child,
                    format!("{raw:?} exceeds 18446744073709551615"),
                )),
                Err(error) => out.push(Diagnostic::new(
                    code::U64_NOT_CANONICAL,
                    child,
                    format!("{raw:?} is not a canonical decimal string ({error:?})"),
                )),
            },
            Value::Object(_) => decimal_strings(Some(member), &child, out),
            _ => {}
        }
    }
}
