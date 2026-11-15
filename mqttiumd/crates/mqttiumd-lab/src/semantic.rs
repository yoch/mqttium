//! Semantic validation: what a JSON Schema cannot state (DEM-014, DEM-015).
//!
//! It runs only on documents that already satisfy their schema, and checks
//! numeric bounds, references between documents and catalogues, causal
//! order of steps, and the coverage of a transition registry.

use std::collections::{BTreeMap, BTreeSet};

use mqttiumd_contract::decimal::{DecimalError, parse_u64};
use serde_json::Value;

use crate::diagnostic::{Diagnostic, MAX_DIAGNOSTICS, Report, code};
use crate::manifest::contracts_digest;
use crate::workspace::{
    REGISTRY_CATALOG, RESULT_MANIFEST, RESULT_SCENARIO, SCENARIO_FIXTURE, Workspace,
};

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
        "mqttiumd.run-result" => run_result(workspace, document, &mut report.diagnostics),
        "mqttiumd.run-manifest" => run_manifest(document, &mut report.diagnostics),
        "mqttiumd.trace-record" => trace_record(document, &mut report.diagnostics),
        "mqttiumd.store-snapshot" => store_snapshot(document, &mut report.diagnostics),
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
            // A point offers only actions that act on what it carries.
            for (index, point) in items(document, "points").iter().enumerate() {
                for (action_index, action) in names(point, "actions").into_iter().enumerate() {
                    let fits = match acts_on(action) {
                        None => false,
                        Some(subject) => subject.is_none_or(|item| item == text(point, "carries")),
                    };
                    if !fits {
                        report.diagnostics.push(Diagnostic::new(
                            code::FAULT_ACTION_UNKNOWN,
                            format!("/points/{index}/actions/{action_index}"),
                            format!(
                                "{action:?} is not an action on a {}",
                                text(point, "carries")
                            ),
                        ));
                    }
                }
            }
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

/// What an action of an injection point acts on: `Some(None)` for an action
/// that fits any point, `None` for a name outside the vocabulary.
fn acts_on(action: &str) -> Option<Option<&'static str>> {
    match action {
        "barrier" => Some(None),
        "hold_command" => Some(Some("command")),
        "drop_result" | "delay_result" | "duplicate_result" => Some(Some("result")),
        _ => None,
    }
}

/// What a fault keeps until a step hands it over, if its action holds
/// anything.
fn held_item(action: &str) -> Option<&'static str> {
    match action {
        "hold_command" => Some("command"),
        "delay_result" => Some("result"),
        _ => None,
    }
}

/// Whether a fault holds the command or result it stopped, as the steps
/// read so far establish it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Held {
    /// No step has driven the broker until the fault was hit.
    NotYet,
    /// The fault was hit and nothing took the item away.
    Yes,
    /// A step released or delivered it.
    Handed,
    /// The broker crashed while the fault held it.
    Destroyed,
}

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

    let clients: Option<BTreeSet<String>> =
        match workspace.load_reference(&SCENARIO_FIXTURE, text(document, "initial_fixture")) {
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
    // What each fault keeps once a step waited for its hit (a command, a
    // result, or nothing), whether it still keeps it, and whether the
    // scenario expects it to be reached at all.
    let mut holds: BTreeMap<&str, (Option<&str>, Held, bool)> = BTreeMap::new();
    for (index, fault) in faults.iter().enumerate() {
        let base = format!("/faults/{index}");
        u64_at(document, &format!("{base}/occurrence"), out);
        let expected = u64_at(document, &format!("{base}/expected_hits"), out);
        definitions.push((format!("{base}/id"), text(fault, "id")));
        fault_ids.insert(text(fault, "id"));
        let item = held_item(text(fault, "action"));
        holds
            .entry(text(fault, "id"))
            .or_insert((item, Held::NotYet, expected != Some(0)));
        if item.is_some() && expected.is_some_and(|hits| hits > 1) {
            out.push(Diagnostic::new(
                code::HELD_ITEM_MISMATCH,
                format!("{base}/expected_hits"),
                "a fault that holds something holds one item: it is reached at most once",
            ));
        }
        if fault.pointer("/match/result_of").is_some()
            && catalogs
                .fault_points
                .get(text(fault, "point"))
                .is_some_and(|point| point.carries != "result")
        {
            out.push(Diagnostic::new(
                code::FAULT_MATCH_INVALID,
                format!("{base}/match/result_of"),
                format!(
                    "{:?} carries a command; `result_of` selects among results",
                    text(fault, "point")
                ),
            ));
        }
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
            Some(offered) => {
                let action = text(fault, "action");
                if !offered.actions.contains(action) {
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
    // Two faults must not select the same command or result: which of them
    // acts would depend on the store. Selections of one point, one command
    // kind and compatible bindings overlap when they name the same
    // occurrence, or when only one of them names the effect, because the
    // two then count occurrences over different sets.
    let member = |fault: &'_ Value, pointer: &str| -> Option<String> {
        fault
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let compatible = |left: &Option<String>, right: &Option<String>| {
        left.is_none() || right.is_none() || left == right
    };
    for (index, fault) in faults.iter().enumerate() {
        for earlier in &faults[..index] {
            let same = |pointer: &str| member(fault, pointer) == member(earlier, pointer);
            let effects = (
                member(fault, "/match/result_of"),
                member(earlier, "/match/result_of"),
            );
            if same("/point")
                && same("/match/command_kind")
                && compatible(
                    &member(fault, "/match/publication_binding"),
                    &member(earlier, "/match/publication_binding"),
                )
                && compatible(&effects.0, &effects.1)
                && (same("/occurrence") || effects.0.is_some() != effects.1.is_some())
            {
                out.push(Diagnostic::new(
                    code::FAULT_MATCH_INVALID,
                    format!("/faults/{index}/match"),
                    format!(
                        "{:?} and {:?} can select the same item",
                        text(earlier, "id"),
                        text(fault, "id")
                    ),
                ));
            }
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
        // A step that is already refused where it stands is not judged
        // further: one misplaced step is one diagnostic.
        match kind {
            _ if order_error.is_some() => {}
            "CrashBroker" => {
                broker_up = false;
                // A held command or result lives in the broker process.
                for (_, state, _) in holds.values_mut() {
                    if *state == Held::Yes {
                        *state = Held::Destroyed;
                    }
                }
            }
            "RestartBroker" => broker_up = true,
            "DriveUntil" => {
                let binding = step.get("binding").and_then(Value::as_str);
                if let Some((item, state, expected)) =
                    binding.and_then(|binding| holds.get_mut(binding))
                {
                    if !*expected {
                        out.push(Diagnostic::new(
                            code::STEP_ORDER_INVALID,
                            base.clone(),
                            "the scenario waits for a fault it expects never to reach",
                        ));
                    } else if item.is_some() && *state != Held::NotYet {
                        out.push(Diagnostic::new(
                            code::STEP_ORDER_INVALID,
                            base.clone(),
                            "a fault that holds something is waited for once",
                        ));
                    } else if item.is_some() {
                        *state = Held::Yes;
                    }
                }
            }
            "ReleaseHeldCommand" | "DeliverHeldResult" => {
                let wanted = match kind {
                    "ReleaseHeldCommand" => "command",
                    _ => "result",
                };
                let name = text(step, "fault");
                let pointer = format!("{base}/fault");
                match holds.get_mut(name) {
                    None => out.push(Diagnostic::new(
                        code::BINDING_UNDEFINED,
                        pointer,
                        format!("no fault is declared as {name:?}"),
                    )),
                    Some((item, _, _)) if *item != Some(wanted) => out.push(Diagnostic::new(
                        code::HELD_ITEM_MISMATCH,
                        pointer,
                        format!("{name:?} holds no {wanted}"),
                    )),
                    Some((_, state, _)) => {
                        let reason = match *state {
                            Held::Yes => None,
                            Held::NotYet => Some("no earlier step drives until this fault is hit"),
                            Held::Handed => Some("an earlier step already handed it over"),
                            Held::Destroyed => Some("a crash destroyed what the fault held"),
                        };
                        match reason {
                            None => *state = Held::Handed,
                            Some(reason) => out.push(Diagnostic::new(
                                code::STEP_ORDER_INVALID,
                                base.clone(),
                                format!("the {wanted} of {name:?} cannot be handed over: {reason}"),
                            )),
                        }
                    }
                }
            }
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

    /// The guard dimensions of `event`, each with its values, in the order
    /// the catalogue lists them.
    fn guard_domain(&self, event: &str) -> Vec<(&'a str, &[&'a str])> {
        self.events
            .get(event)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .map(|dimension| {
                let values = self
                    .dimensions
                    .get(dimension)
                    .map_or(&[][..], Vec::as_slice);
                (*dimension, values)
            })
            .collect()
    }
}

/// Guard points listed per uncovered cell; the count is always exact.
const MAX_HOLES_SHOWN: usize = 3;

/// The most guard points one cell of a registry may have.
pub const MAX_POINTS_PER_CELL: u64 = 4096;

/// The most guard points a whole registry may have, over all its cells.
pub const MAX_POINTS_PER_REGISTRY: u64 = 262_144;

/// The number of points of a guard domain whose dimensions have `sizes`
/// values, or `None` when it does not fit 64 bits. Nothing is allocated.
fn domain_size(sizes: impl IntoIterator<Item = usize>) -> Option<u64> {
    sizes.into_iter().try_fold(1u64, |points, size| {
        points.checked_mul(u64::try_from(size).ok()?)
    })
}

/// Why a registry is too wide for its coverage to be checked.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BudgetRefusal {
    /// One cell has this many points, or more than 64 bits can count.
    Cell(Option<u64>),
    /// The cells together have this many points, or more than 64 bits can
    /// count.
    Registry(Option<u64>),
}

/// Checks the sizes of all cells against both budgets before any point is
/// enumerated, and returns the total number of points.
fn check_budget(
    cells: impl IntoIterator<Item = Option<u64>>,
    per_cell: u64,
    per_registry: u64,
) -> Result<u64, BudgetRefusal> {
    let mut total = 0u64;
    for size in cells {
        let size = size
            .filter(|size| *size <= per_cell)
            .ok_or(BudgetRefusal::Cell(size))?;
        total = total
            .checked_add(size)
            .ok_or(BudgetRefusal::Registry(None))?;
        if total > per_registry {
            return Err(BudgetRefusal::Registry(Some(total)));
        }
    }
    Ok(total)
}

/// Visits every point of a guard domain, one index per dimension, without
/// materialising the domain. The last dimension varies fastest.
fn for_each_point(radices: &[usize], mut visit: impl FnMut(&[usize])) {
    if radices.contains(&0) {
        return;
    }
    let mut indices = vec![0usize; radices.len()];
    loop {
        visit(&indices);
        let mut position = radices.len();
        loop {
            if position == 0 {
                return;
            }
            position -= 1;
            indices[position] += 1;
            if indices[position] < radices[position] {
                break;
            }
            indices[position] = 0;
        }
    }
}

/// For each dimension of a guard domain, the value indices `rule` covers;
/// `None` for a dimension the rule does not name, which it covers entirely.
fn rule_coverage(rule: &Value, domain: &[(&str, &[&str])]) -> Vec<Option<Vec<usize>>> {
    domain
        .iter()
        .map(|(dimension, values)| {
            let named = rule
                .get("guard_partition")
                .and_then(|partition| partition.get(*dimension))?
                .as_array()?;
            Some(
                values
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| named.iter().any(|item| item.as_str() == Some(**value)))
                    .map(|(index, _)| index)
                    .collect(),
            )
        })
        .collect()
}

/// A well-formed rule of a cell: its index in the registry, the rule and
/// the guard values it covers.
type CellRule<'a> = (usize, &'a Value, Vec<Option<Vec<usize>>>);

fn covers(coverage: &[Option<Vec<usize>>], point: &[usize]) -> bool {
    coverage.iter().zip(point).all(|(allowed, index)| {
        allowed
            .as_ref()
            .is_none_or(|allowed| allowed.contains(index))
    })
}

fn describe(domain: &[(&str, &[&str])], point: &[usize]) -> String {
    let parts: Vec<String> = domain
        .iter()
        .zip(point)
        .map(|((dimension, values), index)| format!("{dimension}={}", values[*index]))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

fn registry(workspace: &Workspace, document: &Value, report: &mut Report) {
    let catalog = match workspace.load_reference(&REGISTRY_CATALOG, text(document, "catalog")) {
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

    // Budget first: the size of every cell is computed with checked
    // arithmetic and compared to both budgets before any point exists.
    let sizes: BTreeMap<&str, Option<u64>> = vocabulary
        .events
        .keys()
        .map(|event| {
            let domain = vocabulary.guard_domain(event);
            (
                *event,
                domain_size(domain.iter().map(|(_, values)| values.len())),
            )
        })
        .collect();
    let cells = vocabulary.machines.values().flat_map(|(states, events)| {
        states.iter().flat_map(|_| {
            events
                .iter()
                .map(|event| sizes.get(event).copied().unwrap_or(Some(1)))
        })
    });
    if let Err(refusal) = check_budget(cells, MAX_POINTS_PER_CELL, MAX_POINTS_PER_REGISTRY) {
        let count = |points: Option<u64>| {
            points.map_or("more than 2^64".to_owned(), |points| points.to_string())
        };
        let message = match refusal {
            BudgetRefusal::Cell(points) => format!(
                "a cell has {} guard points; the budget is {MAX_POINTS_PER_CELL}",
                count(points)
            ),
            BudgetRefusal::Registry(points) => format!(
                "the registry has {} guard points; the budget is {MAX_POINTS_PER_REGISTRY}",
                count(points)
            ),
        };
        report.diagnostics.push(Diagnostic::new(
            code::COVERAGE_BUDGET_EXCEEDED,
            "/catalog",
            message,
        ));
        return;
    }

    // Coverage: each point of each cell is covered by exactly one rule.
    // Diagnostics and notes beyond what a report keeps are counted, not
    // built.
    let complete = text(document, "status") == "complete";
    let mut uncovered_cells = 0usize;
    for (machine, (states, events)) in &vocabulary.machines {
        for state in states {
            for event in events {
                let domain = vocabulary.guard_domain(event);
                let radices: Vec<usize> = domain.iter().map(|(_, values)| values.len()).collect();
                let cell: Vec<CellRule<'_>> = well_formed
                    .iter()
                    .filter(|(_, rule)| {
                        text(rule, "machine") == *machine
                            && text(rule, "state_class") == *state
                            && text(rule, "event_class") == *event
                    })
                    .map(|(index, rule)| (*index, *rule, rule_coverage(rule, &domain)))
                    .collect();
                let mut points = 0usize;
                let mut holes = 0usize;
                let mut shown = Vec::new();
                for_each_point(&radices, |point| {
                    points += 1;
                    let mut covering = cell
                        .iter()
                        .filter(|(_, _, coverage)| covers(coverage, point));
                    match (covering.next(), covering.next()) {
                        (None, _) => {
                            holes += 1;
                            if shown.len() < MAX_HOLES_SHOWN {
                                shown.push(describe(&domain, point));
                            }
                        }
                        (Some(_), None) => {}
                        (Some(first), Some(second)) => {
                            if report.diagnostics.len() > MAX_DIAGNOSTICS {
                                report.omitted_diagnostics += 1;
                            } else {
                                report.diagnostics.push(Diagnostic::new(
                                    code::REGISTRY_OVERLAP,
                                    format!("/rules/{}", second.0),
                                    format!(
                                        "{:?} and {:?} both cover {machine} {state} x {event} {}",
                                        text(first.1, "id"),
                                        text(second.1, "id"),
                                        describe(&domain, point)
                                    ),
                                ));
                            }
                        }
                    }
                });
                if holes == 0 {
                    continue;
                }
                uncovered_cells += 1;
                let more = match holes - shown.len() {
                    0 => String::new(),
                    hidden => format!(" and {hidden} more"),
                };
                let summary = format!(
                    "{machine} {state} x {event}: {holes} of {points} guard points uncovered: {}{more}",
                    shown.join(" ")
                );
                if !complete {
                    if report.notes.len() < MAX_DIAGNOSTICS {
                        report.notes.push(format!("registry hole: {summary}"));
                    } else {
                        report.omitted_notes += 1;
                    }
                } else if report.diagnostics.len() > MAX_DIAGNOSTICS {
                    report.omitted_diagnostics += 1;
                } else {
                    report.diagnostics.push(Diagnostic::new(
                        code::REGISTRY_HOLE,
                        "/rules",
                        summary,
                    ));
                }
            }
        }
    }
    if !complete {
        // First, so that truncation never drops it.
        report.notes.insert(
            0,
            format!(
                "registry {:?} is a seed with {uncovered_cells} uncovered cells; it cannot support a PASS",
                text(document, "id")
            ),
        );
    }
}

// ---------------------------------------------------------------- run result

/// The verdict that the assertions and injections of a result determine
/// (DEM-023). An established FAIL stands; an inconclusive assertion or an
/// injection that was not reached as announced blocks; PASS needs at least
/// one assertion and all of them passing; an empty list concludes nothing.
fn computed_verdict(statuses: &[&str], injections_reached: bool) -> &'static str {
    if statuses.contains(&"FAIL") {
        "FAIL"
    } else if statuses.contains(&"INCONCLUSIVE") || !injections_reached || statuses.is_empty() {
        "INCONCLUSIVE"
    } else if statuses.iter().all(|status| *status == "PASS") {
        "PASS"
    } else {
        "OUT_OF_SCOPE"
    }
}

/// The path, digest and size that `document` cites at `member`.
fn cited<'a>(
    document: &'a Value,
    member: &str,
    out: &mut Vec<Diagnostic>,
) -> (&'a str, &'a str, Option<u64>) {
    let bytes = u64_at(document, &format!("/{member}/bytes"), out);
    let file = &document[member];
    (text(file, "path"), text(file, "sha256"), bytes)
}

fn run_result(workspace: &Workspace, document: &Value, out: &mut Vec<Diagnostic>) {
    unique(
        &entry_names(document, "assertions", "name"),
        "assertion",
        code::DUPLICATE_NAME,
        out,
    );
    unique(
        &entry_names(document, "faults", "id"),
        "fault",
        code::DUPLICATE_NAME,
        out,
    );
    let statuses: Vec<&str> = items(document, "assertions")
        .iter()
        .map(|assertion| text(assertion, "status"))
        .collect();
    let mut injections_reached = true;
    for index in 0..items(document, "faults").len() {
        let expected = u64_at(document, &format!("/faults/{index}/expected_hits"), out);
        let hits = u64_at(document, &format!("/faults/{index}/hits"), out);
        // An injection that was not reached as announced proves nothing.
        injections_reached &= expected.is_some() && expected == hits;
    }
    for index in 0..items(document, "artifacts").len() {
        u64_at(document, &format!("/artifacts/{index}/bytes"), out);
    }

    let qualifying = text(document, "qualification") == "qualifying";
    let computed = computed_verdict(&statuses, injections_reached);
    // A provisional document lacks the evidence a PASS states: at best it
    // is inconclusive.
    let expected = if !qualifying && computed == "PASS" {
        "INCONCLUSIVE"
    } else {
        computed
    };
    let verdict = text(document, "verdict");
    if verdict != expected {
        out.push(Diagnostic::new(
            code::VERDICT_INCONSISTENT,
            "/verdict",
            format!("the assertions and injections determine {expected}, not {verdict}"),
        ));
    }
    if !qualifying {
        return;
    }

    // A qualifying result is checked against the exact files it names.
    if text(document, "contracts_digest") != workspace.contracts_digest() {
        out.push(Diagnostic::new(
            code::EVIDENCE_MISMATCH,
            "/contracts_digest",
            format!(
                "the result names other contracts than the ones in use ({})",
                workspace.contracts_digest()
            ),
        ));
    }
    // The trace may be large: it is hashed as a stream, never loaded.
    let (path, sha256, bytes) = cited(document, "trace", out);
    match workspace.hash_evidence(path) {
        Err((code, message)) => out.push(Diagnostic::new(code, "/trace", message)),
        Ok((digest, length)) => {
            if digest != sha256 || bytes != Some(length) {
                out.push(Diagnostic::new(
                    code::EVIDENCE_MISMATCH,
                    "/trace",
                    format!("{path} has {length} bytes and the digest {digest}"),
                ));
            }
        }
    }
    // The manifest and the scenario are documents: each is read once, and
    // the bytes that are hashed are the bytes that are validated.
    let (path, sha256, bytes) = cited(document, "manifest", out);
    match workspace.load_cited(&RESULT_MANIFEST, path, Some((sha256, bytes))) {
        Err(diagnostic) => out.push(diagnostic),
        Ok(manifest) => {
            if text(&manifest, "contracts_digest") != text(document, "contracts_digest") {
                out.push(Diagnostic::new(
                    code::EVIDENCE_MISMATCH,
                    "/manifest",
                    "the manifest and the result name different contracts",
                ));
            }
        }
    }
    let (path, sha256, bytes) = cited(document, "scenario", out);
    let scenario = match workspace.load_cited(&RESULT_SCENARIO, path, Some((sha256, bytes))) {
        Ok(scenario) => scenario,
        Err(diagnostic) => {
            out.push(diagnostic);
            return;
        }
    };

    // Every assertion of the scenario is mandatory for a run of it: none
    // may be missing, added, or declared out of scope.
    let required: BTreeSet<&str> = names(&scenario, "assertions").into_iter().collect();
    let reported: BTreeSet<&str> = items(document, "assertions")
        .iter()
        .map(|assertion| text(assertion, "name"))
        .collect();
    if required != reported {
        let missing: Vec<&&str> = required.difference(&reported).collect();
        let extra: Vec<&&str> = reported.difference(&required).collect();
        out.push(Diagnostic::new(
            code::RESULT_INCOMPLETE,
            "/assertions",
            format!("against the scenario: missing {missing:?}, not required {extra:?}"),
        ));
    }
    for (index, status) in statuses.iter().enumerate() {
        if *status == "OUT_OF_SCOPE" {
            out.push(Diagnostic::new(
                code::RESULT_INCOMPLETE,
                format!("/assertions/{index}/status"),
                "an assertion of the scenario cannot be out of scope for a run of it",
            ));
        }
    }
    let declared: BTreeMap<&str, &str> = items(&scenario, "faults")
        .iter()
        .map(|fault| (text(fault, "id"), text(fault, "expected_hits")))
        .collect();
    let observed: BTreeMap<&str, &str> = items(document, "faults")
        .iter()
        .map(|fault| (text(fault, "id"), text(fault, "expected_hits")))
        .collect();
    if declared != observed {
        out.push(Diagnostic::new(
            code::RESULT_INCOMPLETE,
            "/faults",
            format!("the scenario declares {declared:?}, the result reports {observed:?}"),
        ));
    }
}

// ------------------------------------------------------- manifest and trace

fn run_manifest(document: &Value, out: &mut Vec<Diagnostic>) {
    u64_at(document, "/artifact/bytes", out);
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
    // The digest is defined over the files in path order, each once.
    let paths: Vec<&str> = items(document, "contracts")
        .iter()
        .map(|file| text(file, "path"))
        .collect();
    if let Some(index) = paths.windows(2).position(|pair| pair[0] >= pair[1]) {
        out.push(Diagnostic::new(
            code::EVIDENCE_MISMATCH,
            format!("/contracts/{}", index + 1),
            "contract files are listed once each, in path order",
        ));
    }
    // The digest must be the digest of the files the manifest lists.
    let listed = contracts_digest(
        items(document, "contracts")
            .iter()
            .map(|file| (text(file, "sha256"), text(file, "path"))),
    );
    if listed != text(document, "contracts_digest") {
        out.push(Diagnostic::new(
            code::EVIDENCE_MISMATCH,
            "/contracts_digest",
            format!("the listed contract files have the digest {listed}"),
        ));
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

/// A store snapshot is checked for form only: every 64-bit member is in
/// range and no row is listed twice under its key. Whether the rows are
/// sound together is what the oracle asserts, not what makes a snapshot
/// readable.
fn store_snapshot(document: &Value, out: &mut Vec<Diagnostic>) {
    const TABLES: [(&str, &[&str], &str, &str); 7] = [
        ("sessions", &["session"], "/session", "session"),
        ("routes", &["topic_hex"], "/topic_hex", "route"),
        (
            "operations",
            &["operation_id"],
            "/operation_id",
            "operation",
        ),
        ("fences", &["operation_id"], "/operation_id", "fence"),
        (
            "publications",
            &["publication_id"],
            "/publication_id",
            "publication",
        ),
        (
            "obligations",
            &["publication_id", "index"],
            "",
            "obligation",
        ),
        ("accounts", &["account"], "/account", "account"),
    ];
    decimal_strings(Some(document), "", out);
    let mut duplicate = |table: &str, index: usize, suffix: &str, what: &str, key: String| {
        out.push(Diagnostic::new(
            code::DUPLICATE_NAME,
            format!("/{table}/{index}{suffix}"),
            format!("{what} {key} is listed twice"),
        ));
    };
    for (table, key, suffix, what) in TABLES {
        let mut seen = BTreeSet::new();
        for (index, row) in items(document, table).iter().enumerate() {
            let identity: Vec<String> = key.iter().map(|member| row[*member].to_string()).collect();
            let identity = identity.join(" ");
            if !seen.insert(identity.clone()) {
                duplicate(table, index, suffix, what, identity);
            }
        }
    }
    // A session has one inbound exchange per Packet Identifier, whatever
    // generation a row claims.
    let mut slots = BTreeSet::new();
    for (index, row) in items(document, "inbound_exchanges").iter().enumerate() {
        let slot = format!("{} {}", row["exchange"]["session"], row["packet_id"]);
        if !slots.insert(slot.clone()) {
            duplicate("inbound_exchanges", index, "", "inbound slot", slot);
        }
    }
}

/// Checks every decimal-looking identity component below `value`. Payload
/// members that hold 64-bit components are objects of decimal strings.
fn decimal_strings(value: Option<&Value>, pointer: &str, out: &mut Vec<Diagnostic>) {
    const COMPONENTS: [&str; 25] = [
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
        "connection_generation",
        "ingress_order",
        "token",
        "after",
        "now",
        "page",
        "revision",
        "length",
        "payload_bytes",
        "references",
        "exchange_slots",
        "receipt_slots",
        "bytes",
    ];
    let members = match value {
        Some(Value::Object(members)) => members,
        // The facts of a recovery page are objects in a list.
        Some(Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                decimal_strings(Some(item), &format!("{pointer}/{index}"), out);
            }
            return;
        }
        _ => return,
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
            Value::Object(_) | Value::Array(_) => decimal_strings(Some(member), &child, out),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// The rules of the P1 registry and the notes of its validation.
    fn p1_registry() -> (Value, Report) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let workspace = Workspace::open(&root, Path::new("contracts/v1")).expect("contracts");
        let path = "contracts/v1/p1.transitions.json";
        let bytes = std::fs::read(root.join(path)).expect("registry");
        let document: Value = serde_json::from_slice(&bytes).expect("JSON");
        let report = workspace.check_file(Path::new(path));
        (document, report)
    }

    /// The rules of one cell, as (guard partition, disposition, required
    /// effects, forbidden effects).
    fn cell<'a>(
        registry: &'a Value,
        state: &str,
        event: &str,
    ) -> Vec<(&'a Value, &'a str, Vec<&'a str>, Vec<&'a str>)> {
        items(registry, "rules")
            .iter()
            .filter(|rule| text(rule, "state_class") == state && text(rule, "event_class") == event)
            .map(|rule| {
                (
                    &rule["guard_partition"],
                    text(rule, "disposition"),
                    names(rule, "required_effects"),
                    names(rule, "forbidden_effects"),
                )
            })
            .collect()
    }

    fn has_hole(report: &Report, cell: &str) -> bool {
        report.notes.iter().any(|note| note.contains(cell))
            || report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains(cell))
    }

    /// The recipe of the snapshot example is what the contract crate
    /// calls canonical: decoding and encoding it again gives the same bytes.
    #[test]
    fn the_recipe_of_the_snapshot_example_is_canonical() {
        use mqttiumd_contract::command::Recipe;

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = "contracts/examples/v1/valid/store-snapshot-after-commit.json";
        let bytes = std::fs::read(root.join(path)).expect("example");
        let snapshot: Value = serde_json::from_slice(&bytes).expect("JSON");
        let hex = snapshot["operations"][0]["recipe_hex"]
            .as_str()
            .expect("hex");
        let recipe: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex digit"))
            .collect();
        let decoded = Recipe::decode_canonical(&recipe, 4, 256).expect("canonical recipe");
        let mut encoded = Vec::new();
        decoded.encode_canonical(&mut encoded).expect("encodes");
        assert_eq!(encoded, recipe);
        let Recipe::AdmitPublication(admit) = decoded else {
            panic!("an admission");
        };
        assert_eq!(
            admit.targets.len(),
            snapshot["obligations"].as_array().map_or(0, Vec::len)
        );
        assert_eq!(admit.source.packet_id.get(), 7);
    }

    /// ADR 0001, "Replayed PUBLISH": while the outcome of the admission is
    /// unknown, a PUBLISH of the exchange creates no command and no
    /// acknowledgement, after a reconnect and on a live connection.
    #[test]
    fn a_publish_replayed_while_the_outcome_is_unknown_admits_and_acknowledges_nothing() {
        let (registry, report) = p1_registry();
        assert!(report.is_accepted(), "{:?}", report.diagnostics);
        assert!(!has_hole(
            &report,
            "inbound_qos2 AdmissionPending x PublishQos2"
        ));
        let rules = cell(&registry, "AdmissionPending", "PublishQos2");
        let connections: BTreeSet<&str> = rules
            .iter()
            .flat_map(|(guard, ..)| names(guard, "connection"))
            .collect();
        assert_eq!(
            connections,
            BTreeSet::from(["resumed", "same"]),
            "one row after a reconnect and one on a live connection"
        );
        for (guard, disposition, required, forbidden) in rules {
            assert_eq!(disposition, "TRANSITION", "{guard}");
            assert!(required.is_empty(), "{guard}: nothing is emitted");
            assert!(forbidden.contains(&"Persist"), "{guard}: no command");
            assert!(forbidden.contains(&"EmitPubrec"), "{guard}: no ack");
        }
    }

    /// The rule of a cell that covers one guard point, given as
    /// (dimension, value) pairs. Panics unless exactly one rule covers it.
    fn rule_at<'a>(
        registry: &'a Value,
        state: &str,
        event: &str,
        point: &[(&str, &str)],
    ) -> &'a Value {
        let covering: Vec<&Value> = items(registry, "rules")
            .iter()
            .filter(|rule| text(rule, "state_class") == state && text(rule, "event_class") == event)
            .filter(|rule| {
                point.iter().all(|(dimension, value)| {
                    match rule["guard_partition"].get(*dimension) {
                        None => true,
                        Some(values) => values
                            .as_array()
                            .is_some_and(|values| values.iter().any(|item| item == value)),
                    }
                })
            })
            .collect();
        assert_eq!(
            covering.len(),
            1,
            "{state} x {event} {point:?}: {} rules",
            covering.len()
        );
        covering[0]
    }

    /// One obligation the specification puts on a cell: the disposition,
    /// the next state, effects that must be required and effects that must
    /// be forbidden.
    struct Expected {
        state: &'static str,
        event: &'static str,
        point: &'static [(&'static str, &'static str)],
        disposition: &'static str,
        next: &'static str,
        required: &'static [&'static str],
        forbidden: &'static [&'static str],
    }

    fn check_expectations(expectations: &[Expected]) {
        let (registry, report) = p1_registry();
        assert!(report.is_accepted(), "{:?}", report.diagnostics);
        for expected in expectations {
            let Expected {
                state,
                event,
                point,
                ..
            } = expected;
            let rule = rule_at(&registry, state, event, point);
            let context = format!("{state} x {event} {point:?} ({})", text(rule, "id"));
            assert_eq!(text(rule, "disposition"), expected.disposition, "{context}");
            assert_eq!(text(rule, "next_state_class"), expected.next, "{context}");
            assert_eq!(
                names(rule, "required_effects"),
                expected.required,
                "{context}"
            );
            for effect in expected.forbidden {
                assert!(
                    names(rule, "forbidden_effects").contains(effect),
                    "{context}: {effect} must be forbidden"
                );
            }
        }
    }

    const SUFFICIENT: (&str, &str) = ("effect_capacity", "sufficient");
    const INSUFFICIENT: (&str, &str) = ("effect_capacity", "insufficient");
    const CURRENT: (&str, &str) = ("identity", "current");
    const STALE: (&str, &str) = ("identity", "stale");
    const COMMITTED: (&str, &str) = ("outcome", "committed");
    const REJECTED: (&str, &str) = ("outcome", "rejected_certain");
    const UNKNOWN: (&str, &str) = ("outcome", "unknown");
    const RETIRED: (&str, &str) = ("outcome", "retired");

    /// ARC-011, ADM-005, ADM-006, ACK-001, DEM-010: what the inbound QoS 2
    /// machine owes outside the nominal path.
    #[test]
    fn the_inbound_machine_follows_the_specification_off_the_nominal_path() {
        check_expectations(&[
            // An unknown PUBREL never makes a publication (ARC-011).
            Expected {
                state: "Absent",
                event: "Pubrel",
                point: &[SUFFICIENT],
                disposition: "REJECT",
                next: "Absent",
                required: &["EmitPubcompNotFound"],
                forbidden: &["Persist", "EmitPubcomp"],
            },
            Expected {
                state: "Absent",
                event: "Pubrel",
                point: &[INSUFFICIENT],
                disposition: "DEFER",
                next: "Absent",
                required: &[],
                forbidden: &["Persist", "EmitPubcompNotFound"],
            },
            // A late return of an old operation does not touch the slot.
            Expected {
                state: "Absent",
                event: "AdmissionResult",
                point: &[STALE, COMMITTED, SUFFICIENT],
                disposition: "IGNORE_STALE",
                next: "Absent",
                required: &[],
                forbidden: &["Persist", "EmitPubrec"],
            },
            Expected {
                state: "Absent",
                event: "ReleaseResult",
                point: &[STALE, COMMITTED, SUFFICIENT],
                disposition: "IGNORE_STALE",
                next: "Absent",
                required: &[],
                forbidden: &["Persist", "EmitPubcomp"],
            },
            // A certain refusal ends the exchange without distribution and
            // answers as the protocol says (ARC-011).
            Expected {
                state: "AdmissionPending",
                event: "AdmissionResult",
                point: &[CURRENT, REJECTED, SUFFICIENT],
                disposition: "TRANSITION",
                next: "Absent",
                required: &["EmitPubrecRefusal"],
                forbidden: &["Persist", "EmitPubrec"],
            },
            Expected {
                state: "AdmissionPending",
                event: "AdmissionResult",
                point: &[CURRENT, REJECTED, INSUFFICIENT],
                disposition: "DEFER",
                next: "AdmissionPending",
                required: &[],
                forbidden: &["EmitPubrecRefusal", "EmitPubrec"],
            },
            // An unknown result keeps the operation and reconciles.
            Expected {
                state: "AdmissionPending",
                event: "AdmissionResult",
                point: &[CURRENT, UNKNOWN, SUFFICIENT],
                disposition: "TRANSITION",
                next: "AdmissionPending",
                required: &[],
                forbidden: &["Persist", "EmitPubrec", "EmitPubrecRefusal"],
            },
            // The commit of an older exchange does not acknowledge this one.
            Expected {
                state: "AdmissionPending",
                event: "AdmissionResult",
                point: &[STALE, COMMITTED, SUFFICIENT],
                disposition: "IGNORE_STALE",
                next: "AdmissionPending",
                required: &[],
                forbidden: &["Persist", "EmitPubrec"],
            },
            // PUBREL never creates the initial distribution (ADM-005).
            Expected {
                state: "AdmissionPending",
                event: "Pubrel",
                point: &[SUFFICIENT],
                disposition: "CLOSE",
                next: "AdmissionPending",
                required: &["CloseConnection"],
                forbidden: &["Persist", "EmitPubcomp", "EmitPubcompNotFound"],
            },
            // Terminal knowledge never regresses; a contradiction is a fault.
            Expected {
                state: "AwaitPubrel",
                event: "AdmissionResult",
                point: &[CURRENT, COMMITTED, SUFFICIENT],
                disposition: "IGNORE_STALE",
                next: "AwaitPubrel",
                required: &[],
                forbidden: &["Persist", "EmitPubrec"],
            },
            Expected {
                state: "AwaitPubrel",
                event: "AdmissionResult",
                point: &[CURRENT, REJECTED, SUFFICIENT],
                disposition: "FAULT",
                next: "AwaitPubrel",
                required: &[],
                forbidden: &["Persist", "EmitPubrec", "EmitPubrecRefusal"],
            },
            Expected {
                state: "ReleasePending",
                event: "AdmissionResult",
                point: &[CURRENT, REJECTED, INSUFFICIENT],
                disposition: "FAULT",
                next: "ReleasePending",
                required: &[],
                forbidden: &["Persist", "EmitPubcomp"],
            },
            // A replayed PUBREL asks for no second release (ADM-005).
            Expected {
                state: "ReleasePending",
                event: "Pubrel",
                point: &[SUFFICIENT],
                disposition: "TRANSITION",
                next: "ReleasePending",
                required: &[],
                forbidden: &["Persist", "EmitPubcomp"],
            },
            // No second admission during the exchange (ADM-006).
            Expected {
                state: "ReleasePending",
                event: "PublishQos2",
                point: &[("connection", "same"), SUFFICIENT],
                disposition: "CLOSE",
                next: "ReleasePending",
                required: &["CloseConnection"],
                forbidden: &["Persist", "EmitPubrec"],
            },
            // A refused release is prepared again under a new identity
            // (DEM-009); PUBCOMP still waits for a commit (ACK-001).
            Expected {
                state: "ReleasePending",
                event: "ReleaseResult",
                point: &[CURRENT, REJECTED, SUFFICIENT],
                disposition: "TRANSITION",
                next: "ReleasePending",
                required: &["Persist"],
                forbidden: &["EmitPubcomp"],
            },
            Expected {
                state: "ReleasePending",
                event: "ReleaseResult",
                point: &[CURRENT, UNKNOWN, INSUFFICIENT],
                disposition: "TRANSITION",
                next: "ReleasePending",
                required: &[],
                forbidden: &["Persist", "EmitPubcomp"],
            },
            Expected {
                state: "ReleasePending",
                event: "ReleaseResult",
                point: &[STALE, COMMITTED, SUFFICIENT],
                disposition: "IGNORE_STALE",
                next: "ReleasePending",
                required: &[],
                forbidden: &["Persist", "EmitPubcomp"],
            },
        ]);
    }

    /// V3-006, DEM-009, DEM-010: an identity is executed at most once, a
    /// different recipe under it is a fault, and nothing reopens.
    #[test]
    fn the_command_machine_never_executes_an_identity_twice() {
        let mut expectations = Vec::new();
        for state in [
            "Submitted",
            "Indeterminate",
            "Committed",
            "RejectedCertain",
            "Consumed",
            "ReceiptClosed",
        ] {
            expectations.push(Expected {
                state,
                event: "Submit",
                point: &[SUFFICIENT],
                disposition: "REJECT",
                next: state,
                required: &[],
                forbidden: &["Persist"],
            });
        }
        for state in [
            "Submitted",
            "Indeterminate",
            "Committed",
            "RejectedCertain",
            "Consumed",
        ] {
            expectations.push(Expected {
                state,
                event: "Replay",
                point: &[("recipe", "different")],
                disposition: "FAULT",
                next: state,
                required: &[],
                forbidden: &["Persist"],
            });
        }
        for (state, disposition) in [
            ("Submitted", "IGNORE_STALE"),
            ("Indeterminate", "IGNORE_STALE"),
            ("Committed", "IGNORE_STALE"),
            ("Consumed", "IGNORE_STALE"),
            ("RejectedCertain", "REJECT"),
        ] {
            expectations.push(Expected {
                state,
                event: "Replay",
                point: &[("recipe", "same")],
                disposition,
                next: state,
                required: &[],
                forbidden: &["Persist"],
            });
        }
        // An identity whose result was collected is closed, not guessed.
        for state in ["Submitted", "Indeterminate"] {
            expectations.push(Expected {
                state,
                event: "StoreResult",
                point: &[RETIRED],
                disposition: "TRANSITION",
                next: "ReceiptClosed",
                required: &[],
                forbidden: &["Persist"],
            });
        }
        for outcome in [COMMITTED, REJECTED, UNKNOWN, RETIRED] {
            let point: &'static [(&str, &str)] = Box::leak(Box::new([outcome]));
            expectations.push(Expected {
                state: "ReceiptClosed",
                event: "StoreResult",
                point,
                disposition: "IGNORE_STALE",
                next: "ReceiptClosed",
                required: &[],
                forbidden: &["Persist"],
            });
        }
        expectations.push(Expected {
            state: "Consumed",
            event: "StoreResult",
            point: &[REJECTED],
            disposition: "FAULT",
            next: "Consumed",
            required: &[],
            forbidden: &["Persist"],
        });
        expectations.push(Expected {
            state: "RejectedCertain",
            event: "StoreResult",
            point: &[REJECTED],
            disposition: "IGNORE_STALE",
            next: "RejectedCertain",
            required: &[],
            forbidden: &["Persist"],
        });
        for event in ["StoreResult", "ResultTimeout", "Replay"] {
            expectations.push(Expected {
                state: "Prepared",
                event,
                point: &[],
                disposition: "UNREACHABLE",
                next: "Prepared",
                required: &[],
                forbidden: &["Persist"],
            });
        }
        check_expectations(&expectations);
    }

    /// A result is current only for a state that waits for it.
    #[test]
    fn a_state_that_waits_for_no_operation_has_no_current_result() {
        let (registry, _) = p1_registry();
        for (state, event) in [
            ("Absent", "AdmissionResult"),
            ("Absent", "ReleaseResult"),
            ("AdmissionPending", "ReleaseResult"),
            ("AwaitPubrel", "ReleaseResult"),
        ] {
            for outcome in [COMMITTED, REJECTED, UNKNOWN, RETIRED] {
                for capacity in [SUFFICIENT, INSUFFICIENT] {
                    let rule = rule_at(&registry, state, event, &[CURRENT, outcome, capacity]);
                    assert_eq!(
                        text(rule, "disposition"),
                        "UNREACHABLE",
                        "{state} x {event}"
                    );
                    let rule = rule_at(&registry, state, event, &[STALE, outcome, capacity]);
                    assert_eq!(
                        text(rule, "disposition"),
                        "IGNORE_STALE",
                        "{state} x {event}"
                    );
                }
            }
        }
    }

    /// V3-006: an old command presented again after its receipt was
    /// closed is refused, whatever recipe it carries: the stored result is
    /// gone, and none is invented.
    #[test]
    fn a_command_replayed_after_its_receipt_was_closed_is_refused() {
        let expectations: Vec<Expected> = ["same", "different"]
            .into_iter()
            .map(|recipe| Expected {
                state: "ReceiptClosed",
                event: "Replay",
                point: Box::leak(Box::new([("recipe", recipe)])),
                disposition: "REJECT",
                next: "ReceiptClosed",
                required: &[],
                forbidden: &["Persist"],
            })
            .collect();
        check_expectations(&expectations);
    }

    /// ADR 0001, "Guards": a deadline for an operation that became terminal
    /// is ignored as stale; for an unresolved one it starts a resolution
    /// and arms the next deadline.
    #[test]
    fn a_deadline_resolves_an_unresolved_operation_and_is_stale_for_a_terminal_one() {
        let (registry, report) = p1_registry();
        for state in ["Committed", "RejectedCertain", "Consumed", "ReceiptClosed"] {
            assert!(!has_hole(
                &report,
                &format!("command {state} x ResultTimeout")
            ));
            let rules = cell(&registry, state, "ResultTimeout");
            assert_eq!(rules.len(), 1, "{state}");
            let (_, disposition, required, forbidden) = &rules[0];
            assert_eq!(*disposition, "IGNORE_STALE", "{state}");
            assert!(required.is_empty(), "{state}");
            for effect in ["Persist", "ResolveAndFence", "ArmDeadline"] {
                assert!(forbidden.contains(&effect), "{state}: {effect}");
            }
        }
        for state in ["Submitted", "Indeterminate"] {
            assert!(!has_hole(
                &report,
                &format!("command {state} x ResultTimeout")
            ));
            let rules = cell(&registry, state, "ResultTimeout");
            let resolving: Vec<_> = rules
                .iter()
                .filter(|(guard, ..)| names(guard, "effect_capacity") == ["sufficient"])
                .collect();
            assert_eq!(resolving.len(), 1, "{state}");
            let (_, disposition, required, forbidden) = resolving[0];
            assert_eq!(*disposition, "TRANSITION", "{state}");
            assert!(required.contains(&"ResolveAndFence"), "{state}");
            assert!(required.contains(&"ArmDeadline"), "{state}");
            assert!(forbidden.contains(&"Persist"), "{state}");
            let deferred: Vec<_> = rules
                .iter()
                .filter(|(guard, ..)| names(guard, "effect_capacity") == ["insufficient"])
                .collect();
            assert_eq!(deferred.len(), 1, "{state}");
            assert_eq!(deferred[0].1, "DEFER", "{state}");
        }
    }

    #[test]
    fn the_size_of_a_guard_domain_is_computed_without_building_it() {
        assert_eq!(domain_size([]), Some(1));
        assert_eq!(domain_size([2, 4, 2]), Some(16));
        assert_eq!(domain_size([2; 32]), Some(1 << 32));
        assert_eq!(domain_size([2; 63]), Some(1 << 63));
        assert_eq!(domain_size([2; 64]), None, "2^64 does not fit");
        assert_eq!(domain_size([usize::MAX, usize::MAX]), None);
        assert_eq!(domain_size([5, 0, 7]), Some(0));
    }

    #[test]
    fn a_cell_is_accepted_at_the_budget_and_refused_one_point_above() {
        assert_eq!(check_budget([Some(8)], 8, 100), Ok(8));
        assert_eq!(
            check_budget([Some(9)], 8, 100),
            Err(BudgetRefusal::Cell(Some(9)))
        );
        assert_eq!(check_budget([None], 8, 100), Err(BudgetRefusal::Cell(None)));
        assert_eq!(check_budget([], 8, 100), Ok(0));
    }

    #[test]
    fn a_registry_is_accepted_at_the_budget_and_refused_one_point_above() {
        assert_eq!(check_budget([Some(8); 3], 8, 24), Ok(24));
        assert_eq!(
            check_budget([Some(8), Some(8), Some(8), Some(1)], 8, 24),
            Err(BudgetRefusal::Registry(Some(25)))
        );
        assert_eq!(
            check_budget([Some(u64::MAX), Some(1)], u64::MAX, u64::MAX),
            Err(BudgetRefusal::Registry(None))
        );
    }

    #[test]
    fn thirty_two_binary_guards_are_refused_before_any_point_exists() {
        // 2^32 points per cell: enumerating them would never finish here.
        let size = domain_size([2; 32]);
        assert_eq!(
            check_budget([size], MAX_POINTS_PER_CELL, MAX_POINTS_PER_REGISTRY),
            Err(BudgetRefusal::Cell(Some(1 << 32)))
        );
    }

    #[test]
    fn every_point_is_visited_once_in_a_fixed_order() {
        let mut seen = Vec::new();
        for_each_point(&[2, 3], |point| seen.push(point.to_vec()));
        assert_eq!(
            seen,
            vec![
                vec![0, 0],
                vec![0, 1],
                vec![0, 2],
                vec![1, 0],
                vec![1, 1],
                vec![1, 2]
            ]
        );
        let mut empty_domain = 0;
        for_each_point(&[], |_| empty_domain += 1);
        assert_eq!(empty_domain, 1, "a domain without dimension has one point");
        let mut no_value = 0;
        for_each_point(&[3, 0], |_| no_value += 1);
        assert_eq!(no_value, 0);
    }
}
