"""Join the observed pairs of transcripts with the checked-in classification.

The bench does not decide what a difference means: Mosquitto is not the
oracle, the MQTT specification is. `divergences.json` holds one entry per
scenario whose two transcripts differ:

    {"id": "area/name-v5",
     "class": "defect",
     "difference": "one line: what the reference does, what mqttiumd does",
     "statement": "MQTT-3.1.4-2",        (required for reference-nonconformant)
     "mqttiumd": "what mqttiumd does",   (required for reference-nonconformant)
     "lot": "product lot that closes it" (profile-refused and defect, when known)
     "incident": {"mqttiumd": ["exited"]} (only when a broker exits, hangs or panics)
     "fingerprint": "16 hex digits"}

`fingerprint` names the exact pair of normalized transcripts the entry
was written against, so that a classification never silently covers
another difference than the one that was read.

`incident` names, per broker, the incidents (`INCIDENTS`) that are part
of the difference that was read. A broker that exits by itself, has to be
killed or prints a panic fails the run in every class unless that exact
incident is recorded here, and a recorded incident that does not happen
fails it too. An incident of mqttiumd is a `defect` by definition.
"""
from __future__ import annotations

import json
from pathlib import Path
import re

SAME, DIFFERENT, ERROR = "same", "different", "error"

CLASSES = {
    "profile-refused": "mqttiumd deliberately does not support the feature yet",
    "reference-nonconformant": "Mosquitto departs from the specification; mqttiumd follows it",
    "defect": "mqttiumd is wrong or needlessly different: a to-do for the product",
    "unspecified": "both behaviours are allowed by the specification",
}
UNCLASSIFIED = "unclassified"
# What a broker process can do besides answering: `harness.Running.incidents`.
INCIDENTS = {
    "exited": "exited by itself during the scenario",
    "killed": "did not stop when asked to and was killed",
    "panic": "printed a panic",
}
BACKENDS = ("reference", "mqttiumd")
FIELDS = {"id", "class", "difference", "statement", "mqttiumd", "lot", "incident", "fingerprint"}
STATEMENT = re.compile(r"MQTT-\d+(\.\d+)+-\d+")

# Problems that make a run fail.
NOT_LISTED = "the transcripts differ and divergences.json has no entry"
STALE = "divergences.json has an entry but the transcripts are now the same"
CHANGED = "the difference is not the one the entry was written against (fingerprint {now}, entry {then})"
UNKNOWN = "divergences.json names a scenario that does not exist"
BROKEN = "a broker could not be started or the harness failed"
UNRECORDED = "{backend} {what}, and no entry of divergences.json records that incident for this scenario"
NO_INCIDENT = "divergences.json records that {backend} {what}, and it did not"


class DivergenceError(ValueError):
    """`divergences.json` is not usable."""


def one_line(entry: dict, key: str, name: str, *, required: bool) -> None:
    value = entry.get(key)
    if value is None and not required:
        return
    if not isinstance(value, str) or not value.strip() or "\n" in value:
        raise DivergenceError(f"{name}: `{key}` must be one non-empty line")


def check_divergences(data) -> dict:
    """Validate the list and return it keyed by scenario id."""
    if not isinstance(data, list):
        raise DivergenceError("divergences.json must be a list of entries")
    out: dict = {}
    for entry in data:
        if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
            raise DivergenceError("every entry is an object with an `id`")
        name = entry["id"]
        if name in out:
            raise DivergenceError(f"{name}: listed twice")
        if set(entry) - FIELDS:
            raise DivergenceError(f"{name}: unknown field {sorted(set(entry) - FIELDS)}")
        kind = entry.get("class")
        if kind not in CLASSES:
            raise DivergenceError(f"{name}: class must be one of {', '.join(CLASSES)}")
        nonconformant = kind == "reference-nonconformant"
        one_line(entry, "difference", name, required=True)
        one_line(entry, "statement", name, required=nonconformant)
        one_line(entry, "mqttiumd", name, required=nonconformant)
        one_line(entry, "lot", name, required=False)
        one_line(entry, "fingerprint", name, required=True)
        if "statement" in entry and not STATEMENT.search(entry["statement"]):
            raise DivergenceError(f"{name}: `statement` cites no statement identifier such as MQTT-3.1.4-2")
        if "lot" in entry and kind not in ("profile-refused", "defect"):
            raise DivergenceError(f"{name}: `lot` is for profile-refused and defect entries")
        if "incident" in entry:
            check_incident(entry["incident"], name, kind)
        out[name] = entry
    return out


def check_incident(incident, name: str, kind: str) -> None:
    if not isinstance(incident, dict) or not incident:
        raise DivergenceError(f"{name}: `incident` maps a broker to the kinds of its incidents")
    for backend, kinds in incident.items():
        if backend not in BACKENDS:
            raise DivergenceError(f"{name}: `incident` names {backend!r}, not one of {', '.join(BACKENDS)}")
        if (not isinstance(kinds, list) or not kinds or any(item not in INCIDENTS for item in kinds)
                or kinds != sorted(set(kinds))):
            raise DivergenceError(f"{name}: the incidents of {backend} are a sorted list without repetition of "
                                  f"{', '.join(INCIDENTS)}")
    if "mqttiumd" in incident and kind != "defect":
        raise DivergenceError(f"{name}: an incident of mqttiumd is a defect, not {kind}")


def load_divergences(path: Path) -> dict:
    return check_divergences(json.loads(path.read_text()))


def known_statements(spec_directory: Path):
    """`{version: set of statement ids}` from the repository's extracted
    statement lists, or None where the bench runs without them."""
    out = {}
    for version in ("3.1.1", "5.0"):
        path = spec_directory / f"mqtt-v{version}-statements.json"
        if not path.is_file():
            return None
        out[version] = {item["id"] for item in json.loads(path.read_text())["statements"]}
    return out


def check_statements(divergences: dict, versions: dict, known) -> list:
    """Cited identifiers that the specification of the scenario does not
    contain. `versions` maps a scenario id to its protocol version."""
    problems = []
    if known is None:
        return problems
    for name, entry in divergences.items():
        version = versions.get(name)
        pools = [known[version]] if version in known else list(known.values())
        for cited in STATEMENT.finditer(entry.get("statement", "")):
            if not any(cited.group(0) in pool for pool in pools):
                problems.append((name, f"statement {cited.group(0)} is not in the MQTT {version} specification"))
    return problems


def judge(observed: dict, divergences: dict, known) -> tuple[dict, list]:
    """Classify each observed scenario.

    `observed` maps a scenario played in this run to `(status,
    fingerprint)`; `known` is every scenario name that exists. Returns
    `(classes, problems)`: the class of each played scenario (`same` when
    the transcripts are equal) and the `(scenario, problem)` pairs that
    fail the run.
    """
    classes, problems = {}, []
    for name in sorted(divergences):
        if name not in known:
            problems.append((name, UNKNOWN))
    for name in sorted(observed):
        status, fingerprint = observed[name]
        entry = divergences.get(name)
        if status == ERROR:
            classes[name] = ERROR
            problems.append((name, BROKEN))
        elif status == SAME:
            classes[name] = SAME
            if entry is not None:
                problems.append((name, STALE))
        elif entry is None:
            classes[name] = UNCLASSIFIED
            problems.append((name, NOT_LISTED))
        else:
            classes[name] = entry["class"]
            if entry["fingerprint"] != fingerprint:
                problems.append((name, CHANGED.format(now=fingerprint, then=entry["fingerprint"])))
    return classes, problems


def judge_incidents(observed: dict, classes: dict, divergences: dict) -> list:
    """The `(scenario, problem)` pairs of the broker incidents of a run.

    `observed` maps each scenario played to `{backend: [kinds]}`, the
    kinds being those of `INCIDENTS`. Whatever the class of the scenario
    (`classes`, the first result of `judge`; a scenario that is `same` has
    no entry), an incident that its entry does not record is a problem,
    and so is a recorded incident that did not happen.
    """
    problems = []
    for name in sorted(observed):
        recorded = divergences.get(name, {}).get("incident", {}) if classes.get(name) in CLASSES else {}
        for backend in BACKENDS:
            seen, expected = observed[name].get(backend, ()), recorded.get(backend, ())
            for kind, what in INCIDENTS.items():
                if kind in seen and kind not in expected:
                    problems.append((name, UNRECORDED.format(backend=backend, what=what)))
                elif kind in expected and kind not in seen:
                    problems.append((name, NO_INCIDENT.format(backend=backend, what=what)))
    return problems
