"""Second reader of the v1 schemas.

It reads the example corpus with the Python ``jsonschema`` package and checks
that it agrees with ``contracts/examples/v1/index.json`` on everything a
schema reader can decide:

* a valid example, and every contract file, parses strictly and satisfies its
  schema;
* an invalid example declared ``schema-violation`` violates its schema at the
  declared pointer;
* an invalid example declared with a strict-parsing code fails strict parsing
  for that reason;
* an invalid example declared with a semantic code satisfies its schema: the
  schema must not be the layer that refuses it.

This is not the independent oracle of lot L3. It reads no trace and no store,
computes no expectation about broker behaviour, and repeats no semantic rule.
Its purpose is narrower: two unrelated JSON Schema implementations must read
the same schema files the same way.

Run from the ``mqttiumd`` directory::

    python -m pip install -r tools/requirements.txt
    python tools/check_schemas.py
"""

from __future__ import annotations

import json
import pathlib
import sys

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMAS = ROOT / "contracts" / "schema" / "v1"
CONTRACTS = ROOT / "contracts" / "v1"
INDEX = ROOT / "contracts" / "examples" / "v1" / "index.json"

FORMATS = {
    "mqttiumd.scenario": "scenario.schema.json",
    "mqttiumd.fixture": "fixture.schema.json",
    "mqttiumd.trace-record": "trace-record.schema.json",
    "mqttiumd.transition-registry": "transition-registry.schema.json",
    "mqttiumd.run-manifest": "run-manifest.schema.json",
    "mqttiumd.run-result": "run-result.schema.json",
    "mqttiumd.fault-catalog": "catalogs.schema.json",
    "mqttiumd.profile-catalog": "catalogs.schema.json",
    "mqttiumd.assertion-catalog": "catalogs.schema.json",
    "mqttiumd.requirement-register": "catalogs.schema.json",
    "mqttiumd.transition-catalog": "catalogs.schema.json",
    "mqttiumd.example-index": "catalogs.schema.json",
}

U64_MAX = 2**64 - 1


class Refused(Exception):
    """A document is refused; ``code`` is the corpus code of the reason."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


def _no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    members: dict[str, object] = {}
    for key, value in pairs:
        if key in members:
            raise Refused("duplicate-key", key)
        members[key] = value
    return members


def _integer(text: str) -> int:
    value = int(text)
    if not -(2**63) <= value <= U64_MAX:
        raise Refused("number-not-integer", text)
    return value


def _not_integer(text: str) -> float:
    raise Refused("number-not-integer", text)


def parse_strict(data: bytes) -> object:
    """Parse one JSON value, refusing what version 1 refuses."""
    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_no_duplicates,
            parse_int=_integer,
            parse_float=_not_integer,
            parse_constant=_not_integer,
        )
    except Refused:
        raise
    except (ValueError, UnicodeDecodeError) as error:
        raise Refused("json-syntax", str(error)) from error


def load_validators() -> dict[str, Draft202012Validator]:
    schemas = {
        path.name: parse_strict(path.read_bytes()) for path in sorted(SCHEMAS.glob("*.json"))
    }
    for name, schema in schemas.items():
        Draft202012Validator.check_schema(schema)
        if "$id" in schema:
            raise SystemExit(f"{name}: schema files carry no $id")
    # The files have no $id: each is registered under its own name, which is
    # exactly how their relative references name each other.
    registry = Registry().with_resources(
        (name, Resource.from_contents(schema, default_specification=DRAFT202012))
        for name, schema in schemas.items()
    )
    return {
        name: Draft202012Validator(schema, registry=registry) for name, schema in schemas.items()
    }


def pointer(error) -> str:
    return "".join(f"/{part}" for part in error.absolute_path)


def schema_pointers(validators: dict[str, Draft202012Validator], document: object) -> list[str]:
    """Pointers of the schema violations of a parsed document."""
    form = document.get("format") if isinstance(document, dict) else None
    if form not in FORMATS:
        raise Refused("format-unknown", repr(form))
    version = document.get("schema_version")
    if type(version) is not int or version != 1:
        raise Refused("schema-version-unknown", repr(version))
    return [pointer(error) for error in validators[FORMATS[form]].iter_errors(document)]


PARSE_CODES = {"json-syntax", "duplicate-key", "number-not-integer"}
ENVELOPE_CODES = {"format-unknown", "schema-version-unknown"}


def main() -> int:
    validators = load_validators()
    index = parse_strict(INDEX.read_bytes())
    problems: list[str] = []
    checked = 0

    accepted = [str(path.relative_to(ROOT)) for path in sorted(CONTRACTS.glob("*.json"))]
    accepted += index["valid"] + [str(INDEX.relative_to(ROOT))]
    for name in accepted:
        checked += 1
        try:
            found = schema_pointers(validators, parse_strict((ROOT / name).read_bytes()))
        except Refused as refusal:
            problems.append(f"{name}: must be accepted, refused with {refusal}")
            continue
        if found:
            problems.append(f"{name}: must satisfy its schema, violations at {sorted(set(found))}")

    for case in index["invalid"]:
        checked += 1
        name, code, where = case["file"], case["code"], case["pointer"]
        try:
            found = schema_pointers(validators, parse_strict((ROOT / name).read_bytes()))
        except Refused as refusal:
            if refusal.code != code:
                problems.append(f"{name}: declared {code}, refused with {refusal}")
            continue
        if code in PARSE_CODES | ENVELOPE_CODES:
            problems.append(f"{name}: declared {code}, and it parsed with a known envelope")
        elif code == "schema-violation":
            if where not in found:
                problems.append(
                    f"{name}: no schema violation at {where!r}, found {sorted(set(found))}"
                )
        elif found:
            problems.append(
                f"{name}: declared {code}, yet the schema refuses it at {sorted(set(found))}"
            )

    for problem in problems:
        print(f"disagreement {problem}", file=sys.stderr)
    print(
        f"{'agreed' if not problems else 'disagreed'}: {checked} documents, {len(problems)} disagreements"
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
