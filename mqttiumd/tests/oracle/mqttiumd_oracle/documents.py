"""Reading v1 documents against one snapshot of the contracts.

Validation runs in the order ``contracts/README.md`` fixes and stops at the
first stage that refuses: strict parsing, known format and version, JSON
Schema, semantic pass.

The contract files and the schemas are read once. The digest that names the
contracts is computed from those bytes, and the same bytes are parsed: the
rules this reader applies are the rules the digest names.
"""

from __future__ import annotations

import hashlib
import pathlib

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from . import semantic
from .strict import Diagnostic, Refused, parse

MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_REFERENCE_DEPTH = 4
DIALECT = "https://json-schema.org/draft/2020-12/schema"

FORMATS = {
    "mqttiumd.product-event": "product-event.schema.json",
    "mqttiumd.product-profile": "product-profile.schema.json",
    "mqttiumd.product-transition-registry": "product-transitions.schema.json",
    "mqttiumd.scenario": "scenario.schema.json",
    "mqttiumd.fixture": "fixture.schema.json",
    "mqttiumd.trace-record": "trace-record.schema.json",
    "mqttiumd.transition-registry": "transition-registry.schema.json",
    "mqttiumd.run-manifest": "run-manifest.schema.json",
    "mqttiumd.run-result": "run-result.schema.json",
    "mqttiumd.store-snapshot": "store-snapshot.schema.json",
    "mqttiumd.fault-catalog": "catalogs.schema.json",
    "mqttiumd.profile-catalog": "catalogs.schema.json",
    "mqttiumd.assertion-catalog": "catalogs.schema.json",
    "mqttiumd.requirement-register": "catalogs.schema.json",
    "mqttiumd.transition-catalog": "catalogs.schema.json",
    "mqttiumd.example-index": "catalogs.schema.json",
}

CATALOGS = {
    "fault-points.json": "mqttiumd.fault-catalog",
    "profiles.json": "mqttiumd.profile-catalog",
    "assertions.json": "mqttiumd.assertion-catalog",
    "requirements.json": "mqttiumd.requirement-register",
}


class ContractsUnreadable(Exception):
    """The contract directory itself cannot be trusted."""


def contained(path: str) -> bool:
    """Whether a relative path stays below the root it is joined to."""
    return (
        bool(path)
        and not path.startswith("/")
        and "\\" not in path
        and all(segment not in ("", ".", "..") for segment in path.split("/"))
    )


class Contracts:
    """The schemas and catalogues of one contract directory, read once."""

    def __init__(self, root, files, digest, validators, catalogs):
        self.root = root
        self.files = files
        self.digest = digest
        self._validators = validators
        self.catalogs = catalogs
        self._active = []

    @classmethod
    def open(cls, root, contracts="contracts/v1"):
        root = pathlib.Path(root).resolve()
        directory = root / contracts
        schema_directory = directory.parent / "schema" / directory.name
        paths = sorted(directory.glob("*.json")) + sorted(schema_directory.glob("*.json"))
        # A contract file is bounded like any document, before it is read.
        for path in paths:
            if path.stat().st_size > MAX_DOCUMENT_BYTES:
                raise ContractsUnreadable(f"{path.name} exceeds {MAX_DOCUMENT_BYTES} bytes")
        # One read: these bytes are hashed, and these bytes are parsed.
        content = {path: path.read_bytes() for path in paths}
        files = {
            path.relative_to(root).as_posix(): hashlib.sha256(data).hexdigest()
            for path, data in content.items()
        }
        listing = "".join(f"{sha256}  {name}\n" for name, sha256 in sorted(files.items()))
        digest = hashlib.sha256(listing.encode()).hexdigest()

        schemas = {}
        for path, data in content.items():
            if path.parent != schema_directory:
                continue
            # Every file of the schema directory is hashed into the digest,
            # so every one must be a schema the readers load by its name.
            if not path.name.endswith(".schema.json"):
                raise ContractsUnreadable(f"{path.name}: in the schema directory, not a schema")
            try:
                schema = parse(data)
            except Refused as refusal:
                raise ContractsUnreadable(f"{path.name}: {refusal}") from refusal
            _check_schema(path.name, schema, [other.name for other in content])
            schemas[path.name] = schema
        for name in set(FORMATS.values()) - set(schemas):
            raise ContractsUnreadable(f"{name}: schema file is missing")
        registry = Registry().with_resources(
            (name, Resource.from_contents(schema, default_specification=DRAFT202012))
            for name, schema in schemas.items()
        )
        validators = {
            name: Draft202012Validator(schema, registry=registry)
            for name, schema in schemas.items()
        }

        contracts = cls(root, files, digest, validators, {})
        contracts.schemas = schemas
        contracts._contract_paths = [
            path.relative_to(root).as_posix() for path in content if path.parent == directory
        ]
        loaded = {}
        for name, form in CATALOGS.items():
            data = content.get(directory / name)
            if data is None:
                raise ContractsUnreadable(f"{name}: catalogue file is missing")
            try:
                document = parse(data)
            except Refused as refusal:
                raise ContractsUnreadable(f"{name}: {refusal}") from refusal
            problems = contracts._schema(document)
            if problems or document.get("format") != form:
                raise ContractsUnreadable(f"{name}: not a valid {form}: {problems[:1]}")
            loaded[form] = document
        contracts._bytes = {
            path.relative_to(root).as_posix(): data for path, data in content.items()
        }
        # The reasons EmitPubrecRefusal carries, from the transition
        # catalogues of the directory; two that state them must agree.
        refusal_reasons = None
        for path, data in content.items():
            if path.parent != directory:
                continue
            try:
                document = parse(data)
            except Refused:
                continue
            if not isinstance(document, dict) or "refusal_reasons" not in document:
                continue
            if document.get("format") != "mqttiumd.transition-catalog":
                continue
            if refusal_reasons not in (None, document["refusal_reasons"]):
                raise ContractsUnreadable(f"{path.name}: another table of refusal reasons")
            refusal_reasons = document["refusal_reasons"]
        faults = loaded["mqttiumd.fault-catalog"]
        contracts.catalogs = {
            "fault_points": {
                point["point"]: (point["carries"], set(point["actions"]))
                for point in faults["points"]
            },
            "command_kinds": set(faults["command_kinds"]),
            "profiles": {
                profile["id"] for profile in loaded["mqttiumd.profile-catalog"]["profiles"]
            },
            "assertions": {
                entry["name"] for entry in loaded["mqttiumd.assertion-catalog"]["assertions"]
            },
            "every_run": [
                entry["name"]
                for entry in loaded["mqttiumd.assertion-catalog"]["assertions"]
                if entry["every_run"]
            ],
            "requirements": {
                entry["id"] for entry in loaded["mqttiumd.requirement-register"]["entries"]
            },
            "refusal_reasons": refusal_reasons,
        }
        return contracts

    # ------------------------------------------------------------ validation

    def _schema(self, document):
        form = document.get("format") if isinstance(document, dict) else None
        if form not in FORMATS:
            return [Diagnostic("format-unknown", "/format", f"{form!r} is not a known format")]
        version = document.get("schema_version")
        if type(version) is not int or version != 1:
            return [
                Diagnostic(
                    "schema-version-unknown", "/schema_version", "only schema_version 1 is known"
                )
            ]
        return [
            Diagnostic(
                "schema-violation",
                "".join(f"/{part}" for part in error.absolute_path),
                error.message,
            )
            for error in self._validators[FORMATS[form]].iter_errors(document)
        ]

    def check_value(self, document):
        """The diagnostics of a parsed document; empty when it is accepted."""
        diagnostics = self._schema(document)
        if not diagnostics:
            diagnostics = semantic.check(self, document)
        return diagnostics

    def check_bytes(self, data: bytes):
        try:
            document = parse(data)
        except Refused as refusal:
            return [refusal.diagnostic]
        return self.check_value(document)

    def check_contracts(self):
        """Every file of the contract directory, from the bytes that were hashed."""
        return [(name, self.check_bytes(self._bytes[name])) for name in self._contract_paths]

    # ----------------------------------------------------------------- files

    def read(self, relative: str) -> bytes:
        """The bytes of a document below the root, or ``Refused``."""
        if not contained(relative):
            raise Refused("path-escape", "", f"{relative!r} is not a path below the root")
        if relative in self._bytes:
            return self._bytes[relative]
        try:
            path = (self.root / relative).resolve(strict=True)
        except OSError as error:
            raise Refused("file-unreadable", "", f"{relative}: {error}") from error
        if not path.is_relative_to(self.root):
            raise Refused("path-escape", "", f"{relative} resolves outside the root")
        if not path.is_file():
            raise Refused("file-unreadable", "", f"{relative} is not a regular file")
        if path.stat().st_size > MAX_DOCUMENT_BYTES:
            raise Refused("file-too-large", "", f"{relative} exceeds {MAX_DOCUMENT_BYTES} bytes")
        try:
            return path.read_bytes()
        except OSError as error:
            raise Refused("file-unreadable", "", f"{relative}: {error}") from error

    def check_file(self, relative: str):
        try:
            data = self.read(relative)
        except Refused as refusal:
            return [refusal.diagnostic]
        return self._guarded(relative, lambda: self.check_bytes(data))

    def _guarded(self, relative, check):
        if relative in self._active or len(self._active) >= MAX_REFERENCE_DEPTH:
            return [Diagnostic("reference-cycle", "", f"{relative} is already being checked")]
        self._active.append(relative)
        try:
            return check()
        finally:
            self._active.pop()

    def load_reference(self, pointer, relative, expected_format, cited=None):
        """The valid document of ``expected_format`` that a member names.

        ``cited`` is the ``(sha256, bytes)`` the referring document states;
        when given, the bytes that are validated are the bytes that were
        hashed. Raises ``Refused`` with the diagnostic to put at ``pointer``.
        """
        try:
            data = self.read(relative)
        except Refused as refusal:
            found = refusal.diagnostic
            raise Refused(found.code, pointer, found.message) from refusal
        if cited is not None:
            sha256, size = cited
            if hashlib.sha256(data).hexdigest() != sha256 or len(data) != size:
                raise Refused(
                    "evidence-mismatch",
                    pointer,
                    f"{relative} has {len(data)} bytes and the digest "
                    f"{hashlib.sha256(data).hexdigest()}",
                )
        try:
            document = parse(data)
        except Refused as refusal:
            raise Refused(
                "reference-invalid", pointer, f"{relative}: {refusal.diagnostic}"
            ) from refusal
        form = document.get("format") if isinstance(document, dict) else None
        if form != expected_format:
            raise Refused(
                "reference-invalid", pointer, f"{relative} is {form!r}, not {expected_format}"
            )
        problems = self._guarded(relative, lambda: self.check_value(document))
        if problems:
            code = (
                "reference-cycle" if problems[0].code == "reference-cycle" else "reference-invalid"
            )
            raise Refused(code, pointer, f"{relative}: {problems[0]}")
        return document

    def hash_evidence(self, relative: str):
        """Digest and size of a file cited as evidence; it is never loaded."""
        if not contained(relative):
            raise Refused("path-escape", "", f"{relative!r} is not a path below the root")
        try:
            path = (self.root / relative).resolve(strict=True)
        except OSError as error:
            raise Refused("file-unreadable", "", f"{relative}: {error}") from error
        if not path.is_relative_to(self.root):
            raise Refused("path-escape", "", f"{relative} resolves outside the root")
        if not path.is_file():
            raise Refused("file-unreadable", "", f"{relative} is not a regular file")
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            while chunk := stream.read(1 << 16):
                digest.update(chunk)
                size += len(chunk)
        return digest.hexdigest(), size


def _check_schema(name, schema, files):
    """A schema file is a local, self-contained 2020-12 schema."""
    if not isinstance(schema, dict) or schema.get("$schema") != DIALECT:
        raise ContractsUnreadable(f"{name}: $schema must be {DIALECT}")
    Draft202012Validator.check_schema(schema)

    def walk(value):
        if isinstance(value, dict):
            if "$id" in value:
                raise ContractsUnreadable(f"{name}: schema files carry no $id")
            reference = value.get("$ref")
            if isinstance(reference, str):
                target = reference.split("#")[0]
                if target and target not in files:
                    raise ContractsUnreadable(f"{name}: $ref {reference!r} is not local")
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(schema)
