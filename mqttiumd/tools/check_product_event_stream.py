#!/usr/bin/env python3
"""Bounded strict/schema check of product event streams, not a semantic oracle."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sys

from check_schemas import Refused, load_validators, parse_strict, schema_pointers


@dataclass(frozen=True)
class Limits:
    files: int = 256
    events: int = 100_000
    line_bytes: int = 65_536
    total_bytes: int = 128 * 1024 * 1024
    entries: int = 16_384


class StreamRefused(Exception):
    def __init__(self, code: str, **facts):
        self.facts = {"code": code, **facts}
        super().__init__(code)


def bounded(text: str, limit: int = 512) -> str:
    return text[:limit]


def is_stream(path: Path) -> bool:
    # Include the retained predecessor used by product log rotation. Client
    # observations and wire transcripts have other names/formats and are not
    # product streams. Explicit file arguments may use any name.
    name = path.name
    return (name in ("events.jsonl", "events.jsonl.previous") or ".events" in name) and (name.endswith(".jsonl") or name.endswith(".jsonl.previous"))


def event_files(roots: list[Path], limits: Limits) -> list[Path]:
    pending = list(roots)
    seen, selected, entries = set(), [], 0
    while pending:
        path = pending.pop()
        entries += 1
        if entries > limits.entries:
            raise StreamRefused("product_event_stream_budget", resource="directory_entries")
        if path.is_symlink():
            raise StreamRefused("product_event_stream_unavailable", file=bounded(str(path)), cause="symlink")
        if path.is_dir():
            with os.scandir(path) as directory:
                for entry in directory:
                    entries += 1
                    if entries > limits.entries:
                        raise StreamRefused("product_event_stream_budget", resource="directory_entries")
                    child = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(child)
                    elif is_stream(child):
                        pending.append(child)
        elif path.is_file():
            identity = path.resolve()
            if identity in seen:
                continue
            seen.add(identity)
            selected.append(path)
            if len(selected) > limits.files:
                raise StreamRefused("product_event_stream_budget", resource="files")
        else:
            raise StreamRefused("product_event_stream_unavailable", file=bounded(str(path)), cause="missing_or_not_regular")
    return sorted(selected)


def check_streams(roots: list[Path], *, limits: Limits = Limits(), validators=None) -> dict:
    result = {"format": "mqttiumd.product-event-stream-check", "schema_version": 1,
              "status": "FAIL", "scope": "strict-json-and-schema-only", "qualification": "none",
              "product_events": 0, "records": 0, "skipped_records": 0, "bytes": 0, "files": [],
              "schema": "contracts/schema/v1/product-event.schema.json",
              "schema_reader": "tools/check_schemas.py",
              "limits": {"files": limits.files, "events": limits.events,
                         "line_bytes": limits.line_bytes, "total_bytes": limits.total_bytes,
                         "directory_entries": limits.entries}}
    try:
        validators = load_validators() if validators is None else validators
        for path in event_files(roots, limits):
            location = bounded(str(path))
            record = {"path": location, "product_events": 0, "skipped_records": 0, "bytes": 0}
            result["files"].append(record)
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                line_number = 0
                while line := stream.readline(limits.line_bytes + 1):
                    line_number += 1
                    if len(line) > limits.line_bytes:
                        raise StreamRefused("product_event_stream_budget", resource="line_bytes", file=location, line=line_number)
                    result["bytes"] += len(line)
                    record["bytes"] += len(line)
                    if result["bytes"] > limits.total_bytes:
                        raise StreamRefused("product_event_stream_budget", resource="total_bytes", file=location, line=line_number)
                    digest.update(line)
                    if result["records"] >= limits.events:
                        raise StreamRefused("product_event_stream_budget", resource="events", file=location, line=line_number)
                    result["records"] += 1
                    try:
                        document = parse_strict(line)
                    except Refused as error:
                        raise StreamRefused("product_event_parse_refused", cause=error.code, file=location, line=line_number) from error
                    except RecursionError as error:
                        raise StreamRefused("product_event_parse_refused", cause="nesting_depth", file=location, line=line_number) from error
                    if not isinstance(document, dict) or document.get("format") != "mqttiumd.product-event":
                        result["skipped_records"] += 1
                        record["skipped_records"] += 1
                        continue
                    try:
                        pointers = schema_pointers(validators, document)
                    except Refused as error:
                        raise StreamRefused("product_event_schema_mismatch", cause=error.code,
                                            file=location, line=line_number, pointer="") from error
                    if pointers:
                        raise StreamRefused("product_event_schema_mismatch", file=location,
                                            line=line_number, pointer=bounded(pointers[0], 256))
                    result["product_events"] += 1
                    record["product_events"] += 1
            record["sha256"] = digest.hexdigest()
        if result["product_events"] == 0:
            raise StreamRefused("product_event_stream_empty")
    except StreamRefused as error:
        result["failure"] = error.facts
    except OSError:
        result["failure"] = {"code": "product_event_stream_unavailable", "cause": "io"}
    else:
        result["status"] = "PASS"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--max-files", type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.max_files <= 4096:
        parser.error("max-files must be between1 and4096")
    result = check_streams(args.paths, limits=Limits(files=args.max_files))
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.out is not None:
        args.out.write_text(encoded)
    print(encoded, end="")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
