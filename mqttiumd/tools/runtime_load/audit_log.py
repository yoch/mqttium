#!/usr/bin/env python3
"""Check newline termination and contiguous event sequence over the retained journal."""
import argparse
import hashlib
import json
from pathlib import Path

from wire import write_json


def audit(path):
    rows, files = 0, []
    previous = None
    first_kind = last_kind = None
    for part in (path.with_name(path.name + ".previous"), path):
        if not part.exists() and part != path:
            continue
        count = 0
        digest = hashlib.sha256()
        with part.open("rb") as stream:
            for line in stream:
                digest.update(line)
                if not line.endswith(b"\n"):
                    raise ValueError(f"unterminated line {count + 1} of {part.name}")
                event = json.loads(line)
                if event["format"] != "mqttiumd.product-event":
                    raise ValueError("unexpected format")
                sequence = int(event["seq"])
                if previous is not None and sequence != previous + 1:
                    raise ValueError(f"sequence {previous} followed by {sequence}")
                if first_kind is None:
                    first_kind = event["kind"]
                last_kind = event["kind"]
                previous = sequence
                count += 1
        files.append({"file": part.name, "lines": count, "sha256": digest.hexdigest()})
        rows += count
    if not rows or first_kind != "starting" or last_kind != "stopped":
        raise ValueError("journal does not span a complete retained run")
    return {"rows": rows, "first_kind": first_kind, "last_kind": last_kind, "files": files}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("events", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    write_json(args.out, audit(args.events))
