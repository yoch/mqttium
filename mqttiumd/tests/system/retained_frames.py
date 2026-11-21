"""Recompute the terminal outcomes of retained socket effects."""

import argparse
from collections import Counter
import json
from pathlib import Path

from tcp_probe import read_events, require


TERMINALS = {"frame_written", "frame_failed", "message_transmission_not_started"}


def verify_retained_frames(events, *, generation=None):
    """Use a complete journal so observations before the refusal remain visible."""
    require(events and events[0]["kind"] == "starting", "retained_frame_journal_missing_start")
    tracked = {}
    pid = events[0]["pid"]
    for sequence, event in enumerate(events, 1):
        require(int(event["seq"]) == sequence and event["pid"] == pid,
                "retained_frame_journal_discontinuous", str(sequence))
        details = event.get("details", {})
        if event["kind"] != "effect_retained" or details.get("code") != "output_queue_full":
            continue
        connection = event.get("connection")
        if generation is not None and connection != generation:
            continue
        effect, kind = details.get("effect"), details.get("packet_kind")
        require(connection and effect and kind, "retained_frame_missing_identity", str(sequence))
        key = (connection, effect)
        row = tracked.setdefault(key, {"generation": connection, "effect": effect, "packet_kind": kind,
                                       "retained_sequences": []})
        require(row["packet_kind"] == kind, "retained_frame_kind_changed", repr(key))
        row["retained_sequences"].append(sequence)

    for sequence, event in enumerate(events, 1):
        details = event.get("details", {})
        key = (event.get("connection"), details.get("effect"))
        row = tracked.get(key)
        kind = event["kind"]
        if row is None or kind not in TERMINALS | {"frame_queued"}:
            continue
        require(sequence > row["retained_sequences"][-1],
                "retained_frame_observation_before_retention", repr(key))
        require("terminal" not in row, "retained_frame_observation_after_terminal", repr(key))
        if kind == "message_transmission_not_started":
            compatible = row["packet_kind"] == "PUBLISH" and "queued_sequence" in row
        else:
            compatible = details.get("packet_kind") == row["packet_kind"] and details.get("qos") != 0
        require(compatible, "retained_frame_incompatible_kind", f"{key!r} {kind}")
        if kind == "frame_queued":
            require("queued_sequence" not in row, "retained_frame_queued_twice", repr(key))
            row["queued_sequence"] = sequence
        else:
            if kind == "frame_written" and row["packet_kind"] == "PUBLISH":
                require("queued_sequence" in row, "retained_publish_written_before_queue", repr(key))
            row["terminal"] = {"kind": kind, "sequence": sequence,
                               "code": details.get("code", details.get("reason"))}

    outcomes = Counter()
    for key, row in tracked.items():
        require("terminal" in row, "retained_frame_missing_terminal", repr(key))
        outcomes[row["terminal"]["kind"]] += 1
    return {"outcomes": dict(outcomes),
            "repeat_refusals": sum(len(row["retained_sequences"]) - 1 for row in tracked.values()),
            "effects": list(tracked.values())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_retained_frames(read_events(args.journal)), indent=2))
