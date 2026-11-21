#!/usr/bin/env python3
"""Separate outbox COMMIT, LIVE scheduling and receive time on a verified pilot copy."""
from __future__ import annotations

import argparse
import atexit
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic_ns


FIRST, UNTIL = 300, 800
TRACE_DIR = "MQTTIUMD_LIVE_TIMING_DIR"
PROBE = "_live_timing.py"
rows = []


def flush():
    directory = Path(os.environ[TRACE_DIR])
    with (directory / f"{os.getpid()}.jsonl").open("x") as output:
        for row in rows:
            output.write(json.dumps(row) + "\n")


def observe(point, event, *timestamps):
    ordinal = (event.sequence - 1) * 8 + int(event.sensor.removeprefix("capteur-")) - 1
    if not FIRST <= ordinal < UNTIL:
        return
    if not rows:
        atexit.register(flush)
    rows.append([event.event_id, ordinal, point, *timestamps])


def edit(path, pairs):
    source = path.read_text()
    for original, timed in pairs:
        if source.count(original) != 1:
            raise ValueError(f"unexpected_probe_anchor:{path.name}:{original!r}")
        source = source.replace(original, timed, 1)
    path.write_text(source)


def instrument(package):
    pilot = package / "pilot"
    (pilot / PROBE).write_bytes(Path(__file__).read_bytes())
    edit(pilot / "forward_store.py", [
        ("from __future__ import annotations\n",
         "from __future__ import annotations\nfrom _live_timing import monotonic_ns, observe\n"),
        ("    def transaction(self):", "    def transaction(self, commit_trace=None):"),
        ('                yield\n                self.db.execute("COMMIT")',
         '                yield\n'
         '                if commit_trace is not None:\n'
         '                    commit_trace.append(monotonic_ns())\n'
         '                self.db.execute("COMMIT")\n'
         '                if commit_trace is not None:\n'
         '                    commit_trace.append(monotonic_ns())'),
        ("    def enqueue(self, body):\n        with self.transaction():",
         "    def enqueue(self, body):\n"
         "        entered = monotonic_ns()\n"
         "        commit_trace = []\n"
         "        with self.transaction(commit_trace):"),
        ("            event = Event.encode(self.identity, stream, seq, time.time_ns(), body)",
         "            created = monotonic_ns()\n"
         "            event = Event.encode(self.identity, stream, seq, time.time_ns(), body)"),
        ('            self.db.execute("UPDATE meta SET enqueued=?", (seq,))\n        return event',
         '            self.db.execute("UPDATE meta SET enqueued=?", (seq,))\n'
         '        observe("outbox", event, entered, created, *commit_trace)\n        return event'),
    ])
    edit(pilot / "split_client.py", [
        ("from __future__ import annotations\n",
         "from __future__ import annotations\nfrom _live_timing import monotonic_ns, observe\n"),
        ('                    sent = wire.client.publish(event.topic + "/live",',
         '                    observe("publish", event, monotonic_ns())\n'
         '                    sent = wire.client.publish(event.topic + "/live",'),
        ('    def observed(client, _data, message):\n        if message.topic.endswith("/status"):',
         '    def observed(client, _data, message):\n        callback = monotonic_ns()\n'
         '        if message.topic.endswith("/status"):'),
        ("            display.observe(event, args.port, message)",
         "            display.observe(event, args.port, message, callback)"),
        ("    def observe(self, event, port, packet):", "    def observe(self, event, port, packet, callback):"),
        ('            emit("live_received",', '            received = time.time_ns()\n'
         '            observe("callback", event, callback, event.created_ns, received)\n'
         '            emit("live_received",'),
        ("created_ns=event.created_ns, received_ns=time.time_ns())",
         "created_ns=event.created_ns, received_ns=received)"),
    ])
    edit(pilot / "rc_reception.py", [
        ('        window = recovered["metrics"]["live"]["window"]\n',
         '        window = recovered["metrics"]["live"]["window"]\n'
         '        self.result["measured_gate"] = {**window, "ordinal_range": [first, until],\n'
         '            "invalid_latency": recovered["metrics"]["live"]["invalid_latency"]}\n'),
        ('        resumed = self.planned_broker("history", recovered)',
         '        return  # End the diagnostic copy at the measured gate.\n'
         '        resumed = self.planned_broker("history", recovered)'),
    ])


def rank99(values):
    return sorted(values)[math.ceil(len(values) * .99) - 1] if values else None


def correlate(raw):
    joined = {}
    for trace in sorted(raw.glob("*.jsonl")):
        for line in trace.read_text().splitlines():
            event, ordinal, point, *timestamps = json.loads(line)
            target = joined.setdefault(event, {"event_id": event, "ordinal": ordinal})
            if point in target or target["ordinal"] != ordinal:
                raise ValueError(f"inconsistent_live_timing:{event}:{point}")
            target[point] = timestamps
    return sorted(joined.values(), key=lambda event: event["ordinal"])


def latencies(events):
    results = []
    for event in events:
        values = {}
        if "outbox" in event:
            entered, created, start, committed = event["outbox"]
            values.update(outbox_total_ms=(committed - entered) / 1e6,
                          creation_to_commit_ms=(committed - created) / 1e6,
                          sqlite_commit_ms=(committed - start) / 1e6)
        if "outbox" in event and "publish" in event:
            sent, = event["publish"]
            values["commit_to_live_publish_ms"] = (sent - committed) / 1e6
        if "callback" in event:
            callback, created_wall, received_wall = event["callback"]
            lag = (received_wall - created_wall) / 1e6
            values["end_to_end_ms"] = lag
            values["gate_bucket_ms"] = max(0, min(4001, math.ceil(lag)))
            if "publish" in event:
                values["publish_to_callback_ms"] = (callback - event["publish"][0]) / 1e6
        if any(value < 0 for key, value in values.items() if key != "end_to_end_ms"):
            raise ValueError(f"nonmonotone_live_timing:{event['event_id']}")
        results.append({**event, "ms": values})
    return results


def main():
    from preview_archive import checked_archive, digest

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    out = args.out.resolve()
    out.mkdir()
    raw = out / "traces"
    raw.mkdir()
    with TemporaryDirectory(dir=out, prefix="diagnostic-package-") as scratch:
        with checked_archive(args.archive, args.sha256) as (tar, root, _):
            tar.extractall(scratch, filter="data")
        package = Path(scratch) / root
        source = json.loads((package / "PACKAGE.json").read_text())["package_source_sha"]
        original = {name: digest(package / "pilot" / name)
                    for name in ("forward_store.py", "split_client.py", "rc_reception.py")}
        instrument(package)
        command = [sys.executable, str(package / "pilot" / "rc_reception.py"), str(out / "site"),
                   "--duration", "16", "--outage", "10", "--catchup-limit", "60", "--source", source]
        with (out / "runner.log").open("x") as log:
            result = subprocess.run(command, env=dict(os.environ, **{TRACE_DIR: str(raw)}),
                                    stdout=log, stderr=log, timeout=150)
    data = json.loads((out / "site" / "result.json").read_text())
    merged = latencies(correlate(raw))
    metrics = {}
    for name in sorted({name for event in merged for name in event["ms"]}):
        values = [event["ms"][name] for event in merged if name in event["ms"]]
        metrics[name] = {"count": len(values), "p99": rank99(values), "maximum": max(values)}
    gate = data.get("measured_gate")
    calculated = metrics.get("gate_bucket_ms", {"count": 0, "p99": None})
    summary = {"archive_sha256": args.sha256, "probe_sha256": digest(Path(__file__)),
               "original_components": original, "package_source": source, "workload": data["workload"],
               "platform": data["platform"], "broker": data["broker"], "gate": gate,
               "gate_exit_status": result.returncode, "failure": data.get("failure"), "stages_ms": metrics}
    (out / "timing.json").write_text(json.dumps(summary, indent=2) + "\n")
    (out / "events.jsonl").write_text("".join(json.dumps(row) + "\n" for row in merged))
    if not gate or (gate["received"], gate["p99_ms_rounded_up"]) != (calculated["count"], calculated["p99"]):
        raise ValueError("event_timing_cannot_reproduce_site_window")
    print(json.dumps({"gate": gate, "failure": data.get("failure"), "milliseconds": metrics}, indent=2))
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
