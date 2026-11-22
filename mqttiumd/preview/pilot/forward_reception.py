#!/usr/bin/env python3
"""Receive bounded store-and-forward on the frozen, normal mTLS broker."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from common import (Broker, CAPTEURS, DASHBOARDS, PACKAGE, inventory, machine,
                    processes, provision, read_db, require, sha, wait_for, write)
from forward_client import body_for
from forward_store import (DASHBOARD_BYTES, HEADER, PRODUCER_BYTES, Event, footprint, snapshot)
from load import EventMeter


BROKER_SOURCE = "51be4079f7ee42f46500e993315eddc971f0a8d4"
BROKER_SHA = "9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119"
RATE = 50
SAMPLE_SECONDS = 1
CLIENT = Path(__file__).with_name("forward_client.py")


def require_binary(binary, package):
    build = json.loads((package / "BUILD.json").read_text())
    require(build["source_sha"] == BROKER_SOURCE and build["binary_sha256"] == BROKER_SHA
            and not build["instrumented"] and build["features"] == [] and sha(binary) == BROKER_SHA,
            "qualified_normal_binary_mismatch")
    return build


class Actor:
    def __init__(self, command, identity, path, pki, port, log, *, schedule=None, script=CLIENT, extra=()):
        self.identity = identity
        self.db = path
        self.log = log
        self.argv = [sys.executable, str(script), command, "--id", identity, "--db", str(path),
                     "--pki", str(pki), "--port", str(port), *extra]
        if schedule:
            self.argv += ["--schedule", str(schedule)]
        with log.open("xb") as output:
            self.child = subprocess.Popen(self.argv, stdout=output, stderr=output)

    def check(self):
        require(self.child.poll() is None, f"{self.identity}_exited_{self.child.returncode}:{self.log.name}")

    def facts(self, kind):
        for line in self.log.read_text().splitlines():
            if line.startswith("{"):
                try:
                    fact = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if fact.get("kind") == kind:
                    yield fact

    def ready(self, kind):
        self.check()
        return next(self.facts(kind), None)

    def stop(self, *, kill=False):
        chosen = signal.SIGKILL if kill else signal.SIGINT
        sent = self.child.poll() is None
        if sent:
            self.child.send_signal(chosen)
        forced = False
        try:
            status = self.child.wait(timeout=12)
        except subprocess.TimeoutExpired:
            forced = True
            self.child.kill()
            status = self.child.wait(timeout=5)
        return {"identity": self.identity, "pid": self.child.pid, "signalled": sent,
                "signal": chosen.name, "wait_status": status, "forced": forced, "argv": self.argv}


def atomically_write(path, data):
    temp = path.with_suffix(".tmp")
    write(temp, data)
    temp.replace(path)


def totals(stats):
    return {name: sum(row[name] for row in stats) for name in
            ("enqueued", "confirmed", "quota", "unknown", "rejected", "no_match", "attempts", "retries", "pending", "rows")}


def prove_delivery(sensors, dashboards, expected, cutoff):
    counts = [0, 0]
    latency = [Counter(), Counter()]
    offline_latency = [Counter(), Counter()]
    with read_db(dashboards[0].db) as first, read_db(dashboards[1].db) as second:
        def rows(db):
            return db.execute("SELECT sensor,stream,seq,wire,received_ns,received_mono FROM applied ORDER BY sensor,stream,seq")

        observed = [iter(rows(first)), iter(rows(second))]
        for sensor_index, state in enumerate(sensors):
            sensor = CAPTEURS[sensor_index]
            stream = bytes.fromhex(state["stream"])
            for seq in range(1, state["enqueued"] + 1):
                ordinal = (seq - 1) * len(CAPTEURS) + sensor_index
                require(ordinal < expected, "unexpected_sensor_sequence")
                pair = [next(cursor, None) for cursor in observed]
                require(all(row is not None and row[:3] == (sensor, stream, seq) for row in pair),
                        f"missing_or_wrong_event_identity:{sensor}/{seq}")
                require(pair[0][3] == pair[1][3], f"dashboard_payload_diverged:{sensor}/{seq}")
                event = Event.decode(f"atelier/{sensor}/telemetry", pair[0][3])
                body = body_for(ordinal)
                require(event.wire[HEADER.size:HEADER.size + len(body)] == body, f"unexpected_event_payload:{sensor}/{seq}")
                for index, row in enumerate(pair):
                    ms = math.ceil((row[4] - event.created_ns) / 1e6)
                    latency[index][ms] += 1
                    if ordinal < cutoff:
                        offline_latency[index][ms] += 1
                    counts[index] += 1
        require(all(next(cursor, None) is None for cursor in observed) and counts == [expected, expected],
                "extra_or_missing_applied_event")
    return {"verified_identities_by_dashboard": counts, "delay_ms": [percentiles(x) for x in latency],
            "outage_events_delay_ms": [percentiles(x) for x in offline_latency]}


def percentiles(histogram):
    n = sum(histogram.values())
    if not n:
        return {}
    seen = 0
    result = {"samples": n, "max": max(histogram)}
    for value, count in sorted(histogram.items()):
        seen += count
        for rank in (50, 95, 99):
            if seen >= math.ceil(n * rank / 100):
                result.setdefault(f"p{rank}", value)
    return result


class Campaign:
    def __init__(self, args, pki, receipt):
        self.args, self.out, self.pki = args, args.output, pki
        self.receipt = receipt
        self.actors = []
        self.sensors = []
        self.dashboards = []
        self.meter = None
        self.peak = {}
        self.history = []
        self.broker = None
        self.samples = None

    def actor(self, command, identity, suffix="", *, schedule=None):
        instance = Actor(command, identity, self.out / (identity + ".sqlite"), self.pki,
                         self.broker.port, self.out / (identity + suffix + ".jsonl"), schedule=schedule)
        self.actors.append(instance)
        return instance

    def sample(self, phase):
        require(self.broker.child.poll() is None, "normal_broker_exited")
        for actor in self.sensors + [a for a in self.dashboards if a is not None]:
            actor.check()
        sensors = [snapshot(actor.db) for actor in self.sensors]
        dashboards = [snapshot(self.out / (name + ".sqlite")) for name in DASHBOARDS]
        counts = totals(sensors)
        broker = inventory(self.broker.db)
        self.meter.pump()
        require(broker["invariants_ok"] and broker["limits"]["references"] == 128,
                "broker_reference_quota_or_accounting_changed")
        require(all(stats["occupied_bytes"] <= stats["budget"] for stats in sensors + dashboards),
                "application_sqlite_budget_exceeded")
        stamp = time.monotonic_ns()
        resources = {a.identity: processes(a.child.pid, a.db) for a in self.sensors + [a for a in self.dashboards if a]}
        resources["broker"] = processes(self.broker.child.pid, self.broker.db)
        for identity, resource in resources.items():
            peak = self.peak.setdefault(identity, {})
            for field in ("rss_kib", "cpu_user_s", "cpu_system_s", "write_bytes", "wchar"):
                peak[field] = max(peak.get(field, 0), resource[field])
            for field, val in resource["disk"].items():
                peak["peak_" + field] = max(peak.get("peak_" + field, 0), val)
        for state in sensors + dashboards:
            peak = self.peak.setdefault(state["identity"], {})
            for field in ("occupied_bytes", "pending", "oldest_age_ms", "page_count"):
                peak["peak_" + field] = max(peak.get("peak_" + field, 0), state.get(field, 0))
        row = {"phase": phase, "monotonic_ns": stamp, "sensors": sensors,
               "dashboards": dashboards, "total": counts, "broker": broker, "resources": resources}
        self.samples.write(json.dumps(row, sort_keys=True) + "\n")
        self.samples.flush()
        if phase == "catchup":
            self.history.append((stamp, counts["enqueued"], counts["confirmed"], counts["pending"]))
        return row

    def begin(self):
        self.broker = Broker(self.args.binary, self.out / "broker.sqlite", self.out, self.pki)
        self.meter = EventMeter(self.broker.events)
        self.samples = (self.out / "samples.jsonl").open("x", buffering=1)
        self.dashboards = [self.actor("dashboard", name) for name in DASHBOARDS]
        for dashboard in self.dashboards:
            record = wait_for(lambda: dashboard.ready("dashboard_ready"), "dashboard_no_suback")
            require(not record["session_present"], "unexpected_initial_dashboard_session")
        stopped = self.dashboards[1].stop()
        require(stopped["wait_status"] == 0, "offline_dashboard_did_not_stop")
        self.receipt["offline_dashboard_process"] = stopped
        self.offline_start = time.monotonic_ns()
        self.dashboards[1] = None
        self.schedule = self.out / "schedule.json"
        self.sensors = [self.actor("producer", name, schedule=self.schedule) for name in CAPTEURS]
        for sensor in self.sensors:
            wait_for(lambda: sensor.ready("producer_ready"), "producer_not_ready")
        self.start = time.monotonic_ns() + 500_000_000
        self.cutoff = self.args.outage * RATE
        self.maximum = self.cutoff if self.args.short else (self.args.outage + self.args.catchup_limit + 10) * RATE
        atomically_write(self.schedule, {"start_ns": self.start, "events": self.maximum, "rate": RATE})

    def until(self, phase, predicate, deadline):
        report = 0
        while True:
            state = self.sample(phase)
            now = state["monotonic_ns"]
            if predicate(state):
                return state
            require(now < deadline, f"{phase}_deadline_exceeded")
            if now >= report:
                print(json.dumps({"phase": phase, "elapsed_s": round((now - self.start) / 1e9, 2),
                                  "total": state["total"], "dashboard_applied": [d["unique_count"] for d in state["dashboards"]]}),
                      flush=True)
                report = now + 60_000_000_000
            time.sleep(SAMPLE_SECONDS)

    def receive(self):
        self.begin()
        outage_end = self.start + self.args.outage * 1_000_000_000
        at_outage = self.until("outage", lambda s: s["monotonic_ns"] >= outage_end and s["total"]["enqueued"] >= self.cutoff,
                               outage_end + 5_000_000_000)
        require(at_outage["total"]["quota"] > 0 and at_outage["total"]["pending"] > 0
                and at_outage["dashboards"][1]["unique_count"] == 0, "outage_quota_or_local_backlog_not_exercised")
        self.receipt["outage"] = at_outage
        self.dashboards[1] = self.actor("dashboard", DASHBOARDS[1], "-resumed")
        ready = wait_for(lambda: self.dashboards[1].ready("dashboard_ready"), "offline_dashboard_did_not_resume")
        require(ready["session_present"], "offline_dashboard_session_not_present")
        returned = ready["monotonic_ns"]
        self.receipt["dashboard_return"] = {**ready, "offline_s": (returned - self.offline_start) / 1e9,
                                            "outage_boundary_ns": outage_end}
        deadline = returned + self.args.catchup_limit * 1_000_000_000
        def caught_up(state):
            backlog = state["total"]["pending"]
            lag = state["total"]["enqueued"] - min(d["unique_count"] for d in state["dashboards"])
            return backlog <= (0 if self.args.short else len(CAPTEURS)) and lag <= (0 if self.args.short else 2 * len(CAPTEURS))

        caught = self.until("catchup", caught_up, deadline)
        elapsed = (caught["monotonic_ns"] - returned) / 1e9
        require(elapsed > 0, "catchup_duration_invalid")
        self.receipt["catchup"] = {"seconds": elapsed, "counts": caught["total"],
                                   "admission_rate": (caught["total"]["confirmed"] - at_outage["total"]["confirmed"]) / elapsed,
                                   "generation_rate": (caught["total"]["enqueued"] - at_outage["total"]["enqueued"]) / elapsed,
                                   "best_backlogged_60s": self.best_minute()}
        self.receipt["catchup"]["net_rate"] = self.receipt["catchup"]["admission_rate"] - self.receipt["catchup"]["generation_rate"]
        if self.args.short:
            expected = self.cutoff
        else:
            # Leave generation running through the observation of catch-up.
            # Every sensor independently reaches the same scheduled cutoff.
            expected = min(self.maximum, (time.monotonic_ns() + 2_000_000_000 - self.start) * RATE // 1_000_000_000)
            atomically_write(self.schedule.with_suffix(".stop.json"), {"events": expected})
        def drained(state):
            return state["total"]["enqueued"] == state["total"]["confirmed"] == expected and state["total"]["rows"] == 0 and all(
                d["unique_count"] == expected for d in state["dashboards"]) and not any(
                state["broker"]["counts"][name] for name in ("obligations", "outbound", "wills"))

        final = self.until("drained", drained, time.monotonic_ns() + 40_000_000_000)
        self.receipt["final"] = final
        self.receipt["delivery"] = prove_delivery(final["sensors"], self.dashboards, expected, self.cutoff)
        require(final["total"]["unknown"] == 0 and final["total"]["rejected"] == 0 and final["total"]["no_match"] == 0,
                "unexpected_normal_admission_outcome")
        require(all(d["conflicts"] == d["invalid"] == d["duplicates"] == 0 for d in final["dashboards"]),
                "normal_dashboard_unexpected_message")
        require(self.meter.gaps == self.meter.duplicate_commits == 0, "broker_event_gap_or_duplicate_operation")
        require(self.meter.counts["Admission_committed"] == expected
                and self.meter.counts["Finish_committed"] == 2 * expected,
                "broker_admission_or_finish_count_mismatch")
        require(all(len(list(sensor.facts("producer_ready"))) == 1 for sensor in self.sensors)
                and all(len(list(dashboard.facts("dashboard_ready"))) == 1 for dashboard in self.dashboards),
                "unexpected_reconnect_during_normal_workload")
        if not self.args.short:
            require(self.receipt["catchup"]["net_rate"] > 0 and self.receipt["catchup"]["counts"]["enqueued"] > self.cutoff,
                    "no_net_catchup_under_live_generation")

    def best_minute(self):
        best = None
        end = 0
        for start in self.history:
            while end < len(self.history) and self.history[end][0] - start[0] < 60_000_000_000:
                end += 1
            if end == len(self.history):
                break
            last = self.history[end]
            if last[3] < RATE:
                continue
            elapsed = (last[0] - start[0]) / 1e9
            result = {"seconds": elapsed, "admission_rate": (last[2] - start[2]) / elapsed,
                      "generation_rate": (last[1] - start[1]) / elapsed,
                      "backlog_start": start[3], "backlog_end": last[3], "start_ns": start[0], "end_ns": last[0]}
            if best is None or result["admission_rate"] > best["admission_rate"]:
                best = result
        return best

    def close(self):
        self.receipt["processes"] = [actor.stop() for actor in self.actors]
        self.receipt["application_final_metrics"] = {
            actor.identity: next(actor.facts("outbox_final" if actor in self.sensors else "inbox_final"), None)
            for actor in self.actors
        }
        self.receipt["source_events_offered"] = sum(
            (self.receipt["application_final_metrics"][actor.identity] or {}).get("source_offered", 0)
            for actor in self.sensors
        )
        if self.broker:
            self.receipt["broker_process"] = self.broker.stop()
        if self.samples:
            self.samples.close()
        if self.meter:
            self.meter.pump()
            self.meter.close()
            self.receipt["broker_events"] = {"counts": dict(self.meter.counts), "outcomes": dict(self.meter.outcomes),
                                             "gaps": self.meter.gaps, "runtime": self.meter.metadata}
        self.receipt["peaks"] = self.peak
        self.receipt["application_rest_footprint"] = {a.identity: footprint(a.db) for a in self.actors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    parser.add_argument("--short", action="store_true", help="500 events, then restore and drain")
    parser.add_argument("--catchup-limit", type=int, default=3600, help="seconds allowed with generation continuing")
    args = parser.parse_args()
    args.outage = 10 if args.short else 1800
    require(60 <= args.catchup_limit <= 3600, "catchup_limit_out_of_bounds")
    os.umask(0o077)
    args.output = args.output.resolve()
    args.output.mkdir(mode=0o700)
    receipt = {"status": "FAIL", "expected": {"rate": RATE, "outage_s": args.outage, "payload_bytes": 256,
               "outage_events": args.outage * RATE, "catchup_limit_s": args.catchup_limit,
               "producer_budget_bytes": PRODUCER_BYTES, "dashboard_budget_bytes": DASHBOARD_BYTES},
               "broker_build": require_binary(args.binary, args.package), "machine": machine(args.output),
               "implementation": {name: sha(Path(__file__).with_name(name)) for name in
                                  ("forward_store.py", "forward_client.py", "forward_reception.py", "common.py", "load.py")}}
    with tempfile.TemporaryDirectory(prefix="mqttiumd-forward-pki-") as path:
        pki = Path(path) / "pki"
        provision(pki, args.package)
        receipt["policy_sha256"] = sha(pki / "identities.json")
        campaign = Campaign(args, pki, receipt)
        try:
            campaign.receive()
            receipt["status"] = "PASS"
        except (Exception, KeyboardInterrupt) as error:
            receipt["failure"] = repr(error)
        finally:
            campaign.close()
            if any(p["wait_status"] != 0 for p in receipt["processes"]) or receipt.get("broker_process", {}).get("wait_status") != 0:
                receipt["status"] = "FAIL"
                receipt.setdefault("failure", "graceful_actor_exit_failed")
            if receipt["status"] == "PASS" and receipt["final"]["total"]["enqueued"] != receipt["source_events_offered"]:
                receipt.update(status="FAIL", failure="generated_and_accepted_count_mismatch")
            write(args.output / "reception.json", receipt)
    print(json.dumps({"status": receipt["status"], "failure": receipt.get("failure"),
                      "receipt": str(args.output / "reception.json")}), flush=True)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
