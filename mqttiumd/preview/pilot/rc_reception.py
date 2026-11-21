#!/usr/bin/env python3
"""Measured deployment, outage, supervision and recovery reception for the pilot."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

from common import PACKAGE, machine, require, sha, topic, wait_for, write
from forward_client import body_for
from forward_reception import require_binary
from forward_store import Event, HEADER, snapshot
from operate import request, schedule
from segment_store import open_existing
from site_health import probe
from site_ops import (DEFAULTS, CONSUMERS, PRODUCERS, ROLES, Site, atomic_json, document, init_site, tls_context)
import site_backup


RATE = 50
CLIENT = Path(__file__).resolve().with_name("operate.py")
FILES = ("operate.py", "site_ops.py", "site_health.py", "site_supervisor.py", "site_backup.py",
         "split_client.py", "forward_client.py", "forward_store.py", "segment_store.py", "segment_admin.py",
         "split_reception.py", "common.py", "rc_reception.py")


def ports():
    with ExitStack() as stack:
        handles = {lane: stack.enter_context(socket.socket()) for lane in ("live", "history")}
        for handle in handles.values():
            handle.bind(("127.0.0.1", 0))
        return {lane: handle.getsockname()[1] for lane, handle in handles.items()}


def cleanup(proc):
    if proc is None:
        return None
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        return proc.wait(timeout=35)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.wait(timeout=5)


def coverage(site, expected):
    sources = {name: snapshot(site.db(name)) for name in PRODUCERS}
    require(sum(row["confirmed"] for row in sources.values()) == expected
            and all(r["pending"] == r["rows"] == 0 for r in sources.values()), "source_frontiers_not_drained")
    counts, digests = {}, {}
    for role in CONSUMERS:
        digest = hashlib.sha256()
        with open_existing(site.db(role)) as inbox:
            rows = iter(inbox.records())
            for index, sensor in enumerate(PRODUCERS):
                record = sources[sensor]
                for sequence in range(1, record["enqueued"] + 1):
                    observed_name, stream, seq, wire, *_ = next(rows)
                    require((observed_name, stream.hex(), seq) == (sensor, record["stream"], sequence),
                            "history_event_identity_missing")
                    item = Event.decode(topic(sensor), wire)
                    body = body_for((sequence - 1) * len(PRODUCERS) + index)
                    require(item.wire[HEADER.size:HEADER.size + len(body)] == body, "history_event_payload_diverged")
                    digest.update(item.wire)
            require(next(rows, None) is None and inbox.state()["unique_count"] == expected, "extra_history_events")
            counts[role] = inbox.state()["unique_count"]
            digests[role] = digest.hexdigest()
    require(len(set(digests.values())) == 1, "paired_history_digests_diverge")
    return {"counts": counts, "envelope_sha256": next(iter(digests.values())),
            "streams": {n: row["stream"] for n, row in sources.items()}}


class Campaign:
    def __init__(self, args, data, result):
        self.args, self.data, self.result = args, data, result
        self.site = None
        self.actor = None
        self.name = "supervised"
        self.samples = None
        self.sample_at = 0
        self.last_notice = 0
        self.interval = max(5, args.duration // 1200)
        self.process = {}
        self.results = result.setdefault("phases", [])

    def start(self, site, window=None):
        self.site = site
        argv = [sys.executable, str(CLIENT), "--package", str(site.package), "run", str(site.root)]
        if window:
            argv += ["--observe-ordinals", *map(str, window)]
        with (self.args.output / (self.name + ".log")).open("xb") as out:
            self.actor = subprocess.Popen(argv, stdout=out, stderr=out)
        active = self.until(lambda row: len(row["actors"]) == len(ROLES)
                            and all(a["ready"] for a in row["actors"].values())
                            and all(lane["mqtt_probe"] for lane in row["metrics"]["brokers"].values()),
                            "ready", 45)
        self.add("ready", active)
        return active

    def sampled(self, row):
        stamp = row["monotonic_ns"]
        if stamp < self.sample_at:
            return
        self.samples.write(json.dumps(row, sort_keys=True) + "\n")
        self.samples.flush()
        self.sample_at = stamp + self.interval * 1_000_000_000
        for role, metrics in row["metrics"].get("services", {}).items():
            key = f"{role}:{metrics['pid']}"
            latest = self.process.setdefault(key, {})
            for k in ("rss_kib", "cpu_system_s", "cpu_user_s", "wchar", "write_bytes"):
                latest[k] = max(latest.get(k, 0), metrics[k])
            for k, value in metrics["disk"].items():
                latest[f"peak_{k}_bytes"] = max(latest.get(f"peak_{k}_bytes", 0), value)

    def status(self, label):
        require(self.actor.poll() is None, f"supervisor_exited:{self.actor.returncode}:{label}")
        data = self.site.status()
        require(data["phase"] not in ("failed",), f"site_failed:{label}")
        if data.get("pid") != self.actor.pid or data["phase"] != "running" or "history" not in data["metrics"]:
            return None
        require(not any(role["suspended"] for role in data["actors"].values()), f"service_suspended:{label}")
        require(all(lane["invariants_ok"] for lane in data["metrics"]["brokers"].values()),
                f"broker_invariant_failure:{label}")
        require(not data["metrics"].get("collection_error"), f"collection_failure:{label}")
        self.sampled(data)
        now = time.monotonic()
        if now > self.last_notice + 60:
            print(json.dumps({"phase": label, "durable_enqueued": data["metrics"]["history"]["enqueued"],
                              "pending": data["metrics"]["history"]["pending"],
                              "live": data["metrics"]["live"]["received"]}), flush=True)
            self.last_notice = now
        return data

    def until(self, check, label, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            data = self.status(label)
            if data and check(data):
                return data
            time.sleep(.1)
        raise ValueError(f"reception_deadline:{label}")

    def add(self, phase, row, **details):
        self.results.append({"phase": phase, "observed_mono_ns": row["monotonic_ns"],
                             "supervisor_pid": row["pid"], "actors": row["actors"],
                             "enqueued": row["metrics"]["history"]["enqueued"],
                             "confirmed": row["metrics"]["history"]["confirmed"],
                             "pending": row["metrics"]["history"]["pending"],
                             "live": row["metrics"]["live"]["received"], **details})

    def drained(self, expected, label):
        def check(row):
            metrics = row["metrics"]
            hist = metrics["history"]
            return (hist["enqueued"] == hist["confirmed"] == expected
                    and all(s["rows"] == 0 for s in hist["sources"].values())
                    and all(s["unique_count"] == expected for s in hist["ledgers"].values())
                    and metrics["brokers"]["history"]["counts"]["obligations"] == 0)
        return self.until(check, label, self.args.catchup_limit)

    @staticmethod
    def role_pids(row):
        return {k: value["pid"] for k, value in row["actors"].items()}

    def refused_identities(self):
        for lane in ("live", "history"):
            try:
                probe(tls_context(self.site, lane, PRODUCERS[0]), self.site.config["ports"][lane],
                      client_id=PRODUCERS[1])
            except ValueError as error:
                require(str(error) == "health_connack_refused:0087", f"unexpected_acl_failure:{lane}:{error}")
            else:
                raise ValueError(f"cross_identity_connection_accepted:{lane}")
        self.result["auth_refusal_both_lanes"] = "MQTT5 0x87 with valid TLS client certificate"

    def planned_broker(self, lane, baseline):
        role = "broker-" + lane
        before = self.role_pids(baseline)
        paused = request(self.site, "pause", role)
        require(paused["actors"][role]["pid"] is None and paused["actors"][role]["last_exit"] == 0,
                "planned_stop_not_observed")
        hist = baseline["metrics"]["history"]["confirmed"]
        live = baseline["metrics"]["live"]["received"]
        active_lane = (lambda row: row["metrics"]["live"]["received"] >= live + 20) if lane == "history" else (
            lambda row: row["metrics"]["history"]["confirmed"] >= hist + 20)
        observed = self.until(lambda row: row["actors"][role]["paused"] and active_lane(row),
                              f"{lane}_stopped_other_lane_progress", 12)
        self.add(f"{lane}_stopped_other_lane_progress", observed, expected_interruption=True)
        request(self.site, "resume", role)
        resumed = self.until(lambda row: row["actors"][role]["ready"] and row["actors"][role]["pid"] != before[role]
                             and row["metrics"]["brokers"][lane]["mqtt_probe"], f"{lane}_resume", 25)
        require(all(pid == self.role_pids(resumed)[k] for k, pid in before.items() if k != role),
                "unrelated_service_restarted")
        self.add(f"{lane}_restarted", resumed)
        return resumed

    def producer_cut(self, baseline):
        role = PRODUCERS[0]
        pids = self.role_pids(baseline)
        identity = pids[role]
        status = Path(f"/proc/{identity}/stat").read_text().rpartition(") ")[2].split()
        require(int(status[1]) == self.actor.pid, "can_only_kill_test_supervisor_child")
        os.kill(identity, signal.SIGKILL)
        row = self.until(lambda state: state["actors"][role]["ready"] and state["actors"][role]["pid"] != identity,
                         "producer_respawned", 25)
        require(row["actors"][role]["last_exit"] == -signal.SIGKILL and all(
            self.role_pids(row)[k] == v for k,v in pids.items() if k != role), "producer_restart_not_isolated")
        self.add("producer_killed_and_restarted", row, killed_pid=identity, expected_interruption=True)
        return row

    def main_run(self):
        with (self.args.output / "samples.jsonl").open("x") as self.samples:
            self.run_and_restore()

    def run_and_restore(self):
        args = self.args
        first = (args.duration - args.outage) * RATE
        until = args.duration * RATE
        budgets = {**DEFAULTS, "segment_rows": 32} if args.duration < 90 else DEFAULTS
        site = init_site(self.data / "primary", args.package, ports(), None, demo=True, budgets=budgets)
        self.result["application_budgets"] = budgets
        self.start(site, (first, until))
        self.refused_identities()
        plan = schedule(site, until + (args.catchup_limit + 120) * RATE, RATE)
        start = plan["start_ns"]
        self.result["schedule"] = plan
        advance = (first * 1_000_000_000 // RATE) - 700_000_000
        self.until(lambda row: time.monotonic_ns() >= start + advance, "nominal", args.duration - args.outage + 5)
        gone = time.monotonic_ns()
        paused = request(site, "pause", CONSUMERS[1])
        require(paused["actors"][CONSUMERS[1]]["paused"], "history_dashboard_not_paused")
        at_cut = self.until(lambda row: row["monotonic_ns"] >= max(start + args.duration * 1_000_000_000,
                                                                  gone + args.outage * 1_000_000_000),
                            "history_saturated", args.outage + 8)
        sat = at_cut["metrics"]["history"]
        require(sat["pending"] > 0 and sum(s["quota"] for s in sat["sources"].values()) > 0
                and at_cut["metrics"]["brokers"]["history"]["accounts"]["references"] >= 127,
                "history_capacity_saturation_not_observed")
        self.add("history_saturated", at_cut, sustained_s=(time.monotonic_ns() - gone) / 1e9,
                 references=at_cut["metrics"]["brokers"]["history"]["accounts"]["references"])
        request(site, "resume", CONSUMERS[1])
        recovered = self.until(lambda row: row["actors"][CONSUMERS[1]]["ready"]
                               and row["metrics"]["history"]["pending"] <= 25,
                               "catchup", args.catchup_limit)
        ready_h = recovered["metrics"]["history"]
        elapsed = (recovered["monotonic_ns"] - at_cut["monotonic_ns"]) / 1e9
        require(elapsed > 0, "catchup_window_invalid")
        rate = (ready_h["confirmed"] - sat["confirmed"]) / elapsed
        generation = (ready_h["enqueued"] - sat["enqueued"]) / elapsed
        require(rate > generation and generation >= 45, "no_live_generation_or_net_catchup")
        window = recovered["metrics"]["live"]["window"]
        require(window["received"] >= args.outage * RATE * .999 and window["p99_ms_rounded_up"] <= 250
                and window["max_gap_ms"] <= 2000 and recovered["metrics"]["live"]["invalid_latency"] == 0,
                "live_slo_during_history_pressure")
        self.add("history_caught_up", recovered, admissions_per_s=rate, generated_per_s=generation,
                 net_per_s=rate-generation, duration_s=elapsed, live_window=window, quota_replies=sum(
                     s["quota"] for s in ready_h["sources"].values()))
        resumed = self.planned_broker("history", recovered)
        resumed = self.planned_broker("live", resumed)
        respawned = self.producer_cut(resumed)
        before = self.role_pids(respawned)
        request(site, "pause", CONSUMERS[0])
        request(site, "resume", CONSUMERS[0])
        respawned = self.until(lambda row: row["actors"][CONSUMERS[0]]["ready"]
                               and row["actors"][CONSUMERS[0]]["pid"] != before[CONSUMERS[0]],
                               "history_consumer_reconnected", 30)
        require(all(self.role_pids(respawned)[k] == pid for k, pid in before.items() if k != CONSUMERS[0]),
                "history_consumer_restart_not_isolated")
        self.add("history_consumer_restarted", respawned)

        stop = site.runtime / "schedule.stop.json"
        end = (time.monotonic_ns() - start) * RATE // 1_000_000_000 + 2 * RATE
        atomic_json(stop, {"events": end})
        drained = self.drained(end, "before_charged_backup")
        self.result["observed_generation_s"] = (drained["monotonic_ns"] - start) / 1e9
        self.add("all_responsibilities_drained", drained)
        self.result["normal_metrics"] = drained["metrics"]

        request(site, "pause", CONSUMERS[1])
        charged = end + 400
        atomic_json(stop, {"events": charged})
        pressure = self.until(lambda row: row["metrics"]["history"]["enqueued"] == charged
                              and row["metrics"]["history"]["pending"] > 200
                              and row["metrics"]["brokers"]["history"]["counts"]["obligations"] > 0,
                              "charged_backup", 30)
        self.add("charged_backup", pressure)
        request(site, "stop")
        require(self.actor.wait(20) == 0, "primary_supervisor_failed_shutdown")
        self.actor = None
        bundle = self.data / "backup"
        snap = site_backup.backup(site, bundle)
        write(self.args.output / "backup-receipt.json", snap)
        (self.args.output / "site-backup-manifest.json").write_bytes((bundle / "MANIFEST.json").read_bytes())
        target = self.data / "restored"
        site_backup.restore(bundle, target, site.credentials, args.package)
        copy = Site(target, args.package)
        try:
            copy.allow_run()
        except ValueError as error:
            require(str(error) == "restored_site_requires_handoff", "wrong_unfenced_restore_refusal")
        else:
            raise ValueError("restored_site_can_reuse_streams_without_fence")
        fence = site_backup.handoff(site, copy)
        require(site_backup.handoff(site, copy) == fence, "handoff_retry_not_idempotent")
        try:
            site.allow_run()
        except ValueError as error:
            require(str(error) == "site_fenced_after_handoff", "wrong_fenced_source_refusal")
        else:
            raise ValueError("original_site_not_fenced")
        self.name = "restored"
        self.start(copy)
        settled = self.drained(charged, "restored_old_pending")
        self.add("restored_pending_acknowledged", settled)
        next_plan = schedule(copy, 200, RATE)
        require(next_plan["events"] == charged + 200, "restored_generator_frontier_changed")
        new = self.drained(charged + 200, "restored_new_measurements")
        self.add("restored_new_measurements", new)
        request(copy, "stop")
        require(self.actor.wait(20) == 0, "restored_supervisor_failed_shutdown")
        self.actor = None
        proof = coverage(copy, charged + 200)
        require(proof["streams"] == {k: s["stream"] for k, s in snap["frontiers"]["sources"].items()},
                "restored_stream_replaced")
        self.result["local_backup"] = {"stopped": site.status()["shutdown"], "fence": fence,
                                        "copy_result": proof, "end_of_primary": end,
                                        "charged_events": charged, "final_events": charged + 200,
                                        "pending_at_backup": sum(r["pending"] for r in snap["frontiers"]["sources"].values())}


def run(args):
    out = args.output
    out.mkdir(mode=0o700)
    summary = {"status": "FAIL", "workload": {"rate": RATE, "duration_s": args.duration, "outage_s": args.outage},
               "platform": machine(out), "broker": require_binary(args.package / "mqttiumd", args.package),
               "component_sha256": {name: sha(CLIENT.with_name(name)) for name in FILES},
               "application_source": args.source}
    with tempfile.TemporaryDirectory(prefix="mqttiumd-operated-", dir=out.parent) as directory:
        campaign = Campaign(args, Path(directory), summary)
        try:
            campaign.main_run()
            summary["status"] = "PASS"
        except Exception as error:
            import traceback
            traceback.print_exc()
            summary["failure"] = str(error)
        finally:
            summary["last_exit"] = cleanup(campaign.actor)
            summary["process_observations"] = campaign.process
            write(out / "result.json", summary)
    print(json.dumps({k: summary[k] for k in ("status", "workload")}, sort_keys=True))
    return int(summary["status"] != "PASS")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--duration", type=int, default=72 * 3600)
    parser.add_argument("--outage", type=int, default=1800)
    parser.add_argument("--catchup-limit", type=int, default=900)
    parser.add_argument("--source")
    args = parser.parse_args()
    require(10 <= args.outage < args.duration <= 72 * 3600 and 60 <= args.catchup_limit <= 1800, "duration_outside_budget")
    args.output = args.output.absolute()
    os.umask(0o077)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
