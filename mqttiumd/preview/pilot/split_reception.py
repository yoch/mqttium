#!/usr/bin/env python3
"""Measure isolated live telemetry during a bounded history outage and drain."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time

from common import (Broker, CAPTEURS, DASHBOARDS, PACKAGE, broker_argv, inventory, machine,
                    processes, provision, require, sha, tls_context, topic, wait_for, write)
from forward_client import body_for
from forward_reception import (Actor, Campaign, RATE, atomically_write, percentiles,
                               require_binary, totals)
from forward_store import HEADER, Event, MIB, PRODUCER_BYTES, snapshot
from load import EventMeter, distribution
from segment_store import open_existing


CLIENT = Path(__file__).with_name("split_client.py")
APP_NAMES = ("split_reception.py", "split_client.py", "segment_store.py",
             "forward_client.py", "forward_store.py", "forward_reception.py", "common.py", "load.py")
ARCHIVE_BUDGET = 512 * MIB
HOT_LIMIT = 1024
ARCHIVE_FLAGS = ("--budget", str(4 * MIB), "--hot-limit", str(HOT_LIMIT),
                 "--segment-rows", "64", "--archive-budget", str(ARCHIVE_BUDGET), "--segment-limit", "4096")


def provision_planes(root, package):
    result = {}
    for name in ("live", "history"):
        pki = root / name
        provision(pki, package)
        policy = pki / "identities.json"
        config = json.loads(policy.read_text())
        for principal in config["principals"]:
            sensor = principal["name"]
            if sensor in CAPTEURS:
                principal["publish"] = [topic(sensor, f"telemetry/{name}")]
                if name == "live":
                    principal["publish"].append(topic(sensor, "status"))
        write(policy, config)
        result[name] = pki
    return result


class Plane(Broker):
    def __init__(self, binary, out, pki, name, *, port=0, run=1):
        self.db = out / f"{name}.sqlite"
        self.events = out / f"{name}-{run}.broker.jsonl"
        self.argv = broker_argv(binary, self.db, self.events, pki, listen=f"127.0.0.1:{port}")
        with (out / f"{name}-{run}.broker.log").open("xb") as log:
            self.child = subprocess.Popen(self.argv, stdout=log, stderr=log)
        try:
            ready = wait_for(self.find_ready, f"{name}_did_not_report_ready")
            self.port = int(ready["details"]["address"].rsplit(":", 1)[1])
            self.ready = ready["details"]
            require(self.ready["sqlite_schema"] == 13, "unexpected_mtls_schema")
        except BaseException:
            self.stop()
            raise

    def identity(self):
        info = self.db.stat()
        return {"pid": self.child.pid, "port": self.port, "file": str(self.db),
                "device": info.st_dev, "inode": info.st_ino, "ready": self.ready}


def independent_control(plane, pki, *, presented="dashboard-2", client_id="dashboard-2", refused=False):
    """Open an independent TLS connection and check MQTT responses on the wire."""
    import socket

    name = client_id.encode("ascii")
    content = b"\x00\x04MQTT\x05\x02\x00\x0f\x00" + len(name).to_bytes(2, "big") + name
    ctx = tls_context(pki, presented)
    with socket.create_connection(("127.0.0.1", plane.port), timeout=3) as source:
        with ctx.wrap_socket(source, server_hostname="127.0.0.1") as wire:
            require(wire.version() == "TLSv1.3", "independent_client_tls_version")
            with wire.makefile("rb") as packets:
                wire.sendall(bytes((0x10, len(content))) + content)
                header = packets.read(2)
                require(len(header) == 2 and header[0] == 0x20 and header[1] < 128, "independent_connack_header")
                ack = packets.read(header[1])
                require(ack[:2] == (b"\x00\x87" if refused else b"\x00\x00"), "independent_connack_outcome")
                if not refused:
                    wire.sendall(b"\xc0\x00")
                    require(packets.read(2) == b"\xd0\x00", "independent_pingresp")
                    wire.sendall(b"\xe0\x00")
    return {"port": plane.port, "tls": "TLSv1.3", "connack_reason": 0x87 if refused else 0,
            "pingresp": not refused, "monotonic_ns": time.monotonic_ns()}


class SplitCampaign(Campaign):
    def __init__(self, args, pkis, receipt):
        super().__init__(args, pkis["history"], receipt)
        self.pkis = pkis
        self.live = None
        self.viewer = None
        self.saturated = None
        self.reference_peak = 0

    def actor(self, command, identity, suffix="", *, schedule=None):
        plane = self.live if command == "live" else self.broker
        pki = self.pkis["live" if command == "live" else "history"]
        flags = ARCHIVE_FLAGS if command == "history" else ("--trace",)
        if command == "producer":
            flags += ("--live-port", str(self.live.port), "--live-pki", str(self.pkis["live"]))
        path = self.out / f"{identity}{'-live' if command == 'live' else ''}.sqlite"
        actor = Actor(command, identity, path, pki, plane.port, self.out / f"{command}-{identity}{suffix}.jsonl",
                      schedule=schedule, script=CLIENT, extra=flags)
        self.actors.append(actor)
        return actor

    def begin(self):
        self.live = Plane(self.args.binary, self.out, self.pkis["live"], "live")
        self.broker = Plane(self.args.binary, self.out, self.pkis["history"], "history")
        self.receipt["planes"] = {"live": self.live.identity(), "history": self.broker.identity()}
        a, b = self.receipt["planes"].values()
        require(all(a[key] != b[key] for key in ("pid", "port", "file", "inode")), "shared_broker_resources")
        self.meter = EventMeter(self.broker.events)
        self.live_meter = EventMeter(self.live.events)
        self.samples = (self.out / "samples.jsonl").open("x", buffering=1)
        self.viewer = self.actor("live", DASHBOARDS[0])
        wait_for(lambda: self.viewer.ready("live_ready"), "live_viewer_no_suback")
        self.dashboards = [self.actor("history", name) for name in DASHBOARDS]
        for dashboard in self.dashboards:
            record = wait_for(lambda: dashboard.ready("history_ready"), "history_dashboard_no_suback")
            require(not record["session_present"], "unexpected_initial_history_session")
        stopped = self.dashboards[1].stop()
        require(stopped["wait_status"] == 0, "offline_dashboard_did_not_stop")
        self.receipt["offline_dashboard_process"] = stopped
        self.dashboards[1] = None
        self.schedule = self.out / "schedule.json"
        self.sensors = [self.actor("producer", name, schedule=self.schedule) for name in CAPTEURS]
        for sensor in self.sensors:
            wait_for(lambda: sensor.ready("producer_ready"), "history_producer_not_ready")
            wait_for(lambda: sensor.ready("live_producer_ready"), "live_producer_not_ready")
        self.start = time.monotonic_ns() + 500_000_000
        self.cutoff = self.args.outage * RATE
        self.maximum = self.cutoff if self.args.short else (self.args.outage + self.args.catchup_limit + 10) * RATE
        atomically_write(self.schedule, {"start_ns": self.start, "events": self.maximum, "rate": RATE})

    def sample(self, phase):
        stamp = time.monotonic_ns()
        actors = self.sensors + [self.viewer, self.dashboards[0]] + [x for x in self.dashboards[1:] if x]
        for actor in actors:
            actor.check()
        sensors = [snapshot(actor.db) for actor in self.sensors]
        dashboards = [snapshot(self.out / (name + ".sqlite")) for name in DASHBOARDS]
        counts = totals(sensors)
        planes = {"live": self.live, "history": self.broker}
        brokers = {name: inventory(plane.db) for name, plane in planes.items()}
        self.meter.pump()
        self.live_meter.pump()
        for name, broker in brokers.items():
            require(planes[name].child.poll() is None and broker["invariants_ok"]
                    and broker["limits"]["references"] == 128, f"{name}_inventory_or_quota_invalid")
        require(all(s["occupied_bytes"] <= s["budget"] for s in sensors + dashboards), "sqlite_budget_exceeded")
        require(all(s["hot_count"] <= HOT_LIMIT and s["archive_charge"] <= ARCHIVE_BUDGET for s in dashboards),
                "history_archive_budget_exceeded")
        require(brokers["live"]["counts"]["routes"] == 2
                and brokers["live"]["counts"]["retained"] == 8,
                "unexpected_live_durable_scope")
        references = brokers["history"]["accounts"]["references"]
        self.reference_peak = max(self.reference_peak, references)
        if phase == "outage" and self.saturated is None and references >= 127 and counts["quota"]:
            self.saturated = stamp
            self.receipt["control_during_saturation"] = independent_control(self.live, self.pkis["live"])
        resources = {actor.log.stem: processes(actor.child.pid, actor.db) for actor in actors}
        resources.update({name: processes(plane.child.pid, plane.db) for name, plane in planes.items()})
        for identity, resource in resources.items():
            peak = self.peak.setdefault(identity, {})
            for field, value in resource.items():
                if field != "disk":
                    peak[field] = max(peak.get(field, 0), value)
            for field, value in resource["disk"].items():
                peak["peak_" + field] = max(peak.get("peak_" + field, 0), value)
        for state in sensors + dashboards:
            peak = self.peak.setdefault(state["identity"], {})
            for field in ("pending", "oldest_age_ms", "occupied_bytes", "page_count", "freelist_count",
                          "hot_count", "archived_count", "archive_charge", "segment_count"):
                if field in state:
                    peak["peak_" + field] = max(peak.get("peak_" + field, 0), state[field])
        row = {"phase": phase, "monotonic_ns": stamp, "sensors": sensors, "dashboards": dashboards,
               "total": counts, "brokers": brokers, "resources": resources}
        self.samples.write(json.dumps(row, sort_keys=True) + "\n")
        if phase == "catchup":
            self.history.append((stamp, counts["enqueued"], counts["confirmed"], counts["pending"]))
        return row

    def receive(self):
        self.begin()
        boundary = self.start + self.args.outage * 1_000_000_000
        outage = self.until("outage", lambda row: row["monotonic_ns"] >= boundary
                            and row["total"]["enqueued"] >= self.cutoff, boundary + 5_000_000_000)
        require(self.saturated is not None and outage["dashboards"][1]["unique_count"] == 0
                and outage["total"]["quota"] > 0 and outage["total"]["pending"] > 0,
                "history_saturation_not_exercised")
        self.receipt["outage"] = outage
        self.receipt["schedule"] = {"start_ns": self.start, "outage_boundary_ns": boundary,
                                    "history_saturation_observed_ns": self.saturated}
        self.dashboards[1] = self.actor("history", DASHBOARDS[1], "-resumed")
        ready = wait_for(lambda: self.dashboards[1].ready("history_ready"), "offline_dashboard_did_not_resume")
        require(ready["session_present"], "offline_dashboard_session_not_present")
        self.receipt["dashboard_return"] = ready
        returned = ready["monotonic_ns"]

        def caught_up(state):
            pending = state["total"]["pending"]
            lag = state["total"]["enqueued"] - min(d["unique_count"] for d in state["dashboards"])
            return pending <= (0 if self.args.short else len(CAPTEURS)) and lag <= (0 if self.args.short else 2 * len(CAPTEURS))

        caught = self.until("catchup", caught_up, returned + self.args.catchup_limit * 1_000_000_000)
        elapsed = (caught["monotonic_ns"] - returned) / 1e9
        admission_rate = (caught["total"]["confirmed"] - outage["total"]["confirmed"]) / elapsed
        new_rate = (caught["total"]["enqueued"] - outage["total"]["enqueued"]) / elapsed
        self.receipt["catchup"] = {"seconds": elapsed, "admission_rate": admission_rate, "generation_rate": new_rate,
                                   "net_rate": admission_rate - new_rate, "best_backlogged_60s": self.best_minute(),
                                   "counts": caught["total"]}
        expected = self.cutoff
        if not self.args.short:
            require(admission_rate > 50 and new_rate > 49 and admission_rate > new_rate, "no_sustained_catchup")
            expected = min(self.maximum, (time.monotonic_ns() + 2_000_000_000 - self.start) * RATE // 1_000_000_000)
            atomically_write(self.schedule.with_suffix(".stop.json"), {"events": expected})

        def drained(state):
            return state["total"]["enqueued"] == state["total"]["confirmed"] == expected and state["total"]["rows"] == 0 and all(
                d["unique_count"] == expected for d in state["dashboards"]) and not any(
                state["brokers"]["history"]["counts"][name] for name in ("obligations", "outbound", "wills"))

        final = self.until("drained", drained, time.monotonic_ns() + 40_000_000_000)
        self.receipt["final"] = final
        require(final["total"]["unknown"] == final["total"]["rejected"] == final["total"]["no_match"] == 0,
                "unexpected_normal_history_admission")
        require(all(d["conflicts"] == d["invalid"] == d["duplicates"] == 0 for d in final["dashboards"]),
                "unexpected_normal_history_apply")
        for sensor in self.sensors:
            require(len(list(sensor.facts("producer_ready"))) == len(list(sensor.facts("live_producer_ready"))) == 1,
                    "producer_reconnected_in_normal_run")
        require(len(list(self.viewer.facts("live_ready"))) == 1, "live_viewer_reconnected_in_normal_run")
        require(self.meter.gaps == self.meter.duplicate_commits == 0
                and self.meter.counts["Admission_committed"] == expected
                and self.meter.counts["Finish_committed"] == 2 * expected,
                "history_admission_or_finish_count_mismatch")
        self.receipt["expected_events"] = expected

    def close(self):
        self.receipt["actors"] = [actor.stop() for actor in reversed(self.actors)]
        self.receipt["producer_final"] = [next(actor.facts("outbox_final"), None) for actor in self.sensors]
        if self.viewer:
            self.receipt["live_final"] = next(self.viewer.facts("live_final"), None)
        self.receipt["broker_processes"] = {lane: plane.stop() for lane, plane in
                                             (("live", self.live), ("history", self.broker)) if plane}
        if self.samples:
            self.samples.close()
        for name, meter in (("history", self.meter), ("live", getattr(self, "live_meter", None))):
            if meter:
                meter.pump()
                meter.close()
                self.receipt[name + "_broker_events"] = {"counts": dict(meter.counts), "gaps": meter.gaps,
                                                         "outcomes": dict(meter.outcomes),
                                                         "runtime": meter.metadata}
        self.receipt["peaks"] = self.peak
        self.receipt["history_reference_peak"] = self.reference_peak

    def verify(self):
        expected = self.receipt["expected_events"]
        require(all(row["wait_status"] == 0 for row in self.receipt["actors"])
                and all(row["wait_status"] == 0 for row in self.receipt["broker_processes"].values()),
                "unclean_normal_process_exit")
        require(sum(row["source_offered"] for row in self.receipt["producer_final"]) == expected,
                "source_and_enqueued_counts_differ")
        seen = {}
        received, delays = [], []
        in_window = 0
        boundary = self.receipt["schedule"]["outage_boundary_ns"]
        for fact in self.viewer.facts("live_received"):
            identity = fact["event_id"]
            require(identity not in seen and fact["qos"] == 0 and fact["retain"] is False
                    and fact["port"] == self.live.port and fact["topic"].endswith("/telemetry/live"),
                    "live_delivery_duplicate_or_wrong_plane")
            seen[identity] = fact["digest"]
            if self.start <= fact["monotonic_ns"] <= boundary:
                in_window += 1
                delays.append(fact["received_ns"] - fact["created_ns"])
            received.append(fact["monotonic_ns"])
        saturated = [self.saturated] + [tick for tick in received if self.saturated <= tick <= boundary] + [boundary]
        max_gap_s = max(b - a for a, b in zip(saturated, saturated[1:])) / 1e9
        missing = []
        archived = []
        first_window = 0
        offline_latencies = [Counter(), Counter()]
        with ExitStack() as stack:
            stores = [stack.enter_context(open_existing(actor.db)) for actor in self.dashboards]
            iterators = [store.records() for store in stores]
            with (self.out / "reconciliation.jsonl").open("x") as proof:
                for sensor_index, state in enumerate(self.receipt["final"]["sensors"]):
                    for seq in range(1, state["enqueued"] + 1):
                        ordinal = (seq - 1) * len(CAPTEURS) + sensor_index
                        sensor = state["identity"]
                        key = (sensor, bytes.fromhex(state["stream"]), seq)
                        rows = [next(cursor, None) for cursor in iterators]
                        require(all(r is not None and r[:3] == key for r in rows) and rows[0][3] == rows[1][3],
                                f"missing_or_wrong_historical_event:{sensor}/{seq}")
                        event = Event.decode(topic(sensor), rows[0][3])
                        body = body_for(ordinal)
                        require(event.wire[HEADER.size:HEADER.size + len(body)] == body, "history_body_mismatch")
                        if ordinal < self.cutoff:
                            for i, row in enumerate(rows):
                                offline_latencies[i][math.ceil((row[4] - event.created_ns) / 1e6)] += 1
                        live_digest = seen.pop(event.event_id, None)
                        if live_digest is None:
                            missing.append(event.event_id)
                        else:
                            require(live_digest == event.digest.hex(), "live_history_payload_diverged")
                            first_window += ordinal < self.cutoff
                        proof.write(json.dumps({"id": event.event_id, "sha256": event.digest.hex(),
                                                "live": live_digest is not None}) + "\n")
            require(all(next(cursor, None) is None for cursor in iterators) and not seen, "unexpected_extra_event")
            for store in stores:
                while store.compact(final=True):
                    pass
                current = snapshot(store.path)
                require(current["unique_count"] == current["archived_count"] == expected
                        and current["hot_count"] == 0, "archive_does_not_cover_history")
                sizes = [file.stat() for directory in store.archive.iterdir() for file in directory.iterdir()]
                archived.append({**current, "data_and_manifest_bytes": sum(s.st_size for s in sizes),
                                 "data_and_manifest_blocks_bytes": sum(s.st_blocks * 512 for s in sizes)})
        self.receipt["live_delivery"] = {"expected": expected, "received": len(received),
                                         "outage_expected": self.cutoff, "outage_events_eventually_seen_live": first_window,
                                         "interval_arrivals": in_window, "creation_to_live": distribution(delays),
                                         "max_gap_in_saturation_s": max_gap_s,
                                         "missing_count": len(missing), "missing": missing,
                                         "repaired_from_each_history": len(missing)}
        self.receipt["history_delivery"] = {"verified_on_each_dashboard": expected, "archived": archived,
                                            "outage_event_creation_to_history_ms": [percentiles(c) for c in offline_latencies]}
        require(first_window * 1000 >= self.cutoff * 999, "live_completeness_below_99_9_percent")
        require(delays and all(d >= 0 for d in delays) and distribution(delays)["p99_ms"] <= 250,
                "live_p99_above_250ms_or_clock_invalid")
        require(max_gap_s <= 2 and len(saturated) > 2, "live_silence_during_history_saturation")
        require(len(received) + len(missing) == expected, "unaccounted_live_gap")


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("output", type=Path)
    cli.add_argument("--package", type=Path, default=PACKAGE)
    cli.add_argument("--binary", type=Path)
    cli.add_argument("--short", action="store_true", help="500 events and ten seconds offline")
    cli.add_argument("--catchup-limit", type=int, default=3600)
    cli.add_argument("--application-source", help="record the measured application revision")
    args = cli.parse_args()
    args.binary = (args.binary or args.package / "mqttiumd").resolve()
    args.outage = 10 if args.short else 1800
    require(60 <= args.catchup_limit <= 3600, "catchup_limit_out_of_bounds")
    os.umask(0o077)
    args.output = args.output.resolve()
    args.output.mkdir(mode=0o700)
    receipt = {"status": "FAIL", "parameters": {"rate": RATE, "outage_s": args.outage, "payload_bytes": 256,
                "outage_events": args.outage * RATE, "catchup_limit_s": args.catchup_limit,
                "producer_budget_bytes": PRODUCER_BYTES, "history_store_args": ARCHIVE_FLAGS},
               "broker_build": require_binary(args.binary, args.package), "machine": machine(args.output),
               "application_source": args.application_source,
               "application_files": {name: sha(Path(__file__).with_name(name)) for name in APP_NAMES}}
    with tempfile.TemporaryDirectory(prefix="mqttiumd-split-pki-") as directory:
        pkis = provision_planes(Path(directory), args.package)
        receipt["policy_sha256"] = {name: sha(pki / "identities.json") for name, pki in pkis.items()}
        campaign = SplitCampaign(args, pkis, receipt)
        try:
            campaign.receive()
        except (Exception, KeyboardInterrupt) as error:
            receipt["failure"] = repr(error)
        finally:
            campaign.close()
        if "failure" not in receipt:
            try:
                campaign.verify()
                receipt["status"] = "PASS"
            except Exception as error:
                receipt["failure"] = repr(error)
        write(args.output / "reception.json", receipt)
    print(json.dumps({"status": receipt["status"], "failure": receipt.get("failure"),
                      "receipt": str(args.output / "reception.json")}), flush=True)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
