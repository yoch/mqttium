#!/usr/bin/env python3
"""Count actual arrivals, ACKs and commits for a paced eight-by-two TLS workload."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import socket
import subprocess
import tempfile
import threading
import time

from common import (Broker, CAPTEURS, DASHBOARDS, IDENTITIES, PACKAGE, event_rows, inventory,
                    machine, mqtt_client, processes, provision, require, sha, topic, wait_for, write)


def payload(seq, size):
    serial = seq.to_bytes(8, "big")
    return serial + (hashlib.sha256(serial).digest() * ((size + 31)//32))[:size - len(serial)]


def distribution(values):
    if not values:
        return {}
    ordered = sorted(values)
    percent = lambda rank: ordered[max(0, math.ceil(rank * len(ordered)) - 1)] / 1e6
    return {"samples": len(values), "p50_ms": percent(.50), "p95_ms": percent(.95),
            "p99_ms": percent(.99), "max_ms": percent(1)}


class EventMeter:
    """Read the complete sequence across rotations, retaining only bounded summaries."""

    def __init__(self, path):
        self.path = path
        self.file = path.open("rb")
        self.inode = os.fstat(self.file.fileno()).st_ino
        self.seq = 0
        self.pending = b""
        self.gaps = 0
        self.counts = Counter()
        self.outcomes = Counter()
        self.last_operation = {}
        self.metadata = {}
        self.duplicate_commits = 0
        # Each exact authority can issue at most one commit event per operation.
        # The set is bounded by the frozen campaign's planned maximum.
        self.committed = set()

    def ingest(self):
        block = self.file.read()
        *lines, self.pending = (self.pending + block).split(b"\n")
        for line in lines:
            row = json.loads(line)
            seq = int(row["seq"])
            self.gaps += int(seq != self.seq + 1)
            self.seq = seq
            self.counts[row["kind"]] += 1
            info = row.get("details", {})
            if row["kind"] in ("sqlite_configuration", "ready", "tls_profile",
                               "session_clock_anchor", "message_expiry_clock", "receipt_policy"):
                self.metadata[row["kind"]] = info
            if row["kind"] == "store_result":
                kind = row["command_kind"]
                status = info["outcome"]
                self.outcomes[kind + "/" + status] += 1
                if info["wrote"]:
                    self.counts["writer_operations_with_write"] += 1
                if info["committed_now"]:
                    self.counts[kind + "_committed"] += 1
                    operation = row["operation"]
                    key = tuple(operation[k] for k in ("incarnation", "epoch", "owner", "sequence"))
                    self.duplicate_commits += int(key in self.committed)
                    self.committed.add(key)
                    self.last_operation[kind] = key

    def pump(self):
        self.ingest()
        try:
            changed = self.path.stat().st_ino != self.inode
        except FileNotFoundError:
            return
        if changed:
            self.ingest()
            self.file.close()
            self.file = self.path.open("rb")
            self.inode = os.fstat(self.file.fileno()).st_ino
            self.ingest()

    def close(self):
        self.file.close()


class Traffic:
    def __init__(self, total, size):
        self.rows = [[0, 0, 0, 0, 0] for _ in range(total)]
        self.size = size
        self.lock = threading.Lock()
        self.pending = {}
        self.acks = queue.Queue(maxsize=1024)
        self.counts = Counter()
        self.errors = []
        self.acked = 0
        self.received = [0, 0]

    def error(self, message):
        if len(self.errors) < 40:
            self.errors.append(message)
        self.counts["errors"] += 1

    def delivered(self, index, client, message):
        received = time.monotonic_ns()
        data = message.payload
        seq = int.from_bytes(data[:8], "big")
        with self.lock:
            if (not 0 <= seq < len(self.rows) or message.qos != 1 or
                    message.topic != topic(CAPTEURS[seq % len(CAPTEURS)]) or data != payload(seq, self.size)):
                self.error("unexpected_topic_qos_or_payload")
            else:
                row = self.rows[seq]
                if row[index + 2]:
                    self.counts[f"dashboard_{index + 1}_duplicates"] += 1
                    if message.dup:
                        self.counts["duplicate_with_MQTT_DUP"] += 1
                    else:
                        self.error("duplicate_without_MQTT_DUP")
                else:
                    row[index + 2] = received
                    self.received[index] += 1
                self.counts["received_MQTT_DUP"] += int(message.dup)
        if client.ack(message.mid, message.qos) != 0:
            self.error("subscriber_puback_submit")

    def puback(self, index, mid, reason):
        received = time.monotonic_ns()
        try:
            self.acks.put_nowait((index, mid, reason.value, reason.is_failure, received))
        except queue.Full:
            self.error("client_ack_observation_queue_full")

    def drain_acks(self):
        while True:
            try:
                index, mid, reason, failed, received = self.acks.get_nowait()
            except queue.Empty:
                return
            seq = self.pending.pop((index, mid), None)
            if seq is None:
                self.error("unexpected_puback")
            elif failed:
                self.counts["negative_puback_" + str(reason)] += 1
                self.rows[seq][4] = reason
            else:
                self.acked += 1
                self.rows[seq][1] = received

    def send(self, client, seq, scheduled):
        index = seq % len(CAPTEURS)
        self.drain_acks()
        sent = time.monotonic_ns()
        self.rows[seq][0] = sent
        info = client.publish(topic(CAPTEURS[index]), payload(seq, self.size),
                              qos=1, retain=seq % 1991 == 0)
        with self.lock:
            if sent - scheduled > 250_000_000:
                self.counts["scheduler_late_over_250ms"] += 1
            if info.rc != 0:
                self.rows[seq][4] = -int(info.rc)
                self.counts["submit_refused_" + str(info.rc)] += 1
                return
            self.pending[index, info.mid] = seq
            self.counts["submitted"] += 1

    def snapshot(self):
        with self.lock:
            self.drain_acks()
            return {"acked": self.acked, "received": self.received[:], "client_pending": len(self.pending),
                    "client_counts": dict(self.counts)}

    def receipt(self, path):
        with gzip.open(path, "wt", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(("seq", "publish_ns", "puback_ns", "dashboard1_ns", "dashboard2_ns", "refusal"))
            writer.writerows((seq, *row) for seq, row in enumerate(self.rows))
        return {**self.snapshot(), "errors": self.errors,
                "puback": distribution([ack - sent for sent, ack, _, _, _ in self.rows if ack]),
                "deliveries": [distribution([row[i] - row[0] for row in self.rows if row[i]])
                               for i in (2, 3)]}


def connect_traffic(pki, port, traffic, stack):
    connections = []
    for cid in (*DASHBOARDS, *CAPTEURS):
        is_sensor = cid in CAPTEURS
        client, properties, keepalive = mqtt_client(pki, cid, will=is_sensor)
        ready, subscribed = threading.Event(), threading.Event()
        details = {}
        def connected(c, _data, flags, reason, _properties, *, r=ready, d=details):
            d.update(reason=reason.value, session_present=bool(flags.session_present))
            r.set()

        def suback(c, _data, _mid, reasons, _properties, *, s=subscribed, d=details):
            d["subscribe"] = [r.value for r in reasons]
            s.set()

        client.on_connect = connected
        client.on_subscribe = suback
        if is_sensor:
            def ack(_c, _d, mid, reason, _p, *, index=CAPTEURS.index(cid)):
                traffic.puback(index, mid, reason)
            client.on_publish = ack
        else:
            def receive(c, _d, msg, *, index=DASHBOARDS.index(cid)):
                traffic.delivered(index, c, msg)
            client.on_message = receive
        client.connect("127.0.0.1", port, keepalive, clean_start=False, properties=properties)
        client.loop_start()
        stack.callback(client.loop_stop)
        stack.callback(client.disconnect)
        require(ready.wait(5) and details == {"reason": 0, "session_present": False}, "connect_not_confirmed")
        if is_sensor:
            connections.append(client)
        else:
            filters = ([("atelier/+/telemetry", 1), (topic(CAPTEURS[0], "status"), 1)]
                       if cid == DASHBOARDS[0] else
                       [*(("atelier/" + sensor + "/telemetry", 1) for sensor in CAPTEURS), ("atelier/+/status", 1)])
            for offset in range(0, len(filters), 8):
                batch = filters[offset:offset + 8]
                subscribed.clear()
                rc, _ = client.subscribe(batch)
                require(rc == 0 and subscribed.wait(5) and details["subscribe"] == [1] * len(batch),
                        "suback_not_confirmed")
    return connections


class Mosquitto:
    def __init__(self, binary, out, pki):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        acl = out / "mosquitto.acl"
        acl.write_text("".join(
            f"user {cid}\ntopic write atelier/{cid}/#\n" if cid in CAPTEURS
            else f"user {cid}\ntopic read atelier/#\n" for cid in IDENTITIES))
        config = out / "mosquitto.conf"
        config.write_text(f"listener {self.port} 127.0.0.1\nallow_anonymous false\n"
                          f"cafile {pki}/ca.crt\ncertfile {pki}/server.crt\nkeyfile {pki}/server.key\n"
                          f"tls_version tlsv1.3\nrequire_certificate true\nuse_identity_as_username true\n"
                          f"acl_file {acl}\npersistence true\npersistence_location {out}/\n"
                          "autosave_interval 30\nlog_type warning\nmax_inflight_messages 8\nmax_queued_messages 128\n")
        self.db = out / "mosquitto.db"
        self.argv = [str(binary), "-c", str(config)]
        with (out / "mosquitto.log").open("wb") as log:
            self.child = subprocess.Popen(self.argv, stdout=log, stderr=log)
        try:
            wait_for(self.is_ready, "mosquitto_not_ready")
        except BaseException:
            self.stop()
            raise

    def is_ready(self):
        require(self.child.poll() is None, "mosquitto_exited")
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=.2):
                return True
        except OSError:
            return False

    stop = Broker.stop


def campaign(args):
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    require(args.seconds > 0 and 0 < args.rate <= 50 and args.size in (256, 1024), "campaign_bounds")
    if args.backend == "mqttiumd":
        build = json.loads((args.package / "BUILD.json").read_text())
        require(build["binary_sha256"] == sha(args.binary) and build["features"] == []
                and build["instrumented"] is False, "normal_packaged_binary_identity_mismatch")
        require(args.source is None or args.source == build["source_sha"], "source_identity_mismatch")
        args.source = build["source_sha"]
    report = {"status": "INCOMPLETE", "source_sha": args.source, "backend": args.backend,
              "binary_sha256": sha(args.binary), "machine": machine(out),
              "parameters": {"seconds": args.seconds, "publications_per_second": args.rate, "payload_bytes": args.size,
                             "subscribers_per_publication": 2, "sensors": 8, "mqtt": 5, "qos": 1, "keep_alive_s": 15,
                             "tls": "TLS 1.3", "receive_maximum": 8, "paho_queue_bound": 16,
                             "retained_update_each": 1991}}
    write(out / "campaign.json", report)
    total = int(args.seconds * args.rate)
    traffic = Traffic(total, args.size)
    broker = meter = None
    try:
        with ExitStack() as secrets:
            pki = args.pki
            if pki is None:
                pki = Path(secrets.enter_context(tempfile.TemporaryDirectory(prefix="mqttiumd-pilot-pki-"))) / "pki"
                provision(pki, args.package)
            report["policy_sha256"] = sha(pki / "identities.json")
            persistent_policy = out / "identities.json"
            persistent_policy.write_bytes((pki / "identities.json").read_bytes())
            broker = (Broker(args.binary, out / "broker.sqlite", out, pki)
                      if args.backend == "mqttiumd" else Mosquitto(args.binary, out, pki))
            try:
                if args.backend == "mqttiumd":
                    meter = EventMeter(broker.events)
                with ExitStack() as peers, (out / "samples.jsonl").open("x", buffering=1) as samples:
                    clients = connect_traffic(pki, broker.port, traffic, peers)

                    def sample(now, stage):
                        if meter:
                            meter.pump()
                        obs = {**processes(broker.child.pid, broker.db), **traffic.snapshot(),
                               "elapsed_s": (now - start) / 1e9, "stage": stage,
                               "event_counts": dict(meter.counts) if meter else {},
                               "sqlite": inventory(broker.db) if meter else None}
                        samples.write(json.dumps(obs, sort_keys=True) + "\n")
                        if meter:
                            require(obs["sqlite"]["invariants_ok"], "SQLite_account_or_reference_mismatch")
                        return obs

                    start = time.monotonic_ns()
                    report["start"] = sample(start, "idle_before")
                    next_sample = start + 1_000_000_000
                    next_pump = start + 100_000_000
                    for seq in range(total):
                        due = start + int(seq * 1e9 / args.rate)
                        now = time.monotonic_ns()
                        if now < due:
                            time.sleep((due - now) / 1e9)
                            now = time.monotonic_ns()
                        if now >= next_pump and meter:
                            meter.pump()
                            next_pump = now + 100_000_000
                        if now >= next_sample:
                            require(broker.child.poll() is None, f"broker_exited_{broker.child.returncode}")
                            sample(now, "traffic")
                            next_sample = now + 1_000_000_000
                        traffic.send(clients[seq % len(clients)], seq, due)
                    until = start + int(args.seconds * 1e9)
                    if until > time.monotonic_ns():
                        time.sleep((until - time.monotonic_ns()) / 1e9)
                    report["end_of_rate_window"] = sample(time.monotonic_ns(), "rate_window_end")

                    def drained():
                        counts = traffic.snapshot()
                        completed = counts["acked"] == total and counts["received"] == [total, total]
                        return completed and (not meter or inventory(broker.db)["counts"]["obligations"] == 0)

                    try:
                        wait_for(drained, "not_drained_or_incomplete", duration=12)
                    except ValueError as error:
                        report["drain_warning"] = str(error)
                    report["drained"] = sample(time.monotonic_ns(), "idle_after")
                if meter:
                    meter.pump()
            finally:
                report["process_exit"] = broker.stop()
    except Exception as error:
        report["failure"] = str(error)
    finally:
        if meter:
            meter.pump()
            report["events"] = {"counts": dict(meter.counts), "outcomes": dict(meter.outcomes),
                                "metadata": meter.metadata, "gaps": meter.gaps, "last_seq": meter.seq,
                                "duplicate_commits": meter.duplicate_commits,
                                "last_operation": meter.last_operation}
            meter.close()
        report["ledger"] = traffic.receipt(out / "sequence-ledger.csv.gz")
        if meter and broker.db.is_file():
            report["after_broker_exit"] = inventory(broker.db)
        checks = report["ledger"]
        terminal = (not meter or (meter.counts["Admission_committed"] + meter.counts["RetainedAdmission_committed"] == total
                                  and meter.counts["Finish_committed"] == total * 2
                                  and meter.gaps == meter.duplicate_commits == 0
                                  and report["after_broker_exit"]["invariants_ok"]))
        report["normal_disconnect_will_commits"] = meter.counts["WillAdmission_committed"] if meter else None
        report["passed_counts"] = (checks["acked"] == total and checks["received"] == [total, total]
                                   and not checks["errors"] and terminal
                                   and (not meter or meter.counts["WillAdmission_committed"] == 0))
        report["status"] = ("PASS" if report["passed_counts"] and not report.get("failure")
                            and report.get("process_exit", {}).get("wait_status") == 0
                            else "FAIL")
        write(out / "campaign.json", report)
        print(json.dumps({"status": report["status"], "failure": report.get("failure"),
                          "client": checks, "observed": report.get("events", {}).get("counts")}), flush=True)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    os.umask(0o077)
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("output", type=Path)
    cli.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    cli.add_argument("--package", type=Path, default=PACKAGE)
    cli.add_argument("--pki", type=Path)
    cli.add_argument("--source")
    cli.add_argument("--backend", choices=["mqttiumd", "mosquitto"], default="mqttiumd")
    cli.add_argument("--rate", type=float, default=50)
    cli.add_argument("--seconds", type=int, default=1800)
    cli.add_argument("--size", type=int, default=256)
    raise SystemExit(campaign(cli.parse_args()))
