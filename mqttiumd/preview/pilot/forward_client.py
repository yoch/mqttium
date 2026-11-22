#!/usr/bin/env python3
"""Run the reference outbox and durable dashboard using public Paho APIs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import socket
import ssl
import threading
import time

from common import CAPTEURS, DASHBOARDS, FOREVER, mqtt_client, require
from forward_store import (DASHBOARD_BYTES, MIB, PRODUCER_BYTES, QUOTA, SUCCESS, UNKNOWN,
                           Backpressure, Event, Inbox, Outbox, backup, snapshot)


ACK_SECONDS = 5
PUMP_SECONDS = .05
FILTER = "atelier/+/telemetry"
PRINT_LOCK = threading.Lock()


def emit(kind, **fields):
    with PRINT_LOCK:
        print(json.dumps({"kind": kind, "monotonic_ns": time.monotonic_ns(), **fields}, sort_keys=True), flush=True)


def retry_delay(streak, rng):
    ceiling = min(1.0, .1 * 2 ** min(max(streak - 1, 0), 4))
    return rng.uniform(ceiling / 2, ceiling)


class TransportLost(ConnectionError):
    pass


class Wire:
    """Exactly one thread pumps this Paho object. A replacement gets a new object."""

    def __init__(self, args, expiry):
        self.args = args
        self.client, self.properties, self.keepalive = mqtt_client(args.pki, args.identity, expiry=expiry)
        self.client.connect_timeout = 3
        self.client.max_inflight_messages_set(1)
        self.client.max_queued_messages_set(1)
        self.connack = None
        self.reply = None
        self.client.on_connect = self.connected
        self.client.on_publish = self.published

    def connected(self, _client, _data, flags, reason, _properties):
        self.connack = (reason.value, bool(flags.session_present))

    def published(self, _client, _data, mid, reason, _properties):
        require(self.reply is None, "multiple_pubacks_for_one_attempt")
        self.reply = (mid, reason.value)

    def connect(self, *, fresh):
        try:
            rc = self.client.connect(self.args.host, self.args.port, self.keepalive,
                                     clean_start=fresh, properties=self.properties)
        except ssl.SSLCertVerificationError:
            raise
        except OSError as error:
            raise TransportLost("connect_failed") from error
        if rc != 0:
            raise TransportLost(f"connect_return_{int(rc)}")
        self.until(lambda: self.connack is not None, "connack_timeout")
        require(self.connack[0] == 0, f"connack_refused_0x{self.connack[0]:02x}")
        return self.connack[1]

    def pump(self, timeout=PUMP_SECONDS):
        try:
            rc = self.client.loop(timeout=timeout)
        except OSError as error:
            raise TransportLost("mqtt_io_failed") from error
        if rc != 0:
            raise TransportLost(f"mqtt_loop_return_{int(rc)}")

    def until(self, predicate, reason):
        deadline = time.monotonic() + ACK_SECONDS
        while not predicate():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportLost(reason)
            try:
                self.pump(min(PUMP_SECONDS, remaining))
            except TransportLost:
                if not predicate():
                    raise

    def send(self, event, cut):
        require(self.reply is None, "stale_puback_before_publish")
        try:
            result = self.client.publish(event.topic, event.wire, qos=1, retain=False)
        except OSError as error:
            raise TransportLost("publish_io_failed") from error
        if result.rc != 0:
            raise TransportLost(f"publish_return_{int(result.rc)}")
        if cut:
            # On this path the fault receiver can stop after socket writes
            # without pumping reads. Its observer verifies actual admission.
            require(self.client.loop_write() == 0 and not self.client.want_write(), "publish_flush_not_reached")
            cut("sent", event, mid=result.mid)
        self.until(lambda: self.reply is not None, "puback_timeout")
        mid, reason = self.reply
        self.reply = None
        require(mid == result.mid, "puback_mid_mismatch")
        if cut:
            cut("puback_observed", event, mid=mid, reason=reason)
        return reason

    def close(self, *, abort=False):
        sock = self.client.socket()
        if sock is None:
            return
        try:
            if abort:
                sock.shutdown(socket.SHUT_RDWR)
            else:
                self.client.disconnect()
        except OSError:
            pass
        finally:
            sock.close()


def send_pending(args, store, stop, cut):
    rng = random.Random()
    wire = None
    connected = 0
    transport_failures = 0
    event, streak = store.head()
    wake = time.monotonic() + (retry_delay(streak, rng) if event and streak else 0)
    try:
        while not stop.is_set():
            delay = wake - time.monotonic()
            if delay > 0:
                if wire is None:
                    stop.wait(min(delay, PUMP_SECONDS))
                else:
                    try:
                        wire.pump(min(delay, PUMP_SECONDS))
                    except TransportLost:
                        wire.close(abort=True)
                        wire = None
                continue

            if wire is None:
                wire = Wire(args, 0)
                try:
                    require(not wire.connect(fresh=True), "unexpected_producer_session_present")
                except TransportLost:
                    wire.close(abort=True)
                    wire = None
                    transport_failures += 1
                    wake = time.monotonic() + retry_delay(transport_failures, rng)
                    continue
                transport_failures = 0
                connected += 1
                emit("producer_ready", identity=args.identity, connections=connected, session_present=False)

            event, streak = store.head()
            if event is None:
                try:
                    store.collect()
                    wire.pump()
                except TransportLost:
                    wire.close(abort=True)
                    wire = None
                    wake = time.monotonic() + retry_delay(1, rng)
                except Backpressure as error:
                    emit("storage_pressure", identity=args.identity, reason=str(error))
                    wake = time.monotonic() + retry_delay(5, rng)
                continue
            if cut:
                cut("before_publish", event)
            try:
                attempt = store.begin(event)
            except Backpressure as error:
                emit("storage_pressure", identity=args.identity, reason=str(error))
                wake = time.monotonic() + retry_delay(5, rng)
                continue
            try:
                reason = wire.send(event, cut)
            except TransportLost:
                reason = UNKNOWN
                wire.close(abort=True)
                wire = None

            while True:
                try:
                    store.complete(event.sequence, attempt, reason)
                    break
                except Backpressure as error:
                    emit("storage_pressure", identity=args.identity, reason=str(error))
                    if stop.wait(retry_delay(5, rng)):
                        return
            if cut:
                cut("result_recorded", event, reason=reason, attempt=attempt)
            if reason in (QUOTA, UNKNOWN):
                wake = time.monotonic() + retry_delay(streak + 1, rng)
    finally:
        if wire is not None:
            wire.close()


def body_for(ordinal):
    encoded = ordinal.to_bytes(8, "big")
    return encoded + hashlib.sha256(encoded).digest() * 5


def pace(args, store, stop, run):
    """Drive the synthetic sensor. Applications can call Outbox.enqueue directly."""
    schedule = None
    offered = run["source_offered"]
    ordinal = snapshot(args.db)["enqueued"] * len(CAPTEURS) + CAPTEURS.index(args.identity)
    report = 0
    while not stop.is_set():
        now = time.monotonic_ns()
        if now >= report:
            emit("outbox_metrics", **snapshot(args.db), **store.metrics(), source_offered=offered)
            report = now + 5_000_000_000
        if args.schedule is not None:
            if schedule is None and args.schedule.is_file():
                schedule = json.loads(args.schedule.read_text())
            if schedule:
                stop_file = args.schedule.with_suffix(".stop.json")
                limit = json.loads(stop_file.read_text())["events"] if stop_file.is_file() else schedule["events"]
                due = schedule["start_ns"] + ordinal * 1_000_000_000 // schedule["rate"]
                if ordinal < limit and now >= due:
                    offered += 1
                    run["source_offered"] = offered
                    try:
                        store.enqueue(body_for(ordinal))
                    except Backpressure as error:
                        emit("enqueue_refused", ordinal=ordinal, reason=str(error))
                        raise
                    ordinal += len(CAPTEURS)
                    continue
                if ordinal < limit:
                    stop.wait(min(PUMP_SECONDS, max(0, due - now) / 1e9))
                    continue
        stop.wait(PUMP_SECONDS)


def stop_event():
    event = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: event.set())
    return event


def run_sensor(args, cut=None):
    stop = stop_event()
    run = {"source_offered": 0, "error": None}
    with Outbox(args.db, args.identity, args.budget) as store:
        def transfer():
            try:
                send_pending(args, store, stop, cut)
            except Exception as error:
                run["error"] = error
                stop.set()

        worker = threading.Thread(target=transfer, name="outbox-sender")
        worker.start()
        try:
            pace(args, store, stop, run)
        finally:
            stop.set()
            worker.join()
            emit("outbox_final", **snapshot(args.db), **store.metrics(), source_offered=run["source_offered"])
        if run["error"]:
            raise run["error"]


def run_dashboard(args, cut=None):
    stop = stop_event()
    with Inbox(args.db, args.identity, args.budget) as store:
        connections = 0
        wait = 0
        rng = random.Random()
        while not stop.is_set():
            wire = Wire(args, FOREVER)
            subscribed = False

            def granted(_client, _data, mid, reasons, _properties):
                nonlocal subscribed
                require(mid == expected and [reason.value for reason in reasons] == [1], "dashboard_subscription_refused")
                store.mark_subscribed()
                subscribed = True

            def received(client, _data, message):
                try:
                    require(message.qos == 1 and not message.retain, "unexpected_telemetry_qos_or_retain")
                    event = Event.decode(message.topic, message.payload)
                except ValueError:
                    store.record_invalid()
                    raise
                result = store.apply(event, message.dup)
                require(result != "conflict", f"event_payload_conflict:{event.event_id}")
                if cut:
                    cut("consumer_committed", event, mid=message.mid, duplicate=message.dup, applied=result)
                if client.ack(message.mid, message.qos) != 0:
                    raise TransportLost("dashboard_puback_submit_failed")

            wire.client.on_subscribe = granted
            wire.client.on_message = received
            try:
                session = wire.connect(fresh=False)
                if store.subscribed():
                    require(session, "dashboard_session_missing")
                else:
                    rc, expected = wire.client.subscribe(FILTER, qos=1)
                    require(rc == 0, "dashboard_subscribe_submit_failed")
                    wire.until(lambda: subscribed, "dashboard_suback_timeout")
                connections += 1
                wait = 0
                emit("dashboard_ready", identity=args.identity, session_present=session, connections=connections)
                report = 0
                while not stop.is_set():
                    wire.pump()
                    if time.monotonic() >= report:
                        emit("inbox_metrics", **snapshot(args.db), **store.metrics())
                        report = time.monotonic() + 5
            except TransportLost:
                wire.close(abort=True)
                wait += 1
                stop.wait(retry_delay(wait, rng))
            finally:
                wire.close()
        emit("inbox_final", **snapshot(args.db), **store.metrics())


def network_args(p, *, producer):
    p.add_argument("--id", dest="identity", required=True, choices=CAPTEURS if producer else DASHBOARDS)
    p.add_argument("--db", required=True, type=Path)
    p.add_argument("--pki", required=True, type=Path)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8883)
    p.add_argument("--budget", type=int, default=PRODUCER_BYTES if producer else DASHBOARD_BYTES,
                   help="SQLite budget in bytes")
    if producer:
        p.add_argument("--schedule", type=Path, help="paced reception input")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    network_args(commands.add_parser("producer"), producer=True)
    network_args(commands.add_parser("dashboard"), producer=False)
    put = commands.add_parser("enqueue", help="accept an event while its writer is stopped")
    put.add_argument("--id", dest="identity", required=True, choices=CAPTEURS)
    put.add_argument("--db", required=True, type=Path)
    put.add_argument("--budget", type=int, default=PRODUCER_BYTES)
    put.add_argument("body")
    stats = commands.add_parser("stats")
    stats.add_argument("db", type=Path)
    saved = commands.add_parser("backup")
    saved.add_argument("db", type=Path)
    saved.add_argument("target", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.command == "producer":
            run_sensor(args)
        elif args.command == "dashboard":
            run_dashboard(args)
        elif args.command == "enqueue":
            with Outbox(args.db, args.identity, args.budget) as store:
                event = store.enqueue(args.body.encode("utf-8"))
                emit("event_enqueued", event_id=event.event_id, sha256=event.digest.hex())
        elif args.command == "stats":
            print(json.dumps(snapshot(args.db), indent=2))
        else:
            backup(args.db, args.target)
            emit("backup_verified", target=str(args.target))
    except (OSError, RuntimeError, ValueError) as error:
        emit("error", reason=str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
