#!/usr/bin/env python3
"""Publish bounded best-effort telemetry independently of durable history."""
from __future__ import annotations

import argparse
from pathlib import Path
from queue import Empty, Full, Queue
import os
import random
import threading
import time

from common import CAPTEURS, require, topic
from forward_client import (TransportLost, Wire, emit, network_args, pace, retry_delay,
                            send_pending, stop_event)
from forward_store import Backpressure, Event, Outbox, snapshot
from segment_store import ArchiveInbox, HOT_BYTES, archive_args, archive_options


LIVE_FILTER = "atelier/+/telemetry/live"
HISTORY_FILTER = "atelier/+/telemetry/history"
STATUS_FILTER = "atelier/+/status"
LIVE_QUEUE = 32
LIVE_MAX_AGE_NS = 200_000_000


class HistoryEvent(Event):
    @property
    def topic(self):
        return super().topic + "/history"


def decode_lane(name, payload, lane):
    require(name.endswith("/" + lane), "telemetry_arrived_on_wrong_lane")
    return Event.decode(name.removesuffix("/" + lane), payload)


class SplitOutbox(Outbox):
    def __init__(self, path, identity, budget, live_queue, dropped, cut=None):
        super().__init__(path, identity, budget)
        self.live_queue, self.dropped, self.cut = live_queue, dropped, cut

    def enqueue(self, body):
        event = super().enqueue(body)
        if self.cut:
            self.cut("enqueued_before_live", event)
        try:
            self.live_queue.put_nowait((time.monotonic_ns(), event))
        except Full:
            self.dropped(event, "queue_full")
        return event

    def head(self):
        event, streak = super().head()
        routed = HistoryEvent(event.sensor, event.stream, event.sequence, event.created_ns, event.wire) if event else None
        return routed, streak


def retained_online(wire, identity):
    sent = wire.client.publish(topic(identity, "status"), b"online", qos=1, retain=True)
    if sent.rc != 0:
        raise TransportLost("online_publish_failed")
    wire.until(lambda: wire.reply is not None, "online_puback_timeout")
    mid, reason = wire.reply
    require(mid == sent.mid and reason in (0, 0x10), "online_puback_negative_or_mismatched")
    wire.reply = None
    wire.client.on_publish = None


def send_live(args, messages, stop, dropped):
    """This task owns a second Paho client and never acquires the outbox lock."""
    rng = random.Random()
    tries = 0
    while not stop.is_set():
        wire = Wire(args, 0)
        try:
            wire.client.will_set(topic(args.identity, "status"), b"offline", qos=1, retain=True)
            require(not wire.connect(fresh=True), "live_producer_session_present")
            retained_online(wire, args.identity)
            tries = 0
            emit("live_producer_ready", identity=args.identity, port=args.port,
                 tls=wire.client.socket().version(), session_present=False)
            while not stop.is_set():
                wire.pump(0)
                try:
                    stamp, event = messages.get(timeout=.005)
                except Empty:
                    continue
                if time.monotonic_ns() > stamp + LIVE_MAX_AGE_NS:
                    dropped(event, "stale_before_publish")
                    continue
                try:
                    sent = wire.client.publish(event.topic + "/live", event.wire, qos=0, retain=False)
                    if sent.rc != 0:
                        raise TransportLost(f"live_submit_{int(sent.rc)}")
                    # Do not feed an unbounded Paho QoS 0 packet queue while
                    # the socket is stalled. There is at most one pending frame.
                    until = stamp + LIVE_MAX_AGE_NS
                    while wire.client.want_write():
                        if stop.is_set() or time.monotonic_ns() >= until:
                            raise TransportLost("live_socket_backpressure")
                        wire.pump(.005)
                except (TransportLost, OSError):
                    dropped(event, "live_transport_lost")
                    raise
        except (TransportLost, OSError):
            wire.close(abort=True)
            tries += 1
            stop.wait(retry_delay(tries, rng))
        finally:
            wire.close()
    while not messages.empty():
        _, event = messages.get_nowait()
        dropped(event, "producer_stopped")


def run_sensor(args, cut=None):
    stop = stop_event()
    live = argparse.Namespace(**{**vars(args), "pki": args.live_pki, "port": args.live_port})
    require(args.port != live.port and args.pki.resolve() != live.pki.resolve(), "live_history_must_be_distinct")
    run = {"source_offered": 0}
    errors = []
    messages = Queue(LIVE_QUEUE)
    dropped_count = 0
    counter_lock = threading.Lock()

    def dropped(event, reason):
        nonlocal dropped_count
        with counter_lock:
            dropped_count += 1
        if args.trace:
            emit("live_drop", event_id=event.event_id, reason=reason)

    def drive(action, *params):
        try:
            action(*params)
        except Exception as error:
            errors.append(error)
            stop.set()

    with SplitOutbox(args.db, args.identity, args.budget, messages, dropped, cut) as store:
        history = threading.Thread(target=drive, args=(send_pending, args, store, stop, None), name="history")
        observer = threading.Thread(target=drive, args=(send_live, live, messages, stop, dropped), name="live")
        history.start()
        observer.start()
        try:
            pace(args, store, stop, run)
        finally:
            stop.set()
            history.join()
            observer.join()
            emit("outbox_final", **snapshot(args.db), **store.metrics(), **run, live_local_dropped=dropped_count)
        if errors:
            raise errors[0]


def subscribe(wire, filters, grant=None):
    received = []

    def granted(_client, _data, mid, reasons, _properties):
        require(mid == expected and [reason.value for reason in reasons] == [qos for _, qos in filters],
                "dashboard_subscription_refused")
        if grant:
            grant()
        received.append(mid)

    wire.client.on_subscribe = granted
    rc, expected = wire.client.subscribe(filters)
    require(rc == 0, "dashboard_subscribe_submit_failed")
    wire.until(lambda: bool(received), "dashboard_suback_timeout")


def run_history(args):
    stop = stop_event()
    rng = random.Random()
    connections, tries = 0, 0
    with ArchiveInbox(args.db, args.identity, args.budget, **archive_options(args)) as store:
        checked = -1
        next_archive_retry = 0

        def compact():
            nonlocal checked, next_archive_retry
            unique = store.state()["unique_count"]
            if checked == unique or time.monotonic() < next_archive_retry:
                return
            try:
                if not store.compact():
                    checked = unique
            except Backpressure as error:
                emit("archive_pressure", identity=args.identity, reason=str(error))
                next_archive_retry = time.monotonic() + 1

        while not stop.is_set():
            # Recover capacity even while the connection is offline.
            compact()
            wire = Wire(args, (1 << 32) - 1)

            def received(client, _data, message):
                try:
                    require(message.qos == 1 and not message.retain, "unexpected_history_qos_or_retain")
                    event = decode_lane(message.topic, message.payload, "history")
                except ValueError:
                    store.record_invalid()
                    raise
                result = store.apply(event, message.dup)
                require(result != "conflict", f"event_payload_conflict:{event.event_id}")
                if client.ack(message.mid, message.qos) != 0:
                    raise TransportLost("history_puback_submit_failed")

            wire.client.on_message = received
            try:
                session = wire.connect(fresh=False)
                if store.subscribed():
                    require(session, "history_session_missing")
                else:
                    subscribe(wire, [(HISTORY_FILTER, 1)], store.mark_subscribed)
                connections += 1
                tries = 0
                emit("history_ready", identity=args.identity, session_present=session, connections=connections,
                     port=args.port, tls=wire.client.socket().version())
                report = 0
                while not stop.is_set():
                    wire.pump()
                    compact()
                    if time.monotonic() >= report:
                        emit("history_metrics", **snapshot(args.db), **store.metrics())
                        report = time.monotonic() + 5
            except (TransportLost, Backpressure) as error:
                emit("history_retry", identity=args.identity, reason=str(error))
                wire.close(abort=True)
                tries += 1
                stop.wait(retry_delay(tries, rng))
            finally:
                wire.close()
        emit("history_final", **snapshot(args.db), **store.metrics())


class Display:
    """History fulfills missing identities separately from the live display."""

    def __init__(self, trace):
        self.trace = trace
        self.latest = {}
        self.sequence_gaps = 0
        self.stale = 0
        self.received = 0

    def observe(self, event, port, packet):
        current = self.latest.get(event.sensor)
        updated = current is None or current[0] != event.stream or event.sequence > current[1]
        if updated:
            previous = current[1] if current and current[0] == event.stream else 0
            gap = event.sequence - previous - 1
            self.sequence_gaps += gap
            if gap:
                emit("live_gap", identity=event.sensor, stream=event.stream.hex(),
                     first=previous + 1, last=event.sequence - 1)
            self.latest[event.sensor] = (event.stream, event.sequence, event.digest)
        else:
            self.stale += 1
        self.received += 1
        if self.trace:
            emit("live_received", event_id=event.event_id, digest=event.digest.hex(), topic=packet.topic,
                 qos=packet.qos, retain=packet.retain, port=port, display_updated=updated,
                 created_ns=event.created_ns, received_ns=time.time_ns())

    def state(self):
        return {sensor: {"stream": st.hex(), "seq": n, "digest": proof.hex()}
                for sensor, (st, n, proof) in self.latest.items()}


def run_live_dashboard(args):
    stop = stop_event()
    rng = random.Random()
    display = Display(args.trace)
    tries, connections = 0, 0
    with_status = {}

    def observed(client, _data, message):
        if message.topic.endswith("/status"):
            name = message.topic.split("/")[1]
            require(name in CAPTEURS and message.topic == topic(name, "status")
                    and message.qos == 1 and message.payload in (b"online", b"offline"), "invalid_presence_message")
            state = message.payload.decode("ascii")
            with_status[name] = state
            emit("presence", identity=name, state=state, qos=message.qos, retain=message.retain, port=args.port)
            require(client.ack(message.mid, 1) == 0, "presence_ack_failed")
        else:
            require(message.qos == 0 and not message.retain, "unexpected_live_qos_or_retain")
            event = decode_lane(message.topic, message.payload, "live")
            display.observe(event, args.port, message)

    while not stop.is_set():
        wire = Wire(args, 0)
        wire.client.on_message = observed
        try:
            require(not wire.connect(fresh=True), "live_dashboard_session_present")
            subscribe(wire, [(LIVE_FILTER, 0), (STATUS_FILTER, 1)])
            tries = 0
            connections += 1
            emit("live_ready", identity=args.identity, port=args.port, connections=connections,
                 session_present=False, tls=wire.client.socket().version())
            report = 0
            while not stop.is_set():
                wire.pump()
                if time.monotonic() >= report:
                    emit("live_metrics", latest=display.state(), status=with_status, received=display.received,
                         gap_sequences=display.sequence_gaps, nonadvancing=display.stale)
                    report = time.monotonic() + 5
        except TransportLost:
            wire.close(abort=True)
            tries += 1
            stop.wait(retry_delay(tries, rng))
        finally:
            wire.close()
    emit("live_final", latest=display.state(), status=with_status, received=display.received,
         gap_sequences=display.sequence_gaps, nonadvancing=display.stale)


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="command", required=True)
    producer = commands.add_parser("producer")
    network_args(producer, producer=True)
    producer.add_argument("--live-port", required=True, type=int)
    producer.add_argument("--live-pki", required=True, type=Path)
    producer.add_argument("--trace", action="store_true")
    history = commands.add_parser("history")
    network_args(history, producer=False)
    history.set_defaults(budget=HOT_BYTES)
    archive_args(history)
    live = commands.add_parser("live")
    network_args(live, producer=False)
    live.add_argument("--trace", action="store_true")
    return cli


def main(args=None):
    args = args or parser().parse_args()
    os.umask(0o077)
    {"producer": run_sensor, "live": run_live_dashboard, "history": run_history}[args.command](args)


if __name__ == "__main__":
    main()
