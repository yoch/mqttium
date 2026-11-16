#!/usr/bin/env python3
"""Finite QoS 0 product reception, deliberately outside fixture qualification.

The public-client receipt of a QoS 0 write is not an observation of the broker
or destination. Positive cells observe the message at both an independent raw
subscriber and a second public implementation. Artificial SQL interventions
are identified as corruption/capacity controls, never product operations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time

from pubsub_probe import PubsubWire, Publication, durable, session_key
from pubsub_reception import (Client as PreviousClient, accounts_consistent,
                              delivery_keys, drained, obligations, until)
from tcp_probe import ReceiptFailure, operation_hex, publish_packet, pubrel_packet, require, write_json
from tcp_reception import Broker, writer_available


class Client(PreviousClient):
    """Same process/receipt ownership as the pub/sub pilot, with explicit QoS."""

    def __init__(self, broker: Broker, case: Path, name: str, implementation: str,
                 action: str, *, qos: int = 0, delivered: int = 0,
                 filters: tuple[str, ...] = (), count: int = 1,
                 topic: str = "reception/qos0", payload: bytes = b"qos0",
                 present: bool | None = None, resume: bool = False,
                 client_id: str | None = None):
        self.broker = broker
        self.events = case / f"{name}.client.jsonl"
        self.stdout_path = case / f"{name}.client.stdout"
        self.stderr_path = case / f"{name}.client.stderr"
        self.stdout = self.stdout_path.open("wb")
        self.stderr = self.stderr_path.open("wb")
        self.command = [sys.executable, str(Path(__file__).with_name("external_pubsub.py")),
                        "--host", broker.address[0], "--port", str(broker.address[1]),
                        "--client-id", client_id or name, "--client", implementation,
                        "--action", action, "--qos", str(qos), "--count", str(count),
                        "--events", str(self.events), "--wait-timeout", "12",
                        "--expected-granted-qos", str(min(qos, 1)),
                        "--expected-delivery-qos", str(delivered)]
        for value in filters:
            self.command += ["--filter", value]
        if action == "publish":
            self.command += ["--topic", topic, "--payload", payload.decode()]
        if present is not None:
            self.command += ["--expected-session-present", str(int(present))]
        if resume:
            self.command.append("--resume-without-subscribe")
        self.process = subprocess.Popen(self.command, stdout=self.stdout, stderr=self.stderr)
        self.done = False
        broker.observations.record("external_client_started", pid=self.process.pid,
                                   implementation=implementation, action=action, command=self.command)


def publish(broker: Broker, case: Path, name: str, implementation: str,
            topic: str, payload: bytes, qos: int = 0, expected: str = "success") -> dict:
    client = Client(broker, case, name, implementation, "publish", qos=qos, topic=topic, payload=payload)
    try:
        return client.collect(expected)
    finally:
        client.cleanup()


def message_is(message: Publication, topic: str, payload: bytes, qos: int,
               code: str = "wrong_effective_qos_or_payload") -> None:
    require(message.qos == qos and message.topic == topic and message.payload == payload
            and not message.retain, code, repr(message.observation())[:256])
    require((message.packet_id is None and not message.dup) if qos == 0 else message.packet_id is not None,
            "qos0_entered_qos1_accounting" if qos == 0 else "outgoing_identifier_missing")


def acknowledge_if_needed(wire: PubsubWire, message: Publication) -> None:
    if message.qos == 1:
        wire.acknowledge(message)


def ping(wire: PubsubWire) -> None:
    wire.send(b"\xc0\x00")
    wire.wait(0xD0)


def new_events(broker: Broker, after: int) -> list[dict]:
    return [event for event in broker.events() if int(event["seq"]) > after]


def sequence(broker: Broker) -> int:
    return max((int(event["seq"]) for event in broker.events()), default=0)


def matrix(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    clients = []
    try:
        broker.ready()
        cells = []
        for incoming in (0, 1, 2):
            for granted in (0, 1):
                effective = min(incoming, granted)
                for variant, (sender, receiver) in enumerate((("mqttium", "paho"), ("paho", "mqttium"))):
                    tag = f"p{incoming}-s{granted}-v{variant}"
                    topic, payload = f"reception/qos0/matrix/{tag}", tag.encode()
                    listener = Client(broker, case, f"matrix-{tag}-public", receiver, "receive",
                                      qos=granted, delivered=effective, filters=(topic,), present=False)
                    clients.append(listener)
                    listener.wait_event("subscription_ready")
                    with PubsubWire(broker.address, broker.observations, f"matrix-{tag}-raw") as raw:
                        raw.connect(f"matrix-{tag}-raw", session_present=False)
                        raw.subscribe(1, [(topic, granted)], expected=(granted,))
                        sent = publish(broker, case, f"matrix-{tag}-publisher", sender, topic, payload, incoming)
                        observed = raw.next_publication()
                        message_is(observed, topic, payload, effective)
                        acknowledge_if_needed(raw, observed)
                        public = listener.collect()
                        messages = public["result"]["messages"]
                        require(len(messages) == 1 and messages[0]["qos"] == effective
                                and messages[0]["topic"] == topic and messages[0]["payload_hex"] == payload.hex(),
                                "public_matrix_cell_wrong")
                        if effective == 0:
                            require(messages[0]["packet_id"] is None, "qos0_entered_qos1_accounting")
                        cells.append({"incoming_qos": incoming, "granted_qos": granted,
                                      "effective_qos": effective, "publisher": sender, "subscriber": receiver,
                                      "publisher_receipt": sent, "subscriber_receipt": public,
                                      "independent_wire": observed.observation()})
        state = drained(broker, case, admissions=8)
        broker.stop()
        writer_available(broker.db)
        return {"criterion": "A", "status": "PASS", "cells": cells,
                "all_six_cells_both_public_client_directions": True,
                "durable_admissions": len(state["admissions"])}
    finally:
        for client in clients:
            client.cleanup()
        broker.cleanup()


def pure_no_commit(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    client = None
    topic = "reception/qos0/pure"
    try:
        broker.ready()
        client = Client(broker, case, "pure-public", "mqttium", "receive", qos=0,
                        delivered=0, filters=(topic,), count=12, present=False)
        client.wait_event("subscription_ready")
        with PubsubWire(broker.address, broker.observations, "pure-raw") as raw, \
                PubsubWire(broker.address, broker.observations, "pure-source") as source:
            raw.connect("pure-raw", session_present=False)
            raw.subscribe(1, [(topic, 1)])
            source.connect("pure-source", session_present=False)
            before = durable(broker.db)
            mark = sequence(broker)
            for _ in range(12):
                source.publish_qos0(topic, b"identical-content-new-occurrence")
                observed = raw.next_publication()
                message_is(observed, topic, b"identical-content-new-occurrence", 0)
            received = client.collect()
            require(len(received["result"]["messages"]) == 12, "identical_qos0_occurrences_deduplicated")
            after = durable(broker.db)
            require(after["tables"] == before["tables"], "pure_qos0_created_durable_fact")
            events = new_events(broker, mark)
            require(not any(e["kind"] == "command_submitted" for e in events), "pure_qos0_forced_commit")
            require(not any(e["kind"] in ("qos0_receive_credit", "receive_credit", "wire_receive_credit") for e in events),
                    "qos0_entered_qos1_accounting")
            frame = source.poll(.15)
            require(frame is None, "qos0_received_mqtt_ack", hex(frame.header) if frame else "")
            ping(source)
            ping(raw)
            write_json(case / "before-pure.json", before)
            write_json(case / "after-pure.json", after)
        broker.stop()
        return {"criterion": "B", "status": "PASS", "messages": 12, "public_client": received,
                "same_content_distinct_occurrences": True, "sql_tables_unchanged": True,
                "publication_commit_commands": 0, "mqtt_acknowledgements": 0,
                "fake_packet_identifiers": 0, "receive_maximum_credits": 0}
    finally:
        if client:
            client.cleanup()
        broker.cleanup()


def drop_events(broker: Broker, mark: int, *, timeout: float = 5) -> list[dict]:
    return until(lambda: ([e for e in new_events(broker, mark) if e["kind"] == "qos0_dropped"] or None),
                 "qos0_abandon_not_observable", timeout)


def bounds_and_abandon(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    topic = "reception/qos0/absent"
    try:
        ready = broker.ready()
        with PubsubWire(broker.address, broker.observations, "absent-setup") as absent:
            absent.connect("absent", session_present=False)
            absent.subscribe(1, [(topic, 0)], expected=(0,))
        until(lambda: any(e["kind"] == "connection_closed" for e in broker.events()), "subscriber_not_closed")
        with PubsubWire(broker.address, broker.observations, "drop-source") as source:
            source.connect("drop-source", session_present=False)
            mark = sequence(broker)
            source.publish_qos0(topic, b"not-kept-for-offline-session")
            absent_drop = drop_events(broker, mark)
            require(any(e["details"]["reason"] == "destination_absent" for e in absent_drop), "absent_drop_reason")
            require(not obligations(durable(broker.db)), "volatile_qos0_retained_offline")
            with PubsubWire(broker.address, broker.observations, "absent-resumed") as resumed:
                resumed.connect("absent", session_present=True)
                resumed.no_publication(.15, code="volatile_qos0_replayed_after_reconnect")
                ping(resumed)
            with PubsubWire(broker.address, broker.observations, "small-qos0") as small:
                small.connect("small-qos0", session_present=False, maximum_packet_size=64)
                small.subscribe(1, [("reception/qos0/small", 0)], expected=(0,))
                mark = sequence(broker)
                source.publish_qos0("reception/qos0/small", b"p" * 128)
                small_drop = drop_events(broker, mark)
                require(any(e["details"]["reason"] == "packet_too_large" for e in small_drop), "packet_size_drop_reason")
                small.no_publication(.15, code="qos0_peer_packet_limit_ignored")
                ping(small)
            mark = sequence(broker)
            source.publish_qos0("reception/qos0/nobody", b"not-subscribed")
            no_target = drop_events(broker, mark)
            require(any(e["details"]["reason"] == "no_subscribers" for e in no_target), "no_subscriber_drop_reason")
            with PubsubWire(broker.address, broker.observations, "slow-qos0", receive_buffer=1024) as slow, \
                    PubsubWire(broker.address, broker.observations, "independent-qos0") as independent:
                slow.connect("slow-qos0", session_present=False)
                slow.subscribe(1, [("reception/qos0/slow", 0)], expected=(0,))
                independent.connect("independent-qos0", session_present=False)
                independent.subscribe(1, [("reception/qos0/independent", 0)], expected=(0,))
                mark = sequence(broker)
                sent = 0
                for _ in range(64):
                    source.publish_qos0("reception/qos0/slow", b"s" * 32768)
                    sent += 1
                    if any(e["kind"] == "qos0_dropped" and e.get("details", {}).get("reason")
                           in ("output_saturated", "write_failed", "capacity_exceeded") for e in new_events(broker, mark)):
                        break
                pressure = drop_events(broker, mark)
                require(any(e["details"]["reason"] in ("output_saturated", "write_failed", "capacity_exceeded") for e in pressure),
                        "qos0_output_pressure_not_reached")
                source.publish_qos0("reception/qos0/independent", b"still-progressing")
                message_is(independent.next_publication(), "reception/qos0/independent", b"still-progressing", 0)
                ping(independent)
                ping(source)
            state = durable(broker.db)
            require(not obligations(state) and not state["tables"]["outbound"] and not state["admissions"],
                    "volatile_pressure_created_durable_work")
        broker.stop()
        return {"criterion": "C", "status": "PASS", "profile_bounds": ready["details"],
                "absent": absent_drop, "packet_limit": small_drop, "no_subscriber": no_target,
                "pressure": pressure, "pressure_publications_bounded_at": sent,
                "independent_destination_and_control_progress": True}
    finally:
        broker.cleanup()


def consumption_barrier(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("consume.prepared",))
    topic, payload = "reception/qos0/consume", b"durably-consumed-before-wire"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "consume-sub") as subscriber, \
                PubsubWire(broker.address, broker.observations, "consume-source") as source:
            subscriber.connect("consume-sub", session_present=False)
            subscriber.subscribe(1, [(topic, 0)], expected=(0,))
            source.connect("consume-source", session_present=False)
            source.send(publish_packet(topic, payload, 7, qos=2))
            require(source.wait(0x50, 7).ack() == (7, 0), "publisher_admission_not_confirmed")
            hit = broker.wait_event("barrier_hit", point="consume.prepared")
            require(hit.get("command_kind") == "Consume", "wrong_consume_barrier")
            held = durable(broker.db)
            require(len(obligations(held)) == 1 and obligations(held)[0]["qos"] == 0
                    and not held["tables"]["outbound"], "consume_precondition_missing")
            require(not any(row["id"] == operation_hex(hit["operation"]) for row in held["tables"]["operations"]),
                    "consume_already_committed_while_prepared")
            write_json(case / "consume-held.json", held)
            subscriber.no_publication(.2, code="premature_qos0_publish")
            ping(subscriber)
            broker.release("consume.prepared")
            observed = subscriber.next_publication()
            message_is(observed, topic, payload, 0)
            require(not obligations(durable(broker.db)), "qos0_emitted_before_obligation_consumed")
            subscriber.no_publication(.15, code="consumed_qos0_reemitted")
            source.send(pubrel_packet(7))
            require(source.wait(0x70, 7).ack() == (7, 0), "source_release_not_completed")
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "D-consume", "status": "PASS", "point": hit,
                "observed": observed.observation(), "no_publish_before_known_consumption": True}
    finally:
        broker.cleanup()


def consumption_resolution(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    topic, payload = "reception/qos0/resolution", b"same-consume-operation"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "resolution-sub") as subscriber, \
                PubsubWire(broker.address, broker.observations, "resolution-source") as source:
            subscriber.connect("resolution-sub", session_present=False)
            subscriber.subscribe(1, [(topic, 0)], expected=(0,))
            source.connect("resolution-source", session_present=False)
            (broker.control / "drop-next-consume-result").touch(exist_ok=False)
            source.publish(7, topic, payload, qos=2)
            dropped = broker.wait_event("store_result_dropped")
            require(dropped.get("command_kind") == "Consume", "wrong_consume_result_dropped")
            operation = operation_hex(dropped["operation"])
            pending = durable(broker.db)
            require(not obligations(pending) and not pending["tables"]["outbound"], "consume_drop_not_committed")
            require(sum(row["id"] == operation for row in pending["tables"]["operations"]) == 1,
                    "consume_drop_receipt_missing")
            subscriber.no_publication(.2, code="unknown_consumption_authorized_emit")
            ping(subscriber)
            observed = subscriber.next_publication(timeout=15)
            message_is(observed, topic, payload, 0)
            resolved = broker.wait_event("store_resolved", after=int(dropped["seq"]))
            require(resolved["operation"] == dropped["operation"] and resolved["details"]["outcome"] == "committed",
                    "consumption_resolution_changed_identity")
            (broker.control / "replay-last-consume-result").touch(exist_ok=False)
            replay = broker.wait_event("consume_result_replayed", after=int(resolved["seq"]))
            subscriber.no_publication(.2, code="consumed_qos0_reemitted")
            final = drained(broker, case, admissions=1)
            require(sum(row["id"] == operation for row in final["tables"]["operations"]) == 1,
                    "consumption_resolution_created_another_operation")
        broker.stop()
        return {"criterion": "D-resolution", "status": "PASS", "dropped": dropped,
                "resolved": resolved, "duplicate_result": replay,
                "same_operation": True, "one_emission": observed.observation()}
    finally:
        broker.cleanup()


def crash(binary: Path, case: Path, position: str) -> dict:
    point = {"before": "consume.prepared", "between": "consume.committed", "after": None}[position]
    broker = Broker(binary, case, pauses=(point,) if point else ())
    restarted = None
    topic, payload = "reception/qos0/crash", f"crash-{position}".encode()
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "crash-sub") as subscriber, \
                PubsubWire(broker.address, broker.observations, "crash-source") as source:
            subscriber.connect("crash-sub", session_present=False)
            subscriber.subscribe(1, [(topic, 0)], expected=(0,))
            source.connect("crash-source", session_present=False)
            if point:
                source.send(publish_packet(topic, payload, 7, qos=2))
                require(source.wait(0x50, 7).ack() == (7, 0), "crash_admission_not_known")
                hit = broker.wait_event("barrier_hit", point=point)
                before = durable(broker.db)
                require(len(obligations(before)) == (1 if position == "before" else 0), "crash_position_not_established")
                subscriber.no_publication(.15, code="premature_qos0_publish")
                observed = None
            else:
                source.publish(7, topic, payload, qos=2)
                observed = subscriber.next_publication()
                message_is(observed, topic, payload, 0)
                subscriber.no_publication(.15, code="consumed_qos0_reemitted")
                before = drained(broker, case, admissions=1)
                hit = {"point": "independent subscriber observed complete QoS 0 PUBLISH"}
            inode = broker.db.stat().st_ino
            write_json(case / "before-kill.json", before)
            status = broker.stop(killed=True)
        restarted = Broker(binary, case, db=broker.db, label="restarted")
        restarted.ready()
        require(restarted.db.stat().st_ino == inode, "qos0_restart_replaced_database")
        with PubsubWire(restarted.address, restarted.observations, "resumed-sub") as subscriber:
            subscriber.connect("crash-sub", session_present=True)
            if position == "before":
                recovered = subscriber.next_publication()
                message_is(recovered, topic, payload, 0)
                subscriber.no_publication(.15, code="consumed_qos0_reemitted")
            else:
                subscriber.no_publication(.25, code="consumed_qos0_reemitted")
            ping(subscriber)
            if point:
                with PubsubWire(restarted.address, restarted.observations, "resumed-source") as source:
                    source.connect("crash-source", session_present=True)
                    source.publish(7, topic, payload, qos=2, dup=True)
                subscriber.no_publication(.15, code="consumed_qos0_reemitted")
        final = drained(restarted, case, admissions=1)
        require(not final["tables"]["outbound"], "qos0_recovery_created_qos1_claim")
        restarted.stop()
        writer_available(broker.db)
        return {"criterion": f"E-{position}", "status": "PASS", "kill_status": status,
                "point": hit, "original_obligations": sorted(delivery_keys(before)),
                "same_file_inode": inode, "same_admission": True,
                "expected_policy": "pending obligation resumes" if position == "before" else "consumed obligation never resumes",
                "observed_before_kill": observed.observation() if observed else None}
    finally:
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def mixed_fanout(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("consume.prepared",))
    topic, payload = "reception/qos0/mixed", b"one-atomic-admission-two-delivery-cycles"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "mixed-zero") as zero, \
                PubsubWire(broker.address, broker.observations, "mixed-one") as one, \
                PubsubWire(broker.address, broker.observations, "mixed-source") as source:
            zero.connect("mixed-zero", session_present=False)
            one.connect("mixed-one", session_present=False, receive_maximum=1)
            zero.subscribe(1, [(topic, 0)], expected=(0,))
            one.subscribe(1, [(topic, 1)])
            source.connect("mixed-source", session_present=False)
            source.send(publish_packet(topic, payload, 7, qos=2))
            require(source.wait(0x50, 7).ack() == (7, 0), "mixed_publisher_not_admitted")
            hit = broker.wait_event("barrier_hit", point="consume.prepared")
            before = durable(broker.db)
            require(sorted(o["qos"] for o in obligations(before)) == [0, 1]
                    and len(before["admissions"]) == 1 and len(before["tables"]["payloads"]) == 1,
                    "mixed_admission_not_atomic")
            accounts_consistent(before)
            zero.no_publication(.15, code="premature_qos0_publish")
            ping(one)
            broker.release("consume.prepared")
            zm, om = zero.next_publication(), one.next_publication()
            message_is(zm, topic, payload, 0)
            message_is(om, topic, payload, 1)
            remaining = until(lambda: (s if len(obligations(s := durable(broker.db))) == 1 else None),
                              "qos0_obligation_not_consumed")
            require(obligations(remaining)[0]["qos"] == 1, "qos0_consumption_ended_qos1")
            accounts_consistent(remaining)
            one.acknowledge(om)
            source.send(pubrel_packet(7))
            require(source.wait(0x70, 7).ack() == (7, 0), "mixed_source_not_completed")
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "F", "status": "PASS", "point": hit,
                "admitted_qos": [0, 1], "zero": zm.observation(), "one": om.observation(),
                "last_reference_released": True}
    finally:
        broker.cleanup()


def atomic_capacity(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    original = None
    topic = "reception/qos0/capacity"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "capacity-zero") as zero, \
                PubsubWire(broker.address, broker.observations, "capacity-one") as one:
            zero.connect("capacity-zero", session_present=False)
            one.connect("capacity-one", session_present=False)
            zero.subscribe(1, [(topic, 0)], expected=(0,))
            one.subscribe(1, [(topic, 1)])
            before = durable(broker.db)
            with sqlite3.connect(broker.db, timeout=.25) as db:
                original = db.execute("SELECT limit_references FROM meta").fetchone()[0]
                db.execute("UPDATE meta SET limit_references=?", ((1).to_bytes(8, "big"),))
            rejected = publish(broker, case, "capacity-rejected", "mqttium", topic, b"no-partial-fanout", 1, "rejected")
            require(any(e["kind"] == "publication_rejected" and e.get("reason_code") == 0x97
                        for e in rejected["result"]["events"]), "mixed_capacity_wrong_refusal")
            after = durable(broker.db)
            require(not after["admissions"] and not obligations(after), "mixed_capacity_partial_admission")
            for name in ("obligations", "payloads", "publications", "routes", "outbound"):
                require(after["tables"][name] == before["tables"][name], "mixed_capacity_partial_admission", name)
            zero.no_publication(.15, code="mixed_capacity_early_qos0")
            one.no_publication(.15, code="mixed_capacity_early_qos1")
            with sqlite3.connect(broker.db, timeout=.25) as db:
                db.execute("UPDATE meta SET limit_references=?", (original,))
            original = None
            accepted = publish(broker, case, "capacity-accepted", "paho", topic, b"capacity-restored", 2)
            zm, om = zero.next_publication(), one.next_publication()
            message_is(zm, topic, b"capacity-restored", 0)
            message_is(om, topic, b"capacity-restored", 1)
            one.acknowledge(om)
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "F-capacity", "status": "PASS", "clients": [rejected, accepted],
                "intervention": "isolated post-readiness reference limit lowered to one, then restored",
                "refusal_atomic_across_both_qos": True}
    finally:
        if original is not None and broker.process.poll() is None:
            with sqlite3.connect(broker.db, timeout=.25) as db:
                db.execute("UPDATE meta SET limit_references=?", (original,))
        broker.cleanup()


def replacements(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    topic = "reception/qos0/replacement"
    try:
        broker.ready()
        outcomes = []
        for old, new in ((0, 1), (1, 0)):
            name = f"replacement-{old}-{new}"
            with PubsubWire(broker.address, broker.observations, f"{name}-setup") as subscriber:
                subscriber.connect(name, session_present=False)
                subscriber.subscribe(1, [(topic, old)], expected=(old,))
                generation = [e["connection"] for e in broker.events() if e["kind"] == "connection_opened"][-1]
            until(lambda: any(e["kind"] == "connection_closed" and e.get("connection") == generation
                              for e in broker.events()), "old_subscription_connection_not_closed")
            publish(broker, case, f"{name}-old-publisher", "mqttium", topic, f"old-{old}".encode(), 2)
            before = durable(broker.db)
            target = session_key(before, name)
            old_rows = obligations(before, target)
            require(len(old_rows) == 1 and old_rows[0]["qos"] == old, "old_subscription_work_missing")
            hold = broker.control / "hold-work-pages"
            hold.touch(exist_ok=False)
            with PubsubWire(broker.address, broker.observations, f"{name}-resumed") as subscriber:
                subscriber.connect(name, session_present=True)
                broker.wait_event("work_page_held")
                subscriber.subscribe(2, [(topic, new)], expected=(new,))
                changed = durable(broker.db)
                require(obligations(changed, target) == old_rows, "subscription_retroactively_changed_delivery_qos")
                routes = [r for r in changed["tables"]["routes"] if r["session"] == target]
                require(len(routes) == 1 and routes[0]["qos"] == new, "subscription_grant_not_replaced")
                ping(subscriber)
                hold.unlink()
                previous = subscriber.next_publication()
                message_is(previous, topic, f"old-{old}".encode(), old)
                acknowledge_if_needed(subscriber, previous)
                subscriber.no_publication(.1, code="old_obligation_duplicated_by_resubscription")
                publish(broker, case, f"{name}-new-publisher", "paho", topic, f"new-{new}".encode(), 2)
                current = subscriber.next_publication()
                message_is(current, topic, f"new-{new}".encode(), new)
                acknowledge_if_needed(subscriber, current)
                subscriber.unsubscribe(3, [topic])
            outcomes.append({"old_grant": old, "new_grant": new,
                             "old_delivery": previous.observation(), "new_delivery": current.observation()})
        drained(broker, case, admissions=4)
        broker.stop()
        return {"criterion": "G", "status": "PASS", "replacements": outcomes,
                "admitted_delivery_qos_immutable": True}
    finally:
        broker.cleanup()


def credits(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    topic = "reception/qos0/credits"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "credits-sub") as subscriber, \
                PubsubWire(broker.address, broker.observations, "credits-source") as source:
            subscriber.connect("credits-sub", session_present=False)
            subscriber.subscribe(1, [(topic, 0)], expected=(0,))
            source.connect("credits-source", session_present=False)
            for identifier in range(1, 9):
                source.send(publish_packet(topic, str(identifier).encode(), identifier, qos=2))
                require(source.wait(0x50, identifier).ack() == (identifier, 0), "held_qos2_not_admitted")
                message_is(subscriber.next_publication(), topic, str(identifier).encode(), 0)
            before = durable(broker.db)
            require(before["counts"]["inbound"] == 8 and not before["tables"]["outbound"], "eight_exchange_precondition")
            mark = sequence(broker)
            source.publish_qos0(topic, b"no-ninth-mqtt-exchange")
            response = source.poll(.15)
            require(response is None or response.kind != 0xE0 or response.body[0] != 0x93,
                    "qos0_entered_qos1_accounting", "QoS 0 was rejected against a full Receive Maximum window")
            require(response is None, "qos0_received_mqtt_ack")
            observed = subscriber.next_publication()
            message_is(observed, topic, b"no-ninth-mqtt-exchange", 0, "qos0_entered_qos1_accounting")
            events = new_events(broker, mark)
            require(not any(e["kind"] == "qos0_receive_credit" for e in events), "qos0_entered_qos1_accounting")
            after = durable(broker.db)
            require(after["tables"] == before["tables"], "qos0_entered_qos1_accounting", "pure QoS 0 changed durable exchange facts")
            ping(source)
            ping(subscriber)
            for identifier in range(1, 9):
                source.send(pubrel_packet(identifier))
                require(source.wait(0x70, identifier).ack() == (identifier, 0), "qos0_released_another_exchange_credit")
            source.publish(1, topic, b"identifier-can-now-be-reused", qos=1)
            message_is(subscriber.next_publication(), topic, b"identifier-can-now-be-reused", 0)
        drained(broker, case, admissions=9)
        broker.stop()
        return {"criterion": "H", "status": "PASS", "held_qos2": 8,
                "pure_qos0_used_no_receive_credit": True, "no_fake_packet_identifier": True,
                "old_exchanges_completed_and_reused": True}
    finally:
        broker.cleanup()


def corrupt_live_work(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    restarted = None
    topic = "reception/qos0/corrupt-work"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "corrupt-setup") as subscriber:
            subscriber.connect("corrupt-subscriber", session_present=False)
            subscriber.subscribe(1, [(topic, 1)])
            generation = [e["connection"] for e in broker.events() if e["kind"] == "connection_opened"][-1]
        until(lambda: any(e["kind"] == "connection_closed" and e.get("connection") == generation
                          for e in broker.events()), "corruption_destination_not_offline")
        publish(broker, case, "corrupt-source", "mqttium", topic, b"durable-before-corruption", 1)
        before = durable(broker.db)
        require(len(obligations(before)) == 1 and not before["tables"]["outbound"], "corruption_requires_unclaimed_obligation")
        with sqlite3.connect(broker.db, timeout=.25) as db:
            changed = db.execute("UPDATE obligations SET qos=99").rowcount
            db.commit()
            require(changed == 1 and db.execute("PRAGMA quick_check").fetchone()[0] == "ok", "corruption_image_not_physically_sound")
        with PubsubWire(broker.address, broker.observations, "corrupt-resumed") as subscriber:
            try:
                subscriber.connect("corrupt-subscriber", session_present=True)
            except ReceiptFailure as error:
                require(error.code in ("peer_closed", "probe_truncated"), "unexpected_corruption_connect_failure", error.code)
            status = broker.process.wait(timeout=8)
        failure = [e for e in broker.events() if e["kind"] == "startup_or_runtime_refused"]
        require(status != 0 and failure and failure[-1].get("cause") == "InvalidData"
                and failure[-1].get("phase") == "Work", "invalid_work_was_silently_absent")
        require(any(e["kind"] == "stopped" and e["details"]["writer_released"] for e in broker.events()), "corrupt_work_writer_not_released")
        writer_available(broker.db)
        broker.stdout.close(); broker.stderr.close(); broker.cleaned = True
        broker.observations.record("process_wait", pid=broker.process.pid, status=status, requested="typed-read-failure")
        restarted = Broker(binary, case, db=broker.db, label="restarted")
        restarted_status = restarted.process.wait(timeout=8)
        errors = [e for e in restarted.events() if e["kind"] == "startup_or_runtime_refused"]
        require(restarted_status != 0 and errors and errors[-1].get("cause") == "InvalidData"
                and errors[-1].get("phase") == "Snapshot", "invalid_startup_regression")
        require(not any(e["kind"] == "ready" for e in restarted.events()), "corrupt_store_became_ready")
        writer_available(broker.db)
        return {"criterion": "R-read-domain", "status": "PASS", "quick_check": "ok",
                "intervention": "isolated after-readiness obligations.qos=99 without a claim",
                "runtime_failure": failure[-1], "runtime_exit": status,
                "startup_failure": errors[-1], "startup_exit": restarted_status,
                "writer_released": True}
    finally:
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--mode", choices=("normal", "complete"), default="normal")
    parser.add_argument("--case")
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing")
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    cases = [("A", matrix), ("B", pure_no_commit), ("C", bounds_and_abandon),
             ("E-after", lambda b, p: crash(b, p, "after")), ("F-capacity", atomic_capacity),
             ("H", credits), ("R-read-domain", corrupt_live_work)]
    if args.mode == "complete" or args.case:
        cases += [("D-consume", consumption_barrier), ("D-resolution", consumption_resolution),
                  ("E-before", lambda b, p: crash(b, p, "before")),
                  ("E-between", lambda b, p: crash(b, p, "between")),
                  ("F", mixed_fanout), ("G", replacements)]
    require(args.case is None or args.case in {name for name, _ in cases}, "unknown_case")
    result = {"format": "mqttiumd.qos0-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "mode": args.mode,
              "scope": "volatile incoming QoS 0 and consumed durable QoS 0 obligations",
              "binary": {"path": str(args.binary.resolve()),
                         "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}, "cases": []}
    try:
        for name, function in cases:
            if args.case is None or args.case == name:
                value = function(args.binary, args.out / name)
                result["cases"].append(value)
                print(f"{name}: {value['status']}", flush=True)
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "cases": len(result["cases"]),
                      "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
