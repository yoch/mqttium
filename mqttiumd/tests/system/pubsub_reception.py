#!/usr/bin/env python3
"""Finite reception of exact persistent subscriptions and outgoing QoS 1.

The public clients, raw pilot and independent SQLite reader establish concrete
system behavior. This is not an extension of the fixture oracle's qualifying
profile. Every command, process, wire observation and durable image belongs to
the run whose receipt retains it.
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

from pubsub_probe import (
    PubsubWire, Publication, decode_filter_ack, durable, puback_packet,
    session_key, subscribe_packet, unsubscribe_packet,
)
from tcp_probe import ReceiptFailure, operation_hex, publish_packet, pubrel_packet, require, write_json
from tcp_reception import Broker, writer_available


def until(check, code: str, timeout: float = 5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(.005)
    raise ReceiptFailure(code)


class Client:
    """Own one independent public-client process, including error cleanup."""

    def __init__(self, broker: Broker, case: Path, name: str, implementation: str,
                 action: str, *, filters: tuple[str, ...] = (), count: int = 1,
                 topic: str = "reception/pubsub", payload: bytes = b"pubsub", qos: int = 1,
                 present: bool | None = None, resume: bool = False,
                 hold: bool = False, release: Path | None = None):
        self.broker = broker
        self.events = case / f"{name}.client.jsonl"
        self.stdout_path = case / f"{name}.client.stdout"
        self.stderr_path = case / f"{name}.client.stderr"
        self.stdout = self.stdout_path.open("wb")
        self.stderr = self.stderr_path.open("wb")
        self.command = [sys.executable, str(Path(__file__).with_name("external_pubsub.py")),
                        "--host", broker.address[0], "--port", str(broker.address[1]),
                        "--client-id", name, "--client", implementation, "--action", action,
                        "--qos", str(qos), "--count", str(count), "--events", str(self.events),
                        "--wait-timeout", "12"]
        for value in filters:
            self.command += ["--filter", value]
        if action == "publish":
            self.command += ["--topic", topic, "--payload", payload.decode()]
        if present is not None:
            self.command += ["--expected-session-present", str(int(present))]
        if resume:
            self.command += ["--resume-without-subscribe"]
        if hold:
            self.command += ["--hold-acks"]
        if release:
            self.command += ["--release-acks", str(release)]
        self.process = subprocess.Popen(self.command, stdout=self.stdout, stderr=self.stderr)
        self.done = False
        broker.observations.record("external_client_started", pid=self.process.pid,
                                   implementation=implementation, action=action, command=self.command)

    def records(self) -> list[dict]:
        if not self.events.exists():
            return []
        data = self.events.read_bytes()
        require(len(data) <= 1024 * 1024, "external_client_event_budget")
        return [json.loads(line) for line in data.split(b"\n")[:-1]]

    def wait_event(self, kind: str, timeout: float = 8) -> dict:
        def find():
            records = [record for record in self.records() if record["kind"] == kind]
            if records:
                return records[-1]
            if self.process.poll() is not None:
                raise ReceiptFailure("external_client_exited_before_event",
                                     f"{kind}: {self.process.returncode}, {self.stderr_path}")
            return None
        return until(find, "external_client_event_missing", timeout)

    def collect(self, expected: str = "success", timeout: float = 18) -> dict:
        try:
            status = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise ReceiptFailure("external_client_deadline", str(self.process.pid)) from error
        finally:
            self.stdout.close()
            self.stderr.close()
        self.done = True
        self.broker.observations.record("external_client_wait", pid=self.process.pid, status=status)
        data = self.stdout_path.read_bytes()
        require(len(data) <= 1024 * 1024, "external_client_receipt_budget")
        try:
            result = json.loads(data)
        except (ValueError, UnicodeDecodeError) as error:
            raise ReceiptFailure("external_client_receipt_invalid", self.stderr_path.read_text()[-512:]) from error
        require(result["status"] == expected and status == {"success": 0, "rejected": 2}[expected],
                "external_client_failed", json.dumps(result.get("failure", result["status"])))
        result = {"command": self.command, "process_status": status, "result": result}
        write_json(self.stdout_path.with_suffix(".json"), result)
        return result

    def cleanup(self) -> None:
        if self.done:
            return
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=3)
        self.broker.observations.record("external_client_wait", pid=self.process.pid,
                                       status=self.process.returncode, requested="receipt_cleanup")
        self.stdout.close()
        self.stderr.close()
        self.done = True


def accounts_consistent(state: dict) -> None:
    payloads = state["tables"]["payloads"]
    obligations = state["tables"]["obligations"]
    references = sum(int(row["refs"], 16) for row in payloads)
    require(references == len(obligations), "payload_reference_accounting")
    meta = state["tables"]["meta"][0]
    require(int(meta["used_references"], 16) == references, "reference_accounting")
    require(int(meta["used_payload_bytes"], 16) == sum(int(row["length"], 16) for row in payloads),
            "payload_byte_accounting")


def drained(broker: Broker, case: Path, admissions: int | None = None) -> dict:
    def read():
        state = durable(broker.db)
        return state if all(state["counts"][name] == 0
                            for name in ("obligations", "payloads", "publications", "inbound", "outbound")) else None
    state = until(read, "delivery_resources_not_released")
    accounts_consistent(state)
    if admissions is not None:
        require(len(state["admissions"]) == admissions, "unexpected_admission_count")
    write_json(case / "durable-final.json", state)
    return state


def obligations(state: dict, target: str | None = None) -> list[dict]:
    return [row for row in state["tables"]["obligations"] if target is None or row["target"] == target]


def delivery_keys(state: dict) -> set[tuple]:
    return {(row["publication"], row["idx"], row["target"]) for row in obligations(state)}


def assignment(state: dict, target: str) -> dict:
    candidates = []
    excluded = {"meta", "sessions", "routes", "operations", "fences", "publications", "payloads", "inbound"}
    for name, rows in state["tables"].items():
        if name in excluded:
            continue
        for row in rows:
            if "packet_id" in row and row.get("session", row.get("target")) == target:
                candidates.append({"table": name, "row": row})
    require(len(candidates) == 1, "durable_outgoing_assignment_missing", repr(candidates)[:256])
    return candidates[0]


def message_matches(message: Publication, topic: str, payload: bytes) -> None:
    require(message.topic == topic and message.payload == payload and message.qos == 1 and not message.retain,
            "wrong_outgoing_publication", repr(message.observation())[:256])


def publisher(broker: Broker, case: Path, name: str, implementation: str,
              topic: str, payload: bytes, qos: int = 1, expected: str = "success") -> dict:
    client = Client(broker, case, name, implementation, "publish", topic=topic, payload=payload, qos=qos)
    try:
        return client.collect(expected)
    finally:
        client.cleanup()


def nominal(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    clients = []
    try:
        broker.ready()
        receipts = []
        for index, (receiving, publishing, qos) in enumerate((("paho", "mqttium", 1), ("mqttium", "paho", 2))):
            topic = f"reception/external/{index}"
            payload = f"external-{publishing}-to-{receiving}".encode()
            subscriber = Client(broker, case, f"external-{receiving}-subscriber", receiving,
                                "receive", filters=(topic,), count=1, present=False, qos=2)
            clients.append(subscriber)
            subscriber.wait_event("subscription_ready")
            receipts.append(publisher(broker, case, f"external-{publishing}-publisher", publishing,
                                      topic, payload, qos))
            result = subscriber.collect()
            messages = result["result"]["messages"]
            require(len(messages) == 1 and messages[0]["topic"] == topic
                    and messages[0]["payload_hex"] == payload.hex() and messages[0]["qos"] == 1,
                    "external_subscriber_wrong_message")
            require(result["result"]["subscriptions"][0]["reason_codes"] == [1], "requested_qos2_not_granted_qos1")
            receipts.append(result)
        with PubsubWire(broker.address, broker.observations, "exact-neighbor") as subscriber:
            subscriber.connect("exact-neighbor", session_present=False)
            subscriber.subscribe(1, [("reception/exact", 1)])
            with PubsubWire(broker.address, broker.observations, "neighbor-publisher") as source:
                source.connect("neighbor-publisher", session_present=False)
                source.publish(1, "reception/exact/neighbor", b"not-matched", qos=1)
                source.publish(2, "reception/exacta", b"also-not-matched", qos=2)
            subscriber.no_publication(.25, code="neighbor_topic_matched")
        state = drained(broker, case, admissions=4)
        broker.stop()
        writer_available(broker.db)
        return {"criterion": "A", "status": "PASS", "clients": receipts,
                "both_public_client_directions": True, "incoming_qos": [1, 2],
                "outgoing_qos": 1, "neighbor_did_not_match": True,
                "admissions": len(state["admissions"])}
    finally:
        for client in clients:
            client.cleanup()
        broker.cleanup()


def persistent_subscription(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    restarted = None
    client = None
    topic, payload = "reception/persistent", b"published-while-subscriber-offline"
    try:
        broker.ready()
        client = Client(broker, case, "persistent-subscriber", "mqttium", "subscribe",
                        filters=(topic,), present=False)
        subscribed = client.collect()
        before = durable(broker.db)
        require(before["counts"]["routes"] == 1, "subscription_not_durable")
        published = publisher(broker, case, "offline-publisher", "paho", topic, payload, 2)
        pending = durable(broker.db)
        require(len(obligations(pending)) == 1, "offline_obligation_missing")
        accounts_consistent(pending)
        write_json(case / "offline-durable.json", pending)
        inode = broker.db.stat().st_ino
        broker.stop()
        restarted = Broker(binary, case, db=broker.db, label="restarted")
        restarted.ready()
        require(restarted.db.stat().st_ino == inode, "restart_replaced_database")
        client = Client(restarted, case, "persistent-subscriber", "mqttium", "receive", count=1,
                        resume=True, present=True)
        received = client.collect()
        require(not received["result"]["subscriptions"] and len(received["result"]["messages"]) == 1,
                "resume_resubscribed_or_message_missing")
        require(received["result"]["messages"][0]["payload_hex"] == payload.hex(), "offline_payload_changed")
        state = drained(restarted, case, admissions=1)
        require(state["tables"]["routes"] == pending["tables"]["routes"], "restart_lost_subscription")
        restarted.stop()
        writer_available(broker.db)
        return {"criterion": "B", "status": "PASS", "clients": [subscribed, published, received],
                "no_resubscribe": True, "same_file_inode": inode,
                "original_delivery_keys": sorted(delivery_keys(pending)), "admissions": 1}
    finally:
        if client:
            client.cleanup()
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def two_destinations(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    fast = None
    topic, payload = "reception/two", b"one-admission-two-responsibilities"
    try:
        broker.ready()
        release = case / "release-public-subscriber"
        fast = Client(broker, case, "public-fast-subscriber", "mqttium", "receive", filters=(topic,),
                      hold=True, release=release, present=False)
        fast.wait_event("subscription_ready")
        with PubsubWire(broker.address, broker.observations, "raw-slow") as slow:
            slow.connect("raw-slow", session_present=False, receive_maximum=1)
            slow.subscribe(1, [(topic, 1)])
            published = publisher(broker, case, "two-publisher", "mqttium", topic, payload, 1)
            message = slow.next_publication()
            message_matches(message, topic, payload)
            fast.wait_event("ack_held")
            pending = durable(broker.db)
            require(len(obligations(pending)) == 2 and len(pending["admissions"]) == 1,
                    "admission_did_not_own_both_obligations")
            accounts_consistent(pending)
            write_json(case / "two-pending.json", pending)
            release.touch(exist_ok=False)
            received = fast.collect()
            slow_key = session_key(pending, "raw-slow")
            remaining = until(lambda: (state if len(obligations(state := durable(broker.db))) == 1 else None),
                              "fast_destination_not_finished")
            require(len(obligations(remaining, slow_key)) == 1, "wrong_destination_finished")
            accounts_consistent(remaining)
            write_json(case / "only-slow-pending.json", remaining)
            slow.send(b"\xc0\x00")
            slow.wait(0xD0)
            unrelated = publisher(broker, case, "independent-publisher", "paho", "reception/unrelated", b"progress", 2)
            slow.no_publication(.2, code="live_connection_retransmitted")
            slow.acknowledge(message)
        drained(broker, case, admissions=2)
        broker.stop()
        return {"criterion": "C", "status": "PASS", "clients": [published, received, unrelated],
                "publisher_completed_while_both_acks_held": True,
                "slow_ack_explicitly_withheld": True, "controls_and_independent_flow_progressed": True}
    finally:
        if fast:
            fast.cleanup()
        broker.cleanup()


def claim_barrier(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("claim.prepared",))
    topic, payload = "reception/claim", b"only-after-durable-claim"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "claim-subscriber") as subscriber:
            subscriber.connect("claim-subscriber", session_present=False)
            subscriber.subscribe(1, [(topic, 1)])
            published = publisher(broker, case, "claim-publisher", "mqttium", topic, payload)
            hit = broker.wait_event("barrier_hit", point="claim.prepared")
            before = durable(broker.db)
            require(len(obligations(before)) == 1 and len(before["admissions"]) == 1,
                    "claim_target_not_durably_queued")
            require(operation_hex(hit["operation"]) not in {row["id"] for row in before["tables"]["operations"]},
                    "claim_visible_before_commit")
            write_json(case / "before-claim-commit.json", before)
            subscriber.no_publication(.2, code="premature_outgoing_publish")
            broker.release("claim.prepared")
            delivered = subscriber.next_publication()
            message_matches(delivered, topic, payload)
            require(not delivered.dup, "fresh_delivery_marked_duplicate")
            after = durable(broker.db)
            require(operation_hex(hit["operation"]) in {row["id"] for row in after["tables"]["operations"]},
                    "publish_without_committed_claim")
            require(delivery_keys(before) == delivery_keys(after), "claim_recreated_obligation")
            write_json(case / "claimed.json", after)
            subscriber.acknowledge(delivered)
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "D-claim", "status": "PASS", "clients": [published], "point": hit,
                "no_publish_before_claim_commit": True, "publication": delivered.observation()}
    finally:
        broker.cleanup()


def finish_barrier(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("finish.prepared",))
    topic = "reception/finish"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "finish-subscriber") as subscriber:
            subscriber.connect("finish-subscriber", session_present=False, receive_maximum=1)
            subscriber.subscribe(1, [(topic, 1)])
            first = publisher(broker, case, "finish-publisher-a", "mqttium", topic, b"first")
            delivered = subscriber.next_publication()
            second = publisher(broker, case, "finish-publisher-b", "paho", topic, b"second", 2)
            before = durable(broker.db)
            require(len(obligations(before)) == 2, "second_obligation_not_queued")
            subscriber.no_publication(.15, code="destination_receive_maximum_ignored")
            before_sequence = max(int(event["seq"]) for event in broker.events())
            subscriber.acknowledge(delivered)
            hit = broker.wait_event("barrier_hit", point="finish.prepared", after=before_sequence)
            held = durable(broker.db)
            require(delivery_keys(held) == delivery_keys(before), "premature_delivery_release")
            require(held["tables"]["payloads"] == before["tables"]["payloads"], "premature_delivery_release")
            accounts_consistent(held)
            write_json(case / "before-finish-commit.json", held)
            subscriber.no_publication(.2, code="premature_delivery_release")
            # A premature free can submit Claim2 even while the single writer
            # is held in Finish1. Observe this transition, not only wire output.
            early_claims = [event for event in broker.events() if int(event["seq"]) > before_sequence
                            and event["kind"] == "command_submitted"
                            and (event.get("command_kind") == "Claim"
                                 or event.get("details", {}).get("command_kind") == "Claim")]
            require(not early_claims, "premature_delivery_release", "another claim submitted before finish confirmation")
            broker.release("finish.prepared")
            next_delivery = subscriber.next_publication()
            message_matches(next_delivery, topic, b"second")
            subscriber.acknowledge(next_delivery)
            next_hit = broker.wait_event("barrier_hit", point="finish.prepared", after=int(hit["seq"]))
            require(next_hit["operation"] != hit["operation"], "finish_reused_operation")
            broker.release("finish.prepared")
        drained(broker, case, admissions=2)
        broker.stop()
        return {"criterion": "D-finish", "status": "PASS", "clients": [first, second],
                "point": hit, "second_point": next_hit,
                "no_release_or_reuse_before_finish": True}
    finally:
        broker.cleanup()


def lost_committed_results(binary: Path, case: Path) -> dict:
    """A lost terminal reply is resolved using the original durable recipe."""
    broker = Broker(binary, case, test_control=True)
    topic = "reception/lost-result"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "lost-result-subscriber") as subscriber:
            subscriber.connect("lost-result-subscriber", session_present=False, receive_maximum=1)
            subscriber.subscribe(1, [(topic, 1)])
            (broker.control / "drop-next-claim-result").touch(exist_ok=False)
            first = publisher(broker, case, "lost-result-publisher-a", "mqttium", topic, b"claim-result-lost")
            claim_dropped = broker.wait_event("store_result_dropped")
            require(claim_dropped.get("command_kind") == "Claim", "wrong_result_suppressed")
            claim_state = durable(broker.db)
            target = session_key(claim_state, "lost-result-subscriber")
            claim = assignment(claim_state, target)
            claim_id = operation_hex(claim_dropped["operation"])
            require(claim["row"]["claim"] == claim_id
                    and sum(row["id"] == claim_id for row in claim_state["tables"]["operations"]) == 1,
                    "dropped_claim_not_durably_committed")
            write_json(case / "claim-committed-result-lost.json", claim_state)
            subscriber.no_publication(.2, code="unknown_claim_accepted_as_committed")
            subscriber.send(b"\xc0\x00")
            subscriber.wait(0xD0)
            delivered = subscriber.next_publication(timeout=15)
            message_matches(delivered, topic, b"claim-result-lost")
            claim_resolved = broker.wait_event("store_resolved", after=int(claim_dropped["seq"]))
            require(claim_resolved["operation"] == claim_dropped["operation"]
                    and claim_resolved["details"]["outcome"] == "committed",
                    "claim_resolution_changed_operation")
            require(assignment(durable(broker.db), target) == claim, "claim_resolution_recreated_slot")
            second = publisher(broker, case, "lost-result-publisher-b", "paho", topic,
                               b"finish-result-lost", 2)
            subscriber.no_publication(.15, code="destination_receive_maximum_ignored")
            (broker.control / "drop-next-finish-result").touch(exist_ok=False)
            subscriber.acknowledge(delivered)
            finish_dropped = broker.wait_event("store_result_dropped", after=int(claim_dropped["seq"]))
            require(finish_dropped.get("command_kind") == "Finish", "wrong_result_suppressed")
            finish_state = durable(broker.db)
            finish_id = operation_hex(finish_dropped["operation"])
            require(sum(row["id"] == finish_id for row in finish_state["tables"]["operations"]) == 1
                    and len(obligations(finish_state)) == 1,
                    "dropped_finish_not_durably_committed")
            accounts_consistent(finish_state)
            write_json(case / "finish-committed-result-lost.json", finish_state)
            subscriber.no_publication(.2, code="unknown_finish_freed_slot")
            subscriber.send(b"\xc0\x00")
            subscriber.wait(0xD0)
            next_delivery = subscriber.next_publication(timeout=15)
            message_matches(next_delivery, topic, b"finish-result-lost")
            finish_resolved = broker.wait_event("store_resolved", after=int(finish_dropped["seq"]))
            require(finish_resolved["operation"] == finish_dropped["operation"]
                    and finish_resolved["details"]["outcome"] == "committed",
                    "finish_resolution_changed_operation")
            subscriber.acknowledge(next_delivery)
        final = drained(broker, case, admissions=2)
        for operation in (claim_id, finish_id):
            require(sum(row["id"] == operation for row in final["tables"]["operations"]) == 1,
                    "resolution_reexecuted_under_new_identity")
        broker.stop()
        writer_available(broker.db)
        return {"criterion": "D-resolution", "status": "PASS", "clients": [first, second],
                "claim_drop": claim_dropped, "claim_resolution": claim_resolved,
                "finish_drop": finish_dropped, "finish_resolution": finish_resolved,
                "same_operation_resolved": True, "unknown_did_not_authorize_progress": True}
    finally:
        broker.cleanup()


def crash_delivery(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    restarted = None
    first = None
    topic, payload = "reception/crash", b"observe-before-sigkill"
    try:
        broker.ready()
        first = PubsubWire(broker.address, broker.observations, "crash-subscriber")
        first.connect("crash-subscriber", session_present=False, receive_maximum=1)
        first.subscribe(1, [(topic, 1)])
        published = publisher(broker, case, "crash-publisher", "mqttium", topic, payload, 2)
        observed = first.next_publication()
        message_matches(observed, topic, payload)
        pending = durable(broker.db)
        require(len(obligations(pending)) == 1, "crash_obligation_missing")
        target = session_key(pending, "crash-subscriber")
        old_assignment = assignment(pending, target)
        write_json(case / "before-crash.json", pending)
        inode = broker.db.stat().st_ino
        status = broker.stop(killed=True)
        first.close()
        restarted = Broker(binary, case, db=broker.db, label="restarted")
        restarted.ready()
        require(restarted.db.stat().st_ino == inode, "restart_replaced_database")
        with PubsubWire(restarted.address, restarted.observations, "resumed-subscriber") as subscriber:
            subscriber.connect("crash-subscriber", session_present=True, receive_maximum=1)
            replay = subscriber.next_publication()
            message_matches(replay, topic, payload)
            require(replay.packet_id == observed.packet_id and replay.dup,
                    "outgoing_exchange_identity_not_resumed")
            recovered = durable(restarted.db)
            require(delivery_keys(recovered) == delivery_keys(pending)
                    and len(recovered["admissions"]) == 1, "recovery_recreated_obligation")
            new_assignment = assignment(recovered, target)
            for field in ("packet_id", "claim", "generation", "slot_generation", "publication", "idx", "delivery"):
                if field in old_assignment["row"]:
                    require(old_assignment["row"][field] == new_assignment["row"][field],
                            "outgoing_exchange_identity_not_resumed", field)
            write_json(case / "after-recovery.json", recovered)
            subscriber.acknowledge(replay)
        drained(restarted, case, admissions=1)
        restarted.stop()
        writer_available(broker.db)
        return {"criterion": "E", "status": "PASS", "clients": [published], "kill_status": status,
                "same_file_inode": inode, "first_publish": observed.observation(),
                "retransmission": replay.observation(), "durable_assignment": old_assignment,
                "same_delivery_retransmitted": True, "no_obligation_recreation": True}
    finally:
        if first:
            first.close()
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def identities_and_windows(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    topic = "reception/window"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "bidirectional") as subscriber:
            subscriber.connect("bidirectional", session_present=False, receive_maximum=1)
            subscriber.subscribe(1, [(topic, 1)])
            seeded = publisher(broker, case, "window-publisher", "mqttium", topic, b"both-directions", 1)
            initial = subscriber.next_publication()
            message_matches(initial, topic, b"both-directions")
            # The subscriber now deliberately chooses the broker's observed
            # outgoing PI for its own PUBLISH, while that outgoing exchange is
            # still unacknowledged. The namespaces are independent; no choice
            # of the broker's allocation algorithm is assumed by this test.
            subscriber.publish(initial.packet_id, topic, b"queued-second", qos=2)
            pending = durable(broker.db)
            require(len(obligations(pending)) == 2, "window_durable_backlog_missing")
            for _ in range(4):
                subscriber.send(b"\xc0\x00")
                subscriber.wait(0xD0)
                subscriber.no_publication(.15, code="destination_receive_maximum_ignored")
            subscriber.acknowledge(initial, reason=0x80)
            second = subscriber.next_publication()
            message_matches(second, topic, b"queued-second")
            # A repeated old PUBACK cannot finish a newer slot. Implementations
            # can avoid wire ambiguity by not reusing live-generation PIs.
            if second.packet_id != initial.packet_id:
                subscriber.send(puback_packet(initial.packet_id))
                time.sleep(.05)
                require(len(obligations(durable(broker.db))) == 1, "old_puback_finished_new_delivery")
            subscriber.acknowledge(second)
            subscriber.no_publication(.2, code="negative_puback_retransmitted_forever")
        drained(broker, case, admissions=2)
        # This normal-build sequence uses a new PI after every PUBACK actually
        # received. It also covers the write-complete/internal-receipt gap;
        # reusing only the same PI would miss the confirmed window defect.
        with PubsubWire(broker.address, broker.observations, "receive-credit") as credit:
            credit.connect("receive-credit", session_present=False)
            for identifier in range(1, 8):
                credit.send(publish_packet("reception/no-route", b"held-qos2", identifier, qos=2))
                require(credit.wait(0x50, identifier).ack() == (identifier, 0), "held_exchange_refused")
            for identifier in range(8, 40):
                credit.publish(identifier, "reception/no-route", b"fresh-after-puback", qos=1)
            for identifier in range(1, 8):
                credit.send(pubrel_packet(identifier))
                require(credit.wait(0x70, identifier).ack() == (identifier, 0), "held_exchange_not_completed")
        # A genuine ninth in-flight exchange is still over the declared limit.
        with PubsubWire(broker.address, broker.observations, "true-ninth") as ninth:
            ninth.connect("true-ninth", session_present=False)
            for identifier in range(1, 9):
                ninth.send(publish_packet("reception/no-route", b"held-qos2", identifier, qos=2))
                require(ninth.wait(0x50, identifier).ack() == (identifier, 0), "held_exchange_refused")
            ninth.send(publish_packet("reception/no-route", b"actual-ninth", 9, qos=1))
            refusal = ninth.wait(0xE0)
            require(refusal.body and refusal.body[0] == 0x93, "true_ninth_not_refused")
        with PubsubWire(broker.address, broker.observations, "collision") as collision:
            collision.connect("collision", session_present=False)
            collision.send(publish_packet("reception/no-route", b"held-qos2", 7, qos=2))
            collision.wait(0x50, 7)
            collision.send(subscribe_packet(7, [("reception/collision", 1)]))
            try:
                answer = collision.poll(2)
            except ReceiptFailure as error:
                require(error.code == "peer_closed", "collision_failed_unexpectedly")
                answer = None
            if answer is not None and answer.kind == 0x90:
                require(decode_filter_ack(answer) == (7, (0x91,)), "packet_identifier_collision_accepted")
            elif answer is not None:
                require(answer.kind == 0xE0 and answer.body and answer.body[0] >= 128,
                        "packet_identifier_collision_accepted")
            else:
                require(collision.closed, "collision_refusal_missing")
        state = durable(broker.db)
        require(not any(bytes.fromhex(row["topic"]) == b"reception/collision"
                        for row in state["tables"]["routes"]), "colliding_subscription_became_durable")
        broker.stop()
        return {"criterion": "F", "status": "PASS", "clients": [seeded],
                "same_integer_both_directions": initial.packet_id,
                "receive_maximum": 1, "no_live_periodic_retransmission": True,
                "negative_puback_terminal": True, "same_sender_collision_refused": True,
                "receive_window_seven_held_qos2": 7, "fresh_sequential_qos1_after_puback": 32,
                "genuine_ninth_refused": "0x93"}
    finally:
        broker.cleanup()


def filter_lifecycle(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    topic = "reception/filter-life"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "filter-owner") as subscriber:
            subscriber.connect("filter-owner", session_present=False)
            subscriber.subscribe(1, [(topic, 1)])
            subscriber.subscribe(2, [(topic, 2)])
            state = durable(broker.db)
            target = session_key(state, "filter-owner")
            matches = [row for row in state["tables"]["routes"]
                       if row["topic"] == topic.encode().hex() and row["session"] == target]
            require(len(matches) == 1 and matches[0]["qos"] == 1, "resubscribe_duplicated_or_misgranted")
        published = publisher(broker, case, "filter-publisher-a", "mqttium", topic, b"owed-before-unsubscribe")
        pending = durable(broker.db)
        require(len(obligations(pending, target)) == 1, "offline_obligation_missing")
        with PubsubWire(broker.address, broker.observations, "filter-resumed") as subscriber:
            subscriber.connect("filter-owner", session_present=True)
            subscriber.unsubscribe(3, [topic])
            removed = durable(broker.db)
            require(not any(row["topic"] == topic.encode().hex() and row["session"] == target
                            for row in removed["tables"]["routes"]), "unsuback_before_durable_removal")
            require(delivery_keys(removed) == delivery_keys(pending), "unsubscribe_deleted_existing_obligation")
            published_after = publisher(broker, case, "filter-publisher-b", "paho", topic, b"after-unsubscribe", 2)
            require(delivery_keys(durable(broker.db)) == delivery_keys(pending), "removed_filter_admitted_new_obligation")
            owed = subscriber.next_publication()
            message_matches(owed, topic, b"owed-before-unsubscribe")
            subscriber.acknowledge(owed)
            subscriber.no_publication(.2, code="removed_filter_still_matched")
            subscriber.unsubscribe(4, [topic])
        final = drained(broker, case, admissions=2)
        require(final["counts"]["routes"] == 0, "subscription_not_removed")
        broker.stop()
        return {"criterion": "G", "status": "PASS", "clients": [published, published_after],
                "resubscribe_replaced_without_duplicate": True,
                "old_obligation_survived_unsubscribe": True, "new_admission_has_no_target": True}
    finally:
        broker.cleanup()


def filter_result_order(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "filter-results") as subscriber:
            subscriber.connect("filter-results", session_present=False)
            subscriber.send(subscribe_packet(17, [("reception/allowed", 2),
                                                  ("reception/qos0-refused", 0),
                                                  ("reception/wildcard/+", 1)]))
            mid, reasons = decode_filter_ack(subscriber.wait(0x90, 17))
            require(mid == 17 and len(reasons) == 3 and reasons[0] == 1
                    and reasons[1] >= 128 and reasons[2] == 0xA2,
                    "subscription_results_out_of_order_or_unsupported_granted", repr(reasons))
            state = durable(broker.db)
            routes = state["tables"]["routes"]
            require(len(routes) == 1 and routes[0]["topic"] == b"reception/allowed".hex()
                    and routes[0]["qos"] == 1, "refused_filter_became_durable")
            subscriber.unsubscribe(18, ["reception/allowed", "reception/not-subscribed"])
        require(durable(broker.db)["counts"]["routes"] == 0, "unsubscribe_not_durable")
        broker.stop()
        return {"criterion": "F-filter-results", "status": "PASS", "ordered_reason_codes": list(reasons),
                "qos2_granted_qos1": True, "qos0_and_wildcard_refused": True}
    finally:
        broker.cleanup()


def destination_packet_limit(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    topic, payload = "reception/packet-limit", b"p" * 128
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "small-destination") as subscriber:
            subscriber.connect("small-destination", session_present=False, maximum_packet_size=64)
            subscriber.subscribe(1, [(topic, 1)])
            published = publisher(broker, case, "packet-limit-publisher", "mqttium", topic, payload)
            try:
                frame = subscriber.poll(.3)
            except ReceiptFailure as error:
                require(error.code == "peer_closed", "packet_limit_failed_unexpectedly")
                frame = None
            if frame is not None:
                require(frame.kind != 0x30, "destination_maximum_packet_ignored")
                require(frame.kind == 0xE0 and len(frame.body) <= 62 and frame.body[0] >= 128,
                        "packet_limit_refusal_invalid")
            pending = durable(broker.db)
            require(len(obligations(pending)) == 1, "oversized_delivery_obligation_discarded")
            write_json(case / "retained-for-larger-connection.json", pending)
        with PubsubWire(broker.address, broker.observations, "larger-destination") as subscriber:
            subscriber.connect("small-destination", session_present=True, maximum_packet_size=65536)
            delivered = subscriber.next_publication()
            message_matches(delivered, topic, payload)
            require(delivery_keys(durable(broker.db)) == delivery_keys(pending), "packet_limit_recreated_obligation")
            subscriber.acknowledge(delivered)
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "F-packet-limit", "status": "PASS", "clients": [published],
                "destination_maximum_packet_size": 64, "payload_bytes": 128,
                "obligation_retained_and_later_delivered": True}
    finally:
        broker.cleanup()


def filter_admission_order(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("subscription.prepared",))
    topic = "reception/filter-order"
    source = None
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "ordered-subscriber") as subscriber:
            subscriber.connect("ordered-subscriber", session_present=False)
            subscriber.send(subscribe_packet(1, [(topic, 1)]))
            added = broker.wait_event("barrier_hit", point="subscription.prepared")
            before = durable(broker.db)
            require(before["counts"]["routes"] == 0, "subscription_visible_before_commit")
            broker.release("subscription.prepared")
            require(decode_filter_ack(subscriber.wait(0x90, 1)) == (1, (1,)), "subscription_not_confirmed")
            source = PubsubWire(broker.address, broker.observations, "ordered-publisher")
            source.connect("ordered-publisher", session_present=False)
            subscriber.send(unsubscribe_packet(2, [topic]))
            removed = broker.wait_event("barrier_hit", point="subscription.prepared", after=int(added["seq"]))
            source.send(publish_packet(topic, b"ordered-after-removal", 1, qos=1))
            held = durable(broker.db)
            require(held["counts"]["routes"] == 1 and not held["admissions"], "route_order_precondition")
            frame = source.poll(.15)
            require(frame is None, "publication_bypassed_pending_route_projection")
            broker.release("subscription.prepared")
            require(decode_filter_ack(subscriber.wait(0xB0, 2)) == (2, (0,)), "unsubscribe_not_confirmed")
            require(source.wait(0x40, 1).ack() == (1, 0), "ordered_publication_refused")
            subscriber.no_publication(.2, code="admission_used_removed_route_projection")
        state = drained(broker, case, admissions=1)
        require(state["counts"]["routes"] == 0, "route_removal_not_durable")
        broker.stop()
        return {"criterion": "G-order", "status": "PASS", "subscribe_point": added,
                "unsubscribe_point": removed, "publisher_after_removal_has_no_obligation": True}
    finally:
        if source:
            source.close()
        broker.cleanup()


def partial_destination_recovery(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    restarted = None
    topic, payload = "reception/partial", b"shared-payload-two-references"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "destination-B") as b, \
                PubsubWire(broker.address, broker.observations, "destination-C") as c:
            b.connect("destination-B", session_present=False)
            c.connect("destination-C", session_present=False)
            b.subscribe(1, [(topic, 1)])
            c.subscribe(1, [(topic, 1)])
            published = publisher(broker, case, "partial-publisher", "mqttium", topic, payload, 2)
            bm, cm = b.next_publication(), c.next_publication()
            message_matches(bm, topic, payload)
            message_matches(cm, topic, payload)
            both = durable(broker.db)
            require(len(obligations(both)) == 2 and both["counts"]["payloads"] == 1,
                    "two_destinations_not_one_shared_payload")
            accounts_consistent(both)
            b_target, c_target = session_key(both, "destination-B"), session_key(both, "destination-C")
            b.acknowledge(bm)
            remaining = until(lambda: (state if len(obligations(state := durable(broker.db))) == 1 else None),
                              "destination_B_not_finished")
            require(not obligations(remaining, b_target) and len(obligations(remaining, c_target)) == 1,
                    "wrong_destination_finished")
            accounts_consistent(remaining)
            write_json(case / "only-C-remains.json", remaining)
            status = broker.stop(killed=True)
        restarted = Broker(binary, case, db=broker.db, label="restarted")
        restarted.ready()
        with PubsubWire(restarted.address, restarted.observations, "resumed-B") as b, \
                PubsubWire(restarted.address, restarted.observations, "resumed-C") as c:
            b.connect("destination-B", session_present=True)
            b.no_publication(.2, code="finished_destination_reappeared")
            c.connect("destination-C", session_present=True)
            replay = c.next_publication()
            message_matches(replay, topic, payload)
            require(replay.packet_id == cm.packet_id and replay.dup, "pending_destination_not_resumed")
            recovered = durable(restarted.db)
            require(delivery_keys(recovered) == delivery_keys(remaining), "recovery_recreated_finished_obligation")
            require(recovered["tables"]["payloads"] == remaining["tables"]["payloads"], "recovery_changed_reference_count")
            c.acknowledge(replay)
        drained(restarted, case, admissions=1)
        restarted.stop()
        writer_available(broker.db)
        return {"criterion": "H-recovery", "status": "PASS", "clients": [published],
                "kill_status": status, "B_not_recreated": True, "C_same_exchange_replayed": True,
                "last_reference_released_payload": True}
    finally:
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def discovery_without_notification(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    topic, payload = "reception/discovery", b"durable-work-without-notification"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "silent-subscriber") as subscriber:
            subscriber.connect("silent-subscriber", session_present=False)
            subscriber.subscribe(1, [(topic, 1)])
            (broker.control / "drop-work-notifications").touch()
            published = publisher(broker, case, "silent-publisher", "mqttium", topic, payload)
            dropped = broker.wait_event("work_notification_dropped")
            pending = durable(broker.db)
            require(len(obligations(pending)) == 1, "suppressed_notification_has_no_durable_work")
            write_json(case / "durable-without-notification.json", pending)
            subscriber.send(b"\xc0\x00")
            subscriber.wait(0xD0)
            try:
                message = subscriber.next_publication(timeout=3)
            except ReceiptFailure as error:
                if error.code != "outgoing_delivery_missing":
                    raise
                subscriber.send(b"\xc0\x00")
                subscriber.wait(0xD0)
                still = durable(broker.db)
                require(delivery_keys(still) == delivery_keys(pending) and broker.process.poll() is None,
                        "discovery_failure_unrelated")
                raise ReceiptFailure("durable_work_lost_without_notification",
                                     "suppressed notification was reached, durable obligation remains, controls still progress") from error
            message_matches(message, topic, payload)
            subscriber.acknowledge(message)
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "H-discovery", "status": "PASS", "clients": [published],
                "notification_drop": dropped, "idle_durable_discovery": True}
    finally:
        broker.cleanup()


def atomic_capacity_refusal(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    original = None
    topic = "reception/capacity"
    try:
        broker.ready()
        with PubsubWire(broker.address, broker.observations, "capacity-B") as b, \
                PubsubWire(broker.address, broker.observations, "capacity-C") as c:
            b.connect("capacity-B", session_present=False)
            c.connect("capacity-C", session_present=False)
            b.subscribe(1, [(topic, 1)])
            c.subscribe(1, [(topic, 1)])
            before = durable(broker.db)
            with sqlite3.connect(broker.db, timeout=.25) as db:
                original = db.execute("SELECT limit_references FROM meta").fetchone()[0]
                db.execute("UPDATE meta SET limit_references=?", ((1).to_bytes(8, "big"),))
            rejected = publisher(broker, case, "capacity-publisher-rejected", "mqttium", topic,
                                 b"must-not-partially-admit", expected="rejected")
            require(any(event["kind"] == "receipt_rejected" and event.get("reason_code") == 0x97
                        for event in rejected["result"]["events"]), "capacity_refusal_wrong_reason")
            after = durable(broker.db)
            require(len(after["admissions"]) == 0 and len(obligations(after)) == 0,
                    "capacity_refusal_partially_admitted")
            for name in ("obligations", "payloads", "publications", "routes"):
                require(after["tables"][name] == before["tables"][name], "capacity_refusal_partially_admitted", name)
            accounts_consistent(after)
            write_json(case / "certain-refusal.json", after)
            with sqlite3.connect(broker.db, timeout=.25) as db:
                db.execute("UPDATE meta SET limit_references=?", (original,))
            original = None
            accepted = publisher(broker, case, "capacity-publisher-accepted", "paho", topic, b"capacity-restored", 2)
            bm, cm = b.next_publication(), c.next_publication()
            b.acknowledge(bm)
            c.acknowledge(cm)
        drained(broker, case, admissions=1)
        broker.stop()
        return {"criterion": "H-capacity", "status": "PASS", "clients": [rejected, accepted],
                "atomic_refusal_for_two_destinations": True,
                "intervention": "isolated post-readiness reference limit, restored before completion"}
    finally:
        if original is not None and broker.process.poll() is None:
            with sqlite3.connect(broker.db, timeout=.25) as db:
                db.execute("UPDATE meta SET limit_references=?", (original,))
        broker.cleanup()


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
    cases = [("A", nominal), ("B", persistent_subscription), ("C", two_destinations),
             ("E", crash_delivery), ("F", identities_and_windows),
             ("F-filter-results", filter_result_order), ("F-packet-limit", destination_packet_limit),
             ("G", filter_lifecycle),
             ("H-recovery", partial_destination_recovery)]
    if args.mode == "complete" or args.case:
        cases += [("D-claim", claim_barrier), ("D-finish", finish_barrier),
                  ("D-resolution", lost_committed_results),
                  ("G-order", filter_admission_order),
                  ("H-discovery", discovery_without_notification), ("H-capacity", atomic_capacity_refusal)]
    require(args.case is None or args.case in {name for name, _ in cases}, "unknown_case")
    result = {"format": "mqttiumd.pubsub-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "mode": args.mode,
              "scope": "exact persistent subscriptions; incoming QoS 1/2 to outgoing QoS 1",
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
