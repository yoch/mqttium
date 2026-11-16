#!/usr/bin/env python3
"""Finite live receipt-collection system tests, not fixture qualification.

The historical ceiling case is derived from the supplied review probe. Its
binary and source identity are explicit; an old run is never relabeled as a
new implementation. A missing response counts as the known receipt ceiling
only when the exact full receipt table, bounded live state, unknown result
and independent control progress are observed together.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time

from external_mqttium import client_identity
from pubsub_probe import PubsubWire, decode_filter_ack, durable, session_key, subscribe_packet
from pubsub_reception import accounts_consistent, assignment, delivery_keys, drained, until
from tcp_probe import ReceiptFailure, operation_hex, publish_packet, pubrel_packet, require, write_json
from tcp_reception import Broker, writer_available


HISTORICAL_RECEIPT_LIMIT = 8192
BUSINESS_COMMANDS = 32769
WINDOW_COMMANDS = 1024


class BusinessTranscript:
    """Every public business ACK is retained on disk, not in a growing list."""
    def __init__(self, path: Path):
        self.path = path
        self.stream = path.open("xb")
        self.records, self.bytes = 0, 0
        self.digest = hashlib.sha256()

    def record(self, kind: str, **fields):
        record = {"kind": kind, "sequence": self.records,
                  "monotonic_ns": str(time.monotonic_ns()), **fields}
        encoded = (json.dumps(record, sort_keys=True) + "\n").encode()
        require(len(encoded) <= 2048 and self.bytes + len(encoded) <= 16 * 1024 * 1024,
                "business_transcript_budget")
        self.stream.write(encoded)
        self.digest.update(encoded)
        self.records += 1
        self.bytes += len(encoded)

    def flush(self):
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self):
        self.flush()
        self.stream.close()

    def identity(self) -> dict:
        return {"path": str(self.path), "sha256": self.digest.hexdigest(),
                "records": self.records, "bytes": self.bytes,
                "scope": "public API observations; packet bytes are separate transport witnesses"}


def storage_sample(broker: Broker) -> dict:
    """Bounded independent read; no database cleanup or writer PRAGMAs.

    Page count and free-list count describe the shared file. Connection-local
    synchronous/autocheckpoint getters cannot establish the writer's settings;
    those must come from the product's effective-configuration observation.
    """
    started = time.monotonic_ns()
    connection = sqlite3.connect(f"file:{broker.db}?mode=ro", uri=True, timeout=2)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        require(len(tables) <= 32 and all(re.fullmatch(r"[a-z_]+", name) for name in tables),
                "measurement_schema_budget")
        counts = {name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                  for name in tables}
        compact = {}
        for name in tables:
            if not any(word in name for word in ("frontier", "origin", "retire", "confirm", "closure", "fence")):
                continue
            cursor = connection.execute(f'SELECT * FROM "{name}" ORDER BY 1 LIMIT 65')
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            require(len(rows) <= 64, "measurement_origin_budget", name)
            compact[name] = [{column: value.hex() if isinstance(value, bytes) else value
                              for column, value in zip(columns, row)} for row in rows]
        pragmas = {name: connection.execute(f"PRAGMA {name}").fetchone()[0]
                   for name in ("page_count", "freelist_count", "page_size", "journal_mode", "user_version")}
        meta_cursor = connection.execute("SELECT * FROM meta LIMIT 2")
        meta_columns = [description[0] for description in meta_cursor.description]
        meta_rows = meta_cursor.fetchall()
        require(len(meta_rows) == 1, "measurement_metadata_invalid")
        metadata = {column: value.hex() if isinstance(value, bytes) else value
                    for column, value in zip(meta_columns, meta_rows[0])}
        used_receipts = int(metadata["used_receipt_slots"], 16)
        require(used_receipts == counts.get("operations", 0) + counts.get("fences", 0),
                "receipt_credit_double_returned",
                f"metadata={used_receipts}, physical={counts.get('operations', 0) + counts.get('fences', 0)}")
        connection.execute("COMMIT")
    finally:
        connection.close()
    rss = None
    status = Path(f"/proc/{broker.process.pid}/status")
    if status.exists():
        match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", status.read_text(), re.MULTILINE)
        if match:
            rss = int(match[1]) * 1024
    return {"monotonic_ns": str(time.monotonic_ns()), "pid": broker.process.pid,
            "file_inode": broker.db.stat().st_ino, "counts": counts, "retirement": compact,
            "metadata": metadata,
            "sqlite_file": pragmas,
            "file_bytes": {"main": broker.db.stat().st_size,
                           "wal": Path(str(broker.db) + "-wal").stat().st_size
                           if Path(str(broker.db) + "-wal").exists() else 0,
                           "shm": Path(str(broker.db) + "-shm").stat().st_size
                           if Path(str(broker.db) + "-shm").exists() else 0},
            "resident_bytes": rss, "measurement_ms": (time.monotonic_ns() - started) / 1e6}


class EventTail:
    """Observe a long run without retaining its whole product stream in RAM.

    File offsets follow inodes, so a bounded product log rotation may rename
    the current file. Only counters and 256 maintenance witnesses are retained
    here; the actual retained files belong to this same process and run.
    """
    def __init__(self, path: Path, pid: int):
        self.path, self.pid = path, pid
        self.offsets = {}
        self.counts = Counter()
        self.recent = deque(maxlen=256)
        self.last_sequence = 0

    def read(self) -> dict:
        streams = [path for path in self.path.parent.glob(self.path.name + "*") if path.is_file()]
        require(len(streams) <= 16, "event_rotation_budget")
        records = []
        for path in streams:
            inode = path.stat().st_ino
            offset = self.offsets.get(inode, 0)
            with path.open("rb") as stream:
                stream.seek(offset)
                total = 0
                while True:
                    line = stream.readline(2049)
                    if not line or not line.endswith(b"\n"):
                        break
                    require(len(line) <= 2048, "product_event_record_budget")
                    total += len(line)
                    require(total <= 32 * 1024 * 1024, "measurement_event_window_budget")
                    event = json.loads(line)
                    require(event["pid"] == self.pid, "wrong_event_process")
                    records.append(event)
                    offset = stream.tell()
            self.offsets[inode] = offset
        # Rotation can expose an older retained segment after a newer one.
        for event in sorted(records, key=lambda value: int(value["seq"])):
            sequence = int(event["seq"])
            if sequence <= self.last_sequence:
                continue
            self.last_sequence = sequence
            self.counts[event["kind"]] += 1
            if any(word in event["kind"] for word in ("receipt", "retire", "collect", "frontier", "confirm", "closure")):
                self.recent.append(event)
        require(len(self.offsets) <= 64, "event_inode_budget")
        return {"last_sequence": str(self.last_sequence), "counts": dict(self.counts),
                "recent_maintenance": list(self.recent)}


class PahoPeer:
    """A second public implementation kept connected during the live run."""
    def __init__(self, address: tuple[str, int], topic: str):
        import paho.mqtt.client as mqtt
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties
        require(importlib.metadata.version("paho-mqtt") == "2.1.0", "paho_version")
        self.identity = {"name": "paho-mqtt", "version": "2.1.0",
                         "module_path": str(Path(mqtt.__file__).resolve()),
                         "module_sha256": hashlib.sha256(Path(mqtt.__file__).read_bytes()).hexdigest()}
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                                  client_id="receipt-live-paho", protocol=mqtt.MQTTv5,
                                  reconnect_on_failure=False, manual_ack=True)
        self.client.max_queued_messages_set(16)
        self.client.max_inflight_messages_set(8)
        self.connected, self.subscribed = threading.Event(), threading.Event()
        self.messages = queue.Queue(maxsize=16)
        self.failure = None
        self.connect_count = 0
        self.connect_observation = None
        self.suback = None

        def connected(_client, _userdata, flags, reason, _properties):
            self.connect_count += 1
            self.connect_observation = {"reason": reason.value, "session_present": flags.session_present}
            self.connected.set()

        def subscribed(_client, _userdata, mid, reasons, _properties):
            self.suback = {"packet_id": mid, "reason_codes": [reason.value for reason in reasons]}
            self.subscribed.set()

        def message(_client, _userdata, publication):
            try:
                self.messages.put_nowait(publication)
            except queue.Full:
                self.failure = "paho_inbound_budget"

        self.client.on_connect, self.client.on_subscribe, self.client.on_message = connected, subscribed, message
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 0xFFFFFFFF
        properties.ReceiveMaximum, properties.MaximumPacketSize = 8, 65536
        try:
            self.client.connect(*address, keepalive=10, clean_start=False, properties=properties)
            self.client.loop_start()
            require(self.connected.wait(3), "paho_connack_missing")
            require(self.connect_observation == {"reason": 0, "session_present": False}, "paho_connack_invalid")
            code, mid = self.client.subscribe([(topic, 1)])
            require(code == mqtt.MQTT_ERR_SUCCESS and self.subscribed.wait(12), "paho_suback_missing")
            require(self.suback == {"packet_id": mid, "reason_codes": [1]}, "paho_suback_invalid")
        except Exception:
            self.close()
            raise

    def publish(self, topic: str, payload: bytes, qos: int) -> dict:
        receipt = self.client.publish(topic, payload, qos=qos, retain=False)
        receipt.wait_for_publish(timeout=12)
        require(receipt.is_published(), "paho_publication_not_complete")
        return {"packet_id": receipt.mid, "qos": qos, "result": "mqtt-acknowledgement"}

    def receive(self, topic: str, payload: bytes) -> dict:
        try:
            message = self.messages.get(timeout=12)
        except queue.Empty as error:
            raise ReceiptFailure("paho_delivery_missing") from error
        require(self.failure is None and message.topic == topic and bytes(message.payload) == payload
                and int(message.qos) == 1, "paho_delivery_wrong")
        require(self.client.ack(message.mid, message.qos) == 0, "paho_ack_submission")
        return {"packet_id": message.mid, "qos": 1, "payload_sha256": hashlib.sha256(payload).hexdigest()}

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


class HeldBusiness:
    """Live responsibilities deliberately older than several receipt cycles."""
    def __init__(self, broker: Broker, case: Path):
        self.broker, self.case = broker, case
        self.claim_subscriber = None
        self.source = None
        self.seed = None

    def prepare(self) -> dict:
        broker = self.broker
        with PubsubWire(broker.address, broker.observations, "old-offline-subscription") as offline:
            offline.connect("receipt-old-offline", session_present=False, keep_alive=0)
            offline.subscribe(1, [("reception/receipt-offline", 1)])
            old_sequence = max(int(event["seq"]) for event in broker.events())
            offline.send(b"\xe0\x00")
        broker.wait_event("connection_closed", after=old_sequence)
        self.claim_subscriber = PubsubWire(broker.address, broker.observations, "old-unacknowledged-claim")
        self.claim_subscriber.connect("receipt-old-claim", session_present=False, receive_maximum=1,
                                      keep_alive=0)
        self.claim_subscriber.subscribe(1, [("reception/receipt-claim", 1)])
        self.source = PubsubWire(broker.address, broker.observations, "old-qos2-source")
        self.source.connect("receipt-old-source", session_present=False, keep_alive=0)
        self.source.publish(1, "reception/receipt-offline", b"old-offline-obligation", qos=2)
        self.source.publish(2, "reception/receipt-claim", b"old-claimed-obligation", qos=2)
        self.first_claim = self.claim_subscriber.next_publication()
        require(self.first_claim.payload == b"old-claimed-obligation" and self.first_claim.qos == 1,
                "held_claim_publish_wrong")
        self.source.send(publish_packet("reception/receipt-inbound", b"old-await-pubrel", 77, qos=2))
        require(self.source.wait(0x50, 77).ack() == (77, 0), "held_inbound_not_admitted")
        with PubsubWire(broker.address, broker.observations, "old-consumed-qos0") as qos0:
            qos0.connect("receipt-old-consumed", session_present=False, keep_alive=0)
            qos0.subscribe(1, [("reception/receipt-consumed", 0)], expected=(0,))
            self.source.publish(3, "reception/receipt-consumed", b"already-consumed-once", qos=2)
            consumed = qos0.next_publication()
            require(consumed.qos == 0 and consumed.packet_id is None
                    and consumed.payload == b"already-consumed-once", "held_qos0_not_consumed")
        self.seed = durable(broker.db)
        self.offline_key = session_key(self.seed, "receipt-old-offline")
        self.claim_key = session_key(self.seed, "receipt-old-claim")
        self.source_key = session_key(self.seed, "receipt-old-source")
        self.claim = assignment(self.seed, self.claim_key)
        self.inbound = [row for row in self.seed["tables"]["inbound"] if row["session"] == self.source_key]
        require(len(self.inbound) == 1 and self.inbound[0]["packet_id"] == 77,
                "held_inbound_marker_missing")
        require(len(self.seed["tables"]["obligations"]) == 2
                and self.seed["counts"]["outbound"] == 1, "held_business_setup_incomplete")
        accounts_consistent(self.seed)
        self.seed_receipts = {row["id"] for row in self.seed["tables"]["operations"]}
        write_json(self.case / "held-before-cycles.json", self.seed)
        return {"offline_session": self.offline_key, "claim_session": self.claim_key,
                "source_session": self.source_key, "claim": self.claim,
                "incoming": self.inbound, "seed_receipt_ids": sorted(self.seed_receipts),
                "first_publish": self.first_claim.observation(),
                "consumed_qos0_observed": consumed.observation()}

    def after_cycles(self) -> dict:
        expected_deliveries = delivery_keys(self.seed)
        def settled():
            candidate = durable(self.broker.db)
            actual = delivery_keys(candidate)
            require(expected_deliveries <= actual, "live_obligation_lost_during_collection")
            # Public ACK calls establish submission, not durable Finish. Let
            # the last mixed exchange settle before isolating the old work.
            return candidate if actual == expected_deliveries else None
        state = until(settled, "last_live_delivery_not_finished")
        accounts_consistent(state)
        require(delivery_keys(state) == delivery_keys(self.seed), "live_obligation_lost_during_collection")
        require(assignment(state, self.claim_key) == self.claim,
                "live_claim_changed_during_collection")
        require([row for row in state["tables"]["inbound"] if row["session"] == self.source_key] == self.inbound,
                "await_pubrel_lost_during_collection")
        retained = self.seed_receipts & {row["id"] for row in state["tables"]["operations"]}
        require(not retained, "live_object_pinned_historical_receipt", repr(sorted(retained))[:256])
        write_json(self.case / "held-after-cycles.json", state)
        return {"old_receipts_collected": len(self.seed_receipts),
                "delivery_identities": sorted(delivery_keys(state)),
                "claim": self.claim, "incoming": self.inbound}

    def after_crash(self, restarted: Broker) -> dict:
        before = durable(restarted.db)
        accounts_consistent(before)
        require(delivery_keys(before) == delivery_keys(self.seed), "collection_crash_lost_obligation")
        require(assignment(before, self.claim_key) == self.claim, "collection_crash_changed_claim")
        require([row for row in before["tables"]["inbound"] if row["session"] == self.source_key] == self.inbound,
                "collection_crash_lost_await_pubrel")
        with PubsubWire(restarted.address, restarted.observations, "old-claim-after-restart") as claimed:
            claimed.connect("receipt-old-claim", session_present=True, receive_maximum=1, keep_alive=0)
            replay = claimed.next_publication()
            require(replay.payload == self.first_claim.payload and replay.topic == self.first_claim.topic
                    and replay.qos == 1 and replay.packet_id == self.first_claim.packet_id and replay.dup,
                    "collected_claim_not_resumed_with_same_exchange")
            claimed.acknowledge(replay)
        with PubsubWire(restarted.address, restarted.observations, "old-offline-after-restart") as offline:
            offline.connect("receipt-old-offline", session_present=True, keep_alive=0)
            queued = offline.next_publication()
            require(queued.topic == "reception/receipt-offline" and queued.payload == b"old-offline-obligation"
                    and queued.qos == 1, "collected_admission_lost_offline_work")
            offline.acknowledge(queued)
        with PubsubWire(restarted.address, restarted.observations, "old-inbound-after-restart") as source:
            source.connect("receipt-old-source", session_present=True, keep_alive=0)
            source.publish(77, "reception/receipt-inbound", b"old-await-pubrel", qos=2, dup=True)
        with PubsubWire(restarted.address, restarted.observations, "consumed-not-rebuilt") as consumed:
            consumed.connect("receipt-old-consumed", session_present=True, keep_alive=0)
            consumed.no_publication(.2, code="consumed_qos0_rebuilt_after_collection")
            control_progress(consumed)
        state = drained(restarted, self.case / "held-final")
        return {"criterion": "D-live", "status": "PASS", "claim_retransmission": replay.observation(),
                "offline_delivery": queued.observation(), "resources_final": state["counts"],
                "consumed_qos0_not_rebuilt": True,
                "same_obligations_finished_after_receipt_retirement": True}

    def close(self):
        if self.claim_subscriber:
            self.claim_subscriber.close()
        if self.source:
            self.source.close()


async def live_public_workload(broker: Broker, case: Path, terminal_commands: int) -> dict:
    """Public mqttium APIs drive business operations while two clients stay up."""
    from mqttium import MQTTProtocolVersion
    from mqttium.api import AsyncClient, Properties
    require(1 <= terminal_commands <= 40000, "live_command_budget")
    topic = "reception/receipt-live"
    client = AsyncClient("receipt-live-mqttium", protocol=MQTTProtocolVersion.MQTTv5,
                         clean_start=False, keepalive=10,
                         connect_properties=Properties({"session_expiry_interval": 0xFFFFFFFF}),
                         maximum_packet_size=65536, max_inbound_inflight=8,
                         max_inbound_inflight_bytes=65536, max_outbound_inflight=8,
                         max_unacknowledged_messages=16, max_unacknowledged_bytes=65536,
                         max_iterator_messages=16, max_iterator_bytes=65536,
                         max_write_queue_messages=16, max_write_queue_bytes=65536,
                         message_delivery="iterator", manual_ack=True, connect_timeout=3)
    paho = None
    transcript = BusinessTranscript(case / "public-client-transcript.jsonl")
    tail = EventTail(broker.events_path, broker.process.pid)
    result = {"clients": {"mqttium": client_identity()}, "client_process_id": os.getpid(),
              "windows": [], "mixed_exchanges": [],
              "business_terminal": {"subscription_replacements": 0, "mqttium_publications": 0,
                                    "paho_publications": 0},
              "parameters": {"replacement_budget": terminal_commands, "window_commands": WINDOW_COMMANDS,
                             "max_client_packet_identifiers": 40000 + 128,
                             "outgoing_qos1_budget": 128, "sql_cleanup": False}}
    started = time.monotonic_ns()
    previous = started
    try:
        connack = await client.connect(*broker.address, timeout=3)
        require(not connack.session_present and connack.reason_code == 0, "mqttium_connack_invalid")
        result["mqttium_connack"] = {"session_present": connack.session_present, "reason": connack.reason_code}
        transcript.record("mqttium_connack", **result["mqttium_connack"])
        paho = await asyncio.to_thread(PahoPeer, broker.address, topic)
        result["clients"]["paho"] = paho.identity
        result["paho_connack"], result["paho_suback"] = paho.connect_observation, paho.suback
        transcript.record("paho_connack", **paho.connect_observation)
        transcript.record("paho_suback", **paho.suback)
        iterator = client.messages()
        first_mid = last_mid = None
        for attempt in range(1, terminal_commands + 1):
            granted = (attempt // WINDOW_COMMANDS) % 2
            request_started = time.monotonic_ns()
            acknowledgement = await client.subscribe([(topic, granted)], timeout=12)
            require(acknowledgement.reason_codes == (granted,), "live_subscription_refused")
            require(1 <= acknowledgement.mid <= 65535, "live_subscription_packet_identifier")
            first_mid = first_mid if first_mid is not None else acknowledgement.mid
            last_mid = acknowledgement.mid
            result["business_terminal"]["subscription_replacements"] += 1
            transcript.record("mqttium_suback", attempt=attempt, packet_id=acknowledgement.mid,
                              reason_codes=list(acknowledgement.reason_codes),
                              elapsed_ms=(time.monotonic_ns() - request_started) / 1e6)
            if attempt % WINDOW_COMMANDS == 0 or attempt == terminal_commands:
                # Both durable input QoS classes and both output paths are
                # observed; no QoS 0 local client receipt stands in for receipt.
                qos = 1 if (attempt // WINDOW_COMMANDS) % 2 else 2
                payload = f"live-window-{attempt}-qos-{qos}".encode()
                receipt = await client.publish(topic, payload, qos=qos, retain=False, timeout=12)
                await receipt.wait(timeout=12)
                require(receipt.is_done() and receipt.exception() is None, "live_mqttium_publish_failed")
                result["business_terminal"]["mqttium_publications"] += 1
                message = await asyncio.wait_for(anext(iterator), timeout=12)
                require(message.topic == topic and bytes(message.payload) == payload
                        and int(message.qos) == granted, "live_mqttium_delivery_wrong")
                if granted:
                    await client.ack(message)
                observed_paho = await asyncio.to_thread(paho.receive, topic, payload)
                row = {"attempt": attempt, "incoming_qos": qos, "mqttium_publication_id": receipt.mid,
                       "mqttium_outgoing": {"qos": int(message.qos),
                                            "packet_id": message.mid if granted else None},
                       "paho_outgoing": observed_paho}
                if attempt % (4 * WINDOW_COMMANDS) == 0 or attempt == terminal_commands:
                    reverse_payload = f"live-paho-{attempt}".encode()
                    row["paho_publication"] = await asyncio.to_thread(paho.publish, topic, reverse_payload, 2)
                    result["business_terminal"]["paho_publications"] += 1
                    reverse = await asyncio.wait_for(anext(iterator), timeout=12)
                    require(reverse.topic == topic and bytes(reverse.payload) == reverse_payload
                            and int(reverse.qos) == granted, "live_mqttium_reverse_delivery_wrong")
                    if granted:
                        await client.ack(reverse)
                    row["paho_self_delivery"] = await asyncio.to_thread(paho.receive, topic, reverse_payload)
                result["mixed_exchanges"].append(row)
                transcript.record("mixed_exchange_completed", **row)
                transcript.flush()
                now = time.monotonic_ns()
                sample = {"through_replacement": attempt, "elapsed_ms": (now - started) / 1e6,
                          "window_ms": (now - previous) / 1e6,
                          "business_terminal_observed": dict(result["business_terminal"]),
                          "storage": storage_sample(broker), "events": tail.read()}
                previous = now
                result["windows"].append(sample)
                write_json(case / "live-progress.json", {**result, "status": "RUNNING"})
                print(json.dumps({"kind": "live_window", "through": attempt,
                                  "window_ms": sample["window_ms"],
                                  "receipt_rows": sample["storage"]["counts"].get("operations"),
                                  "resident_bytes": sample["storage"]["resident_bytes"]}), flush=True)
                require(broker.process.poll() is None, "live_broker_exited")
                require(sample["storage"]["counts"].get("operations", 0) <= HISTORICAL_RECEIPT_LIMIT,
                        "receipt_budget_exceeded")
        result["client_packet_identifier_range"] = {"first": first_mid, "last": last_mid}
        result["client_connections"] = client.stats().connections
        require(result["client_connections"] == 1, "live_primary_reconnected")
        result["paho_connections"] = paho.connect_count
        require(paho.connect_count == 1, "live_second_implementation_reconnected")
        require(result["business_terminal"]["subscription_replacements"] == terminal_commands,
                "business_terminals_incomplete")
        require(len({window["storage"]["pid"] for window in result["windows"]}) == 1
                and len({window["storage"]["file_inode"] for window in result["windows"]}) == 1,
                "live_process_or_file_changed")
        if terminal_commands > 4 * HISTORICAL_RECEIPT_LIMIT:
            floors = [max((sql_u64(row["consumed_through"])
                           for row in window["storage"]["retirement"].get("receipt_origins", [])), default=0)
                      for window in result["windows"]]
            result["durable_frontier_windows"] = floors
            result["frontier_advances_observed"] = sum(right > left for left, right in zip([0] + floors, floors))
            require(result["frontier_advances_observed"] >= 4, "multiple_collection_cycles_not_observed")
            require(all(sql_u64(window["storage"]["metadata"]["limit_receipt_slots"]) == HISTORICAL_RECEIPT_LIMIT
                        for window in result["windows"]), "receipt_budget_changed_during_live_run")
            require({row["incoming_qos"] for row in result["mixed_exchanges"]} == {1, 2}
                    and {row["mqttium_outgoing"]["qos"] for row in result["mixed_exchanges"]} == {0, 1},
                    "live_mixed_qos_paths_missing")
        result["elapsed_ms"] = (time.monotonic_ns() - started) / 1e6
        result["transcript"] = transcript.identity()
        return result
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=3)
        finally:
            try:
                if paho:
                    await asyncio.to_thread(paho.close)
            finally:
                transcript.close()


def live_receipts(binary: Path, case: Path, terminal_commands: int = BUSINESS_COMMANDS) -> dict:
    require(4 * HISTORICAL_RECEIPT_LIMIT < terminal_commands <= 40000, "insufficient_business_budget")
    broker = Broker(binary, case)
    restarted = None
    held = HeldBusiness(broker, case)
    try:
        ready = broker.ready()
        require(ready["details"]["receipt_slots"] == HISTORICAL_RECEIPT_LIMIT, "receipt_budget_changed")
        result = {"criterion": "B-live", "status": "PASS", "ready": ready,
                  "normal_build_required": True, "qualification": "system-test-only"}
        startup = broker.events()
        result["startup_observations"] = [event for event in startup
                                           if event["kind"] in ("sqlite_configuration", "receipt_policy", "ready")]
        configurations = [event for event in startup if event["kind"] == "sqlite_configuration"]
        require(len(configurations) == 1, "effective_sqlite_configuration_missing")
        result["sqlite_configuration"] = configurations[0]
        require(configurations[0]["details"]["kill_points"] is False,
                "normal_measurement_contains_kill_instrumentation")
        result["held_before_cycles"] = held.prepare()
        async def bounded():
            async with asyncio.timeout(900):
                return await live_public_workload(broker, case, terminal_commands)
        result["workload"] = asyncio.run(bounded())
        result["held_after_cycles"] = held.after_cycles()
        result["final_storage_before_crash"] = storage_sample(broker)
        inode = broker.db.stat().st_ino
        result["stop_status"] = broker.stop(killed=True)
        held.close()
        writer_available(broker.db)
        restarted = Broker(binary, case / "restart", db=broker.db, label="restarted")
        result["restart_ready"] = restarted.ready()
        require(restarted.db.stat().st_ino == inode, "live_restart_replaced_database")
        result["held_reception"] = held.after_crash(restarted)
        result["final_storage"] = storage_sample(restarted)
        result["restart_stop_status"] = restarted.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        held.close()
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def sql_u64(value) -> int:
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{16}", value):
        return int(value, 16)
    return int(value)


def operation_floor(state: dict, operation: dict) -> int:
    """Read the compact origin guard, independently of the writer's reply."""
    matching = []
    prefix = operation_hex(operation)[:48]
    incarnation = sql_u64(state["tables"]["meta"][0]["incarnation"])
    for name, rows in state["tables"].items():
        if not any(part in name for part in ("frontier", "origin")):
            continue
        for row in rows:
            same = row.get("origin") == prefix or all(
                key in row and sql_u64(row[key]) == int(operation[key])
                for key in ("incarnation", "epoch", "owner"))
            same = same or (incarnation == int(operation["incarnation"]) and all(
                key in row and sql_u64(row[key]) == int(operation[key]) for key in ("epoch", "owner")))
            if same:
                for field in ("consumed_through", "through", "operation_through", "operation_floor", "op_floor"):
                    if field in row:
                        matching.append(sql_u64(row[field]))
                        break
                else:
                    raise ReceiptFailure("origin_frontier_format_unrecognized", name)
    require(len(matching) <= 1, "origin_frontier_duplicated")
    return matching[0] if matching else 0


def wait_control_or_pressure(broker, control, *, after: int, pressure_ready,
                             timeout: float = 12, clock=None):
    """Wait for a business response or its explicit causal pressure witness."""
    clock = clock or time.monotonic
    deadline = clock() + timeout
    negative = None
    while clock() < deadline:
        frame = control.poll(min(.05, deadline - clock()))
        if frame is not None:
            if frame.kind != 0x90 or all(code < 128 for code in decode_filter_ack(frame)[1]):
                return frame, None
            negative = frame
        for event in broker.events():
            if (event["kind"] == "receipt_window_pressure" and int(event["seq"]) > after
                    and pressure_ready(event)):
                broker.observations.record("event_retained", event=event)
                return negative, event
        require(broker.process.poll() is None, "broker_exited_before_control_response")
    raise ReceiptFailure("control_response_deadline", "no response or stable causal pressure within action budget")


def origin_pressure_state(path: Path, operation: dict) -> dict:
    """Bounded indexed facts, excluding canonical bodies and unrelated origins."""
    prefix = bytes.fromhex(operation_hex(operation))[:24]
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        incarnation = connection.execute("SELECT incarnation FROM meta").fetchone()[0]
        require(incarnation == int(operation["incarnation"]).to_bytes(8, "big"), "pressure_scope_mismatch")
        row = connection.execute(
            "SELECT consumed_through FROM receipt_origins WHERE epoch=? AND owner=?",
            (int(operation["epoch"]).to_bytes(8, "big"), int(operation["owner"]).to_bytes(8, "big"))).fetchone()
        floor = int.from_bytes(row[0], "big") if row else 0
        largest = []
        for table in ("operations", "fences"):
            value = connection.execute(f"SELECT MAX(id) FROM {table} WHERE id>=? AND id<=?",
                                       (prefix + bytes(8), prefix + b"\xff" * 8)).fetchone()[0]
            if value is not None:
                require(len(value) == 32, "pressure_operation_identity_invalid")
                largest.append(int.from_bytes(value[24:], "big"))
        unknown = connection.execute("SELECT 1 FROM operations WHERE id=?",
                                     (bytes.fromhex(operation_hex(operation)),)).fetchone() is not None
        maximum = max(largest, default=0)
        return {"durable_floor": floor, "maximum_retained_sequence": maximum,
                "retained_span": max(0, maximum - floor), "unknown_receipt_present": unknown}
    finally:
        connection.close()


def stable_receipt_pressure(event: dict, state: dict, unknown_sequence: int) -> bool:
    require(state["durable_floor"] < unknown_sequence, "closure_crossed_unknown")
    require(state["unknown_receipt_present"], "unknown_receipt_deleted")
    return (state["durable_floor"] == unknown_sequence - 1 and state["retained_span"] >= 768
            and int(event["details"].get("span", 0)) >= 768
            and int(event["details"].get("unconsumed_holes", 0)) >= 1)


def wait_committed_resolution(broker, operation: dict, *, after: int, timeout: float = 3):
    resolved = broker.wait_event("store_resolved", after=after, timeout=timeout)
    require(resolved["operation"] == operation and resolved["details"]["outcome"].lower() == "committed",
            "unknown_resolution_changed_operation")
    return resolved


def unknown_hole(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    held = HeldBusiness(broker, case)
    result = {"criterion": "C-pressure", "status": "PASS", "samples": [],
              "parameters": {"business_window": 768, "total_window": 1024,
                             "attempt_budget": 1025, "result_release_timeout_s": 15,
                             "control_response_timeout_s": 12}}
    try:
        result["ready"] = broker.ready()
        result["held_continuations"] = held.prepare()
        (broker.control / "hold-admission-result").touch(exist_ok=False)
        with PubsubWire(broker.address, broker.observations, "committed-unknown-source") as source, \
                PubsubWire(broker.address, broker.observations, "pressure-control") as control:
            source.connect("receipt-unknown-source", session_present=False, keep_alive=0)
            control.connect("receipt-pressure-control", session_present=False, keep_alive=0)
            source.send(publish_packet("reception/receipt-unknown", b"committed-result-held", 1, qos=1))
            hit = broker.wait_event("store_result_held")
            require(hit.get("command_kind") == "Admission" and hit["details"]["outcome"] == "Committed",
                    "wrong_result_held")
            operation = hit["operation"]
            key = operation_hex(operation)
            before = durable(broker.db)
            require(any(row["id"] == key and row["decision"] == 1 for row in before["tables"]["operations"]),
                    "unknown_hole_not_committed")
            require(source.poll(.02) is None, "unknown_admission_acknowledged")
            result["held_result"], result["unknown_operation"] = hit, operation
            result["known_committed_row"] = key
            write_json(case / "hole-before-pressure.json", before)
            positives = 0
            pressure = None
            pressure_measurements = []
            def pressure_ready(event):
                measured = origin_pressure_state(broker.db, operation)
                stable = stable_receipt_pressure(event, measured, int(operation["sequence"]))
                pressure_measurements.append({"event_sequence": event["seq"], "stable": stable, **measured})
                require(len(pressure_measurements) <= 1024, "pressure_measurement_budget")
                return stable
            for attempt in range(1, 1026):
                # An older transient pressure report must not stand in for
                # this request's response after an intervening prefix commit.
                latest = max(int(event["seq"]) for event in broker.events())
                control.send(subscribe_packet(1, [("reception/receipt-pressure", attempt % 2)]))
                frame, pressure = wait_control_or_pressure(
                    broker, control, after=latest, pressure_ready=pressure_ready)
                if pressure is not None:
                    require(pressure["details"]["code"] in
                            ("receipt_window_business_full", "receipt_window_total_full"),
                            "wrong_pressure_reason")
                    break
                require(frame.kind == 0x90 and decode_filter_ack(frame) == (1, (attempt % 2,)),
                        "unexpected_pressure_response")
                positives += 1
                if attempt % 32 == 0:
                    state = durable(broker.db)
                    floor = operation_floor(state, operation)
                    write_json(case / "hole-latest-check.json", state)
                    require(floor < int(operation["sequence"]), "closure_crossed_unknown",
                            f"durable floor={floor}, unknown operation sequence={operation['sequence']}")
                    require(any(row["id"] == key for row in state["tables"]["operations"]),
                            "unknown_receipt_deleted")
                    result["samples"].append({"through_positive": positives, "floor": floor,
                                              "storage": storage_sample(broker)})
            require(pressure is not None and positives <= 768, "planned_window_pressure_not_reached")
            result["positive_commands_while_hole_open"], result["pressure"] = positives, pressure
            result["pressure_measurements"] = pressure_measurements
            blocked = durable(broker.db)
            require(operation_floor(blocked, operation) < int(operation["sequence"]), "closure_crossed_unknown")
            require(any(row["id"] == key for row in blocked["tables"]["operations"]), "unknown_receipt_deleted")
            control_progress(control)
            # These established responsibilities need continuation capacity,
            # even while the next business command cannot enter the window.
            held.claim_subscriber.acknowledge(held.first_claim)
            held.source.send(pubrel_packet(77))
            require(held.source.wait(0x70, 77).ack() == (77, 0), "reserved_release_did_not_progress")
            until(lambda: durable(broker.db)["counts"]["outbound"] == 0,
                  "reserved_finish_did_not_progress")
            result["controls_and_terminations_progressed"] = True
            write_json(case / "hole-at-pressure.json", blocked)
            (broker.control / "hold-admission-result").unlink()
            broker.observations.record("held_result_release", operation=operation)
            require(source.wait(0x40, 1, timeout=15).ack() == (1, 0), "same_operation_resolution_did_not_ack")
            resolved = wait_committed_resolution(broker, operation, after=int(hit["seq"]))
            result["same_operation_resolution"] = resolved
        advanced = until(lambda: (state if operation_floor(state, operation) >= int(operation["sequence"]) else None)
                         if (state := durable(broker.db)) else None,
                         "frontier_did_not_reopen_after_resolution")
        result["frontier_after_resolution"] = operation_floor(advanced, operation)
        with PubsubWire(broker.address, broker.observations, "reopened-business") as reopened:
            reopened.connect("receipt-reopened-control", session_present=False, keep_alive=0)
            for identifier in range(1, 66):
                reopened.subscribe(identifier, [("reception/receipt-reopened", identifier % 2)])
            control_progress(reopened)
        with PubsubWire(broker.address, broker.observations, "old-offline-after-pressure") as offline:
            offline.connect("receipt-old-offline", session_present=True, keep_alive=0)
            message = offline.next_publication()
            require(message.payload == b"old-offline-obligation", "offline_after_pressure_wrong")
            offline.acknowledge(message)
        held.close()
        drained(broker, case / "final")
        result["final_storage"] = storage_sample(broker)
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        held.close()
        broker.cleanup()


def retired_replays(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    result = {"criterion": "E-replay", "status": "PASS", "probes": [],
              "parameters": {"aging_subscription_budget": 256}}
    try:
        result["ready"] = broker.ready()
        with PubsubWire(broker.address, broker.observations, "retired-qos0-recipient") as subscriber, \
                PubsubWire(broker.address, broker.observations, "retired-admission-source") as source, \
                PubsubWire(broker.address, broker.observations, "retired-recipe-aging") as aging:
            subscriber.connect("receipt-retired-recipient", session_present=False, keep_alive=0)
            subscriber.subscribe(1, [("reception/receipt-retired", 0)], expected=(0,))
            source.connect("receipt-retired-source", session_present=False, keep_alive=0)
            aging.connect("receipt-retired-aging", session_present=False, keep_alive=0)
            (broker.control / "capture-next-admission").touch(exist_ok=False)
            source.publish(1, "reception/receipt-retired", b"old-identities-fully-collected", qos=2)
            captured = broker.wait_event("receipt_replay_captured")
            publication = subscriber.next_publication()
            require(publication.qos == 0 and publication.packet_id is None
                    and publication.payload == b"old-identities-fully-collected",
                    "retired_replay_seed_not_observed")
            drained(broker, case / "seed-drained")
            operation = captured["operation"]
            old_id = operation_hex(operation)
            result["captured"], result["first_delivery"] = captured, publication.observation()
            for identifier in range(1, 257):
                aging.subscribe(identifier, [("reception/receipt-aging", identifier % 2)])
            retired = until(lambda: (state if operation_floor(state, operation) >= int(operation["sequence"])
                                     and not any(row["id"] == old_id for row in state["tables"]["operations"])
                                     else None) if (state := durable(broker.db)) else None,
                            "captured_operation_not_retired")
            require(all(retired["counts"][name] == 0
                        for name in ("publications", "payloads", "obligations", "inbound", "outbound")),
                    "old_identity_objects_still_live")
            write_json(case / "retired-before-replays.json", retired)
            result["retired_floor"] = operation_floor(retired, operation)
        # New connection generations are established before old callbacks are
        # injected. No subsequent business command can collide with the fresh
        # test-only identities issued by the writer replay probes.
        with PubsubWire(broker.address, broker.observations, "retired-recipient-new-generation") as resumed:
            resumed.connect("receipt-retired-recipient", session_present=True, keep_alive=0)
            (broker.control / "replay-last-consume-result").touch(exist_ok=False)
            result["old_consume_callback"] = broker.wait_event("consume_result_replayed")
            resumed.no_publication(.15, code="retired_consume_callback_reemitted")
            control_progress(resumed)
            last = int(result["old_consume_callback"]["seq"])
            for mode, expected, failure in (
                ("same", "RetiredOutcomeUnavailable", "retired_operation_reexecuted"),
                ("different", "RetiredOutcomeUnavailable", "retired_operation_reexecuted"),
                ("publication", "RejectedCertain", "retired_publication_reused"),
                ("payload", "RejectedCertain", "retired_payload_reused"),
            ):
                (broker.control / f"replay-retired-{mode}").touch(exist_ok=False)
                observed = broker.wait_event("receipt_replay_result", after=last)
                require(observed["details"]["mode"] == mode, "wrong_replay_probe")
                after = durable(broker.db)
                write_json(case / f"replay-{mode}-state.json", after)
                require(observed["details"]["outcome"] == expected, failure,
                        str(observed["details"]["outcome"]))
                require(all(after["counts"][name] == 0
                            for name in ("publications", "payloads", "obligations", "inbound", "outbound")),
                        failure, "a replay recreated a live business object")
                accounts_consistent(after)
                storage_sample(broker)
                resumed.no_publication(.05, code=failure)
                result["probes"].append(observed)
                last = int(observed["seq"])
        result["final_storage"] = storage_sample(broker)
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        broker.cleanup()


def close_crash(binary: Path, case: Path, *, committed: bool, limit_one: bool = False) -> dict:
    point = "receipt.close.committed" if committed else "receipt.close.prepared"
    broker = Broker(binary, case, pauses=(point,))
    restarted = None
    held = HeldBusiness(broker, case)
    result = {"criterion": "F-credit" if limit_one else "F-after" if committed else "F-before", "status": "PASS",
              "point": point, "parameters": {"trigger_attempt_budget": 128}}
    try:
        result["ready"] = broker.ready()
        result["held_business"] = held.prepare()
        if limit_one:
            (broker.control / "receipt-collection-limit-one").touch(exist_ok=False)
        with PubsubWire(broker.address, broker.observations, "closure-crash-control") as control:
            control.connect("receipt-close-control", session_present=False, keep_alive=0)
            hit = None
            positives = 0
            for identifier in range(1, 129):
                control.send(subscribe_packet(identifier, [("reception/receipt-close", identifier % 2)]))
                frame = control.poll(.3)
                if frame is not None:
                    require(frame.kind == 0x90 and decode_filter_ack(frame) == (identifier, (identifier % 2,)),
                            "close_trigger_business_response")
                    positives += 1
                candidates = [event for event in broker.events()
                              if event["kind"] == "barrier_hit" and event.get("point") == point]
                if candidates:
                    hit = candidates[-1]
                    break
                require(frame is not None, "closure_trigger_stalled_without_point")
            require(hit is not None and hit.get("command_kind") == "ReceiptCollection", "closure_point_not_reached")
            details = hit["details"]
            if limit_one:
                require(details["limit"] == 1, "single_row_pass_not_reached")
            operation = {key: details[key] for key in ("incarnation", "epoch", "owner")}
            operation["sequence"] = details["through"]
            before = durable(broker.db)
            write_json(case / "at-close-barrier.json", before)
            floor = operation_floor(before, operation)
            if committed:
                deleted_seed_receipts = held.seed_receipts - {row["id"] for row in before["tables"]["operations"]}
                if floor < int(details["through"]):
                    require(deleted_seed_receipts, "closure_commit_without_visible_floor")
                    raise ReceiptFailure("receipt_deleted_without_closure",
                                         f"{len(deleted_seed_receipts)} known receipts disappeared with visible floor {floor}")
            else:
                require(floor < int(details["through"]), "uncommitted_closure_visible")
            result["hit"], result["positive_subacks_before_hit"], result["visible_floor_at_hit"] = hit, positives, floor
            result["storage_at_hit"] = storage_sample(broker)
            inode = broker.db.stat().st_ino
            result["kill_status"] = broker.stop(killed=True)
        held.close()
        writer_available(broker.db)
        restarted = Broker(binary, case / "restart", db=broker.db, label="restarted")
        result["restart_ready"] = restarted.ready()
        require(restarted.db.stat().st_ino == inode, "closure_crash_replaced_database")
        recovered = durable(restarted.db)
        if committed:
            require(operation_floor(recovered, operation) >= int(details["through"])
                    or sql_u64(recovered["tables"]["meta"][0]["collected_through"]) >= int(operation["epoch"]),
                    "committed_closure_lost_on_restart")
        result["held_after_restart"] = held.after_crash(restarted)
        result["final_storage"] = storage_sample(restarted)
        result["restart_stop_status"] = restarted.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        held.close()
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def close_result_replay(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    result = {"criterion": "F-results", "status": "PASS", "passes": [],
              "parameters": {"business_budget_per_phase": 256}}
    try:
        result["ready"] = broker.ready()
        with PubsubWire(broker.address, broker.observations, "closure-result-controls") as control:
            control.connect("receipt-close-results", session_present=False, keep_alive=0)
            last = -1
            for file, event_name in (("drop-next-close-result", "receipt_collection_result_dropped"),
                                     ("duplicate-close-result", "receipt_collection_result_duplicated")):
                (broker.control / file).touch(exist_ok=False)
                for identifier in range(1, 257):
                    control.subscribe(identifier, [("reception/receipt-close-results", identifier % 2)])
                observed = broker.wait_event(event_name, after=last, timeout=15)
                control_progress(control)
                result["passes"].append({"control": file, "injection": observed,
                                          "storage": storage_sample(broker)})
                last = int(observed["seq"])
            result["final_storage"] = storage_sample(broker)
            require(result["final_storage"]["counts"]["operations"] <= HISTORICAL_RECEIPT_LIMIT,
                    "maintenance_created_cumulative_receipts")
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        broker.cleanup()


def table_counts(path: Path) -> dict:
    state = durable(path)
    return {"schema_version": state["schema_version"], "epoch": state["epoch"],
            "revision": state["revision"], "counts": state["counts"]}


def replace_one(wire: PubsubWire, attempt: int, timeout: float) -> dict:
    """MID 1 is reused only after the preceding request's terminal SUBACK."""
    wire.send(subscribe_packet(1, [("reception/receipt-live", 1)]))
    frame = wire.poll(timeout)
    if frame is None:
        return {"attempt": attempt, "result": "no_frame", "observation_s": timeout}
    if frame.kind == 0x90:
        mid, codes = decode_filter_ack(frame)
        return {"attempt": attempt, "result": "suback", "identifier": mid, "codes": list(codes)}
    return {"attempt": attempt, "result": "other_frame", "header": frame.header,
            "body": frame.body[:128].hex()}


def control_progress(wire: PubsubWire) -> None:
    wire.send(b"\xc0\x00")
    wire.wait(0xD0, timeout=2)


def bounded_empty_business(snapshot: dict) -> bool:
    return snapshot["counts"]["routes"] == 1 and all(snapshot["counts"][name] == 0
             for name in ("publications", "payloads", "obligations", "outbound", "inbound"))


def historical_ceiling(binary: Path, case: Path, response_timeout: float = 12) -> dict:
    """A regression that fails semantically on the former cumulative limit."""
    broker = Broker(binary, case / "first")
    restarted = None
    result = {"criterion": "A-ceiling", "historical_receipt_limit": HISTORICAL_RECEIPT_LIMIT,
              "sql_mutations_by_probe": False, "constant_live_filter": True,
              "packet_identifier": 1, "positive_subacks": 0, "samples": [],
              "parameters": {"attempt_budget": HISTORICAL_RECEIPT_LIMIT + 1,
                             "keep_alive": 0, "response_timeout_s": response_timeout,
                             "first_phase_deadline_s": 300}}
    started = time.monotonic_ns()
    try:
        ready = broker.ready()
        result["effective_limits"] = ready["details"]
        require(ready["details"]["receipt_slots"] == HISTORICAL_RECEIPT_LIMIT,
                "historical_budget_changed")
        with PubsubWire(broker.address, broker.observations, "constant-live-filter") as wire:
            wire.connect("receipt-ceiling-client", keep_alive=0, session_present=False)
            result["initial"] = table_counts(broker.db)
            answer = None
            for attempt in range(1, HISTORICAL_RECEIPT_LIMIT + 2):
                require((time.monotonic_ns() - started) / 1e9 < 300, "ceiling_probe_deadline")
                answer = replace_one(wire, attempt, response_timeout)
                if answer.get("codes") != [1]:
                    break
                require(answer["identifier"] == 1, "subscription_identifier_changed")
                result["positive_subacks"] += 1
                if attempt % 1024 == 0:
                    sample = {"attempt": attempt, "elapsed_ms": (time.monotonic_ns() - started) / 1e6,
                              "snapshot": table_counts(broker.db)}
                    result["samples"].append(sample)
                    print(json.dumps({"kind": "ceiling_sample", **sample}), flush=True)
            result["first_nonpositive"] = answer if answer.get("codes") != [1] else None
            result["before_restart"] = table_counts(broker.db)
            if result["first_nonpositive"] is not None:
                control_progress(wire)
                result["ping_progress_at_pressure"] = True
                events = broker.events()
                replies = [event for event in events if event["kind"] == "store_result"]
                result["last_store_result"] = replies[-1] if replies else None
                full = result["before_restart"]["counts"]["operations"] == HISTORICAL_RECEIPT_LIMIT
                exact = (result["positive_subacks"] == HISTORICAL_RECEIPT_LIMIT
                         and answer["result"] == "no_frame" and full
                         and bounded_empty_business(result["before_restart"])
                         and replies and replies[-1]["details"]["outcome"] == "PendingOrUnknown")
                require(exact, "unexpected_business_response_failure", json.dumps(answer))
        inode = broker.db.stat().st_ino
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        restarted = Broker(binary, case / "restart", db=broker.db, label="restarted")
        restarted.ready()
        require(restarted.db.stat().st_ino == inode, "ceiling_restart_replaced_database")
        with PubsubWire(restarted.address, restarted.observations, "same-filter-after-restart") as wire:
            result["session_present_after_restart"] = wire.connect("receipt-ceiling-client", keep_alive=0,
                                                                    session_present=True)
            result["after_restart"] = replace_one(wire, 1, response_timeout)
            if result["first_nonpositive"]:
                control_progress(wire)
                require(result["after_restart"]["result"] == "no_frame"
                        and table_counts(restarted.db)["counts"]["operations"] == HISTORICAL_RECEIPT_LIMIT,
                        "historical_ceiling_not_preserved_on_restart")
            else:
                require(result["after_restart"].get("codes") == [1], "receipt_progress_lost_on_restart")
            result["restart_snapshot"] = table_counts(restarted.db)
        result["restart_stop_status"] = restarted.stop()
        writer_available(broker.db)
        result["elapsed_ms"] = (time.monotonic_ns() - started) / 1e6
        result["same_file_inode"] = inode
        if result["first_nonpositive"]:
            result["status"] = "FAIL"
            result["failure"] = {"code": "receipt_capacity_ceiling",
                                 "detail": "8192 terminal replacements exhausted receipts while one live route and no publications remained; PING and same-file restart corroborated the ceiling"}
        else:
            result["status"] = "PASS"
        write_json(case / "observation.json", result)
        return result
    finally:
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--binary-source-head")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--case", choices=("A-ceiling", "B-live", "C-pressure", "E-replay",
                                          "F-before", "F-after", "F-credit", "F-results"), default="A-ceiling")
    parser.add_argument("--build-mode", choices=("normal", "test-hooks"), default="normal")
    parser.add_argument("--business-commands", type=int, default=BUSINESS_COMMANDS)
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing")
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    if args.expected_binary_sha256:
        require(digest == args.expected_binary_sha256, "wrong_baseline_binary")
    harness = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    result = {"format": "mqttiumd.receipt-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "source_head": args.binary_source_head or harness,
              "harness_source_head": harness, "binary": {"path": str(args.binary.resolve()), "sha256": digest},
              "command": [sys.executable, *sys.argv],
              "build_mode": args.build_mode,
              "cases": []}
    try:
        if args.case == "B-live":
            require(args.build_mode == "normal", "normal_build_required_for_live_measurements")
            observed = live_receipts(args.binary.resolve(), args.out / args.case, args.business_commands)
        elif args.case == "C-pressure":
            require(args.build_mode == "test-hooks", "instrumented_build_required")
            observed = unknown_hole(args.binary.resolve(), args.out / args.case)
        elif args.case == "E-replay":
            require(args.build_mode == "test-hooks", "instrumented_build_required")
            observed = retired_replays(args.binary.resolve(), args.out / args.case)
        elif args.case in ("F-before", "F-after", "F-credit"):
            require(args.build_mode == "test-hooks", "instrumented_build_required")
            observed = close_crash(args.binary.resolve(), args.out / args.case,
                                   committed=args.case != "F-before", limit_one=args.case == "F-credit")
        elif args.case == "F-results":
            require(args.build_mode == "test-hooks", "instrumented_build_required")
            observed = close_result_replay(args.binary.resolve(), args.out / args.case)
        else:
            observed = historical_ceiling(args.binary.resolve(), args.out / args.case)
        result["cases"].append(observed)
        result["status"] = observed["status"]
        if observed.get("failure"):
            result["failure"] = observed["failure"]
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"),
                      "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
