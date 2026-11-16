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
from pathlib import Path
import queue
import re
import sqlite3
import subprocess
import threading
import time

from external_mqttium import client_identity
from pubsub_probe import PubsubWire, decode_filter_ack, durable, subscribe_packet
from tcp_probe import ReceiptFailure, require, write_json
from tcp_reception import Broker, writer_available


HISTORICAL_RECEIPT_LIMIT = 8192
BUSINESS_COMMANDS = 32769
WINDOW_COMMANDS = 1024


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
        self.failures = []
        self.connect_observation = None
        self.suback = None

        def connected(_client, _userdata, flags, reason, _properties):
            self.connect_observation = {"reason": reason.value, "session_present": flags.session_present}
            self.connected.set()

        def subscribed(_client, _userdata, mid, reasons, _properties):
            self.suback = {"packet_id": mid, "reason_codes": [reason.value for reason in reasons]}
            self.subscribed.set()

        def message(_client, _userdata, publication):
            try:
                self.messages.put_nowait(publication)
            except queue.Full:
                self.failures.append("paho_inbound_budget")

        self.client.on_connect, self.client.on_subscribe, self.client.on_message = connected, subscribed, message
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 0xFFFFFFFF
        properties.ReceiveMaximum, properties.MaximumPacketSize = 8, 65536
        self.client.connect(*address, keepalive=10, clean_start=False, properties=properties)
        self.client.loop_start()
        require(self.connected.wait(3), "paho_connack_missing")
        require(self.connect_observation == {"reason": 0, "session_present": False}, "paho_connack_invalid")
        code, mid = self.client.subscribe([(topic, 1)])
        require(code == mqtt.MQTT_ERR_SUCCESS and self.subscribed.wait(12), "paho_suback_missing")
        require(self.suback == {"packet_id": mid, "reason_codes": [1]}, "paho_suback_invalid")

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
        require(not self.failures and message.topic == topic and bytes(message.payload) == payload
                and int(message.qos) == 1, "paho_delivery_wrong")
        require(self.client.ack(message.mid, message.qos) == 0, "paho_ack_submission")
        return {"packet_id": message.mid, "qos": 1, "payload_sha256": hashlib.sha256(payload).hexdigest()}

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


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
    tail = EventTail(broker.events_path, broker.process.pid)
    result = {"clients": {"mqttium": client_identity()}, "windows": [], "mixed_exchanges": [],
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
        paho = await asyncio.to_thread(PahoPeer, broker.address, topic)
        result["clients"]["paho"] = paho.identity
        result["paho_connack"], result["paho_suback"] = paho.connect_observation, paho.suback
        iterator = client.messages()
        first_mid = last_mid = None
        for attempt in range(1, terminal_commands + 1):
            granted = (attempt // WINDOW_COMMANDS) % 2
            acknowledgement = await client.subscribe([(topic, granted)], timeout=12)
            require(acknowledgement.reason_codes == (granted,), "live_subscription_refused")
            require(1 <= acknowledgement.mid <= 65535, "live_subscription_packet_identifier")
            first_mid = first_mid if first_mid is not None else acknowledgement.mid
            last_mid = acknowledgement.mid
            result["business_terminal"]["subscription_replacements"] += 1
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
        require(result["business_terminal"]["subscription_replacements"] == terminal_commands,
                "business_terminals_incomplete")
        require(len({window["storage"]["pid"] for window in result["windows"]}) == 1
                and len({window["storage"]["file_inode"] for window in result["windows"]}) == 1,
                "live_process_or_file_changed")
        result["elapsed_ms"] = (time.monotonic_ns() - started) / 1e6
        return result
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=3)
        finally:
            if paho:
                await asyncio.to_thread(paho.close)


def live_receipts(binary: Path, case: Path, terminal_commands: int = BUSINESS_COMMANDS) -> dict:
    require(4 * HISTORICAL_RECEIPT_LIMIT < terminal_commands <= 40000, "insufficient_business_budget")
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        require(ready["details"]["receipt_slots"] == HISTORICAL_RECEIPT_LIMIT, "receipt_budget_changed")
        result = {"criterion": "B-live", "status": "PASS", "ready": ready,
                  "normal_build_required": True, "qualification": "system-test-only"}
        async def bounded():
            async with asyncio.timeout(900):
                return await live_public_workload(broker, case, terminal_commands)
        result["workload"] = asyncio.run(bounded())
        deadline = time.monotonic() + 5
        while True:
            result["final_storage"] = storage_sample(broker)
            if all(result["final_storage"]["counts"][name] == 0
                   for name in ("publications", "payloads", "obligations", "outbound", "inbound")):
                break
            require(time.monotonic() < deadline, "live_objects_not_drained")
            time.sleep(.01)
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
                             "keep_alive": 0, "response_timeout_s": response_timeout}}
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
    parser.add_argument("--case", choices=("A-ceiling", "B-live"), default="A-ceiling")
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
              "build_mode": args.build_mode,
              "cases": []}
    try:
        if args.case == "B-live":
            require(args.build_mode == "normal", "normal_build_required_for_live_measurements")
            observed = live_receipts(args.binary.resolve(), args.out / args.case, args.business_commands)
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
