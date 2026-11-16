#!/usr/bin/env python3
"""Finite system reception of protected outgoing QoS 1 identifier reuse.

Public clients establish application reception; a packet-aware TCP observer
records actual wire identifiers; correlated product facts and independent SQL
snapshots establish durable completion. This is not fixture qualification.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import threading
import time

from external_mqttium import client_identity
from pubsub_probe import decode_publish, durable
from pubsub_reception import accounts_consistent, drained
from pubsub_reception import Client, publisher
from receipt_reception import PahoPeer, storage_sample
from tcp_probe import Frame, FrameDecoder, ReceiptFailure, packet, require, write_json
from tcp_reception import Broker, writer_available


TARGET_DELIVERIES = 65537
MAX_DELIVERIES = 70000
MAX_PACKET = 65536
MAX_LIVE = 128
MAX_LEDGER_BYTES = 128 * 1024 * 1024
ACTION_TIMEOUT = 12
RUN_TIMEOUT = 3600


class Ledger:
    """A bounded streamed transcript, with no historical message map in RAM."""
    def __init__(self, path: Path, maximum: int = MAX_LEDGER_BYTES):
        self.path, self.maximum = path, maximum
        self.stream = path.open("xb")
        self.digest = hashlib.sha256()
        self.records = self.bytes = 0

    def record(self, kind: str, **fields):
        record = {"kind": kind, "sequence": self.records,
                  "monotonic_ns": str(time.monotonic_ns()), **fields}
        raw = (json.dumps(record, sort_keys=True) + "\n").encode()
        require(len(raw) <= 4096 and self.bytes + len(raw) <= self.maximum, "pi_ledger_budget")
        self.stream.write(raw)
        self.digest.update(raw)
        self.records += 1
        self.bytes += len(raw)

    def flush(self):
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self):
        if not self.stream.closed:
            self.flush()
            self.stream.close()

    def identity(self):
        return {"path": str(self.path), "records": self.records, "bytes": self.bytes,
                "sha256": self.digest.hexdigest()}


class WireOccurrences:
    """Independent wire facts for one serial, numbered normal stream.

    This map describes currently unacknowledged *wire* exchanges only. It does
    not assign the broker's causal claim identity or authorize its release.
    """
    def __init__(self, prefix: bytes, limit: int, ledger):
        self.prefix, self.limit, self.ledger = prefix, limit, ledger
        self.active = {}
        self.pending = {}
        self.publishes = self.acks = self.reuses = 0
        self.seen_identifiers = bytearray(8192)
        self.unique_identifiers = 0

    def publish(self, frame: Frame):
        publication = decode_publish(frame)
        require(publication.qos == 1 and publication.packet_id is not None,
                "normal_outgoing_not_qos1")
        require(not publication.dup, "new_claim_has_dup")
        raw_number = publication.payload[len(self.prefix):]
        require(publication.payload.startswith(self.prefix) and len(raw_number) == 8
                and raw_number.isdigit(), "numbered_payload_invalid")
        number = int(raw_number)
        require(number == self.publishes + 1 and number <= self.limit,
                "recipient_sequence_repeated_or_missing", str(number))
        identifier = publication.packet_id
        require(identifier not in self.active, "wire_identifier_reused_while_unacknowledged")
        require(len(self.active) < MAX_LIVE and len(self.pending) < MAX_LIVE, "wire_observer_live_budget")
        byte, bit = divmod(identifier, 8)
        if self.seen_identifiers[byte] & (1 << bit):
            self.reuses += 1
        else:
            self.seen_identifiers[byte] |= 1 << bit
            self.unique_identifiers += 1
        observed = {"number": number, "packet_id": identifier, "dup": publication.dup,
                    "topic": publication.topic, "payload_sha256": hashlib.sha256(publication.payload).hexdigest(),
                    "publish_hex": packet(frame.header, frame.body).hex(),
                    "puback_seen": False, "puback_forwarded": False}
        self.active[identifier] = number
        self.pending[number] = observed
        self.publishes += 1
        self.ledger.record("wire_publish", **observed)

    def acknowledge(self, frame: Frame):
        identifier, reason = frame.ack()
        require(reason == 0 and identifier in self.active, "normal_puback_target_invalid")
        number = self.active.pop(identifier)
        observed = self.pending[number]
        observed["puback_seen"] = True
        observed["puback_hex"] = packet(frame.header, frame.body).hex()
        self.acks += 1
        self.ledger.record("wire_puback_observed", number=number, packet_id=identifier,
                           reason=reason, bytes=observed["puback_hex"])
        return number

    def forwarded(self, number: int):
        self.pending[number]["puback_forwarded"] = True

    def take(self, number: int):
        observed = self.pending.get(number)
        if observed and observed["puback_forwarded"]:
            return self.pending.pop(number)
        return None


class PacketProxy:
    """One bounded, packet-aware, byte-preserving TCP forwarding thread."""
    def __init__(self, upstream: tuple[str, int], case: Path, *, prefix=b"pi-main:", limit=TARGET_DELIVERIES):
        self.upstream = upstream
        self.ledger = Ledger(case / "wire.jsonl")
        self.protocol = WireOccurrences(prefix, limit, self.ledger)
        self.lock = threading.Condition()
        self.stop_event = threading.Event()
        self.error = None
        self.connections = 0
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.setblocking(False)
        self.address = self.listener.getsockname()
        self.thread = threading.Thread(target=self._run, name="pi-packet-observer", daemon=True)
        self.thread.start()

    def _run(self):
        peers = []
        try:
            while not self.stop_event.is_set():
                if select.select([self.listener], [], [], .05)[0]:
                    downstream, _ = self.listener.accept()
                    upstream = socket.create_connection(self.upstream, timeout=3)
                    peers = [downstream, upstream]
                    self.connections += 1
                    break
            if not peers:
                return
            for peer in peers:
                peer.setblocking(False)
            decoders = [FrameDecoder(MAX_PACKET), FrameDecoder(MAX_PACKET)]
            buffers = [bytearray(), bytearray()]
            enqueued = [0, 0]
            written = [0, 0]
            ack_marks = deque()
            ended = [False, False]
            shutdown = [False, False]
            while not self.stop_event.is_set() and not all(ended):
                readers = [peers[index] for index in range(2) if not ended[index] and not buffers[index]]
                writers = [peers[1 - index] for index in range(2) if buffers[index]]
                readable, writable, _ = select.select(readers, writers, [], .05)
                for index in range(2):
                    if peers[index] in readable:
                        data = peers[index].recv(4096)
                        if not data:
                            decoders[index].finish()
                            ended[index] = True
                        else:
                            for frame in decoders[index].feed(data):
                                raw = packet(frame.header, frame.body)
                                with self.lock:
                                    if index == 1 and frame.kind == 0x30:
                                        self.protocol.publish(frame)
                                    if index == 0 and frame.kind == 0x40:
                                        number = self.protocol.acknowledge(frame)
                                        ack_marks.append((enqueued[index] + len(raw), number))
                                    buffers[index].extend(raw)
                                    enqueued[index] += len(raw)
                                    require(len(buffers[index]) <= 2 * (MAX_PACKET + 5), "proxy_buffer_budget")
                                    self.lock.notify_all()
                    if peers[1 - index] in writable and buffers[index]:
                        sent = peers[1 - index].send(buffers[index])
                        require(sent > 0, "proxy_write_no_progress")
                        del buffers[index][:sent]
                        written[index] += sent
                        if index == 0:
                            with self.lock:
                                while ack_marks and ack_marks[0][0] <= written[index]:
                                    _, number = ack_marks.popleft()
                                    self.protocol.forwarded(number)
                                self.lock.notify_all()
                    if ended[index] and not buffers[index] and not shutdown[index]:
                        peers[1 - index].shutdown(socket.SHUT_WR)
                        shutdown[index] = True
            self.ledger.record("proxy_end", connections=self.connections,
                               publishes=self.protocol.publishes, acknowledgements=self.protocol.acks)
        except Exception as error:
            with self.lock:
                self.error = error
                self.lock.notify_all()
        finally:
            for peer in peers:
                peer.close()
            self.listener.close()
            self.ledger.close()

    def take(self, number: int, timeout=ACTION_TIMEOUT):
        deadline = time.monotonic() + timeout
        with self.lock:
            while time.monotonic() < deadline:
                if self.error:
                    raise self.error
                observed = self.protocol.take(number)
                if observed:
                    return observed
                self.lock.wait(min(.05, max(0, deadline - time.monotonic())))
        raise ReceiptFailure("wire_puback_not_forwarded", str(number))

    def close(self):
        self.stop_event.set()
        self.thread.join(timeout=3)
        require(not self.thread.is_alive(), "proxy_thread_survived")
        if self.error:
            raise self.error

    def summary(self):
        return {"connections": self.connections, "publish_frames": self.protocol.publishes,
                "puback_frames": self.protocol.acks, "identifier_reuses": self.protocol.reuses,
                "unique_identifiers": self.protocol.unique_identifiers,
                "live_wire_exchanges": len(self.protocol.active), "pending_observations": len(self.protocol.pending),
                "ledger": self.ledger.identity(), "observer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "packet_limit": MAX_PACKET, "direction_buffer_limit": 2 * (MAX_PACKET + 5)}


class FinishCursor:
    """Read all product records once; retain only live claim/Finish pairs."""
    def __init__(self, broker: Broker):
        self.broker = broker
        self.offsets = {}
        self.last_sequence = 0
        self.counts = Counter()
        self.active = {}
        self.by_identifier = {}
        self.order = deque()
        self.finishes = 0
        self.last_claim = self.last_publication = 0
        self.last_finish = self.last_finished_publication = 0
        self.recent_finishes = deque()
        self.recent_finish_set = set()
        self.duplicate_finishes = 0
        self.origin = None
        self.connection = None

    def read(self):
        records = []
        live_inodes = set()
        for path in sorted(self.broker.events_path.parent.glob(self.broker.events_path.name + "*")):
            if not path.is_file():
                continue
            with path.open("rb") as stream:
                inode = os.fstat(stream.fileno()).st_ino
                live_inodes.add(inode)
                offset = self.offsets.get(inode, 0)
                stream.seek(offset)
                while True:
                    raw = stream.readline(2049)
                    if not raw or not raw.endswith(b"\n"):
                        break
                    require(len(raw) <= 2048, "product_event_record_budget")
                    records.append(json.loads(raw))
                    require(len(records) <= 16384, "product_event_window_budget")
                    offset = stream.tell()
                self.offsets[inode] = offset
        self.offsets = {inode: value for inode, value in self.offsets.items() if inode in live_inodes}
        require(len(self.offsets) <= 4, "product_log_rotation_budget")
        for event in sorted(records, key=lambda row: int(row["seq"])):
            sequence = int(event["seq"])
            if sequence <= self.last_sequence:
                continue
            require(sequence == self.last_sequence + 1, "product_event_observation_gap")
            require(event["pid"] == self.broker.process.pid, "wrong_product_event_pid")
            self.last_sequence = sequence
            self.counts[event["kind"]] += 1
            details = event.get("details", {})
            if event["kind"] == "frame_queued" and details.get("packet_kind") == "PUBLISH":
                identifier = details["packet_id"]
                occupied = self.by_identifier.get(identifier)
                require(occupied is None or occupied["finish"] is not None,
                        "pi_reused_before_finish_known")
                require(not details["dup"] and details["delivery"] not in self.active,
                        "normal_delivery_replayed")
                claim = tuple(map(int, details["claim"].split("/")))
                publication = tuple(map(int, details["delivery"].split("/")))
                require(len(claim) == 4 and len(publication) == 5, "product_identity_encoding")
                self.origin = self.origin or claim[:3]
                self.connection = self.connection or event["connection"]
                require(claim[:3] == self.origin and publication[:3] == self.origin
                        and claim[3] > self.last_claim and publication[3] > self.last_publication,
                        "new_claim_identity_reused")
                require(event["connection"] == self.connection, "subscriber_connection_changed")
                self.last_claim, self.last_publication = claim[3], publication[3]
                entry = {"delivery": details["delivery"], "claim": details["claim"],
                         "packet_id": identifier, "dup": details["dup"], "effect": details["effect"],
                         "queued_sequence": event["seq"], "connection": event["connection"], "finish": None}
                require(len(self.active) < MAX_LIVE, "claim_observer_live_budget")
                self.active[entry["delivery"]] = entry
                self.by_identifier[identifier] = entry
                self.order.append(entry)
            if event["kind"] == "core_finish_observed":
                entry = self.active.get(details["delivery"])
                operation = tuple(int(event["operation"][key]) for key in
                                  ("incarnation", "epoch", "owner", "sequence"))
                publication = tuple(map(int, details["delivery"].split("/")))
                pair = (operation, details["delivery"])
                if pair in self.recent_finish_set or operation[3] <= self.last_finish:
                    require(operation[:3] == self.origin and publication[:3] == self.origin
                            and publication[3] <= self.last_finished_publication,
                            "old_finish_rebound_to_new_delivery")
                    self.duplicate_finishes += 1
                    continue
                require(entry is not None and entry["finish"] is None, "unexpected_finish_identity")
                entry["finish"] = {"operation": event["operation"], "event_sequence": event["seq"]}
                self.finishes += 1
                self.last_finish, self.last_finished_publication = operation[3], publication[3]
                self.recent_finishes.append(pair)
                self.recent_finish_set.add(pair)
                if len(self.recent_finishes) > MAX_LIVE:
                    self.recent_finish_set.remove(self.recent_finishes.popleft())

    def take(self, identifier: int, timeout=ACTION_TIMEOUT):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.read()
            if self.order and self.order[0]["finish"] is not None:
                entry = self.order.popleft()
                require(entry["packet_id"] == identifier, "wire_and_claim_identifier_differ")
                del self.active[entry["delivery"]]
                if self.by_identifier.get(identifier) is entry:
                    del self.by_identifier[identifier]
                return entry
            require(self.broker.process.poll() is None, "broker_exited_before_finish")
            time.sleep(.001)
        raise ReceiptFailure("durable_finish_not_observed")


async def numbered_deliveries(broker: Broker, case: Path, count: int):
    from mqttium import MQTTProtocolVersion
    from mqttium.api import AsyncClient, Properties
    require(1 <= count <= MAX_DELIVERIES, "delivery_budget_invalid")
    topic, prefix = "reception/pi-main", b"pi-main:"
    proxy = PacketProxy(broker.address, case, prefix=prefix, limit=count)
    peer = None
    client = AsyncClient("pi-main-mqttium", protocol=MQTTProtocolVersion.MQTTv5,
                         clean_start=False, keepalive=10,
                         connect_properties=Properties({"session_expiry_interval": 0xFFFFFFFF}),
                         maximum_packet_size=65536, max_outbound_inflight=8,
                         max_write_queue_messages=16, max_write_queue_bytes=65536)
    ledger = Ledger(case / "deliveries.jsonl")
    cursor = FinishCursor(broker)
    result = {"mqttium_identity": client_identity(), "parameters": {"deliveries": count,
              "action_timeout_s": ACTION_TIMEOUT, "run_timeout_s": RUN_TIMEOUT,
              "max_outbound_inflight": 8, "sql_cleanup": False}, "windows": []}
    started = previous = time.monotonic_ns()
    try:
        peer = await asyncio.to_thread(PahoPeer, proxy.address, topic)
        result["paho_identity"] = peer.identity
        acknowledgement = await client.connect(*broker.address, timeout=3)
        require(not acknowledgement.session_present and acknowledgement.reason_code == 0,
                "publisher_connect_refused")
        for number in range(1, count + 1):
            payload = prefix + f"{number:08d}".encode()
            receipt = await client.publish(topic, payload, qos=1, retain=False, timeout=ACTION_TIMEOUT)
            await receipt.wait(timeout=ACTION_TIMEOUT)
            require(receipt.is_done() and receipt.exception() is None, "publisher_receipt_failed")
            received = await asyncio.to_thread(peer.receive, topic, payload)
            wire = await asyncio.to_thread(proxy.take, number)
            completed = await asyncio.to_thread(cursor.take, wire["packet_id"])
            require(received["packet_id"] == wire["packet_id"], "client_and_wire_identifier_differ")
            ledger.record("delivery_finished", number=number, wire=wire, claim=completed,
                          publisher_client_mid=receipt.mid, recipient=received)
            if number % 1024 == 0 or number == count:
                ledger.flush()
                now = time.monotonic_ns()
                sample = {"received_and_finished": number, "elapsed_ms": (now - started) / 1e6,
                          "window_ms": (now - previous) / 1e6, "storage": storage_sample(broker),
                          "finish_events": cursor.finishes, "product_event_counts": dict(cursor.counts),
                          "wire_identifier_reuses": proxy.protocol.reuses}
                previous = now
                require(cursor.finishes == number and proxy.protocol.publishes == number,
                        "delivery_finish_count_disagrees")
                result["windows"].append(sample)
                write_json(case / "progress.json", {**result, "status": "RUNNING"})
                print(json.dumps({"kind": "pi_window", "received_and_finished": number,
                                  "reuses": proxy.protocol.reuses, "window_ms": sample["window_ms"]}), flush=True)
        result["publisher_connections"] = client.stats().connections
        result["subscriber_connections"] = peer.connect_count
        require(result["publisher_connections"] == result["subscriber_connections"] == proxy.connections == 1,
                "normal_connection_replaced")
        require(cursor.finishes == count and not cursor.active, "unfinished_numbered_deliveries")
        if count >= TARGET_DELIVERIES:
            require(proxy.protocol.reuses >= count - 65535, "wire_identifier_reuse_not_observed")
        result["finished_deliveries"] = cursor.finishes
        result["elapsed_ms"] = (time.monotonic_ns() - started) / 1e6
        result["wire"] = proxy.summary()
        result["ledger"] = ledger.identity()
        result["event_counts"] = dict(cursor.counts)
        return result
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=3)
        finally:
            try:
                if peer:
                    await asyncio.to_thread(peer.close)
            finally:
                try:
                    proxy.close()
                finally:
                    ledger.close()
                    result["wire"] = proxy.summary()


def normal_long(binary: Path, case: Path, count=TARGET_DELIVERIES):
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        require(ready["details"]["receipt_slots"] == 8192, "receipt_quota_changed")
        result = {"criterion": "B" if count >= TARGET_DELIVERIES else "driver-smoke",
                  "status": "PASS", "ready": ready, "build": "normal",
                  "required_long_deliveries": TARGET_DELIVERIES,
                  "long_requirement_met": count >= TARGET_DELIVERIES}
        result["startup"] = [event for event in broker.events()
                             if event["kind"] in ("receipt_policy", "sqlite_configuration")]
        async def bounded():
            async with asyncio.timeout(RUN_TIMEOUT):
                return await numbered_deliveries(broker, case, count)
        result["deliveries"] = asyncio.run(bounded())
        final = drained(broker, case / "final")
        accounts_consistent(final)
        result["final_sql"] = storage_sample(broker)
        require(all(sample["storage"]["pid"] == broker.process.pid
                    and sample["storage"]["file_inode"] == broker.db.stat().st_ino
                    for sample in result["deliveries"]["windows"]), "normal_process_or_file_changed")
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        broker.cleanup()


def reverse_clients(binary: Path, case: Path):
    """A separate short run inverts the two public implementations."""
    broker = Broker(binary, case)
    subscriber = None
    try:
        ready = broker.ready()
        topic, payload = "reception/pi-reverse", b"pi-reverse:00000001"
        subscriber = Client(broker, case, "pi-reverse-mqttium", "mqttium", "receive",
                            filters=(topic,), count=1, qos=1, present=False)
        subscriber.wait_event("subscription_ready")
        published = publisher(broker, case, "pi-reverse-paho", "paho", topic, payload, qos=1)
        received = subscriber.collect()
        messages = received["result"]["messages"]
        require(len(messages) == 1 and messages[0]["payload_hex"] == payload.hex()
                and messages[0]["qos"] == 1, "reverse_public_delivery_wrong")
        final = drained(broker, case)
        result = {"criterion": "B-reverse", "status": "PASS", "ready": ready,
                  "publisher": published, "recipient": received, "final_sql": final,
                  "stop_status": broker.stop()}
        writer_available(broker.db)
        write_json(case / "observation.json", result)
        return result
    finally:
        if subscriber:
            subscriber.cleanup()
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--binary-source-head")
    parser.add_argument("--case", choices=("B", "reverse", "smoke"), default="B")
    parser.add_argument("--count", type=int)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    result = {"format": "mqttiumd.pi-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "source_head": args.binary_source_head or head,
              "harness_source_head": head, "command": [sys.executable, *sys.argv],
              "binary": {"path": str(args.binary.resolve()),
                         "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}, "cases": []}
    try:
        count = args.count if args.count is not None else 128 if args.case == "smoke" else TARGET_DELIVERIES
        if args.case == "reverse":
            case = reverse_clients(args.binary.resolve(), args.out / args.case)
        else:
            require((args.case == "B" and count >= TARGET_DELIVERIES)
                    or (args.case == "smoke" and count < TARGET_DELIVERIES), "wrong_delivery_scope")
            case = normal_long(args.binary.resolve(), args.out / args.case, count)
        result["cases"].append(case)
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"),
                      "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
