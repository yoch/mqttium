"""Independent, bounded MQTT 5 pilot and evidence checks for product tests.

This module imports neither the product codec nor the lab/oracle. Its small
wire grammar serves adversarial system probes, not a general MQTT client.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import sqlite3
import threading
import time
from urllib.parse import quote

MAX_RESPONSE = 4096
MAX_EVENTS = 16 * 1024 * 1024
MAX_RECORD = 2048
TABLES = ("meta", "sessions", "routes", "operations", "fences", "publications",
          "payloads", "obligations", "inbound")


class ReceiptFailure(AssertionError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail[:512]
        super().__init__(f"{code}: {self.detail}")


def require(value: object, code: str, detail: str = "") -> None:
    if not value:
        raise ReceiptFailure(code, detail)


def variable_integer(value: int) -> bytes:
    if not 0 <= value <= 268_435_455:
        raise ValueError("MQTT variable integer out of range")
    encoded = bytearray()
    while True:
        digit = value % 128
        value //= 128
        encoded.append(digit | (0x80 if value else 0))
        if not value:
            return bytes(encoded)


def utf8(value: str) -> bytes:
    raw = value.encode("utf-8", "strict")
    if not raw or len(raw) > 65535 or "\x00" in value:
        raise ValueError("invalid probe MQTT string")
    return len(raw).to_bytes(2, "big") + raw


def packet(header: int, body: bytes) -> bytes:
    return bytes([header]) + variable_integer(len(body)) + body


def connect_packet(client_id: str, keep_alive: int = 30, *, properties: bytes | None = None,
                   flags: int = 0) -> bytes:
    properties = b"\x11\xff\xff\xff\xff" if properties is None else properties
    body = b"\x00\x04MQTT\x05" + bytes([flags]) + keep_alive.to_bytes(2, "big")
    return packet(0x10, body + variable_integer(len(properties)) + properties + utf8(client_id))


def publish_packet(topic: str, payload: bytes, identifier: int, *, dup: bool = False) -> bytes:
    if not 1 <= identifier <= 65535:
        raise ValueError("zero or excessive Packet Identifier")
    return packet(0x3C if dup else 0x34, utf8(topic) + identifier.to_bytes(2, "big") + b"\x00" + payload)


def pubrel_packet(identifier: int) -> bytes:
    return packet(0x62, identifier.to_bytes(2, "big"))


@dataclass(frozen=True)
class Frame:
    header: int
    body: bytes

    @property
    def kind(self) -> int:
        return self.header & 0xF0

    def ack(self) -> tuple[int, int]:
        require(self.kind in (0x50, 0x70) and self.header & 15 == 0,
                "unexpected_ack", hex(self.header))
        require(len(self.body) >= 2, "probe_malformed_ack")
        identifier = int.from_bytes(self.body[:2], "big")
        require(identifier != 0, "probe_malformed_ack")
        return identifier, self.body[2] if len(self.body) >= 3 else 0


class FrameDecoder:
    def __init__(self, max_body: int = MAX_RESPONSE):
        self.max_body = max_body
        self.buffer = bytearray()

    def feed(self, data: bytes) -> list[Frame]:
        # Caller recv size is bounded; even a hostile response cannot request a
        # giant application allocation by declaring a large Remaining Length.
        require(len(data) <= self.max_body + 5, "probe_packet_too_large")
        self.buffer.extend(data)
        result = []
        while len(self.buffer) >= 2:
            remaining = 0
            multiplier = 1
            end = None
            for index in range(1, min(len(self.buffer), 5)):
                digit = self.buffer[index]
                remaining += (digit & 127) * multiplier
                if digit & 128 == 0:
                    end = index + 1
                    require(bytes(self.buffer[1:end]) == variable_integer(remaining),
                            "probe_malformed_remaining_length")
                    break
                require(index != 4, "probe_malformed_remaining_length")
                multiplier *= 128
            if end is None:
                break
            require(remaining <= self.max_body, "probe_packet_too_large")
            if len(self.buffer) < end + remaining:
                break
            result.append(Frame(self.buffer[0], bytes(self.buffer[end:end + remaining])))
            del self.buffer[:end + remaining]
        require(len(self.buffer) <= self.max_body + 5, "probe_packet_too_large")
        return result

    def finish(self) -> None:
        require(not self.buffer, "probe_truncated")


class Observations:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.sequence = 0
        self.bytes = 0
        self.lock = threading.Lock()

    def record(self, kind: str, **fields: object) -> None:
        with self.lock:
            self._record(kind, **fields)

    def record_bytes(self, kind: str, data: bytes, **fields: object) -> None:
        # Keep complete small frames; larger test writes/reads retain an exact
        # digest and length rather than exceeding the observer's own budget.
        if len(data) <= 2048:
            self.record(kind, bytes=data.hex(), **fields)
        else:
            self.record(kind, byte_count=len(data), sha256=hashlib.sha256(data).hexdigest(),
                        prefix=data[:64].hex(), **fields)

    def _record(self, kind: str, **fields: object) -> None:
        value = {"kind": kind, "seq": str(self.sequence), "monotonic_ns": str(time.monotonic_ns()), **fields}
        raw = (json.dumps(value, sort_keys=True) + "\n").encode()
        require(len(raw) <= 8192, "probe_record_budget")
        self.bytes += len(raw)
        require(self.bytes <= MAX_EVENTS, "probe_log_budget")
        with self.path.open("ab") as target:
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        self.sequence += 1


class Wire:
    def __init__(self, address: tuple[str, int], observations: Observations, label: str,
                 *, receive_buffer: int | None = None):
        # The pilot uses the product's explicit IPv4 loopback address. Apply
        # receive limits before SYN; reducing RCVBUF after connect leaves the
        # handshake's larger advertised receive window outside this control.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clamp = getattr(socket, "TCP_WINDOW_CLAMP", None)
        try:
            self.socket.settimeout(3)
            if receive_buffer is not None:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
                if clamp is not None:
                    self.socket.setsockopt(socket.IPPROTO_TCP, clamp, receive_buffer)
            before = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            self.socket.connect(address)
        except BaseException:
            self.socket.close()
            raise
        self.observations = observations
        self.label = label
        self.decoder = FrameDecoder()
        self.frames: list[Frame] = []
        self.closed = False
        observations.record("socket_open", label=label, address=list(address),
                            receive_buffer=self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
                            receive_buffer_requested=receive_buffer or 0,
                            receive_buffer_before_connect=before,
                            receive_window_clamp_requested=receive_buffer if receive_buffer and clamp is not None else 0,
                            receive_window_clamp=self.socket.getsockopt(socket.IPPROTO_TCP, clamp) if clamp is not None else 0)

    def send(self, data: bytes, *, stride: int | None = None, record: bool = True) -> None:
        if record:
            require(len(data) <= 131072, "probe_send_budget")
            self.observations.record_bytes("client_tx", data, label=self.label)
        try:
            if stride:
                for index in range(0, len(data), stride):
                    self.socket.sendall(data[index:index + stride])
            else:
                self.socket.sendall(data)
        except OSError as error:
            raise ReceiptFailure("probe_send_failed", type(error).__name__) from error

    def poll(self, timeout: float) -> Frame | None:
        deadline = time.monotonic() + timeout
        while True:
            if self.frames:
                return self.frames.pop(0)
            left = deadline - time.monotonic()
            if left <= 0:
                return None
            if not select.select([self.socket], [], [], left)[0]:
                return None
            try:
                data = self.socket.recv(MAX_RESPONSE)
            except OSError as error:
                raise ReceiptFailure("peer_closed", type(error).__name__) from error
            if not data:
                self.closed = True
                self.decoder.finish()
                raise ReceiptFailure("peer_closed")
            self.observations.record_bytes("client_rx", data, label=self.label)
            self.frames.extend(self.decoder.feed(data))

    def expect(self, kind: int, identifier: int | None = None, timeout: float = 5) -> Frame:
        frame = self.poll(timeout)
        require(frame is not None, "missing_frame", hex(kind))
        require(frame.kind == kind, "unexpected_frame", f"wanted {kind:02x}, got {frame.header:02x}")
        if identifier is not None:
            actual, reason = frame.ack()
            require(actual == identifier, "wrong_packet_identifier", f"{actual} != {identifier}")
            require(reason == 0, "negative_business_ack", str(reason))
        return frame

    def expect_batch(self, expected: list[tuple[int, int | None]], timeout: float = 5) -> None:
        # Responses to independent MQTT control packets may interleave while
        # PUBREL waits for its durable result. Require the exact multiset.
        pending = expected.copy()
        deadline = time.monotonic() + timeout
        while pending:
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, "missing_frame", repr(pending))
            identifier = None
            if frame.kind in (0x50, 0x70):
                identifier, reason = frame.ack()
                require(reason == 0, "negative_business_ack", str(reason))
            actual = (frame.kind, identifier)
            require(actual in pending, "unexpected_frame", repr(actual))
            pending.remove(actual)

    def connect(self, client_id: str, *, session_present: bool | None = None,
                keep_alive: int = 30, stride: int | None = None) -> bool:
        self.send(connect_packet(client_id, keep_alive), stride=stride)
        frame = self.expect(0x20)
        require(len(frame.body) >= 3 and frame.body[0] in (0, 1), "malformed_connack")
        require(frame.body[1] == 0, "negative_connack", str(frame.body[1]))
        present = frame.body[0] == 1
        if session_present is not None:
            require(present == session_present, "wrong_session_present", str(present))
        self.observations.record("connack", label=self.label, session_present=present)
        return present

    def exchange(self, identifier: int, payload: bytes = b"durable", *, topic: str = "reception/qos2",
                 stride: int | None = None, concat: bool = False) -> None:
        self.send(publish_packet(topic, payload, identifier), stride=stride)
        self.expect(0x50, identifier)
        self.send(pubrel_packet(identifier) + (b"\xc0\x00\xc0\x00" if concat else b""), stride=stride)
        self.expect_batch([(0x70, identifier)] + ([(0xD0, None)] * 2 if concat else []))

    def close(self) -> None:
        self.socket.close()
        self.closed = True

    def __enter__(self) -> Wire:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def assert_no_positive_ack(frames: list[Frame], header: int, identifier: int) -> None:
    for frame in frames:
        if frame.kind == header:
            actual, reason = frame.ack()
            if actual == identifier and reason < 128:
                raise ReceiptFailure("premature_pubrec" if header == 0x50 else "premature_pubcomp",
                                     f"positive acknowledgement for Packet Identifier {identifier} before commit")


def assert_stale_filtered(events: list[dict], generation: str) -> None:
    stale = [event for event in events if event.get("connection") == generation]
    require(not any(event["kind"] == "stale_generation_accepted" for event in stale), "stale_generation_forwarded")
    require(any(event["kind"] == "stale_generation_ignored" for event in stale), "missing_stale_rejection")


def read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    require(path.stat().st_size <= MAX_EVENTS, "product_event_budget")
    raw = path.read_bytes()
    # A writer may currently be between write and newline. Do not invent an
    # event from that prefix; the acknowledged barrier has a complete record.
    lines = raw.split(b"\n")[:-1]
    result = []
    for line in lines:
        require(len(line) <= MAX_RECORD, "product_record_budget")
        event = json.loads(line)
        require(event.get("format") == "mqttiumd.product-event" and event.get("schema_version") == 1,
                "product_event_format")
        require(isinstance(event.get("seq"), str) and event["seq"].isdecimal(), "product_event_sequence")
        seq = int(event["seq"])
        require(seq <= 2**64 - 1 and (not result or seq == int(result[-1]["seq"]) + 1),
                "product_event_sequence")
        require(isinstance(event.get("kind"), str) and len(event["kind"]) <= 48, "product_event_kind")
        require(isinstance(event.get("pid"), int) and event["pid"] > 0, "product_event_pid")
        details = event.get("details", {})
        require(isinstance(details, dict) and len(details) <= 32, "product_event_details")
        require(all(isinstance(value, (str, int, bool)) and (not isinstance(value, str) or len(value) <= 256)
                    for value in details.values()), "product_event_details")
        result.append(event)
    return result


def operation_hex(operation: dict) -> str:
    return b"".join(int(operation[key]).to_bytes(8, "big") for key in
                    ("incarnation", "epoch", "owner", "sequence")).hex()


def database(path: Path) -> dict:
    require(path.is_file(), "database_missing", str(path))
    with sqlite3.connect(f"file:{quote(str(path.resolve()), safe='/')}?mode=ro", uri=True, timeout=.25) as connection:
        connection.execute("BEGIN")
        integrity = connection.execute("PRAGMA quick_check").fetchmany(8)
        require(integrity == [("ok",)], "sqlite_integrity", repr(integrity))
        tables = {}
        for table in TABLES:
            count = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            require(count <= 16384, "inspection_row_budget", table)
            columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
            rows = []
            for values in connection.execute(f"SELECT * FROM {table}"):
                row = {}
                for column, value in zip(columns, values):
                    if isinstance(value, bytes):
                        require(len(value) <= 1024 * 1024, "inspection_field_budget", column)
                        row[column] = value.hex() if len(value) <= 1024 else {
                            "bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
                    else:
                        row[column] = value
                rows.append(row)
            tables[table] = sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))
        meta = tables["meta"][0]
        return {"quick_check": "ok", "tables": tables,
                "counts": {name: len(rows) for name, rows in tables.items()},
                "admissions": [row for row in tables["operations"] if row["decision"] == 1],
                "releases": [row for row in tables["operations"] if row["decision"] == 2],
                "epoch": int(meta["last_opened"], 16), "revision": int(meta["revision"], 16)}


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
