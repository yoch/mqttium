"""Independent bounded MQTT 5 grammar for the exact-filter pub/sub receipt.

This is an adversarial TCP pilot, not a product codec or a general MQTT client.
It deliberately withholds subscriber PUBACK until the caller explicitly sends
one. An application sleep is never used as evidence of a withheld ACK.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import quote

from tcp_probe import (
    Frame, FrameDecoder, Observations, ReceiptFailure, Wire, connect_packet,
    packet, publish_packet, pubrel_packet, require, utf8, variable_integer,
)

MAX_FILTERS = 8
MAX_PACKET = 65536
MAX_TOPIC = 1024
MAX_PAYLOAD = 32768
MAX_BUFFERED = 128


def _identifier(identifier: int) -> bytes:
    if not 1 <= identifier <= 65535:
        raise ValueError("Packet Identifier must be in 1..65535")
    return identifier.to_bytes(2, "big")


def subscribe_packet(identifier: int, filters: list[tuple[str, int]]) -> bytes:
    if not 1 <= len(filters) <= MAX_FILTERS:
        raise ValueError("bounded nonempty filter list required")
    body = _identifier(identifier) + b"\x00"
    for name, qos in filters:
        if qos not in (0, 1, 2) or len(name.encode()) > MAX_TOPIC:
            raise ValueError("invalid subscription request")
        body += utf8(name) + bytes([qos])
    return packet(0x82, body)


def unsubscribe_packet(identifier: int, filters: list[str]) -> bytes:
    if not 1 <= len(filters) <= MAX_FILTERS:
        raise ValueError("bounded nonempty filter list required")
    if any(len(name.encode()) > MAX_TOPIC for name in filters):
        raise ValueError("filter too large")
    return packet(0xA2, _identifier(identifier) + b"\x00" + b"".join(map(utf8, filters)))


def puback_packet(identifier: int, reason: int = 0) -> bytes:
    if reason not in (0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99):
        raise ValueError("reason is not valid for MQTT 5 PUBACK")
    return packet(0x40, _identifier(identifier) + (bytes([reason]) if reason else b""))


def _variable(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for shift in range(4):
        require(offset + shift < len(data), "probe_truncated_properties")
        digit = data[offset + shift]
        value |= (digit & 127) << (7 * shift)
        if digit & 128 == 0:
            end = offset + shift + 1
            require(data[offset:end] == variable_integer(value), "probe_noncanonical_properties")
            return value, end
    raise ReceiptFailure("probe_malformed_properties")


@dataclass(frozen=True)
class Publication:
    packet_id: int
    topic: str
    payload: bytes
    qos: int
    dup: bool
    retain: bool

    def observation(self) -> dict:
        return {"packet_id": self.packet_id, "topic": self.topic,
                "payload_prefix": self.payload[:128].hex(), "payload_bytes": len(self.payload),
                "payload_truncated": len(self.payload) > 128,
                "payload_sha256": hashlib.sha256(self.payload).hexdigest(),
                "qos": self.qos, "dup": self.dup, "retain": self.retain}


def decode_publish(frame: Frame) -> Publication:
    require(frame.kind == 0x30 and (frame.header >> 1) & 3 == 1,
            "unexpected_outgoing_qos", hex(frame.header))
    require(frame.header & 1 == 0, "unexpected_retained_delivery")
    body = frame.body
    require(len(body) >= 5, "probe_truncated_publish")
    length = int.from_bytes(body[:2], "big")
    require(0 < length <= MAX_TOPIC and 2 + length + 3 <= len(body), "probe_bad_topic_length")
    try:
        topic = body[2:2 + length].decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise ReceiptFailure("probe_invalid_topic_utf8") from error
    require(not any(c in topic for c in ("\x00", "+", "#")), "probe_invalid_topic")
    offset = 2 + length
    identifier = int.from_bytes(body[offset:offset + 2], "big")
    require(identifier != 0, "probe_zero_packet_identifier")
    size, offset = _variable(body, offset + 2)
    require(size == 0, "unexpected_outgoing_properties")
    payload = body[offset:]
    require(len(payload) <= MAX_PAYLOAD, "probe_payload_budget")
    return Publication(identifier, topic, payload, 1, bool(frame.header & 8), False)


def decode_filter_ack(frame: Frame) -> tuple[int, tuple[int, ...]]:
    require(frame.header in (0x90, 0xB0), "unexpected_filter_ack", hex(frame.header))
    require(len(frame.body) >= 4, "probe_truncated_filter_ack")
    identifier = int.from_bytes(frame.body[:2], "big")
    require(identifier != 0, "probe_zero_packet_identifier")
    size, offset = _variable(frame.body, 2)
    require(size == 0, "unexpected_ack_properties")
    reasons = tuple(frame.body[offset:])
    require(1 <= len(reasons) <= MAX_FILTERS, "probe_filter_ack_budget")
    allowed = ((0, 1, 2, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2)
               if frame.header == 0x90 else (0, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91))
    require(all(reason in allowed for reason in reasons), "invalid_filter_ack_reason")
    return identifier, reasons


class PubsubWire(Wire):
    """Raw subscriber/publisher with explicit ACK and bounded interleaving."""

    def __init__(self, address: tuple[str, int], observations: Observations, label: str,
                 *, receive_buffer: int | None = None):
        super().__init__(address, observations, label, receive_buffer=receive_buffer)
        self.decoder = FrameDecoder(MAX_PACKET)
        self.publications: list[Publication] = []
        self.controls: list[Frame] = []

    def connect(self, client_id: str, *, session_present: bool | None = None,
                receive_maximum: int = 8, maximum_packet_size: int = MAX_PACKET,
                keep_alive: int = 30) -> bool:
        require(1 <= receive_maximum <= 65535, "bad_test_receive_maximum")
        require(16 <= maximum_packet_size <= MAX_PACKET, "bad_test_maximum_packet")
        props = b"\x11\xff\xff\xff\xff\x21" + receive_maximum.to_bytes(2, "big")
        props += b"\x27" + maximum_packet_size.to_bytes(4, "big")
        self.send(connect_packet(client_id, keep_alive, properties=props))
        frame = self.wait(0x20)
        require(len(frame.body) >= 3 and frame.body[0] in (0, 1), "malformed_connack")
        require(frame.body[1] == 0, "negative_connack", str(frame.body[1]))
        present = bool(frame.body[0])
        if session_present is not None:
            require(present == session_present, "wrong_session_present")
        self.observations.record("connack", label=self.label, session_present=present,
                                 receive_maximum=receive_maximum, maximum_packet_size=maximum_packet_size)
        return present

    def wait(self, kind: int, identifier: int | None = None, timeout: float = 5) -> Frame:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for i, frame in enumerate(self.controls):
                actual = int.from_bytes(frame.body[:2], "big") if kind != 0x20 else None
                if frame.kind == kind and (identifier is None or actual == identifier):
                    return self.controls.pop(i)
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, "missing_frame", hex(kind))
            if frame.kind == 0x30:
                require(len(self.publications) < MAX_BUFFERED, "probe_message_queue_budget")
                self.publications.append(decode_publish(frame))
                continue
            actual = int.from_bytes(frame.body[:2], "big") if kind != 0x20 else None
            if frame.kind == kind and (identifier is None or actual == identifier):
                return frame
            require(len(self.controls) < MAX_BUFFERED, "probe_control_queue_budget")
            self.controls.append(frame)
        raise ReceiptFailure("missing_frame", hex(kind))

    def subscribe(self, identifier: int, filters: list[tuple[str, int]],
                  expected: tuple[int, ...] | None = None) -> tuple[int, ...]:
        self.send(subscribe_packet(identifier, filters))
        mid, reasons = decode_filter_ack(self.wait(0x90, identifier))
        require(mid == identifier and len(reasons) == len(filters), "suback_request_mismatch")
        require(reasons == (expected if expected is not None else (1,) * len(filters)),
                "unexpected_suback_reasons", repr(reasons))
        self.observations.record("suback", label=self.label, packet_id=mid, reasons=list(reasons))
        return reasons

    def unsubscribe(self, identifier: int, filters: list[str]) -> tuple[int, ...]:
        self.send(unsubscribe_packet(identifier, filters))
        mid, reasons = decode_filter_ack(self.wait(0xB0, identifier))
        require(mid == identifier and len(reasons) == len(filters), "unsuback_request_mismatch")
        require(all(reason in (0, 0x11) for reason in reasons), "unsubscribe_refused")
        self.observations.record("unsuback", label=self.label, packet_id=mid, reasons=list(reasons))
        return reasons

    def publish(self, identifier: int, topic: str, payload: bytes, *, qos: int = 1,
                dup: bool = False) -> None:
        self.send(publish_packet(topic, payload, identifier, qos=qos, dup=dup))
        frame = self.wait(0x40 if qos == 1 else 0x50, identifier)
        require(frame.ack() == (identifier, 0), "publisher_ack_refused")
        if qos == 2:
            self.send(pubrel_packet(identifier))
            require(self.wait(0x70, identifier).ack() == (identifier, 0), "publisher_completion_refused")

    def next_publication(self, timeout: float = 5) -> Publication:
        if self.publications:
            return self.publications.pop(0)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, "outgoing_delivery_missing")
            if frame.kind == 0x30:
                publication = decode_publish(frame)
                self.observations.record("outgoing_publish_received", label=self.label, **publication.observation())
                return publication
            require(len(self.controls) < MAX_BUFFERED, "probe_control_queue_budget")
            self.controls.append(frame)
        raise ReceiptFailure("outgoing_delivery_missing")

    def acknowledge(self, publication: Publication, reason: int = 0) -> None:
        self.send(puback_packet(publication.packet_id, reason))
        self.observations.record("subscriber_puback_sent", label=self.label,
                                 packet_id=publication.packet_id, reason=reason)

    def no_publication(self, timeout: float = .2, *, code: str = "unexpected_delivery") -> None:
        require(not self.publications, code)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.poll(max(0, deadline - time.monotonic()))
            if frame is None:
                return
            require(frame.kind != 0x30, code)
            require(len(self.controls) < MAX_BUFFERED, "probe_control_queue_budget")
            self.controls.append(frame)


def durable(path: Path) -> dict:
    """Independent bounded transaction over every application table.

    Table/column introspection makes this reader useful across the explicit
    schema transition without inventing empty projections for unknown tables.
    """
    require(path.is_file(), "database_missing")
    with sqlite3.connect(f"file:{quote(str(path.resolve()), safe='/')}?mode=ro", uri=True, timeout=.25) as db:
        db.execute("BEGIN")
        require(db.execute("PRAGMA quick_check").fetchmany(8) == [("ok",)], "sqlite_integrity")
        names = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchmany(33)
        require(len(names) <= 32, "inspection_table_budget")
        tables = {}
        budget = 0
        for (name,) in names:
            require(isinstance(name, str) and re.fullmatch(r"[a-z_]{1,64}", name), "inspection_table_name")
            count = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            require(count <= 16384, "inspection_row_budget", name)
            columns = [row[1] for row in db.execute(f'PRAGMA table_info("{name}")')]
            rows = []
            for values in db.execute(f'SELECT * FROM "{name}"'):
                row = {}
                for column, value in zip(columns, values):
                    if isinstance(value, bytes):
                        require(len(value) <= 1024 * 1024, "inspection_field_budget", column)
                        budget += len(value)
                        require(budget <= 64 * 1024 * 1024, "inspection_snapshot_budget")
                        row[column] = value.hex() if len(value) <= 1024 else {
                            "bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
                    else:
                        row[column] = value
                rows.append(row)
            tables[name] = sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))
        meta = tables["meta"][0]
        return {"quick_check": "ok", "schema_version": db.execute("PRAGMA user_version").fetchone()[0],
                "tables": tables, "counts": {name: len(rows) for name, rows in tables.items()},
                "admissions": [row for row in tables["operations"] if row["decision"] == 1],
                "epoch": int(meta["last_opened"], 16), "revision": int(meta["revision"], 16)}


def session_key(state: dict, client_id: str) -> str:
    matching = [row["id"] for row in state["tables"]["sessions"]
                if row["client_id"] == client_id.encode().hex()]
    require(len(matching) == 1, "durable_session_identity_missing", client_id)
    return matching[0]
