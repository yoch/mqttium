"""Read a Mosquitto 2.0.21 persistence snapshot without restoring or exposing messages."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import os
import stat
import struct


MAGIC = b"\x00\xb5\x00mosquitto db"
DB_VERSION = 6
MAX_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 200_000
COUNT_UNITS = {
    "persistent_sessions": "client session records",
    "subscriptions": "client/filter records",
    "stored_messages": "unique message store records",
    "stored_payload_bytes": "bytes across message store records",
    "outgoing_queued": "client references in the outgoing queue",
    "outgoing_inflight": "client references in an active outgoing exchange",
    "incoming_queued": "client references in the incoming queue",
    "incoming_inflight": "client references in an active incoming exchange",
    "retained_values": "distinct retained topics",
}
DELIVERY_COUNTS = ("outgoing_queued", "outgoing_inflight", "incoming_queued", "incoming_inflight")


class SnapshotError(Exception):
    def __init__(self, status, detail, version=None):
        super().__init__(detail)
        self.status = status
        self.version = version


def valid(condition, detail):
    if not condition:
        raise SnapshotError("invalid_format", detail, DB_VERSION)


def text_field(raw, field):
    if not raw or 0 in raw:
        raise SnapshotError("invalid_format", f"empty or NUL-containing {field}", DB_VERSION)
    try:
        return bytes(raw).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SnapshotError("invalid_format", f"invalid UTF-8 in {field}", DB_VERSION) from exc


def property_bounds(rest):
    if not rest:
        return
    length = 0
    for index, byte in enumerate(rest[:4]):
        length += (byte & 127) << (7 * index)
        if byte < 128:
            valid(length == len(rest) - index - 1, "wrong property section size")
            return
    valid(False, "invalid property length")


def chunks(raw):
    position, n = 23, 0
    while position < len(raw):
        n += 1
        if n > MAX_RECORDS:
            raise SnapshotError("inventory_limit", "too many persistence records", DB_VERSION)
        valid(position + 8 <= len(raw), "truncated record header")
        kind, length = struct.unpack_from(">II", raw, position)
        start = position + 8
        position = start + length
        valid(position <= len(raw), "truncated record body")
        yield kind, raw[start:position]


def parse_snapshot(data, known_clients):
    # v6 mixes network-order fields with native LE64 dbids and fixed-size
    # struct padding. See persist.h and persist_write_v5.c in 2.0.21.
    raw = memoryview(data)
    if raw[:len(MAGIC)] != MAGIC:
        raise SnapshotError("unknown_format", "Mosquitto persistence signature not found")
    valid(len(raw) >= 23, "truncated persistence header")
    version = struct.unpack_from(">I", raw, 19)[0]
    if version != DB_VERSION or raw[15:19] != bytes(4):
        raise SnapshotError("unknown_format", "expected Mosquitto format 6 with its reserved CRC field", version)

    last_id = shutdown = None
    stores, sessions, subscriptions, retained, references = {}, set(), set(), set(), set()
    by_client = {}
    counts = dict.fromkeys(COUNT_UNITS, 0)
    for kind, record in chunks(raw):
        valid(last_id is not None or kind == 1, "configuration record must come first")
        if kind == 1:
            valid(last_id is None and len(record) == 16, "invalid or duplicate configuration record")
            last_id, shutdown, id_size = struct.unpack_from("<QBB", record)
            if id_size != 8:
                raise SnapshotError("unknown_format", "expected 64-bit Mosquitto store IDs", version)
            valid(shutdown in (0, 1), "unknown checkpoint flag")
        elif kind == 2:
            valid(len(record) >= 32, "truncated stored message")
            store_id = struct.unpack_from("<Q", record)[0]
            payload_len, _, source_len, username_len, topic_len, _, qos, retain = struct.unpack_from(">IHHHHHBB", record, 16)
            topic_at = 32 + source_len + username_len
            topic_end = topic_at + topic_len
            payload_end = topic_end + payload_len
            valid(0 < store_id <= last_id and store_id not in stores and qos <= 2 and retain <= 1
                  and payload_end <= len(record), "invalid stored message fields")
            stores[store_id] = text_field(record[topic_at:topic_end], "stored topic")
            property_bounds(record[payload_end:])
            counts["stored_payload_bytes"] += payload_len
        elif kind == 3:
            valid(len(record) >= 16, "truncated client message reference")
            store_id = struct.unpack_from("<Q", record)[0]
            _, id_len, qos, state, retain_dup, direction = struct.unpack_from(">HHBBBB", record, 8)
            end = 16 + id_len
            valid(end <= len(record) and qos <= 2 and 1 <= state <= 11 and direction in (0, 1)
                  and retain_dup in (0, 1, 16, 17), "invalid client message reference fields")
            client = text_field(record[16:end], "reference ClientID")
            property_bounds(record[end:])
            label = ("outgoing_" if direction else "incoming_") + ("queued" if state == 11 else "inflight")
            by_client.setdefault(client, Counter())[label] += 1
            counts[label] += 1
            references.add(store_id)
        elif kind == 4:
            valid(len(record) == 8, "invalid retained reference size")
            store_id = struct.unpack_from("<Q", record)[0]
            valid(store_id not in retained, "duplicate retained reference")
            retained.add(store_id)
        elif kind == 5:
            valid(len(record) >= 12, "truncated subscription")
            _, id_len, filter_len, qos, _ = struct.unpack_from(">IHHBB", record)
            end = 12 + id_len
            valid(end + filter_len == len(record) and qos <= 2, "invalid subscription lengths or QoS")
            sub = (text_field(record[12:end], "subscription ClientID"), text_field(record[end:], "subscription filter"))
            valid(sub not in subscriptions, "duplicate subscription")
            subscriptions.add(sub)
        elif kind == 6:
            valid(len(record) >= 24, "truncated persistent session")
            id_len, _, username_len = struct.unpack_from(">HHH", record, 14)
            end = 24 + id_len
            valid(end + username_len == len(record), "invalid session field lengths")
            client = text_field(record[24:end], "session ClientID")
            valid(client not in sessions, "duplicate persistent session")
            sessions.add(client)
        else:
            raise SnapshotError("unknown_format", f"unsupported persistence record type {kind}", version)
    valid(last_id is not None, "configuration record missing")
    valid(references <= stores.keys() and retained <= stores.keys(), "dangling message store reference")
    valid(by_client.keys() <= sessions and all(client in sessions for client, _ in subscriptions),
          "dangling client session reference")
    valid(len({stores[key] for key in retained}) == len(retained), "duplicate retained topic")
    counts.update(persistent_sessions=len(sessions), subscriptions=len(subscriptions),
                  stored_messages=len(stores), retained_values=len(retained))
    return {
        "database_version": version,
        "saved_on": "clean_shutdown" if shutdown else "autosave_or_manual_save",
        "counts": counts,
        "units": COUNT_UNITS,
        "pending_by_declared_client": {client: dict(by_client[client])
                                       for client in sorted(known_clients & by_client.keys())},
        "unmapped_clients_with_pending": len(by_client.keys() - known_clients),
    }


def read_inventory(path, known_clients):
    capture = {"file": str(path)}
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise SnapshotError("unreadable", "expected a regular persistence file")
            if before.st_size > MAX_BYTES:
                raise SnapshotError("inventory_limit", "persistence file exceeds the 64 MiB inspection budget")
            snapshot = source.read(MAX_BYTES + 1)
            after = os.fstat(source.fileno())
            if len(snapshot) != before.st_size or before.st_mtime_ns != after.st_mtime_ns:
                raise SnapshotError("unreadable", "persistence file changed in place during capture")
        capture.update(bytes=len(snapshot), sha256=hashlib.sha256(snapshot).hexdigest(),
                       file_mtime_ns=str(before.st_mtime_ns),
                       captured_at=datetime.now(timezone.utc).isoformat())
        capture.update(status="captured", **parse_snapshot(snapshot, known_clients))
    except FileNotFoundError:
        capture.update(status="absent", detail="no published persistence file")
    except OSError as exc:
        capture.update(status="unreadable", detail=exc.strerror)
    except SnapshotError as exc:
        capture.update(status=exc.status, detail=str(exc))
        if exc.version is not None:
            capture["database_version"] = exc.version
    return capture
