"""Bounded local health and progress observations for a private site."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import time

from common import CAPTEURS, inventory, processes, read_db, require
from forward_store import snapshot
from segment_store import MANIFEST_LIMIT, RECORD, read_bounded, segment_name
from site_ops import BROKERS, CONSUMERS, LANES, PRODUCERS, PROBE, document, tls_context


def probe(context, port, client_id=PROBE):
    identity = client_id.encode("ascii")
    content = b"\x00\x04MQTT\x05\x02\x00\x0f\x00" + len(identity).to_bytes(2, "big") + identity
    with socket.create_connection(("127.0.0.1", port), timeout=.4) as tcp:
        with context.wrap_socket(tcp, server_hostname="127.0.0.1") as tls:
            require(tls.version() == "TLSv1.3", "unexpected_tls_version")
            with tls.makefile("rb") as view:
                tls.sendall(bytes((0x10, len(content))) + content)
                head = view.read(2)
                require(len(head) == 2 and head[0] == 0x20 and 3 <= head[1] < 128, "invalid_health_connack")
                ack = view.read(head[1])
                require(len(ack) == head[1] and ack[:2] == b"\x00\x00", f"health_connack_refused:{ack[:2].hex()}")
                tls.sendall(b"\xc0\x00")
                require(view.read(2) == b"\xd0\x00", "health_pingresp_missing")
                tls.sendall(b"\xe0\x00")
    return {"tls": "TLSv1.3", "mqtt": 5, "pingresp": True}


def alert(cause, severity, action, **more):
    return {"cause": cause, "severity": severity, "action": action, **more}


def capacity(cause, usage, limit, action):
    if limit <= 0:
        return None
    percent = 100 * usage / limit
    for threshold, severity in ((95, "critical"), (85, "warning"), (70, "notice")):
        if percent >= threshold:
            return alert(cause, severity, action, percent=round(percent, 3), threshold=threshold)
    return None


class LiveCounters:
    def __init__(self, window=None):
        self.window = window
        self.last = {}
        self.samples = {}
        self.presence = {}
        self.buckets = Counter()
        self.window_buckets = Counter()
        self.received = 0
        self.gaps = 0
        self.drops = Counter()
        self.last_at = None
        self.max_gap = 0
        self.window_last_at = None
        self.window_gap = 0
        self.connections = Counter()
        self.invalid_latency = 0

    def record(self, role, row):
        kind = row.get("kind")
        if kind in ("live_ready", "live_producer_ready", "producer_ready", "history_ready"):
            self.connections[f"{role}/{kind}"] += 1
        elif kind == "live_drop":
            self.drops[role] += 1
        elif kind == "presence":
            self.presence[row["identity"]] = row["state"]
        elif kind == "live_received":
            sensor, stream, sequence = row["event_id"].split("/")
            seq = int(sequence)
            earlier = self.last.get(sensor)
            if earlier and earlier[0] == stream:
                if seq <= earlier[1]:
                    return
                self.gaps += seq - earlier[1] - 1
            ts = row["monotonic_ns"]
            self.last[sensor] = (stream, seq, ts)
            self.received += 1
            if earlier:
                self.samples[sensor] = (ts - earlier[2]) / 1e6
            if self.last_at is not None:
                self.max_gap = max(self.max_gap, ts - self.last_at)
            self.last_at = ts
            lag = row["received_ns"] - row["created_ns"]
            if lag < 0:
                self.invalid_latency += 1
            latency_ms = max(0, min(4001, math.ceil(lag / 1e6)))
            self.buckets[latency_ms] += 1
            ordinal = (seq - 1) * len(CAPTEURS) + CAPTEURS.index(sensor)
            if self.window and self.window[0] <= ordinal < self.window[1]:
                self.window_buckets[latency_ms] += 1
                if self.window_last_at is not None:
                    self.window_gap = max(self.window_gap, ts - self.window_last_at)
                self.window_last_at = ts

    @staticmethod
    def latency(buckets):
        total = sum(buckets.values())
        rank = math.ceil(total * .99)
        running = 0
        p99 = None
        for value in sorted(buckets):
            running += buckets[value]
            if running >= rank:
                p99 = value
                break
        return {"received": total, "p99_ms_rounded_up": p99, "maximum_ms_bucket": max(buckets, default=None)}

    def state(self):
        now = time.monotonic_ns()
        return {"received": self.received, "gaps": self.gaps,
                "drop_count_by_service": dict(self.drops), "connections": dict(self.connections),
                "retained_presence": self.presence, "max_gap_ms": self.max_gap / 1e6,
                "invalid_latency": self.invalid_latency, "latency": self.latency(self.buckets),
                "window": {**self.latency(self.window_buckets), "ordinals": self.window,
                           "max_gap_ms": self.window_gap / 1e6},
                "per_sensor": {name: {"sequence": seq, "stream": stream, "age_ms": (now - stamp) / 1e6,
                                      "last_interval_ms": self.samples.get(name)}
                               for name, (stream, seq, stamp) in self.last.items()}}


def cgroup_io():
    path = Path("/sys/fs/cgroup/io.stat")
    return path.read_text().splitlines() if path.is_file() else []


class Health:
    def __init__(self, site, window):
        self.site = site
        self.live = LiveCounters(window)
        self.context = {lane: tls_context(site, lane) for lane in LANES}
        self.previous = None
        self.stall = {}
        self.suspicion = {}
        self.suspensions = {}
        self.scrub_position = {}
        self.scrubbed = Counter()
        self.last_device = None

    def progress(self, name, value, expected, now, alerts):
        previous, changed = self.stall.get(name, (None, now))
        if value != previous or not expected:
            changed = now
        self.stall[name] = (value, changed)
        if expected and now - changed > 12:
            alerts.append(alert("application_progress_stalled", "warning",
                                "Check credentials, session continuity and the oldest pending event.", role=name))

    def disk(self):
        stat = self.site.root.stat()
        info = os.statvfs(self.site.root)
        topology = f"{os.major(stat.st_dev)}:{os.minor(stat.st_dev)}"
        device = Path("/sys/dev/block") / topology
        physical = None
        if device.exists():
            for row in Path("/proc/diskstats").read_text().splitlines():
                fields = row.split()
                if f"{fields[0]}:{fields[1]}" == topology:
                    physical = {"name": fields[2], "read_ios": int(fields[3]), "write_ios": int(fields[7]),
                                "sectors_written_512b": int(fields[9]), "write_ms": int(fields[10])}
                    break
        return {"total_bytes": info.f_blocks * info.f_frsize, "available_bytes": info.f_bavail * info.f_frsize,
                "free_files": info.f_favail, "used_files": info.f_files - info.f_ffree,
                "fs_device": topology, "device_io_if_mapped": physical, "cgroup_io": cgroup_io()}

    def scrub(self, role, database, limit=4):
        cursor = self.scrub_position.get(role, ("", b"", 0))
        with read_db(database) as db:
            rows = db.execute(
                "SELECT sensor,stream,first,last,fingerprint FROM segments "
                "WHERE (sensor,stream,first) > (?,?,?) ORDER BY sensor,stream,first LIMIT ?",
                (*cursor, limit),
            ).fetchall()
        directory = Path(str(database) + ".segments")
        if not rows:
            self.scrub_position.pop(role, None)
        for sensor, stream, first, last, expected in rows:
            count = last - first + 1
            require(1 <= count <= 1024 and first > 0, "archive_scrub_range_invalid")
            folder = directory / segment_name(sensor, stream, first)
            require(not folder.is_symlink() and folder.is_dir(), "archive_scrub_segment_missing")
            meta = read_bounded(folder / "manifest.json", MANIFEST_LIMIT)
            require(hashlib.sha256(meta).digest() == expected, "archive_scrub_manifest_mismatch")
            data = read_bounded(folder / "events.zlib", count * RECORD.size + 1024)
            require(hashlib.sha256(data).hexdigest() == json.loads(meta)["data_sha256"], "archive_scrub_data_mismatch")
            self.scrub_position[role] = (sensor, stream, first)
            self.scrubbed[role] += 1

    def collect(self, actors, active_generation):
        site = self.site
        now = time.monotonic()
        alerts = []
        readings, history, broker, services = {}, {}, {}, {}
        disk = self.disk()
        if (device := disk["device_io_if_mapped"]) is not None:
            if self.last_device and self.last_device["name"] == device["name"]:
                writes = device["write_ios"] - self.last_device["write_ios"]
                device["window_written_bytes"] = (device["sectors_written_512b"] -
                                                   self.last_device["sectors_written_512b"]) * 512
                device["mean_write_ms"] = (device["write_ms"] - self.last_device["write_ms"]) / writes if writes > 0 else None
            self.last_device = dict(device)
        used = disk["total_bytes"] - disk["available_bytes"]
        alerts.append(capacity("filesystem_capacity", used, disk["total_bytes"],
                               "Reserve free space or stop generation and export to a different volume."))
        for role, child in actors.items():
            if child.process is None or child.process.poll() is not None:
                continue
            db = site.db(role) if role in (*BROKERS, *PRODUCERS, *CONSUMERS) else site.runtime / "live-unused.sqlite"
            try:
                services[role] = {"pid": child.process.pid, **processes(child.process.pid, db)}
            except (FileNotFoundError, StopIteration):
                continue
        for lane, role in zip(LANES, BROKERS):
            actor = actors[role]
            ping = None
            if actor.ready and actor.process and actor.process.poll() is None:
                try:
                    ping = probe(self.context[lane], site.config["ports"][lane])
                except (OSError, ValueError) as error:
                    alerts.append(alert("mqtt_tls_health_probe_failed", "warning",
                                        "Check TLS credentials and broker journal.", lane=lane, detail=str(error)))
            try:
                site.assert_store(role)
                broker[lane] = {**inventory(site.db(role)), "mqtt_probe": ping, "security_events": dict(actor.security),
                                "event_gaps": actor.event_gaps}
                if not broker[lane]["invariants_ok"]:
                    self.confirmed_suspicion(role, "broker_reference_invariant", alerts)
                else:
                    self.suspicion.pop(role, None)
            except (OSError, ValueError, RuntimeError) as error:
                self.confirmed_suspicion(role, "broker_store_unreadable", alerts, str(error))
        for role in PRODUCERS:
            site.assert_store(role)
            state = snapshot(site.db(role))
            readings[role] = state
            alerts.append(capacity("outbox_capacity", state["occupied_bytes"], state["budget"],
                                   "Slow the producer and restore HISTORY drain capacity."))
            self.progress(role, state["enqueued"], active_generation and actors[role].ready, now, alerts)
        backlog = broker.get("history", {}).get("counts", {}).get("obligations")
        for role in CONSUMERS:
            site.assert_store(role)
            state = snapshot(site.db(role))
            with read_db(site.db(role)) as db:
                rows = db.execute("SELECT sensor,hex(stream),archived FROM streams LIMIT 65").fetchall()
                require(len(rows) <= 64, "history_stream_budget_invalid")
            state["contiguous_archived_frontiers"] = {sensor + "/" + stream.lower(): seq for sensor, stream, seq in rows}
            try:
                self.scrub(role, site.db(role))
            except (ValueError, OSError) as error:
                self.suspensions[role] = str(error)
                alerts.append(alert("archive_integrity_unverifiable", "critical",
                                    "Leave the consumer stopped and recover a verified archive.", role=role, detail=str(error)))
            state["cold_files_checked"] = self.scrubbed[role]
            state["cold_committed_file_count"] = state["segment_count"] * 2
            state["cold_committed_directory_count"] = state["segment_count"]
            history[role] = state
            alerts.append(capacity("archive_capacity", state["archive_charge"], state["archive_budget"],
                                   "Export a verified backup and stop before the cold budget is exhausted."))
            alerts.append(capacity("segment_capacity", state["segment_count"], state["segment_limit"],
                                   "Plan controlled retention before registering another segment."))
            self.progress(role, state["unique_count"], bool(backlog) and actors[role].ready, now, alerts)
            confirmed = sum(row["confirmed"] for row in readings.values())
            # Readers take independent snapshots. Require consecutive empty-backlog
            # observations before treating a lower applied count as an identity fault.
            if state["invalid"] or state["conflicts"] or (not backlog and backlog is not None
                    and confirmed > state["unique_count"] and all(row["pending"] == 0 for row in readings.values())):
                self.confirmed_suspicion(role, "history_ledger_gap_or_contradiction", alerts)
            else:
                self.suspicion.pop(role, None)
        confirmed = sum(s["confirmed"] for s in readings.values())
        enqueued = sum(s["enqueued"] for s in readings.values())
        pending = sum(s["pending"] for s in readings.values())
        history_rate = None
        if self.previous:
            when, old_confirmed, old_enqueued = self.previous
            span = now - when
            history_rate = {"window_s": span, "admissions_per_s": (confirmed - old_confirmed) / span,
                            "generation_per_s": (enqueued - old_enqueued) / span,
                            "net_drain_per_s": ((confirmed - old_confirmed) - (enqueued - old_enqueued)) / span}
        self.previous = (now, confirmed, enqueued)
        live = self.live.state()
        if active_generation:
            for name in CAPTEURS:
                if live["per_sensor"].get(name, {}).get("age_ms", 1e9) > 2000:
                    alerts.append(alert("live_sensor_stale", "warning", "Check LIVE connection and reconcile HISTORY.",
                                        sensor=name))
        last = site.runtime / "backup.json"
        backup = document(last) if last.is_file() else None
        if backup is None or time.time_ns() - backup["created_ns"] > 86_400_000_000_000:
            alerts.append(alert("backup_missing_or_old", "notice", "Quiesce and verify a coordinated backup."))
        return {"disk": disk, "services": services, "live": live, "history": {"sources": readings, "ledgers": history,
                 "confirmed": confirmed, "enqueued": enqueued, "pending": pending, "rates": history_rate},
                "brokers": broker, "backup": backup,
                "alerts": [value for value in alerts if value is not None]}

    def confirmed_suspicion(self, role, cause, alerts, detail=""):
        count = self.suspicion.get(role, 0) + 1
        self.suspicion[role] = count
        alerts.append(alert(cause, "critical" if count >= 3 else "warning",
                            "Keep data offline and reconcile the original store before resuming.", role=role, detail=detail))
        if count >= 3:
            self.suspensions[role] = cause
