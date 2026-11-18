"""Bounded process supervision and a private file-based operator control plane."""
from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
import ctypes
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import subprocess
import tempfile
import time

from common import broker_argv, require
from site_health import Health, alert, probe
from site_ops import BROKERS, CONSUMERS, DATABASES, LANES, PRODUCERS, ROLES, VIEWER, atomic_json, document, locked


POLL = .05
HEALTH_SECONDS = 2
START_SECONDS = 180
RESTART_WINDOW = 60
RESTART_LIMIT = 5
SERVICE_LOG_BYTES = 1024 * 1024
FATAL_LINES = ("history_session_missing", "event_payload_conflict", "archive_missing", "archive_manifest",
               "archive_catalog", "archive_record", "archive_coverage_gap", "archive_hot_", "invalid_archive_",
               "unsupported_local_store_format", "store_missing_or_replaced", "security_policy",
               "local_store_identity_or_budget_mismatch", "invalid_event_")


@dataclass
class Child:
    role: str
    process: subprocess.Popen | None = None
    ready: bool = False
    paused: bool = False
    suspended: str | None = None
    launches: int = 0
    started: float = 0
    wake: float = 0
    failures: deque = field(default_factory=deque)
    last_exit: int | None = None
    partial: bytes = b""
    events: Path | None = None
    seq: int = 0
    offset: int = 0
    inode: int | None = None
    event_gaps: int = 0
    security: Counter = field(default_factory=Counter)
    last_status: str | None = None
    ready_events: set = field(default_factory=set)

    def view(self):
        return {"pid": self.process.pid if self.process and self.process.poll() is None else None,
                "ready": self.ready, "paused": self.paused, "suspended": self.suspended,
                "launches": self.launches, "last_exit": self.last_exit, "diagnostic": self.last_status}


def die_with_supervisor(parent):
    # This runs between fork and exec in a single-threaded supervisor on Linux.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0 or os.getppid() != parent:
        os._exit(126)


class Supervisor:
    def __init__(self, site, window=None):
        self.site = site
        self.actors = {role: Child(role) for role in ROLES}
        self.health = Health(site, window)
        self.phase = "starting"
        self.last_command = None
        self.stopping = False
        self.metrics = {}
        self.run_id = str(time.time_ns())
        self.health_at = 0
        self.drain = None

    def start(self, actor):
        site = self.site
        if actor.role in DATABASES:
            site.assert_store(actor.role)
        if actor.role in BROKERS:
            lane = actor.role.removeprefix("broker-")
            journals = site.root / "logs" / f"{actor.role}-journals"
            journals.mkdir(mode=0o700, exist_ok=True)
            # Keep two complete, exclusively allocated journal generations.
            prior = sorted(journals.glob("run-*"), key=lambda p: p.stat().st_mtime_ns)
            for stale in prior[:-1]:
                require(stale.is_dir() and not stale.is_symlink(), "invalid_journal_retention_path")
                shutil.rmtree(stale)
            directory = Path(tempfile.mkdtemp(prefix="run-", dir=journals))
            actor.events = directory / "events.jsonl"
            actor.seq, actor.offset, actor.inode = 0, 0, None
            argv = broker_argv(site.package / "mqttiumd", site.db(actor.role), actor.events,
                               site.pki(lane), listen=f'127.0.0.1:{site.config["ports"][lane]}')
        else:
            argv = site.client_argv(actor.role)
        parent = os.getpid()
        actor.process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, preexec_fn=lambda: die_with_supervisor(parent))
        os.set_blocking(actor.process.stdout.fileno(), False)
        actor.started = time.monotonic()
        actor.ready = False
        actor.ready_events.clear()
        actor.partial = b""
        actor.launches += 1

    def append_log(self, role, data):
        path = self.site.root / "logs" / (role + ".log")
        if path.exists() and path.stat().st_size + len(data) > SERVICE_LOG_BYTES:
            path.replace(path.with_suffix(".previous"))
        with path.open("ab") as target:
            target.write(data)

    def output(self, actor):
        proc = actor.process
        if proc is None:
            return
        try:
            data = os.read(proc.stdout.fileno(), 65536)
        except BlockingIOError:
            return
        if not data:
            return
        self.append_log(actor.role, data)
        lines = (actor.partial + data).split(b"\n")
        actor.partial = lines.pop()
        require(len(actor.partial) <= 65536, "service_log_record_budget")
        import json
        for line in lines:
            if line.startswith(b"{"):
                row = json.loads(line)
                self.health.live.record(actor.role, row)
                kind = row.get("kind")
                if kind in ("history_ready", "live_ready", "live_producer_ready", "producer_ready"):
                    if actor.role in PRODUCERS:
                        actor.ready_events.add(kind)
                        actor.ready = {"live_producer_ready", "producer_ready"} <= actor.ready_events
                    else:
                        actor.ready = True
                elif kind in ("history_retry", "archive_pressure", "enqueue_refused", "storage_pressure"):
                    actor.last_status = row.get("reason", kind)
            else:
                message = line.decode("utf-8", "replace")
                for check in FATAL_LINES:
                    if check in message:
                        actor.suspended = check
                        break

    def broker_events(self, actor):
        path = actor.events
        if path is None or not path.exists():
            return
        import json
        current = path.stat().st_ino
        if actor.inode is not None and current != actor.inode:
            prior = path.with_suffix(path.suffix + ".previous")
            if prior.exists() and prior.stat().st_ino == actor.inode:
                self.read_events(actor, prior)
            actor.offset = 0
        actor.inode = current
        self.read_events(actor, path)

    def read_events(self, actor, path):
        import json
        with path.open("rb") as handle:
            handle.seek(actor.offset)
            # 1 MiB per turn, independent of the amount of retained log.
            data = handle.read(1024 * 1024)
        end = data.rfind(b"\n")
        if end < 0:
            return
        actor.offset += end + 1
        for raw in data[:end].split(b"\n"):
            row = json.loads(raw)
            seq = int(row["seq"])
            if seq > actor.seq + 1:
                actor.event_gaps += seq - actor.seq - 1
            actor.seq = max(actor.seq, seq)
            kind = row["kind"]
            info = row.get("details", {})
            if kind == "ready":
                actor.ready = (info["sqlite_schema"] == 13
                               and info["address"] == "127.0.0.1:" + str(
                                   self.site.config["ports"][actor.role.removeprefix("broker-")]))
            if kind in ("tls_authenticated", "authorization_refused"):
                actor.security[kind] += 1
            for reason in (row.get("reason"), info.get("reason"), info.get("code")):
                if isinstance(reason, str) and ("tls_" in reason or "handshake" in reason):
                    actor.security["tls_refusal_or_error"] += 1
                    break

    def reap(self, actor, status):
        self.output(actor)
        actor.process.stdout.close()
        actor.process, actor.ready, actor.last_exit = None, False, status

    def stop_roles(self, roles):
        waiting = [self.actors[role] for role in roles if self.actors[role].process is not None]
        for actor in waiting:
            if actor.process.poll() is None:
                actor.process.send_signal(signal.SIGINT)
        until = time.monotonic() + 10
        exits = {}
        while waiting:
            for actor in waiting[:]:
                self.output(actor)
                status = actor.process.poll()
                if status is None and time.monotonic() > until:
                    actor.process.kill()
                    status = actor.process.wait(3)
                if status is not None:
                    exits[actor.role] = status
                    self.reap(actor, status)
                    waiting.remove(actor)
            if waiting:
                time.sleep(POLL)
        return exits

    def failed(self, actor, status):
        self.reap(actor, status)
        now = time.monotonic()
        actor.failures.append(now)
        while actor.failures[0] < now - RESTART_WINDOW:
            actor.failures.popleft()
        if len(actor.failures) >= RESTART_LIMIT:
            actor.suspended = actor.suspended or "restart_limit"
        actor.wake = now + min(.5 * 2 ** (len(actor.failures) - 1), 8)

    def tick(self, *, restart=False):
        for actor in self.actors.values():
            if actor.process is not None:
                self.output(actor)
                if actor.events is not None:
                    self.broker_events(actor)
                status = actor.process.poll()
                if status is not None:
                    self.failed(actor, status)
                elif not actor.ready and time.monotonic() - actor.started > START_SECONDS:
                    actor.suspended = "recovery_readiness_deadline"
                    self.stop_roles((actor.role,))
            if (restart and actor.process is None and not actor.paused and not actor.suspended
                    and time.monotonic() >= actor.wake):
                try:
                    self.start(actor)
                except (OSError, ValueError) as error:
                    actor.suspended = str(error)

    def active_generation(self):
        schedule = self.site.runtime / "schedule.json"
        if not schedule.exists():
            return False
        plan = document(schedule)
        stop = schedule.with_suffix(".stop.json")
        limit = document(stop)["events"] if stop.exists() else plan["events"]
        current = time.monotonic_ns()
        return plan["start_ns"] <= current < plan["start_ns"] + limit * 1_000_000_000 // plan["rate"]

    def publish_status(self, *, sample=False, durable=False):
        if sample:
            for role in DATABASES:
                try:
                    self.site.assert_store(role)
                except (OSError, ValueError):
                    self.health.suspensions[role] = "durable_store_missing_or_replaced"
            try:
                self.metrics = self.health.collect(self.actors, self.active_generation())
            except (OSError, sqlite3.Error, ValueError) as error:
                self.metrics["collection_error"] = alert("durable_sample_unavailable", "critical",
                                                        "Check the original files and writer; preserve all stores.",
                                                        detail=str(error))
            for role, reason in self.health.suspensions.items():
                actor = self.actors[role]
                if not actor.suspended:
                    actor.suspended = reason
                    self.stop_roles((role,))
        self.metrics["suspended_services"] = [
            alert(child.suspended, "critical", "Leave the role stopped and reconcile the original store.", role=role)
            for role, child in self.actors.items() if child.suspended
        ]
        services = {role: actor.view() for role, actor in self.actors.items()}
        atomic_json(self.site.runtime / "status.json",
                    {"phase": self.phase, "pid": os.getpid(), "run": self.run_id,
                     "monotonic_ns": time.monotonic_ns(), "updated_ns": time.time_ns(),
                     "actors": services, "last_command": self.last_command, "shutdown": self.drain,
                     "metrics": self.metrics}, durable=durable)

    def command(self):
        path = self.site.runtime / "request.json"
        if not path.exists():
            return
        request = document(path)
        mode, role = request.get("mode"), request.get("role")
        try:
            if mode == "stop":
                self.stopping = True
            elif role in self.actors and mode == "pause":
                self.actors[role].paused = True
                self.stop_roles((role,))
            elif role in self.actors and mode == "resume":
                require(not self.actors[role].suspended, "component_suspended_reconcile_offline")
                self.actors[role].paused = False
            else:
                raise ValueError("unknown_operator_request")
            self.last_command = {"token": request["token"], "status": "accepted"}
        except (ValueError, OSError) as error:
            self.last_command = {"token": request["token"], "status": "refused", "reason": str(error)}
        self.publish_status()
        path.unlink()

    def await_group(self, roles):
        for role in roles:
            self.start(self.actors[role])
        expires = time.monotonic() + START_SECONDS
        while not all(self.actors[role].ready for role in roles):
            require(time.monotonic() < expires and not self.stopping, "initial_recovery_did_not_complete")
            self.tick()
            require(all(self.actors[role].process is not None for role in roles), "initial_service_exited")
            time.sleep(POLL)

    def shutdown(self):
        self.phase = "quiescing"
        self.publish_status()
        exits = self.stop_roles(PRODUCERS)
        limit = time.monotonic() + 5
        drained = False
        while time.monotonic() < limit:
            self.tick()
            from common import inventory
            if inventory(self.site.db("broker-history"))["counts"]["obligations"] == 0:
                drained = True
                break
            time.sleep(.1)
        exits.update(self.stop_roles((*CONSUMERS, VIEWER)))
        exits.update(self.stop_roles(BROKERS))
        schedule = self.site.runtime / "schedule.json"
        if schedule.exists():
            schedule.replace(schedule.with_name("last-schedule.json"))
        schedule.with_suffix(".stop.json").unlink(missing_ok=True)
        return {"known_backlog_drained": drained, "exits": exits}

    def serve(self):
        self.site.allow_run()
        self.site.check()
        request = self.site.runtime / "request.json"
        require(not request.exists(), "stale_operator_request")
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.signal)
        self.publish_status()
        fault = None
        try:
            self.await_group(BROKERS)
            for lane in LANES:
                probe(self.health.context[lane], self.site.config["ports"][lane])
            self.await_group(CONSUMERS)
            self.await_group((VIEWER,))
            self.await_group(PRODUCERS)
            self.phase = "running"
            while not self.stopping:
                self.tick(restart=True)
                self.command()
                if time.monotonic() >= self.health_at:
                    self.publish_status(sample=True)
                    self.health_at = time.monotonic() + HEALTH_SECONDS
                time.sleep(POLL)
        except BaseException as error:
            fault = error
            raise
        finally:
            try:
                self.drain = self.shutdown()
            finally:
                self.stop_roles(ROLES)
                self.phase = "failed" if fault else "stopped"
                if fault:
                    self.metrics["fault"] = str(fault)
                self.publish_status(durable=True)

    def signal(self, _signal, _frame):
        self.stopping = True


def run(site, window):
    with locked(site.root / "site.lock"):
        Supervisor(site, window).serve()
