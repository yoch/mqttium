"""Shared private-fixture helpers for the operator and qualification pilots."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import sqlite3
import ssl
import subprocess
import time

import paho.mqtt
from paho.mqtt import client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


PACKAGE = Path(__file__).resolve().parents[1]
CAPTEURS = tuple(f"capteur-{i}" for i in range(1, 9))
DASHBOARDS = ("dashboard-1", "dashboard-2")
IDENTITIES = (*CAPTEURS, *DASHBOARDS)
FOREVER = 4294967295
COUNTS = ("sessions", "routes", "wills", "retained", "publications", "payloads",
          "obligations", "outbound", "inbound", "operations", "fences", "receipt_origins")


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def write(path, data):
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")


def topic(sensor, leaf="telemetry"):
    return f"atelier/{sensor}/{leaf}"


def wait_for(probe, reason, duration=8):
    end = time.monotonic() + duration
    while time.monotonic() < end:
        value = probe()
        if value:
            return value
        time.sleep(0.01)
    raise ValueError(reason)


def pki_tools(package=PACKAGE):
    spec = importlib.util.spec_from_file_location("pilot_pki", package / "demo-pki.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def principal(cid, pin):
    grants = []
    if cid.startswith("capteur-"):
        grants.append(f"atelier/{cid}")
    return {"name": cid, "certificates": [pin], "client_ids": [cid], "publish": grants,
            "subscribe": [] if grants else ["atelier"]}


def provision(root, package=PACKAGE, host="127.0.0.1"):
    tools = pki_tools(package)
    previous = os.umask(0o077)
    try:
        Path(root).mkdir(mode=0o700)
        tools.ca(root, "ca")
        tools.issue(root, "server", "serverAuth", san=tools.server_san(host))
        policies = [principal(cid, tools.issue(root, cid, "clientAuth")) for cid in IDENTITIES]
        write(root / "identities.json", {"version": 1, "principals": policies})
    finally:
        os.umask(previous)


def tls_context(pki, cid=None, *, issuer="ca"):
    ctx = ssl.create_default_context(cafile=str(pki / f"{issuer}.crt"))
    ctx.minimum_version = ctx.maximum_version = ssl.TLSVersion.TLSv1_3
    if cid:
        ctx.load_cert_chain(pki / f"{cid}.crt", pki / f"{cid}.key")
    return ctx


def mqtt_client(pki, cid, *, cert=None, keepalive=15, will=False, manual_ack=True,
                expiry=FOREVER, receive_maximum=8, expiry_message=None):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid,
                         protocol=mqtt.MQTTv5, reconnect_on_failure=False, manual_ack=manual_ack)
    client.tls_set_context(tls_context(pki, cert or cid))
    client.max_inflight_messages_set(8)
    client.max_queued_messages_set(16)
    if will:
        settings = None
        if expiry_message is not None:
            settings = Properties(PacketTypes.WILLMESSAGE)
            settings.MessageExpiryInterval = expiry_message
        client.will_set(topic(cid, "status"), b"offline", qos=1, retain=True, properties=settings)
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = expiry
    properties.ReceiveMaximum = receive_maximum
    return client, properties, keepalive


@contextmanager
def read_db(path):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    try:
        connection.execute("BEGIN")
        yield connection
    finally:
        connection.close()


def inventory(path):
    with read_db(path) as db:
        info = {name: db.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("user_version", "page_count", "page_size", "freelist_count")}
        counts = {table: db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in COUNTS}
        fields = ("payload_bytes", "references", "exchange_slots", "receipt_slots")
        measured = db.execute("SELECT " + ", ".join(f"used_{f}, limit_{f}" for f in fields)
                              + " FROM meta WHERE one=1").fetchone()
        accounts = dict(zip(fields, (int.from_bytes(n) for n in measured[::2])))
        limits = dict(zip(fields, (int.from_bytes(n) for n in measured[1::2])))
        actual = {
            "payload_bytes": db.execute("SELECT COALESCE(sum(length(bytes)),0) FROM payloads").fetchone()[0],
            "references": counts["obligations"] + counts["retained"],
            "exchange_slots": counts["inbound"] + counts["outbound"],
            "receipt_slots": counts["operations"] + counts["fences"],
        }
        broken_references = sum(
            int.from_bytes(row[0]) != row[1] for row in db.execute("""
            WITH refs AS (
                SELECT p.payload AS id FROM obligations o JOIN publications p ON p.id=o.publication
                UNION ALL
                SELECT p.payload FROM retained r JOIN publications p ON p.id=r.publication
            ), ref_counts AS (SELECT id, count(*) n FROM refs GROUP BY id)
            SELECT b.refs, coalesce(r.n,0) FROM payloads b LEFT JOIN ref_counts r ON r.id=b.id
            UNION ALL
            SELECT x'00', r.n FROM ref_counts r LEFT JOIN payloads b ON r.id=b.id WHERE b.id IS NULL
        """))
        broken_references += db.execute("""
            SELECT (SELECT count(*) FROM obligations o LEFT JOIN publications p ON o.publication=p.id WHERE p.id IS NULL)
                 + (SELECT count(*) FROM retained r LEFT JOIN publications p ON r.publication=p.id WHERE p.id IS NULL)
                 + (SELECT count(*) FROM outbound d LEFT JOIN obligations o ON d.publication=o.publication
                    AND d.idx=o.idx AND d.target=o.target AND d.target_revision=o.target_revision WHERE o.publication IS NULL)
        """).fetchone()[0]
        backlog = {str(int.from_bytes(session)): count for session, count in
                   db.execute("SELECT target,count(*) FROM obligations GROUP BY target")}
        return {**info, "counts": counts, "accounts": accounts, "limits": limits,
                "measured": actual, "backlog_by_session": backlog,
                "invariants_ok": accounts == actual and not broken_references
                and all(accounts[name] <= limits[name] for name in limits),
                "broken_references": broken_references}


def processes(pid, db):
    proc = Path("/proc") / str(pid)
    ticks = os.sysconf("SC_CLK_TCK")
    stat = (proc / "stat").read_text().rpartition(") ")[2].split()
    io = dict(line.split(": ", 1) for line in (proc / "io").read_text().splitlines())
    rss = next(int(line.split()[1]) for line in (proc / "status").read_text().splitlines()
               if line.startswith("VmRSS:"))
    return {"rss_kib": rss, "cpu_user_s": int(stat[11]) / ticks, "cpu_system_s": int(stat[12]) / ticks,
            "threads": int(stat[17]), "write_bytes": int(io["write_bytes"]),
            "wchar": int(io["wchar"]),
            "disk": {suffix or "database": file.stat().st_size if file.exists() else 0
                     for suffix in ("", "-wal", "-shm")
                     if (file := Path(f"{db}{suffix}"))}}


def machine(root):
    def read(path):
        p = Path(path)
        return p.read_text().strip() if p.is_file() else None

    stat = os.statvfs(root)
    return {"platform": platform.platform(), "glibc": platform.libc_ver(), "uname": list(os.uname()),
            "python": platform.python_version(), "inspector_sqlite": sqlite3.sqlite_version,
            "paho": paho.mqtt.__version__, "paho_module_sha256": sha(Path(mqtt.__file__)),
            "cpu_model": next((line.partition(":")[2].strip() for line in
                               Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), None),
            "affinity": sorted(os.sched_getaffinity(0)),
            "cpu_max": read("/sys/fs/cgroup/cpu.max"), "cpuset": read("/sys/fs/cgroup/cpuset.cpus.effective"),
            "memory_max": read("/sys/fs/cgroup/memory.max"), "memory_current": read("/sys/fs/cgroup/memory.current"),
            "cgroup_io": read("/sys/fs/cgroup/io.stat"),
            "fs": subprocess.check_output(["stat", "-f", "-c", "%T", root], text=True).strip(),
            "storage": subprocess.check_output(["findmnt", "-T", root, "-n", "-o", "SOURCE,FSTYPE"], text=True).strip(),
            "free_bytes": stat.f_bavail * stat.f_frsize}


def broker_argv(binary, db, events, pki, *, policy=None, listen="127.0.0.1:0"):
    return list(map(str, [binary, "--listen", listen, "--db", db, "--events", events,
                          "--tls-cert", pki / "server.crt", "--tls-key", pki / "server.key",
                          "--client-ca", pki / "ca.crt", "--identities", policy or pki / "identities.json"]))


def event_rows(path):
    if not path.exists():
        return []
    lines = path.read_bytes().split(b"\n")[:-1]
    return [json.loads(line) for line in lines]


class Broker:
    def __init__(self, binary, db, out, pki, *, policy=None, label="broker"):
        self.db = db
        self.events = out / (label + ".jsonl")
        self.argv = broker_argv(binary, db, self.events, pki, policy=policy)
        with (out / (label + ".log")).open("wb") as log:
            self.child = subprocess.Popen(self.argv, stdout=log, stderr=log)
        try:
            ready = wait_for(self.find_ready, "broker_did_not_report_ready")
            self.port = int(ready["details"]["address"].rsplit(":", 1)[1])
        except BaseException:
            self.stop()
            raise

    def find_ready(self):
        require(self.child.poll() is None, f"broker_exited_{self.child.returncode}")
        return next((row for row in event_rows(self.events) if row["kind"] == "ready"), None)

    def stop(self, kill=False):
        was_alive = self.child.poll() is None
        if was_alive:
            self.child.send_signal(signal.SIGKILL if kill else signal.SIGINT)
        try:
            status = self.child.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.child.kill()
            status = self.child.wait(timeout=5)
        return {"pid": self.child.pid, "argv": self.argv, "signalled": was_alive,
                "requested_signal": "SIGKILL" if kill else "SIGINT", "wait_status": status}
