"""Private single-host pilot layout and its on-disk operating identity."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import ssl
import stat
import tempfile
import time
import uuid

from common import Broker, CAPTEURS, DASHBOARDS, PACKAGE, inventory, pki_tools, principal, require, sha, topic, write
from forward_reception import BROKER_SHA, require_binary
from forward_store import MIB, Outbox, PRODUCER_BYTES
from segment_store import ArchiveInbox, ARCHIVE_BYTES, HOT_BYTES, SEGMENT_LIMIT, SEGMENT_ROWS, HOT_ROWS, sync_directory
from split_reception import provision_planes


LANES = ("live", "history")
BROKERS = tuple(f"broker-{lane}" for lane in LANES)
CONSUMERS = tuple(f"history-{name}" for name in DASHBOARDS)
VIEWER = "live-dashboard"
PRODUCERS = CAPTEURS
ROLES = (*BROKERS, *CONSUMERS, VIEWER, *PRODUCERS)
DATABASES = (*BROKERS, *CONSUMERS, *PRODUCERS)
PROBE = "site-health"
FORMAT = "mqttiumd.private-site.v1"
DEFAULTS = {"producer_budget": PRODUCER_BYTES, "budget": HOT_BYTES, "archive_budget": ARCHIVE_BYTES,
            "hot_limit": HOT_ROWS, "segment_rows": SEGMENT_ROWS, "segment_limit": SEGMENT_LIMIT}
METADATA_LIMIT = 128 * 1024
SCRIPT = Path(__file__).resolve().parent


def atomic_json(path, value, *, durable=False):
    temp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix="." + path.name + "-",
                                         delete=False) as output:
            temp = Path(output.name)
            json.dump(value, output, sort_keys=True, allow_nan=False)
            output.write("\n")
            if durable:
                output.flush()
                os.fsync(output.fileno())
        os.replace(temp, path)
        if durable:
            sync_directory(path.parent)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


def document(path, limit=METADATA_LIMIT):
    with path.open("rb") as reader:
        data = reader.read(limit + 1)
    require(len(data) <= limit, "site_document_budget_exceeded")
    return json.loads(data)


def private_directory(path):
    require(path.is_dir() and not path.is_symlink(), f"private_directory_missing:{path}")
    s = path.stat()
    require(s.st_uid == os.getuid() and s.st_mode & 0o077 == 0, f"private_directory_required:{path}")


@contextmanager
def locked(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError(f"operating_lease_held:{path.name}") from None
        yield
    finally:
        os.close(fd)


def store_id(path):
    s = path.lstat()
    require(stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and s.st_size > 0
            and s.st_mode & 0o077 == 0 and s.st_uid == os.getuid(), f"invalid_store:{path.name}")
    return [s.st_dev, s.st_ino]


def archive_options(config):
    return {k: config["budgets"][k] for k in ("archive_budget", "hot_limit", "segment_rows", "segment_limit")}


def expected_policy(lane):
    policy = {cid: ([topic(cid, "telemetry/" + lane)] + ([topic(cid, "status")] if lane == "live" else []), [])
              for cid in CAPTEURS}
    policy.update({cid: ([], ["atelier"]) for cid in DASHBOARDS})
    policy[PROBE] = ([], [])
    return policy


def policy_identity(credentials):
    private_directory(credentials)
    digests = {}
    for lane in LANES:
        directory = credentials / lane
        private_directory(directory)
        rules = directory / "identities.json"
        expected = expected_policy(lane)
        config = document(rules)
        require(config["version"] == 1 and len(config["principals"]) == len(expected), "invalid_site_policy")
        names = set()
        for p in config["principals"]:
            cid = p["name"]
            require(cid in expected and cid not in names and p["client_ids"] == [cid]
                    and (p["publish"], p["subscribe"]) == expected[cid], f"invalid_role_policy:{lane}:{cid}")
            names.add(cid)
        files = ["identities.json", "ca.crt", "server.crt", *(cid + ".crt" for cid in expected)]
        for name in files:
            file = directory / name
            require(file.is_file() and not file.is_symlink() and file.stat().st_size <= 128 * 1024,
                    f"unsafe_credential_file:{lane}:{name}")
        for name in ("server", *expected):
            key = directory / (name + ".key")
            require(key.is_file() and not key.is_symlink() and key.stat().st_mode & 0o077 == 0
                    and key.stat().st_uid == os.getuid(), f"private_key_file_required:{lane}:{name}")
        digests[lane] = {name: sha(directory / name) for name in files}
    require(digests["live"]["identities.json"] != digests["history"]["identities.json"], "lane_policies_aliased")
    return digests


def demo_credentials(root, package):
    root.mkdir(mode=0o700)
    tools = pki_tools(package)
    for lane, pki in provision_planes(root, package).items():
        path = pki / "identities.json"
        rules = document(path)
        control = principal(PROBE, tools.issue(pki, PROBE, "clientAuth"))
        control["subscribe"] = []
        rules["principals"].append(control)
        write(path, rules)


class Site:
    def __init__(self, root, package=PACKAGE):
        require(not root.is_symlink(), "site_root_alias")
        self.root = root.resolve(strict=True)
        private_directory(self.root)
        self.package = package.resolve()
        self.runtime = self.root / "runtime"
        self.config = document(self.root / "site.json")
        self.seal = document(self.root / "sealed.json")
        loc = document(self.root / "local.json")
        self.credentials = Path(loc["credentials"])
        require(self.credentials.is_absolute() and self.config["format"] == FORMAT
                and self.seal["config_sha256"] == sha(self.root / "site.json")
                and self.config["binary_sha256"] == BROKER_SHA, "site_identity_mismatch")
        require(self.config["ports"]["live"] != self.config["ports"]["history"]
                and all(0 < self.config["ports"][lane] < 65536 for lane in LANES), "invalid_site_ports")
        for p in (self.root / "data", self.root / "logs", self.runtime):
            private_directory(p)

    def db(self, role):
        require(role in DATABASES, "unknown_durable_role")
        return self.root / "data" / (role + ".sqlite")

    def pki(self, lane):
        return self.credentials / lane

    def assert_store(self, role):
        require(store_id(self.db(role)) == self.seal["stores"][role], f"store_missing_or_replaced:{role}")

    def check(self):
        require_binary(self.package / "mqttiumd", self.package)
        require(policy_identity(self.credentials) == self.config["credentials"], "credential_or_policy_changed")
        for role in DATABASES:
            self.assert_store(role)
        require(len({tuple(s) for s in self.seal["stores"].values()}) == len(DATABASES), "aliased_site_stores")
        for role in BROKERS:
            info = inventory(self.db(role))
            require(info["user_version"] == 13 and info["invariants_ok"], f"broker_inventory_invalid:{role}")

    def allow_run(self):
        require(not (self.root / "retired.json").exists(), "site_fenced_after_handoff")
        marker = self.root / "restore.json"
        if marker.exists():
            restored = document(marker)
            require(restored.get("activated") is True and restored.get("fenced_source"), "restored_site_requires_handoff")

    def status(self):
        p = self.runtime / "status.json"
        return document(p) if p.exists() else {"phase": "not_started"}

    def client_argv(self, role):
        import sys

        base = [sys.executable, "-u", str(SCRIPT / "split_client.py")]
        budgets = self.config["budgets"]
        if role == VIEWER:
            return base + ["live", "--id", DASHBOARDS[0], "--db", str(self.runtime / "live-unused.sqlite"),
                           "--pki", str(self.pki("live")), "--port", str(self.config["ports"]["live"]), "--trace"]
        arguments = ["--db", str(self.db(role)), "--pki", str(self.pki("history")),
                     "--port", str(self.config["ports"]["history"])]
        if role in PRODUCERS:
            return base + ["producer", "--id", role, *arguments, "--trace",
                           "--live-pki", str(self.pki("live")), "--live-port", str(self.config["ports"]["live"]),
                           "--budget", str(budgets["producer_budget"]), "--schedule", str(self.runtime / "schedule.json")]
        archive = [str(x) for k in ("budget", *archive_options(self.config))
                   for x in ("--" + k.replace("_", "-"), budgets[k])]
        return base + ["history", "--id", role.removeprefix("history-"), *arguments, *archive]


def seal_site(root, config, credentials):
    atomic_json(root / "site.json", config, durable=True)
    atomic_json(root / "local.json", {"credentials": str(credentials)}, durable=True)
    stores = {role: store_id(root / "data" / f"{role}.sqlite") for role in DATABASES}
    atomic_json(root / "sealed.json", {"config_sha256": sha(root / "site.json"), "stores": stores}, durable=True)


def layout(root):
    root.mkdir(mode=0o700)
    for dirname in ("data", "logs", "runtime"):
        (root / dirname).mkdir(mode=0o700)


def init_site(root, package, ports, credentials, *, demo=False, budgets=DEFAULTS):
    root = root.absolute()
    require_binary(package / "mqttiumd", package)
    require(set(ports) == set(LANES) and ports["live"] != ports["history"]
            and all(0 < value < 65536 for value in ports.values()), "invalid_site_ports")
    require(demo != (credentials is not None), "choose_explicit_credentials_or_demo_pki")
    layout(root)
    if demo:
        credentials = root / "credentials"
        demo_credentials(credentials, package)
    require(not credentials.is_symlink(), "credentials_root_alias")
    credentials = credentials.resolve(strict=True)
    config = {"format": FORMAT, "site_id": uuid.uuid4().hex, "ports": ports, "budgets": dict(budgets),
              "binary_sha256": BROKER_SHA, "credentials": policy_identity(credentials), "demo": demo}
    for lane in LANES:
        actor = Broker(package / "mqttiumd", root / "data" / f"broker-{lane}.sqlite", root / "logs",
                       credentials / lane, label=f"bootstrap-{lane}")
        require(actor.stop()["wait_status"] == 0, "bootstrap_broker_stop")
    for role in PRODUCERS:
        with Outbox(root / "data" / (role + ".sqlite"), role, budgets["producer_budget"]):
            pass
    for identity in DASHBOARDS:
        with ArchiveInbox(root / "data" / f"history-{identity}.sqlite", identity,
                          budgets["budget"], **archive_options(config)):
            pass
    sync_directory(root / "data")
    seal_site(root, config, credentials)
    site = Site(root, package)
    site.check()
    atomic_json(site.runtime / "status.json", {"phase": "stopped", "initialized_ns": time.time_ns()}, durable=True)
    return site


def tls_context(site, lane, certificate=PROBE):
    directory = site.pki(lane)
    ctx = ssl.create_default_context(cafile=str(directory / "ca.crt"))
    ctx.minimum_version = ctx.maximum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(directory / f"{certificate}.crt", directory / f"{certificate}.key")
    return ctx
