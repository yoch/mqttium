#!/usr/bin/env python3
"""Check one Mosquitto 2.0.21 mTLS listener, then start it from the checked plan."""
import argparse
from contextlib import ExitStack
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

try:
    import fcntl
except ImportError:
    fcntl = None


BINARY = Path(__file__).resolve().with_name("mqttiumd")
NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
LIMIT = 256 * 1024
CANDIDATE_MARKER = ".mqttiumd-cutover"


def read_file(path, limit):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("regular file required")
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError("file exceeds size budget")
    return data


def encoding(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


class SealingUnavailable(Exception):
    pass


def private_input(stack, data):
    try:
        seals = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SEAL
        fd = os.memfd_create("mqttiumd-checked-input", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        file = stack.enter_context(os.fdopen(fd, "w+b"))
        file.write(data)
        file.flush()
        file.seek(0)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(fd, fcntl.F_GET_SEALS) & seals != seals or not os.access(fdpath(fd), os.R_OK):
            raise SealingUnavailable("the sealed memory file must be readable through Linux procfs")
        return fd
    except (AttributeError, NotImplementedError, OSError) as exc:
        raise SealingUnavailable("Linux memfd, file seals and procfs are required: " + str(exc)) from exc


def fdpath(fd):
    return f"/proc/self/fd/{fd}"


def absolute(value):
    path = Path(value)
    if not path.is_absolute() or any(c in value for c in '\\"\'#'):
        raise ValueError("an absolute path without quoting or escapes is required")
    return path.resolve()


class Report:
    def __init__(self):
        self.rows = []
        self.hashes = {}

    def row(self, path, line, directive, status, effect, *, code=None):
        row = dict(file=str(path), line=line, directive=directive, status=status, effect=effect)
        if code is not None:
            row["code"] = code
        self.rows.append(row)

    def source(self, path):
        try:
            data = read_file(path, LIMIT)
            self.hashes[str(path)] = digest(data)
            lines = data.decode("utf-8").split("\n")
        except (OSError, ValueError) as exc:
            self.row(path, 1, "file", "blocking", str(exc))
            return
        if len(lines) > 2048:
            self.row(path, 2049, "file", "blocking", "maximum 2048 source lines")
            return
        for n, raw in enumerate(lines, 1):
            line = raw.removesuffix("\r")
            if not line or line.startswith("#"):
                continue
            fields = line.split(" ")
            if (len(line.encode()) > 4096 or any(not word for word in fields)
                    or any(not c.isprintable() or (c.isspace() and c != " ") for c in line)):
                self.row(path, n, "syntax", "blocking", "use single ASCII spaces and one directive per line")
                continue
            yield n, fields

    def reject(self, path, line, directive, reason):
        for row in self.rows:
            if (row["file"], row["line"], row["directive"]) == (str(path), line, directive):
                row.update(status="blocking", effect=reason)
                return
        self.row(path, line, directive, "blocking", reason)

    def blocked(self):
        return any(row["status"] == "blocking" for row in self.rows)


FLAGS = {
    "allow_anonymous": ("false", "authentication required"),
    "require_certificate": ("true", "client certificate verified by the configured CA"),
    "use_identity_as_username": ("true", "CN mapped to explicit leaf fingerprints and ClientIDs"),
    "tls_version": ("tlsv1.3", "TLS 1.3 only"),
    "log_type": ("all", "mqttiumd structured event stream in a new run directory"),
}
FILES = {"acl_file", "cafile", "certfile", "keyfile"}


def listener(conf, report):
    values, origins = {}, {}
    for line, fields in report.source(conf):
        key, *args = fields
        try:
            if key in origins:
                raise ValueError("duplicate directive or multiple listeners")
            origins[key] = line
            status = "equivalent"
            if key == "listener" and len(args) == 2:
                port, address = args
                ip = ipaddress.ip_address(address)
                if not ip.is_loopback or ip.version != 4 or not re.fullmatch(r"[1-9][0-9]{0,4}", port) or int(port) > 65535:
                    raise ValueError("this profile requires an explicit IPv4 loopback listener and TCP port")
                value = f"{ip}:{port}"
                effect = value
            elif key in FLAGS and args == [FLAGS[key][0]]:
                value, effect = FLAGS[key]
                if key in {"use_identity_as_username", "log_type"}:
                    status = "translated"
            elif key in FILES and len(args) == 1:
                value = str(absolute(args[0]))
                effect = value
                if key == "acl_file":
                    status = "translated"
            elif key == "persistence" and args in (["true"], ["false"]):
                value = args[0]
                status, effect = "translated", f"source persistence {value} maps to a separate SQLite WAL with FULL synchronization"
            elif key == "persistence_location" and len(args) == 1 and args[0].endswith("/"):
                value = str(absolute(args[0]))
                status, effect = "translated", "Mosquitto storage remains at " + value
            elif key == "persistence_file" and len(args) == 1 and NAME.fullmatch(args[0]) and args[0] not in (".", ".."):
                value = args[0]
                status, effect = "translated", "original Mosquitto database only"
            else:
                raise ValueError("unsupported directive, value or argument count in the 2.0.21 single-listener profile")
            values[key] = value
            report.row(conf, line, key, status, effect)
        except (ValueError, OSError) as exc:
            report.row(conf, line, key, "blocking", str(exc))
    required = set(FLAGS) | FILES | {"listener", "persistence"}
    if values.get("persistence") == "true":
        required |= {"persistence_location", "persistence_file"}
    for missing in sorted(required - origins.keys()):
        report.row(conf, 1, missing, "blocking", "required explicit directive missing from this file")
    return values, origins


def bindings(path, report):
    names, clients = {}, set()
    version = False
    for line, fields in report.source(path):
        try:
            if fields == ["mosquitto_version", "2.0.21"] and not version:
                version = True
                report.row(path, line, fields[0], "equivalent", "source configuration version 2.0.21")
                continue
            if len(fields) < 4 or fields[0] != "client":
                raise ValueError("expected mosquitto_version 2.0.21 or client CN CERT CLIENTID [CLIENTID ...]")
            _, cn, cert, *ids = fields
            if (not NAME.fullmatch(cn) or cn in names or not all(NAME.fullmatch(i) for i in ids)
                    or clients.intersection(ids) or len(set(ids)) != len(ids) or len(clients) + len(ids) > 128):
                raise ValueError("invalid, duplicate or over-budget CN/ClientID mapping")
            names[cn] = (absolute(cert), line, ids)
            clients.update(ids)
            report.row(path, line, "client", "translated", f"CN={cn} permits ClientIDs {', '.join(ids)}")
        except (OSError, ValueError) as exc:
            report.row(path, line, fields[0], "blocking", str(exc))
    if not version or not names:
        report.row(path, 1, "bindings", "blocking", "explicit source version and at least one client required")
    return names


def acl(path, mapping, report):
    rules, user = {}, None
    for line, fields in report.source(path):
        try:
            if fields[0] == "user":
                user = None
                if len(fields) != 2 or fields[1] not in mapping or fields[1] in rules:
                    raise ValueError("each CN requires one block and an explicit certificate/ClientID mapping")
                user = fields[1]
                rules[user] = {key: [] for key in ("publish", "subscribe", "publish_exact", "subscribe_exact")}
                report.row(path, line, "user", "translated", "leaf-pinned principal " + user)
                continue
            if (fields[0] != "topic" or len(fields) != 3 or not user
                    or fields[1] not in ("read", "write", "readwrite")):
                raise ValueError("only explicit per-user read/write/readwrite topic ACLs are supported")
            _, access, topic = fields
            subtree = topic.endswith("/#")
            root = topic[:-2] if subtree else topic
            if (not root or root.startswith("$") or len(root.encode()) > 1024
                    or any(ch in root for ch in ('+', '#', '%', '"', "'", chr(92)))):
                raise ValueError("use an exact ordinary topic or a literal subtree ending in /#")
            directions = {"read": ["subscribe"], "write": ["publish"], "readwrite": ["publish", "subscribe"]}
            for direction in directions[access]:
                key = direction if subtree else direction + "_exact"
                if root in rules[user][key]:
                    raise ValueError("duplicate topic grant")
                rules[user][key].append(root)
            shape = "subtree including its root" if subtree else "one exact topic"
            effect = f"{user} {access} {topic}: {shape}"
            if "read" in access:
                effect += ". mqttiumd grants a filter only if one read rule covers it completely"
            report.row(path, line, "topic", "translated", effect)
        except ValueError as exc:
            report.row(path, line, fields[0], "blocking", str(exc))
    for missing in sorted(mapping.keys() - rules.keys()):
        report.row(path, 1, "user", "blocking", "no explicit ACL block for " + missing)
    return rules


def openssl(*args, data, fds=()):
    result = subprocess.run(["openssl", *args], input=data, capture_output=True, timeout=10, pass_fds=fds)
    if result.returncode:
        raise ValueError("OpenSSL rejected the certificate, its purpose, dates, name or trust chain")
    return result.stdout


class DestinationError(ValueError):
    def __init__(self, path, code, message):
        super().__init__(message)
        self.path = path
        self.code = code

    def diagnostic(self):
        return dict(file=str(self.path), line=None, directive="data", status="blocking",
                    code=self.code, effect=str(self))


def marker_bytes(conf):
    return encoding({"profile": "mqttiumd.mosquitto-cutover.v1", "source_configuration": str(conf)})


def lstat_if_present(path):
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def validate_destination(data, conf):
    """Return whether this path needs its first marker. This function only reads."""
    parent = data.parent
    try:
        parent_info = lstat_if_present(parent)
        if parent_info is None:
            raise DestinationError(parent, "destination_parent_missing", "create the parent directory before cutover")
        if not stat.S_ISDIR(parent_info.st_mode):
            raise DestinationError(parent, "destination_parent_invalid", "destination parent must be a directory")
        info = lstat_if_present(data)
        if info is None:
            if not os.access(parent, os.W_OK | os.X_OK, effective_ids=True):
                raise DestinationError(parent, "destination_parent_unwritable", "destination parent must be writable")
            return True
        if not stat.S_ISDIR(info.st_mode):
            raise DestinationError(data, "destination_occupied", "candidate path must be a directory")
        if info.st_uid != os.geteuid() or info.st_mode & 0o077 or not os.access(
                data, os.R_OK | os.W_OK | os.X_OK, effective_ids=True):
            raise DestinationError(data, "destination_not_private", "candidate storage requires an operator-owned private directory")
        marker = data / CANDIDATE_MARKER
        owner = lstat_if_present(marker)
        if owner is None:
            if next(data.iterdir(), None) is not None:
                raise DestinationError(data, "destination_occupied", "first start requires an empty dedicated candidate directory")
            return True
        if (not stat.S_ISREG(owner.st_mode) or owner.st_nlink != 1
                or read_file(marker, 8192) != marker_bytes(conf)):
            raise DestinationError(marker, "destination_marker_mismatch", "candidate directory belongs to another cutover")
        for suffix in ("", "-wal", "-shm", "-journal", ".writer-lock"):
            path = data / ("broker.sqlite" + suffix)
            artifact = lstat_if_present(path)
            if artifact is not None and (not stat.S_ISREG(artifact.st_mode) or artifact.st_nlink != 1):
                raise DestinationError(path, "destination_linked_or_non_regular", "candidate database files must be regular files with one link")
        return False
    except DestinationError:
        raise
    except (OSError, ValueError) as exc:
        raise DestinationError(data, "destination_unreadable", str(exc)) from exc


def compile_plan(conf, mapping, data, stack):
    report = Report()
    values, origins = listener(conf, report)
    names = bindings(mapping, report)
    rules = acl(Path(values["acl_file"]), names, report) if "acl_file" in values else {}
    if "persistence_location" in values:
        original = Path(values["persistence_location"])
        if data.is_relative_to(original) or original.is_relative_to(data):
            report.reject(conf, origins["persistence_location"], "persistence_location", "separate storage directories required")
    try:
        validate_destination(data, conf)
    except DestinationError as exc:
        report.rows.append(exc.diagnostic())
    descriptors, principals = {}, []
    for key in ("cafile", "certfile", "keyfile"):
        if key in values:
            try:
                material = read_file(values[key], 48 * 1024)
                descriptors[key] = private_input(stack, material)
                if key != "keyfile":
                    report.hashes[values[key]] = digest(material)
            except SealingUnavailable as exc:
                report.row(conf, origins[key], "sealing", "blocking", str(exc), code="sealing_unavailable")
                break
            except (OSError, ValueError) as exc:
                report.reject(conf, origins[key], key, str(exc))
    if not report.blocked():
        ca = descriptors["cafile"]
        for cn, (cert, line, ids) in names.items():
            try:
                pem = read_file(cert, 8192)
                if pem.count(b"-----BEGIN CERTIFICATE-----") != 1:
                    raise ValueError("one client leaf required")
                openssl("verify", "-no-CApath", "-no-CAstore", "-purpose", "sslclient", "-CAfile", fdpath(ca), data=pem, fds=(ca,))
                subject = openssl("x509", "-noout", "-subject", "-nameopt", "RFC2253", data=pem).decode().strip()
                if subject != "subject=CN=" + cn:
                    raise ValueError("client certificate subject must be exactly the declared CN in this profile")
                pin = digest(openssl("x509", "-outform", "DER", data=pem))
                report.hashes[str(cert)] = digest(pem)
                principals.append(dict(name=cn, client_ids=ids, certificates=[pin], **rules[cn]))
            except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
                report.reject(mapping, line, "client", str(exc))
    policy = dict(version=1, principals=principals)
    if not report.blocked():
        try:
            ca = descriptors["cafile"]
            server = descriptors["certfile"]
            openssl("verify", "-no-CApath", "-no-CAstore", "-purpose", "sslserver", "-verify_ip", values["listener"].rsplit(":", 1)[0],
                    "-CAfile", fdpath(ca), fdpath(server), data=b"", fds=(ca, server))
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            report.reject(conf, origins["certfile"], "certfile", str(exc))
    if not report.blocked():
        try:
            descriptors["identities"] = private_input(stack, encoding(policy))
        except SealingUnavailable as exc:
            report.row(mapping, None, "sealing", "blocking", str(exc), code="sealing_unavailable")
    if not report.blocked():
        native = [descriptors[k] for k in ("certfile", "keyfile", "cafile", "identities")]
        check = subprocess.run([BINARY, "check-security", *map(fdpath, native)], pass_fds=native,
                               capture_output=True, timeout=10)
        if check.returncode:
            reason = check.stderr.decode(errors="replace").strip()
            key = "keyfile" if "key" in reason else "acl_file"
            report.reject(conf, origins[key], key, reason)
    with BINARY.open("rb") as executable:
        binary_sha256 = hashlib.file_digest(executable, "sha256").hexdigest()
    plan = dict(format="mqttiumd.mosquitto-cutover.v1", inputs={"config": str(conf), "bindings": str(mapping),
                "data": str(data)}, source_version="2.0.21", binary_sha256=binary_sha256,
                source_hashes=report.hashes, effective={"source": values, "identities": policy,
                "database": str(data / "broker.sqlite"), "journal_mode": "WAL", "synchronous": "FULL"},
                diagnostics=report.rows, status="blocked" if report.blocked() else "compatible")
    plan["plan_sha256"] = digest(encoding(plan))
    return plan, descriptors


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="operation", required=True)
    check = sub.add_parser("check", help="write the effective plan to stdout without opening a listener or database")
    for name in ("config", "bindings", "data"):
        check.add_argument(name, type=Path)
    start = sub.add_parser("start", help="recheck the plan and run the packaged broker in the foreground")
    start.add_argument("plan", type=Path)
    args = cli.parse_args()
    os.umask(0o077)
    with ExitStack() as stack:
        saved = None
        if args.operation == "start":
            saved = json.loads(read_file(args.plan, 4 << 20))
            inputs = saved["inputs"]
            paths = [Path(inputs[k]) for k in ("config", "bindings", "data")]
        else:
            paths = [args.config, args.bindings, args.data]
        conf, mapping, data = (path.resolve() for path in paths)
        plan, fds = compile_plan(conf, mapping, data, stack)
        if args.operation == "check":
            print(json.dumps(plan, indent=2))
            return 0 if plan["status"] == "compatible" else 2
        if saved != plan or plan["status"] != "compatible":
            print(json.dumps(dict(error="blocked_or_changed_plan", diagnostics=plan["diagnostics"])), file=sys.stderr)
            return 2
        try:
            data.mkdir(mode=0o700)
        except FileExistsError:
            pass
        if validate_destination(data, conf):
            with (data / CANDIDATE_MARKER).open("xb") as output:
                output.write(marker_bytes(conf))
                output.flush()
                os.fsync(output.fileno())
        db = data / "broker.sqlite"
        log = Path(tempfile.mkdtemp(prefix="run.", dir=data)) / "events.jsonl"
        for fd in fds.values():
            os.set_inheritable(fd, True)
        os.execv(BINARY, [str(BINARY), "--db", str(db), "--events", str(log),
                          "--listen", plan["effective"]["source"]["listener"],
                          "--tls-cert", fdpath(fds["certfile"]), "--tls-key", fdpath(fds["keyfile"]),
                          "--client-ca", fdpath(fds["cafile"]), "--identities", fdpath(fds["identities"])])


if __name__ == "__main__":
    try:
        sys.exit(main())
    except DestinationError as error:
        print(json.dumps(dict(error="destination_blocked", diagnostics=[error.diagnostic()])), file=sys.stderr)
        sys.exit(2)
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        sys.exit(f"cutover refused: {error}")
