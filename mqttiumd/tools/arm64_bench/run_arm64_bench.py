#!/usr/bin/env python3
"""Reproducible ARM64 benchmark runner: mqttiumd vs Mosquitto reference."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


def mosquitto_identity(binary: Path | str) -> dict:
    path = Path(binary).resolve()
    help_text = subprocess.run([str(path), "-h"], capture_output=True, text=True, check=True).stdout
    version = re.search(r"^mosquitto version (\S+)\s*$", help_text, re.MULTILINE)
    if version is None:
        raise ValueError(f"cannot read Mosquitto version from {path}")
    with path.open("rb") as source:
        return {"version": version.group(1), "sha256": hashlib.file_digest(source, "sha256").hexdigest()}


def process_cpu_seconds(pid: int) -> float:
    """User and system CPU seconds of a live process, from /proc."""
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def children_cpu_seconds() -> float:
    """User and system CPU seconds of the children this process waited for."""
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ensure_driver(build_dir: Path | None = None) -> Path:
    src = HERE / "arm64_bench.c"
    out_dir = build_dir or Path(tempfile.gettempdir()) / "mqttiumd-arm64-bench"
    out_dir.mkdir(parents=True, exist_ok=True)
    exe = out_dir / "arm64_bench"
    if not exe.exists() or exe.stat().st_mtime < src.stat().st_mtime:
        subprocess.run(
            ["cc", "-O2", "-Wall", "-Wextra", str(src), "-lssl", "-lcrypto", "-o", str(exe)],
            check=True,
        )
    return exe


def make_pki(root: Path, max_pairs: int) -> Path:
    pki = root / "pki"
    subprocess.run(
        [sys.executable, str(REPO / "preview/demo-pki.py"), str(pki)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    pol = pki / "identities.json"
    data = json.loads(pol.read_text())
    for ident, prefix in zip(data["principals"], ("sensor", "dashboard"), strict=True):
        ident["client_ids"] = [f"{prefix}-{n}" for n in range(max_pairs)]
        ident["publish"] = ["atelier/load"] if prefix == "sensor" else []
        ident["subscribe"] = ["atelier/load"] if prefix == "dashboard" else []
    pol.write_text(json.dumps(data, indent=2) + "\n")
    return pki


def wait_port(port: int, proc: subprocess.Popen, log_path: Path) -> None:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"broker exited {proc.returncode}:\n{log_path.read_text()}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError(f"broker port timeout:\n{log_path.read_text()}")


def backing_info(path: Path) -> dict:
    """Inspect real mount/filesystem backing for DB, WAL, and .mlog paths."""
    out = subprocess.check_output(
        ["findmnt", "-n", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-T", str(path)],
        text=True,
    ).strip()
    parts = out.split(None, 3)
    return {
        "path": str(path),
        "mount": parts[0] if len(parts) > 0 else "",
        "source": parts[1] if len(parts) > 1 else "",
        "fstype": parts[2] if len(parts) > 2 else "",
        "options": parts[3] if len(parts) > 3 else "",
    }


def run_one(
    driver: Path,
    kind: str,
    binary: Path | str,
    extra_args: list[str],
    pki: Path | None,
    tls: bool,
    pairs: int,
    qos: int,
    rate: int,
    duration_ms: int,
    payload_bytes: int,
    broker_cpus: str,
    gen_cpus: str,
    work_dir: str = "/var/tmp",
) -> dict:
    port = free_port()
    with tempfile.TemporaryDirectory(dir=work_dir, prefix=f"bench-{kind}-") as td:
        d = Path(td)
        log = d / "broker.log"
        db_path = d / "broker.sqlite"
        wal_path = d / "broker.sqlite-wal"
        mlog_path = d / "broker.sqlite.mlog"
        config_text = None
        if kind == "mosquitto":
            conf = d / "mosquitto.conf"
            lines = [
                f"listener {port} 127.0.0.1",
                "max_connections -1",
                "max_inflight_messages 64",
                "max_queued_messages 0",
            ]
            if tls:
                acl = d / "acl"
                acl.write_text(
                    "user sensor\ntopic write atelier/load/#\nuser dashboard\ntopic read atelier/load/#\n"
                )
                lines += [
                    "allow_anonymous false",
                    "require_certificate true",
                    "use_identity_as_username true",
                    "tls_version tlsv1.3",
                    f"cafile {pki}/ca.crt",
                    f"certfile {pki}/server.crt",
                    f"keyfile {pki}/server.key",
                    f"acl_file {acl}",
                ]
            else:
                lines += ["allow_anonymous true"]
            if os.geteuid() == 0:
                # Run as root, Mosquitto would drop to `nobody` and could not
                # read the ACL and keys of this private directory.
                lines += ["user root"]
            lines += ["persistence false", "log_type error", ""]
            config_text = "\n".join(lines)
            conf.write_text(config_text)
            cmd = [str(binary), "-c", str(conf)]
        else:
            cmd = [
                str(binary),
                "--listen",
                f"127.0.0.1:{port}",
                "--events",
                str(d / "events.jsonl"),
                "--event-level",
                "ops",
            ]
            if "--no-db" not in extra_args:
                cmd += ["--db", str(db_path)]
            clean_extra = [a for a in extra_args if a != "--no-db"]
            if tls:
                cmd += [
                    "--tls-cert",
                    str(pki / "server.crt"),
                    "--tls-key",
                    str(pki / "server.key"),
                    "--client-ca",
                    str(pki / "ca.crt"),
                    "--identities",
                    str(pki / "identities.json"),
                ]
            cmd += clean_extra
        if broker_cpus:
            cmd = ["taskset", "-c", broker_cpus, *cmd]
        with log.open("wb") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT)
            try:
                if kind == "mosquitto":
                    wait_port(port, proc, log)
                else:
                    deadline = time.monotonic() + 8
                    ev = d / "events.jsonl"
                    while time.monotonic() < deadline:
                        if proc.poll() is not None:
                            raise RuntimeError(
                                f"mqttiumd exited {proc.returncode}:\n{log.read_text()}"
                            )
                        if ev.exists() and b'"ready"' in ev.read_bytes():
                            break
                        time.sleep(0.01)
                    else:
                        raise RuntimeError(f"mqttiumd ready timeout:\n{log.read_text()}")
                dcmd = [
                    str(driver),
                    str(port),
                    str(pki) if tls else "none",
                    str(pairs),
                    str(qos),
                    str(rate),
                    str(duration_ms),
                    str(payload_bytes),
                ]
                if gen_cpus:
                    dcmd = ["taskset", "-c", gen_cpus, *dcmd]
                broker_before = process_cpu_seconds(proc.pid)
                generator_before = children_cpu_seconds()
                res = subprocess.run(dcmd, capture_output=True, text=True, timeout=45)
                generator_cpu = children_cpu_seconds() - generator_before
                broker_cpu = process_cpu_seconds(proc.pid) - broker_before
                if res.returncode != 0:
                    raise RuntimeError(
                        f"driver failed ({res.returncode}): {res.stderr}\n{log.read_text()}"
                    )
                row = json.loads(res.stdout.strip())
                row["broker"] = kind
                # CPU while the driver ran, its drain included: the
                # broker's start and stop are not counted.
                row["broker_cpu_s"] = broker_cpu
                row["generator_cpu_s"] = generator_cpu
                row["work_dir_backing"] = backing_info(d)
                row["db_present"] = db_path.exists()
                row["db_bytes"] = db_path.stat().st_size if db_path.exists() else 0
                row["wal_present"] = wal_path.exists()
                row["wal_bytes"] = wal_path.stat().st_size if wal_path.exists() else 0
                row["mlog_present"] = mlog_path.exists()
                row["mlog_bytes"] = mlog_path.stat().st_size if mlog_path.exists() else 0
                row["cmd"] = cmd
                if config_text is not None:
                    row["config_text"] = config_text
                return row
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=str(REPO / "target/dist/mqttiumd"))
    ap.add_argument("--mosquitto", default="/usr/sbin/mosquitto")
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--broker-cpus", default="0,1,2")
    ap.add_argument("--gen-cpus", default="3")
    ap.add_argument("--duration-ms", type=int, default=2000)
    ap.add_argument("--payload", type=int, default=256)
    ap.add_argument("--extra", action="append", default=[])
    ap.add_argument("--only", choices=("mqttiumd", "mosquitto", "both"), default="both")
    ap.add_argument("--label", default="mqttiumd")
    ap.add_argument("--work-dir", default="/var/tmp")
    ap.add_argument(
        "--cases",
        default="4:0:60000,4:0:120000,32:0:60000,32:0:120000,4:1:3000,4:1:20000,32:1:3000,32:1:20000",
    )
    args = ap.parse_args()

    driver = ensure_driver()
    cases = [tuple(map(int, c.split(":"))) for c in args.cases.split(",")]
    max_pairs = max(p for p, _, _ in cases)
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="bench-pki-") as td:
        pki = make_pki(Path(td), max(max_pairs, 64)) if args.tls else None
        brokers = []
        if args.only in ("mqttiumd", "both"):
            brokers.append((args.label, args.binary, args.extra))
        if args.only in ("mosquitto", "both"):
            reference = mosquitto_identity(args.mosquitto)
            brokers.append(("mosquitto", args.mosquitto, []))
        results = []
        for pairs, qos, rate in cases:
            for kind, binary, extra in brokers:
                r = run_one(
                    driver,
                    kind,
                    binary,
                    extra,
                    pki,
                    args.tls,
                    pairs,
                    qos,
                    rate,
                    args.duration_ms,
                    args.payload,
                    args.broker_cpus,
                    args.gen_cpus,
                    work_dir=args.work_dir,
                )
                if kind == "mosquitto":
                    r["reference"] = reference
                results.append(r)
                loss = 100.0 * (1.0 - r["delivered_after_drain"] / max(1, r["submitted"]))
                print(
                    f"{kind:16s} tls={int(args.tls)} p={pairs:3d} q={qos} offer={rate:7d}/s -> "
                    f"deliv={r['delivered_mps']:8.0f}/s wire={r['wire_mps']:8.0f}/s "
                    f"sub={r['submitted']:6d} ack={r['acked_ok']:6d} ref={r['acked_refused']:5d} "
                    f"deliv_n={r['delivered_after_drain']:6d} ack_nodel={r['ack_no_deliv']:4d} "
                    f"loss={loss:5.1f}% disc={r['disconnects']} inv={r['invalid']} "
                    f"fs={r['work_dir_backing']['fstype']}({r['work_dir_backing']['source']}) "
                    f"lat_ms(p50/p99)={r['deliv_ms'][0]:.2f}/{r['deliv_ms'][2]:.2f}",
                    flush=True,
                )
        print(json.dumps(results))


if __name__ == "__main__":
    main()
