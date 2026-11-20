#!/usr/bin/env python3
"""Run a paced loopback mTLS comparison using one libmosquitto client for both brokers."""
import argparse
from collections import Counter
from contextlib import contextmanager
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time


TOOLS = Path(__file__).resolve().parent
REFERENCE_SHA = "7ad5e84caeb8d2bb6ed0c04614b2a7042def961af82d87f688ba33db857b899d"
NS = 1_000_000_000
CGROUP = Path("/sys/fs/cgroup")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def cpus_text(cpus):
    return ",".join(map(str, cpus))


def counter(path):
    if not path.exists():
        return {}
    return dict((name, int(n)) for name, n in map(str.split, path.read_text().splitlines()))


def current_group():
    # Without a cgroup v2 hierarchy the quota and throttling counters are
    # absent: the run still records what it can observe.
    entry = next((line.removeprefix("0::") for line in Path("/proc/self/cgroup").read_text().splitlines()
                  if line.startswith("0::")), None)
    if entry is None or not (CGROUP / "cgroup.controllers").is_file():
        return CGROUP / ".absent-cgroup-v2"
    return CGROUP / entry.lstrip("/")


def machine(scratch):
    group = current_group()
    ancestors = []
    while group.is_relative_to(CGROUP) and group.exists():
        quota = group / "cpu.max"
        if quota.is_file():
            ancestors.append({"path": str(group), "cpu_max": quota.read_text().strip()})
        group = group.parent
    cpuset = current_group() / "cpuset.cpus.effective"
    return {
        "uname": platform.uname()._asdict(),
        "lscpu": subprocess.check_output(["lscpu", "-J"], text=True, env=dict(os.environ, LC_ALL="C")),
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "cpu_limits": ancestors,
        "cpuset": (cpuset.read_text().strip() if cpuset.is_file() else "unavailable"),
        "cgroup": Path("/proc/self/cgroup").read_text().strip(),
        "storage": subprocess.check_output(["findmnt", "-T", scratch, "-n", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"], text=True).strip(),
        "clock": "CLOCK_MONOTONIC",
    }


def compile_driver(reference, out):
    source = TOOLS / "load_client.c"
    library = reference / "lib/libmosquitto.so.1"
    archive = reference.parent / "mosquitto-2.0.21.tar.gz"
    if digest(archive) != REFERENCE_SHA:
        raise ValueError("unexpected Mosquitto source archive digest")
    driver = out / "load-client"
    command = ["cc", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror",
               "-I", str(reference / "include"), str(source), str(library),
               "-Wl,-rpath," + str(library.parent), "-o", str(driver)]
    subprocess.run(command, check=True)
    return driver, {"argv": command, "source_sha256": digest(source),
                    "binary_sha256": digest(driver), "library_sha256": digest(library),
                    "compiler": subprocess.check_output(["cc", "--version"], text=True).splitlines()[0]}


def provision(root):
    pki = root / "pki"
    subprocess.run([sys.executable, TOOLS.parent / "preview/demo-pki.py", pki], check=True,
                   stdout=subprocess.DEVNULL)
    policy = pki / "identities.json"
    current = json.loads(policy.read_text())
    for identity, prefix in zip(current["principals"], ("sensor", "dashboard"), strict=True):
        identity["client_ids"] = [f"{prefix}-{n}" for n in range(64)]
        identity["publish"] = ["atelier/load"] if prefix == "sensor" else []
        identity["subscribe"] = ["atelier/load"] if prefix == "dashboard" else []
    write_json(policy, current)
    return pki


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


# Extra options of the mqttiumd executable, from --broker-option.
BROKER_OPTIONS = []


def broker_command(kind, binary, directory, pki, port):
    listen = f"127.0.0.1:{port}"
    if kind != "mosquitto":
        return [binary, "--db", directory / "broker.sqlite", "--listen", listen,
                "--events", directory / "events.jsonl", "--tls-cert", pki / "server.crt",
                "--tls-key", pki / "server.key", "--client-ca", pki / "ca.crt",
                "--identities", pki / "identities.json", *BROKER_OPTIONS]
    acl = directory / "acl"
    acl.write_text("user sensor\ntopic write atelier/load/#\nuser dashboard\ntopic read atelier/load/#\n")
    config = directory / "mosquitto.conf"
    # Started by root, Mosquitto drops to another user that cannot read the
    # run's private files; a host that only has root keeps it.
    identity = ["user root"] if os.geteuid() == 0 else []
    config.write_text("\n".join([
        *identity,
        f"listener {port} 127.0.0.1", "allow_anonymous false", "require_certificate true",
        "use_identity_as_username true", "tls_version tlsv1.3",
        f"cafile {pki}/ca.crt", f"certfile {pki}/server.crt", f"keyfile {pki}/server.key",
        f"acl_file {acl}", "persistence true", f"persistence_location {directory}/",
        "autosave_interval 3600", "log_timestamp false", "log_type information",
        "log_type notice", "log_type warning", "log_type error", "",
    ]))
    return [binary, "-c", config]


@contextmanager
def process(argv, log, cpus):
    with log.open("xb") as output:
        child = subprocess.Popen(["taskset", "-c", cpus_text(cpus), *map(str, argv)],
                                 stdout=output, stderr=subprocess.STDOUT)
        try:
            yield child
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


def events(path):
    with path.open("rb") as source:
        for line in source:
            if line.endswith(b"\n"):
                yield json.loads(line)


def broker_observations(directory):
    kinds, dispositions = Counter(), Counter()
    terminal = {"connection_closed", "effect_retained", "frame_failed", "startup_or_runtime_refused"}
    for name in ("events.jsonl.previous", "events.jsonl"):
        path = directory / name
        if not path.is_file():
            continue
        for row in events(path):
            kind = row["kind"]
            kinds[kind] += 1
            if kind in terminal:
                code = row.get("details", {}).get("code", "unspecified")
                dispositions[f"{kind}:{code}"] += 1
    return kinds, dispositions


def ready(child, directory, kind):
    deadline = time.monotonic() + 8
    while child.poll() is None and time.monotonic() < deadline:
        if kind != "mosquitto":
            path = directory / "events.jsonl"
            if path.exists():
                rows = {row["kind"]: row for row in events(path) if "details" in row}
                if "ready" in rows:
                    if rows["sqlite_configuration"]["details"]["synchronous"] != 2:
                        raise ValueError("broker did not report synchronous FULL")
                    return {"pid": rows["ready"]["pid"],
                            **{key: rows[key]["details"] for key in ("ready", "sqlite_configuration", "tls_profile")}}
        elif "mosquitto version 2.0.21 running" in (directory / "broker.log").read_text():
            return {"pid": child.pid, "version": "2.0.21", "persistence": True, "autosave_interval_s": 3600}
        time.sleep(0.01)
    raise RuntimeError((directory / "broker.log").read_text())


def proc_sample(pid):
    root = Path(f"/proc/{pid}")
    threads = {}
    try:
        status = root.joinpath("status").read_text().splitlines()
        rss = next(int(row.split()[1]) for row in status if row.startswith("VmRSS:"))
        for entry in root.joinpath("task").iterdir():
            try:
                stat = entry.joinpath("stat").read_text()
                name = stat[stat.index("(") + 1:stat.rindex(")")]
                fields = stat[stat.rindex(")") + 2:].split()
                cpu, wait, _ = map(int, entry.joinpath("schedstat").read_text().split())
                threads[entry.name] = {"name": name, "state": fields[0], "user_ticks": int(fields[11]),
                                       "system_ticks": int(fields[12]), "cpu_ns": cpu, "runqueue_ns": wait,
                                       "wchan": entry.joinpath("wchan").read_text().strip()}
            except (FileNotFoundError, ProcessLookupError):
                continue
        return {"rss_kib": rss, "threads": threads}
    except (FileNotFoundError, ProcessLookupError, StopIteration):
        return None


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return None
    return {f"p{percent}": ordered[math.ceil(percent * len(ordered) / 100) - 1] / 1e6
            for percent in (50, 95, 99)}


def summarize_events(raw, window, pairs, qos):
    per_pair = [Counter() for _ in range(pairs)]
    ack, delivery, delay = [], [], []
    reasons, local = Counter(), Counter()
    total = Counter()
    with raw.open() as source:
        for line in csv.DictReader(source):
            event = {name: int(value) for name, value in line.items()}
            pair = per_pair[event["pair"]]
            pair["scheduled"] += 1
            pair["duplicates"] += max(0, event["copies"] - 1)
            sent, received, acked = (event[name] for name in ("submitted_ns", "received_ns", "ack_ns"))
            if event["send_rc"]:
                local[event["send_rc"]] += 1
            elif sent:
                pair["submitted"] += 1
                delay.append(sent - event["scheduled_ns"])
                if qos and not acked:
                    pair["ack_missing"] += 1
            if received:
                pair["received"] += 1
                if received <= window["end_ns"]:
                    pair["in_window"] += 1
                delivery.append(received - sent)
            if acked:
                reasons[event["ack_reason"]] += 1
                if event["ack_reason"] < 128:
                    ack.append(acked - sent)
                if not received and event["ack_reason"] == 0:
                    pair["positive_ack_without_delivery"] += 1
            if not received:
                pair["missing"] += 1
    for pair in per_pair:
        total.update(pair)
    counts = {key: total[key] for key in ("scheduled", "submitted", "received", "in_window", "missing", "duplicates",
                                         "ack_missing", "positive_ack_without_delivery")}
    counts["schedule_missed"] = counts["scheduled"] - counts["submitted"] - sum(local.values())
    duration = (window["end_ns"] - window["start_ns"]) / NS
    return {"counts": counts, "puback_reasons": dict(reasons), "local_refusals": dict(local),
            "wire_mps": counts["in_window"] / duration, "delivered_mps_including_500ms_drain": counts["received"] / duration,
            "submit_schedule_lag_ms": distribution(delay), "submit_to_subscriber_ms": distribution(delivery),
            "submit_to_positive_puback_ms": distribution(ack), "per_pair": per_pair}


def summarize_process(samples, window, role):
    rows = [(s["at_ns"], s[role]) for s in samples if window["start_ns"] <= s["at_ns"] <= window["end_ns"] and s[role]]
    first, last, states = {}, {}, {}
    for at, sampled in rows:
        for tid, values in sampled["threads"].items():
            first.setdefault(tid, (at, values))
            last[tid] = (at, values)
            states.setdefault(tid, Counter())[values["wchan"]] += 1
    per_thread = {}
    for tid, (start, previous) in first.items():
        end, current = last[tid]
        elapsed = end - start
        per_thread[tid] = {"name": current["name"], "sample_ns": elapsed,
                           "cpu_ns": current["cpu_ns"] - previous["cpu_ns"],
                           "cpu_percent_one_core": (current["cpu_ns"] - previous["cpu_ns"]) * 100 / elapsed if elapsed else None,
                           "runqueue_ns": current["runqueue_ns"] - previous["runqueue_ns"],
                           "wchan_samples": states[tid]}
    return {"peak_sampled_rss_kib": max((row["rss_kib"] for _, row in rows), default=None),
            "threads": per_thread}


EXPIRING_TOPIC = "atelier/load/deadline"


def retained_present(directory):
    db = directory / "broker.sqlite"
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as reader:
        return reader.execute("SELECT 1 FROM retained WHERE topic=?", (EXPIRING_TOPIC.encode(),)).fetchone() is not None


def expiry_probe(args, directory, pki, port, cpus):
    publisher = args.reference / "client/mosquitto_pub"
    command = [publisher, "-h", "127.0.0.1", "-p", port, "-i", "sensor-0", "-V", "mqttv5",
               "--tls-version", "tlsv1.3", "--cafile", pki / "ca.crt", "--cert", pki / "sensor.crt",
               "--key", pki / "sensor.key", "-d", "-t", EXPIRING_TOPIC, "-m", "expiry-witness",
               "-q", 1, "-r", "-D", "publish", "message-expiry-interval", 2]
    submitted = time.monotonic_ns()
    result = subprocess.run(["taskset", "-c", cpus_text(cpus), *map(str, command)], text=True,
                            env=dict(os.environ, LD_LIBRARY_PATH=str(args.reference / "lib")),
                            capture_output=True, check=True, timeout=10)
    if "PUBACK" not in result.stdout or not retained_present(directory):
        raise ValueError("retained expiry witness was not admitted")
    return {"topic": EXPIRING_TOPIC, "property_seconds": 2, "publish_invoked_ns": submitted,
            "present_after_puback_ns": time.monotonic_ns(), "publish_stdout": result.stdout}


def run_case(args, root, pki, driver, kind, binary, cores, pairs, qos, rate, index, affinity):
    label = f"{kind}-q{qos}-c{cores}-p{pairs}-r{rate}-{index:02d}"
    result = args.out / label
    result.mkdir()
    broker_cpus = affinity[:cores]
    generator_cpus = affinity[-1:]
    port = free_port()
    with tempfile.TemporaryDirectory(dir=root, prefix=label + "-") as scratch:
        directory = Path(scratch)
        broker = broker_command(kind, binary, directory, pki, port)
        trace = directory / "main-sleep.strace"
        if args.trace_main:
            broker = ["strace", "--kill-on-exit", "-e", "trace=clock_nanosleep", "-ttt", "-T", "-o", trace, *broker]
        raw = directory / "messages.csv"
        gen = [driver, port, pki, pairs, qos, rate, args.duration_ms, args.payload_bytes, raw]
        samples = []
        with process(broker, directory / "broker.log", broker_cpus) as server:
            readiness = ready(server, directory, kind)
            broker_pid = readiness["pid"]
            actual_broker_cpus = sorted(os.sched_getaffinity(broker_pid))
            witness = expiry_probe(args, directory, pki, port, generator_cpus) if args.expiry_witness else None
            before = counter(current_group() / "cpu.stat")
            realtime_offset = time.time_ns() - time.monotonic_ns()
            with process(gen, directory / "driver.log", generator_cpus) as client:
                deadline = time.monotonic() + args.duration_ms / 1000 + 15
                while client.poll() is None and time.monotonic() < deadline:
                    samples.append({"at_ns": time.monotonic_ns(), "broker": proc_sample(broker_pid),
                                    "generator": proc_sample(client.pid)})
                    if witness and "first_absent_ns" not in witness and not retained_present(directory):
                        witness["first_absent_ns"] = time.monotonic_ns()
                    time.sleep(0.05)
                if client.poll() != 0:
                    shutil.copyfile(directory / "driver.log", result / "failed-driver.log")
                    # The broker's own account of a failed run is the first
                    # thing its diagnosis needs.
                    if (directory / "events.jsonl").exists():
                        shutil.copyfile(directory / "events.jsonl", result / "failed-events.jsonl")
                    raise RuntimeError(f"{label}: generator status {client.poll()}\n"
                                       + (directory / "driver.log").read_text())
            after = counter(current_group() / "cpu.stat")
            if args.trace_main:
                os.kill(broker_pid, signal.SIGTERM)
                server.wait(timeout=10)
        driver_lines = (directory / "driver.log").read_text().splitlines()
        window, report = map(json.loads, driver_lines)
        if report["invalid"] or report["unmapped_ack"]:
            raise ValueError(f"invalid driver observations in {label}: {report}")
        configured = {"broker": kind, "qos": qos, "pairs": pairs, "cores": cores, "target_mps": rate, "repeat": index,
                      "broker_pid": broker_pid, "generator_pid": client.pid,
                      "broker_affinity": actual_broker_cpus, "generator_launch_affinity": generator_cpus,
                      "generator_shares_cpu_set": bool(set(generator_cpus) & set(broker_cpus)), "port": port,
                      "broker_exit": server.returncode}
        result_record = {**configured, "window": window, "driver": report, "service": readiness,
                         "trace_wall_minus_monotonic_ns": realtime_offset if args.trace_main else None,
                         "cgroup_delta": {key: n - before.get(key, 0) for key, n in after.items()},
                         "brokercpu": summarize_process(samples, window, "broker"),
                         "generatorcpu": summarize_process(samples, window, "generator"),
                         **summarize_events(raw, window, pairs, qos)}
        if witness:
            absent = witness.get("first_absent_ns")
            witness["expired_inside_send_window"] = absent is not None and window["start_ns"] <= absent <= window["end_ns"]
            result_record["expiry_witness"] = witness
        with raw.open("rb") as source, gzip.open(result / "messages.csv.gz", "wb", compresslevel=3) as archive:
            shutil.copyfileobj(source, archive)
        with gzip.open(result / "process-samples.jsonl.gz", "wt", compresslevel=3) as archive:
            for sample in samples:
                archive.write(json.dumps(sample) + "\n")
        shutil.copyfile(directory / "broker.log", result / "broker.log")
        if args.trace_main:
            shutil.copyfile(trace, result / trace.name)
        if kind == "mosquitto":
            shutil.copyfile(directory / "mosquitto.conf", result / "mosquitto.conf")
            shutil.copyfile(directory / "acl", result / "acl")
        else:
            result_record["event_kinds"], result_record["broker_dispositions"] = broker_observations(directory)
        write_json(result / "result.json", result_record)
        print(label, f"{result_record['wire_mps']:.1f} received/s", result_record["counts"], flush=True)
    return result_record


def integers(value):
    return sorted(set(map(int, value.split(","))))


def knees(rows):
    # Observation rule for repeat selection only. The full sweep is retained.
    viable = [row for row in rows if row["counts"]["received"] * 100 >= row["counts"]["scheduled"] * 99]
    good = viable[-1]["target_mps"] if viable else -1
    return sorted({row["target_mps"] for row in viable[-1:] + [
        row for row in rows if row["target_mps"] > good][:1]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mqttiumd", type=Path, required=True, help="release executable")
    parser.add_argument("--source", required=True, help="source SHA of the release executable")
    parser.add_argument("--reference", type=Path, required=True, help="built Mosquitto 2.0.21 source directory")
    parser.add_argument("--out", type=Path, required=True, help="new directory for measurements")
    parser.add_argument("--scratch", type=Path, required=True, help="existing filesystem for the temporary stores and logs")
    parser.add_argument("--cores", type=integers, default=[1, 2, 4])
    parser.add_argument("--pairs", type=integers, default=[1, 4, 8])
    parser.add_argument("--broker-option", action="append", default=[],
                        help="one more argument of the mqttiumd executable; repeat for a flag and its value")
    parser.add_argument("--qos", type=integers, default=[0, 1])
    parser.add_argument("--rates-q0", type=integers, default=[500, 900, 1500, 6000, 24000, 48000, 96000, 150000])
    parser.add_argument("--rates-q1", type=integers, default=[100, 300, 500, 800, 1500, 3000, 6000, 12000, 48000])
    parser.add_argument("--duration-ms", type=int, default=2200)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=3, help="total runs of each point at the knee")
    parser.add_argument("--only-broker", choices=("mqttiumd", "mosquitto"))
    parser.add_argument("--candidate", type=Path, help="compare the baseline and a second mqttiumd executable on alternating cases")
    parser.add_argument("--candidate-source", help="source identity of the second executable")
    parser.add_argument("--trace-main", action="store_true", help="measure only mqttiumd's main thread sleeps under strace")
    parser.add_argument("--expiry-witness", action="store_true", help="separate run inspecting retained expiry during load")
    args = parser.parse_args()
    if args.candidate and (args.only_broker or not args.candidate_source):
        parser.error("--candidate requires --candidate-source and a paired run")
    if args.trace_main and args.only_broker != "mqttiumd":
        parser.error("--trace-main requires --only-broker mqttiumd")
    if args.expiry_witness and (not (args.candidate or args.only_broker == "mqttiumd") or args.duration_ms < 3000):
        parser.error("--expiry-witness needs a mqttiumd-only trial of at least three seconds")
    BROKER_OPTIONS.extend(args.broker_option)
    os.umask(0o077)
    for name in ("mqttiumd", "reference", "out", "scratch"):
        setattr(args, name, getattr(args, name).resolve())
    affinity = sorted(os.sched_getaffinity(0))
    host = machine(args.scratch)
    limits = [item["cpu_max"].split() for item in host["cpu_limits"]]
    cpu_cap = min([len(affinity)] + [int(quota) / int(period) for quota, period in limits if quota != "max"])
    effective = [n for n in args.cores if 0 < n <= cpu_cap]
    if (not effective or not set(args.qos) <= {0, 1} or not all(1 <= p <= 64 for p in args.pairs)
            or args.repeats < 1):
        parser.error("invalid matrix for the available CPU budget and 128-connection limit")
    args.out.mkdir(mode=0o700)
    driver, client = compile_driver(args.reference, args.out)
    binaries = {"mqttiumd": args.mqttiumd}
    if args.candidate:
        binaries["candidate"] = args.candidate.resolve()
    else:
        binaries["mosquitto"] = args.reference / "src/mosquitto"
    broker_info = {k: {"path": str(binary), "sha256": digest(binary)} for k, binary in binaries.items()}
    broker_info["mqttiumd"]["source"] = args.source
    if args.candidate:
        broker_info["candidate"]["source"] = args.candidate_source
    else:
        broker_info["mosquitto"].update(version="2.0.21", source_archive_sha256=REFERENCE_SHA)
    choices = {(c, 4 if 4 in args.pairs else args.pairs[0]) for c in effective}
    choices.update((2 if 2 in effective else effective[0], p) for p in args.pairs)
    write_json(args.out / "machine.json", {"machine": host, "cpu_cap_shared": cpu_cap, "tested_affinity_counts": effective,
                                          "driver": client, "brokers": broker_info, "axes": sorted(choices),
                                          "command": sys.argv, "harness_sha256": digest(Path(__file__))})
    with tempfile.TemporaryDirectory(dir=args.scratch, prefix="mqttiumd-load-") as temporary:
        root = Path(temporary)
        pki = provision(root)
        results = []
        for qos in args.qos:
            rates = args.rates_q0 if qos == 0 else args.rates_q1
            for cores, pairs in sorted(choices):
                baselines = {kind: [] for kind in binaries if args.only_broker in (None, kind)}
                for rate in rates:
                    for kind, initial in baselines.items():
                        row = run_case(args, root, pki, driver, kind, binaries[kind], cores, pairs, qos, rate, 0, affinity)
                        initial.append(row)
                        results.append(row)
                if args.candidate:
                    # Re-measure the same points in both binaries, reversing
                    # order on alternate repetitions to reduce drift bias.
                    repeats = dict.fromkeys(baselines, rates)
                else:
                    repeats = {kind: knees(initial) for kind, initial in baselines.items()}
                for i in range(1, args.repeats):
                    kinds = list(repeats) if i % 2 == 0 else list(reversed(repeats))
                    for rate in sorted({rate for points in repeats.values() for rate in points}):
                        for kind in kinds:
                            if rate in repeats[kind]:
                                results.append(run_case(args, root, pki, driver, kind, binaries[kind],
                                                        cores, pairs, qos, rate, i, affinity))
        write_json(args.out / "matrix.json", results)


if __name__ == "__main__":
    main()
