#!/usr/bin/env python3
"""Build and compare a fixed-polling broker and a bounded-batch variant."""

import argparse
import concurrent.futures
from collections import Counter
from contextlib import closing
import csv
import gzip
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import time

import run_broker_load as bench
from progression_witness import run as progression_witness


REPO = bench.TOOLS.parents[1]
PATCH = bench.TOOLS / "bounded-batch.mutant"
PAIRS = 4
PAYLOAD_BYTES = 256
IDLE_MS = 3000
DRAIN_MS = 2000


def mount(path):
    info = subprocess.check_output(
        ["findmnt", "--uniq", "-J", "-T", path, "-o", "TARGET,SOURCE,FSTYPE,MAJ:MIN,OPTIONS"], text=True
    )
    filesystem, = json.loads(info)["filesystems"]
    device = path.stat().st_dev
    if filesystem["maj:min"] != f"{os.major(device)}:{os.minor(device)}":
        raise ValueError(f"findmnt and stat disagree about {path}")
    return {"path": str(path), "device": device, **filesystem}


def build(base, work, output):
    source = work / "source"
    source.mkdir()
    with subprocess.Popen(["git", "-C", REPO, "archive", base, "mqttiumd"], stdout=subprocess.PIPE) as git:
        with tarfile.open(fileobj=git.stdout, mode="r|") as archive:
            archive.extractall(source, filter="data")
        if git.wait():
            raise RuntimeError("could not extract the fixed-polling source")
    broker = source / "mqttiumd"
    target = work / "target"
    command = ["cargo", "build", "--release", "--locked", "-p", "mqttiumd", "--no-default-features",
               "--target-dir", str(target)]
    env = dict(os.environ, CARGO_INCREMENTAL="0")
    subprocess.run(["git", "apply", "--check", PATCH], cwd=source, check=True)
    binaries = {}
    for label in ("A", "B"):
        if label == "B":
            subprocess.run(["git", "apply", PATCH], cwd=source, check=True)
        with (output / f"build-{label}.log").open("xb") as log:
            subprocess.run(command, cwd=broker, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        path = output / f"mqttiumd-{label}"
        shutil.copy2(target / "release/mqttiumd", path)
        binaries[label] = {"path": path.name, "sha256": bench.digest(path)}
    with (output / "batch-rust-witness.log").open("xb") as log:
        # The integrated regression exercises blocked connections, real
        # writer results and an old callback on the resumed connection.
        subprocess.run(["cargo", "test", "--release", "--locked", "-p", "mqttiumd",
                        "--target-dir", target, "--lib", "runtime::tests::", "--", "--test-threads=2"],
                       cwd=broker, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    return {"source_sha": base, "patch_sha256": bench.digest(PATCH), "build_command": command,
            "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=broker, text=True),
            "lockfile_sha256": bench.digest(broker / "Cargo.lock"), "binaries": binaries}


class Journal:
    """Hard-link rotated segments before the next rename can replace them."""

    def __init__(self, directory):
        self.directory = directory
        self.segments = {}

    def save(self, *, stopped=False):
        names = ("events.jsonl.previous", "events.jsonl") if stopped else ("events.jsonl.previous",)
        for name in names:
            source = self.directory / name
            try:
                if source.stat().st_ino in self.segments:
                    continue
                target = self.directory / f"saved-{len(self.segments):05d}.jsonl"
                os.link(source, target)
            except FileNotFoundError:
                continue
            inode = target.stat().st_ino
            if inode in self.segments:
                target.unlink()
            else:
                self.segments[inode] = target

    def finish(self, output, pid):
        self.save(stopped=True)
        segments = []
        for path in self.segments.values():
            with path.open("rb") as stream:
                line = stream.readline()
            if line:
                segments.append((int(json.loads(line)["seq"]), path))
        expected = 1
        counts, dispositions = Counter(), Counter()
        gaps = []
        closed = []
        batch = None
        first = last = None
        with gzip.open(output / "broker-events.jsonl.gz", "wb", compresslevel=3) as raw:
            for _, path in sorted(segments):
                with path.open("rb") as stream:
                    for line in stream:
                        raw.write(line)
                        event = json.loads(line)
                        sequence, kind = int(event["seq"]), event["kind"]
                        if sequence != expected or event["pid"] != pid:
                            if len(gaps) < 16:
                                gaps.append({"expected": expected, "actual": sequence, "pid": event["pid"]})
                        expected = sequence + 1
                        if first is None:
                            first = kind
                        last = kind
                        counts[kind] += 1
                        if kind in ("connection_closed", "effect_retained", "frame_failed", "startup_or_runtime_refused"):
                            dispositions[f"{kind}:{event['details'].get('code', 'unspecified')}"] += 1
                        if kind == "connection_closed" and "observed_monotonic_ns" in event["details"]:
                            closed.append(int(event["details"]["observed_monotonic_ns"]))
                        if kind == "continuation_batch":
                            batch = event["details"]
        return {"sequence_last": expected - 1, "segments": len(segments), "gaps": gaps,
                "complete": first == "starting" and last == "stopped" and not gaps,
                "kinds": dict(counts), "dispositions": dict(dispositions),
                "closed_monotonic_ns": closed, "continuation_batch": batch}


def pending_deliveries(db):
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=2)) as reader:
        rows = reader.execute("""
            SELECT p.topic, b.bytes, o.phase FROM obligations o
            JOIN publications p ON p.id=o.publication JOIN payloads b ON b.id=p.payload
        """).fetchall()
    pending = {}
    for topic, payload, phase in rows:
        if len(payload) != PAYLOAD_BYTES or payload[8:] != b"P" * (PAYLOAD_BYTES - 8):
            continue
        identity = int.from_bytes(payload[:8], "little")
        if topic == f"atelier/load/{identity % PAIRS}".encode():
            pending[identity] = {"topic": topic.decode(), "phase": phase}
    return pending


def submitted(row):
    return row["submitted_ns"] > 0 and row["send_rc"] == 0


def positive(row):
    return row["ack_ns"] > 0 and 0 <= row["ack_reason"] < 128


def summarize(rows, begin, end, qos, pending):
    local = Counter(str(row["send_rc"]) for row in rows if row["send_rc"])
    mqtt = Counter(str(row["ack_reason"]) for row in rows if row["ack_ns"] and not positive(row))
    first_unexpected = []
    count = Counter()
    acknowledged, arrivals = [], []
    for row in rows:
        identity = row["id"]
        sent, received = submitted(row), row["received_ns"]
        ok = positive(row)
        count["scheduled"] += 1
        count["submitted"] += sent
        count["received"] += bool(received)
        count["in_window"] += bool(received and begin <= received < end)
        count["after_window"] += bool(received >= end)
        count["duplicates"] += max(0, row["copies"] - 1)
        count["positive_acks"] += ok
        count["missing_acks"] += bool(qos and sent and not row["ack_ns"])
        count["positive_without_delivery"] += bool(ok and not received)
        count["positive_still_pending"] += bool(ok and not received and identity in pending)
        count["positive_without_explanation"] += bool(ok and not received and identity not in pending)
        if received:
            arrivals.append(row)
        if ok:
            acknowledged.append(row)
        if (not sent or not received or (qos and not ok)) and len(first_unexpected) < 8:
            first_unexpected.append(row | {"pending_after_drain": pending.get(identity)})
    times = sorted(row["received_ns"] for row in arrivals if begin <= row["received_ns"] < end)
    boundaries = [begin, *times, end]
    return {"counts": dict(count), "local_refusals": dict(local), "mqtt_refusals": dict(mqtt),
            "latency_ms": {
                "scheduled_to_delivery": bench.distribution([r["received_ns"] - r["scheduled_ns"] for r in arrivals]),
                "submitted_to_delivery": bench.distribution([r["received_ns"] - r["submitted_ns"] for r in arrivals]),
                "scheduled_to_puback": bench.distribution([r["ack_ns"] - r["scheduled_ns"] for r in acknowledged]),
                "submitted_to_puback": bench.distribution([r["ack_ns"] - r["submitted_ns"] for r in acknowledged]),
            },
            "longest_receive_gap_ms": max(b-a for a, b in zip(boundaries, boundaries[1:])) / 1e6,
            "first_unexpected_slots": first_unexpected}


def settled(rows, switch, end):
    older = [r for r in rows if r["scheduled_ns"] < switch and submitted(r)]
    if any(not r["ack_ns"] or (positive(r) and not r["received_ns"]) for r in older):
        return {"status": "not_observed", "reason": "unresolved_pretransition_work"}
    old_completion = max((max(r["ack_ns"], r["received_ns"]) for r in older), default=switch)
    buckets = {}
    for row in rows:
        if row["scheduled_ns"] >= switch:
            buckets.setdefault((row["scheduled_ns"] - switch) // bench.NS, []).append(row)
    consecutive = 0
    for second in range((end - switch) // bench.NS):
        start = switch + second * bench.NS
        bucket = buckets.get(second, [])
        # Allow one second after each measured bucket to observe its ACK and
        # delivery. Three complete buckets establish recovery.
        good = start >= old_completion and len(bucket) == 800 and all(
            submitted(r) and positive(r) and 0 < r["received_ns"] <= start + 2 * bench.NS
            and r["ack_ns"] <= start + 2 * bench.NS for r in bucket
        )
        consecutive = consecutive + 1 if good else 0
        if consecutive == 3:
            return {"status": "observed", "stable_run_started_after_s": second - 2,
                    "confirmed_after_s": second + 2}
    return {"status": "not_observed", "reason": "three_stable_seconds_not_seen"}


def observations(raw, window, qos, expected, pending, losses):
    with raw.open() as source:
        rows = [{name: int(value) for name, value in line.items()} for line in csv.DictReader(source)]
    if len(rows) != expected or any(
        row["id"] != i or row["pair"] != i % PAIRS for i, row in enumerate(rows)
    ):
        raise ValueError("client event IDs do not match the offered schedule")
    start, switch, end = (window[name] for name in ("start_ns", "switch_ns", "end_ns"))
    phases = [(start, end)] if switch == end else [(start, switch), (switch, end)]
    reports = []
    for begin, stop in phases:
        offered = [r for r in rows if begin <= r["scheduled_ns"] < stop]
        results = summarize(offered, begin, stop, qos, pending)
        results["per_pair"] = [
            summarize([r for r in offered if r["pair"] == pair], begin, stop, qos, pending)
            | {"connection_losses_in_trial": losses[pair]} for pair in range(PAIRS)
        ]
        reports.append(results | {"begin_ns": begin, "end_ns": stop})
    return {"phases": reports, "recovery": settled(rows, switch, end) if switch < end else None}


def preserve(source, destination):
    if source.exists():
        with source.open("rb") as src, gzip.open(destination, "wb", compresslevel=3) as dst:
            shutil.copyfileobj(src, dst)


def trial(binary, label, port, qos, rate, duration, recovery, proof, work, pki,
          driver, broker_cpus, generator_cpus, *, witness=False):
    proof.mkdir()
    result = {"trial": proof.name, "variant": label, "qos": qos, "rate": rate,
              "duration_ms": duration, "recovery_ms": recovery, "errors": []}
    with tempfile.TemporaryDirectory(dir=work, prefix="case-") as scratch:
        directory = Path(scratch)
        journal = Journal(directory)
        db = directory / "broker.sqlite"
        raw = directory / "messages.csv"
        broker_log = proof / "broker.log"
        driver_log = proof / "driver.log"
        samples = []
        broker = bench.broker_command("mqttiumd", binary, directory, pki, port)
        idle_ms = 500 if duration < 1000 else IDLE_MS
        generator = [driver, port, pki, PAIRS, qos, rate, duration, PAYLOAD_BYTES, raw,
                     idle_ms, DRAIN_MS, 800 if recovery else 0, recovery]
        server = None
        try:
            with bench.process(broker, broker_log, broker_cpus) as server:
                readiness = bench.ready(server, directory, "mqttiumd")
                result.update(service=readiness, broker_pid=server.pid,
                              broker_affinity=sorted(os.sched_getaffinity(server.pid)))
                config = readiness["sqlite_configuration"]
                if readiness["pid"] != server.pid or config["journal_mode"] != "wal" or config["synchronous"] != 2:
                    raise ValueError("writer did not reread WAL/FULL")
                tls = readiness["tls_profile"]
                if (tls["version"], tls["client_certificate"]) != ("TLS1.3", "required"):
                    raise ValueError("wrong TLS authority")
                with bench.process(generator, driver_log, generator_cpus) as client:
                    result["generator_pid"] = client.pid
                    deadline = time.monotonic() + (duration + recovery + idle_ms + DRAIN_MS) / 1000 + 25
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        window = future = None
                        while client.poll() is None and time.monotonic() < deadline:
                            journal.save()
                            if window is None:
                                line = driver_log.read_bytes().splitlines(keepends=True)[:1]
                                if line and line[0].endswith(b"\n"):
                                    window = json.loads(line[0])
                                    result.update(window=window, sqlite=mount(db),
                                                  wal=mount(db.with_suffix(".sqlite-wal")), sqlite_inode=db.stat().st_ino,
                                                  generator_affinity=sorted(os.sched_getaffinity(client.pid)))
                                    if (result["generator_affinity"] != generator_cpus
                                            or result["broker_affinity"] != broker_cpus):
                                        raise ValueError("unexpected CPU affinity")
                                    if result["sqlite"]["device"] != result["wal"]["device"]:
                                        raise ValueError("SQLite and its WAL are on different devices")
                                    if witness:
                                        future = pool.submit(progression_witness, directory, proof, pki, port, window)
                            samples.append({"at_ns": time.monotonic_ns(), "broker": bench.proc_sample(server.pid),
                                            "generator": bench.proc_sample(client.pid)})
                            time.sleep(0.075)
                        if client.poll() != 0:
                            raise ValueError(f"driver exit {client.poll()}")
                        if witness:
                            if future is None:
                                raise ValueError("QoS 0 flow did not start")
                            result["witness"] = future.result()
                    if server.poll() is not None or db.stat().st_ino != result["sqlite_inode"]:
                        raise ValueError("server PID or SQLite identity changed during the trial")
                    result["sqlite_inode_after_drain"] = db.stat().st_ino
                    recorded, driver_result = map(json.loads, driver_log.read_text().splitlines())
                    if recorded != window or driver_result["invalid"] or driver_result["unmapped_ack"]:
                        raise ValueError("invalid driver observations")
                    result["driver"] = driver_result
                    result["connections_survived"] = driver_result["disconnects"] == 0
                    pending = pending_deliveries(db)
                    bench.write_json(proof / "pending-after-drain.json", pending)
                    result.update(observations(
                        raw, window, qos, (rate * duration + 800 * recovery) // 1000,
                        pending, driver_result["disconnects_by_pair"]
                    ))
                intervals = {"idle": (window["idle_start_ns"], window["start_ns"]),
                             "load": (window["start_ns"], window["end_ns"])}
                result["cpu"] = {name: {
                    role: bench.summarize_process(samples, {"start_ns": start, "end_ns": stop}, role)
                    for role in ("broker", "generator")
                } for name, (start, stop) in intervals.items()}
        except (OSError, ValueError, KeyError, RuntimeError, AssertionError, subprocess.SubprocessError) as error:
            result["errors"].append(f"{type(error).__name__}: {error}")
        finally:
            preserve(raw, proof / "messages.csv.gz")
            with gzip.open(proof / "process-samples.jsonl.gz", "wt", compresslevel=3) as stream:
                for sample in samples:
                    stream.write(json.dumps(sample) + "\n")
            if server is not None:
                result["broker_exit"] = server.returncode
                try:
                    result["journal"] = journal.finish(proof, server.pid)
                except (OSError, ValueError, KeyError) as error:
                    result["errors"].append(f"journal: {error}")
        if not result.get("journal", {}).get("complete") or result.get("broker_exit") != 0:
            result["errors"].append("incomplete or unsuccessful broker run")
        if witness:
            if "witness" not in result or result.get("phases", [{}])[0].get("counts", {}).get("received", 0) == 0:
                result["errors"].append("missing concurrent QoS 0 witness")
            else:
                timings = result["witness"]["timings_ns"]
                closed = [at for at in result.get("journal", {}).get("closed_monotonic_ns", [])
                          if timings["cut_started"] <= at <= timings["deadline_removed"]]
                result["witness"]["broker_observed_closes"] = closed
                if len(closed) < 3:
                    result["errors"].append("broker did not observe the three transport closes during the witness")
        bench.write_json(proof / "result.json", result)
    print(proof.name, json.dumps(result.get("phases", [{}])[0].get("counts", result["errors"])), flush=True)
    return result


def decide(trials, *, smoke):
    failures = []
    profile = None
    for run in trials:
        if run["errors"]:
            failures.append({"trial": run["trial"], "criterion": "measurement", "detail": run["errors"]})
        if "service" in run:
            service = run["service"]
            current = {key: service[key] for key in ("sqlite_configuration", "tls_profile")}
            current["limits"] = {key: value for key, value in service["ready"].items()
                                 if key not in ("address", "incarnation", "epoch")}
            if profile is None:
                profile = current
            elif current != profile:
                failures.append({"trial": run["trial"], "criterion": "same_broker_configuration", "detail": current})
    pairs = {row["trial"]: row for row in trials}
    gains = []
    for index in range(1 if smoke else 5):
        a, b = (pairs[f"q0-3500-{index + 1}-{variant}"] for variant in ("A", "B"))
        if "phases" in a and "phases" in b:
            delivered_a, delivered_b = (x["phases"][0]["counts"]["in_window"] for x in (a, b))
            gains.append({"pair": index + 1, "A": delivered_a, "B": delivered_b})
            if not smoke and delivered_b <= delivered_a:
                failures.append({"trial": b["trial"], "criterion": "qos0_gain", "detail": gains[-1]})
    for row in trials:
        if row["variant"] != "B" or "phases" not in row:
            continue
        phase = row["phases"][0]
        counts = phase["counts"]
        if not smoke and row["trial"].startswith("q1-800") and (
            phase["local_refusals"] or phase["mqtt_refusals"] or row["driver"]["disconnects"]
            or counts["received"] != counts["scheduled"] or counts["positive_acks"] != counts["scheduled"]
            or counts["positive_without_explanation"]
        ):
            failures.append({"trial": row["trial"], "criterion": "qos1_at_800", "detail": counts,
                             "first_divergent_slots": phase["first_unexpected_slots"]})
        if row["qos"] == 0:
            batch = row.get("journal", {}).get("continuation_batch")
            if not batch or int(batch["extra_inputs_applied"]) == 0 or batch["maximum_slots_taken_in_outer_turn"] > 8:
                failures.append({"trial": row["trial"], "criterion": "extra_continuation_budget", "detail": batch})
    recovery = {row["variant"]: row.get("recovery") for row in trials if row["recovery_ms"]}
    return {"eligible": not smoke and not failures, "smoke_only": smoke, "qos0_pairs": gains,
            "recovery": recovery, "first_failure": failures[0] if failures else None, "failures": failures}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("volume", type=Path, help="choose the volume for both SQLite databases and WALs")
    parser.add_argument("output", type=Path, help="new directory for the receipts and binaries")
    parser.add_argument("--base", required=True, help="full Git SHA of the fixed-polling increment")
    parser.add_argument("--smoke", action="store_true", help="short harness validation with no performance verdict")
    parser.add_argument("--reference", type=Path, help="reuse the pinned Mosquitto 2.0.21 build")
    args = parser.parse_args()
    base = subprocess.check_output(["git", "-C", REPO, "rev-parse", f"{args.base}^{{commit}}"], text=True).strip()
    if base != args.base:
        parser.error("--base must be the complete immutable commit SHA")
    volume, output = args.volume.resolve(), args.output.resolve()
    if volume.is_relative_to(REPO):
        parser.error("choose a scratch directory outside the source checkout")
    filesystem = mount(volume)
    if not args.smoke and filesystem["fstype"] in ("tmpfs", "ramfs"):
        parser.error("choose a persistent runner volume")
    affinity = sorted(os.sched_getaffinity(0))
    if len(affinity) < 2:
        parser.error("the broker and driver require different CPUs")
    broker_cpus = affinity[:2] if len(affinity) >= 4 else affinity[:1]
    generator_cpus = [affinity[len(broker_cpus)]]
    observer_cpus = [affinity[-1]] if len(affinity) > len(broker_cpus) + 1 else generator_cpus
    os.umask(0o077)
    output.mkdir()
    harness_sha = subprocess.check_output(["git", "-C", REPO, "rev-parse", "HEAD"], text=True).strip()
    with tempfile.TemporaryDirectory(dir=volume, prefix="mqttiumd-ab-") as scratch:
        work = Path(scratch)
        binaries = build(base, work, output)
        reference = args.reference
        if reference is None:
            reference = work / "mosquitto-reference"
            with (output / "reference-build.log").open("xb") as log:
                subprocess.run(["sh", REPO / "mqttiumd/preview/build-mosquitto-reference.sh", reference],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
            reference /= "mosquitto-2.0.21"
        driver, client = bench.compile_driver(reference.resolve(), work)
        info = bench.machine(volume) | {"volume": filesystem, "driver": client,
                "broker_cpus": broker_cpus, "generator_cpus": generator_cpus, "observer_cpus": observer_cpus}
        bench.write_json(output / "manifest.json", binaries | {
            "harness_sha": harness_sha, "machine": info, "smoke_only": args.smoke
        })
        pki = bench.provision(work)
        os.sched_setaffinity(0, observer_cpus)
        seconds = 700 if args.smoke else 30000
        configurations = [("witness", 0, 3500, 10000, 0, ("A", "B"))]
        for index in range(1 if args.smoke else 5):
            order = ("A", "B") if index % 2 == 0 else ("B", "A")
            configurations += [(f"q0-3500-{index + 1}", 0, 3500, seconds, 0, order),
                               (f"q1-800-{index + 1}", 1, 800, seconds, 0, order)]
        configurations += [("q0-5500", 0, 5500, seconds, 0, ("B", "A")),
                           ("q1-recovery", 1, 1500, seconds, seconds, ("A", "B"))]
        records = []
        for name, qos, rate, duration, recovery, order in configurations:
            for label in order:
                records.append(trial(output / f"mqttiumd-{label}", label, bench.free_port(), qos, rate,
                                     duration, recovery, output / f"{name}-{label}", work, pki,
                                     driver, broker_cpus, generator_cpus, witness=name == "witness"))
                bench.write_json(output / "trials.json", records)
        verdict = decide(records, smoke=args.smoke)
        bench.write_json(output / "verdict.json", verdict)
        print(json.dumps(verdict, indent=2))


if __name__ == "__main__":
    main()
