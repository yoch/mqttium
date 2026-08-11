"""Orchestrate a short profile-first MQTTium optimisation campaign.

``discover`` profiles one source tree.  ``compare`` runs clean, fresh workers
in alternating order; instrumented samples are never included in its ratios.
Generated artefacts default to ``/tmp`` and are not source baselines.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

if __package__:
    from benchmarks.benchmark_support import runner_metadata
else:
    from benchmark_support import runner_metadata


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).resolve()
USAGE_SCRIPT = SCRIPT.with_name("research_scenarios.py")
CAMPAIGN_BASELINE = "6f72296c579a26b421bdb793d3ab58a3be75c47f"
NETWORK_PROFILES: dict[str, list[str]] = {
    "local": [],
    "wan": ["delay", "20ms", "5ms", "distribution", "normal", "loss", "0.1%"],
    "degraded": [
        "delay",
        "60ms",
        "20ms",
        "distribution",
        "normal",
        "loss",
        "1%",
        "rate",
        "10mbit",
    ],
}


@dataclass(frozen=True, slots=True)
class UsageSpec:
    name: str
    scenario: str
    priority: str
    protocol: str = "311"
    count: int = 1_000
    payload_size: int = 256
    window: int = 64
    target_rate: float = 0.0
    callback_delay: float = 0.001


DISCOVERY_SPECS = (
    UsageSpec(
        "telemetry-steady", "telemetry", "common", count=1_000, payload_size=128, target_rate=1_000
    ),
    UsageSpec("telemetry-burst", "telemetry", "common", count=2_000, payload_size=128),
    UsageSpec("reliable-window-1", "reliable", "common", count=300, window=1),
    UsageSpec("reliable-window-64-capacity", "reliable", "common", count=1_000, window=64),
    UsageSpec("duplex-gateway", "duplex", "common", protocol="5", count=500, payload_size=1_024),
    UsageSpec("batch-delivery", "batch_delivery", "common", count=500),
    UsageSpec("large-payload", "large_payload", "rare", count=8, payload_size=1_048_576, window=8),
    UsageSpec("slow-consumer", "slow_consumer", "rare", count=100, callback_delay=0.01),
    UsageSpec("property-heavy", "property_heavy", "rare", protocol="5", count=100),
)


@dataclass
class CommandRecord:
    name: str
    classification: str
    command: list[str]
    elapsed_s: float
    returncode: int
    stdout: str
    stderr: str


def _version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _tool(name: str) -> str | None:
    adjacent = Path(sys.executable).with_name(name)
    return str(adjacent) if adjacent.is_file() else shutil.which(name)


def _git(*arguments: str, cwd: Path = ROOT) -> str:
    return subprocess.check_output(["git", *arguments], cwd=cwd, text=True).strip()


def _source_state(root: Path) -> dict[str, Any]:
    return {
        "root": str(root),
        "revision": _git("rev-parse", "HEAD", cwd=root),
        "worktree_status": _git("status", "--porcelain", cwd=root).splitlines(),
    }


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _usage_command(
    spec: UsageSpec,
    args: argparse.Namespace,
    *,
    output: Path | None = None,
) -> list[str]:
    command = [
        sys.executable,
        str(USAGE_SCRIPT),
        "--scenario",
        spec.scenario,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        spec.protocol,
        "--count",
        str(spec.count),
        "--payload-size",
        str(spec.payload_size),
        "--window",
        str(spec.window),
        "--target-rate",
        str(spec.target_rate),
        "--callback-delay",
        str(spec.callback_delay),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    if args.cafile:
        command.extend(("--cafile", args.cafile))
    if args.ws_url:
        command.extend(("--ws-url", args.ws_url))
    if output is not None:
        command.extend(("--output", str(output)))
    return command


def _run(
    command: list[str],
    *,
    name: str,
    classification: str,
    env: dict[str, str] | None = None,
    timeout: float = 600,
    check: bool = True,
) -> CommandRecord:
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else exc.stdout
        )
        stderr = (
            exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else exc.stderr
        )
        record = CommandRecord(
            name=name,
            classification=classification,
            command=command,
            elapsed_s=time.perf_counter() - started,
            returncode=124,
            stdout=stdout or "",
            stderr=stderr or f"timed out after {timeout:.1f}s",
        )
    else:
        record = CommandRecord(
            name=name,
            classification=classification,
            command=command,
            elapsed_s=time.perf_counter() - started,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
    if check and record.returncode:
        raise RuntimeError(
            f"{name} failed with exit {record.returncode}: "
            f"{(record.stderr or record.stdout)[-2000:]}"
        )
    return record


def _json_stdout(record: CommandRecord) -> dict[str, Any]:
    lines = [line for line in record.stdout.splitlines() if line.strip()]
    try:
        return json.loads("\n".join(lines))
    except (IndexError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{record.name} did not emit one JSON document") from exc


def _write_manifest(output: Path, payload: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "manifest.json").write_text(
        json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
    )


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait_port(port: int, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Mosquitto exited with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Mosquitto did not listen on port {port}")


@contextmanager
def _broker(args: argparse.Namespace, output: Path) -> Iterator[None]:
    if args.external_broker:
        yield
        return
    executable = _tool("mosquitto")
    if executable is None:
        raise RuntimeError("mosquitto is required unless --external-broker is used")
    config = output / "mosquitto.conf"
    log_path = output / "mosquitto.log"
    tls_lines: tuple[str, ...] = ()
    if args.managed_tls:
        openssl = _tool("openssl")
        if openssl is None:
            raise RuntimeError("openssl is required for --managed-tls")
        certificate = output / "broker.crt"
        key = output / "broker.key"
        subprocess.run(
            [
                openssl,
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-keyout",
                str(key),
                "-out",
                str(certificate),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        args.cafile = str(certificate)
        tls_lines = (
            f"cafile {certificate}",
            f"certfile {certificate}",
            f"keyfile {key}",
            "require_certificate false",
        )
    config.write_text(
        "\n".join(
            (
                "persistence false",
                "allow_anonymous true",
                "max_inflight_messages 1000",
                "max_queued_messages 100000",
                "max_queued_bytes 0",
                "connection_messages false",
                "log_type error",
                f"listener {args.port} 127.0.0.1",
                *tls_lines,
                "",
            )
        ),
        encoding="utf-8",
    )
    with log_path.open("wb") as log:
        process = subprocess.Popen([executable, "-c", str(config)], stdout=log, stderr=log)
        try:
            _wait_port(args.port, process)
            yield
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@contextmanager
def _network_profile(args: argparse.Namespace) -> Iterator[None]:
    parameters = NETWORK_PROFILES[args.network_profile]
    if not parameters:
        yield
        return
    if not args.allow_netem:
        raise RuntimeError("non-local profiles require explicit --allow-netem")
    tc = _tool("tc")
    if tc is None:
        raise RuntimeError("tc is required for controlled WAN profiles")
    current = subprocess.check_output([tc, "-j", "qdisc", "show", "dev", "lo"], text=True)
    qdiscs = json.loads(current or "[]")
    foreign = [item for item in qdiscs if item.get("kind") not in (None, "noqueue")]
    if foreign:
        raise RuntimeError(f"refusing to replace an existing loopback qdisc: {foreign}")
    privileged = [tc]
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        sudo = _tool("sudo")
        if sudo is None:
            raise RuntimeError("netem requires root or passwordless sudo")
        privileged = [sudo, "-n", tc]
    subprocess.run(
        [*privileged, "qdisc", "replace", "dev", "lo", "root", "netem", *parameters],
        check=True,
    )
    try:
        yield
    finally:
        subprocess.run([*privileged, "qdisc", "del", "dev", "lo", "root"], check=False)


def _base_manifest(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "mode": args.mode,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "revision": _git("rev-parse", "HEAD"),
        "worktree_status": _git("status", "--porcelain").splitlines(),
        "environment": runner_metadata(broker_version_command=["mosquitto", "-h"]),
        "package_versions": {
            name: _version(name) for name in ("mqttium", "psutil", "py-spy", "paho-mqtt", "gmqtt")
        },
        "network_profile": args.network_profile,
        "remote_confirmation": False,
        "instrumented_runs_are_comparison_evidence": False,
        "commands": [],
    }


def _append(payload: dict[str, Any], record: CommandRecord) -> None:
    commands = payload["commands"]
    assert isinstance(commands, list)
    commands.append(asdict(record))


def discover(args: argparse.Namespace, output: Path) -> dict[str, Any]:
    payload = _base_manifest(args)
    measurements: list[dict[str, Any]] = []
    payload["measurements"] = measurements
    preflight = output / "runner.json"
    record = _run(
        [sys.executable, "benchmarks/runner_probe.py", "--output", str(preflight)],
        name="runner-preflight",
        classification="environment",
    )
    _append(payload, record)
    record = _run(
        [
            sys.executable,
            "benchmarks/hotpath_profile.py",
            "--output",
            str(output / "hotpaths.json"),
        ],
        name="exact-hotpath-profile",
        classification="diagnostic",
        timeout=3600,
    )
    _append(payload, record)

    for spec in DISCOVERY_SPECS:
        destination = output / f"usage-{spec.name}.json"
        record = _run(
            _usage_command(spec, args, output=destination),
            name=f"usage-{spec.name}",
            classification=f"clean-{spec.priority}",
            timeout=args.timeout + 60,
        )
        _append(payload, record)
        measurement = _json_stdout(record)
        measurement["name"] = spec.name
        measurement["priority"] = spec.priority
        measurements.append(measurement)

    capacity_result = next(
        item for item in measurements if item["name"] == "reliable-window-64-capacity"
    )
    capacity = float(capacity_result["primary_rate"])
    for fraction in (0.5, 0.9):
        spec = UsageSpec(
            f"reliable-window-64-load-{int(fraction * 100)}",
            "reliable",
            "common",
            count=1_000,
            window=64,
            target_rate=capacity * fraction,
        )
        destination = output / f"usage-{spec.name}.json"
        record = _run(
            _usage_command(spec, args, output=destination),
            name=f"usage-{spec.name}",
            classification="clean-common",
            timeout=args.timeout + 60,
        )
        _append(payload, record)
        measurement = _json_stdout(record)
        measurement.update(name=spec.name, priority="common", load_fraction=fraction)
        measurements.append(measurement)

    for name, command in (
        (
            "application-stress",
            [
                sys.executable,
                "benchmarks/application_stress.py",
                "--count",
                "2000",
                "--sqlite-count",
                "500",
                "--output",
                str(output / "application-stress.json"),
            ],
        ),
        (
            "reconnect-soak",
            [
                sys.executable,
                "benchmarks/soak.py",
                "--port",
                str(args.port),
                "--protocol",
                "5",
                "--cycles",
                "2",
                "--warmup-cycles",
                "1",
                "--messages-per-cycle",
                "50",
                "--payload-size",
                "256",
                "--force-reconnect-every",
                "1",
                "--output",
                str(output / "reconnect-soak.json"),
            ],
        ),
        (
            "memory-smoke",
            [
                sys.executable,
                "benchmarks/memory_profile.py",
                "--scale",
                "0.05",
                "--output",
                str(output / "memory-smoke.json"),
            ],
        ),
    ):
        record = _run(command, name=name, classification="clean-guardrail", timeout=3600)
        _append(payload, record)

    if not args.skip_samplers:
        short_spec = UsageSpec("reliable-profile", "reliable", "diagnostic", count=5_000)
        sampled_spec = UsageSpec(
            "reliable-profile-sampled",
            "reliable",
            "diagnostic",
            count=25_000,
            target_rate=5_000,
        )
        worker = _usage_command(short_spec, args)
        sampled_worker = _usage_command(sampled_spec, args)
        worker.append("--skip-ack-timestamps")
        sampled_worker.append("--skip-ack-timestamps")
        py_spy = _tool("py-spy") or "py-spy"
        perf = _tool("perf") or "perf"
        strace = _tool("strace") or "strace"
        samplers = (
            (
                "py-spy-reliable",
                [
                    py_spy,
                    "record",
                    "--format",
                    "speedscope",
                    "--rate",
                    "200",
                    "--nonblocking",
                    "--output",
                    str(output / "reliable-speedscope.json"),
                    "--",
                    *sampled_worker,
                ],
            ),
            (
                "perf-reliable",
                [
                    perf,
                    "stat",
                    "--no-inherit",
                    "--output",
                    str(output / "reliable-perf.txt"),
                    "-e",
                    "task-clock,cycles,instructions,cache-misses,context-switches",
                    "--",
                    *worker,
                ],
            ),
            (
                "strace-reliable",
                [
                    strace,
                    "-c",
                    "-o",
                    str(output / "reliable-strace.txt"),
                    *worker,
                ],
            ),
        )
        diagnostic_failures: list[str] = []
        for name, command in samplers:
            if not Path(command[0]).is_file() and shutil.which(command[0]) is None:
                raise RuntimeError(f"{command[0]} is required; use --skip-samplers to omit it")
            record = _run(
                command,
                name=name,
                classification="diagnostic-instrumented",
                timeout=args.timeout + 120,
                check=False,
            )
            _append(payload, record)
            if record.returncode:
                diagnostic_failures.append(name)
        cprofile_command = [*worker, "--cprofile-output", str(output / "reliable-cprofile.prof")]
        record = _run(
            cprofile_command,
            name="cprofile-reliable",
            classification="diagnostic-instrumented",
            timeout=args.timeout + 120,
        )
        _append(payload, record)
        payload["diagnostic_failures"] = diagnostic_failures
    payload["status"] = "complete"
    return payload


def _worker_for_root(
    spec: UsageSpec,
    args: argparse.Namespace,
    root: Path,
) -> CommandRecord:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    return _run(
        _usage_command(spec, args),
        name=f"compare-{spec.name}",
        classification="clean-comparison",
        env=environment,
        timeout=args.timeout + 60,
    )


def comparison_summary(
    pairs: list[dict[str, Any]],
    *,
    confirm: bool,
    aa_control: bool = False,
) -> dict[str, Any]:
    ratios = [float(pair["candidate_rate"]) / float(pair["base_rate"]) for pair in pairs]
    base_rates = [float(pair["base_rate"]) for pair in pairs]
    ratio = statistics.median(ratios)
    wins = sum(value > 1 for value in ratios)
    neutral_control = abs(ratio - 1.0) <= 0.02
    valid = _cv(base_rates) <= 0.05 and (not aa_control or neutral_control)
    quick_signal = not aa_control and valid and ratio >= 1.05 and wins == len(ratios)
    return {
        "median_candidate_over_base": ratio,
        "base_cv": _cv(base_rates),
        "pairs_favouring_candidate": wins,
        "pair_count": len(pairs),
        "aa_control": aa_control,
        "neutral_control": neutral_control,
        "valid": valid,
        "quick_signal": quick_signal,
        "formal_network_gain": confirm and not aa_control and valid and ratio >= 1.05,
        "note": "one workload is a screen; two load points are required for retention",
    }


def compare(args: argparse.Namespace, output: Path) -> dict[str, Any]:
    payload = _base_manifest(args)
    base = args.base_root.resolve()
    candidate = args.candidate_root.resolve()
    base_state = _source_state(base)
    candidate_state = _source_state(candidate)
    if base != candidate:
        if base_state["revision"] != args.expected_base_revision:
            raise RuntimeError(
                "comparison base is not the immutable campaign baseline: "
                f"expected {args.expected_base_revision}, got {base_state['revision']}"
            )
        if base_state["worktree_status"]:
            raise RuntimeError("comparison base worktree is not clean")
    spec = UsageSpec(
        name=args.scenario,
        scenario=args.scenario,
        priority="comparison",
        protocol=args.protocol,
        count=args.count,
        payload_size=args.payload_size,
        window=args.window,
        target_rate=args.target_rate,
        callback_delay=args.callback_delay,
    )
    pairs: list[dict[str, Any]] = []
    for index in range(args.repeat):
        order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        measured: dict[str, dict[str, Any]] = {}
        for variant in order:
            root = base if variant == "base" else candidate
            record = _worker_for_root(spec, args, root)
            _append(payload, record)
            measured[variant] = _json_stdout(record)
        pairs.append(
            {
                "order": list(order),
                "base_rate": measured["base"]["primary_rate"],
                "candidate_rate": measured["candidate"]["primary_rate"],
                "base": measured["base"],
                "candidate": measured["candidate"],
            }
        )
    summary = comparison_summary(pairs, confirm=args.confirm, aa_control=base == candidate)
    payload.update(
        {
            "base_root": str(base),
            "candidate_root": str(candidate),
            "base_source": base_state,
            "candidate_source": candidate_state,
            "scenario": asdict(spec),
            "pairs": pairs,
            "summary": summary,
            "status": "invalid" if not summary["valid"] else "complete",
        }
    )
    if args.confirm:
        preflight = output / "runner.json"
        record = _run(
            [
                sys.executable,
                "benchmarks/runner_probe.py",
                "--output",
                str(preflight),
            ],
            name="confirmation-preflight",
            classification="environment",
        )
        _append(payload, record)
        if args.micro_scenario:
            record = _run(
                [
                    sys.executable,
                    "benchmarks/paired_regression.py",
                    "--base-root",
                    str(base),
                    "--candidate-root",
                    str(candidate),
                    "--scenario",
                    args.micro_scenario,
                    "--repeat",
                    "11",
                    "--cpu",
                    str(args.cpu),
                    "--output",
                    str(output / "paired-micro-confirmation.json"),
                ],
                name="micro-confirmation",
                classification="clean-confirmation",
                timeout=1800,
            )
            _append(payload, record)
        record = _run(
            [
                sys.executable,
                "benchmarks/paired_open_loop.py",
                "--base-root",
                str(base),
                "--candidate-root",
                str(candidate),
                "--port",
                str(args.port),
                "--policy",
                "strict",
                "--preflight-report",
                str(preflight),
                "--protocols",
                args.protocol,
                "--payloads",
                str(args.payload_size),
                "--completions",
                "receipt",
                "--fractions",
                "0.50,0.90",
                "--repeat",
                "4",
                "--cpu",
                str(args.cpu),
                "--output",
                str(output / "paired-open-loop-confirmation.json"),
            ],
            name="network-confirmation-two-loads",
            classification="clean-confirmation",
            timeout=7200,
            check=False,
        )
        _append(payload, record)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("discover", "compare"))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--external-broker", action="store_true")
    parser.add_argument("--managed-tls", action="store_true")
    parser.add_argument("--cafile")
    parser.add_argument("--ws-url")
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--network-profile", choices=tuple(NETWORK_PROFILES), default="local")
    parser.add_argument("--allow-netem", action="store_true")
    parser.add_argument("--skip-samplers", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--expected-base-revision", default=CAMPAIGN_BASELINE)
    parser.add_argument(
        "--scenario", choices=tuple(spec.scenario for spec in DISCOVERY_SPECS), default="reliable"
    )
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--window", type=int, default=64)
    parser.add_argument("--target-rate", type=float, default=0.0)
    parser.add_argument("--callback-delay", type=float, default=0.001)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--micro-scenario")
    args = parser.parse_args()
    if args.timeout <= 0 or args.repeat <= 0:
        parser.error("timeout and repeat must be positive")
    if args.mode == "compare" and (args.base_root is None or args.candidate_root is None):
        parser.error("compare requires --base-root and --candidate-root")
    if args.mode == "compare" and args.cpu is None:
        parser.error("compare requires --cpu so every worker is pinned")
    if args.mode == "compare" and (args.repeat < 4 or args.repeat % 2):
        parser.error("compare requires at least four repeats in complete ABBA cycles")
    if args.confirm and args.repeat < 3:
        parser.error("confirmation requires at least three quick-screen pairs")
    if not args.external_broker and (args.cafile or args.ws_url):
        parser.error("TLS/WebSocket discovery requires --external-broker")
    if args.external_broker and args.managed_tls:
        parser.error("--managed-tls cannot be combined with --external-broker")
    return args


def main() -> None:
    args = parse_args()
    short = _git("rev-parse", "--short", "HEAD")
    output = (args.output_dir or Path("/tmp") / "mqttium-research" / short / args.mode).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if not args.external_broker and args.port == 0:
        args.port = _free_port()
    payload: dict[str, Any] = {}
    try:
        with _broker(args, output), _network_profile(args):
            payload = discover(args, output) if args.mode == "discover" else compare(args, output)
    except BaseException as exc:
        payload = payload or _base_manifest(args)
        payload["status"] = "failed"
        payload["failure"] = f"{type(exc).__name__}: {exc}"
        _write_manifest(output, payload)
        raise
    _write_manifest(output, payload)
    print(json.dumps({"status": payload["status"], "output": str(output)}))


if __name__ == "__main__":
    main()
