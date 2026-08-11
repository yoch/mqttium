"""Run reproducible release gates locally without consuming GitHub runners."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class CommandResult:
    name: str
    command: list[str]
    started_utc: str
    elapsed_s: float
    returncode: int
    log: str


class Recorder:
    def __init__(self, output: Path, profile: str) -> None:
        self.output = output
        self.profile = profile
        self.started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.started = time.perf_counter()
        self.results: list[CommandResult] = []
        self.output.mkdir(parents=True, exist_ok=True)
        self.source_sha256 = _source_fingerprint()
        self.worktree_status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True
        ).rstrip("\n")
        packages = (
            "bandit",
            "build",
            "check-wheel-contents",
            "hypothesis",
            "mypy",
            "paho-mqtt",
            "psutil",
            "pytest",
            "pytest-cov",
            "ruff",
            "twine",
            "validate-pyproject",
        )
        self.package_versions = {
            package: importlib.metadata.version(package)
            for package in packages
            if _package_installed(package)
        }

    def write_manifest(self) -> None:
        payload = {
            "profile": self.profile,
            "started_utc": self.started_utc,
            "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsed_s": time.perf_counter() - self.started,
            "repository": str(ROOT),
            "revision": _capture(["git", "rev-parse", "HEAD"], cwd=ROOT),
            "worktree_dirty": bool(self.worktree_status),
            "worktree_status": self.worktree_status.splitlines(),
            "source_sha256": self.source_sha256,
            "python": sys.version,
            "platform": sys.platform,
            "package_versions": self.package_versions,
            "commands": [asdict(result) for result in self.results],
            "status": (
                "passed"
                if self.results and all(result.returncode == 0 for result in self.results)
                else "failed"
            ),
        }
        (self.output / "manifest.json").write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8"
        )

    def run(
        self,
        name: str,
        command: list[str],
        *,
        cwd: Path = ROOT,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> None:
        print(f"[{name}] {' '.join(command)}", flush=True)
        started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        started = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
        elapsed = time.perf_counter() - started
        log_name = f"{len(self.results):02d}-{_slug(name)}.log"
        (self.output / log_name).write_text(completed.stdout, encoding="utf-8")
        self.results.append(
            CommandResult(
                name=name,
                command=command,
                started_utc=started_utc,
                elapsed_s=elapsed,
                returncode=completed.returncode,
                log=log_name,
            )
        )
        self.write_manifest()
        if completed.returncode:
            print(completed.stdout, file=sys.stderr)
            raise subprocess.CalledProcessError(completed.returncode, command)


def _slug(value: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in value).strip("-")


def _package_installed(name: str) -> bool:
    try:
        importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


def _source_fingerprint() -> str:
    tracked = _capture(["git", "ls-files"], cwd=ROOT).splitlines()
    untracked = _capture(
        ["git", "ls-files", "--others", "--exclude-standard"], cwd=ROOT
    ).splitlines()
    digest = hashlib.sha256()
    for relative in sorted(set(tracked + untracked)):
        path = ROOT / relative
        digest.update(relative.encode("utf-8") + b"\0")
        if path.is_file():
            digest.update(path.read_bytes())
        else:
            digest.update(b"<deleted>")
        digest.update(b"\0")
    return digest.hexdigest()


def _capture(command: list[str], *, cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def _python_tool(*arguments: str) -> list[str]:
    return [sys.executable, "-m", *arguments]


def _wait_port(port: int, process: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Mosquitto exited with status {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Mosquitto did not listen on 127.0.0.1:{port}")


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextmanager
def managed_mosquitto(output: Path, port: int) -> Iterator[None]:
    executable = shutil.which("mosquitto")
    if executable is None:
        raise RuntimeError("mosquitto is required for local release validation")
    config = output / "mosquitto.conf"
    log_path = output / "mosquitto.log"
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
                f"listener {port} 127.0.0.1",
                "",
            )
        ),
        encoding="utf-8",
    )
    with log_path.open("wb") as log:
        process = subprocess.Popen([executable, "-c", str(config)], stdout=log, stderr=log)
        try:
            _wait_port(port, process)
            yield
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@contextmanager
def managed_artifact_mosquitto(
    output: Path, port: int | None = None
) -> Iterator[dict[str, object]]:
    executable = shutil.which("mosquitto")
    openssl = shutil.which("openssl")
    if executable is None or openssl is None:
        raise RuntimeError("mosquitto and openssl are required for artifact validation")
    root = output / "artifact-broker"
    root.mkdir(parents=True, exist_ok=True)
    ca = root / "ca.crt"
    commands = (
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
            "/CN=MQTTium RC CA",
            "-keyout",
            str(root / "ca.key"),
            "-out",
            str(ca),
        ],
        [
            openssl,
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(root / "server.key"),
            "-out",
            str(root / "server.csr"),
        ],
    )
    for command in commands:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    extension = root / "server.ext"
    extension.write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            openssl,
            "x509",
            "-req",
            "-days",
            "1",
            "-in",
            str(root / "server.csr"),
            "-CA",
            str(ca),
            "-CAkey",
            str(root / "ca.key"),
            "-CAcreateserial",
            "-extfile",
            str(extension),
            "-out",
            str(root / "server.crt"),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (root / "server.key").chmod(0o644)
    port = port or _free_port()
    websocket_port = _free_port()
    while websocket_port == port:
        websocket_port = _free_port()
    tls_port = _free_port()
    while tls_port in (port, websocket_port):
        tls_port = _free_port()
    unix_socket = root / "mqttium.sock"
    if unix_socket.exists():
        unix_socket.unlink()
    config = root / "mosquitto.conf"
    config.write_text(
        "\n".join(
            (
                "persistence false",
                f"listener {port} 127.0.0.1",
                "protocol mqtt",
                "allow_anonymous true",
                f"listener {websocket_port} 127.0.0.1",
                "protocol websockets",
                "socket_domain ipv4",
                "allow_anonymous true",
                f"listener 0 {unix_socket}",
                "protocol mqtt",
                "allow_anonymous true",
                f"listener {tls_port} 127.0.0.1",
                "protocol mqtt",
                "allow_anonymous true",
                f"cafile {ca}",
                f"certfile {root / 'server.crt'}",
                f"keyfile {root / 'server.key'}",
                "",
            )
        ),
        encoding="utf-8",
    )
    with (root / "mosquitto.log").open("wb") as log:
        process = subprocess.Popen([executable, "-c", str(config)], stdout=log, stderr=log)
        try:
            _wait_port(port, process)
            _wait_port(websocket_port, process)
            _wait_port(tls_port, process)
            deadline = time.monotonic() + 10
            while not unix_socket.is_socket() and time.monotonic() < deadline:
                time.sleep(0.05)
            if not unix_socket.is_socket():
                raise TimeoutError("Mosquitto Unix listener was not created")
            yield {
                "tcp": port,
                "websocket": websocket_port,
                "tls": tls_port,
                "unix": unix_socket,
                "ca": ca,
            }
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@contextmanager
def baseline_root(base_ref: str, supplied: Path | None) -> Iterator[Path]:
    if supplied is not None:
        yield supplied.resolve()
        return
    temporary = Path(tempfile.mkdtemp(prefix="mqttium-baseline-"))
    try:
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(temporary), base_ref],
            cwd=ROOT,
            check=True,
        )
        yield temporary
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(temporary)],
            cwd=ROOT,
            check=False,
        )
        shutil.rmtree(temporary, ignore_errors=True)


def _assert_tracked_sources_clean() -> None:
    forbidden = (".patch", ".diff", ".pyc", ".pyo")
    tracked = _capture(["git", "ls-files"], cwd=ROOT).splitlines()
    bad = [path for path in tracked if path.endswith(forbidden) and (ROOT / path).exists()]
    if bad:
        raise RuntimeError("tracked generated/review artefacts: " + ", ".join(bad))


def run_quality(recorder: Recorder, port: int) -> None:
    _assert_tracked_sources_clean()
    recorder.run(
        "ruff-format", _python_tool("ruff", "format", "--check", "src", "tests", "benchmarks")
    )
    recorder.run("ruff", _python_tool("ruff", "check", "src", "tests", "benchmarks"))
    recorder.run("mypy", _python_tool("mypy", "src"))
    recorder.run("bandit", _python_tool("bandit", "-q", "-ll", "-r", "src"))
    recorder.run(
        "unit-coverage",
        _python_tool(
            "pytest",
            "-q",
            "tests/unit",
            "--cov=mqttium",
            "--cov-report=term-missing",
            "--cov-fail-under=87.36",
        ),
    )
    environment = os.environ.copy()
    environment["MQTTIUM_REQUIRE_BROKER"] = "1"
    recorder.run(
        "integration-required",
        _python_tool("pytest", "-q", "tests/integration"),
        env=environment,
        timeout=300,
    )


def run_robustness(recorder: Recorder, *, port: int) -> None:
    recorder.run(
        "hotpath-call-allocation-profile",
        [
            sys.executable,
            "benchmarks/hotpath_profile.py",
            "--output",
            str(recorder.output / "hotpaths.json"),
        ],
        timeout=3600,
    )
    recorder.run(
        "memory-profile",
        [
            sys.executable,
            "benchmarks/memory_profile.py",
            "--scale",
            "1",
            "--output",
            str(recorder.output / "memory.json"),
        ],
        timeout=3600,
    )
    recorder.run(
        "memory-thresholds",
        [
            sys.executable,
            "benchmarks/check_memory_thresholds.py",
            "--profile",
            str(recorder.output / "memory.json"),
            "--thresholds",
            "benchmarks/memory_thresholds.json",
        ],
    )
    recorder.run(
        "application-stress",
        [
            sys.executable,
            "benchmarks/application_stress.py",
            "--output",
            str(recorder.output / "application-stress.json"),
        ],
        timeout=3600,
    )
    duration = "30"
    for protocol in ("311", "5"):
        recorder.run(
            f"mosquitto-soak-{protocol}",
            [
                sys.executable,
                "benchmarks/soak.py",
                "--port",
                str(port),
                "--protocol",
                protocol,
                "--duration-seconds",
                duration,
                "--warmup-cycles",
                "2",
                "--force-reconnect-every",
                "1",
                "--output",
                str(recorder.output / f"soak-mosquitto-{protocol}.json"),
            ],
            timeout=float(duration) + 600,
        )


def run_performance(
    recorder: Recorder,
    *,
    base: Path,
    port: int,
    network_repeat: int,
    cpu: int | None,
) -> None:
    preflight = recorder.output / "runner.json"
    recorder.run(
        "runner-preflight",
        [
            sys.executable,
            "benchmarks/runner_probe.py",
            "--enforce",
            "--output",
            str(preflight),
        ],
    )
    micro_command = [
        sys.executable,
        "benchmarks/paired_regression.py",
        "--base-root",
        str(base),
        "--candidate-root",
        str(ROOT),
        "--repeat",
        "11",
        "--output",
        str(recorder.output / "paired-micro.json"),
    ]
    if cpu is not None:
        micro_command.extend(("--cpu", str(cpu)))
    recorder.run(
        "paired-micro",
        micro_command,
        timeout=1800,
    )
    network_command = [
        sys.executable,
        "benchmarks/paired_network.py",
        "--base-root",
        str(base),
        "--candidate-root",
        str(ROOT),
        "--port",
        str(port),
        "--policy",
        "advisory",
        "--preflight-report",
        str(preflight),
        "--repeat",
        str(network_repeat),
        "--output",
        str(recorder.output / "paired-network.json"),
    ]
    if cpu is not None:
        network_command.extend(("--cpu", str(cpu)))
    recorder.run(
        "paired-network-advisory",
        network_command,
        timeout=7200,
    )
    open_loop_command = [
        sys.executable,
        "benchmarks/paired_open_loop.py",
        "--base-root",
        str(base),
        "--candidate-root",
        str(ROOT),
        "--port",
        str(port),
        "--policy",
        "strict",
        "--preflight-report",
        str(preflight),
        "--output",
        str(recorder.output / "paired-open-loop.json"),
    ]
    if cpu is not None:
        open_loop_command.extend(("--cpu", str(cpu)))
    recorder.run(
        "paired-open-loop-strict",
        open_loop_command,
        timeout=14400,
    )


def run_package(recorder: Recorder) -> None:
    distribution = recorder.output / "dist"
    recorder.run("validate-pyproject", _python_tool("validate_pyproject", "pyproject.toml"))
    recorder.run("build", _python_tool("build", "--outdir", str(distribution)))
    artifacts = sorted(str(path) for path in distribution.iterdir())
    recorder.run("twine", _python_tool("twine", "check", "--strict", *artifacts))
    wheels = sorted(str(path) for path in distribution.glob("*.whl"))
    recorder.run("wheel-contents", _python_tool("check_wheel_contents", *wheels))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one wheel, found {len(wheels)}")
    environment = recorder.output / "wheel-venv"
    recorder.run("wheel-venv", [sys.executable, "-m", "venv", str(environment)])
    python = environment / "bin" / "python"
    recorder.run("wheel-install", [str(python), "-m", "pip", "install", "--no-deps", wheels[0]])
    recorder.run("wheel-pip-check", [str(python), "-m", "pip", "check"])
    version = _capture(
        [sys.executable, "-c", "import mqttium; print(mqttium.__version__)"], cwd=ROOT
    )
    recorder.run(
        "wheel-import-all",
        [
            str(python),
            "-I",
            "-c",
            "import importlib,pkgutil,mqttium; [importlib.import_module(m.name) for m in pkgutil.walk_packages(mqttium.__path__, mqttium.__name__+'.')]",
        ],
    )
    with managed_artifact_mosquitto(recorder.output) as broker:
        common = ["--expected-version", version]
        recorder.run(
            "wheel-tcp",
            [
                str(python),
                "-I",
                "tests/installed_distribution_smoke.py",
                *common,
                "--port",
                str(broker["tcp"]),
            ],
        )
        recorder.run(
            "wheel-websocket",
            [
                str(python),
                "-I",
                "tests/installed_distribution_extended_smoke.py",
                "websocket",
                *common,
                "--url",
                f"ws://127.0.0.1:{broker['websocket']}/mqtt",
            ],
        )
        recorder.run(
            "wheel-unix",
            [
                str(python),
                "-I",
                "tests/installed_distribution_extended_smoke.py",
                "unix",
                *common,
                "--socket",
                str(broker["unix"]),
            ],
        )
        for command in ("paho", "shutdown"):
            recorder.run(
                f"wheel-{command}",
                [
                    str(python),
                    "-I",
                    "tests/installed_distribution_extended_smoke.py",
                    command,
                    *common,
                    "--port",
                    str(broker["tcp"]),
                ],
            )
        database = recorder.output / "wheel-inflight.sqlite"
        recorder.run(
            "wheel-sqlite-write",
            [
                str(python),
                "-I",
                "tests/installed_distribution_resilience_smoke.py",
                "sqlite-write",
                *common,
                "--database",
                str(database),
            ],
        )
        recorder.run(
            "wheel-sqlite-read",
            [
                str(python),
                "-I",
                "tests/installed_distribution_resilience_smoke.py",
                "sqlite-read",
                *common,
                "--database",
                str(database),
            ],
        )
        recorder.run(
            "wheel-tls",
            [
                str(python),
                "-I",
                "tests/installed_distribution_resilience_smoke.py",
                "tls",
                *common,
                "--host",
                "localhost",
                "--port",
                str(broker["tls"]),
                "--ca-file",
                str(broker["ca"]),
            ],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("quick", "performance", "rc"))
    parser.add_argument("--base-ref", default="fbf1887")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--network-repeat", type=int, default=8)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.network_repeat <= 0 or args.network_repeat % 2:
        parser.error("--network-repeat must be a positive even number")
    return args


def main() -> None:
    args = parse_args()
    revision = _capture(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT)
    output = args.output_dir or Path("/tmp") / "mqttium-rc1" / revision / args.profile
    recorder = Recorder(output.resolve(), args.profile)
    try:
        with managed_mosquitto(recorder.output, args.port):
            if args.profile in ("quick", "rc"):
                run_quality(recorder, args.port)
            if args.profile in ("performance", "rc"):
                with baseline_root(args.base_ref, args.base_root) as base:
                    run_performance(
                        recorder,
                        base=base,
                        port=args.port,
                        network_repeat=args.network_repeat,
                        cpu=args.cpu,
                    )
            if args.profile in ("quick", "rc"):
                run_robustness(recorder, port=args.port)
            if args.profile == "rc":
                run_package(recorder)
    finally:
        recorder.write_manifest()


if __name__ == "__main__":
    main()
