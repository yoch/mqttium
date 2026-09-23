"""Run reproducible release gates locally without consuming GitHub runners.

Runs retain a fresh private output directory and print its path. An explicit
--output-dir must not exist, and its parent must already exist. POSIX ancestors
must be controlled by the caller or root, without unprotected group/other write
access. Run the gate as the normal developer account; Windows ACLs remain the
deployment's responsibility. Existing evidence is never reused or overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class CommandResult:
    name: str
    command: list[str]
    started_utc: str
    elapsed_s: float
    # None when the command timed out or could not be started.
    returncode: int | None
    log: str
    error: str | None = None


class ReleaseGateFailed(Exception):
    """Expected non-zero result from a recorded release gate."""

    def __init__(
        self,
        *,
        name: str,
        returncode: int | None,
        log: Path,
        manifest: Path,
        error: str | None = None,
    ) -> None:
        super().__init__(
            f"{name} exited with status {returncode}"
            if error is None
            else f"{name} did not complete: {error}"
        )
        self.name = name
        self.returncode = returncode
        self.log = log
        self.manifest = manifest


def _private_output_directory(output: Path | None) -> Path:
    parent = Path(tempfile.gettempdir()) if output is None else output.parent
    parent = parent.resolve(strict=True)
    if os.name == "posix":
        for ancestor in (parent, *parent.parents):
            info = ancestor.stat()
            if info.st_uid not in (os.geteuid(), 0) or (
                info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX
            ):
                raise PermissionError(f"Untrusted release output ancestor: {ancestor}")
    if output is None:
        return Path(tempfile.mkdtemp(prefix="mqttium-release-", dir=parent))
    output = parent / output.name
    # Exclusive mkdir rejects all pre-existing entries, including dangling
    # symlinks. Never resolve the final component before this check.
    output.mkdir(mode=0o700)
    return output


def _open_private_file(path: Path) -> BinaryIO:
    # O_EXCL | O_CREAT refuses existing files and symlinks on every platform.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return os.fdopen(descriptor, "wb")


def _write_private_text(path: Path, text: str) -> None:
    with _open_private_file(path) as output:
        output.write(text.encode("utf-8"))


def _replace_manifest(path: Path, text: str) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise FileExistsError(f"Refusing non-regular release manifest: {path}")
    descriptor, temporary = tempfile.mkstemp(prefix=".manifest-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(text)
        # Replace the directory entry; never open an old manifest's target.
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


class Recorder:
    def __init__(self, output: Path | None, profile: str) -> None:
        self.output = _private_output_directory(output)
        self.profile = profile
        self.started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.started = time.perf_counter()
        self.results: list[CommandResult] = []
        # Set only after every phase of the requested profile has run.
        self.profile_completed = False
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
            "mkdocs",
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
            "status": self._status(),
        }
        _replace_manifest(self.output / "manifest.json", json.dumps(payload, indent=2) + "\n")

    def _status(self) -> str:
        if any(result.returncode != 0 for result in self.results):
            return "failed"
        return "passed" if self.profile_completed and self.results else "incomplete"

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
        log_name = f"{len(self.results):02d}-{_slug(name)}.log"
        returncode: int | None = None
        error: str | None = None
        with _open_private_file(self.output / log_name) as log:
            try:
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
            except subprocess.TimeoutExpired as exc:
                error = f"timed out after {exc.timeout}s"
                output = exc.stdout or ""
                log.write(output.encode("utf-8") if isinstance(output, str) else output)
            except OSError as exc:
                error = f"could not start: {exc}"
            else:
                returncode = completed.returncode
                log.write(completed.stdout.encode("utf-8"))
        elapsed = time.perf_counter() - started
        # Record every attempted gate, so a manifest cannot pass without it.
        self.results.append(
            CommandResult(
                name=name,
                command=command,
                started_utc=started_utc,
                elapsed_s=elapsed,
                returncode=returncode,
                log=log_name,
                error=error,
            )
        )
        self.write_manifest()
        if returncode != 0:
            raise ReleaseGateFailed(
                name=name,
                returncode=returncode,
                log=self.output / log_name,
                manifest=self.output / "manifest.json",
                error=error,
            )


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
    _write_private_text(
        config,
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
    )
    with _open_private_file(log_path) as log:
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
    root.mkdir(mode=0o700)
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
    _write_private_text(
        extension,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
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
    _write_private_text(
        config,
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
    )
    with _open_private_file(root / "mosquitto.log") as log:
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
        "ruff-format",
        _python_tool("ruff", "format", "--check", "src", "tests", "benchmarks", "tools"),
    )
    recorder.run("ruff", _python_tool("ruff", "check", "src", "tests", "benchmarks", "tools"))
    recorder.run("mypy", _python_tool("mypy", "src"))
    recorder.run("bandit", _python_tool("bandit", "-q", "-ll", "-r", "src"))
    recorder.run(
        "unit-coverage",
        _python_tool(
            "pytest",
            "-q",
            "tests/unit",
            "tests/project",
            "--cov=mqttium",
            "--cov-report=term-missing",
        ),
    )
    recorder.run(
        "docs-strict",
        _python_tool("mkdocs", "build", "--strict", "--site-dir", str(recorder.output / "site")),
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


def _runner_preflight_command(output: Path, *, ignore_historical_load: bool = False) -> list[str]:
    command = [
        sys.executable,
        "benchmarks/runner_probe.py",
        "--enforce",
        "--wait-seconds",
        "60",
        "--poll-seconds",
        "5",
        "--consecutive-eligible",
        "2",
        "--output",
        str(output),
    ]
    if ignore_historical_load:
        command.append("--ignore-historical-load")
    return command


def run_performance(
    recorder: Recorder,
    *,
    base: Path,
    port: int,
    network_repeat: int,
    cpu: int | None,
) -> None:
    micro_preflight = recorder.output / "runner.json"
    recorder.run(
        "runner-preflight",
        _runner_preflight_command(micro_preflight),
        timeout=90,
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

    network_preflight = recorder.output / "runner-network.json"
    recorder.run(
        "runner-preflight-network",
        _runner_preflight_command(network_preflight, ignore_historical_load=True),
        timeout=90,
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
        str(network_preflight),
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
        "benchmarks/open_loop_release_gate.py",
        "--base-root",
        str(base),
        "--candidate-root",
        str(ROOT),
        "--port",
        str(port),
        "--policy",
        "strict",
        "--engine",
        "benchmarks/paired_open_loop.py",
        "--runner-probe",
        "benchmarks/runner_probe.py",
        "--output",
        str(recorder.output / "paired-open-loop.json"),
    ]
    if cpu is not None:
        open_loop_command.extend(("--cpu", str(cpu)))
    recorder.run(
        "open-loop-release-gate-strict",
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
        [str(python), "-I", "-c", "import mqttium; print(mqttium.__version__)"], cwd=ROOT
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
        for command in ("shutdown",):
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
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="new output directory (must not exist); default: retained private temporary directory",
    )
    args = parser.parse_args()
    if args.network_repeat <= 0 or args.network_repeat % 2:
        parser.error("--network-repeat must be a positive even number")
    if args.profile in ("performance", "rc") and args.cpu is None:
        parser.error("--cpu is required for performance and rc release gates")
    return args


def main() -> int:
    args = parse_args()
    try:
        recorder = Recorder(args.output_dir, args.profile)
    except (FileExistsError, FileNotFoundError, PermissionError) as exc:
        # An unusable output path is an expected refusal, not a crash.
        print(f"local release output refused: {exc}", file=sys.stderr)
        return 2
    print(f"release output: {recorder.output}", flush=True)
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
        recorder.profile_completed = True
    except ReleaseGateFailed as exc:
        print(f"local release gate failed: {exc}", file=sys.stderr)
        print(f"log: {exc.log}", file=sys.stderr)
        print(f"manifest: {exc.manifest}", file=sys.stderr)
        return exc.returncode or 1
    finally:
        recorder.write_manifest()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
