"""Runner metadata and preflight checks for benchmark evidence."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None


def _cpu_governors() -> list[str]:
    governors = {
        value
        for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_governor")
        if (value := _read_text(path))
    }
    return sorted(governors)


def _temperatures() -> dict[str, float]:
    readings: dict[str, float] = {}
    for path in Path("/sys/class/thermal").glob("thermal_zone*/temp"):
        raw = _read_text(path)
        if raw is None:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if value > 1_000:
            value /= 1_000
        zone = path.parent.name
        label = _read_text(path.parent / "type")
        readings[label or zone] = value
    return readings


def _command_version(command: list[str] | None) -> str | None:
    if not command:
        return None
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    output = completed.stdout.strip() or completed.stderr.strip()
    return output.splitlines()[0] if output else None


def runner_metadata(*, broker_version_command: list[str] | None = None) -> dict[str, Any]:
    """Return stable identity and scheduling facts for a benchmark artefact."""
    affinity: list[int] | None = None
    if hasattr(os, "sched_getaffinity"):
        affinity = sorted(os.sched_getaffinity(0))
    cpu_model = None
    cpuinfo = _read_text(Path("/proc/cpuinfo"))
    if cpuinfo:
        for line in cpuinfo.splitlines():
            if line.lower().startswith("model name"):
                cpu_model = line.partition(":")[2].strip()
                break
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model,
        "logical_cpu_count": os.cpu_count(),
        "affinity": affinity,
        "cpu_governors": _cpu_governors(),
        "temperatures_c": _temperatures(),
        "runner_name": os.environ.get("RUNNER_NAME"),
        "runner_environment": os.environ.get("RUNNER_ENVIRONMENT"),
        "github_sha": os.environ.get("GITHUB_SHA"),
        "broker": _command_version(broker_version_command),
    }


@dataclass(frozen=True, slots=True)
class PreflightLimits:
    max_load_per_cpu: float = 0.25
    max_cpu_percent: float = 20.0
    max_temperature_c: float = 80.0
    required_governor: str | None = "performance"
    require_temperature: bool = False


def sample_runner(*, interval_s: float = 1.0) -> dict[str, Any]:
    """Sample load without adding a dependency to the installed library."""
    try:
        import psutil
    except ImportError as exc:  # pragma: no cover - benchmark environment error
        raise RuntimeError("runner preflight requires psutil>=6") from exc

    cpu_percent = psutil.cpu_percent(interval=max(0.0, interval_s))
    logical_cpus = os.cpu_count() or 1
    try:
        load_1m, load_5m, load_15m = os.getloadavg()
    except (AttributeError, OSError):
        load_1m = load_5m = load_15m = 0.0
    temperatures = _temperatures()
    return {
        "cpu_percent": cpu_percent,
        "load_1m": load_1m,
        "load_5m": load_5m,
        "load_15m": load_15m,
        "load_1m_per_cpu": load_1m / logical_cpus,
        "temperatures_c": temperatures,
        "max_temperature_c": max(temperatures.values(), default=None),
        "cpu_governors": _cpu_governors(),
    }


def evaluate_preflight(sample: dict[str, Any], limits: PreflightLimits) -> list[str]:
    """Return reasons that make a sample unsuitable as gate evidence."""
    failures: list[str] = []
    load = float(sample["load_1m_per_cpu"])
    if load > limits.max_load_per_cpu:
        failures.append(f"1-minute load per CPU {load:.3f} exceeds {limits.max_load_per_cpu:.3f}")
    cpu = float(sample["cpu_percent"])
    if cpu > limits.max_cpu_percent:
        failures.append(f"CPU use {cpu:.1f}% exceeds {limits.max_cpu_percent:.1f}%")
    temperature = sample.get("max_temperature_c")
    if temperature is None:
        if limits.require_temperature:
            failures.append("temperature sensors are unavailable")
    elif float(temperature) > limits.max_temperature_c:
        failures.append(
            f"temperature {float(temperature):.1f}C exceeds {limits.max_temperature_c:.1f}C"
        )
    if limits.required_governor is not None:
        governors = sample.get("cpu_governors") or []
        if not governors:
            failures.append("CPU governor is unavailable")
        elif governors != [limits.required_governor]:
            failures.append(
                f"CPU governors {governors!r} do not match {limits.required_governor!r}"
            )
    return failures
