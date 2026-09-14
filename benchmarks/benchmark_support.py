"""Runner metadata, preflight checks and source-compatibility shims for benchmarks.

Paired harnesses measure two source trees in fresh subprocesses, so a harness
must construct clients and read diagnostics on whichever API each arm exposes.
:func:`client_options` and :func:`runtime_counters` are the only sanctioned
shims; keep them dependency-free and keyed on the current public names.
"""

from __future__ import annotations

import inspect
import os
import platform
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

# Current constructor keyword -> the keyword the 1.0.0rc14 reference source
# accepted for the same bound. Applies to ``AsyncClient`` and ``EngineConfig``.
CLIENT_OPTION_ALIASES: dict[str, str] = {
    "max_inbound_inflight": "local_receive_maximum",
    "max_inbound_inflight_bytes": "max_pending_inbound_bytes",
    "max_unacknowledged_messages": "max_pending_outbound_messages",
    "max_unacknowledged_bytes": "max_pending_outbound_bytes",
    "max_write_queue_messages": "max_outbound_messages",
    "max_write_queue_bytes": "max_outbound_bytes",
    "max_iterator_messages": "max_pending_messages",
    "max_iterator_bytes": "max_pending_delivery_bytes",
    "iterator_admission_timeout": "delivery_timeout",
    "subscribe_timeout": "ack_timeout",
}

# Bounds the current source accepts for iterator delivery only. The reference
# source applied their aliases to callback delivery as well (its callback worker
# shared the delivery byte budget and admission deadline), so they are dropped
# only when the constructor speaks the current names.
_ITERATOR_ONLY_OPTIONS = frozenset(
    {"max_iterator_messages", "max_iterator_bytes", "iterator_admission_timeout"}
)

# Current ``client.stats()`` field -> the reference source's field for the same
# quantity, per snapshot section. Fields without an exact reference
# counterpart (``callback_invocations``, ``iterator_byte_limit``) are absent.
STATS_FIELD_ALIASES: dict[str, dict[str, str]] = {
    "outbound": {
        "unacknowledged_messages": "pending_messages",
        "unacknowledged_bytes": "pending_bytes",
        "unacknowledged_high_water_messages": "pending_high_water_messages",
        "unacknowledged_high_water_bytes": "pending_high_water_bytes",
        "awaiting_slot": "queued_messages",
        "inflight": "flow_inflight",
        "inflight_limit": "flow_limit",
    },
    "inbound": {
        "inflight_limit": "receive_maximum",
        "inflight_bytes": "pending_bytes",
        "inflight_high_water_bytes": "pending_high_water_bytes",
        "inflight_byte_limit": "pending_byte_limit",
    },
    "delivery": {
        "iterator_bytes": "pending_bytes",
        "iterator_high_water_bytes": "pending_high_water_bytes",
    },
}

# Every WritePump counter a harness may report; both sources own these on the
# pump instance, whether or not the snapshot also publishes them.
_WRITE_PUMP_COUNTERS = (
    "queued_bytes",
    "high_water_messages",
    "high_water_bytes",
    "max_messages",
    "max_bytes",
    "waiters",
    "batches",
    "batched_items",
    "batched_bytes",
    "segmented_writes",
    "enqueue_suspensions",
    "eager_writes",
    "eager_bytes",
)


def client_options(constructor: type | Any, **wanted: Any) -> dict[str, Any]:
    """Translate current constructor keywords for whichever source is imported.

    ``wanted`` uses the current public names (plus any source-specific extras
    such as the reference source's ``max_pending_callbacks``). A keyword the
    constructor accepts is kept; one it does not accept is renamed through
    :data:`CLIENT_OPTION_ALIASES` when the alias exists, otherwise dropped.
    With ``message_delivery="callback"`` the iterator-only bounds are dropped
    on the current source, which rejects them for callback delivery.
    """
    parameters = inspect.signature(constructor).parameters
    callback_mode = wanted.get("message_delivery") == "callback"
    options: dict[str, Any] = {}
    for name, value in wanted.items():
        if name in parameters:
            if callback_mode and name in _ITERATOR_ONLY_OPTIONS:
                continue
            options[name] = value
            continue
        alias = CLIENT_OPTION_ALIASES.get(name)
        if alias is not None and alias in parameters:
            options[alias] = value
    return options


def runtime_counters(client: Any, section: str) -> dict[str, Any]:
    """Return one diagnostics section of ``client`` as a dict keyed by current names.

    Snapshot fields the reference source named differently are renamed through
    :data:`STATS_FIELD_ALIASES`. The reference source also published writer
    batching counters, effect-pump counters (``effects``) and task liveness
    (``tasks``) inside ``client.stats()``; the current snapshot only describes
    application-sized queues and keeps those maintainer counters on
    ``client._write_pump``, ``client._effect_pump.counters()`` and
    ``client._running_tasks()``. Either layout yields the same keys here.
    """
    snapshot = getattr(client.stats(), section, None)
    counters: dict[str, Any] = asdict(snapshot) if is_dataclass(snapshot) else {}
    for current, legacy in STATS_FIELD_ALIASES.get(section, {}).items():
        if current not in counters and legacy in counters:
            counters[current] = counters.pop(legacy)
    if section == "writer":
        pump = client._write_pump
        for name in _WRITE_PUMP_COUNTERS:
            counters.setdefault(name, getattr(pump, name))
        counters.setdefault("queued_messages", pump.queue.qsize())
    elif section == "effects" and not counters:
        counters = dict(client._effect_pump.counters())
    elif section == "tasks" and not counters:
        counters = dict(client._running_tasks())
    return counters


def stats_counter(client: Any, section: str, name: str) -> Any:
    """Read one snapshot field or maintainer counter by its current name."""
    return runtime_counters(client, section)[name]


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
    max_load_per_cpu: float | None = 0.25
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
    if limits.max_load_per_cpu is not None and load > limits.max_load_per_cpu:
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


def stored_record(message):
    """Build a persisted test/benchmark record with its mandatory logical size."""
    from mqttium.protocol._sizing import publish_logical_size

    if message.logical_size > 0:
        return message
    message.logical_size = publish_logical_size(
        bool(message.properties),
        message.topic,
        len(message.payload),
        message.properties,
    )
    return message
