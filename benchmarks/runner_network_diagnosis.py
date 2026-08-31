#!/usr/bin/env python3
"""Capture per-process diagnostics around repeated paired-network workers."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


CPUFREQ_ROOT = Path("/sys/devices/system/cpu/cpufreq/policy0")


def _read_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _read_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        key, separator, raw_value = line.partition(":")
        if not separator:
            continue
        tokens = raw_value.strip().split()
        if not tokens:
            continue
        value = tokens[0]
        try:
            values[key] = int(value)
        except ValueError:
            continue
    return values


def _read_process_stat(pid: int) -> dict[str, int]:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return {}
    closing_parenthesis = raw.rfind(")")
    if closing_parenthesis < 0:
        return {}
    fields = raw[closing_parenthesis + 2 :].split()
    try:
        return {
            "minor_faults": int(fields[7]),
            "major_faults": int(fields[9]),
            "user_ticks": int(fields[11]),
            "system_ticks": int(fields[12]),
        }
    except (IndexError, ValueError):
        return {}


def _read_memory_layout(pid: int) -> dict[str, Any]:
    try:
        executable = os.readlink(f"/proc/{pid}/exe")
        mappings = Path(f"/proc/{pid}/maps").read_text(encoding="utf-8").splitlines()
        personality = Path(f"/proc/{pid}/personality").read_text(encoding="utf-8").strip()
    except OSError:
        return {}

    selected: dict[str, str] = {}
    anonymous_rw_bytes = 0
    anonymous_rw_mappings = 0
    for line in mappings:
        fields = line.split(maxsplit=5)
        if len(fields) < 5:
            continue
        address, permissions, _offset, _device, _inode = fields[:5]
        path = fields[5] if len(fields) == 6 else ""
        start_text, _, end_text = address.partition("-")
        try:
            size = int(end_text, 16) - int(start_text, 16)
        except ValueError:
            continue
        if not path and permissions.startswith("rw"):
            anonymous_rw_mappings += 1
            anonymous_rw_bytes += size

        basename = Path(path).name if path.startswith("/") else path
        key: str | None = None
        if path == executable and "x" in permissions:
            key = "python_text"
        elif "libpython" in basename and "x" in permissions:
            key = "libpython_text"
        elif basename.startswith("libc.so") and "x" in permissions:
            key = "libc_text"
        elif basename.startswith("libm.so") and "x" in permissions:
            key = "libm_text"
        elif path in {"[heap]", "[stack]", "[vdso]", "[vvar]"}:
            key = path[1:-1]
        if key is not None and key not in selected:
            selected[key] = f"0x{int(start_text, 16):x}"

    return {
        "executable": executable,
        "personality": personality,
        "selected_bases": selected,
        "mapping_count": len(mappings),
        "anonymous_rw_mapping_count": anonymous_rw_mappings,
        "anonymous_rw_bytes": anonymous_rw_bytes,
    }


def _time_in_state() -> dict[str, int]:
    values: dict[str, int] = {}
    path = CPUFREQ_ROOT / "stats" / "time_in_state"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        frequency, _, ticks = line.partition(" ")
        try:
            values[frequency] = int(ticks.strip())
        except ValueError:
            continue
    return values


def _firmware_value(*arguments: str) -> str | None:
    try:
        completed = subprocess.run(
            ("vcgencmd", *arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = completed.stdout.strip()
    return value or None


def _host_snapshot() -> dict[str, Any]:
    return {
        "scaling_cur_khz": _read_int(CPUFREQ_ROOT / "scaling_cur_freq"),
        "scaling_governor": (CPUFREQ_ROOT / "scaling_governor").read_text(encoding="utf-8").strip(),
        "firmware_clock": _firmware_value("measure_clock", "arm"),
        "throttled": _firmware_value("get_throttled"),
        "temperature": _firmware_value("measure_temp"),
        "time_in_state": _time_in_state(),
    }


def _support_cpu_only(cpu: int) -> None:
    os.sched_setaffinity(0, {cpu})


def _worker_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(args.engine.resolve()),
        "--worker",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--payload-bytes",
        str(args.payload_bytes),
        "--count",
        str(args.count),
        "--window",
        str(args.window),
        "--protocol",
        args.protocol,
        "--completion",
        args.completion,
        "--timeout",
        str(args.timeout),
        "--cpu",
        str(args.publisher_cpu),
    ]


def _run_worker(args: argparse.Namespace, iteration: int) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(args.root.resolve() / "src")
    environment["PYTHONHASHSEED"] = "0"
    command = _worker_command(args)
    before = _host_snapshot()
    sampled_frequencies: list[int] = []
    last_status: dict[str, int] = {}
    last_io: dict[str, int] = {}
    fault_timeline: list[dict[str, int]] = []
    worker_cgroup: str | None = None
    memory_layout: dict[str, Any] = {}
    wall_started = time.perf_counter()

    with tempfile.TemporaryDirectory(prefix="mqttium-worker-diagnosis-") as directory:
        stdout_path = Path(directory) / "stdout"
        stderr_path = Path(directory) / "stderr"
        with (
            stdout_path.open("w", encoding="utf-8") as stdout,
            stderr_path.open("w", encoding="utf-8") as stderr,
        ):
            process = subprocess.Popen(
                command,
                stdout=stdout,
                stderr=stderr,
                text=True,
                env=environment,
                preexec_fn=lambda: _support_cpu_only(args.support_cpu),
            )
            while True:
                finished_pid, wait_status, usage = os.wait4(process.pid, os.WNOHANG)
                if finished_pid:
                    break
                frequency = _read_int(CPUFREQ_ROOT / "scaling_cur_freq")
                if frequency is not None:
                    sampled_frequencies.append(frequency)
                status = _read_key_values(Path(f"/proc/{process.pid}/status"))
                io_values = _read_key_values(Path(f"/proc/{process.pid}/io"))
                process_stat = _read_process_stat(process.pid)
                if status:
                    last_status = status
                if io_values:
                    last_io = io_values
                if process_stat:
                    fault_timeline.append(
                        {
                            "elapsed_ms": round((time.perf_counter() - wall_started) * 1000),
                            **process_stat,
                        }
                    )
                if worker_cgroup is None:
                    try:
                        worker_cgroup = (
                            Path(f"/proc/{process.pid}/cgroup").read_text(encoding="utf-8").strip()
                        )
                    except OSError:
                        pass
                if not memory_layout and time.perf_counter() - wall_started >= 0.2:
                    memory_layout = _read_memory_layout(process.pid)
                time.sleep(args.sample_interval)
            returncode = os.waitstatus_to_exitcode(wait_status)
            process.returncode = returncode
        stdout_text = stdout_path.read_text(encoding="utf-8")
        stderr_text = stderr_path.read_text(encoding="utf-8")

    if returncode:
        raise RuntimeError(
            f"worker {iteration} exited {returncode}: {(stderr_text or stdout_text)[-2000:]}"
        )
    output_lines = [line for line in stdout_text.splitlines() if line.strip()]
    worker = json.loads(output_lines[-1])
    after = _host_snapshot()
    ticks_before = before["time_in_state"]
    ticks_after = after["time_in_state"]
    tick_delta = {
        frequency: ticks_after.get(frequency, 0) - ticks_before.get(frequency, 0)
        for frequency in sorted(set(ticks_before) | set(ticks_after))
    }
    return {
        "iteration": iteration,
        "worker": worker,
        "diagnostics": {
            "parent_wall_seconds": time.perf_counter() - wall_started,
            "rusage_user_seconds": usage.ru_utime,
            "rusage_system_seconds": usage.ru_stime,
            "voluntary_context_switches": usage.ru_nvcsw,
            "involuntary_context_switches": usage.ru_nivcsw,
            "minor_faults": usage.ru_minflt,
            "major_faults": usage.ru_majflt,
            "max_rss_kib": usage.ru_maxrss,
            "sampled_frequency_min_khz": min(sampled_frequencies, default=None),
            "sampled_frequency_max_khz": max(sampled_frequencies, default=None),
            "sample_count": len(sampled_frequencies),
            "last_proc_status": last_status,
            "last_proc_io": last_io,
            "fault_timeline": fault_timeline,
            "worker_cgroup": worker_cgroup,
            "memory_layout": memory_layout,
            "time_in_state_delta": tick_delta,
            "before": before,
            "after": after,
        },
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rates = [float(row["worker"]["publisher_ack_msg_s"]) for row in rows]
    cpu_per_message = [
        float(row["worker"]["cpu_seconds"]) / int(row["worker"]["count"]) * 1e6 for row in rows
    ]
    ack_p50 = [float(row["worker"]["ack_latency_p50_ms"]) * 1000 for row in rows]
    median_rate = statistics.median(rates)
    slow_iterations = [
        row["iteration"] for row, rate in zip(rows, rates, strict=True) if rate < median_rate * 0.9
    ]
    return {
        "iterations": len(rows),
        "throughput_msg_s": {
            "min": min(rates),
            "median": median_rate,
            "max": max(rates),
            "cv": statistics.stdev(rates) / statistics.fmean(rates),
        },
        "cpu_us_per_message": {
            "min": min(cpu_per_message),
            "median": statistics.median(cpu_per_message),
            "max": max(cpu_per_message),
        },
        "ack_p50_us": {
            "min": min(ack_p50),
            "median": statistics.median(ack_p50),
            "max": max(ack_p50),
        },
        "slow_iterations": slow_iterations,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--iterations", type=int, default=32)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--count", type=int, default=26_000)
    parser.add_argument("--window", type=int, default=1)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--completion", choices=("receipt", "callback"), default="callback")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--publisher-cpu", type=int, default=2)
    parser.add_argument("--support-cpu", type=int, default=1)
    parser.add_argument("--sample-interval", type=float, default=0.02)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows: list[dict[str, Any]] = []
    for iteration in range(1, args.iterations + 1):
        row = _run_worker(args, iteration)
        rows.append(row)
        worker = row["worker"]
        print(
            json.dumps(
                {
                    "iteration": iteration,
                    "throughput": worker["publisher_ack_msg_s"],
                    "cpu_seconds": worker["cpu_seconds"],
                    "ack_p50_ms": worker["ack_latency_p50_ms"],
                    "diagnostics": row["diagnostics"],
                }
            ),
            flush=True,
        )

    payload = {
        "metadata": {
            "python": sys.version,
            "pythonmalloc": os.environ.get("PYTHONMALLOC", "default"),
            "root": str(args.root.resolve()),
            "engine": str(args.engine.resolve()),
            "publisher_cpu": args.publisher_cpu,
            "support_cpu": args.support_cpu,
            "monitor_affinity": sorted(os.sched_getaffinity(0)),
            "randomize_va_space": _read_int(Path("/proc/sys/kernel/randomize_va_space")),
            "monitor_personality": Path("/proc/self/personality")
            .read_text(encoding="utf-8")
            .strip(),
        },
        "summary": _summarize(rows),
        "rows": rows,
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
