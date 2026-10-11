"""Qualify Tachyon overhead in fresh processes and retain diagnostic profiles.

Python 3.15 is required. Outputs are diagnostic local evidence, not a release
gate or a cross-client comparison. Each cycle alternates baseline/sample/sample/
baseline; call counting and allocation profiling remain separate experiments.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import sysconfig
import time
from pathlib import Path
from typing import Any

MODES = ("cpu", "wall", "gil", "exception", "async-running", "async-all")


def _identity() -> dict[str, Any]:
    jit = getattr(sys, "_jit", None)
    return {
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "soabi": sysconfig.get_config_var("SOABI"),
        "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
        "jit_available": bool(jit and jit.is_available()),
        "jit_enabled": bool(jit and jit.is_enabled()),
        "python_jit": os.environ.get("PYTHON_JIT"),
        "python_gil": os.environ.get("PYTHON_GIL"),
    }


def _flags(mode: str) -> list[str]:
    if mode.startswith("async-"):
        return ["--async-aware", "--async-mode", mode.split("-", 1)[1]]
    return ["--mode", mode, "--all-threads"]


def _worker(args: argparse.Namespace) -> None:
    import asyncio

    if args.suite == "micro":
        from _paired_scenarios import run_scenario

        def phase() -> int:
            return run_scenario(args.scenario).operations
    else:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from lean_native_diagnostics import _phase

        def phase() -> int:
            result = asyncio.run(_phase(args.scenario, args.count))
            return int(result["count"])

    phase()  # warm imports, tables and specialization before the timed window
    scope = Path("/proc/sys/kernel/yama/ptrace_scope")
    if (
        args.instrumented
        and sys.platform == "linux"
        and scope.exists()
        and scope.read_text(encoding="ascii").strip() != "0"
    ):
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl.argtypes = [ctypes.c_int, *([ctypes.c_ulong] * 4)]
        if libc.prctl(0x59616D61, os.getppid(), 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "cannot authorize the parent profiler")
    print("READY", flush=True)
    if sys.stdin.readline().strip() != "GO":
        raise RuntimeError("missing profiling controller start signal")
    start = time.perf_counter()
    cpu_start = time.process_time()
    operations = iterations = 0
    while time.perf_counter() - start < args.seconds:
        operations += phase()
        iterations += 1
    elapsed = time.perf_counter() - start
    args.result.write_text(
        json.dumps(
            {
                "elapsed_s": elapsed,
                "cpu_s": time.process_time() - cpu_start,
                "operations": operations,
                "iterations": iterations,
                "operations_per_s": operations / elapsed,
                "runtime": _identity(),
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _stop(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _trial(args: argparse.Namespace, root: Path, index: int, instrumented: bool) -> dict[str, Any]:
    directory = root / f"{index:02d}-{'sampled' if instrumented else 'baseline'}"
    directory.mkdir()
    result = directory / "result.json"
    argv = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--suite",
        args.suite,
        "--scenario",
        args.scenario,
        "--seconds",
        str(args.seconds),
        "--count",
        str(args.count),
        "--result",
        str(result),
    ]
    if instrumented:
        argv.append("--instrumented")
    if args.worker_cpu is not None:
        argv = ["taskset", "-c", str(args.worker_cpu), *argv]
    capture = None
    with (
        (directory / "worker.log").open("w", encoding="utf-8") as worker_log,
        (directory / "capture.log").open("w", encoding="utf-8") as capture_log,
    ):
        worker = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=worker_log, text=True
        )
        try:
            # Reading happens in a bounded helper thread: a hung warm-up must not hang the controller.
            import concurrent.futures

            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            ready = pool.submit(worker.stdout.readline)
            try:
                if ready.result(timeout=args.timeout).strip() != "READY":
                    raise RuntimeError(
                        f"worker failed before READY; see {directory / 'worker.log'}"
                    )
            finally:
                pool.shutdown(wait=False)
            if instrumented:
                async_aware = args.mode.startswith("async-")
                binary = directory / ("flamegraph.html" if async_aware else "profile.bin")
                profile_argv = [
                    sys.executable,
                    "-m",
                    "profiling.sampling",
                    "attach",
                    *_flags(args.mode),
                    "--sampling-rate",
                    str(args.rate),
                    "--duration",
                    str(math.ceil(args.seconds)),
                    *(["--flamegraph"] if async_aware else ["--binary", "--compression", "none"]),
                    "--output",
                    str(binary),
                    str(worker.pid),
                ]
                if args.profiler_cpu is not None:
                    profile_argv = ["taskset", "-c", str(args.profiler_cpu), *profile_argv]
                capture = subprocess.Popen(
                    profile_argv, stdout=capture_log, stderr=subprocess.STDOUT, text=True
                )
            worker.stdin.write("GO\n")
            worker.stdin.flush()
            rc = worker.wait(timeout=args.timeout)
            if rc:
                raise RuntimeError(f"worker failed (exit {rc}); see {directory / 'worker.log'}")
            if capture is not None:
                rc = capture.wait(timeout=15)
                if rc or not binary.is_file() or not binary.stat().st_size:
                    raise RuntimeError(
                        f"Tachyon failed (exit {rc}); see {directory / 'capture.log'}"
                    )
                if not async_aware:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "profiling.sampling",
                            "replay",
                            str(binary),
                            "--flamegraph",
                            "--output",
                            str(directory / "flamegraph.html"),
                        ],
                        stdout=capture_log,
                        stderr=subprocess.STDOUT,
                        timeout=60,
                        check=True,
                    )
                html = (directory / "flamegraph.html").read_text(encoding="utf-8")
                marker = "const EMBEDDED_DATA = "
                if marker not in html:
                    raise RuntimeError("Tachyon flamegraph data format changed; capture retained")
                tree, _ = json.JSONDecoder().raw_decode(html.split(marker, 1)[1])
                if not tree.get("value"):
                    raise RuntimeError(
                        "Tachyon captured no stacks; extend the capture or change mode"
                    )
                (directory / "profile.json").write_text(json.dumps(tree) + "\n", encoding="utf-8")
            return {
                "instrumented": instrumented,
                "directory": str(directory),
                **json.loads(result.read_text(encoding="utf-8")),
            }
        finally:
            if capture is not None:
                _stop(capture)
            _stop(worker)
            for stream in (worker.stdin, worker.stdout):
                if stream is not None:
                    stream.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("micro", "native"), default="micro")
    parser.add_argument("--scenario", default="encode_qos0")
    parser.add_argument("--mode", choices=MODES, default="cpu")
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--count", type=int, default=20000)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--worker-cpu", type=int)
    parser.add_argument("--profiler-cpu", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--instrumented", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.cycles, args.count, args.timeout) <= 0:
        parser.error("rate, seconds, cycles, count and timeout must be positive")
    if args.worker:
        _worker(args)
        return
    if sys.version_info[:2] != (3, 15):
        parser.error("Tachyon qualification currently requires Python 3.15")
    if args.suite == "micro" and args.mode.startswith("async-"):
        parser.error("use --suite native for async-aware profiles")
    if args.output is None:
        parser.error("--output must name a new external artifact directory")
    if args.worker_cpu is not None and args.worker_cpu == args.profiler_cpu:
        parser.error("worker and profiler must use different CPUs")
    args.output.mkdir(parents=True, exist_ok=False)
    trials = [
        _trial(args, args.output, index, arm)
        for index, arm in enumerate([False, False] + [False, True, True, False] * args.cycles)
    ]
    baseline = [t["operations_per_s"] for t in trials if not t["instrumented"]]
    sampled = [t["operations_per_s"] for t in trials if t["instrumented"]]
    payload = {
        "diagnostic": True,
        "suite": args.suite,
        "scenario": args.scenario,
        "mode": args.mode,
        "rate_hz": args.rate,
        "runtime": _identity(),
        "worker_cpu": args.worker_cpu,
        "profiler_cpu": args.profiler_cpu,
        "baseline_ops_per_s": statistics.median(baseline),
        "sampled_ops_per_s": statistics.median(sampled),
        "sampled_over_baseline": statistics.median(sampled) / statistics.median(baseline),
        "baseline_max_over_min": max(baseline) / min(baseline),
        "trials": trials,
    }
    (args.output / "summary.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: v for k, v in payload.items() if k != "trials"}))


if __name__ == "__main__":
    main()
