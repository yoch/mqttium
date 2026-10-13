"""Build isolated Cython experiments and measure them against unchanged Python.

Requires Cython and setuptools only in the development environment. The prepare-wheel action compares the complete native codec and its Python
reference from the same wheel. The build action retains the initial per-module
experiment. See the dated reports under docs/reports/.
"""

from __future__ import annotations

import argparse
import cProfile
import hashlib
import importlib
import importlib.machinery
import json
import os
import platform
import pstats
import shutil
import statistics
import subprocess
import sys
import sysconfig
import time
from zipfile import ZipFile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEAVES = ("codec.vbi", "codec.primitives", "codec.properties", "codec.packet_validation", "topics")
WIRE = (*LEAVES, "codec.buffer", "packets._publish", "packets._ack", "packets._common")
GROUPS = {
    "python": (),
    "native": (),
    "leaves": LEAVES,
    "wire": WIRE,
    "packets": tuple(name for name in WIRE if name != "codec.buffer"),
    "matcher": ("dispatch.matcher",),
    "combined": (*WIRE, "dispatch.matcher"),
}
DEFAULT_CASES = (
    "encode_qos0",
    "encode_qos1_v5",
    "ingress_engine_qos0",
    "ingress_engine_qos0_v5",
    "ingress_publish_qos1",
    "qos1_cycle_memory",
    "native_publish_nowait_qos0",
    "dispatch_match_exact",
    "dispatch_match_100",
    "frame_64",
    "frame_4096",
    "frame_fragmented",
    "properties_v5",
    "vbi_four",
)
NETWORK_CASES = tuple(f"network_{version}_qos{qos}" for version in (311, 5) for qos in (0, 1, 2))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def build(destination):
    from Cython import __version__ as cython_version
    from Cython.Build import cythonize
    from setuptools import Distribution, Extension

    # Refuse to overwrite previous evidence or accidentally build in src/.
    destination.mkdir(parents=True, exist_ok=False)
    source = destination / "build-source"
    shutil.copytree(ROOT / "src", source, ignore=shutil.ignore_patterns("__pycache__", "*.so"))
    extensions = [
        Extension("mqttium." + name, [str(source / "mqttium" / (name.replace(".", "/") + ".py"))])
        for name in GROUPS["combined"]
    ]
    directives = {"language_level": 3, "annotation_typing": False, "binding": True}
    distribution = Distribution(
        {
            "name": "mqttium-compiled-experiment",
            "ext_modules": cythonize(extensions, compiler_directives=directives),
        }
    )
    command = distribution.get_command_obj("build_ext")
    command.build_lib = str(destination / "extensions")
    command.build_temp = str(destination / "objects")
    distribution.run_command("build_ext")
    hashes = {}
    for variant, modules in GROUPS.items():
        if variant == "native":
            continue
        target = destination / variant / "src"
        shutil.copytree(ROOT / "src", target, ignore=shutil.ignore_patterns("__pycache__", "*.so"))
        for name in modules:
            relative = Path("mqttium") / name.replace(".", "/")
            artifact = (
                destination
                / "extensions"
                / relative.with_suffix(sysconfig.get_config_var("EXT_SUFFIX"))
            )
            shutil.copy2(artifact, target / artifact.relative_to(destination / "extensions"))
            hashes[name] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    write_json(
        destination / "manifest.json",
        {
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "python": sys.version,
            "platform": platform.platform(),
            "cython": cython_version,
            "compiler": subprocess.check_output(["gcc", "--version"], text=True).splitlines()[0],
            "directives": directives,
            "groups": GROUPS,
            "extension_sha256": hashes,
            "source_sha256": {
                str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted((ROOT / "src").rglob("*.py"))
            },
        },
    )


def prepare_wheel(destination, wheel):
    """Compare exactly the same installed bytes with an explicit backend choice."""
    destination.mkdir(parents=True, exist_ok=False)
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        if not any(
            "mqttium/codec/_native." in name and name.endswith((".so", ".pyd")) for name in names
        ):
            raise ValueError("Expected a compiled codec wheel")
        for variant in ("python", "native"):
            archive.extractall(destination / variant / "src")
    write_json(
        destination / "manifest.json",
        {
            "wheel": wheel.name,
            "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "python": sys.version,
            "platform": platform.platform(),
            "backend_selection": "MQTTIUM_CODEC_BACKEND, same wheel in both arms",
        },
    )


def custom_case(name):
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
    from mqttium.codec.vbi import decode_vbi
    from mqttium.packets import PublishPacket
    from mqttium.types import Properties

    if name == "vbi_four":
        return lambda: decode_vbi(b"\xff\xff\xff\x7f"), 1
    if name == "properties_v5":
        wire = encode_properties(
            Properties(
                {
                    "content_type": "application/json",
                    "user_property": (("site", "north"), ("sensor", "temperature")),
                    "message_expiry_interval": 60,
                }
            ),
            PUBLISH,
        )
        return lambda: decode_properties(wire, 0, PUBLISH), 1
    size = 4096 if name == "frame_4096" else 64
    wire = PublishPacket(
        topic="bench/sensors/temp", payload=b"x" * size, qos=0, retain=False, dup=False
    ).encode()
    decoder = IncrementalDecoder()
    batch = wire * 32
    fragments = (
        [batch[i : i + 13] for i in range(0, len(batch), 13)]
        if name == "frame_fragmented"
        else [batch]
    )

    def run():
        count = 0
        for fragment in fragments:
            decoder.feed(fragment)
            while decoder.next_packet() is not None:
                count += 1
        if count != 32:
            raise AssertionError(count)

    return run, 32


def load_backend(args):
    os.environ["MQTTIUM_CODEC_BACKEND"] = "native" if args.variant == "native" else "python"
    os.sched_setaffinity(0, {args.cpu})
    sys.path.insert(0, str(args.root / "src"))
    loaded = {}
    for name in GROUPS["combined"]:
        module = importlib.import_module("mqttium." + name)
        path = Path(module.__file__).resolve()
        if not path.is_relative_to(args.root.resolve()):
            raise AssertionError(f"Wrong source imported: {path}")
        compiled = any(str(path).endswith(s) for s in importlib.machinery.EXTENSION_SUFFIXES)
        if compiled != (name in GROUPS[args.variant]):
            raise AssertionError(f"Wrong backend: {path}")
        loaded[name] = str(path)
    if args.variant == "native":
        native = importlib.import_module("mqttium.codec._native")
        if not Path(native.__file__).resolve().is_relative_to(args.root.resolve()):
            raise AssertionError("Wrong native extension imported")
        for name, exports in native.EXPORTS.items():
            module = importlib.import_module(name)
            if any(getattr(module, symbol) is not value for symbol, value in exports.items()):
                raise AssertionError(f"Incomplete native binding: {name}")
        loaded["native"] = str(native.__file__)
        loaded["source_digest"] = native.SOURCE_DIGEST
    return loaded


def worker(args):
    loaded = load_backend(args)
    from _paired_scenarios import REGISTRY, ScenarioMeasurement, _measure, run_scenario

    def run():
        if args.case in NETWORK_CASES:
            import asyncio

            from lean_native_compare import _phase

            _, version, qos = args.case.split("_")
            args.scenario = json.dumps(
                {
                    "protocol": int(version),
                    "qos": int(qos[-1]),
                    "store": "memory",
                    "mode": "callback",
                    "burst": 32,
                }
            )

            async def network():
                async with asyncio.timeout(120):
                    await _phase(args, count=1000, trace=False)
                    return await _phase(args, count=args.count, trace=False)

            performance = asyncio.run(network())
            result = ScenarioMeasurement(
                performance["elapsed_s"], args.count, performance["delivered_per_s"]
            )
            result.cpu_s = performance["cpu_s"]
            result.network = performance
            return result
        if args.case in REGISTRY:
            # Preserve the existing workload while timing CPU over the same window.
            import _paired_scenarios as scenarios

            def measure(fn, *, operations, warmup):
                operations *= args.scale
                for _ in range(warmup):
                    fn()
                cpu = time.process_time()
                start = time.perf_counter()
                for _ in range(operations):
                    fn()
                elapsed = time.perf_counter() - start
                result = ScenarioMeasurement(elapsed, operations, operations / elapsed)
                result.cpu_s = time.process_time() - cpu
                return result

            scenarios._measure = measure
            if args.case == "native_publish_nowait_qos0":
                import asyncio

                from mqttium.api import AsyncClient
                from mqttium.enums import ConnectionState

                async def native():
                    client = AsyncClient(client_id="compiled-study")
                    scenarios._install_discard_writer(client)
                    client._engine.state = ConnectionState.CONNECTED
                    return measure(
                        lambda: client.publish_nowait(scenarios.TOPIC, b"x", qos=0),
                        operations=60_000,
                        warmup=2_000,
                    )

                return asyncio.run(native())
            return run_scenario(args.case)
        fn, per_call = custom_case(args.case)
        # Calibrate outside the measured window, with a minimum 0.2 s sample.
        probe = _measure(fn, operations=1000, warmup=100)
        operations = max(1000, int(0.25 * 1000 / probe.elapsed_s)) * args.scale
        cpu = time.process_time()
        start = time.perf_counter()
        for _ in range(operations):
            fn()
        elapsed = time.perf_counter() - start
        result = ScenarioMeasurement(
            elapsed, operations * per_call, operations * per_call / elapsed
        )
        result.cpu_s = time.process_time() - cpu
        return result

    if args.profile:
        profiler = cProfile.Profile()
        result = profiler.runcall(run)
        stats = pstats.Stats(profiler)
        profile = [
            {"file": k[0], "line": k[1], "function": k[2], "self_s": v[2], "cumulative_s": v[3]}
            for k, v in sorted(stats.stats.items(), key=lambda x: x[1][2], reverse=True)[:30]
        ]
    else:
        result, profile = run(), None
    print(
        json.dumps(
            {
                "case": args.case,
                "cpu_ns_op": result.cpu_s * 1e9 / result.operations,
                "wall_ns_op": result.elapsed_s * 1e9 / result.operations,
                "operations": result.operations,
                "loaded": loaded,
                "profile": profile,
                "network": getattr(result, "network", None),
            }
        )
    )


def compare(args):
    rows = []
    for case in args.cases.split(","):
        pairs = []
        for repeat in range(args.repeat):
            pair = {}
            order = ("base", "candidate") if repeat % 2 == 0 else ("candidate", "base")
            for arm in order:
                variant = "python" if arm == "base" else args.variant
                command = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "worker",
                    "--root",
                    str(args.root / variant),
                    "--variant",
                    variant,
                    "--case",
                    case,
                    "--cpu",
                    str(args.cpu),
                    "--port",
                    str(args.port),
                    "--host",
                    args.host,
                    "--count",
                    str(args.count),
                    "--scale",
                    str(args.scale),
                ]
                completed = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "PYTHONHASHSEED": "0"},
                    timeout=120,
                )
                pair[arm] = json.loads(completed.stdout)
            pairs.append({"order": order, **pair})
        ratios = [p["base"]["cpu_ns_op"] / p["candidate"]["cpu_ns_op"] for p in pairs]
        rates = {arm: [p[arm]["wall_ns_op"] for p in pairs] for arm in ("base", "candidate")}
        summary = {
            "case": case,
            "cpu_speedup": statistics.median(ratios),
            "cpu_reduction_pct": 100 * (1 - 1 / statistics.median(ratios)),
            "ratio_range": [min(ratios), max(ratios)],
            "wall_speedup": statistics.median(
                [p["base"]["wall_ns_op"] / p["candidate"]["wall_ns_op"] for p in pairs]
            ),
            "wall_cv": {
                arm: statistics.stdev(v) / statistics.mean(v) if len(v) > 1 else 0
                for arm, v in rates.items()
            },
            "pairs": pairs,
        }
        rows.append(summary)
        write_json(
            args.output,
            {
                "variant": args.variant,
                "cpu": args.cpu,
                "repeat": args.repeat,
                "scale": args.scale,
                "manifest": json.loads((args.root / "manifest.json").read_text()),
                "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "rows": rows,
            },
        )
        print(
            f"{case}: CPU x{summary['cpu_speedup']:.3f}, {summary['cpu_reduction_pct']:.1f}% less",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "prepare-wheel", "worker", "compare"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--variant", choices=GROUPS, default="wire")
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--case", choices=(*DEFAULT_CASES, *NETWORK_CASES), default="encode_qos0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18885)
    parser.add_argument("--count", type=int, default=12000)
    parser.add_argument("--scale", type=int, default=1, help="multiply timed microbenchmark work")
    parser.add_argument("--cases", default=",".join(DEFAULT_CASES))
    parser.add_argument("--repeat", type=int, default=6)
    parser.add_argument("--output", type=Path, default=Path("/tmp/compiled-modules.json"))
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare-wheel" and args.wheel is None:
        parser.error("prepare-wheel requires --wheel")
    if args.repeat < 2 or args.repeat % 2:
        parser.error("--repeat must be a positive even count for ABBA")
    if args.scale < 1:
        parser.error("--scale must be positive")
    {
        "build": lambda: build(args.root.resolve()),
        "prepare-wheel": lambda: prepare_wheel(args.root.resolve(), args.wheel),
        "worker": lambda: worker(args),
        "compare": lambda: compare(args),
    }[args.action]()


if __name__ == "__main__":
    main()
