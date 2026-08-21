from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path


def worker(args: argparse.Namespace) -> None:
    sys.path.insert(0, str(Path(args.root) / "src"))
    from mqttium.api import AsyncClient
    from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
    from mqttium.packets import PublishPacket
    from mqttium.types import Properties

    proto = MQTTProtocolVersion.MQTTv311 if args.protocol == "311" else MQTTProtocolVersion.MQTTv5
    props = None
    if args.props:
        props = Properties()
        props.set("content_type", "application/json")
        props.set("user_property", [("schema", "telemetry.v1"), ("source", "arm64")])
    frame = PublishPacket(
        topic="bench/org/site/device/telemetry/temp",
        payload=b"x" * args.payload,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=props,
    ).encode(proto)

    class Transport:
        def __init__(self) -> None:
            self.left = args.count
            self.closed = False
            self.full = frame * args.batch

        async def read(self, _n: int = 65536) -> bytes:
            if self.left <= 0:
                self.closed = True
                return b""
            n = min(args.batch, self.left)
            self.left -= n
            await asyncio.sleep(0)
            return self.full if n == args.batch else frame * n

        def is_closing(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    async def run() -> dict[str, float | int]:
        seen = 0
        if args.compat:
            from mqttium.compat.paho import CallbackAPIVersion, Client

            facade = Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=proto)

            def callback(_client, _userdata, _message) -> None:
                nonlocal seen
                seen += 1

            facade.on_message = callback
            client = facade._async
        else:
            client = AsyncClient(
                protocol=proto,
                message_delivery="callback",
                max_pending_callbacks=max(1024, args.batch * 8),
            )

            def callback(_message) -> None:
                nonlocal seen
                seen += 1

            client.on_message = callback

        client._engine.state = ConnectionState.CONNECTED
        client._transport = Transport()
        t0 = time.perf_counter()
        c0 = time.process_time()
        await client._read_loop()
        wall = time.perf_counter() - t0
        cpu = time.process_time() - c0
        if seen != args.count:
            raise RuntimeError(f"delivery mismatch {seen}/{args.count}")
        return {
            "rate": args.count / wall,
            "cpu_us": cpu / args.count * 1e6,
            "wall": wall,
            "seen": seen,
            "frame_bytes": len(frame),
        }

    print(json.dumps(asyncio.run(run())))


def run_pairs(
    *,
    base: str,
    candidate: str,
    protocol: str,
    batch: int,
    count: int,
    payload: int,
    repeat: int,
    props: bool,
    compat: bool,
    python: str,
) -> dict[str, object]:
    vals = {"base": [], "candidate": []}
    cpus = {"base": [], "candidate": []}
    ratios: list[float] = []
    roots = {"base": base, "candidate": candidate}
    for i in range(repeat):
        order = ["base", "candidate"] if i % 2 == 0 else ["candidate", "base"]
        pair: dict[str, dict[str, float]] = {}
        for side in order:
            cmd = [
                "taskset", "-c", "2", python, __file__, "worker",
                "--root", roots[side], "--protocol", protocol,
                "--batch", str(batch), "--count", str(count), "--payload", str(payload),
            ]
            if props:
                cmd.append("--props")
            if compat:
                cmd.append("--compat")
            proc = subprocess.run(cmd, text=True, capture_output=True, check=True)
            data = json.loads(proc.stdout.strip().splitlines()[-1])
            pair[side] = data
            vals[side].append(data["rate"])
            cpus[side].append(data["cpu_us"])
        ratios.append(pair["candidate"]["rate"] / pair["base"]["rate"])

    def cv(items: list[float]) -> float:
        return statistics.stdev(items) / statistics.mean(items) * 100 if len(items) > 1 else 0.0

    return {
        "base_median": statistics.median(vals["base"]),
        "candidate_median": statistics.median(vals["candidate"]),
        "base_cv_pct": cv(vals["base"]),
        "candidate_cv_pct": cv(vals["candidate"]),
        "ratio_median": statistics.median(ratios),
        "ratio_min": min(ratios),
        "ratio_max": max(ratios),
        "wins": sum(r > 1.0 for r in ratios),
        "repeat": repeat,
        "cpu_ratio_median": statistics.median(cpus["candidate"]) / statistics.median(cpus["base"]),
        "ratios": ratios,
    }


def matrix(args: argparse.Namespace) -> None:
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    aa_valid = None
    for attempt in (1, 2):
        aa = run_pairs(
            base=args.base,
            candidate=args.base,
            protocol="311",
            batch=32,
            count=80000,
            payload=256,
            repeat=9,
            props=False,
            compat=False,
            python=args.python,
        )
        (out / f"aa-{attempt}.json").write_text(json.dumps(aa, indent=2) + "\n")
        if (
            0.97 <= aa["ratio_median"] <= 1.03
            and aa["base_cv_pct"] <= 5.0
            and aa["candidate_cv_pct"] <= 5.0
        ):
            aa_valid = aa
            break
    if aa_valid is None:
        raise SystemExit("A/A control invalid after two attempts")
    (out / "aa.json").write_text(json.dumps(aa_valid, indent=2) + "\n")

    cases = {
        "v311-b32": dict(protocol="311", batch=32, count=100000, payload=256, props=False, compat=False),
        "v311-b256": dict(protocol="311", batch=256, count=100000, payload=256, props=False, compat=False),
        "v5-b32": dict(protocol="5", batch=32, count=70000, payload=256, props=False, compat=False),
        "v5-props-b32": dict(protocol="5", batch=32, count=50000, payload=256, props=True, compat=False),
        "v311-16k": dict(protocol="311", batch=32, count=10000, payload=16384, props=False, compat=False),
        "v5-16k": dict(protocol="5", batch=32, count=8000, payload=16384, props=False, compat=False),
        "compat-v311": dict(protocol="311", batch=32, count=70000, payload=256, props=False, compat=True),
        "compat-v5-props": dict(protocol="5", batch=32, count=40000, payload=256, props=True, compat=True),
    }
    gates = {
        "v311-b32": (1.10, 9),
        "v311-b256": (1.10, 9),
        "v5-b32": (1.08, 9),
        "v5-props-b32": (1.05, 9),
        "v311-16k": (0.98, 7),
        "v5-16k": (0.98, 7),
        "compat-v311": (1.08, 9),
        "compat-v5-props": (1.03, 8),
    }
    failed: list[str] = []
    lines: list[str] = []
    for name, spec in cases.items():
        result = run_pairs(
            base=args.base,
            candidate=args.candidate,
            repeat=11,
            python=args.python,
            **spec,
        )
        (out / f"{name}.json").write_text(json.dumps(result, indent=2) + "\n")
        minimum, wins = gates[name]
        ok = result["ratio_median"] >= minimum and result["wins"] >= wins
        lines.append(
            f"{name}: ratio={result['ratio_median']:.4f}, wins={result['wins']}/{result['repeat']}, "
            f"cpu={result['cpu_ratio_median']:.4f}, CV={result['base_cv_pct']:.2f}%/"
            f"{result['candidate_cv_pct']:.2f}% -> {'PASS' if ok else 'FAIL'}"
        )
        if not ok:
            failed.append(name)
    summary = "\n".join(lines) + "\n"
    (out / "summary.txt").write_text(summary)
    print(summary)
    if failed:
        raise SystemExit("failed gates: " + ", ".join(failed))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--root", required=True)
    w.add_argument("--protocol", choices=["311", "5"], required=True)
    w.add_argument("--batch", type=int, required=True)
    w.add_argument("--count", type=int, required=True)
    w.add_argument("--payload", type=int, required=True)
    w.add_argument("--props", action="store_true")
    w.add_argument("--compat", action="store_true")

    m = sub.add_parser("matrix")
    m.add_argument("--base", required=True)
    m.add_argument("--candidate", required=True)
    m.add_argument("--python", required=True)
    m.add_argument("--output-dir", required=True)

    args = parser.parse_args()
    if args.command == "worker":
        worker(args)
    else:
        matrix(args)


if __name__ == "__main__":
    main()
