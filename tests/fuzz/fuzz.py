"""Dependency-free, seed-reproducible fuzzer for mqttium.

Targets (jalon E):
  1. codec/properties/packets — mutated wire bytes
  2. engine — stateful protocol sequences (CONNACK/PUBLISH/ACK/alias storms)
  3. WebSocket frame parser — bounded-memory invariants

Oracle (all targets): only MQTTError-family exceptions may escape; no crash,
no unbounded buffer growth, engine invariants hold (mids consistent, flow
bounded, no negative counters).

Run:
  PYTHONPATH=mqttium/src python3 mqttium/tests/fuzz/fuzz.py --seed 1 --iterations 5000
  PYTHONPATH=mqttium/src python3 mqttium/tests/fuzz/fuzz.py --target engine --iterations 20000
"""

from __future__ import annotations

import argparse
import random
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder, RawPacket
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    DisconnectPacket,
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    SubAckPacket,
    UnsubAckPacket,
    encode_frame,
)
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import Properties

# Exceptions a robust client may raise on hostile input. Anything else = bug.
ALLOWED = (MQTTError, ConnectionError)


@dataclass
class FuzzResult:
    target: str
    iterations: int
    crashes: int
    invariant_violations: int
    first_failure: str | None = None


class FuzzLogger:
    """Real-time, parseable fuzz logging (stderr) + optional artifact dump.

    - Progress lines every ``progress_every`` iterations (rate, elapsed).
    - Immediate FAIL lines with full context the moment a crash/invariant hits.
    - On failure, the offending input is written to ``artifacts_dir`` for replay.
    """

    def __init__(
        self,
        target: str,
        iterations: int,
        *,
        seed: int,
        progress_every: int = 1000,
        artifacts_dir: Path | None = None,
        quiet: bool = False,
    ) -> None:
        self.target = target
        self.iterations = iterations
        self.seed = seed
        self.progress_every = max(1, progress_every)
        self.artifacts_dir = artifacts_dir
        self.quiet = quiet
        self._t0 = time.monotonic()
        self._last = 0

    def _emit(self, msg: str) -> None:
        if not self.quiet:
            print(msg, file=sys.stderr, flush=True)

    def start(self) -> None:
        self._emit(f"[START] target={self.target} seed={self.seed} iterations={self.iterations}")

    def progress(self, i: int) -> None:
        if i - self._last >= self.progress_every or i + 1 == self.iterations:
            self._last = i
            elapsed = time.monotonic() - self._t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0.0
            self._emit(
                f"[PROGRESS] target={self.target} iter={i + 1}/{self.iterations} "
                f"rate={rate:,.0f}/s elapsed={elapsed:.1f}s"
            )

    def failure(self, i: int, kind: str, detail: str, artifact: bytes | None) -> str:
        elapsed = time.monotonic() - self._t0
        self._emit(
            f"[FAIL] target={self.target} iter={i} kind={kind} "
            f"seed={self.seed} elapsed={elapsed:.2f}s"
        )
        path_str = ""
        if artifact is not None and self.artifacts_dir is not None:
            self.artifacts_dir.mkdir(parents=True, exist_ok=True)
            path = self.artifacts_dir / f"{self.target}-seed{self.seed}-iter{i}.bin"
            path.write_bytes(artifact)
            path_str = str(path)
            self._emit(f"[ARTIFACT] {path_str}")
        return f"iter {i} ({kind}): {detail}" + (f" | artifact={path_str}" if path_str else "")

    def done(self, result: FuzzResult) -> None:
        elapsed = time.monotonic() - self._t0
        status = "OK" if result.crashes == 0 and result.invariant_violations == 0 else "FAIL"
        self._emit(
            f"[DONE] target={self.target} status={status} iters={result.iterations} "
            f"crashes={result.crashes} invariant_violations={result.invariant_violations} "
            f"elapsed={elapsed:.1f}s"
        )


# ---------------------------------------------------------------------------
# Target 1: codec / properties / packets
# ---------------------------------------------------------------------------


def _mutate(rng: random.Random, data: bytes) -> bytes:
    if not data:
        return bytes([rng.randrange(256)])
    buf = bytearray(data)
    for _ in range(rng.randrange(1, 4)):
        op = rng.randrange(4)
        if op == 0 and buf:
            buf[rng.randrange(len(buf))] = rng.randrange(256)
        elif op == 1 and buf:
            del buf[rng.randrange(len(buf))]
        elif op == 2:
            buf.insert(rng.randrange(len(buf) + 1), rng.randrange(256))
        elif buf:
            buf = buf[: rng.randrange(1, len(buf) + 1)]
    return bytes(buf)


def fuzz_codec(rng: random.Random, iterations: int, logger: FuzzLogger | None = None) -> FuzzResult:
    result = FuzzResult("codec", iterations, 0, 0)
    base_props = Properties()
    base_props.set("payload_format_indicator", 1)
    base_props.set("content_type", "text/plain")
    base_props.add_user_property("k", "v")
    base_wire = encode_properties(base_props, PUBLISH)
    base_pub = PublishPacket(
        topic="a/b", payload=b"hello", qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, mid=7
    ).encode(MQTTProtocolVersion.MQTTv5)

    if logger:
        logger.start()
    for i in range(iterations):
        choice = rng.randrange(3)
        blob = b""
        try:
            if choice == 0:
                # Mutated properties blob
                blob = _mutate(rng, base_wire)
                decode_properties(blob, 0, PUBLISH)
            elif choice == 1:
                # Mutated full PUBLISH frame through the incremental decoder
                blob = _mutate(rng, base_pub)
                dec = IncrementalDecoder(max_packet_size=4096)
                dec.feed(blob)
                while dec.next_packet() is not None:
                    pass
            else:
                # Random structured-ish frame
                ptype = rng.choice(list(PacketType))
                body = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 32)))
                blob = encode_frame(ptype, rng.randrange(16), body)
                dec = IncrementalDecoder(max_packet_size=4096)
                dec.feed(blob)
                while True:
                    pkt = dec.next_packet()
                    if pkt is None:
                        break
                    # Exercise the typed decoder for the type.
                    _decode_by_type(pkt)
        except ALLOWED:
            pass
        except Exception as exc:  # noqa: BLE001
            result.crashes += 1
            detail = f"{exc!r}\n{traceback.format_exc()}"
            if logger:
                msg = logger.failure(i, "crash", detail, bytes(blob))
                if result.first_failure is None:
                    result.first_failure = msg
            elif result.first_failure is None:
                result.first_failure = f"iter {i}: {detail}"
        if logger:
            logger.progress(i)
    if logger:
        logger.done(result)
    return result


def _decode_by_type(pkt: RawPacket) -> None:
    v5 = MQTTProtocolVersion.MQTTv5
    t = pkt.packet_type
    if t is PacketType.PUBLISH:
        PublishPacket.decode(pkt.flags, pkt.remaining, v5)
    elif t is PacketType.CONNACK:
        ConnAckPacket.decode(pkt.remaining, v5)
    elif t is PacketType.PUBACK:
        PubAckPacket.decode(pkt.remaining, v5)
    elif t is PacketType.PUBREC:
        PubRecPacket.decode(pkt.remaining, v5)
    elif t is PacketType.PUBREL:
        PubRelPacket.decode(pkt.remaining, v5)
    elif t is PacketType.PUBCOMP:
        PubCompPacket.decode(pkt.remaining, v5)
    elif t is PacketType.SUBACK:
        SubAckPacket.decode(pkt.remaining, v5)
    elif t is PacketType.UNSUBACK:
        UnsubAckPacket.decode(pkt.remaining, v5)
    elif t is PacketType.DISCONNECT:
        DisconnectPacket.decode(pkt.remaining, v5)
    elif t is PacketType.AUTH:
        AuthPacket.decode(pkt.remaining, v5)


# ---------------------------------------------------------------------------
# Target 2: engine — stateful sequences
# ---------------------------------------------------------------------------


def _rand_publish_frame(rng: random.Random, proto: MQTTProtocolVersion) -> bytes:
    qos = rng.choice([0, 1, 2])
    mid = rng.randrange(0, 4) if qos else None  # include invalid 0 sometimes
    topic = rng.choice(["a/b", "", "x", "t/" + str(rng.randrange(3))])
    props = None
    if proto == MQTTProtocolVersion.MQTTv5 and rng.random() < 0.3:
        props = Properties()
        if rng.random() < 0.5:
            props.set("topic_alias", rng.randrange(0, 3))
    pkt = PublishPacket(
        topic=topic,
        payload=bytes(rng.randrange(256) for _ in range(rng.randrange(0, 16))),
        qos=QoS(qos),
        retain=bool(rng.random() < 0.2),
        dup=bool(rng.random() < 0.3),
        mid=mid,
        properties=props,
    )
    try:
        return pkt.encode(proto)
    except MQTTError:
        return b""


def fuzz_engine(  # noqa: C901
    rng: random.Random, iterations: int, logger: FuzzLogger | None = None
) -> FuzzResult:
    result = FuzzResult("engine", iterations, 0, 0)
    proto = rng.choice([MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
    engine = ProtocolEngine(
        EngineConfig(
            client_id="fuzz",
            protocol=proto,
            local_receive_maximum=rng.choice([1, 4, 100]),
            topic_alias_maximum=rng.choice([0, 2]),
            manual_ack=bool(rng.random() < 0.5),
        )
    )
    dec = IncrementalDecoder(max_packet_size=DEFAULT_MAX_PACKET_SIZE)
    last_wire = b""

    def feed(wire: bytes) -> None:
        nonlocal last_wire
        if not wire:
            return
        last_wire = wire
        dec.feed(wire)
        while True:
            pkt = dec.next_packet()
            if pkt is None:
                break
            engine.handle_raw(pkt)
        engine.take_effects()

    try:
        engine.begin_connect()
        engine.take_effects()
    except ALLOWED:
        pass

    if logger:
        logger.start()
    for i in range(iterations):
        op = rng.randrange(10)
        try:
            if op == 0:
                # CONNACK (sometimes duplicate / out of state)
                body = bytes([rng.randrange(2), rng.choice([0, 0, 0, 5, 135])])
                if proto == MQTTProtocolVersion.MQTTv5:
                    body += b"\x00"
                feed(encode_frame(PacketType.CONNACK, 0, body))
            elif op in (1, 2, 3):
                wire = _rand_publish_frame(rng, proto)
                # Half the time, mutate a valid frame to stress the decoder+engine.
                if wire and rng.random() < 0.5:
                    wire = _mutate(rng, wire)
                feed(wire)
            elif op == 4:
                # Random ACK storm with arbitrary mids
                ack_type = rng.choice(
                    [PacketType.PUBACK, PacketType.PUBREC, PacketType.PUBREL, PacketType.PUBCOMP]
                )
                mid = rng.randrange(0, 5)
                feed(
                    encode_frame(
                        ack_type,
                        0x2 if ack_type is PacketType.PUBREL else 0,
                        mid.to_bytes(2, "big"),
                    )
                )
            elif op == 5:
                # Outbound publish from the app side
                engine.queue_publish(
                    "out/t",
                    b"x",
                    qos=QoS(rng.choice([0, 1, 2])),
                )
                engine.take_effects()
            elif op == 6:
                # SUBACK / UNSUBACK with arbitrary mids
                mid = rng.randrange(0, 5)
                t = rng.choice([PacketType.SUBACK, PacketType.UNSUBACK])
                feed(encode_frame(t, 0, mid.to_bytes(2, "big") + b"\x00"))
            elif op == 7:
                # Transport close + reconnect cycle
                engine.notify_transport_closed()
                engine.take_effects()
                try:
                    engine.begin_connect()
                    engine.take_effects()
                except ALLOWED:
                    pass
            elif op == 8:
                # manual ack random mids
                try:
                    engine.ack(rng.randrange(0, 5))
                except ALLOWED:
                    pass
                engine.take_effects()
            else:
                # PINGRESP / DISCONNECT / AUTH
                t = rng.choice([PacketType.PINGRESP, PacketType.DISCONNECT, PacketType.AUTH])
                feed(encode_frame(t, 0, b"\x00" if t is not PacketType.PINGRESP else b""))
        except ALLOWED:
            pass
        except Exception as exc:  # noqa: BLE001
            result.crashes += 1
            detail = f"op {op}: {exc!r}\n{traceback.format_exc()}"
            if logger:
                msg = logger.failure(i, "crash", detail, last_wire)
                if result.first_failure is None:
                    result.first_failure = msg
            elif result.first_failure is None:
                result.first_failure = f"iter {i} {detail}"

        # Invariants after each step.
        try:
            _check_engine_invariants(engine)
        except AssertionError as exc:
            result.invariant_violations += 1
            detail = f"INVARIANT: {exc}\n{traceback.format_exc()}"
            if logger:
                msg = logger.failure(i, "invariant", detail, last_wire)
                if result.first_failure is None:
                    result.first_failure = msg
            elif result.first_failure is None:
                result.first_failure = f"iter {i} {detail}"
        if logger:
            logger.progress(i)
    if logger:
        logger.done(result)
    return result


def _check_engine_invariants(engine: ProtocolEngine) -> None:
    assert engine.flow.inflight >= 0, "negative flow inflight"
    assert engine.flow.inflight <= engine.flow.limit, "flow inflight exceeds limit"
    assert engine._inbound_inflight >= 0, "negative inbound inflight"

    outbound = list(engine.store.out_items())
    outbound_mids = {msg.mid for msg in outbound}
    expected_mids = outbound_mids | set(engine._pending_sub_mids)
    actual_mids = set(engine.packet_ids._used)
    assert actual_mids == expected_mids, (
        f"packet-id mismatch: actual={sorted(actual_mids)} expected={sorted(expected_mids)}"
    )

    queued_mids = {msg.mid for msg in engine._queued}
    expected_flow = sum(
        1
        for msg in outbound
        if msg.state is not OutboundQoSState.QUEUED and msg.mid not in queued_mids
    )
    assert engine.flow.inflight == expected_flow, (
        f"flow mismatch: actual={engine.flow.inflight} expected={expected_flow}"
    )


# ---------------------------------------------------------------------------
# Target 3: WebSocket frame parser (bounded memory)
# ---------------------------------------------------------------------------


def fuzz_websocket(  # noqa: C901
    rng: random.Random, iterations: int, logger: FuzzLogger | None = None
) -> FuzzResult:
    from mqttium.transport.websocket import _parse_frame

    result = FuzzResult("websocket", iterations, 0, 0)
    max_frame = 1024
    if logger:
        logger.start()
    for i in range(iterations):
        buf = bytearray()
        # Random frame header
        buf.append(rng.randrange(256))
        b1 = rng.randrange(256)
        buf.append(b1)
        ln = b1 & 0x7F
        if ln == 126:
            buf.extend(rng.randrange(65536).to_bytes(2, "big"))
        elif ln == 127:
            buf.extend(rng.randrange(2**64).to_bytes(8, "big"))
        buf.extend(bytes(rng.randrange(256) for _ in range(rng.randrange(0, 64))))
        before = len(buf)
        try:
            _parse_frame(buf, max_frame)
        except ALLOWED:
            pass
        except Exception as exc:  # noqa: BLE001
            result.crashes += 1
            detail = f"{exc!r}\n{traceback.format_exc()}"
            if logger:
                msg = logger.failure(i, "crash", detail, bytes(buf))
                if result.first_failure is None:
                    result.first_failure = msg
            elif result.first_failure is None:
                result.first_failure = f"iter {i}: {detail}"
        if len(buf) > before:
            result.invariant_violations += 1
            detail = f"parser grew input buffer from {before} to {len(buf)}"
            if logger:
                msg = logger.failure(i, "invariant", detail, bytes(buf))
                if result.first_failure is None:
                    result.first_failure = msg
            elif result.first_failure is None:
                result.first_failure = f"iter {i} INVARIANT: {detail}"
        if logger:
            logger.progress(i)
    if logger:
        logger.done(result)
    return result


def _is_plausible_partial(buf: bytearray) -> bool:
    # A partial frame header that _parse_frame would keep waiting on.
    return len(buf) >= 2 and (buf[1] & 0x7F) in (126, 127)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_TARGETS = {
    "codec": fuzz_codec,
    "engine": fuzz_engine,
    "websocket": fuzz_websocket,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--target", choices=[*_TARGETS, "all"], default="all")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Emit a PROGRESS line every N iterations (stderr).",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("mqttium/tests/fuzz/artifacts"),
        help="Directory to write failing inputs for replay.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress real-time logs.")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    targets = list(_TARGETS) if args.target == "all" else [args.target]
    failed = False
    for name in targets:
        logger = FuzzLogger(
            name,
            args.iterations,
            seed=args.seed,
            progress_every=args.progress_every,
            artifacts_dir=args.artifacts_dir,
            quiet=args.quiet,
        )
        res = _TARGETS[name](rng, args.iterations, logger)
        status = "OK" if res.crashes == 0 and res.invariant_violations == 0 else "FAIL"
        print(
            f"[{status}] {res.target}: {res.iterations} iters, "
            f"{res.crashes} crashes, {res.invariant_violations} invariant violations"
        )
        if res.first_failure:
            print("  first failure:\n  " + res.first_failure.replace("\n", "\n  "))
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
