from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"missing patch anchor in {path}: {old[:100]!r}")
    file.write_text(text.replace(old, new, 1))


replace(
    "src/mqttium/codec/buffer.py",
    "from collections.abc import Callable\nfrom dataclasses import dataclass\n",
    "from collections.abc import Callable\nfrom dataclasses import dataclass\nfrom typing import TypeVar, cast\n",
)
replace(
    "src/mqttium/codec/buffer.py",
    """DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024
_COMPACT_THRESHOLD = 64 * 1024
""",
    """DEFAULT_MAX_PACKET_SIZE = 16 * 1024 * 1024
_COMPACT_THRESHOLD = 64 * 1024
# Above this body size a transient memoryview avoids the extra bytearray slice
# allocated by ``bytes(buf[a:b])``. Small frames keep the cheaper simple path.
_VIEW_COPY_THRESHOLD = 4096
# A large PUBLISH can be parsed while the decoder owns a transient view, so its
# payload is copied directly into final immutable bytes exactly once.
_DIRECT_PUBLISH_THRESHOLD = 4096

PublishT = TypeVar("PublishT")
""",
)
old_next = '''    def next_packet(self) -> RawPacket | None:
        buf = self._buf
        start = self._start
        available = len(buf) - start
        if available < 2:
            return None

        header = buf[start]
        try:
            remaining_length, rl_end = decode_vbi(buf, start + 1)
        except MalformedPacketError:
            # Incomplete VBI — need more bytes unless clearly malformed length.
            if available >= 5:
                raise
            # Distinguish "need more" from "too long": if we have continuation
            # bits on all available bytes and < 5 total header bytes, wait.
            if all(buf[start + i] & 0x80 for i in range(1, available)):
                return None
            raise

        fixed_header_len = rl_end - start
        total = fixed_header_len + remaining_length
        if total > self._max_packet_size:
            raise PacketTooLargeError(
                f"Packet size {total} exceeds maximum {self._max_packet_size}"
            )
        if available < total:
            return None

        packet_type = PacketType.from_byte(header)
        flags = header & 0x0F
        body_start = start + fixed_header_len
        body_end = start + total
        # Copy body out so callers never alias the reusable buffer.
        body = bytes(buf[body_start:body_end])
        self._start = body_end
        if self._start == len(self._buf):
            # Reuse the buffer object (avoid allocating a fresh bytearray).
            self._buf.clear()
            self._start = 0
        return RawPacket(packet_type=packet_type, flags=flags, remaining=body)
'''
new_next = '''    def next_packet(self) -> RawPacket | None:
        result = self._next_packet()
        if result is None:
            return None
        packet, _ = result
        assert isinstance(packet, RawPacket)
        return packet

    def _next_packet(
        self,
        publish_decoder: Callable[[int, memoryview], object] | None = None,
    ) -> tuple[object, int] | None:
        buf = self._buf
        start = self._start
        available = len(buf) - start
        if available < 2:
            return None

        header = buf[start]
        try:
            remaining_length, rl_end = decode_vbi(buf, start + 1)
        except MalformedPacketError:
            if available >= 5:
                raise
            if all(buf[start + i] & 0x80 for i in range(1, available)):
                return None
            raise

        fixed_header_len = rl_end - start
        total = fixed_header_len + remaining_length
        if total > self._max_packet_size:
            raise PacketTooLargeError(
                f"Packet size {total} exceeds maximum {self._max_packet_size}"
            )
        if available < total:
            return None

        packet_type = PacketType.from_byte(header)
        flags = header & 0x0F
        body_start = start + fixed_header_len
        body_end = start + total

        packet: object
        if (
            packet_type is PacketType.PUBLISH
            and publish_decoder is not None
            and remaining_length >= _DIRECT_PUBLISH_THRESHOLD
        ):
            parent_view = memoryview(buf)
            body_view = parent_view[body_start:body_end]
            try:
                try:
                    packet = publish_decoder(flags, body_view)
                except MalformedPacketError:
                    # Preserve the existing error boundary: malformed typed
                    # packets still reach ProtocolEngine as an owned RawPacket.
                    packet = RawPacket(
                        packet_type=packet_type,
                        flags=flags,
                        remaining=bytes(body_view),
                    )
            finally:
                body_view.release()
                parent_view.release()
        else:
            # ``bytes(bytearray[a:b])`` copies twice. A transient view is worth
            # its object cost only for larger bodies; it is released before the
            # reusable bytearray is cleared or resized.
            if remaining_length >= _VIEW_COPY_THRESHOLD:
                view = memoryview(buf)
                try:
                    body = bytes(view[body_start:body_end])
                finally:
                    view.release()
            else:
                body = bytes(buf[body_start:body_end])
            packet = RawPacket(packet_type=packet_type, flags=flags, remaining=body)

        self._start = body_end
        if self._start == len(self._buf):
            self._buf.clear()
            self._start = 0
        return packet, total
'''
replace("src/mqttium/codec/buffer.py", old_next, new_next)
old_bounded = '''    def process_packets_bounded(
        self,
        callback: Callable[[RawPacket], None],
        *,
        limit: int,
        max_bytes: int,
    ) -> tuple[int, int]:
        """Decode one count- and byte-bounded packet batch.

        ``max_bytes`` is a target rather than a packet-size limit: the packet
        that reaches the target is included, so one individually larger packet
        is always allowed to make progress. Five bytes of fixed-header overhead
        are conservatively charged per packet.
        """
        if limit <= 0:
            raise ValueError("limit must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        count = 0
        decoded_bytes = 0
        for _ in range(limit):
            packet = self.next_packet()
            if packet is None:
                break
            callback(packet)
            count += 1
            decoded_bytes += len(packet.remaining) + 5
            if decoded_bytes >= max_bytes:
                break
        return count, decoded_bytes
'''
new_bounded = '''    def process_packets_bounded(
        self,
        callback: Callable[[RawPacket], None],
        *,
        limit: int,
        max_bytes: int,
    ) -> tuple[int, int]:
        """Decode one count- and byte-bounded packet batch."""
        return self._process_packets_bounded(
            cast(Callable[[object], None], callback),
            limit=limit,
            max_bytes=max_bytes,
            publish_decoder=None,
        )

    def process_packets_bounded_with_publish(
        self,
        callback: Callable[[RawPacket | PublishT], None],
        *,
        publish_decoder: Callable[[int, memoryview], PublishT],
        limit: int,
        max_bytes: int,
    ) -> tuple[int, int]:
        """Decode a batch with direct parsing for large inbound PUBLISH frames.

        The decoder-owned memoryview exists only during ``publish_decoder``.
        The returned object must own every value it retains.
        """
        return self._process_packets_bounded(
            cast(Callable[[object], None], callback),
            limit=limit,
            max_bytes=max_bytes,
            publish_decoder=cast(Callable[[int, memoryview], object], publish_decoder),
        )

    def _process_packets_bounded(
        self,
        callback: Callable[[object], None],
        *,
        limit: int,
        max_bytes: int,
        publish_decoder: Callable[[int, memoryview], object] | None,
    ) -> tuple[int, int]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        count = 0
        decoded_bytes = 0
        for _ in range(limit):
            result = self._next_packet(publish_decoder)
            if result is None:
                break
            packet, packet_bytes = result
            callback(packet)
            count += 1
            decoded_bytes += packet_bytes
            if decoded_bytes >= max_bytes:
                break
        return count, decoded_bytes
'''
replace("src/mqttium/codec/buffer.py", old_bounded, new_bounded)

replace(
    "src/mqttium/packets/publish.py",
    """        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
""",
    """        remaining: bytes | bytearray | memoryview,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
""",
)
replace(
    "src/mqttium/packets/publish.py",
    """        # The remainder is the PUBLISH payload and may be empty.
        payload = remaining[pos:]
""",
    """        # The remainder is the PUBLISH payload and may be empty. A direct
        # decoder view is copied once into final immutable application bytes;
        # the legacy owned-body path keeps its normal bytes slice.
        payload = remaining[pos:] if isinstance(remaining, bytes) else bytes(remaining[pos:])
""",
)
replace(
    "src/mqttium/codec/properties.py",
    "def _decode_value(ptype: PropType, buf: bytes | bytearray, offset: int) -> tuple[Any, int]:",
    "def _decode_value(\n    ptype: PropType, buf: bytes | bytearray | memoryview, offset: int\n) -> tuple[Any, int]:",
)
replace(
    "src/mqttium/codec/properties.py",
    """def decode_properties(
    buf: bytes | bytearray,
""",
    """def decode_properties(
    buf: bytes | bytearray | memoryview,
""",
)

replace(
    "src/mqttium/protocol/inbound.py",
    '''    def on_publish(self, raw: RawPacket) -> None:
        """Handle one inbound PUBLISH without adding an engine wrapper frame."""
        engine = self._engine
        config = self.config
        store = self.store
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
        topic = self._resolve_topic(packet)
''',
    '''    def on_publish(self, raw: RawPacket) -> None:
        """Decode the ordinary owned-body path, then handle one PUBLISH."""
        self.on_publish_packet(
            PublishPacket.decode(raw.flags, raw.remaining, self.config.protocol)
        )

    def on_publish_packet(self, packet: PublishPacket) -> None:
        """Handle a fully owned PUBLISH from either ingress decode path."""
        engine = self._engine
        config = self.config
        store = self.store
        topic = self._resolve_topic(packet)
''',
)

replace(
    "src/mqttium/protocol/engine.py",
    """    DisconnectPacket,
    SubAckPacket,
""",
    """    DisconnectPacket,
    PublishPacket,
    SubAckPacket,
""",
)
replace(
    "src/mqttium/protocol/engine.py",
    """    def handle_raw(self, raw: RawPacket) -> None:
""",
    '''    def handle_packet(self, packet: RawPacket | PublishPacket) -> None:
        """Dispatch an owned raw packet or a directly decoded large PUBLISH."""
        if isinstance(packet, RawPacket):
            self.handle_raw(packet)
            return
        try:
            allowed = _ALLOWED_PACKETS_BY_STATE.get(self.state, frozenset())
            if PacketType.PUBLISH not in allowed:
                raise ProtocolError(
                    f"Unexpected PUBLISH while state={self.state.name}"
                )
            self.inbound.on_publish_packet(packet)
        except (ProtocolError, MalformedPacketError) as exc:
            self._emit(EffectKind.PROTOCOL_ERROR, str(exc))
        except Exception as exc:
            self._emit(EffectKind.PROTOCOL_ERROR, f"Internal handler error: {exc!r}")

    def handle_raw(self, raw: RawPacket) -> None:
''',
)

replace(
    "src/mqttium/api/async_client.py",
    """    async def _read_loop(self) -> None:
""",
    '''    def _decode_inbound_publish(self, flags: int, remaining: memoryview) -> PublishPacket:
        return PublishPacket.decode(flags, remaining, self._engine.config.protocol)

    async def _read_loop(self) -> None:
''',
)
replace(
    "src/mqttium/api/async_client.py",
    """                            handled, handled_bytes = self._decoder.process_packets_bounded(
                                self._engine.handle_raw,
                                limit=256,
                                max_bytes=self._max_ingress_batch_bytes,
                            )
""",
    """                            handled, handled_bytes = (
                                self._decoder.process_packets_bounded_with_publish(
                                    self._engine.handle_packet,
                                    publish_decoder=self._decode_inbound_publish,
                                    limit=256,
                                    max_bytes=self._max_ingress_batch_bytes,
                                )
                            )
""",
)

replace(
    "docs/ROADMAP.md",
    """- [ ] Specialised in-buffer PUBLISH decode, if profiling still shows
      payload-sized copies as a material peak after ingress batching.""",
    """- [x] Specialised in-buffer PUBLISH decode for large frames, retaining the
      ordinary raw-packet path for small frames and malformed-packet isolation.""",
)
replace(
    "CHANGELOG.md",
    "### Changed\n",
    """### Changed

- Inbound PUBLISH frames of at least 4 KiB are parsed through a transient view
  of the reusable decoder buffer. The final payload remains immutable owned
  bytes, but the intermediate owned packet-body copy is eliminated. Smaller
  frames retain the existing low-overhead path.
""",
)

Path("tests/unit/test_single_copy_publish_decode.py").write_text(r'''"""Direct large-PUBLISH decoding without exposing the reusable buffer."""

from __future__ import annotations

from mqttium.codec.buffer import (
    _DIRECT_PUBLISH_THRESHOLD,
    IncrementalDecoder,
    RawPacket,
)
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.types import Properties


def decode_publish(protocol: MQTTProtocolVersion):
    return lambda flags, body: PublishPacket.decode(flags, body, protocol)


def make_packet(
    payload: bytes,
    *,
    protocol: MQTTProtocolVersion,
    qos: QoS = QoS.AT_LEAST_ONCE,
) -> PublishPacket:
    properties = None
    if protocol == MQTTProtocolVersion.MQTTv5:
        properties = Properties()
        properties.set("message_expiry_interval", 30)
        properties.add_user_property("mode", "direct")
    return PublishPacket(
        topic="large/direct",
        payload=payload,
        qos=qos,
        retain=True,
        dup=False,
        mid=71 if qos else None,
        properties=properties,
    )


def test_large_publish_is_returned_typed_with_owned_payload() -> None:
    protocol = MQTTProtocolVersion.MQTTv5
    payload = bytes(range(256)) * 32
    expected = make_packet(payload, protocol=protocol)
    decoder = IncrementalDecoder()
    decoder.feed(expected.encode(protocol))
    decoded: list[RawPacket | PublishPacket] = []

    count, decoded_bytes = decoder.process_packets_bounded_with_publish(
        decoded.append,
        publish_decoder=decode_publish(protocol),
        limit=8,
        max_bytes=1 << 20,
    )

    assert count == 1 and decoded_bytes >= len(payload)
    packet = decoded[0]
    assert isinstance(packet, PublishPacket)
    assert packet.topic == expected.topic
    assert packet.payload == payload
    assert type(packet.payload) is bytes
    assert packet.properties == expected.properties

    # A leaked memoryview would make either resize fail with BufferError.
    decoder.feed(expected.encode(protocol))
    decoder.clear()
    assert packet.payload == payload


def test_small_publish_keeps_the_raw_packet_fast_path() -> None:
    payload = b"s" * max(1, _DIRECT_PUBLISH_THRESHOLD // 4)
    packet = make_packet(payload, protocol=MQTTProtocolVersion.MQTTv311)
    decoder = IncrementalDecoder()
    decoder.feed(packet.encode())
    decoded: list[RawPacket | PublishPacket] = []

    decoder.process_packets_bounded_with_publish(
        decoded.append,
        publish_decoder=decode_publish(MQTTProtocolVersion.MQTTv311),
        limit=1,
        max_bytes=1 << 20,
    )

    assert isinstance(decoded[0], RawPacket)
    round_trip = PublishPacket.decode(decoded[0].flags, decoded[0].remaining)
    assert round_trip.payload == payload


def test_malformed_large_publish_falls_back_to_owned_raw_packet() -> None:
    # QoS bits 3 are invalid. Direct typed decoding rejects them, then the
    # decoder returns the normal RawPacket so ProtocolEngine owns error policy.
    body = b"\x00\x01t" + b"x" * _DIRECT_PUBLISH_THRESHOLD
    frame = encode_frame(PacketType.PUBLISH, 0x06, body)
    decoder = IncrementalDecoder()
    decoder.feed(frame)
    decoded: list[RawPacket | PublishPacket] = []

    decoder.process_packets_bounded_with_publish(
        decoded.append,
        publish_decoder=decode_publish(MQTTProtocolVersion.MQTTv311),
        limit=1,
        max_bytes=1 << 20,
    )

    assert isinstance(decoded[0], RawPacket)
    assert decoded[0].flags == 0x06


def test_direct_and_legacy_paths_are_semantically_identical() -> None:
    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        for qos in QoS:
            payload = b"p" * (2 * _DIRECT_PUBLISH_THRESHOLD)
            expected = make_packet(payload, protocol=protocol, qos=qos)
            frame = expected.encode(protocol)

            legacy = IncrementalDecoder()
            legacy.feed(frame)
            raw = legacy.next_packet()
            assert raw is not None
            legacy_packet = PublishPacket.decode(raw.flags, raw.remaining, protocol)

            direct = IncrementalDecoder()
            # Exercise fragmented input without changing ownership semantics.
            for offset in range(0, len(frame), 137):
                direct.feed(frame[offset : offset + 137])
            output: list[RawPacket | PublishPacket] = []
            direct.process_packets_bounded_with_publish(
                output.append,
                publish_decoder=decode_publish(protocol),
                limit=1,
                max_bytes=1 << 20,
            )
            assert output == [legacy_packet]
''')

Path("benchmarks/codec_allocations.py").write_text(r'''"""Allocation profile for ordinary and direct inbound PUBLISH decoding."""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Properties

PAYLOAD_SIZES = (64 * 1024, 1024 * 1024, 8 * 1024 * 1024)


@dataclass
class Sample:
    mode: str
    protocol: str
    payload_bytes: int
    allocated_bytes: int
    copies_of_payload: float
    mib_per_s: float


def frame_for(size: int, protocol: MQTTProtocolVersion) -> bytes:
    properties = None
    if protocol == MQTTProtocolVersion.MQTTv5:
        properties = Properties()
        properties.set("message_expiry_interval", 60)
        properties.add_user_property("trace", "allocation")
    return PublishPacket(
        topic="bench/direct/publish",
        payload=b"z" * size,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=123,
        properties=properties,
    ).encode(protocol)


def decode_once(
    decoder: IncrementalDecoder,
    frame: bytes,
    protocol: MQTTProtocolVersion,
    mode: str,
) -> int:
    decoder.feed(frame)
    if mode == "legacy":
        raw = decoder.next_packet()
        assert raw is not None
        packet = PublishPacket.decode(raw.flags, raw.remaining, protocol)
    else:
        output: list[RawPacket | PublishPacket] = []
        decoder.process_packets_bounded_with_publish(
            output.append,
            publish_decoder=lambda flags, body: PublishPacket.decode(flags, body, protocol),
            limit=1,
            max_bytes=64 * 1024 * 1024,
        )
        packet = output[0]
        assert isinstance(packet, PublishPacket)
    return len(packet.payload)


def measure(size: int, protocol: MQTTProtocolVersion, mode: str) -> Sample:
    frame = frame_for(size, protocol)
    decoder = IncrementalDecoder(max_packet_size=64 * 1024 * 1024)
    for _ in range(3):
        decode_once(decoder, frame, protocol, mode)

    gc.collect()
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    decode_once(decoder, frame, protocol, mode)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    allocated = max(0, peak - before)

    iterations = 30 if size >= 1024 * 1024 else 300
    started = time.perf_counter()
    for _ in range(iterations):
        decode_once(decoder, frame, protocol, mode)
    elapsed = time.perf_counter() - started
    return Sample(
        mode=mode,
        protocol="v5" if protocol == MQTTProtocolVersion.MQTTv5 else "v311",
        payload_bytes=size,
        allocated_bytes=allocated,
        copies_of_payload=allocated / size,
        mib_per_s=(size * iterations / (1024 * 1024)) / elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    samples: list[Sample] = []
    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        for size in PAYLOAD_SIZES:
            for mode in ("legacy", "direct"):
                repeats = [measure(size, protocol, mode) for _ in range(args.repeat)]
                best = min(repeats, key=lambda item: item.allocated_bytes)
                best.mib_per_s = statistics.median(item.mib_per_s for item in repeats)
                samples.append(best)
    result = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "samples": [asdict(sample) for sample in samples],
    }
    print(json.dumps(result, indent=2))
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
''')
