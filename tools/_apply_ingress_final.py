from pathlib import Path

p = Path("src/mqttium/codec/vbi.py")
s = p.read_text()
old = '''def decode_vbi(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    """Decode a canonical MQTT VBI starting at *offset*.

    Returns ``(value, new_offset)``. MQTT requires the shortest possible
    representation, so encodings such as ``80 00`` are malformed even though
    they numerically represent zero.
    """
    length = len(buffer)
'''
new = '''def decode_vbi(
    buffer: bytes | bytearray | memoryview,
    offset: int = 0,
    *,
    end: int | None = None,
) -> tuple[int, int]:
    """Decode a canonical MQTT VBI starting at *offset*.

    ``end`` can bound the logical readable extent when *buffer* is a reusable
    capacity slab whose physical length exceeds its committed bytes. Returns
    ``(value, new_offset)``. MQTT requires the shortest possible representation,
    so encodings such as ``80 00`` are malformed even though they numerically
    represent zero.
    """
    length = len(buffer) if end is None else min(len(buffer), end)
'''
assert s.count(old) == 1
p.write_text(s.replace(old, new, 1))

p = Path("src/mqttium/codec/buffer.py")
s = p.read_text()
old = "remaining, rl_end = decode_vbi(self._buf, self._start + 1)"
new = "remaining, rl_end = decode_vbi(self._buf, self._start + 1, end=self._end)"
assert s.count(old) == 1
p.write_text(s.replace(old, new, 1))

p = Path("src/mqttium/transport/_push.py")
s = p.read_text()
old = '''    ``receive()`` is edge-triggered on selector callbacks; buffered partial MQTT
    data is not itself a readiness condition.  The temporary ``read`` override
    is kept only for compatibility with the current AsyncTransport protocol and
    fails loudly; the follow-up transport-contract commit removes that method
    from the capability entirely.
'''
new = '''    ``receive()`` is edge-triggered on selector callbacks; buffered partial MQTT
    data is not itself a readiness condition.  This class intentionally has no
    ``read()`` method: decoder ingress is a distinct receive capability rather
    than a byte-stream transport with altered semantics.
'''
assert s.count(old) == 1
p.write_text(s.replace(old, new, 1))

p = Path("tests/unit/test_decoder_ingress_storage.py")
s = p.read_text()
extra = '''\n\ndef test_vbi_decoder_respects_logical_end_of_capacity_slab() -> None:\n    from mqttium.codec.vbi import decode_vbi\n    from mqttium.errors import MalformedPacketError\n\n    slab = bytearray(64 * 1024)\n    slab[:2] = b"\\x30\\x80"\n    try:\n        decode_vbi(slab, 1, end=2)\n    except MalformedPacketError as exc:\n        assert "Incomplete" in str(exc)\n    else:\n        raise AssertionError("uncommitted slab capacity participated in VBI decode")\n'''
assert "test_vbi_decoder_respects_logical_end_of_capacity_slab" not in s
p.write_text(s + extra)
