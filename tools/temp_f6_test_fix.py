from pathlib import Path


path = Path("tests/unit/test_qos0_v5_decode_fastpath.py")
text = path.read_text()
old = '@pytest.mark.parametrize(("alias", "maximum"), [(0, 10), (2, 1)])'
new = '@pytest.mark.parametrize(("alias", "maximum"), [(2, 1)])'
if text.count(old) != 1:
    raise SystemExit("invalid-alias parametrization mismatch")
text = text.replace(old, new, 1)
text += r'''


def test_v5_qos0_zero_topic_alias_on_wire_never_delivers() -> None:
    # Topic Alias is property id 0x23 with a two-byte integer value. The
    # outbound encoder correctly refuses zero, so construct the malformed peer
    # packet directly to exercise inbound validation.
    remaining = pack_utf8("sensors/temp") + b"\x03\x23\x00\x00" + b"x"
    engine = _connected(alias_maximum=10)
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0, remaining))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(
        effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
        for effect in effects
    )
'''
path.write_text(text)
