"""Executable conformance checks, each tied to a numbered MQTT statement.

Every test here names the normative statement it exercises and quotes it from
``docs/spec/mqtt-v*-statements.json``, which is extracted verbatim from the
OASIS documents. ``test_quoted_statements_match_the_vendored_specification``
then verifies those quotes are still accurate, so a docstring cannot drift away
from the text it claims to enforce.

This is a sample, not a proof of full conformance — see ``docs/CONFORMANCE.md``
for what is and is not covered.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties

SPEC_DIR = Path(__file__).resolve().parents[2] / "docs" / "spec"


def statements(version: str) -> dict[str, str]:
    data = json.loads((SPEC_DIR / f"mqtt-v{version}-statements.json").read_text(encoding="utf-8"))
    return {item["id"]: item["text"] for item in data["statements"]}


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected(protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="conformance", protocol=protocol), MemoryInflightStore()
    )
    engine.begin_connect()
    remaining = b"\x00\x00"
    if protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, remaining))
    engine.take_effects()
    return engine


def _rejects(engine: ProtocolEngine, wire: bytes) -> bool:
    """True when the engine answers a malformed frame with a protocol error."""
    _feed(engine, wire)
    return any(e.kind is EffectKind.PROTOCOL_ERROR for e in engine.take_effects())


# --------------------------------------------------------------------- sending


def test_mqtt_3_3_4_6_outbound_publish_rejects_a_subscription_identifier() -> None:
    """[MQTT-3.3.4-6] A PUBLISH packet sent from a Client to a Server MUST NOT
    contain a Subscription Identifier.

    The property table cannot enforce this: the identifier is legal on the
    inbound PUBLISH the broker sends us, so the restriction is on the direction
    rather than on the packet type.
    """
    properties = Properties()
    properties.set("subscription_identifier", 7)

    for qos in (QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE):
        engine = _connected()
        with pytest.raises(ProtocolError, match="subscription_identifier"):
            engine.queue_publish("t/x", b"hi", qos=qos, properties=properties)
        assert engine.take_effects() == [], "nothing may reach the wire"
        assert engine.outbound.pending_messages == 0, "nothing may be queued"

    engine = _connected()
    with pytest.raises(ProtocolError, match="subscription_identifier"):
        engine.outbound.prepare_qos0("t/x", b"hi", properties=properties)


def test_inbound_publish_still_carries_a_subscription_identifier() -> None:
    """The mirror of [MQTT-3.3.4-6]: the broker→client direction is legal, and a
    subscription identifier must be delivered to the application untouched."""
    engine = _connected()
    properties = Properties()
    properties.set("subscription_identifier", 7)
    _feed(
        engine,
        PublishPacket(
            topic="in/t",
            payload=b"hello",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            mid=None,
            properties=properties,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    messages = [e.data for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
    assert len(messages) == 1
    assert messages[0].properties is not None
    assert messages[0].properties.get("subscription_identifier") == [7]


def test_mqtt_3_8_3_2_subscribe_requires_at_least_one_filter() -> None:
    """[MQTT-3.8.3-2] The Payload MUST contain at least one Topic Filter and
    Subscription Options pair."""
    engine = _connected()
    with pytest.raises(ProtocolError):
        engine.queue_subscribe([])
    assert engine.take_effects() == []


# ------------------------------------------------------------------- receiving


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
def test_mqtt_3_3_1_4_publish_must_not_have_both_qos_bits_set(
    protocol: MQTTProtocolVersion,
) -> None:
    """[MQTT-3.3.1-4] A PUBLISH Packet MUST NOT have both QoS bits set to 1."""
    engine = _connected(protocol)
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b0110, b"\x00\x01t\x00\x01\x00"))


def test_mqtt_3_3_1_2_qos0_publish_must_not_set_dup() -> None:
    """[MQTT-3.3.1-2] The DUP flag MUST be set to 0 for all QoS 0 messages."""
    engine = _connected()
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b1000, b"\x00\x01t\x00"))


def test_publish_above_qos0_must_carry_a_non_zero_packet_identifier() -> None:
    """The receiving mirror of [MQTT-2.2.1-3]/[MQTT-2.2.1-4]: an identifier of
    zero on a QoS > 0 PUBLISH is not a currently-unused identifier."""
    engine = _connected()
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b0010, b"\x00\x01t\x00\x00\x00"))


@pytest.mark.parametrize(
    ("name", "packet_type", "flags", "body"),
    [
        ("PUBACK", PacketType.PUBACK, 0b0010, b"\x00\x01\x00"),
        ("PUBREL", PacketType.PUBREL, 0b0000, b"\x00\x01\x00"),
        ("SUBACK", PacketType.SUBACK, 0b0100, b"\x00\x01\x00\x00"),
        ("PINGRESP", PacketType.PINGRESP, 0b0001, b""),
        ("DISCONNECT", PacketType.DISCONNECT, 0b0001, b"\x00\x00"),
    ],
)
def test_mqtt_2_1_3_1_reserved_fixed_header_flags_are_validated(
    name: str, packet_type: PacketType, flags: int, body: bytes
) -> None:
    """[MQTT-2.1.3-1] Where a flag bit is marked as "Reserved", it is reserved
    for future use and MUST be set to the value listed.

    DISCONNECT additionally has [MQTT-3.14.1-1], which names the reason code the
    receiver reports.
    """
    engine = _connected()
    assert _rejects(engine, encode_frame(packet_type, flags, body)), f"{name} accepted bad flags"


# ------------------------------------------------------- the quotes themselves


# A *quotation* opens a docstring: `"""[MQTT-x.y-n] <verbatim text>`. A label
# appearing anywhere else is a cross-reference and carries no text of its own.
_QUOTATION = re.compile(
    r'"""\[(MQTT-\d+(?:\.\d+)*-\d+)\]\s*(.+?)(?:\n\n|""")',
    re.DOTALL,
)
_ANY_LABEL = re.compile(r"\[(MQTT-\d+(?:\.\d+)*-\d+)\]")


def _normalise(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def test_quoted_statements_match_the_vendored_specification() -> None:
    """Every ``[MQTT-…]`` quote in this module must match the extracted text.

    Without this, a docstring could confidently cite a statement it does not
    actually reproduce — which is exactly how the wrong clause was cited for the
    inbound packet-id collision fix.
    """
    source = Path(__file__).read_text(encoding="utf-8")
    # 119 labels exist in both specifications and 112 of them say different
    # things, so a bare label is only meaningful per version. A quote is
    # accepted when it matches the statement in either vendored document.
    by_version = {version: statements(version) for version in ("5.0", "3.1.1")}

    for label in set(_ANY_LABEL.findall(source)):
        if not any(label in known for known in by_version.values()):
            pytest.fail(f"{label} is not a statement in either vendored specification")

    checked = 0
    for label, quoted in _QUOTATION.findall(source):
        quoted_words = _normalise(quoted)
        if not quoted_words:
            continue
        candidates = {
            version: known[label] for version, known in by_version.items() if label in known
        }
        matched = False
        for text in candidates.values():
            actual = _normalise(text)
            # The docstring may stop early, but what it does quote must be exact.
            prefix = " ".join(quoted_words.split()[: len(actual.split())])
            if actual.startswith(prefix) or prefix in actual:
                matched = True
                break
        if not matched:
            rendered = "\n".join(f"  MQTT {v}: {t[:170]}" for v, t in candidates.items())
            pytest.fail(f"{label} is misquoted\n  docstring: {quoted.strip()[:170]}\n{rendered}")
        checked += 1

    assert checked >= 5, f"expected the module to cite several statements, found {checked}"


@pytest.mark.parametrize("version", ["3.1.1", "5.0"])
def test_vendored_statement_index_is_well_formed(version: str) -> None:
    """The index is generated; a malformed or truncated one would silently
    weaken every check above that reads from it."""
    data = json.loads((SPEC_DIR / f"mqtt-v{version}-statements.json").read_text(encoding="utf-8"))

    assert data["mqtt_version"] == version
    assert data["source"]["url"].startswith("https://docs.oasis-open.org/mqtt/")
    assert re.fullmatch(r"[0-9a-f]{64}", data["source_sha256"])
    assert data["source_encoding"] == "cp1252"

    items = data["statements"]
    assert data["statement_count"] == len(items)
    assert len({item["id"] for item in items}) == len(items), "duplicate statement ids"
    for item in items:
        assert re.fullmatch(r"MQTT-\d+(\.\d+)*-\d+", item["id"])
        assert item["text"].strip(), f"{item['id']} has no text"
        assert item["origin"] in {"body", "appendix"}
        assert "\x01" not in item["text"] and "�" not in item["text"], (
            f"{item['id']} carries an extraction sentinel or a decoding failure"
        )
