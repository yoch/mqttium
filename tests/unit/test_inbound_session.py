"""Structural contracts for the inbound protocol-state owner."""

from __future__ import annotations

from mqttium.enums import PacketType
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.inbound import InboundSession


def test_publish_and_pubrel_dispatch_directly_to_inbound_session() -> None:
    engine = ProtocolEngine()

    publish = engine._handlers[PacketType.PUBLISH]
    pubrel = engine._handlers[PacketType.PUBREL]

    assert publish.__self__ is engine.inbound
    assert publish.__func__ is InboundSession.on_publish
    assert pubrel.__self__ is engine.inbound
    assert pubrel.__func__ is InboundSession.on_pubrel


def test_legacy_inbound_diagnostics_are_views_not_duplicate_state() -> None:
    engine = ProtocolEngine()

    assert engine._topic_aliases is engine.inbound._aliases
    assert engine._recovered_inbound_mids is engine.inbound._recovered_mids

    engine._inbound_inflight = 3
    assert engine.inbound._inflight == 3
    assert engine._inbound_inflight == 3
