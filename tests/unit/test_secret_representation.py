"""Credentials stay out of the representation of the objects that carry them.

The library writes no log, but a traceback that prints local variables, or a
debugger, shows ``repr()``. The password and the CONNECT properties, where
Authentication Data travels, must not appear there.
"""

from __future__ import annotations

from mqttium.enums import MQTTProtocolVersion
from mqttium.packets.connect import ConnectPacket
from mqttium.protocol.config import EngineConfig
from mqttium.types import Properties

_PASSWORD = b"correct horse battery staple"
_TOKEN = b"bearer-token-0123456789"
_PROPERTIES = Properties({"authentication_method": "TOKEN", "authentication_data": _TOKEN})


def _leaks(text: str) -> bool:
    return any(secret.decode() in text for secret in (_PASSWORD, _TOKEN))


def test_connect_packet_representation_hides_credentials() -> None:
    packet = ConnectPacket(
        client_id="client",
        username="user",
        password=_PASSWORD,
        protocol=MQTTProtocolVersion.MQTTv5,
        properties=_PROPERTIES,
    )
    assert not _leaks(repr(packet))
    assert "client" in repr(packet)
    # Hidden from the representation only: the packet still carries them.
    wire = packet.encode()
    assert _PASSWORD in wire and _TOKEN in wire


def test_engine_configuration_representation_hides_credentials() -> None:
    config = EngineConfig(
        client_id="client",
        protocol=MQTTProtocolVersion.MQTTv5,
        username="user",
        password=_PASSWORD,
        connect_properties=_PROPERTIES,
    )
    assert not _leaks(repr(config))
    assert "client" in repr(config)
