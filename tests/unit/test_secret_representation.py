"""Credentials stay out of the representation of the objects that carry them.

The library writes no log, but a traceback that prints local variables, or a
debugger, shows ``repr()``. The password and the CONNECT properties, where
Authentication Data travels, must not appear there.
"""

from __future__ import annotations

import pytest

from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.packets.connect import ConnectPacket
from mqttium.protocol.config import EngineConfig
from mqttium.types import Message, Properties

_PASSWORD = b"correct horse battery staple"
_TOKEN = b"bearer-token-0123456789"
_WILL_PAYLOAD = b"last-words-of-the-device"
_WILL_PROPERTY = "will-property-value"
_PROPERTIES = Properties({"authentication_method": "TOKEN", "authentication_data": _TOKEN})
_WILL_PROPERTIES = Properties({"user_property": [("k", _WILL_PROPERTY)]})


def _leaks(text: str) -> bool:
    secrets = (_PASSWORD, _TOKEN, _WILL_PAYLOAD, _WILL_PROPERTY.encode())
    return any(secret.decode() in text for secret in secrets)


def test_connect_packet_representation_hides_credentials() -> None:
    packet = ConnectPacket(
        client_id="client",
        username="user",
        password=_PASSWORD,
        protocol=MQTTProtocolVersion.MQTTv5,
        properties=_PROPERTIES,
        will_topic="will/topic",
        will_payload=_WILL_PAYLOAD,
        will_properties=_WILL_PROPERTIES,
    )
    assert not _leaks(repr(packet))
    assert "client" in repr(packet)
    # Hidden from the representation only: the packet still carries them.
    wire = packet.encode()
    assert _PASSWORD in wire and _TOKEN in wire and _WILL_PAYLOAD in wire


def test_engine_configuration_representation_hides_credentials() -> None:
    config = EngineConfig(
        client_id="client",
        protocol=MQTTProtocolVersion.MQTTv5,
        username="user",
        password=_PASSWORD,
        connect_properties=_PROPERTIES,
        will=Message(topic="will/topic", payload=_WILL_PAYLOAD),
        will_properties=_WILL_PROPERTIES,
    )
    assert not _leaks(repr(config))
    assert "client" in repr(config)


def test_a_mistyped_binary_property_is_refused_without_quoting_it() -> None:
    """Authentication data passed as ``str`` is an easy mistake; the error must
    not put the token in a message that ends up in a log."""
    packet = ConnectPacket(
        client_id="client",
        protocol=MQTTProtocolVersion.MQTTv5,
        properties=Properties(
            {"authentication_method": "TOKEN", "authentication_data": _TOKEN.decode()}
        ),
    )
    with pytest.raises(ProtocolError) as refused:
        packet.encode()
    assert not _leaks(str(refused.value))
    assert "str" in str(refused.value)
