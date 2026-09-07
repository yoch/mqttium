from __future__ import annotations

import pytest

from mqttium import AsyncClient, MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.packets import ConnectPacket
from mqttium.protocol.config import EngineConfig


def test_mqtt31_enum_member_remains_stable() -> None:
    assert MQTTProtocolVersion.MQTTv31.name == "MQTTv31"
    assert int(MQTTProtocolVersion.MQTTv31) == 3


def test_engine_config_rejects_mqtt31_before_engine_construction() -> None:
    with pytest.raises(ValueError, match="MQTT 3.1 is not supported"):
        EngineConfig(protocol=MQTTProtocolVersion.MQTTv31)


def test_async_client_rejects_mqtt31_at_construction() -> None:
    with pytest.raises(ValueError, match="MQTT 3.1 is not supported"):
        AsyncClient(protocol=MQTTProtocolVersion.MQTTv31)


def test_connect_packet_has_no_mqtt31_encoder() -> None:
    with pytest.raises(ProtocolError, match="Unsupported protocol"):
        ConnectPacket(client_id="legacy", protocol=MQTTProtocolVersion.MQTTv31).encode()
