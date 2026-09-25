from __future__ import annotations

import pytest

from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient
from mqttium.errors import ProtocolError
from mqttium.packets import ConnectPacket
from mqttium.protocol.config import EngineConfig


def test_mqtt31_has_no_protocol_member() -> None:
    assert [level.value for level in MQTTProtocolVersion] == [4, 5]


def test_engine_config_rejects_mqtt31_before_engine_construction() -> None:
    with pytest.raises(ValueError, match="MQTT 3.1 is not supported"):
        EngineConfig(protocol=3)  # type: ignore[arg-type]


def test_async_client_rejects_mqtt31_at_construction() -> None:
    with pytest.raises(ValueError, match="MQTT 3.1 is not supported"):
        AsyncClient(protocol=3)  # type: ignore[arg-type]


def test_connect_packet_has_no_mqtt31_encoder() -> None:
    with pytest.raises(ProtocolError, match="Unsupported protocol"):
        ConnectPacket(client_id="legacy", protocol=3).encode()  # type: ignore[arg-type]
