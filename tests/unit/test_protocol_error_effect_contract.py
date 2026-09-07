"""Internal PROTOCOL_ERROR effects carry real protocol exceptions only."""

from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError


@pytest.mark.parametrize(
    "error",
    [
        MalformedPacketError("malformed packet"),
        ProtocolError("protocol violation"),
        PacketTooLargeError("packet too large"),
    ],
)
def test_protocol_error_effect_preserves_valid_error_identity(error: ProtocolError) -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.DISCONNECTED

    with pytest.raises(type(error)) as caught:
        client._raise_protocol_effect(error)

    assert caught.value is error
    assert client._disconnect_exc is error


def test_protocol_error_effect_rejects_arbitrary_payload() -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.DISCONNECTED

    with pytest.raises(TypeError, match="PROTOCOL_ERROR effect payload"):
        client._raise_protocol_effect("peer diagnostic")

    assert client._disconnect_exc is None
