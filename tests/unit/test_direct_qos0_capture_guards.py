from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.packets._publish import (
    decode_qos0_message_v311_borrowed,
    decode_qos0_message_v5_borrowed,
)
from mqttium.types import Properties


@pytest.mark.parametrize(
    ("body", "flags", "error_text"),
    [
        (b"\x00\x01ax", 0x08, "QoS 0 PUBLISH must not set DUP"),
        (b"\x00", 0, "Incomplete uint16"),
        (b"\x00\x02a", 0, "Incomplete UTF-8 string"),
        (b"\x00\x01\xff", 0, "Invalid UTF-8 data"),
        (b"\x00\x00", 0, "PUBLISH topic must not be empty"),
    ],
)
def test_borrowed_v311_rejects_malformed_fields(
    body: bytes, flags: int, error_text: str
) -> None:
    with pytest.raises(MalformedPacketError) as error:
        decode_qos0_message_v311_borrowed(bytearray(body), 0, len(body), flags)
    assert error_text in str(error.value)


@pytest.mark.parametrize(
    ("body", "flags", "error_text"),
    [
        (b"\x00\x01a\x00", 0x08, "QoS 0 PUBLISH must not set DUP"),
        (b"\x00", 0, "Incomplete uint16"),
        (b"\x00\x02a", 0, "Incomplete UTF-8 string"),
        (b"\x00\x01\xff\x00", 0, "Invalid UTF-8 data"),
        (b"\x00\x01\x00\x00", 0, "Null in UTF-8 data"),
        (b"\x00\x01a", 0, "Missing properties length"),
        (b"\x00\x01a\x80", 0, "Incomplete Variable Byte Integer"),
        (b"\x00\x01a\x80\x00", 0, "Non-canonical Variable Byte Integer"),
        (b"\x00\x01a\xff\xff\xff\xff\x01", 0, "Malformed Variable Byte Integer"),
    ],
)
def test_borrowed_v5_rejects_malformed_fields(
    body: bytes, flags: int, error_text: str
) -> None:
    with pytest.raises(MalformedPacketError) as error:
        decode_qos0_message_v5_borrowed(bytearray(body), 0, len(body), flags)
    assert error_text in str(error.value)


def _v5_client() -> AsyncClient:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        message_delivery="callback",
        max_pending_callbacks=8,
    )
    client._engine.state = ConnectionState.CONNECTED
    client.on_message = lambda _message: None
    return client


def _publish_v5(properties: Properties | None = None) -> bytes:
    return PublishPacket(
        topic="coverage/topic",
        payload=b"x",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_decoded_property_sizes_must_align_with_messages() -> None:
    client = _v5_client()
    client._decoder.feed(_publish_v5())
    _, _, _, captured, _ = client._process_direct_qos0_batch()

    with pytest.raises(AssertionError, match="decoded property sizes must align"):
        client._delivery.deliver_callback_messages_inline(captured, client.on_message, [])


def test_decoded_empty_properties_respect_small_message_limit() -> None:
    client = _v5_client()
    client._delivery.small_message_limit = 0
    client._decoder.feed(_publish_v5())
    _, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert sizes == [None]
    assert not client._delivery.deliver_callback_messages_inline(
        captured, client.on_message, sizes
    )


def test_decoded_nonempty_properties_respect_small_message_limit() -> None:
    properties = Properties()
    properties.set("content_type", "application/json")
    client = _v5_client()
    client._delivery.small_message_limit = 0
    client._decoder.feed(_publish_v5(properties))
    _, _, _, captured, sizes = client._process_direct_qos0_batch()

    assert sizes is not None and sizes[0] is not None
    assert not client._delivery.deliver_callback_messages_inline(
        captured, client.on_message, sizes
    )
