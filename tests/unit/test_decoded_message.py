"""The internal delivery constructor builds the same frozen Message."""

from __future__ import annotations

import dataclasses

import pytest

from mqttium.enums import QoS
from mqttium.types import Message, Properties, _decoded_message


def test_decoded_message_equals_the_public_constructor() -> None:
    properties = Properties({"content_type": "text/plain"})
    fast = _decoded_message("a/b", b"payload", QoS.AT_LEAST_ONCE, True, True, 7, properties)
    public = Message("a/b", b"payload", QoS.AT_LEAST_ONCE, True, True, 7, properties)
    assert fast == public
    plain = _decoded_message("a/b", b"x", QoS.AT_MOST_ONCE, False, False, None, None)
    assert hash(plain) == hash(Message("a/b", b"x"))
    assert repr(fast) == repr(public)
    assert fast._ack_token is None
    assert all(
        getattr(fast, f.name) == getattr(public, f.name) for f in dataclasses.fields(Message)
    )


def test_decoded_message_stays_frozen() -> None:
    message = _decoded_message("a/b", b"x", QoS.AT_MOST_ONCE, False, False, None, None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        message.payload = b"y"  # type: ignore[misc]
