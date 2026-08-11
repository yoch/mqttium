"""Reproduction for GitHub issue #111.

Will Topic is a Topic Name and must reject empty strings and wildcards before
CONNECT is encoded.
"""

from __future__ import annotations

import pytest

from mqttium.errors import ProtocolError
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/111 — Will Topic not validated as Topic Name",
)
@pytest.mark.parametrize("topic", ["", "bad/#", "bad/+"])
def test_will_topic_rejects_empty_and_wildcards(topic: str) -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug111", will=Message(topic=topic, payload=b"bye"))
    )
    with pytest.raises(ProtocolError):
        engine.begin_connect()
