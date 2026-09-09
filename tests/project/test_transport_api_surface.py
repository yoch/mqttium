"""Executable contract for the Provisional custom-transport receive surface."""

from __future__ import annotations

from mqttium import transport
from mqttium.transport import AsyncTransport, DecoderPushTransport, PullTransport


class _LegacyPullTransport:
    async def read(self, n: int = 65536) -> bytes:
        del n
        return b""


class _PushTransport:
    def attach_decoder(self, decoder: object) -> None:
        self.decoder = decoder

    async def receive(self) -> bool:
        return False


def test_transport_capabilities_are_canonical_provisional_exports() -> None:
    assert transport.AsyncTransport is AsyncTransport
    assert transport.PullTransport is PullTransport
    assert transport.DecoderPushTransport is DecoderPushTransport
    assert {"AsyncTransport", "PullTransport", "DecoderPushTransport"} <= set(transport.__all__)


def test_historical_read_transport_satisfies_public_pull_capability() -> None:
    legacy = _LegacyPullTransport()
    assert isinstance(legacy, PullTransport)
    assert not isinstance(legacy, DecoderPushTransport)


def test_decoder_push_transport_is_a_distinct_receive_capability() -> None:
    push = _PushTransport()
    assert isinstance(push, DecoderPushTransport)
    assert not isinstance(push, PullTransport)
    assert not hasattr(DecoderPushTransport, "read")


def test_common_protocol_no_longer_promises_a_receive_method() -> None:
    assert not hasattr(AsyncTransport, "read")
    assert hasattr(PullTransport, "read")
    assert hasattr(DecoderPushTransport, "attach_decoder")
    assert hasattr(DecoderPushTransport, "receive")
