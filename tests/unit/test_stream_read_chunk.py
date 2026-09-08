"""CPython selector receive-allocation ablation."""

from __future__ import annotations

from types import SimpleNamespace

from mqttium.transport._stream import _SELECTOR_READ_CHUNK, StreamTransport, _cap_selector_read_chunk


class _MutableSelectorTransport:
    def __init__(self, max_size: int = 256 * 1024) -> None:
        self.max_size = max_size


class _ReadOnlySelectorTransport:
    @property
    def max_size(self) -> int:
        return 256 * 1024


class _NoSelectorKnob:
    pass


class _Writer:
    def __init__(self, transport) -> None:
        self.transport = transport


def test_selector_recv_size_is_capped_to_64k() -> None:
    transport = _MutableSelectorTransport()
    effective = _cap_selector_read_chunk(_Writer(transport))
    assert effective == _SELECTOR_READ_CHUNK
    assert transport.max_size == _SELECTOR_READ_CHUNK


def test_existing_smaller_recv_size_is_preserved() -> None:
    transport = _MutableSelectorTransport(32 * 1024)
    effective = _cap_selector_read_chunk(_Writer(transport))
    assert effective == 32 * 1024
    assert transport.max_size == 32 * 1024


def test_transport_without_selector_knob_is_untouched() -> None:
    transport = _NoSelectorKnob()
    assert _cap_selector_read_chunk(_Writer(transport)) is None
    assert not hasattr(transport, "max_size")


def test_read_only_selector_knob_fails_open_without_breaking_transport() -> None:
    transport = _ReadOnlySelectorTransport()
    assert _cap_selector_read_chunk(_Writer(transport)) == 256 * 1024
    assert transport.max_size == 256 * 1024


def test_stream_transport_applies_cap_at_construction() -> None:
    selector = _MutableSelectorTransport()
    writer = _Writer(selector)
    StreamTransport(SimpleNamespace(), writer)  # type: ignore[arg-type]
    assert selector.max_size == _SELECTOR_READ_CHUNK
