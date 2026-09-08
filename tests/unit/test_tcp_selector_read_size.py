from __future__ import annotations

import sys

from mqttium.transport.tcp import (
    _CPYTHON_SELECTOR_DEFAULT_READ_SIZE,
    _CPYTHON_SELECTOR_READ_SIZE,
    _cap_cpython_selector_read_size,
)


def _transport(module: str, max_size: int) -> object:
    cls = type("Transport", (), {"__module__": module})
    value = cls()
    value.max_size = max_size
    return value


def test_cpython_selector_default_read_size_is_capped() -> None:
    transport = _transport("asyncio.selector_events", _CPYTHON_SELECTOR_DEFAULT_READ_SIZE)
    _cap_cpython_selector_read_size(transport)
    expected = (
        _CPYTHON_SELECTOR_READ_SIZE
        if sys.implementation.name == "cpython"
        else _CPYTHON_SELECTOR_DEFAULT_READ_SIZE
    )
    assert transport.max_size == expected


def test_alternate_event_loop_is_not_modified() -> None:
    transport = _transport("uvloop.loop", _CPYTHON_SELECTOR_DEFAULT_READ_SIZE)
    _cap_cpython_selector_read_size(transport)
    assert transport.max_size == _CPYTHON_SELECTOR_DEFAULT_READ_SIZE


def test_nondefault_selector_read_size_is_not_overridden() -> None:
    transport = _transport("asyncio.selector_events", 32 * 1024)
    _cap_cpython_selector_read_size(transport)
    assert transport.max_size == 32 * 1024
