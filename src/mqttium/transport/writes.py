"""Outbound write items: contiguous bytes or segmented (header, payload)."""

from __future__ import annotations

from typing import TypeAlias

# A single socket write unit. Segmented form avoids copying large immutable
# payloads into the PUBLISH frame. The threshold is the QoS 1/2 retain boundary
# as well (see docs/reports/QOS1-FRAME-POLICY.md): contiguous frames are dropped
# after SEND, segmented ones keep a shared payload. 128 KiB sits below the
# measured contiguous-copy cliff (~256 KiB on CPython 3.12) without pushing
# small telemetry into two-write form.
WriteItem: TypeAlias = bytes | tuple[bytes, bytes]

SEGMENT_THRESHOLD = 128 * 1024


def item_size(item: WriteItem) -> int:
    if isinstance(item, bytes):
        return len(item)
    return len(item[0]) + len(item[1])


# Select the complete codec implementation.
from mqttium.codec._backend import bind as _bind_backend

_bind_backend(globals(), __name__)
