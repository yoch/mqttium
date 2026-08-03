"""Outbound write items: contiguous bytes or segmented (header, payload)."""

from __future__ import annotations

from typing import TypeAlias

# A single socket write unit. Segmented form avoids copying large immutable
# payloads into the PUBLISH frame (audit §18, threshold 1 MiB).
WriteItem: TypeAlias = bytes | tuple[bytes, bytes]

SEGMENT_THRESHOLD = 1 * 1024 * 1024


def item_size(item: WriteItem) -> int:
    if isinstance(item, bytes):
        return len(item)
    return len(item[0]) + len(item[1])
