"""Select the complete codec backend once, before clients bind codec functions."""

from __future__ import annotations

import importlib
import os
from types import ModuleType
from typing import Any

_choice = os.environ.get("MQTTIUM_CODEC_BACKEND", "auto")
if _choice not in {"auto", "python", "native"}:
    raise ValueError("MQTTIUM_CODEC_BACKEND must be auto, python or native")

_native: ModuleType | None = None
if _choice != "python":
    try:
        _native = importlib.import_module("mqttium.codec._native")
    except ModuleNotFoundError as exc:
        if exc.name != "mqttium.codec._native" or _choice == "native":
            raise
    if _native is not None:
        from mqttium.codec._digest import SOURCE_DIGEST

        # A stale extension would silently run superseded codec code.
        if _native.SOURCE_DIGEST != SOURCE_DIGEST:
            raise ImportError(
                "The native codec was generated from different MQTTium sources; "
                "reinstall it or set MQTTIUM_CODEC_BACKEND=python"
            )

BACKEND = "native" if _native is not None else "python"


def bind(namespace: dict[str, Any], module: str) -> None:
    """Replace a codec module's definitions with the selected implementation."""
    if _native is not None:
        namespace.update(_native.EXPORTS[module])


def implementation(module: ModuleType) -> ModuleType:
    """Return the implementation owning codec globals (internal test/profiling seam)."""
    if _native is not None and module.__name__ in _native.EXPORTS:
        return _native
    return module
