"""Protocol package with lazy public re-exports.

Submodules such as :mod:`mqttium.protocol.validate` are imported by the packet
codec. Importing the engine eagerly here would create a cycle
``packets -> protocol.validate -> protocol -> engine -> packets``. Public names
remain available through module-level ``__getattr__`` without loading unrelated
layers.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mqttium.protocol.engine import (
        DisconnectInfo,
        EffectKind,
        EngineConfig,
        EngineEffect,
        ProtocolEngine,
        PublishFailure,
        PublishHandle,
    )
    from mqttium.protocol.flow_control import FlowControl
    from mqttium.protocol.negotiated import NegotiatedSettings
    from mqttium.protocol.packet_ids import PacketIdPool
    from mqttium.protocol.reconnect import ReconnectPolicy

_EXPORT_MODULES = {
    "DisconnectInfo": "mqttium.protocol.engine",
    "EffectKind": "mqttium.protocol.engine",
    "EngineConfig": "mqttium.protocol.engine",
    "EngineEffect": "mqttium.protocol.engine",
    "ProtocolEngine": "mqttium.protocol.engine",
    "PublishFailure": "mqttium.protocol.engine",
    "PublishHandle": "mqttium.protocol.engine",
    "FlowControl": "mqttium.protocol.flow_control",
    "NegotiatedSettings": "mqttium.protocol.negotiated",
    "PacketIdPool": "mqttium.protocol.packet_ids",
    "ReconnectPolicy": "mqttium.protocol.reconnect",
}

__all__ = list(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
