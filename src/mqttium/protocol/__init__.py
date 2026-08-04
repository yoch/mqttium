"""Protocol package with lazy public re-exports.

Importing the engine eagerly here would pull the whole state machine, the packet
codec and the persistence layer into every ``import mqttium.protocol``. Public
names stay available through module-level ``__getattr__`` without loading
unrelated layers.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mqttium.protocol.config import EngineConfig
    from mqttium.protocol.effects import (
        DisconnectInfo,
        EffectKind,
        EngineEffect,
        PublishFailure,
        PublishHandle,
    )
    from mqttium.protocol.engine import ProtocolEngine
    from mqttium.protocol.flow_control import FlowControl
    from mqttium.protocol.negotiated import NegotiatedSettings
    from mqttium.protocol.packet_ids import PacketIdPool
    from mqttium.protocol.reconnect import ReconnectPolicy

_EXPORT_MODULES = {
    "DisconnectInfo": "mqttium.protocol.effects",
    "EffectKind": "mqttium.protocol.effects",
    "EngineConfig": "mqttium.protocol.config",
    "EngineEffect": "mqttium.protocol.effects",
    "ProtocolEngine": "mqttium.protocol.engine",
    "PublishFailure": "mqttium.protocol.effects",
    "PublishHandle": "mqttium.protocol.effects",
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
