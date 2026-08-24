"""Engine configuration and the rules for changing it at runtime."""

from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any

from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message, Properties

_RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS = frozenset(
    {
        "keepalive",
        "username",
        "password",
        "will",
        "will_properties",
        "max_pending_outbound_messages",
        "max_pending_outbound_bytes",
        "max_pending_inbound_bytes",
        "accept_auth",
    }
)


@dataclass
class EngineConfig:
    client_id: str = ""
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
    clean_start: bool = True
    keepalive: int = 60
    username: str | None = None
    password: bytes | None = field(default=None, repr=False)
    local_receive_maximum: int = 65535
    # Optional local cap on outbound inflight QoS>0 (None = broker's Receive
    # Maximum only). Use to self-throttle a fast publisher.
    max_outbound_inflight: int | None = None
    # Total locally retained QoS 1/2 publications, including inflight and queued.
    # None disables the corresponding limit; zero rejects every new QoS>0 publish.
    max_pending_outbound_messages: int | None = 10_000
    max_pending_outbound_bytes: int | None = 64 * 1024 * 1024
    # Logical application bytes retained for inbound QoS handshakes. None
    # disables the cap; zero rejects every new message that needs persistence.
    max_pending_inbound_bytes: int | None = 64 * 1024 * 1024
    connect_properties: Properties | None = None
    will: Message | None = field(default=None, repr=False)
    will_properties: Properties | None = None
    # Local maximum packet size announced to broker (and enforced on ingress).
    maximum_packet_size: int | None = None
    topic_alias_maximum: int = 0  # announced to broker for inbound aliases
    manual_ack: bool = False  # defer PUBACK (QoS1) / PUBCOMP (QoS2) until ack()
    # When False, inbound AUTH is rejected with DISCONNECT 0x82. AsyncClient
    # derives this capability from whether an auth_handler is registered.
    accept_auth: bool = False
    _attached: bool = field(default=False, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not 0 <= self.keepalive <= 65535:
            raise ValueError("keepalive must be between 0 and 65535")
        if not 1 <= self.local_receive_maximum <= 65535:
            raise ValueError("local_receive_maximum must be between 1 and 65535")
        if self.max_outbound_inflight is not None and not (
            1 <= self.max_outbound_inflight <= 65535
        ):
            raise ValueError("max_outbound_inflight must be between 1 and 65535")
        if (
            self.max_pending_outbound_messages is not None
            and self.max_pending_outbound_messages < 0
        ):
            raise ValueError("max_pending_outbound_messages must be non-negative or None")
        if self.max_pending_outbound_bytes is not None and self.max_pending_outbound_bytes < 0:
            raise ValueError("max_pending_outbound_bytes must be non-negative or None")
        if self.max_pending_inbound_bytes is not None and self.max_pending_inbound_bytes < 0:
            raise ValueError("max_pending_inbound_bytes must be non-negative or None")
        if self.maximum_packet_size is not None and not (
            2 <= self.maximum_packet_size <= 268_435_460
        ):
            raise ValueError("maximum_packet_size must be between 2 and 268435460")
        if not 0 <= self.topic_alias_maximum <= 65535:
            raise ValueError("topic_alias_maximum must be between 0 and 65535")

    def update(self, **changes: Any) -> None:
        """Validate a candidate configuration, then commit its changed fields.

        No mutation occurs when validation raises, including type errors. Once
        attached to a ProtocolEngine, only fields without derived engine state
        may be changed through this method.
        """
        known = {f.name for f in fields(self) if f.init}
        unknown = set(changes) - known
        if unknown:
            raise AttributeError(f"unknown EngineConfig fields: {sorted(unknown)}")
        if self._attached:
            unsafe = set(changes) - _RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS
            if unsafe:
                raise AttributeError(
                    "EngineConfig fields require a new ProtocolEngine once attached: "
                    f"{sorted(unsafe)}"
                )
        candidate = replace(self, **changes)
        for name in changes:
            setattr(self, name, getattr(candidate, name))
