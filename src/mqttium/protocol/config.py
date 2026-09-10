"""Immutable internal protocol configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.types import Message, Properties


@dataclass(frozen=True)
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

    def __post_init__(self) -> None:
        if self.protocol not in (
            MQTTProtocolVersion.MQTTv311,
            MQTTProtocolVersion.MQTTv5,
        ):
            raise ValueError("MQTT 3.1 is not supported; use MQTT 3.1.1 or MQTT 5")
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

        if self.connect_properties is not None:
            reserved = {"receive_maximum", "maximum_packet_size", "topic_alias_maximum"}
            if reserved.intersection(self.connect_properties.values):
                raise ProtocolError("CONNECT limits must use the dedicated constructor arguments")
