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
    # Concurrent inbound QoS 1/2 exchanges; advertised as Receive Maximum on
    # MQTT 5 and enforced locally on both protocols (DISCONNECT 0x93).
    max_inbound_inflight: int = 65535
    # Logical application bytes retained for inbound QoS handshakes. None
    # disables the cap; zero rejects every new message that needs persistence
    # (DISCONNECT 0x97).
    max_inbound_inflight_bytes: int | None = 64 * 1024 * 1024
    # Optional local cap on outbound inflight QoS>0 (None = broker's Receive
    # Maximum only). Use to self-throttle a fast publisher.
    max_outbound_inflight: int | None = None
    # Total locally retained QoS 1/2 publications, including inflight and queued.
    # None disables the corresponding limit; zero rejects every new QoS>0 publish.
    max_unacknowledged_messages: int | None = 10_000
    max_unacknowledged_bytes: int | None = 64 * 1024 * 1024
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
        self._validate_bounds()
        if self.protocol is not MQTTProtocolVersion.MQTTv5:
            self._refuse_mqtt5_options()
        elif self.will_properties is not None and self.will_properties.values:
            # Refuse a property a Will cannot carry now, not at the first CONNECT.
            from mqttium.codec.properties import WILL, encode_properties

            encode_properties(self.will_properties, WILL)
        if self.connect_properties is not None:
            reserved = {"receive_maximum", "maximum_packet_size", "topic_alias_maximum"}
            if reserved.intersection(self.connect_properties.values):
                raise ProtocolError("CONNECT limits must use the dedicated constructor arguments")

    def _validate_bounds(self) -> None:
        if not 1 <= self.max_inbound_inflight <= 65535:
            raise ValueError("max_inbound_inflight must be between 1 and 65535")
        if self.max_outbound_inflight is not None and not (
            1 <= self.max_outbound_inflight <= 65535
        ):
            raise ValueError("max_outbound_inflight must be between 1 and 65535")
        for name in (
            "max_unacknowledged_messages",
            "max_unacknowledged_bytes",
            "max_inbound_inflight_bytes",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative or None")
        if self.maximum_packet_size is not None and not (
            2 <= self.maximum_packet_size <= 268_435_460
        ):
            raise ValueError("maximum_packet_size must be between 2 and 268435460")
        if not 0 <= self.topic_alias_maximum <= 65535:
            raise ValueError("topic_alias_maximum must be between 0 and 65535")

    def _refuse_mqtt5_options(self) -> None:
        # MQTT 5 options are refused here rather than at CONNECT so a
        # misconfigured client fails at construction, not at first use.
        if self.connect_properties is not None and self.connect_properties.values:
            raise ProtocolError("CONNECT properties require MQTT 5")
        if self.will_properties is not None and self.will_properties.values:
            raise ProtocolError("Will properties require MQTT 5")
        if self.topic_alias_maximum:
            raise ProtocolError("topic_alias_maximum requires MQTT 5")
        if self.accept_auth:
            raise ProtocolError("Enhanced authentication requires MQTT 5")
