"""Negotiated settings from CONNACK (IMPLEMENTATION-GUIDE.md §3)."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.types import Properties


@dataclass(frozen=True, slots=True)
class NegotiatedSettings:
    receive_maximum: int = 65535
    maximum_packet_size: int | None = None  # None = unlimited
    maximum_qos: int = 2
    retain_available: bool = True
    topic_alias_maximum: int = 0
    server_keep_alive: int | None = None  # None = use client keepalive
    assigned_client_identifier: str | None = None
    session_expiry_interval: int | None = None
    wildcard_subscription_available: bool = True
    shared_subscription_available: bool = True
    subscription_identifier_available: bool = True
    server_reference: str | None = None
    response_information: str | None = None

    @classmethod
    def from_connack(
        cls,
        properties: Properties | None,
        *,
        requested_keepalive: int,
        requested_session_expiry: int | None = None,
        local_client_id: str = "",
    ) -> NegotiatedSettings:
        props = properties.values if properties else {}
        server_ka = props.get("server_keep_alive")
        return cls(
            receive_maximum=int(props.get("receive_maximum", 65535)),
            maximum_packet_size=props.get("maximum_packet_size"),
            maximum_qos=int(props.get("maximum_qos", 2)),
            retain_available=bool(props.get("retain_available", 1)),
            topic_alias_maximum=int(props.get("topic_alias_maximum", 0)),
            server_keep_alive=int(server_ka) if server_ka is not None else None,
            assigned_client_identifier=props.get("assigned_client_identifier") or None,
            session_expiry_interval=(
                props.get("session_expiry_interval")
                if "session_expiry_interval" in props
                else requested_session_expiry
            ),
            wildcard_subscription_available=bool(props.get("wildcard_subscription_available", 1)),
            shared_subscription_available=bool(props.get("shared_subscription_available", 1)),
            subscription_identifier_available=bool(
                props.get("subscription_identifier_available", 1)
            ),
            server_reference=props.get("server_reference"),
            response_information=props.get("response_information"),
        )

    @property
    def effective_keepalive(self) -> int | None:
        """Return None to mean 'use client-requested keepalive'."""
        return self.server_keep_alive

    def effective_client_id(self, local: str) -> str:
        return self.assigned_client_identifier or local
