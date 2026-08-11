"""Topic and filter validation (IMPLEMENTATION-GUIDE.md §7)."""

from __future__ import annotations

from mqttium.codec.primitives import validate_utf8
from mqttium.errors import MalformedPacketError, ProtocolError


def validate_publish_topic(topic: str, *, allow_empty: bool = False) -> None:
    """Validate an outbound PUBLISH Topic Name (no wildcards).

    MQTT 5 permits an empty Topic Name only when a Topic Alias has already been
    established on the current Network Connection. MQTTium does not currently
    retain authoritative outbound-alias state across the engine/runtime handoff,
    so accepting an empty name here can emit a malformed first-use alias and can
    make durable QoS replay depend on connection-scoped state that no longer
    exists. Until that state is modeled explicitly, outbound omission is refused
    even when legacy callers pass ``allow_empty=True``.
    """
    if not topic:
        raise ProtocolError("PUBLISH topic must not be empty")
    _check_utf8_mqtt_topic(topic)
    if "+" in topic or "#" in topic:
        raise ProtocolError("PUBLISH topic must not contain wildcards")


def validate_received_publish_topic(
    topic: str,
    *,
    utf8_validated: bool = False,
) -> None:
    """Validate an inbound PUBLISH topic.

    Packet decoding already performs the complete MQTT UTF-8 validation. The
    internal ``utf8_validated`` fast path avoids encoding and validating the
    same topic a second time while preserving the public standalone contract.
    """
    if not topic:
        return  # empty topic may be alias-resolved later
    if not utf8_validated:
        try:
            _check_utf8_mqtt_topic(topic)
        except ProtocolError as exc:
            raise MalformedPacketError(str(exc)) from exc
    if "+" in topic or "#" in topic:
        raise MalformedPacketError("PUBLISH topic must not contain wildcards")


def validate_subscribe_filter(topic_filter: str) -> None:
    """Validate a SUBSCRIBE topic filter (may include +/# and $share/)."""
    if not topic_filter:
        raise ProtocolError("SUBSCRIBE filter must not be empty")
    _check_utf8_mqtt_topic(topic_filter)

    if topic_filter.startswith("$share/"):
        rest = topic_filter[len("$share/") :]
        if "/" not in rest:
            raise ProtocolError("Shared subscription missing group or filter")
        group, filter_ = rest.split("/", 1)
        if not group or any(c in group for c in "/+#"):
            raise ProtocolError("Invalid shared subscription group name")
        if not filter_:
            raise ProtocolError("Shared subscription missing filter")
        _validate_filter_levels(filter_)
        return

    _validate_filter_levels(topic_filter)


def _validate_filter_levels(topic_filter: str) -> None:
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if level == "#":
            if i != len(levels) - 1:
                raise ProtocolError("'#' wildcard must be last filter level")
            continue
        if "#" in level:
            raise ProtocolError("'#' must occupy its own filter level")
        if level == "+":
            continue
        if "+" in level:
            raise ProtocolError("'+' must occupy its own filter level")


def _check_utf8_mqtt_topic(topic: str) -> None:
    try:
        validate_utf8(topic)
    except ProtocolError as exc:
        raise ProtocolError(f"Invalid MQTT topic: {exc}") from exc
