"""Topic and filter validation (implementation-guide.md §7)."""

from __future__ import annotations

from mqttium.codec.primitives import encode_utf8, validate_utf8
from mqttium.errors import MalformedPacketError, ProtocolError


def validate_publish_topic(topic: str, *, allow_empty: bool = False) -> int:
    """Validate a PUBLISH topic name (no wildcards) and return its MQTT byte length.

    ``allow_empty`` is for MQTT 5 topic-alias reuse (topic string empty, alias set).
    """
    if not topic:
        if allow_empty:
            return 0
        raise ProtocolError("PUBLISH topic must not be empty")
    size = _check_utf8_mqtt_topic(topic)
    if "+" in topic or "#" in topic:
        raise ProtocolError("PUBLISH topic must not contain wildcards")
    return size


def encode_validated_publish_topic(topic: str, *, allow_empty: bool = False) -> bytes:
    """Validate an outbound PUBLISH topic and return its MQTT UTF-8 bytes.

    Companion to :func:`validate_publish_topic` for callers that encode
    immediately (QoS 0, and connected QoS 1/2 launch). Retaining the validated
    bytes avoids scanning/encoding the Topic Name a second time.
    """
    if not topic and not allow_empty:
        raise ProtocolError("PUBLISH topic must not be empty")
    try:
        encoded = encode_utf8(topic)
    except ProtocolError as exc:
        raise ProtocolError(f"Invalid MQTT topic: {exc}") from exc
    if "+" in topic or "#" in topic:
        raise ProtocolError("PUBLISH topic must not contain wildcards")
    return encoded


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


def _check_utf8_mqtt_topic(topic: str) -> int:
    try:
        return validate_utf8(topic)
    except ProtocolError as exc:
        raise ProtocolError(f"Invalid MQTT topic: {exc}") from exc
