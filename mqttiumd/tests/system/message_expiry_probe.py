"""Independent bounded MQTT 5 Message Expiry wire grammar.

This reader imports no product or client codec. The explicit property grammar
accepts only absent Message Expiry or one Four Byte Integer property 0x02.
"""
from __future__ import annotations

from dataclasses import dataclass
import time

from pubsub_probe import MAX_BUFFERED, MAX_PAYLOAD, MAX_TOPIC, Publication
from session_reception import SessionWire
from tcp_probe import Frame, ReceiptFailure, packet, pubrel_packet, require, utf8, variable_integer
from external_wildcard import Subscription, subscribe_packet
from pubsub_probe import decode_filter_ack

MAX_INTERVAL = 0xFFFFFFFF


def interval(value):
    if value is not None and (type(value) is not int or not 0 <= value <= MAX_INTERVAL):
        raise ValueError('Message Expiry must be absent or an MQTT Four Byte Integer')
    return value


def expiry_properties(value):
    value = interval(value)
    return b'' if value is None else b'\x02' + value.to_bytes(4, 'big')


def publish_packet(topic, payload, identifier=None, *, qos=1, retain=False,
                   expiry=None, dup=False, property_bytes=None):
    """Encode a valid publication, or explicit adversarial property bytes."""
    if type(qos) is not int or qos not in (0, 1, 2) or type(retain) is not bool or type(dup) is not bool:
        raise ValueError('explicit QoS, RETAIN and DUP required')
    if not isinstance(payload, bytes) or len(payload) > MAX_PAYLOAD or len(topic.encode()) > MAX_TOPIC:
        raise ValueError('publication exceeds probe bounds')
    if '+' in topic or '#' in topic:
        raise ValueError('PUBLISH topic cannot contain a wildcard')
    if qos == 0:
        if identifier is not None or dup:
            raise ValueError('QoS0 has no Packet Identifier and requires DUP0')
        mid = b''
    else:
        if type(identifier) is not int or not 1 <= identifier <= 65535:
            raise ValueError('QoS1/2 requires a Packet Identifier')
        mid = identifier.to_bytes(2, 'big')
    if property_bytes is not None:
        if expiry is not None or not isinstance(property_bytes, bytes) or len(property_bytes) > 64:
            raise ValueError('bounded explicit adversarial properties required')
        props = property_bytes
    else:
        props = expiry_properties(expiry)
    return packet(0x30 | qos << 1 | int(retain) | int(dup) << 3,
                  utf8(topic) + mid + variable_integer(len(props)) + props + payload)


def _variable(data, offset):
    value = 0
    start = offset
    for count in range(4):
        require(offset < len(data), 'message_expiry_truncated_property_length')
        digit = data[offset]
        offset += 1
        value |= (digit & 127) << (7 * count)
        if not digit & 128:
            require(data[start:offset] == variable_integer(value), 'message_expiry_noncanonical_property_length')
            return value, offset
    raise ReceiptFailure('message_expiry_malformed_property_length')


@dataclass(frozen=True)
class ExpiringPublication(Publication):
    message_expiry_interval: int | None
    property_bytes: bytes

    def observation(self):
        return {**super().observation(), 'message_expiry_present': self.message_expiry_interval is not None,
                'message_expiry_interval': self.message_expiry_interval,
                'property_bytes_hex': self.property_bytes.hex()}


def decode_publish(frame: Frame, *, incoming=False):
    qos = (frame.header >> 1) & 3
    require(frame.kind == 0x30 and qos in ((0, 1, 2) if incoming else (0, 1)),
            'message_expiry_unexpected_qos', hex(frame.header))
    require(qos != 0 or not frame.header & 8, 'message_expiry_qos0_dup')
    body = frame.body
    require(len(body) >= 3, 'message_expiry_truncated_publish')
    size = int.from_bytes(body[:2], 'big')
    offset = 2 + size
    require(0 < size <= MAX_TOPIC and offset + 2 * bool(qos) < len(body), 'message_expiry_bad_topic_length')
    try:
        topic = body[2:offset].decode('utf-8', 'strict')
    except UnicodeDecodeError as error:
        raise ReceiptFailure('message_expiry_bad_topic_utf8') from error
    require(not any(c in topic for c in ('\x00', '+', '#')), 'message_expiry_bad_topic')
    identifier = None
    if qos:
        identifier = int.from_bytes(body[offset:offset + 2], 'big')
        require(identifier != 0, 'message_expiry_zero_identifier')
        offset += 2
    count, offset = _variable(body, offset)
    end = offset + count
    require(count <= 64 and end <= len(body), 'message_expiry_truncated_properties')
    props = body[offset:end]
    expiry = None
    while offset < end:
        require(body[offset] == 0x02, 'message_expiry_unexpected_property', hex(body[offset]))
        require(expiry is None, 'message_expiry_duplicate_property')
        require(offset + 5 <= end, 'message_expiry_truncated_interval')
        expiry = int.from_bytes(body[offset + 1:offset + 5], 'big')
        offset += 5
    payload = body[end:]
    require(len(payload) <= MAX_PAYLOAD, 'message_expiry_payload_budget')
    return ExpiringPublication(identifier, topic, payload, qos, bool(frame.header & 8),
                               bool(frame.header & 1), expiry, props)


class MessageExpiryWire(SessionWire):
    """Raw peer with an independent property reader and explicit PUBACK."""
    def wait(self, kind, identifier=None, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for index, frame in enumerate(self.controls):
                actual = int.from_bytes(frame.body[:2], 'big') if kind != 0x20 else None
                if frame.kind == kind and (identifier is None or actual == identifier):
                    return self.controls.pop(index)
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, 'message_expiry_missing_control', hex(kind))
            if frame.kind == 0x30:
                require(len(self.publications) < MAX_BUFFERED, 'message_expiry_message_queue_budget')
                self.publications.append(decode_publish(frame))
            else:
                actual = int.from_bytes(frame.body[:2], 'big') if kind != 0x20 else None
                if frame.kind == kind and (identifier is None or actual == identifier):
                    return frame
                require(len(self.controls) < MAX_BUFFERED, 'message_expiry_control_queue_budget')
                self.controls.append(frame)
        raise ReceiptFailure('message_expiry_missing_control', hex(kind))

    def next_publication(self, timeout=5):
        deadline = time.monotonic() + timeout
        while not self.publications:
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, 'message_expiry_delivery_missing')
            if frame.kind == 0x30:
                self.publications.append(decode_publish(frame))
            else:
                require(len(self.controls) < MAX_BUFFERED, 'message_expiry_control_queue_budget')
                self.controls.append(frame)
            require(time.monotonic() <= deadline, 'message_expiry_delivery_missing')
        value = self.publications.pop(0)
        self.observations.record('outgoing_publish_received', label=self.label, **value.observation())
        return value

    def subscribe_options(self, identifier, values):
        values = tuple(values)
        self.send(subscribe_packet(identifier, values))
        actual, reasons = decode_filter_ack(self.wait(0x90, identifier))
        require(actual == identifier and reasons == tuple(min(value.qos, 1) for value in values),
                'message_expiry_suback_mismatch')
        return reasons

    def publish(self, identifier, topic, payload, *, qos=1, retain=False, expiry=None, dup=False):
        self.send(publish_packet(topic, payload, identifier, qos=qos, retain=retain, expiry=expiry, dup=dup))
        if qos:
            require(self.wait(0x40 if qos == 1 else 0x50, identifier).ack() == (identifier, 0),
                    'message_expiry_publisher_ack_refused')
            if qos == 2:
                self.send(pubrel_packet(identifier))
                require(self.wait(0x70, identifier).ack() == (identifier, 0),
                        'message_expiry_publisher_completion_refused')

    def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        value = self.next_publication()
        require(payload is None or value.payload == payload, 'message_expiry_payload_mismatch')
        require(qos is None or value.qos == qos, 'message_expiry_qos_mismatch')
        require(retain is None or value.retain == retain, 'message_expiry_retain_mismatch')
        if value.qos and acknowledge:
            self.acknowledge(value)
        return value
