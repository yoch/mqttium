"""Small independent MQTT 5 Will frames and observations for system tests.

This is a byte pilot, not a second implementation of the broker's policy. It
imports no mqttium/product codec and never associates a Will with a fabricated
Packet Identifier. The profile intentionally excludes all Will properties other
than an absent or zero Will Delay Interval.
"""
from __future__ import annotations

import hashlib

from retained_probe import RetainedWire, decode_publish
from tcp_probe import Frame, ReceiptFailure, packet, require, utf8, variable_integer


def will_connect_packet(client_id, topic, payload=b'offline', *, expiry=0xFFFFFFFF,
                        clean_start=False, keep_alive=10, delay=0, qos=1, retain=False,
                        will_properties=None):
    if not 0 <= expiry <= 0xFFFFFFFF or not 0 <= keep_alive <= 65535:
        raise ValueError('bounded CONNECT values required')
    if len(topic.encode()) > 1024 or len(payload) > 32768 or qos not in (0, 1, 2):
        raise ValueError('bounded explicit Will required')
    props = b'\x11' + expiry.to_bytes(4, 'big') + b'\x21\x00\x08\x27\x00\x01\x00\x00'
    will_props = (b'' if delay is None else b'\x18' + delay.to_bytes(4, 'big'))
    if will_properties is not None:
        will_props = will_properties
    flags = 4 | (qos << 3) | (int(retain) << 5) | (int(clean_start) << 1)
    body = (b'\x00\x04MQTT\x05' + bytes([flags]) + keep_alive.to_bytes(2, 'big')
            + variable_integer(len(props)) + props + utf8(client_id)
            + variable_integer(len(will_props)) + will_props + utf8(topic)
            + len(payload).to_bytes(2, 'big') + payload)
    return packet(0x10, body)


def _variable(data, at):
    value = 0
    for shift in range(0, 28, 7):
        require(at < len(data), 'will_probe_truncated')
        digit = data[at]
        at += 1
        value += (digit & 127) << shift
        if digit & 128 == 0:
            return value, at
    raise ReceiptFailure('will_probe_bad_variable_integer')


def _binary(data, at, limit):
    require(at + 2 <= len(data), 'will_probe_truncated')
    size = int.from_bytes(data[at:at + 2], 'big')
    at += 2
    require(size <= limit and at + size <= len(data), 'will_probe_field_budget')
    return data[at:at + size], at + size


def _properties(data, at, sizes):
    size, at = _variable(data, at)
    end = at + size
    require(end <= len(data), 'will_probe_truncated')
    values = {}
    while at < end:
        key = data[at]
        at += 1
        require(key in sizes and key not in values, 'will_probe_unexpected_or_duplicate_property')
        size = sizes[key]
        require(at + size <= end, 'will_probe_truncated')
        values[key] = int.from_bytes(data[at:at + size], 'big')
        at += size
    return values, at


def decode_will_connect(frame):
    """Independently inspect the actual public client's CONNECT, without payload logs."""
    require(frame.header == 0x10 and len(frame.body) <= 65536, 'will_probe_not_connect')
    data = frame.body
    require(len(data) >= 11 and data[:7] == b'\x00\x04MQTT\x05', 'will_probe_connect_profile')
    flags = data[7]
    require(flags & 0xC1 == 0 and flags & 4, 'will_probe_connect_flags')
    props, at = _properties(data, 10, {0x11: 4, 0x21: 2, 0x22: 2, 0x27: 4})
    client, at = _binary(data, at, 1024)
    will_props, at = _properties(data, at, {0x18: 4})
    topic, at = _binary(data, at, 1024)
    payload, at = _binary(data, at, 32768)
    require(at == len(data), 'will_probe_connect_trailing_bytes')
    return {'client_id': client.decode('utf-8', 'strict'), 'clean_start': bool(flags & 2),
            'keep_alive': int.from_bytes(data[8:10], 'big'),
            'session_expiry_interval': props.get(0x11), 'will_qos': (flags >> 3) & 3,
            'will_retain': bool(flags & 0x20), 'will_topic': topic.decode('utf-8', 'strict'),
            'will_payload_bytes': len(payload), 'will_payload_sha256': hashlib.sha256(payload).hexdigest(),
            'will_delay_present': 0x18 in will_props, 'will_delay_interval': will_props.get(0x18)}


class WillWire(RetainedWire):
    def will_connect(self, client_id, topic, payload=b'offline', *, expected_present=False,
                     expiry=0xFFFFFFFF, clean_start=False, keep_alive=10, delay=0,
                     qos=1, retain=False, will_properties=None):
        data = will_connect_packet(client_id, topic, payload, expiry=expiry, clean_start=clean_start,
                                   keep_alive=keep_alive, delay=delay, qos=qos, retain=retain,
                                   will_properties=will_properties)
        self.send(data)
        ack = self.wait(0x20)
        require(len(ack.body) >= 3 and ack.body[0] in (0, 1), 'will_malformed_connack')
        require(ack.body[1] == 0, 'will_connect_not_accepted', str(ack.body[1]))
        require(bool(ack.body[0]) == expected_present, 'will_wrong_session_present')
        self.observations.record('will_connack', label=self.label, reason_code=0,
                                 session_present=bool(ack.body[0]))
        return ack


def assert_will_frame(frame, topic, payload=b'offline'):
    value = decode_publish(frame) if isinstance(frame, Frame) else frame
    require(value.topic == topic and value.payload == payload, 'will_wire_payload_or_topic_mismatch')
    require(value.qos == 1 and not value.retain, 'will_wire_qos_or_retain_mismatch')
    return value.observation()
