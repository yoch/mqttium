"""Bounded, independent retained MQTT frames for system reception.

The existing raw pilot validates the body grammar. Only the now-supported
RETAIN bit differs; neither product codecs nor client internals are imported.
"""
from __future__ import annotations

from dataclasses import replace
import time

from external_wildcard import Subscription, subscribe_packet
from pubsub_probe import MAX_BUFFERED, PubsubWire, decode_filter_ack, decode_publish as plain_publish
from session_reception import SessionWire
from tcp_probe import Frame, ReceiptFailure, packet, publish_packet, pubrel_packet, require, utf8


def decode_publish(frame):
    publication = plain_publish(Frame(frame.header & ~1, frame.body))
    return replace(publication, retain=bool(frame.header & 1))


def retained_publish_packet(topic, payload, identifier=None, *, qos=1, retain=True, dup=False):
    if type(retain) is not bool or qos not in (0, 1, 2):
        raise ValueError('explicit QoS and RETAIN required')
    if len(topic.encode()) > 1024 or len(payload) > 32768:
        raise ValueError('retained probe packet exceeds product bounds')
    if qos == 0:
        if identifier is not None or dup:
            raise ValueError('QoS0 has no Packet Identifier and requires DUP0')
        wire = packet(0x30, utf8(topic) + b'\x00' + payload)
    else:
        if identifier is None:
            raise ValueError('QoS1/2 requires a Packet Identifier')
        wire = publish_packet(topic, payload, identifier, qos=qos, dup=dup)
    return bytes([wire[0] | int(retain)]) + wire[1:]


class RetainedWire(SessionWire):
    """Raw full-profile connection with explicit subscriber acknowledgements."""
    def wait(self, kind, identifier=None, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for index, frame in enumerate(self.controls):
                actual = int.from_bytes(frame.body[:2], 'big') if kind != 0x20 else None
                if frame.kind == kind and (identifier is None or actual == identifier):
                    return self.controls.pop(index)
            frame = self.poll(max(0, deadline - time.monotonic()))
            require(frame is not None, 'retained_missing_control', hex(kind))
            if frame.kind == 0x30:
                require(len(self.publications) < MAX_BUFFERED, 'retained_message_queue_budget')
                self.publications.append(decode_publish(frame))
                continue
            actual = int.from_bytes(frame.body[:2], 'big') if kind != 0x20 else None
            if frame.kind == kind and (identifier is None or actual == identifier):
                return frame
            require(len(self.controls) < MAX_BUFFERED, 'retained_control_queue_budget')
            self.controls.append(frame)
        raise ReceiptFailure('retained_missing_control', hex(kind))

    def next_publication(self, timeout=5):
        if self.publications:
            publication = self.publications.pop(0)
        else:
            deadline = time.monotonic() + timeout
            publication = None
            while time.monotonic() < deadline:
                frame = self.poll(max(0, deadline - time.monotonic()))
                require(frame is not None, 'retained_delivery_missing')
                if frame.kind == 0x30:
                    publication = decode_publish(frame)
                    break
                require(len(self.controls) < MAX_BUFFERED, 'retained_control_queue_budget')
                self.controls.append(frame)
            require(publication is not None, 'retained_delivery_missing')
        self.observations.record('outgoing_publish_received', label=self.label, **publication.observation())
        return publication

    def subscribe_options(self, identifier, values, expected=None):
        values = tuple(values)
        self.send(subscribe_packet(identifier, values))
        mid, reasons = decode_filter_ack(self.wait(0x90, identifier))
        require(mid == identifier and reasons == (tuple(expected) if expected is not None else
                tuple(min(value.qos, 1) for value in values)), 'retained_suback_mismatch', repr(reasons))
        self.observations.record('suback', label=self.label, packet_id=mid, reasons=list(reasons),
                                 filters=[value.filter for value in values], options=[value.bits() for value in values])
        return reasons

    def publish(self, identifier, topic, payload, *, qos=1, retain=True, dup=False):
        self.send(retained_publish_packet(topic, payload, identifier, qos=qos, retain=retain, dup=dup))
        if qos == 0:
            return
        first = self.wait(0x40 if qos == 1 else 0x50, identifier)
        require(first.ack() == (identifier, 0), 'retained_publisher_ack_refused', repr(first.ack()))
        if qos == 2:
            self.send(pubrel_packet(identifier))
            require(self.wait(0x70, identifier).ack() == (identifier, 0), 'retained_publisher_completion_refused')

    def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        publication = self.next_publication()
        require(payload is None or publication.payload == payload, 'retained_payload_mismatch')
        require(qos is None or publication.qos == qos, 'retained_qos_mismatch')
        require(retain is None or publication.retain == retain, 'retained_flag_mismatch')
        if publication.qos and acknowledge:
            self.acknowledge(publication)
        return publication
