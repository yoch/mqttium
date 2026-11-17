#!/usr/bin/env python3
"""Bounded public mqttium/Paho clients for explicit session lifecycle tests.

A local disconnect completion is deliberately not a broker close/cleanup fact.
The reception orchestrator establishes those facts independently.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import queue
import threading
import time

from external_mqttium import client_identity
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties


INFINITE = 0xFFFFFFFF
TIMEOUT = 5
MAX_MESSAGES = 32


def options(clean_start, expiry):
    if not isinstance(clean_start, bool):
        raise ValueError('Clean Start must be explicit boolean')
    if expiry is not None and (type(expiry) is not int or not 0 <= expiry <= INFINITE):
        raise ValueError('expiry must be absent or an MQTT Four Byte Integer')
    return {'protocol': 5, 'clean_start': clean_start, 'session_expiry_interval': expiry,
            'session_expiry_present': expiry is not None, 'keepalive': 10,
            'maximum_packet_size': 65536, 'receive_maximum': 8}


class ClientLog:
    def __init__(self, path: Path | None):
        self.events = []
        self.lock = threading.Lock()
        self.stream = path.open('x') if path else None

    def record(self, kind, **fields):
        with self.lock:
            if len(self.events) >= 256:
                raise RuntimeError('session client event budget')
            event = {'kind': kind, 'sequence': len(self.events),
                     'monotonic_ns': str(time.monotonic_ns()), **fields}
            raw = json.dumps(event, sort_keys=True) + '\n'
            if len(raw.encode()) > 4096:
                raise RuntimeError('session client event byte budget')
            self.events.append(event)
            if self.stream:
                self.stream.write(raw)
                self.stream.flush()

    def close(self):
        with self.lock:
            if self.stream:
                self.stream.close()
                self.stream = None


def message_row(message):
    payload = bytes(message.payload)
    if len(payload) > 32768 or len(message.topic.encode()) > 1024:
        raise RuntimeError('session client message budget')
    return {'topic': message.topic, 'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'payload_hex': payload.hex() if len(payload) <= 128 else None,
            'payload_bytes': len(payload), 'qos': int(message.qos), 'dup': bool(message.dup),
            'packet_id': int(message.mid) if int(message.qos) else None}


class MqttiumSession:
    """One actual public client/transport; no private engine or writer access."""
    def __init__(self, address, client_id, *, clean_start=False, expiry=INFINITE,
                 events_path=None, identity=None):
        self.parameters = options(clean_start, expiry)
        self.address, self.client_id = address, client_id
        self.log = ClientLog(events_path)
        self.identity = identity or client_identity()
        self.client = AsyncClient(
            client_id, protocol=MQTTProtocolVersion.MQTTv5, clean_start=clean_start,
            keepalive=10, connect_properties=None if expiry is None else
            Properties({'session_expiry_interval': expiry}), maximum_packet_size=65536,
            max_inbound_inflight=8, max_inbound_inflight_bytes=65536,
            max_outbound_inflight=8, max_unacknowledged_messages=32,
            max_unacknowledged_bytes=65536, max_write_queue_messages=32,
            max_write_queue_bytes=65536, message_delivery='iterator', manual_ack=True,
            max_iterator_messages=32, max_iterator_bytes=65536, connect_timeout=3)
        self.result = {'client': 'mqttium', 'identity': self.identity, 'client_id': client_id,
                       'pid': os.getpid(), 'parameters': self.parameters,
                       'events': self.log.events, 'status': 'not_connected'}
        self.log.record('client_identity', **self.identity)
        self.messages = None
        self.closed = False

    async def open(self, expected=None):
        self.log.record('connect_submitted', **self.parameters)
        try:
            ack = await self.client.connect(*self.address, timeout=3)
        except Exception as error:
            reason = getattr(error, 'reason_code', None)
            self.result.update(status='rejected' if reason is not None else 'error',
                               exception=type(error).__name__, reason_code=reason)
            self.log.record('connect_failed', exception=type(error).__name__, reason_code=reason)
            raise
        self.result.update(status='connected', session_present=ack.session_present,
                           reason_code=ack.reason_code)
        self.log.record('connack', reason_code=ack.reason_code, session_present=ack.session_present)
        if ack.reason_code or (expected is not None and ack.session_present != expected):
            raise AssertionError('session_present_mismatch')
        self.messages = self.client.messages()
        return ack.session_present

    async def subscribe(self, topic, qos=1):
        ack = await self.client.subscribe([(topic, qos)], timeout=TIMEOUT)
        self.log.record('suback', packet_id=ack.mid, reasons=list(ack.reason_codes), topic=topic)
        if ack.reason_codes != (min(qos, 1),):
            raise AssertionError('session_subscription_refused')
        return ack.mid

    async def publish(self, topic, payload, qos=1):
        receipt = await self.client.publish(topic, payload, qos=qos, retain=False, timeout=TIMEOUT)
        self.log.record('publish_submitted', packet_id=receipt.mid, qos=qos,
                        payload_sha256=hashlib.sha256(payload).hexdigest())
        await receipt.wait(timeout=TIMEOUT)
        if not receipt.is_done() or receipt.exception() is not None:
            raise AssertionError('session_publication_not_completed')
        self.log.record('publication_completed', packet_id=receipt.mid, qos=qos)
        return receipt.mid

    async def receive(self, expected_payload=None):
        if self.messages is None:
            raise RuntimeError('session is not connected')
        message = await asyncio.wait_for(anext(self.messages), timeout=TIMEOUT)
        row = message_row(message)
        self.log.record('message', **row)
        if expected_payload is not None and bytes(message.payload) != expected_payload:
            raise AssertionError('session_recipient_payload_mismatch')
        if row['qos'] != 1:
            raise AssertionError('session_recipient_qos_mismatch')
        await self.client.ack(message)
        self.log.record('puback_submitted', packet_id=row['packet_id'])
        return row

    async def disconnect(self, expiry=None):
        if expiry is not None:
            raise ValueError('mqttium public disconnect accepts no properties; use Paho/probe')
        if self.closed:
            return self.result
        self.log.record('disconnect_submitted', expiry_present=False)
        try:
            await asyncio.wait_for(self.client.disconnect(), timeout=3)
            self.result['connections'] = self.client.stats().connections
            self.result['transport_closed'] = True
            if self.result['status'] == 'connected':
                self.result['status'] = 'success'
            self.log.record('local_disconnect_complete', connections=self.result['connections'])
        finally:
            self.closed = True
            self.log.close()
        return self.result


class PahoSession:
    """Paho 2.1.0 public properties/callback API, with bounded callback queues."""
    def __init__(self, address, client_id, *, clean_start=False, expiry=INFINITE,
                 events_path=None):
        import paho.mqtt.client as mqtt
        self.mqtt = mqtt
        if importlib.metadata.version('paho-mqtt') != '2.1.0':
            raise RuntimeError('paho-mqtt2.1.0 required')
        self.parameters = options(clean_start, expiry)
        self.address, self.client_id = address, client_id
        self.log = ClientLog(events_path)
        self.ready, self.disconnected = threading.Event(), threading.Event()
        self.subacks, self.pubacks, self.messages = (queue.Queue(MAX_MESSAGES) for _ in range(3))
        self.failure, self.connections, self.closed = None, 0, False
        identity = {'name': 'paho-mqtt', 'version': '2.1.0', 'module_path': mqtt.__file__,
                    'module_sha256': hashlib.sha256(Path(mqtt.__file__).read_bytes()).hexdigest()}
        self.result = {'client': 'paho', 'identity': identity, 'client_id': client_id,
                       'pid': os.getpid(), 'parameters': self.parameters,
                       'events': self.log.events, 'status': 'not_connected'}
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                                  protocol=mqtt.MQTTv5, reconnect_on_failure=False, manual_ack=True)
        self.client.connect_timeout = 3
        self.client.max_inflight_messages_set(8)
        self.client.max_queued_messages_set(32)
        self.client.on_connect = self._connected
        self.client.on_disconnect = self._disconnected
        self.client.on_message = self._message
        self.client.on_subscribe = self._subscribed
        self.client.on_publish = self._published
        self.log.record('client_identity', **identity)

    def _put(self, target, value):
        try:
            target.put_nowait(value)
        except queue.Full:
            self.failure = 'paho_callback_queue_budget'

    def _connected(self, _client, _data, flags, reason, _properties):
        self.connections += 1
        self.result.update(reason_code=reason.value, session_present=bool(flags.session_present),
                           status='rejected' if reason.is_failure else 'connected')
        self.log.record('connack', reason_code=reason.value, session_present=bool(flags.session_present))
        self.ready.set()

    def _disconnected(self, _client, _data, _flags, reason, _properties):
        self.log.record('transport_closed', reason_code=reason.value)
        self.disconnected.set()

    def _message(self, _client, _data, message):
        row = message_row(message)
        self.log.record('message', **row)
        self._put(self.messages, row)

    def _subscribed(self, _client, _data, mid, reasons, _properties):
        codes = tuple(reason.value for reason in reasons)
        self.log.record('suback', packet_id=mid, reasons=list(codes))
        self._put(self.subacks, (mid, codes))

    def _published(self, _client, _data, mid, reason, _properties):
        self.log.record('publication_completed', packet_id=mid, reason_code=reason.value)
        self._put(self.pubacks, (mid, reason.value))

    def _get(self, target):
        if self.failure:
            raise AssertionError(self.failure)
        try:
            return target.get(timeout=TIMEOUT)
        except queue.Empty as error:
            raise TimeoutError('bounded Paho callback wait') from error

    async def open(self, expected=None):
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties as PahoProperties
        properties = PahoProperties(PacketTypes.CONNECT)
        properties.ReceiveMaximum = 8
        properties.MaximumPacketSize = 65536
        expiry = self.parameters['session_expiry_interval']
        if expiry is not None:
            properties.SessionExpiryInterval = expiry
        self.log.record('connect_submitted', **self.parameters)
        result = await asyncio.to_thread(self.client.connect, *self.address, keepalive=10,
                                         clean_start=self.parameters['clean_start'], properties=properties)
        if result != self.mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('paho_connect_submission_failed')
        self.client.loop_start()
        if not await asyncio.to_thread(self.ready.wait, 3):
            raise TimeoutError('Paho CONNACK deadline')
        if self.result['reason_code']:
            raise ConnectionError('paho_connect_refused', self.result['reason_code'])
        if expected is not None and self.result['session_present'] != expected:
            raise AssertionError('session_present_mismatch')
        return self.result['session_present']

    async def subscribe(self, topic, qos=1):
        result, mid = self.client.subscribe(topic, qos=qos)
        if result != self.mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('paho_subscribe_submission_failed')
        got, codes = await asyncio.to_thread(self._get, self.subacks)
        if got != mid or codes != (min(qos, 1),):
            raise AssertionError('session_subscription_refused')
        return mid

    async def publish(self, topic, payload, qos=1):
        info = self.client.publish(topic, payload, qos=qos, retain=False)
        if info.rc != self.mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('paho_publish_submission_failed')
        await asyncio.to_thread(info.wait_for_publish, TIMEOUT)
        if not info.is_published():
            raise TimeoutError('Paho publication deadline')
        mid, reason = await asyncio.to_thread(self._get, self.pubacks)
        if mid != info.mid or reason >= 128:
            raise AssertionError('session_publication_not_completed')
        return mid

    async def receive(self, expected_payload=None):
        row = await asyncio.to_thread(self._get, self.messages)
        if expected_payload is not None and row['payload_sha256'] != hashlib.sha256(expected_payload).hexdigest():
            raise AssertionError('session_recipient_payload_mismatch')
        if row['qos'] != 1:
            raise AssertionError('session_recipient_qos_mismatch')
        if self.client.ack(row['packet_id'], 1) != self.mqtt.MQTT_ERR_SUCCESS:
            raise AssertionError('paho_ack_submission_failed')
        self.log.record('puback_submitted', packet_id=row['packet_id'])
        return row

    async def disconnect(self, expiry=None):
        if self.closed:
            return self.result
        properties = None
        if expiry is not None:
            options(False, expiry)
            from paho.mqtt.packettypes import PacketTypes
            from paho.mqtt.properties import Properties as PahoProperties
            properties = PahoProperties(PacketTypes.DISCONNECT)
            properties.SessionExpiryInterval = expiry
        self.log.record('disconnect_submitted', expiry_present=expiry is not None,
                        session_expiry_interval=expiry)
        try:
            self.client.disconnect(properties=properties)
            await asyncio.to_thread(self.disconnected.wait, 3)
            await asyncio.wait_for(asyncio.to_thread(self.client.loop_stop), timeout=3)
            self.result.update(transport_closed=True, connections=self.connections)
            if self.result['status'] == 'connected':
                self.result['status'] = 'success'
            self.log.record('local_disconnect_complete', connections=self.connections)
        finally:
            self.closed = True
            self.log.close()
        return self.result


async def exchange(host, port, client_id, *, implementation='mqttium', clean_start=False,
                   expiry=INFINITE, disconnect_expiry=None, topic=None, payload=b'lifecycle',
                   qos=1, expected_session_present=None, events_path=None):
    kind = MqttiumSession if implementation == 'mqttium' else PahoSession
    session = kind((host, port), client_id, clean_start=clean_start, expiry=expiry,
                   events_path=events_path)
    try:
        await session.open(expected_session_present)
        if topic is not None:
            await session.subscribe(topic)
            await session.publish(topic, payload, qos=qos)
            await session.receive(payload)
        return session.result
    finally:
        await session.disconnect(disconnect_expiry)


def expiry_argument(value):
    if value == 'absent':
        return None
    if value == 'infinite':
        return INFINITE
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError('expiry must be absent, infinite or a decimal MQTT interval') from error
    if not 0 <= parsed <= INFINITE:
        raise argparse.ArgumentTypeError('expiry must be in 0..4294967295')
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', choices=('mqttium', 'paho'), default='mqttium')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--client-id', required=True)
    parser.add_argument('--clean-start', type=int, choices=(0, 1), required=True)
    parser.add_argument('--expiry', type=expiry_argument, required=True)
    parser.add_argument('--disconnect-expiry', type=expiry_argument, default=None)
    parser.add_argument('--expected-session-present', type=int, choices=(0, 1))
    parser.add_argument('--topic')
    parser.add_argument('--payload', default='lifecycle')
    parser.add_argument('--qos', type=int, choices=(1, 2), default=1)
    parser.add_argument('--events', type=Path)
    args = parser.parse_args()
    result = asyncio.run(exchange(args.host, args.port, args.client_id,
                                 implementation=args.client, clean_start=bool(args.clean_start),
                                 expiry=args.expiry, disconnect_expiry=args.disconnect_expiry,
                                 topic=args.topic, payload=args.payload.encode(), qos=args.qos,
                                 expected_session_present=None if args.expected_session_present is None else
                                 bool(args.expected_session_present), events_path=args.events))
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
