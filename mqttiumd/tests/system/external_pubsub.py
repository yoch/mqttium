#!/usr/bin/env python3
"""Bounded mqttium/Paho pub/sub clients using only their public APIs.

With ``ack_policy='hold'`` no terminal acknowledgement is requested until the
optional release file exists. Without that file, receive returns and disconnects
with its messages deliberately unacknowledged. Sleeping after auto-ACK delivery
is never used as evidence that a broker obligation remains pending.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import queue
import threading
import time

from external_mqttium import Events, client_identity
from mqttium import MQTTProtocolVersion, MQTTTimeoutError, PublishRejectedError, SubscribeError
from mqttium.api import AsyncClient, Properties


class Observations(Events):
    def __init__(self, path: Path | None):
        super().__init__(path)
        self.lock = threading.RLock()

    def record(self, kind: str, **fields) -> None:
        with self.lock:
            super().record(kind, **fields)


def _check(filters, count, qos, wait_timeout, ack_policy, receive_maximum, maximum_packet_size):
    if not 0 <= count <= 32 or not 0 < wait_timeout <= 30:
        raise ValueError('count must be 0..32 and wait timeout in (0,30]')
    if len(filters) > 8 or any(not value or len(value.encode()) > 1024 for value in filters):
        raise ValueError('at most eight nonempty filters of at most 1024 bytes')
    if qos not in (0, 1, 2) or ack_policy not in ('immediate', 'hold'):
        raise ValueError('invalid requested QoS or acknowledgement policy')
    if not 1 <= receive_maximum <= 32 or not 64 <= maximum_packet_size <= 65536:
        raise ValueError('bounded client receive window and packet size required')


def _message(message):
    payload = bytes(message.payload)
    topic = message.topic
    return {'topic': topic[:128], 'topic_truncated': len(topic) > 128,
            'topic_sha256': hashlib.sha256(topic.encode()).hexdigest(),
            'payload_hex': payload[:128].hex(), 'payload_bytes': len(payload),
            'payload_truncated': len(payload) > 128,
            'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'qos': int(message.qos), 'packet_id': message.mid if int(message.qos) else None,
            'client_message_id': message.mid,
            'dup': bool(message.dup), 'retain': bool(message.retain)}


def _base(name, identity, host, port, client_id, observations, **parameters):
    observations.record('client_identity', **identity)
    return {'client': name, 'version': identity['version'], 'identity': identity,
            'pid': os.getpid(), 'python': platform.python_version(),
            'host': host, 'port': port, 'client_id': client_id,
            'status': 'success', 'events': observations.events,
            'subscriptions': [], 'unsubscriptions': [], 'messages': [], 'publications': [],
            'parameters': {'protocol': 5, 'clean_start': False,
                           'session_expiry_interval': 0xFFFFFFFF, **parameters}}


def _subscription_event(row):
    """Retain complete filters in the receipt, bounded names plus hashes in each event."""
    return {**row, 'filters': [value[:128] for value in row['filters']],
            'filters_truncated': any(len(value) > 128 for value in row['filters']),
            'filter_sha256': [hashlib.sha256(value.encode()).hexdigest() for value in row['filters']]}


def effective_qos(published_qos: int, granted_qos: int) -> int:
    """MQTT matches the smaller of the published QoS and the grant."""
    if published_qos not in (0, 1, 2) or granted_qos not in (0, 1, 2):
        raise ValueError('published and granted QoS must be 0, 1 or 2')
    return min(published_qos, granted_qos)


def _expected_qoses(requested, granted, delivery):
    if granted is not None and granted not in (0, 1, 2):
        raise ValueError('expected granted QoS must be 0, 1 or 2')
    if delivery is not None and delivery not in (0, 1, 2):
        raise ValueError('expected delivery QoS must be 0, 1, 2 or None for mixed delivery')
    return requested if granted is None else granted


async def _mqttium(action, host, port, client_id, *, filters=(), count=1, qos=1,
                   events_path=None, ack_policy='immediate', ack_release=None,
                   expected_session_present=None, subscribe=True, wait_timeout=5,
                   receive_maximum=8, maximum_packet_size=65536,
                   topic='reception/pubsub', payload=b'pubsub',
                   expected_granted_qos=None, expected_delivery_qos=1):
    filters = tuple(filters)
    _check(filters, count, qos, wait_timeout, ack_policy, receive_maximum, maximum_packet_size)
    granted = _expected_qoses(qos, expected_granted_qos, expected_delivery_qos)
    if action == 'publish' and len(payload) > 32768:
        raise ValueError('publisher supports bounded QoS 0/1/2 only')
    observations = Observations(events_path)
    result = _base('mqttium', client_identity(), host, port, client_id, observations,
                   action=action, filters=list(filters), requested_qos=qos, count=count,
                   ack_policy=ack_policy, receive_maximum=receive_maximum,
                   maximum_packet_size=maximum_packet_size, wait_timeout=wait_timeout,
                   expected_granted_qos=granted, expected_delivery_qos=expected_delivery_qos)
    client = AsyncClient(client_id, protocol=MQTTProtocolVersion.MQTTv5,
                         clean_start=False, keepalive=10,
                         connect_properties=Properties({'session_expiry_interval': 0xFFFFFFFF}),
                         maximum_packet_size=maximum_packet_size,
                         max_inbound_inflight=receive_maximum,
                         max_inbound_inflight_bytes=65536, max_outbound_inflight=8,
                         max_unacknowledged_messages=32, max_unacknowledged_bytes=65536,
                         max_iterator_messages=32, max_iterator_bytes=65536,
                         max_write_queue_messages=32, max_write_queue_bytes=65536,
                         message_delivery='iterator', manual_ack=True, connect_timeout=3)
    try:
        connack = await client.connect(host, port, timeout=3)
        result['session_present'] = connack.session_present
        observations.record('connack', session_present=connack.session_present,
                            reason_code=connack.reason_code)
        if expected_session_present is not None and connack.session_present != expected_session_present:
            raise AssertionError('unexpected Session Present')
        observations.record('connected_ready', session_present=connack.session_present)
        if action in ('receive', 'subscribe') and subscribe:
            if not filters:
                raise ValueError('subscribe requires at least one filter')
            ack = await client.subscribe([(value, qos) for value in filters], timeout=wait_timeout)
            row = {'packet_id': ack.mid, 'filters': list(filters),
                   'reason_codes': list(ack.reason_codes),
                   'granted_qos': [int(value) if value is not None else None for value in ack.granted_qos]}
            result['subscriptions'].append(row)
            observations.record('suback', **_subscription_event(row))
            if ack.reason_codes != (granted,) * len(filters):
                raise AssertionError('unexpected granted QoS in request order')
            observations.record('subscription_ready', **_subscription_event(row))
        if action == 'unsubscribe':
            ack = await client.unsubscribe(filters, timeout=wait_timeout)
            row = {'packet_id': ack.mid, 'filters': list(filters), 'reason_codes': list(ack.reason_codes)}
            result['unsubscriptions'].append(row)
            observations.record('unsuback', **_subscription_event(row))
            if any(code >= 128 for code in ack.reason_codes):
                result['status'] = 'rejected'
        elif action == 'publish':
            receipt = await client.publish(topic, payload, qos=qos, retain=False, timeout=wait_timeout)
            row = {'packet_id': receipt.mid if qos else None,
                   'client_receipt_id': receipt.mid, 'qos': qos, 'topic': topic,
                   'completion_scope': 'mqtt-acknowledgement' if qos else 'local-write-only',
                   'payload_sha256': hashlib.sha256(payload).hexdigest()}
            result['publications'].append(row)
            observations.record('publish', **row)
            await receipt.wait(timeout=wait_timeout)
            if not receipt.is_done() or receipt.exception() is not None:
                raise AssertionError('publication receipt did not complete successfully')
            row['result'] = 'success'
            observations.record('receipt_success' if qos else 'local_write_complete',
                                packet_id=row['packet_id'], client_receipt_id=receipt.mid,
                                qos=qos, completion_scope=row['completion_scope'])
        elif action == 'receive':
            iterator = client.messages()
            async with asyncio.timeout(wait_timeout):
                for _ in range(count):
                    message = await anext(iterator)
                    row = _message(message)
                    result['messages'].append(row)
                    observations.record('message', **row)
                    if int(message.qos) not in (0, 1, 2) or (expected_delivery_qos is not None
                                                        and int(message.qos) != expected_delivery_qos):
                        raise AssertionError('unexpected outgoing QoS')
                    if not int(message.qos):
                        if message.dup:
                            raise AssertionError('QoS 0 delivery must have DUP=0')
                        observations.record('qos0_received_no_ack', packet_id=None)
                        continue
                    if ack_policy == 'hold':
                        observations.record('ack_held', packet_id=message.mid)
                        if ack_release is None:
                            continue
                        while not Path(ack_release).exists():
                            await asyncio.sleep(.005)
                        observations.record('ack_release_observed', packet_id=message.mid)
                    await client.ack(message)
                    observations.record('manual_ack_submitted', packet_id=message.mid, qos=int(message.qos))
    except SubscribeError as error:
        result['status'] = 'rejected'
        row = {'packet_id': error.result.mid, 'filters': list(filters),
               'reason_codes': list(error.result.reason_codes)}
        result['subscriptions'].append(row)
        observations.record('subscription_rejected', **_subscription_event(row))
    except PublishRejectedError as error:
        result['status'] = 'rejected'
        observations.record('publication_rejected', reason_code=error.reason_code)
    except (TimeoutError, MQTTTimeoutError):
        result['status'] = 'timeout'
        observations.record('timeout', phase=action)
    except Exception as error:
        result['status'] = 'error'
        result['failure'] = {'code': type(error).__name__, 'detail': str(error)[:256]}
        observations.record('client_error', **result['failure'])
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=3)
        except Exception as error:
            result['cleanup_error'] = type(error).__name__
            if result['status'] == 'success':
                result['status'] = 'error'
        finally:
            observations.record('client_stopped', state=client.state.name)
            observations.close()
    return result


async def mqttium_receive(host, port, client_id, filters=(), count=1, **options):
    return await _mqttium('receive', host, port, client_id, filters=filters, count=count, **options)


async def mqttium_publish(host, port, client_id, topic, payload, qos=1, **options):
    return await _mqttium('publish', host, port, client_id, topic=topic, payload=payload, qos=qos, **options)


async def mqttium_subscribe(host, port, client_id, filters, qos=1, **options):
    return await _mqttium('subscribe', host, port, client_id, filters=filters, qos=qos, **options)


async def mqttium_unsubscribe(host, port, client_id, filters, **options):
    return await _mqttium('unsubscribe', host, port, client_id, filters=filters, **options)


def _paho(action, host, port, client_id, *, filters=(), count=1, qos=1,
          events_path=None, ack_policy='immediate', ack_release=None,
          expected_session_present=None, subscribe=True, wait_timeout=5,
          receive_maximum=8, maximum_packet_size=65536,
          topic='reception/pubsub', payload=b'pubsub',
          expected_granted_qos=None, expected_delivery_qos=1):
    import paho.mqtt.client as mqtt
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties as PahoProperties

    filters = tuple(filters)
    _check(filters, count, qos, wait_timeout, ack_policy, receive_maximum, maximum_packet_size)
    granted = _expected_qoses(qos, expected_granted_qos, expected_delivery_qos)
    if action == 'publish' and len(payload) > 32768:
        raise ValueError('publisher supports bounded QoS 0/1/2 only')
    version = importlib.metadata.version('paho-mqtt')
    if version != '2.1.0':
        raise RuntimeError('paho-mqtt 2.1.0 required')
    identity = {'name': 'paho-mqtt', 'version': version, 'module_path': str(Path(mqtt.__file__).resolve()),
                'module_sha256': hashlib.sha256(Path(mqtt.__file__).read_bytes()).hexdigest()}
    observations = Observations(events_path)
    result = _base('paho-mqtt', identity, host, port, client_id, observations,
                   action=action, filters=list(filters), requested_qos=qos, count=count,
                   ack_policy=ack_policy, receive_maximum=receive_maximum,
                   maximum_packet_size=maximum_packet_size, wait_timeout=wait_timeout,
                   expected_granted_qos=granted, expected_delivery_qos=expected_delivery_qos)
    connected, subscribed, unsubscribed, published = (threading.Event() for _ in range(4))
    incoming = queue.Queue(maxsize=32)
    callbacks = {}

    def on_connect(_client, _userdata, flags, reason, _properties):
        result['session_present'] = flags.session_present
        callbacks['connect_reason'] = reason.value
        observations.record('connack', session_present=flags.session_present, reason_code=reason.value)
        connected.set()

    def on_subscribe(_client, _userdata, mid, reasons, _properties):
        callbacks['suback'] = {'packet_id': mid, 'filters': list(filters),
                               'reason_codes': [reason.value for reason in reasons]}
        subscribed.set()

    def on_unsubscribe(_client, _userdata, mid, reasons, _properties):
        callbacks['unsuback'] = {'packet_id': mid, 'filters': list(filters),
                                 'reason_codes': [reason.value for reason in reasons]}
        unsubscribed.set()

    def on_publish(_client, _userdata, mid, reason, _properties):
        callbacks['publish'] = {'packet_id': mid if qos else None, 'client_receipt_id': mid,
                                'completion_scope': 'mqtt-acknowledgement' if qos else 'local-write-only',
                                'qos': qos, 'reason_code': reason.value,
                                'result': 'rejected' if reason.value >= 128 else 'success'}
        published.set()

    def on_message(_client, _userdata, message):
        try:
            incoming.put_nowait(message)
        except queue.Full:
            callbacks['error'] = 'message_queue_full'

    def on_disconnect(_client, _userdata, _flags, reason, _properties):
        observations.record('disconnected', reason_code=reason.value)

    def wait(event, phase):
        if not event.wait(wait_timeout):
            raise TimeoutError(phase)
        if 'error' in callbacks:
            raise RuntimeError(callbacks['error'])

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=client_id, protocol=mqtt.MQTTv5,
                         reconnect_on_failure=False, manual_ack=True)
    client.max_queued_messages_set(32)
    client.max_inflight_messages_set(8)
    client.on_connect, client.on_subscribe = on_connect, on_subscribe
    client.on_unsubscribe, client.on_publish, client.on_message = on_unsubscribe, on_publish, on_message
    client.on_disconnect = on_disconnect
    properties = PahoProperties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 0xFFFFFFFF
    properties.ReceiveMaximum = receive_maximum
    properties.MaximumPacketSize = maximum_packet_size
    try:
        client.connect(host, port, keepalive=10, clean_start=False, properties=properties)
        client.loop_start()
        wait(connected, 'CONNACK')
        if callbacks['connect_reason'] != 0:
            raise AssertionError('CONNECT refused')
        if expected_session_present is not None and result['session_present'] != expected_session_present:
            raise AssertionError('unexpected Session Present')
        observations.record('connected_ready', session_present=result['session_present'])
        if action in ('receive', 'subscribe') and subscribe:
            if not filters:
                raise ValueError('subscribe requires at least one filter')
            code, mid = client.subscribe([(value, qos) for value in filters])
            if code != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('SUBSCRIBE submission refused')
            wait(subscribed, 'SUBACK')
            row = callbacks['suback'];assert row['packet_id'] == mid
            result['subscriptions'].append(row)
            observations.record('suback', **_subscription_event(row))
            if any(reason >= 128 for reason in row['reason_codes']):
                result['status'] = 'rejected'
                observations.record('subscription_rejected', **_subscription_event(row))
                return result
            if row['reason_codes'] != [granted] * len(filters):
                raise AssertionError('unexpected granted QoS in request order')
            observations.record('subscription_ready', **_subscription_event(row))
        if action == 'unsubscribe':
            code, mid = client.unsubscribe(list(filters))
            if code != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('UNSUBSCRIBE submission refused')
            wait(unsubscribed, 'UNSUBACK')
            row = callbacks['unsuback'];assert row['packet_id'] == mid
            result['unsubscriptions'].append(row)
            observations.record('unsuback', **_subscription_event(row))
            if any(reason >= 128 for reason in row['reason_codes']):
                result['status'] = 'rejected'
        elif action == 'publish':
            info = client.publish(topic, payload, qos=qos, retain=False)
            observations.record('publish', packet_id=info.mid if qos else None,
                                client_receipt_id=info.mid, qos=qos, topic=topic,
                                payload_sha256=hashlib.sha256(payload).hexdigest())
            info.wait_for_publish(timeout=wait_timeout)
            if not info.is_published():
                raise TimeoutError('publication receipt')
            wait(published, 'publication callback')
            row = callbacks['publish'];assert row['client_receipt_id'] == info.mid
            result['publications'].append(row)
            result['status'] = row['result']
            event = ('local_write_complete' if qos == 0 else 'receipt_success') if result['status'] == 'success' else 'publication_rejected'
            observations.record(event, **row)
        elif action == 'receive':
            deadline = time.monotonic() + wait_timeout
            for _ in range(count):
                message = incoming.get(timeout=max(.001, deadline - time.monotonic()))
                row = _message(message)
                result['messages'].append(row)
                observations.record('message', **row)
                if message.qos not in (0, 1, 2) or (expected_delivery_qos is not None
                                               and message.qos != expected_delivery_qos):
                    raise AssertionError('unexpected outgoing QoS')
                if message.qos == 0:
                    if message.dup:
                        raise AssertionError('QoS 0 delivery must have DUP=0')
                    observations.record('qos0_received_no_ack', packet_id=None)
                    continue
                if ack_policy == 'hold':
                    observations.record('ack_held', packet_id=message.mid)
                    if ack_release is None:
                        continue
                    while not Path(ack_release).exists():
                        if time.monotonic() >= deadline:
                            raise TimeoutError('manual acknowledgement release')
                        time.sleep(.005)
                    observations.record('ack_release_observed', packet_id=message.mid)
                if client.ack(message.mid, message.qos) != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError('manual acknowledgement submission refused')
                observations.record('manual_ack_submitted', packet_id=message.mid, qos=message.qos)
    except (TimeoutError, queue.Empty):
        result['status'] = 'timeout'
        observations.record('timeout', phase=action)
    except Exception as error:
        result['status'] = 'error'
        result['failure'] = {'code': type(error).__name__, 'detail': str(error)[:256]}
        observations.record('client_error', **result['failure'])
    finally:
        client.disconnect()
        client.loop_stop()
        observations.record('client_stopped')
        observations.close()
    return result


def paho_receive(host, port, client_id, filters=(), count=1, **options):
    return _paho('receive', host, port, client_id, filters=filters, count=count, **options)


def paho_publish(host, port, client_id, topic, payload, qos=1, **options):
    return _paho('publish', host, port, client_id, topic=topic, payload=payload, qos=qos, **options)


def paho_subscribe(host, port, client_id, filters, qos=1, **options):
    return _paho('subscribe', host, port, client_id, filters=filters, qos=qos, **options)


def paho_unsubscribe(host, port, client_id, filters, **options):
    return _paho('unsubscribe', host, port, client_id, filters=filters, **options)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--client-id', required=True)
    parser.add_argument('--client', choices=('mqttium', 'paho'), default='mqttium')
    parser.add_argument('--action', choices=('receive', 'publish', 'subscribe', 'unsubscribe'), required=True)
    parser.add_argument('--filter', action='append', default=[])
    parser.add_argument('--qos', type=int, choices=(0, 1, 2), default=1)
    parser.add_argument('--topic', default='reception/pubsub')
    parser.add_argument('--payload', default='pubsub')
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--events', type=Path)
    parser.add_argument('--expected-session-present', type=int, choices=(0, 1))
    parser.add_argument('--resume-without-subscribe', action='store_true')
    parser.add_argument('--hold-acks', action='store_true')
    parser.add_argument('--release-acks', type=Path)
    parser.add_argument('--wait-timeout', type=float, default=5)
    parser.add_argument('--receive-maximum', type=int, default=8)
    parser.add_argument('--maximum-packet-size', type=int, default=65536)
    parser.add_argument('--expected-granted-qos', type=int, choices=(0, 1, 2))
    expected = parser.add_mutually_exclusive_group()
    expected.add_argument('--expected-delivery-qos', type=int, choices=(0, 1, 2), default=1)
    expected.add_argument('--allow-mixed-delivery-qos', action='store_true')
    args = parser.parse_args()
    options = dict(filters=args.filter, count=args.count, qos=args.qos,
                   events_path=args.events, expected_session_present=None if args.expected_session_present is None
                   else bool(args.expected_session_present), subscribe=not args.resume_without_subscribe,
                   ack_policy='hold' if args.hold_acks else 'immediate', ack_release=args.release_acks,
                   wait_timeout=args.wait_timeout, receive_maximum=args.receive_maximum,
                   maximum_packet_size=args.maximum_packet_size, topic=args.topic, payload=args.payload.encode(),
                   expected_granted_qos=args.expected_granted_qos,
                   expected_delivery_qos=None if args.allow_mixed_delivery_qos else args.expected_delivery_qos)
    result = asyncio.run(_mqttium(args.action, args.host, args.port, args.client_id, **options)) \
        if args.client == 'mqttium' else _paho(args.action, args.host, args.port, args.client_id, **options)
    print(json.dumps(result, indent=2, sort_keys=True))
    return {'success': 0, 'rejected': 2, 'timeout': 3, 'error': 1}[result['status']]


if __name__ == '__main__':
    raise SystemExit(main())
