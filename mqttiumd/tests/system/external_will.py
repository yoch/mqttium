#!/usr/bin/env python3
"""Bounded public mqttium 1.3/Paho 2.1 Will clients and killable sensor.

The parent reception owns the process signal and wait status. The sensor never
uses disconnect() to imitate a transport loss, nor private client attributes.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import time

from external_retained import MqttiumRetained, PahoRetained
from external_sessions import INFINITE
from external_wildcard import PacketTap as PlainTap
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, PublishMessage
from retained_probe import decode_publish
from tcp_probe import ReceiptFailure, require, write_json


@dataclass(frozen=True)
class Will:
    topic: str
    payload: bytes = b'offline'
    delay: int | None = 0

    def __post_init__(self):
        if (not isinstance(self.topic, str) or not 0 < len(self.topic.encode()) <= 1024
                or any(value in self.topic for value in ('\0', '+', '#'))
                or not isinstance(self.payload, bytes) or len(self.payload) > 32768
                or not (self.delay is None or type(self.delay) is int and self.delay == 0)):
            raise ValueError('non-retained QoS1 Will, exact topic, bounded payload and delay0 required')

    def observation(self):
        return {'topic': self.topic, 'payload_sha256': hashlib.sha256(self.payload).hexdigest(),
                'payload_bytes': len(self.payload), 'qos': 1, 'retain': False,
                'will_delay_present': self.delay is not None, 'will_delay_interval': self.delay}


class PacketTap(PlainTap):
    def publications(self, payload=None):
        values = [decode_publish(frame) for frame in self.server_frames() if frame.kind == 0x30]
        return values if payload is None else [value for value in values if value.payload == payload]


class MqttiumWill(MqttiumRetained):
    def __init__(self, address, client_id, *, will=None, clean_start=False, expiry=INFINITE,
                 events_path=None, identity=None):
        super().__init__(address, client_id, clean_start=clean_start, expiry=expiry,
                         events_path=events_path, identity=identity)
        if will is not None:
            require(isinstance(will, Will), 'will_public_client_invalid_spec')
            self.parameters['will'] = will.observation()
            # The inherited helper has not connected or started any tasks.
            # Reuse its observations/manual-ACK reader with an explicitly
            # configured public AsyncClient constructor, not engine mutation.
            self.client = AsyncClient(
                client_id, protocol=MQTTProtocolVersion.MQTTv5, clean_start=clean_start,
                keepalive=10, connect_properties=None if expiry is None else
                Properties({'session_expiry_interval': expiry}), maximum_packet_size=65536,
                max_inbound_inflight=8, max_inbound_inflight_bytes=65536,
                max_outbound_inflight=8, max_unacknowledged_messages=32,
                max_unacknowledged_bytes=65536, max_write_queue_messages=32,
                max_write_queue_bytes=65536, message_delivery='iterator', manual_ack=True,
                max_iterator_messages=32, max_iterator_bytes=65536, connect_timeout=3,
                will=PublishMessage(will.topic, will.payload, qos=1, retain=False,
                                    properties=None if will.delay is None else
                                    Properties({'will_delay_interval': will.delay})))

    async def open(self, expected=None):
        try:
            return await super().open(expected)
        except Exception as error:
            if 'will' in self.parameters and self.result.get('reason_code') is not None:
                raise ReceiptFailure('will_connect_not_accepted',
                                     str(self.result['reason_code'])) from error
            raise


class PahoWill(PahoRetained):
    def __init__(self, address, client_id, *, will=None, clean_start=False, expiry=INFINITE,
                 events_path=None):
        super().__init__(address, client_id, clean_start=clean_start, expiry=expiry,
                         events_path=events_path)
        if will is not None:
            require(isinstance(will, Will), 'will_public_client_invalid_spec')
            from paho.mqtt.packettypes import PacketTypes
            from paho.mqtt.properties import Properties as PahoProperties
            properties = None
            if will.delay is not None:
                properties = PahoProperties(PacketTypes.WILLMESSAGE)
                properties.WillDelayInterval = will.delay
            self.client.will_set(will.topic, will.payload, qos=1, retain=False, properties=properties)
            self.parameters['will'] = will.observation()

    async def open(self, expected=None):
        try:
            return await super().open(expected)
        except Exception as error:
            if 'will' in self.parameters and self.result.get('reason_code') is not None:
                raise ReceiptFailure('will_connect_not_accepted',
                                     str(self.result['reason_code'])) from error
            raise


async def sensor(arguments):
    constructor = MqttiumWill if arguments.client == 'mqttium' else PahoWill
    will = Will(arguments.topic, b'offline', None if arguments.delay_absent else 0)
    client = constructor((arguments.host, arguments.port), arguments.client_id, will=will,
                         expiry=arguments.expiry, events_path=arguments.events)
    result = {'client': arguments.client, 'client_id': arguments.client_id, 'pid': os.getpid(),
              'will': will.observation(), 'status': 'not_connected', 'identity': client.result['identity']}
    try:
        await client.open(False)
        await client.publish(arguments.topic, b'online', qos=1, retain=True)
        result.update(status='ready', session_present=client.result['session_present'],
                      reason_code=client.result['reason_code'])
        client.log.record('sensor_ready', pid=os.getpid(),
                          meaning='positive CONNACK and retained online MQTT PUBACK received')
        write_json(arguments.ready, result)
        deadline = time.monotonic() + 30
        while not arguments.finish.exists():
            require(time.monotonic() < deadline, 'will_sensor_control_deadline')
            await asyncio.sleep(.01)
        await client.disconnect()
        result.update(status='normal_disconnect_completed', result=client.result)
        write_json(arguments.result, result)
        return 0
    except Exception as error:
        result.update(status='FAIL', code=getattr(error, 'code', type(error).__name__),
                      detail=str(error)[:512], result=client.result)
        write_json(arguments.result, result)
        return 1
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', choices=('mqttium', 'paho'), required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--client-id', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--expiry', type=int, default=INFINITE)
    parser.add_argument('--delay-absent', action='store_true')
    for name in ('events', 'ready', 'finish', 'result'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    return asyncio.run(sensor(args))


if __name__ == '__main__':
    raise SystemExit(main())
