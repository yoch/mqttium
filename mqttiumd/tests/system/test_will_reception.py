"""Independent byte peers check the new public Will drivers and exact causes."""
from __future__ import annotations

import asyncio
import hashlib
import unittest

from external_will import MqttiumWill, PahoWill, Will
from tcp_probe import Frame, FrameDecoder, ReceiptFailure
from test_external_clients import ScriptedBroker, read_packet
from will_probe import decode_will_connect, will_connect_packet


class WillProbeTests(unittest.TestCase):
    def test_explicit_absent_and_zero_delay_are_distinct(self):
        for delay in (None, 0):
            data = will_connect_packet('sensor', 'atelier/sensor/status', delay=delay)
            decoder = FrameDecoder(65536)
            frames = decoder.feed(data)
            decoder.finish()
            row = decode_will_connect(frames[0])
            self.assertEqual(row['client_id'], 'sensor')
            self.assertEqual(row['will_qos'], 1)
            self.assertFalse(row['will_retain'])
            self.assertEqual(row['will_delay_present'], delay is not None)
            self.assertEqual(row['will_delay_interval'], delay)
            self.assertEqual(row['will_payload_sha256'], hashlib.sha256(b'offline').hexdigest())

    def test_will_shape_is_bounded_and_logs_no_payload(self):
        row = Will('atelier/sensor/status').observation()
        self.assertNotIn('payload', row)
        for topic, payload, delay in (('', b'x', 0), ('a/+', b'x', 0),
                                      ('a', b'x' * 32769, 0), ('a', b'x', 1)):
            with self.assertRaises(ValueError):
                Will(topic, payload, delay)

    def test_duplicate_will_properties_are_explicit(self):
        data = will_connect_packet('sensor', 'status', will_properties=b'\x18' + b'\0' * 4 + b'\x18' + b'\0' * 4)
        frame = FrameDecoder(65536).feed(data)[0]
        with self.assertRaisesRegex(ReceiptFailure, 'will_probe_unexpected_or_duplicate_property'):
            decode_will_connect(frame)


class PublicWillDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_public_clients_send_will_and_normal_disconnect(self):
        for constructor in (MqttiumWill, PahoWill):
            for delay in (None, 0):
                with self.subTest(client=constructor.__name__, delay=delay):
                    observations = []
                    async def script(reader, writer, _connection):
                        header, body = await read_packet(reader)
                        observations.append(decode_will_connect(Frame(header, body)))
                        writer.write(b'\x20\x03\x00\x00\x00')
                        await writer.drain()
                        header, body = await read_packet(reader)
                        self.assertEqual(header, 0xE0)
                        self.assertTrue(not body or body[0] == 0)

                    async with ScriptedBroker(script) as broker:
                        client = constructor(('127.0.0.1', broker.port), 'public-will',
                                             will=Will('atelier/sensor/status', delay=delay))
                        try:
                            await client.open(False)
                        finally:
                            await client.disconnect()
                    self.assertEqual(len(observations), 1)
                    self.assertEqual(observations[0]['will_qos'], 1)
                    self.assertFalse(observations[0]['will_retain'])
                    self.assertEqual(observations[0]['will_delay_interval'], delay)
                    self.assertEqual(client.result['connections'], 1)

    async def test_broker_connack_refusal_has_precise_will_reception_cause(self):
        for constructor in (MqttiumWill, PahoWill):
            with self.subTest(client=constructor.__name__):
                async def script(reader, writer, _connection):
                    header, body = await read_packet(reader)
                    decode_will_connect(Frame(header, body))
                    writer.write(b'\x20\x03\x00\x83\x00')
                    await writer.drain()
                    await reader.read()

                async with ScriptedBroker(script) as broker:
                    client = constructor(('127.0.0.1', broker.port), 'refused-will', will=Will('status'))
                    try:
                        with self.assertRaisesRegex(ReceiptFailure, 'will_connect_not_accepted'):
                            await client.open(False)
                    finally:
                        await client.disconnect()
