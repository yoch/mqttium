"""Discriminating checks of the public session driver and independent evidence."""
import asyncio
import hashlib
from pathlib import Path
import tempfile
import unittest

from external_sessions import INFINITE, MqttiumSession, PahoSession, exchange, options
from session_reception import CARDINALITY_ZERO, assert_drained, cardinality_drained
from test_external_clients import ScriptedBroker, read_packet
from tcp_probe import ReceiptFailure


def connect_fields(header, body):
    if header != 0x10 or body[:7] != b'\x00\x04MQTT\x05':
        raise AssertionError('expected explicit MQTT5 CONNECT')
    offset, length = 11, body[10]
    end = offset + length
    properties = {}
    while offset < end:
        key = body[offset]
        size = {0x11: 4, 0x21: 2, 0x27: 4, 0x22: 2}[key]
        properties[key] = int.from_bytes(body[offset + 1:offset + 1 + size], 'big')
        offset += 1 + size
    return bool(body[7] & 2), properties


async def accept(reader, writer, *, clean=False, expiry=INFINITE, present=False):
    actual_clean, properties = connect_fields(*(await read_packet(reader)))
    if actual_clean != clean or properties.get(0x11) != expiry:
        raise AssertionError('wrong Clean Start/expiry wire decision')
    if properties.get(0x21) != 8 or properties.get(0x27) != 65536:
        raise AssertionError('missing explicit client bounds')
    writer.write(bytes((0x20, 3, int(present), 0, 0)))
    await writer.drain()


class SessionClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_mqttium_matrix_is_encoded_by_public_api(self):
        for clean in (False, True):
            for expiry in (None, 0, INFINITE):
                with self.subTest(clean=clean, expiry=expiry):
                    async def script(reader, writer, _connection):
                        await accept(reader, writer, clean=clean, expiry=expiry)
                        self.assertEqual(await read_packet(reader), (0xE0, b''))
                    async with ScriptedBroker(script) as broker:
                        result = await exchange('127.0.0.1', broker.port, 'session-matrix',
                                                clean_start=clean, expiry=expiry,
                                                expected_session_present=False)
                    self.assertEqual(result['status'], 'success')
                    self.assertEqual(result['connections'], 1)
                    self.assertEqual(result['identity']['version'], '1.3.0')
                    self.assertEqual(result['parameters']['session_expiry_present'], expiry is not None)

    async def test_mqttium_activity_waits_for_receipt_and_actual_recipient(self):
        payload = b'received-by-same-public-client'
        topic = b'lifecycle/unit'
        async def script(reader, writer, _connection):
            await accept(reader, writer, clean=True, expiry=0)
            header, body = await read_packet(reader)
            self.assertEqual(header, 0x82)
            writer.write(b'\x90\x04' + body[:2] + b'\x00\x01')
            await writer.drain()
            header, body = await read_packet(reader)
            self.assertEqual(header, 0x34)
            size = int.from_bytes(body[:2], 'big')
            mid = body[2 + size:4 + size]
            writer.write(b'\x50\x02' + mid)
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0x62, mid))
            writer.write(b'\x70\x02' + mid)
            outgoing = len(topic).to_bytes(2, 'big') + topic + b'\x00\x07\x00' + payload
            writer.write(bytes((0x32, len(outgoing))) + outgoing)
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0x40, b'\x00\x07'))
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            result = await exchange('127.0.0.1', broker.port, 'session-self', clean_start=True,
                                    expiry=0, topic=topic.decode(), payload=payload, qos=2)
        kinds = [event['kind'] for event in result['events']]
        self.assertIn('publication_completed', kinds)
        self.assertIn('message', kinds)
        self.assertIn('puback_submitted', kinds)
        self.assertEqual(next(e for e in result['events'] if e['kind'] == 'message')['payload_sha256'],
                         hashlib.sha256(payload).hexdigest())

    async def test_paho_disconnect_expiry_property_is_observed_on_wire(self):
        for expiry in (None, 0, INFINITE):
            with self.subTest(expiry=expiry):
                async def script(reader, writer, _connection):
                    await accept(reader, writer)
                    frame = await read_packet(reader)
                    expected = b'' if expiry is None else b'\x00\x05\x11' + expiry.to_bytes(4, 'big')
                    self.assertEqual(frame, (0xE0, expected))
                async with ScriptedBroker(script) as broker:
                    result = await exchange('127.0.0.1', broker.port, 'paho-disconnect',
                                            implementation='paho', disconnect_expiry=expiry)
                self.assertEqual(result['status'], 'success')
                self.assertEqual(result['connections'], 1)
                self.assertFalse(any(e['kind'] == 'broker_session_closed' for e in result['events']))

    async def test_mqttium_does_not_access_private_disconnect_properties(self):
        session = MqttiumSession(('127.0.0.1', 1), 'no-private-properties')
        with self.assertRaisesRegex(ValueError, 'public disconnect'):
            await session.disconnect(0)
        await session.disconnect()

    async def test_wrong_session_present_is_not_a_success(self):
        async def script(reader, writer, _connection):
            await accept(reader, writer, present=True)
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            with self.assertRaisesRegex(AssertionError, 'session_present_mismatch'):
                await exchange('127.0.0.1', broker.port, 'wrong-present', expected_session_present=False)


class SessionEvidenceTests(unittest.TestCase):
    def test_driver_rejects_non_wire_expiry_and_non_boolean_clean_start(self):
        for value in (-1, 1 << 32, True, '0'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                options(False, value)
        with self.assertRaises(ValueError):
            options(1, 0)

    def test_terminal_rows_are_detected_even_if_active_capacity_is_returned(self):
        state = {'terminal_sessions': 1, 'active_sessions': 0, 'counts': {'sessions': 1}}
        with self.assertRaises(ReceiptFailure) as raised:
            assert_drained(state)
        self.assertEqual(raised.exception.code, 'terminal_session_rows_retained')

    def test_absent_cardinality_cannot_be_presented_as_drained(self):
        with self.assertRaises(ReceiptFailure) as raised:
            cardinality_drained({'details': {}})
        self.assertEqual(raised.exception.code, 'session_cardinality_fields_missing')

    def test_owner_and_runtime_must_both_drain_with_bounded_closed_memory(self):
        row = {name: 0 for name in CARDINALITY_ZERO}
        row['closed_generations'] = 8
        self.assertTrue(cardinality_drained({'details': row}))
        for name in CARDINALITY_ZERO:
            with self.subTest(name=name):
                self.assertFalse(cardinality_drained({'details': dict(row, **{name: 1})}))
        with self.assertRaises(ReceiptFailure) as raised:
            cardinality_drained({'details': dict(row, closed_generations=9)})
        self.assertEqual(raised.exception.code, 'session_closed_generation_memory_grew')
