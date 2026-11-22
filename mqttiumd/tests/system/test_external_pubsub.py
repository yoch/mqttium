"""The public client helpers observe SUBACK, delivery and real manual ACKs.

The peer is an independent small byte script. These checks receive the client
driver, not the broker implementation or the MQTT specification universally.
"""
import asyncio
import json
from pathlib import Path
import tempfile
import unittest

import external_pubsub as clients
from test_external_clients import ScriptedBroker, connack, read_packet


def packet(first, body):
    if len(body) >= 128:
        raise AssertionError('test packet must use a one-byte Remaining Length')
    return bytes((first, len(body))) + body


def server_publish(topic, payload, mid=7, dup=False):
    name = topic.encode()
    return packet(0x3A if dup else 0x32,
                  len(name).to_bytes(2, 'big') + name + mid.to_bytes(2, 'big') + b'\0' + payload)


def server_publish_qos0(topic, payload):
    name = topic.encode()
    return packet(0x30, len(name).to_bytes(2, 'big') + name + b'\0' + payload)


def filters(body, options):
    if body[2] != 0:
        raise AssertionError('test client sent unrequested subscription properties')
    at = 3
    result = []
    while at < len(body):
        length = int.from_bytes(body[at:at + 2], 'big')
        at += 2
        topic = body[at:at + length].decode()
        at += length
        qos = body[at] if options else None
        at += int(options)
        result.append((topic, qos))
    return result


async def call(kind, action, host, port, client_id, **options):
    helper = getattr(clients, f'{kind}_{action}')
    if kind == 'mqttium':
        return await helper(host, port, client_id, **options)
    return await asyncio.to_thread(helper, host, port, client_id, **options)


class ExternalPubsubTests(unittest.IsolatedAsyncioTestCase):
    def test_effective_qos_matrix_keeps_published_and_subscription_qos_distinct(self):
        self.assertEqual([[clients.effective_qos(published, granted) for granted in (0, 1, 2)]
                          for published in (0, 1, 2)], [[0, 0, 0], [0, 1, 1], [0, 1, 2]])
        for published, granted in ((3, 1), (1, 3)):
            with self.assertRaises(ValueError):
                clients.effective_qos(published, granted)

    async def test_qos0_publish_receipt_is_local_and_never_waits_for_a_broker_ack(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x30)
                    self.assertEqual(body, b'\x00\x0freception/exact\x00data')
                    # The independent peer sends no ACK. Client completion is
                    # deliberately weaker than broker admission or delivery.
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'publish', '127.0.0.1', broker.port, f'{kind}-qos0-publisher',
                                        topic='reception/exact', payload=b'data', qos=0)
                self.assertEqual(result['status'], 'success', result)
                row = result['publications'][0]
                self.assertEqual(row['completion_scope'], 'local-write-only')
                self.assertIsNone(row['packet_id'], 'an API receipt token is not a QoS 0 wire identifier')
                self.assertIn('client_receipt_id', row)

    async def test_qos0_receive_never_submits_manual_ack_and_checks_granted_qos(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    self.assertEqual(filters(body, True), [('reception/exact', 0)])
                    writer.write(packet(0x90, body[:2] + b'\0\0'))
                    writer.write(server_publish_qos0('reception/exact', b'volatile'))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0xE0, b''),
                                     'QoS 0 must never issue PUBACK even in manual-ACK mode')

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-qos0-receiver',
                                        filters=('reception/exact',), qos=0,
                                        expected_granted_qos=0, expected_delivery_qos=0,
                                        ack_policy='hold')
                self.assertEqual(result['status'], 'success', result)
                self.assertEqual(result['subscriptions'][0]['reason_codes'], [0])
                message = result['messages'][0]
                self.assertEqual(message['qos'], 0)
                self.assertIsNone(message['packet_id'])
                kinds = [event['kind'] for event in result['events']]
                self.assertNotIn('manual_ack_submitted', kinds)
                self.assertNotIn('ack_held', kinds)

    async def test_one_qos1_subscription_can_receive_qos0_and_qos1_without_ack_confusion(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    self.assertEqual(filters(body, True), [('reception/mixed', 1)])
                    writer.write(packet(0x90, body[:2] + b'\0\1'))
                    writer.write(server_publish_qos0('reception/mixed', b'zero'))
                    writer.write(server_publish('reception/mixed', b'one', mid=9))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0x40, b'\0\x09'))
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-mixed-receiver',
                                        filters=('reception/mixed',), qos=1, count=2,
                                        expected_delivery_qos=None)
                self.assertEqual(result['status'], 'success', result)
                self.assertEqual([message['qos'] for message in result['messages']], [0, 1])
                self.assertEqual([message['packet_id'] for message in result['messages']], [None, 9])
                kinds = [event['kind'] for event in result['events']]
                self.assertEqual(kinds.count('manual_ack_submitted'), 1)
                self.assertEqual(kinds.count('qos0_received_no_ack'), 1)

    async def test_both_public_clients_wait_for_suback_and_receive_qos1(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    self.assertEqual(filters(body, True), [('reception/exact', 2), ('reception/second', 2)])
                    writer.write(packet(0x90, body[:2] + b'\0\x01\x01'))
                    writer.write(server_publish('reception/exact', b'publication'))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0x40, b'\0\x07'))
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-receiver',
                                        filters=('reception/exact', 'reception/second'), qos=2,
                                        expected_granted_qos=1,
                                        expected_session_present=False)
                self.assertEqual(result['status'], 'success', result)
                self.assertEqual(result['subscriptions'][0]['reason_codes'], [1, 1])
                self.assertEqual(result['messages'][0]['payload_hex'], b'publication'.hex())
                self.assertEqual(result['messages'][0]['qos'], 1)
                kinds = [event['kind'] for event in result['events']]
                self.assertLess(kinds.index('subscription_ready'), kinds.index('message'))
                self.assertIn('manual_ack_submitted', kinds)
                self.assertEqual(result['identity']['version'], '1.3.0' if kind == 'mqttium' else '2.1.0')

    async def test_public_manual_ack_really_withholds_puback_until_release(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind), tempfile.TemporaryDirectory() as directory:
                events = Path(directory) / 'client.jsonl'
                release = Path(directory) / 'release'

                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    writer.write(packet(0x90, body[:2] + b'\0\x01'))
                    writer.write(server_publish('reception/held', b'held'))
                    await writer.drain()
                    async with asyncio.timeout(2):
                        while not events.exists() or not any(json.loads(line)['kind'] == 'ack_held'
                                                             for line in events.read_text().splitlines()):
                            await asyncio.sleep(.005)
                    reading = asyncio.create_task(read_packet(reader))
                    ready, _ = await asyncio.wait((reading,), timeout=.05)
                    self.assertFalse(ready, 'a sleep after auto-ACK would already expose PUBACK here')
                    release.touch()
                    self.assertEqual(await reading, (0x40, b'\0\x07'))
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-held',
                                        filters=('reception/held',), events_path=events,
                                        ack_policy='hold', ack_release=release)
                self.assertEqual(result['status'], 'success', result)
                kinds = [event['kind'] for event in result['events']]
                self.assertLess(kinds.index('ack_held'), kinds.index('ack_release_observed'))
                self.assertLess(kinds.index('ack_release_observed'), kinds.index('manual_ack_submitted'))

    async def test_resume_receives_without_sending_a_new_subscribe(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer, session_present=True)
                    writer.write(server_publish('reception/offline', b'durable', mid=8, dup=True))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0x40, b'\0\x08'),
                                     'the first client packet after CONNACK must be its PUBACK, not SUBSCRIBE')
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-resumed',
                                        subscribe=False, expected_session_present=True)
                self.assertEqual(result['status'], 'success', result)
                self.assertEqual(result['subscriptions'], [])
                self.assertTrue(result['messages'][0]['dup'])

    async def test_hold_without_release_disconnects_without_terminal_ack(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    writer.write(packet(0x90, body[:2] + b'\0\x01'))
                    writer.write(server_publish('reception/unacked', b'pending'))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0xE0, b''),
                                     'intentional disconnect must not implicitly acknowledge the message')

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'receive', '127.0.0.1', broker.port, f'{kind}-unacked',
                                        filters=('reception/unacked',), ack_policy='hold')
                self.assertEqual(result['status'], 'success', result)
                kinds = [event['kind'] for event in result['events']]
                self.assertIn('ack_held', kinds)
                self.assertNotIn('manual_ack_submitted', kinds)

    async def test_refusal_is_not_subscription_ready_and_retains_filter_order(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0x82)
                    self.assertEqual(filters(body, True), [('first', 1), ('second', 1)])
                    writer.write(packet(0x90, body[:2] + b'\0\x01\x80'))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'subscribe', '127.0.0.1', broker.port, f'{kind}-refused',
                                        filters=('first', 'second'))
                self.assertEqual(result['status'], 'rejected', result)
                self.assertEqual(result['subscriptions'][0]['reason_codes'], [1, 128])
                self.assertNotIn('subscription_ready', [event['kind'] for event in result['events']])

    async def test_unsubscribe_observes_ordered_terminal_reason_codes(self):
        for kind in ('mqttium', 'paho'):
            with self.subTest(client=kind):
                async def script(reader, writer, _connection):
                    await connack(reader, writer, session_present=True)
                    first, body = await read_packet(reader)
                    self.assertEqual(first, 0xA2)
                    self.assertEqual(filters(body, False), [('first', None), ('absent', None)])
                    writer.write(packet(0xB0, body[:2] + b'\0\0\x11'))
                    await writer.drain()
                    self.assertEqual(await read_packet(reader), (0xE0, b''))

                async with ScriptedBroker(script) as broker:
                    result = await call(kind, 'unsubscribe', '127.0.0.1', broker.port, f'{kind}-unsubscribe',
                                        filters=('first', 'absent'), expected_session_present=True)
                self.assertEqual(result['status'], 'success', result)
                self.assertEqual(result['unsubscriptions'][0]['reason_codes'], [0, 17])

    async def test_publish_helpers_wait_for_qos1_and_qos2_protocol_completion(self):
        for kind in ('mqttium', 'paho'):
            for qos in (1, 2):
                with self.subTest(client=kind, qos=qos):
                    async def script(reader, writer, _connection):
                        await connack(reader, writer)
                        first, body = await read_packet(reader)
                        self.assertEqual((first >> 1) & 3, qos)
                        size = int.from_bytes(body[:2], 'big')
                        mid = body[2 + size:4 + size]
                        writer.write(packet(0x40 if qos == 1 else 0x50, mid))
                        await writer.drain()
                        if qos == 2:
                            self.assertEqual(await read_packet(reader), (0x62, mid))
                            writer.write(packet(0x70, mid))
                            await writer.drain()
                        self.assertEqual(await read_packet(reader), (0xE0, b''))

                    async with ScriptedBroker(script) as broker:
                        result = await call(kind, 'publish', '127.0.0.1', broker.port, f'{kind}-publisher',
                                            topic='reception/exact', payload=b'data', qos=qos)
                    self.assertEqual(result['status'], 'success', result)
                    self.assertEqual(result['publications'][0]['result'], 'success')


if __name__ == '__main__':
    unittest.main()
