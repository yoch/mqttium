"""Independent 0x02 wire discriminants and public API property checks."""
import unittest

from external_message_expiry import MqttiumMessageExpiry, PahoMessageExpiry
from external_wildcard import Subscription
from message_expiry_probe import decode_publish, publish_packet
from message_expiry_reception import BUSINESS, physically_drained, one_delivery
from message_expiry_boundaries import canonical_timed, continuous, _positive
from tcp_probe import Frame, FrameDecoder, ReceiptFailure, operation_hex
from test_external_clients import ScriptedBroker, read_packet
from test_session_reception import accept


class IndependentMessageExpiryFrames(unittest.TestCase):
    def test_property_identifier_and_four_byte_width_have_literal_witnesses(self):
        self.assertEqual(publish_packet('t', b'v', qos=0, retain=True, expiry=2),
                         b'\x31\x0a\x00\x01t\x05\x02\x00\x00\x00\x02v')
        self.assertEqual(publish_packet('t', b'v', 7, qos=1, expiry=0xffffffff),
                         b'\x32\x0c\x00\x01t\x00\x07\x05\x02\xff\xff\xff\xffv')

    def test_absent_zero_short_and_u32max_round_trip_at_each_qos(self):
        for qos in (0, 1, 2):
            for expiry in (None, 0, 1, 2, 0xffffffff):
                with self.subTest(qos=qos, expiry=expiry):
                    raw = publish_packet('a/b', b'value', 23 if qos else None,
                                         qos=qos, retain=True, expiry=expiry)
                    decoder = FrameDecoder()
                    frames = []
                    for byte in raw:
                        frames.extend(decoder.feed(bytes([byte])))
                    decoder.finish()
                    self.assertEqual(len(frames), 1)
                    row = decode_publish(frames[0], incoming=True)
                    self.assertEqual((row.topic, row.payload, row.qos, row.packet_id, row.retain),
                                     ('a/b', b'value', qos, 23 if qos else None, True))
                    self.assertEqual(row.message_expiry_interval, expiry)
                    self.assertEqual(row.observation()['message_expiry_present'], expiry is not None)

    def test_duplicate_zero_does_not_look_absent(self):
        raw = publish_packet('t', b'v', 1, property_bytes=b'\x02\x00\x00\x00\x00' * 2)
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_duplicate_property'):
            decode_publish(FrameDecoder().feed(raw)[0])

    def test_truncated_oversized_unknown_and_noncanonical_properties_are_rejected(self):
        bodies = (
            b'\x00\x01t\x00\x01\x05\x02\x00\x00\x00',
            b'\x00\x01t\x00\x01\x04\x02\x00\x00\x00v',
            b'\x00\x01t\x00\x01\x05\x11\x00\x00\x00\x02v',
            b'\x00\x01t\x00\x01\x80\x00v',
            b'\x00\x01t\x00\x01\x80\x80\x80\x80v',
        )
        for body in bodies:
            with self.subTest(body=body), self.assertRaises(ReceiptFailure):
                decode_publish(Frame(0x32, body))

    def test_encoder_rejects_invalid_integer_identifier_and_flags(self):
        for value in (-1, 2 ** 32, True, 1.5, '2'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                publish_packet('t', b'v', 1, expiry=value)
        for options in ({'qos': 0, 'identifier': 1}, {'qos': 0, 'dup': True},
                        {'qos': 1}, {'qos': True}, {'qos': 1, 'identifier': 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                publish_packet('t', b'v', **options)

    def test_absence_requires_every_physical_business_reference_to_be_reclaimed(self):
        state = {'counts': dict.fromkeys(BUSINESS, 0)}
        self.assertTrue(physically_drained(state))
        for name in BUSINESS:
            with self.subTest(name=name):
                state['counts'][name] = 1
                self.assertFalse(physically_drained(state))
                state['counts'][name] = 0
        del state['counts']['retained']
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_business_table_missing'):
            physically_drained(state)

    def test_wire_witness_does_not_invent_or_drop_expiry(self):
        class Tap:
            def __init__(self, value):
                raw = publish_packet('t', b'v', 1, expiry=value)
                self.value = decode_publish(FrameDecoder().feed(raw)[0])

            def publications(self, _payload):
                return [self.value]

        self.assertIsNone(one_delivery(Tap(None), b'v', 1, False, None)['message_expiry_interval'])
        self.assertEqual(one_delivery(Tap(1), b'v', 1, False, 2)['message_expiry_interval'], 1)
        for observed, requested in ((0, 2), (None, 2), (3, 2), (1, None)):
            with self.subTest(observed=observed, requested=requested), self.assertRaises(ReceiptFailure):
                one_delivery(Tap(observed), b'v', 1, False, requested)

    def test_timed_wrapped_recipe_correlates_original_operation_and_frozen_clock(self):
        event = {'operation': {'incarnation': '1', 'epoch': '2', 'owner': '3', 'sequence': '4'}}
        identity = operation_hex(event['operation'])
        clock = b'd' * 32 + (123456789).to_bytes(8, 'big')
        for tag, inner_tags in ((13, (1,)), (13, (10, 1)), (13, (11,)), (14, (12, 9))):
            raw = b'MQDR\x00\x01' + bytes([inner_tags[-1]]) + bytes.fromhex(identity)
            for inner in reversed(inner_tags[:-1]):
                raw = b'MQDR\x00\x01' + bytes([inner]) + len(raw).to_bytes(4, 'big') + raw
            raw = b'MQDR\x00\x01' + bytes([tag]) + len(raw).to_bytes(4, 'big') + raw + clock
            if tag == 13:
                raw += (2).to_bytes(4, 'big')
            state = {'tables': {'operations': [{'id': identity, 'recipe': raw.hex()}]}}
            with self.subTest(tag=tag, inner=inner_tags):
                result = canonical_timed(state, event, tag)
                self.assertEqual(result['tick'], '123456789')
                self.assertEqual(result['domain'], (b'd' * 32).hex())
                self.assertEqual(result['nested_tags'], [tag, *inner_tags])
                altered = raw.replace(bytes.fromhex(identity), b'\xff' * 32, 1)
                state['tables']['operations'][0]['recipe'] = altered.hex()
                with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_timed_canonical_identity_changed'):
                    canonical_timed(state, event, tag)

    def test_clock_continuity_requires_recognized_equal_domains_and_positive_quarantine(self):
        known = {'domain': 'a' * 64, 'origin_status': 'boottime_origin_recognized'}
        self.assertTrue(continuous(known, dict(known)))
        self.assertFalse(continuous(known, dict(known, domain='b' * 64)))
        unknown = dict(known, origin_status='boottime_namespace_unavailable')
        self.assertFalse(continuous(unknown, dict(unknown)))
        for value in (0, '0', [], {}, None, False):
            self.assertFalse(_positive(value))
        for value in (1, '1', ['claim'], {'claim': '1'}):
            self.assertTrue(_positive(value))


class PublicMessageExpiryAPIs(unittest.IsolatedAsyncioTestCase):
    async def public_publish(self, constructor):
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            for qos in (0, 1, 2):
                for expiry in (None, 0, 1, 2, 0xffffffff):
                    header, body = await read_packet(reader)
                    row = decode_publish(Frame(header, body), incoming=True)
                    self.assertEqual((row.qos, row.retain, row.message_expiry_interval), (qos, True, expiry))
                    if qos:
                        identifier = row.packet_id.to_bytes(2, 'big')
                        writer.write(bytes([0x40 if qos == 1 else 0x50, 2]) + identifier)
                        await writer.drain()
                        if qos == 2:
                            rel_header, rel_body = await read_packet(reader)
                            self.assertEqual((rel_header, rel_body[:2]), (0x62, identifier))
                            writer.write(b'\x70\x02' + identifier)
                            await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            client = constructor(('127.0.0.1', broker.port), 'message-expiry-public-publisher')
            try:
                await client.open(False)
                for qos in (0, 1, 2):
                    for expiry in (None, 0, 1, 2, 0xffffffff):
                        await client.publish('t', b'value', qos, retain=True, message_expiry=expiry)
            finally:
                await client.disconnect()

    async def public_receive(self, constructor):
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            header, body = await read_packet(reader)
            self.assertEqual(header, 0x82)
            writer.write(b'\x90\x04' + body[:2] + b'\x00\x01')
            writer.write(publish_packet('t', b'value', 9, expiry=1, retain=True))
            await writer.drain()
            self.assertEqual((await read_packet(reader))[0], 0x40)
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            client = constructor(('127.0.0.1', broker.port), 'message-expiry-public-subscriber')
            try:
                await client.open(False)
                await client.subscribe((Subscription('t', 1),))
                row = await client.receive(b'value', 1, True, acknowledge=False)
                self.assertEqual(row['message_expiry_interval'], 1)
                self.assertTrue(row['message_expiry_present'])
                await client.acknowledge(row)
            finally:
                await client.disconnect()

    async def test_mqttium_public_property_at_all_qos_and_boundaries(self):
        await self.public_publish(MqttiumMessageExpiry)

    async def test_paho_public_property_at_all_qos_and_boundaries(self):
        await self.public_publish(PahoMessageExpiry)

    async def test_mqttium_public_message_property_and_manual_puback(self):
        await self.public_receive(MqttiumMessageExpiry)

    async def test_paho_public_message_property_and_manual_puback(self):
        await self.public_receive(PahoMessageExpiry)


if __name__ == '__main__':
    unittest.main()
