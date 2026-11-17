"""Independent 0x02 wire discriminants and public API property checks."""
import copy
import unittest

from external_message_expiry import MqttiumMessageExpiry, PahoMessageExpiry
from external_wildcard import Subscription
from message_expiry_probe import decode_publish, publish_packet
from message_expiry_reception import (BUSINESS, physically_drained, one_delivery, closed_connection,
                                       original_copies, negative_puback)
from message_expiry_boundaries import (canonical_timed, canonical_expire, continuous, _positive,
                                       recipe_quarantine_inventory)
from tcp_probe import Frame, FrameDecoder, ReceiptFailure, operation_hex
from test_external_clients import ScriptedBroker, read_packet
from test_session_reception import accept


def frozen_quarantine_fixture():
    original_id, replay_id, payload_id = '22' * 32, '44' * 32, '11' * 32
    original = {'id': original_id, 'operation': '33' * 32, 'payload': payload_id,
                'source': '0000000000000001', 'qos': 1, 'retain': 1, 'topic': b'q'.hex(),
                'retained_version': None, 'expiry_domain': '61' * 32,
                'expiry_tick': '000000000000007b', 'expiry_interval': 2}
    replay = dict(original, id=replay_id, operation='55' * 32, retained_version=original_id)
    tables = {'publications': [original, replay],
              'payloads': [{'id': payload_id, 'bytes': b'expired'.hex(),
                            'length': '0000000000000007', 'refs': '0000000000000002'}],
              'retained': [{'topic': b'q'.hex(), 'publication': original_id,
                            'producer_client_id': b'source'.hex()}],
              'obligations': [{'publication': replay_id, 'idx': 0, 'target': '0000000000000002',
                               'target_revision': '0000000000000001', 'qos': 1,
                               'cause': 2, 'retain': 1, 'phase': 1}], 'inbound': [], 'outbound': [],
              'meta': [{'used_payload_bytes': '0000000000000007', 'used_references': '0000000000000002',
                        'used_exchange_slots': '0000000000000000'}]}
    before = {'tables': tables, 'counts': {name: len(rows) for name, rows in tables.items()}}
    return before, copy.deepcopy(before)


class IndependentMessageExpiryFrames(unittest.TestCase):
    def test_unknown_domain_keeps_exact_original_and_frozen_replay_inventory(self):
        before, after = frozen_quarantine_fixture()
        value = recipe_quarantine_inventory(before, after, 'replay', True, '55' * 32,
            expected_payload=b'expired', expected_stamp={'domain': '61' * 32, 'tick': '123', 'interval': 2})
        self.assertEqual(value['original_publication'], '22' * 32)
        self.assertEqual(value['publication_ids'], ['22' * 32, '44' * 32])
        self.assertEqual(value['payload'], '11' * 32)
        self.assertEqual(value['references'], 2)

    def test_single_publication_quarantine_stays_exact_for_other_recipe_cuts(self):
        for kind, committed in (('qos0', True), ('qos1', True), ('replay', False)):
            before, _ = frozen_quarantine_fixture()
            before['tables']['publications'].pop()
            before['counts']['publications'] = 1
            original = before['tables']['publications'][0]
            if kind == 'qos0':
                original['qos'] = 0
            if committed:
                owed = before['tables']['obligations'][0]
                owed.update(publication=original['id'], qos=original['qos'], cause=1, retain=0)
                operation = original['operation']
            else:
                before['tables']['obligations'] = []
                before['counts']['obligations'] = 0
                before['tables']['payloads'][0]['refs'] = '0000000000000001'
                before['tables']['meta'][0]['used_references'] = '0000000000000001'
                operation = '55' * 32
            with self.subTest(kind=kind, committed=committed):
                value = recipe_quarantine_inventory(before, copy.deepcopy(before), kind, committed, operation,
                    expected_payload=b'expired', expected_stamp={'domain': '61' * 32, 'tick': '123', 'interval': 2})
                self.assertEqual(value['publication_ids'], ['22' * 32])
                self.assertEqual(value['references'], 1 + int(committed))

    def test_quarantine_rejects_mutated_identity_clock_payload_or_reference(self):
        mutations = (
            ('clock', lambda tables: tables['publications'][1].update(expiry_tick='000000000000007c')),
            ('version', lambda tables: tables['publications'][1].update(retained_version='99' * 32)),
            ('owed', lambda tables: tables['obligations'][0].update(publication='22' * 32)),
            ('payload', lambda tables: tables['payloads'][0].update(bytes=b'changed'.hex())),
            ('reference', lambda tables: tables['payloads'][0].update(refs='0000000000000001')),
            ('account', lambda tables: tables['meta'][0].update(used_references='0000000000000001')),
        )
        for label, mutate in mutations:
            for both in (False, True):
                before, after = frozen_quarantine_fixture()
                mutate(after['tables'])
                if both:
                    mutate(before['tables'])
                with self.subTest(field=label, changed_before_and_after=both), self.assertRaises(ReceiptFailure):
                    recipe_quarantine_inventory(before, after, 'replay', True, '55' * 32,
                        expected_payload=b'expired', expected_stamp={'domain': '61' * 32, 'tick': '123', 'interval': 2})

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
        accounts = dict.fromkeys(('used_payload_bytes', 'used_references', 'used_exchange_slots'),
                                 '0000000000000000')
        state = {'counts': dict.fromkeys(BUSINESS, 0), 'tables': {'meta': [accounts]}}
        self.assertTrue(physically_drained(state))
        for name in BUSINESS:
            with self.subTest(name=name):
                state['counts'][name] = 1
                self.assertFalse(physically_drained(state))
                state['counts'][name] = 0
        for name in accounts:
            with self.subTest(account=name):
                accounts[name] = '0000000000000001'
                self.assertFalse(physically_drained(state))
                accounts[name] = '0000000000000000'
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

    def test_negative_ack_is_scoped_to_its_publication_when_identifier_one_is_reused(self):
        class Tap:
            def server_frames(self):
                return self.frames
        tap = Tap()
        tap.frames = (Frame(0x20, b'\x00\x00\x00'),) + (Frame(0x40, b'\x00\x01'),) * 32
        cursor = len(tap.frames)
        expected = Frame(0x40, b'\x00\x01\x97')
        tap.frames += (expected,)
        self.assertEqual(negative_puback(tap, 1, after=cursor), expected)
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_capacity_negative_puback_not_observed'):
            negative_puback(tap, 1, after=0)
        cursor = len(tap.frames)
        tap.frames += (Frame(0x40, b'\x00\x01'),)
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_capacity_negative_puback_not_observed'):
            negative_puback(tap, 1, after=cursor)

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

    def test_tag16_cancellation_correlates_the_exact_claim_and_original_deadline(self):
        event = {'operation': {'incarnation': '1', 'epoch': '2', 'owner': '3', 'sequence': '4'}}
        identity = operation_hex(event['operation'])
        claim = {'publication': '55' * 32, 'idx': 3, 'target': '0000000000000007',
                 'target_revision': '0000000000000008', 'packet_id': 31, 'claim': '66' * 32}
        stamp = {'domain': '61' * 32, 'tick': '123', 'interval': 2}
        raw = (b'MQDR\x00\x01\x10' + bytes.fromhex(identity) + bytes.fromhex(claim['publication']) +
               claim['idx'].to_bytes(4, 'big') + bytes.fromhex(claim['target']) +
               bytes.fromhex(claim['target_revision']) + claim['packet_id'].to_bytes(2, 'big') +
               bytes.fromhex(claim['claim']) + b'a' * 32 + (123).to_bytes(8, 'big') +
               (2).to_bytes(4, 'big') + b'a' * 32 + (2_000_000_123).to_bytes(8, 'big'))
        state = {'tables': {'operations': [{'id': identity, 'recipe': raw.hex()}]}}
        self.assertEqual(canonical_expire(state, event, claim, stamp)['tag'], 16)
        altered = raw[:93] + b'\xff' + raw[94:]
        state['tables']['operations'][0]['recipe'] = altered.hex()
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_expire_canonical_claim_changed'):
            canonical_expire(state, event, claim, stamp)
        state['tables']['operations'][0]['recipe'] = (raw[:-8] + (122).to_bytes(8, 'big')).hex()
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_expire_canonical_clock_changed'):
            canonical_expire(state, event, claim, stamp)

    def test_infinite_offline_session_uses_its_exact_observed_connection_close(self):
        state = {'tables': {'sessions': [{'id': '0000000000000001', 'client_id': b'forever'.hex(),
                                         'expiry': 1, 'detached_by': None}]}}
        events = [{'kind': 'session_established', 'seq': '1', 'connection': '7',
                   'details': {'session': '1'}},
                  {'kind': 'connection_closed', 'seq': '2', 'connection': '9', 'details': {}}]
        self.assertIsNone(closed_connection(state, events, 'forever'))
        expected = {'kind': 'connection_closed', 'seq': '3', 'connection': '7', 'details': {}}
        events.append(expected)
        self.assertEqual(closed_connection(state, events, 'forever'), expected)
        events.append({'kind': 'session_established', 'seq': '4', 'connection': '10',
                       'details': {'session': '1'}})
        self.assertIsNone(closed_connection(state, events, 'forever'))

    def test_frozen_replay_copy_preserves_the_original_durable_stamp(self):
        stamp = {'domain': '61' * 32, 'tick': '123', 'interval': 2}
        original = {'id': 'origin', 'retained_version': None, 'expiry_domain': stamp['domain'],
                    'expiry_tick': '000000000000007b', 'expiry_interval': 2}
        replay = dict(original, id='replay', retained_version='origin')
        state = {'tables': {'publications': [original, replay]}}
        self.assertEqual(original_copies(state, 'origin', stamp), ['origin', 'replay'])
        replay['expiry_tick'] = '000000000000007c'
        with self.assertRaisesRegex(ReceiptFailure, 'message_expiry_replay_changed_original_stamp'):
            original_copies(state, 'origin', stamp)


class PublicMessageExpiryAPIs(unittest.IsolatedAsyncioTestCase):
    async def test_capacity_refusal_preserves_the_exact_public_publish_rejected_error(self):
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            header, body = await read_packet(reader)
            row = decode_publish(Frame(header, body), incoming=True)
            self.assertEqual((row.qos, row.retain, row.message_expiry_interval), (1, True, 2))
            writer.write(b'\x40\x04' + row.packet_id.to_bytes(2, 'big') + b'\x97\x00')
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            source = MqttiumMessageExpiry(('127.0.0.1', broker.port), 'public-capacity-refusal')
            try:
                await source.open(False)
                result = await source.publish_rejected('t', b'v', reason=0x97, message_expiry=2)
                self.assertEqual(result['exception'], 'PublishRejectedError')
                self.assertEqual(result['reason_code'], 0x97)
                self.assertTrue(result['receipt_done'])
            finally:
                await source.disconnect()

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
