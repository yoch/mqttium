"""Public API and independent frame discriminants for retained evidence."""
import unittest

from external_retained import MqttiumRetained, PahoRetained
from external_wildcard import Subscription
from retained_probe import decode_publish, retained_publish_packet
from retained_boundaries import canonical_receipt
from tcp_probe import Frame, ReceiptFailure, operation_hex
from test_external_clients import ScriptedBroker, read_packet
from test_session_reception import accept


class IndependentRetainedFrames(unittest.TestCase):
    def test_qos0_has_retain_without_an_invented_packet_identifier(self):
        wire = retained_publish_packet('a/b', b'value', qos=0)
        self.assertEqual(wire, b'\x31\x0b\x00\x03a/b\x00value')
        decoded = decode_publish(Frame(wire[0], wire[2:]))
        self.assertEqual((decoded.qos, decoded.packet_id, decoded.retain), (0, None, True))

    def test_qos1_retained_and_live_flags_are_independently_observed(self):
        retained = decode_publish(Frame(0x33, b'\x00\x01t\x00\x08\x00v'))
        live = decode_publish(Frame(0x32, b'\x00\x01t\x00\x08\x00v'))
        self.assertTrue(retained.retain)
        self.assertFalse(live.retain)
        self.assertEqual(retained.observation()['packet_id'], 8)

    def test_invalid_qos0_dup_and_zero_qos1_identifier_are_rejected(self):
        for frame in (Frame(0x39, b'\x00\x01t\x00v'), Frame(0x33, b'\x00\x01t\x00\x00\x00v')):
            with self.subTest(frame=frame), self.assertRaises(ReceiptFailure):
                decode_publish(frame)
        with self.assertRaises(ValueError):
            retained_publish_packet('a', b'b', 1, qos=0)

    def test_actual_wrapped_recipe_layout_correlates_inner_operation(self):
        event = {'operation': {'incarnation': '1', 'epoch': '2', 'owner': '3', 'sequence': '4'}}
        identity = operation_hex(event['operation'])
        number = lambda value, width=8: value.to_bytes(width, 'big')
        scoped = lambda sequence: bytes.fromhex(identity)[:24] + number(sequence)
        for tag, inner in ((10, 1), (12, 7), (12, 9)):
            old = b'MQDR\x00\x01' + bytes([inner]) + bytes.fromhex(identity)
            if tag == 10:
                # Complete kind1: request/publication, source exchange, route,
                # QoS2+RETAIN, topic, payload, costs and zero live targets.
                old += (scoped(5) + scoped(6) + number(1) * 2 + number(7, 2)
                        + number(1) * 2 + b'\x02\x01\x00\x01t'
                        + scoped(7) + number(1) * 5 + number(0, 2))
                suffix = number(0, 2)  # Empty live-target RETAIN bitmap.
            else:
                # Complete kind7/9 route update; kind9 has nondefault options.
                old += (number(1) * 3 + b'\x01\x02'
                        + (b'\x1c' if inner == 9 else b'') + b'\x00\x01t')
                suffix = scoped(8)  # Reserved replay range, in the same scope.
            raw = b'MQDR\x00\x01' + bytes([tag]) + number(len(old), 4) + old + suffix
            state = {'tables': {'operations': [{'id': identity, 'recipe': raw.hex()}]}}
            with self.subTest(tag=tag, inner=inner):
                self.assertEqual(canonical_receipt(state, event, tag)['tag'], tag)
                altered = raw[:18] + b'\xff' * 32 + raw[50:]
                state['tables']['operations'][0]['recipe'] = altered.hex()
                code = ('retained_result_canonical_identity_changed' if tag == 10
                        else 'subscription_options_missing_from_canonical_recipe')
                with self.assertRaisesRegex(ReceiptFailure, code):
                    canonical_receipt(state, event, tag)


class PublicRetainedAPIs(unittest.IsolatedAsyncioTestCase):
    async def public_publish(self, constructor):
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            for qos in (0, 1, 2):
                header, body = await read_packet(reader)
                self.assertEqual(header, 0x31 | (qos << 1))
                self.assertEqual(body[:3], b'\x00\x01t')
                if qos:
                    identifier = body[3:5]
                    writer.write(bytes([0x40 if qos == 1 else 0x50, 2]) + identifier)
                    await writer.drain()
                    if qos == 2:
                        rel_header, rel_body = await read_packet(reader)
                        self.assertEqual((rel_header, rel_body[:2]), (0x62, identifier))
                        writer.write(b'\x70\x02' + identifier)
                        await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            client = constructor(('127.0.0.1', broker.port), 'retained-public-publisher')
            try:
                await client.open(False)
                for qos in (0, 1, 2):
                    await client.publish('t', b'value', qos=qos)
            finally:
                await client.disconnect()

    async def public_receive(self, constructor):
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            header, body = await read_packet(reader)
            self.assertEqual(header, 0x82)
            writer.write(b'\x90\x04' + body[:2] + b'\x00\x01')
            writer.write(retained_publish_packet('t', b'value', 9, qos=1))
            await writer.drain()
            header, body = await read_packet(reader)
            self.assertEqual((header, body[:2]), (0x40, b'\x00\x09'))
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            client = constructor(('127.0.0.1', broker.port), 'retained-public-subscriber')
            try:
                await client.open(False)
                await client.subscribe((Subscription('t', 1),))
                row = await client.receive(b'value', 1, True)
                self.assertTrue(row['retain'])
            finally:
                await client.disconnect()

    async def test_mqttium_public_retained_publishes_all_three_qos(self):
        await self.public_publish(MqttiumRetained)

    async def test_paho_public_retained_publishes_all_three_qos(self):
        await self.public_publish(PahoRetained)

    async def test_mqttium_reports_actual_retained_delivery(self):
        await self.public_receive(MqttiumRetained)

    async def test_paho_reports_actual_retained_delivery(self):
        await self.public_receive(PahoRetained)


if __name__ == '__main__':
    unittest.main()
