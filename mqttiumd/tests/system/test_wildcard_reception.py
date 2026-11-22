"""Independent public subscription-option encoders and wildcard evidence checks."""
import asyncio
import unittest

from external_wildcard import MqttiumPeer, PahoPeer, Subscription, subscribe_packet
from test_external_clients import ScriptedBroker, read_packet
from test_session_reception import accept


def options_from_body(body):
    if body[2] != 0:
        raise AssertionError('expected no SUBSCRIBE properties')
    offset, rows = 3, []
    while offset < len(body):
        size = int.from_bytes(body[offset:offset + 2], 'big')
        end = offset + 2 + size
        rows.append((body[offset + 2:end].decode(), body[end]))
        offset = end + 1
    return rows


class PublicWildcardOptions(unittest.IsolatedAsyncioTestCase):
    async def _public_options(self, constructor):
        values = (Subscription('atelier/#', 1, True),
                  Subscription('atelier/+/temperature', 0, False, True, 2),
                  Subscription('$SYS/#', 2, False, False, 1))
        async def script(reader, writer, _connection):
            await accept(reader, writer)
            header, body = await read_packet(reader)
            self.assertEqual(header, 0x82)
            self.assertEqual(options_from_body(body),
                             [('atelier/#', 5), ('atelier/+/temperature', 40), ('$SYS/#', 18)])
            writer.write(b'\x90\x06' + body[:2] + b'\x00\x01\x00\x02')
            await writer.drain()
            header, body = await read_packet(reader)
            self.assertEqual(header, 0xA2)
            self.assertEqual(body[2:], b'\x00\x00\x09atelier/#')
            writer.write(b'\xb0\x04' + body[:2] + b'\x00\x00')
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b''))
        async with ScriptedBroker(script) as broker:
            client = constructor(('127.0.0.1', broker.port), 'public-wildcard-unit')
            try:
                await client.open(False)
                await client.subscribe(values)
                await client.unsubscribe(('atelier/#',))
            finally:
                await client.disconnect()
        self.assertEqual(client.result['connections'], 1)

    async def test_mqttium_public_api_encodes_each_filter_option(self):
        await self._public_options(MqttiumPeer)

    async def test_paho_public_api_encodes_each_filter_option(self):
        await self._public_options(PahoPeer)


class IndependentWireOptions(unittest.TestCase):
    def test_raw_encoder_has_actual_no_local_and_retained_bits(self):
        wire = subscribe_packet(8, (Subscription('a/+/#', 2, True, True, 2),))
        self.assertEqual(wire, b'\x82\x0b\x00\x08\x00\x00\x05a/+/#\x2e')

    def test_raw_encoder_refuses_values_outside_its_explicit_budget(self):
        for values in ((), (Subscription('a'),) * 9, (Subscription('a', 3),),
                       (Subscription('a', 1, 1),), (Subscription('a', 1, False, False, 3),)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                subscribe_packet(1, values)


if __name__ == '__main__':
    unittest.main()
