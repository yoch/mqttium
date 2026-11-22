"""Independent public-property encoding and temporal evidence discriminants."""
import argparse
import asyncio
import unittest

from external_sessions import INFINITE, exchange, expiry_argument
from expiry_reception import check_policy, detachment, has_client
from test_external_clients import ScriptedBroker, read_packet
from test_session_reception import accept
from tcp_probe import ReceiptFailure


class FiniteClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_mqttium_public_connect_carries_finite_extremes(self):
        for interval in (1, 23, INFINITE - 1):
            with self.subTest(interval=interval):
                async def script(reader, writer, _connection):
                    await accept(reader, writer, expiry=interval)
                    self.assertEqual(await read_packet(reader), (0xE0, b''))
                async with ScriptedBroker(script) as broker:
                    result = await exchange('127.0.0.1', broker.port, 'finite-mqttium',
                                            expiry=interval, expected_session_present=False)
                self.assertEqual(result['status'], 'success')
                self.assertEqual(result['connections'], 1)
                self.assertEqual(result['parameters']['session_expiry_interval'], interval)

    async def test_paho_public_disconnect_changes_finite_policy_on_wire(self):
        for connected, requested in ((INFINITE, 2), (2, 1), (2, INFINITE), (2, 0)):
            with self.subTest(connected=connected, requested=requested):
                async def script(reader, writer, _connection):
                    await accept(reader, writer, expiry=connected)
                    self.assertEqual(await read_packet(reader),
                                     (0xE0, b'\x00\x05\x11' + requested.to_bytes(4, 'big')))
                async with ScriptedBroker(script) as broker:
                    result = await exchange('127.0.0.1', broker.port, 'finite-paho',
                                            implementation='paho', expiry=connected,
                                            disconnect_expiry=requested)
                self.assertEqual(result['status'], 'success')
                self.assertEqual(result['connections'], 1)


class TemporalEvidenceTests(unittest.TestCase):
    def test_cli_accepts_the_wire_domain_without_float_or_unbounded_values(self):
        for raw, value in (('absent', None), ('infinite', INFINITE), ('0', 0),
                           ('1', 1), ('4294967294', INFINITE - 1), ('4294967295', INFINITE)):
            self.assertEqual(expiry_argument(raw), value)
        for raw in ('-1', '4294967296', '1.5', 'nan'):
            with self.subTest(raw=raw), self.assertRaises(argparse.ArgumentTypeError):
                expiry_argument(raw)

    def test_a_fixed_detachment_requires_its_occurrence_and_deadline(self):
        row = {'detached_by': '01' * 32, 'deadline': '0000000000000007'}
        self.assertEqual(detachment(row), {'operation': '01' * 32, 'deadline': '7'})
        for missing, reason in (({'detached_by': None, 'deadline': row['deadline']},
                                  'expiry_detachment_not_durable'),
                                 ({'detached_by': row['detached_by'], 'deadline': None},
                                  'expiry_deadline_not_durable')):
            with self.subTest(missing=missing), self.assertRaises(ReceiptFailure) as raised:
                detachment(missing)
            self.assertEqual(raised.exception.code, reason)
        self.assertEqual(detachment({'detached_by': row['detached_by'], 'deadline': None},
                                    finite=False)['deadline'], None)
        with self.assertRaises(ReceiptFailure) as raised:
            detachment(row, finite=False)
        self.assertEqual(raised.exception.code, 'infinite_detachment_has_deadline')

    def test_finite_policy_does_not_alias_zero_or_infinite(self):
        for requested, code in ((0, 0), (INFINITE, 1), (1, 3), (INFINITE - 1, INFINITE + 1)):
            row = {'expiry': code, 'opened_by': '01' * 32}
            self.assertIs(check_policy(row, requested), row)
            with self.assertRaises(ReceiptFailure) as raised:
                check_policy(dict(row, expiry=code + 1), requested)
            self.assertEqual(raised.exception.code, 'expiry_policy_not_durable')

    def test_absence_is_checked_by_client_name_in_a_bounded_inventory(self):
        self.assertTrue(has_client({'sessions': [{'client_id': b'a'.hex()}]}, 'a'))
        self.assertFalse(has_client({'sessions': [{'client_id': b'b'.hex()}]}, 'a'))


if __name__ == '__main__':
    unittest.main()
