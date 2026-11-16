"""Lifetime matrix evidence must survive bounded durable receipt retirement."""
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch
import unittest

import qos0_reception as reception
from pubsub_probe import Publication
from tcp_probe import ReceiptFailure, operation_hex, require


def admission(sequence, *, committed_now=True, kind='Admission'):
    return {'kind': 'store_result', 'command_kind': kind,
            'operation': {'incarnation': '1', 'epoch': '1', 'owner': '0', 'sequence': str(sequence)},
            'details': {'outcome': 'Committed', 'committed_now': committed_now, 'wrote': committed_now}}


class MatrixClient:
    def __init__(self, *args, filters, delivered, **kwargs):
        self.topic, self.qos = filters[0], delivered

    def wait_event(self, kind):
        return None

    def collect(self):
        return {'result': {'messages': [{'qos': self.qos, 'topic': self.topic,
                'payload_hex': self.topic.rsplit('/', 1)[1].encode().hex(),
                'packet_id': None if self.qos == 0 else 1}]}}

    def cleanup(self):
        pass


class MatrixWire:
    def __init__(self, *args):
        self.topic, self.granted = '', 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def connect(self, *args, **kwargs):
        pass

    def subscribe(self, identifier, filters, **kwargs):
        self.topic, self.granted = filters[0]

    def next_publication(self):
        tag = self.topic.rsplit('/', 1)[1]
        qos = min(int(tag[1]), self.granted)
        return Publication(None if qos == 0 else 1, self.topic, tag.encode(), qos, False, False)

    def acknowledge(self, message):
        pass


class MatrixReceiptRetirementTests(unittest.TestCase):
    def matrix(self, events):
        retained = [dict(id=operation_hex(admission(n)['operation'])) for n in (6, 7, 8)]
        state = {'admissions': retained, 'tables': {'receipt_origins': [{'consumed_through': '5'}]}}
        broker = MagicMock()
        broker.events.return_value = events

        def resources_drained(*args, admissions=None):
            if admissions is not None:
                require(len(retained) == admissions, 'unexpected_admission_count')
            return state

        with ExitStack() as stack:
            for name, replacement in [('Broker', lambda *args: broker), ('Client', MatrixClient),
                                      ('PubsubWire', MatrixWire), ('publish', lambda *args: {}),
                                      ('drained', resources_drained), ('writer_available', lambda *args: None)]:
                stack.enter_context(patch.object(reception, name, replacement))
            return reception.matrix(Path('unused-binary'), Path('unused-case'))

    def test_matrix_counts_all_known_admissions_after_receipt_retirement(self):
        result = self.matrix([admission(n) for n in range(1, 9)])
        self.assertEqual(result['durable_admissions'], 8)
        self.assertEqual(result['retained_admission_receipts'], 3)

    def test_replayed_result_and_lifecycle_commit_are_not_new_admissions(self):
        events = [admission(n) for n in range(1, 9)]
        events += [admission(1, committed_now=False), admission(1), admission(9, kind='SessionOpen')]
        self.assertEqual(self.matrix(events)['durable_admissions'], 8)

    def test_extra_distinct_admission_is_still_a_failure(self):
        with self.assertRaises(ReceiptFailure) as caught:
            self.matrix([admission(n) for n in range(1, 10)])
        self.assertEqual(caught.exception.code, 'unexpected_admission_count')

    def test_missing_known_admission_is_not_proved_by_retained_count(self):
        with self.assertRaises(ReceiptFailure) as caught:
            self.matrix([admission(n) for n in range(2, 9)])
        self.assertEqual(caught.exception.code, 'unexpected_admission_count')


if __name__ == '__main__':
    unittest.main()
