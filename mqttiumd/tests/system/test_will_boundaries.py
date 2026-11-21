"""The boundary pilot rejects specific violations before it judges a real run."""
from __future__ import annotations

import copy
import unittest

from tcp_probe import ReceiptFailure
from will_boundaries import assert_cut_inventory, will_commands


def inventory(*, wills=(), obligations=0, outbound=0, publications=0, payloads=0):
    return {'tables': {'wills': list(wills), 'operations': []},
            'counts': {'obligations': obligations, 'outbound': outbound,
                       'publications': publications, 'payloads': payloads}}


class WillBoundaryDriverTests(unittest.TestCase):
    def test_prepared_open_is_not_a_durable_installation(self):
        state = inventory()
        assert_cut_inventory('open-prepared', state)
        state['tables']['wills'] = [{'state': 1}]
        with self.assertRaisesRegex(ReceiptFailure, 'will_open_prepared_became_durable'):
            assert_cut_inventory('open-prepared', state)

    def test_admission_cut_distinguishes_pending_intent_from_consumed_responsibility(self):
        assert_cut_inventory('admission-prepared', inventory(wills=({'state': 2},)))
        with self.assertRaisesRegex(ReceiptFailure, 'will_admission_prepared_lost_or_consumed_intent'):
            assert_cut_inventory('admission-prepared', inventory())
        committed = inventory(obligations=1, publications=1, payloads=1)
        assert_cut_inventory('admission-committed', committed)
        for table in ('obligations', 'publications', 'payloads'):
            broken = copy.deepcopy(committed)
            broken['counts'][table] = 0
            with self.assertRaisesRegex(ReceiptFailure, 'will_admission_commit_not_atomic'):
                assert_cut_inventory('admission-committed', broken)

    def test_observed_and_finished_publication_have_distinct_reference_owners(self):
        assert_cut_inventory('publish-observed', inventory(obligations=1, outbound=1,
                                                         publications=1, payloads=1))
        assert_cut_inventory('finish-committed', inventory())
        with self.assertRaisesRegex(ReceiptFailure, 'will_finish_commit_not_terminal'):
            assert_cut_inventory('finish-committed', inventory(payloads=1))

    def test_canonical_will_receipt_keeps_installation_and_publication_identities_distinct(self):
        operation, will, publication = (bytes([value]) * 32 for value in (1, 2, 3))
        raw = b'MQDR\0\x01\x11' + operation + will + publication
        state = inventory()
        state['tables']['operations'] = [{'id': operation.hex(), 'recipe': raw.hex(), 'decision': 12}]
        result = will_commands(state)
        self.assertEqual(result[0]['operation'], operation.hex())
        self.assertEqual(result[0]['will'], will.hex())
        self.assertEqual(result[0]['publication'], publication.hex())
        state['tables']['operations'][0]['id'] = will.hex()
        with self.assertRaisesRegex(ReceiptFailure, 'will_canonical_receipt_identity_invalid'):
            will_commands(state)
