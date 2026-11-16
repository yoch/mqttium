"""Finite TCP reception distinguishes command history from retained receipts."""
import unittest

from tcp_reception import committed_admission_ids


def admission(sequence, **details):
    return {"kind": "store_result", "command_kind": "Admission",
            "operation": {"incarnation": "1", "epoch": "1", "owner": "0",
                          "sequence": str(sequence)},
            "details": {"committed_now": True, "outcome": "Committed", **details}}


class AdmissionHistoryTests(unittest.TestCase):
    def test_replayed_results_and_lifecycle_commits_are_not_new_admissions(self):
        event = admission(1)
        self.assertEqual(len(committed_admission_ids([
            event, event, admission(2, committed_now=False),
            {**admission(3), "command_kind": "SessionOpen"},
            {**admission(4), "kind": "store_resolved"},
            admission(5, outcome="PendingOrUnknown"),
        ])), 1)

    def test_receipt_collection_does_not_erase_observed_admission_identity(self):
        before = committed_admission_ids([admission(1), admission(2)])
        after = committed_admission_ids([admission(1), admission(2),
            {"kind": "receipts_collected", "details": {"removed": 64}}])
        self.assertEqual(before, after)
        self.assertNotEqual(before, committed_admission_ids([admission(1), admission(2), admission(3)]))


if __name__ == "__main__":
    unittest.main()
