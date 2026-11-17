"""Finite TCP reception distinguishes command history from retained receipts."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from tcp_reception import Broker, committed_admission_ids
from tcp_probe import Observations, ReceiptFailure


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


class ExitedProcessObservationTests(unittest.TestCase):
    def test_final_event_written_between_scan_and_poll_is_not_masked_by_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            path=root/"events.jsonl"
            path.write_text("")
            event={"format":"mqttiumd.product-event","schema_version":1,"pid":7,
                   "seq":"1","kind":"receipt_replay_result",
                   "details":{"mode":"publication","outcome":"Committed","business_changed":True}}
            reached=[]
            def poll():
                reached.append(True)
                if len(reached)==1:
                    with path.open("a") as stream:
                        stream.write(json.dumps(event)+"\n")
                return 2
            broker=Broker.__new__(Broker)
            broker.events_path=path
            broker.stderr_path=root/"broker.stderr"
            broker.process=SimpleNamespace(pid=7,returncode=2,poll=poll)
            broker.observations=Observations(root/"observer.jsonl")
            try:
                observed=broker.wait_event("receipt_replay_result",timeout=.1)
            except ReceiptFailure as error:
                self.fail("retained_final_event_masked_by_exit: "+str(error))
            self.assertEqual(observed,event)
            self.assertTrue(reached)

    def test_exit_without_the_requested_event_keeps_its_explicit_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            broker=Broker.__new__(Broker)
            broker.events_path=root/"events.jsonl"
            broker.events_path.write_text("")
            broker.stderr_path=root/"broker.stderr"
            broker.process=SimpleNamespace(pid=7,returncode=2,poll=lambda:2)
            broker.observations=Observations(root/"observer.jsonl")
            with self.assertRaises(ReceiptFailure) as raised:
                broker.wait_event("receipt_replay_result",timeout=.1)
            self.assertEqual(raised.exception.code,"broker_exited_before_event")


if __name__ == "__main__":
    unittest.main()
