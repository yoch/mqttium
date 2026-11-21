"""The actual product-stream checker refuses shape and bounded-reader defects."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

TOOLS = Path(__file__).resolve().parents[2] / "tools"
sys.path.insert(0, str(TOOLS))
from check_product_event_stream import Limits, check_streams


class ProductEventStreamTests(unittest.TestCase):
    def event(self, **fields):
        return {"format": "mqttiumd.product-event", "schema_version": 1,
                "seq": "1", "kind": "starting", "pid": 7, **fields}

    def run_stream(self, lines, *, limits=Limits()):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.events.jsonl"
            encoded = b"".join(line if isinstance(line, bytes) else (json.dumps(line) + "\n").encode() for line in lines)
            path.write_bytes(encoded)
            result = check_streams([path], limits=limits)
            return result, hashlib.sha256(encoded).hexdigest()

    def test_packaged_events_filename_is_included_when_scanning_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/"events.jsonl").write_text(json.dumps(self.event())+"\n")
            result=check_streams([root])
            self.assertEqual(result["status"],"PASS","packaged_event_stream_was_silently_skipped")
            self.assertEqual(result["product_events"],1)

    def test_a_valid_stream_keeps_its_actual_byte_digest_and_schema_only_scope(self):
        result, digest = self.run_stream([self.event(), self.event(seq="2", kind="connection_closed")])
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["product_events"], 2)
        self.assertEqual(result["files"][0]["sha256"], digest)
        self.assertEqual(result["qualification"], "none")

    def test_qos0_event_name_is_valid_but_a_leading_digit_is_not(self):
        result, _ = self.run_stream([self.event(kind="qos0_dropped",
            details={"origin": "volatile/1/1/3", "reason": "no_subscribers", "target": ""})])
        self.assertEqual(result["status"], "PASS", "qos0_event_name_rejected_by_schema")
        result, _ = self.run_stream([self.event(kind="0qos_dropped")])
        self.assertEqual(result["failure"]["code"], "product_event_schema_mismatch")
        self.assertEqual(result["failure"]["pointer"], "/kind")

    def test_generic_lifecycle_command_observations_use_the_published_vocabulary(self):
        event = self.event(kind="command_submitted", command_kind="SessionLifecycle",
                           operation={"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"},
                           details={"action": "Open", "clean_start": False,
                                    "expected_session": None, "expiry": {"After": 1}})
        result, _ = self.run_stream([event])
        self.assertEqual(result["status"], "PASS")

    def test_a_missing_required_field_is_a_schema_failure_with_location_not_values(self):
        event = self.event()
        del event["pid"]
        result, _ = self.run_stream([event])
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["failure"]["code"], "product_event_schema_mismatch")
        self.assertEqual(result["failure"]["line"], 1)
        self.assertEqual(result["failure"]["pointer"], "")
        self.assertNotIn("document", result["failure"])

    def test_duplicate_json_members_cannot_hide_an_invalid_event(self):
        result, _ = self.run_stream([b'{"format":"mqttiumd.product-event","pid":7,"pid":8}\n'])
        self.assertEqual(result["failure"]["code"], "product_event_parse_refused")
        self.assertEqual(result["failure"]["cause"], "duplicate-key")

    def test_line_budget_precedes_json_parsing(self):
        with patch("check_product_event_stream.parse_strict", side_effect=AssertionError("parser reached")):
            result, _ = self.run_stream([b"x" * 65], limits=Limits(line_bytes=64))
        self.assertEqual(result["failure"]["resource"], "line_bytes")

    def test_event_and_aggregate_byte_budgets_are_explicit(self):
        result, _ = self.run_stream([self.event(), self.event(seq="2")], limits=Limits(events=1))
        self.assertEqual(result["failure"]["resource"], "events")
        result, _ = self.run_stream([self.event()], limits=Limits(total_bytes=1))
        self.assertEqual(result["failure"]["resource"], "total_bytes")

    def test_filtered_records_cannot_bypass_the_work_budget(self):
        result, _ = self.run_stream([{"format": "client"}, self.event()], limits=Limits(events=1))
        self.assertEqual(result["failure"]["resource"], "events")

    def test_rotated_product_streams_are_checked_and_wire_transcripts_are_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "broker.events.jsonl").write_text(json.dumps(self.event(seq="2")) + "\n")
            (root / "broker.events.jsonl.previous").write_text(json.dumps(self.event()) + "\n")
            (root / "client-wire.jsonl").write_text("not a product event or JSON\n")
            result = check_streams([root])
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["product_events"], 2)
            self.assertEqual(len(result["files"]), 2)
            refused = check_streams([root], limits=Limits(files=1))
            self.assertEqual(refused["failure"]["resource"], "files")

    def test_another_named_event_format_is_filtered_without_qualifying_an_empty_scan(self):
        result, _ = self.run_stream([{"format": "mqttiumd.client-observation"}, self.event()])
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["skipped_records"], 1)
        result, _ = self.run_stream([{"format": "mqttiumd.client-observation"}])
        self.assertEqual(result["failure"]["code"], "product_event_stream_empty")

    def test_cli_schema_failure_is_exit_one_and_retains_machine_readable_cause(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.events.jsonl"
            path.write_text(json.dumps(self.event(kind="INVALID")) + "\n")
            completed = subprocess.run([sys.executable, str(TOOLS / "check_product_event_stream.py"), str(path)],
                                       capture_output=True, check=False, timeout=10)
            self.assertEqual(completed.returncode, 1)
            result = json.loads(completed.stdout)
            self.assertEqual(result["failure"]["code"], "product_event_schema_mismatch")
            self.assertEqual(result["failure"]["pointer"], "/kind")


if __name__ == "__main__":
    unittest.main()
