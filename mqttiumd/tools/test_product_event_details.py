"""Check the product journal with the field limits for each event kind."""
import json
from pathlib import Path
import tempfile
import unittest

from check_product_event_stream import check_streams
from check_schemas import load_validators


class OperationalEventDetailsTests(unittest.TestCase):
    def test_each_event_kind_keeps_its_own_field_bounds(self):
        examples = Path(__file__).resolve().parents[1] / "contracts/examples/v1"
        ready = json.loads((examples / "valid/product-ready-limits.json").read_text())["details"]
        names = [f"field{chr(97 + i // 26)}{chr(97 + i % 26)}" for i in range(114)]
        validators = load_validators()
        cases = (
            ("starting", dict.fromkeys(names[:40], 1), "PASS"),
            ("starting", dict.fromkeys(names[:41], 1), "FAIL"),
            ("ready", ready, "PASS"),
            ("ready", {**ready, "will": True}, "FAIL"),
            ("ready", {"qos0_window": 1, "qos0_typo": 1}, "FAIL"),
            ("starting", {"qos0_window": 1}, "FAIL"),
            ("counters", {"counts": dict.fromkeys(names[:113], "1")}, "PASS"),
            ("counters", {"counts": dict.fromkeys(names, "1")}, "FAIL"),
            ("counters", {"counts": {}}, "FAIL"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            for kind, details, expected in cases:
                with self.subTest(kind=kind, fields=len(details), expected=expected):
                    event = {"format": "mqttiumd.product-event", "schema_version": 1,
                             "seq": "1", "kind": kind, "pid": 7, "details": details}
                    path.write_text(json.dumps(event) + "\n")
                    result = check_streams([path], validators=validators)
                    self.assertEqual(result["status"], expected, result.get("failure"))


if __name__ == "__main__":
    unittest.main()
