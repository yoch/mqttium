"""The oracle reads the v1 formats like the Rust reader does.

``contracts/examples/v1/index.json`` is the shared statement: a valid
example is accepted, an invalid one is refused for its declared reason.
"""

import json
import unittest

from mqttiumd_oracle.documents import Contracts

from .support import INDEX, ROOT


class ReaderAgreement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.index = json.loads((ROOT / INDEX).read_text())

    def test_the_contract_files_are_accepted(self):
        for name, diagnostics in self.contracts.check_contracts():
            self.assertEqual(diagnostics, [], name)

    def test_every_valid_example_is_accepted(self):
        self.assertGreaterEqual(len(self.index["valid"]), 10)
        for name in self.index["valid"] + [INDEX]:
            with self.subTest(name):
                self.assertEqual(self.contracts.check_file(name), [])

    def test_every_invalid_example_is_refused_for_its_declared_reason(self):
        self.assertGreaterEqual(len(self.index["invalid"]), 60)
        for case in self.index["invalid"]:
            with self.subTest(case["file"]):
                diagnostics = self.contracts.check_file(case["file"])
                self.assertTrue(diagnostics, "the example was accepted")
                self.assertEqual({d.code for d in diagnostics}, {case["code"]}, diagnostics)
                self.assertIn(case["pointer"], [d.pointer for d in diagnostics], diagnostics)

    def test_a_scenario_with_one_misplaced_step_gets_one_diagnostic(self):
        for name in [
            "scenario-deliver-while-crashed.json",
            "scenario-second-wait-on-a-holding-fault.json",
            "scenario-release-twice.json",
            "scenario-release-before-hold.json",
            "scenario-inspect-while-crashed.json",
        ]:
            with self.subTest(name):
                found = self.contracts.check_file(f"contracts/examples/v1/invalid/{name}")
                self.assertEqual(len(found), 1, found)

    def test_the_oracle_and_the_rust_reader_name_the_same_contracts(self):
        # The digest `mqttiumd-lab validate` prints is defined by
        # contracts/README.md; the oracle computes it from the same bytes
        # it interprets.
        listing = "".join(
            f"{sha256}  {path}\n" for path, sha256 in sorted(self.contracts.files.items())
        )
        import hashlib

        self.assertEqual(self.contracts.digest, hashlib.sha256(listing.encode()).hexdigest())
        self.assertEqual(len(self.contracts.digest), 64)


if __name__ == "__main__":
    unittest.main()
