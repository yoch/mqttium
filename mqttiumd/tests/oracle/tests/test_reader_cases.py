"""Cases of the readers that no static example can carry.

A qualifying result names files and contracts by digest; a cycle, a file of
16 MiB and a non-canonical decimal string are built here. The strict parser
has its own cases of invalid input.
"""

import copy
import hashlib
import json
import shutil
import unittest

from mqttiumd_oracle import semantic, strict
from mqttiumd_oracle.documents import MAX_DOCUMENT_BYTES, Contracts, ContractsUnreadable

from .support import ROOT

EVERY_RUN = (
    "trace_complete",
    "broker_inputs_have_provenance",
    "one_grant_per_opened_epoch",
    "scenario_reaches_its_end",
)

SCENARIO = "tests/scenarios/p1/P1-02.json"
SCRATCH = "target/oracle-tests"


def codes(diagnostics):
    return sorted({diagnostic.code for diagnostic in diagnostics})


class Scratch(unittest.TestCase):
    """Tests that write documents below the root, in a directory of their own."""

    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def setUp(self):
        self.relative = f"{SCRATCH}/{self.id().rsplit('.', 1)[-1]}"
        self.directory = ROOT / self.relative
        shutil.rmtree(self.directory, ignore_errors=True)
        self.directory.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.directory, ignore_errors=True)

    def write(self, name, value):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        (self.directory / name).write_bytes(data)
        return f"{self.relative}/{name}"

    def cite(self, path):
        data = (ROOT / path).read_bytes()
        return {"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": str(len(data))}


class QualifyingResult(Scratch):
    def passing(self):
        """A complete qualifying PASS for P1-02, citing files of this test."""
        manifest = json.loads(
            (ROOT / "contracts/examples/v1/valid/run-manifest-content-only.json").read_text()
        )
        manifest["contracts"] = [
            {
                "path": path,
                "sha256": sha256,
                "bytes": str(len((ROOT / path).read_bytes())),
            }
            for path, sha256 in sorted(self.contracts.files.items())
        ]
        manifest["contracts_digest"] = self.contracts.digest
        scenario = json.loads((ROOT / SCENARIO).read_text())
        return {
            "format": "mqttiumd.run-result",
            "schema_version": 1,
            "qualification": "qualifying",
            "run_id": "p1-02-qualifying",
            "verdict": "PASS",
            "exit_code": 0,
            "scope": "P1-02, built by the tests of the oracle",
            "reasons": [],
            "assertions": [
                {"name": name, "status": "PASS"}
                for name in [
                    *scenario["assertions"],
                    *(owed for owed in EVERY_RUN if owed not in scenario["assertions"]),
                ]
            ],
            "faults": [{"id": "lost_admission_result", "expected_hits": "1", "hits": "1"}],
            "scenario": self.cite(SCENARIO),
            "manifest": self.cite(self.write("manifest.json", manifest)),
            "trace": self.cite(self.write("trace.jsonl", b"{}\n")),
            "contracts_digest": self.contracts.digest,
            "artifacts": [],
        }

    def check(self, result):
        return codes(self.contracts.check_file(self.write("result.json", result)))

    def test_a_complete_qualifying_pass_is_accepted(self):
        self.assertEqual(self.check(self.passing()), [])

    def test_an_assertion_of_the_scenario_cannot_be_left_out(self):
        result = self.passing()
        result["assertions"].pop()
        self.assertEqual(self.check(result), ["result-incomplete"])

    def test_an_assertion_the_scenario_does_not_list_is_refused(self):
        result = self.passing()
        result["assertions"].append({"name": "same_recipe_for_op_B", "status": "PASS"})
        self.assertEqual(self.check(result), ["result-incomplete"])

    def test_an_assertion_every_run_owes_cannot_be_left_out(self):
        # P1-02 does not list them all; every run owes them (ADR 0003,
        # DEM-MU-03, DEM-012).
        for name in EVERY_RUN:
            with self.subTest(name):
                result = self.passing()
                kept = [entry for entry in result["assertions"] if entry["name"] != name]
                self.assertEqual(len(kept), len(result["assertions"]) - 1)
                result["assertions"] = kept
                self.assertEqual(self.check(result), ["result-incomplete"])

    def test_an_assertion_of_the_scenario_cannot_be_out_of_scope(self):
        result = self.passing()
        result["assertions"][0]["status"] = "OUT_OF_SCOPE"
        result["verdict"], result["exit_code"] = "OUT_OF_SCOPE", 3
        self.assertEqual(self.check(result), ["result-incomplete"])

    def test_the_faults_are_exactly_those_of_the_scenario(self):
        result = self.passing()
        result["faults"] = []
        self.assertEqual(self.check(result), ["result-incomplete"])
        result = self.passing()
        result["faults"][0]["expected_hits"] = result["faults"][0]["hits"] = "2"
        self.assertEqual(self.check(result), ["result-incomplete"])

    def test_a_missed_injection_blocks_the_pass(self):
        result = self.passing()
        result["faults"][0]["hits"] = "0"
        self.assertEqual(self.check(result), ["verdict-inconsistent"])

    def test_cited_files_have_the_declared_digest_and_size(self):
        for member in ("trace", "manifest", "scenario"):
            for edit in ({"sha256": "0" * 64}, {"bytes": "1"}):
                with self.subTest(member=member, edit=edit):
                    result = self.passing()
                    result[member].update(edit)
                    self.assertEqual(self.check(result), ["evidence-mismatch"])

    def test_a_result_is_evaluated_only_against_the_contracts_it_names(self):
        result = self.passing()
        result["contracts_digest"] = "0" * 64
        diagnostics = self.contracts.check_file(self.write("result.json", result))
        self.assertEqual(codes(diagnostics), ["evidence-mismatch"])
        # The result itself names other contracts, whatever its manifest says.
        self.assertIn("/contracts_digest", [diagnostic.pointer for diagnostic in diagnostics])

    def test_the_cited_scenario_is_a_valid_scenario(self):
        result = self.passing()
        result["scenario"] = self.cite("contracts/v1/profiles.json")
        self.assertEqual(self.check(result), ["reference-invalid"])

    def test_a_cited_trace_that_does_not_exist_is_refused(self):
        result = self.passing()
        result["trace"]["path"] = f"{self.relative}/absent.jsonl"
        self.assertEqual(self.check(result), ["file-unreadable"])

    def test_a_cited_trace_outside_the_root_is_refused(self):
        result = self.passing()
        result["trace"]["path"] = "tests/../../outside.jsonl"
        self.assertEqual(self.check(result), ["path-escape"])


class Verdict(unittest.TestCase):
    def test_the_verdict_is_the_one_statuses_and_injections_determine(self):
        cases = [
            (["PASS", "PASS"], True, "PASS"),
            (["PASS", "FAIL", "INCONCLUSIVE"], True, "FAIL"),
            (["FAIL"], False, "FAIL"),
            (["PASS", "INCONCLUSIVE"], True, "INCONCLUSIVE"),
            (["PASS"], False, "INCONCLUSIVE"),
            # Nothing evaluated concludes nothing: not a PASS, not out of scope.
            ([], True, "INCONCLUSIVE"),
            (["PASS", "OUT_OF_SCOPE"], True, "OUT_OF_SCOPE"),
            (["OUT_OF_SCOPE"], True, "OUT_OF_SCOPE"),
            (["OUT_OF_SCOPE"], False, "INCONCLUSIVE"),
        ]
        for statuses, reached, expected in cases:
            with self.subTest(statuses=statuses, reached=reached):
                self.assertEqual(semantic.verdict_of(statuses, reached), expected)


class References(Scratch):
    def scenario(self, fixture):
        value = json.loads((ROOT / SCENARIO).read_text())
        value["initial_fixture"] = fixture
        return value

    def test_a_scenario_that_names_itself_as_fixture_is_refused(self):
        path = f"{self.relative}/self.json"
        self.write("self.json", self.scenario(path))
        self.assertEqual(codes(self.contracts.check_file(path)), ["reference-invalid"])

    def test_a_document_larger_than_the_reader_loads_is_refused(self):
        path = self.write("large.json", b" " * (MAX_DOCUMENT_BYTES + 1))
        self.assertEqual(codes(self.contracts.check_file(path)), ["file-too-large"])
        scenario = self.write("scenario.json", self.scenario(path))
        self.assertEqual(codes(self.contracts.check_file(scenario)), ["file-too-large"])

    def test_a_reference_to_a_directory_is_refused(self):
        scenario = self.write("scenario.json", self.scenario("tests/fixtures/p1"))
        self.assertEqual(codes(self.contracts.check_file(scenario)), ["file-unreadable"])

    def test_a_link_is_followed_only_while_it_stays_below_the_root(self):
        inside = self.directory / "inside.json"
        inside.symlink_to(ROOT / "tests/fixtures/p1/three-persistent-sessions.json")
        scenario = self.write("ok.json", self.scenario(f"{self.relative}/inside.json"))
        self.assertEqual(self.contracts.check_file(scenario), [])
        outside = self.directory / "outside.json"
        outside.symlink_to("/etc/hostname")
        scenario = self.write("bad.json", self.scenario(f"{self.relative}/outside.json"))
        self.assertEqual(codes(self.contracts.check_file(scenario)), ["path-escape"])

    def test_the_chain_of_checks_is_bounded(self):
        # The table of references is acyclic; a reader re-entered on the
        # document it is checking refuses instead of recursing.
        path = self.write("scenario.json", json.loads((ROOT / SCENARIO).read_text()))
        self.contracts._active.append(path)
        self.addCleanup(self.contracts._active.clear)
        self.assertEqual(codes(self.contracts.check_file(path)), ["reference-cycle"])


class StrictParser(unittest.TestCase):
    def refused(self, data):
        with self.assertRaises(strict.Refused) as raised:
            strict.parse(data)
        return raised.exception.diagnostic.code

    def test_invalid_input_is_refused_for_its_own_reason(self):
        cases = {
            b"": "json-syntax",
            b"{": "json-syntax",
            b'{"a": 1} {"a": 1}': "json-syntax",
            b'{"a": NaN}': "json-syntax",
            b'{"a": Infinity}': "json-syntax",
            b'{"a": "\\ud800"}': "json-syntax",
            b"\xff\xfe": "json-syntax",
            b'{"a": 1, "a": 2}': "duplicate-key",
            b'{"a": {"b": 1, "b": 1}}': "duplicate-key",
            b'{"a": 7.0}': "number-not-integer",
            b'{"a": 7e0}': "number-not-integer",
            b'{"a": -0}': "number-not-integer",
            b'{"a": 18446744073709551616}': "number-not-integer",
            b'{"a": -9223372036854775809}': "number-not-integer",
        }
        for data, code in cases.items():
            with self.subTest(data):
                self.assertEqual(self.refused(data), code)

    def test_the_bounds_of_an_integer_literal_are_accepted(self):
        self.assertEqual(strict.parse(b"18446744073709551615"), 2**64 - 1)
        self.assertEqual(strict.parse(b"-9223372036854775808"), -(2**63))
        self.assertEqual(strict.parse(b'{"a": "\\ud83d\\ude00"}'), {"a": "\U0001f600"})

    def test_a_deeply_nested_document_is_refused_not_crashed_on(self):
        self.assertEqual(self.refused(b"[" * 100_000 + b"]" * 100_000), "json-syntax")

    def test_a_decimal_string_is_canonical_and_fits_64_bits(self):
        self.assertEqual(strict.u64("0"), 0)
        self.assertEqual(strict.u64("18446744073709551615"), 2**64 - 1)
        for text in ["", "00", "01", "+1", "-1", "1.0", " 1", "1 ", "1\n", "१", "0x1", 1, None]:
            with self.subTest(text):
                with self.assertRaisesRegex(ValueError, "u64-not-canonical"):
                    strict.u64(text)
        for text in ["18446744073709551616", "99999999999999999999", "1" + "0" * 30]:
            with self.subTest(text):
                with self.assertRaisesRegex(ValueError, "u64-out-of-range"):
                    strict.u64(text)


class ContractDirectory(Scratch):
    def copy_of_the_contracts(self):
        shutil.copytree(ROOT / "contracts", self.directory / "contracts")
        return self.directory

    def test_a_schema_with_an_identifier_or_a_remote_reference_is_refused(self):
        root = self.copy_of_the_contracts()
        path = root / "contracts/schema/v1/fixture.schema.json"
        original = json.loads(path.read_text())
        for edit in (
            {"$id": "https://example.com/fixture.schema.json"},
            {"$schema": "http://json-schema.org/draft-07/schema#"},
            {"properties": {"x": {"$ref": "https://example.com/x.json"}}},
            {"properties": {"x": {"$ref": "../v2/primitives.schema.json#/$defs/u64"}}},
        ):
            with self.subTest(edit):
                path.write_text(json.dumps({**copy.deepcopy(original), **edit}))
                with self.assertRaises(ContractsUnreadable):
                    Contracts.open(root)

    def test_the_digest_names_the_bytes_that_are_interpreted(self):
        root = self.copy_of_the_contracts()
        before = Contracts.open(root)
        self.assertEqual(before.digest, self.contracts.digest)
        path = root / "contracts/v1/profiles.json"
        profiles = json.loads(path.read_text())
        profiles["profiles"][0]["id"] = "p1-other"
        path.write_text(json.dumps(profiles))
        # The reader opened earlier keeps the rules its digest names.
        self.assertIn("p1-durable-mqtt5-without-expiry", before.catalogs["profiles"])
        self.assertEqual(before.check_contracts()[0][1], [])
        after = Contracts.open(root)
        self.assertNotEqual(after.digest, before.digest)
        self.assertEqual(after.catalogs["profiles"], {"p1-other"})


if __name__ == "__main__":
    unittest.main()
