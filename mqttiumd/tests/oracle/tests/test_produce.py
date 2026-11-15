"""The command ``produce`` writes the result of a run, never qualifying on its own authority.

A qualifying result is evidence. The command writes one only when every
condition of a qualifying run holds, as far as the files it is given can
establish it; otherwise it writes a provisional result that says which
conditions are unmet, with the statuses the oracle found. A provisional
result never carries PASS.
"""

import contextlib
import io
import json
import shutil
import unittest

from mqttiumd_oracle import __main__ as command
from mqttiumd_oracle import results
from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.semantic import contracts_digest

from .support import ROOT

TRACES = "tests/oracle/traces"
SCENARIO = f"{TRACES}/scenarios/p1-02.json"
FULL = "contracts/examples/v1/valid/run-manifest-with-oracle-and-generators.json"
BARE = "contracts/examples/v1/valid/run-manifest-content-only.json"


class Produce(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def setUp(self):
        self.relative = f"target/oracle-tests/{self.id().rsplit('.', 1)[-1]}"
        self.directory = ROOT / self.relative
        shutil.rmtree(self.directory, ignore_errors=True)
        self.directory.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.directory, ignore_errors=True)

    def cite(self, path):
        sha256, size = self.contracts.hash_evidence(path)
        return {"path": path, "sha256": sha256, "bytes": str(size)}

    def manifest(self, base=FULL, **changes):
        """A manifest of this tree: its contracts, scenario, fixture, parameters and oracle."""
        manifest = json.loads((ROOT / base).read_text())
        manifest["contracts"] = [self.cite(path) for path in sorted(self.contracts.files)]
        manifest["contracts_digest"] = self.contracts.digest
        if base == FULL:
            scenario = json.loads((ROOT / SCENARIO).read_text())
            manifest.update(
                scenario=self.cite(SCENARIO),
                fixture=self.cite(scenario["initial_fixture"]),
                backend="sim-store",
                failure_model="process",
                scheduler={"version": "1", "seed": "1"},
                commands=["mqttiumd-lab run", "mqttiumd_oracle judge"],
            )
            manifest["oracle"]["files"] = [
                self.cite(entry["path"]) for entry in manifest["oracle"]["files"]
            ]
        manifest.update(changes)
        return self.store(manifest)

    def store(self, manifest):
        for member in ("oracle", "generators"):
            if member in manifest:
                files = manifest[member]["files"]
                manifest[member]["sha256"] = contracts_digest(
                    (entry["sha256"], entry["path"]) for entry in files
                )
        path = f"{self.relative}/manifest.json"
        (ROOT / path).write_text(json.dumps(manifest))
        return path

    def run_command(self, trace, manifest):
        out = f"{self.relative}/run-result.json"
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed), contextlib.redirect_stderr(io.StringIO()):
            code = command.main(
                [
                    "--root",
                    str(ROOT),
                    "produce",
                    "--scenario",
                    SCENARIO,
                    "--trace",
                    f"{TRACES}/{trace}/trace.jsonl",
                    "--manifest",
                    manifest,
                    "--out",
                    out,
                    "--scope",
                    "P1-02 on the hand-made corpus",
                ]
            )
        return code, out

    def written(self, out):
        self.assertEqual(self.contracts.check_file(out), [])
        return json.loads((ROOT / out).read_text())

    def unmet(self, result):
        return [reason for reason in result["reasons"] if reason.startswith("not qualifying: ")]

    def test_a_good_run_is_provisional_while_the_binary_is_not_tied_to_its_content(self):
        code, out = self.run_command("p1-02-good", self.manifest())
        result = self.written(out)
        self.assertEqual(result["qualification"], "provisional")
        self.assertEqual((result["verdict"], result["exit_code"], code), ("INCONCLUSIVE", 2, 2))
        self.assertEqual(
            self.unmet(result),
            [
                "not qualifying: nothing establishes that the binary was built from the "
                "recorded content (no member of run-manifest v1 says so)"
            ],
        )
        self.assertEqual({entry["status"] for entry in result["assertions"]}, {"PASS"})
        # A provisional result is no evidence: recompute refuses it.
        refused = results.recompute(self.contracts, out)
        self.assertEqual([diagnostic.code for diagnostic in refused], ["result-not-qualifying"])

    def test_a_failing_run_keeps_its_fail(self):
        code, out = self.run_command("pubrec-eligible-on-a-hidden-commit", self.manifest())
        result = self.written(out)
        self.assertEqual(
            (result["qualification"], result["verdict"], code), ("provisional", "FAIL", 1)
        )

    def test_each_condition_the_files_can_check_is_named_when_unmet(self):
        code, out = self.run_command("p1-02-good", self.manifest(BARE))
        unmet = self.unmet(self.written(out))
        for words in (
            "the manifest names no scenario",
            "the manifest names no fixture",
            "the manifest records no backend",
            "the manifest records no failure model",
            "the manifest records no scheduler",
            "the manifest records no command",
            "the manifest does not identify the oracle",
            "the manifest does not identify the generators",
        ):
            self.assertTrue(any(words in reason for reason in unmet), (words, unmet))

    def test_a_manifest_of_other_files_is_named(self):
        manifest = self.manifest()
        manifest_value = json.loads((ROOT / manifest).read_text())
        listed = manifest_value["contracts"]
        listed[0]["sha256"] = "0" * 64
        manifest_value["contracts_digest"] = contracts_digest(
            (entry["sha256"], entry["path"]) for entry in listed
        )
        manifest_value["scenario"]["sha256"] = "1" * 64
        manifest_value["oracle"]["files"][0]["sha256"] = "2" * 64
        manifest_value["checkout"]["tree_state"] = "modified"
        self.store(manifest_value)
        code, out = self.run_command("p1-02-good", manifest)
        unmet = self.unmet(self.written(out))
        for words in (
            "the manifest names other contracts",
            "the manifest names another scenario",
            "the oracle that judged is not the one the manifest identifies: "
            "tests/oracle/mqttiumd_oracle/judge.py",
            "the tree was modified and its content is not kept in the run directory",
        ):
            self.assertTrue(any(words in reason for reason in unmet), (words, unmet))

    def test_the_command_has_no_way_to_claim_qualifying(self):
        errors = io.StringIO()
        arguments = ["produce", "--scenario", SCENARIO, "--trace", "t", "--manifest", "m"]
        with contextlib.redirect_stderr(errors), self.assertRaises(SystemExit):
            command._main([*arguments, "--out", "o", "--scope", "s", "--qualifying"])
        self.assertIn("unrecognized arguments: --qualifying", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
