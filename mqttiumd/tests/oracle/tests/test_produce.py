"""The command ``produce`` writes the result of a run, never qualifying on its own authority.

A qualifying result is evidence. The command writes one only when every
condition of a qualifying run holds, as far as the files it is given can
establish it; otherwise it writes a provisional result that says which
conditions are unmet, with the statuses the oracle found. A provisional
result never carries PASS.
"""

import contextlib
import hashlib
import io
import json
import shutil
import tarfile
import unittest
from unittest import mock

from mqttiumd_oracle import __main__ as command
from mqttiumd_oracle import results
from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.semantic import contracts_digest

from .support import ROOT

TRACES = "tests/oracle/traces"
SCENARIO = f"{TRACES}/scenarios/p1-02.json"
FULL = "contracts/examples/v1/valid/run-manifest-with-oracle-and-generators.json"
BARE = "contracts/examples/v1/valid/run-manifest-content-only.json"


ORACLE = "tests/oracle/mqttiumd_oracle"
REMAPPED = "--remap-path-prefix=/tmp/mqttiumd-source-0123456789abcdef=/mqttiumd-source"


def running_oracle():
    """The files of the oracle as the bench lists them: its modules and its requirements."""
    paths = sorted(f"{ORACLE}/{path.name}" for path in (ROOT / ORACLE).glob("*.py")) + [
        "tests/oracle/requirements.txt"
    ]
    return [
        {
            "path": path,
            "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest(),
            "bytes": str(len((ROOT / path).read_bytes())),
        }
        for path in paths
    ]


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
            manifest["oracle"]["files"] = running_oracle()
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
                "recorded content (the manifest has no artifact.built_from)"
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
        manifest_value["checkout"]["modified_paths"] = ["tools/probe.txt"]
        self.store(manifest_value)
        code, out = self.run_command("p1-02-good", manifest)
        unmet = self.unmet(self.written(out))
        for words in (
            "the manifest names other contracts",
            "the manifest names another scenario",
            "the oracle that judged is not the one the manifest identifies: "
            f"{manifest_value['oracle']['files'][0]['path']}",
            "the modified path tools/probe.txt is not kept in the run directory",
        ):
            self.assertTrue(any(words in reason for reason in unmet), (words, unmet))

    def test_the_command_has_no_way_to_claim_qualifying(self):
        errors = io.StringIO()
        arguments = ["produce", "--scenario", SCENARIO, "--trace", "t", "--manifest", "m"]
        with contextlib.redirect_stderr(errors), self.assertRaises(SystemExit):
            command._main([*arguments, "--out", "o", "--scope", "s", "--qualifying"])
        self.assertIn("unrecognized arguments: --qualifying", errors.getvalue())


class Campaign(unittest.TestCase):
    """``produce`` run as a campaign runs it: ``--root`` is the campaign directory.

    The campaign holds its contracts, scenario, fixture and trace, not the
    oracle: the oracle that judges is the one that runs, and the manifest
    names its files by their paths in the checkout.
    """

    def setUp(self):
        self.campaign = ROOT / f"target/oracle-tests/{self.id().rsplit('.', 1)[-1]}/campaign"
        shutil.rmtree(self.campaign.parent, ignore_errors=True)
        self.campaign.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.campaign.parent, ignore_errors=True)
        shutil.copytree(ROOT / "contracts", self.campaign / "contracts")
        scenario = json.loads((ROOT / SCENARIO).read_text())
        for path in (SCENARIO, scenario["initial_fixture"]):
            (self.campaign / path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / path, self.campaign / path)
        # The trace with the snapshots it names.
        shutil.copytree(ROOT / TRACES / "p1-02-good", self.campaign / TRACES / "p1-02-good")
        self.contracts = Contracts.open(self.campaign)

    def cite(self, path):
        data = (self.campaign / path).read_bytes()
        return {"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": str(len(data))}

    def keep(self, path, data):
        (self.campaign / path).parent.mkdir(parents=True, exist_ok=True)
        (self.campaign / path).write_bytes(data)

    def produce(self, built_from=None, rustflags=None, broker=None, **checkout):
        manifest = json.loads((ROOT / FULL).read_text())
        scenario = json.loads((ROOT / SCENARIO).read_text())
        manifest.update(
            scenario=self.cite(SCENARIO),
            fixture=self.cite(scenario["initial_fixture"]),
            backend="sim-store",
            failure_model="process",
            scheduler={"version": "1", "seed": "1"},
            commands=["mqttiumd-lab run", "mqttiumd_oracle produce"],
        )
        manifest["contracts"] = [self.cite(path) for path in sorted(self.contracts.files)]
        manifest["contracts_digest"] = self.contracts.digest
        manifest["oracle"]["files"] = running_oracle()
        for member in ("oracle", "generators"):
            manifest[member]["sha256"] = contracts_digest(
                (entry["sha256"], entry["path"]) for entry in manifest[member]["files"]
            )
        manifest["checkout"].update(checkout)
        if built_from is not None:
            manifest["artifact"]["built_from"] = built_from
        if rustflags is not None:
            manifest["artifact"]["built_with"]["rustflags"] = rustflags
        if broker is not None:
            manifest["broker"] = broker
        (self.campaign / "manifest.json").write_text(json.dumps(manifest))
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed), contextlib.redirect_stderr(io.StringIO()):
            command.main(
                [
                    "--root",
                    str(self.campaign),
                    "produce",
                    "--scenario",
                    SCENARIO,
                    "--trace",
                    f"{TRACES}/p1-02-good/trace.jsonl",
                    "--manifest",
                    "manifest.json",
                    "--out",
                    "run-result.json",
                    "--scope",
                    "P1-02 in a campaign",
                ]
            )
        self.result = json.loads((self.campaign / "run-result.json").read_text())
        return [
            reason for reason in self.result["reasons"] if reason.startswith("not qualifying: ")
        ]

    def archive(self, replace=None, drop=(), extra=None):
        """A source.tar as run --rebuild writes it: the checkout, members named ``./<path>``.

        It holds the contract files of the campaign and the files of the
        running oracle; ``replace`` changes the content of members, ``drop``
        leaves members out, ``extra`` adds members.
        """
        members = {path: (self.campaign / path).read_bytes() for path in self.contracts.files}
        members.update(
            {entry["path"]: (ROOT / entry["path"]).read_bytes() for entry in running_oracle()}
        )
        members.update(extra or {})
        members.update(replace or {})
        for path in drop:
            members.pop(path)
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
            directory = tarfile.TarInfo("./")
            directory.type = tarfile.DIRTYPE
            archive.addfile(directory)
            for path, data in sorted(members.items()):
                info = tarfile.TarInfo(f"./{path}")
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        return buffer.getvalue()

    def rebuilt(self, data=None):
        """A built_from member naming ``source.tar`` as the campaign keeps it."""
        self.keep("source.tar", self.archive() if data is None else data)
        return {
            "source": self.cite("source.tar"),
            "build": "cargo build --locked --offline -p mqttiumd-lab --bin mqttiumd-lab",
        }

    def produce_rebuilt(self, data=None, rustflags=REMAPPED, **checkout):
        member = self.rebuilt(data)
        return self.produce(member, rustflags=rustflags, **checkout)

    def test_a_complete_run_qualifies_and_stands(self):
        self.assertEqual(self.produce_rebuilt(), [])
        self.assertEqual(self.result["qualification"], "qualifying")
        self.assertEqual((self.result["verdict"], self.result["exit_code"]), ("PASS", 0))
        self.assertEqual(self.contracts.check_file("run-result.json"), [])
        self.assertEqual(results.recompute(self.contracts, "run-result.json"), [])

    def revise_evidence(self, snapshot_edit=None, recovery_edit=None):
        """Edit descriptive evidence and regenerate its citations, not its verdict."""
        path = self.campaign / TRACES / "p1-02-good/trace.jsonl"
        records = [json.loads(line) for line in path.read_text().splitlines()]
        for record in records:
            if record["kind"] == "DurableStateInspected":
                cited = record["payload"]["snapshot"]
                snapshot_path = self.campaign / cited["path"]
                value = json.loads(snapshot_path.read_text())
                if snapshot_edit is not None:
                    snapshot_edit(value)
                    snapshot_path.write_text(json.dumps(value))
                    cited.update(self.cite(cited["path"]))
            if recovery_edit is not None and record["kind"] in (
                "RecoveryPageRead", "RecoveryPageDelivered"
            ):
                recovery_edit(record["payload"])
        path.write_text("".join(json.dumps(record) + "\n" for record in records))

    def test_outgoing_claims_are_descriptive_but_not_fixture_qualified(self):
        def claim(value):
            obligation = value["obligations"][0]
            value["outbound"] = [{
                "delivery_id": {"publication_id": obligation["publication_id"],
                                "index": obligation["index"]},
                "target_session": obligation["session"], "target_revision": "1",
                "packet_id": 1, "claim_operation_id": value["operations"][0]["operation_id"],
                "phase": "AwaitPuback", "emission": "MayHaveStarted",
            }]

        self.revise_evidence(snapshot_edit=claim)
        self.assertEqual(self.produce_rebuilt(), [
            "not qualifying: the fixture-based qualifier does not cover outgoing claims "
            "or subscription mutations"
        ])
        self.assertEqual((self.result["qualification"], self.result["verdict"]),
                         ("provisional", "INCONCLUSIVE"))

    def test_subscription_receipt_does_not_extend_fixture_qualification(self):
        def subscriptions(value):
            operation = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "20"}
            numbers = (1, 1, 0, 20, 2, 1, 2)
            body = b"".join(n.to_bytes(8, "big") for n in numbers)
            canonical = b"MQDR\x00\x01\x03" + body + b"\x01\x01\x00\x01x"
            value["operations"].append({
                "operation_id": operation, "recipe_hex": canonical.hex(),
                "decision": {"kind": "Committed", "facts": {
                    "kind": "SubscriptionsChanged", "session": "2", "route_revision": "2",
                }},
            })

        self.revise_evidence(snapshot_edit=subscriptions)
        self.assertEqual(self.produce_rebuilt(), [
            "not qualifying: the fixture-based qualifier does not cover outgoing claims "
            "or subscription mutations"
        ])
        self.assertEqual(self.result["verdict"], "INCONCLUSIVE")

    def test_queued_recovery_metadata_keeps_existing_fixture_qualification(self):
        snapshot = json.loads((self.campaign / TRACES / "p1-02-good/final.snapshot.json").read_text())
        obligation = snapshot["obligations"][0]

        def recovered(page):
            if page["run_epoch"] != "2":
                return
            fact = {
                "kind": "OutboundDelivery",
                "delivery_id": {"publication_id": obligation["publication_id"],
                                "index": obligation["index"]},
                "target_session": obligation["session"], "target_revision": "1", "claim": None,
            }
            at = next((i for i, old in enumerate(page["facts"]) if old["kind"] == "Operation"),
                      len(page["facts"]))
            page["facts"].insert(at, fact)

        self.revise_evidence(recovery_edit=recovered)
        self.assertEqual(self.produce_rebuilt(), [])
        self.assertEqual(self.result["verdict"], "PASS")
        self.assertEqual(results.recompute(self.contracts, "run-result.json"), [])

    def test_unsubstantiated_queued_recovery_metadata_cannot_qualify(self):
        def recovered(page):
            if page["run_epoch"] == "2":
                at = next((i for i, old in enumerate(page["facts"]) if old["kind"] == "Operation"),
                          len(page["facts"]))
                page["facts"].insert(at, {
                    "kind": "OutboundDelivery",
                    "delivery_id": {"publication_id": {
                        "incarnation": "1", "epoch": "1", "owner": "0", "sequence": "900"
                    }, "index": 0},
                    "target_session": "2", "target_revision": "1", "claim": None,
                })

        self.revise_evidence(recovery_edit=recovered)
        self.assertEqual(self.produce_rebuilt(), [
            "not qualifying: queued recovery delivery metadata is not corroborated "
            "by a durable snapshot"
        ])
        self.assertEqual(self.result["verdict"], "INCONCLUSIVE")

    def test_a_manifest_and_a_trace_that_disagree_on_the_broker_are_named(self):
        # Record 0019: the trace of p1-02-good says nothing, so in-process.
        member = self.rebuilt()
        manifest = json.loads((ROOT / FULL).read_text())
        self.assertNotIn("broker", manifest)
        unmet = self.produce(member, rustflags=REMAPPED, broker="child-process")
        self.assertEqual(
            unmet,
            [
                "not qualifying: the manifest says the broker ran as a child-process "
                "and the trace header says in-process"
            ],
        )
        self.assertEqual(self.produce(member, rustflags=REMAPPED, broker="in-process"), [])

    # R1 of the audit of a4aea95f: recompute checks the conditions of a
    # qualifying run again, not only the assertions and the faults. Each
    # result below qualified when it was produced; one condition is then
    # broken, and recompute refuses it for that condition alone.

    def refused(self):
        """What recompute says of the result produced, as (code, text) pairs."""
        return [
            (diagnostic.code, diagnostic.message)
            for diagnostic in results.recompute(self.contracts, "run-result.json")
        ]

    def qualified(self, **checkout):
        self.assertEqual(self.produce_rebuilt(**checkout), [])
        self.assertEqual(self.result["qualification"], "qualifying")
        self.assertEqual(self.refused(), [])

    def unqualified(self, condition):
        return [
            (
                "result-not-qualifying",
                f"a condition of a qualifying run no longer holds: {condition}",
            )
        ]

    def test_recompute_refuses_a_result_whose_source_is_gone(self):
        self.qualified()
        (self.campaign / "source.tar").unlink()
        self.assertEqual(
            self.refused(),
            self.unqualified("the source the manifest names, source.tar, is not in the campaign"),
        )

    def test_recompute_refuses_a_result_whose_source_changed_by_one_byte(self):
        self.qualified()
        data = bytearray((self.campaign / "source.tar").read_bytes())
        data[0] ^= 1
        (self.campaign / "source.tar").write_bytes(bytes(data))
        self.assertEqual(
            self.refused(),
            self.unqualified(
                "source.tar of the campaign is not the source the manifest names: "
                "its digest or its size differs"
            ),
        )

    def test_recompute_refuses_a_result_judged_by_another_oracle(self):
        self.qualified()
        # The oracle that runs now differs from the one the manifest names
        # in one file: a checkout of it with judge.py changed.
        checkout = self.campaign.parent / "checkout"
        shutil.copytree(ROOT / "tests/oracle/mqttiumd_oracle", checkout / ORACLE)
        (checkout / ORACLE / "judge.py").write_bytes(b"# another judge\n")
        shutil.copy(ROOT / "tests/oracle/requirements.txt", checkout / "tests/oracle")
        with (
            mock.patch.object(results, "CHECKOUT", checkout),
            mock.patch.object(results, "PACKAGE", checkout / ORACLE),
        ):
            refused = self.refused()
        self.assertEqual(
            refused,
            self.unqualified(
                f"the oracle that judged is not the one the manifest identifies: {ORACLE}/judge.py"
            ),
        )

    def test_recompute_refuses_a_result_whose_kept_untracked_file_is_gone(self):
        self.keep("modified/untracked/tools/probe.txt", b"probe\n")
        member = self.rebuilt(self.archive(extra={"tools/probe.txt": b"probe\n"}))
        self.assertEqual(
            self.produce(
                member,
                rustflags=REMAPPED,
                tree_state="modified",
                modified_paths=["tools/probe.txt"],
            ),
            [],
        )
        self.assertEqual(self.refused(), [])
        (self.campaign / "modified/untracked/tools/probe.txt").unlink()
        self.assertEqual(
            self.refused(),
            self.unqualified("the modified path tools/probe.txt is not kept in the run directory"),
        )

    def test_recompute_refuses_a_result_whose_kept_patch_is_gone(self):
        self.keep("modified/tracked.patch", b"diff --git a/crates/x.rs b/crates/x.rs\n")
        self.qualified(tree_state="modified", modified_paths=["crates/x.rs"])
        (self.campaign / "modified/tracked.patch").unlink()
        self.assertEqual(
            self.refused(),
            self.unqualified("the modified path crates/x.rs is not kept in the run directory"),
        )

    def test_a_source_the_campaign_does_not_hold_is_named(self):
        member = self.rebuilt()
        (self.campaign / "source.tar").unlink()
        self.assertEqual(
            self.produce(member, rustflags=REMAPPED),
            ["not qualifying: the source the manifest names, source.tar, is not in the campaign"],
        )

    def test_a_source_that_differs_is_named(self):
        member = self.rebuilt(b"the source of the binary\n")
        # The same size, other bytes: only the digest tells them apart.
        self.keep("source.tar", b"the source of the binarY\n")
        self.assertEqual(
            self.produce(member, rustflags=REMAPPED),
            [
                "not qualifying: source.tar of the campaign is not the source the manifest "
                "names: its digest or its size differs"
            ],
        )

    def test_the_oracle_that_runs_is_the_one_the_manifest_names(self):
        self.assertEqual(self.produce(), [f"not qualifying: {results.UNTIED_BINARY}"])

    def test_an_oracle_file_the_manifest_does_not_list_is_named(self):
        listed = running_oracle()
        missing = listed.pop(0)["path"]
        with mock.patch(f"{__name__}.running_oracle", return_value=listed):
            unmet = self.produce()
        self.assertIn(
            f"not qualifying: the manifest does not list the oracle file {missing}", unmet
        )

    def test_a_modified_tree_whose_content_is_kept_qualifies_on_that_count(self):
        self.keep("modified/untracked/tools/probe.txt", b"probe\n")
        # A long binary line before a diff does not hide that diff.
        self.keep(
            "modified/tracked.patch",
            b"diff --git a/assets/blob b/assets/blob\nGIT binary patch\n"
            + b"z" * (results.MAX_HEADER_BYTES * 3)
            + b"\ndiff --git a/crates/x.rs b/crates/x.rs\n--- a/crates/x.rs\n+++ b/crates/x.rs\n",
        )
        unmet = self.produce(
            tree_state="modified", modified_paths=["crates/x.rs", "tools/probe.txt"]
        )
        self.assertEqual(unmet, [f"not qualifying: {results.UNTIED_BINARY}"])

    def test_a_modified_path_the_campaign_does_not_keep_is_named(self):
        self.keep(
            "modified/tracked.patch",
            b"diff --git a/crates/x.rs b/crates/x.rs\n",
        )
        unmet = self.produce(tree_state="modified", modified_paths=["crates/x.rs", "crates/y.rs"])
        self.assertIn(
            "not qualifying: the modified path crates/y.rs is not kept in the run directory",
            unmet,
        )
        self.assertNotIn(
            "not qualifying: the modified path crates/x.rs is not kept in the run directory",
            unmet,
        )

    def test_a_build_whose_paths_are_not_remapped_is_named(self):
        self.assertEqual(
            self.produce_rebuilt(rustflags=""),
            [
                "not qualifying: a binary rebuilt from its source is built with "
                "--remap-path-prefix=<dir>=/mqttiumd-source, and these rustflags are not"
            ],
        )

    def test_an_archive_without_a_contract_file_is_named(self):
        path = "contracts/v1/assertions.json"
        self.assertEqual(
            self.produce_rebuilt(self.archive(drop=[path])),
            [f"not qualifying: source.tar does not hold {path} as the manifest lists it"],
        )

    def test_an_archive_with_another_oracle_is_named(self):
        path = f"{ORACLE}/judge.py"
        self.assertEqual(
            self.produce_rebuilt(self.archive(replace={path: b"another judge\n"})),
            [f"not qualifying: source.tar does not hold {path} as the manifest lists it"],
        )

    def test_an_untracked_file_kept_is_the_one_the_archive_holds(self):
        self.keep("modified/untracked/tools/probe.txt", b"probe\n")
        kept = self.produce_rebuilt(
            self.archive(extra={"tools/probe.txt": b"probe\n"}),
            tree_state="modified",
            modified_paths=["tools/probe.txt"],
        )
        self.assertEqual(kept, [])
        other = self.produce_rebuilt(
            self.archive(extra={"tools/probe.txt": b"other\n"}),
            tree_state="modified",
            modified_paths=["tools/probe.txt"],
        )
        self.assertEqual(
            other,
            [
                "not qualifying: source.tar does not hold the untracked tools/probe.txt "
                "the campaign keeps"
            ],
        )

    def test_an_archive_that_cannot_be_read_is_named(self):
        self.assertEqual(
            self.produce_rebuilt(b"not an archive at all\n"),
            ["not qualifying: source.tar is not an archive the oracle can read"],
        )

    def test_an_archive_of_too_many_members_is_refused_before_it_is_read_whole(self):
        with mock.patch.object(results, "MAX_SOURCE_MEMBERS", 3):
            unmet = self.produce_rebuilt()
        self.assertEqual(unmet, ["not qualifying: source.tar has more than 3 members"])


if __name__ == "__main__":
    unittest.main()
