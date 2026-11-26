"""Tests of ``check_models.py``. Standard library only.

Run from ``mqttiumd/``::

    python -m unittest discover -s models -p "test_*.py"

Each test copies the real ``models/`` tree, breaks one thing and expects
the script to name it. The unbroken copy must stay consistent.
"""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import check_models  # noqa: E402


def run_on(change: Callable[[Path], None]) -> tuple[int, str]:
    """Run the script on a changed copy; return its exit code and its defects."""
    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch) / "models"
        shutil.copytree(HERE, root, ignore=shutil.ignore_patterns("__pycache__"))
        change(root)
        defects = io.StringIO()
        with contextlib.redirect_stderr(defects), contextlib.redirect_stdout(io.StringIO()):
            code = check_models.main(root)
        return code, defects.getvalue()


def edit(name: str, old: str, new: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        path = root / "m1" / name
        text = path.read_text(encoding="utf-8")
        assert old in text, (name, old)
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    return change


def append(name: str, text: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        path = root / "m1" / name
        path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")

    return change


def sidecar(name: str, change_data: Callable[[dict], None]) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        path = root / "m1" / name
        data = json.loads(path.read_text(encoding="utf-8"))
        change_data(data)
        path.write_text(json.dumps(data), encoding="utf-8")

    return change


def entry(data: dict, config: str) -> dict:
    return next(item for item in data["configs"] if item["config"] == config)


class CheckModels(unittest.TestCase):
    def refused(self, change: Callable[[Path], None], reason: str) -> None:
        code, defects = run_on(change)
        self.assertEqual(code, 1, defects)
        self.assertIn(reason, defects)

    def test_the_tree_as_it_is_is_consistent(self) -> None:
        self.assertEqual(run_on(lambda root: None), (0, ""))

    # One temporal property per configuration, however it is spelled: TLC
    # does not say which property a temporal violation belongs to.
    def test_a_second_property_under_the_plural_keyword_is_seen(self) -> None:
        self.refused(
            append("M1a-live-path-no-rearm.cfg", "PROPERTIES RecoveryCompletes\n"),
            "M1a/M1a-live-path-no-rearm.cfg: PROPERTY ['LivePathResolves', 'RecoveryCompletes']",
        )

    def test_a_second_property_after_the_same_keyword_is_seen(self) -> None:
        self.refused(
            edit(
                "M1a-live-path.cfg",
                "PROPERTY LivePathResolves",
                "PROPERTY LivePathResolves\n    RecoveryCompletes",
            ),
            "M1a/M1a-live-path.cfg: PROPERTY ['LivePathResolves', 'RecoveryCompletes']",
        )

    def test_a_property_in_a_configuration_that_declares_none_is_seen(self) -> None:
        self.refused(
            append("M1a-safety.cfg", "PROPERTIES\n    LivePathResolves\n"),
            "M1a/M1a-safety.cfg: PROPERTY ['LivePathResolves']",
        )

    def test_plural_and_multi_line_invariants_are_read(self) -> None:
        change = edit(
            "M1a-mut-recipe-change.cfg",
            "INVARIANT TypeOK\nINVARIANT OneRecipePerIdentity",
            "INVARIANTS\n    TypeOK\n    OneRecipePerIdentity",
        )
        self.assertEqual(run_on(change), (0, ""))

    # A directory is checked as soon as it holds a model file.
    def test_a_directory_with_model_files_and_no_sidecar_is_reported(self) -> None:
        def change(root: Path) -> None:
            (root / "m2").mkdir()
            (root / "m2" / "M2.tla").write_text("---- MODULE M2 ----\n====\n", encoding="utf-8")
            (root / "m2" / "M2.cfg").write_text("SPECIFICATION Spec\n", encoding="utf-8")

        code, defects = run_on(change)
        self.assertEqual(code, 1)
        self.assertIn("m2/M2.tla: belongs to no sidecar", defects)
        self.assertIn("m2/M2.cfg: belongs to no sidecar", defects)

    def test_a_directory_without_model_files_is_not_a_model_directory(self) -> None:
        def change(root: Path) -> None:
            (root / "notes").mkdir()
            (root / "notes" / "README.md").write_text("Notes.\n", encoding="utf-8")

        self.assertEqual(run_on(change), (0, ""))

    # A passing configuration states what it checks.
    def test_a_passing_configuration_that_lost_its_invariants_is_seen(self) -> None:
        def change(root: Path) -> None:
            path = root / "m1" / "M1a-safety.cfg"
            kept = [
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if not line.startswith("INVARIANT")
            ]
            path.write_text("\n".join(kept) + "\n", encoding="utf-8")

        self.refused(change, "M1a/M1a-safety.cfg: INVARIANT [] differs from the sidecar's")

    def test_a_passing_configuration_that_lost_one_invariant_is_seen(self) -> None:
        self.refused(
            edit("M1b-safety-epoch.cfg", "INVARIANT NoStaleTarget\n", ""),
            "M1b/M1b-safety-epoch.cfg: INVARIANT",
        )

    def test_a_passing_entry_that_states_nothing_is_refused(self) -> None:
        def change_data(data: dict) -> None:
            del entry(data, "M1a-safety.cfg")["invariants"]

        self.refused(
            sidecar("M1a.json", change_data),
            "M1a/M1a-safety.cfg: a passing configuration states its invariants or its property",
        )

    def test_an_entry_that_lists_an_invariant_the_configuration_lacks_is_seen(self) -> None:
        def change_data(data: dict) -> None:
            entry(data, "M1a-safety.cfg")["invariants"].append("NoSuspension")

        self.refused(sidecar("M1a.json", change_data), "M1a/M1a-safety.cfg: INVARIANT")

    # What the runner refuses, this script refuses too, with the runner's words.
    def test_what_the_runner_refuses_is_refused(self) -> None:
        def change_data(data: dict) -> None:
            entry(data, "M1a-safety.cfg")["expect"] = "maybe"

        self.refused(sidecar("M1a.json", change_data), "unknown expectation 'maybe'")

    # A replay names a test function that exists, in a file of 'tests'.
    def test_a_replay_naming_a_missing_test_function_is_refused(self) -> None:
        def change_data(data: dict) -> None:
            data["replays"][0]["test"] = "a_test_that_does_not_exist"

        self.refused(
            sidecar("M1Answers.json", change_data),
            "M1Answers: replay a_test_that_does_not_exist is not a test of "
            "mqttiumd/crates/mqttiumd-core/src/owner.rs",
        )

    def test_a_replay_in_a_file_outside_tests_is_refused(self) -> None:
        def change_data(data: dict) -> None:
            data["replays"][0]["file"] = "mqttiumd/crates/mqttiumd-core/src/lib.rs"

        self.refused(
            sidecar("M1Answers.json", change_data),
            "M1Answers: replay file mqttiumd/crates/mqttiumd-core/src/lib.rs is not in 'tests'",
        )

    def test_a_refinement_model_names_its_replays(self) -> None:
        def change_data(data: dict) -> None:
            del data["replays"]

        self.refused(
            sidecar("M1Answers.json", change_data),
            "M1Answers: a refinement model names the test functions that replay it",
        )

    def test_a_readme_that_names_code_which_does_not_exist_is_refused(self) -> None:
        self.refused(
            append("README.md", "\nSee `Owner::no_such_member_anywhere`.\n"),
            "m1/README.md: names Owner::no_such_member_anywhere; "
            "nothing under crates/ declares no_such_member_anywhere",
        )
        self.refused(
            append("README.md", "\nSee `NoSuchTypeAnywhere::step`.\n"),
            "m1/README.md: names NoSuchTypeAnywhere::step; no type NoSuchTypeAnywhere under crates/",
        )

    def test_a_readme_may_name_real_code_and_types_of_other_crates(self) -> None:
        code, defects = run_on(
            append("README.md", "\n`Owner::step`, `Shutdown::Both`, and prose Not::code.\n")
        )
        self.assertEqual(code, 0, defects)

    def test_a_sidecar_that_is_not_an_object_is_refused(self) -> None:
        def change(root: Path) -> None:
            (root / "m1" / "Stray.json").write_text("[]", encoding="utf-8")

        self.refused(change, "Stray.json: a sidecar is an object")


if __name__ == "__main__":
    unittest.main()
