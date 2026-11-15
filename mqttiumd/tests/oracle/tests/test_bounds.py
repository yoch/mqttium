"""What the oracle reads is bounded before it is allocated (rule 9 of AGENTS.md).

A trace has no size bound as a whole, but a line of it does, and a contract
file does: neither may be read into memory before its size is known to be
within bounds.
"""

import io
import shutil
import tempfile
import unittest
from unittest import mock

from mqttiumd_oracle import documents, judge, trace
from mqttiumd_oracle.documents import Contracts, ContractsUnreadable

from .support import ROOT


class Counting(io.RawIOBase):
    """A stream of ``size`` bytes without a line break, that counts what is read."""

    def __init__(self, size):
        self.left = size
        self.consumed = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        count = min(len(buffer), self.left)
        buffer[:count] = b"x" * count
        self.left -= count
        self.consumed += count
        return count


class Bounds(unittest.TestCase):
    LIMIT = 1024

    def test_a_long_line_is_refused_before_it_is_read_whole(self):
        stream = Counting(64 * self.LIMIT)
        with mock.patch.object(trace, "MAX_LINE_BYTES", self.LIMIT):
            found = trace.read(Contracts.open(ROOT), trace.lines_of(stream))
        self.assertTrue(found.unreadable, found)
        self.assertLessEqual(stream.consumed, 2 * (self.LIMIT + 1))

    def test_the_judge_reads_a_trace_by_bounded_lines(self):
        contracts = Contracts.open(ROOT)
        seen = []
        original = trace.lines_of

        def spy(stream, *arguments):
            seen.append(stream)
            return original(stream, *arguments)

        with mock.patch.object(judge, "lines_of", side_effect=spy):
            judge.Run.load(
                contracts,
                "tests/oracle/traces/scenarios/p1-02.json",
                "tests/oracle/traces/p1-02-good/trace.jsonl",
            )
        self.assertEqual(len(seen), 1)

    def test_a_contract_file_too_large_is_refused_before_it_is_read(self):
        with tempfile.TemporaryDirectory() as directory:
            shutil.copytree(ROOT / "contracts", f"{directory}/contracts")
            largest = max(
                path.stat().st_size
                for folder in ("contracts/v1", "contracts/schema/v1")
                for path in (ROOT / folder).glob("*.json")
            )
            target = f"{directory}/contracts/v1/profiles.json"
            with open(target, "ab") as stream:
                stream.write(b" " * (largest + 1))
            reads = []
            original = documents.pathlib.Path.read_bytes

            def counting(path):
                reads.append(path.name)
                return original(path)

            with (
                mock.patch.object(documents, "MAX_DOCUMENT_BYTES", largest),
                mock.patch.object(documents.pathlib.Path, "read_bytes", counting),
                self.assertRaises(ContractsUnreadable),
            ):
                Contracts.open(directory)
            self.assertNotIn("profiles.json", reads)


if __name__ == "__main__":
    unittest.main()
