"""Exercise the disconnect reception's reader with a growing broker journal."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from recovery import Broker


class LiveJournalTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "events.jsonl"
        self.broker = Broker.__new__(Broker)
        self.broker.events_path = self.path
        self.broker.process = SimpleNamespace(pid=7)

    def test_event_completed_after_eof_is_read_whole_at_the_next_poll(self):
        self.path.write_bytes(b'{"pid":7,"seq":1}\n{"pid":7,"seq":2')
        with self.path.open("ab", buffering=0) as writer:
            class GrowingFile(io.FileIO):
                appended = False

                def readinto(self, buffer):
                    size = super().readinto(buffer)
                    if size == 0 and not self.appended:
                        # EOF ends a partial readline. The very next read on the
                        # same file descriptor will see the appended suffix.
                        writer.write(b'}\n{"pid":7,"seq":3}\n')
                        self.appended = True
                    return size

            with io.BufferedReader(GrowingFile(self.path)) as reader:
                with patch.object(Path, "open", return_value=reader):
                    self.assertEqual([e["seq"] for e in self.broker.events()], [1])
        self.assertEqual([e["seq"] for e in self.broker.events()], [1, 2, 3])

    def test_complete_invalid_records_are_rejected(self):
        for line in [b'\n', b'not-json\n', b'{"pid":}\n']:
            with self.subTest(line=line):
                self.path.write_bytes(b'{"pid":7}\n' + line + b'{"pid":7}\n')
                with self.assertRaises(json.JSONDecodeError):
                    self.broker.events()

    def test_a_complete_event_from_another_process_is_rejected(self):
        self.path.write_bytes(b'{"pid":8}\n')
        with self.assertRaises(AssertionError):
            self.broker.events()
