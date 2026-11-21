"""A broker journal may rotate between the supervisor's open and read."""
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "preview" / "pilot"))

from site_supervisor import Child, Supervisor


class JournalRotationTests(unittest.TestCase):
    def test_missing_active_path_during_rotation_does_not_kill_supervisor(self):
        with TemporaryDirectory() as temp:
            current = Path(temp) / "events.jsonl"
            prior = current.with_suffix(".jsonl.previous")
            def record(n):
                return json.dumps({"seq": str(n), "kind": "tls_authenticated"}) + "\n"

            current.write_text(record(1))
            supervisor = object.__new__(Supervisor)
            actor = Child("broker-live", events=current)
            supervisor.broker_events(actor)
            self.assertEqual(actor.seq, 1)
            with current.open("a") as output:
                output.write(record(2))

            open_file = Path.open
            def rotating(path, *args, **kwargs):
                if path == current and current.exists():
                    current.rename(prior)
                return open_file(path, *args, **kwargs)

            with patch.object(Path, "open", rotating):
                try:
                    supervisor.broker_events(actor)
                except FileNotFoundError:
                    self.fail("transient_journal_rotation_stopped_supervisor")
            self.assertEqual(actor.seq, 1)
            current.write_text(record(3))
            supervisor.broker_events(actor)
            self.assertEqual(actor.seq, 3)
            self.assertEqual(actor.security["tls_authenticated"], 3)
            self.assertEqual(actor.event_gaps, 0)


if __name__ == "__main__":
    unittest.main()
