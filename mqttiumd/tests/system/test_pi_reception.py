"""Independent wire-observer invariants for the outgoing PI reception."""
import unittest
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import os
import shutil
import subprocess
import sys
import time

from pi_reception import FinishCursor, WireOccurrences, verify_normal_bounds, verify_small_domain
from tcp_probe import Frame, FrameDecoder, ReceiptFailure, packet, utf8


class Sink:
    def __init__(self):
        self.rows = []

    def record(self, kind, **fields):
        self.rows.append((kind, fields))


def publish(number, identifier=1):
    return Frame(0x32, utf8("reception/pi-main") + identifier.to_bytes(2, "big")
                 + b"\x00pi-main:" + f"{number:08d}".encode())


class PiWireTests(unittest.TestCase):
    def test_zero_filtered_tests_cannot_receive_the_small_identifier_domain(self):
        with self.assertRaises(ReceiptFailure) as raised:
            verify_small_domain('test result: ok. 0 passed; 0 failed; 1 filtered out;')
        self.assertEqual(raised.exception.code, 'small_domain_not_executed')
        verify_small_domain('test owner::pubsub::tests::the_complete_small_domain_counts_unknown_finish_and_unloaded_recovered_claims ... ok\n'
                            'test result: ok. 1 passed; 0 failed; 200 filtered out;')

    def test_wrapper_build_uses_pinned_source_when_caller_commits_during_build(self):
        tool = Path(__file__).resolve().parents[2] / 'tools/run_pi_reception.sh'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); repository = root / 'repository'; control = root / 'control'
            controls = root / 'bin'; controls.mkdir(); control.mkdir()
            product = repository / 'mqttiumd'
            (product / 'tools').mkdir(parents=True)
            (product / 'tests/system').mkdir(parents=True)
            (product / 'tests/system/__init__.py').write_text('')
            (product / 'tests/system/test_stub.py').write_text(
                'import unittest\nclass Stub(unittest.TestCase):\n def test_preflight(self): self.assertTrue(True)\n')
            target_tool = product / 'tools/run_pi_reception.sh'
            shutil.copyfile(tool, target_tool); target_tool.chmod(0o755)
            (product / 'sentinel').write_text('before')
            for command in (['git', 'init', '-q', str(repository)],
                            ['git', '-C', str(repository), 'config', 'user.email', 'probe@example.invalid'],
                            ['git', '-C', str(repository), 'config', 'user.name', 'Wrapper probe'],
                            ['git', '-C', str(repository), 'add', '.'],
                            ['git', '-C', str(repository), '-c', 'commit.gpgsign=false', 'commit', '-qm', 'before']):
                subprocess.run(command, check=True, capture_output=True)
            cargo = controls / 'cargo'
            cargo.write_text('''#!/usr/bin/env python3
from pathlib import Path
import json, os, sys, time
control=Path(os.environ['PI_WRAPPER_TEST_CONTROL'])
if sys.argv[1]=='build':
 (control/'ready').touch()
 deadline=time.monotonic()+10
 while not (control/'release').exists():
  if time.monotonic()>deadline: raise SystemExit(72)
  time.sleep(.01)
 (control/'observed.json').write_text(json.dumps({'cwd':str(Path.cwd()),'sentinel':Path('sentinel').read_text()}))
 raise SystemExit(71) # deliberate stop after observing the build source; no broker is fabricated
print('mqttiumd features=' if sys.argv[1]=='tree' else 'cargo-test-stub')
''')
            cargo.chmod(0o755)
            rustc = controls / 'rustc'; rustc.write_text('#!/bin/sh\necho rustc-test-stub\n'); rustc.chmod(0o755)
            inherited = {key: value for key, value in os.environ.items()
                         if not key.startswith('MQTTIUMD_PI_PIN_') and key != 'MQTTIUMD_PI_CALLER_SOURCE'}
            environment = dict(inherited, PATH=str(controls) + os.pathsep + os.environ['PATH'],
                               PYTHON=sys.executable, PI_WRAPPER_TEST_CONTROL=str(control),
                               MQTTIUMD_PI_TARGET_ROOT=str(root / 'targets'))
            process = subprocess.Popen([str(target_tool), str(root / 'out')], cwd=product,
                                       env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 10
                while not (control / 'ready').exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(.01)
                error = process.stderr.read().decode() if process.poll() is not None else ''
                if process.poll() is not None:
                    error += ''.join(path.name + ': ' + path.read_text()[-1024:]
                                     for path in (root / 'out').glob('*.log'))
                self.assertTrue((control / 'ready').exists(), 'Cargo pause was not reached: ' + error[-1024:])
                (product / 'sentinel').write_text('after')
                subprocess.run(['git', '-C', str(repository), 'add', '.'], check=True, capture_output=True)
                subprocess.run(['git', '-C', str(repository), '-c', 'commit.gpgsign=false', 'commit', '-qm', 'after'],
                               check=True, capture_output=True)
            finally:
                (control / 'release').touch()
                stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 71, (stdout + stderr).decode()[-512:])
            observed = json.loads((control / 'observed.json').read_text())
            self.assertEqual(observed['sentinel'], 'before', 'build read the moving caller checkout')
            self.assertNotEqual(Path(observed['cwd']), product)

    def long_evidence(self, floors=(64, 128, 192, 256), limit=8192):
        def snapshot(floor):
            return {"metadata": {"limit_receipt_slots": limit.to_bytes(8, "big").hex()},
                    "counts": {"operations": 3, "fences": 0},
                    "retirement": {"receipt_origins": [{"consumed_through": floor.to_bytes(8, "big").hex()}]}}
        return ({"windows": [{"storage": snapshot(floor)} for floor in floors],
                 "event_counts": {"receipt_collection_applied": len(floors) + 1}}, snapshot(floors[-1]))

    def test_receipt_registration_alone_does_not_prove_active_collection(self):
        evidence, final = self.long_evidence((0, 0, 0, 0))
        with self.assertRaises(ReceiptFailure) as raised:
            verify_normal_bounds(evidence, final)
        self.assertEqual(raised.exception.code, 'receipt_collection_not_observed')

    def test_changed_receipt_quota_cannot_be_hidden_in_long_summary(self):
        evidence, final = self.long_evidence(limit=16384)
        with self.assertRaises(ReceiptFailure) as raised:
            verify_normal_bounds(evidence, final)
        self.assertEqual(raised.exception.code, 'receipt_limit_changed')

    def test_multiple_real_floor_advances_and_unchanged_quota_are_received(self):
        evidence, final = self.long_evidence()
        verify_normal_bounds(evidence, final)

    def test_product_cursor_starts_at_the_real_sequence_one(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.events.jsonl"
            path.write_text(json.dumps({"seq": "1", "pid": 7, "kind": "starting"}) + "\n")
            cursor = FinishCursor(SimpleNamespace(events_path=path, process=SimpleNamespace(pid=7)))
            cursor.read()
            self.assertEqual(cursor.last_sequence, 1)

    def test_finish_duplicates_do_not_inflate_distinct_delivery_count(self):
        operation = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "4"}
        queued = {"seq": "1", "pid": 7, "kind": "frame_queued", "connection": "1",
                  "details": {"packet_kind": "PUBLISH", "packet_id": 1, "dup": False,
                              "claim": "1/1/0/2", "delivery": "1/1/0/1/0", "effect": "1/0/3"}}
        finished = {"seq": "2", "pid": 7, "kind": "core_finish_observed", "operation": operation,
                    "details": {"delivery": "1/1/0/1/0"}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.events.jsonl"
            path.write_text(''.join(json.dumps(row) + '\n' for row in
                                    (queued, finished, {**finished, "seq": "3"})))
            cursor = FinishCursor(SimpleNamespace(events_path=path, process=SimpleNamespace(pid=7)))
            cursor.read()
            self.assertEqual(cursor.finishes, 1)
            self.assertEqual(cursor.duplicate_finishes, 1)

    def test_old_finish_identity_cannot_be_rebound_to_new_occupant(self):
        operation = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "4"}
        first = {"seq": "1", "pid": 7, "kind": "frame_queued", "connection": "1",
                 "details": {"packet_kind": "PUBLISH", "packet_id": 1, "dup": False,
                             "claim": "1/1/0/2", "delivery": "1/1/0/1/0", "effect": "1/0/3"}}
        done = {"seq": "2", "pid": 7, "kind": "core_finish_observed", "operation": operation,
                "details": {"delivery": "1/1/0/1/0"}}
        newer = {**first, "seq": "3", "details": {**first['details'], "claim": "1/1/0/5",
                                                    "delivery": "1/1/0/2/0", "effect": "1/0/6"}}
        rebound = {**done, "seq": "4", "details": {"delivery": "1/1/0/2/0"}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.events.jsonl"
            path.write_text(''.join(json.dumps(row) + '\n' for row in (first, done, newer, rebound)))
            cursor = FinishCursor(SimpleNamespace(events_path=path, process=SimpleNamespace(pid=7)))
            with self.assertRaises(ReceiptFailure) as raised:
                cursor.read()
            self.assertEqual(raised.exception.code, 'old_finish_rebound_to_new_delivery')

    def test_fragmented_and_concatenated_packets_keep_exact_bytes(self):
        frames = (publish(1), Frame(0x40, b"\x00\x01"))
        wire = b"".join(packet(frame.header, frame.body) for frame in frames)
        for stride in (1, 2, 7, len(wire)):
            decoder = FrameDecoder(65536)
            found = []
            for offset in range(0, len(wire), stride):
                found.extend(decoder.feed(wire[offset:offset + stride]))
            decoder.finish()
            self.assertEqual(b"".join(packet(frame.header, frame.body) for frame in found), wire)

    def test_same_wire_identifier_is_a_new_number_after_ack(self):
        state = WireOccurrences(b"pi-main:", 2, Sink())
        state.publish(publish(1))
        self.assertEqual(state.acknowledge(Frame(0x40, b"\x00\x01")), 1)
        state.forwarded(1)
        self.assertEqual(state.take(1)["number"], 1)
        state.publish(publish(2))
        self.assertEqual(state.reuses, 1)
        self.assertEqual(state.unique_identifiers, 1)

    def test_live_identifier_cannot_be_rebound_by_the_observer(self):
        state = WireOccurrences(b"pi-main:", 2, Sink())
        state.publish(publish(1))
        with self.assertRaises(ReceiptFailure) as raised:
            state.publish(publish(2))
        self.assertEqual(raised.exception.code, "wire_identifier_reused_while_unacknowledged")

    def test_ack_submission_is_not_forwarding(self):
        state = WireOccurrences(b"pi-main:", 1, Sink())
        state.publish(publish(1))
        state.acknowledge(Frame(0x40, b"\x00\x01"))
        self.assertIsNone(state.take(1))
        state.forwarded(1)
        self.assertTrue(state.take(1)["puback_forwarded"])

    def test_content_number_cannot_repeat_under_a_new_identifier(self):
        state = WireOccurrences(b"pi-main:", 2, Sink())
        state.publish(publish(1))
        state.acknowledge(Frame(0x40, b"\x00\x01"))
        state.forwarded(1)
        state.take(1)
        with self.assertRaises(ReceiptFailure) as raised:
            state.publish(publish(1, 2))
        self.assertEqual(raised.exception.code, "recipient_sequence_repeated_or_missing")


if __name__ == "__main__":
    unittest.main()
