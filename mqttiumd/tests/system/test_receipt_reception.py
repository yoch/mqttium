"""Discriminating scheduling tests for the real receipt probe's wait path."""
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import json
import os
from pathlib import Path
import tempfile

from receipt_reception import EventTail, BUSINESS_WINDOW, stable_receipt_pressure, wait_committed_resolution, wait_control_or_pressure
from tcp_probe import Frame, ReceiptFailure


ACK = Frame(0x90, b"\x00\x01\x00\x01")
PRESSURE = {"kind": "receipt_window_pressure", "seq": "50",
            "details": {"span": BUSINESS_WINDOW, "unconsumed_holes": 1,
                        "code": "receipt_window_business_full"}}


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now


class Wire:
    def __init__(self, clock, frames):
        self.clock, self.frames = clock, list(frames)
        self.polls = []

    def poll(self, timeout):
        self.polls.append(timeout)
        self.clock.now += .4
        return self.frames.pop(0) if self.frames else None


class Broker:
    def __init__(self, events):
        self.records = events
        self.process = SimpleNamespace(poll=lambda: None)
        self.observations = SimpleNamespace(record=lambda *args, **kwargs: None)

    def events(self):
        return self.records

    def wait_event(self, kind, *, after, timeout):
        for event in self.records:
            if event["kind"] == kind and int(event["seq"]) > after:
                return event
        raise ReceiptFailure("event_not_reached", kind)


class ReceiptControlWaitTests(unittest.TestCase):
    def test_interim_unknown_resolution_does_not_hide_later_commit(self):
        operation = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "13"}
        unknown = {"kind": "store_resolved", "seq": "10", "operation": operation,
                   "details": {"outcome": "pendingorunknown"}}
        committed = {**unknown, "seq": "11", "details": {"outcome": "committed"}}
        self.assertIs(wait_committed_resolution(Broker([unknown, committed]), operation, after=1), committed)

    def test_contradictory_terminal_resolution_cannot_be_skipped(self):
        operation = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "13"}
        refused = {"kind": "store_resolved", "seq": "10", "operation": operation,
                   "details": {"outcome": "rejectedcertain"}}
        committed = {**refused, "seq": "11", "details": {"outcome": "committed"}}
        with self.assertRaises(ReceiptFailure):
            wait_committed_resolution(Broker([refused, committed]), operation, after=1)

    def test_late_suback_without_pressure_is_still_consumed(self):
        clock = Clock()
        wire = Wire(clock, [None, ACK])
        response, pressure = wait_control_or_pressure(
            Broker([]), wire, after=1, pressure_ready=lambda event: True, clock=clock)
        self.assertIs(response, ACK)
        self.assertIsNone(pressure)
        self.assertEqual(len(wire.polls), 2)

    def test_explicit_stable_pressure_ends_the_wait(self):
        clock = Clock()
        response, pressure = wait_control_or_pressure(
            Broker([PRESSURE]), Wire(clock, [None]), after=1,
            pressure_ready=lambda event: True, clock=clock)
        self.assertIsNone(response)
        self.assertIs(pressure, PRESSURE)

    def test_transient_pressure_does_not_replace_a_late_suback(self):
        clock = Clock()
        response, pressure = wait_control_or_pressure(
            Broker([PRESSURE]), Wire(clock, [None, ACK]), after=1,
            pressure_ready=lambda event: False, clock=clock)
        self.assertIs(response, ACK)
        self.assertIsNone(pressure)

    def test_durable_prefix_advance_can_make_pressure_transient(self):
        before = {"durable_floor": 0, "maximum_retained_sequence": BUSINESS_WINDOW,
                  "retained_span": BUSINESS_WINDOW, "unknown_receipt_present": True}
        after = {**before, "durable_floor": 12, "retained_span": BUSINESS_WINDOW - 12}
        full = {**after, "maximum_retained_sequence": BUSINESS_WINDOW + 12, "retained_span": BUSINESS_WINDOW}
        self.assertFalse(stable_receipt_pressure(PRESSURE, before, 13))
        self.assertFalse(stable_receipt_pressure(PRESSURE, after, 13))
        self.assertTrue(stable_receipt_pressure(PRESSURE, full, 13))

    def test_silence_without_any_witness_is_an_explicit_deadline(self):
        clock = Clock()
        with self.assertRaises(ReceiptFailure) as raised:
            wait_control_or_pressure(Broker([]), Wire(clock, []), after=1,
                                     pressure_ready=lambda event: True, clock=clock, timeout=.5)
        self.assertEqual(raised.exception.code, "control_response_deadline")


class ReceiptEventTailTests(unittest.TestCase):
    def encoded(self, *rows):
        return ''.join(json.dumps(row) + '\n' for row in rows).encode()

    def row(self, sequence, kind="receipt_collection_applied", **details):
        return {"seq": str(sequence), "pid": 7, "kind": kind, "details": details}

    def reused_inode(self, *, replacement_larger):
        fresh = self.encoded(self.row(4, "log_rotated"), self.row(5))
        old_size = len(fresh) + (0 if replacement_larger else 512)
        starting = self.row(1, "starting", padding="")
        starting["details"]["padding"] = 'x' * (old_size - len(self.encoded(starting)))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'broker.events.jsonl'
            previous = root / 'broker.events.jsonl.previous'
            retired = root / 'retired-log'
            path.write_bytes(self.encoded(starting))
            inode = path.stat().st_ino
            self.assertEqual(path.stat().st_size, old_size)
            tail = EventTail(path, 7)
            tail.read()
            os.link(path, retired)
            path.rename(previous)
            path.write_bytes(self.encoded(self.row(2, "log_rotated"), self.row(3)))
            tail.read()
            self.assertEqual(tail.last_sequence, 3)
            previous.unlink()
            path.rename(previous)
            os.link(retired, path)
            path.write_bytes(fresh + (self.encoded(self.row(6)) if replacement_larger else b''))
            self.assertEqual(path.stat().st_ino, inode, 'actual retained inode must be recycled')
            if replacement_larger:
                self.assertGreater(path.stat().st_size, old_size)
            else:
                self.assertLess(path.stat().st_size, old_size)
            tail.read()
            self.assertEqual(tail.counts['log_rotated'], 2, 'receipt_event_generation_skipped')
            self.assertEqual(tail.last_sequence, 6 if replacement_larger else 5,
                             'receipt_event_generation_skipped')
            self.assertEqual(tail.counts['receipt_collection_applied'], 3 if replacement_larger else 2)
            self.assertEqual(len(tail.offsets), 2, 'only live generations may retain cursors')
            before = dict(tail.counts)
            tail.read()
            self.assertEqual(dict(tail.counts), before, 're-reading cannot count maintenance twice')

    def test_receipt_tail_observes_shorter_generation_on_recycled_real_inode(self):
        self.reused_inode(replacement_larger=False)

    def test_receipt_tail_observes_larger_generation_on_recycled_real_inode(self):
        self.reused_inode(replacement_larger=True)

    def test_receipt_tail_binds_offset_to_file_actually_opened_after_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'broker.events.jsonl'
            previous = path.with_name(path.name + '.previous')
            path.write_bytes(self.encoded(self.row(1, "starting", padding='x' * 512)))
            tail = EventTail(path, 7)
            tail.read()
            real_open, reached = Path.open, False
            def opening(candidate, *args, **kwargs):
                nonlocal reached
                if candidate == path and not reached:
                    reached = True
                    path.rename(previous)
                    with real_open(path, 'wb') as stream:
                        stream.write(self.encoded(self.row(2, "log_rotated"), self.row(3)))
                return real_open(candidate, *args, **kwargs)
            with patch.object(Path, 'open', opening):
                tail.read()
            self.assertTrue(reached, 'the scan/open replacement must be reached')
            self.assertEqual(tail.last_sequence, 3, 'receipt_stat_open_generation_mismatch')
            self.assertEqual(tail.counts['receipt_collection_applied'], 1)
            tail.read()
            self.assertEqual(tail.counts['receipt_collection_applied'], 1)

    def disappearing_scan(self, *, after_another_file):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'broker.events.jsonl'
            previous = root / 'broker.events.jsonl.previous'
            retired = root / 'retired-log'
            path.write_bytes(self.encoded(self.row(1, 'starting')))
            if after_another_file:
                previous.write_bytes(path.read_bytes())
            tail = EventTail(path, 7)
            before = tail.read()
            offsets = dict(tail.offsets)
            with path.open('ab') as stream:
                stream.write(self.encoded(self.row(2)))
            real_open, reached = Path.open, False
            def opening(candidate, *args, **kwargs):
                nonlocal reached
                selected = previous if after_another_file else path
                if candidate == selected and not reached:
                    reached = True
                    selected.rename(retired if after_another_file else previous)
                return real_open(candidate, *args, **kwargs)
            with patch.object(Path, 'open', opening):
                try:
                    observed = tail.read()
                except FileNotFoundError as error:
                    self.fail('temporary_receipt_rotation_gap: ' + str(error))
            self.assertTrue(reached, 'the interrupted scan must be reached')
            self.assertEqual(observed, before, 'incomplete_event_scan_committed_observations')
            self.assertEqual(tail.offsets, offsets, 'incomplete_event_scan_committed_offsets')
            if after_another_file:
                retired.rename(previous)
            else:
                path.write_bytes(self.encoded(self.row(3, 'log_rotated')))
            tail.read()
            self.assertEqual(tail.last_sequence, 2 if after_another_file else 3)
            self.assertEqual(tail.counts['receipt_collection_applied'], 1)
            tail.read()
            self.assertEqual(tail.counts['receipt_collection_applied'], 1)

    def test_receipt_tail_retries_a_real_rotation_gap_without_advancing_state(self):
        self.disappearing_scan(after_another_file=False)

    def test_receipt_tail_rolls_back_offsets_of_already_buffered_files_after_gap(self):
        self.disappearing_scan(after_another_file=True)

    def test_receipt_tail_refuses_an_unobserved_sequence_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'broker.events.jsonl'
            path.write_bytes(self.encoded(self.row(1, 'starting')))
            tail = EventTail(path, 7)
            tail.read()
            with path.open('ab') as stream:
                stream.write(self.encoded(self.row(3)))
            with self.assertRaises(ReceiptFailure) as raised:
                tail.read()
            self.assertEqual(raised.exception.code, 'receipt_event_observation_gap')
            self.assertEqual(tail.last_sequence, 1)
            self.assertEqual(tail.counts['receipt_collection_applied'], 0)


if __name__ == "__main__":
    unittest.main()
