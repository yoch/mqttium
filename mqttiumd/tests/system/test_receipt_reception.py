"""Discriminating scheduling tests for the real receipt probe's wait path."""
from types import SimpleNamespace
import unittest

from receipt_reception import BUSINESS_WINDOW, stable_receipt_pressure, wait_committed_resolution, wait_control_or_pressure
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


if __name__ == "__main__":
    unittest.main()
