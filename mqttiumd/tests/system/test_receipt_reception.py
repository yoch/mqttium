"""Discriminating scheduling tests for the real receipt probe's wait path."""
from types import SimpleNamespace
import unittest

from receipt_reception import wait_control_or_pressure
from tcp_probe import Frame, ReceiptFailure


ACK = Frame(0x90, b"\x00\x01\x00\x01")
PRESSURE = {"kind": "receipt_window_pressure", "seq": "50",
            "details": {"span": 768, "unconsumed_holes": 1,
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


if __name__ == "__main__":
    unittest.main()
