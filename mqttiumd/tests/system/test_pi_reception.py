"""Independent wire-observer invariants for the outgoing PI reception."""
import unittest

from pi_reception import WireOccurrences
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
