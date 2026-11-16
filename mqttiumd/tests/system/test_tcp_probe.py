"""Discriminating tests for the independent TCP receipt machinery."""
import hashlib
import json
from pathlib import Path
import socket
import tempfile
import unittest

from tcp_probe import (
    Frame, FrameDecoder, Observations, ReceiptFailure, Wire, assert_no_positive_ack,
    assert_stale_filtered, connect_packet, publish_packet, variable_integer,
)


class ProbeTests(unittest.TestCase):
    def test_variable_integer_boundaries_and_fragmented_concatenation(self):
        for value in (0, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, 268_435_455):
            self.assertLessEqual(len(variable_integer(value)), 4)
        packets = b"\xd0\x00\x50\x02\x00\x07"
        for split in range(len(packets) + 1):
            parser = FrameDecoder(1024)
            frames = parser.feed(packets[:split]) + parser.feed(packets[split:])
            self.assertEqual(frames, [Frame(0xD0, b""), Frame(0x50, b"\x00\x07")])
            parser.finish()

    def test_declared_size_is_refused_before_any_body_arrives(self):
        with self.assertRaises(ReceiptFailure) as failure:
            FrameDecoder(32).feed(b"\x50" + variable_integer(33))
        self.assertEqual(failure.exception.code, "probe_packet_too_large")

    def test_fifth_and_noncanonical_remaining_length_are_invalid(self):
        for data in (b"\x50\x80\x80\x80\x80", b"\x50\x80\x00"):
            with self.assertRaises(ReceiptFailure) as failure:
                FrameDecoder(1024).feed(data)
            self.assertEqual(failure.exception.code, "probe_malformed_remaining_length")

    def test_clean_eof_differs_from_a_started_frame(self):
        FrameDecoder(1024).finish()
        parser = FrameDecoder(1024)
        parser.feed(b"\x50\x02\x00")
        with self.assertRaises(ReceiptFailure) as failure:
            parser.finish()
        self.assertEqual(failure.exception.code, "probe_truncated")

    def test_premature_pubrec_and_pubcomp_are_detected_for_their_reason(self):
        for header, code in ((0x50, "premature_pubrec"), (0x70, "premature_pubcomp")):
            with self.assertRaises(ReceiptFailure) as failure:
                assert_no_positive_ack([Frame(header, b"\x00\x07")], header, 7)
            self.assertEqual(failure.exception.code, code)
        assert_no_positive_ack([Frame(0x50, b"\x00\x07\x97\x00")], 0x50, 7)
        assert_no_positive_ack([], 0x50, 7)

    def test_stale_forwarding_is_detected_even_if_another_guard_later_drops_it(self):
        events = [{"kind": "stale_generation_ignored", "connection": "1"}]
        assert_stale_filtered(events, "1")
        events.append({"kind": "stale_generation_accepted", "connection": "1"})
        with self.assertRaises(ReceiptFailure) as failure:
            assert_stale_filtered(events, "1")
        self.assertEqual(failure.exception.code, "stale_generation_forwarded")
        with self.assertRaises(ReceiptFailure):
            assert_stale_filtered([], "1")

    def test_profile_packets_have_independently_specified_wire_values(self):
        self.assertEqual(
            connect_packet("probe", 30),
            bytes.fromhex("101700044d5154540500001e0511ffffffff000570726f6265"),
        )
        self.assertEqual(publish_packet("t", b"x", 7), bytes.fromhex("340700017400070078"))
        self.assertEqual(publish_packet("t", b"x", 7, dup=True)[0], 0x3C)

    def test_large_probe_bytes_keep_bounded_evidence_and_exact_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            left, right = socket.socketpair()
            try:
                wire = Wire.__new__(Wire)
                wire.socket = left
                wire.label = "large-packet"
                path = Path(directory) / "observer.jsonl"
                wire.observations = Observations(path)
                data = b"x" * 4096
                wire.send(data)
                self.assertEqual(right.recv(8192), data)
                value = json.loads(path.read_text())
                self.assertEqual(value["byte_count"], len(data))
                self.assertEqual(value["sha256"], hashlib.sha256(data).hexdigest())
                self.assertLessEqual(len(path.read_bytes()), 8192)
            finally:
                left.close();right.close()

    def test_concat_accepts_ping_responses_while_release_commit_is_pending(self):
        for completion in (
                [Frame(0x70, b"\x00\x07"), Frame(0xD0, b""), Frame(0xD0, b"")],
                [Frame(0xD0, b""), Frame(0x70, b"\x00\x07"), Frame(0xD0, b"")],
                [Frame(0xD0, b""), Frame(0xD0, b""), Frame(0x70, b"\x00\x07")]):
            wire = Wire.__new__(Wire)
            wire.frames = [Frame(0x50, b"\x00\x07")] + completion
            wire.send = lambda *args, **kwargs: None
            wire.exchange(7, concat=True)
            self.assertEqual(wire.frames, [])


if __name__ == "__main__":
    unittest.main()
