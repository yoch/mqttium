import json
import unittest

from pubsub_probe import (
    Frame, ReceiptFailure, decode_filter_ack, decode_publish, packet,
    puback_packet, subscribe_packet, unsubscribe_packet,
)


class PubsubProbeTests(unittest.TestCase):
    def test_requests_use_the_senders_packet_identifier_space(self):
        self.assertEqual(subscribe_packet(7, [("a/b", 1)]).hex(), "82090007000003612f6201")
        self.assertEqual(unsubscribe_packet(7, ["a/b"]).hex(), "a2080007000003612f62")
        self.assertEqual(puback_packet(7).hex(), "40020007")
        self.assertEqual(puback_packet(7, 0x97).hex(), "4003000797")

    def test_filter_ack_preserves_request_order_and_negative_reasons(self):
        self.assertEqual(decode_filter_ack(Frame(0x90, bytes.fromhex("000700018001"))),
                         (7, (1, 0x80, 1)))
        self.assertEqual(decode_filter_ack(Frame(0xB0, bytes.fromhex("0007000011"))),
                         (7, (0, 0x11)))

    def test_publish_decodes_independent_topic_payload_identifier_and_dup(self):
        message = decode_publish(Frame(0x3A, bytes.fromhex("0003612f62000900") + b"payload"))
        self.assertEqual((message.topic, message.payload, message.packet_id, message.qos,
                          message.dup, message.retain), ("a/b", b"payload", 9, 1, True, False))

    def test_qos0_has_no_packet_identifier_or_duplicate_flag(self):
        message = decode_publish(Frame(0x30, bytes.fromhex("0003612f6200") + b"payload"))
        self.assertEqual((message.topic, message.payload, message.packet_id, message.qos,
                          message.dup, message.retain), ("a/b", b"payload", None, 0, False, False))
        self.assertIsNone(message.observation()['packet_id'])
        for frame in [Frame(0x38, b"\x00\x01a\x00data"),
                      Frame(0x30, b"\x00\x01a\x80\x00data")]:
            with self.assertRaises(ReceiptFailure):
                decode_publish(frame)

    def test_invalid_or_unbounded_outgoing_packets_are_refused(self):
        for frame in [Frame(0x34, b"\x00\x01a\x00\x01\x00"),
                      Frame(0x32, b"\x00\x01a\x00\x00\x00"),
                      Frame(0x32, b"\x00\x01a\x00\x01\x80\x00"),
                      Frame(0x32, b"\x00\x01a\x00\x01\x00" + b"x" * 32769)]:
            with self.subTest(frame=frame.header, size=len(frame.body)):
                with self.assertRaises(ReceiptFailure):
                    decode_publish(frame)
        with self.assertRaises(ValueError):
            puback_packet(1, 0x92)
        with self.assertRaises(ValueError):
            subscribe_packet(1, [("a", 1)] * 9)

    def test_maximum_profile_payload_has_a_bounded_observation(self):
        message = decode_publish(Frame(0x32, b"\x00\x01a\x00\x01\x00" + b"p" * 32768))
        self.assertLess(len(json.dumps(message.observation()).encode()), 8192,
                        "a valid maximum payload must not overflow the independent observer")


if __name__ == "__main__":
    unittest.main()
