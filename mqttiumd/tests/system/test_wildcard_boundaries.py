"""Independent packet controls used by the real wildcard boundary reception."""
from __future__ import annotations

import unittest

from tcp_probe import FrameDecoder
from wildcard_boundaries import subscribe_options


class SubscriptionEncodingTests(unittest.TestCase):
    def test_preserves_no_local_rap_and_retention_handling_separately_from_qos(self):
        encoded = subscribe_options(17, [("a/+/#", 0, 0x2C), ("b/#", 2, 4)])
        frames = FrameDecoder().feed(encoded)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].header, 0x82)
        self.assertEqual(frames[0].body,
                         b"\x00\x11\x00\x00\x05a/+/#\x2c\x00\x03b/#\x06")

    def test_refuses_reserved_options_and_qos_bits_in_the_options_argument(self):
        for invalid in (1, 2, 3, 0x30, 0x40, 0x80, 0x100, -1):
            with self.subTest(options=invalid), self.assertRaises(ValueError):
                subscribe_options(1, [("a/#", 1, invalid)])

    def test_bounds_identifier_filter_count_and_field_bytes(self):
        for identifier, filters in ((0, [("a", 1, 0)]),
                                    (65536, [("a", 1, 0)]),
                                    (1, []), (1, [("a", 1, 0)] * 9),
                                    (1, [("a" * 1025, 1, 0)]),
                                    (1, [("a", 3, 0)])):
            with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                subscribe_options(identifier, filters)


if __name__ == "__main__":
    unittest.main()
