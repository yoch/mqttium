"""Independent packet controls used by the real wildcard boundary reception."""
from __future__ import annotations

import unittest

from tcp_probe import FrameDecoder
from wildcard_boundaries import subscribe_options, receipt_row
from tcp_probe import ReceiptFailure


class SubscriptionEncodingTests(unittest.TestCase):
    def canonical(self, *, wrapped=True, bad_identity=False):
        operation={"incarnation":"1","epoch":"2","owner":"0","sequence":"3"}
        identity=b"".join(int(operation[k]).to_bytes(8,"big") for k in ("incarnation","epoch","owner","sequence"))
        inner=b"MQDR\x00\x01\x09"+identity+(1).to_bytes(8,"big")*3+b"\x01\x02\x1c\x00\x03a/#"
        if bad_identity: inner=inner[:7]+bytes(32)+inner[39:]
        raw=(b"MQDR\x00\x01\x0c"+len(inner).to_bytes(4,"big")+inner+identity) if wrapped else inner
        return {"tables":{"operations":[{"id":identity.hex(),"recipe":raw.hex()}]}},{"operation":operation}

    def test_versioned_replay_envelope_preserves_options_and_operation_correlation(self):
        state,event=self.canonical()
        observed=receipt_row(state,event)
        self.assertEqual(observed["canonical_kind"],12,"subscription_replay_envelope_not_read")
        self.assertEqual(observed["inner_kind"],9)

    def test_legacy_options_encoding_remains_readable_and_wrong_inner_identity_is_refused(self):
        state,event=self.canonical(wrapped=False)
        self.assertEqual(receipt_row(state,event)["canonical_kind"],9)
        state,event=self.canonical(bad_identity=True)
        with self.assertRaises(ReceiptFailure): receipt_row(state,event)

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
