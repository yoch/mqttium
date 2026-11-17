"""Independent canonical receipt compatibility; these fixtures are not run evidence."""
from __future__ import annotations

import unittest

from retained_boundaries import canonical_receipt
from tcp_probe import ReceiptFailure


def number(value, width=8):
    return value.to_bytes(width, "big")


def scoped(sequence):
    return b"".join(number(value) for value in (1, 2, 0, sequence))


def header(kind):
    return b"MQDR\x00\x01" + bytes([kind])


class CanonicalRetainedReceiptTests(unittest.TestCase):
    def receipt(self, *, timed=False, options=False):
        identity = scoped(3)
        operation = dict(zip(("incarnation", "epoch", "owner", "sequence"), ("1", "2", "0", "3")))
        inner = (header(9 if options else 7) + identity + number(1) * 3 + b"\x01\x02"
                 + (b"\x1c" if options else b"") + b"\x00\x03a/#")
        raw = header(12) + number(len(inner), 4) + inner + scoped(4)
        if timed:
            raw = header(14) + number(len(raw), 4) + raw + b"\xab" * 32 + number(100)
        return {"tables": {"operations": [{"id": identity.hex(), "recipe": raw.hex()}]}}, {"operation": operation}

    def test_timed_and_legacy_replay_preserve_original_operation_and_semantic_tag(self):
        for options in (False, True):
            for timed in (True, False):
                state, event = self.receipt(timed=timed, options=options)
                result = canonical_receipt(state, event, 12)
                self.assertEqual(result["tag"], 14 if timed else 12)
                self.assertEqual(result["semantic_tag"], 12)
                self.assertEqual(result["operation"], event["operation"])

    def test_wrong_operation_and_every_incomplete_or_trailing_encoding_are_refused(self):
        for timed in (False, True):
            state, event = self.receipt(timed=timed)
            raw = bytes.fromhex(state["tables"]["operations"][0]["recipe"])
            offset = 29 if timed else 18
            wrong = raw[:offset] + scoped(99) + raw[offset + 32:]
            invalid = [wrong, raw + b"\x00", raw[:7] + number(65537, 4) + raw[11:]]
            invalid.extend(raw[:cut] for cut in range(len(raw)))
            for value in invalid:
                state["tables"]["operations"][0]["recipe"] = value.hex()
                with self.assertRaises(ReceiptFailure): canonical_receipt(state, event, 12)

    def test_timed_wrapper_has_only_one_legacy_replay_child(self):
        state, event = self.receipt(timed=True)
        raw = bytes.fromhex(state["tables"]["operations"][0]["recipe"])
        for tag in (1, 7, 9, 10, 11, 13, 14, 15, 16):
            state["tables"]["operations"][0]["recipe"] = (raw[:17] + bytes([tag]) + raw[18:]).hex()
            with self.assertRaises(ReceiptFailure): canonical_receipt(state, event, 12)


if __name__ == "__main__":
    unittest.main()
