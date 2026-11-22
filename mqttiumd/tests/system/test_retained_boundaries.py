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

    def test_replay_range_cannot_name_a_foreign_scope_or_overflow(self):
        for first in (scoped(0), number(9) + scoped(4)[8:], scoped(2**64 - 255)):
            state, event = self.receipt(timed=True)
            raw = bytes.fromhex(state["tables"]["operations"][0]["recipe"])
            state["tables"]["operations"][0]["recipe"] = (raw[:-72] + first + raw[-40:]).hex()
            with self.assertRaises(ReceiptFailure): canonical_receipt(state, event, 12)

    def test_retained_admission_tags10_and11_keep_their_original_layout(self):
        operation = dict(zip(("incarnation", "epoch", "owner", "sequence"), ("1", "2", "0", "3")))
        target = number(2) + number(1) + number(0, 4) + b"\x01\x01"
        payload = scoped(6) + number(1) + number(1) + number(2) + number(0) + number(1)
        admission = (header(1) + scoped(3) + scoped(4) + scoped(5)
            + number(1) * 2 + number(7, 2) + number(1) * 2 + b"\x01\x01\x00\x01t"
            + payload + number(1, 2) + target)
        retained = header(10) + number(len(admission), 4) + admission + number(1, 2)
        qos0 = (header(11) + scoped(3) + scoped(4) + scoped(5) + number(1) * 5
            + b"\x00\x01t" + payload + b"\x01" + target + number(1, 2))
        for tag, raw in ((10, retained), (11, qos0)):
            state = {"tables": {"operations": [{"id": scoped(3).hex(), "recipe": raw.hex()}]}}
            result = canonical_receipt(state, {"operation": operation}, tag)
            self.assertEqual((result["tag"], result["semantic_tag"]), (tag, tag))
            self.assertEqual(result["canonical_bytes"], len(raw))


if __name__ == "__main__":
    unittest.main()
