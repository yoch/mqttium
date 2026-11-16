"""The public pub/sub recipe layouts are descriptive, independently decoded."""

import json
import unittest

from mqttiumd_oracle import recipe

from .support import ROOT


def be(number, width=8):
    return number.to_bytes(width, "big")


def scoped(sequence):
    return b"".join(be(number) for number in (1, 1, 0, sequence))


class ProductRecipes(unittest.TestCase):
    def canonical(self, name, index=-1):
        path = ROOT / f"contracts/examples/v1/valid/store-snapshot-{name}-receipt.json"
        value = json.loads(path.read_text())
        return bytes.fromhex(value["operations"][index]["recipe_hex"])

    def test_subscriptions_retain_order_and_exact_bytes(self):
        found = recipe.decode(self.canonical("subscription"))
        self.assertEqual(found.kind, "ChangeSubscriptions")
        self.assertEqual((found.operation, found.session, found.session_revision, found.route_revision),
                         ((1, 1, 0, 20), 2, 1, 2))
        self.assertEqual(found.changes, (recipe.SubscriptionChange(b"p1/example", True),))

    def test_claim_and_finish_preserve_the_delivery_and_claim_identity(self):
        claim = recipe.decode(self.canonical("delivery-claim"))
        finish = recipe.decode(self.canonical("delivery-finish"))
        self.assertEqual(claim.kind, "ClaimDelivery")
        self.assertEqual(finish.kind, "FinishDelivery")
        self.assertEqual((claim.publication, claim.index, claim.target, claim.target_revision,
                          claim.packet_id), ((1, 1, 0, 1), 0, 2, 1, 5))
        self.assertEqual(finish.claim, claim.operation)
        self.assertNotEqual(finish.operation, claim.operation)

    def test_subscription_counts_filters_and_flags_are_bounded(self):
        prefix = b"MQDR\x00\x01\x03" + scoped(20) + be(2) + be(1) + be(2)
        invalid = [prefix + b"\x00", prefix + b"\x09"]
        for flag, topic in [(2, b"x"), (1, b""), (1, b"a/#"), (1, b"a/+"),
                            (1, b"a\x00b"), (1, b"\xff")]:
            invalid.append(prefix + b"\x01" + bytes([flag]) + be(len(topic), 2) + topic)
        for data in invalid:
            with self.subTest(data=data.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(data)

    def test_claim_packet_zero_truncation_and_trailing_bytes_are_refused(self):
        data = self.canonical("delivery-claim")
        for invalid in (data[:-2] + b"\x00\x00", data[:-1], data + b"\x00"):
            with self.subTest(data=invalid.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(invalid)


if __name__ == "__main__":
    unittest.main()
