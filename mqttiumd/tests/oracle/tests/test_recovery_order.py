"""The oracle's order of a recovery inventory agrees with the contract's.

The order is ``RecoveryOrder::admit`` of the contract crate (record 0014).
The oracle keeps its own copy, ``InventoryOrder``, on purpose: the oracle
role does not run the code it judges. The two are held together by one
file of cases that the Rust test of the lab crate runs through ``admit``
and this test runs through the copy.
"""

import json
import unittest

from mqttiumd_oracle.semantic import InventoryOrder

from .support import ROOT

CASES = ROOT / "crates/mqttiumd-lab/tests/support/recovery-order-cases.json"


def refusal_kind(code, member):
    if code == "duplicate-name":
        return "Repeated"
    return "RoutesUnsorted" if member == "topic_hex" else "RankDecreased"


class RecoveryOrder(unittest.TestCase):
    def test_the_oracle_finds_the_refusals_of_the_contract(self):
        cases = json.loads(CASES.read_text())["cases"]
        self.assertGreaterEqual(len(cases), 10)
        for case in cases:
            with self.subTest(case["name"]):
                order, found = InventoryOrder(), []
                for index, (kind, *topic) in enumerate(case["facts"]):
                    fact = {"kind": kind}
                    if topic:
                        fact["topic_hex"] = topic[0]
                    refusal = order.admit(fact)
                    if refusal is not None:
                        found.append([index, refusal_kind(refusal[0], refusal[1])])
                self.assertEqual(found, case["refused"])


if __name__ == "__main__":
    unittest.main()
