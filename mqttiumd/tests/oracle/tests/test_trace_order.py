"""One causal order: the local order of each origin and the explicit causes, together.

Finding F5 of the audit of the first wave: with A1 before A2 by the local
order of their origin, A2 before B and B before A1 by explicit causes,
the reader declared the trace complete and confirmed all three relations.
The order is the union of both relations; it must be acyclic, and
``before`` is computed from that same union.
"""

import json
import unittest

from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.trace import read

from .support import ROOT


def record(sequence, origin, origin_sequence, kind, payload, parents=()):
    return {
        "format": "mqttiumd.trace-record",
        "schema_version": 1,
        "run_id": "order",
        "record_sequence": str(sequence),
        "origin_component": origin,
        "origin_incarnation": "1",
        "origin_sequence": str(origin_sequence),
        "causal_parents": [
            {"origin_component": o, "origin_incarnation": "1", "origin_sequence": str(s)}
            for o, s in parents
        ],
        "time_domain": "logical-step",
        "time_value": str(sequence),
        "kind": kind,
        "payload": payload,
    }


def sent(sequence, origin_sequence, parents=()):
    return record(
        sequence,
        "peer",
        origin_sequence,
        "PeerPacketSent",
        {"client": "producer", "packet_kind": "PUBREL", "packet_id": 7, "bytes_hex": "62020007"},
        parents,
    )


def received(sequence, origin_sequence, parents=()):
    return record(
        sequence,
        "transport",
        origin_sequence,
        "ClientBytesReceived",
        {"connection_generation": "1", "bytes_hex": "62020007"},
        parents,
    )


def trace_of(*body):
    header = record(
        0,
        "bench-supervisor",
        0,
        "TraceHeader",
        {"scenario_id": "P1-02", "backend": "sqlite", "perimeter": "full"},
    )
    records = [header, *body]
    records.append(
        record(
            len(records),
            "bench-supervisor",
            1,
            "TraceFooter",
            {"record_count": str(len(records) + 1), "complete": True},
        )
    )
    return [json.dumps(item).encode() + b"\n" for item in records]


class Order(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def test_a_cycle_through_local_order_and_causes_is_refused(self):
        # A1 < A2 locally, A2 -> B and B -> A1 explicitly.
        trace = read(
            self.contracts,
            trace_of(
                sent(1, 0, [("transport", 0)]),  # A1, caused by B
                sent(2, 1),  # A2
                received(3, 0, [("peer", 1)]),  # B, caused by A2
            ),
        )
        self.assertEqual(trace.unreadable, [])
        self.assertTrue(trace.incomplete, "a causal cycle makes the trace incomplete")
        self.assertTrue(any("cycle" in reason for reason in trace.incomplete), trace.incomplete)

    def test_the_order_is_transitive_across_both_relations(self):
        # A1 < A2 locally, A2 -> B explicitly, B < B2 locally: A1 < B2.
        trace = read(
            self.contracts,
            trace_of(
                sent(1, 0),  # A1
                sent(2, 1),  # A2
                received(3, 0, [("peer", 1)]),  # B
                received(4, 1),  # B2
                record(
                    5,
                    "timer",
                    0,
                    "DeadlineArmed",
                    {
                        "effect_id": {"epoch": "1", "owner": "0", "sequence": "1"},
                        "token": "1",
                        "after": "30",
                    },
                ),  # C, independent
            ),
        )
        self.assertEqual((trace.unreadable, trace.incomplete), ([], []))
        a1, _, _, b2, c = trace.records[1:6]
        self.assertIs(trace.before(a1, b2), True)
        self.assertIs(trace.before(b2, a1), False)
        self.assertIsNone(trace.before(a1, c))
        self.assertIsNone(trace.before(c, b2))


if __name__ == "__main__":
    unittest.main()
