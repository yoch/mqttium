"""Rules of the judge that a whole trace of the corpus would hide.

Each test edits the hand-made history of P1-02 and judges it in memory.
"""

import json
import sys
import unittest

from mqttiumd_oracle import judge
from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.strict import parse
from mqttiumd_oracle.trace import read

from .support import ORACLE, ROOT

sys.path.insert(0, str(ORACLE))
import build_traces as corpus  # noqa: E402

PROVENANCE = "broker_inputs_have_provenance"
ACK = "no_positive_ack_before_confirmed_commit"
ADMISSION = "one_admission_per_observed_inbound_exchange"


class Rules(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.fixture = parse(
            (ROOT / "tests/fixtures/p1/three-persistent-sessions.json").read_bytes()
        )

    def run_of(self, edit=lambda events: None, edit_states=lambda states: None):
        events = corpus.p1_02()
        edit(events)
        states = corpus.p1_02_snapshots("in-memory")
        edit_states(states)
        records = corpus.render("in-memory", events, states)
        trace = read(self.contracts, [json.dumps(record).encode() + b"\n" for record in records])
        self.assertEqual(trace.unreadable, [])
        self.assertEqual(trace.incomplete, [])
        run = judge.Run(self.contracts, corpus.P1_02, self.fixture, trace)
        for record in trace.of_kind("DurableStateInspected"):
            name = record.payload["checkpoint"]
            run.snapshots[name] = (record, states[name])
        return run

    def statuses(self, edit=lambda events: None, edit_states=lambda states: None):
        run = self.run_of(edit, edit_states)
        return {name: status.status for name, status in judge.judge(run).items()}

    def only(self, assertion, status, edit):
        found = self.statuses(edit)
        expected = dict.fromkeys(found, "PASS") | {assertion: status}
        self.assertEqual(found, expected)

    def test_the_unedited_history_passes(self):
        self.assertEqual(set(self.statuses().values()), {"PASS"})

    # ------------------------------------------------- the oracle's exchange

    def exchanges(self, *packets):
        """The exchanges of a client that sent and observed ``packets``."""

        def edit(events):
            del events[corpus.at(events, "publish") :]
            for index, (kind, flags, code) in enumerate(packets):
                if kind == "PUBLISH":
                    payload = {
                        "client": "producer",
                        "packet_kind": "PUBLISH",
                        "packet_id": 7,
                        "dup": bool(flags & 0x08),
                        "bytes_hex": f"{0x30 | flags:02x}" + corpus.PUBLISH_HEX[2:],
                    }
                    events.append(corpus.ev(f"p{index}", "peer", "PeerPacketSent", payload))
                else:
                    payload = {
                        "client": "producer",
                        "packet_kind": kind,
                        "packet_id": 7,
                        "reason_code": code,
                        # The bytes the peer observed are the packet it states.
                        "bytes_hex": {"PUBREC": "50", "PUBCOMP": "70"}[kind]
                        + "030007"
                        + f"{code:02x}",
                    }
                    events.append(corpus.ev(f"p{index}", "peer", "PeerPacketObserved", payload))

        found, notes = self.run_of(edit).exchanges()
        return [(exchange.ordinal, exchange.closed is not None) for exchange in found], notes

    def test_a_replayed_publish_is_the_same_exchange(self):
        qos2, dup = 0x04, 0x0C
        found, notes = self.exchanges(("PUBLISH", qos2, 0), ("PUBLISH", dup, 0))
        self.assertEqual((found, notes), ([(0, False)], []))
        # Still the same exchange after a positive PUBREC.
        found, _ = self.exchanges(("PUBLISH", qos2, 0), ("PUBREC", 0, 0), ("PUBLISH", dup, 0))
        self.assertEqual(found, [(0, False)])

    def test_an_exchange_ends_with_pubcomp_or_a_refusing_pubrec(self):
        qos2 = 0x04
        found, _ = self.exchanges(
            ("PUBLISH", qos2, 0), ("PUBREC", 0, 0), ("PUBCOMP", 0, 0), ("PUBLISH", qos2, 0)
        )
        self.assertEqual(found, [(0, True), (1, False)])
        found, _ = self.exchanges(("PUBLISH", qos2, 0), ("PUBREC", 0, 0x97), ("PUBLISH", qos2, 0))
        self.assertEqual(found, [(0, True), (1, False)])
        # 0x7f is still a success.
        found, _ = self.exchanges(("PUBLISH", qos2, 0), ("PUBREC", 0, 0x7F), ("PUBLISH", qos2, 0))
        self.assertEqual(found, [(0, False)])

    def test_only_a_qos_2_publish_opens_an_exchange(self):
        found, notes = self.exchanges(("PUBLISH", 0x02, 0), ("PUBLISH", 0x00, 0))
        self.assertEqual((found, notes), ([], []))

    def test_a_record_whose_bytes_contradict_it_is_not_trusted(self):
        def edit(events):
            sent = events[corpus.at(events, "publish")]
            sent["payload"] = dict(sent["payload"], dup=True)

        found = judge.judge(self.run_of(edit))
        self.assertEqual(found[ADMISSION].status, "INCONCLUSIVE")

    def test_an_admission_belongs_to_the_exchange_of_its_own_packet_identifier(self):
        # Two exchanges are open at once, on Packet Identifiers 7 and 8, and
        # each is admitted once.
        other = corpus.scoped(1, 5)
        recipe = corpus.admit_recipe(other, corpus.scoped(1, 5), corpus.scoped(1, 5), 1)
        # The Packet Identifier follows the three identities and the source
        # session and revision.
        at = 2 * (4 + 2 + 1 + 3 * 32 + 8 + 8)
        self.assertEqual(recipe[at : at + 4], "0007")
        recipe = recipe[:at] + "0008" + recipe[at + 4 :]

        def edit(events):
            sent = dict(events[corpus.at(events, "publish")]["payload"], packet_id=8)
            corpus.insert_after(
                events,
                "publish.in",
                [corpus.ev("other", "peer", "PeerPacketSent", sent)]
                + corpus.committed("other", 1, 7, other, "AdmitPublication", cause="other"),
            )

        def edit_states(states):
            for state in states.values():
                state["operations"].append(
                    {
                        "operation_id": other,
                        "recipe_hex": recipe,
                        "decision": {
                            "kind": "Committed",
                            "facts": {
                                "kind": "PublicationAdmitted",
                                "publication_id": other,
                                "exchange": {"session": "1", "generation": "1"},
                            },
                        },
                    }
                )

        self.assertEqual(self.statuses(edit, edit_states)[ADMISSION], "PASS")

    # ------------------------------------------------- positive acknowledgement

    def test_a_refusing_pubrec_needs_no_commit(self):
        def edit(events):
            corpus.insert_after(
                events,
                "publish.in",
                [
                    corpus.ev(
                        "refusal",
                        "broker-core",
                        "FrameEligible",
                        {
                            "effect_id": corpus.effect(1, 9),
                            "connection": {"session": "1", "generation": "1"},
                            "packet_kind": "PUBREC",
                            "packet_id": 7,
                            "reason": "QuotaExceeded",
                        },
                        ["publish.in"],
                    )
                ],
            )

        self.assertEqual(self.statuses(edit)[ACK], "PASS")

    def test_knowledge_of_one_exchange_does_not_acknowledge_the_next(self):
        # After PUBCOMP the slot is free: a second PUBREC needs a commit of
        # its own.
        def edit(events):
            events.insert(
                corpus.at(events, "inspect.final"),
                corpus.ack_out("late", 2, 2, 9, "PUBREC", corpus.PUBREC_HEX, "pubcomp")[0],
            )

        self.only(ACK, "FAIL", edit)

    # --------------------------------------------------------------- provenance

    def test_a_result_names_the_operation_of_its_submission(self):
        def edit(events):
            result = events[corpus.at(events, "release.delivered")]
            result["payload"] = dict(result["payload"], operation_id=corpus.OP_ADMIT)

        self.assertEqual(self.statuses(edit)[PROVENANCE], "FAIL")

    def test_recovery_pages_are_delivered_in_order_from_the_first(self):
        def edit(events):
            for label in ("recovery.read", "recovery.in"):
                page = events[corpus.at(events, label)]
                page["payload"] = dict(page["payload"], page="1")

        self.only(PROVENANCE, "FAIL", edit)

    def test_write_progress_belongs_to_a_frame_the_core_made_eligible(self):
        def edit(events):
            written = events[corpus.at(events, "pubrec.written")]
            written["payload"] = dict(written["payload"], effect_id=corpus.effect(2, 77))

        self.only(PROVENANCE, "FAIL", edit)

    def test_a_result_before_its_submission_is_not_an_answer(self):
        def edit(events):
            result = events.pop(corpus.at(events, "release.delivered"))
            result["parents"] = ["pubrel.in"]
            events.insert(corpus.at(events, "release.submitted"), result)
            events[corpus.at(events, "pubcomp")]["parents"] = ["pubrel.in"]

        self.assertEqual(self.statuses(edit)[PROVENANCE], "FAIL")


class Closures(unittest.TestCase):
    """What makes a certain refusal certain, on the history of QABS-B."""

    CLOSURE = "certain_rejection_has_a_closure"

    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.fixture = parse(
            (ROOT / "tests/fixtures/p1/three-persistent-sessions.json").read_bytes()
        )
        cls.scenario = parse((ROOT / "tests/scenarios/qabs/QABS-B.json").read_bytes())

    def status(self, edit=lambda events: None, edit_state=lambda state: None):
        events = corpus.qabs_b()
        edit(events)
        states = corpus.qabs_b_snapshots("in-memory")
        edit_state(states["final"])
        records = corpus.render("in-memory", events, states)
        trace = read(self.contracts, [json.dumps(record).encode() + b"\n" for record in records])
        self.assertEqual((trace.unreadable, trace.incomplete), ([], []))
        run = judge.Run(self.contracts, self.scenario, self.fixture, trace)
        for record in trace.of_kind("DurableStateInspected"):
            run.snapshots[record.payload["checkpoint"]] = (record, states["final"])
        return judge.judge(run)[self.CLOSURE].status

    @staticmethod
    def without_fence_record(events):
        del events[corpus.at(events, "fence.stable")]
        events[corpus.at(events, "fenced.ready")]["parents"] = ["released.held_fence"]

    def test_a_fence_written_before_the_refusal_closes_it(self):
        # The snapshot was taken before any fence could show: the trace is enough.
        self.assertEqual(self.status(edit_state=lambda state: state.update(fences=[])), "PASS")

    @classmethod
    def inspected_first(cls, events):
        # The snapshot is inspected before the store answers: what it holds
        # was established before the refusal.
        cls.without_fence_record(events)
        events.insert(
            corpus.at(events, "fenced.ready"), events.pop(corpus.at(events, "inspect.final"))
        )

    def test_a_receipt_of_rejection_closes_it(self):
        # A refusal by a guard writes no fence; its receipt, inspected before
        # the refusal, keeps it refused.
        def receipt(state):
            state["fences"] = []
            state["operations"] = [
                {
                    "operation_id": corpus.OP_ADMIT,
                    "recipe_hex": corpus.admit_recipe(
                        corpus.OP_ADMIT, corpus.PUB, corpus.PAYLOAD_ID, 1
                    ),
                    "decision": {"kind": "RejectedCertain", "reason": "PreconditionFailed"},
                }
            ]

        self.assertEqual(self.status(self.inspected_first, receipt), "PASS")

    def test_a_fence_the_snapshot_holds_closes_it(self):
        self.assertEqual(self.status(self.inspected_first), "PASS")

    def test_a_fence_a_later_snapshot_holds_does_not_close_it(self):
        # DEM-MU-01: the store refuses on an absence, the fence comes after.
        self.assertEqual(self.status(self.without_fence_record), "FAIL")

    def test_nothing_written_closes_nothing(self):
        self.assertEqual(
            self.status(self.without_fence_record, lambda state: state.update(fences=[])), "FAIL"
        )

    def test_a_fence_written_after_the_refusal_does_not_close_it(self):
        def late(events):
            fence = events.pop(corpus.at(events, "fence.stable"))
            events[corpus.at(events, "fenced.ready")]["parents"] = ["released.held_fence"]
            fence["parents"] = ["fenced.delivered"]
            corpus.insert_after(events, "fenced.delivered", [fence])

        self.assertEqual(self.status(late, lambda state: state.update(fences=[])), "FAIL")


if __name__ == "__main__":
    unittest.main()
