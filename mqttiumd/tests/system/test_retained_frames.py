import unittest

from retained_frames import verify_retained_frames
from tcp_probe import ReceiptFailure


def journal(*observations):
    events = [{"kind": "starting", "pid": 1, "seq": "1"}]
    for sequence, (kind, frame) in enumerate(observations, 2):
        details = {"effect": "1/0/1", "packet_kind": frame, "code": "output_queue_full"}
        events.append({"kind": kind, "pid": 1, "seq": str(sequence), "connection": "5", "details": details})
    return events


class TestRetainedFrames(unittest.TestCase):
    def reject(self, reason, *observations):
        with self.assertRaises(ReceiptFailure) as rejected:
            verify_retained_frames(journal(*observations))
        self.assertEqual(rejected.exception.code, reason)

    def test_terminal_before_retention_is_visible_even_with_a_later_terminal(self):
        self.reject("retained_frame_observation_before_retention",
                    ("frame_written", "PINGRESP"), ("effect_retained", "PINGRESP"),
                    ("frame_written", "PINGRESP"))

    def test_duplicate_terminal_is_refused(self):
        self.reject("retained_frame_observation_after_terminal",
                    ("effect_retained", "PINGRESP"), ("frame_written", "PINGRESP"),
                    ("frame_written", "PINGRESP"))

    def test_written_then_failed_is_refused(self):
        self.reject("retained_frame_observation_after_terminal",
                    ("effect_retained", "PINGRESP"), ("frame_written", "PINGRESP"),
                    ("frame_failed", "PINGRESP"))

    def test_incompatible_terminal_is_refused(self):
        self.reject("retained_frame_incompatible_kind",
                    ("effect_retained", "PINGRESP"), ("frame_failed", "PUBACK"))
        self.reject("retained_frame_incompatible_kind",
                    ("effect_retained", "PINGRESP"), ("message_transmission_not_started", "PINGRESP"))

    def test_retries_count_as_one_effect_and_publish_may_fail_without_enqueue(self):
        events = journal(("effect_retained", "PUBLISH"), ("effect_retained", "PUBLISH"),
                         ("frame_failed", "PUBLISH"))
        result = verify_retained_frames(events)
        self.assertEqual(result["outcomes"], {"frame_failed": 1})
        self.assertEqual(result["repeat_refusals"], 1)
        self.assertNotIn("queued_sequence", result["effects"][0])

    def test_other_generation_cannot_dispose_the_effect(self):
        events = journal(("effect_retained", "PINGRESP"), ("frame_failed", "PINGRESP"))
        events[-1]["connection"] = "6"
        with self.assertRaises(ReceiptFailure) as rejected:
            verify_retained_frames(events)
        self.assertEqual(rejected.exception.code, "retained_frame_missing_terminal")
