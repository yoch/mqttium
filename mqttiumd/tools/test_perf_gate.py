#!/usr/bin/env python3
"""The arithmetic and the packets of the performance gate's tools.

No broker is started: these tests check what the gate computes from the
figures, the rule it applies, the MQTT bytes the QoS 1 measure writes, and
the CPU reading in /proc.
"""

import os
import unittest

import measure_qos0_delivery as qos0
import measure_qos1_pairs as qos1
import run_perf_gate as gate


class TheStatistic(unittest.TestCase):
    def test_it_is_the_median_of_the_ratios_of_each_pass(self):
        # Ratios 1.2 and 1.1: their median is 1.15. The ratio of the medians
        # would be 1.7 / 1.5, about 1.133.
        self.assertAlmostEqual(gate.median_of_ratios([1.0, 2.0], [1.2, 2.2]), 1.15)
        self.assertAlmostEqual(gate.median_of_ratios([1.0, 10.0, 2.0], [1.1, 9.0, 2.0]), 1.0)

    def test_a_pass_without_a_figure_on_either_side_is_left_out(self):
        self.assertAlmostEqual(gate.median_of_ratios([1.0, None, 2.0], [1.0, 3.0, None]), 1.0)
        self.assertIsNone(gate.median_of_ratios([None], [1.0]))


class TheRule(unittest.TestCase):
    def test_a_median_above_the_threshold_is_played_again(self):
        self.assertEqual(gate.verdict(1.051, 1.05), "above")

    def test_a_median_between_1_03_and_the_threshold_asks_for_an_instruction_count(self):
        self.assertEqual(gate.verdict(1.037, 1.05), "count instructions")
        self.assertEqual(gate.verdict(1.05, 1.05), "count instructions")

    def test_a_median_up_to_1_03_passes(self):
        self.assertEqual(gate.verdict(1.03, 1.05), "pass")
        self.assertEqual(gate.verdict(0.95, 1.05), "pass")

    def test_no_figure_is_not_a_pass(self):
        self.assertEqual(gate.verdict(None, 1.05), "no figure")

    def test_four_shapes_decide_and_blast_is_only_reported(self):
        self.assertEqual(sorted(gate.DECIDING), ["qos0 bursts x1", "qos0 bursts x8", "qos1 pairs x1", "qos1 pairs x8"])
        self.assertTrue(all("blast" in form for form in gate.REPORTED))


class TheQos1Packets(unittest.TestCase):
    def test_a_publish_is_qos_1_with_its_identifier_and_no_properties(self):
        frame = qos1.publish_qos1(b"t", 0x0102, b"xy")
        self.assertEqual(frame, bytes([0x32, 8, 0, 1]) + b"t" + bytes([1, 2, 0]) + b"xy")

    def test_a_puback_carries_the_identifier_and_reason_0(self):
        self.assertEqual(qos1.puback(b"\x01\x02"), bytes([0x40, 3, 1, 2, 0]))

    def test_the_remaining_length_is_a_variable_byte_integer(self):
        self.assertEqual(qos1.remaining(127), bytes([127]))
        self.assertEqual(qos1.remaining(128), bytes([0x80, 1]))
        self.assertEqual(qos1.remaining(16_383), bytes([0xFF, 0x7F]))

    def test_the_reader_splits_packets_across_chunks(self):
        reader = qos1.Reader(None)
        frame = qos1.puback(b"\x00\x07")
        self.assertEqual(reader.feed(frame[:2]), [])
        self.assertEqual(reader.feed(frame[2:] + frame), [(0x40, b"\x00\x07\x00")] * 2)


class TheCpuReading(unittest.TestCase):
    def test_both_tools_read_the_cpu_time_of_a_process(self):
        sum(range(200_000))
        for tool in (qos0, qos1):
            seconds = tool.cpu_seconds(os.getpid())
            self.assertGreaterEqual(seconds, 0.0)
            self.assertLess(seconds, 3600.0)


if __name__ == "__main__":
    unittest.main()
