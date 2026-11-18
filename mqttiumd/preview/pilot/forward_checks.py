#!/usr/bin/env python3
"""Check the producer and dashboard guarantees independently of broker timing."""
from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

import forward_client
from forward_store import (MIB, NO_MATCH, QUOTA, SUCCESS, UNKNOWN, Backpressure, Event, Inbox, Outbox,
                           backup, footprint, snapshot)


class ForwardChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="forward-check-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.path = self.directory / "outbox.sqlite"
        self.inbox = self.directory / "inbox.sqlite"

    def test_quota_preserves_fifo_and_event_id(self):
        with Outbox(self.path, "capteur-1") as store:
            first = store.enqueue(b"first")
            second = store.enqueue(b"second")
            old_attempt = store.begin(first)
            store.complete(first.sequence, old_attempt, QUOTA)
            head, streak = store.head()
            self.assertEqual(head, first, "negative_puback_dropped_fifo_head")
            self.assertEqual(streak, 1)
            self.assertEqual(snapshot(self.path)["confirmed"], 0)
            current_attempt = store.begin(head)
            with self.assertRaisesRegex(ValueError, "outbox_result_without_current_attempt"):
                store.complete(head.sequence, old_attempt, SUCCESS)
            store.complete(head.sequence, current_attempt, SUCCESS)
            self.assertEqual(store.head()[0], second)
            self.assertEqual(snapshot(self.path)["rows"], 2)
            store.collect()
            self.assertEqual(snapshot(self.path)["rows"], 1)

    def test_recovery_keeps_uncertain_event(self):
        with Outbox(self.path, "capteur-1") as store:
            first = store.enqueue(b"ambiguous")
            attempt = store.begin(first)
        with Outbox(self.path, "capteur-1") as restored:
            self.assertEqual(restored.head()[0], first, "uncertain_event_lost_on_reopen")
            state = snapshot(self.path)
            self.assertEqual((state["unknown"], state["last_reason"], state["confirmed"]), (1, UNKNOWN, 0))
            retry = restored.begin(first)
            self.assertGreater(retry, attempt)
            restored.complete(first.sequence, retry, SUCCESS)
        with Outbox(self.path, "capteur-1") as restored:
            self.assertEqual(restored.head()[0], None)
            restored.collect()
            second = restored.enqueue(b"new")
            self.assertEqual((second.stream, second.sequence), (first.stream, 2))

    def test_permanent_results_keep_the_row(self):
        for reason, diagnostic in ((0x87, "not_authorized"), (NO_MATCH, "no_matching_subscribers")):
            with self.subTest(reason=reason):
                path = self.directory / f"blocked-{reason}.sqlite"
                with Outbox(path, "capteur-1") as store:
                    event = store.enqueue(b"keep me")
                    store.complete(event.sequence, store.begin(event), reason)
                    with self.assertRaisesRegex(RuntimeError, diagnostic):
                        store.head()
                    store.collect()
                    stats = snapshot(path)
                    self.assertEqual((stats["rows"], stats["confirmed"], stats["blocked"]), (1, 0, 1))

    def test_repeat_has_no_second_application(self):
        event = Event.encode("capteur-1", b"s" * 16, 1, 12, b"body")
        with Inbox(self.inbox, "dashboard-1") as inbox:
            self.assertEqual(inbox.apply(event, False), "applied")
        with Inbox(self.inbox, "dashboard-1") as inbox:
            self.assertEqual(inbox.apply(event, True), "duplicate", "duplicate_was_applied_again")
        state = snapshot(self.inbox)
        self.assertEqual((state["mqtt_received"], state["unique_count"], state["duplicates"], state["mqtt_dup"]),
                         (2, 1, 1, 1))

    def test_conflict_cannot_replace_application(self):
        first = Event.encode("capteur-1", b"s" * 16, 1, 12, b"body")
        conflicting = Event.encode("capteur-1", first.stream, 1, 12, b"contradiction")
        with Inbox(self.inbox, "dashboard-1") as inbox:
            self.assertEqual(inbox.apply(first, False), "applied")
            self.assertEqual(inbox.apply(conflicting, False), "conflict", "contradictory_payload_applied")
            self.assertEqual(inbox.db.execute("SELECT wire FROM applied").fetchone()[0], first.wire)
        self.assertEqual(snapshot(self.inbox)["conflicts"], 1)

    def test_wire_binds_digest_to_topic_and_rejects_bad_bytes(self):
        event = Event.encode("capteur-1", b"s" * 16, 1, 12, b"body")
        self.assertEqual(len(event.wire), 256)
        self.assertEqual(Event.decode(event.topic, event.wire), event)
        with self.assertRaisesRegex(ValueError, "invalid_event_digest_or_padding"):
            Event.decode("atelier/capteur-2/telemetry", event.wire)
        for invalid in (event.wire[:255], event.wire[:-1] + bytes([event.wire[-1] ^ 1])):
            with self.assertRaises(ValueError):
                Event.decode(event.topic, invalid)

    def test_local_disk_quota_preserves_prior_events_and_recovers(self):
        with Outbox(self.path, "capteur-1", MIB) as store:
            first = store.enqueue(b"first")
            refused = False
            for _ in range(4_000):
                try:
                    store.enqueue(b"x" * 120)
                except Backpressure as error:
                    self.assertIn("full", str(error))
                    refused = True
                    break
                self.assertLessEqual(sum(footprint(self.path).values()), MIB, "physical_local_quota_exceeded")
            self.assertTrue(refused, "full_local_outbox_accepted_new_events")
            before = snapshot(self.path)
            self.assertGreater(before["pending"], 0)
            self.assertEqual(store.head()[0], first)
            for seq in range(1, 51):
                event, _ = store.head()
                self.assertEqual(event.sequence, seq)
                store.complete(seq, store.begin(event), SUCCESS)
            store.collect()
            new = store.enqueue(b"after capacity returned")
            self.assertEqual(new.sequence, before["enqueued"] + 1)
            self.assertLessEqual(sum(footprint(self.path).values()), MIB)
            self.assertEqual(snapshot(self.path)["pending"], before["pending"] - 49)
            self.assertGreater(store.checkpoints, 0)

    def test_stopped_backup_reopens_pending_event(self):
        with Outbox(self.path, "capteur-1") as store:
            original = store.enqueue(b"backup")
            with self.assertRaises(BlockingIOError):
                backup(self.path, self.directory / "active.sqlite")
        copied = self.directory / "restored.sqlite"
        backup(self.path, copied)
        with Outbox(copied, "capteur-1") as store:
            self.assertEqual(store.head()[0], original)
            self.assertEqual(store.enqueue(b"next").sequence, 2)

    def test_real_sender_waits_between_quota_retries(self):
        stop = threading.Event()
        timestamps = []
        clock = [100.0]

        class QuotaWire:
            def __init__(self, args, expiry):
                pass

            def connect(self, *, fresh):
                return False

            def pump(self, duration):
                clock[0] += duration

            def send(self, event, cut):
                timestamps.append((clock[0], event.event_id))
                if len(timestamps) == 4:
                    stop.set()
                return QUOTA

            def close(self, **_):
                pass

        with Outbox(self.path, "capteur-1") as store:
            event = store.enqueue(b"under pressure")
            args = types.SimpleNamespace(identity="capteur-1")
            with patch.object(forward_client, "Wire", QuotaWire), patch.object(forward_client.time, "monotonic", lambda: clock[0]):
                forward_client.send_pending(args, store, stop, None)
        self.assertEqual([identity for _, identity in timestamps], [event.event_id] * 4)
        for index, (earlier, later) in enumerate(zip(timestamps, timestamps[1:])):
            ceiling = .1 * 2 ** index
            self.assertGreaterEqual(later[0] - earlier[0], ceiling / 2 - 1e-9, "quota_retries_without_backoff")
            self.assertLessEqual(later[0] - earlier[0], ceiling + 1e-9)


if __name__ == "__main__":
    unittest.main()
