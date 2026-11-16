"""Focused checks of external-client observations, through their public API."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from external_paho import exchange as paho_exchange


class ExternalPahoTests(unittest.TestCase):
    def test_a_negative_ack_is_reported_as_rejection_not_success(self):
        class RefusedClient:
            def __init__(self, **_options):
                pass

            def connect(self, *_args, **_kwargs):
                self.on_connect(self, None, SimpleNamespace(session_present=False),
                                SimpleNamespace(value=0), None)

            def loop_start(self):
                pass

            def publish(self, *_args, **_kwargs):
                self.on_publish(self, None, 7, SimpleNamespace(value=0x97), None)
                return SimpleNamespace(mid=7, wait_for_publish=lambda **_kwargs: None,
                                       is_published=lambda: True)

            def disconnect(self):
                pass

            def loop_stop(self):
                pass

        with patch("paho.mqtt.client.Client", RefusedClient):
            receipt = paho_exchange("127.0.0.1", 1883, "negative-paho")
        self.assertFalse(any(event["kind"] == "qos2_completed" for event in receipt["events"]),
                         "a negative terminal ACK is rejection, not successful completion")
        self.assertEqual(receipt["status"], "rejected")
        self.assertEqual(receipt["publications"][0]["reason_code"], 0x97)


if __name__ == "__main__":
    unittest.main()
