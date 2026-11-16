"""Check that the lost-PUBACK witness discards an actually observed frame."""
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest

from qos1_reception import Proxy
from tcp_probe import Frame, FrameDecoder, Observations


class ProxyTests(unittest.TestCase):
    def test_complete_puback_is_retained_but_not_forwarded_to_client(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "observer.jsonl"
            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream.bind(("127.0.0.1", 0))
            upstream.listen(1)
            upstream.settimeout(2)
            errors = []

            def broker():
                try:
                    peer, _ = upstream.accept()
                    with peer:
                        peer.settimeout(2)
                        self.assertEqual(peer.recv(32), b"\xc0\x00")
                        peer.sendall(b"\xd0\x00\x40\x02\x00\x07")
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=broker)
            worker.start()
            proxy = Proxy(upstream.getsockname(), Observations(path), drop_ack=True)
            try:
                with socket.create_connection(proxy.address, timeout=2) as client:
                    client.settimeout(2)
                    client.sendall(b"\xc0\x00")
                    received = bytearray()
                    while data := client.recv(64):
                        received.extend(data)
                self.assertTrue(proxy.ack_dropped.is_set())
                decoder = FrameDecoder()
                self.assertEqual(decoder.feed(bytes(received)), [Frame(0xD0, b"")])
                decoder.finish()
                self.assertEqual(proxy.frames, [Frame(0xD0, b""), Frame(0x40, b"\x00\x07")])
                events = [json.loads(line) for line in path.read_text().splitlines()]
                dropped = [event for event in events if event["kind"] == "proxy_puback_discarded"]
                self.assertEqual(len(dropped), 1)
                self.assertEqual(dropped[0]["packet_id"], 7)
                self.assertFalse(dropped[0]["forwarded"])
            finally:
                proxy.close()
                worker.join(timeout=2)
                upstream.close()
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
