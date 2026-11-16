"""Check that the lost-PUBACK witness discards an actually observed frame."""
import json
from pathlib import Path
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch
import socket
import tempfile
import threading
import unittest

import qos1_reception as reception
from qos1_reception import Proxy
from tcp_probe import Frame, FrameDecoder, Observations, operation_hex


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


class LifecycleReceiptCompatibilityTests(unittest.TestCase):
    """Exercise the real probe bodies with a preceding durable session result."""
    @staticmethod
    def operation(sequence):
        return {'incarnation': '1', 'epoch': '1', 'owner': '0', 'sequence': str(sequence)}

    @staticmethod
    def sqlite(*args, **kwargs):
        connection = SimpleNamespace(execute=lambda *args: SimpleNamespace(fetchone=lambda: ((8192).to_bytes(8, 'big'),)))
        class Transaction:
            def __enter__(self): return connection
            def __exit__(self, *args): pass
        return Transaction()

    def test_unknown_publish_selects_its_own_result_after_known_session_open(self):
        opening, pending = self.operation(1), self.operation(2)
        events = [{'kind': 'store_result', 'seq': '1', 'command_kind': 'SessionOpen',
                   'operation': opening, 'details': {'outcome': 'Committed'}}]
        baseline = {'admissions': [], 'tables': {'operations': [{'id': operation_hex(opening), 'decision': 8}],
                                               'fences': []}}
        resolved = {'admissions': [], 'tables': {'operations': baseline['tables']['operations'],
                                               'fences': [{'id': operation_hex(pending)}]}}
        class Broker:
            address, observations, db = ('unused', 1), SimpleNamespace(record=lambda *args, **kwargs: None), Path('unused-db')
            def __init__(self, *args): self.process = SimpleNamespace(poll=lambda: None)
            def ready(self): pass
            def events(self): return events
            def wait_event(self, kind, *, after=-1, **kwargs):
                return next(e for e in events if e['kind'] == kind and int(e['seq']) > after)
            def stop(self): pass
            def cleanup(self): pass
        class Wire:
            def __init__(self, *args): self.observations = SimpleNamespace(record=lambda *args, **kwargs: None)
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def connect(self, *args, **kwargs): pass
            def send(self, *args):
                events.append({'kind': 'store_result', 'seq': '2', 'command_kind': 'Admission',
                               'operation': pending, 'details': {'outcome': 'PendingOrUnknown'}})
            def poll(self, *args): return None
            def expect(self, *args, **kwargs): return Frame(0x40, b'\x00\x07\x83')
            def exchange(self, *args, **kwargs): pass
        reads = iter((baseline, baseline, resolved))
        with ExitStack() as stack:
            for name, value in [('Broker', Broker), ('Wire', Wire), ('database', lambda *args: next(reads)),
                                ('write_json', lambda *args: None), ('finished_state', lambda *args, **kwargs: None)]:
                stack.enter_context(patch.object(reception, name, value))
            stack.enter_context(patch.object(reception.sqlite3, 'connect', self.sqlite))
            outcome = reception.unknown_outcome(Path('unused-binary'), Path('unused-case'))
        self.assertEqual(outcome['unknown_operation'], pending)
        self.assertTrue(outcome['same_operation_fenced'])

    def test_certain_publish_refusals_are_distinct_from_session_open_receipt(self):
        opening = self.operation(1)
        rejected = []
        class Broker:
            address, observations, db = ('unused', 1), SimpleNamespace(record=lambda *args, **kwargs: None), Path('unused-db')
            def __init__(self, *args): self.process = SimpleNamespace(poll=lambda: None)
            def ready(self): pass
            def stop(self): pass
            def cleanup(self): pass
        class Wire:
            def __init__(self, *args): pass
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def connect(self, *args, **kwargs): pass
            def send(self, *args): rejected.append(len(rejected)+1)
            def expect(self, kind): return Frame(kind, rejected[-1].to_bytes(2, 'big')+b'\x97')
            def exchange(self, *args, **kwargs): pass
        def state(*args):
            receipts = [{'id': operation_hex(opening), 'decision': 8}]
            receipts += [{'id': operation_hex(self.operation(n+1)), 'decision': 3} for n in rejected]
            return {'admissions': [], 'counts': {'operations': len(receipts)}, 'tables': {'operations': receipts}}
        client = {'status': 'rejected', 'publications': [{'reason_code': 0x97, 'exception': 'PublishRejectedError'}]}
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            for name, value in [('Broker', Broker), ('Wire', Wire), ('database', state),
                                ('write_json', lambda *args: None), ('finished_state', lambda *args, **kwargs: None)]:
                stack.enter_context(patch.object(reception, name, value))
            stack.enter_context(patch.object(reception.sqlite3, 'connect', self.sqlite))
            stack.enter_context(patch.object(reception.subprocess, 'run', lambda *args, **kwargs:
                SimpleNamespace(returncode=2, stderr='', stdout=json.dumps(client))))
            outcome = reception.quota_refusal(Path('unused-binary'), Path(directory))
        self.assertEqual(len(outcome['refusals']), 18)
        self.assertTrue(outcome['credit_reused'])


if __name__ == "__main__":
    unittest.main()
