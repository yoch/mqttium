"""Exercise ADR 0059 with a real TLS 1.3 peer, process death and a fresh reader."""
import argparse
import hashlib
import json
import sqlite3
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path

# Reuse the independent packet decoder, event reader and demo certificates.
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tests/system"))
from secure_reception import Peer, STATUS, client_context, latest, observed, pki_module, tls_args
from tcp_probe import packet
from tcp_reception import Broker as ProcessBroker
from message_expiry_probe import decode_publish

TARGET = "capteur-1"
MONITOR = "dashboard"
WITNESS = "dashboard-check"
WILL = b"external restart witness"
PUBLISHER = "capteur-check"
MARKER = b"post-recovery ordering witness"


class Broker(ProcessBroker):
    def events(self):
        # The frozen external reception limits the number of ready fields to
        # its historical schema. Read only complete records from this run.
        if not self.events_path.is_file():
            return []
        with self.events_path.open("rb") as stream:
            events = []
            for line in stream:
                if not line.endswith(b"\n"):
                    # A later read on this descriptor could see only the suffix.
                    # Start the next poll from the beginning of the journal.
                    break
                events.append(json.loads(line))
        assert all(event["pid"] == self.process.pid for event in events)
        return events


@contextmanager
def server(binary, case, pki, label, mode, shards, pause=None):
    options = (*(tls_args(pki) if pki else ()), "--storage-mode", mode, "--io-threads", "4",
               "--delivery-shards", str(shards), "--event-level", "audit")
    if mode == "volatile":
        options += ("--snapshot-interval", "2")
    broker = Broker(binary, case, label=label, pauses=(pause,) if pause else (), extra_args=options)
    try:
        ready = broker.ready()["details"]
        assert (ready["profile"], ready["storage_mode"], ready["delivery_shards"]) == (
            "mtls-will-pubsub-v1" if pki else "tcp-will-pubsub-v1", mode, shards), ready
        yield broker
    except Exception:
        print(f"Broker stderr: {broker.stderr_path.read_text()}", file=sys.stderr)
        print(f"Last broker events: {broker.events()[-5:]}", file=sys.stderr)
        raise
    finally:
        broker.cleanup()


def peer(broker, pki, client_id):
    identity = "sensor" if client_id in (TARGET, PUBLISHER) else "dashboard"
    return Peer(broker, client_id, client_context(pki, identity) if pki else None)


def wait_fact(broker, kind, after, **details):
    def matching():
        for event in broker.events():
            if event["kind"] != kind or int(event["seq"]) <= after:
                continue
            if all(event.get("details", {}).get(k) == v for k, v in details.items()):
                return event
        return None

    return observed(broker, matching, f"missing_{kind}_{details}", seconds=10)


def snapshot_sessions_and_wills(broker):
    with closing(sqlite3.connect(broker.db.as_uri() + "?mode=ro", uri=True)) as db:
        return tuple(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                     for table in ("sessions", "wills"))


def wait_for_snapshot(broker, after, sessions, wills=None):
    deadline = time.monotonic() + 10
    while True:
        event = broker.wait_event("volatile_snapshot_saved", after=after,
                                  timeout=deadline - time.monotonic())
        after = int(event["seq"])
        stored_sessions, stored_wills = snapshot_sessions_and_wills(broker)
        if stored_sessions == sessions and (wills is None or stored_wills == wills):
            return after


def end_and_read(peer, reason):
    # OpenSSL reports an unannounced TCP EOF as SSLEOFError with suppression off.
    if peer.tls:
        peer.socket.suppress_ragged_eofs = False
    peer.send(packet(0xE0, bytes([reason, 5, 0x11, 0, 0, 0, 0])))
    assert not peer.frames and peer.socket.recv(1) == b"", "expected end of stream"
    peer.decoder.finish()
    return "TLS close_notify" if peer.tls else "TCP EOF"


def durability(binary, case, pki, mode, cut, reason, shards, survives):
    with server(binary, case, pki, "cut", mode, shards,
                None if cut.endswith("snapshot") else cut) as broker, ExitStack() as peers:
        monitor = peers.enter_context(peer(broker, pki, MONITOR))
        assert not monitor.open(MONITOR, expiry=600)
        monitor.subscribe(1, [(STATUS, 1)])
        witness = peers.enter_context(peer(broker, pki, WITNESS))
        assert not witness.open(WITNESS, expiry=600)
        target = peers.enter_context(peer(broker, pki, TARGET))
        before = latest(broker)
        assert not target.open(TARGET, expiry=600, will=(STATUS, WILL), will_retain=True)
        if mode == "volatile":
            # The previous atomic image actually contains the connected target's Will.
            before = wait_for_snapshot(broker, before, sessions=3, wills=1)
        terminal = end_and_read(target, reason)
        if cut.endswith("snapshot"):
            closed = wait_fact(broker, "session_changed", before, action="Closed")
            observed_seq = int(closed["seq"])
            if cut == "next-snapshot":
                observed_seq = wait_for_snapshot(broker, before, sessions=2)
        else:
            held = broker.wait_event("barrier_hit", point=cut, after=before)
            assert held["command_kind"] == "WillEnd"
            observed_seq = int(held["seq"])
        status = broker.stop(killed=True)
        if mode == "volatile":
            sessions, wills = snapshot_sessions_and_wills(broker)
            assert sessions == (3 if survives else 2)
            if survives:
                assert wills == 1
    with server(binary, case, pki, "reopened", mode, shards) as broker, ExitStack() as peers:
        monitor = peers.enter_context(peer(broker, pki, MONITOR))
        assert monitor.open(MONITOR, expiry=600)
        witness = peers.enter_context(peer(broker, pki, WITNESS))
        witness_present = witness.open(WITNESS, expiry=600)
        assert witness_present
        published = retained = None
        if survives or reason == 4:
            published = monitor.message(WILL)
            assert (published.topic, published.qos) == (STATUS, 1)
        witness.subscribe(1, [(STATUS, 1)])
        if survives or reason == 4:
            retained = witness.message(WILL)
            assert retained.retain and retained.topic == STATUS
        publisher = peers.enter_context(peer(broker, pki, PUBLISHER))
        assert not publisher.open(PUBLISHER, expiry=0)
        publisher.publish(STATUS, MARKER)
        # Both a resumed route and a fresh retained subscription see this fence.
        assert monitor.message(MARKER).qos == 1
        assert witness.message(MARKER).qos == 1
        target = peers.enter_context(peer(broker, pki, TARGET))
        target_present = target.open(TARGET, expiry=600)
        assert target_present == survives
        witness.ping()
        broker.stop()
    return dict(transport_end=terminal, exit_status=status, observed_event_seq=observed_seq,
                session_present=target_present, witness_present=witness_present,
                received_will=published is not None, received_retained_will=retained is not None)


def takeover(binary, case, pki, shards, allow_drain, *, barrier_delay=0.0):
    point = "tls.frame.first_bytes"
    with server(binary, case, pki, "takeover", "durable", shards, point) as broker, ExitStack() as peers:
        old = peers.enter_context(peer(broker, pki, MONITOR))
        assert not old.open(MONITOR, expiry=600)
        old.subscribe(1, [(STATUS, 1)])
        source = peers.enter_context(peer(broker, pki, TARGET))
        assert not source.open(TARGET, expiry=600)
        # Complete this independent handshake before stopping the old TLS reactor.
        new = peers.enter_context(peer(broker, pki, MONITOR))
        (broker.control / "fragment-next-tls-publish").touch(exist_ok=False)
        payload = b"Q" * 24_576
        source.publish(STATUS, payload, expiry=600)
        held = broker.wait_event("barrier_hit", point=point)
        assert held["details"]["ciphertext_bytes_sent"] == 3
        if barrier_delay > 0:
            time.sleep(barrier_delay)
        with closing(sqlite3.connect(broker.db.as_uri() + "?mode=ro", uri=True)) as db:
            [(packet_id,)] = db.execute("SELECT packet_id FROM outbound").fetchall()
        with ThreadPoolExecutor(max_workers=1) as pool:
            opening = pool.submit(new.open, MONITOR, expiry=600)
            accepted = broker.wait_event("connect_validated", after=int(held["seq"]))
            assert accepted["connection"] != held["connection"]
            queued = broker.wait_event("takeover_disconnect_queued", after=int(accepted["seq"]))
            assert queued["connection"] == held["connection"]
            old.socket.suppress_ragged_eofs = False
            if allow_drain:
                broker.release(point)
                # A complete PUBLISH and terminal MQTT frame precede close_notify.
                original = decode_publish(old.expect(0x30))
                assert original.payload == payload and original.qos == 1 and not original.dup
                assert old.expect(0xE0).body[:1] == b"\x8e"
                assert not old.frames and old.socket.recv(1) == b""
                old.decoder.finish()
            else:
                # Keep the three-byte prefix held until the broker retires this
                # generation. A partial record cannot be followed by a TLS alert.
                try:
                    data = old.socket.recv(1)
                except ssl.SSLEOFError:
                    pass
                else:
                    raise AssertionError(f"expected a truncated TLS record, received {data!r}")
            assert opening.result(timeout=5)
        closed = wait_fact(broker, "connection_closed", int(held["seq"]), code="session_taken_over")
        opened = wait_fact(broker, "session_changed", int(closed["seq"]), action="Opened")
        assert closed["connection"] == held["connection"]
        assert opened["connection"] == accepted["connection"]
        replay = new.message(payload)
        assert replay.dup and replay.qos == 1 and replay.packet_id == packet_id
        if allow_drain:
            assert replay.packet_id == original.packet_id
        source.publish(STATUS, MARKER, mid=2)
        assert new.message(MARKER).qos == 1
        broker.stop()
    return dict(ciphertext_prefix=held["details"]["ciphertext_bytes_sent"], old_closed_seq=int(closed["seq"]),
                new_opened_seq=int(opened["seq"]), replay_dup=replay.dup, packet_id=replay.packet_id,
                old_stream="DISCONNECT 0x8e, TLS close_notify" if allow_drain else "SSLEOFError")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("suite", choices=("durable", "log", "volatile", "volatile-pre-apply", "takeover"))
    parser.add_argument("--results", type=Path, help="new JSON-lines file of observed case outcomes")
    args = parser.parse_args()
    binary, out, suite = args.binary, args.out, args.suite
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    pki = out / "pki"
    policy = pki_module.generate(pki)
    # An independent sensor can publish a fence without reclaiming the target.
    policy["principals"][0]["client_ids"].append(PUBLISHER)
    (pki / "identities.json").write_text(json.dumps(policy))
    cuts = {
        "durable": (("will.end.prepared", True), ("will.end.logged", False), ("will.end.committed", False)),
        "log": (("log.will.end.before_append", True), ("log.will.end.before_sync", False),
                ("log.will.end.after_sync", False)),
        "volatile": (("previous-snapshot", True), ("next-snapshot", False)),
        "volatile-pre-apply": (("volatile.will.end.before_apply", True),),
    }
    with ExitStack() as files:
        result_file = files.enter_context(args.results.open("x")) if args.results else None

        def play(label, action, *parameters, **settings):
            print(label, flush=True)
            start = time.monotonic_ns()
            facts = action(binary, out / label, *parameters)
            if result_file:
                row = dict(settings, **facts, verdict="passed", elapsed_ns=time.monotonic_ns() - start,
                           broker_sha256=digest)
                print(json.dumps(row, sort_keys=True), file=result_file, flush=True)

        if suite == "takeover":
            for shards in (0, 2):
                for drain in (True, False):
                    play(f"takeover-k{shards}-{drain}", takeover, pki, shards, drain,
                         suite=suite, transport="mtls", shards=shards, drain=drain)
            # Regression for R7: holding the test barrier longer than WRITE_TIMEOUT
            # (250 ms) before the takeover opens must still drain cleanly on release.
            play("takeover-k0-delayed-drain", lambda *a: takeover(*a, barrier_delay=0.30),
                 pki, 0, True, suite=suite, transport="mtls", shards=0, drain=True)
            return
        mode = "volatile" if suite == "volatile-pre-apply" else suite
        for transport, credentials in (("tcp", None), ("mtls", pki)):
            for cut, survives in cuts[suite]:
                for reason in (0, 4):
                    for shards in (0, 2):
                        label = f"{transport}-{cut}-{reason}-k{shards}"
                        play(label, durability, credentials, mode, cut, reason, shards, survives,
                             suite=suite, transport=transport, shards=shards, cut=cut, reason=reason)


if __name__ == "__main__":
    main()
