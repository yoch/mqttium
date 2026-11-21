#!/usr/bin/env python3
"""Real writer boundaries for hierarchical routing; system tests, not qualification.

Raw MQTT observations locate the routing cut independently of the product codec.
Every case owns its process, SQLite file, observer and cleanup. No SQL writes or
fabricated store results are used. The optional lost-reply control suppresses an
actual committed Subscriptions reply, then the existing resolver supplies it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from pubsub_probe import PubsubWire, decode_filter_ack, durable, session_key
from pubsub_reception import accounts_consistent, drained, obligations, until
from qos0_reception import message_is
from tcp_probe import ReceiptFailure, operation_hex, packet, require, utf8, write_json
from tcp_reception import Broker, writer_available

POINT = "subscription.prepared"
RESOLUTION_TIMEOUT = 15
QUIET_SECONDS = .15


def subscribe_options(identifier: int, filters: list[tuple[str, int, int]]) -> bytes:
    """Independent MQTT 5 options byte; no product/client private API."""
    if not 1 <= identifier <= 65535 or not 1 <= len(filters) <= 8:
        raise ValueError("bounded nonempty subscription and identifier required")
    body = identifier.to_bytes(2, "big") + b"\x00"
    for name, qos, options in filters:
        raw = name.encode("utf-8")
        if (not raw or len(raw) > 1024 or qos not in (0, 1, 2)
                or not 0 <= options <= 255 or options & 0xC3
                or (options >> 4) & 3 == 3):
            raise ValueError("invalid bounded filter/options")
        body += utf8(name) + bytes([qos | options])
    return packet(0x82, body)


def sequence(broker: Broker) -> int:
    return max((int(event["seq"]) for event in broker.events()), default=0)


def later(broker: Broker, after: int) -> list[dict]:
    return [event for event in broker.events() if int(event["seq"]) > after]


def snapshot(broker: Broker, case: Path, name: str) -> dict:
    state = durable(broker.db)
    accounts_consistent(state)
    path = case / f"{name}.sql.json"
    write_json(path, state)
    broker.observations.record("durable_snapshot", path=path.name,
                               sha256=hash_file(path), sqlite_inode=broker.db.stat().st_ino)
    return state


def route_row(state: dict, client: str, filter_name: str) -> dict:
    target = session_key(state, client)
    rows = [row for row in state["tables"]["routes"]
            if row["session"] == target and row["topic"] == filter_name.encode().hex()]
    require(len(rows) == 1, "wildcard_route_not_unique", filter_name)
    return rows[0]


def receipt_row(state: dict, event: dict) -> dict:
    identity = operation_hex(event["operation"])
    rows = [row for row in state["tables"]["operations"] if row["id"] == identity]
    require(len(rows) == 1, "subscription_receipt_identity_not_unique")
    raw = bytes.fromhex(rows[0]["recipe"])
    kinds = subscription_recipe(raw, identity, require_options=True)
    return {"row": rows[0], "canonical_sha256": hashlib.sha256(raw).hexdigest(),
            "canonical_bytes": len(raw), **kinds, "operation_hex": identity}


def subscription_recipe(raw: bytes, identity: str, *, require_options: bool = False) -> dict:
    """Read only base7/9, legacy12→7/9, or timed14→12→7/9.

    Tags are checked before each bounded unwrap. The temporal suffix is one
    domain32/tick8 observation, not another operation or a renewed identity.
    """
    magic = b"MQDR\x00\x01"
    require(len(raw) >= 7 and raw[:6] == magic,
            "subscription_options_missing_from_canonical_recipe")
    outer = raw[6]
    inner = raw
    if outer == 14:
        size = int.from_bytes(raw[7:11], "big")
        require(39 <= size <= 65536 and len(raw) == 11 + size + 40,
                "subscription_time_envelope_invalid")
        inner = raw[11:11 + size]
        require(inner[:6] == magic and inner[6] == 12,
                "subscription_time_child_invalid")
    semantic = inner[6]
    if semantic == 12:
        size = int.from_bytes(inner[7:11], "big")
        require(39 <= size <= 65536 and len(inner) == 11 + size + 32,
                "subscription_replay_envelope_invalid")
        first = inner[-32:]
        sequence = int.from_bytes(first[24:], "big")
        require(first[:24].hex() == identity[:48] and 1 <= sequence <= 2**64 - 256,
                "subscription_replay_identity_range_invalid")
        inner = inner[11:11 + size]
    else:
        require(outer == 9, "subscription_canonical_kind_invalid")
    require(len(inner) >= 64 and len(inner) <= 65536 and inner[:6] == magic
            and inner[6] in ((9,) if require_options else (7, 9))
            and inner[7:39].hex() == identity,
            "subscription_options_missing_from_canonical_recipe")
    count, at, nondefault = inner[63], 64, False
    require(1 <= count <= 8, "subscription_canonical_filter_count_invalid")
    for _ in range(count):
        width = 4 if inner[6] == 9 else 3
        require(at + width <= len(inner), "subscription_canonical_filter_truncated")
        granted = inner[at]
        options = inner[at + 1] if inner[6] == 9 else 0
        length_at = at + width - 2
        length = int.from_bytes(inner[length_at:length_at + 2], "big")
        require(granted in (0, 1, 2, 3) and not options & 0xC3 and (options >> 4) & 3 != 3
                and (granted != 0 or options == 0) and 1 <= length <= 1024,
                "subscription_canonical_filter_invalid")
        at += width + length
        require(at <= len(inner), "subscription_canonical_filter_truncated")
        nondefault |= options != 0
    require(at == len(inner) and (inner[6] != 9 or nondefault),
            "subscription_canonical_trailing_or_alias")
    return {"canonical_kind": outer, "semantic_kind": semantic, "inner_kind": inner[6]}


def expect_quiet_update(wire: PubsubWire) -> None:
    require(not wire.publications and not wire.controls, "boundary_probe_not_drained")
    frame = wire.poll(QUIET_SECONDS)
    if frame is None:
        return
    if frame.kind == 0x90:
        _, reasons = decode_filter_ack(frame)
        require(not any(reason < 128 for reason in reasons),
                "positive_suback_before_known_subscription_commit")
        raise ReceiptFailure("wildcard_subscription_refused_during_cut", repr(reasons))
    if frame.kind == 0x30:
        raise ReceiptFailure("volatile_publication_bypassed_pending_wildcard_cut")
    raise ReceiptFailure("unexpected_frame_during_subscription_cut", hex(frame.header))


def ping(wire: PubsubWire, broker: Broker) -> None:
    start = time.monotonic_ns()
    wire.send(b"\xc0\x00")
    wire.wait(0xD0, timeout=2)
    broker.observations.record("independent_ping_completed", label=wire.label,
                               elapsed_ns=str(time.monotonic_ns() - start))


def suback_after(broker: Broker, target: PubsubWire, identifier: int,
                 expected: int, known_event: dict) -> dict:
    actual, reasons = decode_filter_ack(target.wait(0x90, identifier, timeout=RESOLUTION_TIMEOUT))
    require(actual == identifier and reasons == (expected,), "wildcard_suback_result_mismatch")
    acknowledgements = [event for event in broker.events()
                        if event["kind"] == "frame_queued"
                        and event.get("details", {}).get("packet_kind") == "SUBACK"
                        and event["details"].get("packet_id") == identifier]
    require(len(acknowledgements) == 1 and int(acknowledgements[0]["seq"]) > int(known_event["seq"]),
            "suback_preceded_known_routing_cut")
    broker.observations.record("suback_received", packet_id=actual, reasons=list(reasons),
                               known_event_seq=known_event["seq"])
    return acknowledgements[0]


def assert_no_pre_cut_decision(broker: Broker, after: int) -> None:
    require(not any(event["kind"] == "qos0_dropped" for event in later(broker, after)),
            "volatile_publication_used_pre_cut_routing_or_absence")
    require(not any(event["kind"] == "frame_queued"
                    and event.get("details", {}).get("packet_kind") == "SUBACK"
                    and event["details"].get("packet_id") == 2
                    for event in later(broker, after)),
            "positive_suback_before_known_subscription_commit")


def post_cut(broker: Broker, target: PubsubWire, source: PubsubWire,
             case: Path, client: str, filter_name: str, topic: str,
             marker: int, known: dict, options: int) -> dict:
    ack = suback_after(broker, target, 2, 0, known)
    observed = target.next_publication()
    message_is(observed, topic, b"other-during-cut", 0,
               "deferred_volatile_publication_used_wrong_projection")
    target.no_publication(QUIET_SECONDS, code="no_local_volatile_self_publication_escaped")
    drops = until(lambda: [event for event in later(broker, marker)
                          if event["kind"] == "qos0_dropped"
                          and event.get("details", {}).get("reason") == "no_subscribers"],
                  "deferred_no_local_decision_missing")
    require(len(drops) == 1 and int(drops[0]["seq"]) > int(known["seq"]),
            "no_local_decision_preceded_known_cut")
    state = snapshot(broker, case, "known-projection")
    row = route_row(state, client, filter_name)
    require(row["qos"] == 0 and row["options"] == options,
            "wildcard_subscription_options_not_applied")
    require(not state["admissions"] and not obligations(state),
            "pure_volatile_routing_cut_forced_durable_admission")
    source.publish(7, topic, b"durable-after-known-cut", qos=2)
    delivered = target.next_publication()
    message_is(delivered, topic, b"durable-after-known-cut", 0,
               "durable_admission_used_pre_cut_projection")
    final = drained(broker, case)
    return {"suback_queued": ack, "volatile_message": observed.observation(),
            "self_drop": drops[0], "durable_message": delivered.observation(),
            "projection": row, "final_counts": final["counts"]}


def prepared_cut(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=(POINT,))
    client, filter_name, topic = "prepared-wildcard-target", "boundary/+/value", "boundary/one/value"
    try:
        ready = broker.ready()
        with PubsubWire(broker.address, broker.observations, "target") as target, \
                PubsubWire(broker.address, broker.observations, "independent-source") as source:
            target.connect(client, session_present=False)
            source.connect("prepared-independent-source", session_present=False)
            # Prepare the original route normally; only the replacement is held.
            broker.release(POINT)
            target.subscribe(1, [(filter_name, 1)], expected=(1,))
            before = snapshot(broker, case, "old-projection")
            old = route_row(before, client, filter_name)
            require(old["qos"] == 1 and old["options"] == 0, "old_projection_precondition")
            mark = sequence(broker)
            options = 0x2C  # No Local, RAP, RH2; non-retained input remains non-retained.
            target.send(subscribe_options(2, [(filter_name, 0, options)]))
            hit = broker.wait_event("barrier_hit", point=POINT, after=mark)
            require(hit.get("command_kind") == "Subscriptions", "wrong_subscription_barrier")
            held = snapshot(broker, case, "prepared-uncommitted")
            require(route_row(held, client, filter_name) == old,
                    "wildcard_projection_visible_before_commit")
            identity = operation_hex(hit["operation"])
            require(not any(row["id"] == identity for row in held["tables"]["operations"]),
                    "subscription_receipt_visible_before_commit")
            target.publish_qos0(topic, b"self-during-cut")
            source.publish_qos0(topic, b"other-during-cut")
            expect_quiet_update(target)
            ping(source, broker)
            assert_no_pre_cut_decision(broker, mark)
            broker.release(POINT)
            committed = broker.wait_event("store_result", after=int(hit["seq"]))
            require(committed.get("operation") == hit["operation"]
                    and committed.get("command_kind") == "Subscriptions"
                    and committed.get("details", {}).get("committed_now") is True,
                    "subscription_not_really_committed")
            result = post_cut(broker, target, source, case, client, filter_name,
                              topic, mark, committed, options)
            result.update(criterion="D-prepared", status="PASS", ready=ready,
                          point=hit, commit=committed,
                          canonical=receipt_row(snapshot(broker, case, "after-prepared-cut"), committed))
        result["process"] = {"pid": broker.process.pid, "sqlite_inode": broker.db.stat().st_ino,
                             "stop_status": broker.stop()}
        writer_available(broker.db)
        return result
    finally:
        broker.cleanup()


def lost_result(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    client, filter_name, topic = "lost-wildcard-target", "lost/#", "lost/one/value"
    try:
        ready = broker.ready()
        with PubsubWire(broker.address, broker.observations, "target") as target, \
                PubsubWire(broker.address, broker.observations, "independent-source") as source:
            target.connect(client, session_present=False)
            source.connect("lost-independent-source", session_present=False)
            target.subscribe(1, [(filter_name, 1)], expected=(1,))
            old = route_row(snapshot(broker, case, "old-projection"), client, filter_name)
            mark = sequence(broker)
            options = 0x1C  # No Local, RAP, RH1, all preserved in canonical kind9.
            control = broker.control / "drop-next-subscriptions-result"
            control.touch(exist_ok=False)
            broker.observations.record("fault_armed", control=control.name)
            target.send(subscribe_options(2, [(filter_name, 0, options)]))
            dropped = broker.wait_event("store_result_dropped", after=mark)
            require(dropped.get("command_kind") == "Subscriptions" and not control.exists(),
                    "subscription_result_drop_not_reached")
            committed = snapshot(broker, case, "committed-result-lost")
            row = route_row(committed, client, filter_name)
            require(old["qos"] == 1 and old["options"] == 0
                    and row["qos"] == 0 and row["options"] == options,
                    "lost_subscription_result_not_committed")
            canonical = receipt_row(committed, dropped)
            target.publish_qos0(topic, b"self-during-cut")
            source.publish_qos0(topic, b"other-during-cut")
            expect_quiet_update(target)
            ping(source, broker)
            assert_no_pre_cut_decision(broker, mark)
            # This is the existing bounded resolver, not a second SUBSCRIBE.
            received_ack = target.wait(0x90, 2, timeout=RESOLUTION_TIMEOUT)
            resolved = broker.wait_event("store_resolved", after=int(dropped["seq"]),
                                         timeout=RESOLUTION_TIMEOUT)
            require(resolved.get("operation") == dropped["operation"]
                    and resolved.get("details", {}).get("outcome") == "committed",
                    "subscription_resolution_changed_operation")
            # Consume the actual retained frame through the common assertions;
            # no packet or store result is fabricated or transmitted again.
            target.controls.append(received_ack)
            result = post_cut(broker, target, source, case, client, filter_name,
                              topic, mark, resolved, options)
            retained = receipt_row(snapshot(broker, case, "after-resolution"), dropped)
            require(retained == canonical, "subscription_resolution_changed_canonical_recipe")
            actual_commits = [event for event in later(broker, mark)
                              if event["kind"] == "store_result"
                              and event.get("operation") == dropped["operation"]
                              and event.get("details", {}).get("committed_now") is True]
            submissions = [event for event in later(broker, mark)
                           if event["kind"] == "command_submitted"
                           and event.get("operation") == dropped["operation"]]
            require(len(actual_commits) == 1 and len(submissions) == 1,
                    "subscription_resolution_reexecuted_command")
            result.update(criterion="D-lost-result", status="PASS", ready=ready,
                          dropped=dropped, resolution=resolved, canonical=canonical,
                          actual_commit_count=len(actual_commits),
                          submission_count=len(submissions))
        result["process"] = {"pid": broker.process.pid, "sqlite_inode": broker.db.stat().st_ino,
                             "stop_status": broker.stop()}
        writer_available(broker.db)
        return result
    finally:
        broker.cleanup()


def frozen_responsibility(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        topic, filter_name = "frozen/one", "frozen/#"
        with PubsubWire(broker.address, broker.observations, "target") as target, \
                PubsubWire(broker.address, broker.observations, "independent-source") as source:
            # One held QoS1 claim remains live while another work slot can
            # discover the newer QoS0 obligation; this is a routing-cut test.
            target.connect("frozen-target", session_present=False, receive_maximum=2)
            source.connect("frozen-source", session_present=False)
            target.subscribe(1, [(filter_name, 1)], expected=(1,))
            source.publish(7, topic, b"old-qos2-responsibility", qos=2)
            original = target.next_publication()
            message_is(original, topic, b"old-qos2-responsibility", 1)
            before = snapshot(broker, case, "original-unacknowledged")
            target_id = session_key(before, "frozen-target")
            original_obligations = obligations(before, target_id)
            original_claims = before["tables"]["outbound"]
            require(len(original_obligations) == 1 and len(original_claims) == 1,
                    "frozen_claim_precondition")
            target.send(subscribe_options(2, [(filter_name, 0, 4)]))
            require(decode_filter_ack(target.wait(0x90, 2)) == (2, (0,)), "replacement_suback_missing")
            source.publish(8, topic, b"new-qos0-responsibility", qos=2)
            current = target.next_publication()
            message_is(current, topic, b"new-qos0-responsibility", 0)
            target.unsubscribe(3, [filter_name])
            after = snapshot(broker, case, "after-replacement-and-removal")
            require(obligations(after, target_id) == original_obligations
                    and after["tables"]["outbound"] == original_claims,
                    "admitted_wildcard_obligation_changed_after_subscription_update")
            require(not after["tables"]["routes"], "wildcard_unsubscribe_not_durable")
            source.publish(9, topic, b"after-removal-no-destination", qos=2)
            target.no_publication(QUIET_SECONDS, code="removed_wildcard_created_new_obligation")
            target.acknowledge(original)
            final = drained(broker, case)
        result = {"criterion": "G-frozen", "status": "PASS", "ready": ready,
                  "old_qos2_delivery": original.observation(), "new_qos0_delivery": current.observation(),
                  "original_obligations": original_obligations, "original_claims": original_claims,
                  "final_counts": final["counts"],
                  "process": {"pid": broker.process.pid, "sqlite_inode": broker.db.stat().st_ino,
                              "stop_status": broker.stop()}}
        writer_available(broker.db)
        return result
    finally:
        broker.cleanup()


def hash_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--binary-source-head", required=True)
    parser.add_argument("--case", choices=("all", "prepared", "lost", "frozen"), default="all")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True, timeout=3).strip()
    receipt = {"format": "mqttiumd.wildcard-boundary-system-reception", "schema_version": 1,
               "qualification": "system-test-only", "source_head": args.binary_source_head,
               "harness_source_head": head, "status": "FAIL", "cases": [],
               "command": [sys.executable, *sys.argv],
               "parameters": {"case": args.case, "resolution_timeout_seconds": RESOLUTION_TIMEOUT,
                              "quiet_observation_seconds": QUIET_SECONDS, "maximum_process_runs": 3},
               "binary": {"path": str(args.binary.resolve()), "sha256": hash_file(args.binary),
                          "features_required": ["test-hooks", "mqttiumd-store-sqlite/kill-points"]},
               "harness_sha256": hash_file(Path(__file__).resolve()),
               "harness_files": {name: hash_file(Path(__file__).with_name(name))
                                 for name in ("wildcard_boundaries.py", "pubsub_probe.py",
                                              "pubsub_reception.py", "qos0_reception.py",
                                              "tcp_probe.py", "tcp_reception.py")},
               "python": sys.version}
    try:
        require(args.binary.is_file(), "binary_missing")
        paths = ["mqttiumd/crates", "mqttiumd/Cargo.toml", "mqttiumd/Cargo.lock",
                 "mqttiumd/rust-toolchain.toml"]
        compared = subprocess.run(["git", "-C", str(source), "diff", "--quiet",
                                   args.binary_source_head, "--", *paths], timeout=3)
        require(compared.returncode == 0, "boundary_binary_source_mismatch")
        receipt["broker_source_matches"] = True
        receipt["broker_compared_paths"] = paths
        receipt["harness_tracked"] = subprocess.run(
            ["git", "-C", str(source), "ls-files", "--error-unmatch", str(Path(__file__).resolve())],
            capture_output=True, timeout=3).returncode == 0
        functions = {"prepared": prepared_cut, "lost": lost_result, "frozen": frozen_responsibility}
        for name in functions if args.case == "all" else (args.case,):
            value = functions[name](args.binary.resolve(), args.out / name)
            receipt["cases"].append(value)
            print(f"{name}: {value['status']}", flush=True)
        receipt["status"] = "PASS"
    except Exception as error:
        receipt["failure"] = {"code": getattr(error, "code", type(error).__name__),
                              "detail": str(error)[:1024]}
    finally:
        write_json(args.out / "receipt.json", receipt)
    print(json.dumps({"status": receipt["status"], "failure": receipt.get("failure"),
                      "receipt": str(args.out / "receipt.json")}))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
