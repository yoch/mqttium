#!/usr/bin/env python3
"""Exercise the receipt checker against a copy of an actual rehearsal."""
import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from check_receipts import correlate


def test_claim_mismatch(evidence, broker_ip, peer_ip):
    assert correlate(evidence, broker_ip, peer_ip)["status"] == "PASS"
    with TemporaryDirectory() as temporary:
        copy = Path(temporary) / "evidence"
        shutil.copytree(evidence, copy)
        broker_path = copy / "lan-broker.json"
        broker = json.loads(broker_path.read_text())
        delivery = broker["held_before_puback"]["delivery"]
        claim = delivery["claim"]
        delivery["claim"] = claim[:-1] + ("0" if claim[-1] != "0" else "1")
        broker_path.write_text(json.dumps(broker))
        try:
            correlate(copy, broker_ip, peer_ip)
        except ValueError as error:
            assert str(error) == "puback_target_does_not_match_held_delivery", str(error)
        else:
            raise AssertionError("a different held claim passed with the original broker PUBACK")


def test_field_inputs(evidence, broker_ip, peer_ip):
    # Change the host metadata to exercise the reader for two physical roles.
    # This is a checker test, not evidence of a Raspberry Pi run.
    with TemporaryDirectory() as temporary:
        copy = Path(temporary) / "evidence"
        shutil.copytree(evidence, copy)
        for name in ("network.json", "mounts.jsonl"):
            (copy / name).unlink()
        broker_path = copy / "lan-broker.json"
        broker = json.loads(broker_path.read_text())
        broker.update(board="Raspberry Pi 5 Model B Rev 1.0", host="pi-fixture")
        broker_path.write_text(json.dumps(broker))
        peer_path = copy / "lan-peer.json"
        peer = json.loads(peer_path.read_text())
        peer.update(machine="x86_64", host="peer-fixture", netns=broker["netns"], mntns=broker["mntns"])
        peer_path.write_text(json.dumps(peer))
        receipt = correlate(copy, broker_ip, peer_ip, mode="field")
        assert receipt["status"] == "PASS"
        assert receipt["hosts"]["broker"]["board"] == broker["board"]
        assert receipt["hosts"]["peer"]["machine"] == "x86_64"
        assert receipt["scope"] == "field observations from two role receipts"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("broker_ip")
    parser.add_argument("peer_ip")
    args = parser.parse_args()
    test_claim_mismatch(args.evidence, args.broker_ip, args.peer_ip)
    test_field_inputs(args.evidence, args.broker_ip, args.peer_ip)
    print("authentic claim mutation rejected and mixed-architecture field input accepted")
