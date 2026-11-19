#!/usr/bin/env python3
"""Correlate the separate MQTT and SQLite observations from both roles."""
import argparse
from ipaddress import ip_address, ip_network
import json
from pathlib import Path
import shlex


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def check_docker(evidence, broker, peer, broker_ip, peer_ip):
    require(peer["machine"] == "aarch64", "rehearsal_peer_is_not_arm64")
    require(broker["netns"] != peer["netns"] and broker["mntns"] != peer["mntns"], "roles_share_a_namespace")
    network, = json.loads((evidence / "network.json").read_text())
    subnet, = network["IPAM"]["Config"]
    require(network["Internal"] and network["Driver"] == "bridge"
            and all(ip_address(address) in ip_network(subnet["Subnet"]) for address in (broker_ip, peer_ip)),
            "roles_do_not_share_the_private_bridge")
    mounts = [json.loads(line) for line in (evidence / "mounts.jsonl").read_text().splitlines()]
    broker_mounts, peer_mounts = mounts
    for role, mount in (("broker", broker_mounts), ("peer", peer_mounts)):
        require(len(mount) == 1 and mount[0]["Type"] == "bind" and mount[0]["Destination"] == "/role"
                and Path(mount[0]["Source"]).parts[-2:] == ("site", role), "roles_share_a_data_mount")


def identifier(event):
    return b"".join(int(event[key]).to_bytes(8, "big") for key in ("incarnation", "epoch", "owner", "sequence")).hex()


def check_puback(event, delivery):
    ack = event["puback"]
    target = ack["target"]
    require(event["kind"] == "puback_observed" and ack["reason_code"] == 0 and target is not None
            and identifier(target["claim_operation_id"]) == delivery["claim"]
            and identifier(target["delivery_id"]["publication_id"]) == delivery["publication"]
            and target["delivery_id"]["index"] == delivery["index"]
            and ack["connection"]["session"] == delivery["target_session"]
            and ack["packet_id"] == delivery["mid"], "puback_target_does_not_match_held_delivery")


def correlate(evidence, broker_ip, peer_ip, *, mode="docker"):
    broker = json.loads((evidence / "lan-broker.json").read_text())
    peer = json.loads((evidence / "lan-peer.json").read_text())
    require(broker["status"] == peer["status"] == "PASS", "role_failed")
    require(broker["config"] == peer["config"] and broker["launcher_sha256"] == peer["launcher_sha256"],
            "campaign_provenance_mismatch")
    config = broker["config"]
    require(broker["machine"] == "aarch64" and config["host"] == broker_ip and broker_ip != peer_ip,
            "unexpected_broker_platform_or_endpoints")
    if mode == "docker":
        check_docker(evidence, broker, peer, broker_ip, peer_ip)
        scope = "single-host network rehearsal"
    else:
        require(mode == "field" and peer["machine"] in ("x86_64", "aarch64"), "unsupported_field_peer")
        scope = "field observations from two role receipts"
    for record, local, remote in ((broker, broker_ip, peer_ip), (peer, peer_ip, broker_ip)):
        control = record["control"]
        require((control["local"][0], control["remote"][0], control["tls"]) == (local, remote, "TLSv1.3"),
                "control_connection_does_not_match_endpoints")

    first, second = broker["starts"]
    require(first["events"] != second["events"] and
            all(start["listener"] == f"{broker_ip}:{config['mqtt_port']}" and
                start["binary_sha256"] == config["broker"]["binary_sha256"] and start["exit"] == 0
                for start in (first, second)), "restart_did_not_use_the_frozen_broker")
    delivery = broker["held_before_puback"]["delivery"]
    recovered = peer["recovered"]
    require(all(recovered[k] == delivery[k] for k in ("topic", "payload", "mid", "qos", "retain"))
            and recovered["connack"]["session_present"] and recovered["connack"]["remote"][0] == broker_ip
            and peer["hostile_connack"]["reason"] == 0x87 and peer["mqttium_puback"]["accepted"],
            "mqtt_and_durable_observations_disagree")
    held, after = broker["held_before_puback"], broker["after_puback"]
    require((held["obligations"], held["claims"], after["obligations"], after["claims"]) == (1, 1, 0, 0),
            "wrong_puback_boundary")
    for number, expected in ((1, broker["subscriber_closed_event"]), (2, broker["puback_observed_event"])):
        events = (evidence / f"broker-{number}.events.jsonl").read_text().splitlines()
        require(any(json.loads(event) == expected for event in events), "event_missing_from_broker_stream")
    check_puback(broker["puback_observed_event"], delivery)
    sent = [{"received" if key == "sent" else "sent" if key == "received" else key: value
             for key, value in signal.items()} for signal in peer["control"]["signals"]]
    require(broker["control"]["signals"] == sent, "control_signals_disagree")
    hosts = {name: {key: record.get(key) for key in ("host", "machine", "kernel", "board")}
             for name, record in (("broker", broker), ("peer", peer))}
    return {"format": "mqttiumd.lan-restart.rehearsal.v1", "status": "PASS",
            "scope": scope, "mode": mode, "hosts": hosts,
            "campaign": config["campaign"], "broker_address": broker_ip, "peer_address": peer_ip,
            "broker": config["broker"], "broker_tar_sha256": config["broker_tar_sha256"],
            "kit_source": config["kit_source"], "publication": delivery["publication"], "delivery_index": delivery["index"],
            "claim": delivery["claim"], "packet_id": delivery["mid"], "topic": recovered["topic"],
            "payload": recovered["payload"], "qos": recovered["qos"], "session_present": True,
            "before_puback": [held["obligations"], held["claims"]],
            "after_puback": [after["obligations"], after["claims"]], "foreign_connack": 0x87,
            "mosquitto_commands": [shlex.join(command) for command in peer["mosquitto"]["commands"]]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("broker_ip")
    parser.add_argument("peer_ip")
    parser.add_argument("--mode", choices=("docker", "field"), default="docker")
    args = parser.parse_args()
    result = correlate(args.evidence, args.broker_ip, args.peer_ip, mode=args.mode)
    report = json.dumps(result, indent=2, sort_keys=True)
    name = "rehearsal.json" if args.mode == "docker" else "field.json"
    (args.evidence / name).write_text(report + "\n")
    print(report)
