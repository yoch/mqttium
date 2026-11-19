#!/usr/bin/env python3
"""Correlate the separate MQTT and SQLite observations from the rehearsal."""
import argparse
from ipaddress import ip_address, ip_network
import json
from pathlib import Path
import shlex


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def correlate(evidence, broker_ip, peer_ip):
    broker = json.loads((evidence / "lan-broker.json").read_text())
    peer = json.loads((evidence / "lan-peer.json").read_text())
    require(broker["status"] == peer["status"] == "PASS", "role_failed")
    require(broker["config"] == peer["config"] and broker["launcher_sha256"] == peer["launcher_sha256"],
            "campaign_provenance_mismatch")
    config = broker["config"]
    require(broker["machine"] == peer["machine"] == "aarch64" and config["host"] == broker_ip,
            "rehearsal_did_not_use_native_arm64_and_the_expected_endpoint")
    require(broker["netns"] != peer["netns"] and broker["mntns"] != peer["mntns"], "roles_share_a_namespace")
    network, = json.loads((evidence / "network.json").read_text())
    subnet, = network["IPAM"]["Config"]
    require(network["Internal"] and network["Driver"] == "bridge"
            and all(ip_address(address) in ip_network(subnet["Subnet"]) for address in (broker_ip, peer_ip)),
            "roles_do_not_share_the_private_bridge")
    for record, local, remote in ((broker, broker_ip, peer_ip), (peer, peer_ip, broker_ip)):
        control = record["control"]
        require((control["local"][0], control["remote"][0], control["tls"]) == (local, remote, "TLSv1.3"),
                "control_connection_did_not_cross_the_bridge")
    mounts = [json.loads(line) for line in (evidence / "mounts.jsonl").read_text().splitlines()]
    broker_mounts, peer_mounts = mounts
    for role, mount in (("broker", broker_mounts), ("peer", peer_mounts)):
        require(len(mount) == 1 and mount[0]["Type"] == "bind" and mount[0]["Destination"] == "/role"
                and Path(mount[0]["Source"]).parts[-2:] == ("site", role), "roles_share_a_data_mount")

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
    sent = [{"received" if key == "sent" else "sent" if key == "received" else key: value
             for key, value in signal.items()} for signal in peer["control"]["signals"]]
    require(broker["control"]["signals"] == sent, "control_signals_disagree")
    return {"format": "mqttiumd.lan-restart.rehearsal.v1", "status": "PASS",
            "scope": "single-host network rehearsal; no physical Raspberry Pi or LAN qualification",
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
    args = parser.parse_args()
    result = correlate(args.evidence, args.broker_ip, args.peer_ip)
    report = json.dumps(result, indent=2, sort_keys=True)
    (args.evidence / "rehearsal.json").write_text(report + "\n")
    print(report)
