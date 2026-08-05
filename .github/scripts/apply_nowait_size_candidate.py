from __future__ import annotations

import sys
from pathlib import Path


def apply(root: Path) -> None:
    outbound_path = root / "src/mqttium/protocol/outbound.py"
    outbound = outbound_path.read_text()
    old_import = "from mqttium.codec.vbi import encode_vbi\n"
    if old_import not in outbound:
        raise RuntimeError("outbound VBI import did not match")
    outbound = outbound.replace(old_import, "from mqttium.codec.vbi import vbi_len\n", 1)

    marker = """    def _check_publish_wire_size(
        self,
        topic_bytes: int,
        wire_property_bytes: int,
        payload_size: int,
        qos: QoS,
    ) -> None:
"""
    if marker not in outbound:
        raise RuntimeError("wire-size marker did not match")
    helpers = """    @staticmethod
    def _publish_wire_size_from_parts(
        topic_bytes: int,
        wire_property_bytes: int,
        payload_size: int,
        qos: QoS,
    ) -> int:
        remaining = 2 + topic_bytes + (2 if qos else 0) + wire_property_bytes + payload_size
        return 1 + vbi_len(remaining) + remaining

    def publish_wire_size(
        self,
        topic: str,
        payload_size: int,
        qos: QoS | int,
        properties: Properties | None,
    ) -> int:
        level = QoS(qos)
        topic_bytes, wire_property_bytes, _ = self.size_parts(topic, properties)
        return self._publish_wire_size_from_parts(
            topic_bytes, wire_property_bytes, payload_size, level
        )

"""
    outbound = outbound.replace(marker, helpers + marker, 1)
    old_size = """        remaining = 2 + topic_bytes + (2 if qos else 0) + wire_property_bytes + payload_size
        encoded_size = 1 + len(encode_vbi(remaining)) + remaining
"""
    new_size = """        encoded_size = self._publish_wire_size_from_parts(
            topic_bytes, wire_property_bytes, payload_size, qos
        )
"""
    if old_size not in outbound:
        raise RuntimeError("wire-size body did not match")
    outbound_path.write_text(outbound.replace(old_size, new_size, 1))

    client_path = root / "src/mqttium/api/async_client.py"
    client = client_path.read_text()
    import_line = "from mqttium.transport.writes import WriteItem, item_size\n"
    if import_line not in client:
        raise RuntimeError("transport write import did not match")
    client = client.replace(import_line, "from mqttium.transport.writes import WriteItem\n", 1)
    packet_import = "    PublishPacket,\n"
    if packet_import not in client:
        raise RuntimeError("PublishPacket import did not match")
    client = client.replace(packet_import, "", 1)

    old_preview = """    def _preview_publish_write(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        retain: bool,
        properties: Properties | None,
    ) -> WriteItem:
        level = QoS(qos)
        return PublishPacket(
            topic=topic,
            payload=payload,
            qos=level,
            retain=retain,
            dup=False,
            mid=None if level == QoS.AT_MOST_ONCE else 1,
            properties=properties,
        ).encode_write_item(self._engine.config.protocol)

"""
    new_preview = """    def _preview_publish_size(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        properties: Properties | None,
    ) -> int:
        return self._engine.outbound.publish_wire_size(
            topic, len(payload), qos, properties
        )

"""
    if old_preview not in client:
        raise RuntimeError("preview method did not match")
    client = client.replace(old_preview, new_preview, 1)

    old_call = "size = item_size(self._preview_publish_write(topic, payload, level, retain, properties))"
    new_call = "size = self._preview_publish_size(topic, payload, level, properties)"
    if client.count(old_call) != 2:
        raise RuntimeError(f"expected two preview calls, found {client.count(old_call)}")
    client = client.replace(old_call, new_call)
    client = client.replace(
        "for topic, payload, qos, retain, properties in requests:\n",
        "for topic, payload, qos, _retain, properties in requests:\n",
        1,
    )
    client_path.write_text(client)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_nowait_size_candidate.py ROOT")
    apply(Path(sys.argv[1]))
