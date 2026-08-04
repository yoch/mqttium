from pathlib import Path


def replace(path: str, replacements: dict[str, str]) -> None:
    target = Path(path)
    content = target.read_text()
    for before, after in replacements.items():
        if before not in content:
            raise RuntimeError(f"Expected formatting block not found in {path}: {before!r}")
        content = content.replace(before, after, 1)
    target.write_text(content)


replace(
    "src/mqttium/api/async_client.py",
    {
        """        if (\n            max_pending_outbound_messages is not None\n            and max_pending_outbound_messages < 0\n        ):\n""": """        if max_pending_outbound_messages is not None and max_pending_outbound_messages < 0:\n""",
    },
)

replace(
    "src/mqttium/protocol/engine.py",
    {
        """        if (\n            self.max_pending_outbound_bytes is not None\n            and self.max_pending_outbound_bytes < 0\n        ):\n""": """        if self.max_pending_outbound_bytes is not None and self.max_pending_outbound_bytes < 0:\n""",
        """    def _hydrate_outbound_message(\n        self, msg: OutboundMessage | OutboundMessageSummary\n    ) -> None:\n""": """    def _hydrate_outbound_message(self, msg: OutboundMessage | OutboundMessageSummary) -> None:\n""",
        """    def _replay_outbound_message(\n        self, msg: OutboundMessage | OutboundMessageSummary\n    ) -> None:\n""": """    def _replay_outbound_message(self, msg: OutboundMessage | OutboundMessageSummary) -> None:\n""",
        """        return self._outbound_logical_size_from_size(\n            topic, len(payload), properties\n        )\n""": """        return self._outbound_logical_size_from_size(topic, len(payload), properties)\n""",
        """        payload_size = (\n            len(stored.payload)\n            if isinstance(stored, OutboundMessage)\n            else stored.payload_size\n        )\n        return self._outbound_logical_size_from_size(\n            stored.topic, payload_size, stored.properties\n        )\n""": """        payload_size = (\n            len(stored.payload) if isinstance(stored, OutboundMessage) else stored.payload_size\n        )\n        return self._outbound_logical_size_from_size(stored.topic, payload_size, stored.properties)\n""",
        """        self._check_outbound_publish_size(\n            topic, len(payload), qos, properties\n        )\n""": """        self._check_outbound_publish_size(topic, len(payload), qos, properties)\n""",
        """        encoded_publish = (\n            msg.encoded_publish if isinstance(msg, OutboundMessage) else None\n        )\n""": """        encoded_publish = msg.encoded_publish if isinstance(msg, OutboundMessage) else None\n""",
        """            payload_size = (\n                len(msg.payload)\n                if isinstance(msg, OutboundMessage)\n                else msg.payload_size\n            )\n            self._check_outbound_publish_size(\n                msg.topic, payload_size, msg.qos, msg.properties\n            )\n""": """            payload_size = (\n                len(msg.payload) if isinstance(msg, OutboundMessage) else msg.payload_size\n            )\n            self._check_outbound_publish_size(msg.topic, payload_size, msg.qos, msg.properties)\n""",
    },
)
