from pathlib import Path

path = Path(".github/apply_paho_boundary.py")
text = path.read_text()

text = text.replace(
    "    def _commit_qosn_publish_on_loop(\\n",
    "    def _commit_qosn_publish_on_loop(self, request: _PendingPublish) -> MQTTMessageInfo:\\n",
)
text = text.replace(
    "    def _finalize_publish_effects(\\n",
    "    def _finalize_publish_effects(self) -> None:\\n",
)
text = text.replace(
    "    def _drain_publish_requests(\\n",
    "    def _drain_publish_requests(self) -> None:\\n",
)

old_end = '    "        if requested_qos is QoS.AT_MOST_ONCE:\\n",\n'
new_end = (
    '    "        if requested_qos is QoS.AT_MOST_ONCE:\\n'
    '            info = MQTTMessageInfo(\\n",\n'
)
if text.count(old_end) != 1:
    raise SystemExit(f"expected one publish end anchor, found {text.count(old_end)}")
text = text.replace(old_end, new_end, 1)

old_signature = """        properties: Properties | None = None,
        create_qos0_receipt: bool = True,
    ) -> PublishReceipt | None:
"""
new_signature = """        properties: Properties | None = None,
    ) -> PublishReceipt:
"""
if text.count(old_signature) != 1:
    raise SystemExit(
        f"expected one optional receipt signature, found {text.count(old_signature)}"
    )
text = text.replace(old_signature, new_signature, 1)
text = text.replace(
    '        \\\"\\\"\\\"Admit one publish and optionally register its receipt without flushing.\n',
    '        \\\"\\\"\\\"Admit one publish and register its receipt without flushing.\n',
    1,
)
old_doc = """        Keeping finalization separate lets adapters commit a bounded batch and
        collect/drain effects once. Paho's off-thread QoS 0 batch disables the
        otherwise public-facing QoS 0 receipt allocation.
"""
new_doc = """        Keeping finalization separate lets adapters commit a bounded batch and
        collect/drain effects once.
"""
if text.count(old_doc) != 1:
    raise SystemExit(f"expected one receipt doc block, found {text.count(old_doc)}")
text = text.replace(old_doc, new_doc, 1)

old_qos0_return = """        if handle.qos == QoS.AT_MOST_ONCE:
            if not create_qos0_receipt:
                return None
            return PublishReceipt(mid=None, qos=handle.qos, _event=None)
"""
new_qos0_return = """        if handle.qos == QoS.AT_MOST_ONCE:
            return PublishReceipt(mid=None, qos=handle.qos, _event=None)
"""
if text.count(old_qos0_return) != 1:
    raise SystemExit(
        f"expected one optional QoS0 return, found {text.count(old_qos0_return)}"
    )
text = text.replace(old_qos0_return, new_qos0_return, 1)

finalize_marker = """    def _finalize_loop_commands(self) -> None:
"""
qos0_method = '''    def _queue_qos0_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        retain: bool,
        properties: Properties | None = None,
    ) -> None:
        \\\"\\\"\\\"Admit QoS 0 without allocating a receipt, for batched adapters.\\\"\\\"\\\"
        self._engine.queue_publish(
            topic,
            payload,
            qos=QoS.AT_MOST_ONCE,
            retain=retain,
            properties=properties,
        )

'''
if text.count(finalize_marker) != 1:
    raise SystemExit(
        f"expected one finalization marker, found {text.count(finalize_marker)}"
    )
text = text.replace(finalize_marker, qos0_method + finalize_marker, 1)

old_batch = '''                    self._async._queue_publish_on_loop(
                        request.topic,
                        request.payload if request.payload is not None else b\\\"\\\",
                        qos=request.qos,
                        retain=request.retain,
                        create_qos0_receipt=False,
                    )
'''
new_batch = '''                    self._async._queue_qos0_on_loop(
                        request.topic,
                        request.payload if request.payload is not None else b\\\"\\\",
                        retain=request.retain,
                    )
'''
if text.count(old_batch) != 1:
    raise SystemExit(f"expected one Paho QoS0 batch call, found {text.count(old_batch)}")
text = text.replace(old_batch, new_batch, 1)

old_return = """                return receipt
            try:
"""
new_return = """                assert receipt is not None
            return receipt
            try:
"""
if text.count(old_return) != 1:
    raise SystemExit(f"expected one async receipt return, found {text.count(old_return)}")
text = text.replace(old_return, new_return, 1)

path.write_text(text)
