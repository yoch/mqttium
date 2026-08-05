from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1))


async_path = "src/mqttium/api/async_client.py"
marker = """    def _queue_qos0_on_loop(
"""
specialized = '''    def _queue_qosn_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: QoS,
        retain: bool,
        properties: Properties | None = None,
    ) -> PublishReceipt:
        """Admit QoS 1/2 and register its receipt for loop-bound adapters."""
        handle = self._engine.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        assert handle.mid is not None
        receipt = PublishReceipt(
            mid=handle.mid,
            qos=handle.qos,
            _event=asyncio.Event(),
        )
        self._register_publish_receipt(handle.mid, receipt)
        return receipt

'''
replace_once(async_path, marker, specialized + marker)

paho_path = "src/mqttium/compat/paho.py"
replace_once(
    paho_path,
    """        self._loop: asyncio.AbstractEventLoop | None = None
""",
    """        # Cache the hot adapter boundary methods once. This avoids repeated
        # AsyncClient attribute traversal in every Paho publication while keeping
        # protocol and receipt state behind the AsyncClient boundary.
        self._queue_qosn_on_loop = self._async._queue_qosn_on_loop
        self._queue_qos0_on_loop = self._async._queue_qos0_on_loop
        self._finalize_async_commands = self._async._finalize_loop_commands
        self._loop: asyncio.AbstractEventLoop | None = None
""",
)
replace_once(
    paho_path,
    """        receipt = self._async._queue_publish_on_loop(
            request.topic,
            request.payload if request.payload is not None else b"",
            qos=request.qos,
            retain=request.retain,
        )
""",
    """        receipt = self._queue_qosn_on_loop(
            request.topic,
            request.payload if request.payload is not None else b"",
            qos=request.qos,
            retain=request.retain,
        )
""",
)
replace_once(
    paho_path,
    """        self._async._finalize_loop_commands()
""",
    """        self._finalize_async_commands()
""",
)
replace_once(
    paho_path,
    """                    self._async._queue_qos0_on_loop(
                        request.topic,
                        request.payload if request.payload is not None else b"",
                        retain=request.retain,
                    )
""",
    """                    self._queue_qos0_on_loop(
                        request.topic,
                        request.payload if request.payload is not None else b"",
                        retain=request.retain,
                    )
""",
)

# Make the loop-thread QoS>0 branch use the same specialized boundary. QoS 0
# keeps the general method because it needs a receipt for MQTTMessageInfo.
old_loop = """        if self._on_network_thread():
            try:
                receipt = self._async._queue_publish_on_loop(
                    topic,
                    data,
                    qos=requested_qos,
                    retain=retain,
                )
                info = MQTTMessageInfo(
"""
new_loop = """        if self._on_network_thread():
            try:
                if requested_qos is QoS.AT_MOST_ONCE:
                    receipt = self._async._queue_publish_on_loop(
                        topic,
                        data,
                        qos=requested_qos,
                        retain=retain,
                    )
                else:
                    receipt = self._queue_qosn_on_loop(
                        topic,
                        data,
                        qos=requested_qos,
                        retain=retain,
                    )
                info = MQTTMessageInfo(
"""
replace_once(paho_path, old_loop, new_loop)

# Remove temporary measurement/publication harnesses after this workflow has
# used them. They must never remain in the review diff.
for temporary in (
    ".github/compat_boundary_ab.py",
    ".github/workflows/compat-boundary-ab.yml",
    ".github/optimize_paho_boundary.py",
    ".github/workflows/optimize-paho-boundary.yml",
):
    Path(temporary).unlink(missing_ok=True)
