from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:80]!r}")
    file.write_text(text.replace(old, new, 1))


def replace_section(path: str, start: str, end: str, replacement: str) -> None:
    file = Path(path)
    text = file.read_text()
    left = text.index(start)
    right = text.index(end, left)
    file.write_text(text[:left] + replacement + text[right:])


def replace_section_after(
    path: str,
    anchor: str,
    start: str,
    end: str,
    replacement: str,
) -> None:
    file = Path(path)
    text = file.read_text()
    anchor_at = text.index(anchor)
    left = text.index(start, anchor_at)
    right = text.index(end, left)
    file.write_text(text[:left] + replacement + text[right:])


replace_once(
    "src/mqttium/protocol/engine.py",
    """    def take_effects(self) -> list[EngineEffect]:
        effects = self._effects
        self._effects = []
        return effects

""",
    """    def take_effects(self) -> list[EngineEffect]:
        effects = self._effects
        self._effects = []
        return effects

    def reconfigure(self, **changes: Any) -> None:
        \"\"\"Validate and apply fields that are safe to change after construction.\"\"\"
        self.config.update(**changes)

""",
)

async_path = "src/mqttium/api/async_client.py"
replace_once(
    async_path,
    """    @property
    def effective_client_id(self) -> str:
        return self.negotiated.effective_client_id(self._engine.config.client_id)

""",
    """    @property
    def effective_client_id(self) -> str:
        return self.negotiated.effective_client_id(self._engine.config.client_id)

    @property
    def last_disconnect(self) -> DisconnectInfo | None:
        \"\"\"Details from the most recent broker or transport disconnect.\"\"\"
        return self._last_disconnect

    @property
    def _compat_connection_settings(self) -> tuple[str, int, int]:
        \"\"\"Loop-confined connection target used by the Paho adapter.\"\"\"
        return self._host, self._port, self._engine.config.keepalive

    def _reconfigure(self, **changes: Any) -> None:
        \"\"\"Internal loop-confined configuration boundary for adapters.\"\"\"
        self._engine.reconfigure(**changes)

    def _queue_publish_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int | QoS,
        retain: bool,
        properties: Properties | None = None,
        create_qos0_receipt: bool = True,
    ) -> PublishReceipt | None:
        \"\"\"Admit one publish and optionally register its receipt without flushing.

        The caller must already execute synchronously on the client's event loop.
        Keeping finalization separate lets adapters commit a bounded batch and
        collect/drain effects once. Paho's off-thread QoS 0 batch disables the
        otherwise public-facing QoS 0 receipt allocation.
        \"\"\"
        handle = self._engine.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        if handle.qos == QoS.AT_MOST_ONCE:
            if not create_qos0_receipt:
                return None
            return PublishReceipt(mid=None, qos=handle.qos, _event=None)
        assert handle.mid is not None
        receipt = PublishReceipt(
            mid=handle.mid,
            qos=handle.qos,
            _event=asyncio.Event(),
        )
        self._register_publish_receipt(handle.mid, receipt)
        return receipt

    def _finalize_loop_commands(self) -> None:
        \"\"\"Collect engine effects and apply/schedule them without suspending.\"\"\"
        self._collect_effects_locked()
        self._drain_effects_inline()

    def _queue_subscribe_on_loop(
        self,
        topics: str | Iterable[str | tuple[str, SubscribeOptions | int | QoS]],
        *,
        qos: int | QoS = 0,
        properties: Properties | None = None,
    ) -> int:
        return self._engine.queue_subscribe(topics, qos=qos, properties=properties)

    def _queue_unsubscribe_on_loop(self, topics: str | Iterable[str]) -> int:
        return self._engine.queue_unsubscribe(topics)

""",
)

publish_nowait = """    def publish_nowait(
        self,
        topic: str,
        payload: bytes | str = b\"\",
        *,
        qos: int | QoS = 0,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishReceipt:
        \"\"\"Queue a publication synchronously on the owning event-loop thread.

        This is the non-suspending counterpart to ``publish(..., nowait=True)``.
        It never waits for engine or writer capacity and raises ``FlowControlError``
        immediately when either bound is full. Like ``asyncio.Queue.put_nowait()``,
        it is loop-bound rather than thread-safe; cross-thread producers need an
        adapter that hands work to the owning loop.
        \"\"\"
        try:
            asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                \"publish_nowait() must be called from the client's event-loop thread\"
            ) from exc
        data = payload.encode(\"utf-8\") if isinstance(payload, str) else payload
        self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
        receipt = self._queue_publish_on_loop(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        assert receipt is not None
        self._finalize_loop_commands()
        return receipt

"""
replace_once(async_path, "    async def publish(\n", publish_nowait + "    async def publish(\n")

replace_section(
    async_path,
    "    async def publish(\n",
    "    async def _admit_publish_many(\n",
    """    async def publish(
        self,
        topic: str,
        payload: bytes | str = b\"\",
        *,
        qos: int | QoS = 0,
        retain: bool = False,
        properties: Properties | None = None,
        nowait: bool = False,
    ) -> PublishReceipt:
        data = payload.encode(\"utf-8\") if isinstance(payload, str) else payload
        while True:
            wait_for_space = False
            async with self._engine_lock:
                try:
                    if nowait:
                        self._check_nowait_publish_capacity(
                            topic, data, qos, retain, properties
                        )
                    receipt = self._queue_publish_on_loop(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
                    assert receipt is not None
                except FlowControlError as flow_exc:
                    if (
                        nowait
                        or self._publish_backpressure == \"error\"
                        or not self._engine.can_ever_admit_publish(
                            topic, data, qos, properties
                        )
                    ):
                        raise
                    terminal = self._publish_wait_failure()
                    if terminal is not None:
                        raise terminal from flow_exc
                    self._publish_space.clear()
                    self._publish_waiters += 1
                    wait_for_space = True
                else:
                    self._finalize_loop_commands()
            if not wait_for_space:
                if self._pending_effects:
                    if nowait:
                        self._schedule_effect_flush()
                    else:
                        await self._drain_effects()
                return receipt
            try:
                await self._publish_space.wait()
            finally:
                self._publish_waiters -= 1

""",
)

replace_once(
    async_path,
    "            self._engine.config.accept_auth = True\n",
    "            self._engine.reconfigure(accept_auth=True)\n",
)
replace_once(
    async_path,
    "        self._engine.config.accept_auth = handler is not None\n",
    "        self._engine.reconfigure(accept_auth=handler is not None)\n",
)
replace_once(
    async_path,
    """            mid = self._engine.queue_subscribe(
                topics,
                qos=qos,
                properties=properties,
            )
""",
    """            mid = self._queue_subscribe_on_loop(
                topics,
                qos=qos,
                properties=properties,
            )
""",
)
replace_once(
    async_path,
    "            mid = self._engine.queue_unsubscribe(topics)\n",
    "            mid = self._queue_unsubscribe_on_loop(topics)\n",
)

paho_path = "src/mqttium/compat/paho.py"
for old, new in (
    (
        "lambda: self._async._engine.config.update(max_pending_outbound_messages=value)",
        "lambda: self._async._reconfigure(max_pending_outbound_messages=value)",
    ),
    (
        "lambda: self._async._engine.config.update(max_pending_outbound_bytes=queue_size)",
        "lambda: self._async._reconfigure(max_pending_outbound_bytes=queue_size)",
    ),
    (
        "self._async._engine.config.update(\n                username=username,\n                password=pwd,\n            )",
        "self._async._reconfigure(\n                username=username,\n                password=pwd,\n            )",
    ),
    (
        "lambda: self._async._engine.config.update(will=message)",
        "lambda: self._async._reconfigure(will=message)",
    ),
    (
        "                    await self._async._flush_effects()",
        "                    self._async._finalize_loop_commands()",
    ),
    (
        "            self._async._collect_effects_locked()\n            self._async._drain_effects_inline()",
        "            self._async._finalize_loop_commands()",
    ),
    (
        "self._run_loop_mutation(lambda: self._async._engine.config.update(keepalive=keepalive))",
        "self._run_loop_mutation(lambda: self._async._reconfigure(keepalive=keepalive))",
    ),
    (
        """            lambda: (
                self._async._host,
                self._async._port,
                self._async._engine.config.keepalive,
            )
""",
        "            lambda: self._async._compat_connection_settings\n",
    ),
    (
        "        self._async._collect_effects_locked()\n        self._async._drain_effects_inline()",
        "        self._async._finalize_loop_commands()",
    ),
    (
        "lambda: self._async._engine.queue_subscribe(topic, qos=qos)",
        "lambda: self._async._queue_subscribe_on_loop(topic, qos=qos)",
    ),
    (
        "lambda: self._async._engine.queue_unsubscribe(topic)",
        "lambda: self._async._queue_unsubscribe_on_loop(topic)",
    ),
    ("info = self._async._last_disconnect", "info = self._async.last_disconnect"),
):
    replace_once(paho_path, old, new)

replace_section(
    paho_path,
    "    def _commit_qosn_publish_on_loop(\n",
    "    def _finalize_publish_effects(\n",
    """    def _commit_qosn_publish_on_loop(self, request: _PendingPublish) -> MQTTMessageInfo:
        receipt = self._async._queue_publish_on_loop(
            request.topic,
            request.payload if request.payload is not None else b\"\",
            qos=request.qos,
            retain=request.retain,
        )
        assert receipt is not None and receipt.mid is not None
        return MQTTMessageInfo(mid=receipt.mid, _receipt=receipt, _loop=self._loop)

""",
)
replace_section(
    paho_path,
    "    def _finalize_publish_effects(\n",
    "    def _drain_publish_requests(\n",
    """    def _finalize_publish_effects(self) -> None:
        self._async._finalize_loop_commands()

""",
)
replace_once(
    paho_path,
    """                if request.qos is QoS.AT_MOST_ONCE:
                    self._async._engine.queue_publish(
                        request.topic,
                        request.payload if request.payload is not None else b\"\",
                        qos=request.qos,
                        retain=request.retain,
                    )
                    committed = True
                else:
""",
    """                if request.qos is QoS.AT_MOST_ONCE:
                    self._async._queue_publish_on_loop(
                        request.topic,
                        request.payload if request.payload is not None else b\"\",
                        qos=request.qos,
                        retain=request.retain,
                        create_qos0_receipt=False,
                    )
                    committed = True
                else:
""",
)
replace_section_after(
    paho_path,
    "    def publish(\n",
    "        if self._on_network_thread():\n",
    "        if requested_qos is QoS.AT_MOST_ONCE:\n",
    """        if self._on_network_thread():
            try:
                receipt = self._async._queue_publish_on_loop(
                    topic,
                    data,
                    qos=requested_qos,
                    retain=retain,
                )
                assert receipt is not None
                info = MQTTMessageInfo(
                    mid=receipt.mid,
                    _receipt=receipt,
                    _loop=self._loop,
                )
            except FlowControlError:
                return MQTTMessageInfo(mid=None, rc=MQTT_ERR_QUEUE_SIZE)
            self._finalize_publish_effects()
            return info

""",
)

paho_text = Path(paho_path).read_text()
for forbidden in (
    "self._async._engine",
    "self._async._register_publish_receipt",
    "self._async._collect_effects_locked",
    "self._async._drain_effects_inline",
):
    if forbidden in paho_text:
        raise SystemExit(f"Paho still bypasses AsyncClient boundary: {forbidden}")

replace_once(
    "benchmarks/compat_qosn_submit_ab.py",
    """def _commit_one_on_loop(client: Client, payload: bytes) -> int:
    handle = client._async._engine.queue_publish(
        _TOPIC,
        payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
    )
    assert handle.mid is not None
    receipt = PublishReceipt(
        mid=handle.mid,
        qos=handle.qos,
        _event=asyncio.Event(),
    )
    client._async._register_publish_receipt(handle.mid, receipt)
    client._async._collect_effects_locked()
    client._async._drain_effects_inline()
    return handle.mid
""",
    """def _commit_one_on_loop(client: Client, payload: bytes) -> int:
    receipt = client._async._queue_publish_on_loop(
        _TOPIC,
        payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
    )
    assert receipt is not None and receipt.mid is not None
    client._async._finalize_loop_commands()
    return receipt.mid
""",
)
replace_once(
    "benchmarks/compat_qosn_submit_ab.py",
    "from mqttium.api.models import PublishReceipt\n",
    "",
)

paired = "benchmarks/paired_regression.py"
replace_once(
    paired,
    "    \"effect_batch_inline\",\n)",
    "    \"effect_batch_inline\",\n    \"native_publish_nowait_qos0\",\n)",
)
replace_once(
    paired,
    """    elif scenario in (\"effect_send_inline\", \"effect_batch_inline\"):
""",
    """    elif scenario == \"native_publish_nowait_qos0\":
        client = AsyncClient(client_id=\"paired-native-nowait\")
        client._engine.state = ConnectionState.CONNECTED
        payload = b\"x\"
        use_native = hasattr(client, \"publish_nowait\")

        async def run_native() -> WorkerResult:
            warmup = 2_000
            operations = 60_000
            for index in range(warmup + operations):
                if index == warmup:
                    started = time.perf_counter()
                if use_native:
                    client.publish_nowait(topic, payload, qos=0)
                else:
                    await client.publish(topic, payload, qos=0, nowait=True)
                item = client._outbound.get_nowait()
                client._outbound_bytes -= len(item)
                client._outbound.task_done()
            elapsed = time.perf_counter() - started
            return WorkerResult(scenario, elapsed, operations, operations / elapsed)

        result = asyncio.run(run_native())
    elif scenario in (\"effect_send_inline\", \"effect_batch_inline\"):
""",
)

Path("tests/unit/test_native_publish_nowait.py").write_text(
    '''"""Loop-bound native publish admission and adapter-boundary contracts."""

from __future__ import annotations

import inspect

import pytest

import mqttium.compat.paho as paho_compat
from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, QoS


def test_publish_nowait_requires_a_running_loop() -> None:
    client = AsyncClient()
    with pytest.raises(RuntimeError, match="event-loop thread"):
        client.publish_nowait("native/off-loop", b"x")


async def test_publish_nowait_registers_qos1_receipt() -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.CONNECTED
    receipt = client.publish_nowait("native/qos1", b"x", qos=1)
    assert receipt.qos is QoS.AT_LEAST_ONCE
    assert receipt.mid is not None
    assert client._pop_publish_receipt(receipt.mid) is receipt


async def test_publish_nowait_coalesces_async_effect_flush() -> None:
    client = AsyncClient(max_outbound_messages=512)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[int | None] = []
    client.on_publish = lambda mid, _error: seen.append(mid)

    for _ in range(100):
        client.publish_nowait("native/qos0", b"x", qos=0)

    task = client._effect_flush_task
    assert task is not None
    assert not task.done()
    await task
    await client._callback_queue.join()
    assert seen == [None] * 100
    await client._shutdown_callback_worker(drain=False)


def test_paho_uses_the_async_client_adapter_boundary() -> None:
    source = inspect.getsource(paho_compat.Client)
    for forbidden in (
        "self._async._engine",
        "self._async._register_publish_receipt",
        "self._async._collect_effects_locked",
        "self._async._drain_effects_inline",
    ):
        assert forbidden not in source
'''
)

design = Path("docs/DESIGN.md")
design_text = design.read_text()
marker = "### Découpage interne du moteur\n"
section = """### Commandes loop-bound et façade Paho

`AsyncClient.publish_nowait()` est une primitive synchrone mais attachée au
thread de l'event loop, analogue à `asyncio.Queue.put_nowait()`. Elle partage
l'admission, la création des receipts et l'application coalescée des effets
avec `publish()`, sans créer de coroutine. Elle n'est volontairement pas une
API thread-safe : la façade Paho garde sa file inter-thread bornée et commit
un batch sur le loop avant de finaliser les effets une seule fois.

La façade Paho ne touche plus directement `ProtocolEngine`, les registres de
receipts ou `EffectPump`. Elle passe par une petite frontière interne
loop-confined d'`AsyncClient`, afin de préserver le batching et les fast paths
sans introduire de bus de commandes générique.

"""
if marker not in design_text:
    raise SystemExit("DESIGN marker missing")
design.write_text(design_text.replace(marker, section + marker, 1))

Path(".github/workflows/apply-paho-boundary.yml").unlink()
Path(".github/apply_paho_boundary.py").unlink()
