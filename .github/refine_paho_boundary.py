from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:100]!r}")
    file.write_text(text.replace(old, new, 1))


async_path = "src/mqttium/api/async_client.py"
replace_once(
    async_path,
    """    @property
    def last_disconnect(self) -> DisconnectInfo | None:
        \"\"\"Details from the most recent broker or transport disconnect.\"\"\"
        return self._last_disconnect
""",
    """    @property
    def _last_disconnect_info(self) -> DisconnectInfo | None:
        \"\"\"Loop-confined disconnect metadata used by compatibility adapters.\"\"\"
        return self._last_disconnect
""",
)
replace_once(
    async_path,
    """        receipt = self._queue_publish_on_loop(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        assert receipt is not None
        self._finalize_loop_commands()
""",
    """        receipt = self._queue_publish_on_loop(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        self._finalize_loop_commands()
""",
)
replace_once(
    async_path,
    """                    receipt = self._queue_publish_on_loop(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
                    assert receipt is not None
""",
    """                    receipt = self._queue_publish_on_loop(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
""",
)

paho_path = "src/mqttium/compat/paho.py"
replace_once(
    paho_path,
    "info = self._async.last_disconnect",
    "info = self._async._last_disconnect_info",
)
replace_once(
    paho_path,
    "assert receipt is not None and receipt.mid is not None",
    "assert receipt.mid is not None",
)
replace_once(
    paho_path,
    """                assert receipt is not None
                info = MQTTMessageInfo(
""",
    """                info = MQTTMessageInfo(
""",
)

benchmark = "benchmarks/paired_regression.py"
replace_once(
    benchmark,
    """    \"effect_batch_inline\",
    \"native_publish_nowait_qos0\",
)
""",
    """    \"effect_batch_inline\",
    \"async_publish_nowait_qos0\",
    \"native_publish_nowait_qos0\",
    \"compat_publish_qos1\",
    \"compat_publish_qos0_batch\",
)
""",
)
replace_once(
    benchmark,
    """    elif scenario == \"native_publish_nowait_qos0\":
""",
    """    elif scenario == \"async_publish_nowait_qos0\":
        client = AsyncClient(client_id=\"paired-async-nowait\")
        client._engine.state = ConnectionState.CONNECTED
        publish_payload = b\"x\"

        async def run_async_nowait() -> WorkerResult:
            warmup = 2_000
            operations = 60_000
            for index in range(warmup + operations):
                if index == warmup:
                    started = time.perf_counter()
                await client.publish(topic, publish_payload, qos=0, nowait=True)
                item = client._outbound.get_nowait()
                client._outbound_bytes -= len(item)
                client._outbound.task_done()
            elapsed = time.perf_counter() - started
            return WorkerResult(scenario, elapsed, operations, operations / elapsed)

        result = asyncio.run(run_async_nowait())
    elif scenario == \"native_publish_nowait_qos0\":
""",
)
replace_once(
    benchmark,
    """        result = asyncio.run(run_native())
    elif scenario in (\"effect_send_inline\", \"effect_batch_inline\"):
""",
    """        result = asyncio.run(run_native())
    elif scenario == \"compat_publish_qos1\":
        from mqttium.compat.paho import CallbackAPIVersion, Client

        client = Client(
            CallbackAPIVersion.VERSION2,
            client_id=\"paired-compat-qos1\",
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
        client.loop_start()
        try:
            client._run_loop_mutation(
                lambda: setattr(client._async._engine, \"state\", ConnectionState.CONNECTED)
            )

            def run_compat_qos1() -> None:
                info = client.publish(topic, b\"x\", qos=1)
                if info.mid is None:
                    raise RuntimeError(\"QoS 1 publish returned no MID\")

            result = _measure(run_compat_qos1, operations=2_000, warmup=200)
        finally:
            client.loop_stop()
    elif scenario == \"compat_publish_qos0_batch\":
        import threading

        from mqttium.compat.paho import CallbackAPIVersion, Client

        client = Client(
            CallbackAPIVersion.VERSION2,
            client_id=\"paired-compat-qos0\",
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
        client._async._max_outbound_messages = 100_000
        client._async._max_outbound_bytes = 8 * 1024 * 1024
        client.loop_start()
        try:
            client._run_loop_mutation(
                lambda: setattr(client._async._engine, \"state\", ConnectionState.CONNECTED)
            )

            def submit_and_drain(count: int) -> float:
                started = time.perf_counter()
                for _ in range(count):
                    client.publish(topic, b\"x\", qos=0)
                drained = threading.Event()

                def fence() -> None:
                    with client._publish_schedule_lock:
                        idle = (
                            not client._publish_drain_scheduled
                            and client._publish_spillover is None
                            and client._publish_pending.empty()
                        )
                    if idle:
                        drained.set()
                    else:
                        assert client._loop is not None
                        client._loop.call_soon(fence)

                assert client._loop is not None
                client._loop.call_soon_threadsafe(fence)
                if not drained.wait(timeout=10.0):
                    raise RuntimeError(\"QoS 0 compatibility batch did not drain\")
                return time.perf_counter() - started

            submit_and_drain(1_000)
            operations = 20_000
            elapsed = submit_and_drain(operations)
            result = WorkerResult(scenario, elapsed, operations, operations / elapsed)
        finally:
            client.loop_stop()
    elif scenario in (\"effect_send_inline\", \"effect_batch_inline\"):
""",
)

# The targeted benchmark is internal, but keep its annotations aligned with the
# now non-optional receipt contract.
replace_once(
    "benchmarks/compat_qosn_submit_ab.py",
    "assert receipt is not None and receipt.mid is not None",
    "assert receipt.mid is not None",
)

# Keep the public API surface intentional: publish_nowait is public, adapter
# metadata remains private.
test_path = Path("tests/unit/test_native_publish_nowait.py")
test_text = test_path.read_text()
test_text += '''\n\ndef test_disconnect_metadata_boundary_is_private() -> None:\n    assert not hasattr(AsyncClient, "last_disconnect")\n    assert hasattr(AsyncClient, "_last_disconnect_info")\n'''
test_path.write_text(test_text)

Path(".github/refine_paho_boundary.py").unlink()
Path(".github/workflows/refine-paho-boundary.yml").unlink()
