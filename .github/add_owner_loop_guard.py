from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1))


path = "src/mqttium/api/async_client.py"
replace_once(
    path,
    """        self._lifecycle_lock = asyncio.Lock()
        self._engine_lock = asyncio.Lock()
        self._connection_epoch = 0
""",
    """        self._lifecycle_lock = asyncio.Lock()
        self._engine_lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._connection_epoch = 0
""",
)
replace_once(
    path,
    """    ) -> ConnAckPacket:
        if self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
""",
    """    ) -> ConnAckPacket:
        loop = asyncio.get_running_loop()
        owner_loop = self._owner_loop
        if owner_loop is None:
            self._owner_loop = loop
        elif owner_loop is not loop:
            raise RuntimeError("AsyncClient is bound to a different event loop")
        if self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
""",
)
replace_once(
    path,
    """            connect_packet = self._engine.begin_connect()
            loop = asyncio.get_running_loop()
            self._connack_fut = loop.create_future()
""",
    """            connect_packet = self._engine.begin_connect()
            self._connack_fut = loop.create_future()
""",
)
replace_once(
    path,
    """        try:
            asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                "publish_nowait() must be called from the client's event-loop thread"
            ) from exc
""",
    """        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                "publish_nowait() must be called from the client's event-loop thread"
            ) from exc
        owner_loop = self._owner_loop
        if owner_loop is None:
            self._owner_loop = loop
        elif owner_loop is not loop:
            raise RuntimeError("AsyncClient is bound to a different event loop")
""",
)

test_path = Path("tests/unit/test_native_publish_nowait.py")
test_text = test_path.read_text()
needle = """async def test_publish_nowait_registers_qos1_receipt() -> None:
"""
new_test = '''async def test_publish_nowait_rejects_a_different_running_loop() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client.publish_nowait("native/owner", b"x", qos=0)

    errors: list[BaseException] = []

    def run_other_loop() -> None:
        async def attempt() -> None:
            try:
                client.publish_nowait("native/other-loop", b"x", qos=0)
            except BaseException as exc:
                errors.append(exc)

        asyncio.run(attempt())

    await asyncio.to_thread(run_other_loop)
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "different event loop" in str(errors[0])


'''
if test_text.count(needle) != 1:
    raise SystemExit("native publish test insertion point missing")
test_path.write_text(test_text.replace(needle, new_test + needle, 1))

Path(".github/add_owner_loop_guard.py").unlink()
Path(".github/workflows/add-owner-loop-guard.yml").unlink()
