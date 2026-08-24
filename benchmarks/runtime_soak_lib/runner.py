"""Execute a seed-reproducible soak schedule against a fake or live broker."""

from __future__ import annotations

import asyncio
import concurrent.futures
import tempfile
from collections.abc import Callable, Coroutine, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient
from mqttium.api.models import PublishMessage, PublishReceipt
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import FlowControlError, MQTTError, MQTTTimeoutError, NotConnectedError
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties

from benchmarks.runtime_soak_lib.broker import SoakBroker
from benchmarks.runtime_soak_lib.ownership import (
    OwnershipSnapshot,
    connected_idle_violations,
    disconnected_idle_violations,
    drift_from,
    take_ownership,
)
from benchmarks.runtime_soak_lib.profiles import SoakProfile
from benchmarks.runtime_soak_lib.schedule import Op, OpKind, reduce_schedule, schedule_for_seed

_EXPECTED = (FlowControlError, NotConnectedError, MQTTTimeoutError)
_Handler = Callable[["SoakSession", Op], Coroutine[Any, Any, None]]


class SoakFailure(RuntimeError):
    """A soak schedule violated a quiescence or liveness oracle."""

    def __init__(
        self,
        message: str,
        *,
        op_index: int,
        op: Op | None,
        history: list[str],
        reduced: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.op_index = op_index
        self.op = op
        self.history = history
        self.reduced = reduced or []


@dataclass
class SoakReport:
    profile: str
    seed: int
    protocol: str
    operations: int
    elapsed_s: float
    checkpoints: int
    reduced: list[str] = field(default_factory=list)
    ownership: dict[str, Any] = field(default_factory=dict)
    history: list[str] = field(default_factory=list)
    ok: bool = True
    error: str | None = None


class SoakSession:
    """One client + broker pair executing a concrete op list."""

    def __init__(
        self,
        *,
        seed: int,
        protocol: MQTTProtocolVersion,
        timeout: float,
        durable: bool,
        sqlite: bool,
        backend: str,
        host: str,
        port: int,
    ) -> None:
        self.seed = seed
        self.protocol = protocol
        self.timeout = timeout
        self.durable = durable
        self.sqlite = sqlite
        self.backend = backend
        self.host = host
        self.port = port
        self.topic = f"soak/{seed}"
        self.broker: SoakBroker | None = None
        self.client: AsyncClient | None = None
        self.store: MemoryInflightStore | SqliteInflightStore | None = None
        self._sqlite_path: Path | None = None
        self._consumer: asyncio.Task[None] | None = None
        self._received = 0
        self._slow_callback = False
        self._pending: list[PublishReceipt] = []
        self._subscribed = False
        self._checkpoints = 0
        self._baseline: OwnershipSnapshot | None = None
        self.history: list[str] = []

    async def setup(self) -> None:
        if self.sqlite:
            handle = tempfile.NamedTemporaryFile(prefix="mqttium-soak-", suffix=".db", delete=False)
            handle.close()
            self._sqlite_path = Path(handle.name)
            self.store = SqliteInflightStore(self._sqlite_path)
        else:
            self.store = MemoryInflightStore()
        props = None
        if self.protocol is MQTTProtocolVersion.MQTTv5 and self.durable:
            props = Properties({"session_expiry_interval": 3600})
        self.client = AsyncClient(
            f"soak-{self.seed}",
            protocol=self.protocol,
            clean_start=not self.durable,
            keepalive=0,
            reconnect=ReconnectPolicy(
                enabled=True,
                initial_delay=0.01,
                multiplier=1.5,
                max_delay=0.05,
                max_retries=80,
                stable_after=0.02,
                connect_timeout=self.timeout,
            ),
            message_delivery="both",
            store=self.store,
            connect_properties=props,
            max_outbound_inflight=16,
            max_pending_outbound_messages=64,
            max_outbound_messages=32,
            max_outbound_bytes=64 * 1024,
            max_pending_messages=64,
            max_pending_callbacks=32,
            max_pending_delivery_bytes=64 * 1024,
            ack_timeout=self.timeout,
            delivery_timeout=self.timeout,
        )
        self.client.on_message = self._on_message
        if self.backend == "fake":
            self.broker = SoakBroker(
                protocol=self.protocol,
                echo=True,
                session_present=self.durable,
            )
            self.client._transport_factory = self.broker.factory()

    async def close(self) -> None:
        await self._stop_consumer()
        if self.client is not None:
            with suppress(Exception):
                await asyncio.wait_for(self.client.disconnect(), timeout=self.timeout)
        if self.store is not None:
            closer = getattr(self.store, "close", None)
            if closer is not None:
                with suppress(Exception):
                    closer()
        if self._sqlite_path is not None:
            self._sqlite_path.unlink(missing_ok=True)

    async def run_ops(self, ops: Sequence[Op]) -> None:
        for index, op in enumerate(ops):
            self.history.append(f"{index}:{op.label()}")
            try:
                await asyncio.wait_for(self._apply(op), timeout=self.timeout)
            except SoakFailure:
                raise
            except TimeoutError as exc:
                detail = ""
                if self.client is not None:
                    detail = f" ownership={take_ownership(self.client).as_dict()}"
                raise SoakFailure(
                    f"timed out on {op.label()}{detail}",
                    op_index=index,
                    op=op,
                    history=self.history,
                ) from exc
            except asyncio.CancelledError:
                raise
            except _EXPECTED:
                continue
            except MQTTError as exc:
                raise SoakFailure(
                    f"{op.label()}: {exc}",
                    op_index=index,
                    op=op,
                    history=self.history,
                ) from exc

    async def _apply(self, op: Op) -> None:
        await _HANDLERS[op.kind](self, op)

    async def _connect(self, op: Op) -> None:
        del op
        client = self._require_client()
        if client.is_connected:
            return
        await client.connect(self.host, self.port, timeout=self.timeout)
        await self._ensure_consumer()

    async def _subscribe(self, op: Op) -> None:
        await self._connect(op)
        await self._require_client().subscribe(op.topic, qos=QoS.EXACTLY_ONCE, timeout=self.timeout)
        self._subscribed = True
        self.topic = op.topic

    async def _unsubscribe(self, op: Op) -> None:
        client = self._require_client()
        if client.is_connected:
            await client.unsubscribe(op.topic, timeout=self.timeout)
        self._subscribed = False

    async def _publish(self, op: Op, *, nowait: bool) -> None:
        client = self._require_client()
        await self._connect(op)
        payload = b"x" * op.payload_size
        if nowait:
            receipt = client.publish_nowait(op.topic, payload, qos=op.qos)
        else:
            receipt = await client.publish(op.topic, payload, qos=op.qos)
        if op.qos:
            self._pending.append(receipt)

    async def _publish_wait(self, op: Op) -> None:
        await self._publish(op, nowait=False)

    async def _publish_nowait(self, op: Op) -> None:
        await self._publish(op, nowait=True)

    async def _publish_many(self, op: Op) -> None:
        client = self._require_client()
        await self._connect(op)
        messages = [
            PublishMessage(op.topic, f"{index}".encode(), qos=op.qos) for index in range(op.count)
        ]
        receipt = await client.publish_many(messages)
        if op.qos:
            await receipt.wait()

    async def _cancel_publish(self, op: Op) -> None:
        client = self._require_client()
        await self._connect(op)

        async def _wait() -> None:
            receipt = await client.publish(op.topic, b"cancel", qos=max(op.qos, 1))
            await receipt.wait()

        task = asyncio.create_task(_wait(), name="soak-cancel-publish")
        await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError, *_EXPECTED):
            await task

    async def _consume(self, op: Op) -> None:
        del op
        await asyncio.sleep(0)

    async def _slow_cb(self, op: Op) -> None:
        del op
        self._slow_callback = True

    async def _fast_cb(self, op: Op) -> None:
        del op
        self._slow_callback = False

    async def _drop_network(self, op: Op) -> None:
        client = self._require_client()
        if not client.is_connected:
            return
        epoch = client.stats().connection_epoch
        if self.broker is not None:
            self.broker.session_present = op.session_present and self.durable
            self.broker.drop_current()
        else:
            transport = client._transport
            if transport is not None:
                await transport.close()
        await asyncio.sleep(0)
        await self._wait_reconnect(op, previous_epoch=epoch)

    async def _wait_reconnect(self, op: Op, previous_epoch: int | None = None) -> None:
        del op
        client = self._require_client()

        def _settled() -> bool:
            stats = client.stats()
            connected = (
                client.is_connected
                and not stats.tasks.reconnect
                and stats.tasks.reader
                and stats.tasks.writer
            )
            if previous_epoch is None:
                return connected
            return connected and stats.connection_epoch > previous_epoch

        await _wait_until(_settled, timeout=self.timeout, what="reconnect settle")
        await self._ensure_consumer()
        if self._subscribed:
            await client.subscribe(self.topic, qos=QoS.EXACTLY_ONCE, timeout=self.timeout)

    async def _drain(self, op: Op) -> None:
        del op
        pending = [receipt for receipt in self._pending if not receipt.is_done()]
        if pending:
            await asyncio.wait_for(
                asyncio.gather(*(receipt.wait() for receipt in pending), return_exceptions=True),
                timeout=self.timeout,
            )
        self._pending = [receipt for receipt in self._pending if not receipt.is_done()]
        await asyncio.sleep(0)

    async def _quiesce(self, op: Op) -> None:
        client = self._require_client()
        self._slow_callback = False
        await self._drain(op)
        await asyncio.sleep(0)
        if client.is_connected:
            await _wait_until(
                lambda: not connected_idle_violations(take_ownership(client)),
                timeout=self.timeout,
                what="connected idle",
            )
            snapshot = take_ownership(client)
            violations = connected_idle_violations(snapshot)
            kind = "connected idle leak"
            if not violations:
                violations = self._drift_violations(snapshot)
                kind = "ownership drift"
        else:
            await _wait_until(
                lambda: not disconnected_idle_violations(take_ownership(client)),
                timeout=self.timeout,
                what="disconnected idle",
            )
            snapshot = take_ownership(client)
            violations = disconnected_idle_violations(snapshot)
            kind = "disconnected idle leak"
            self._baseline = None
        if violations:
            raise SoakFailure(
                f"{kind}: {violations}",
                op_index=len(self.history) - 1,
                op=None,
                history=self.history,
            )
        self._checkpoints += 1

    def _drift_violations(self, snapshot: OwnershipSnapshot) -> list[str]:
        if self._baseline is None:
            self._baseline = snapshot
            return []
        return drift_from(self._baseline, snapshot)

    async def _graceful_shutdown(self, op: Op) -> None:
        del op
        await self._stop_consumer()
        await self._require_client().disconnect()
        self._subscribed = False
        self._pending.clear()
        self._baseline = None

    async def _force_shutdown(self, op: Op) -> None:
        del op
        client = self._require_client()
        task = asyncio.create_task(client.disconnect(), name="soak-force-disconnect")
        await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
        transport = client._transport
        if transport is not None:
            with suppress(Exception):
                await transport.close()
        with suppress(Exception):
            await client.disconnect()
        await self._stop_consumer()
        self._subscribed = False
        self._pending.clear()
        self._baseline = None

    async def _on_message(self, _message: object) -> None:
        self._received += 1
        if self._slow_callback:
            await asyncio.sleep(0)

    async def _ensure_consumer(self) -> None:
        client = self._require_client()
        if self._consumer is not None and not self._consumer.done():
            return

        async def _consume() -> None:
            with suppress(Exception, asyncio.CancelledError):
                async for _incoming in client.messages():
                    self._received += 1

        self._consumer = asyncio.create_task(_consume(), name="soak-consumer")

    async def _stop_consumer(self) -> None:
        task = self._consumer
        self._consumer = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    def _require_client(self) -> AsyncClient:
        assert self.client is not None
        return self.client


_HANDLERS: dict[OpKind, _Handler] = {
    OpKind.CONNECT: SoakSession._connect,
    OpKind.SUBSCRIBE: SoakSession._subscribe,
    OpKind.UNSUBSCRIBE: SoakSession._unsubscribe,
    OpKind.PUBLISH: SoakSession._publish_wait,
    OpKind.PUBLISH_NOWAIT: SoakSession._publish_nowait,
    OpKind.PUBLISH_MANY: SoakSession._publish_many,
    OpKind.CANCEL_PUBLISH: SoakSession._cancel_publish,
    OpKind.CONSUME: SoakSession._consume,
    OpKind.SLOW_CALLBACK: SoakSession._slow_cb,
    OpKind.FAST_CALLBACK: SoakSession._fast_cb,
    OpKind.DROP_NETWORK: SoakSession._drop_network,
    OpKind.WAIT_RECONNECT: SoakSession._wait_reconnect,
    OpKind.DRAIN: SoakSession._drain,
    OpKind.QUIESCE: SoakSession._quiesce,
    OpKind.GRACEFUL_SHUTDOWN: SoakSession._graceful_shutdown,
    OpKind.FORCE_SHUTDOWN: SoakSession._force_shutdown,
}


async def _wait_until(predicate: Any, *, timeout: float, what: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(what)
        await asyncio.sleep(0.01)


async def _run_once(
    ops: Sequence[Op],
    *,
    seed: int,
    protocol: MQTTProtocolVersion,
    timeout: float,
    durable: bool,
    sqlite: bool,
    backend: str,
    host: str,
    port: int,
) -> SoakSession:
    session = SoakSession(
        seed=seed,
        protocol=protocol,
        timeout=timeout,
        durable=durable,
        sqlite=sqlite,
        backend=backend,
        host=host,
        port=port,
    )
    await session.setup()
    try:
        await session.run_ops(ops)
    except Exception:
        await session.close()
        raise
    return session


def _replay_sync(ops: Sequence[Op], kwargs: dict[str, Any]) -> bool:
    async def _go() -> bool:
        session = None
        try:
            session = await _run_once(ops, **kwargs)
            await session.close()
        except SoakFailure:
            if session is not None:
                await session.close()
            return False
        except Exception:
            if session is not None:
                with suppress(Exception):
                    await session.close()
            return False
        return True

    def _call() -> bool:
        return asyncio.run(_go())

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _call()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_call).result()


async def run_soak(
    profile: SoakProfile,
    *,
    seed: int,
    protocol: MQTTProtocolVersion,
    backend: str = "fake",
    host: str = "127.0.0.1",
    port: int = 1883,
    operations: int | None = None,
) -> SoakReport:
    """Run one profile/seed/protocol combination and reduce on failure."""
    loop = asyncio.get_running_loop()
    started = loop.time()
    count = profile.operations if operations is None else operations
    ops = schedule_for_seed(seed, operations=count, protocol=protocol)
    kwargs = {
        "seed": seed,
        "protocol": protocol,
        "timeout": profile.timeout,
        "durable": profile.durable,
        "sqlite": profile.sqlite,
        "backend": backend,
        "host": host,
        "port": port,
    }
    report = SoakReport(
        profile=profile.name,
        seed=seed,
        protocol=protocol.name,
        operations=len(ops),
        elapsed_s=0.0,
        checkpoints=0,
    )
    try:
        session = await _run_once(ops, **kwargs)
        report.checkpoints = session._checkpoints
        report.history = list(session.history)
        if session.client is not None:
            report.ownership = take_ownership(session.client).as_dict()
        await session.close()
    except SoakFailure as exc:
        report.ok = False
        report.error = str(exc)
        report.history = list(exc.history)
        report.elapsed_s = loop.time() - started
        if profile.reduce_on_fail:
            failing = ops[: max(exc.op_index + 1, 1)]
            reduced = reduce_schedule(failing, lambda candidate: _replay_sync(candidate, kwargs))
            report.reduced = [item.label() for item in reduced]
            exc.reduced = report.reduced
        raise
    report.elapsed_s = loop.time() - started
    return report
