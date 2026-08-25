"""Small deterministic schedule fuzzer for the real :class:`AsyncClient` runtime.

This is deliberately test infrastructure, not a generic asyncio simulator.  A
packet-aware transport exposes only the write/read boundaries needed to order
real AsyncClient, WritePump, EffectPump, and application-delivery work.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from collections.abc import Callable, Coroutine, Iterable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from types import MethodType
from typing import Any

from mqttium.api import AsyncClient, PublishReceipt
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.transport.writes import WriteItem, item_size


class RuntimeMutation(StrEnum):
    """Test-only mechanical breakages used to qualify the target."""

    WRITER_FAILURE_NO_WAKE = "writer_failure_no_wake"
    EPOCH_NOT_INVALIDATED = "epoch_not_invalidated"
    EFFECT_NOT_SETTLED = "effect_not_settled"
    CALLBACK_CANCEL_STOPS_WORKER = "callback_cancel_stops_worker"
    LATE_EFFECT_ABANDONED = "late_effect_abandoned"
    USER_TAKEOVER_LOSES = "user_takeover_loses"
    PUBLISH_WAITER_ACCOUNTING_LEAK = "publish_waiter_accounting_leak"


@dataclass(slots=True, frozen=True)
class RuntimeOperation:
    actor: str
    action: str
    value: str | int | None = None

    def render(self) -> str:
        suffix = "" if self.value is None else f" {self.value}"
        return f"{self.actor}.{self.action}{suffix}"


@dataclass(slots=True, frozen=True)
class RuntimeSchedule:
    seed: int
    operations: tuple[RuntimeOperation, ...]
    auto_reconnect: bool = False


@dataclass(slots=True)
class RuntimeFailureArtifact:
    seed: int
    mutation: str | None
    operations: list[str]
    checkpoints: list[str]
    owners: dict[str, Any]
    failure: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "mqttium-runtime-fuzz-v1",
            "seed": self.seed,
            "mutation": self.mutation,
            "operations": self.operations,
            "checkpoints": self.checkpoints,
            "owners": self.owners,
            "failure": self.failure,
        }

    def to_text(self) -> str:
        operations = "\n".join(f"{index} {op}" for index, op in enumerate(self.operations))
        return (
            "mqttium-runtime-fuzz-v1\n"
            f"seed={self.seed}\n"
            f"mutation={self.mutation or 'none'}\n"
            f"failure={self.failure}\n"
            "operations:\n"
            f"{operations}\n"
            "owners:\n"
            f"{json.dumps(self.owners, indent=2, sort_keys=True)}"
        )


class RuntimeFuzzFailure(AssertionError):
    def __init__(self, artifact: RuntimeFailureArtifact) -> None:
        super().__init__(artifact.to_text())
        self.artifact = artifact


@dataclass(slots=True, frozen=True)
class RuntimeRun:
    seed: int
    operations: tuple[str, ...]
    final_snapshot: dict[str, Any]
    failure: None = None


@dataclass(slots=True, frozen=True)
class CampaignResult:
    completed: int
    failures: int
    failing_seeds: tuple[int, ...]
    unique_operation_traces: int
    unique_scheduling_traces: int
    coverage: dict[str, int]


@dataclass(slots=True)
class _ApplicationTask:
    task: asyncio.Task[Any]
    label: str
    expected_exceptions: tuple[type[BaseException], ...] = ()
    expect_cancelled: bool = False
    allow_cancelled: bool = False
    observed: bool = False
    cleanup_cancelled: bool = False


class _ScheduleTransport:
    """Packet-aware transport with one explicit write gate and failure switch."""

    def __init__(
        self,
        generation: int,
        owner_epoch: int,
        current_epoch: Callable[[], int],
    ) -> None:
        self.generation = generation
        self.owner_epoch = owner_epoch
        self._current_epoch = current_epoch
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.write_gate = asyncio.Event()
        self.write_gate.set()
        self.write_entered = asyncio.Event()
        self.close_gate = asyncio.Event()
        self.close_gate.set()
        self.close_entered = asyncio.Event()
        self.fail_active_write = False
        self.active_payload: bytes | None = None
        self.attempted: list[RawPacket] = []
        self.completed: list[tuple[RawPacket, int]] = []

    async def write(self, data: WriteItem) -> None:
        payload = b"".join(data) if isinstance(data, tuple) else data
        self.active_payload = payload
        self._decoder.feed(payload)
        attempted = list(self._decoder.drain_packets())
        self.attempted.extend(attempted)
        self.write_entered.set()
        try:
            await self.write_gate.wait()
            if self.fail_active_write:
                self.fail_active_write = False
                raise ConnectionResetError("runtime fuzzer injected write failure")
            completion_epoch = self._current_epoch()
            self.completed.extend((packet, completion_epoch) for packet in attempted)
        finally:
            self.active_payload = None

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            self.close_entered.set()
            await self.close_gate.wait()
            self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing

    def push(self, packet: bytes) -> None:
        if not self._closing:
            self._rx.put_nowait(packet)

    def hold_writes(self) -> None:
        self.write_entered.clear()
        self.write_gate.clear()

    def release_writes(self) -> None:
        self.write_gate.set()

    def hold_close(self) -> None:
        self.close_entered.clear()
        self.close_gate.clear()

    def release_close(self) -> None:
        self.close_gate.set()

    def count(self, packet_type: PacketType) -> int:
        return sum(packet.packet_type is packet_type for packet, _epoch in self.completed)

    def last(self, packet_type: PacketType) -> RawPacket | None:
        return next(
            (
                packet
                for packet, _epoch in reversed(self.completed)
                if packet.packet_type is packet_type
            ),
            None,
        )


def _op(actor: str, action: str, value: str | int | None = None) -> RuntimeOperation:
    return RuntimeOperation(actor, action, value)


def _anchor_operations(  # noqa: C901
    family: int,
    rng: random.Random,
) -> tuple[list[RuntimeOperation], bool]:
    """Return one bounded ownership motif and whether it needs auto reconnect."""
    if family == 0:
        # Historical writer-failure / reader-admission motif.
        return (
            [
                _op("schedule", "hold_writes"),
                _op("app", "publish", 0),
                _op("checkpoint", "writer_active"),
                _op("broker", "publish", 1),
                _op("checkpoint", "writer_waiter"),
                _op("transport", "fail_active_write"),
                _op("schedule", "release_writes"),
                _op("checkpoint", "terminal"),
            ],
            False,
        )
    if family == 1:
        # Historical callback CancelledError ownership motif.
        return (
            [
                _op("callback", "cancel_once"),
                _op("broker", "publish", 0),
                _op("broker", "publish", 0),
                _op("checkpoint", "callbacks_drained"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ],
            False,
        )
    if family == 2:
        # Historical explicit reconnect / epoch-replacement motif.
        return (
            [
                _op("broker", "eof"),
                _op("checkpoint", "terminal"),
                _op("app", "connect"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ],
            False,
        )
    if family == 3:
        reconnect_variant = rng.randrange(4)
        if reconnect_variant == 0:
            operations = [
                _op("factory", "fail_next"),
                _op("broker", "eof"),
                _op("checkpoint", "factory_failed"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ]
        elif reconnect_variant == 1:
            operations = [
                _op("factory", "block_next"),
                _op("broker", "eof"),
                _op("checkpoint", "factory_blocked"),
                _op("schedule", "release_factory"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ]
        elif reconnect_variant == 2:
            operations = [
                _op("factory", "block_next"),
                _op("broker", "eof"),
                _op("checkpoint", "factory_blocked"),
                _op("app", "disconnect"),
                _op("schedule", "release_factory"),
                _op("checkpoint", "terminal"),
            ]
        else:
            operations = [
                _op("factory", "block_next"),
                _op("broker", "eof"),
                _op("checkpoint", "factory_blocked"),
                _op("app", "connect"),
                _op("schedule", "release_factory"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ]
        return operations, True
    if family == 4:
        callback_variant = rng.randrange(4)
        if callback_variant == 0:
            operations = [
                _op("callback", "block_once"),
                _op("broker", "publish", 0),
                _op("checkpoint", "callback_active"),
                _op("schedule", "release_callback"),
                _op("checkpoint", "callbacks_drained"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ]
            return operations, False
        if callback_variant == 1:
            operations = [
                _op("callback", "block_once"),
                _op("broker", "publish", 0),
                _op("checkpoint", "callback_active"),
                _op("schedule", "hold_close"),
                _op("app", "disconnect"),
                _op("app", "cancel_last"),
                _op("schedule", "release_close"),
                _op("schedule", "release_callback"),
                _op("checkpoint", "callbacks_drained"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            ]
            return operations, False
        if callback_variant == 2:
            operations = [
                _op("callback", "disconnect_once"),
                _op("broker", "publish", 0),
                _op("checkpoint", "terminal"),
            ]
            return operations, False
        operations = [
            _op("callback", "on_disconnect_connect_once"),
            _op("broker", "eof"),
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
            _op("checkpoint", "connected"),
            _op("checkpoint", "takeover_stable"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
        return operations, True

    # EffectPump apply/failure/late-collection interleaving. Vary which drain
    # already owns the target and where writer capacity is released relative to
    # effect failure and close settlement.
    operations = [
        _op("schedule", "hold_writes"),
        _op("app", "publish", 0),
        _op("checkpoint", "writer_active"),
        _op("effect", "block_next"),
        _op("schedule", "hold_close"),
        _op("broker", "publish", 1),
        _op("checkpoint", "effect_active"),
    ]
    drains = [_op("effect", "drain_failure") for _ in range(1 + rng.randrange(2))]
    if rng.randrange(2):
        operations.append(_op("effect", "fail_on_release"))
        operations.extend(drains)
    else:
        operations.extend(drains)
        operations.append(_op("effect", "fail_on_release"))
    release_position = rng.randrange(3)
    if release_position == 0:
        operations.extend(
            (
                _op("schedule", "release_writes"),
                _op("checkpoint", "wire", "PUBLISH"),
            )
        )
    operations.extend(
        (
            _op("schedule", "release_effect"),
            _op("checkpoint", "effect_failing_close"),
            _op("effect", "collect_late"),
        )
    )
    if release_position == 1:
        operations.append(_op("schedule", "release_writes"))
    operations.append(_op("schedule", "release_close"))
    if release_position == 2:
        operations.append(_op("schedule", "release_writes"))
    operations.append(_op("checkpoint", "terminal"))
    return operations, False


def generate_schedule(seed: int, steps: int = 24) -> RuntimeSchedule:  # noqa: C901
    """Generate one state-valid schedule whose operation count is ``steps``.

    A tiny grammar varies legal connected-state work and explicit release
    decisions before one of six ownership motifs. It models only the seams the
    target controls; the real event loop and AsyncClient still execute every
    transition.
    """

    if steps < 12:
        raise ValueError("runtime schedules require at least 12 steps")
    rng = random.Random(seed)
    family = seed % 6
    anchor, auto_reconnect = _anchor_operations(family, rng)
    if len(anchor) + 4 > steps:
        # The original motifs are the compact fallback for small custom budgets.
        anchor, auto_reconnect = _anchor_operations(seed % 3, rng)
    operations: list[RuntimeOperation] = [
        _op("app", "connect"),
        _op("checkpoint", "wire", "CONNECT"),
        _op("broker", "connack"),
        _op("checkpoint", "connected"),
    ]
    remaining = steps - len(operations) - len(anchor)
    state = "connected"
    qos = 0
    while remaining:
        if state == "connected":
            legal: list[tuple[str, int]] = [("yield", 1)]
            if remaining >= 2:
                legal.extend((("outbound0", 2), ("inbound0", 2)))
            if remaining >= 3:
                legal.extend((("outbound1", 3), ("inbound1", 3)))
            if remaining >= 5:
                legal.extend((("writer_gate", 5), ("callback_gate", 5)))
            choice, _cost = rng.choice(legal)
            if choice == "yield":
                operations.append(_op("schedule", "yield", rng.randrange(1, 6)))
                remaining -= 1
            elif choice.startswith("outbound"):
                qos = int(choice[-1])
                operations.append(_op("app", "publish", qos))
                remaining -= 1
                state = "outbound_wire"
            elif choice.startswith("inbound"):
                qos = int(choice[-1])
                operations.append(_op("broker", "publish", qos))
                remaining -= 1
                state = "inbound_wire" if qos else "inbound_callback"
            elif choice == "writer_gate":
                operations.append(_op("schedule", "hold_writes"))
                remaining -= 1
                state = "writer_publish"
            else:
                operations.append(_op("callback", "block_once"))
                remaining -= 1
                state = "callback_publish"
        elif state == "outbound_wire":
            operations.append(_op("checkpoint", "wire", "PUBLISH"))
            remaining -= 1
            state = "outbound_ack" if qos else "connected"
        elif state == "outbound_ack":
            operations.append(_op("broker", "puback_last"))
            remaining -= 1
            state = "connected"
        elif state == "inbound_wire":
            operations.append(_op("checkpoint", "wire", "PUBACK"))
            remaining -= 1
            state = "inbound_callback"
        elif state == "inbound_callback":
            operations.append(_op("checkpoint", "callbacks_drained"))
            remaining -= 1
            state = "connected"
        elif state == "writer_publish":
            operations.append(_op("app", "publish", 0))
            remaining -= 1
            state = "writer_active"
        elif state == "writer_active":
            operations.append(_op("checkpoint", "writer_active"))
            remaining -= 1
            state = "writer_release"
        elif state == "writer_release":
            operations.append(_op("schedule", "release_writes"))
            remaining -= 1
            state = "writer_wire"
        elif state == "writer_wire":
            operations.append(_op("checkpoint", "wire", "PUBLISH"))
            remaining -= 1
            state = "connected"
        elif state == "callback_publish":
            operations.append(_op("broker", "publish", 0))
            remaining -= 1
            state = "callback_active"
        elif state == "callback_active":
            operations.append(_op("checkpoint", "callback_active"))
            remaining -= 1
            state = "callback_release"
        elif state == "callback_release":
            operations.append(_op("schedule", "release_callback"))
            remaining -= 1
            state = "callback_drained"
        else:
            operations.append(_op("checkpoint", "callbacks_drained"))
            remaining -= 1
            state = "connected"
    assert state == "connected"
    operations.extend(anchor)
    assert len(operations) == steps
    return RuntimeSchedule(
        seed=seed,
        operations=tuple(operations),
        auto_reconnect=auto_reconnect,
    )


class _RuntimeHarness:
    def __init__(self, schedule: RuntimeSchedule, mutation: RuntimeMutation | None) -> None:
        self.schedule = schedule
        self.mutation = mutation
        self.transports: list[_ScheduleTransport] = []
        self.tasks: list[_ApplicationTask] = []
        self.operations: list[str] = []
        self.checkpoints: list[str] = []
        self.callback_expected = 0
        self.callback_attempted = 0
        self.callback_epoch: int | None = None
        self.cancel_callback_once = False
        self.raise_callback_once = False
        self.block_callback_once = False
        self.disconnect_callback_once = False
        self.connect_callback_once = False
        self.disconnect_callback_connect_once = False
        self.callback_gate = asyncio.Event()
        self.callback_gate.set()
        self.callback_entered = asyncio.Event()
        self.effect_gate = asyncio.Event()
        self.effect_gate.set()
        self.effect_entered = asyncio.Event()
        self.block_effect_once = False
        self.fail_effect_once = False
        self.factory_gate = asyncio.Event()
        self.factory_gate.set()
        self.factory_entered = asyncio.Event()
        self.factory_failed = asyncio.Event()
        self.block_factory_once = False
        self.fail_factory_once = False
        self.factory_attempts = 0
        self.receipt_settlements: dict[int, int] = {}
        self.loop_contexts: list[dict[str, Any]] = []
        self._checked_loop_contexts = 0
        self._expected_loop_cancellations = 0
        self._mutation_restores: list[tuple[object, str, object]] = []
        self._wire_targets: dict[PacketType, int] = {}
        # Event-loop turns awaited after every operation. V1/V2 schedules keep
        # the historical unconditional four-turn convergence; the pressure
        # profile varies it per schedule to reach short-lived interleavings.
        self.settle_turns = 4
        self.client = AsyncClient(**self._client_options())
        self.client._transport_factory = self._factory
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self._install_receipt_oracle()
        self._install_effect_gate()
        self._install_mutation()

    def _client_options(self) -> dict[str, Any]:
        """Constructor options for the schedule's client; profiles override."""
        return {
            "client_id": f"runtime-fuzz-{self.schedule.seed}",
            "protocol": MQTTProtocolVersion.MQTTv5,
            "reconnect": ReconnectPolicy(
                enabled=self.schedule.auto_reconnect,
                initial_delay=0,
                max_delay=0,
                max_retries=3,
                stable_after=0.05,
                connect_timeout=0.5,
            ),
            "max_outbound_messages": 1,
            "max_outbound_bytes": 4096,
            "max_pending_callbacks": 4,
            "message_delivery": "callback",
            "keepalive": 0,
        }

    @property
    def transport(self) -> _ScheduleTransport:
        if not self.transports:
            raise AssertionError("schedule has no transport")
        return self.transports[-1]

    async def _factory(
        self, host: str, port: int, *, ssl: object | None = None
    ) -> _ScheduleTransport:
        del host, port, ssl
        self.factory_attempts += 1
        if self.fail_factory_once:
            self.fail_factory_once = False
            self.factory_failed.set()
            raise ConnectionRefusedError("runtime fuzzer reconnect factory failure")
        if self.block_factory_once:
            self.block_factory_once = False
            self.factory_entered.set()
            await self.factory_gate.wait()
        transport = _ScheduleTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
            lambda: self.client._connection_epoch,
        )
        self.transports.append(transport)
        return transport

    async def _on_message(self, _message: object) -> None:
        self.callback_attempted += 1
        self.callback_epoch = self.client._connection_epoch
        if self.block_callback_once:
            self.block_callback_once = False
            self.callback_entered.set()
            await self.callback_gate.wait()
        if self.cancel_callback_once:
            self.cancel_callback_once = False
            raise asyncio.CancelledError("runtime fuzzer callback self-cancellation")
        if self.raise_callback_once:
            self.raise_callback_once = False
            raise RuntimeError("runtime fuzzer callback failure")
        if self.disconnect_callback_once:
            self.disconnect_callback_once = False
            await self.client.disconnect()
        if self.connect_callback_once:
            self.connect_callback_once = False
            await self.client.connect("runtime.invalid", timeout=0.5)

    async def _on_disconnect(self, _error: BaseException | None) -> None:
        if not self.disconnect_callback_connect_once:
            return
        self.disconnect_callback_connect_once = False
        await self.client.connect("runtime.invalid", timeout=0.5)

    def _install_receipt_oracle(self) -> None:
        original = PublishReceipt._settle

        def tracked_settle(receipt: PublishReceipt) -> None:
            key = id(receipt)
            count = self.receipt_settlements.get(key, 0) + 1
            self.receipt_settlements[key] = count
            if count > 1:
                raise AssertionError("publish receipt settled more than once")
            original(receipt)

        self._replace(PublishReceipt, "_settle", tracked_settle)

    def _install_effect_gate(self) -> None:
        original = self.client._apply_effect

        async def gated_apply(
            _client: AsyncClient,
            effect: EngineEffect,
            *,
            nowait: bool,
            epoch: int | None = None,
        ) -> None:
            if self.block_effect_once:
                self.block_effect_once = False
                self.effect_entered.set()
                await self.effect_gate.wait()
            if self.fail_effect_once:
                self.fail_effect_once = False
                raise RuntimeError("runtime fuzzer injected effect failure")
            await original(effect, nowait=nowait, epoch=epoch)

        self._replace(
            self.client,
            "_apply_effect",
            MethodType(gated_apply, self.client),
        )

    def _install_mutation(self) -> None:  # noqa: C901
        if self.mutation is RuntimeMutation.WRITER_FAILURE_NO_WAKE:

            async def advance_without_wakeup(pump: object, epoch: int) -> None:
                pump.epoch = epoch  # type: ignore[attr-defined]

            self._replace(
                self.client._write_pump,
                "advance_epoch",
                MethodType(advance_without_wakeup, self.client._write_pump),
            )
        elif self.mutation is RuntimeMutation.EPOCH_NOT_INVALIDATED:

            async def keep_epoch(_client: AsyncClient) -> None:
                return None

            self._replace(
                self.client,
                "_invalidate_connection_epoch",
                MethodType(keep_epoch, self.client),
            )
        elif self.mutation is RuntimeMutation.EFFECT_NOT_SETTLED:

            def omit_progress(_pump: object) -> None:
                return None

            self._replace(
                self.client._effect_pump,
                "_complete",
                MethodType(omit_progress, self.client._effect_pump),
            )
        elif self.mutation is RuntimeMutation.CALLBACK_CANCEL_STOPS_WORKER:

            def kill_worker(
                _delivery: object,
                _callback: object,
                exc: asyncio.CancelledError,
            ) -> None:
                raise exc

            self._replace(
                self.client._delivery,
                "_propagate_callback_cancellation",
                MethodType(kill_worker, self.client._delivery),
            )
        elif self.mutation is RuntimeMutation.LATE_EFFECT_ABANDONED:
            pump = self.client._effect_pump
            original = pump.discard_connection_effects

            def abandon_late_effects(_pump: object, *, settle_publish: bool = False) -> None:
                if pump._failing_close and pump.pending:
                    return
                original(settle_publish=settle_publish)

            self._replace(
                pump,
                "discard_connection_effects",
                MethodType(abandon_late_effects, pump),
            )
        elif self.mutation is RuntimeMutation.USER_TAKEOVER_LOSES:
            original = self.client._prepare_explicit_connect

            async def connected_auto_generation_wins(client: AsyncClient) -> None:
                if client._engine.state in (
                    ConnectionState.CONNECTED,
                    ConnectionState.CONNECTING,
                ):
                    return
                await original()

            self._replace(
                self.client,
                "_prepare_explicit_connect",
                MethodType(connected_auto_generation_wins, self.client),
            )
        elif self.mutation is RuntimeMutation.PUBLISH_WAITER_ACCOUNTING_LEAK:
            self._replace(self.client, "_publish_waiters", 1)

    def _replace(self, owner: object, name: str, replacement: object) -> None:
        self._mutation_restores.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def _restore_mutations(self) -> None:
        while self._mutation_restores:
            owner, name, original = self._mutation_restores.pop()
            setattr(owner, name, original)

    def _spawn_application_task(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        label: str,
        expected_exceptions: tuple[type[BaseException], ...] = (),
        expect_cancelled: bool = False,
    ) -> _ApplicationTask:
        tracked = _ApplicationTask(
            asyncio.create_task(coroutine, name=f"runtime-fuzz-{label}"),
            label,
            expected_exceptions,
            expect_cancelled,
        )
        self.tasks.append(tracked)
        return tracked

    def _check_application_tasks(self, *, final: bool = False) -> None:
        for tracked in self.tasks:
            task = tracked.task
            if not task.done():
                if final:
                    raise AssertionError(f"application task did not settle: {tracked.label}")
                continue
            if tracked.observed:
                continue
            tracked.observed = True
            if task.cancelled():
                if not (tracked.expect_cancelled or tracked.allow_cancelled):
                    raise AssertionError(
                        f"unexpected application task cancellation: {tracked.label}"
                    )
                continue
            exception = task.exception()
            if exception is None:
                if tracked.expect_cancelled:
                    raise AssertionError(
                        f"application task unexpectedly completed: {tracked.label}"
                    )
                continue
            if not isinstance(exception, tracked.expected_exceptions):
                raise AssertionError(
                    "unexpected application task exception: "
                    f"{tracked.label}: {type(exception).__name__}: {exception}"
                )

    def _check_loop_contexts(self, *, final: bool = False) -> None:
        while self._checked_loop_contexts < len(self.loop_contexts):
            context = self.loop_contexts[self._checked_loop_contexts]
            self._checked_loop_contexts += 1
            message = str(context.get("message", ""))
            exception = context.get("exception")
            if (
                self._expected_loop_cancellations
                and message == "mqttium user callback failed"
                and isinstance(exception, asyncio.CancelledError)
            ):
                self._expected_loop_cancellations -= 1
                continue
            name = type(exception).__name__ if exception is not None else "none"
            raise AssertionError(f"unexpected event-loop exception: {message}: {name}: {exception}")
        if final and self._expected_loop_cancellations:
            raise AssertionError(
                "expected callback cancellation was not reported to the event loop"
            )

    async def execute(self, operation: RuntimeOperation) -> None:  # noqa: C901
        rendered = operation.render()
        self.operations.append(rendered)
        actor, action, value = operation.actor, operation.action, operation.value
        if (actor, action) == ("app", "connect"):
            self._spawn_application_task(
                self.client.connect("runtime.invalid", timeout=10),
                label="connect",
            )
        elif (actor, action) == ("app", "publish"):
            qos = QoS(int(value))
            index = len(self.tasks)
            self._spawn_application_task(
                self.client.publish(
                    f"runtime/out/{index}",
                    f"payload-{index}".encode(),
                    qos=qos,
                ),
                label=f"publish-qos{int(qos)}",
            )
        elif (actor, action) == ("app", "disconnect"):
            # Lifecycle operations are schedule participants.  Never await one
            # inline: a mutant may deadlock it, which the terminal checkpoint
            # must classify as a liveness failure and preserve in the artifact.
            self._spawn_application_task(
                self.client.disconnect(),
                label="disconnect",
            )
        elif (actor, action) == ("app", "cancel_last"):
            tracked = next(
                (tracked for tracked in reversed(self.tasks) if not tracked.task.done()),
                None,
            )
            if tracked is None:
                raise AssertionError("no pending application task to cancel")
            tracked.expect_cancelled = False
            tracked.allow_cancelled = True
            tracked.task.cancel()
        elif (actor, action) == ("broker", "connack"):
            self.transport.push(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
        elif (actor, action) == ("broker", "publish"):
            qos = QoS(int(value))
            mid = self.callback_expected + 1 if qos is not QoS.AT_MOST_ONCE else None
            self.callback_expected += 1
            self.transport.push(
                PublishPacket(
                    topic=f"runtime/in/{self.callback_expected}",
                    payload=b"inbound",
                    qos=qos,
                    retain=False,
                    dup=False,
                    mid=mid,
                ).encode(MQTTProtocolVersion.MQTTv5)
            )
        elif (actor, action) == ("broker", "puback_last"):
            raw = self.transport.last(PacketType.PUBLISH)
            if raw is None:
                raise AssertionError("PUBACK requested before an outbound PUBLISH")
            publish = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)
            if publish.mid is None:
                raise AssertionError("PUBACK requested for QoS 0 PUBLISH")
            self.transport.push(PubAckPacket(publish.mid).encode(MQTTProtocolVersion.MQTTv5))
        elif (actor, action) == ("broker", "eof"):
            await self.transport.close()
        elif (actor, action) == ("schedule", "hold_writes"):
            self.transport.hold_writes()
        elif (actor, action) == ("schedule", "release_writes"):
            self.transport.release_writes()
        elif (actor, action) == ("schedule", "hold_close"):
            self.transport.hold_close()
        elif (actor, action) == ("schedule", "release_close"):
            self.transport.release_close()
        elif (actor, action) == ("schedule", "release_factory"):
            self.factory_gate.set()
        elif (actor, action) == ("schedule", "release_callback"):
            self.callback_gate.set()
        elif (actor, action) == ("schedule", "release_effect"):
            self.effect_gate.set()
        elif (actor, action) == ("schedule", "yield"):
            await self._turns(int(value))
        elif (actor, action) == ("transport", "fail_active_write"):
            self.transport.fail_active_write = True
        elif (actor, action) == ("factory", "block_next"):
            self.block_factory_once = True
            self.factory_entered.clear()
            self.factory_gate.clear()
        elif (actor, action) == ("factory", "fail_next"):
            self.fail_factory_once = True
            self.factory_failed.clear()
        elif (actor, action) == ("callback", "cancel_once"):
            self.cancel_callback_once = True
            self._expected_loop_cancellations += 1
        elif (actor, action) == ("callback", "raise_once"):
            self.raise_callback_once = True
        elif (actor, action) == ("callback", "block_once"):
            self.block_callback_once = True
            self.callback_entered.clear()
            self.callback_gate.clear()
        elif (actor, action) == ("callback", "disconnect_once"):
            self.disconnect_callback_once = True
        elif (actor, action) == ("callback", "connect_once"):
            self.connect_callback_once = True
        elif (actor, action) == ("callback", "on_disconnect_connect_once"):
            self.disconnect_callback_connect_once = True
        elif (actor, action) == ("effect", "block_next"):
            self.block_effect_once = True
            self.effect_entered.clear()
            self.effect_gate.clear()
        elif (actor, action) == ("effect", "fail_on_release"):
            self.fail_effect_once = True
        elif (actor, action) == ("effect", "drain_failure"):
            self._spawn_application_task(
                self.client._drain_effects(),
                label="effect-drain",
                expected_exceptions=(RuntimeError,),
            )
        elif (actor, action) == ("effect", "collect_late"):
            async with self.client._engine_lock:
                self.client._engine._effects.append(
                    EngineEffect(
                        EffectKind.SEND,
                        encode_frame(PacketType.PINGREQ, 0, b""),
                    )
                )
                self.client._collect_effects_locked()
        elif actor == "checkpoint":
            await self._checkpoint(action, value)
        else:
            raise AssertionError(f"unknown runtime operation: {rendered}")
        await self._turns(self.settle_turns)
        self._check_application_tasks()
        self._check_loop_contexts()
        self._check_oracles()

    async def _checkpoint(self, action: str, value: str | int | None) -> None:
        label = f"{action}" if value is None else f"{action}:{value}"
        self.checkpoints.append(label)
        if action == "wire":
            packet_type = PacketType[str(value)]
            target = self._wire_targets.get(packet_type, 0) + 1
            self._wire_targets[packet_type] = target
            await self._wait_until(
                lambda: (
                    sum(transport.count(packet_type) for transport in self.transports) >= target
                ),
                f"transport write checkpoint {packet_type.name} was not reached",
            )
            observed = sum(transport.count(packet_type) for transport in self.transports)
            if observed != target:
                raise AssertionError(
                    f"wire multiplicity mismatch for {packet_type.name}: "
                    f"expected={target} observed={observed}"
                )
        elif action == "connected":
            await self._wait_until(
                lambda: self.client.is_connected,
                "client did not reach CONNECTED",
            )
        elif action == "writer_active":
            await self._wait_until(
                lambda: self.transport.active_payload is not None,
                "transport write did not become active",
            )
        elif action == "writer_waiter":
            await self._wait_until(
                lambda: self.client.stats().writer.waiters > 0,
                "reader never blocked on writer admission",
            )
        elif action == "factory_blocked":
            await self._wait_until(
                lambda: self.factory_entered.is_set() and not self.factory_gate.is_set(),
                "reconnect transport factory did not block",
            )
        elif action == "factory_failed":
            await self._wait_until(
                self.factory_failed.is_set,
                "reconnect transport factory failure was not exercised",
            )
        elif action == "callback_active":
            await self._wait_until(
                lambda: self.callback_entered.is_set() and not self.callback_gate.is_set(),
                "callback did not enter its blocked window",
            )
        elif action == "effect_active":
            await self._wait_until(
                lambda: self.effect_entered.is_set() and not self.effect_gate.is_set(),
                "effect application did not enter its blocked window",
            )
        elif action == "effect_failing_close":
            await self._wait_until(
                lambda: (
                    self.client._effect_pump._failing_close
                    and self.transport.close_entered.is_set()
                    and not self.transport.close_gate.is_set()
                ),
                "effect failure did not reach its blocked close window",
            )
        elif action == "takeover_stable":
            await self._turns(40)
            assert len(self.transports) == 2, (
                "automatic reconnect replaced the user-owned callback connection"
            )
            reconnect = self.client._reconnect_task
            assert reconnect is None or reconnect.done(), (
                "automatic reconnect survived explicit callback takeover"
            )
        elif action == "callbacks_drained":
            await self._wait_until(
                lambda: self.client.stats().delivery.callback_queued == 0,
                "callback queue did not drain",
            )
            if self.callback_attempted != self.callback_expected:
                raise AssertionError(
                    "callback worker lost delivery after callback cancellation: "
                    f"attempted={self.callback_attempted} expected={self.callback_expected}"
                )
        elif action == "terminal":
            await self._wait_until(
                lambda: (
                    self.client.stats().state is ConnectionState.DISCONNECTED
                    and not any(asdict(self.client.stats().tasks).values())
                ),
                "terminal teardown did not settle",
            )
            self._check_oracles(terminal=True)
        else:
            raise AssertionError(f"unknown checkpoint {label}")

    async def _wait_until(self, predicate: Any, failure: str) -> None:
        for _ in range(250):
            if predicate():
                return
            await asyncio.sleep(0)
        raise AssertionError(f"liveness timeout: {failure}")

    @staticmethod
    async def _turns(count: int) -> None:
        for _ in range(count):
            await asyncio.sleep(0)

    def _check_oracles(self, *, terminal: bool = False) -> None:
        stats = self.client.stats()
        pump = self.client._write_pump
        queue_bytes = sum(item_size(item) for item in pump.queue._queue)  # type: ignore[attr-defined]
        active_bytes = len(self.transport.active_payload or b"") if self.transports else 0
        assert pump.resident_messages == pump.queue.qsize() + int(active_bytes > 0), (
            "writer resident message count is inconsistent with queued and active ownership"
        )
        assert pump.queued_bytes == queue_bytes + active_bytes, (
            "writer queued byte count is inconsistent with queued and active ownership"
        )
        writer_task = pump.task
        if writer_task is not None and writer_task.done():
            assert stats.writer.waiters == 0, "writer admission waiter survived a dead writer"

        assert stats.effects.applied <= stats.effects.enqueued, (
            "effect pump applied more effects than it enqueued"
        )
        assert stats.effects.pending == stats.effects.enqueued - stats.effects.applied, (
            "effect pump settlement counters cannot reach their drain target"
        )
        if self.client._effect_pump._failing_close:
            assert stats.effects.pending == 0, (
                "effect collected during failing-close was left without an owner"
            )

        outbound = stats.outbound
        assert 0 <= outbound.flow_inflight <= outbound.flow_limit
        assert outbound.queued_messages + outbound.flow_inflight <= outbound.pending_messages
        assert outbound.packet_ids_in_use == outbound.pending_messages
        if not self.client._teardown_final:
            # Receipts mirror unfinished engine records only until terminal
            # teardown fails them; durable session records legitimately
            # outlive their receipts so a present session can be resumed.
            assert stats.receipts.publish == outbound.pending_messages, (
                "publish receipts diverged from unfinished engine records"
            )
        assert 0 <= stats.inbound.inflight <= stats.inbound.receive_maximum
        assert stats.delivery.pending_bytes >= 0
        callback_task = self.client._callback_worker_task
        if self.callback_epoch == stats.connection_epoch and self.client.is_connected:
            assert callback_task is not None and not callback_task.done(), (
                "callback self-cancellation terminated the connection callback worker"
            )

        if self.transports:
            assert all(
                completion_epoch == transport.owner_epoch
                for transport in self.transports
                for _packet, completion_epoch in transport.completed
            ), "transport completed a write after its connection epoch was retired"
            transport_epochs = [transport.owner_epoch for transport in self.transports]
            assert all(
                newer > older
                for older, newer in zip(transport_epochs, transport_epochs[1:], strict=False)
            ), "replacement transports reused a connection epoch"
            assert stats.connection_epoch >= self.transport.owner_epoch, (
                "connection epoch was not invalidated before transport ownership changed"
            )
        if terminal:
            for packet_type, target in self._wire_targets.items():
                observed = sum(transport.count(packet_type) for transport in self.transports)
                assert observed == target, (
                    f"wire multiplicity mismatch for {packet_type.name}: "
                    f"expected={target} observed={observed}"
                )
            assert stats.writer.waiters == 0, "writer waiter survived terminal teardown"
            assert stats.effects.waiters == 0, "effect drain waiter survived terminal teardown"
            assert stats.delivery.waiters == 0, "delivery waiter survived terminal teardown"
            assert stats.receipts.publish_waiters == 0, "publish waiter survived terminal teardown"
            assert pump.resident_messages == 0, "writer retained a message after teardown"
            assert stats.writer.queued_bytes == 0, "writer retained bytes after teardown"
            assert stats.effects.pending == 0, "effect survived terminal teardown"
            assert stats.receipts.publish == 0, "publish receipt survived terminal teardown"
            assert not any(asdict(stats.tasks).values()), (
                "connection-scoped task survived terminal teardown"
            )
            if self.transports:
                assert stats.connection_epoch > self.transport.owner_epoch, (
                    "terminal teardown did not retire its connection epoch"
                )

    def owner_snapshot(self) -> dict[str, Any]:
        stats = asdict(self.client.stats())
        stats["state"] = self.client.stats().state.name
        stats["writer"].pop("last_outbound", None)
        return {
            "client": stats,
            "writer": {
                "epoch": self.client._write_pump.epoch,
                "resident_messages": self.client._write_pump.resident_messages,
                "task_done": (
                    self.client._write_pump.task.done()
                    if self.client._write_pump.task is not None
                    else None
                ),
            },
            "effects": {
                "pending_epoch": self.client._effect_pump.pending_epoch,
                "failing_close": self.client._effect_pump._failing_close,
                "failure_owner": type(self.client._effect_pump.error).__name__
                if self.client._effect_pump.error is not None
                else None,
            },
            "transports": [
                {
                    "generation": transport.generation,
                    "owner_epoch": transport.owner_epoch,
                    "closing": transport.is_closing(),
                    "active_write": transport.active_payload is not None,
                    "attempted_packets": [
                        packet.packet_type.name for packet in transport.attempted
                    ],
                    "completed_packets": [
                        packet.packet_type.name for packet, _epoch in transport.completed
                    ],
                    "completed_epochs": [epoch for _packet, epoch in transport.completed],
                }
                for transport in self.transports
            ],
            "callbacks": {
                "expected": self.callback_expected,
                "attempted": self.callback_attempted,
            },
            "factory": {
                "attempts": self.factory_attempts,
                "blocked": self.factory_entered.is_set() and not self.factory_gate.is_set(),
                "failed": self.factory_failed.is_set(),
            },
            "receipt_settlements": sorted(self.receipt_settlements.values()),
            "application_tasks": [
                {
                    "label": tracked.label,
                    "done": tracked.task.done(),
                    "cancelled": tracked.task.cancelled(),
                    "observed": tracked.observed,
                    "expected_exceptions": [
                        exception.__name__ for exception in tracked.expected_exceptions
                    ],
                    "expect_cancelled": tracked.expect_cancelled,
                    "allow_cancelled": tracked.allow_cancelled,
                }
                for tracked in self.tasks
            ],
            "loop_exceptions": [
                {
                    "message": str(context.get("message", "")),
                    "exception": type(context.get("exception")).__name__
                    if context.get("exception") is not None
                    else None,
                }
                for context in self.loop_contexts
            ],
        }

    async def cleanup(self) -> None:
        self._restore_mutations()
        # Cleanup is terminal even for schedules that enabled reconnect. Set
        # intent before cancelling a reader so its finally block cannot create
        # a successor reconnect owner behind the cleanup pass.
        self.client._intentional_disconnect = True
        for transport in self.transports:
            transport.release_writes()
            transport.release_close()
        self.factory_gate.set()
        self.callback_gate.set()
        self.effect_gate.set()
        # A settlement mutant may have deliberately made an already-removed
        # effect's drain target impossible. Repair only the test harness after
        # the failure snapshot has been captured, then let normal teardown run.
        effect_pump = self.client._effect_pump
        effect_pump.applied = effect_pump.enqueued - len(effect_pump.pending)
        effect_pump.progress.set()
        await self.client._write_pump.wake_waiters()
        with suppress(Exception):
            await asyncio.wait_for(self.client._force_close(), timeout=0.25)
        connection_tasks = {
            task
            for task in (
                self.client._reader_task,
                self.client._keepalive_task,
                self.client._reconnect_task,
                self.client._effect_pump.task,
                self.client._write_pump.task,
                self.client._callback_worker_task,
            )
            if task is not None and task is not asyncio.current_task()
        }
        for task in connection_tasks:
            if not task.done():
                task.cancel()
        if connection_tasks:
            await asyncio.gather(*connection_tasks, return_exceptions=True)
        for tracked in self.tasks:
            if not tracked.task.done():
                tracked.cleanup_cancelled = True
                tracked.task.cancel()
        if self.tasks:
            results = await asyncio.gather(
                *(tracked.task for tracked in self.tasks),
                return_exceptions=True,
            )
            for tracked, result in zip(self.tasks, results, strict=True):
                if isinstance(result, BaseException) and not (
                    tracked.cleanup_cancelled and isinstance(result, asyncio.CancelledError)
                ):
                    # Results reached here only after the schedule's failure
                    # snapshot. Mark them consumed explicitly; normal schedule
                    # results are always checked by _check_application_tasks().
                    tracked.observed = True


async def run_schedule(
    schedule: RuntimeSchedule,
    *,
    mutation: RuntimeMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 2.0,
) -> RuntimeRun:
    harness = _RuntimeHarness(schedule, mutation)
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: harness.loop_contexts.append(context))
    failure: BaseException | None = None
    owners: dict[str, Any] = {}

    async def execute_schedule() -> None:
        for operation in schedule.operations:
            await harness.execute(operation)
        harness._check_application_tasks(final=True)
        harness._check_loop_contexts(final=True)

    try:
        await asyncio.wait_for(execute_schedule(), timeout=watchdog_seconds)
        owners = harness.owner_snapshot()
    except TimeoutError:
        failure = AssertionError(
            f"whole-schedule liveness watchdog expired after {watchdog_seconds:.3f}s"
        )
        owners = harness.owner_snapshot()
    except Exception as exc:
        failure = exc
        owners = harness.owner_snapshot()
    finally:
        try:
            await harness.cleanup()
        except Exception as exc:
            if failure is None:
                failure = AssertionError(f"schedule cleanup failed: {type(exc).__name__}: {exc}")
        if failure is None:
            try:
                harness._check_loop_contexts(final=True)
            except Exception as exc:
                failure = exc
        loop.set_exception_handler(previous_handler)

    if failure is not None:
        artifact = RuntimeFailureArtifact(
            seed=schedule.seed,
            mutation=mutation.value if mutation is not None else None,
            operations=list(harness.operations),
            checkpoints=list(harness.checkpoints),
            owners=owners,
            failure=f"{type(failure).__name__}: {failure}",
        )
        if artifacts_dir is not None:
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / f"runtime-seed{schedule.seed}.json").write_text(
                json.dumps(artifact.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        raise RuntimeFuzzFailure(artifact) from failure
    return RuntimeRun(
        seed=schedule.seed,
        operations=tuple(harness.operations),
        final_snapshot=owners,
    )


async def run_campaign(
    *,
    seeds: Iterable[int],
    steps: int,
    mutation: RuntimeMutation | None = None,
    artifacts_dir: Path | None = None,
) -> CampaignResult:
    completed = 0
    failures: list[int] = []
    operation_traces: set[tuple[str, ...]] = set()
    scheduling_traces: set[tuple[str, ...]] = set()
    coverage: dict[str, int] = {}
    for seed in seeds:
        schedule = generate_schedule(seed, steps)
        rendered = tuple(operation.render() for operation in schedule.operations)
        operation_traces.add(rendered)
        scheduling_traces.add(
            tuple(
                operation.render()
                for operation in schedule.operations
                if operation.actor in {"checkpoint", "schedule", "factory", "effect"}
                or (operation.actor, operation.action)
                in {
                    ("callback", "block_once"),
                    ("callback", "cancel_once"),
                    ("transport", "fail_active_write"),
                    ("app", "cancel_last"),
                }
            )
        )
        for operation in schedule.operations:
            key = f"{operation.actor}.{operation.action}"
            coverage[key] = coverage.get(key, 0) + 1
        try:
            await run_schedule(
                schedule,
                mutation=mutation,
                artifacts_dir=artifacts_dir,
            )
        except RuntimeFuzzFailure:
            failures.append(seed)
        completed += 1
    return CampaignResult(
        completed,
        len(failures),
        tuple(failures),
        len(operation_traces),
        len(scheduling_traces),
        coverage,
    )


async def _main_async(args: argparse.Namespace) -> int:
    result = await run_campaign(
        seeds=range(args.seed, args.seed + args.seeds),
        steps=args.steps,
        mutation=RuntimeMutation(args.mutation) if args.mutation is not None else None,
        artifacts_dir=args.artifacts_dir,
    )
    print(
        f"[DONE] target=runtime seeds={result.completed} failures={result.failures} "
        f"operation_traces={result.unique_operation_traces} "
        f"scheduling_traces={result.unique_scheduling_traces} "
        f"seed_start={args.seed} steps={args.steps}"
    )
    print(f"[COVERAGE] {json.dumps(result.coverage, sort_keys=True)}")
    return int(bool(result.failures))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--seeds", type=int, default=6)
    parser.add_argument("--steps", type=int, default=24)
    parser.add_argument("--mutation", choices=tuple(RuntimeMutation), default=None)
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("/tmp/mqttium-runtime-fuzz"),
    )
    args = parser.parse_args(argv)
    if args.seed < 0 or args.seeds <= 0 or args.steps < 12:
        parser.error("seed must be non-negative; seeds positive; steps at least 12")
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
