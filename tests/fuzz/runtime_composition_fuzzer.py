"""Two-window MQTTium runtime schedule composition on top of the V1 harness."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from types import MethodType
from typing import Any

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.packets import encode_frame
from mqttium.protocol.effects import EngineEffect
from mqttium.transport.writes import WriteItem
from tests.fuzz import runtime_fuzzer as v1
from tests.fuzz.runtime_fuzzer import (
    RuntimeFuzzFailure,
    RuntimeOperation,
    RuntimeRun,
    RuntimeSchedule,
)


class CompositionPair(StrEnum):
    """The deliberately bounded V2 ownership-window pairs."""

    CALLBACK_RECONNECT = "callback_x_reconnect"
    WRITER_RECONNECT = "writer_x_reconnect"
    EFFECT_RECONNECT = "effect_x_reconnect"
    CALLBACK_READER = "callback_x_reader_teardown"
    CALLBACK_WRITER = "callback_x_writer"
    EFFECT_WRITER = "effect_x_writer"


class CompositionMutation(StrEnum):
    """Behavioral breakages that need two ownership windows to become visible."""

    OLD_WRITER_SURVIVES_RECONNECT = "old_writer_survives_reconnect"
    CALLBACK_TAKEOVER_LOSES = "callback_takeover_loses"
    EFFECT_CROSSES_GENERATION = "effect_crosses_generation"
    CLOSING_TRANSPORT_TAKEOVER_LOSES = "closing_transport_takeover_loses"


@dataclass(slots=True, frozen=True)
class ComposedSchedule:
    seed: int
    operations: tuple[RuntimeOperation, ...]
    pair: CompositionPair
    release_order: tuple[str, str]
    release_trace: tuple[str, ...]
    window_depths: tuple[int, ...]
    mutation_window: bool

    def as_v1_schedule(self) -> RuntimeSchedule:
        return RuntimeSchedule(self.seed, self.operations, auto_reconnect=True)


@dataclass(slots=True, frozen=True)
class CompositionCampaignResult:
    completed: int
    failures: int
    failing_seeds: tuple[int, ...]
    total_operations: int
    wall_seconds: float
    schedules_per_second: float
    unique_operation_traces: int
    unique_scheduling_traces: int
    unique_release_traces: int
    coverage: dict[str, int]
    pair_coverage: dict[str, int]
    window_depth_counts: dict[int, int]


@dataclass(slots=True)
class CompositionFailureArtifact:
    seed: int
    pair: str
    mutation: str | None
    operations: list[str]
    release_trace: list[str]
    window_depths: list[int]
    checkpoints: list[str]
    owners: dict[str, Any]
    failure: str
    timing: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {"schema": "mqttium-runtime-fuzz-v2", **asdict(self)}

    def to_text(self) -> str:
        operations = "\n".join(f"{index} {op}" for index, op in enumerate(self.operations))
        return (
            "mqttium-runtime-fuzz-v2\n"
            f"seed={self.seed}\n"
            f"pair={self.pair}\n"
            f"mutation={self.mutation or 'none'}\n"
            f"release_trace={self.release_trace}\n"
            f"failure={self.failure}\n"
            f"timing={json.dumps(self.timing, sort_keys=True)}\n"
            "operations:\n"
            f"{operations}\n"
            "owners:\n"
            f"{json.dumps(self.owners, indent=2, sort_keys=True)}"
        )


def _op(actor: str, action: str, value: str | int | None = None) -> RuntimeOperation:
    return RuntimeOperation(actor, action, value)


def _legal_history(rng: random.Random, budget: int) -> list[RuntimeOperation]:
    """Fill a connected-state prefix exactly without leaving an open window."""
    operations: list[RuntimeOperation] = []
    state = "connected"
    qos = 0
    while budget:
        if state == "connected":
            legal: list[tuple[str, int]] = [("yield", 1)]
            if budget >= 2:
                legal.extend((("outbound0", 2), ("inbound0", 2)))
            if budget >= 3:
                legal.extend((("outbound1", 3), ("inbound1", 3)))
            choice, _cost = rng.choice(legal)
            if choice == "yield":
                operations.append(_op("schedule", "yield", rng.randrange(1, 7)))
                budget -= 1
            elif choice.startswith("outbound"):
                qos = int(choice[-1])
                operations.append(_op("app", "publish", qos))
                budget -= 1
                state = "outbound_wire"
            else:
                qos = int(choice[-1])
                operations.append(_op("broker", "publish", qos))
                budget -= 1
                state = "inbound_wire" if qos else "inbound_callback"
        elif state == "outbound_wire":
            operations.append(_op("checkpoint", "wire", "PUBLISH"))
            budget -= 1
            state = "outbound_ack" if qos else "connected"
        elif state == "outbound_ack":
            operations.append(_op("broker", "puback_last"))
            budget -= 1
            state = "connected"
        elif state == "inbound_wire":
            operations.append(_op("checkpoint", "wire", "PUBACK"))
            budget -= 1
            state = "inbound_callback"
        else:
            operations.append(_op("checkpoint", "callbacks_drained"))
            budget -= 1
            state = "connected"
    assert state == "connected"
    return operations


def _ordered_releases(
    rng: random.Random,
    first: tuple[str, RuntimeOperation],
    second: tuple[str, RuntimeOperation],
) -> tuple[list[RuntimeOperation], tuple[str, str], tuple[str, ...]]:
    releases = [first, second]
    rng.shuffle(releases)
    gap = rng.randrange(1, 7)
    operations = [releases[0][1], _op("schedule", "yield", gap), releases[1][1]]
    order = (releases[0][0], releases[1][0])
    trace = (f"release:{order[0]}", f"yield:{gap}", f"release:{order[1]}")
    return operations, order, trace


def _composition_operations(
    pair: CompositionPair, rng: random.Random
) -> tuple[list[RuntimeOperation], tuple[str, str], tuple[str, ...], bool]:
    """Build one pair-specific overlap and a valid settlement sequence."""
    if pair is CompositionPair.CALLBACK_RECONNECT:
        releases, order, trace = _ordered_releases(
            rng,
            ("callback", _op("schedule", "release_callback")),
            ("reconnect", _op("schedule", "release_factory")),
        )
        operations = [
            _op("callback", "connect_once"),
            _op("callback", "block_once"),
            _op("broker", "publish", 0),
            _op("checkpoint", "callback_active"),
            _op("factory", "block_next"),
            _op("broker", "inject_eof"),
            _op("checkpoint", "factory_blocked"),
            *releases,
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
            _op("checkpoint", "connected"),
            _op("checkpoint", "takeover_generation", 3),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
        return operations, order, (*trace, "takeover:callback"), True

    if pair is CompositionPair.WRITER_RECONNECT:
        release_after_replacement = bool(rng.randrange(2))
        release = _op("schedule", "release_writer", 1)
        operations = [
            _op("schedule", "hold_writes"),
            _op("app", "publish", 0),
            _op("checkpoint", "writer_active"),
            _op("factory", "block_next"),
            _op("broker", "disconnect"),
            _op("checkpoint", "factory_blocked"),
        ]
        if not release_after_replacement:
            operations.append(release)
        operations.extend(
            (
                _op("schedule", "release_factory"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
            )
        )
        if release_after_replacement:
            operations.append(release)
        operations.extend((_op("app", "disconnect"), _op("checkpoint", "terminal")))
        order = ("reconnect", "writer") if release_after_replacement else ("writer", "reconnect")
        trace = (f"release:{order[0]}", "replacement:connected", f"release:{order[1]}")
        return operations, order, trace, release_after_replacement

    if pair is CompositionPair.EFFECT_RECONNECT:
        replay_after_replacement = bool(rng.randrange(2))
        operations = [
            _op("effect", "block_next"),
            _op("broker", "publish", 1),
            _op("checkpoint", "effect_active"),
            _op("factory", "block_next"),
            _op("broker", "inject_eof"),
            _op("schedule", "release_effect"),
            _op("checkpoint", "wire", "PUBACK"),
        ]
        if not replay_after_replacement:
            operations.append(_op("effect", "replay_retired"))
        operations.extend(
            (
                _op("checkpoint", "factory_blocked"),
                _op("schedule", "release_factory"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
            )
        )
        if replay_after_replacement:
            operations.append(_op("effect", "replay_retired"))
        operations.extend(
            (
                _op("checkpoint", "callbacks_drained"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            )
        )
        order = ("effect", "reconnect")
        replay_phase = "after-replacement" if replay_after_replacement else "before-replacement"
        trace = ("release:effect", "release:reconnect", f"retired-effect:{replay_phase}")
        return operations, order, trace, replay_after_replacement

    if pair is CompositionPair.CALLBACK_READER:
        teardown = "keepalive" if rng.randrange(2) else "eof"
        teardown_operation = (
            _op("keepalive", "timeout_due")
            if teardown == "keepalive"
            else _op("broker", "inject_eof")
        )
        releases, order, trace = _ordered_releases(
            rng,
            ("callback", _op("schedule", "release_callback")),
            ("reader", _op("schedule", "release_close", 1)),
        )
        operations = [
            _op("callback", "connect_once"),
            _op("callback", "block_once"),
            _op("broker", "publish", 0),
            _op("checkpoint", "callback_active"),
            _op("schedule", "hold_close"),
            teardown_operation,
            _op("checkpoint", "close_blocked"),
            *releases,
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
        ]
        generation = 2
        if order[0] == "reader":
            operations.extend(
                (
                    _op("checkpoint", "wire", "CONNECT"),
                    _op("broker", "connack"),
                )
            )
            generation = 3
        operations.extend(
            (
                _op("checkpoint", "connected"),
                _op("checkpoint", "takeover_generation", generation),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            )
        )
        return operations, order, (f"teardown:{teardown}", *trace, "takeover:callback"), True

    if pair is CompositionPair.CALLBACK_WRITER:
        releases, order, trace = _ordered_releases(
            rng,
            ("callback", _op("schedule", "release_callback")),
            ("writer", _op("schedule", "release_writes")),
        )
        operations = [
            _op("schedule", "hold_writes"),
            _op("app", "publish", 0),
            _op("checkpoint", "writer_active"),
            _op("callback", "block_once"),
            _op("broker", "publish", 0),
            _op("checkpoint", "callback_active"),
            *releases,
            _op("checkpoint", "wire", "PUBLISH"),
            _op("checkpoint", "callbacks_drained"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
        return operations, order, trace, True

    releases, order, trace = _ordered_releases(
        rng,
        ("effect", _op("schedule", "release_effect")),
        ("writer", _op("schedule", "release_writes")),
    )
    operations = [
        _op("schedule", "hold_writes"),
        _op("app", "publish", 0),
        _op("checkpoint", "writer_active"),
        _op("effect", "block_next"),
        _op("broker", "publish", 1),
        _op("checkpoint", "effect_active"),
        *releases,
        _op("checkpoint", "wire", "PUBLISH"),
        _op("checkpoint", "wire", "PUBACK"),
        _op("checkpoint", "callbacks_drained"),
        _op("app", "disconnect"),
        _op("checkpoint", "terminal"),
    ]
    return operations, order, trace, True


def _window_depths(operations: Sequence[RuntimeOperation]) -> tuple[int, ...]:
    windows: set[str] = set()
    depths: list[int] = []
    opens = {
        ("checkpoint", "writer_active"): "writer",
        ("checkpoint", "callback_active"): "callback",
        ("checkpoint", "effect_active"): "effect",
        ("factory", "block_next"): "reconnect",
        ("checkpoint", "close_blocked"): "reader",
    }
    closes = {
        ("schedule", "release_writes"): "writer",
        ("schedule", "release_writer"): "writer",
        ("schedule", "release_callback"): "callback",
        ("schedule", "release_effect"): "effect",
        ("schedule", "release_factory"): "reconnect",
        ("schedule", "release_close"): "reader",
    }
    for operation in operations:
        opened = opens.get((operation.actor, operation.action))
        if opened is not None:
            windows.add(opened)
        closed = closes.get((operation.actor, operation.action))
        if closed is not None:
            assert closed in windows, f"release without an open {closed} window"
            windows.remove(closed)
        depths.append(len(windows))
    assert not windows, f"unsettled generated windows: {sorted(windows)}"
    return tuple(depths)


def generate_composed_schedule(seed: int, steps: int = 48) -> ComposedSchedule:
    """Generate legal history plus exactly one bounded two-window overlap."""
    if seed < 0 or steps < 32:
        raise ValueError("composition schedules require a non-negative seed and at least 32 steps")
    rng = random.Random(seed)
    pairs = tuple(CompositionPair)
    pair = pairs[seed % len(pairs)]
    composition, order, release_trace, mutation_window = _composition_operations(pair, rng)
    initial = [
        _op("app", "connect"),
        _op("checkpoint", "wire", "CONNECT"),
        _op("broker", "connack"),
        _op("checkpoint", "connected"),
    ]
    history_budget = steps - len(initial) - len(composition)
    if history_budget < 0:
        raise ValueError(f"{steps} steps cannot hold the {pair.value} composition")
    operations = [*initial, *_legal_history(rng, history_budget), *composition]
    depths = _window_depths(operations)
    assert len(operations) == steps
    assert max(depths) == 2
    assert depths[-1] == 0
    return ComposedSchedule(
        seed,
        tuple(operations),
        pair,
        order,
        release_trace,
        depths,
        mutation_window,
    )


class _CompositionTransport(v1._ScheduleTransport):
    def __init__(
        self,
        generation: int,
        owner_epoch: int,
        current_epoch: Any,
        *,
        retain_cancelled_write: bool,
    ) -> None:
        super().__init__(generation, owner_epoch, current_epoch)
        self.retain_cancelled_write = retain_cancelled_write
        self.late_packets: list[Any] = []
        self.close_transitions = 0

    async def write(self, data: WriteItem) -> None:
        attempted_at = len(self.attempted)
        try:
            await super().write(data)
        except asyncio.CancelledError:
            if self.retain_cancelled_write:
                self.late_packets.extend(self.attempted[attempted_at:])
            raise

    async def close(self) -> None:
        if not self.is_closing():
            self.close_transitions += 1
        await super().close()

    def inject_eof(self) -> None:
        self._rx.put_nowait(b"")

    def complete_late_write(self) -> None:
        if not self.late_packets:
            return
        completion_epoch = self._current_epoch()
        self.completed.extend((packet, completion_epoch) for packet in self.late_packets)
        self.late_packets.clear()


class _CompositionHarness(v1._RuntimeHarness):
    def __init__(
        self,
        schedule: ComposedSchedule,
        mutation: CompositionMutation | None,
        *,
        connect_timeout_seconds: float = 0.5,
    ) -> None:
        self.composed_schedule = schedule
        self.composition_mutation = mutation
        self.effect_outcomes: list[tuple[int, int]] = []
        self.retired_effect: tuple[EngineEffect, bool, int, Any] | None = None
        super().__init__(
            schedule.as_v1_schedule(),
            None,
            connect_timeout_seconds=connect_timeout_seconds,
        )
        self._install_composition_mutation()

    async def _factory(
        self, host: str, port: int, *, ssl: object | None = None
    ) -> _CompositionTransport:
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
        transport = _CompositionTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
            lambda: self.client._connection_epoch,
            retain_cancelled_write=(
                self.composition_mutation is CompositionMutation.OLD_WRITER_SURVIVES_RECONNECT
            ),
        )
        self.transports.append(transport)
        return transport

    def _install_effect_gate(self) -> None:
        original = self.client._apply_effect
        original_inline = self.client._apply_effect_inline

        def defer_gated_effect(_client: AsyncClient, effect: EngineEffect, epoch: int) -> bool:
            if self.block_effect_once:
                return False
            return original_inline(effect, epoch)

        async def gated_apply(
            _client: AsyncClient,
            effect: EngineEffect,
            *,
            nowait: bool,
            epoch: int | None = None,
        ) -> None:
            origin = self.client._connection_epoch if epoch is None else epoch
            if self.block_effect_once:
                self.block_effect_once = False
                self.effect_entered.set()
                if self.composition_mutation is CompositionMutation.EFFECT_CROSSES_GENERATION:
                    if self.composed_schedule.pair is CompositionPair.EFFECT_RECONNECT:
                        self.retired_effect = (effect, nowait, origin, original)
                await self.effect_gate.wait()
            if self.fail_effect_once:
                self.fail_effect_once = False
                raise RuntimeError("runtime fuzzer injected effect failure")
            await original(effect, nowait=nowait, epoch=epoch)
            self.effect_outcomes.append((origin, self.client._connection_epoch))

        self._replace(self.client, "_apply_effect", MethodType(gated_apply, self.client))
        self._replace(
            self.client,
            "_apply_effect_inline",
            MethodType(defer_gated_effect, self.client),
        )

    def _install_composition_mutation(self) -> None:
        if self.composition_mutation is CompositionMutation.CALLBACK_TAKEOVER_LOSES:
            original = self.client._prepare_explicit_connect

            async def retired_callback_loses(client: AsyncClient) -> None:
                if (
                    self.callback_epoch is not None
                    and self.callback_epoch != client._connection_epoch
                    and client._engine.state
                    in (ConnectionState.CONNECTED, ConnectionState.CONNECTING)
                ):
                    return
                await original()

            self._replace(
                self.client,
                "_prepare_explicit_connect",
                MethodType(retired_callback_loses, self.client),
            )
        elif self.composition_mutation is CompositionMutation.CLOSING_TRANSPORT_TAKEOVER_LOSES:
            original = self.client._prepare_explicit_connect

            async def ignore_closing_transport(client: AsyncClient) -> None:
                reconnect = client._reconnect_task
                automatic_generation = (
                    reconnect is not None
                    and reconnect is not asyncio.current_task()
                    and not reconnect.done()
                )
                if (
                    client._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING)
                    and not automatic_generation
                ):
                    return
                await original()

            self._replace(
                self.client,
                "_prepare_explicit_connect",
                MethodType(ignore_closing_transport, self.client),
            )

    async def execute(self, operation: RuntimeOperation) -> None:
        actor, action, value = operation.actor, operation.action, operation.value
        if (actor, action) == ("broker", "inject_eof"):
            self.operations.append(operation.render())
            self.transport.inject_eof()
        elif (actor, action) == ("keepalive", "timeout_due"):
            self.operations.append(operation.render())
            keepalive = self.client._keepalive_task
            if keepalive is not None and not keepalive.done():
                keepalive.cancel()
                try:
                    await keepalive
                except asyncio.CancelledError:
                    pass
            self.client._reconfigure(keepalive=1)
            self.client._ping_pending = True
            self.client._ping_deadline = 0.0
            self.client._keepalive_task = asyncio.create_task(
                self.client._keepalive_loop(), name="mqttium-keepalive"
            )
        elif (actor, action) == ("broker", "disconnect"):
            self.operations.append(operation.render())
            self.transport.push(encode_frame(PacketType.DISCONNECT, 0, b""))
        elif (actor, action) == ("schedule", "release_writer"):
            self.operations.append(operation.render())
            generation = int(value)
            transport = self.transports[generation - 1]
            transport.release_writes()
            if len(self.transports) > generation:
                transport.complete_late_write()
            else:
                transport.late_packets.clear()
        elif (actor, action) == ("schedule", "release_close") and value is not None:
            self.operations.append(operation.render())
            generation = int(value)
            self.transports[generation - 1].release_close()
        elif (actor, action) == ("effect", "replay_retired"):
            self.operations.append(operation.render())
            if self.retired_effect is not None:
                effect, nowait, origin, original = self.retired_effect
                self.retired_effect = None
                await original(effect, nowait=nowait, epoch=self.client._connection_epoch)
                self.effect_outcomes.append((origin, self.client._connection_epoch))
        elif (actor, action) == ("checkpoint", "close_blocked"):
            self.operations.append(operation.render())
            self.checkpoints.append("close_blocked")
            await self._wait_until(
                lambda: (
                    self.transport.close_entered.is_set() and not self.transport.close_gate.is_set()
                ),
                "reader teardown did not block closing its transport",
            )
        elif (actor, action) == ("checkpoint", "takeover_generation"):
            self.operations.append(operation.render())
            generation = int(value)
            self.checkpoints.append(f"takeover_generation:{generation}")
            await self._wait_until(
                lambda: self.client.is_connected and len(self.transports) == generation,
                "explicit callback takeover did not install its generation",
            )
            await self._turns(40)
            reconnect = self.client._reconnect_task
            assert len(self.transports) == generation
            assert reconnect is None or reconnect.done(), (
                "automatic reconnect survived explicit callback takeover"
            )
        else:
            await super().execute(operation)
            return
        await self._turns(4)
        self._check_application_tasks()
        self._check_loop_contexts()
        self._check_oracles()

    def _check_oracles(self, *, terminal: bool = False) -> None:
        super()._check_oracles(terminal=terminal)
        assert all(origin == terminal_epoch for origin, terminal_epoch in self.effect_outcomes), (
            "effect crossed from a retired connection generation"
        )
        assert all(transport.close_transitions <= 1 for transport in self.transports), (
            "connection generation began visible teardown more than once"
        )
        if self.client._transport is not None:
            assert self.client._transport is self.transport, (
                "retired transport remained installed after replacement"
            )
        if terminal:
            assert self.callback_gate.is_set(), "callback ownership window survived quiescence"
            assert self.effect_gate.is_set(), "effect ownership window survived quiescence"
            assert self.factory_gate.is_set(), "reconnect ownership window survived quiescence"
            assert all(transport.write_gate.is_set() for transport in self.transports), (
                "writer ownership window survived quiescence"
            )
            assert all(transport.close_gate.is_set() for transport in self.transports), (
                "reader teardown window survived quiescence"
            )
            assert not self.retired_effect, "effect had no terminal owner after quiescence"
            assert not any(transport.late_packets for transport in self.transports), (
                "cancelled write had no terminal owner after quiescence"
            )

    def owner_snapshot(self) -> dict[str, Any]:
        snapshot = super().owner_snapshot()
        snapshot["composition"] = {
            "pair": self.composed_schedule.pair.value,
            "release_trace": list(self.composed_schedule.release_trace),
            "callback_blocked": not self.callback_gate.is_set(),
            "effect_blocked": not self.effect_gate.is_set(),
            "factory_blocked": not self.factory_gate.is_set(),
            "writer_gates": [
                transport.generation
                for transport in self.transports
                if not transport.write_gate.is_set()
            ],
            "close_gates": [
                transport.generation
                for transport in self.transports
                if not transport.close_gate.is_set()
            ],
            "effect_outcomes": [list(outcome) for outcome in self.effect_outcomes],
        }
        return snapshot


async def run_composed_schedule(
    schedule: ComposedSchedule,
    *,
    mutation: CompositionMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 2.0,
    connect_timeout_seconds: float = 0.5,
) -> RuntimeRun:
    harness = _CompositionHarness(
        schedule,
        mutation,
        connect_timeout_seconds=connect_timeout_seconds,
    )
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
        artifact = CompositionFailureArtifact(
            schedule.seed,
            schedule.pair.value,
            mutation.value if mutation is not None else None,
            list(harness.operations),
            list(schedule.release_trace),
            list(schedule.window_depths),
            list(harness.checkpoints),
            owners,
            f"{type(failure).__name__}: {failure}",
            {
                "connect_timeout_seconds": connect_timeout_seconds,
                "watchdog_seconds": watchdog_seconds,
            },
        )
        if artifacts_dir is not None:
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            path = artifacts_dir / f"runtime-composition-seed{schedule.seed}.json"
            path.write_text(
                json.dumps(artifact.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        raise RuntimeFuzzFailure(artifact) from failure  # type: ignore[arg-type]
    return RuntimeRun(schedule.seed, tuple(harness.operations), owners)


async def run_composition_campaign(
    *,
    seeds: Iterable[int],
    steps: int,
    mutation: CompositionMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 2.0,
    connect_timeout_seconds: float = 0.5,
) -> CompositionCampaignResult:
    started = time.monotonic()
    completed = 0
    failing_seeds: list[int] = []
    operation_traces: set[tuple[str, ...]] = set()
    scheduling_traces: set[tuple[str, ...]] = set()
    release_traces: set[tuple[str, ...]] = set()
    coverage: Counter[str] = Counter()
    pair_coverage: Counter[str] = Counter()
    window_depth_counts: Counter[int] = Counter()
    for seed in seeds:
        schedule = generate_composed_schedule(seed, steps)
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
                    ("callback", "connect_once"),
                    ("broker", "inject_eof"),
                    ("app", "cancel_last"),
                }
            )
        )
        release_traces.add(schedule.release_trace)
        pair_coverage[schedule.pair.value] += 1
        window_depth_counts.update(schedule.window_depths)
        coverage.update(
            f"{operation.actor}.{operation.action}" for operation in schedule.operations
        )
        try:
            await run_composed_schedule(
                schedule,
                mutation=mutation,
                artifacts_dir=artifacts_dir,
                watchdog_seconds=watchdog_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
            )
        except RuntimeFuzzFailure:
            failing_seeds.append(seed)
        completed += 1
    wall = time.monotonic() - started
    return CompositionCampaignResult(
        completed,
        len(failing_seeds),
        tuple(failing_seeds),
        sum(coverage.values()),
        wall,
        completed / wall,
        len(operation_traces),
        len(scheduling_traces),
        len(release_traces),
        dict(sorted(coverage.items())),
        dict(sorted(pair_coverage.items())),
        dict(sorted(window_depth_counts.items())),
    )


async def _main_async(args: argparse.Namespace) -> int:
    result = await run_composition_campaign(
        seeds=range(args.seed, args.seed + args.seeds),
        steps=args.steps,
        mutation=(CompositionMutation(args.mutation) if args.mutation is not None else None),
        artifacts_dir=args.artifacts_dir,
        watchdog_seconds=args.watchdog_seconds,
        connect_timeout_seconds=args.connect_timeout_seconds,
    )
    print(
        f"[DONE] target=runtime-composition seeds={result.completed} "
        f"failures={result.failures} operation_traces={result.unique_operation_traces} "
        f"scheduling_traces={result.unique_scheduling_traces} "
        f"release_traces={result.unique_release_traces} seed_start={args.seed} "
        f"steps={args.steps} wall_seconds={result.wall_seconds:.6f} "
        f"schedules_per_second={result.schedules_per_second:.2f}"
    )
    print(f"[PAIRS] {json.dumps(result.pair_coverage, sort_keys=True)}")
    print(f"[WINDOWS] {json.dumps(result.window_depth_counts, sort_keys=True)}")
    print(f"[COVERAGE] {json.dumps(result.coverage, sort_keys=True)}")
    return int(bool(result.failures))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--seeds", type=int, default=24)
    parser.add_argument("--steps", type=int, default=48)
    parser.add_argument(
        "--watchdog-seconds",
        type=float,
        default=2.0,
        help="whole-schedule wall-clock watchdog (raise for a shared low-priority runner)",
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=0.5,
        help="harness reconnect/callback-connect deadline",
    )
    parser.add_argument("--mutation", choices=tuple(CompositionMutation), default=None)
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("/tmp/mqttium-runtime-composition-fuzz"),
    )
    args = parser.parse_args(argv)
    if (
        args.seed < 0
        or args.seeds <= 0
        or args.steps < 32
        or args.watchdog_seconds <= 0
        or args.connect_timeout_seconds <= 0
    ):
        parser.error("seed must be non-negative; counts and timeouts positive; steps at least 32")
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
