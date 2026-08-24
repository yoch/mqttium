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
from collections.abc import Iterable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from types import MethodType
from typing import Any

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.transport.writes import WriteItem, item_size


class RuntimeMutation(StrEnum):
    """Test-only mechanical breakages used to qualify the target."""

    WRITER_FAILURE_NO_WAKE = "writer_failure_no_wake"
    EPOCH_NOT_INVALIDATED = "epoch_not_invalidated"
    EFFECT_NOT_SETTLED = "effect_not_settled"
    CALLBACK_CANCEL_STOPS_WORKER = "callback_cancel_stops_worker"


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


class _ScheduleTransport:
    """Packet-aware transport with one explicit write gate and failure switch."""

    def __init__(self, generation: int, owner_epoch: int) -> None:
        self.generation = generation
        self.owner_epoch = owner_epoch
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.write_gate = asyncio.Event()
        self.write_gate.set()
        self.write_entered = asyncio.Event()
        self.fail_active_write = False
        self.active_payload: bytes | None = None
        self.written: list[RawPacket] = []

    async def write(self, data: WriteItem) -> None:
        payload = b"".join(data) if isinstance(data, tuple) else data
        self.active_payload = payload
        self._decoder.feed(payload)
        self.written.extend(self._decoder.drain_packets())
        self.write_entered.set()
        try:
            await self.write_gate.wait()
            if self.fail_active_write:
                self.fail_active_write = False
                raise ConnectionResetError("runtime fuzzer injected write failure")
        finally:
            self.active_payload = None

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
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

    def count(self, packet_type: PacketType) -> int:
        return sum(packet.packet_type is packet_type for packet in self.written)

    def last(self, packet_type: PacketType) -> RawPacket | None:
        return next(
            (packet for packet in reversed(self.written) if packet.packet_type is packet_type),
            None,
        )


def _op(actor: str, action: str, value: str | int | None = None) -> RuntimeOperation:
    return RuntimeOperation(actor, action, value)


def generate_schedule(seed: int, steps: int = 24) -> RuntimeSchedule:
    """Generate one short adversarial schedule from a stable seed.

    The three motifs target ownership boundaries, while neutral QoS 1 round
    trips vary packet identifiers and event-loop history before the boundary.
    """

    if steps < 12:
        raise ValueError("runtime schedules require at least 12 steps")
    rng = random.Random(seed)
    operations = [
        _op("app", "connect"),
        _op("checkpoint", "wire", "CONNECT"),
        _op("broker", "connack"),
        _op("checkpoint", "connected"),
    ]

    # Vary the history without letting it dominate the ownership race.
    neutral_rounds = min(rng.randrange(3), max(0, (steps - 12) // 3))
    for _ in range(neutral_rounds):
        operations.extend(
            (
                _op("app", "publish", 1),
                _op("checkpoint", "wire", "PUBLISH"),
                _op("broker", "puback_last"),
            )
        )

    motif = seed % 3
    if motif == 0:
        operations.extend(
            (
                _op("schedule", "hold_writes"),
                _op("app", "publish", 0),
                _op("checkpoint", "writer_active"),
                _op("broker", "publish", 1),
                _op("checkpoint", "writer_waiter"),
                _op("transport", "fail_active_write"),
                _op("schedule", "release_writes"),
                _op("checkpoint", "terminal"),
            )
        )
    elif motif == 1:
        operations.extend(
            (
                _op("callback", "cancel_once"),
                _op("broker", "publish", 0),
                _op("broker", "publish", 0),
                _op("checkpoint", "callbacks_drained"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            )
        )
    else:
        operations.extend(
            (
                _op("broker", "eof"),
                _op("checkpoint", "terminal"),
                _op("app", "connect"),
                _op("checkpoint", "wire", "CONNECT"),
                _op("broker", "connack"),
                _op("checkpoint", "connected"),
                _op("app", "disconnect"),
                _op("checkpoint", "terminal"),
            )
        )
    if len(operations) > steps:
        raise AssertionError("generated runtime schedule exceeded its step budget")
    return RuntimeSchedule(seed=seed, operations=tuple(operations))


class _RuntimeHarness:
    def __init__(self, schedule: RuntimeSchedule, mutation: RuntimeMutation | None) -> None:
        self.schedule = schedule
        self.mutation = mutation
        self.transports: list[_ScheduleTransport] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.operations: list[str] = []
        self.checkpoints: list[str] = []
        self.callback_expected = 0
        self.callback_attempted = 0
        self.cancel_callback_once = False
        self.loop_contexts: list[dict[str, Any]] = []
        self._mutation_restores: list[tuple[object, str, object]] = []
        self._wire_targets: dict[tuple[int, PacketType], int] = {}
        self.client = AsyncClient(
            client_id=f"runtime-fuzz-{schedule.seed}",
            protocol=MQTTProtocolVersion.MQTTv5,
            reconnect=ReconnectPolicy(enabled=False),
            max_outbound_messages=1,
            max_outbound_bytes=4096,
            max_pending_callbacks=4,
            message_delivery="callback",
            keepalive=0,
        )
        self.client._transport_factory = self._factory
        self.client.on_message = self._on_message
        self._install_mutation()

    @property
    def transport(self) -> _ScheduleTransport:
        if not self.transports:
            raise AssertionError("schedule has no transport")
        return self.transports[-1]

    async def _factory(
        self, host: str, port: int, *, ssl: object | None = None
    ) -> _ScheduleTransport:
        del host, port, ssl
        transport = _ScheduleTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
        )
        self.transports.append(transport)
        return transport

    async def _on_message(self, _message: object) -> None:
        self.callback_attempted += 1
        if self.cancel_callback_once:
            self.cancel_callback_once = False
            raise asyncio.CancelledError("runtime fuzzer callback self-cancellation")

    def _install_mutation(self) -> None:
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

    def _replace(self, owner: object, name: str, replacement: object) -> None:
        self._mutation_restores.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def _restore_mutations(self) -> None:
        while self._mutation_restores:
            owner, name, original = self._mutation_restores.pop()
            setattr(owner, name, original)

    async def execute(self, operation: RuntimeOperation) -> None:  # noqa: C901
        rendered = operation.render()
        self.operations.append(rendered)
        actor, action, value = operation.actor, operation.action, operation.value
        if (actor, action) == ("app", "connect"):
            self.tasks.append(
                asyncio.create_task(
                    self.client.connect("runtime.invalid", timeout=10),
                    name="runtime-fuzz-connect",
                )
            )
        elif (actor, action) == ("app", "publish"):
            qos = QoS(int(value))
            index = len(self.tasks)
            self.tasks.append(
                asyncio.create_task(
                    self.client.publish(
                        f"runtime/out/{index}",
                        f"payload-{index}".encode(),
                        qos=qos,
                    ),
                    name=f"runtime-fuzz-publish-qos{int(qos)}",
                )
            )
        elif (actor, action) == ("app", "disconnect"):
            # Lifecycle operations are schedule participants.  Never await one
            # inline: a mutant may deadlock it, which the terminal checkpoint
            # must classify as a liveness failure and preserve in the artifact.
            self.tasks.append(
                asyncio.create_task(
                    self.client.disconnect(),
                    name="runtime-fuzz-disconnect",
                )
            )
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
        elif (actor, action) == ("transport", "fail_active_write"):
            self.transport.fail_active_write = True
        elif (actor, action) == ("callback", "cancel_once"):
            self.cancel_callback_once = True
        elif actor == "checkpoint":
            await self._checkpoint(action, value)
        else:
            raise AssertionError(f"unknown runtime operation: {rendered}")
        await self._turns(4)
        self._check_oracles()

    async def _checkpoint(self, action: str, value: str | int | None) -> None:
        label = f"{action}" if value is None else f"{action}:{value}"
        self.checkpoints.append(label)
        if action == "wire":
            packet_type = PacketType[str(value)]
            key = (self.transport.generation, packet_type)
            target = self._wire_targets.get(key, 0) + 1
            self._wire_targets[key] = target
            await self._wait_until(
                lambda: self.transport.count(packet_type) >= target,
                f"transport write checkpoint {packet_type.name} was not reached",
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

        outbound = stats.outbound
        assert 0 <= outbound.flow_inflight <= outbound.flow_limit
        assert outbound.queued_messages + outbound.flow_inflight <= outbound.pending_messages
        assert outbound.packet_ids_in_use == outbound.pending_messages
        assert stats.receipts.publish == outbound.pending_messages
        assert 0 <= stats.inbound.inflight <= stats.inbound.receive_maximum
        assert stats.delivery.pending_bytes >= 0
        callback_task = self.client._callback_worker_task
        if self.callback_attempted and self.client.is_connected:
            assert callback_task is not None and not callback_task.done(), (
                "callback self-cancellation terminated the connection callback worker"
            )

        if self.transports:
            transport_epochs = [transport.owner_epoch for transport in self.transports]
            assert all(
                newer > older
                for older, newer in zip(transport_epochs, transport_epochs[1:], strict=False)
            ), "replacement transports reused a connection epoch"
            assert stats.connection_epoch >= self.transport.owner_epoch, (
                "connection epoch was not invalidated before transport ownership changed"
            )
        if terminal:
            assert stats.writer.waiters == 0, "writer waiter survived terminal teardown"
            assert stats.effects.waiters == 0, "effect drain waiter survived terminal teardown"
            assert stats.delivery.waiters == 0, "delivery waiter survived terminal teardown"
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
                    "packets": [packet.packet_type.name for packet in transport.written],
                }
                for transport in self.transports
            ],
            "callbacks": {
                "expected": self.callback_expected,
                "attempted": self.callback_attempted,
            },
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
        for transport in self.transports:
            transport.release_writes()
        # A settlement mutant may have deliberately made an already-removed
        # effect's drain target impossible. Repair only the test harness after
        # the failure snapshot has been captured, then let normal teardown run.
        effect_pump = self.client._effect_pump
        effect_pump.applied = effect_pump.enqueued - len(effect_pump.pending)
        effect_pump.progress.set()
        await self.client._write_pump.wake_waiters()
        with suppress(Exception):
            await asyncio.wait_for(self.client._force_close(), timeout=0.25)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)


async def run_schedule(
    schedule: RuntimeSchedule,
    *,
    mutation: RuntimeMutation | None = None,
    artifacts_dir: Path | None = None,
) -> RuntimeRun:
    harness = _RuntimeHarness(schedule, mutation)
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: harness.loop_contexts.append(context))
    failure: BaseException | None = None
    owners: dict[str, Any] = {}
    try:
        for operation in schedule.operations:
            await harness.execute(operation)
        owners = harness.owner_snapshot()
    except Exception as exc:
        failure = exc
        owners = harness.owner_snapshot()
    finally:
        await harness.cleanup()
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
    for seed in seeds:
        try:
            await run_schedule(
                generate_schedule(seed, steps),
                mutation=mutation,
                artifacts_dir=artifacts_dir,
            )
        except RuntimeFuzzFailure:
            failures.append(seed)
        completed += 1
    return CampaignResult(completed, len(failures), tuple(failures))


async def _main_async(args: argparse.Namespace) -> int:
    result = await run_campaign(
        seeds=range(args.seed, args.seed + args.seeds),
        steps=args.steps,
        artifacts_dir=args.artifacts_dir,
    )
    print(
        f"[DONE] target=runtime seeds={result.completed} failures={result.failures} "
        f"seed_start={args.seed} steps={args.steps}"
    )
    return int(bool(result.failures))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--seeds", type=int, default=6)
    parser.add_argument("--steps", type=int, default=24)
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
