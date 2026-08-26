"""V3 pressure/interleaving MQTTium runtime schedules on top of the V1 harness.

The V1/V2 targets keep most of the runtime pressure surface structurally cold:
their transport has no ``write_nowait``/``write_many``, the writer admits one
frame, payloads are tiny, and every operation settles four event-loop turns.
This bounded profile varies exactly those axes — transport capabilities,
writer sizing, producer bursts, payload shape classes, and the per-operation
settlement budget — and composes the resulting pressure with at most one
lifecycle ownership window at a time (issue #388). It also creates and
observes *active* parked application publishers and qualifies every exit from
the parked state (issue #389 part B).

Like V1/V2 this is deliberately test infrastructure, not a generic asyncio
simulator: schedules are seed-reproducible and the real event loop, client,
pumps, and delivery queues execute every transition.
"""

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

from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PubAckPacket, PublishPacket
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from tests.fuzz import runtime_fuzzer as v1
from tests.fuzz.runtime_composition_fuzzer import _legal_history
from tests.fuzz.runtime_fuzzer import (
    RuntimeFuzzFailure,
    RuntimeOperation,
    RuntimeRun,
    RuntimeSchedule,
)


class PressureFamily(StrEnum):
    """The deliberately bounded V3 pressure motifs."""

    EAGER_PACED = "eager_paced"
    BURST_LATENCY_BATCH = "burst_latency_batch"
    BURST_WRITE_MANY = "burst_write_many"
    SEGMENTED = "segmented"
    PARKED_RELEASE = "parked_release"
    PARKED_CANCEL = "parked_cancel"
    PARKED_TEARDOWN = "parked_teardown"
    PRESSURE_READER_TEARDOWN = "pressure_reader_teardown"
    PRESSURE_RECONNECT = "pressure_reconnect"
    PRESSURE_CALLBACK = "pressure_callback"
    PRESSURE_EFFECT = "pressure_effect"


class PressureMutation(StrEnum):
    """Behavioral breakages that need the pressure surfaces to become visible."""

    EAGER_ACCEPT_DROPS_FRAME = "eager_accept_drops_frame"
    EAGER_REFUSAL_LIES = "eager_refusal_lies"
    SEGMENTED_PAYLOAD_DROPPED = "segmented_payload_dropped"
    PARKED_PUBLISHER_NOT_WOKEN = "parked_publisher_not_woken"
    PUBLISH_WAITER_DECREMENT_LOST = "publish_waiter_decrement_lost"
    WRITE_MANY_DECOALESCED = "write_many_decoalesced"
    WRITER_PRESSURE_BYPASSED = "writer_pressure_bypassed"
    PRESSURE_LIFECYCLE_SEPARATED = "pressure_lifecycle_separated"


# Payload shape classes (issue #388): tiny frames, frames sized so a four-item
# burst crosses the writer's 48 KiB latency-batch target, and payloads past
# SEGMENT_THRESHOLD (128 KiB) that must take the segmented two-write form.
_PAYLOAD_SIZES = {
    "tiny": 16,
    "batch": 13_000,
    "segmented": 140_000,
}

# Families that saturate the engine's pending-outbound admission to park a
# third concurrent application publisher behind two unacknowledged QoS 1
# exchanges.
_PARKED_FAMILIES = frozenset(
    {
        PressureFamily.PARKED_RELEASE,
        PressureFamily.PARKED_CANCEL,
        PressureFamily.PARKED_TEARDOWN,
    }
)


@dataclass(slots=True, frozen=True)
class PressureProfile:
    """Per-schedule transport capability surface."""

    write_nowait: bool
    write_many: bool


@dataclass(slots=True, frozen=True)
class PressureSchedule:
    seed: int
    operations: tuple[RuntimeOperation, ...]
    family: PressureFamily
    profile: PressureProfile
    settle_plan: tuple[int, ...]

    def as_v1_schedule(self) -> RuntimeSchedule:
        return RuntimeSchedule(
            self.seed,
            self.operations,
            auto_reconnect=self.family is PressureFamily.PRESSURE_RECONNECT,
        )


@dataclass(slots=True, frozen=True)
class PressureCampaignResult:
    completed: int
    failures: int
    failing_seeds: tuple[int, ...]
    wall_seconds: float
    unique_operation_traces: int
    unique_scheduling_traces: int
    coverage: dict[str, int]
    family_coverage: dict[str, int]
    pressure_coverage: dict[str, int]


@dataclass(slots=True)
class PressureFailureArtifact:
    seed: int
    family: str
    profile: dict[str, bool]
    mutation: str | None
    operations: list[str]
    settle_plan: list[int]
    checkpoints: list[str]
    owners: dict[str, Any]
    failure: str
    timing: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {"schema": "mqttium-runtime-fuzz-v3", **asdict(self)}

    def to_text(self) -> str:
        operations = "\n".join(f"{index} {op}" for index, op in enumerate(self.operations))
        return (
            "mqttium-runtime-fuzz-v3\n"
            f"seed={self.seed}\n"
            f"family={self.family}\n"
            f"profile={json.dumps(self.profile, sort_keys=True)}\n"
            f"timing={json.dumps(self.timing, sort_keys=True)}\n"
            f"mutation={self.mutation or 'none'}\n"
            f"failure={self.failure}\n"
            "operations:\n"
            f"{operations}\n"
            "owners:\n"
            f"{json.dumps(self.owners, indent=2, sort_keys=True)}"
        )


# The pressure surfaces a campaign must actually reach before its green result
# can be trusted (issue #388). Zero hits on any of them is a coverage failure.
REQUIRED_PRESSURE_COVERAGE = (
    "eager_accepted",
    "eager_refused",
    "latency_batches",
    "write_many_calls",
    "segmented_writes",
    "parked_publisher_observed",
    "writer_waiters_observed",
    "writer_4_resident_observed",
    "writer_16_resident_observed",
    "pressure_lifecycle_overlaps",
    "pressure_reader_teardown_overlaps",
    "pressure_reconnect_overlaps",
    "pressure_callback_overlaps",
    "pressure_effect_overlaps",
)


def _op(actor: str, action: str, value: str | int | None = None) -> RuntimeOperation:
    return RuntimeOperation(actor, action, value)


def _family_operations(  # noqa: C901
    family: PressureFamily,
    rng: random.Random,
) -> list[RuntimeOperation]:
    """Build one bounded pressure motif ending in terminal quiescence."""
    if family is PressureFamily.EAGER_PACED:
        # One paced publish takes the zero-hop eager path; a deterministic
        # refusal forces the next one through fallback queueing and the
        # writer task. Re-arming needs one idle loop turn between them.
        operations = [
            _op("schedule", "settle", rng.choice((1, 2, 4))),
            _op("app", "publish_class", "0:tiny"),
            # The CONNECT frame already rode the eager path, so the floor for
            # this publish is two accepted eager writes.
            _op("checkpoint", "eager_accepted", 2),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("schedule", "yield", rng.randrange(1, 4)),
            _op("transport", "refuse_nowait", 1),
            _op("app", "publish_class", "0:tiny"),
            _op("checkpoint", "eager_refused", 1),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
        return operations
    if family is PressureFamily.BURST_LATENCY_BATCH:
        # Four QoS 1 batch-class frames admitted in one event-loop turn: the
        # first eager attempt is refused so all four stay resident, and the
        # fourth publisher flushes the whole burst through the latency batch.
        return [
            _op("transport", "refuse_nowait", 1),
            _op("schedule", "settle", 0),
            _op("app", "burst", "4:1:batch"),
            _op("schedule", "settle", 2),
            _op("checkpoint", "latency_batch", 1),
            _op("checkpoint", "wire_bulk", "PUBLISH:4"),
            _op("broker", "puback_pending"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
    if family is PressureFamily.BURST_WRITE_MANY:
        # No eager path: the same one-turn burst reaches the writer task as
        # one four-frame batch, coalesced through the transport's write_many.
        return [
            _op("transport", "reset_write_many"),
            _op("schedule", "settle", 0),
            _op("app", "burst", "4:1:batch"),
            _op("schedule", "settle", 2),
            _op("checkpoint", "write_many", 1),
            _op("checkpoint", "wire_bulk", "PUBLISH:4"),
            _op("broker", "puback_pending"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
    if family is PressureFamily.SEGMENTED:
        # A payload past SEGMENT_THRESHOLD takes the two-write segmented form,
        # which must never ride the eager path even when one is available.
        return [
            _op("schedule", "settle", rng.choice((1, 2, 4))),
            _op("app", "publish_class", "1:segmented"),
            _op("checkpoint", "segmented", 1),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("broker", "puback_pending"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
    if family in _PARKED_FAMILIES:
        operations = [
            _op("schedule", "settle", rng.choice((1, 2))),
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            # The third concurrent publisher exceeds
            # max_pending_outbound_messages=2 and parks on admission capacity.
            _op(
                "app",
                "publish_class_terminal"
                if family is PressureFamily.PARKED_TEARDOWN
                else ("publish_class"),
                "1:tiny",
            ),
            _op("checkpoint", "publisher_parked"),
        ]
        if family is PressureFamily.PARKED_RELEASE:
            # Normal exit: broker acknowledgements release admission capacity,
            # the parked publisher retries, admits, and reaches the wire.
            operations.extend(
                (
                    _op("broker", "puback_pending"),
                    _op("checkpoint", "wire", "PUBLISH"),
                    _op("broker", "puback_pending"),
                    _op("app", "disconnect"),
                    _op("checkpoint", "terminal"),
                )
            )
        elif family is PressureFamily.PARKED_CANCEL:
            # Explicit cancellation: the parked publisher hands its wakeup on
            # and settles exactly once as cancelled.
            operations.extend(
                (
                    _op("app", "cancel_last"),
                    _op("broker", "puback_pending"),
                    _op("app", "disconnect"),
                    _op("checkpoint", "terminal"),
                )
            )
        else:
            # Teardown exit: terminal disconnect wakes the parked publisher,
            # whose retry must fail terminally rather than park forever.
            operations.extend(
                (
                    _op("app", "disconnect"),
                    _op("checkpoint", "terminal"),
                )
            )
        return operations
    if family is PressureFamily.PRESSURE_READER_TEARDOWN:
        # Hold transport.close() so the reader teardown is an observable owner
        # while two unfinished QoS 1 records retain outbound pressure.
        return [
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("schedule", "hold_close"),
            _op("broker", "inject_eof"),
            _op("checkpoint", "close_blocked"),
            _op("checkpoint", "overlap", "reader_teardown"),
            _op("schedule", "release_close"),
            _op("checkpoint", "terminal"),
        ]
    if family is PressureFamily.PRESSURE_RECONNECT:
        # The reconnect factory owns the lifecycle transition while a third
        # application publisher remains parked behind two unfinished records.
        return [
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("app", "publish_class", "1:tiny"),
            _op("checkpoint", "publisher_parked"),
            _op("factory", "block_next"),
            _op("broker", "inject_eof"),
            _op("checkpoint", "factory_blocked"),
            _op("checkpoint", "overlap", "reconnect"),
            _op("schedule", "release_factory"),
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
            _op("checkpoint", "connected"),
            _op("checkpoint", "wire", "PUBLISH"),
            _op("broker", "puback_pending"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]

    if family is PressureFamily.PRESSURE_CALLBACK:
        # Open the callback worker first: it is independent from the EffectPump
        # path that subsequently parks application publishers on the writer.
        burst = 18
        return [
            _op("callback", "block_once"),
            _op("broker", "publish", 0),
            _op("checkpoint", "callback_active"),
            _op("schedule", "hold_writes"),
            _op("app", "publish_class", "0:tiny"),
            _op("checkpoint", "writer_active"),
            _op("schedule", "settle", 0),
            _op("app", "burst", f"{burst}:0:tiny"),
            _op("schedule", "settle", 2),
            _op("checkpoint", "writer_waiter"),
            _op("checkpoint", "overlap", "callback"),
            _op("schedule", "release_callback"),
            _op("schedule", "release_writes"),
            _op("checkpoint", "wire_bulk", f"PUBLISH:{burst + 1}"),
            _op("checkpoint", "callbacks_drained"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]

    assert family is PressureFamily.PRESSURE_EFFECT
    # EffectPump ownership cannot be opened behind writer-blocked SEND effects;
    # instead, retain unfinished QoS 1 session pressure while blocking a later
    # inbound effect. This still composes exactly one lifecycle owner.
    return [
        _op("app", "publish_class", "1:tiny"),
        _op("checkpoint", "wire", "PUBLISH"),
        _op("app", "publish_class", "1:tiny"),
        _op("checkpoint", "wire", "PUBLISH"),
        _op("effect", "block_next"),
        _op("broker", "publish", 1),
        _op("checkpoint", "effect_active"),
        _op("checkpoint", "overlap", "effect"),
        _op("schedule", "release_effect"),
        _op("checkpoint", "wire", "PUBACK"),
        _op("checkpoint", "callbacks_drained"),
        _op("broker", "puback_pending"),
        _op("app", "disconnect"),
        _op("checkpoint", "terminal"),
    ]


def generate_pressure_schedule(seed: int, steps: int = 36) -> PressureSchedule:
    """Generate legal history plus exactly one bounded pressure motif."""
    if seed < 0 or steps < 28:
        raise ValueError("pressure schedules require a non-negative seed and at least 28 steps")
    rng = random.Random(seed)
    families = tuple(PressureFamily)
    family = families[seed % len(families)]
    # Capability surface: motifs that exist to exercise one capability pin it;
    # the rest draw both axes so absence is also composed with every motif.
    if family in (PressureFamily.EAGER_PACED, PressureFamily.BURST_LATENCY_BATCH):
        profile = PressureProfile(write_nowait=True, write_many=bool(rng.randrange(2)))
    elif family is PressureFamily.BURST_WRITE_MANY:
        profile = PressureProfile(write_nowait=False, write_many=True)
    elif family in (PressureFamily.PRESSURE_CALLBACK, PressureFamily.PRESSURE_EFFECT):
        profile = PressureProfile(write_nowait=False, write_many=bool(rng.randrange(2)))
    else:
        profile = PressureProfile(
            write_nowait=bool(rng.randrange(2)),
            write_many=bool(rng.randrange(2)),
        )
    motif = _family_operations(family, rng)
    initial = [
        _op("app", "connect"),
        _op("checkpoint", "wire", "CONNECT"),
        _op("broker", "connack"),
        _op("checkpoint", "connected"),
        _op("schedule", "settle", rng.choice((0, 1, 2, 4))),
    ]
    history_budget = steps - len(initial) - len(motif)
    if history_budget < 0:
        raise ValueError(f"{steps} steps cannot hold the {family.value} motif")
    operations = [*initial, *_legal_history(rng, history_budget), *motif]
    settle_plan = tuple(
        int(operation.value)
        for operation in operations
        if (operation.actor, operation.action) == ("schedule", "settle")
    )
    assert len(operations) == steps
    return PressureSchedule(seed, tuple(operations), family, profile, settle_plan)


class _PressureTransport(v1._ScheduleTransport):
    """Schedule transport with optional eager and vectored write capabilities."""

    def __init__(
        self,
        generation: int,
        owner_epoch: int,
        current_epoch: Any,
        *,
        profile: PressureProfile,
        mutation: PressureMutation | None,
    ) -> None:
        super().__init__(generation, owner_epoch, current_epoch)
        self.profile = profile
        self.mutation = mutation
        self.refuse_nowait_pending = 0
        self.eager_accepted = 0
        self.eager_refused = 0
        self.latency_batches = 0
        self.write_many_calls = 0
        if not profile.write_nowait:
            # WritePump resolves the capability once per connection through
            # getattr; a None attribute is "absent" without a second class.
            self.write_nowait = None  # type: ignore[method-assign, assignment]
        if not profile.write_many:
            self.write_many = None  # type: ignore[method-assign, assignment]

    def write_nowait(self, data: bytes) -> bool:
        if self.refuse_nowait_pending > 0:
            self.refuse_nowait_pending -= 1
            self.eager_refused += 1
            # Negative control: claim the refused frame was written.
            return self.mutation is PressureMutation.EAGER_REFUSAL_LIES
        if self.mutation is PressureMutation.EAGER_ACCEPT_DROPS_FRAME:
            return True
        self._decoder.feed(data)
        packets = list(self._decoder.drain_packets())
        self.attempted.extend(packets)
        completion_epoch = self._current_epoch()
        self.completed.extend((packet, completion_epoch) for packet in packets)
        if len(packets) > 1:
            # A multi-frame synchronous write is exactly the latency-batch
            # flush; a single frame is the ordinary zero-hop eager path.
            self.latency_batches += 1
        else:
            self.eager_accepted += 1
        return True

    async def write(self, data: WriteItem) -> None:
        # The writer sends a segmented item as two consecutive bytes writes;
        # only its payload half can reach SEGMENT_THRESHOLD as one bare write.
        if (
            self.mutation is PressureMutation.SEGMENTED_PAYLOAD_DROPPED
            and isinstance(data, bytes)
            and len(data) >= SEGMENT_THRESHOLD
        ):
            return
        await super().write(data)

    async def write_many(self, parts: list[bytes]) -> None:
        if self.mutation is PressureMutation.WRITE_MANY_DECOALESCED:
            for part in parts:
                await super().write(part)
            return
        attempted_at = len(self.attempted)
        await super().write(b"".join(parts))
        publish_count = sum(
            packet.packet_type is PacketType.PUBLISH for packet in self.attempted[attempted_at:]
        )
        if len(parts) > 1 and publish_count >= 4:
            # Mono-frame capability use and unrelated control batches prove no
            # producer coalescing. Require the intended four-PUBLISH burst.
            self.write_many_calls += 1

    def inject_eof(self) -> None:
        """Enter reader-owned teardown without pre-closing the transport."""
        self._rx.put_nowait(b"")


class _PressureHarness(v1._RuntimeHarness):
    def __init__(
        self,
        schedule: PressureSchedule,
        mutation: PressureMutation | None,
        *,
        connect_timeout_seconds: float = 0.5,
    ) -> None:
        self.pressure_schedule = schedule
        self.pressure_mutation = mutation
        self.publish_waiters_high_water = 0
        self.writer_resident_high_water = 0
        self.overlap_observed: set[str] = set()
        self._acked_counts: Counter[int] = Counter()
        super().__init__(
            schedule.as_v1_schedule(),
            None,
            connect_timeout_seconds=connect_timeout_seconds,
        )
        self._install_pressure_mutation()

    def _client_options(self) -> dict[str, Any]:
        options = super()._client_options()
        # Writer sizing sufficient for 4-16+ resident frames and one segmented
        # payload. Production defaults are never changed for this; the profile
        # only configures its own client (issue #388).
        options["max_outbound_messages"] = 32
        options["max_outbound_bytes"] = 256 * 1024
        family = self.pressure_schedule.family
        if family in _PARKED_FAMILIES or family is PressureFamily.PRESSURE_RECONNECT:
            # Two unacknowledged QoS 1 exchanges saturate admission, so the
            # third concurrent publisher parks (issue #389 part B).
            options["max_pending_outbound_messages"] = 2
        elif family is PressureFamily.PRESSURE_CALLBACK:
            options["max_outbound_messages"] = 16
        if self.pressure_mutation is PressureMutation.WRITER_PRESSURE_BYPASSED:
            options["max_outbound_messages"] = 32
        return options

    async def _factory(
        self, host: str, port: int, *, ssl: object | None = None
    ) -> _PressureTransport:
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
        transport = _PressureTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
            lambda: self.client._connection_epoch,
            profile=self.pressure_schedule.profile,
            mutation=self.pressure_mutation,
        )
        self.transports.append(transport)
        return transport

    def _install_effect_gate(self) -> None:
        original = self.client._apply_effect
        original_inline = self.client._apply_effect_inline

        def defer_gated_effect(_client: Any, effect: Any, epoch: int) -> bool:
            if self.block_effect_once:
                return False
            return original_inline(effect, epoch)

        async def gated_apply(
            _client: Any,
            effect: Any,
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

        self._replace(self.client, "_apply_effect", MethodType(gated_apply, self.client))
        self._replace(
            self.client,
            "_apply_effect_inline",
            MethodType(defer_gated_effect, self.client),
        )

    def _install_pressure_mutation(self) -> None:
        if self.pressure_mutation is PressureMutation.PARKED_PUBLISHER_NOT_WOKEN:

            def missed_wakeup(_client: Any, n: int = 1) -> None:
                del n

            self._replace(
                self.client,
                "_wake_publish_waiters",
                MethodType(missed_wakeup, self.client),
            )
        elif self.pressure_mutation is PressureMutation.PUBLISH_WAITER_DECREMENT_LOST:

            async def leaky_wait(client: Any, waiter: asyncio.Future[None]) -> None:
                await waiter
                client._publish_wait_retries += 1

            self._replace(
                self.client,
                "_wait_publish_space",
                MethodType(leaky_wait, self.client),
            )

    def _spawn_publish(self, qos: QoS, size_class: str, *, expect_terminal: bool) -> None:
        index = len(self.tasks)
        payload = bytes(_PAYLOAD_SIZES[size_class])
        self._spawn_application_task(
            self.client.publish(f"runtime/pressure/{index}", payload, qos=qos),
            label=f"publish-{size_class}-qos{int(qos)}",
            expected_exceptions=(MQTTError,) if expect_terminal else (),
        )

    def _pressure_transports(self) -> list[_PressureTransport]:
        return [
            transport for transport in self.transports if isinstance(transport, _PressureTransport)
        ]

    def pressure_counters(self) -> dict[str, int]:
        transports = self._pressure_transports()
        writer = self.client.stats().writer
        return {
            "eager_accepted": sum(t.eager_accepted for t in transports),
            "eager_refused": sum(t.eager_refused for t in transports),
            "latency_batches": sum(t.latency_batches for t in transports),
            "write_many_calls": sum(t.write_many_calls for t in transports),
            "segmented_writes": writer.segmented_writes,
            "parked_publisher_observed": int(self.publish_waiters_high_water > 0),
            "writer_waiters_observed": int(writer.enqueue_suspensions > 0),
            "writer_4_resident_observed": int(self.writer_resident_high_water >= 4),
            "writer_16_resident_observed": int(self.writer_resident_high_water >= 16),
            "pressure_lifecycle_overlaps": int(bool(self.overlap_observed)),
            "pressure_reader_teardown_overlaps": int("reader_teardown" in self.overlap_observed),
            "pressure_reconnect_overlaps": int("reconnect" in self.overlap_observed),
            "pressure_callback_overlaps": int("callback" in self.overlap_observed),
            "pressure_effect_overlaps": int("effect" in self.overlap_observed),
        }

    async def execute(self, operation: RuntimeOperation) -> None:  # noqa: C901
        actor, action, value = operation.actor, operation.action, operation.value
        if (actor, action) == ("schedule", "settle"):
            self.operations.append(operation.render())
            self.settle_turns = int(value)
            return
        if (actor, action) == ("app", "publish_class"):
            self.operations.append(operation.render())
            qos_text, size_class = str(value).split(":")
            self._spawn_publish(QoS(int(qos_text)), size_class, expect_terminal=False)
        elif (actor, action) == ("app", "publish_class_terminal"):
            self.operations.append(operation.render())
            qos_text, size_class = str(value).split(":")
            self._spawn_publish(QoS(int(qos_text)), size_class, expect_terminal=True)
        elif (actor, action) == ("app", "burst"):
            # Producer fan-out: every task is spawned before any of them runs,
            # so their admissions land in the same event-loop turn.
            self.operations.append(operation.render())
            count_text, qos_text, size_class = str(value).split(":")
            for _ in range(int(count_text)):
                self._spawn_publish(QoS(int(qos_text)), size_class, expect_terminal=False)
        elif (actor, action) == ("transport", "refuse_nowait"):
            self.operations.append(operation.render())
            transport = self.transport
            assert isinstance(transport, _PressureTransport)
            transport.refuse_nowait_pending += int(value)
        elif (actor, action) == ("transport", "reset_write_many"):
            self.operations.append(operation.render())
            transport = self.transport
            assert isinstance(transport, _PressureTransport)
            transport.write_many_calls = 0
        elif (actor, action) == ("broker", "inject_eof"):
            self.operations.append(operation.render())
            transport = self.transport
            assert isinstance(transport, _PressureTransport)
            transport.inject_eof()
        elif (actor, action) == ("broker", "puback_pending"):
            # Acknowledge every completed QoS > 0 PUBLISH occurrence that has
            # not been acknowledged yet. Occurrences, not identifiers: a
            # settled identifier is released and legally reused by a later
            # publication in the same schedule.
            self.operations.append(operation.render())
            observed: Counter[int] = Counter()
            for transport in self.transports:
                for packet, _epoch in transport.completed:
                    if packet.packet_type is not PacketType.PUBLISH:
                        continue
                    publish = PublishPacket.decode(
                        packet.flags, packet.remaining, MQTTProtocolVersion.MQTTv5
                    )
                    if publish.mid is None:
                        continue
                    observed[publish.mid] += 1
            for mid, count in observed.items():
                for _ in range(count - self._acked_counts[mid]):
                    self._acked_counts[mid] += 1
                    self.transport.push(PubAckPacket(mid).encode(MQTTProtocolVersion.MQTTv5))
        elif (actor, action) == ("checkpoint", "terminal"):
            # Under a short settlement budget the connection-scoped tasks can
            # all be gone while an application disconnect()/connect() is still
            # finishing its own teardown (which owns the writer-queue discard).
            # Settle those lifecycle owners before the terminal oracle runs.
            await self._wait_until(
                lambda: all(
                    tracked.task.done()
                    for tracked in self.tasks
                    if tracked.label in ("disconnect", "connect")
                ),
                "lifecycle application task did not settle before terminal quiescence",
            )
            await super().execute(operation)
            return
        elif (actor, action) == ("checkpoint", "callbacks_drained"):
            # A settlement budget of zero turns can reach this checkpoint
            # before the reader even decoded the inbound PUBLISH. Wait for the
            # delivery rather than asserting it already happened; a lost
            # delivery still fails as a liveness timeout.
            self.operations.append(operation.render())
            self.checkpoints.append("callbacks_drained")
            await self._wait_until(
                lambda: (
                    self.callback_attempted == self.callback_expected
                    and self.client.stats().delivery.callback_queued == 0
                ),
                "callback deliveries did not drain",
            )
        elif actor == "checkpoint" and action in (
            "eager_accepted",
            "eager_refused",
            "latency_batch",
            "write_many",
            "segmented",
            "wire_bulk",
            "publisher_parked",
            "close_blocked",
            "overlap",
        ):
            self.operations.append(operation.render())
            await self._pressure_checkpoint(action, value)
        else:
            await super().execute(operation)
            return
        await self._turns(self.settle_turns)
        self._check_application_tasks()
        self._check_loop_contexts()
        self._check_oracles()

    async def _pressure_checkpoint(self, action: str, value: str | int | None) -> None:
        label = f"{action}" if value is None else f"{action}:{value}"
        self.checkpoints.append(label)
        target = int(value) if isinstance(value, int) else 1
        if action == "eager_accepted":
            await self._wait_until(
                lambda: self.pressure_counters()["eager_accepted"] >= target,
                "eager write path was not reached",
            )
        elif action == "eager_refused":
            await self._wait_until(
                lambda: self.pressure_counters()["eager_refused"] >= target,
                "eager refusal/fallback path was not reached",
            )
        elif action == "latency_batch":
            await self._wait_until(
                lambda: self.pressure_counters()["latency_batches"] >= target,
                "latency-batch flush was not reached",
            )
        elif action == "write_many":
            await self._wait_until(
                lambda: self.pressure_counters()["write_many_calls"] >= target,
                "write_many coalescing was not reached",
            )
        elif action == "segmented":
            await self._wait_until(
                lambda: self.client.stats().writer.segmented_writes >= target,
                "segmented write path was not reached",
            )
        elif action == "wire_bulk":
            # One batched transport write completes several frames at once;
            # the per-frame wire checkpoint cannot straddle that, so the bulk
            # form raises the multiplicity target in one exact step.
            packet_name, count_text = str(value).split(":")
            packet_type = PacketType[packet_name]
            bulk_target = self._wire_targets.get(packet_type, 0) + int(count_text)
            self._wire_targets[packet_type] = bulk_target
            await self._wait_until(
                lambda: (
                    sum(transport.count(packet_type) for transport in self.transports)
                    >= bulk_target
                ),
                f"bulk transport write checkpoint {packet_type.name} was not reached",
            )
            observed = sum(transport.count(packet_type) for transport in self.transports)
            if observed != bulk_target:
                raise AssertionError(
                    f"wire multiplicity mismatch for {packet_type.name}: "
                    f"expected={bulk_target} observed={observed}"
                )
        elif action == "publisher_parked":
            # Issue #389 part B: the parked state is observed while the
            # schedule is still executing, not inferred after settlement.
            await self._wait_until(
                lambda: self.client.stats().receipts.publish_waiters > 0,
                "application publisher never parked on outbound admission",
            )
        elif action == "close_blocked":
            await self._wait_until(
                lambda: (
                    self.transport.close_entered.is_set() and not self.transport.close_gate.is_set()
                ),
                "reader teardown did not block closing its transport",
            )
        else:
            kind = str(value)
            if self.pressure_mutation is PressureMutation.PRESSURE_LIFECYCLE_SEPARATED:
                if kind == "callback":
                    self.callback_gate.set()
                elif kind == "effect":
                    self.effect_gate.set()
                elif kind == "reader_teardown":
                    self.transport.release_close()
                else:
                    self.factory_gate.set()
                await self._turns(8)
            await self._wait_until(
                lambda: self._overlap_active(kind),
                f"pressure did not overlap the {kind} lifecycle window",
            )
            self.overlap_observed.add(kind)

    def _overlap_active(self, kind: str) -> bool:
        stats = self.client.stats()
        if kind == "reader_teardown":
            return (
                self.transport.close_entered.is_set()
                and not self.transport.close_gate.is_set()
                and stats.outbound.pending_messages > 0
            )
        if kind == "reconnect":
            return (
                self.factory_entered.is_set()
                and not self.factory_gate.is_set()
                and stats.outbound.pending_messages > 0
                and stats.receipts.publish_waiters > 0
            )
        if kind == "callback":
            return (
                self.callback_entered.is_set()
                and not self.callback_gate.is_set()
                and stats.writer.waiters > 0
            )
        if kind == "effect":
            return (
                self.effect_entered.is_set()
                and not self.effect_gate.is_set()
                and stats.outbound.pending_messages > 0
            )
        raise AssertionError(f"unknown pressure/lifecycle overlap kind: {kind}")

    def _check_oracles(self, *, terminal: bool = False) -> None:
        waiters = self.client.stats().receipts.publish_waiters
        if waiters > self.publish_waiters_high_water:
            self.publish_waiters_high_water = waiters
        resident = self.client._write_pump.resident_messages
        if resident > self.writer_resident_high_water:
            self.writer_resident_high_water = resident
        super()._check_oracles(terminal=terminal)

    def owner_snapshot(self) -> dict[str, Any]:
        snapshot = super().owner_snapshot()
        snapshot["pressure"] = {
            "family": self.pressure_schedule.family.value,
            "profile": asdict(self.pressure_schedule.profile),
            "publish_waiters_high_water": self.publish_waiters_high_water,
            "writer_resident_high_water": self.writer_resident_high_water,
            "counters": self.pressure_counters(),
        }
        return snapshot


async def run_pressure_schedule(
    schedule: PressureSchedule,
    *,
    mutation: PressureMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 4.0,
    connect_timeout_seconds: float = 0.5,
) -> RuntimeRun:
    harness = _PressureHarness(
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
        artifact = PressureFailureArtifact(
            schedule.seed,
            schedule.family.value,
            asdict(schedule.profile),
            mutation.value if mutation is not None else None,
            list(harness.operations),
            list(schedule.settle_plan),
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
            path = artifacts_dir / f"runtime-pressure-seed{schedule.seed}.json"
            path.write_text(
                json.dumps(artifact.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        raise RuntimeFuzzFailure(artifact) from failure  # type: ignore[arg-type]
    return RuntimeRun(schedule.seed, tuple(harness.operations), owners)


def assert_pressure_coverage(result: PressureCampaignResult) -> None:
    """Fail when a campaign never reached one of the intended surfaces."""
    cold = [
        counter
        for counter in REQUIRED_PRESSURE_COVERAGE
        if result.pressure_coverage.get(counter, 0) <= 0
    ]
    if cold:
        raise AssertionError(
            "pressure campaign left required surfaces cold: "
            f"{', '.join(sorted(cold))} -- a green run proves nothing about them"
        )


async def run_pressure_campaign(
    *,
    seeds: Iterable[int],
    steps: int,
    mutation: PressureMutation | None = None,
    artifacts_dir: Path | None = None,
    require_coverage: bool = False,
    watchdog_seconds: float = 4.0,
    connect_timeout_seconds: float = 0.5,
) -> PressureCampaignResult:
    started = time.monotonic()
    completed = 0
    failing_seeds: list[int] = []
    operation_traces: set[tuple[str, ...]] = set()
    scheduling_traces: set[tuple[str, ...]] = set()
    coverage: Counter[str] = Counter()
    family_coverage: Counter[str] = Counter()
    pressure_coverage: Counter[str] = Counter()
    for seed in seeds:
        schedule = generate_pressure_schedule(seed, steps)
        rendered = tuple(operation.render() for operation in schedule.operations)
        operation_traces.add(rendered)
        scheduling_traces.add(
            tuple(
                operation.render()
                for operation in schedule.operations
                if operation.actor in {"checkpoint", "schedule", "transport"}
            )
        )
        family_coverage[schedule.family.value] += 1
        coverage.update(
            f"{operation.actor}.{operation.action}" for operation in schedule.operations
        )
        try:
            run = await run_pressure_schedule(
                schedule,
                mutation=mutation,
                artifacts_dir=artifacts_dir,
                watchdog_seconds=watchdog_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
            )
        except RuntimeFuzzFailure:
            failing_seeds.append(seed)
        else:
            pressure_coverage.update(run.final_snapshot["pressure"]["counters"])
        completed += 1
    wall = time.monotonic() - started
    result = PressureCampaignResult(
        completed,
        len(failing_seeds),
        tuple(failing_seeds),
        wall,
        len(operation_traces),
        len(scheduling_traces),
        dict(sorted(coverage.items())),
        dict(sorted(family_coverage.items())),
        dict(sorted(pressure_coverage.items())),
    )
    if require_coverage and mutation is None and not result.failures:
        assert_pressure_coverage(result)
    return result


async def _main_async(args: argparse.Namespace) -> int:
    result = await run_pressure_campaign(
        seeds=range(args.seed, args.seed + args.seeds),
        steps=args.steps,
        mutation=(PressureMutation(args.mutation) if args.mutation is not None else None),
        artifacts_dir=args.artifacts_dir,
        require_coverage=args.require_coverage,
        watchdog_seconds=args.watchdog_seconds,
        connect_timeout_seconds=args.connect_timeout_seconds,
    )
    print(
        f"[DONE] target=runtime-pressure seeds={result.completed} "
        f"failures={result.failures} operation_traces={result.unique_operation_traces} "
        f"scheduling_traces={result.unique_scheduling_traces} seed_start={args.seed} "
        f"steps={args.steps} wall_seconds={result.wall_seconds:.6f}"
    )
    print(f"[FAMILIES] {json.dumps(result.family_coverage, sort_keys=True)}")
    print(f"[PRESSURE] {json.dumps(result.pressure_coverage, sort_keys=True)}")
    print(f"[COVERAGE] {json.dumps(result.coverage, sort_keys=True)}")
    return int(bool(result.failures))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--seeds", type=int, default=16)
    parser.add_argument("--steps", type=int, default=36)
    parser.add_argument(
        "--watchdog-seconds",
        type=float,
        default=4.0,
        help="whole-schedule wall-clock watchdog (raise for a shared low-priority runner)",
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=0.5,
        help="harness reconnect/callback-connect deadline",
    )
    parser.add_argument("--mutation", choices=tuple(PressureMutation), default=None)
    parser.add_argument(
        "--require-coverage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="fail a green campaign that left a required pressure surface cold",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("/tmp/mqttium-runtime-pressure-fuzz"),
    )
    args = parser.parse_args(argv)
    if (
        args.seed < 0
        or args.seeds <= 0
        or args.steps < 28
        or args.watchdog_seconds <= 0
        or args.connect_timeout_seconds <= 0
    ):
        parser.error("seed must be non-negative; counts and timeouts positive; steps at least 28")
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
