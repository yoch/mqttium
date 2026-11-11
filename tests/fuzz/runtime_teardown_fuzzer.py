"""V4 profile of the runtime schedule fuzzer: cancelling the reader's teardown.

The V1 transport gates only the first ``close()``, which a schedule's broker
EOF consumes. The reader's own release of a lost connection (the second
``close()``) therefore never suspended, and no schedule could land a
cancellation inside it. This profile gates every later ``close()`` and aims a
terminal ``disconnect()``, a client-internal cancellation or an external one
at that window.

It is additive: V1, V2 and V3 schedules, seeds and artifacts are unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MethodType
from typing import Any

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from tests.fuzz import runtime_fuzzer as v1
from tests.fuzz.runtime_composition_fuzzer import _legal_history
from tests.fuzz.runtime_fuzzer import (
    RuntimeFailureArtifact,
    RuntimeFuzzFailure,
    RuntimeOperation,
    RuntimeRun,
)


class TeardownFamily(StrEnum):
    """Who cancels the reader while it releases a lost connection."""

    DISCONNECT_RECONNECTABLE = "disconnect_reconnectable"
    DISCONNECT_TERMINAL = "disconnect_terminal"
    INTERNAL_FAILURE = "internal_failure"
    EXTERNAL_CANCEL = "external_cancel"


_FAMILIES = tuple(TeardownFamily)


class TeardownMutation(StrEnum):
    """Test-only breakages that restore the released (1.2.1) behaviour."""

    # The cancellation ends the reader at its awaited release.
    RELEASE_CANCEL_ENDS_READER = "release_cancel_ends_reader"
    # The reader completes its teardown but never settles what it kept.
    INTERRUPTED_TEARDOWN_KEEPS_RECEIPTS = "interrupted_teardown_keeps_receipts"


@dataclass(slots=True, frozen=True)
class TeardownSchedule:
    seed: int
    family: TeardownFamily
    operations: tuple[RuntimeOperation, ...]
    auto_reconnect: bool
    pending_publish: bool


@dataclass(slots=True, frozen=True)
class TeardownCampaignResult:
    completed: int
    failures: int
    failing_seeds: tuple[int, ...]
    families: dict[str, int]
    release_windows: int
    pending_publishes: int


def _op(actor: str, action: str, value: str | int | None = None) -> RuntimeOperation:
    return RuntimeOperation(actor, action, value)


def generate_teardown_schedule(seed: int, steps: int = 28) -> TeardownSchedule:
    """Generate one schedule ending in a cancellation of the reader's release."""
    if steps < 20:
        raise ValueError("teardown schedules require at least 20 steps")
    rng = random.Random(seed)
    family = _FAMILIES[seed % len(_FAMILIES)]
    auto_reconnect = family is not TeardownFamily.DISCONNECT_TERMINAL
    # A publication left unacknowledged is what a stranded teardown loses. It
    # is kept out of the family that reconnects, where a replayed PUBLISH
    # would be a second, unrelated wire event.
    pending_publish = family is not TeardownFamily.INTERNAL_FAILURE and rng.randrange(4) != 0

    anchor: list[RuntimeOperation] = []
    if pending_publish:
        anchor += [_op("app", "publish", 1), _op("checkpoint", "wire", "PUBLISH")]
    anchor += [
        _op("schedule", "hold_reclose"),
        _op("broker", "eof"),
        _op("checkpoint", "reader_releasing"),
    ]
    if rng.randrange(2):
        anchor.append(_op("schedule", "yield", rng.randrange(1, 6)))
    if family in (TeardownFamily.DISCONNECT_RECONNECTABLE, TeardownFamily.DISCONNECT_TERMINAL):
        anchor += [
            _op("app", "disconnect"),
            _op("schedule", "release_reclose"),
            _op("checkpoint", "terminal"),
        ]
    elif family is TeardownFamily.INTERNAL_FAILURE:
        anchor += [
            _op("reader", "cancel_internal"),
            _op("checkpoint", "reconnect_owned"),
            _op("schedule", "release_reclose"),
            _op("checkpoint", "wire", "CONNECT"),
            _op("broker", "connack"),
            _op("checkpoint", "connected"),
            _op("app", "disconnect"),
            _op("checkpoint", "terminal"),
        ]
    else:
        anchor += [
            _op("reader", "cancel_external"),
            _op("schedule", "release_reclose"),
            _op("checkpoint", "terminal"),
        ]

    operations = [
        _op("app", "connect"),
        _op("checkpoint", "wire", "CONNECT"),
        _op("broker", "connack"),
        _op("checkpoint", "connected"),
    ]
    operations += _legal_history(rng, steps - len(operations) - len(anchor))
    operations += anchor
    assert len(operations) == steps
    return TeardownSchedule(seed, family, tuple(operations), auto_reconnect, pending_publish)


class _TeardownTransport(v1._ScheduleTransport):
    """V1 transport whose later ``close()`` calls can be held as well."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.reclose_gate = asyncio.Event()
        self.reclose_gate.set()
        self.reclose_entered = asyncio.Event()

    async def close(self) -> None:
        if not self._closing:
            await super().close()
            return
        self.reclose_entered.set()
        await self.reclose_gate.wait()


class _TeardownHarness(v1._RuntimeHarness):
    def __init__(
        self,
        schedule: TeardownSchedule,
        mutation: TeardownMutation | None,
        *,
        connect_timeout_seconds: float = 0.5,
    ) -> None:
        self.teardown_mutation = mutation
        self.release_windows = 0
        super().__init__(
            schedule,  # type: ignore[arg-type]
            None,
            connect_timeout_seconds=connect_timeout_seconds,
        )
        self._install_teardown_mutation()

    async def _factory(
        self, host: str, port: int, *, ssl: object | None = None
    ) -> _TeardownTransport:
        del host, port, ssl
        self.factory_attempts += 1
        transport = _TeardownTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
            lambda: self.client._connection_epoch,
        )
        self.transports.append(transport)
        return transport

    def _install_teardown_mutation(self) -> None:
        client = self.client
        if self.teardown_mutation is TeardownMutation.RELEASE_CANCEL_ENDS_READER:

            async def release_without_reporting(_client: AsyncClient, transport: Any) -> bool:
                await client._write_pump.wake_waiters()
                await client._write_pump.stop()
                await transport.close()
                return False

            self._replace(
                client,
                "_release_reader_resources",
                MethodType(release_without_reporting, client),
            )
        elif self.teardown_mutation is TeardownMutation.INTERRUPTED_TEARDOWN_KEEPS_RECEIPTS:
            original_finish = client._finish_reader_retirement
            original_fail = client._fail_pending

            async def finish_without_settlement(
                _client: AsyncClient, *args: Any, **kwargs: Any
            ) -> None:
                self._replace(client, "_fail_pending", lambda _exc: None)
                try:
                    await original_finish(*args, **kwargs)
                finally:
                    client._fail_pending = original_fail  # type: ignore[method-assign]

            self._replace(
                client,
                "_finish_reader_retirement",
                MethodType(finish_without_settlement, client),
            )

    async def execute(self, operation: RuntimeOperation) -> None:
        actor, action = operation.actor, operation.action
        handled = True
        if (actor, action) == ("schedule", "hold_reclose"):
            transport = self.transport
            assert isinstance(transport, _TeardownTransport)
            transport.reclose_entered.clear()
            transport.reclose_gate.clear()
        elif (actor, action) == ("schedule", "release_reclose"):
            for transport in self.transports:
                assert isinstance(transport, _TeardownTransport)
                transport.reclose_gate.set()
        elif (actor, action) == ("reader", "cancel_internal"):
            # What a failing writer, keepalive, effect or auth task does once
            # it has closed the transport.
            reader = self._releasing_reader()
            self.client._reader_cancel_requested = True
            reader.cancel()
        elif (actor, action) == ("reader", "cancel_external"):
            self._releasing_reader().cancel()
        elif (actor, action) == ("checkpoint", "reader_releasing"):
            self.checkpoints.append("reader_releasing")
            transport = self.transport
            assert isinstance(transport, _TeardownTransport)
            await self._wait_until(
                transport.reclose_entered.is_set,
                "reader did not reach the release of its lost connection",
            )
            self._releasing_reader()
            self.release_windows += 1
        elif (actor, action) == ("checkpoint", "reconnect_owned"):
            self.checkpoints.append("reconnect_owned")
            await self._wait_until(
                lambda: self.client._running_tasks()["reconnect"],
                "RECONNECTING has no reconnect task after an internal cancellation",
            )
            assert self.client.state is ConnectionState.RECONNECTING
        else:
            handled = False
        if not handled:
            await super().execute(operation)
            return
        self.operations.append(operation.render())
        await self._turns(self.settle_turns)
        self._check_application_tasks()
        self._check_loop_contexts()
        self._check_oracles()

    def _releasing_reader(self) -> asyncio.Task[Any]:
        reader = self.client._reader_task
        assert reader is not None and not reader.done(), (
            "the reader ended before the cancellation could reach its release"
        )
        return reader

    def _check_oracles(self, *, terminal: bool = False) -> None:
        super()._check_oracles(terminal=terminal)
        reconnecting = self.client.state is ConnectionState.RECONNECTING
        reader = self.client._reader_task
        reader_owns = reader is not None and not reader.done()
        if reconnecting and not reader_owns:
            # Once the reader is gone, only a reconnect task can end this state.
            assert self.client._running_tasks()["reconnect"], (
                "client is RECONNECTING with neither a reader nor a reconnect task"
            )

    async def cleanup(self) -> None:
        for transport in self.transports:
            assert isinstance(transport, _TeardownTransport)
            transport.reclose_gate.set()
        await super().cleanup()


async def run_teardown_schedule(
    schedule: TeardownSchedule,
    *,
    mutation: TeardownMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 4.0,
    connect_timeout_seconds: float = 0.5,
) -> RuntimeRun:
    harness = _TeardownHarness(schedule, mutation, connect_timeout_seconds=connect_timeout_seconds)
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

    owners["teardown"] = {
        "family": schedule.family.value,
        "release_windows": harness.release_windows,
        "pending_publish": schedule.pending_publish,
    }
    if failure is not None:
        artifact = RuntimeFailureArtifact(
            seed=schedule.seed,
            mutation=mutation.value if mutation is not None else None,
            operations=list(harness.operations),
            checkpoints=list(harness.checkpoints),
            owners=owners,
            failure=f"{type(failure).__name__}: {failure}",
            timing={
                "connect_timeout_seconds": connect_timeout_seconds,
                "watchdog_seconds": watchdog_seconds,
            },
        )
        if artifacts_dir is not None:
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            path = artifacts_dir / f"runtime-teardown-seed{schedule.seed}.json"
            path.write_text(
                json.dumps(artifact.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        raise RuntimeFuzzFailure(artifact) from failure
    return RuntimeRun(schedule.seed, tuple(harness.operations), owners)


def assert_teardown_coverage(result: TeardownCampaignResult) -> None:
    """A campaign that never reached a surface proves nothing about it."""
    cold = [family.value for family in _FAMILIES if not result.families.get(family.value)]
    assert not cold, f"teardown families never scheduled: {cold}"
    assert result.release_windows == result.completed, (
        "a schedule completed without the reader reaching its release window"
    )
    assert result.pending_publishes, "no schedule carried an unacknowledged publication"


async def run_teardown_campaign(
    *,
    seeds: Iterable[int],
    steps: int = 28,
    mutation: TeardownMutation | None = None,
    artifacts_dir: Path | None = None,
    watchdog_seconds: float = 4.0,
    connect_timeout_seconds: float = 0.5,
) -> TeardownCampaignResult:
    completed = 0
    failing: list[int] = []
    families: Counter[str] = Counter()
    release_windows = 0
    pending = 0
    for seed in seeds:
        schedule = generate_teardown_schedule(seed, steps)
        try:
            run = await run_teardown_schedule(
                schedule,
                mutation=mutation,
                artifacts_dir=artifacts_dir,
                watchdog_seconds=watchdog_seconds,
                connect_timeout_seconds=connect_timeout_seconds,
            )
        except RuntimeFuzzFailure:
            failing.append(seed)
            continue
        completed += 1
        families[schedule.family.value] += 1
        release_windows += run.final_snapshot["teardown"]["release_windows"]
        pending += schedule.pending_publish
    return TeardownCampaignResult(
        completed=completed,
        failures=len(failing),
        failing_seeds=tuple(failing),
        families=dict(families),
        release_windows=release_windows,
        pending_publishes=pending,
    )


async def _main_async(args: argparse.Namespace) -> int:
    seeds = range(args.seed, args.seed + args.seeds)
    result = await run_teardown_campaign(
        seeds=seeds,
        steps=args.steps,
        artifacts_dir=args.artifacts_dir,
        watchdog_seconds=args.watchdog_seconds,
        connect_timeout_seconds=args.connect_timeout_seconds,
    )
    print(
        json.dumps(
            {
                "schema": "mqttium-runtime-teardown-fuzz-v1",
                "completed": result.completed,
                "failures": result.failures,
                "failing_seeds": list(result.failing_seeds),
                "families": result.families,
                "release_windows": result.release_windows,
                "pending_publishes": result.pending_publishes,
            },
            sort_keys=True,
        )
    )
    if result.failures:
        return 1
    assert_teardown_coverage(result)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--seeds", type=int, default=200)
    parser.add_argument("--steps", type=int, default=28)
    parser.add_argument("--artifacts-dir", type=Path, default=None)
    parser.add_argument("--watchdog-seconds", type=float, default=4.0)
    parser.add_argument("--connect-timeout-seconds", type=float, default=0.5)
    return asyncio.run(_main_async(parser.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
