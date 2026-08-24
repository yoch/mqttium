"""Cooperative named-checkpoint scheduler for asyncio concurrency exploration.

This is test infrastructure. It does not replace the event loop. Instrumented
tasks park at named checkpoints; a single driver chooses the next resume or
adversary action. The resulting schedule is compact, printable, and replayable.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

DecisionKind = Literal["resume", "action"]


@dataclass(frozen=True, slots=True)
class Step:
    """One recorded scheduler decision."""

    kind: DecisionKind
    task: str | None = None
    checkpoint: str | None = None
    occurrence: int | None = None
    action: str | None = None

    def format(self) -> str:
        if self.kind == "action":
            return f"action {self.action}"
        return f"resume {self.task} @ {self.checkpoint} #{self.occurrence}"

    @classmethod
    def parse(cls, line: str) -> Step:
        text = line.strip()
        if text.startswith("action "):
            return cls(kind="action", action=text[7:].strip())
        if not text.startswith("resume "):
            raise ValueError(f"unrecognised schedule step: {line!r}")
        body = text[7:]
        at = body.rfind(" @ ")
        hash_at = body.rfind(" #")
        if at < 0 or hash_at < 0 or hash_at < at:
            raise ValueError(f"unrecognised resume step: {line!r}")
        return cls(
            kind="resume",
            task=body[:at].strip(),
            checkpoint=body[at + 3 : hash_at].strip(),
            occurrence=int(body[hash_at + 2 :]),
        )


@dataclass(frozen=True, slots=True)
class Schedule:
    """Compact, printable sequence of scheduler decisions."""

    steps: tuple[Step, ...] = ()
    seed: int | None = None
    policy: str = "explicit"

    def format(self) -> str:
        header = f"# policy={self.policy}"
        if self.seed is not None:
            header += f" seed={self.seed}"
        lines = [header, *[step.format() for step in self.steps]]
        return "\n".join(lines) + "\n"

    @classmethod
    def parse(cls, text: str) -> Schedule:
        seed: int | None = None
        policy = "explicit"
        steps: list[Step] = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                for token in line[1:].split():
                    if token.startswith("seed="):
                        seed = int(token.split("=", 1)[1])
                    elif token.startswith("policy="):
                        policy = token.split("=", 1)[1]
                continue
            steps.append(Step.parse(line))
        return cls(steps=tuple(steps), seed=seed, policy=policy)

    def __add__(self, step: Step) -> Schedule:
        return Schedule(
            steps=(*self.steps, step),
            seed=self.seed,
            policy=self.policy,
        )


@dataclass(frozen=True, slots=True)
class Decision:
    """A currently legal scheduler choice."""

    kind: DecisionKind
    task: str | None = None
    checkpoint: str | None = None
    occurrence: int | None = None
    action: str | None = None

    def to_step(self) -> Step:
        return Step(
            kind=self.kind,
            task=self.task,
            checkpoint=self.checkpoint,
            occurrence=self.occurrence,
            action=self.action,
        )


class Chooser(Protocol):
    def choose(self, options: Sequence[Decision]) -> Decision: ...


class ReplayChooser:
    """Replay a compact schedule, then drain parked tasks in arrival order."""

    def __init__(self, schedule: Schedule) -> None:
        self._remaining = list(schedule.steps)

    def choose(self, options: Sequence[Decision]) -> Decision:
        if self._remaining:
            wanted = self._remaining[0]
            for option in options:
                if _same_decision(option, wanted):
                    self._remaining.pop(0)
                    return option
            available = ", ".join(option.to_step().format() for option in options) or "<none>"
            raise ScheduleMismatch(f"next step {wanted.format()!r} is not among: {available}")
        return options[0]


class FirstChooser:
    """Deterministic drain: always take the first legal option."""

    def choose(self, options: Sequence[Decision]) -> Decision:
        return options[0]


class RandomChooser:
    def __init__(self, rng: Any) -> None:
        self._rng = rng

    def choose(self, options: Sequence[Decision]) -> Decision:
        return self._rng.choice(list(options))


class PrefixChooser:
    """Follow a list of option indices, then take the first remaining option."""

    def __init__(self, prefix: Sequence[int]) -> None:
        self.prefix = list(prefix)
        self.index = 0
        self.branching: list[int] = []

    def choose(self, options: Sequence[Decision]) -> Decision:
        self.branching.append(len(options))
        if self.index < len(self.prefix):
            choice = self.prefix[self.index]
            self.index += 1
            if choice < 0 or choice >= len(options):
                raise ScheduleMismatch(
                    f"forced choice {choice} out of range for {len(options)} options"
                )
            return options[choice]
        return options[0]


@dataclass(slots=True)
class Parked:
    task_name: str
    checkpoint: str
    occurrence: int
    future: asyncio.Future[None]
    task: asyncio.Task[Any]


@dataclass
class TraceEvent:
    task: str
    name: str
    parked: bool = False


@dataclass
class RunResult:
    schedule: Schedule
    events: list[TraceEvent] = field(default_factory=list)
    actor_results: dict[str, Any] = field(default_factory=dict)
    error: BaseException | None = None
    timed_out: bool = False
    deadlock: bool = False
    steps: int = 0

    @property
    def ok(self) -> bool:
        return self.error is None and not self.timed_out and not self.deadlock


class ScheduleMismatch(RuntimeError):
    """A replayed schedule did not match the live parked set."""


class DeadlockError(RuntimeError):
    """No parked checkpoint and no progress before the idle budget expired."""


def _same_decision(option: Decision, step: Step) -> bool:
    if option.kind != step.kind:
        return False
    if option.kind == "action":
        return option.action == step.action
    return (
        option.task == step.task
        and option.checkpoint == step.checkpoint
        and option.occurrence == step.occurrence
    )


class CooperativeScheduler:
    """Park instrumented tasks at named checkpoints and drive their interleaving.

    Synchronous sections remain atomic. Only `await scheduler.checkpoint(...)`
    (and therefore only async instrumentation) is preemptible. Background
    asyncio work that never hits an enabled checkpoint is invisible to the
    driver; a bounded idle timeout turns that into an explicit failure.
    """

    def __init__(
        self,
        *,
        enabled: frozenset[str] | set[str] | None = None,
        timeout: float = 2.0,
        idle_timeout: float = 0.4,
        max_steps: int = 200,
    ) -> None:
        self.enabled = frozenset(enabled) if enabled is not None else None
        self.timeout = timeout
        self.idle_timeout = idle_timeout
        self.max_steps = max_steps
        self.armed = False
        self.parked: list[Parked] = []
        self.events: list[TraceEvent] = []
        self.used_actions: set[str] = set()
        self._occurrences: dict[tuple[str, str], int] = {}
        self._actor_tasks: dict[str, asyncio.Task[Any]] = {}
        self._labels: dict[int, str] = {}
        self._activity = asyncio.Event()
        self._driver: asyncio.Task[Any] | None = None
        self._stopping = False
        self._actions: dict[str, Callable[[], Awaitable[None] | None]] = {}

    def label_task(self, task: asyncio.Task[Any], name: str) -> None:
        self._labels[id(task)] = name

    def current_label(self) -> str:
        task = asyncio.current_task()
        if task is None:
            return "unknown"
        named = self._labels.get(id(task))
        if named is not None:
            return named
        return task.get_name() or "unnamed"

    def spawn(self, name: str, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._actor_tasks[name] = task
        self.label_task(task, name)
        task.add_done_callback(lambda _task: self._nudge())
        return task

    def attach_running(self, name: str, task: asyncio.Task[Any]) -> None:
        self.label_task(task, name)
        task.add_done_callback(lambda _task: self._nudge())

    def _nudge(self) -> None:
        self._activity.set()

    def trace(self, name: str, *, parked: bool = False) -> None:
        self.events.append(TraceEvent(task=self.current_label(), name=name, parked=parked))

    def _may_park(self) -> bool:
        if not self.armed or self._stopping:
            return False
        task = asyncio.current_task()
        if task is None or task is self._driver:
            return False
        if task.get_name() in {"concurrency-scheduler", "concurrency-action"}:
            return False
        if task.cancelling():
            return False
        return True

    async def checkpoint(self, name: str) -> None:
        """Park the current task when `name` is enabled and the scheduler is armed."""
        if self.enabled is not None and name not in self.enabled:
            return
        if not self._may_park():
            self.trace(name, parked=False)
            return
        task = asyncio.current_task()
        assert task is not None
        label = self.current_label()
        key = (label, name)
        occurrence = self._occurrences.get(key, 0) + 1
        self._occurrences[key] = occurrence
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        parked = Parked(
            task_name=label,
            checkpoint=name,
            occurrence=occurrence,
            future=future,
            task=task,
        )
        self.parked.append(parked)
        self.trace(name, parked=True)
        self._nudge()
        try:
            await future
        finally:
            if parked in self.parked:
                self.parked.remove(parked)

    def _legal_decisions(self) -> list[Decision]:
        options = [
            Decision(
                kind="resume",
                task=item.task_name,
                checkpoint=item.checkpoint,
                occurrence=item.occurrence,
            )
            for item in self.parked
            if not item.future.done()
        ]
        # Adversary actions are offered only while at least one instrumented
        # boundary is parked, so a compact schedule names the interleaving
        # rather than "close the transport at a random idle moment".
        if options:
            for name in self._actions:
                if name not in self.used_actions:
                    options.append(Decision(kind="action", action=name))
        return options

    def _resume(self, decision: Decision) -> None:
        for item in self.parked:
            if (
                item.task_name == decision.task
                and item.checkpoint == decision.checkpoint
                and item.occurrence == decision.occurrence
                and not item.future.done()
            ):
                item.future.set_result(None)
                return
        raise ScheduleMismatch(f"cannot resume {decision.to_step().format()}")

    async def _perform_action(self, name: str) -> None:
        action = self._actions[name]
        self.used_actions.add(name)
        result = action()
        if asyncio.iscoroutine(result):
            await result

    def _actors_done(self) -> bool:
        return all(task.done() for task in self._actor_tasks.values())

    async def _wait_for_decision_or_idle(self) -> str:
        idle_deadline = asyncio.get_running_loop().time() + self.idle_timeout
        while True:
            if self.parked or self._actors_done():
                return "ready"
            remaining = idle_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return "idle"
            self._activity.clear()
            try:
                await asyncio.wait_for(self._activity.wait(), timeout=remaining)
            except TimeoutError:
                return "idle"

    async def _drive(self, chooser: Chooser) -> None:
        recorded: list[Step] = []
        try:
            while True:
                state = await self._wait_for_decision_or_idle()
                if self._actors_done() and not self.parked:
                    return
                options = self._legal_decisions()
                if not options:
                    if self._actors_done():
                        return
                    raise DeadlockError(
                        "no parked checkpoint and no remaining adversary action; "
                        f"actors still running: {self._running_actors()}"
                    )
                if state == "idle" and not self.parked:
                    raise DeadlockError(
                        "idle timeout with no parked checkpoint; "
                        f"running={self._running_actors()} events={self._event_tail()}"
                    )
                if len(recorded) >= self.max_steps:
                    raise DeadlockError(
                        f"exceeded max_steps={self.max_steps}; last steps:\n"
                        + "\n".join(step.format() for step in recorded[-8:])
                    )
                decision = chooser.choose(options)
                recorded.append(decision.to_step())
                if decision.kind == "resume":
                    self._resume(decision)
                else:
                    assert decision.action is not None
                    await self._perform_action(decision.action)
                await asyncio.sleep(0)
        finally:
            self._recorded_steps = tuple(recorded)

    def _running_actors(self) -> str:
        running = [name for name, task in self._actor_tasks.items() if not task.done()]
        parks = [f"{item.task_name}@{item.checkpoint}" for item in self.parked]
        return f"tasks={running} parked={parks}"

    def _event_tail(self) -> str:
        return ",".join(f"{event.task}:{event.name}" for event in self.events[-8:])

    async def run(
        self,
        actors: Sequence[tuple[str, Awaitable[Any]]],
        *,
        chooser: Chooser | None = None,
        schedule: Schedule | None = None,
        actions: dict[str, Callable[[], Awaitable[None] | None]] | None = None,
        seed: int | None = None,
        policy: str = "explicit",
    ) -> RunResult:
        if schedule is not None:
            chooser = ReplayChooser(schedule)
            policy = schedule.policy
            seed = schedule.seed if seed is None else seed
        if chooser is None:
            chooser = FirstChooser()
        self._actions = dict(actions or {})
        self.used_actions.clear()
        self.parked.clear()
        self.events.clear()
        self._occurrences.clear()
        self._actor_tasks.clear()
        self._recorded_steps: tuple[Step, ...] = ()
        self._stopping = False
        self.armed = True
        for name, coro in actors:
            self.spawn(name, coro)
        driver = asyncio.create_task(self._drive(chooser), name="concurrency-scheduler")
        self._driver = driver
        result = RunResult(schedule=Schedule(policy=policy, seed=seed))
        try:
            gathered = asyncio.gather(
                *self._actor_tasks.values(),
                return_exceptions=True,
            )
            try:
                actor_values = await asyncio.wait_for(gathered, timeout=self.timeout)
            except TimeoutError:
                result.timed_out = True
                result.error = TimeoutError(
                    "scenario exceeded timeout "
                    f"{self.timeout:.3f}s; schedule so far:\n{self._partial_schedule(policy, seed)}"
                )
                await self._cancel_all()
                return result
            names = list(self._actor_tasks)
            for name, value in zip(names, actor_values, strict=True):
                result.actor_results[name] = value
                if isinstance(value, BaseException) and not isinstance(
                    value, asyncio.CancelledError
                ):
                    result.error = value
        finally:
            self._stopping = True
            self.armed = False
            self._release_all_parks()
            if not driver.done():
                driver.cancel()
                try:
                    await driver
                except (asyncio.CancelledError, Exception):
                    pass
            elif not result.timed_out:
                drive_error = driver.exception() if not driver.cancelled() else None
                if isinstance(drive_error, DeadlockError):
                    result.deadlock = True
                    result.error = drive_error
                elif drive_error is not None:
                    result.error = drive_error
            self._driver = None

        result.events = list(self.events)
        result.schedule = Schedule(
            steps=self._recorded_steps,
            seed=seed,
            policy=policy,
        )
        result.steps = len(result.schedule.steps)
        return result

    def _partial_schedule(self, policy: str, seed: int | None) -> str:
        recorded = getattr(self, "_recorded_steps", ())
        return Schedule(steps=recorded, seed=seed, policy=policy).format()

    def _release_all_parks(self) -> None:
        # Complete remaining parks so background mqttium tasks are not cancelled
        # merely because the scenario actors already finished.
        for item in list(self.parked):
            if not item.future.done():
                item.future.set_result(None)

    async def _cancel_all(self) -> None:
        self._stopping = True
        self.armed = False
        self._release_all_parks()
        for task in self._actor_tasks.values():
            if not task.done():
                task.cancel()
        if self._actor_tasks:
            await asyncio.gather(*self._actor_tasks.values(), return_exceptions=True)
