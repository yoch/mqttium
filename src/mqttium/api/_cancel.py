"""Cancellation ownership at runtime task boundaries.

``asyncio.CancelledError`` does not say *who* cancelled. A task that awaits a
transport, a factory or user code can receive one raised by that dependency
while nobody asked the task itself to stop. Treating the exception type as
proof of an owner request silently kills writers, supervisors and pumps
(issues #509, #510, #522, #525, #529 and #538).

The owner signal is the task's pending cancellation requests. Every runtime
boundary that catches ``CancelledError`` decides through :func:`owner_cancelled`
and converts anything else with :func:`dependency_failure`, which keeps the
original exception as the cause. ``tests/project/test_cancellation_discipline.py``
enforces that no other decision exists in the runtime.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager

from mqttium.errors import MQTTError


class DependencyCancelledError(MQTTError):
    """A dependency raised ``CancelledError`` without a cancellation request.

    Internal: it is exposed only through :class:`~mqttium.errors.MQTTError`
    handlers, with the original ``CancelledError`` as ``__cause__``.
    """


def owner_cancelled(task: asyncio.Task[object] | None = None) -> bool:
    """Whether ``task`` (default: the current task) has been asked to cancel."""
    if task is None:
        task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def dependency_failure(
    exc: asyncio.CancelledError, source: str, message: str | None = None
) -> DependencyCancelledError:
    """Describe a dependency-raised cancellation as an ordinary failure."""
    if message is None:
        message = f"{source} raised CancelledError without a cancel request"
    failure = DependencyCancelledError(message)
    failure.__cause__ = exc
    return failure


def failure_for(exc: BaseException, source: str) -> BaseException:
    """Return ``exc`` unless it is a dependency-raised cancellation.

    Owner cancellations and ordinary exceptions are returned unchanged, so a
    boundary can re-raise the result without re-deciding ownership.
    """
    if isinstance(exc, asyncio.CancelledError) and not owner_cancelled():
        return dependency_failure(exc, source)
    return exc


@contextmanager
def ignoring_dependency_failures() -> Iterator[None]:
    """Suppress teardown failures without swallowing an owner cancellation.

    Closing an already failing transport must not replace the original
    connection cause, whether the dependency raises an ordinary exception or
    ``CancelledError``. A cancellation requested on the current task still
    propagates.
    """
    try:
        yield
    except asyncio.CancelledError:
        if owner_cancelled():
            raise
    except Exception:  # nosec B110 - teardown must not replace the connection cause
        pass
