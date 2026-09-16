"""Reconnect policy (implementation-guide.md §5)."""

from __future__ import annotations

import random
from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion


_V311_TERMINAL = frozenset({1, 2, 4, 5})
_V5_TERMINAL = frozenset(
    {
        0x81,  # Malformed Packet
        0x82,  # Protocol Error
        0x84,  # Unsupported Protocol Version
        0x85,  # Client Identifier not valid
        0x86,  # Bad User Name or Password
        0x87,  # Not authorized
        0x8A,  # Banned
        0x8C,  # Bad authentication method
        0x90,  # Topic Name invalid (Will Topic on CONNACK)
        0x95,  # Packet too large (CONNECT)
        0x99,  # Payload format invalid (Will Payload)
        0x9A,  # Retain not supported
        0x9B,  # QoS not supported
        0x9C,  # Use another server
        0x9D,  # Server moved
    }
)


@dataclass(slots=True, frozen=True)
class ReconnectPolicy:
    """Exponential-backoff and terminal-reason policy for reconnection.

    Passing a policy to ``AsyncClient(reconnect=...)`` enables automatic
    reconnection; ``reconnect=None`` (the default) disables it. Each attempt's
    transport and CONNACK deadline is the client's ``connect_timeout``.

    Args:
        initial_delay: Base delay before the first retry, in seconds.
        multiplier: Factor applied after each retry.
        max_delay: Maximum base delay before jitter.
        max_retries: Maximum attempts, or ``None`` for no count limit.
        stable_after: Connected duration after which attempt state resets.

    Delays use full bounded jitter in the range 50–100% of the current base.
    Terminal authentication, protocol, and capability failures are not retried.
    """

    initial_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    max_retries: int | None = None
    stable_after: float = 30.0

    def __post_init__(self) -> None:
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be greater than or equal to initial_delay")
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError("max_retries must be non-negative or None")
        if self.stable_after < 0:
            raise ValueError("stable_after must be non-negative")


class _ReconnectState:
    """Mutable retry progression owned by exactly one client.

    ``policy is None`` means automatic reconnection is disabled; the state then
    only carries the (always zero) attempt counter.
    """

    def __init__(self, policy: ReconnectPolicy | None) -> None:
        self.policy = policy
        self._attempt = 0
        self._current_delay = policy.initial_delay if policy is not None else 0.0

    @property
    def enabled(self) -> bool:
        return self.policy is not None

    def reset(self) -> None:
        """Reset attempt count and delay to the initial state."""
        self._attempt = 0
        self._current_delay = self.policy.initial_delay if self.policy is not None else 0.0

    def next_delay(self) -> float:
        """Return delay for the next retry (with full jitter) and advance state."""
        policy = self.policy
        assert policy is not None
        base = min(self._current_delay, policy.max_delay)
        delay = random.uniform(0.5, 1.0) * base
        self._attempt += 1
        self._current_delay = min(self._current_delay * policy.multiplier, policy.max_delay)
        return delay

    def should_retry(self, reason_code: int | None, protocol: MQTTProtocolVersion) -> bool:
        """Return whether the next attempt is allowed for a disconnect reason."""
        policy = self.policy
        if policy is None:
            return False
        if policy.max_retries is not None and self._attempt >= policy.max_retries:
            return False
        if reason_code is None:
            return True
        if protocol == MQTTProtocolVersion.MQTTv5:
            if reason_code in _V5_TERMINAL:
                return False
            return True
        return reason_code not in _V311_TERMINAL

    @property
    def attempt(self) -> int:
        """Number of retry delays issued since the last reset."""
        return self._attempt


def is_terminal_connack(reason_code: int, protocol: MQTTProtocolVersion) -> bool:
    """Whether a CONNACK reason code forbids reconnecting, policy aside.

    Reads the terminal sets directly. Answering this by constructing a default
    `ReconnectPolicy` and inverting `should_retry` also ran that dataclass's
    `__post_init__` validation on every call, and tied the answer to policy
    fields such as `max_retries` that have nothing to do
    with whether the reason code itself is terminal.
    """
    if protocol == MQTTProtocolVersion.MQTTv5:
        return reason_code in _V5_TERMINAL
    return reason_code in _V311_TERMINAL
