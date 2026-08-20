"""Reconnect policy (implementation-guide.md §5)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

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
        0x9C,  # Use another server (unless following Server Reference)
        0x9D,  # Server moved
    }
)


@dataclass(slots=True)
class ReconnectPolicy:
    """Exponential-backoff and terminal-reason policy for reconnection.

    Args:
        enabled: Whether unexpected disconnects may schedule reconnection.
        initial_delay: Base delay before the first retry, in seconds.
        multiplier: Factor applied after each retry.
        max_delay: Maximum base delay before jitter.
        max_retries: Maximum attempts, or ``None`` for no count limit.
        stable_after: Connected duration after which attempt state resets.
        connect_timeout: Deadline for each transport and CONNACK attempt.
        follow_server_reference: Whether MQTT 5 ``Use another server`` may be
            retried when a server reference is available.

    Delays use full bounded jitter in the range 50–100% of the current base.
    Terminal authentication, protocol, and capability failures are not retried.
    """

    enabled: bool = True
    initial_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    max_retries: int | None = None
    stable_after: float = 30.0
    connect_timeout: float = 30.0
    follow_server_reference: bool = False
    _attempt: int = field(default=0, init=False, repr=False)
    _current_delay: float = field(default=0.0, init=False, repr=False)

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
        if self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than 0")
        self._attempt = 0
        self._current_delay = self.initial_delay

    def reset(self) -> None:
        """Reset attempt count and delay to the initial state."""
        self._attempt = 0
        self._current_delay = self.initial_delay

    def next_delay(self) -> float:
        """Return delay for the next retry (with full jitter) and advance state."""
        base = min(self._current_delay, self.max_delay)
        delay = random.uniform(0.5, 1.0) * base
        self._attempt += 1
        self._current_delay = min(self._current_delay * self.multiplier, self.max_delay)
        return delay

    def should_retry(self, reason_code: int | None, protocol: MQTTProtocolVersion) -> bool:
        """Return whether the next attempt is allowed for a disconnect reason."""
        if not self.enabled:
            return False
        if self.max_retries is not None and self._attempt >= self.max_retries:
            return False
        if reason_code is None:
            return True
        if protocol == MQTTProtocolVersion.MQTTv5:
            if reason_code in _V5_TERMINAL:
                if reason_code == 0x9C and self.follow_server_reference:
                    return True
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
    fields (`max_retries`, `follow_server_reference`) that have nothing to do
    with whether the reason code itself is terminal.
    """
    if protocol == MQTTProtocolVersion.MQTTv5:
        return reason_code in _V5_TERMINAL
    return reason_code in _V311_TERMINAL
