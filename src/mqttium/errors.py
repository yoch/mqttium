"""Exception hierarchy for mqttium."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mqttium.api.models import PublishBatchReceipt
    from mqttium.types import Properties


class MQTTError(Exception):
    """Base error for mqttium."""


class BrokerDisconnectError(MQTTError):
    """Broker's nonzero MQTT 5 DISCONNECT, with its immutable properties."""

    def __init__(self, reason_code: int, properties: Properties | None = None) -> None:
        self.reason_code = reason_code
        self.properties = properties
        super().__init__(f"Broker disconnected with reason code 0x{reason_code:02x}")


class MalformedPacketError(MQTTError):
    """Wire data cannot be parsed as a valid MQTT packet."""


class ProtocolError(MQTTError):
    """Valid framing but illegal MQTT protocol usage."""


class PacketTooLargeError(ProtocolError):
    """Packet exceeds local or negotiated maximum size."""


class MandatoryResponseTooLargeError(PacketTooLargeError):
    """A peer limit makes a mandatory local MQTT response impossible to send.

    The broker's Maximum Packet Size is below the smallest acknowledgement the
    client must send, so the connection is ended locally and never retried by
    a reconnect policy. Reported through ``on_disconnect`` or raised by
    ``connect()`` when the limit is learned from CONNACK.
    """


class SessionReplayError(MQTTError):
    """A resumed session holds an exchange the new CONNACK forbids resending.

    With Session Present, MQTT requires every unacknowledged QoS 1/2 PUBLISH
    and PUBREL to be resent with its original Packet Identifier
    [MQTT-4.4.0-1]. When the new CONNACK's Maximum QoS, Retain Available or
    Maximum Packet Size forbids one of them, the session cannot be resumed:
    the connection is ended locally, durable state and packet identifiers are
    kept, and a reconnect policy never retries it. Reported through
    ``on_disconnect`` or raised by ``connect()``. Connecting with
    ``clean_start=True`` discards the session and fails those publications
    with ``SessionDiscardedError``.
    """


class FlowControlError(MQTTError):
    """Immediate operation refused because bounded client capacity is unavailable."""


class MessageDeliveryError(FlowControlError):
    """Iterator delivery cannot be admitted within its configured bounds or deadline."""


class NotConnectedError(MQTTError):
    """Operation requires an active connection."""


class MQTTTimeoutError(MQTTError, TimeoutError):
    """Operation exceeded its deadline.

    Also a builtin :class:`TimeoutError`, so ``except TimeoutError`` catches it.
    """


class SessionDiscardedError(MQTTError):
    """Pending publish was discarded because a clean session replaced the old one."""


class PublishBatchError(MQTTError):
    """One or more publications in a batch failed.

    The failure details and counts are those of :attr:`receipt`. When
    submission itself stopped, the exception is raised ``from`` its cause.

    Attributes:
        receipt (PublishBatchReceipt): Receipt of the batch. When submission
            stopped early it covers the committed prefix; await
            ``receipt.wait()`` to settle that prefix.
    """

    def __init__(self, receipt: PublishBatchReceipt, *, cause: BaseException | None = None) -> None:
        self.receipt = receipt
        if cause is not None:
            message = f"Batch submission failed: {cause}"
        else:
            message = f"{receipt.failure_count} publication(s) failed in batch"
        super().__init__(message)
