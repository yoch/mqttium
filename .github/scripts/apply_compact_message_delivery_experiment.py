from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TYPES = ROOT / "src/mqttium/types.py"
CLIENT = ROOT / "src/mqttium/api/async_client.py"
TEST = ROOT / "tests/unit/test_compact_delivery_reservation.py"


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


def patch_types() -> None:
    text = TYPES.read_text()
    old = '''    # Internal delivery accounting. Public Message instances remain frozen;\n    # AsyncClient mutates only these private slots with object.__setattr__.\n    _delivery_logical_bytes: int = field(default=0, init=False, repr=False, compare=False)\n    _delivery_references: int = field(default=0, init=False, repr=False, compare=False)\n'''
    text = replace_once(text, old, "", label="Message private delivery slots")
    TYPES.write_text(text)


def patch_client() -> None:
    text = CLIENT.read_text()

    old_aliases = '''MessageDelivery = Literal["auto", "iterator", "callback", "both"]\nPublishBackpressure = Literal["wait", "error"]\nDeliveryToken = int | Message | None\nCallbackJob = tuple[Callable[..., Any], tuple[Any, ...], DeliveryToken]\nTrackedIteratorMessage = tuple[Message, Message]\nIteratorQueueItem = Message | TrackedIteratorMessage\n\n\nclass _DeliveryQueue(asyncio.Queue[IteratorQueueItem]):\n'''
    new_aliases = '''MessageDelivery = Literal["auto", "iterator", "callback", "both"]\nPublishBackpressure = Literal["wait", "error"]\n\n\nclass _DeliveryReservation:\n    """Exact byte reservation shared by multiple delivery consumers."""\n\n    __slots__ = ("logical_bytes", "references")\n\n    def __init__(self, logical_bytes: int, references: int) -> None:\n        self.logical_bytes = logical_bytes\n        self.references = references\n\n\nAccountedDeliveryToken = int | _DeliveryReservation\nDeliveryToken = AccountedDeliveryToken | None\nCallbackJob = tuple[Callable[..., Any], tuple[Any, ...], DeliveryToken]\nTrackedIteratorMessage = tuple[Message, AccountedDeliveryToken]\nIteratorQueueItem = Message | TrackedIteratorMessage\n\n\nclass _DeliveryQueue(asyncio.Queue[IteratorQueueItem]):\n'''
    text = replace_once(text, old_aliases, new_aliases, label="delivery type aliases")

    old_reserve_inline = '''            delivery_token: DeliveryToken = None\n            iterator_enqueued = False\n            callback_enqueued = False\n            if references:\n                logical_bytes = self._delivery_logical_size(msg)\n                limit = self._delivery_accounted_limit\n                if limit is not None and logical_bytes > limit:\n                    raise MessageDeliveryError(\n                        f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"\n                    )\n                if limit is None or self._pending_delivery_bytes + logical_bytes <= limit:\n                    self._pending_delivery_bytes += logical_bytes\n                    if self._pending_delivery_bytes > self._pending_delivery_high_water_bytes:\n                        self._pending_delivery_high_water_bytes = self._pending_delivery_bytes\n                    if references == 1 and callback_delivery:\n                        delivery_token = logical_bytes\n                    else:\n                        if msg._delivery_logical_bytes or msg._delivery_references:\n                            raise MQTTError("Duplicate delivery reservation for one Message object")\n                        object.__setattr__(msg, "_delivery_logical_bytes", logical_bytes)\n                        if references > 1:\n                            object.__setattr__(msg, "_delivery_references", references)\n                        delivery_token = msg\n                else:\n                    delivery_token = await self._reserve_delivery_slow(\n                        msg,\n                        references,\n                        logical_bytes,\n                        callback_delivery=callback_delivery,\n                    )\n'''
    new_reserve_inline = '''            delivery_token: DeliveryToken = None\n            iterator_enqueued = False\n            callback_enqueued = False\n            if references:\n                logical_bytes = self._delivery_logical_size(msg)\n                limit = self._delivery_accounted_limit\n                if limit is not None and logical_bytes > limit:\n                    raise MessageDeliveryError(\n                        f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"\n                    )\n                delivery_token = self._try_reserve_delivery(\n                    msg,\n                    references,\n                    logical_bytes,\n                    callback_delivery=callback_delivery,\n                )\n                if delivery_token is None:\n                    delivery_token = await self._reserve_delivery_slow(\n                        msg,\n                        references,\n                        logical_bytes,\n                        callback_delivery=callback_delivery,\n                    )\n'''
    text = replace_once(text, old_reserve_inline, new_reserve_inline, label="inline reservation")

    old_iterator_item = '''                    iterator_item: IteratorQueueItem = (\n                        (msg, delivery_token) if isinstance(delivery_token, Message) else msg\n                    )\n'''
    new_iterator_item = '''                    iterator_item: IteratorQueueItem = (\n                        (msg, delivery_token) if delivery_token is not None else msg\n                    )\n'''
    text = replace_once(text, old_iterator_item, new_iterator_item, label="iterator token")

    old_try = '''    def _try_reserve_delivery(\n        self,\n        message: Message,\n        references: int,\n        logical_bytes: int,\n        *,\n        callback_delivery: bool,\n    ) -> DeliveryToken:\n        limit = self._delivery_accounted_limit\n        if limit is not None and self._pending_delivery_bytes + logical_bytes > limit:\n            return None\n        if message._delivery_logical_bytes or message._delivery_references:\n            raise MQTTError("Duplicate delivery reservation for one Message object")\n        self._pending_delivery_bytes += logical_bytes\n        if self._pending_delivery_bytes > self._pending_delivery_high_water_bytes:\n            self._pending_delivery_high_water_bytes = self._pending_delivery_bytes\n        if references == 1 and callback_delivery:\n            # The callback job already needs a token slot, so carrying the byte\n            # count there avoids mutating or indexing the Message object.\n            return logical_bytes\n        object.__setattr__(message, "_delivery_logical_bytes", logical_bytes)\n        if references > 1:\n            object.__setattr__(message, "_delivery_references", references)\n        return message\n'''
    new_try = '''    def _try_reserve_delivery(\n        self,\n        message: Message,\n        references: int,\n        logical_bytes: int,\n        *,\n        callback_delivery: bool,\n    ) -> DeliveryToken:\n        # Keep the private signature stable for focused tests and experiments;\n        # accounting state no longer lives on the public Message object.\n        _ = message, callback_delivery\n        limit = self._delivery_accounted_limit\n        if limit is not None and self._pending_delivery_bytes + logical_bytes > limit:\n            return None\n        self._pending_delivery_bytes += logical_bytes\n        if self._pending_delivery_bytes > self._pending_delivery_high_water_bytes:\n            self._pending_delivery_high_water_bytes = self._pending_delivery_bytes\n        if references == 1:\n            return logical_bytes\n        return _DeliveryReservation(logical_bytes, references)\n'''
    text = replace_once(text, old_try, new_try, label="reservation implementation")

    old_release = '''    def _release_delivery_reference_nowait(self, token: int | Message) -> None:\n        if isinstance(token, int):\n            self._pending_delivery_bytes -= token\n        else:\n            references = token._delivery_references\n            if references > 1:\n                object.__setattr__(token, "_delivery_references", references - 1)\n                return\n            if references == 1:\n                object.__setattr__(token, "_delivery_references", 0)\n            logical_bytes = token._delivery_logical_bytes\n            if logical_bytes <= 0:\n                return\n            object.__setattr__(token, "_delivery_logical_bytes", 0)\n            self._pending_delivery_bytes -= logical_bytes\n        if self._delivery_waiters:\n            self._delivery_space.set()\n\n    async def _release_delivery_reference(self, token: int | Message) -> None:\n        self._release_delivery_reference_nowait(token)\n'''
    new_release = '''    def _release_delivery_reference_nowait(self, token: AccountedDeliveryToken) -> None:\n        if isinstance(token, int):\n            self._pending_delivery_bytes -= token\n        else:\n            references = token.references\n            if references > 1:\n                token.references = references - 1\n                return\n            logical_bytes = token.logical_bytes\n            if references <= 0 or logical_bytes <= 0:\n                return\n            token.references = 0\n            token.logical_bytes = 0\n            self._pending_delivery_bytes -= logical_bytes\n        if self._delivery_waiters:\n            self._delivery_space.set()\n\n    async def _release_delivery_reference(self, token: AccountedDeliveryToken) -> None:\n        self._release_delivery_reference_nowait(token)\n'''
    text = replace_once(text, old_release, new_release, label="reservation release")

    if "_delivery_logical_bytes" in text or "_delivery_references" in text:
        raise RuntimeError("Message-embedded delivery accounting remains in async_client.py")
    CLIENT.write_text(text)


def write_test() -> None:
    TEST.write_text(
        '''from mqttium.api import AsyncClient\nfrom mqttium.types import Message\n\n\ndef test_message_has_no_delivery_accounting_slots() -> None:\n    message = Message(topic="test/topic", payload=b"payload")\n\n    assert not hasattr(message, "_delivery_logical_bytes")\n    assert not hasattr(message, "_delivery_references")\n\n\ndef test_single_consumer_reservation_uses_compact_integer_token() -> None:\n    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="iterator")\n    message = Message(topic="test/topic", payload=b"payload")\n\n    token = client._try_reserve_delivery(\n        message,\n        1,\n        100,\n        callback_delivery=False,\n    )\n\n    assert token == 100\n    assert client.pending_delivery_bytes == 100\n    client._release_delivery_reference_nowait(token)\n    assert client.pending_delivery_bytes == 0\n\n\ndef test_shared_reservation_releases_on_last_consumer() -> None:\n    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="both")\n    message = Message(topic="test/topic", payload=b"payload")\n\n    token = client._try_reserve_delivery(\n        message,\n        2,\n        100,\n        callback_delivery=True,\n    )\n\n    assert token is not None\n    assert not isinstance(token, int)\n    assert client.pending_delivery_bytes == 100\n\n    client._release_delivery_reference_nowait(token)\n    assert client.pending_delivery_bytes == 100\n    client._release_delivery_reference_nowait(token)\n    assert client.pending_delivery_bytes == 0\n'''
    )


def main() -> None:
    patch_types()
    patch_client()
    write_test()


if __name__ == "__main__":
    main()
