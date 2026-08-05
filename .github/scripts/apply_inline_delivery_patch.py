from __future__ import annotations

from pathlib import Path


CLIENT = Path("src/mqttium/api/async_client.py")
TEST = Path("tests/unit/test_native_delivery_hotpath.py")


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old!r}")
    return text.replace(old, new, 1)


def main() -> None:
    text = CLIENT.read_text()
    marker = "    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool:\n"
    helper = '''    def _message_delivery_plan(self, msg: Message) -> tuple[bool, bool, bool]:
        callback_delivery = self.on_message is not None and self._message_delivery in (
            "auto",
            "callback",
            "both",
        )
        iterator_delivery = self._message_delivery in ("iterator", "both") or (
            self._message_delivery == "auto" and self.on_message is None
        )
        references = int(iterator_delivery) + int(callback_delivery)
        small_limit = self._delivery_small_message_limit
        small_delivery = bool(
            references
            and (
                small_limit is None
                or (
                    small_limit > 0
                    # Empty MQTT 5 property tables contribute no logical bytes.
                    and not msg.properties
                    # Four bytes per code point is a cheap UTF-8 upper bound.
                    and len(msg.payload) + 4 * len(msg.topic) <= small_limit
                )
            )
        )
        return callback_delivery, iterator_delivery, small_delivery

'''
    text = replace_once(text, marker, helper + marker)
    text = replace_once(
        text,
        "        if kind is EffectKind.SEND:\n"
        "            return self._try_enqueue_outbound(effect.data, epoch=epoch)\n",
        "        if kind is EffectKind.SEND:\n"
        "            return self._try_enqueue_outbound(effect.data, epoch=epoch)\n"
        "        if kind is EffectKind.MESSAGE:\n"
        "            msg: Message = effect.data\n"
        "            callback_delivery, iterator_delivery, small_delivery = (\n"
        "                self._message_delivery_plan(msg)\n"
        "            )\n"
        "            if not small_delivery:\n"
        "                return False\n"
        "            if msg.mid is not None and (\n"
        "                self._engine.config.manual_ack or msg.qos == QoS.EXACTLY_ONCE\n"
        "            ):\n"
        "                return False\n"
        "            # Check every destination before mutating either queue: the\n"
        "            # async fallback must never observe a half-delivered `both`.\n"
        "            if iterator_delivery and self._messages.full():\n"
        "                return False\n"
        "            if callback_delivery and self._callback_queue.full():\n"
        "                return False\n"
        "            if iterator_delivery:\n"
        "                self._messages.put_nowait(msg)\n"
        "                self._message_ready.set()\n"
        "            if callback_delivery:\n"
        "                assert self.on_message is not None\n"
        "                self._ensure_callback_worker()\n"
        "                self._callback_queue.put_nowait((self.on_message, (msg,), None))\n"
        "            return True\n",
    )
    old_plan = '''            callback_delivery = self.on_message is not None and self._message_delivery in (
                "auto",
                "callback",
                "both",
            )
            iterator_delivery = self._message_delivery in ("iterator", "both") or (
                self._message_delivery == "auto" and self.on_message is None
            )
            references = int(iterator_delivery) + int(callback_delivery)
            small_limit = self._delivery_small_message_limit
            small_delivery = references and (
                small_limit is None
                or (
                    small_limit > 0
                    # An MQTT 5 PUBLISH without properties decodes to an empty
                    # `Properties`, not to None: testing truthiness rather than
                    # identity is what keeps those messages on the fast path.
                    # An empty table contributes no logical bytes, so the bound
                    # below stays a valid upper bound of _delivery_logical_size.
                    and not msg.properties
                    and len(msg.payload) + 4 * len(msg.topic) <= small_limit
                )
            )
'''
    new_plan = '''            callback_delivery, iterator_delivery, small_delivery = (
                self._message_delivery_plan(msg)
            )
            references = int(iterator_delivery) + int(callback_delivery)
'''
    text = replace_once(text, old_plan, new_plan)
    CLIENT.write_text(text)

    tests = TEST.read_text()
    tests += '''

@pytest.mark.asyncio
async def test_small_callback_delivery_applies_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    message = Message(topic="hot/path", payload=b"payload")

    applied = client._apply_effect_inline(
        EngineEffect(kind=EffectKind.MESSAGE, data=message),
        client._connection_epoch,
    )

    assert applied is True
    await client._callback_queue.join()
    assert seen == [message]
    await client._shutdown_callback_worker(drain=False)


def test_inline_both_delivery_is_atomic_when_one_queue_is_full() -> None:
    client = AsyncClient(
        message_delivery="both",
        max_pending_callbacks=1,
        max_pending_messages=1,
    )
    client.on_message = lambda _message: None
    sentinel = (lambda: None, (), None)
    client._callback_queue.put_nowait(sentinel)

    applied = client._apply_effect_inline(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic="hot/path", payload=b"payload"),
        ),
        client._connection_epoch,
    )

    assert applied is False
    assert client._messages.empty()
    assert client._callback_queue.get_nowait() is sentinel
    client._callback_queue.task_done()


def test_inline_delivery_defers_messages_requiring_persistence_mark() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_effect_inline(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(
                topic="hot/path",
                payload=b"payload",
                qos=QoS.EXACTLY_ONCE,
                mid=7,
            ),
        ),
        client._connection_epoch,
    )

    assert applied is False
    assert client._callback_queue.empty()
'''
    TEST.write_text(tests)


if __name__ == "__main__":
    main()
