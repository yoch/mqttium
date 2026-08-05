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
    text = replace_once(
        text,
        "                if msg.mid is not None:\n"
        "                    async with self._engine_lock:\n"
        "                        self._engine.mark_inbound_delivered(msg.mid)\n",
        "                if msg.mid is not None and (\n"
        "                    self._engine.config.manual_ack\n"
        "                    or msg.qos == QoS.EXACTLY_ONCE\n"
        "                ):\n"
        "                    async with self._engine_lock:\n"
        "                        self._engine.mark_inbound_delivered(msg.mid)\n",
    )
    text = replace_once(
        text,
        "            if msg.mid is not None:\n"
        "                async with self._engine_lock:\n"
        "                    self._engine.mark_inbound_delivered(msg.mid)\n",
        "            if msg.mid is not None and (\n"
        "                self._engine.config.manual_ack or msg.qos == QoS.EXACTLY_ONCE\n"
        "            ):\n"
        "                async with self._engine_lock:\n"
        "                    self._engine.mark_inbound_delivered(msg.mid)\n",
    )
    CLIENT.write_text(text)
    TEST.write_text(
        '''from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def _apply_message(client: AsyncClient, qos: QoS) -> None:
    client.on_message = lambda _message: None
    await client._apply_effect(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=7),
        ),
        nowait=False,
    )
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)


@pytest.mark.asyncio
async def test_auto_acked_qos1_delivery_skips_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback")
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.AT_LEAST_ONCE)

    assert marked == []


@pytest.mark.asyncio
async def test_qos2_delivery_keeps_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback")
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.EXACTLY_ONCE)

    assert marked == [7]


@pytest.mark.asyncio
async def test_manual_ack_qos1_delivery_keeps_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback", manual_ack=True)
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.AT_LEAST_ONCE)

    assert marked == [7]
'''
    )


if __name__ == "__main__":
    main()
