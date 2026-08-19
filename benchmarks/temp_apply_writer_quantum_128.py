"""Apply the experimental 128 KiB writer quantum to an exact checkout.

Temporary benchmark helper. It deliberately fails closed if the expected main
implementation moved, so benchmark evidence cannot silently target a different
writer.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

DEFAULT_QUANTUM = 128 * 1024


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch(root: Path, quantum: int) -> None:
    path = root / "src/mqttium/api/_writer.py"
    text = path.read_text(encoding="utf-8")
    before = hashlib.sha256(text.encode()).hexdigest()

    text = replace_once(
        text,
        "_LATENCY_BATCH_TARGET_BYTES = 48 * 1024\n",
        "_LATENCY_BATCH_TARGET_BYTES = 48 * 1024\n"
        "_WRITER_BATCH_MAX_ITEMS = 256\n"
        f"_WRITER_BATCH_MAX_BYTES = {quantum}\n",
        "constants",
    )
    text = replace_once(
        text,
        "        self._eager_generation = 0\n",
        "        self._eager_generation = 0\n"
        "        # FIFO leftover already extracted from asyncio.Queue because the\n"
        "        # next item would cross the byte-bounded writer quantum.\n"
        "        self._held: WriteItem | None = None\n",
        "held init",
    )
    text = replace_once(
        text,
        "    def queued_messages(self) -> int:\n"
        "        return self.queue.qsize()\n",
        "    def queued_messages(self) -> int:\n"
        "        # A held item is no longer visible to qsize(), but remains queued\n"
        "        # work and must stay visible in queue-depth observability.\n"
        "        return self.queue.qsize() + (1 if self._held is not None else 0)\n",
        "queued messages",
    )
    text = replace_once(
        text,
        "        queued_messages = self.queue.qsize()\n"
        "        return WriterStats(\n",
        "        queued_messages = self.queued_messages\n"
        "        return WriterStats(\n",
        "stats",
    )
    text = replace_once(
        text,
        "        messages = self.queue.qsize() if queued_messages is None else queued_messages\n",
        "        messages = self.queued_messages if queued_messages is None else queued_messages\n",
        "high-water",
    )
    text = replace_once(
        text,
        "            and not self._writing\n"
        "            and self.queue.empty()\n",
        "            and not self._writing\n"
        "            and self._held is None\n"
        "            and self.queue.empty()\n",
        "eager rearm",
    )
    text = replace_once(
        text,
        "        self._sample_high_water()\n"
        "        self.queue = asyncio.Queue()\n"
        "        self.queued_bytes = 0\n"
        "        self._resident_messages = 0\n",
        "        self._sample_high_water()\n"
        "        self._held = None\n"
        "        self.queue = asyncio.Queue()\n"
        "        self.queued_bytes = 0\n"
        "        self._resident_messages = 0\n",
        "reset",
    )
    text = replace_once(
        text,
        "        self._writing = False\n"
        "        remaining = 0\n"
        "        while True:\n",
        "        self._writing = False\n"
        "        remaining = 0\n"
        "        held = self._held\n"
        "        self._held = None\n"
        "        if held is not None:\n"
        "            # The held item was already get()'d and still owns one\n"
        "            # unfinished-task / resident slot.\n"
        "            self.queue.task_done()\n"
        "            remaining += 1\n"
        "        while True:\n",
        "discard",
    )
    text = replace_once(
        text,
        "            or self._writing\n"
        "            or self.waiters\n"
        "            or isinstance(item, tuple)\n",
        "            or self._writing\n"
        "            or self._held is not None\n"
        "            or self.waiters\n"
        "            or isinstance(item, tuple)\n",
        "eager exclusion",
    )
    text = replace_once(
        text,
        "        queued = self.queue.qsize()\n"
        "        if (\n"
        "            queued < _LATENCY_BATCH_MIN_ITEMS\n",
        "        queued = self.queued_messages\n"
        "        if (\n"
        "            self._held is not None\n"
        "            or queued < _LATENCY_BATCH_MIN_ITEMS\n",
        "latency-batch exclusion",
    )
    text = replace_once(
        text,
        "                if queue.empty() and self._write_nowait is not None:\n"
        "                    self._eager_armed = True\n"
        "                first = await queue.get()\n"
        "                self._eager_armed = False\n"
        "                self._sample_high_water(queue.qsize() + 1)\n"
        "                batch: list[WriteItem] = [first]\n"
        "                while len(batch) < 256:\n"
        "                    try:\n"
        "                        batch.append(queue.get_nowait())\n"
        "                    except asyncio.QueueEmpty:\n"
        "                        break\n",
        "                held = self._held\n"
        "                if held is not None:\n"
        "                    self._held = None\n"
        "                    first = held\n"
        "                else:\n"
        "                    if queue.empty() and self._write_nowait is not None:\n"
        "                        self._eager_armed = True\n"
        "                    first = await queue.get()\n"
        "                self._eager_armed = False\n"
        "                self._sample_high_water(self.queued_messages + 1)\n"
        "                batch: list[WriteItem] = [first]\n"
        "                batch_bytes = item_size(first)\n"
        "                while len(batch) < _WRITER_BATCH_MAX_ITEMS:\n"
        "                    try:\n"
        "                        item = queue.get_nowait()\n"
        "                    except asyncio.QueueEmpty:\n"
        "                        break\n"
        "                    size = item_size(item)\n"
        "                    if batch_bytes + size > _WRITER_BATCH_MAX_BYTES:\n"
        "                        self._held = item\n"
        "                        break\n"
        "                    batch.append(item)\n"
        "                    batch_bytes += size\n",
        "writer batch loop",
    )

    path.write_text(text, encoding="utf-8")
    after = hashlib.sha256(text.encode()).hexdigest()
    print(f"patched {path} quantum={quantum} sha256_before={before} sha256_after={after}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--quantum", type=int, default=DEFAULT_QUANTUM)
    args = parser.parse_args()
    if args.quantum <= 0:
        raise SystemExit("--quantum must be positive")
    patch(args.root, args.quantum)


if __name__ == "__main__":
    main()
