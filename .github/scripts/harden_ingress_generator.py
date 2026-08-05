from __future__ import annotations

from pathlib import Path


BENCHMARK = Path("benchmarks/native_rtt_ingress.py")


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old!r}")
    return text.replace(old, new, 1)


def main() -> None:
    text = BENCHMARK.read_text()
    text = replace_once(
        text,
        "import json\nimport socket\n",
        "import json\nimport queue\nimport socket\n",
    )
    text = replace_once(text, "import struct\nimport time\n", "import struct\nimport threading\nimport time\n")
    text = replace_once(
        text,
        "from dataclasses import asdict, dataclass\n",
        "from dataclasses import dataclass\n",
    )
    text = replace_once(
        text,
        "def _raw_publish_worker(topic: str, count: int, worker_id: int, connections: int = 8) -> None:\n",
        "def _raw_publish_worker(\n"
        "    topic: str,\n"
        "    count: int,\n"
        "    worker_id: int,\n"
        "    start_event: threading.Event,\n"
        "    ready: queue.SimpleQueue[None],\n"
        "    connections: int = 8,\n"
        ") -> float:\n",
    )
    text = replace_once(
        text,
        "            sockets.append(sock)\n"
        "        remaining = count\n"
        "        batch_frames = 64\n",
        "            sockets.append(sock)\n"
        "        ready.put(None)\n"
        "        start_event.wait()\n"
        "        started = time.perf_counter()\n"
        "        remaining = count\n"
        "        batch_frames = 64\n",
    )
    text = replace_once(
        text,
        "            remaining -= take\n"
        "            cursor = (cursor + 1) % len(sockets)\n"
        "    finally:\n",
        "            remaining -= take\n"
        "            cursor = (cursor + 1) % len(sockets)\n"
        "        return time.perf_counter() - started\n"
        "    finally:\n",
    )
    old = '''        started = time.perf_counter()
        await asyncio.gather(
            *(
                asyncio.to_thread(_raw_publish_worker, topic, worker_count, worker_id)
                for worker_id, worker_count in enumerate(per_worker)
            )
        )
        await asyncio.wait_for(complete.wait(), timeout=30.0)
        elapsed = finished_at - started
'''
    new = '''        start_event = threading.Event()
        ready: queue.SimpleQueue[None] = queue.SimpleQueue()
        publisher_tasks = [
            asyncio.create_task(
                asyncio.to_thread(
                    _raw_publish_worker,
                    topic,
                    worker_count,
                    worker_id,
                    start_event,
                    ready,
                )
            )
            for worker_id, worker_count in enumerate(per_worker)
        ]
        for _ in publisher_tasks:
            await asyncio.to_thread(ready.get)
        started = time.perf_counter()
        start_event.set()
        worker_durations = await asyncio.gather(*publisher_tasks)
        offered_finished = time.perf_counter()
        await asyncio.wait_for(complete.wait(), timeout=30.0)
        elapsed = finished_at - started
'''
    text = replace_once(text, old, new)
    text = replace_once(
        text,
        '                "delivered": delivered,\n',
        '                "delivered": delivered,\n'
        '                "offered_rate": count / (offered_finished - started),\n'
        '                "slowest_publisher_seconds": max(worker_durations),\n'
        '                "post_offer_drain_seconds": max(0.0, finished_at - offered_finished),\n',
    )
    BENCHMARK.write_text(text)


if __name__ == "__main__":
    main()
