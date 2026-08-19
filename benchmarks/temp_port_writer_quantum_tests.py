"""Port the historical #286 invariant test fixture to current resident accounting.

The assertions are preserved. Only direct test-only queue manipulation is taught
to mirror the current WritePump resident counter; production enqueue paths already
do this themselves.
"""
from __future__ import annotations

import argparse
from pathlib import Path


def replace_exact(text: str, old: str, new: str, expected: int, label: str) -> str:
    count = text.count(old)
    if count != expected:
        raise RuntimeError(f"{label}: expected {expected} matches, found {count}")
    return text.replace(old, new)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    text = args.path.read_text(encoding="utf-8")

    text = replace_exact(
        text,
        "    pump.queued_bytes += item_size(item)\n"
        "    pump._held = pump.queue.get_nowait()\n",
        "    pump.queued_bytes += item_size(item)\n"
        "    pump._admit_queued()\n"
        "    pump._held = pump.queue.get_nowait()\n",
        1,
        "held fixture resident admission",
    )
    text = replace_exact(
        text,
        "    pump.queue.put_nowait(b\"queued\")\n"
        "    pump.queued_bytes += len(b\"queued\")\n",
        "    pump.queue.put_nowait(b\"queued\")\n"
        "    pump.queued_bytes += len(b\"queued\")\n"
        "    pump._admit_queued()\n",
        2,
        "raw queued fixture resident admission",
    )
    text = replace_exact(
        text,
        "    assert pump.queue.get_nowait() == b\"later\"\n"
        "    pump.queue.task_done()\n",
        "    assert pump.queue.get_nowait() == b\"later\"\n"
        "    pump.queue.task_done()\n"
        "    pump._release_resident()\n",
        1,
        "raw dequeue fixture resident release",
    )

    args.path.write_text(text, encoding="utf-8")
    print(f"ported historical quantum test fixture: {args.path}")


if __name__ == "__main__":
    main()
