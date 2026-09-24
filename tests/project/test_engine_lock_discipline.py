"""No coroutine suspends while holding ``AsyncClient._engine_lock``.

The engine lock serializes synchronous engine mutations. Because nothing awaits
inside it, a synchronous runtime step (such as marking a delivery at the exact
moment the application takes ownership) can mutate the engine without
acquiring the lock and without racing a half-finished critical section.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLIENT = ROOT / "src" / "mqttium" / "api" / "async_client.py"


def test_engine_lock_bodies_never_suspend() -> None:
    tree = ast.parse(CLIENT.read_text(encoding="utf-8"))
    offenders: list[str] = []
    locked_blocks = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncWith):
            continue
        if not any("_engine_lock" in ast.unparse(item.context_expr) for item in node.items):
            continue
        locked_blocks += 1
        for statement in node.body:
            for inner in ast.walk(statement):
                if isinstance(inner, (ast.Await, ast.AsyncWith, ast.AsyncFor)):
                    offenders.append(f"line {inner.lineno}: {ast.unparse(inner)[:60]}")
    assert locked_blocks > 0
    assert offenders == []
