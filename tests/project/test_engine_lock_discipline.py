"""No coroutine suspends while holding the client's ``_engine_lock``.

The engine lock serializes synchronous engine mutations. Because nothing awaits
inside it, a synchronous runtime step (such as marking a delivery at the exact
moment the application takes ownership) can mutate the engine without
acquiring the lock and without racing a half-finished critical section.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
API = ROOT / "src" / "mqttium" / "api"


def test_engine_lock_bodies_never_suspend() -> None:
    offenders: list[str] = []
    locking_modules: set[str] = set()
    # Every runtime module, not only the client: collaborators such as the
    # authentication exchange take the same lock through their owner.
    for path in sorted(API.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncWith):
                continue
            if not any("_engine_lock" in ast.unparse(item.context_expr) for item in node.items):
                continue
            locking_modules.add(path.name)
            for statement in node.body:
                for inner in ast.walk(statement):
                    if isinstance(inner, (ast.Await, ast.AsyncWith, ast.AsyncFor)):
                        offenders.append(f"{path.name}:{inner.lineno}: {ast.unparse(inner)[:60]}")
    assert {"async_client.py", "_auth.py"} <= locking_modules
    assert offenders == []
