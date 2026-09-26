"""Cancellation ownership is decided in one place (``mqttium.api._cancel``).

``CancelledError`` does not identify who cancelled. Deciding by exception type
alone silently ended writers, pumps and supervisors (#509, #510, #522, #525,
#529, #538). These rules keep that decision out of individual handlers:

1. ``Task.cancelling()`` is read only by ``_cancel.owner_cancelled``;
2. ``except CancelledError: raise`` alone is forbidden. It is exactly the
   type-based decision the helper replaces;
3. a handler that can catch ``CancelledError`` and does not end by re-raising
   must decide through the helper, or appear in ``_SWALLOW_ALLOWED`` with the
   reason it may absorb the exception.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "mqttium"

_HELPERS = frozenset(
    {"owner_cancelled", "failure_for", "dependency_failure", "_propagate_callback_cancellation"}
)
_CANCEL_TYPES = frozenset({"asyncio.CancelledError", "CancelledError", "BaseException"})

# (module, function) -> why the handler may absorb CancelledError without
# asking whose it is.
_SWALLOW_ALLOWED = {
    ("api/_effects.py", "_done"): "done callback of a finished task; cancellation is its outcome",
    ("api/_writer.py", "stop"): "joins the writer task this method just cancelled",
    ("api/async_client.py", "disconnect"): "joins the reconnect task it just cancelled",
    ("api/async_client.py", "_cancel_automatic_reconnect"): "joins the task it just cancelled",
    ("api/async_client.py", "_force_close_transport"): "joins the tasks it just cancelled",
    (
        "api/async_client.py",
        "_retire_reader_connection",
    ): "reader teardown joins cancelled children and must finish",
    (
        "api/async_client.py",
        "_connect_once_locked",
    ): "failure cleanup; the original error is raised",
    ("transport/_stream.py", "close_stream_writer"): "defers the cancellation until the join ends",
}


# The synchronous layers (protocol/, persistence/, codec/) never await, so
# only the asyncio runtime can receive a CancelledError from a dependency.
_ASYNC_LAYERS = ("api", "transport")


def _modules() -> list[tuple[str, ast.Module]]:
    return [
        (path.relative_to(SOURCE).as_posix(), ast.parse(path.read_text(encoding="utf-8")))
        for layer in _ASYNC_LAYERS
        for path in sorted((SOURCE / layer).rglob("*.py"))
    ]


def _catches_cancellation(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    nodes = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return any(ast.unparse(node) in _CANCEL_TYPES for node in nodes)


def _ends_with_reraise(handler: ast.ExceptHandler) -> bool:
    last = handler.body[-1]
    if not isinstance(last, ast.Raise):
        return False
    return last.exc is None or (
        isinstance(last.exc, ast.Name) and handler.name is not None and last.exc.id == handler.name
    )


def _calls_helper(handler: ast.ExceptHandler) -> bool:
    for node in ast.walk(handler):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name in _HELPERS:
                return True
    return False


class _HandlerCollector(ast.NodeVisitor):
    """Record handlers that can catch CancelledError with their enclosing function."""

    def __init__(self, module: str) -> None:
        self.module = module
        self.stack: list[str] = []
        self.found: list[tuple[str, str, ast.ExceptHandler]] = []

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if _catches_cancellation(node):
            function = self.stack[-1] if self.stack else "<module>"
            self.found.append((self.module, function, node))
        self.generic_visit(node)


def _handlers() -> list[tuple[str, str, ast.ExceptHandler]]:
    found: list[tuple[str, str, ast.ExceptHandler]] = []
    for module, tree in _modules():
        collector = _HandlerCollector(module)
        collector.visit(tree)
        found.extend(collector.found)
    return found


def test_only_the_cancel_module_reads_task_cancelling() -> None:
    offenders = [
        f"{module}:{node.lineno}"
        for module, tree in _modules()
        if module != "api/_cancel.py"
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "cancelling"
    ]
    assert offenders == [], "decide cancellation ownership through _cancel.owner_cancelled"


def test_no_type_only_cancellation_decision() -> None:
    offenders = [
        f"{module}:{function}:{handler.lineno}"
        for module, function, handler in _handlers()
        if handler.type is not None
        and ast.unparse(handler.type) in {"asyncio.CancelledError", "CancelledError"}
        and len(handler.body) == 1
        and isinstance(handler.body[0], ast.Raise)
        and handler.body[0].exc is None
    ]
    assert offenders == [], "use _cancel.owner_cancelled instead of `except CancelledError: raise`"


def test_absorbing_handlers_decide_ownership_or_are_justified() -> None:
    offenders = [
        f"{module}:{function}:{handler.lineno}"
        for module, function, handler in _handlers()
        if not _ends_with_reraise(handler)
        and not _calls_helper(handler)
        and (module, function) not in _SWALLOW_ALLOWED
    ]
    assert offenders == []


def test_swallow_allowlist_has_no_stale_entries() -> None:
    absorbing = {
        (module, function)
        for module, function, handler in _handlers()
        if not _ends_with_reraise(handler) and not _calls_helper(handler)
    }
    assert set(_SWALLOW_ALLOWED) - absorbing == set()
