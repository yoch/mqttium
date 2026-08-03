"""Topic filter matcher (prefix trie), adapted from the proven Paho approach.

Kept as a small standalone module so dispatch stays outside the protocol engine.
"""

from __future__ import annotations

from typing import Any
from collections.abc import Iterator


class TopicMatcher:
    """Map topic filters (with ``+`` / ``#``) to values; iterate matches for a topic."""

    class Node:
        __slots__ = ("children", "content")

        def __init__(self) -> None:
            self.children: dict[str, TopicMatcher.Node] = {}
            self.content: Any = None

    def __init__(self) -> None:
        self._root = self.Node()

    def __setitem__(self, key: str, value: Any) -> None:
        node = self._root
        for sym in key.split("/"):
            node = node.children.setdefault(sym, self.Node())
        node.content = value

    def __getitem__(self, key: str) -> Any:
        node = self._root
        try:
            for sym in key.split("/"):
                node = node.children[sym]
        except KeyError as exc:
            raise KeyError(key) from exc
        if node.content is None:
            raise KeyError(key)
        return node.content

    def __delitem__(self, key: str) -> None:
        path: list[tuple[TopicMatcher.Node, str, TopicMatcher.Node]] = []
        parent, node = None, self._root
        try:
            for part in key.split("/"):
                parent, node = node, node.children[part]
                path.append((parent, part, node))
        except KeyError as exc:
            raise KeyError(key) from exc
        node.content = None
        for parent, part, child in reversed(path):
            if child.children or child.content is not None:
                break
            del parent.children[part]

    def iter_match(self, topic: str) -> Iterator[Any]:
        parts = topic.split("/")
        allow_wildcard = not topic.startswith("$")
        # Iterative DFS (explicit stack) — no recursion limit on deep topics.
        stack: list[tuple[TopicMatcher.Node, int]] = [(self._root, 0)]
        while stack:
            node, index = stack.pop()
            if index == len(parts):
                if node.content is not None:
                    yield node.content
                # ``foo/#`` must also match ``foo`` (zero levels under the parent).
                if "#" in node.children and (allow_wildcard or index > 0):
                    content = node.children["#"].content
                    if content is not None:
                        yield content
                continue
            part = parts[index]
            if "#" in node.children and (allow_wildcard or index > 0):
                content = node.children["#"].content
                if content is not None:
                    yield content
            if "+" in node.children and (allow_wildcard or index > 0):
                stack.append((node.children["+"], index + 1))
            child = node.children.get(part)
            if child is not None:
                stack.append((child, index + 1))
