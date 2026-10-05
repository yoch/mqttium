"""Topic-filter matching for callback dispatch.

Exact filters are indexed by Topic Name. Wildcard filters live in a prefix
tree keyed by filter level, after Paho's ``MQTTMatcher``: matching follows the
topic's literal level, ``+`` and ``#`` children, so its cost depends on the
topic depth and the matching branches, not on how many filters are
registered. Matching candidates carry their original insertion sequence so
combining both paths preserves callback order without changing filter
semantics.
"""

from __future__ import annotations

from collections.abc import Iterator
from operator import itemgetter
from typing import Any

# One registered filter: [insertion sequence, wildcard levels or None, value].
# A mutable list shared by the filter index and the tree node, so replacing a
# value updates both without touching the tree.
_Entry = list[Any]

_SEQUENCE = itemgetter(0)


class _Node:
    """One filter level; ``entries`` lists the filters ending here.

    ``children`` holds literal levels only. The ``+`` and ``#`` children are
    separate slots: matching then needs one dict lookup per level, and a topic
    level spelled like a wildcard can never reach a wildcard child literally.
    ``wild`` caches whether either wildcard child exists, so a literal-only
    level costs one attribute test.
    """

    __slots__ = ("children", "entries", "hash", "plus", "wild")

    def __init__(self) -> None:
        self.children: dict[str, _Node] = {}
        self.plus: _Node | None = None
        self.hash: _Node | None = None
        self.wild = False
        self.entries: list[_Entry] | None = None

    def child(self, level: str) -> _Node | None:
        if level == "+":
            return self.plus
        if level == "#":
            return self.hash
        return self.children.get(level)

    def add_child(self, level: str) -> _Node:
        node = _Node()
        if level == "+":
            self.plus = node
            self.wild = True
        elif level == "#":
            self.hash = node
            self.wild = True
        else:
            self.children[level] = node
        return node

    def remove_child(self, level: str) -> None:
        if level == "+":
            self.plus = None
        elif level == "#":
            self.hash = None
        else:
            del self.children[level]
        self.wild = self.plus is not None or self.hash is not None

    def is_empty(self) -> bool:
        return self.entries is None and not self.children and not self.wild


class TopicMatcher:
    """Map MQTT topic filters to values and iterate matches for a topic."""

    def __init__(self) -> None:
        # literal filter -> [insertion sequence, wildcard levels or None, value]
        self._entries: dict[str, _Entry] = {}
        # Wildcard filters only. Several literal filters may end on one node:
        # "$share/a/x/+", "$share/b/x/+" and "x/+" all match as "x/+".
        self._root: _Node | None = None
        self._next_sequence = 0

    @staticmethod
    def _compile(topic_filter: str) -> tuple[str, ...] | None:
        if topic_filter.startswith("$share/"):
            # The broker delivers a shared subscription's messages under their
            # real Topic Name, so "$share/<ShareName>/<filter>" matches as
            # <filter>. It never has an exact-index entry: its key is the
            # literal shared filter, which is no Topic Name.
            return tuple(topic_filter.split("/", 2)[2].split("/"))
        levels = tuple(topic_filter.split("/"))
        return levels if any(level in {"+", "#"} for level in levels) else None

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        current = self._entries.get(topic_filter)
        if current is not None:
            # Replacing a value does not move the filter. Preserve the same
            # observable callback order and avoid touching the wildcard tree.
            current[2] = value
            return

        levels = self._compile(topic_filter)
        entry: _Entry = [self._next_sequence, levels, value]
        self._next_sequence += 1
        self._entries[topic_filter] = entry
        if levels is not None:
            node = self._root
            if node is None:
                node = self._root = _Node()
            for level in levels:
                child = node.child(level)
                if child is None:
                    child = node.add_child(level)
                node = child
            if node.entries is None:
                node.entries = [entry]
            else:
                node.entries.append(entry)

    def __getitem__(self, topic_filter: str) -> Any:
        try:
            return self._entries[topic_filter][2]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def __delitem__(self, topic_filter: str) -> None:
        try:
            entry = self._entries.pop(topic_filter)
        except KeyError as exc:
            raise KeyError(topic_filter) from exc
        levels = entry[1]
        if levels is None:
            return
        root = self._root
        assert root is not None
        path: list[tuple[_Node, str, _Node]] = []
        node = root
        for level in levels:
            child = node.child(level)
            assert child is not None
            path.append((node, level, child))
            node = child
        entries = node.entries
        assert entries is not None
        # By identity: values are arbitrary objects with arbitrary __eq__.
        for position, candidate in enumerate(entries):
            if candidate is entry:
                del entries[position]
                break
        if not entries:
            node.entries = None
        # Prune the branch this filter alone kept alive.
        for parent, level, child in reversed(path):
            if not child.is_empty():
                break
            parent.remove_child(level)
        if root.is_empty():
            self._root = None

    def __bool__(self) -> bool:
        return bool(self._entries)

    def items(self) -> Iterator[tuple[str, Any]]:
        """Iterate registered filters and values in registration order."""
        for topic_filter, entry in self._entries.items():
            yield topic_filter, entry[2]

    def iter_match(self, topic: str) -> Iterator[Any]:  # noqa: C901 - one inlined hot loop
        """Yield values whose MQTT filters match ``topic`` in insertion order.

        The walk stays inline: every message with filtered callbacks runs it,
        and a helper call per level or per branch would cost more than the
        whole linear scan it replaces at one wildcard filter.
        """

        exact_entry = self._entries.get(topic)
        if self._root is None:
            if exact_entry is not None and exact_entry[1] is None:
                yield exact_entry[2]
            return

        candidates: list[_Entry] = []
        if exact_entry is not None and exact_entry[1] is None:
            candidates.append(exact_entry)

        levels = topic.split("/")
        depth = len(levels)
        pending: list[tuple[_Node, int]] | None = None
        node: _Node | None = self._root
        index = 0
        if topic.startswith("$"):
            # Wildcards at the first level never match a topic beginning with
            # "$": only its literal first level is followed.
            # self._root was tested non-None above; mypy cannot narrow an
            # attribute across the generator's first yield.
            node = node.children.get(levels[0])  # type: ignore[union-attr]
            index = 1
        # Iterative instead of Paho's recursive generators: the literal branch
        # is followed in place and only "+" branches wait on a stack, so there
        # is no frame per level and any topic depth is safe.
        while True:
            while node is not None:
                if node.wild:
                    hash_node = node.hash
                    if hash_node is not None and hash_node.entries is not None:
                        # "#" matches the parent level and every level below.
                        candidates.extend(hash_node.entries)
                    if index == depth:
                        if node.entries is not None:
                            candidates.extend(node.entries)
                        break
                    plus_node = node.plus
                    if plus_node is not None:
                        if pending is None:
                            pending = [(plus_node, index + 1)]
                        else:
                            pending.append((plus_node, index + 1))
                elif index == depth:
                    if node.entries is not None:
                        candidates.extend(node.entries)
                    break
                node = node.children.get(levels[index])
                index += 1
            if not pending:
                break
            node, index = pending.pop()

        if len(candidates) > 1:
            candidates.sort(key=_SEQUENCE)
        for entry in candidates:
            yield entry[2]
