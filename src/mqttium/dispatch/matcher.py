"""Topic-filter matching for callback dispatch.

Exact filters are indexed by Topic Name; wildcard filters retain the small
linear matcher. Matching candidates carry their original insertion sequence so
combining both paths preserves callback order without changing filter semantics.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


class TopicMatcher:
    """Map MQTT topic filters to values and iterate matches for a topic."""

    def __init__(self) -> None:
        # literal filter -> (insertion sequence, wildcard levels or None, value)
        self._entries: dict[str, tuple[int, tuple[str, ...] | None, Any]] = {}
        # Literal wildcard filters only; dict preserves their insertion order.
        self._wildcards: dict[str, None] = {}
        self._next_sequence = 0

    @staticmethod
    def _compile(topic_filter: str) -> tuple[str, ...] | None:
        levels = tuple(topic_filter.split("/"))
        return levels if any(level in {"+", "#"} for level in levels) else None

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        current = self._entries.get(topic_filter)
        if current is not None:
            sequence, levels, _old_value = current
            # Replacing a dict value does not move the key. Preserve the same
            # observable callback order and avoid touching the wildcard index.
            self._entries[topic_filter] = (sequence, levels, value)
            return

        levels = self._compile(topic_filter)
        sequence = self._next_sequence
        self._next_sequence += 1
        self._entries[topic_filter] = (sequence, levels, value)
        if levels is not None:
            self._wildcards[topic_filter] = None

    def __getitem__(self, topic_filter: str) -> Any:
        try:
            return self._entries[topic_filter][2]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def __delitem__(self, topic_filter: str) -> None:
        try:
            _sequence, levels, _value = self._entries.pop(topic_filter)
        except KeyError as exc:
            raise KeyError(topic_filter) from exc
        if levels is not None:
            self._wildcards.pop(topic_filter, None)

    def __bool__(self) -> bool:
        return bool(self._entries)

    def iter_match(self, topic: str) -> Iterator[Any]:
        """Yield values whose MQTT filters match ``topic`` in insertion order."""

        candidates: list[tuple[int, Any]] = []
        exact_entry = self._entries.get(topic)
        if exact_entry is not None and exact_entry[1] is None:
            sequence, _levels, value = exact_entry
            candidates.append((sequence, value))

        if self._wildcards:
            topic_levels = tuple(topic.split("/"))
            is_system_topic = topic.startswith("$")
            for topic_filter in self._wildcards:
                sequence, filter_levels, value = self._entries[topic_filter]
                assert filter_levels is not None
                if self._matches(filter_levels, topic_levels, is_system_topic):
                    candidates.append((sequence, value))

        if len(candidates) > 1:
            candidates.sort(key=lambda candidate: candidate[0])
        for _sequence, value in candidates:
            yield value

    @staticmethod
    def _matches(
        filter_levels: tuple[str, ...],
        topic_levels: tuple[str, ...],
        is_system_topic: bool,
    ) -> bool:
        if not filter_levels:
            return not topic_levels
        if is_system_topic and filter_levels[0] in {"+", "#"}:
            return False

        topic_index = 0
        for filter_index, level in enumerate(filter_levels):
            if level == "#":
                return filter_index == len(filter_levels) - 1
            if topic_index >= len(topic_levels):
                return False
            if level != "+" and level != topic_levels[topic_index]:
                return False
            topic_index += 1

        return topic_index == len(topic_levels)
