"""Topic-filter matching for callback dispatch.

Exact filters are indexed by delivered Topic Name; wildcard filters retain the
small linear matcher. Matching candidates carry their original insertion
sequence so combining both paths preserves Paho callback order exactly.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


class TopicMatcher:
    """Map MQTT topic filters to values and iterate matches for a topic."""

    def __init__(self) -> None:
        # literal filter -> (insertion sequence, compiled levels, value, exact topic)
        self._entries: dict[str, tuple[int, tuple[str, ...], Any, str | None]] = {}
        # delivered exact topic -> literal filters compiling to that topic. More
        # than one is possible because several $share groups can use the same
        # underlying exact Topic Filter.
        self._exact: dict[str, dict[str, None]] = {}
        # Literal wildcard filters only; dict preserves their insertion order.
        self._wildcards: dict[str, None] = {}
        self._next_sequence = 0

    @staticmethod
    def _compile(topic_filter: str) -> tuple[tuple[str, ...], str | None]:
        levels = tuple(topic_filter.split("/"))
        # A broker removes "$share/{ShareName}/" before delivering a shared
        # subscription message. Keep the literal filter as the dictionary key
        # for Paho-compatible get/delete semantics, but compile the match side
        # against the Topic Filter that follows the ShareName.
        if len(levels) >= 3 and levels[0] == "$share" and levels[1]:
            levels = levels[2:]
        if any(level in {"+", "#"} for level in levels):
            return levels, None
        return levels, "/".join(levels)

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        current = self._entries.get(topic_filter)
        if current is not None:
            sequence, levels, _old_value, exact_topic = current
            # Replacing a dict value does not move the key. Preserve the same
            # observable callback order and avoid touching either index.
            self._entries[topic_filter] = (sequence, levels, value, exact_topic)
            return

        levels, exact_topic = self._compile(topic_filter)
        sequence = self._next_sequence
        self._next_sequence += 1
        self._entries[topic_filter] = (sequence, levels, value, exact_topic)
        if exact_topic is None:
            self._wildcards[topic_filter] = None
        else:
            self._exact.setdefault(exact_topic, {})[topic_filter] = None

    def __getitem__(self, topic_filter: str) -> Any:
        try:
            return self._entries[topic_filter][2]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def __delitem__(self, topic_filter: str) -> None:
        try:
            _sequence, _levels, _value, exact_topic = self._entries.pop(topic_filter)
        except KeyError as exc:
            raise KeyError(topic_filter) from exc
        if exact_topic is None:
            self._wildcards.pop(topic_filter, None)
            return
        bucket = self._exact[exact_topic]
        bucket.pop(topic_filter, None)
        if not bucket:
            del self._exact[exact_topic]

    def iter_match(self, topic: str) -> Iterator[Any]:
        """Yield values whose MQTT filters match ``topic`` in insertion order."""

        candidates: list[tuple[int, Any]] = []
        exact = self._exact.get(topic)
        if exact is not None:
            for topic_filter in exact:
                sequence, _levels, value, _exact_topic = self._entries[topic_filter]
                candidates.append((sequence, value))

        if self._wildcards:
            topic_levels = tuple(topic.split("/"))
            is_system_topic = topic.startswith("$")
            for topic_filter in self._wildcards:
                sequence, filter_levels, value, _exact_topic = self._entries[topic_filter]
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
