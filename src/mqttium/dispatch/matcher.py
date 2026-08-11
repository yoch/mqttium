"""Topic-filter matching for callback dispatch.

The matcher intentionally uses a compact, explicit representation rather than
sharing protocol-engine state. Exact and wildcard filters retain insertion order,
and values may legitimately be ``None``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def _filter_is_wildcard(topic_filter: str) -> bool:
    return "+" in topic_filter or "#" in topic_filter


class TopicMatcher:
    """Map MQTT topic filters to values and iterate matches for a topic."""

    def __init__(self) -> None:
        # Insertion-ordered filter → (levels, value, is_wildcard).
        self._entries: dict[str, tuple[tuple[str, ...], Any, bool]] = {}
        self._wildcard_count = 0

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        wildcard = _filter_is_wildcard(topic_filter)
        previous = self._entries.get(topic_filter)
        if previous is not None and previous[2]:
            self._wildcard_count -= 1
        self._entries[topic_filter] = (tuple(topic_filter.split("/")), value, wildcard)
        if wildcard:
            self._wildcard_count += 1

    def __getitem__(self, topic_filter: str) -> Any:
        try:
            return self._entries[topic_filter][1]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def __delitem__(self, topic_filter: str) -> None:
        try:
            _levels, _value, wildcard = self._entries.pop(topic_filter)
        except KeyError as exc:
            raise KeyError(topic_filter) from exc
        if wildcard:
            self._wildcard_count -= 1

    def iter_match(self, topic: str) -> Iterator[Any]:
        """Yield values whose MQTT filters match ``topic`` in insertion order."""

        # Exact-only registries are the common Paho `message_callback_add` case:
        # skip the linear scan entirely.
        if self._wildcard_count == 0:
            entry = self._entries.get(topic)
            if entry is not None:
                yield entry[1]
            return

        topic_levels = tuple(topic.split("/"))
        is_system_topic = topic.startswith("$")
        for filter_key, (filter_levels, value, wildcard) in self._entries.items():
            if not wildcard:
                if filter_key == topic:
                    yield value
                continue
            if self._matches(filter_levels, topic_levels, is_system_topic):
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
