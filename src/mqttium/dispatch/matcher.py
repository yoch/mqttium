"""Topic-filter matching for callback dispatch.

The matcher intentionally uses a compact, explicit representation rather than
sharing protocol-engine state. Exact and wildcard filters retain insertion order,
and values may legitimately be ``None``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


class TopicMatcher:
    """Map MQTT topic filters to values and iterate matches for a topic."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[tuple[str, ...], Any]] = {}

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        self._entries[topic_filter] = (self._match_levels(topic_filter), value)

    def __getitem__(self, topic_filter: str) -> Any:
        try:
            return self._entries[topic_filter][1]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def __delitem__(self, topic_filter: str) -> None:
        try:
            del self._entries[topic_filter]
        except KeyError as exc:
            raise KeyError(topic_filter) from exc

    def iter_match(self, topic: str) -> Iterator[Any]:
        """Yield values whose MQTT filters match ``topic`` in insertion order."""

        topic_levels = tuple(topic.split("/"))
        is_system_topic = topic.startswith("$")
        for filter_levels, value in self._entries.values():
            if self._matches(filter_levels, topic_levels, is_system_topic):
                yield value

    @staticmethod
    def _match_levels(topic_filter: str) -> tuple[str, ...]:
        """Return the filter levels present in broker-delivered Topic Names.

        A broker strips ``$share/{ShareName}/`` before delivering a shared
        publication. Keep the original filter as the mapping key, but match its
        underlying filter so callback lookup and deletion remain symmetric.
        Invalid shared-filter shapes are left literal; subscription validation
        remains the protocol layer's responsibility.
        """
        if topic_filter.startswith("$share/"):
            group, separator, shared_filter = topic_filter[7:].partition("/")
            if (
                separator
                and group
                and shared_filter
                and not any(character in group for character in "+#")
            ):
                topic_filter = shared_filter
        return tuple(topic_filter.split("/"))

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
