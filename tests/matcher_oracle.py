"""Linear reference for ``TopicMatcher``, kept as a differential test oracle.

This is the flat-filter matcher that shipped until 1.1.0: every registered
filter is tested in registration order. It is deliberately slow and simple so
the indexed matcher can be checked against it.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any


def _levels(topic_filter: str) -> tuple[str, ...]:
    if topic_filter.startswith("$share/"):
        return tuple(topic_filter.split("/", 2)[2].split("/"))
    return tuple(topic_filter.split("/"))


def _matches(filter_levels: tuple[str, ...], topic: str) -> bool:
    topic_levels = tuple(topic.split("/"))
    if topic.startswith("$") and filter_levels[0] in {"+", "#"}:
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


class LinearTopicMatcher:
    """Registration-ordered filters, each tested against every topic."""

    def __init__(self) -> None:
        self._entries: dict[str, Any] = {}

    def __setitem__(self, topic_filter: str, value: Any) -> None:
        self._entries[topic_filter] = value

    def __getitem__(self, topic_filter: str) -> Any:
        return self._entries[topic_filter]

    def __delitem__(self, topic_filter: str) -> None:
        del self._entries[topic_filter]

    def __bool__(self) -> bool:
        return bool(self._entries)

    def iter_match(self, topic: str) -> Iterator[Any]:
        for topic_filter, value in self._entries.items():
            if _matches(_levels(topic_filter), topic):
                yield value
