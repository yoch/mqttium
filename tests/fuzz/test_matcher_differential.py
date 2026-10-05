"""Hypothesis differential test: indexed ``TopicMatcher`` vs the linear oracle.

Random filters (wildcards, ``$share`` aliases, ``$`` and empty levels) are
inserted, replaced and deleted; every probe topic must yield the same values
in the same order from both matchers.
"""

from __future__ import annotations

import os

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, strategies as st

settings.register_profile("matcher-ci", max_examples=600, deadline=None)
settings.register_profile("matcher-aggressive", max_examples=5000, deadline=None)
settings.load_profile(
    "matcher-aggressive" if os.environ.get("HYPOTHESIS_PROFILE") == "aggressive" else "matcher-ci"
)

from mqttium.dispatch.matcher import TopicMatcher
from tests.matcher_oracle import LinearTopicMatcher

_TOPIC_LEVEL = st.sampled_from(["a", "b", "c", "", "$SYS", "$x"])
_FILTER_LEVEL = st.one_of(_TOPIC_LEVEL, st.sampled_from(["+", "#"]))


@st.composite
def _filters(draw: st.DrawFn) -> str:
    parts = draw(st.lists(_FILTER_LEVEL, min_size=1, max_size=5))
    if "#" in parts:
        parts = parts[: parts.index("#") + 1]
    topic_filter = "/".join(parts)
    if draw(st.booleans()) and draw(st.booleans()):
        topic_filter = f"$share/{draw(st.sampled_from(['g1', 'g2']))}/{topic_filter}"
    return topic_filter


_TOPICS = st.lists(_TOPIC_LEVEL, min_size=1, max_size=6).map("/".join)
_OPERATIONS = st.lists(
    st.tuples(st.sampled_from(["set", "delete"]), _filters(), st.integers(0, 3)),
    min_size=1,
    max_size=40,
)


# The aggressive profile runs for about a minute; the CI profile in seconds.
@pytest.mark.timeout(300)
@given(operations=_OPERATIONS, probes=st.lists(_TOPICS, min_size=1, max_size=15))
def test_indexed_matcher_agrees_with_linear_oracle(
    operations: list[tuple[str, str, int]], probes: list[str]
) -> None:
    matcher = TopicMatcher()
    oracle = LinearTopicMatcher()
    for operation, topic_filter, value in operations:
        if operation == "set":
            matcher[topic_filter] = value
            oracle[topic_filter] = value
            continue
        try:
            del oracle[topic_filter]
        except KeyError:
            with pytest.raises(KeyError):
                del matcher[topic_filter]
        else:
            del matcher[topic_filter]
    assert bool(matcher) == bool(oracle)
    for topic in probes:
        assert list(matcher.iter_match(topic)) == list(oracle.iter_match(topic)), topic
