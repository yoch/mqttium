"""Deciding the assertions of a run from its observations (DEM-019)."""

from dataclasses import dataclass, field

INCONCLUSIVE = "INCONCLUSIVE"


@dataclass
class Status:
    status: str
    reasons: list = field(default_factory=list)


class Run:
    """The observations of one run."""

    def __init__(self, scenario):
        self.scenario = scenario

    @classmethod
    def load(cls, contracts, scenario_path, trace_path):
        from .strict import parse

        return cls(parse(contracts.read(scenario_path)))


def judge(run):
    """The status of every assertion of the scenario, by name."""
    return {
        name: Status(INCONCLUSIVE, ["the oracle does not decide this assertion yet"])
        for name in run.scenario["assertions"]
    }
