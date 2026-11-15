"""Producing a qualifying result, and recomputing one that is offered."""


def produce(contracts, scenario_path, trace_path, manifest_path, scope):
    """A qualifying result for the run a trace records."""
    raise NotImplementedError


def recompute(contracts, result_path):
    """The reasons a result is refused as evidence; empty when it stands."""
    return []
