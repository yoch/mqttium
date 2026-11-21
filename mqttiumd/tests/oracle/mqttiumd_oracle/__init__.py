"""Independent oracle of mqttiumd (DEM-019).

It reads contracts, scenarios, traces and store snapshots, and decides the
assertions of a run. It shares no decision logic with the broker: nothing
here imports, runs or was derived from ``crates/mqttiumd-core``.
"""
