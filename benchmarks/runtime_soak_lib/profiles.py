"""Named soak profiles for CI, local, nightly, and release campaigns."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion


@dataclass(frozen=True, slots=True)
class SoakProfile:
    name: str
    operations: int
    seeds: tuple[int, ...]
    protocols: tuple[MQTTProtocolVersion, ...]
    timeout: float
    durable: bool
    sqlite: bool
    reduce_on_fail: bool
    description: str


PROFILES: dict[str, SoakProfile] = {
    "ci": SoakProfile(
        name="ci",
        operations=64,
        seeds=(1, 7),
        protocols=(MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5),
        timeout=8.0,
        durable=True,
        sqlite=False,
        reduce_on_fail=True,
        description="PR-sized fake-broker search: mixed ops, two seeds, both protocols.",
    ),
    "local": SoakProfile(
        name="local",
        operations=8_000,
        seeds=(1, 7, 13, 42),
        protocols=(MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5),
        timeout=15.0,
        durable=True,
        sqlite=True,
        reduce_on_fail=True,
        description="Workstation campaign before a long nightly run.",
    ),
    "nightly": SoakProfile(
        name="nightly",
        operations=50_000,
        seeds=(1, 7, 13, 42, 99, 256),
        protocols=(MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5),
        timeout=20.0,
        durable=True,
        sqlite=True,
        reduce_on_fail=True,
        description="Overnight fake-broker search with reduction on first failure.",
    ),
    "release": SoakProfile(
        name="release",
        operations=200_000,
        seeds=(1, 7, 13, 42, 99, 256, 1024),
        protocols=(MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5),
        timeout=30.0,
        durable=True,
        sqlite=True,
        reduce_on_fail=True,
        description="Release-evidence campaign. Record commit, profile, and JSON artefact.",
    ),
}
