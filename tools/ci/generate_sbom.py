#!/usr/bin/env python3
"""Write a CycloneDX SBOM for the built MQTTium distributions.

The SBOM is derived from the artifacts in ``dist/``, not from ``pyproject.toml``:
the component hashes are those of the files that will be published, and the
dependency list is what the wheel's own metadata declares. MQTTium promises no
runtime dependency, so a wheel that declares one fails here instead of being
described faithfully.
"""

from __future__ import annotations

import argparse
import email
import hashlib
import json
import sys
from email.message import Message
from pathlib import Path
from zipfile import ZipFile

SPEC_VERSION = "1.6"


class SbomError(Exception):
    """The distributions cannot be described by a dependency-free SBOM."""


def _single(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        raise SbomError(f"expected exactly one {pattern} in {directory}, found {len(matches)}")
    return matches[0]


def _wheel_metadata(wheel: Path) -> Message:
    with ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(names) != 1:
            raise SbomError(f"{wheel.name} must contain exactly one METADATA file")
        return email.message_from_bytes(archive.read(names[0]))


def runtime_requirements(metadata: Message) -> list[str]:
    """Requirements installed without asking for an extra."""
    requirements = []
    for requirement in metadata.get_all("Requires-Dist") or []:
        _, _, marker = requirement.partition(";")
        if "extra" not in marker:
            requirements.append(requirement.strip())
    return requirements


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_sbom(dist: Path) -> dict[str, object]:
    wheel = _single(dist, "*.whl")
    sdist = _single(dist, "*.tar.gz")
    metadata = _wheel_metadata(wheel)
    name, version = metadata["Name"], metadata["Version"]
    if not name or not version:
        raise SbomError(f"{wheel.name} metadata has no Name or Version")
    required = runtime_requirements(metadata)
    if required:
        raise SbomError(
            f"{wheel.name} declares runtime dependencies, which MQTTium does not allow: "
            + ", ".join(required)
        )
    reference = f"pkg:pypi/{name.lower()}@{version}"
    component: dict[str, object] = {
        "type": "library",
        "bom-ref": reference,
        "name": name,
        "version": version,
        "purl": reference,
        "components": [
            {
                "type": "file",
                "name": artifact.name,
                "hashes": [{"alg": "SHA-256", "content": _sha256(artifact)}],
            }
            for artifact in (wheel, sdist)
        ],
    }
    license_expression = metadata["License-Expression"]
    if license_expression:
        component["licenses"] = [{"expression": license_expression}]
    return {
        "bomFormat": "CycloneDX",
        "specVersion": SPEC_VERSION,
        "version": 1,
        "metadata": {"component": component},
        # No timestamp and no serial number: the same distributions always
        # produce the same document, so the SBOM itself can be compared.
        "components": [],
        "dependencies": [{"ref": reference, "dependsOn": []}],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        sbom = build_sbom(args.dist)
    except SbomError as exc:
        print(f"SBOM refused: {exc}", file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(sbom, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
