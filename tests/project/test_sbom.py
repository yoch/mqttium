"""The release SBOM describes the built artifacts and refuses a runtime dependency."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tarfile
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "generate_sbom", ROOT / "tools" / "ci" / "generate_sbom.py"
)
assert _spec is not None and _spec.loader is not None
generate_sbom = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generate_sbom)


def _distributions(directory: Path, *requirements: str) -> tuple[Path, Path]:
    metadata = "Metadata-Version: 2.4\nName: mqttium\nVersion: 9.9.9\n"
    metadata += "License-Expression: Apache-2.0\n"
    metadata += "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements)
    wheel = directory / "mqttium-9.9.9-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("mqttium/__init__.py", "")
        archive.writestr("mqttium-9.9.9.dist-info/METADATA", metadata)
    sdist = directory / "mqttium-9.9.9.tar.gz"
    with tarfile.open(sdist, "w:gz"):
        pass
    return wheel, sdist


def test_sbom_names_the_artifacts_by_hash_and_declares_no_dependency(tmp_path: Path) -> None:
    # Development extras are not installed with the package and are not dependencies.
    wheel, sdist = _distributions(tmp_path, 'pytest>=8; extra == "dev"')
    output = tmp_path / "sbom.cdx.json"

    assert generate_sbom.main(["--dist", str(tmp_path), "--output", str(output)]) == 0

    sbom = json.loads(output.read_text(encoding="utf-8"))
    assert sbom["bomFormat"] == "CycloneDX" and sbom["specVersion"] == "1.6"
    component = sbom["metadata"]["component"]
    assert component["purl"] == "pkg:pypi/mqttium@9.9.9"
    assert component["licenses"] == [{"expression": "Apache-2.0"}]
    assert {file["name"]: file["hashes"][0]["content"] for file in component["components"]} == {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (wheel, sdist)
    }
    assert sbom["components"] == []
    assert sbom["dependencies"] == [{"ref": "pkg:pypi/mqttium@9.9.9", "dependsOn": []}]


def test_sbom_is_reproducible(tmp_path: Path) -> None:
    _distributions(tmp_path)
    assert generate_sbom.build_sbom(tmp_path) == generate_sbom.build_sbom(tmp_path)


@pytest.mark.parametrize("requirement", ["anyio>=4", 'uvloop>=0.19; sys_platform != "win32"'])
def test_a_runtime_dependency_is_refused(
    tmp_path: Path, requirement: str, capsys: pytest.CaptureFixture[str]
) -> None:
    _distributions(tmp_path, requirement)
    output = tmp_path / "sbom.cdx.json"

    assert generate_sbom.main(["--dist", str(tmp_path), "--output", str(output)]) == 1

    assert not output.exists()
    assert requirement in capsys.readouterr().err


def test_a_missing_distribution_is_refused(tmp_path: Path) -> None:
    wheel, sdist = _distributions(tmp_path)
    sdist.unlink()
    with pytest.raises(generate_sbom.SbomError):
        generate_sbom.build_sbom(tmp_path)
    assert wheel.exists()


def test_publication_builds_the_sbom_from_the_validated_artifacts() -> None:
    workflow = (ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")
    validate = workflow.index("python tools/ci/validate_distribution.py")
    generate = workflow.index("python tools/ci/generate_sbom.py")
    upload = workflow.index("name: release-sbom")
    assert validate < generate < upload
    # dist/ is uploaded to PyPI as a whole: the SBOM must not be written there.
    assert '--output "sbom/' in workflow
