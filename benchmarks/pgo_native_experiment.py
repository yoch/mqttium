"""Experiment: profile-guided build of the native codec (GCC).

Usage: pgo_native.py <repo> <roots> <variant> [--port N]

Generates the codec extension from <repo>/src, builds it instrumented, installs
it into a copy of <roots>/native named <roots>/<variant>-train, trains it with
the compiled_modules.py micro cases (and closed-loop cases when --port is set),
then rebuilds it with the profile into <roots>/<variant>.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from Cython.Build import cythonize
from setuptools import Distribution, Extension

TRAIN_MICRO = (
    "encode_qos0,encode_qos1_v5,frame_64,frame_4096,frame_fragmented,properties_v5,"
    "ingress_engine_qos0,ingress_engine_qos0_v5,ingress_publish_qos1,qos1_cycle_memory,"
    "native_publish_nowait_qos0,vbi_four"
)
TRAIN_NETWORK = "network_311_qos0,network_311_qos1,network_5_qos0,network_5_qos1"


def cython_source(pyx: Path) -> Path:
    """Generate C once: Cython records extension arguments inside the C file."""
    cythonize(
        [Extension("mqttium.codec._native", [str(pyx)])],
        compiler_directives={"language_level": 3, "annotation_typing": False, "binding": True},
        quiet=True,
        force=True,
    )
    return pyx.with_suffix(".c")


def build(source: Path, work: Path, flags: list[str]) -> Path:
    extension = Extension(
        "mqttium.codec._native",
        [str(source)],
        extra_compile_args=flags,
        extra_link_args=flags,
    )
    distribution = Distribution({"name": "pgo-native", "ext_modules": [extension]})
    command = distribution.get_command_obj("build_ext")
    command.build_lib = str(work / "lib")
    command.build_temp = str(work / "obj")
    command.force = True
    distribution.run_command("build_ext")
    return Path(command.get_ext_fullpath("mqttium.codec._native"))


def install(artifact: Path, roots: Path, variant: str) -> None:
    target = roots / variant / "src"
    shutil.rmtree(roots / variant, ignore_errors=True)
    shutil.copytree(roots / "native" / "src", target)
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    destination = target / "mqttium" / "codec" / ("_native" + suffix)
    destination.unlink()
    shutil.copy2(artifact, destination)


def build_isolated(source: Path, work: Path, flags: list[str]) -> Path:
    """setuptools/Cython keep state between builds in one process; use a child."""
    output = subprocess.run(
        [sys.executable, __file__, "--build", str(source), str(work), *flags],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return Path(output.strip().splitlines()[-1])


def main() -> None:
    if sys.argv[1:2] == ["--build"]:
        print(build(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4:]))
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("repo", type=Path)
    parser.add_argument("roots", type=Path)
    parser.add_argument("variant")
    parser.add_argument("--port", type=int)
    parser.add_argument("--cpu", type=int, default=2)
    args = parser.parse_args()
    repo, roots = args.repo.resolve(), args.roots.resolve()
    work = roots / f"{args.variant}-build"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    spec = importlib.util.spec_from_file_location("_compile", repo / "src/mqttium/codec/_compile.py")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    source = work / "_native.pyx"
    generator.generate(repo / "src/mqttium", source)
    source = cython_source(source)
    profile = work / "profile"
    profile.mkdir()

    # Same object paths in both builds: GCC names profiles after them.
    instrumented = build_isolated(source, work / "build", [f"-fprofile-generate={profile}"])
    install(instrumented, roots, f"{args.variant}-train")
    harness = repo / "benchmarks/compiled_modules.py"
    cases = TRAIN_MICRO + ("," + TRAIN_NETWORK if args.port else "")
    for case in cases.split(","):
        command = [
            sys.executable,
            str(harness),
            "worker",
            "--root",
            str(roots / f"{args.variant}-train"),
            "--variant",
            f"{args.variant}-train",
            "--case",
            case,
            "--cpu",
            str(args.cpu),
            "--count",
            "6000",
        ]
        if args.port:
            command += ["--port", str(args.port)]
        subprocess.run(
            command, check=True, capture_output=True, cwd=repo / "benchmarks", env=os.environ
        )
        print("trained", case, flush=True)
    profiles = list(profile.rglob("*.gcda"))
    if not profiles:
        raise SystemExit("no profile data written")
    print("profiles", [p.name for p in profiles])

    optimized = build_isolated(
        source,
        work / "build",
        [f"-fprofile-use={profile}", "-fprofile-correction", "-Wno-missing-profile"],
    )
    install(optimized, roots, args.variant)
    print("ok", roots / args.variant)


if __name__ == "__main__":
    main()
