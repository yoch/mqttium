# Compiled codec

MQTTium can build one optional Cython extension for the complete MQTT wire
codec: framing, variable byte integers, primitives, topic validation, properties,
and all MQTT 3.1.1/5 packet encoders and decoders. The client, protocol engine,
stores and event loop remain Python. Runtime dependencies remain empty.

## Installation

The default build is pure Python. From a checkout containing this feature,
use a CPython interpreter with development headers and a working C compiler:

```bash
MQTTIUM_BUILD_CODEC=1 python -m pip install .
MQTTIUM_CODEC_BACKEND=native python -c 'from mqttium.codec._backend import BACKEND; print(BACKEND)'
```

On PowerShell, set `$env:MQTTIUM_BUILD_CODEC = "1"` before running pip.
Build isolation installs Cython and setuptools only for the native wheel build.
The same Hatch hook works when installing the source distribution. Wheels are
specific to the Python ABI and platform; this is not a stable-ABI extension.
Installing an existing pure-Python wheel does not compile it. This unreleased
feature does not imply that native wheels are available on PyPI.

To create an artifact for installation on matching machines:

```bash
MQTTIUM_BUILD_CODEC=1 python -m build
```

Use `MQTTIUM_BUILD_CODEC=0` (the default) for a pure-Python build. A requested
native build fails if compilation fails; it does not silently produce a pure
wheel. The release publication workflow retains its existing default build.

## Backend selection

Set `MQTTIUM_CODEC_BACKEND` before importing MQTTium:

| Value | Behaviour |
| --- | --- |
| `auto` (default) | Use the extension when present; otherwise use Python. |
| `native` | Require the extension; fail if it is missing. |
| `python` | Use the Python reference even with a native wheel installed. |

A broken extension or missing dependency always raises an import error in
`auto` and `native` modes. Selection is process-wide and cannot be switched
after import. Codec implementation types, globals and tracebacks are Internal
and can differ between backends; client APIs and wire behaviour are unchanged.

## Implementation and validation

The build generates a single `_native` extension from maintained Python codec
sources. A small set of bounded Cython kernels handles integer/string reads,
owned frame copies and PUBLISH assembly. Ordinary PUBLISH bytes are allocated
once; large payloads retain the existing segmented-write threshold and payload
reuse. The decoder uses typed counters but always returns owned bytes. Protocol
validation, size limits and malformed-input handling remain in the reference
sources. The GIL remains held; this accelerates individual operations rather
than introducing client parallelism.

The generator rejects global symbol collisions and changed decoder copy
boundaries. When changing codec sources, validate both backends. Native CI
builds from the sdist, exercises the installed wheel, compares deterministic
wire/error corpora with Python, and runs unit, integration and fuzz tests.
Source-tree tests alone do not exercise an installed extension: override the
repository's pytest `pythonpath` setting with `-o pythonpath=`.

The internal benchmark harness compares the same native wheel in fresh
processes with explicit backend selection, alternating order and CPU timing:

```bash
python benchmarks/compiled_modules.py prepare-wheel --root /tmp/codec-study --wheel dist/your-native-wheel.whl
python benchmarks/compiled_modules.py compare --root /tmp/codec-study --variant native --repeat 6 --output /tmp/codec-results.json
```

This harness needs Linux CPU affinity support. Network cases additionally need
Mosquitto; use `--cases network_5_qos1 --port 18885`. Keep the broker and client
on separate CPUs and run without competing tests or builds. CPU savings in the
codec are not equivalent to savings across the entire application; use a
representative message size, QoS, transport and persistence workload.
