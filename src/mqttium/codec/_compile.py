"""Generate one native codec from maintained Python sources and typed kernels.

Used only by the Hatch build hook. Does not import MQTTium during a build.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

MODULES = (
    "transport.writes",
    "codec.vbi",
    "codec.primitives",
    "topics",
    "codec.buffer",
    "codec.packet_validation",
    "codec.properties",
    "packets._common",
    "packets._publish",
    "packets._ack",
    "packets._connack",
    "packets._connect",
    "packets._control",
    "packets._suback",
    "packets._subscribe",
    "packets.acks",
    "packets.connect",
    "packets.control",
    "packets.publish",
    "packets.subscription",
)
MARKER = "# Select the complete codec implementation."
RENAMES = {
    "packets._control": {"encode_pingreq": "_wire_pingreq", "encode_pingresp": "_wire_pingresp"}
}


# These generic tails are replaced by _assemble_publish. Any change needs a review
# of the kernel, including new validation added after header allocation.
ASSEMBLY_DIGESTS = {
    "encode_publish_item_v311": "f30e725495b573b27ad3065d67b9c9689a7e2e19f5dcee132abaa61607b49c50",
    "encode_publish_item_v5": "404fa0bf3ae9f541d6170253124e022f67470cd20ca411e538d2949f21ae8a66",
}
# Kernels answer these functions for exact bytes before their Python body runs.
# Any change to a body, such as added validation, needs a review of its kernel.
FAST_PATH_DIGESTS = {
    "unpack_u16": "32a5785b4989abcdb4a0483a37cc7ce85db342cf3655c6bf4828383e796a1304",
    "unpack_u32": "a04ffce04f19e37b9ec3d8f15225d37e5529c0a7a2c53e34aaf74f34d98d6302",
    "unpack_utf8": "e137ad8744617fadf4a79c0f7b9bdadd3f0d9c674c86da869ed2bf8ae1b597cd",
}


def _digest(node: ast.AST) -> str:
    return hashlib.sha256(repr(_shape(node)).encode()).hexdigest()


def _shape(value: object) -> object:
    """Stable semantic form: ast.dump() changes empty-field formatting in 3.13."""
    if isinstance(value, ast.AST):
        return (
            type(value).__name__,
            tuple(
                (name, _shape(field))
                for name, field in ast.iter_fields(value)
                if field is not None and field != []
            ),
        )
    if isinstance(value, list):
        return tuple(_shape(item) for item in value)
    return value


def _definitions(tree: ast.Module) -> list[str]:
    result = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            result.append(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                result.extend(n.id for n in ast.walk(target) if isinstance(n, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            result.append(node.target.id)
    return result


def _register_globals(tree: ast.Module, module: str, registry: dict[str, tuple[str, str]]) -> None:
    """Fusing namespaces must not change the owner of a global used by a function."""
    renames = RENAMES.get(module, {})
    entries = [(renames.get(name, name), (module, name)) for name in _definitions(tree)]
    for node in tree.body:
        if isinstance(node, ast.Import):
            entries.extend(
                (alias.asname or alias.name.split(".")[0], ("import", alias.name))
                for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            source = node.module or ""
            internal = source.removeprefix("mqttium.") in MODULES
            for alias in node.names:
                if alias.name == "*" or node.level:
                    raise RuntimeError("Native codec requires explicit absolute imports")
                origin_module = source.removeprefix("mqttium.") if internal else source
                entries.append((alias.asname or alias.name, (origin_module, alias.name)))
        elif isinstance(node, ast.For):
            # Current module initialization only attaches codecs to property specs.
            entries.extend(
                (n.id, (module, n.id)) for n in ast.walk(node.target) if isinstance(n, ast.Name)
            )
    for name, origin in entries:
        previous = registry.setdefault(name, origin)
        if previous != origin:
            raise RuntimeError(f"Native codec global collision: {name} ({previous} vs {origin})")


def _locals(node: ast.FunctionDef, types: dict[str, str]) -> None:
    args = ", ".join(f"{name}={kind}" for name, kind in types.items())
    decorated = ast.parse(f"@cython.locals({args})\ndef f(): pass").body[0]
    assert isinstance(decorated, ast.FunctionDef)
    node.decorator_list.append(decorated.decorator_list[0])


def _insert_after_docstring(node: ast.FunctionDef | ast.ClassDef, statements: str) -> None:
    position = (
        1
        if node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
        else 0
    )
    node.body[position:position] = ast.parse(statements).body


def _decoder(node: ast.ClassDef) -> ast.ClassDef:
    fields = next(
        n
        for n in node.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "__slots__" for t in n.targets)
    )
    names = ast.literal_eval(fields.value)
    expected = {
        "_buf",
        "_view",
        "_max_packet_size",
        "_drain_had_large_frame",
        "_start",
        "_end",
        "_capacity",
        "_idle_drains",
        "_window_peak",
        "_offered",
        "_target_window",
        "_full_fills",
        "_high_water",
    }
    if set(names) != expected:
        raise RuntimeError("Decoder fields changed; review native storage types")
    object_fields = {"_buf", "_view", "_max_packet_size"}
    booleans = {"_drain_had_large_frame"}
    declarations = []
    for name in names:
        kind = (
            "object"
            if name in object_fields
            else "cython.bint"
            if name in booleans
            else "cython.Py_ssize_t"
        )
        declarations.append(f"    {name} = cython.declare({kind}, visibility='public')")
    base = ast.parse("@cython.cclass\nclass _DecoderState:\n" + "\n".join(declarations)).body[0]
    assert isinstance(base, ast.ClassDef)
    fields.value = ast.Tuple(elts=[], ctx=ast.Load())
    node.bases = [ast.Name(id="_DecoderState", ctx=ast.Load())]
    for method in node.body:
        if isinstance(method, ast.FunctionDef):
            types = {"self": "_DecoderState"}
            if method.name in {"peek_packet_bounds", "next_packet"}:
                for expression in ast.walk(method):
                    if (
                        isinstance(expression, ast.Call)
                        and isinstance(expression.func, ast.Name)
                        and expression.func.id == "all"
                    ):
                        if (
                            ast.unparse(expression.args[0])
                            != "(buf[start + i] & 128 for i in range(1, available))"
                        ):
                            raise RuntimeError("Decoder continuation check changed")
                        expression.func.id = "_all_continuations"
                        expression.args = [
                            ast.Name(id=n, ctx=ast.Load()) for n in ("buf", "start", "available")
                        ]
                types.update(
                    {
                        "buf": "bytearray",
                        **{
                            n: "cython.Py_ssize_t"
                            for n in (
                                "start",
                                "available",
                                "fixed_header_len",
                                "total",
                                "body_start",
                                "body_end",
                                "remaining_length",
                                "rl_end",
                            )
                        },
                    }
                )
            _locals(method, types)
            if method.name == "next_packet":
                # The framing checks above this copy remain the maintained source.
                for index, statement in enumerate(method.body):
                    if (
                        isinstance(statement, ast.If)
                        and ast.unparse(statement.test)
                        == "remaining_length >= _VIEW_COPY_THRESHOLD"
                    ):
                        expected_copy = ast.parse(
                            "if remaining_length >= _VIEW_COPY_THRESHOLD:\n"
                            "    body = bytes(self._view[body_start:body_end])\n"
                            "else:\n"
                            "    body = bytes(buf[body_start:body_end])"
                        ).body[0]
                        if ast.dump(statement) != ast.dump(expected_copy):
                            raise RuntimeError(
                                "Decoder copy block changed; review native generator"
                            )
                        method.body[index] = ast.parse(
                            "body = _copy_owned(buf, body_start, remaining_length)"
                        ).body[0]
                        break
                else:
                    raise RuntimeError("Decoder copy boundary changed; review native generator")
    return base


def _optimize(tree: ast.Module) -> None:
    for node in list(tree.body):
        if isinstance(node, ast.ClassDef) and node.name == "IncrementalDecoder":
            tree.body.insert(tree.body.index(node), _decoder(node))
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name == "decode_vbi":
            _locals(
                node,
                {
                    "value": "cython.uint",
                    "multiplier": "cython.uint",
                    "encoded_bytes": "cython.int",
                    "pos": "cython.Py_ssize_t",
                },
            )
        elif node.name in FAST_PATH_DIGESTS:
            if _digest(node) != FAST_PATH_DIGESTS[node.name]:
                raise RuntimeError(f"{node.name} changed; review its native kernel")
            kernel = {
                "unpack_u16": "_read_u16",
                "unpack_u32": "_read_u32",
                "unpack_utf8": "_read_utf8",
            }[node.name]
            _insert_after_docstring(
                node,
                f"if type(buffer) is bytes and type(offset) is int and 0 <= offset <= len(buffer):\n    return {kernel}(buffer, offset)",
            )
        elif node.name in {"encode_publish_item_v311", "encode_publish_item_v5"}:
            _locals(node, {"header": "bytearray"})
            # Insert after all maintained validation and the wire maximum check.
            index = next(
                i
                for i, n in enumerate(node.body)
                if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "header" for t in n.targets)
            )
            tail = ast.Module(body=node.body[index:], type_ignores=[])
            if _digest(tail) != ASSEMBLY_DIGESTS[node.name]:
                raise RuntimeError("PUBLISH assembly changed; review native kernel")
            props = "props" if node.name.endswith("v5") else "b''"
            node.body[index:index] = ast.parse(
                f"if type(topic_bytes) is bytes and type(payload) is bytes and type({props}) is bytes and (not level or type(mid) is int):\n    return _assemble_publish(topic_bytes, payload, {props}, int(level), flags, mid if level else 0, remaining_length)"
            ).body


def _wire_source(package: Path, module: str) -> str:
    path = package / (module.replace(".", "/") + ".py")
    return path.read_text(encoding="utf-8").split(MARKER)[0]


def source_digest(package: Path) -> str:
    """Identify the maintained sources an extension must have been generated from."""
    digest = hashlib.sha256()
    for module in MODULES:
        digest.update(module.encode() + b"\0" + _wire_source(package, module).encode())
    return digest.hexdigest()


def generate(package: Path, destination: Path) -> None:
    """Fail on symbol collisions or changed copy boundaries rather than guessing."""
    exports = {}
    definitions: set[str] = set()
    global_origins: dict[str, tuple[str, str]] = {}
    bodies = []
    owners: list[str] = []
    for module in MODULES:
        tree = ast.parse(_wire_source(package, module))
        _register_globals(tree, module, global_origins)
        names = _definitions(tree)
        renames = RENAMES.get(module, {})
        native_names = {name: renames.get(name, name) for name in names}
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in renames:
                node.name = renames[node.name]
        collisions = definitions.intersection(native_names.values())
        if collisions:
            raise RuntimeError(f"Native codec global collision in {module}: {sorted(collisions)}")
        definitions.update(native_names.values())
        exports["mqttium." + module] = native_names
        # Public types keep their module for repr, pickling and introspection.
        # Assigned after creation: dataclasses resolve annotations in their module.
        owners.extend(
            f"{native_names[node.name]}.__module__ = {'mqttium.' + module!r}"
            for node in tree.body
            if isinstance(node, ast.ClassDef)
        )
        body = []
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom):
                body.append(node)
            elif node.module in {"mqttium." + m for m in MODULES}:
                imported_renames = RENAMES.get(node.module.removeprefix("mqttium."), {})
                for alias in node.names:
                    original = imported_renames.get(alias.name, alias.name)
                    local = alias.asname or alias.name
                    if original != local:
                        body.extend(ast.parse(f"{local} = {original}").body)
            elif node.module != "__future__":
                body.append(node)
        tree.body = body
        _optimize(tree)
        bodies.append(ast.unparse(ast.fix_missing_locations(tree)))
    kernels = (package / "codec/_kernels.pxi").read_text(encoding="utf-8")
    sources = source_digest(package)
    build = hashlib.sha256(Path(__file__).read_bytes() + kernels.encode() + sources.encode())
    mapping = ",\n".join(
        f"    {module!r}: {{{', '.join(f'{name!r}: {native}' for name, native in names.items())}}}"
        for module, names in exports.items()
    )
    destination.write_text(
        "from __future__ import annotations\nimport cython\n"
        + kernels
        + "\n"
        + "\n\n".join(bodies)
        + "\n"
        + "\n".join(owners)
        + f"\nSOURCE_DIGEST = {sources!r}\nBUILD_DIGEST = {build.hexdigest()!r}\n"
        + f"EXPORTS = {{\n{mapping}\n}}\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    package = Path(__file__).resolve().parents[1]
    (package / "codec/_digest.py").write_text(
        '"""Sources a native codec must be generated from (python -m mqttium.codec._compile)."""\n\n'
        f'SOURCE_DIGEST = "{source_digest(package)}"\n',
        encoding="utf-8",
    )
