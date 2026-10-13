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
    "encode_publish_item_v311": "f2f60ebe8d5c042b6bff9f295c69c5cf48818086e658428280ef250b0972b17b",
    "encode_publish_item_v5": "d12598076004547a1daf0e1caa3e61fcbfd8fc34a6fa882233e1c8a4eb638928",
}


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


def _fast_path(node: ast.FunctionDef, statements: str) -> None:
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
        elif node.name in {"unpack_u16", "unpack_u32", "unpack_utf8"}:
            kernel = {
                "unpack_u16": "_read_u16",
                "unpack_u32": "_read_u32",
                "unpack_utf8": "_read_utf8",
            }[node.name]
            _fast_path(
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
            digest = hashlib.sha256(ast.dump(tail).encode()).hexdigest()
            if digest != ASSEMBLY_DIGESTS[node.name]:
                raise RuntimeError("PUBLISH assembly changed; review native kernel")
            props = "props" if node.name.endswith("v5") else "b''"
            node.body[index:index] = ast.parse(
                f"if type(topic_bytes) is bytes and type(payload) is bytes and type({props}) is bytes and (not level or type(mid) is int):\n    return _assemble_publish(topic_bytes, payload, {props}, int(level), flags, mid if level else 0, remaining_length)"
            ).body


def generate(package: Path, destination: Path) -> None:
    """Fail on symbol collisions or changed copy boundaries rather than guessing."""
    exports = {}
    definitions: set[str] = set()
    global_origins: dict[str, tuple[str, str]] = {}
    bodies = []
    digest = hashlib.sha256(Path(__file__).read_bytes())
    for module in MODULES:
        source = (
            (package / (module.replace(".", "/") + ".py"))
            .read_text(encoding="utf-8")
            .split(MARKER)[0]
        )
        digest.update(module.encode() + b"\0" + source.encode())
        tree = ast.parse(source)
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
        if module != "transport.writes":
            exports["mqttium." + module] = native_names
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
    digest.update(kernels.encode())
    mapping = ",\n".join(
        f"    {module!r}: {{{', '.join(f'{name!r}: {native}' for name, native in names.items())}}}"
        for module, names in exports.items()
    )
    destination.write_text(
        "from __future__ import annotations\nimport cython\n"
        + kernels
        + "\n"
        + "\n\n".join(bodies)
        + f"\nSOURCE_DIGEST = {digest.hexdigest()!r}\nEXPORTS = {{\n{mapping}\n}}\n",
        encoding="utf-8",
    )
