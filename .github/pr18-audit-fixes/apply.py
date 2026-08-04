from __future__ import annotations

import re
from pathlib import Path

HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
PATCH_DIR = Path(".github/pr18-audit-fixes")


def line_offset(text: str, line_number: int) -> int:
    if line_number <= 1:
        return 0
    pos = 0
    for _ in range(line_number - 1):
        next_pos = text.find("\n", pos)
        if next_pos < 0:
            return len(text)
        pos = next_pos + 1
    return pos


def occurrences(text: str, needle: str) -> list[int]:
    found: list[int] = []
    start = 0
    while True:
        pos = text.find(needle, start)
        if pos < 0:
            return found
        found.append(pos)
        start = pos + max(1, len(needle))


def apply_hunk(text: str, old: str, new: str, expected_line: int, label: str) -> str:
    expected = line_offset(text, expected_line)
    if not old:
        return text[:expected] + new + text[expected:]
    matches = occurrences(text, old)
    if not matches:
        raise RuntimeError(f"{label}: exact source block not found")
    chosen = min(matches, key=lambda pos: abs(pos - expected))
    return text[:chosen] + new + text[chosen + len(old) :]


def apply_patch(path: Path) -> None:
    raw_lines = path.read_text().splitlines(keepends=True)
    # Repair accidental transcript markers in the staged test patch.
    lines = []
    for line in raw_lines:
        if line.startswith("+*+"):
            line = "+" + line[3:]
        elif line.startswith("*+"):
            line = line[1:]
        lines.append(line)

    old_header = next(line for line in lines if line.startswith("--- ")).split(maxsplit=1)[1].strip()
    new_header = next(line for line in lines if line.startswith("+++ ")).split(maxsplit=1)[1].strip()
    old_path = None if old_header == "/dev/null" else old_header.removeprefix("a/")
    new_path = None if new_header == "/dev/null" else new_header.removeprefix("b/")
    target = new_path or old_path
    if target is None:
        raise RuntimeError(f"{path}: patch has no target")

    text = "" if old_path is None else Path(target).read_text()
    i = 0
    hunk_count = 0
    while i < len(lines):
        match = HUNK.match(lines[i])
        if match is None:
            i += 1
            continue
        hunk_count += 1
        new_start = int(match.group(3))
        i += 1
        body: list[str] = []
        while i < len(lines) and not lines[i].startswith("@@ "):
            if lines[i].startswith("diff --git "):
                break
            body.append(lines[i])
            i += 1

        old = "".join(line[1:] for line in body if line[:1] in {" ", "-"})
        new = "".join(line[1:] for line in body if line[:1] in {" ", "+"})
        text = apply_hunk(text, old, new, new_start, f"{path.name} hunk {hunk_count}")

    if hunk_count == 0:
        raise RuntimeError(f"{path}: no hunks found")
    target_path = Path(target)
    if new_path is None:
        target_path.unlink()
    else:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(text)


patches = sorted(PATCH_DIR.glob("*.patch"))
if len(patches) != 7:
    raise RuntimeError(f"expected seven reviewed patches, found {len(patches)}")
for patch in patches:
    apply_patch(patch)
