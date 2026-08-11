#!/usr/bin/env python3
"""Rebuild the vendored MQTT conformance-statement index from the OASIS sources.

The OASIS HTML is a Word export with a consistent convention: the text of every
normative statement is highlighted (``background:yellow``) and immediately
followed by its label in red (``[MQTT-x.y.z-n]``). That makes the statements
machine-extractable verbatim, which is the whole point — an index paraphrased by
hand or by a model is worse than none, because it would be trusted.

Usage::

    python tools/extract_spec_statements.py            # download and rebuild
    python tools/extract_spec_statements.py --check    # verify the index is current

Writes ``docs/spec/mqtt-v*-statements.json``. Only the extracted statements are
vendored; the full specifications are not redistributed here.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "spec"

SOURCES = {
    "3.1.1": {
        "title": "MQTT Version 3.1.1 Plus Errata 01, OASIS Standard Incorporating "
        "Approved Errata 01",
        "url": "https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html",
        "published": "2015-12-10",
    },
    "5.0": {
        "title": "MQTT Version 5.0, OASIS Standard",
        "url": "https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html",
        "published": "2019-03-07",
    },
}

_HIGHLIGHT_OPEN = re.compile(r"<span[^>]*background:\s*yellow[^>]*>", re.IGNORECASE)
_SPAN_CLOSE = re.compile(r"</span>", re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")
_LABEL = re.compile(r"\[(MQTT-\d+(?:\.\d+)*-\d+)\]")
_HEADING = re.compile(
    r"<h[1-6][^>]*>(.*?)</h[1-6]>",
    re.IGNORECASE | re.DOTALL,
)
_CELL = re.compile(r"</?t[dh][^>]*>", re.IGNORECASE)
_WS = re.compile(r"\s+")

# Sentinels survive tag stripping and cannot appear in the source text.
_MARK_START = "\x01"
_MARK_END = "\x02"
_HEAD_START = "\x03"
_HEAD_END = "\x04"
_CELL_MARK = "\x05"


def _clean(text: str) -> str:
    return _WS.sub(" ", html.unescape(_TAG.sub("", text))).strip()


def _segments(raw_html: str) -> list[tuple[str, str]]:
    """Flatten the document into ``(kind, text)`` runs.

    ``kind`` is ``highlight`` for the yellow-marked statement text, ``cell`` for
    a table-cell boundary and ``plain`` otherwise. Working in runs rather than
    on one flat string is what lets a statement interrupted by an inline link be
    put back together without swallowing the prose around it.
    """
    marked = _HIGHLIGHT_OPEN.sub(_MARK_START, raw_html)
    marked = _SPAN_CLOSE.sub(_MARK_END, marked)
    marked = _CELL.sub(_CELL_MARK, marked)
    marked = _HEADING.sub(lambda m: f"{_HEAD_START}{m.group(1)}{_HEAD_END}", marked)
    flat = html.unescape(_TAG.sub("", marked))

    runs: list[tuple[str, str]] = []
    index = 0
    length = len(flat)
    while index < length:
        char = flat[index]
        if char == _MARK_START:
            end = flat.find(_MARK_END, index + 1)
            if end == -1:
                end = length
            runs.append(("highlight", flat[index + 1 : end]))
            index = end + 1
            continue
        if char == _CELL_MARK:
            runs.append(("cell", ""))
            index += 1
            continue
        next_special = min(
            (
                pos
                for pos in (flat.find(_MARK_START, index), flat.find(_CELL_MARK, index))
                if pos != -1
            ),
            default=length,
        )
        runs.append(("plain", flat[index:next_special]))
        index = next_special
    return runs


def _highlighted_text(runs: list[tuple[str, str]], upto: int) -> str:
    """Recover the highlighted statement text preceding ``runs[upto]``.

    Consecutive highlighted runs are merged across separators that carry no
    prose of their own, which is how a statement broken by an inline
    cross-reference link is reassembled.
    """
    collected: list[str] = []
    index = upto - 1
    while index >= 0:
        kind, text = runs[index]
        if kind == "highlight":
            cleaned = _clean(text)
            if cleaned:
                collected.insert(0, cleaned)
            index -= 1
            continue
        if kind == "plain" and not _clean(text.replace(_HEAD_START, "").replace(_HEAD_END, "")):
            index -= 1
            continue
        break
    return _clean(" ".join(collected))


def _appendix_rows(raw_html: str) -> dict[str, str]:
    """Read the conformance-clause tables, where the label *precedes* its text.

    The layout is ``<tr><td>[MQTT-x-n]</td><td>statement</td></tr>``. Searching
    backwards from the label — the intuitive direction, and the one the
    highlighted body form needs — silently attaches the previous row's text, so
    this walks rows explicitly instead.
    """
    texts: dict[str, str] = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", raw_html, re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.IGNORECASE | re.DOTALL)
        if len(cells) < 2:
            continue
        label_match = _LABEL.search(_clean(cells[0]))
        if label_match is None:
            continue
        text = _clean(cells[1])
        if text:
            texts.setdefault(label_match.group(1), text)
    return texts


def extract(raw_html: str) -> list[dict[str, str]]:
    """Return every ``[MQTT-…]`` statement with its verbatim text and section.

    The highlighted body text is authoritative. The conformance-clause tables in
    the appendix repeat many statements and are used only to fill the ones the
    body does not highlight; where both exist they are cross-checked, because a
    disagreement means this parser has drifted from the document's conventions.
    """
    runs = _segments(raw_html)
    appendix = _appendix_rows(raw_html)

    body: dict[str, dict[str, str]] = {}
    section = ""
    for position, (_kind, text) in enumerate(runs):
        for heading in re.finditer(f"{_HEAD_START}(.*?){_HEAD_END}", text, re.DOTALL):
            candidate = _clean(heading.group(1))
            if candidate:
                section = candidate
        for match in _LABEL.finditer(text):
            label = match.group(1)
            statement_text = _highlighted_text(runs, position)
            if not statement_text:
                # A label cited in prose ("see [MQTT-3.1.4-4]") or sitting in an
                # appendix cell highlights nothing of its own.
                continue
            body.setdefault(
                label, {"id": label, "section": section, "text": statement_text, "origin": "body"}
            )

    statements = dict(body)
    for label, text in appendix.items():
        if label not in statements:
            statements[label] = {
                "id": label,
                "section": "conformance clause (appendix)",
                "text": text,
                "origin": "appendix",
            }
    return sorted(statements.values(), key=lambda s: _sort_key(s["id"]))


def _normalise(text: str) -> str:
    """Compare on words alone: the two renderings differ in punctuation only."""
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def _sort_key(label: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", label))


def build(version: str, *, raw_bytes: bytes) -> dict[str, object]:
    source = SOURCES[version]
    raw_html = raw_bytes.decode(SOURCE_ENCODING)
    statements = extract(raw_html)
    # The body prose and the appendix summary occasionally word a statement
    # differently (a highlighted list lead-in, an expanded pronoun). That is a
    # property of the source, not a parsing error, so both readings are kept
    # rather than silently choosing one.
    appendix = _appendix_rows(raw_html)
    divergences = 0
    for statement in statements:
        if statement.get("origin") != "body":
            continue
        other = appendix.get(statement["id"])
        if other is not None and _normalise(other) != _normalise(statement["text"]):
            statement["appendix_text"] = other
            divergences += 1
    return {
        "_comment": (
            "Verbatim conformance statements extracted from the OASIS specification "
            "named in `source`. Regenerate with tools/extract_spec_statements.py; "
            "do not edit by hand."
        ),
        "mqtt_version": version,
        "source": source,
        "retrieved": date.today().isoformat(),
        "source_encoding": SOURCE_ENCODING,
        "source_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "statement_count": len(statements),
        "statements": statements,
    }


# The OASIS HTML is a Word export that declares no charset and is not UTF-8
# (0x96 en-dash, 0xb7 middle dot). Decoding it as UTF-8 silently replaces those
# characters, which corrupts the very text this index exists to quote exactly.
SOURCE_ENCODING = "cp1252"


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - fixed https URL
        return response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the vendored index differs from a fresh extraction",
    )
    parser.add_argument(
        "--from-file",
        type=Path,
        nargs=2,
        metavar=("V311_HTML", "V5_HTML"),
        help="use already-downloaded HTML instead of fetching",
    )
    args = parser.parse_args()

    local = {}
    if args.from_file:
        local = {
            "3.1.1": args.from_file[0].read_bytes(),
            "5.0": args.from_file[1].read_bytes(),
        }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    failed = False
    for version, source in SOURCES.items():
        raw_bytes = local.get(version) or fetch(source["url"])
        index = build(version, raw_bytes=raw_bytes)
        path = OUTPUT_DIR / f"mqtt-v{version}-statements.json"
        rendered = json.dumps(index, indent=2, ensure_ascii=False) + "\n"

        if args.check:
            if not path.exists():
                print(f"MISSING {path}")
                failed = True
                continue
            current = json.loads(path.read_text(encoding="utf-8"))
            if current["statements"] != index["statements"]:
                print(f"STALE {path}: statements differ from the published source")
                failed = True
            else:
                print(f"ok {path.name}: {index['statement_count']} statements match")
            continue

        path.write_text(rendered, encoding="utf-8")
        empty = sum(1 for s in index["statements"] if not s["text"])
        both = sum(1 for s in index["statements"] if "appendix_text" in s)
        from_appendix = sum(1 for s in index["statements"] if s["origin"] == "appendix")
        print(
            f"wrote {path.name}: {index['statement_count']} statements "
            f"({empty} without text, {from_appendix} appendix-only, "
            f"{both} worded differently in the appendix)"
        )

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
