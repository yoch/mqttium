#!/usr/bin/env python3
"""Rebuild the MQTT conformance index from reproducible OASIS archives.

The numbered statements are read from the highlighted normative prose in the
official HTML.  The conformance appendix is parsed independently: it fills the
few labels omitted from the body and exposes source disagreements for review.

Usage::

    python tools/extract_spec_statements.py
    python tools/extract_spec_statements.py --check
    python tools/extract_spec_statements.py --from-archive V311_ZIP V5_ZIP

Only the generated statement indexes are vendored; the specifications and
their archives remain at OASIS.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import urllib.request
import zipfile
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "spec"
SOURCE_ENCODING = "cp1252"  # Alias of the declared windows-1252 charset.

SOURCES: dict[str, dict[str, str]] = {
    "3.1.1": {
        "title": (
            "MQTT Version 3.1.1 Plus Errata 01, OASIS Standard Incorporating Approved Errata 01"
        ),
        "url": (
            "https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/"
            "mqtt-v3.1.1-errata01-os-complete.html"
        ),
        "archive_url": (
            "https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os.zip"
        ),
        "archive_member": "mqtt-v3.1.1-errata01-os-complete.html",
        "published": "2015-12-10",
    },
    "5.0": {
        "title": "MQTT Version 5.0, OASIS Standard",
        "url": "https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html",
        "archive_url": "https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.zip",
        "archive_member": "mqtt-v5.0-os.html",
        "published": "2019-03-07",
    },
}

_LABEL = re.compile(r"\[(MQTT-\d+(?:\.\d+)*-\d+)\]")
_YELLOW = re.compile(r"background(?:-color)?\s*:\s*yellow", re.IGNORECASE)
_WS = re.compile(r"\s+")


def _clean(text: str) -> str:
    return _WS.sub(" ", text).strip()


class _BodyParser(HTMLParser):
    """Emit body text runs while respecting nested highlighted spans.

    Regex substitution is unsafe for the OASIS Word export: highlighted text
    can contain nested spans, so the first closing ``</span>`` is not
    necessarily the end of the statement.  Tracking the actual element stack
    prevents short fragments such as ``(0x00)`` from replacing a full rule.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.runs: list[tuple[str, str]] = []
        self._span_highlights: list[bool] = []
        self._highlight_depth = 0

    def _append(self, kind: str, text: str = "") -> None:
        if text and self.runs and self.runs[-1][0] == kind:
            previous_kind, previous_text = self.runs[-1]
            self.runs[-1] = (previous_kind, previous_text + text)
        else:
            self.runs.append((kind, text))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "span":
            style = dict(attrs).get("style") or ""
            highlighted = _YELLOW.search(style) is not None
            self._span_highlights.append(highlighted)
            self._highlight_depth += int(highlighted)
        if tag in {"td", "th"}:
            self._append("cell")

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._span_highlights:
            self._highlight_depth -= int(self._span_highlights.pop())
        if tag in {"td", "th"}:
            self._append("cell")

    def handle_data(self, data: str) -> None:
        self._append("highlight" if self._highlight_depth else "plain", data)


class _AppendixParser(HTMLParser):
    """Parse conformance-table rows without depending on Word's tag layout."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            self._row.append(_clean("".join(self._cell)))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None
            self._cell = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def _highlighted_text(runs: list[tuple[str, str]], upto: int) -> str:
    """Reassemble highlighted text immediately preceding a statement label."""
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
        if kind == "plain" and not _clean(text):
            index -= 1
            continue
        break
    return _clean(" ".join(collected))


def _body_statements(raw_html: str) -> dict[str, str]:
    parser = _BodyParser()
    parser.feed(raw_html)
    statements: dict[str, str] = {}
    for position, (kind, text) in enumerate(parser.runs):
        if kind != "plain":
            continue
        for match in _LABEL.finditer(text):
            statement = _highlighted_text(parser.runs, position)
            if statement:
                statements.setdefault(match.group(1), statement)
    return statements


def _appendix_rows(raw_html: str) -> dict[str, str]:
    parser = _AppendixParser()
    parser.feed(raw_html)
    statements: dict[str, str] = {}
    for cells in parser.rows:
        if len(cells) < 2:
            continue
        match = _LABEL.search(cells[0])
        if match is not None and cells[1]:
            statements.setdefault(match.group(1), cells[1])
    return statements


def _section(label: str) -> str:
    """Return the stable numeric section encoded in the statement identifier."""
    return label.removeprefix("MQTT-").rsplit("-", 1)[0]


def _normalise(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def _sort_key(label: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", label))


def _without_trailing_section_zero(label: str) -> str:
    """Normalise the one known appendix-only label typo without hiding it."""
    return re.sub(r"\.0(?=-\d+$)", "", label)


def extract(raw_html: str) -> list[dict[str, str]]:
    """Return the numbered statements and independently audited renderings."""
    body = _body_statements(raw_html)
    appendix = _appendix_rows(raw_html)
    aliases: dict[str, str] = {}
    for appendix_label, row_text in list(appendix.items()):
        if appendix_label in body:
            continue
        matching_body_label = _without_trailing_section_zero(appendix_label)
        body_text = body.get(matching_body_label)
        if body_text is not None and _normalise(body_text) == _normalise(row_text):
            # MQTT 5's body labels the network-connection rule MQTT-4.2-1 while
            # its appendix says MQTT-4.2.0-1. Preserve the discrepancy as
            # provenance instead of inflating the statement count with a
            # duplicate or silently rewriting either source.
            aliases[matching_body_label] = appendix_label
            del appendix[appendix_label]

    statements: list[dict[str, str]] = []
    for label in sorted(body.keys() | appendix.keys(), key=_sort_key):
        body_text = body.get(label)
        appendix_text = appendix.get(label)
        item = {
            "id": label,
            "section": _section(label),
            "text": body_text or appendix_text or "",
            "origin": "body" if body_text else "appendix",
        }
        if (
            body_text is not None
            and appendix_text is not None
            and _normalise(body_text) != _normalise(appendix_text)
        ):
            item["appendix_text"] = appendix_text
        if label in aliases:
            item["appendix_id"] = aliases[label]
        statements.append(item)
    return statements


def _html_from_archive(version: str, archive_bytes: bytes) -> bytes:
    member = SOURCES[version]["archive_member"]
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        try:
            return archive.read(member)
        except KeyError as exc:
            raise ValueError(f"OASIS archive does not contain {member!r}") from exc


def build(version: str, *, archive_bytes: bytes) -> dict[str, object]:
    source = SOURCES[version]
    raw_bytes = _html_from_archive(version, archive_bytes)
    statements = extract(raw_bytes.decode(SOURCE_ENCODING))
    return {
        "_comment": (
            "Numbered conformance statements extracted from the OASIS specification "
            "named in `source`. Regenerate with tools/extract_spec_statements.py; "
            "do not edit by hand."
        ),
        "mqtt_version": version,
        "source": source,
        "retrieved": date.today().isoformat(),
        "source_encoding": SOURCE_ENCODING,
        "archive_sha256": hashlib.sha256(archive_bytes).hexdigest(),
        "source_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "statement_count": len(statements),
        "statements": statements,
    }


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - fixed https URL
        return response.read()


def _stable_content(index: dict[str, Any]) -> dict[str, Any]:
    stable = dict(index)
    stable.pop("retrieved", None)
    return stable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if statements or reproducible provenance differ",
    )
    parser.add_argument(
        "--from-archive",
        type=Path,
        nargs=2,
        metavar=("V311_ZIP", "V5_ZIP"),
        help="use already-downloaded official ZIP archives instead of fetching",
    )
    args = parser.parse_args()

    local: dict[str, bytes] = {}
    if args.from_archive:
        local = {
            "3.1.1": args.from_archive[0].read_bytes(),
            "5.0": args.from_archive[1].read_bytes(),
        }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    failed = False
    for version, source in SOURCES.items():
        archive_bytes = local.get(version) or fetch(source["archive_url"])
        index = build(version, archive_bytes=archive_bytes)
        path = OUTPUT_DIR / f"mqtt-v{version}-statements.json"
        rendered = json.dumps(index, indent=2, ensure_ascii=False) + "\n"

        if args.check:
            if not path.exists():
                print(f"MISSING {path}")
                failed = True
                continue
            current = json.loads(path.read_text(encoding="utf-8"))
            if _stable_content(current) != _stable_content(index):
                print(f"STALE {path}: content or provenance differs from OASIS")
                failed = True
            else:
                print(f"ok {path.name}: {index['statement_count']} statements match")
            continue

        path.write_text(rendered, encoding="utf-8")
        items = index["statements"]
        assert isinstance(items, list)
        appendix_only = sum(item["origin"] == "appendix" for item in items)
        divergences = sum("appendix_text" in item for item in items)
        print(
            f"wrote {path.name}: {index['statement_count']} statements "
            f"({appendix_only} appendix-only, {divergences} source divergences)"
        )

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
