"""Regression tests for the reproducible OASIS statement extractor."""

from __future__ import annotations

import hashlib
import io
import zipfile

from tools.extract_spec_statements import SOURCES, build, extract


def test_nested_highlight_spans_do_not_truncate_a_statement() -> None:
    source = """
    <p><span style="background:yellow">The sender MUST keep
      <span class="wrapper"><span style="background: yellow">all nested</span></span>
      text</span> <span style="color:red">[MQTT-1.2.3-4]</span>.</p>
    <table><tr><td>[MQTT-1.2.3-4]</td>
      <td>The sender MUST keep all nested text</td></tr></table>
    """

    assert extract(source) == [
        {
            "id": "MQTT-1.2.3-4",
            "section": "1.2.3",
            "text": "The sender MUST keep all nested text",
            "origin": "body",
        }
    ]


def test_appendix_label_typo_is_preserved_as_an_alias_not_a_duplicate() -> None:
    statement = "A Client MUST support an ordered, lossless stream."
    source = f"""
    <p><span style="background:yellow">{statement}</span>
      <span>[MQTT-4.2-1]</span></p>
    <table><tr><td>[MQTT-4.2.0-1]</td><td>{statement}</td></tr></table>
    """

    assert extract(source) == [
        {
            "id": "MQTT-4.2-1",
            "section": "4.2",
            "text": statement,
            "origin": "body",
            "appendix_id": "MQTT-4.2.0-1",
        }
    ]


def test_build_reads_the_named_member_and_hashes_archive_and_html() -> None:
    html = (
        '<p><span style="background:yellow">A Client MUST behave.</span> '
        "<span>[MQTT-1.1-1]</span></p>"
    ).encode("cp1252")
    buffer = io.BytesIO()
    member = SOURCES["5.0"]["archive_member"]
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, html)
    archive_bytes = buffer.getvalue()

    index = build("5.0", archive_bytes=archive_bytes)

    assert index["archive_sha256"] == hashlib.sha256(archive_bytes).hexdigest()
    assert index["source_sha256"] == hashlib.sha256(html).hexdigest()
    assert index["statement_count"] == 1
    assert index["statements"][0]["text"] == "A Client MUST behave."
