"""Strict reading of a v1 document (DEM-014).

One file holds one JSON value. Duplicate keys, numbers with a fraction or an
exponent, the lexeme ``-0``, integers beyond 64 bits, ``NaN`` and a lone
surrogate escape are refused while parsing.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

U64_MAX = 2**64 - 1
_CANONICAL = re.compile(r"(0|[1-9][0-9]*)\Z")


@dataclass(frozen=True)
class Diagnostic:
    """One reason a document is refused."""

    code: str
    pointer: str
    message: str

    def __str__(self) -> str:
        return f"{self.code} at {self.pointer or '/'}: {self.message}"


class Refused(Exception):
    """A document cannot be read at all."""

    def __init__(self, code: str, pointer: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.diagnostic = Diagnostic(code, pointer, message)


def _members(pairs):
    members = {}
    for key, value in pairs:
        if key in members:
            raise Refused("duplicate-key", "", f"the key {key!r} appears twice")
        members[key] = value
    return members


def _integer(text):
    # Zero is written `0`; `-0` is another lexeme and readers disagree on it.
    if text == "-0" or not -(2**63) <= int(text) <= U64_MAX:
        raise Refused("number-not-integer", "", f"{text} is not a 64-bit integer literal")
    return int(text)


def _fraction(text):
    raise Refused("number-not-integer", "", f"{text} has a fraction or an exponent")


def _constant(text):
    raise Refused("json-syntax", "", f"{text} is not JSON")


def _scalars(value):
    if isinstance(value, str):
        if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise Refused("json-syntax", "", "a lone surrogate is not a Unicode scalar value")
    elif isinstance(value, list):
        for item in value:
            _scalars(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _scalars(key)
            _scalars(item)


def parse(data: bytes):
    """The JSON value held by ``data``, or ``Refused``."""
    try:
        value = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_members,
            parse_int=_integer,
            parse_float=_fraction,
            parse_constant=_constant,
        )
    except Refused:
        raise
    except (ValueError, RecursionError) as error:
        raise Refused("json-syntax", "", str(error)) from error
    _scalars(value)
    return value


def u64(text: str) -> int:
    """The value of a canonical decimal string.

    Raises ``ValueError`` with ``u64-not-canonical`` or ``u64-out-of-range``
    as its message.
    """
    if not isinstance(text, str) or not _CANONICAL.match(text):
        raise ValueError("u64-not-canonical")
    if len(text) > 20 or int(text) > U64_MAX:
        raise ValueError("u64-out-of-range")
    return int(text)
