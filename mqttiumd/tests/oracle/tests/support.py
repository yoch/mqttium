"""Paths shared by the tests of the oracle."""

import pathlib

ORACLE = pathlib.Path(__file__).resolve().parent.parent
ROOT = ORACLE.parent.parent
INDEX = "contracts/examples/v1/index.json"
