"""The scenarios, one module per area. Importing the package registers them."""
from importlib import import_module

import dsl

for _area in dsl.AREAS:
    import_module(f"scenarios.{_area}")
