from pathlib import Path

path = Path("benchmarks/memory_profile.py")
content = path.read_text()
before = "from dataclasses import dataclass\nfrom pathlib import Path\nfrom typing import Any, Callable\n"
after = "from collections.abc import Callable\nfrom dataclasses import dataclass\nfrom pathlib import Path\nfrom typing import Any\n"
if before not in content:
    raise RuntimeError("Expected memory_profile import block was not found")
path.write_text(content.replace(before, after, 1))
