"""Make the sibling conformance stub modules importable by name.

pytest's importlib import mode does not put the test directory on ``sys.path``,
so ``import conformance_stubs`` from a test module fails without this. Same
idiom, and the same reason, as ``tests/soak/conftest.py``.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
