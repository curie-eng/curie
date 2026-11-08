from __future__ import annotations

import sys
from pathlib import Path

# The harness modules live beside this directory and import each other by name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
