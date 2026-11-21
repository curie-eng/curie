from __future__ import annotations

import sys
from pathlib import Path

# The driver lives beside this directory and is imported by name.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
