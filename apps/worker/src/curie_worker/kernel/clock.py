from __future__ import annotations

import time

# One clock binding. Kernel modules call clock.time.time and tests patch this name.
__all__ = ["time"]
