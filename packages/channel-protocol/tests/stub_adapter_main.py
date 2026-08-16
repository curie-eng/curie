"""Run one conformance stub adapter as a standalone process.

This is FIX A's out of process proof. Started with ``subprocess.Popen`` and
configured purely through environment variables and its own HTTP surface, it
shares no memory with the conformance kit, so nothing can correlate an observed
ingress post to a stimulus except the ``delivery_id`` travelling on the wire.

Environment: ``STUB_PORT``, ``STUB_STATE``, ``STUB_BEHAVIOR`` and the optional
``STUB_SECRET`` (absent means the adapter's own egress secret is unset).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from conformance_stubs import serve_from_env  # noqa: E402

if __name__ == "__main__":
    serve_from_env()
