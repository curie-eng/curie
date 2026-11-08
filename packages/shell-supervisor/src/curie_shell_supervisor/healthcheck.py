"""Container health check: exit 0 while the shell can serve claims, 1 otherwise.

A quarantined shell fails this check, so the orchestrator replaces the task and
the worker sees a new incarnation it can lease again.
"""

from __future__ import annotations

import os
import ssl
import sys
import urllib.error
import urllib.request

from .protocol import DEFAULT_PORT, HEALTH_PATH


def main() -> int:
    port = int(os.environ.get("CURIE_SUPERVISOR_PORT", DEFAULT_PORT))
    # Loopback only, so the certificate's name cannot match; the check reads
    # no credential and the answer carries none.
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed loopback https URL
            f"https://127.0.0.1:{port}{HEALTH_PATH}", timeout=2.0, context=context
        ) as response:
            return 0 if response.status == 200 else 1
    except (urllib.error.URLError, OSError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
