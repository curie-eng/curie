"""The worker-to-supervisor wire contract of a static-pool harness shell.

The worker (``curie_worker.sandbox.static``) and the supervisor both import
these names, so a path or field rename cannot drift between the two sides.

Two credentials cross this boundary and never substitute for each other:

* the control credential, an installation secret held by the worker and every
  supervisor, authorizes ``/_curie/supervisor/v1/*``;
* the per-claim runner bearer (``CURIE_RUNNER_TOKEN`` in the claim's boot env)
  authorizes the proxied ACI paths under ``/c/<claim>/<generation>/`` and is
  what the runner child itself enforces.
"""

from __future__ import annotations

import re

DEFAULT_PORT = 8443
CONTROL_PREFIX = "/_curie/supervisor/v1"
SHELL_PATH = f"{CONTROL_PREFIX}/shell"
CLAIMS_PATH = f"{CONTROL_PREFIX}/claims"
# Unauthenticated liveness for the orchestrator's container health check: 200
# while the shell can serve, 503 once it is quarantined and needs replacing.
HEALTH_PATH = "/_curie/supervisor/healthz"
PROXY_PREFIX = "/c"

# The staging archive is an uncompressed tar: its size on the wire bounds its
# size on disk, so no decompression ratio has to be trusted.
ARCHIVE_MEDIA_TYPE = "application/x-tar"
MANIFEST_NAME = "claim.json"
MANIFEST_VERSION = 1
BUNDLE_DIR = "bundle"
WORKSPACE_DIR = "workspace"
ATTACHMENTS_DIR = "attachments"
ARCHIVE_ROOTS = frozenset({MANIFEST_NAME, BUNDLE_DIR, WORKSPACE_DIR, ATTACHMENTS_DIR})

ATTACHMENT_MODES = frozenset({"none", "eager", "lazy"})

# Retirement modes. Both stop the child and wipe the claim; ``suspend`` is the
# approval pause, recorded separately only for the operator's audit trail.
RETIRE_MODES = frozenset({"retire", "suspend"})

# Outcomes of a claim retirement.
OUTCOME_CLEANED = "cleaned"
OUTCOME_ABSENT = "absent"
OUTCOME_QUARANTINED = "quarantined"

# Shell states reported by ``GET SHELL_PATH``.
STATE_IDLE = "idle"
STATE_STAGING = "staging"
STATE_RUNNING = "running"
STATE_STOPPING = "stopping"
STATE_QUARANTINED = "quarantined"

RUNNER_TOKEN_ENV = "CURIE_RUNNER_TOKEN"

# The claim names the worker mints (``curie-thread-<hash>-<nonce>``) are DNS
# labels; anything else never reaches a path or a directory name.
_CLAIM_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?")
MAX_GENERATION = 2**53


def valid_claim_name(name: str) -> bool:
    return _CLAIM_RE.fullmatch(name) is not None


def valid_generation(generation: int) -> bool:
    return type(generation) is int and 0 < generation < MAX_GENERATION


def claim_path(claim: str, generation: int) -> str:
    """The control path that creates or retires one claim lease."""

    return f"{CLAIMS_PATH}/{claim}/{generation}"


def proxy_base_path(claim: str, generation: int) -> str:
    """The ACI base path a claim's handle dials; the generation fences it."""

    return f"{PROXY_PREFIX}/{claim}/{generation}"
