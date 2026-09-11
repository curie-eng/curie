"""The agent-facing interaction harness (dev/test only).

``curie_test_support`` is declared ONLY in the workspace root's
``[dependency-groups] dev``, and every production image installs with
``uv sync --no-dev``, so nothing in this subpackage reaches a shipped artifact.
That posture is asserted by the exclusion suite rather than left as a reading of
the Dockerfiles -- if a future change installs the dev group into an image, a
test fails instead of the harness shipping.

Everything the public contract names is re-exported here so a caller imports
from one place:

    from curie_test_support.interaction import InteractionHarness

    with InteractionHarness() as harness:
        harness.send("scale the payments deployment to 10 replicas")
        card = next(m for m in harness.messages().messages if m.actions)
        harness.act(message=card.message_id, action=card.actions[0], actor="U0EXAMPLE1")

The same surface is scriptable over a pipe with
``python -m curie_test_support.interaction`` (NDJSON in, NDJSON out). Faults are
armed there with the ``arm_fault`` / ``disarm_fault`` verbs, which are the pipe's
form of the in-process ``inject_fault`` context manager -- a ``with`` block
cannot span two NDJSON lines, and without them an agent on the pipe could drive
only the happy path.
"""

from __future__ import annotations

from .faults import SUPPORTED_FAULTS, ArmedFault
from .harness import (
    ComposedApi,
    HarnessTimeout,
    InteractionHarness,
    JourneyEnv,
    JourneyRecorder,
    UncapturedAction,
    composed_api_server,
    disposable_migrated_database,
    gated_call,
    journey_env,
    resolve_client_factory,
)
from .results import (
    ActResult,
    ApprovalRecord,
    AuditEntry,
    AuditResult,
    CapturedAction,
    CapturedCard,
    CapturedMessage,
    MessagesResult,
    OutcomeResult,
    ResetResult,
    ResumeTurn,
    ResumeTurnsResult,
    SendResult,
    Snapshot,
)

__all__ = [
    "SUPPORTED_FAULTS",
    "ActResult",
    "ApprovalRecord",
    "ArmedFault",
    "AuditEntry",
    "AuditResult",
    "CapturedAction",
    "CapturedCard",
    "CapturedMessage",
    "ComposedApi",
    "HarnessTimeout",
    "InteractionHarness",
    "JourneyEnv",
    "JourneyRecorder",
    "MessagesResult",
    "OutcomeResult",
    "ResetResult",
    "ResumeTurn",
    "ResumeTurnsResult",
    "SendResult",
    "Snapshot",
    "UncapturedAction",
    "composed_api_server",
    "disposable_migrated_database",
    "gated_call",
    "journey_env",
    "resolve_client_factory",
]
