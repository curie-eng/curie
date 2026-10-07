"""Bodies of the capability probe and action execution routes.

@spec ACTION-EXECUTOR-1: every body here forbids unknown keys, so no route
accepts a tool name or arguments for execution; a body naming either is a 422
and moves nothing. @spec ACTION-EXECUTOR-18: every transition presents the
fence the claim returned (``lease_owner`` and ``attempt``).

No response here carries an envelope, a state, a version or a result: an
execution reads back as identity, state, fence and codes only.
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# The connector name grammar of ``plugin_format.connectors`` (an RFC 1123
# label), and the only digest form a pinned connector renders at. Public because
# the action completion (``schemas/actions.py``) stores the same pair.
CONNECTOR_PATTERN = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
CONNECTOR_MAX_LENGTH = 63
DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProbeCreate(_Closed):
    """@spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-13: exactly these three keys."""

    agent_id: uuid.UUID
    connector: str = Field(min_length=1, max_length=CONNECTOR_MAX_LENGTH, pattern=CONNECTOR_PATTERN)
    digest: str = Field(pattern=DIGEST_PATTERN)


class ExecutionCreated(BaseModel):
    """A created or adopted execution: its identity and state, nothing else."""

    execution_id: uuid.UUID
    state: str


class ExecutionClaim(_Closed):
    """@spec ACTION-EXECUTOR-17: who claims, and for how long the lease holds."""

    lease_owner: str = Field(min_length=1, max_length=200)
    lease_seconds: int = Field(ge=1, le=3600)


class ExecutionFence(_Closed):
    """@spec ACTION-EXECUTOR-17: the fence every later transition presents."""

    lease_owner: str = Field(min_length=1, max_length=200)
    attempt: int = Field(ge=0)


class ExecutionObservation(ExecutionFence):
    """@spec ACTION-EXECUTOR-15: the version ``observe_version`` reported now.

    Absent, empty or malformed is accepted here and answered as a conflict by
    the route, never as a 422: the spec refuses the restore in that case, and a
    rejected request would leave the execution waiting instead.
    """

    version: str | None = Field(default=None, max_length=1024)


class ExecutionOutcome(ExecutionFence):
    """@spec ACTION-EXECUTOR-18 @spec ACTION-EXECUTOR-20: the terminal report.

    ``state`` is one of ``refused``, ``confirmed``, ``failed`` or
    ``indeterminate``; ``code`` is checked against its stage by the route.
    ``advertised`` is a probe's report only: the verbs whose ``tools/list``
    entry met ACTION-EXECUTOR-13's rule.
    """

    state: str = Field(max_length=32)
    code: str | None = Field(default=None, max_length=256)
    advertised: list[str] | None = Field(default=None, max_length=256)


class ExecutionOut(BaseModel):
    """One execution as the worker and the receipt read it.

    @spec ACTION-EXECUTOR-18. Deliberately without ``outcome``,
    ``arguments_sha256`` or ``forward_arguments``: the read names what ran and
    how it ended, never a version, an argument or a state.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: str
    state: str
    agent_id: uuid.UUID
    connector: str
    tool: str | None
    subject_action_id: uuid.UUID | None
    requested_by: str | None
    attempt: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    refusal_code: str | None
    failure_code: str | None
    dispatched_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
