"""Bodies of the capability probe and action execution routes.

@spec ACTION-EXECUTOR-1: every body here forbids unknown keys, so no route
accepts a tool name or arguments for execution; a body naming either is a 422
and moves nothing. @spec ACTION-EXECUTOR-18: every transition presents the
fence the claim returned (``lease_owner`` and ``attempt``).

No response here carries an envelope, a state, a version or a result: an
execution reads back as identity, state, fence and codes only.
"""

import json
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..action_execution_codes import SAMPLE_KINDS

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

    @spec ACTION-EXECUTOR-18. Deliberately without ``outcome`` or
    ``forward_arguments``: the read names what ran and how it ended, never a
    version, an argument or a state.

    @spec ACTION-EXECUTOR-14 @spec ACTION-EXECUTOR-7. Two non-secret digests
    the worker checks before dispatch: ``connector_digest``, the image the
    call must run against, and ``arguments_sha256``, the ruling's digest over
    the restore's canonical ``{target, prior_state}`` (null on a probe). A
    digest names neither the arguments nor the state it covers.
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
    connector_digest: str
    arguments_sha256: str | None


class ExecutionArguments(BaseModel):
    """A claimed forward execution's bound call, read by its holder only.

    @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-19: the worker recomputes
    ``arguments_sha256`` over the text it sends, so it reads the tool and the
    arguments the authority bound under its fence before dispatch. Answered
    only by ``POST /action-executions/{id}/arguments`` (internal worker token);
    ``ExecutionOut`` keeps carrying no argument.
    """

    tool: str
    arguments: dict[str, Any]


class ReadArguments(BaseModel):
    """A claimed read execution's bound call and pointer, read by its holder only.

    @spec AUTOMATED-REMEDIATION-12: the read is the declaration's, never a
    caller's. Answered only by ``POST /action-executions/{id}/arguments``.
    @spec AUTOMATED-REMEDIATION-18 (executor amendment E3): ``pointer`` is null
    only for an observe-only execution (``observe_version`` of the target).
    """

    tool: str
    arguments: dict[str, Any]
    pointer: str | None


# remediation-predicate.json ``value_max_chars``: a sample's compact JSON text.
SAMPLE_VALUE_MAX_CHARS = 256


class ExecutionSample(ExecutionFence):
    """@spec AUTOMATED-REMEDIATION-12: the fence plus exactly ``sample`` and ``value``.

    remediation-predicate.json ``sample_report``: the sample kind the runner
    answered and, for ``value``, the pointed JSON scalar (at most
    ``SAMPLE_VALUE_MAX_CHARS`` characters of compact JSON); null otherwise.
    ``skipped`` is the API's own record, never a report. The API never receives
    more than the scalar.
    """

    sample: str = Field(max_length=32)
    value: Any

    @model_validator(mode="after")
    def _the_frozen_report(self) -> "ExecutionSample":
        if self.sample not in SAMPLE_KINDS:
            raise ValueError("sample is one of the frozen sample kinds")
        if self.sample != "value":
            if self.value is not None:
                raise ValueError("only a value sample carries a value")
            return self
        if isinstance(self.value, (dict, list)):
            raise ValueError("a sample value is a JSON scalar")
        try:
            text = json.dumps(
                self.value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("a sample value is a JSON scalar") from exc
        if len(text) > SAMPLE_VALUE_MAX_CHARS:
            raise ValueError("a sample value is at most 256 characters of JSON")
        return self
