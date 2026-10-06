import uuid
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class IssueReadContextMint(BaseModel):
    """Trusted worker identity for one factory execution's issue read (ADR 0187)."""

    model_config = ConfigDict(extra="forbid")

    execution_request_id: uuid.UUID


class IssueReadContext(BaseModel):
    """The execution scoped capability and the one issue it names."""

    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    repo_full_name: str
    issue_number: int
    capability: str


class IssueReadRequest(BaseModel):
    """The issue the sandbox asks for. Anything but the capability's is refused."""

    model_config = ConfigDict(extra="forbid")

    repo_full_name: str = Field(min_length=3, max_length=512)
    issue_number: int = Field(gt=0)


class IssueReadComment(BaseModel):
    author: str | None
    created_at: str | None
    body: str


class IssueReadResult(BaseModel):
    """The issue verbatim. The platform parses and stores none of it."""

    repo_full_name: str
    issue_number: int
    title: str
    body: str
    state: str | None
    author: str | None
    comments: list[IssueReadComment]
    comments_truncated: bool


WorkItemOutcomeState = Literal[
    "queued",
    "waiting",
    "running",
    "cancellation_requested",
    "cancelled",
    "expired",
    "failed",
    "awaiting_approval",
    "publishing",
    "published",
    "completed_unpublished",
]
WorkItemCiState = Literal["passing", "failing", "pending", "none", "unavailable", "not_applicable"]


class WorkItemRequestOut(BaseModel):
    """Operator view of one ExecutionRequest (#2577). Built from an explicit
    allowlist dict, never from an ORM row, so no runtime-owner field leaks."""

    sequence: int
    status: str
    created_at: datetime
    wait_deadline: datetime | None
    started_at: datetime | None
    execution_deadline: datetime | None
    terminal_at: datetime | None
    terminal_cause: str | None
    termination_observation: str | None
    capacity_deferrals: int
    last_deferral_reason: str | None


class WorkItemPrOut(BaseModel):
    number: int
    url: str
    status: str


class WorkItemPublicationOut(BaseModel):
    status: str
    revision_number: int | None
    approval_status: str | None


class WorkItemCorrectnessOut(BaseModel):
    """The platform never asserts correctness; the bundle owns it (ADR 0162)."""

    asserted: Literal[False] = False
    owner: Literal["bundle"] = "bundle"


class WorkItemCiOut(BaseModel):
    """A live, unpersisted CI observation of the published head."""

    state: WorkItemCiState
    reason: str | None = None
    head_sha: str | None = None
    observed_at: datetime | None = None


class WorkItemUsageRole(BaseModel):
    """Tokens and estimate of one role (#3223). The estimate covers priced rows only."""

    tokens: int = 0
    estimated_cost_usd: Decimal | None = None
    cost_complete: bool = True


class WorkItemUsageModel(BaseModel):
    model: str
    role: Literal["implementer", "reviewer"]
    input_tokens: int = 0
    cached_input_tokens: int = 0
    cache_write_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: Decimal | None = None


class WorkItemUsageRequest(BaseModel):
    request_id: uuid.UUID
    tokens: int = 0
    estimated_cost_usd: Decimal | None = None


class WorkItemUsagePriceSource(BaseModel):
    source: str
    as_of: datetime


class WorkItemUsageOut(BaseModel):
    """Token usage and estimated cost summed over every request of a WorkItem (#3223).

    ``cost_complete`` is false when any reported model had no price, and then
    ``estimated_cost_usd`` covers only the priced models. It is also false when
    ``requests_without_usage`` (started requests with no usage report) is > 0.
    """

    work_item_id: uuid.UUID
    total_tokens: int
    estimated_cost_usd: Decimal | None
    cost_complete: bool
    requests_without_usage: int = 0
    roles: dict[str, WorkItemUsageRole]
    models: list[WorkItemUsageModel]
    requests: list[WorkItemUsageRequest]
    price_sources: list[WorkItemUsagePriceSource]
    pr_number: int | None = None
    pr_url: str | None = None


WorkItemStageState = Literal["done", "current", "redo", "blocked", "pending"]


class WorkItemStageOut(BaseModel):
    """One stage of the factory progress strip, as the status card shows it."""

    id: str
    label: str
    state: WorkItemStageState
    round_label: str | None


class WorkItemProgressOut(BaseModel):
    """The latest execution's progress, derived by ``factory_progress.phase_view``."""

    current: str | None
    note: str | None
    stages: list[WorkItemStageOut]


class WorkItemTrackerOut(BaseModel):
    """The tracker issue that keys the WorkItem (ADR 0197).

    ``scope_id`` and ``issue_id`` are the tracker's immutable ids as text;
    ``display_key`` (a Jira key) is for display only. ``url`` is the tracker's
    own link to the issue.
    """

    kind: str
    host: str
    scope_id: str
    issue_id: str
    display_key: str | None
    url: str


class WorkItemRepositoryOut(BaseModel):
    """The repository frozen on the WorkItem at admission; ``path`` is display."""

    code_host_kind: str
    host: str
    project_id: str
    path: str


class WorkItemOutcomeOut(BaseModel):
    id: uuid.UUID
    agent_id: uuid.UUID
    tracker: WorkItemTrackerOut
    repository: WorkItemRepositoryOut
    cancelled_at: datetime | None
    created_at: datetime
    updated_at: datetime
    state: WorkItemOutcomeState
    actionable_cause: str
    objective: str | None
    objective_truncated: bool
    requester: str | None
    pr: WorkItemPrOut | None
    publication: WorkItemPublicationOut | None
    correctness: WorkItemCorrectnessOut
    # Null on the list route: CI is observed live on the detail route only.
    ci: WorkItemCiOut | None
    requests: list[WorkItemRequestOut]
    # ``derive_outcome`` stays pure and leaves these null; ``_views`` fills
    # them from the factory status comment rows and phase reports.
    title: str | None = None
    progress: WorkItemProgressOut | None = None


class WorkItemOutcomeList(BaseModel):
    items: list[WorkItemOutcomeOut]
    limit: int
    truncated: bool
