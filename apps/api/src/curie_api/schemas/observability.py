from typing import Any

from pydantic import BaseModel


class ObservationNode(BaseModel):
    """One node in the reconstructed observation tree (Runs view)."""

    id: str
    type: str
    name: str | None = None
    startTime: str | None = None  # noqa: N815 (Langfuse wire field name)
    model: str | None = None
    usageDetails: dict[str, Any] | None = None  # noqa: N815
    toolName: str | None = None  # noqa: N815 (which tool an execute_tool span ran)
    children: list["ObservationNode"] = []


class TraceTree(BaseModel):
    """A trace plus its reconstructed observation tree."""

    trace: dict[str, Any]
    tree: list[ObservationNode]
    # The runner's sandbox id (curie.sandbox_id), hoisted out of the trace/
    # observation resource attributes; None when the trace predates the attr.
    sandbox_id: str | None = None
    # The resolved approval-gate decision (approved/rejected/expired) this turn
    # resumed from (ADR-0076 Stone 3, #889), hoisted out of the trace/
    # observation attributes; None for a turn that resumed no approval.
    approval_decision: str | None = None


class MetricsSummary(BaseModel):
    """Scalar totals for the Metrics tab stat row over a time window."""

    start: str
    end: str
    runs: int
    latency_p95_ms: float
    tokens: int
    cost_usd: float
    # False when work happened (tokens > 0) but Langfuse priced it to exactly 0 --
    # a missing model price row, not a genuinely free run (#547). Additive and
    # defaulting True, so `cost_usd` stays a non-nullable float for existing
    # clients; consumers render "unknown" rather than a misleading $0.00.
    cost_known: bool = True
    error_rate: float


class MetricPoint(BaseModel):
    ts: str
    value: float


class MetricSeries(BaseModel):
    """One metric as a time series for the Metrics tab charts."""

    metric: str
    granularity: str
    start: str
    end: str
    points: list[MetricPoint]


class PodLogs(BaseModel):
    """Runner-pod logs for the per-run runner-logs affordance."""

    namespace: str
    pod: str
    container: str | None
    logs: str


class RunnerPods(BaseModel):
    """The runner sandbox pods in a namespace (populates the Logs pod dropdown)."""

    namespace: str
    pods: list[str]
