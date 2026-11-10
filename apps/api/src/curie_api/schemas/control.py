from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from .observability import MetricPoint


class BudgetConfig(BaseModel):
    """Per-agent budget (L1). Field names match the ACI CURIE_BUDGET so the
    worker passes them straight through; null means platform defaults."""

    model_config = ConfigDict(from_attributes=True)

    max_usd_per_day: Annotated[float, Field(gt=0)] | None = None
    max_output_tokens_per_run: Annotated[int, Field(gt=0)] | None = None


class KillState(BaseModel):
    """Whether an agent is currently killed (kill switch, L1)."""

    killed: bool


class ThreadResetState(BaseModel):
    """Whether a thread has a pending forced-sandbox-release request (#713).

    ``route_existed`` is what the worker found when it drained the reset
    (#3699). None while the reset is pending, and when no outcome is recorded
    (the record expired, or the worker predates this field). False when the
    drained reset matched no route, so nothing was released -- usually a key
    built by hand that left out a named bot's identity segment. True when a
    route existed and was released."""

    requested: bool
    route_existed: bool | None = None


class CostReport(BaseModel):
    """Daily spend series + total for an agent (L1 Cost view)."""

    start: str
    end: str
    total_usd: float
    # False when tokens were spent in the window but Langfuse priced them to 0
    # (a missing model price row, not free usage) -- see MetricsSummary.cost_known
    # (#547). Additive, defaults True; total_usd stays a non-nullable float.
    cost_known: bool = True
    points: list[MetricPoint]
