"""Admission decisions every tracker's intake shares (#4358 plan D5).

The label refusal rules of #4361 decision 9 and the WorkItem cancellation a
removed label or a closed issue makes. The GitHub webhook, the GitHub poll and
the port-driven tracker pass all call these, so the refusal texts and the
cancellation retry are one implementation.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.deployments import WebhookResult
from curie_api.workitems.lifecycle import WorkItemConflict

from . import workitem_dispatch
from .models import WorkItem

# The one label refusal comment per issue (#4361 decision 9), edited, never
# duplicated, like the `base:` refusal.
LABEL_REFUSAL_MARKER = "curie-factory-label-refusal"


def label_span(label: str | None) -> str:
    return f"`{label}`" if label is not None else "its original label"


def label_refusal(
    *,
    label: str | None,
    owner_label: str | None,
    other_agent: bool,
    present: Sequence[str],
) -> tuple[str, str] | None:
    """Whether a label admission is refused, as (code, comment body).

    1. ``work_item_other_agent``: the issue's WorkItem, cancelled or not,
       belongs to another agent (``other_agent``), whose run ``owner_label``
       started; its conversation and PR lineage stay there.
    2. ``label_conflict``: the issue carries two or more labels bound on the
       location (``present``, sorted). A run already admitted is not touched.
    """

    if other_agent:
        return (
            "work_item_other_agent",
            f"Curie did not start this issue under {label_span(label)}: it belongs to the "
            f"run the {label_span(owner_label)} label started, which is another agent's. Open a "
            f"new issue to run it under {label_span(label)}.",
        )
    if len(present) > 1:
        return (
            "label_conflict",
            "Curie did not start this issue: it carries "
            f"{', '.join(label_span(name) for name in present)}, which start different "
            "agents on this repository. Keep exactly one of them; Curie will pick the issue "
            "up on its next pass.",
        )
    return None


def ignored(code: str) -> WebhookResult:
    return WebhookResult(status="factory_ignored", errors=[{"code": code}])


async def cancel_work_item(session: AsyncSession, item: WorkItem) -> WebhookResult:
    """Cancel ``item``, retrying once at the version a stale read reports.

    Callers commit the session.
    """

    result = await workitem_dispatch.cancel(
        session, work_item_id=item.id, expected_version=item.version
    )
    if isinstance(result, WorkItemConflict) and result.code == "stale_version":
        if result.work_item_version is None:
            return ignored("stale_version")
        result = await workitem_dispatch.cancel(
            session,
            work_item_id=item.id,
            expected_version=result.work_item_version,
        )
    if isinstance(result, WorkItemConflict):
        return ignored(result.code)
    if result.replayed:
        return WebhookResult(status="factory_duplicate")
    status = None if result.request is None else result.request.status
    if status == "cancellation_requested":
        return WebhookResult(status="factory_cancellation_requested")
    return WebhookResult(status="factory_cancelled")
