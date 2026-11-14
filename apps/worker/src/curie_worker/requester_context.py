"""Deliver a request's pinned requester coding instructions (ADR 0278, #5592).

Before the first turn of a started WorkItem request, the worker reads the
request's version 1 context envelope from the API for the current execution
fence, posts it to the runner behind the per-claim bearer, and relays the
runner's acknowledgment to the API. Model work starts only after the API
accepted it: every other path raises `RequesterContextRefused`, and the kernel
fails the request before the first turn with the reason visible.

The envelope carries the pinned text, so nothing here logs a body, an
envelope or an exception message that could quote one.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

import aiohttp

from .workitem_dispatch import WorkItemConflict, WorkItemDispatchClient, WorkItemRun

logger = logging.getLogger(__name__)

# A bound runner's start can still be in its verification preflight when the
# worker delivers, so the post waits up to this long for it (within budget).
DELIVERY_TIMEOUT_S = 900.0
# API refusals of the envelope read that fail the request rather than retry it.
_FAILING_READS = frozenset({"tenant_disabled", "coding_instructions_unavailable"})


class RunnerDelivery(Protocol):
    async def deliver_requester_context(
        self, base_url: str, envelope: bytes, *, token: str | None, timeout_s: float
    ) -> tuple[int, dict[str, object] | None]: ...


class RequesterContextRefused(Exception):
    """The request must fail before model work, for ``reason``."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    @property
    def detail(self) -> str:
        return (
            "The requester's coding instructions could not be applied "
            f"({self.reason}); no model work ran."
        )


async def _relay(
    work_items: WorkItemDispatchClient,
    run: WorkItemRun,
    epoch: int,
    acknowledgment: dict[str, Any] | None,
    reason: str | None,
) -> str:
    try:
        return await work_items.acknowledge_requester_context(
            run.request_id, runtime_epoch=epoch, acknowledgment=acknowledgment, reason=reason
        )
    except WorkItemConflict as conflict:
        raise RequesterContextRefused(f"acknowledgment_{conflict.code}") from None


async def deliver(
    *,
    work_items: WorkItemDispatchClient,
    runner: RunnerDelivery,
    run: WorkItemRun,
    base_url: str,
    token: str | None,
    remaining_s: float | None,
) -> str:
    """Deliver and acknowledge; the accepted evidence (``applied`` or ``none``)."""

    epoch = run.runtime_epoch
    if epoch is None:
        raise RequesterContextRefused("not_started")
    try:
        envelope = await work_items.requester_context(run.request_id, runtime_epoch=epoch)
    except WorkItemConflict as conflict:
        if conflict.code not in _FAILING_READS:
            raise
        await _relay(work_items, run, epoch, None, conflict.code)
        raise RequesterContextRefused(conflict.code) from None
    raw = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timeout_s = (
        DELIVERY_TIMEOUT_S
        if remaining_s is None
        else max(1.0, min(remaining_s, DELIVERY_TIMEOUT_S))
    )
    try:
        status, body = await runner.deliver_requester_context(
            base_url, raw, token=token, timeout_s=timeout_s
        )
    except (aiohttp.ClientError, TimeoutError, OSError) as exc:
        logger.warning(
            "requester context delivery failed for %s error_class=%s",
            run.request_id,
            type(exc).__name__,
        )
        status, body = 0, None
    acknowledgment = body.get("acknowledgment") if status == 200 and body is not None else None
    if not isinstance(acknowledgment, dict):
        # An older runner without the route (404), a runner that expects no
        # envelope (409), an error or no answer: it never acknowledged.
        logger.warning("requester context unacknowledged for %s status=%d", run.request_id, status)
        await _relay(work_items, run, epoch, None, "runner_unacknowledged")
        raise RequesterContextRefused("runner_unacknowledged")
    reason = body.get("reason") if body is not None else None
    failed = acknowledgment.get("outcome") == "failed"
    refusal = (
        reason if isinstance(reason, str) and failed else ("runner_refused" if failed else None)
    )
    evidence = await _relay(work_items, run, epoch, acknowledgment, refusal)
    if failed or evidence == "failed":
        raise RequesterContextRefused(refusal or "runner_refused")
    logger.info("requester context accepted for %s evidence=%s", run.request_id, evidence)
    return evidence
