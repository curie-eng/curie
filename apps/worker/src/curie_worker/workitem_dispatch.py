"""Work-item dispatch client, event-id parsing, and termination observation.

The kernel recognizes the work-item event ids on the runs stream (grammar in
``channel_protocol.work_item_events``). SQL in the API owns wait
and ownership; this module is the worker-side HTTP seam and the in-process
run record keyed by request id.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
from channel_protocol.work_item_events import (
    parse_work_item_event_id as _parse_shared_event_id,
)

from .api_retry import DEFAULT_BUDGET_S, post_with_retry
from .binding import FactoryContext, FactoryWake

logger = logging.getLogger(__name__)

_HEARTBEAT_TRANSPORT_FAILURES = 3
_MIN_HEARTBEAT_INTERVAL_S = 1.0
_ACQUIRE_RENEW_INTERVAL_S = 20.0
_renew_sleep = asyncio.sleep


class WorkItemConflict(Exception):
    """HTTP 409: the API refused this dispatch verb as a decision, not a fault."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class WorkItemStartRefused(WorkItemConflict):
    """``start`` was refused; the sandbox is claimed but no turn should open."""


class WorkItemTransportError(Exception):
    """5xx or unreachable API; the wake must stay pending for retry."""


@dataclass(frozen=True)
class WorkItemEvent:
    """Parsed work-item wake identity."""

    request_id: uuid.UUID
    kind: Literal["execute", "terminate", "ci"]
    # The wake generation for ``execute``; the CI fix round for ``ci``.
    generation: int | None = None

    @property
    def is_ci_fix(self) -> bool:
        return self.kind == "ci"

    @property
    def ci_fix_round(self) -> int:
        """The CI fix round of a ``ci`` event; any other event has none."""

        if self.kind != "ci" or self.generation is None:
            raise ValueError(f"work-item {self.kind} event carries no CI fix round")
        return self.generation


@dataclass(frozen=True)
class WorkItemAcquireGrant:
    generation: int
    work_item_id: uuid.UUID
    conversation_id: str
    wait_deadline: datetime
    repository_path: str
    context: FactoryContext

    @property
    def agent_id(self) -> uuid.UUID:
        return self.context.agent_id


@dataclass(frozen=True)
class WorkItemStartGrant:
    runtime_epoch: int
    execution_deadline: datetime
    remaining_s: float
    heartbeat_interval_s: float


@dataclass(frozen=True)
class WorkItemRunning:
    lease_held_to_deadline: bool
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    agent_id: uuid.UUID
    generation: int
    conversation_id: str
    wait_deadline: datetime
    repository_path: str
    runtime_epoch: int
    execution_deadline: datetime
    context: FactoryContext | None
    refusal: str | None


@dataclass(frozen=True)
class WorkItemHeartbeat:
    status: str
    terminal_cause: str | None
    work_item_cancelled: bool


@dataclass(frozen=True)
class WorkItemRequestView:
    status: str
    runtime_epoch: int
    runtime_claim_name: str | None
    runtime_sandbox_name: str | None


@dataclass(frozen=True)
class WorkItemRuntimeOwner:
    request_id: uuid.UUID
    runtime_owner: str
    runtime_epoch: int


@dataclass(frozen=True)
class TerminationObservation:
    """Observed absence of the named claims and sandboxes."""

    claims: tuple[str, ...]
    sandboxes: tuple[str, ...]
    observed_at: datetime
    observer: str

    def render(self) -> str:
        """Single-line machine observation for ``record_termination``."""

        claims = ",".join(self.claims)
        sandboxes = ",".join(self.sandboxes)
        absent_at = self.observed_at.astimezone(UTC).isoformat()
        return (
            f"claims={claims} sandboxes={sandboxes} "
            f"absent_at={absent_at} observer={self.observer}"
        )


def parse_work_item_event_id(event_id: str) -> WorkItemEvent | None:
    """Parse execute, CI-continuation, or terminate work-item event ids.

    Returns None for any other namespace. A ``ci`` id is a continuation turn of
    the same running request; ``generation`` carries its fix round, which lies
    in the shared CI round range. The grammar lives in
    ``channel_protocol.work_item_events``.
    """

    parsed = _parse_shared_event_id(event_id)
    if parsed is None:
        return None
    return WorkItemEvent(
        request_id=parsed.request_id, kind=parsed.kind, generation=parsed.number
    )


def _conflict_code(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "conflict"
    if not isinstance(body, dict):
        return "conflict"
    detail = body.get("detail")
    if isinstance(detail, dict):
        nested = detail.get("code")
        if isinstance(nested, str) and nested:
            return nested
    return "conflict"


_FACTORY_AUTHORITY_REFUSALS = frozenset(
    {
        "factory_agent_missing",
        "factory_agent_mismatch",
        "factory_tenant_mismatch",
        "factory_binding_missing",
        "factory_binding_ambiguous",
        "factory_binding_changed",
        "factory_surface_mismatch",
    }
)


def _required_string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("required nonempty string missing")
    return value


def _required_positive_int(value: object) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("required positive integer missing")
    return value


def _parse_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise WorkItemTransportError("work-item dispatch returned an unusable timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class WorkItemDispatchClient:
    """Internal-worker-token client for ``/v1/internal/work-items``."""

    def __init__(
        self,
        *,
        api_base_url: str,
        worker_token: str,
        client: httpx.AsyncClient,
    ) -> None:
        if not worker_token:
            raise ValueError("work item dispatch requires internal worker auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client

    async def acquire(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        wake: FactoryWake,
        context: FactoryContext | None,
    ) -> WorkItemAcquireGrant:
        body = await self._post(
            f"/v1/internal/work-items/requests/{request_id}/acquire",
            {
                "owner": owner,
                "generation": generation,
                "wake": wake.model_dump(mode="json"),
                "context": context.model_dump(mode="json") if context is not None else None,
            },
        )
        try:
            return WorkItemAcquireGrant(
                generation=_required_positive_int(body["generation"]),
                work_item_id=uuid.UUID(str(body["work_item_id"])),
                conversation_id=_required_string(body["conversation_id"]),
                wait_deadline=_parse_datetime(body["wait_deadline"]),
                repository_path=_required_string(body["repository_path"]),
                context=FactoryContext.model_validate(body["context"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item acquire returned an unusable body"
            ) from exc

    async def defer(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        reason: str,
        capacity: bool,
        start_failure_detail: str | None,
        budget_s: float = DEFAULT_BUDGET_S,
    ) -> str | None:
        """Defer the request; return its terminal cause if the deferral ended it.

        A non-capacity deferral past the start limit settles the request
        ``failed`` instead of rescheduling it (#4170). ``start_failure_detail``
        is the safe diagnostic of a pre-start workspace preparation fault, sent
        as JSON null when there is none; the API stores it only on a
        non-capacity ``not_started:`` deferral (#4930).
        """

        body = await self._post_settlement(
            f"/v1/internal/work-items/requests/{request_id}/defer",
            {
                "owner": owner,
                "generation": generation,
                "reason": reason,
                "capacity": capacity,
                "start_failure_detail": start_failure_detail,
            },
            budget_s=budget_s,
        )
        cause = body.get("terminal_cause")
        if cause is None:
            return None
        if not isinstance(cause, str) or not cause:
            raise WorkItemTransportError("work-item defer returned an unusable body")
        return cause

    async def start(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        claim_name: str,
        sandbox_name: str,
        context: FactoryContext,
    ) -> WorkItemStartGrant:
        try:
            body = await self._post(
                f"/v1/internal/work-items/requests/{request_id}/start",
                {
                    "owner": owner,
                    "generation": generation,
                    "claim_name": claim_name,
                    "sandbox_name": sandbox_name,
                    "context": context.model_dump(mode="json"),
                },
            )
        except WorkItemConflict as exc:
            raise WorkItemStartRefused(exc.code) from exc
        try:
            return WorkItemStartGrant(
                runtime_epoch=int(body["runtime_epoch"]),
                execution_deadline=_parse_datetime(body["execution_deadline"]),
                remaining_s=float(body["remaining_s"]),
                heartbeat_interval_s=float(body["heartbeat_interval_s"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item start returned an unusable body"
            ) from exc

    async def heartbeat(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
    ) -> WorkItemHeartbeat:
        body = await self._post(
            f"/v1/internal/work-items/requests/{request_id}/heartbeat",
            {"runtime_epoch": runtime_epoch},
        )
        try:
            cause = body.get("terminal_cause")
            return WorkItemHeartbeat(
                status=str(body["status"]),
                terminal_cause=str(cause) if cause is not None else None,
                work_item_cancelled=bool(body["work_item_cancelled"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item heartbeat returned an unusable body"
            ) from exc

    async def running_for_conversation(
        self, conversation_id: str, *, wake: FactoryWake
    ) -> WorkItemRunning | None:
        response = await self._client.get(
            f"{self._base}/v1/internal/work-items/running",
            headers=self._headers,
            params={
                "conversation_id": conversation_id,
                "wake_kind": wake.kind,
                "wake_address": wake.address,
                "wake_adapter": wake.adapter or "",
                "wake_conversation_id": wake.conversation_id,
            },
        )
        if response.status_code == 404:
            return None
        if response.status_code == 409 and _conflict_code(response) == "execution_ended":
            raise WorkItemConflict("execution_ended")
        if response.status_code >= 500:
            raise WorkItemTransportError(
                f"work-item running lookup returned {response.status_code}"
            )
        if response.status_code != 200:
            raise WorkItemConflict(_conflict_code(response))
        try:
            body = response.json()
            context = (
                FactoryContext.model_validate(body["context"])
                if body["context"] is not None
                else None
            )
            refusal = body["refusal"]
            if (context is None) != (refusal is not None):
                raise ValueError("running context and refusal disagree")
            if refusal is not None and refusal not in _FACTORY_AUTHORITY_REFUSALS:
                raise ValueError("unknown factory refusal")
            agent_id = uuid.UUID(str(body["agent_id"]))
            if context is not None and context.agent_id != agent_id:
                raise ValueError("running context names a different agent")
            lease_held_to_deadline = body["lease_held_to_deadline"]
            if type(lease_held_to_deadline) is not bool:
                raise ValueError("running deadline lease hold must be a boolean")
            return WorkItemRunning(
                lease_held_to_deadline=lease_held_to_deadline,
                request_id=uuid.UUID(str(body["request_id"])),
                work_item_id=uuid.UUID(str(body["work_item_id"])),
                agent_id=agent_id,
                generation=_required_positive_int(body["generation"]),
                conversation_id=_required_string(body["conversation_id"]),
                wait_deadline=_parse_datetime(body["wait_deadline"]),
                repository_path=_required_string(body["repository_path"]),
                runtime_epoch=_required_positive_int(body["runtime_epoch"]),
                execution_deadline=_parse_datetime(body["execution_deadline"]),
                context=context,
                refusal=refusal,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item running lookup returned an unusable body"
            ) from exc

    async def hold_for_approval(
        self, request_id: uuid.UUID, *, runtime_epoch: int, budget_s: float = DEFAULT_BUDGET_S
    ) -> None:
        await self._post_settlement(
            f"/v1/internal/work-items/requests/{request_id}/hold-approval",
            {"runtime_epoch": runtime_epoch},
            budget_s=budget_s,
        )

    async def finish(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        outcome: str,
        cause: str,
        detail: str | None,
        ci_fix_round: int | None,
        budget_s: float = DEFAULT_BUDGET_S,
    ) -> None:
        await self._post_settlement(
            f"/v1/internal/work-items/requests/{request_id}/finish",
            {
                "runtime_epoch": runtime_epoch,
                "outcome": outcome,
                "cause": cause,
                "detail": detail,
                "ci_fix_round": ci_fix_round,
            },
            budget_s=budget_s,
        )

    async def claim_termination(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
    ) -> int:
        body = await self._post(
            f"/v1/internal/work-items/requests/{request_id}/termination/claim",
            {"owner": owner},
        )
        try:
            return int(body["runtime_epoch"])
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item termination claim returned an unusable body"
            ) from exc

    async def record_termination(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        observation: str,
    ) -> None:
        await self._post(
            f"/v1/internal/work-items/requests/{request_id}/termination",
            {"runtime_epoch": runtime_epoch, "observation": observation},
        )

    async def runtime_owners(
        self, after: uuid.UUID | None = None
    ) -> list[WorkItemRuntimeOwner]:
        """One page of running requests and their owners, ordered by id."""

        try:
            response = await self._client.get(
                f"{self._base}/v1/internal/work-items/runtime-owners",
                params={"after": str(after)} if after is not None else None,
                headers=self._headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise WorkItemTransportError(
                "work-item dispatch endpoint is unreachable"
            ) from exc
        if response.status_code != 200:
            raise WorkItemTransportError(
                f"work-item runtime owners returned HTTP {response.status_code}"
            )
        try:
            body = response.json()
            return [
                WorkItemRuntimeOwner(
                    request_id=uuid.UUID(str(row["request_id"])),
                    runtime_owner=str(row["runtime_owner"]),
                    runtime_epoch=int(row["runtime_epoch"]),
                )
                for row in body["requests"]
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item runtime owners returned an unusable body"
            ) from exc

    async def declare_owner_lost(
        self, request_id: uuid.UUID, *, owner: str, runtime_epoch: int
    ) -> None:
        await self._post(
            f"/v1/internal/work-items/requests/{request_id}/owner-lost",
            {"owner": owner, "runtime_epoch": runtime_epoch},
        )

    async def get_request(self, request_id: uuid.UUID) -> WorkItemRequestView:
        try:
            response = await self._client.get(
                f"{self._base}/v1/internal/work-items/requests/{request_id}",
                headers=self._headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise WorkItemTransportError(
                "work-item dispatch endpoint is unreachable"
            ) from exc
        if response.status_code == 409:
            raise WorkItemConflict(_conflict_code(response))
        if response.status_code == 404:
            raise WorkItemConflict("not_found")
        if response.status_code != 200:
            raise WorkItemTransportError(
                f"work-item dispatch returned HTTP {response.status_code}"
            )
        try:
            body = response.json()
            if not isinstance(body, dict):
                raise TypeError("body")
            epoch = body.get("runtime_epoch")
            return WorkItemRequestView(
                status=str(body["status"]),
                runtime_epoch=int(epoch) if epoch is not None else 0,
                runtime_claim_name=_optional_str(body.get("runtime_claim_name")),
                runtime_sandbox_name=_optional_str(body.get("runtime_sandbox_name")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item request view returned an unusable body"
            ) from exc

    async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
        """The execution scoped issue read capability (ADR 0187).

        Returns the issue the capability names, as its tracker's display
        reference (``owner/repo#number`` on GitHub, ``PROJ-12`` on Jira), and
        the capability itself. The worker never reads the issue.
        """

        body = await self._post(
            "/v1/internal/work-items/issue-read/context",
            {"execution_request_id": str(request_id)},
        )
        try:
            if uuid.UUID(str(body["execution_request_id"])) != request_id:
                raise ValueError("issue read context names another execution")
            capability = body["capability"]
            if not isinstance(capability, str) or not capability:
                raise ValueError("issue read context carries no capability")
            issue = body["issue"]
            if not isinstance(issue, str) or not issue:
                raise ValueError("issue read context names no issue")
            return issue, capability
        except (KeyError, TypeError, ValueError):
            # The body carries the capability: never chain it into a log.
            raise WorkItemTransportError(
                "work-item issue read context returned an unusable body"
            ) from None

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._client.post(
                f"{self._base}{path}",
                headers=self._headers,
                json=payload,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise WorkItemTransportError(
                "work-item dispatch endpoint is unreachable"
            ) from exc
        return self._post_result(response)

    async def _post_settlement(
        self, path: str, payload: dict[str, Any], *, budget_s: float
    ) -> dict[str, Any]:
        try:
            response = await post_with_retry(
                self._client,
                f"{self._base}{path}",
                headers=self._headers,
                json=payload,
                follow_redirects=False,
                budget_s=budget_s,
            )
        except httpx.HTTPError as exc:
            raise WorkItemTransportError(
                f"work-item dispatch endpoint is unreachable: {type(exc).__name__}"
            ) from exc
        return self._post_result(response)

    @staticmethod
    def _post_result(response: httpx.Response) -> dict[str, Any]:
        if response.status_code == 409:
            raise WorkItemConflict(_conflict_code(response))
        if response.status_code != 200:
            raise WorkItemTransportError(
                f"work-item dispatch returned HTTP {response.status_code}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise WorkItemTransportError(
                "work-item dispatch returned unusable JSON"
            ) from exc
        if not isinstance(body, dict):
            raise WorkItemTransportError("work-item dispatch returned unusable JSON")
        return body


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


StopCallback = Callable[["str", "WorkItemRun"], Awaitable[None]]


class WorkItemRun:
    """In-process record of one acquired execute wake, keyed by request id."""

    def __init__(
        self,
        *,
        client: WorkItemDispatchClient,
        request_id: uuid.UUID,
        owner: str,
        grant: WorkItemAcquireGrant | WorkItemRunning,
        wake: FactoryWake,
        event_id: str,
        thread_key: str,
        on_stop: StopCallback,
        on_stale: StopCallback,
    ) -> None:
        self.request_id = request_id
        self.work_item_id = grant.work_item_id
        self.owner = owner
        self.generation = grant.generation
        self.repository_path = grant.repository_path
        self.conversation_id = grant.conversation_id
        self.wait_deadline = grant.wait_deadline
        self.context = grant.context
        self.event_id = event_id
        self.thread_key = thread_key
        self.started = False
        self.finished = False
        # Set when the heartbeat gives the request up as a stale owner. A
        # finished run may release its own claim; an abandoned one releases
        # nothing and creates no approval, since the API termination chain owns
        # the lost request's teardown (ADR 0206, #4331).
        self.abandoned = False
        self.held = False
        self.runtime_epoch: int | None = None
        self.claim_name: str | None = None
        self.sandbox_name: str | None = None
        self.recorded_pr_url: str | None = None
        self.execution_deadline: datetime | None = None
        self.agent_id = grant.agent_id
        self._client = client
        self._wake = wake
        self._on_stop = on_stop
        self._on_stale = on_stale
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._deferred = False
        self._acquire_refusal: str | None = None
        self._acquire_lock = asyncio.Lock()
        self._acquire_renew_task: asyncio.Task[None] | None = asyncio.create_task(
            self._acquire_renew_loop(),
            name=f"work-item-acquire-renew-{self.request_id}",
        )

    async def defer(self, reason: str, *, capacity: bool, start_failure_detail: str | None) -> None:
        self._deferred = True
        terminal_cause = await self._client.defer(
            self.request_id,
            owner=self.owner,
            generation=self.generation,
            reason=reason,
            capacity=capacity,
            start_failure_detail=start_failure_detail,
            budget_s=self._write_budget_s(),
        )
        if terminal_cause is not None:
            # #4170: the deferral ended the request (start_failed). As in
            # #3208, an unstarted request gets no terminate wake, so count the
            # run as settled and let this delivery release its sandbox claim
            # instead of holding quota until the route TTL lapses. A deferral
            # that leaves the request waiting keeps the claim for the next
            # acquire of this thread to adopt.
            self.finished = True

    def ensure_acquired(self) -> None:
        if not self.started and self._acquire_refusal is not None:
            raise WorkItemStartRefused(self._acquire_refusal)

    async def start(self, *, claim_name: str, sandbox_name: str) -> WorkItemStartGrant:
        # Recorded before the API call so a start refused on a terminal code
        # (#3208) still knows the claim it made, and its release stays fenced
        # to that claim (#4331). runtime_epoch stays the started-ness signal.
        self.claim_name = claim_name
        self.sandbox_name = sandbox_name
        async with self._acquire_lock:
            self.ensure_acquired()
            if self.context is None:
                raise WorkItemStartRefused("factory_surface_mismatch")
            grant = await self._client.start(
                self.request_id,
                owner=self.owner,
                generation=self.generation,
                claim_name=claim_name,
                sandbox_name=sandbox_name,
                context=self.context,
            )
            self.started = True
            self.runtime_epoch = grant.runtime_epoch
            self.execution_deadline = grant.execution_deadline
        interval = max(_MIN_HEARTBEAT_INTERVAL_S, grant.heartbeat_interval_s)
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(interval),
            name=f"work-item-heartbeat-{self.request_id}",
        )
        return grant

    def bound_remaining_s(self, remaining_s: float | None) -> float | None:
        if self.execution_deadline is None:
            return remaining_s
        left = max(
            0.0,
            (self.execution_deadline - datetime.now(UTC)).total_seconds(),
        )
        if remaining_s is None:
            return left
        return min(remaining_s, left)

    def _write_budget_s(self) -> float:
        budget = self.bound_remaining_s(DEFAULT_BUDGET_S)
        assert budget is not None
        return budget

    @property
    def heartbeat_running(self) -> bool:
        task = self._heartbeat_task
        return task is not None and not task.done() and not self._stopping

    async def hold_for_approval(self) -> None:
        if self.runtime_epoch is None:
            raise WorkItemTransportError("work-item hold called before start")
        await self._client.hold_for_approval(
            self.request_id, runtime_epoch=self.runtime_epoch, budget_s=self._write_budget_s()
        )

    async def finish(
        self, *, outcome: str, cause: str, detail: str | None, ci_fix_round: int | None
    ) -> None:
        if self.runtime_epoch is None:
            raise WorkItemTransportError("work-item finish called before start")
        await self._client.finish(
            self.request_id,
            runtime_epoch=self.runtime_epoch,
            outcome=outcome,
            cause=cause,
            detail=detail,
            ci_fix_round=ci_fix_round,
            budget_s=self._write_budget_s(),
        )
        self.finished = True

    async def close(self) -> None:
        """Drop renewal and heartbeat. A stop in flight is allowed to finish."""

        renew_task = self._acquire_renew_task
        self._acquire_renew_task = None
        if renew_task is not None:
            renew_task.cancel()
            try:
                await renew_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.warning(
                    "work-item acquire renewal for %s ended with an error",
                    self.request_id,
                    exc_info=True,
                )

        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is None:
            return
        if not task.done() and not self._stopping:
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception:
            logger.warning(
                "work-item heartbeat for %s ended with an error",
                self.request_id,
                exc_info=True,
            )

    async def _acquire_renew_loop(self) -> None:
        while not (self.started or self.finished or self._stopping or self._deferred):
            await _renew_sleep(_ACQUIRE_RENEW_INTERVAL_S)
            async with self._acquire_lock:
                if self.started or self.finished or self._stopping or self._deferred:
                    return
                try:
                    await self._client.acquire(
                        self.request_id,
                        owner=self.owner,
                        generation=self.generation,
                        wake=self._wake,
                        context=self.context,
                    )
                except WorkItemConflict as exc:
                    if not self.started and (
                        exc.code in _FACTORY_AUTHORITY_REFUSALS
                        or exc.code == "factory_start_failed"
                    ):
                        self._acquire_refusal = exc.code
                        self._deferred = True
                        if exc.code == "factory_start_failed":
                            self.finished = True
                    logger.info(
                        "work-item acquire renewal refused for %s: %s",
                        self.request_id,
                        exc.code,
                    )
                    return
                except WorkItemTransportError:
                    logger.warning(
                        "work-item acquire renewal transport failed for %s",
                        self.request_id,
                    )

    async def _heartbeat_loop(self, interval_s: float) -> None:
        failures = 0
        try:
            while not self.finished and not self._stopping:
                await asyncio.sleep(interval_s)
                if self.finished or self._stopping or self.runtime_epoch is None:
                    return
                try:
                    beat = await self._client.heartbeat(
                        self.request_id,
                        runtime_epoch=self.runtime_epoch,
                    )
                except WorkItemConflict as exc:
                    if exc.code == "stale_owner":
                        await self._invoke_stop(self._on_stale)
                        return
                    logger.warning(
                        "work-item heartbeat refused for %s: %s",
                        self.request_id,
                        exc.code,
                    )
                    continue
                except WorkItemTransportError:
                    failures += 1
                    logger.warning(
                        "work-item heartbeat transport failed for %s (%d/%d)",
                        self.request_id,
                        failures,
                        _HEARTBEAT_TRANSPORT_FAILURES,
                    )
                    if failures >= _HEARTBEAT_TRANSPORT_FAILURES:
                        await self._invoke_stop(self._on_stale)
                        return
                    continue
                failures = 0
                if beat.status == "cancellation_requested":
                    await self._invoke_stop(self._on_stop)
                    return
        except asyncio.CancelledError:
            raise

    async def _invoke_stop(self, callback: StopCallback) -> None:
        self._stopping = True
        await asyncio.shield(callback(self.thread_key, self))
