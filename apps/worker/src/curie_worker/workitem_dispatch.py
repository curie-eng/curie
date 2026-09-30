"""Work-item dispatch client, event-id parsing, and termination observation.

The kernel recognizes ``work-item-{uuid}-execute-{generation}`` and
``work-item-{uuid}-terminate`` on the runs stream. SQL in the API owns wait
and ownership; this module is the worker-side HTTP seam and the in-process
run record keyed by request id.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import httpx

logger = logging.getLogger(__name__)

_UUID = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
WORK_ITEM_EXECUTE_RE = re.compile(rf"^work-item-({_UUID})-execute-([1-9][0-9]*)$")
WORK_ITEM_TERMINATE_RE = re.compile(rf"^work-item-({_UUID})-terminate$")
WORK_ITEM_CI_RE = re.compile(rf"^work-item-({_UUID})-ci-([23])$")

_HEARTBEAT_TRANSPORT_FAILURES = 3
_MIN_HEARTBEAT_INTERVAL_S = 1.0


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


@dataclass(frozen=True)
class WorkItemAcquireGrant:
    generation: int
    work_item_id: uuid.UUID
    conversation_id: str
    wait_deadline: str
    # The repository the signed delivery bound to the WorkItem. None only on
    # the synthetic grant an approval resume rebuilds, which keeps the
    # thread's existing workspace selection.
    repo_full_name: str | None


@dataclass(frozen=True)
class WorkItemStartGrant:
    runtime_epoch: int
    execution_deadline: datetime
    remaining_s: float
    heartbeat_interval_s: float


@dataclass(frozen=True)
class WorkItemRunning:
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    runtime_epoch: int
    execution_deadline: datetime


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
    the same running request; ``generation`` carries its fix round (2 or 3).
    """

    matched = WORK_ITEM_EXECUTE_RE.fullmatch(event_id)
    if matched is not None:
        return WorkItemEvent(
            request_id=uuid.UUID(matched.group(1)),
            kind="execute",
            generation=int(matched.group(2)),
        )
    matched = WORK_ITEM_CI_RE.fullmatch(event_id)
    if matched is not None:
        return WorkItemEvent(
            request_id=uuid.UUID(matched.group(1)),
            kind="ci",
            generation=int(matched.group(2)),
        )
    matched = WORK_ITEM_TERMINATE_RE.fullmatch(event_id)
    if matched is not None:
        return WorkItemEvent(
            request_id=uuid.UUID(matched.group(1)),
            kind="terminate",
        )
    return None


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
    ) -> WorkItemAcquireGrant:
        body = await self._post(
            f"/v1/internal/work-items/requests/{request_id}/acquire",
            {"owner": owner, "generation": generation},
        )
        try:
            return WorkItemAcquireGrant(
                generation=int(body["generation"]),
                work_item_id=uuid.UUID(str(body["work_item_id"])),
                conversation_id=str(body["conversation_id"]),
                wait_deadline=str(body["wait_deadline"]),
                # An API replica from before #2992 omits the field mid-rollout.
                # The acquisition is already committed, so read absence as "no
                # WorkItem repository" rather than failing the wake.
                repo_full_name=(
                    str(body["repo_full_name"])
                    if body.get("repo_full_name") is not None
                    else None
                ),
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
    ) -> None:
        await self._post(
            f"/v1/internal/work-items/requests/{request_id}/defer",
            {
                "owner": owner,
                "generation": generation,
                "reason": reason,
                "capacity": capacity,
            },
        )

    async def start(
        self,
        request_id: uuid.UUID,
        *,
        owner: str,
        generation: int,
        claim_name: str,
        sandbox_name: str,
    ) -> WorkItemStartGrant:
        try:
            body = await self._post(
                f"/v1/internal/work-items/requests/{request_id}/start",
                {
                    "owner": owner,
                    "generation": generation,
                    "claim_name": claim_name,
                    "sandbox_name": sandbox_name,
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

    async def running_for_conversation(self, conversation_id: str) -> WorkItemRunning | None:
        response = await self._client.get(
            f"{self._base}/v1/internal/work-items/running",
            headers=self._headers,
            params={"conversation_id": conversation_id},
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
            return WorkItemRunning(
                request_id=uuid.UUID(str(body["request_id"])),
                work_item_id=uuid.UUID(str(body["work_item_id"])),
                runtime_epoch=int(body["runtime_epoch"]),
                execution_deadline=_parse_datetime(body["execution_deadline"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkItemTransportError(
                "work-item running lookup returned an unusable body"
            ) from exc

    async def hold_for_approval(self, request_id: uuid.UUID, *, runtime_epoch: int) -> None:
        await self._post(
            f"/v1/internal/work-items/requests/{request_id}/hold-approval",
            {"runtime_epoch": runtime_epoch},
        )

    async def finish(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        outcome: str,
        cause: str,
        detail: str | None,
    ) -> None:
        await self._post(
            f"/v1/internal/work-items/requests/{request_id}/finish",
            {
                "runtime_epoch": runtime_epoch,
                "outcome": outcome,
                "cause": cause,
                "detail": detail,
            },
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
        grant: WorkItemAcquireGrant,
        event_id: str,
        thread_key: str,
        on_stop: StopCallback,
        on_stale: StopCallback,
    ) -> None:
        self.request_id = request_id
        self.work_item_id = grant.work_item_id
        self.owner = owner
        self.generation = grant.generation
        self.repo_full_name = grant.repo_full_name
        self.event_id = event_id
        self.thread_key = thread_key
        self.started = False
        self.finished = False
        self.held = False
        self.runtime_epoch: int | None = None
        self.claim_name: str | None = None
        self.sandbox_name: str | None = None
        self.execution_deadline: datetime | None = None
        self._client = client
        self._on_stop = on_stop
        self._on_stale = on_stale
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopping = False

    async def defer(self, reason: str, *, capacity: bool) -> None:
        await self._client.defer(
            self.request_id,
            owner=self.owner,
            generation=self.generation,
            reason=reason,
            capacity=capacity,
        )

    async def start(self, *, claim_name: str, sandbox_name: str) -> WorkItemStartGrant:
        grant = await self._client.start(
            self.request_id,
            owner=self.owner,
            generation=self.generation,
            claim_name=claim_name,
            sandbox_name=sandbox_name,
        )
        self.started = True
        self.runtime_epoch = grant.runtime_epoch
        self.claim_name = claim_name
        self.sandbox_name = sandbox_name
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

    async def hold_for_approval(self) -> None:
        if self.runtime_epoch is None:
            raise WorkItemTransportError("work-item hold called before start")
        await self._client.hold_for_approval(
            self.request_id, runtime_epoch=self.runtime_epoch
        )

    async def finish(self, *, outcome: str, cause: str, detail: str | None) -> None:
        if self.runtime_epoch is None:
            raise WorkItemTransportError("work-item finish called before start")
        await self._client.finish(
            self.request_id,
            runtime_epoch=self.runtime_epoch,
            outcome=outcome,
            cause=cause,
            detail=detail,
        )
        self.finished = True

    async def close(self) -> None:
        """Drop the heartbeat. A stop already in flight is allowed to finish."""

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
