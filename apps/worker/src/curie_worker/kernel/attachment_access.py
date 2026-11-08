"""On-demand attachment authority is owned by one logical opener attempt.

The live record contains no token and exists only to let a steering worker
renew the opener. Every mint is recorded before I/O so even a lost reply is
revoked. API revocation and local deletion are both owner checked.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from aci_protocol import ATTACHMENT_ACCESS_STATUS_FIELD, AttachmentAccessCapability, Event

from ..attachment_access_client import AttachmentAccessUnavailable
from ..sandbox.types import SandboxHandle
from . import constants
from .log import logger

_LEASE_RENEW_S = 30.0

if TYPE_CHECKING:
    from .core import Kernel


class AttachmentTurnEnded(Exception):
    """The locked liveness recheck proved the missing opener already ended."""


async def _refuse_or_finished(
    self: Kernel, handle: SandboxHandle, remaining_s: float | None, message: str
) -> NoReturn:
    # Called only under the route lock. A missing/expired authority is not
    # evidence of runner liveness: the opener can finish between the original
    # status snapshot and this lookup. Unknown or still active stays refused.
    budget = 2.0 if remaining_s is None else min(2.0, remaining_s)
    if not await self._turn_active(handle, remaining_s=budget):
        raise AttachmentTurnEnded()
    raise AttachmentAccessUnavailable(message)


@dataclass(frozen=True)
class AttachmentAccessTurn:
    agent_id: str
    deployment_id: str
    thread_key: str
    event_id: str


@dataclass
class AttemptAttachmentAccess:
    owner: str = field(default_factory=lambda: uuid.uuid4().hex)
    attempted: list[dict[str, Any]] = field(default_factory=list)
    heartbeat: asyncio.Task[None] | None = None

    async def stop(self) -> None:
        task, self.heartbeat = self.heartbeat, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def _identity(body: dict[str, Any]) -> dict[str, Any]:
    return {key: body[key] for key in ("agent_id", "thread_key", "event_id", "owner")}


async def _heartbeat(self: Kernel, record: AttemptAttachmentAccess) -> None:
    assert self._attachment_access is not None
    while True:
        await asyncio.sleep(_LEASE_RENEW_S)
        for body in record.attempted:
            try:
                await self._attachment_access.renew(**_identity(body))
            except Exception as exc:  # noqa: BLE001 -- finite API lease fails closed
                logger.warning("attachment access renewal failed: %s", type(exc).__name__)


async def _require_runner(self: Kernel, handle: SandboxHandle, remaining_s: float | None) -> None:
    status = await self._runner.status(
        handle.base_url,
        token=handle.token or None,
        remaining_s=2.0 if remaining_s is None else min(2.0, remaining_s),
    )
    if not isinstance(status, dict) or status.get(ATTACHMENT_ACCESS_STATUS_FIELD) is not True:
        raise AttachmentAccessUnavailable(
            "On-demand attachments require a compatible runner with attachment tools enabled."
        )


async def with_attachment_access(
    self: Kernel,
    event: Event,
    handle: SandboxHandle,
    remaining_s: float | None,
    *,
    turn: AttachmentAccessTurn | None = None,
) -> Event:
    carry = turn if turn is not None else constants._ATTACHMENT_ACCESS_TURN.get()
    if carry is None:
        return event
    if self._attachment_access is None:
        raise AttachmentAccessUnavailable("On-demand attachments require a compatible API client.")
    await _require_runner(self, handle, remaining_s)
    record = constants._ATTACHMENT_ACCESS_ATTEMPT.get()
    if record is None:
        raise AttachmentAccessUnavailable("Attachment access attempt was not initialized.")
    ttl_s = max(1, min(3600, math.ceil(self._runner.turn_deadline_s(remaining_s))))
    body = dict(
        agent_id=carry.agent_id,
        deployment_id=carry.deployment_id,
        thread_key=carry.thread_key,
        event_id=carry.event_id,
        owner=record.owner,
        session_id=handle.session_id,
        sandbox_name=handle.sandbox_name,
        route_generation=handle.generation,
        ttl_s=ttl_s,
    )
    record.attempted.append(body)
    minted = await self._attachment_access.context(**body)
    await self._markers.record_attachment_access_turn(carry.thread_key, body, ttl_s)
    if record.heartbeat is None:
        record.heartbeat = asyncio.create_task(_heartbeat(self, record))
    return event.model_copy(
        update={
            "attachment_access": AttachmentAccessCapability(
                url=self._config.runner_facing_api_base_url.rstrip("/") + "/attachment-access",
                token=minted.token,
            )
        }
    )


async def steer_attachment_access(
    self: Kernel,
    event: Event,
    handle: SandboxHandle,
    thread_key: str,
    *,
    active: bool,
    remaining_s: float | None = None,
) -> Event:
    carry = constants._ATTACHMENT_ACCESS_TURN.get()
    if carry is None:
        return event
    if not active:
        return event
    await _require_runner(self, handle, remaining_s)
    client = self._attachment_access
    if client is None:
        raise AttachmentAccessUnavailable("Attachment access API is unavailable.")
    body = await self._markers.read_attachment_access_turn(thread_key)
    if body is None or (
        body.get("session_id") != handle.session_id
        or body.get("sandbox_name") != handle.sandbox_name
        or body.get("route_generation") != handle.generation
    ):
        await _refuse_or_finished(
            self, handle, remaining_s, "The live turn has no matching attachment access authority."
        )
    assert body is not None
    try:
        renewed = await client.renew(**_identity(body), current_event_id=carry.event_id)
    except AttachmentAccessUnavailable as exc:
        await _refuse_or_finished(self, handle, remaining_s, str(exc))
    if not renewed:
        await _refuse_or_finished(
            self, handle, remaining_s, "The live attachment access lease has expired."
        )
    # Idempotent context returns the SAME opener's token with its original fixed
    # expiry. A steer owns no mint and therefore settles no opener authority.
    try:
        minted = await client.context(**body)
    except AttachmentAccessUnavailable as exc:
        await _refuse_or_finished(self, handle, remaining_s, str(exc))
    return event.model_copy(
        update={
            "attachment_access": AttachmentAccessCapability(
                url=self._config.runner_facing_api_base_url.rstrip("/") + "/attachment-access",
                token=minted.token,
            )
        }
    )


async def close_attachment_access(self: Kernel, record: AttemptAttachmentAccess) -> None:
    await record.stop()
    if self._attachment_access is None:
        return
    for body in record.attempted:
        try:
            # Bound retries. The independent 90 second lease is the crash and
            # partition backstop; never renew from a settlement task.
            for attempt in range(3):
                try:
                    await self._attachment_access.revoke(**_identity(body))
                    break
                except Exception:  # noqa: BLE001 -- bounded retry before lease expiry
                    if attempt == 2:
                        raise
                    await asyncio.sleep(0.1 * (2**attempt))
            await self._markers.take_attachment_access_turn(body["thread_key"], record.owner)
        except Exception as exc:  # noqa: BLE001 -- lease expiry remains fail closed
            logger.warning("attachment access revoke pending lease expiry: %s", type(exc).__name__)
