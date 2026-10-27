"""Per-delivery current authority. @spec HTTP-INVOCATION-EXECUTION-18."""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aci_protocol import QueuedTurn
from curie_internal.identity_authority import (
    Stage,
    TurnAuthorityRequestV1,
    TurnAuthorityResponseV1,
    TurnAuthorityScopeV1,
)

from ..binding import AmbiguousRoute, HttpIdentityBinding, binding_adapter_for_handle
from ..identity_authority import TurnAuthorityUnavailable
from .approval import _approval_id_from_resume_event

if TYPE_CHECKING:
    from .core import Kernel


class HttpIdentityRefused(RuntimeError):
    """@spec HTTP-INVOCATION-EXECUTION-18."""

    def __init__(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-18."""
        super().__init__("identity_invocation_refused")


@dataclass
class HttpTurnCarry:
    """Per-event ownership across copied contexts. @spec HTTP-INVOCATION-EXECUTION-18."""

    event: QueuedTurn = field(repr=False)
    context: str | None = field(repr=False)
    payload_sha256: str | None = field(repr=False)
    scope: TurnAuthorityScopeV1 | None = None
    active: bool = True
    legacy: bool = False
    owner: asyncio.Task[object] | None = field(default_factory=asyncio.current_task, repr=False)


CURRENT: ContextVar[HttpTurnCarry | None] = ContextVar("http_turn_authority", default=None)


def is_live(carry: HttpTurnCarry) -> bool:
    """Owner cancellation also invalidates copied tasks. @spec HTTP-INVOCATION-EXECUTION-18."""
    owner = carry.owner
    return carry.active and owner is not None and not owner.done() and owner.cancelling() == 0


async def prepare(self: Kernel) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-9/18."""
    carry = CURRENT.get()
    if carry is None:
        raise TurnAuthorityUnavailable()
    handle = carry.event.reply_handle
    recognized = carry.context is not None
    if recognized and (
        type(carry.context) is not str or not 1 <= len(carry.context) <= 16_384
        or not carry.context.isascii() or type(carry.payload_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", carry.payload_sha256) is None
    ):
        raise HttpIdentityRefused()
    if self._binding is None:
        if recognized:
            raise TurnAuthorityUnavailable()
        carry.legacy = True
        return
    if handle is None:
        if recognized:
            raise HttpIdentityRefused()
        carry.legacy = True
        return
    classifier = getattr(self._binding, "identity_binding", None)
    if not callable(classifier):
        raise TurnAuthorityUnavailable()
    try:
        classification = await classifier(
            handle.kind, binding_adapter_for_handle(handle), handle.channel,
        )
        if classification is not None:
            if type(classification) is not HttpIdentityBinding:
                raise ValueError
            classification = HttpIdentityBinding.model_validate(classification.model_dump())
    except AmbiguousRoute:
        # @spec HTTP-INVOCATION-EXECUTION-17: an HTTP-selected candidate on a shared
        # route is a definitive refusal, never a pending unavailable turn.
        raise HttpIdentityRefused() from None
    except Exception:  # noqa: BLE001 - fixed unavailable boundary, cancellation propagates.
        raise TurnAuthorityUnavailable() from None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    if classification is None:
        if recognized:
            raise HttpIdentityRefused()
        carry.legacy = True
        return
    if not recognized:
        if classification.http_identity_required:
            raise HttpIdentityRefused()
        carry.legacy = True
        return
    carry.scope = TurnAuthorityScopeV1(
        tenant_id=str(classification.tenant_id), agent_id=str(classification.agent_id),
        binding_id=str(classification.binding_id),
        binding_generation=classification.binding_generation,
    )
    await check(self, "prepare")


async def classify_skip(self: Kernel) -> None:
    """A context-free skip keeps legacy courtesy only once classified.

    Recognized contexts are not checked again for a delivery that is already done.
    @spec HTTP-INVOCATION-EXECUTION-18.
    """
    carry = CURRENT.get()
    if carry is None or carry.context is not None:
        return
    try:
        await prepare(self)
    except (TurnAuthorityUnavailable, HttpIdentityRefused):
        carry.legacy = False


async def check(self: Kernel, stage: Stage) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/18."""
    carry = CURRENT.get()
    if carry is None or carry.context is None:
        return
    if not is_live(carry) or carry.scope is None or self._identity_authority is None:
        raise TurnAuthorityUnavailable()
    if carry.payload_sha256 is None:
        raise HttpIdentityRefused()
    try:
        request = TurnAuthorityRequestV1(
            schema_version=1, request_id=str(uuid.uuid4()), stage=stage,
            context=carry.context, payload_sha256=carry.payload_sha256,
            event_id=carry.event.event_id, scope=carry.scope,
            approval_id=_approval_id_from_resume_event(carry.event.event_id),
        )
    except (ValueError, TypeError):
        raise HttpIdentityRefused() from None
    try:
        result = await self._identity_authority.check(request)
        if type(result) is not TurnAuthorityResponseV1:
            raise ValueError
        result = TurnAuthorityResponseV1.model_validate(result)
        if (
            result.request_id != request.request_id or result.stage != request.stage
            or result.event_id != request.event_id or result.scope != request.scope
            or result.context_sha256 != hashlib.sha256(request.context.encode("ascii")).hexdigest()
        ):
            raise ValueError
    except Exception:  # noqa: BLE001 - fixed unavailable boundary, cancellation propagates.
        raise TurnAuthorityUnavailable() from None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    if result.decision == "refuse":
        raise HttpIdentityRefused()
    if result.decision != "continue":
        raise TurnAuthorityUnavailable()
