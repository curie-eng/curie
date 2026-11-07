"""Per-delivery current authority. @spec HTTP-INVOCATION-EXECUTION-18.

A delivery also retains the verified requester its version 2 answers project,
and keeps exactly one for its lifetime. @spec CONNECTOR-REQUESTER-1.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from curie_internal.identity_approval import (
    ApprovalCreationIdentityV1,
    ApprovalGrantAuthorityRequestV1,
    ApprovalGrantAuthorityResponseV1,
    ApprovalGrantExpectationV1,
)
from curie_internal.identity_authority import (
    Stage,
    TurnAuthorityRequestV1,
    TurnAuthorityResponseV1,
    TurnAuthorityResponseV2,
    TurnAuthorityScopeV1,
    TurnRequesterV1,
)

from ..binding import (
    AmbiguousRoute,
    HttpIdentityBinding,
    RequesterIdentityStateInvalid,
    binding_adapter_for_handle,
)
from ..caller_token import CallerPrincipal
from ..hook_runs import HookRunRecorderError
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
    # Exact stream bytes before any rewrite. @spec HTTP-APPROVAL-CONTINUITY-4.
    original_payload: str | None = field(default=None, repr=False)
    # The binding a context-free cron turn's deployment authority was
    # verified against at prepare. @spec HTTP-INVOCATION-EXECUTION-23.
    scheduled: HttpIdentityBinding | None = None
    # The verified requester a version 2 continue projected, or None for no
    # verified human. Once any version 2 answer arrived (requester_checked),
    # every later check of this delivery must be version 2 with this same
    # requester. Never logged. @spec CONNECTOR-REQUESTER-1.
    requester: TurnRequesterV1 | None = field(default=None, repr=False)
    requester_checked: bool = False


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
    classification = await _classify(self, carry, handle)
    if classification is None:
        if recognized:
            raise HttpIdentityRefused()
        carry.legacy = True
        return
    if not recognized:
        if classification.http_identity_required:
            if not await _scheduled_authority(self, carry, classification):
                raise HttpIdentityRefused()
            carry.scheduled = classification
            return
        carry.legacy = True
        return
    carry.scope = TurnAuthorityScopeV1(
        tenant_id=str(classification.tenant_id), agent_id=str(classification.agent_id),
        binding_id=str(classification.binding_id),
        binding_generation=classification.binding_generation,
    )
    await check(self, "prepare")


async def _classify(
    self: Kernel, carry: HttpTurnCarry, handle: ReplyHandle,
) -> HttpIdentityBinding | None:
    """The route's current binding classification. @spec HTTP-INVOCATION-EXECUTION-17/18."""
    classifier = getattr(self._binding, "identity_binding", None)
    if not callable(classifier):
        raise TurnAuthorityUnavailable()
    classification: HttpIdentityBinding | None
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
    return classification


async def _scheduled_authority(
    self: Kernel, carry: HttpTurnCarry, classification: HttpIdentityBinding,
) -> bool:
    """A context-free cron turn's deployment authority, read from durable state.

    The source and reference only locate the run row; the row decides.
    @spec HTTP-INVOCATION-EXECUTION-23.
    """
    event = carry.event
    ref = event.hook_run
    recorder = self._hook_runs
    if event.source is not TurnSource.CRON or ref is None or recorder is None:
        return False
    try:
        if uuid.UUID(ref.agent_id) != classification.agent_id:
            return False
    except (AttributeError, TypeError, ValueError):
        return False
    try:
        held = await recorder.scheduled_authority(ref)
    except HookRunRecorderError as exc:
        if exc.code == "invalid_ref":
            return False
        raise TurnAuthorityUnavailable() from None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    return held


async def _recheck_scheduled(self: Kernel, carry: HttpTurnCarry) -> None:
    """Repeat the prepare-time scheduled authority at a later boundary.

    @spec HTTP-INVOCATION-EXECUTION-23.
    """
    expected = carry.scheduled
    handle = carry.event.reply_handle
    if expected is None or handle is None:
        raise HttpIdentityRefused()
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    current = await _classify(self, carry, handle)
    if current is None or (
        current.tenant_id, current.agent_id, current.binding_id, current.binding_generation,
    ) != (
        expected.tenant_id, expected.agent_id, expected.binding_id, expected.binding_generation,
    ):
        raise HttpIdentityRefused()
    if not await _scheduled_authority(self, carry, current):
        raise HttpIdentityRefused()


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


def selected() -> bool:
    """A recognized HTTP context selects core continuity. @spec HTTP-APPROVAL-CONTINUITY-1."""
    carry = CURRENT.get()
    return carry is not None and carry.context is not None


def requester() -> TurnRequesterV1 | None:
    """The live delivery's retained verified requester, or None.

    None outside a live HTTP-context delivery, for a delivery whose answers
    were version 1, and for a version 2 answer without a verified human.
    @spec CONNECTOR-REQUESTER-1.
    """
    carry = CURRENT.get()
    if carry is None or carry.context is None or not carry.requester_checked:
        return None
    if not is_live(carry):
        return None
    return carry.requester


def _retain_requester(
    carry: HttpTurnCarry, result: TurnAuthorityResponseV1 | TurnAuthorityResponseV2,
) -> None:
    """One requester per delivery, bound to its checked scope.

    A version 1 continue after any version 2 answer (a mixed-version API
    replica), or a version 2 requester that differs from the retained one,
    including null against non-null, refuses. @spec CONNECTOR-REQUESTER-1.
    """
    if not isinstance(result, TurnAuthorityResponseV2):
        if carry.requester_checked:
            raise HttpIdentityRefused()
        return
    projected = result.requester
    if projected is not None and (
        carry.scope is None or projected.tenant_id != carry.scope.tenant_id
    ):
        raise HttpIdentityRefused()
    if carry.requester_checked:
        if projected != carry.requester:
            raise HttpIdentityRefused()
        return
    carry.requester = projected
    carry.requester_checked = True


async def requester_principal(self: Kernel, agent_id: uuid.UUID) -> CallerPrincipal | None:
    """The principal an HTTP-context boot signs into its ``cct``, or None.

    Called immediately before the targeted ``boot_env`` of a selected turn. The
    operator flag is read for every such turn, with or without a retained
    requester. An unreadable or malformed flag refuses before boot, and so
    does a retained requester on an agent that has not opted in (an
    inconsistent answer). Only an opted-in agent with a retained requester
    for this checked agent gets a principal; everything else boots
    principal-less. A schedule, hook, eval or legacy turn has no HTTP context
    and never reaches here. @spec CONNECTOR-REQUESTER-1 CONNECTOR-REQUESTER-2.
    """
    carry = CURRENT.get()
    if carry is None or carry.context is None:
        return None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    retained = carry.requester if carry.requester_checked else None
    reader = getattr(self._binding, "connector_requester_identity_for", None)
    if not callable(reader):
        # A binding that cannot say whether the agent opted in never mints one.
        if retained is not None:
            raise HttpIdentityRefused()
        return None
    try:
        enabled = await reader(agent_id)
    except RequesterIdentityStateInvalid:
        raise HttpIdentityRefused() from None
    if type(enabled) is not bool:
        raise HttpIdentityRefused()
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    if retained is None:
        return None
    if (
        not enabled or carry.scope is None or retained.tenant_id != carry.scope.tenant_id
        or str(agent_id) != carry.scope.agent_id
    ):
        raise HttpIdentityRefused()
    try:
        return CallerPrincipal(
            tenant_id=retained.tenant_id, issuer=retained.issuer, subject=retained.subject,
        )
    except ValueError:
        raise HttpIdentityRefused() from None


def creation_identity() -> ApprovalCreationIdentityV1:
    """The live owner's exact creation carry. @spec HTTP-APPROVAL-CONTINUITY-4."""
    carry = CURRENT.get()
    if carry is None or carry.context is None:
        raise HttpIdentityRefused()
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    payload = carry.original_payload
    if (
        carry.scope is None or type(payload) is not str or type(carry.payload_sha256) is not str
        or hashlib.sha256(payload.encode("utf-8")).hexdigest() != carry.payload_sha256
    ):
        raise HttpIdentityRefused()
    try:
        return ApprovalCreationIdentityV1(
            schema_version=1, context=carry.context, payload=payload, scope=carry.scope,
        )
    except (ValueError, TypeError):
        raise HttpIdentityRefused() from None


async def check(self: Kernel, stage: Stage) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/18/23."""
    carry = CURRENT.get()
    if carry is not None and carry.context is None and carry.scheduled is not None:
        await _recheck_scheduled(self, carry)
        return
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
    result: TurnAuthorityResponseV1 | TurnAuthorityResponseV2
    try:
        answer = await self._identity_authority.check(request)
        if type(answer) is TurnAuthorityResponseV1:
            result = TurnAuthorityResponseV1.model_validate(answer)
        elif type(answer) is TurnAuthorityResponseV2:
            result = TurnAuthorityResponseV2.model_validate(answer)
        else:
            raise ValueError
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
    _retain_requester(carry, result)


async def approval_grant(self: Kernel, agent_id: uuid.UUID) -> ApprovalGrantExpectationV1 | None:
    """The one exact snapshot a fresh core check continued for, or no grant.

    Called after prepare and immediately before boot_env; nothing from an earlier
    observation is reused. @spec HTTP-APPROVAL-CONTINUITY-12.
    """
    carry = CURRENT.get()
    if carry is None or carry.context is None:
        raise TurnAuthorityUnavailable()
    approval_id = _approval_id_from_resume_event(carry.event.event_id)
    if approval_id is None:
        return None
    reader = getattr(self._binding, "approval_grant_snapshot", None)
    if not callable(reader) or carry.scope is None or self._identity_authority is None:
        raise TurnAuthorityUnavailable()
    try:
        value = await reader(carry.event.event_id, agent_id)
    except ValueError:
        raise HttpIdentityRefused() from None
    except Exception:  # noqa: BLE001 - fixed unavailable boundary, cancellation propagates.
        raise TurnAuthorityUnavailable() from None
    if value is None:
        return None
    try:
        if type(value) is not ApprovalGrantExpectationV1:
            raise ValueError
        grant = ApprovalGrantExpectationV1.model_validate(value)
        if (
            grant.approval_id != approval_id or grant.agent_id != str(agent_id)
            or grant.agent_id != carry.scope.agent_id
            or (grant.gate_kind == "permission" and (
                grant.granted_tool is None or grant.granted_arguments_json is None
            ))
            or carry.payload_sha256 is None
        ):
            raise ValueError
        request = ApprovalGrantAuthorityRequestV1(
            schema_version=1,
            check=TurnAuthorityRequestV1(
                schema_version=1, request_id=str(uuid.uuid4()), stage="prepare",
                context=carry.context, payload_sha256=carry.payload_sha256,
                event_id=carry.event.event_id, scope=carry.scope, approval_id=approval_id,
            ),
            grant=grant,
        )
    except (ValueError, TypeError):
        raise HttpIdentityRefused() from None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    try:
        result = await self._identity_authority.check_grant(request)
        if type(result) is not ApprovalGrantAuthorityResponseV1:
            raise ValueError
        result = ApprovalGrantAuthorityResponseV1.model_validate(result)
        check = result.check
        encoded = json.dumps(
            grant.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        )
        if (
            check.request_id != request.check.request_id or check.stage != "prepare"
            or check.event_id != request.check.event_id or check.scope != request.check.scope
            or check.context_sha256 != hashlib.sha256(carry.context.encode("ascii")).hexdigest()
            or result.grant_sha256 != hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        ):
            raise ValueError
    except Exception:  # noqa: BLE001 - fixed unavailable boundary, cancellation propagates.
        raise TurnAuthorityUnavailable() from None
    if not is_live(carry):
        raise TurnAuthorityUnavailable()
    if check.decision == "refuse":
        raise HttpIdentityRefused()
    if check.decision != "continue":
        raise TurnAuthorityUnavailable()
    return grant
