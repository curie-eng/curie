"""Core-owned resume construction for HTTP-selected approvals.

``ResumeQueue.enqueue_approval`` is the one publisher every resolve, expiry,
recovery and reconciler wake converges on. For a selected row this owner reads
the committed row and its stored seal, obtains current authority for every
required observation, revalidates the unchanged row inside that final read-only
boundary, builds the turn with the existing status-to-turn builder, serializes it
once and seals a ResumeContextV1 over those exact bytes. The queue appends both
while the boundary is still open. @spec HTTP-APPROVAL-CONTINUITY-11.

A rejected or expired notification needs fresh original-invoker authority only
and carries no grant. An approved resume also needs the preserved answering
actor's fresh approver authority (@spec HTTP-APPROVAL-CONTINUITY-8); that
two-subject observation is supplied as ``approved``. Without it an approved
selected row is refused and its wake stays owed.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from aci_protocol import PrincipalRef
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi import HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api import identity_fence
from curie_api.delivery import DeliveryValidityWindow
from curie_api.models import Approval, ApprovalStatus
from curie_api.resumequeue import (
    ApprovalResumeRefused,
    AuthorizedResume,
    resume_event_id,
    resume_turn_for,
)

from .http_approval import approval_is_selected, approval_record_digest, verified_capsule
from .http_invocation import HttpInvocationRuntime, current_http_approval_caller

Role = Literal["invocation", "approver"]

_MICROS_MAX = 9_007_199_254_740_991
_RESUMABLE = (ApprovalStatus.approved, ApprovalStatus.rejected, ApprovalStatus.expired)


@dataclass(frozen=True)
class ResumeWindow:
    """One observation's inclusive T1 window in UTC microseconds.

    @spec HTTP-APPROVAL-CONTINUITY-11.
    """

    valid_from_us: int
    valid_until_us: int

    def __post_init__(self) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-11."""
        if (
            any(
                type(value) is not int or not 0 <= value <= _MICROS_MAX
                for value in (self.valid_from_us, self.valid_until_us)
            )
            or self.valid_from_us > self.valid_until_us
        ):
            raise ApprovalResumeRefused("authority_unavailable")


@dataclass(frozen=True)
class CurrentResumeAuthority:
    """A finished current-authority observation inside its open final reader.

    ``session`` is the read-only reader that holds the identity reader lock;
    ``stable_fence`` is the identity fence value read under it; ``windows`` names
    each verified observation's T1 window by role. It is valid only until the
    context that yielded it exits. @spec HTTP-APPROVAL-CONTINUITY-11.
    """

    session: AsyncSession = field(repr=False)
    stable_fence: str = field(repr=False)
    windows: Mapping[Role, ResumeWindow]


# The two-subject (original invoker and preserved answering actor) current check
# for an approved resume, owned by the approver-authority composition
# (@spec HTTP-APPROVAL-CONTINUITY-8). It runs every lookup outside SQL and yields
# inside its final read-only boundary with both windows. @spec HTTP-APPROVAL-CONTINUITY-11.
ApprovedResumeAuthority = Callable[
    [wire.ApprovalContextV1], AbstractAsyncContextManager[CurrentResumeAuthority]
]


@dataclass(frozen=True)
class _StateApp:
    """@spec HTTP-APPROVAL-CONTINUITY-11."""

    state: Any


@dataclass(frozen=True)
class _StateRequest:
    """The app-state view the caller composition reads; no HTTP request exists here.

    @spec HTTP-APPROVAL-CONTINUITY-11.
    """

    app: _StateApp


def _window(validity: DeliveryValidityWindow) -> ResumeWindow:
    """@spec HTTP-APPROVAL-CONTINUITY-11."""
    return ResumeWindow(
        validity.lower_seconds * 1_000_000 + validity.lower_microseconds,
        validity.upper_seconds * 1_000_000 + validity.upper_microseconds,
    )


def _original(saved: wire.ApprovalContextV1) -> codec.InvocationContextV1:
    """The preserved original-invoker association, as the caller phases read it.

    @spec HTTP-APPROVAL-CONTINUITY-8/11.
    """
    return codec.InvocationContextV1(
        schema_version=1, purpose="invocation", key_id=saved.key_id,
        key_generation=saved.key_generation, event_id=saved.original_event_id,
        original_event_id=saved.original_event_id, scope=saved.scope,
        receiver_id=saved.receiver_id, receiver_generation=saved.receiver_generation,
        independent_human=saved.independent_human, provider_issuer=saved.provider_issuer,
        provider_version=saved.provider_version,
        provider_credential_id=saved.provider_credential_id,
        provider_credential_generation=saved.provider_credential_generation,
        lifecycle_generation=saved.lifecycle_generation,
        role_policy_version=saved.role_policy_version, outcome=saved.outcome,
        canonical=saved.canonical, principal=saved.principal,
        payload_sha256=saved.original_payload_sha256,
    )


def _refusal(error: Exception) -> ApprovalResumeRefused:
    """Bounded codes only; no provider or SQL detail. @spec HTTP-APPROVAL-CONTINUITY-11."""
    if isinstance(error, ApprovalResumeRefused):
        return error
    if isinstance(error, HTTPException) and error.status_code in (401, 403):
        return ApprovalResumeRefused("caller_not_allowed")
    return ApprovalResumeRefused("authority_unavailable")


class HttpResumeOwner:
    """The API owner behind ``ResumeQueue.enqueue_approval``. @spec HTTP-APPROVAL-CONTINUITY-11."""

    def __init__(self, state: Any, *, approved: ApprovedResumeAuthority | None = None) -> None:
        self._state = state
        self._approved = approved

    async def committed(self, approval_id: uuid.UUID) -> tuple[Approval, bool] | None:
        """The committed row and its classification; unavailable never selects legacy.

        @spec HTTP-APPROVAL-CONTINUITY-1/11.
        """
        try:
            async with self._state.sessionmaker() as session:
                row = await session.get(Approval, approval_id)
                if row is None:
                    return None
                return row, await approval_is_selected(session, row)
        except Exception as error:  # noqa: BLE001 - classification failure is a refusal.
            raise _refusal(error) from None

    def _resumable(
        self, runtime: HttpInvocationRuntime, row: Approval | None,
    ) -> wire.ApprovalContextV1:
        """A supported selected row whose stored seal names it exactly.

        @spec HTTP-APPROVAL-CONTINUITY-1/5/11.
        """
        if (
            row is None or row.identity_context is None or row.purpose != "session"
            or row.status not in _RESUMABLE or row.resolved_at is None
        ):
            raise ApprovalResumeRefused("unsupported_resume")
        saved = verified_capsule(runtime, row)
        if saved is None or saved.status == "pending":
            raise ApprovalResumeRefused("unsupported_resume")
        return saved

    @asynccontextmanager
    async def _caller(self, saved: wire.ApprovalContextV1) -> AsyncIterator[CurrentResumeAuthority]:
        """Fresh original-invoker authority through the existing caller phases.

        @spec HTTP-APPROVAL-CONTINUITY-8/11.
        """
        request = cast(Request, _StateRequest(_StateApp(self._state)))
        async with current_http_approval_caller(request, _original(saved)) as live:
            # The live reader's own stable fence and T1 window.
            stable, validity = live.publication_window()
            yield CurrentResumeAuthority(
                session=live.session, stable_fence=stable,
                windows={"invocation": _window(validity)},
            )

    @asynccontextmanager
    async def authorize(self, approval_id: uuid.UUID) -> AsyncIterator[AuthorizedResume]:
        """Authorize one exact publication; the reader stays open across the append.

        @spec HTTP-APPROVAL-CONTINUITY-11.
        """
        runtime = getattr(self._state, "http_identity_runtime", None)
        if type(runtime) is not HttpInvocationRuntime:
            raise ApprovalResumeRefused("authority_unavailable")
        entered = False
        try:
            # A short reader capture; released before any lookup.
            async with self._state.sessionmaker() as session, session.begin():
                await identity_fence.lock_identity_reader(session)
                row = await session.get(Approval, approval_id, populate_existing=True)
                saved = self._resumable(runtime, row)
                assert row is not None
                if not await approval_is_selected(session, row):
                    raise ApprovalResumeRefused("unsupported_resume")
                snapshot = approval_record_digest(row)
            required: frozenset[Role]
            if saved.status == "approved":
                if self._approved is None:
                    raise ApprovalResumeRefused("authority_unavailable")
                authority = self._approved
                required = frozenset({"invocation", "approver"})
            else:
                authority = self._caller
                required = frozenset({"invocation"})
            async with authority(saved) as current:
                if (
                    type(current) is not CurrentResumeAuthority
                    or not current.session.in_transaction()
                    or frozenset(current.windows) != required
                    or any(type(item) is not ResumeWindow for item in current.windows.values())
                ):
                    raise ApprovalResumeRefused("authority_unavailable")
                lower = max(item.valid_from_us for item in current.windows.values())
                upper = min(item.valid_until_us for item in current.windows.values())
                if lower > upper:
                    raise ApprovalResumeRefused("outside_validity")
                # Revalidate the committed row under the final reader.
                row = await current.session.get(Approval, approval_id, populate_existing=True)
                if (
                    self._resumable(runtime, row) != saved
                    or approval_record_digest(cast(Approval, row)) != snapshot
                    or self._state.http_identity_runtime is not runtime
                ):
                    raise ApprovalResumeRefused("authority_unavailable")
                assert row is not None
                authorized = self._publication(runtime, saved, row, current, lower, upper)
                entered = True
                yield authorized
        except Exception as error:
            if entered:
                raise
            raise _refusal(error) from None

    def _publication(
        self, runtime: HttpInvocationRuntime, saved: wire.ApprovalContextV1, row: Approval,
        current: CurrentResumeAuthority, lower: int, upper: int,
    ) -> AuthorizedResume:
        """Build from the committed row, serialize once, seal over those bytes.

        @spec HTTP-APPROVAL-CONTINUITY-6/11.
        """
        principal = saved.principal
        turn = resume_turn_for(row).model_copy(update={
            # The original invoker from verified continuity, never resolved_by/system.
            "author_principal": None if principal is None else PrincipalRef(
                principal_id=principal.principal_id,
                authorization_version=principal.authorization_version,
            ),
        })
        payload = turn.model_dump_json()
        values = saved.model_dump()
        values.update(
            purpose="resume",
            key_id=runtime._signing_key[0],
            key_generation=runtime._signing_key[1],
            event_id=resume_event_id(row.id),
            payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            approval_record_sha256=approval_record_digest(row),
        )
        context = wire.seal_resume_context(
            wire.ResumeContextV1.model_validate(values), runtime._context_keys,
        )
        return AuthorizedResume(
            approval_id=row.id, payload=payload, context=context,
            stable_fence=current.stable_fence, valid_from_us=lower, valid_until_us=upper,
        )


def http_resume_owner(
    state: Any, *, approved: ApprovedResumeAuthority | None = None,
) -> HttpResumeOwner:
    """The selected resume owner, bound to the app's stores. @spec HTTP-APPROVAL-CONTINUITY-11."""
    return HttpResumeOwner(state, approved=approved)
