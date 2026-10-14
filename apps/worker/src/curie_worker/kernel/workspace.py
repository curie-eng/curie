from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aci_protocol import (
    QueuedTurn,
    SessionStatus,
)

from ..sandbox.types import (
    SandboxHandle,
)
from ..workspace import (
    WorkspacePreparationError,
    parse_github_repo_fact,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import failures
from .log import logger


def _workspace_inference_notice(repo: str | None) -> str | None:
    """The platform's one-line account of a repository it inferred (#2659).

    Platform-authored, never model text. ``repo`` is only ever the repository
    the server selected (allowlist-checked and shape-validated), never raw
    message text, so it cannot carry a blank line or the CLI's approval marker.
    It is appended after the model's answer as its own block, and never written
    into the runner's event text or ``TurnOutcome.text``, so the model input,
    the approval record summary and the approval card stay free of it. None when
    nothing was inferred, which ``_join_reply_blocks`` skips.
    """

    if not repo:
        return None
    return f"Working in {repo}, from the repository named in your message."


def _same_repo(left: str | None, right: str | None) -> bool:
    """Whether two repository names are the same repository (#2659).

    GitHub names compare case-insensitively; an absent side is never a match.
    """

    return left is not None and right is not None and left.casefold() == right.casefold()


@dataclass
class _WorkspaceInferenceCarry:
    """One delivery's record of the repository its message inferred (#2659).

    Created in ``process_event``'s local scope and shared by every attempt of
    that delivery. ``_route_and_start`` records the inference as soon as the
    claim attaches the workspace, before any steer or turn start, so an
    attempt that attaches and then fails still leaves the fact for the retry
    that adopts the attached route. It is only ever set, never cleared.
    """

    repo: str | None = None


def _log_workspace_start_failure(
    self: Kernel,
    qevent: QueuedTurn,
    turn_text: str,
    exc: WorkspacePreparationError,
    *,
    agent_id: uuid.UUID | None,
    agent_name: str | None,
    workspace_deployment_id: uuid.UUID | None,
) -> None:
    """Name the deployment behind a workspace PREPARATION failure (#2004).

    The refusal branch below already covers its own half: it answers the
    requester with ``exc.public_detail`` and tells the operator over its own
    INFO line. This complements that rather than replacing it. Every OTHER
    workspace start failure -- a clone, an archive, an upload, a missing
    coordinator -- falls into ``_attempt``'s broad start-failure clause,
    which used to log an event id and an anonymous ``repr``: naming neither
    the agent, nor the deployment, nor the repository. The reported symptom
    is what those faults cost -- the turn acks, creates no sandbox, and an
    operator has nothing to search on.

    So this emits one WARNING carrying everything needed to find the
    deployment from the outside: the event, the agent, the deployment, the
    repository the turn asked for (``<none named>`` when the message named
    none -- that absence is itself the usual cause), the preparation stage
    that failed, and a never-empty reason. WARNING rather than the refusal's
    INFO because the two are different kinds of event: a refusal is a
    decision the feature made on purpose, a preparation failure is a fault
    nobody chose.

    It takes the turn TEXT rather than an ``Event`` so it can name the
    repository fact independently of where preparation failed.
    """
    # Total by construction: parse_github_repo_fact RAISES
    # WorkspaceSelectionRefused on a multi-repository message. That refusal
    # is incidental here -- a second failure raised by the reparse, not the
    # one being reported -- and letting it escape would replace the caller's
    # fault with it, losing the outcome the caller is about to return and
    # leaving the entry pending until dead-letter. Naming one of the two
    # repositories instead would be a lie, so the ambiguity is reported as
    # itself. Narrow on purpose -- a genuine bug here should still surface.
    try:
        repository = parse_github_repo_fact(turn_text) or "<none named>"
    except WorkspacePreparationError:
        repository = "<ambiguous>"
    logger.warning(
        "workspace start failed for %s: agent=%s agent_id=%s deployment=%s "
        "repo=%s stage=%s reason=%s",
        qevent.event_id,
        agent_name or "<unknown>",
        agent_id if agent_id is not None else "<unknown>",
        workspace_deployment_id if workspace_deployment_id is not None else "<unknown>",
        repository,
        exc.stage,
        failures._exception_reason(exc),
    )


async def _cold_handoff_readiness(
    self: Kernel,
    handle: SandboxHandle,
    *,
    remaining_s: float | None = None,
) -> str:
    """Classify whether an authenticated retained runner can be replaced."""

    if not handle.token:
        logger.warning(
            "cold handoff refused an unauthenticated legacy runner at %s",
            handle.base_url,
        )
        return "unsafe"
    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token,
            remaining_s=remaining_s,
        )
    except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
        logger.warning("could not read cold handoff fence at %s: %r", handle.base_url, exc)
        return "unsafe"
    active = status.get("turn_active")
    if active is True:
        return "active"
    if active is not False:
        logger.warning("cold handoff status carried no usable activity state")
        return "unsafe"
    if status.get("history_durable") is not True:
        logger.warning("cold handoff refused a runner without durable history")
        return "unsafe"
    if status.get("status") not in {
        SessionStatus.DONE.value,
        SessionStatus.IDLE_AWAITING_INPUT.value,
        SessionStatus.CLASSIFIED_FAILURE.value,
    }:
        logger.warning("cold handoff refused a runner outside a safe idle status")
        return "unsafe"
    return "ready"


async def _workspace_handoff_ready(
    self: Kernel,
    handle: SandboxHandle,
    *,
    remaining_s: float | None = None,
    lineage_reconciliation: bool = False,
    pending_publication_approval: bool = False,
) -> bool:
    """Fail closed unless the old runner is idle with durable replay state.

    A runner idle in ``classified-failure`` is at a boundary too (#4188).
    The transcript never records a failed turn, so a replacement rehydrates
    everything replay holds; ``history_durable`` still has to say so.
    Refusing that status locked the thread: nothing moves a failed runner
    to another status except a new turn, which this fence blocks.
    """

    if not handle.token:
        logger.warning(
            "workspace handoff refused an unauthenticated legacy runner at %s",
            handle.base_url,
        )
        return False
    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token,
            remaining_s=remaining_s,
        )
    except Exception as exc:  # noqa: BLE001 - unreadable is never safe to replace
        logger.warning("could not read workspace handoff fence at %s: %r", handle.base_url, exc)
        return False
    return (
        status.get("turn_active") is False
        and status.get("history_durable") is True
        and not pending_publication_approval
        and (
            status.get("status")
            in {
                SessionStatus.DONE.value,
                SessionStatus.IDLE_AWAITING_INPUT.value,
                SessionStatus.CLASSIFIED_FAILURE.value,
            }
            or (
                lineage_reconciliation
                and status.get("status") == SessionStatus.AWAITING_APPROVAL.value
            )
        )
    )


async def _workspace_candidate_ready(
    self: Kernel, handle: SandboxHandle, *, remaining_s: float | None = None
) -> bool:
    """Fail closed unless this exact candidate booted the managed checkout."""

    if not handle.token:
        logger.warning(
            "workspace handoff refused an unauthenticated candidate runner at %s",
            handle.base_url,
        )
        return False
    try:
        status = await self._runner.status(
            handle.base_url,
            token=handle.token,
            remaining_s=remaining_s,
        )
    except Exception as exc:  # noqa: BLE001 - unreadable is never safe to expose
        logger.warning(
            "could not attest workspace handoff candidate at %s: %r",
            handle.base_url,
            exc,
        )
        return False
    return (
        status.get("session_id") == handle.session_id
        and status.get("sandbox_id") == handle.sandbox_id
        and status.get("managed_workspace") is True
        and status.get("cwd") == "/workspace"
        and status.get("ready") is True
        and status.get("turn_active") is False
        and status.get("history_durable") is True
        and status.get("status") == SessionStatus.IDLE_AWAITING_INPUT.value
    )
