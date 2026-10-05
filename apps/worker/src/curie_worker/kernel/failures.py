from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    QueuedTurn,
    SessionStatus,
    ToolAccess,
)
from curie_telemetry.redact import redact_text

from ..runner_client import (
    RunnerError,
    RunnerWorkspaceSnapshot,
)
from ..sandbox.types import (
    SandboxTermination,
)

if TYPE_CHECKING:
    pass

from . import constants


def _exception_reason(exc: BaseException) -> str:
    """A never-empty operator-facing reason for a caught exception (#2011).

    ``str(TimeoutError())`` is the empty string, so logging a caught exception
    directly can print nothing at all where the reason belongs. Prefer the
    class name plus the message, and fall back to the class name alone when the
    exception carries no message.
    """
    text = str(exc).strip()
    if text:
        return f"{type(exc).__name__}: {text}"
    return type(exc).__name__


def map_error_classification(raw: str | None) -> str:
    if raw is not None and raw in constants.PLATFORM_ERROR_CLASSIFICATIONS:
        return raw
    return constants.UNCLASSIFIED_ERROR_CLASSIFICATION


def _escalation_cause(failure: TurnOutcome | None) -> str:
    if failure is None or failure.classification is None:
        return "runner_escalated"
    return constants._ESCALATION_CAUSES.get(failure.classification, "runner_escalated")


def _sandbox_termination_detail(termination: SandboxTermination) -> str:
    """Render bounded, redacted Kubernetes diagnosis for terminal surfaces."""

    reason = termination.reason
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", reason) is None:
        reason = "Unknown"
    lead = f"Kubernetes pod terminated: {reason}"
    detail = termination.detail
    if not isinstance(detail, str):
        return f"{lead}."
    safe_detail = " ".join(redact_text(detail).split())
    if not safe_detail:
        return f"{lead}."
    max_detail = constants._ESCALATION_DETAIL_MAX - len(lead) - len(" ().")
    return f"{lead} ({safe_detail[:max_detail]})."


def _display_error_classification(raw: str | None) -> str:
    if raw is not None and raw in constants.WORKER_LOCAL_DISPLAY_CLASSIFICATIONS:
        return raw
    return map_error_classification(raw)


def _max_turns_guidance(delivered_max_turns: str | None) -> str:
    """Name the turn limit this delivery actually ran under (#3403).

    Only a work-item delivery writes CURIE_MAX_TURNS into the boot env, from
    worker.workItemMaxTurns. Every other delivery runs under the runner's own
    CURIE_MAX_TURNS, which the operator sets through runner.extraEnv.
    """

    if delivered_max_turns is not None:
        return (
            f"The work item used its whole turn budget of {delivered_max_turns} "
            "turns; raise worker.workItemMaxTurns (CURIE_WORK_ITEM_MAX_TURNS, "
            f"currently {delivered_max_turns}) to allow more turns."
        )
    return (
        "The run used the runner's whole turn budget; raise CURIE_MAX_TURNS "
        "through runner.extraEnv (runner default "
        f"{constants._RUNNER_DEFAULT_MAX_TURNS} when unset) to allow more turns. "
        "worker.workItemMaxTurns applies only to work items."
    )


def _with_guidance(lead: str, token: str, *, delivered_max_turns: str | None) -> str:
    if token == "max-turns":
        guidance: str | None = _max_turns_guidance(delivered_max_turns)
    else:
        guidance = constants._CLASSIFICATION_GUIDANCE.get(token)
    return f"{lead} {guidance}" if guidance else lead


def failure_class_from_reply(text: str) -> str | None:
    """The failure class on a delivered reply, or None when it is not a failure.

    Only a first line of ``curie-turn-failure: <token>`` counts. A later mention,
    or a token that contains whitespace, is model text and is not a failure.
    """

    if not text or not text.strip():
        return None
    line = text.lstrip("\n").splitlines()[0].strip()
    if not line.startswith(constants.TURN_FAILURE_REPLY_PREFIX):
        return None
    token = line[len(constants.TURN_FAILURE_REPLY_PREFIX) :].strip()
    if not token or any(char.isspace() for char in token):
        return None
    return token


def turn_failure_reply(failure_class: str, message: str) -> str:
    """Prefix ``message`` so a text-only consumer can see the failed turn."""

    if (
        failure_class_from_reply(f"{constants.TURN_FAILURE_REPLY_PREFIX} {failure_class}")
        != failure_class
    ):
        raise ValueError("failure class must be one token")
    body = message.strip()
    if not body:
        return f"{constants.TURN_FAILURE_REPLY_PREFIX} {failure_class}"
    return f"{constants.TURN_FAILURE_REPLY_PREFIX} {failure_class}\n\n{body}"


def _escalation_text(
    qevent: QueuedTurn,
    *,
    lead: str,
    detail: str | None,
) -> str:
    # Redact before clip so a truncated JWT or key still matches the redactor.
    clipped = redact_text((detail or "").strip())
    if len(clipped) > constants._ESCALATION_DETAIL_MAX:
        clipped = clipped[: constants._ESCALATION_DETAIL_MAX]
    extra = f"{clipped} event_id={qevent.event_id}." if clipped else f"event_id={qevent.event_id}."
    return f"{lead} {extra} Flagging for a human."


def _is_context_tool(name: str) -> bool:
    if name in constants._CONTEXT_TOOLS:
        return True
    parts = name.split("__", 2)
    return (
        len(parts) == 3
        and parts[0] == "mcp"
        and parts[2].startswith(constants._CONTEXT_MCP_PREFIXES)
    )


def _unpublished_cause(tools_called: frozenset[str]) -> str:
    """``early_stop`` when the turn reported nothing, published nothing and did
    no work beyond fetching context; otherwise ``no_pull_request``."""

    for name in tools_called:
        if name == constants._APPROVAL_TOOL_NAME or _is_context_tool(name):
            continue
        return "no_pull_request"
    return "early_stop"


def _finish_detail(text: str) -> str | None:
    """The agent's last message for the terminal record: redacted, THEN clipped,
    so a credential straddling the clip can never survive as a raw prefix."""

    detail = redact_text(text.strip())
    if not detail:
        return None
    if len(detail) > constants._FINISH_DETAIL_MAX:
        return detail[: constants._FINISH_DETAIL_MAX - 3] + "..."
    return detail


@dataclass
class TurnOutcome:
    """The result of streaming one turn, feeding the retry/escalate decision."""

    terminal_ok: bool
    saw_side_effect: bool = False
    classification: str | None = None
    error_message: str | None = None
    text: str = ""
    status: SessionStatus | None = None
    steered: bool = False
    # The worker could not start this turn and answered with its own text: no
    # pool for the agent (#2943), an attachment it could not fetch, or no
    # capacity on a source that cannot wait for it. The reply is delivered as
    # is, but the turn did no work, so its telemetry outcome is
    # classified_failure rather than done.
    start_failed: bool = False
    # The approval summary and route off an awaiting-approval final (ADR-0010,
    # #247), persisted onto the durable record by the pause path. None on
    # every other status; route also None when the request named none.
    approval_summary: str | None = None
    approval_route: str | None = None
    # Gate provenance off the awaiting-approval final (#544, Decision C):
    # 'permission'|'policy' and the denied tool name (permission gate only).
    # Threaded onto the durable record. None from an older runner.
    approval_gate_kind: str | None = None
    approval_granted_tool: str | None = None
    approval_granted_arguments: dict[str, Any] | None = None
    approval_display: str | None = None
    publication_snapshot: RunnerWorkspaceSnapshot | None = None
    publication_snapshot_error: str | None = None
    review_origin_key: str | None = None
    # The repository this turn attached because its own message named it
    # (#2659). Read by `_pause_for_approval` for the notice composition. None on
    # every other turn.
    workspace_inferred_repo: str | None = None
    # Every tool the turn called, the model's reply without the receipt, and
    # whether this outcome already includes the one factory continuation (#3128).
    tools_called: frozenset[str] = frozenset()
    assistant_text: str = ""
    continued: bool = False


class _FactoryExecutionEnded(Exception):
    """The factory run already ended, so this approval must not resume it."""


class _WorkItemDeferred(Exception):
    """The execute wake was deferred in SQL; ACK it and start nothing."""


class ThreadBusyError(RuntimeError):
    """A non-steering turn found a live thread, so it was not started.

    Raised INSTEAD of steering or blocking for jobs (ADR-0079) and for verified
    review feedback whose revision must remain attributable to its own origin.
    Deliberately not one of the classes ``_attempt`` converts into a retryable
    outcome: this is a turn that has not begun. Letting it escape leaves the
    stream entry PENDING, so existing bounded reclaim redelivers it after the
    conversation has finished.

    The redelivery interval is therefore ``reclaim_min_idle_ms`` and the give-up
    point is ``max_delivery``, which is a coarse instrument borrowed from crash
    recovery rather than a scheduling policy: a deferred turn behind a
    conversation longer than that budget dead-letters instead of running late.
    That is a visible, bounded outcome rather than a silent one. A targeted
    cron turn does not take this path: the kernel records its hook run
    ``deferred`` and the scheduler retries it within the catch-up bound (#2929).
    """


class ToolAccessUnenforced(Exception):
    """The runner a restricted turn would go to does not enforce its access.

    @spec WORKER-TOOL-ACCESS-2: a runner that does not list the value under
    ``tool_access`` would ignore the field and run the turn unrestricted, or
    cannot enforce it on this session, so the turn is refused once and never
    retried against the same boot.
    """

    def __init__(self, access: ToolAccess) -> None:
        super().__init__(f"the runner does not advertise tool access {access.value!r}")
        self.public_detail = (
            f"This agent cannot start: its runner cannot enforce {access.value} tool "
            "access for this turn, so the turn was not run."
        )


class ChannelReadUnenforced(Exception):
    """A granted turn's runner does not advertise channel read enforcement.

    ADR 0100 (#2877): a runner that does not answer ``channel_read: true`` on
    its status would ignore the capability or cannot honor its revocation, so
    the turn is refused once under ``channel-read-unenforced`` and never
    retried against the same boot. The capability already minted is revoked
    by the attempt's settlement.
    """

    public_detail = (
        "This agent cannot start: its runner cannot enforce channel read for this "
        "turn, so the turn was not run."
    )

    def __init__(self) -> None:
        super().__init__("the runner does not advertise channel read enforcement")


class ChannelReadUnavailable(RunnerError):
    """A turn's channel read grant could not be confirmed either way.

    ADR 0100 review round 2: the bundle manifest was unreadable and the mint
    failed, so the kernel cannot tell whether this turn must be enforced. Not
    run, and retried like any turn the runner did not accept (it is a
    ``RunnerError``, so it classifies as the retryable ``runner-error``; the
    retry metric domain is closed, so it gets no class of its own).
    """

    def __init__(self) -> None:
        super().__init__("channel read grant unknown: bundle unreadable and mint failed")


class LiveSessionBusy(ThreadBusyError):
    """The busy read under the per-thread lock found a live session.

    The one ``ThreadBusyError`` a cron fire records as ``deferred`` (#2929): the
    others (a pending publication, a failed handoff) are not a live session.
    """


class CatchUpExpired(ThreadBusyError):
    """A deferred cron slot's retry reached its start after its catch-up bound."""


class HookPaused(ThreadBusyError):
    """An operator paused the cron hook before the runner accepted its turn."""


class SweepClaimGone(RuntimeError):
    """A sweep continuation found its sandbox claim gone (ADR-0160, #2878).

    Terminal, never "busy, retry later": the sweep stops and reports what it
    did not cover. Not a ThreadBusyError on purpose, so no busy handler can
    defer or redeliver it.
    """


class PendingPublicationError(ThreadBusyError):
    """A thread-owned publication must settle before another turn can start."""

    public_detail = (
        "This thread already has a publication awaiting approval or completion. "
        "Resolve it before continuing."
    )

    def __init__(self, thread_key: str) -> None:
        super().__init__(f"thread {thread_key} has a pending publication revision")
