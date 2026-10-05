from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING

import aiohttp
from aci_protocol import (
    Event,
    PublicationContext,
    QueuedTurn,
    SessionStatus,
)
from channel_protocol.reply import (
    NavAffordance,
)
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

from ..approvals import (
    ApprovalBackendError,
)
from ..reply_sink import (
    TargetRoute,
)
from ..runner_client import (
    RunnerError,
    RunnerWorkspaceSnapshot,
)
from ..sandbox.types import (
    SandboxHandle,
)
from ..workitem_dispatch import (
    WorkItemRun,
    parse_work_item_event_id,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import channel_read, clock, constants, failures, memory, routing
from .log import logger


def _is_publish_provenance(gate_kind: str | None, granted_tool: str | None) -> bool:
    """Require both trusted runner-held publication provenance fields."""

    return (gate_kind, granted_tool) == constants._PUBLISH_PROVENANCE


def _publication_approval_summary(snapshot: RunnerWorkspaceSnapshot) -> str:
    """Build approval text from validated facts, labeling requester prose."""

    visible_paths = list(snapshot.changed_paths[:20])
    path_list = ", ".join(visible_paths)
    if len(snapshot.changed_paths) > len(visible_paths):
        path_list += f", and {len(snapshot.changed_paths) - len(visible_paths)} more"
    workflow_note = " GitHub workflow files are not publishable and none are included."
    requester_title = " ".join(snapshot.publication_title.split())[:256]
    normalized_body = " ".join(snapshot.publication_body.split())
    requester_body = normalized_body[:500]
    if len(normalized_body) > len(requester_body):
        requester_body += (
            f" [description truncated; {len(normalized_body)} characters will be published]"
        )
    return (
        f"Publish {snapshot.repo_full_name} from {snapshot.base_sha[:12]} with "
        f"{len(snapshot.changed_paths)} changed path(s): {path_list}."
        f"{workflow_note} Requester-provided title: {requester_title}. "
        f"Requester-provided description: {requester_body}"
    )


async def _bind_publication_context(
    self: Kernel,
    event: Event,
    *,
    queued_event_id: str,
    workspace_deployment_id: uuid.UUID | None,
    handle: SandboxHandle,
    run: WorkItemRun | None,
    remaining_s: float | None,
) -> tuple[Event, float | None]:
    """Bind fresh API authority immediately before a covered factory turn."""

    parsed = parse_work_item_event_id(queued_event_id)
    factory_execute = parsed is not None and parsed.kind in {"execute", "ci"}
    factory_resume = (
        self._is_approval_resume(queued_event_id)
        and run is not None
        and run.event_id == queued_event_id
    )
    if handle.workspace_repo is None or not (factory_execute or factory_resume):
        return event, remaining_s
    if (
        workspace_deployment_id is None
        or run is None
        or run.event_id != queued_event_id
        or not run.started
        or run.finished
        or run.runtime_epoch is None
        or self._publication_creator is None
    ):
        raise RunnerError("publication context requires current execution authority")
    started = clock.time.monotonic()
    timeout_s = 10.0 if remaining_s is None else min(10.0, remaining_s)
    try:
        async with asyncio.timeout(timeout_s):
            context = await self._publication_creator.get_publication_precheck_context(
                deployment_id=workspace_deployment_id,
                work_item_id=run.work_item_id,
                execution_request_id=run.request_id,
                runtime_epoch=run.runtime_epoch,
                queued_event_id=queued_event_id,
            )
    except (ApprovalBackendError, TimeoutError):
        raise RunnerError("publication context is unavailable") from None
    if run.finished:
        raise RunnerError("publication execution authority ended before turn start")
    if context is not None and (
        not isinstance(context, PublicationContext)
        or context.deployment_id != workspace_deployment_id
        or context.work_item_id != run.work_item_id
        or context.execution_request_id != run.request_id
        or context.runtime_epoch != run.runtime_epoch
        or context.queued_event_id != queued_event_id
    ):
        raise RunnerError("publication context identity was refused")
    if context is not None:
        # The worker may self-dial localhost while the sandbox reaches the
        # API through its bridge network. The API signs the authority, not
        # the transport URL; deliver the same route on the runner's base.
        base = self._config.runner_facing_api_base_url.rstrip("/")
        context = context.model_copy(update={"precheck_url": f"{base}/publications/precheck"})
    constants._PUBLICATION_CONTEXT.set(context)
    remaining_s = run.bound_remaining_s(
        None if remaining_s is None else remaining_s - (clock.time.monotonic() - started)
    )
    return event.model_copy(update={"publication_context": context}), remaining_s


async def _continue_unpublished(
    self: Kernel,
    qevent: QueuedTurn,
    route: TargetRoute,
    handle: SandboxHandle,
    outcome: failures.TurnOutcome,
    nav: NavAffordance | None,
    agent_id: uuid.UUID | None,
    inferred: str | None,
    *,
    workspace_deployment_id: uuid.UUID | None,
    remaining_s: float | None,
    memory_grant: memory.TurnMemoryGrant | None = None,
    channel_read_grant: channel_read.TurnChannelReadGrant | None = None,
) -> failures.TurnOutcome:
    """Re-prompt a factory execute turn that ended without publishing, ONCE.

    The returned outcome merges both turns: the tools either called, and the
    first turn's reply when the continuation said nothing. A continuation
    that cannot start is a runner failure, not an early stop (#3128).
    """

    parsed = parse_work_item_event_id(qevent.event_id)
    if parsed is None or parsed.kind != "execute":
        return outcome
    run = self._work_item_runs.get(parsed.request_id)
    if run is None or run.event_id != qevent.event_id or not run.started or run.finished:
        return outcome
    if (
        not outcome.terminal_ok
        or outcome.steered
        or outcome.continued
        or outcome.status not in (SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT)
        or PLATFORM_PUBLISH_TOOL_NAME in outcome.tools_called
    ):
        return outcome
    left = run.bound_remaining_s(remaining_s)
    if left is not None and left <= constants._MIN_ATTEMPT_BUDGET_S:
        return outcome
    early = failures._unpublished_cause(outcome.tools_called) == "early_stop"
    prompt = constants._EARLY_STOP_PROMPT if early else constants._UNPUBLISHED_PROMPT
    logger.info(
        "work-item continuation for %s (%s)",
        qevent.event_id,
        "early_stop" if early else "unpublished",
    )
    event = self._to_event(qevent).model_copy(update={"text": prompt})
    try:
        event, left = await self._bind_publication_context(
            event,
            queued_event_id=qevent.event_id,
            workspace_deployment_id=workspace_deployment_id,
            handle=handle,
            run=run,
            remaining_s=left,
        )
        if event.tool_access is not None:
            # @spec WORKER-TOOL-ACCESS-2: the continuation opens a turn too.
            await self._require_tool_access(handle, event.tool_access, left)
        # ADR-0188: the continuation opens a turn too, with its own
        # credential, minted from the budget its stream is bound from.
        event = self._with_memory_token(event, qevent, memory_grant, left)
        # ADR 0100: and its own channel read open, as this attempt's owner, on
        # the same logical turn (the queued event id is unchanged).
        if channel_read_grant is not None:
            event = await self._with_channel_read(
                event, handle, left, grant=channel_read_grant, qevent=qevent
            )
        turn = await self._runner.start_turn(
            handle.base_url, event, token=handle.token or None, remaining_s=left
        )
        if memory_grant is not None:
            self._record_turn_deadline(memory_grant, left)
        memory._note_live_memory_turn(turn)
    except failures.ToolAccessUnenforced as exc:
        logger.warning("work-item continuation refused for %s: %s", qevent.event_id, exc)
        return failures.TurnOutcome(
            terminal_ok=False,
            saw_side_effect=outcome.saw_side_effect,
            classification=constants.TOOL_ACCESS_UNENFORCED_CLASSIFICATION,
            error_message=exc.public_detail,
            tools_called=outcome.tools_called,
            assistant_text=outcome.assistant_text,
            continued=True,
        )
    except failures.ChannelReadUnenforced as exc:
        logger.warning("work-item continuation refused for %s: %s", qevent.event_id, exc)
        return failures.TurnOutcome(
            terminal_ok=False,
            saw_side_effect=outcome.saw_side_effect,
            classification=constants.CHANNEL_READ_UNENFORCED_CLASSIFICATION,
            error_message=exc.public_detail,
            tools_called=outcome.tools_called,
            assistant_text=outcome.assistant_text,
            continued=True,
        )
    except (RunnerError, aiohttp.ClientError, TimeoutError) as exc:
        # The agent never saw the prompt, so the ending is the runner's: the
        # normal failure policy (retry, or escalate after a side effect)
        # decides, not early_stop.
        logger.warning(
            "work-item continuation failed to start for %s",
            qevent.event_id,
            exc_info=True,
        )
        return failures.TurnOutcome(
            terminal_ok=False,
            saw_side_effect=outcome.saw_side_effect,
            classification=("runner-timeout" if isinstance(exc, TimeoutError) else "runner-error"),
            error_message=str(exc) or None,
            tools_called=outcome.tools_called,
            assistant_text=outcome.assistant_text,
            continued=True,
        )
    routing._lifecycle_event("runner.turn.started", "continuation")
    try:
        second = await self._consume(
            qevent,
            route,
            turn,
            nav,
            agent_id,
            handle=handle,
            workspace_inferred_repo=inferred,
        )
    finally:
        turn.close()
    second.tools_called = outcome.tools_called | second.tools_called
    second.saw_side_effect = outcome.saw_side_effect or second.saw_side_effect
    second.continued = True
    if not second.assistant_text.strip():
        second.assistant_text = outcome.assistant_text
    return second
