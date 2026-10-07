from __future__ import annotations

import asyncio
import contextlib
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

from aci_protocol import (
    Event,
    QueuedTurn,
    TurnSource,
)
from aci_protocol.turn import DEFAULT_IDENTITY
from curie_telemetry.redact import redact_text

from ..approvals import (
    VerifiedReviewFeedback,
)
from ..attachments import (
    AttachmentResolutionError,
    PreparedAttachments,
)
from ..behaviorpacks import (
    BehaviorPacks,
)
from ..sandbox.types import (
    RouteChangedError,
    SandboxHandle,
)
from ..slack_tokens import token_identity
from ..workspace import (
    WORKSPACES_DISABLED_REFUSAL,
    WorkspaceSelectionRefused,
    trusted_repository_fact,
)

if TYPE_CHECKING:
    from .core import Kernel

from . import clock, constants, failures, routing, workspace
from .log import logger


async def _route_attachment_and_start(
    self: Kernel,
    qevent: QueuedTurn,
    thread_key: str,
    event: Event,
    boot_env: dict[str, str] | None,
    agent_id: uuid.UUID | None,
    packs: BehaviorPacks | None = None,
    *,
    workspace_deployment_id: uuid.UUID | None = None,
    agent_name: str | None = None,
    runner_resources: dict[str, Any] | None = None,
    source: TurnSource = TurnSource.SLACK,
    remaining_s: float | None = None,
    verified_review: VerifiedReviewFeedback | None = None,
    review_turn: QueuedTurn | None = None,
    workspace_inference: workspace._WorkspaceInferenceCarry,
) -> routing._RouteResult:
    """Put a file capability on the runner that will receive this turn."""

    lock_key = self._config.lock_key(thread_key)
    probe_budget_started = clock.time.monotonic()

    def current_probe_budget() -> float:
        if remaining_s is None:
            return constants._ATTACHMENT_HANDOFF_PROBE_TIMEOUT_S
        elapsed = clock.time.monotonic() - probe_budget_started
        return max(
            0.0,
            min(constants._ATTACHMENT_HANDOFF_PROBE_TIMEOUT_S, remaining_s - elapsed),
        )

    async def settle_shielded(task: asyncio.Task[Any]) -> None:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
                pass

    async with self._lock.hold(lock_key):
        expected_handle = await asyncio.to_thread(self._substrate.lookup, thread_key)
        if expected_handle is not None:
            if expected_handle.workspace_repo is not None:
                logger.info(
                    "retained workspace attachment refused for agent=%s "
                    "deployment=%s thread=%s reason=open_workspace followup=#2728",
                    agent_name,
                    workspace_deployment_id,
                    thread_key,
                )
                return routing._RouteResult(
                    steered=False,
                    canned_reply=constants._WORKSPACE_ATTACHMENT_REPLY,
                )
            repository_requested = workspace_inference.repo is not None
            if not repository_requested and workspace_deployment_id is not None:
                retained_fact = trusted_repository_fact(
                    event.text,
                    ignore_message=(
                        verified_review is not None
                        or source is TurnSource.WEBHOOK
                        or self._is_approval_resume(qevent.event_id)
                    ),
                )
                # With workspaces off a bare token has no allowlist to
                # confirm it, so it names no repository (#3671), exactly
                # as on the new-turn path below.
                repository_requested = retained_fact is not None and not (
                    self._workspace is None and retained_fact.bare
                )
            selected_repository = None
            if workspace_deployment_id is not None and self._workspace is not None:
                selected_repository = await asyncio.to_thread(
                    self._workspace.select_repository,
                    thread_key=thread_key,
                    deployment_id=workspace_deployment_id,
                    author=event.user,
                    repo_full_name=None,
                )
            repository_requested = repository_requested or selected_repository is not None
            if repository_requested and self._workspace is None:
                raise WorkspaceSelectionRefused(WORKSPACES_DISABLED_REFUSAL)
            if repository_requested:
                logger.info(
                    "retained repository attachment refused for agent=%s "
                    "deployment=%s thread=%s followup=#2728",
                    agent_name,
                    workspace_deployment_id,
                    thread_key,
                )
                return routing._RouteResult(
                    steered=False,
                    canned_reply=constants._REPOSITORY_ATTACHMENT_REPLY,
                )
            probe_budget = current_probe_budget()
            if probe_budget <= 0:
                raise TimeoutError("attachment handoff probe budget is exhausted")
            readiness = await self._cold_handoff_readiness(
                expected_handle,
                remaining_s=probe_budget,
            )
            if readiness != "ready":
                logger.info(
                    "retained attachment handoff refused for agent=%s "
                    "thread=%s phase=before_fetch state=%s",
                    agent_name,
                    thread_key,
                    readiness,
                )
                return routing._RouteResult(
                    steered=False,
                    canned_reply=(
                        constants._ACTIVE_ATTACHMENT_REPLY
                        if readiness == "active"
                        else constants._UNSAFE_ATTACHMENT_REPLY
                    ),
                )

    resolved_env, prepared = await self._resolve_attachments(
        qevent,
        boot_env,
        agent_id,
    )
    prepared_installed = False
    routed: routing._RouteResult | None = None
    try:
        try:
            async with self._lock.hold(lock_key):
                current_handle = await asyncio.to_thread(
                    self._substrate.lookup,
                    thread_key,
                )
                if current_handle != expected_handle:
                    return routing._RouteResult(
                        steered=False,
                        canned_reply=constants._CHANGED_ATTACHMENT_REPLY,
                    )
                if current_handle is not None:
                    probe_budget = current_probe_budget()
                    if probe_budget <= 0:
                        raise TimeoutError("attachment handoff probe budget is exhausted")
                    readiness = await self._cold_handoff_readiness(
                        current_handle,
                        remaining_s=probe_budget,
                    )
                    if readiness != "ready":
                        logger.info(
                            "retained attachment handoff refused for agent=%s "
                            "thread=%s phase=after_fetch state=%s",
                            agent_name,
                            thread_key,
                            readiness,
                        )
                        return routing._RouteResult(
                            steered=False,
                            canned_reply=(
                                constants._ACTIVE_ATTACHMENT_REPLY
                                if readiness == "active"
                                else constants._UNSAFE_ATTACHMENT_REPLY
                            ),
                        )
                    claim_started = clock.time.monotonic()

                    async def capture_handoff() -> tuple[
                        SandboxHandle | None,
                        BaseException | None,
                    ]:
                        try:
                            return (
                                await asyncio.to_thread(
                                    self._substrate.handoff,
                                    thread_key,
                                    expected=current_handle,
                                    env=resolved_env,
                                    workspace_repo=None,
                                    agent_name=agent_name,
                                    runner_resources=runner_resources,
                                    caller_run=current_handle.caller_run,
                                ),
                                None,
                            )
                        except BaseException as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
                            return None, exc

                    handoff_task = asyncio.create_task(capture_handoff())
                    try:
                        handle, handoff_error = await asyncio.shield(handoff_task)
                    except asyncio.CancelledError as cancellation:
                        await settle_shielded(handoff_task)
                        if handoff_task.cancelled():
                            prepared_installed = True
                        else:
                            _handle, handoff_error = handoff_task.result()
                            if handoff_error is not None:
                                route_task = asyncio.create_task(
                                    asyncio.to_thread(
                                        self._substrate.lookup,
                                        thread_key,
                                    )
                                )
                                await settle_shielded(route_task)
                                try:
                                    route_after_failure = route_task.result()
                                except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
                                    prepared_installed = True
                                else:
                                    prepared_installed = route_after_failure != current_handle
                            else:
                                prepared_installed = True
                        raise cancellation
                    if handoff_error is not None:
                        raise handoff_error
                    if handle is None:
                        raise RuntimeError("attachment handoff returned no handle")
                    prepared_installed = True
                    self._log_claim_latency(thread_key, claim_started)
                    if agent_id is not None:
                        self._register_run(agent_id, thread_key)
                    try:
                        event, remaining_s = await self._bind_publication_context(
                            event,
                            queued_event_id=qevent.event_id,
                            workspace_deployment_id=workspace_deployment_id,
                            handle=handle,
                            run=self._run_for_event(qevent.event_id),
                            remaining_s=remaining_s,
                        )
                        turn = await self._start_turn_under_hook_control(handle, event, remaining_s)
                    except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
                        self._unregister_run(agent_id, thread_key)
                        raise
                    routing._record_route("start")
                    routing._lifecycle_event("runner.turn.started", "start")
                    routed = routing._RouteResult(
                        steered=False,
                        handle=handle,
                        turn=turn,
                    )
                else:
                    try:
                        routed = await self._route_and_start(
                            thread_key,
                            event,
                            resolved_env,
                            packs,
                            queued_event_id=qevent.event_id,
                            workspace_deployment_id=workspace_deployment_id,
                            agent_name=agent_name,
                            runner_resources=runner_resources,
                            source=source,
                            remaining_s=remaining_s,
                            agent_id=agent_id,
                            verified_review=verified_review,
                            review_turn=review_turn,
                            workspace_inference=workspace_inference,
                            attachment_fresh_only=True,
                            approval_resume=self._is_approval_resume(qevent.event_id),
                        )
                    except RouteChangedError:
                        # Another worker bound a runner after the lookup
                        # above; the prepared files never reached it, so
                        # leave them uninstalled for the finally (#2739).
                        logger.info(
                            "attachment claim refused for agent=%s thread=%s: "
                            "route changed during claim",
                            agent_name,
                            thread_key,
                        )
                        return routing._RouteResult(
                            steered=False,
                            canned_reply=constants._CHANGED_ATTACHMENT_REPLY,
                        )
                    except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
                        try:
                            prepared_installed = (
                                await asyncio.to_thread(
                                    self._substrate.lookup,
                                    thread_key,
                                )
                                is not None
                            )
                        except BaseException as lookup_error:  # noqa: BLE001 - broad catch kept at a failure boundary
                            prepared_installed = True
                            logger.warning(
                                "attachment claim state lookup failed while preserving "
                                "the original routing error for thread %s: %r",
                                thread_key,
                                lookup_error,
                            )
                        raise
                    else:
                        prepared_installed = routed.handle is not None
        except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
            if routed is not None and routed.turn is not None:
                self._unregister_run(agent_id, thread_key)
                routed.turn.close()
            raise
        assert routed is not None
        return routed
    finally:
        if not prepared_installed:
            await self._discard_prepared_attachments(thread_key, prepared)


async def _discard_prepared_attachments(
    self: Kernel,
    thread_key: str,
    prepared: PreparedAttachments,
) -> None:
    lane = self._attachments
    assert lane is not None
    try:
        await asyncio.to_thread(
            lane.discard_prepared,
            thread_key=thread_key,
            prepared=prepared,
        )
    except Exception as exc:  # noqa: BLE001 - broad catch kept at a failure boundary
        reason = redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX]
        logger.warning(
            "attachment cleanup failed for thread %s: %s",
            thread_key,
            reason,
        )


async def _carried_attachment_env(
    self: Kernel, thread_key: str, agent_id: uuid.UUID
) -> dict[str, str]:
    """The thread's retained files for a text-only turn, or nothing (#4079).

    Best effort: a ledger or presign failure boots the runner without the
    carried files, which is what every text-only turn did before, rather
    than failing a turn that carries nothing of its own. The same holds for
    a slow store: the lookup runs on every text-only turn, steers included,
    so it is bounded by ``constants._ATTACHMENT_CARRY_TIMEOUT_S`` and abandoned past it.
    Waiting for a free slot counts against the same bound, and an abandoned
    lookup keeps its slot until the store answers.

    The warning names the failure's class and stage, never its message: a
    presign error can quote a signed URL, and ``redact_text`` does not
    recognize every store's signature parameter.
    """

    lane = self._attachments
    assert lane is not None  # guarded by the caller
    deadline = time.monotonic() + constants._ATTACHMENT_CARRY_TIMEOUT_S
    try:
        await asyncio.wait_for(
            self._carry_slots.acquire(), timeout=constants._ATTACHMENT_CARRY_TIMEOUT_S
        )
    except Exception:  # noqa: BLE001 -- a timeout, or anything else, skips carry
        logger.warning("retained attachment carry skipped for thread %s: lookups busy", thread_key)
        return {}
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        self._carry_slots.release()
        logger.warning("retained attachment carry skipped for thread %s: lookups busy", thread_key)
        return {}
    loop = asyncio.get_running_loop()
    done: asyncio.Future[dict[str, str]] = loop.create_future()

    def settle(result: dict[str, str] | None, error: BaseException | None) -> None:
        if done.done():
            return
        if error is not None:
            done.set_exception(error)
        else:
            done.set_result(result or {})

    def deliver(result: dict[str, str] | None, error: BaseException | None) -> None:
        # The turn may have given up and its loop may be gone; either way
        # there is nobody left to hand the answer to.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(settle, result, error)

    def lookup() -> None:
        try:
            result = lane.carry(thread_key, agent_id=str(agent_id))
        except BaseException as exc:  # noqa: BLE001 -- handed to the awaiting turn
            deliver(None, exc)
        else:
            deliver(result, None)
        finally:
            # The semaphore belongs to the loop, so the slot goes back there.
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(self._carry_slots.release)

    try:
        try:
            threading.Thread(target=lookup, name="attachment-carry", daemon=True).start()
        except BaseException:
            # The thread never ran, so its ``finally`` never will.
            self._carry_slots.release()
            raise
        return await asyncio.wait_for(done, timeout=remaining)
    except Exception as exc:  # noqa: BLE001
        stage = exc.stage if isinstance(exc, AttachmentResolutionError) else None
        logger.warning(
            "retained attachment carry failed for thread %s: %s stage=%s",
            thread_key,
            type(exc).__name__,
            stage,
        )
        return {}


async def _resolve_attachments(
    self: Kernel,
    qevent: QueuedTurn,
    boot_env: dict[str, str] | None,
    agent_id: uuid.UUID | None,
) -> tuple[dict[str, str], PreparedAttachments]:
    """Merge this turn's resolved attachment capability into the claim env.

    A turn carrying no attachments -- the overwhelming majority -- never
    reaches here: no channel round trip and no new object. It only asks the
    lane to re-mint the thread's retained set (``_carried_attachment_env``),
    which adds no key to the claim when there is none. Not even an
    empty-valued one, which an init container would read as "there is work
    here".
    """

    lane = self._attachments
    assert lane is not None  # guarded by the caller
    assert qevent.attachments
    handle = qevent.reply_handle
    identity = (
        token_identity(handle.adapter, handle.endpoint) if handle is not None else DEFAULT_IDENTITY
    )
    prepared = await asyncio.to_thread(
        lane.resolve,
        thread_key=routing._thread_key_for(qevent),
        agent_id=str(agent_id) if agent_id is not None else None,
        attachments=list(qevent.attachments),
        identity=identity,
        # ADR-0153: a channel-port turn's files come from its own adapter,
        # and the server-minted handle is what names that adapter.
        handle=handle,
    )
    return {**(boot_env or {}), **prepared.claim_env()}, prepared
