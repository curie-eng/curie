from __future__ import annotations

import asyncio
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
    PreparedThreadSet,
    UnavailableAttachment,
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

from ..ledger_client import LedgerNotDeployed
from . import clock, constants, failures, log, routing, workspace
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
    # The probes measure elapsed time from here against the budget as it was
    # here; ``remaining_s`` itself is charged for the rebuild further down.
    probe_remaining_s = remaining_s

    def current_probe_budget() -> float:
        if probe_remaining_s is None:
            return constants._ATTACHMENT_HANDOFF_PROBE_TIMEOUT_S
        elapsed = clock.time.monotonic() - probe_budget_started
        return max(
            0.0,
            min(constants._ATTACHMENT_HANDOFF_PROBE_TIMEOUT_S, probe_remaining_s - elapsed),
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

    # ADR 0205: with the ledger wired, a file turn's boot carries the whole
    # thread's set, read and prepared here, OUTSIDE the route lock. A failed
    # ledger read refuses the turn before any claim (stage=ledger), exactly as
    # a failed resolve does.
    resolved_env: dict[str, str]
    prepared: PreparedAttachments | PreparedThreadSet
    prepare_started = clock.time.monotonic()
    if self._attachment_ledger is not None:
        resolved_env, prepared = await self._prepare_thread_attachments(
            qevent,
            boot_env,
            agent_id,
            remaining_s,
        )
    else:
        resolved_env, prepared = await self._resolve_attachments(
            qevent,
            boot_env,
            agent_id,
        )
    if remaining_s is not None:
        # The rebuild ran on this delivery's clock: the claim and the turn get
        # only what is left of it.
        remaining_s = max(0.0, remaining_s - (clock.time.monotonic() - prepare_started))
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
            assert routed is not None
            if routed.handle is not None and routed.turn is not None:
                # The files are definitely on the runner that serves this
                # turn, and the route lock is released: record them (ADR 0205
                # decision 1). Never before the install, so a refused or
                # failed claim records nothing; a failure here is logged and
                # the turn the person is waiting on goes on.
                await self._append_thread_attachments(qevent, thread_key, agent_id, prepared)
        except BaseException:  # noqa: BLE001 - broad catch kept at a failure boundary
            if routed is not None and routed.turn is not None:
                self._unregister_run(agent_id, thread_key)
                routed.turn.close()
            raise
        return routed
    finally:
        if not prepared_installed:
            await self._discard_prepared_attachments(thread_key, prepared)


async def _discard_prepared_attachments(
    self: Kernel,
    thread_key: str,
    prepared: PreparedAttachments | PreparedThreadSet,
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


async def _resolve_attachments(
    self: Kernel,
    qevent: QueuedTurn,
    boot_env: dict[str, str] | None,
    agent_id: uuid.UUID | None,
) -> tuple[dict[str, str], PreparedAttachments]:
    """Merge this turn's resolved attachment capability into the claim env.

    A turn carrying no attachments -- the overwhelming majority -- does not
    consult the lane at all: no channel round trip, no object, and no key on
    the claim. Not even an empty-valued one, which an init container would
    read as "there is work here".
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


def _thread_set_budget(self: Kernel, remaining_s: float | None) -> float:
    """How long a boot may spend rebuilding the thread's set (ADR 0205).

    The configured prepare timeout, never past the delivery's remaining budget.
    """

    budget = float(self._config.attachment_thread_prepare_timeout_seconds)
    if remaining_s is not None:
        budget = min(budget, max(0.0, remaining_s))
    return budget


async def _thread_routes(self: Kernel, agent_id: uuid.UUID, deadline_epoch: float) -> list[Any]:
    """The agent's bindings as they are now, or none when they cannot be read.

    No routes only means an earlier file whose parked copy lapsed is named
    unavailable (``no_route``); it never fails a boot.
    """

    reader = getattr(self._binding, "routes_for_agent", None) if self._binding else None
    if reader is None:
        return []
    try:
        async with asyncio.timeout(max(0.0, deadline_epoch - clock.time.time())):
            return list(await reader(agent_id))
    except Exception as exc:  # noqa: BLE001 - an earlier file is best effort
        logger.warning(
            "agent routes unreadable for agent=%s; earlier attachments that need a "
            "re-fetch are unavailable: %s",
            agent_id,
            redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX],
        )
        return []


async def _prepare_thread_attachments(
    self: Kernel,
    qevent: QueuedTurn,
    boot_env: dict[str, str] | None,
    agent_id: uuid.UUID | None,
    remaining_s: float | None,
) -> tuple[dict[str, str], PreparedAttachments | PreparedThreadSet]:
    """A file turn's whole thread set (ADR 0205 decision 3), outside the lock.

    The ledger is read FIRST; any failure refuses the turn before the claim
    with stage ``ledger``, because naming this message's files needs every
    name the thread already holds. The lane then fetches the current files all
    or nothing and the earlier ones best effort, within the prepare budget.
    """

    lane = self._attachments
    ledger = self._attachment_ledger
    assert lane is not None and ledger is not None  # guarded by the caller
    assert qevent.attachments
    thread_key = routing._thread_key_for(qevent)
    if agent_id is None:
        raise AttachmentResolutionError("wiring", "attachment resolution requires a bound agent")
    budget = _thread_set_budget(self, remaining_s)
    deadline_epoch = clock.time.time() + budget
    try:
        async with asyncio.timeout(budget):
            ledger_refs = tuple(
                await ledger.query(agent_id=str(agent_id), thread_key=thread_key)
            )
    except LedgerNotDeployed:
        # Mixed rollout: the API does not serve the ledger yet. Behave exactly
        # as a worker with no ledger wired: this message's files only, and no
        # append. Never a refusal of the person's file.
        logger.warning(
            "thread attachment ledger is not deployed on the API (HTTP 404) for "
            "thread %s; resolving this message's files without the ledger",
            thread_key,
        )
        return await self._resolve_attachments(qevent, boot_env, agent_id)
    except Exception as exc:  # noqa: BLE001 - every ledger failure refuses the same way
        reason = redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX]
        raise AttachmentResolutionError(
            "ledger", f"thread attachment ledger read failed: {reason}"
        ) from exc
    current = list(qevent.attachments)
    current_ids = {attachment.id for attachment in current}
    routes = (
        await _thread_routes(self, agent_id, deadline_epoch)
        if any(ref.file_id not in current_ids for ref in ledger_refs)
        else []
    )
    handle = qevent.reply_handle
    identity = (
        token_identity(handle.adapter, handle.endpoint) if handle is not None else DEFAULT_IDENTITY
    )
    prepared = await asyncio.to_thread(
        lane.prepare_thread_set,
        thread_key=thread_key,
        agent_id=str(agent_id),
        ledger_refs=ledger_refs,
        current=current,
        event_id=qevent.event_id,
        identity=identity,
        # ADR-0153: a channel-port turn's files come from its own adapter,
        # and the server-minted handle is what names that adapter.
        handle=handle,
        routes=routes,
        deadline_epoch=deadline_epoch,
        ledger_unavailable=False,
    )
    return {**(boot_env or {}), **prepared.claim_env()}, prepared


async def _prepare_boot_thread_set(
    self: Kernel,
    thread_key: str,
    agent_id: uuid.UUID | None,
    remaining_s: float | None,
) -> PreparedThreadSet | None:
    """A text turn's boot rebuilds the thread's set (ADR 0205 decision 3).

    Called only for a turn that will BOOT a sandbox, under the route lock, so
    every await here is bounded by the prepare budget: the ledger read by it,
    and the lane by its deadline plus a hard ceiling after which the boot goes
    on without waiting (the abandoned prepare's bytes and owner record are
    swept by retention). Nothing here fails the boot: a failed ledger read
    boots with ``ledger_unavailable``, a failed or overrun prepare names every
    earlier file unavailable.

    Returns None when no ledger is wired, so the caller changes nothing.
    """

    # getattr: the claim path is also driven on a bare ``Kernel`` that never
    # ran ``__init__``, and a kernel with neither lane must change nothing.
    lane = getattr(self, "_attachments", None)
    ledger = getattr(self, "_attachment_ledger", None)
    if lane is None or ledger is None or agent_id is None:
        return None
    budget = _thread_set_budget(self, remaining_s)
    deadline_epoch = clock.time.time() + budget
    ledger_refs: tuple[Any, ...] = ()
    ledger_unavailable = False
    try:
        async with asyncio.timeout(budget):
            ledger_refs = tuple(
                await ledger.query(agent_id=str(agent_id), thread_key=thread_key)
            )
    except LedgerNotDeployed:
        # Mixed rollout: no ledger yet, so the boot is exactly today's.
        logger.warning(
            "thread attachment ledger is not deployed on the API (HTTP 404) for "
            "thread %s; booting without the thread's files",
            thread_key,
        )
        return None
    except Exception as exc:  # noqa: BLE001 - a text boot proceeds without earlier files
        ledger_unavailable = True
        logger.warning(
            "thread attachment ledger read failed for thread %s; booting without "
            "earlier files: %s",
            thread_key,
            redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX],
        )
    routes = await _thread_routes(self, agent_id, deadline_epoch) if ledger_refs else []
    try:
        ceiling = max(0.0, deadline_epoch - clock.time.time()) + (
            constants._THREAD_SET_PREPARE_GRACE_S
        )
        return await asyncio.wait_for(
            asyncio.to_thread(
                lane.prepare_thread_set,
                thread_key=thread_key,
                agent_id=str(agent_id),
                ledger_refs=ledger_refs,
                current=(),
                identity=DEFAULT_IDENTITY,
                handle=None,
                routes=routes,
                deadline_epoch=deadline_epoch,
                ledger_unavailable=ledger_unavailable,
            ),
            timeout=ceiling,
        )
    except Exception as exc:  # noqa: BLE001 - an earlier file never fails the boot
        reason = "deadline" if isinstance(exc, TimeoutError) else "fetch_failed"
        logger.warning(
            "thread attachment set could not be prepared for thread %s; booting with "
            "every earlier file unavailable (%s): %s",
            thread_key,
            reason,
            redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX],
        )
        return PreparedThreadSet(
            unavailable=tuple(
                UnavailableAttachment(name=ref.disk_name, reason=reason) for ref in ledger_refs
            ),
            ledger_unavailable=ledger_unavailable,
        )


async def _append_thread_attachments(
    self: Kernel,
    qevent: QueuedTurn,
    thread_key: str,
    agent_id: uuid.UUID | None,
    prepared: Any,
) -> None:
    """Record an installed turn's files in the thread's ledger, once.

    Idempotent at the API per (event, file id), so a redelivered turn records
    nothing twice. Bounded, and never raised: the turn the person is waiting on
    goes on, and a WARNING names the thread whose later boots will miss these
    files (ADR 0205 Consequences).
    """

    ledger = self._attachment_ledger
    refs = tuple(getattr(prepared, "append_refs", ()) or ())
    if ledger is None or not refs or agent_id is None:
        return
    try:
        async with asyncio.timeout(constants._THREAD_ATTACHMENT_APPEND_TIMEOUT_S):
            await ledger.append(
                agent_id=str(agent_id),
                thread_key=thread_key,
                event_id=qevent.event_id,
                refs=refs,
            )
    except Exception as exc:  # noqa: BLE001 - the turn is already running
        _record_ledger_append("failure")
        logger.warning(
            "thread attachment ledger append failed for thread %s event %s; these "
            "files will be missing from later boots: %s",
            thread_key,
            qevent.event_id,
            redact_text(failures._exception_reason(exc))[: constants._ESCALATION_DETAIL_MAX],
        )
    else:
        _record_ledger_append("success")


def _record_ledger_append(outcome: str) -> None:
    log.record_metric(
        "curie.attachments.ledger.append",
        attributes={"service.name": "curie-worker", "outcome": outcome},
    )


async def _discard_unclaimed_thread_set(
    self: Kernel,
    thread_key: str,
    prepared: Any,
    expected_handle: SandboxHandle | None,
) -> None:
    """Discard a text boot's thread set whose claim raised, if nothing took it.

    Only when the route afterwards is still what it was before the claim (none,
    or the handle a failed handoff was replacing): then no new sandbox can be
    redeeming these capabilities. Any other answer, including a lookup that
    fails, leaves the bytes to the retention sweep rather than risk removing
    files a booting sandbox is about to fetch.
    """

    if not (prepared.object_keys or getattr(prepared, "owner_recorded", False)):
        return
    try:
        route = await asyncio.to_thread(self._substrate.lookup, thread_key)
    except Exception:  # noqa: BLE001 - retention sweeps what is left
        return
    if route is not None and route != expected_handle:
        return
    await self._discard_prepared_attachments(thread_key, prepared)
