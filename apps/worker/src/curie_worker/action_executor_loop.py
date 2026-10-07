"""The worker executor loop: one connector action execution at a time (ADR 0121).

@spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-7
@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-14 @spec ACTION-EXECUTOR-15
@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-21 @spec ACTION-EXECUTOR-22

Runs beside the connector reconcile loop, never inside the consumer: no stream,
no thread lock (each execution has its own ``action-exec:<id>`` key), no turn
marker. The execution state machine on the API carries the no-retry rule the
markers carry for turns.

A restore, in order (each failure before ``dispatched`` reports ``refused`` with
its code, which is a provable non-write):

1. Claim under a lease. The claim route is the ACTION-EXECUTOR-17 sweeper.
2. The kill switch; stopped or unreadable is ``agent_stopped``, with no sandbox.
3. The ledger row; the ruling's ``arguments_sha256`` must cover its
   ``{target, prior_state}`` (``arguments_mismatch``).
4. The connector's Deployment: the pinned digest serving
   (``connector_digest_unavailable``) and ``restore`` in the caller proxy's
   gated set, so the proxy spends the grant (``tool_not_grant_bound``). The
   local tier has no Deployment and no proxy, so every restore there refuses
   ``tool_not_grant_bound``.
5. An executor sandbox under the agent's own binding, stripped to AE-5
   (``sandbox_unavailable``).
6. ``list``; the ``restore`` and ``observe_version`` pair must be advertised.
7. The pinned digest again, immediately before ``observe``.
8. ``observe``; the API compares the version and may end the run.
9. The kill switch again, then the ``dispatched`` commit. Only a confirmed
   commit lets the request leave.
10. One grant, one ``call``; a call whose answer is lost is ``indeterminate``
    and is never repeated.
11. The outcome report, retried for the same fence; a lost report is left to
    the lease sweep. The sandbox is released on every path.

A probe is a ``list`` bracketed by the digest check, reporting the verbs that
meet ACTION-EXECUTOR-13's rule; it never observes, dispatches or calls.

Nothing here logs arguments, envelopes, targets, versions, grants or tokens:
log lines carry the execution id, kind, state, stage, code and connector only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any, Final, Protocol

import httpx
from aci_protocol import BootEnv
from curie_connector_proxy.server import GATED_TOOLS_ENV
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode
from plugin_format.connector_render import CALLER_PROXY_CONTAINER

from . import connector_grant
from .action_digest import READ_TIMEOUT_SECONDS, observe
from .action_executor import (
    OBSERVE_TOOL,
    RESTORE_TOOL,
    ExecutorRefusal,
    call_outcome,
    restore_call,
)
from .caller_token import signing_key
from .runner_client import EXECUTOR_MODE, RUNNER_MODE_ENV, ExecuteRefused, RunnerClient
from .sandbox import EXECUTOR_THREAD_KEY_PREFIX
from .sandbox.claim_tokens import executor_withheld_names

if TYPE_CHECKING:
    from .sandbox import SandboxSubstrate
    from .sandbox.types import SandboxHandle

logger = logging.getLogger(__name__)

# How many times one API transition or report is sent for the same fence. Each
# route is idempotent for the same fence and payload (ACTION-EXECUTOR-18), so a
# resend after a lost answer replays the stored row.
MAX_SENDS: Final = 3
_API_TIMEOUT_S: Final = 10.0

# The code a pre-dispatch run reports when the platform side, not the
# connector, stopped it: the API could not be reached for a transition, or
# refused the dispatch commit. The same code the claim sweep uses when a holder
# vanished before dispatch.
_PLATFORM_UNAVAILABLE: Final = "runner_unavailable"
_LOST: Final = "response_lost"
_DEADLINE: Final = "deadline_exceeded"

# @spec ACTION-EXECUTOR-4. The boot env fields an executor sandbox never carries:
# no history, memory, state, progress or issue token or ref, no model credential
# declaration, and nothing a turn's approval resume carries.
_EXCLUDED_FIELDS: Final = (
    "history_ref",
    "history_token",
    "history_max_bytes",
    "history_max_turns",
    "memory_ref",
    "memory_token",
    "memory_writes",
    "memory_max_facts",
    "channel_memory_ref",
    "state_token",
    "state_url",
    "progress_token",
    "progress_url",
    "issue_read_token",
    "issue_read_url",
    "credentials_ref",
    "model_env_key",
    "approval_decision",
    "approval_grant_arguments",
    "approval_grant_tool",
    "approval_resumed_kind",
    "attachments_manifest",
)
_EXCLUDED_ENVS: Final = frozenset(BootEnv.env_key(name) for name in _EXCLUDED_FIELDS)
_RUNNER_TOKEN_ENV: Final = BootEnv.env_key("runner_token")
_SECRET_KEYS_ENV: Final = BootEnv.env_key("connector_secret_keys")
# The runner-private grant variable (not a BootEnv field). The grant never rides
# claim env (ACTION-EXECUTOR-5); it rides the ``call`` request only.
_GRANT_ENV: Final = "CURIE_CONNECTOR_TOOL_GRANT"

_TERMINAL: Final = frozenset({"confirmed", "failed", "indeterminate", "refused"})


class KillSwitchReader(Protocol):
    async def is_killed(self, agent_id: uuid.UUID) -> bool: ...


class DeploymentReader(Protocol):
    def read_namespaced_deployment(self, name: str, namespace: str, **kwargs: Any) -> Any: ...


DeploymentNameResolver = Callable[[str | None, str], Awaitable[str | None]]
InForceDigest = Callable[[str, str], Awaitable[str | None]]


@dataclass(frozen=True)
class ExecutorBoot:
    """The agent's resolved binding, as an ordinary turn would boot with it.

    ``boot_env`` is the binding's whole boot env; the loop strips it to
    ACTION-EXECUTOR-5. ``header_secret_names`` are the connector secrets the
    target connector's derived MCP entry headers expand; every other connector
    secret is withheld.
    """

    boot_env: Mapping[str, str]
    header_secret_names: frozenset[str]
    agent_name: str


class ExecutorBootResolver(Protocol):
    """Resolves the binding for ``(agent_id, connector)`` under the execution's thread key."""

    def __call__(
        self, agent_id: str, connector: str, *, thread_key: str
    ) -> Awaitable[ExecutorBoot]: ...


@dataclass(frozen=True)
class Execution:
    """One claimed execution: identity, fence and the two digests it pins."""

    id: str
    kind: str
    agent_id: str
    connector: str
    subject_action_id: str | None
    attempt: int
    lease_owner: str
    connector_digest: str | None
    arguments_sha256: str | None

    @classmethod
    def from_claim(cls, body: Mapping[str, Any], lease_owner: str) -> Execution:
        subject = body.get("subject_action_id")
        return cls(
            id=str(body["id"]),
            kind=str(body["kind"]),
            agent_id=str(body["agent_id"]),
            connector=str(body["connector"]),
            subject_action_id=str(subject) if subject else None,
            attempt=int(body["attempt"]),
            lease_owner=str(body.get("lease_owner") or lease_owner),
            connector_digest=body.get("connector_digest"),
            arguments_sha256=body.get("arguments_sha256"),
        )

    def fence(self) -> dict[str, Any]:
        return {"lease_owner": self.lease_owner, "attempt": self.attempt}

    @property
    def thread_key(self) -> str:
        return f"{EXECUTOR_THREAD_KEY_PREFIX}{self.id}"


@dataclass(frozen=True)
class ApiAnswer:
    """One transition's answer: the status, and the stored row when it is 200."""

    status: int
    row: Mapping[str, Any] | None

    @property
    def state(self) -> str | None:
        return None if self.row is None else str(self.row.get("state"))


class ExecutionApi:
    """The executor routes and the ledger read, as the worker reaches them.

    @spec ACTION-EXECUTOR-18. Transitions present the fence under the internal
    worker token; the ledger read (``target`` and ``prior_state``) goes under
    the platform key. Bodies are never logged.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        worker_token: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._base = api_base_url.rstrip("/")
        self._client = client
        self._key_headers = {"X-API-Key": api_key} if api_key else {}
        self._worker_headers = {**self._key_headers, "X-Curie-Worker-Token": worker_token}

    async def claim(self, *, lease_owner: str, lease_seconds: int) -> Mapping[str, Any] | None:
        """The claimed execution, or None when nothing is claimable (204)."""

        response = await self._client.post(
            f"{self._base}/action-executions/claim",
            json={"lease_owner": lease_owner, "lease_seconds": lease_seconds},
            headers=self._worker_headers,
            timeout=_API_TIMEOUT_S,
        )
        if response.status_code == 204:
            return None
        response.raise_for_status()
        body: Mapping[str, Any] = response.json()
        return body

    async def ledger(self, action_id: str) -> Mapping[str, Any] | None:
        """The recorded action, or None when the ledger has no such row."""

        response = await self._client.get(
            f"{self._base}/actions/{action_id}",
            headers=self._key_headers,
            timeout=_API_TIMEOUT_S,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        body: Mapping[str, Any] = response.json()
        return body

    async def transition(
        self, execution: Execution, route: str, body: Mapping[str, Any]
    ) -> ApiAnswer:
        """POST one fenced transition (``observation``, ``dispatch`` or ``outcome``)."""

        response = await self._client.post(
            f"{self._base}/action-executions/{execution.id}/{route}",
            json={**execution.fence(), **body},
            headers=self._worker_headers,
            timeout=_API_TIMEOUT_S,
        )
        row = response.json() if response.status_code == 200 else None
        return ApiAnswer(status=response.status_code, row=row)


class _Abandon(Exception):  # noqa: N818 -- a control signal, not an error
    """Stop this run without a report; the row waits for its lease to expire."""

    def __init__(self, stage: str) -> None:
        super().__init__(stage)
        self.stage = stage


class _Refuse(Exception):  # noqa: N818 -- a control signal, not an error
    """End this run ``refused`` with ``code``: provably no write call was made."""

    def __init__(self, code: str, stage: str) -> None:
        super().__init__(code)
        self.code = code
        self.stage = stage


@dataclass
class _Run:
    """What one run ended with, for its single telemetry point."""

    state: str = "unreported"
    stage: str = "claim"
    code: str | None = None
    # Set once a sandbox claim was attempted, so release runs only when needed.
    sandbox: bool = False


def executor_env(boot: ExecutorBoot) -> dict[str, str]:
    """The executor sandbox's claim env. @spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-5.

    The binding's boot env minus every excluded field, every model credential
    (the names ``CURIE_MODEL_ENV_KEY`` declares included), any grant, and every
    connector secret outside the target's header set; the secret marker is
    narrowed to match. Adds the runner-private executor mode and a runner token
    of its own, minted here so no ordinary turn's bearer is reused.
    """

    env = dict(boot.boot_env)
    marker = env.get(_SECRET_KEYS_ENV, "")
    marked = {name for name in marker.split(",") if name}
    dropped = (
        _EXCLUDED_ENVS
        | executor_withheld_names(env)
        | (marked - boot.header_secret_names)
        | {_GRANT_ENV}
    )
    kept = {
        key: value
        for key, value in env.items()
        if key not in dropped and not value.startswith(f"{connector_grant.PREFIX}.")
    }
    narrowed = ",".join(name for name in marker.split(",") if name in boot.header_secret_names)
    if narrowed:
        kept[_SECRET_KEYS_ENV] = narrowed
    else:
        kept.pop(_SECRET_KEYS_ENV, None)
    kept[RUNNER_MODE_ENV] = EXECUTOR_MODE
    kept[_RUNNER_TOKEN_ENV] = secrets.token_urlsafe(32)
    return kept


def _required(schema: object) -> frozenset[str]:
    if not isinstance(schema, Mapping):
        return frozenset()
    required = schema.get("required")
    if not isinstance(required, list):
        return frozenset()
    return frozenset(item for item in required if isinstance(item, str))


def _read_only(tool: Mapping[str, Any]) -> bool:
    annotations = tool.get("annotations")
    return isinstance(annotations, Mapping) and annotations.get("readOnlyHint") is True


def advertised_verbs(tools: object) -> list[str]:
    """The paired verbs whose ``tools/list`` entry meets the capability rule.

    @spec ACTION-EXECUTOR-13: ``restore`` advertised, not annotated read-only,
    requiring ``target`` and ``prior_state``; ``observe_version`` advertised,
    annotated read-only, requiring ``target``. A name listed twice is judged by
    its first entry.
    """

    by_name: dict[str, Mapping[str, Any]] = {}
    for tool in tools if isinstance(tools, list) else ():
        if isinstance(tool, Mapping) and isinstance(tool.get("name"), str):
            by_name.setdefault(tool["name"], tool)
    verbs: list[str] = []
    restore = by_name.get(RESTORE_TOOL)
    if (
        restore is not None
        and not _read_only(restore)
        and {"target", "prior_state"} <= _required(restore.get("input_schema"))
    ):
        verbs.append(RESTORE_TOOL)
    observe_tool = by_name.get(OBSERVE_TOOL)
    if (
        observe_tool is not None
        and _read_only(observe_tool)
        and "target" in _required(observe_tool.get("input_schema"))
    ):
        verbs.append(OBSERVE_TOOL)
    return verbs


def _gated_patterns(body: Mapping[str, Any]) -> tuple[str, ...]:
    """The caller proxy's gated tool patterns, read from the rendered Deployment."""

    spec = body.get("spec") or {}
    containers = ((spec.get("template") or {}).get("spec") or {}).get("containers") or []
    for container in containers:
        if not isinstance(container, Mapping) or container.get("name") != CALLER_PROXY_CONTAINER:
            continue
        for entry in container.get("env") or ():
            if isinstance(entry, Mapping) and entry.get("name") == GATED_TOOLS_ENV:
                try:
                    parsed = json.loads(str(entry.get("value") or "[]"))
                except ValueError:
                    return ()
                if isinstance(parsed, list):
                    return tuple(item for item in parsed if isinstance(item, str))
                return ()
    return ()


def tool_is_gated(patterns: tuple[str, ...], connector: str, tool: str) -> bool:
    """The proxy's own match (``curie_connector_proxy.server._is_gated``).

    Both spellings, ``<connector>/<tool>`` and ``mcp__<connector>__<tool>``,
    against each pattern with case-sensitive glob matching.
    """

    candidates = (f"{connector}/{tool}", f"mcp__{connector}__{tool}")
    return any(fnmatchcase(c, pattern) for pattern in patterns for c in candidates)


def _stage(state: str, code: str | None) -> str:
    if state == "refused":
        return "pre_dispatch"
    if state == "failed" and code in {
        "version_conflict_at_write",
        "sealing_key_unavailable",
        "snapshot_unopenable",
    }:
        return "connector_refusal"
    if state in {"failed", "indeterminate"}:
        return "post_dispatch"
    return "none"


class ActionExecutorLoop:
    """Claims, runs and reports connector action executions, one at a time."""

    def __init__(
        self,
        *,
        api: ExecutionApi,
        substrate: SandboxSubstrate,
        runner: RunnerClient,
        killswitch: KillSwitchReader,
        deployments: DeploymentReader | None,
        namespace: str,
        deployment_name: DeploymentNameResolver,
        in_force_digest: InForceDigest,
        executor_boot: ExecutorBootResolver,
        grant_signing_key: str,
        lease_owner: str,
        lease_seconds: int,
        dispatch_deadline_s: float,
        interval_seconds: float,
    ) -> None:
        self._api = api
        self._substrate = substrate
        self._runner = runner
        self._killswitch = killswitch
        # None on the local tier: no reconciled Deployment and no caller proxy.
        self._deployments = deployments
        self._namespace = namespace
        self._deployment_name = deployment_name
        self._in_force_digest = in_force_digest
        self._executor_boot = executor_boot
        self._grant_signing_key = grant_signing_key
        # Checked once here so a missing or malformed key refuses before the
        # ``dispatched`` commit rather than failing the mint after it.
        try:
            signing_key(grant_signing_key)
        except ValueError:
            self._grant_key_ok = False
        else:
            self._grant_key_ok = True
        self._lease_owner = lease_owner
        self._lease_seconds = lease_seconds
        self._dispatch_deadline_s = dispatch_deadline_s
        self._interval_seconds = interval_seconds

    # -- the loop ------------------------------------------------------------

    async def run_forever(self, shutdown: asyncio.Event) -> None:
        """Claim until nothing is claimable, then wait one interval or shutdown."""

        while not shutdown.is_set():
            claimed = False
            try:
                claimed = await self.run_once()
            except Exception as exc:  # noqa: BLE001 -- one bad pass never ends the loop
                logger.warning("action executor pass failed error=%s", type(exc).__name__)
            if claimed:
                continue
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass

    async def run_once(self) -> bool:
        """Claim and run one execution. True when one was claimed."""

        body = await self._api.claim(
            lease_owner=self._lease_owner, lease_seconds=self._lease_seconds
        )
        if body is None:
            return False
        execution = Execution.from_claim(body, self._lease_owner)
        run = _Run()
        with operation_span(
            "curie.action_executor.execution",
            kind=SpanKind.INTERNAL,
            attributes={
                "service.name": "curie-worker",
                "operation": f"action-exec-{execution.kind}",
            },
        ) as span:
            try:
                await self._run(execution, run)
            except Exception as exc:  # noqa: BLE001 -- the row's state machine decides
                logger.warning(
                    "action execution %s stopped kind=%s stage=%s connector=%s error=%s",
                    execution.id,
                    execution.kind,
                    run.stage,
                    execution.connector,
                    type(exc).__name__,
                )
            if run.state != "confirmed" and hasattr(span, "set_status"):
                span.set_status(StatusCode.ERROR)
            span.add_event("action_executor.finished", {"outcome": run.state})
        self._record(execution, run)
        return True

    # -- one execution -------------------------------------------------------

    async def _run(self, execution: Execution, run: _Run) -> None:
        try:
            try:
                if execution.kind == "restore":
                    await self._restore(execution, run)
                elif execution.kind == "probe":
                    await self._probe(execution, run)
                else:
                    # Forward execution (ACTION-EXECUTOR-19) is not served by this
                    # loop yet; its authority cannot be verified here.
                    raise _Refuse("authority_unavailable", "kind")
            except _Refuse as refusal:
                run.stage = refusal.stage
                await self._report(execution, run, "refused", refusal.code)
            except _Abandon as abandoned:
                run.stage = abandoned.stage
                logger.info(
                    "action execution %s left for its lease kind=%s stage=%s connector=%s",
                    execution.id,
                    execution.kind,
                    abandoned.stage,
                    execution.connector,
                )
        finally:
            if run.sandbox:
                await self._release(execution)

    async def _restore(self, execution: Execution, run: _Run) -> None:
        run.stage = "killswitch"
        await self._check_killswitch(execution)
        run.stage = "ledger"
        target, prior_state = await self._ruled_arguments(execution)
        run.stage = "deployment"
        await self._require_serving(execution, gated_tool=RESTORE_TOOL)
        run.stage = "sandbox"
        run.sandbox = True
        handle, agent_name = await self._claim_sandbox(execution)
        run.stage = "list"
        tools = await self._list(execution, handle)
        names = {t.get("name") for t in tools if isinstance(t, Mapping)}
        if not {RESTORE_TOOL, OBSERVE_TOOL} <= names:
            raise _Refuse("restore_not_advertised", "list")
        if set(advertised_verbs(tools)) != {RESTORE_TOOL, OBSERVE_TOOL}:
            raise _Refuse("restore_schema_mismatch", "list")
        restore_schema = next(
            t.get("input_schema")
            for t in tools
            if isinstance(t, Mapping) and t.get("name") == RESTORE_TOOL
        )
        run.stage = "digest"
        await self._require_serving(execution)
        run.stage = "observe"
        version = await self._observe(execution, handle, target)
        await self._post_observation(execution, run, version)
        if run.state == "refused":
            return
        run.stage = "killswitch"
        await self._check_killswitch(execution)
        # The exact text, computed before the commit so any refusal here is
        # still pre-dispatch. ``version`` equals the recorded post_version once
        # the API answered the observation without refusing.
        try:
            arguments = restore_call(
                target=target,
                prior_state=prior_state,
                recorded_version=version or "",
                restore_input_schema=restore_schema,
                arguments_sha256=execution.arguments_sha256 or "",
            )
        except ExecutorRefusal as refusal:
            raise _Refuse(refusal.code, "arguments") from None
        except ValueError:
            raise _Refuse("arguments_mismatch", "arguments") from None
        if not self._grant_key_ok:
            # No grant can be minted, so the proxy would refuse the call.
            raise _Refuse("tool_not_grant_bound", "grant")
        run.stage = "dispatch"
        await self._dispatch(execution)
        run.stage = "call"
        state, code = await self._call(execution, handle, agent_name, arguments)
        run.stage = "report"
        await self._report(execution, run, state, code)

    async def _probe(self, execution: Execution, run: _Run) -> None:
        run.stage = "killswitch"
        await self._check_killswitch(execution)
        run.stage = "digest"
        await self._require_serving(execution)
        run.stage = "sandbox"
        run.sandbox = True
        handle, _agent_name = await self._claim_sandbox(execution)
        run.stage = "list"
        tools = await self._list(execution, handle)
        run.stage = "digest"
        await self._require_serving(execution)
        run.stage = "report"
        await self._report(execution, run, "confirmed", None, advertised=advertised_verbs(tools))

    # -- checks --------------------------------------------------------------

    async def _check_killswitch(self, execution: Execution) -> None:
        """@spec ACTION-EXECUTOR-21: stopped or unreadable refuses ``agent_stopped``."""

        try:
            killed = await self._killswitch.is_killed(uuid.UUID(execution.agent_id))
        except Exception:  # noqa: BLE001 -- an unreadable switch is a stop
            killed = True
        if killed:
            raise _Refuse("agent_stopped", "killswitch")

    async def _ruled_arguments(
        self, execution: Execution
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        """``target`` and ``prior_state``, checked against the ruling's digest.

        @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-18. The read is sent up to
        ``MAX_SENDS`` times on a transport error or a 5xx; a ledger that never
        answers refuses ``runner_unavailable`` (a pre-dispatch refusal, before
        any sandbox). No such row, a row without the two keys, or a digest that
        differs refuses ``arguments_mismatch``.
        """

        if execution.subject_action_id is None or not execution.arguments_sha256:
            raise _Refuse("arguments_mismatch", "ledger")
        row: Mapping[str, Any] | None = None
        for attempt in range(MAX_SENDS):
            try:
                row = await self._api.ledger(execution.subject_action_id)
                break
            except httpx.HTTPError:
                if attempt == MAX_SENDS - 1:
                    raise _Refuse(_PLATFORM_UNAVAILABLE, "ledger") from None
        target = row.get("target") if row is not None else None
        prior_state = row.get("prior_state") if row is not None else None
        if not isinstance(target, Mapping) or not isinstance(prior_state, Mapping):
            raise _Refuse("arguments_mismatch", "ledger")
        try:
            restore_call(
                target=target,
                prior_state=prior_state,
                recorded_version="",
                restore_input_schema=None,
                arguments_sha256=execution.arguments_sha256,
            )
        except ExecutorRefusal as refusal:
            raise _Refuse(refusal.code, "ledger") from None
        except ValueError:
            raise _Refuse("arguments_mismatch", "ledger") from None
        return target, prior_state

    async def _require_serving(
        self, execution: Execution, *, gated_tool: str | None = None
    ) -> None:
        """@spec ACTION-EXECUTOR-14: the pinned digest, rolled out, or nothing.

        The agent's in-force version must render the connector at the
        execution's digest, and the owned Deployment must show a completed
        rollout at that exact image. With ``gated_tool``, the caller proxy must
        also gate it (@spec ACTION-EXECUTOR-7), or the proxy would not spend
        the grant. The local tier has no Deployment: a restore refuses
        ``tool_not_grant_bound`` and a probe ``connector_digest_unavailable``.
        """

        unavailable = _Refuse("connector_digest_unavailable", "digest")
        if self._deployments is None:
            raise _Refuse("tool_not_grant_bound", "gate") if gated_tool else unavailable
        digest = execution.connector_digest
        if not digest:
            raise unavailable
        try:
            in_force = await asyncio.wait_for(
                self._in_force_digest(execution.agent_id, execution.connector),
                timeout=_API_TIMEOUT_S,
            )
            body = await asyncio.wait_for(
                self._read_deployment(execution), timeout=READ_TIMEOUT_SECONDS
            )
        except Exception as exc:  # noqa: BLE001 -- an unreadable digest is not serving
            logger.info(
                "action execution %s digest unreadable connector=%s error=%s",
                execution.id,
                execution.connector,
                type(exc).__name__,
            )
            raise unavailable from None
        observed = observe(body) if body is not None else None
        if (
            in_force != digest
            or observed is None
            or not observed.rolled_out
            or observed.digest != digest
        ):
            raise unavailable
        if gated_tool is not None and body is not None:
            if not tool_is_gated(_gated_patterns(body), execution.connector, gated_tool):
                raise _Refuse("tool_not_grant_bound", "gate")

    async def _read_deployment(self, execution: Execution) -> dict[str, Any] | None:
        deployments = self._deployments
        assert deployments is not None
        name = await self._deployment_name(execution.agent_id, execution.connector)
        if not name:
            return None

        def get() -> dict[str, Any]:
            response = deployments.read_namespaced_deployment(
                name,
                self._namespace,
                _preload_content=False,
                _request_timeout=READ_TIMEOUT_SECONDS,
            )
            body = json.loads(response.data)
            if not isinstance(body, dict):
                raise ValueError("Deployment body is not an object")
            return body

        return await asyncio.to_thread(get)

    # -- the sandbox and the runner -----------------------------------------

    async def _claim_sandbox(self, execution: Execution) -> tuple[SandboxHandle, str]:
        """@spec ACTION-EXECUTOR-5: the agent's pool, the stripped env, fresh only.

        Returns the handle and the agent name the grant is signed for.
        """

        try:
            boot = await self._executor_boot(
                execution.agent_id, execution.connector, thread_key=execution.thread_key
            )
            env = executor_env(boot)
            handle = await asyncio.to_thread(
                self._substrate.claim,
                execution.thread_key,
                env=env,
                agent_name=boot.agent_name,
                fresh_only=True,
                executor_secret_names=frozenset(boot.header_secret_names),
            )
        except Exception as exc:  # noqa: BLE001 -- quota, timeout or route race alike
            logger.info(
                "action execution %s sandbox unavailable connector=%s error=%s",
                execution.id,
                execution.connector,
                type(exc).__name__,
            )
            raise _Refuse("sandbox_unavailable", "sandbox") from None
        return handle, boot.agent_name

    def _request(
        self,
        execution: Execution,
        phase: str,
        *,
        tool: str | None = None,
        arguments: str | None = None,
        grant: str | None = None,
        target: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "execution_id": execution.id,
            "phase": phase,
            "connector": execution.connector,
            "tool": tool,
            "arguments": arguments,
            "grant": grant,
            "target": dict(target) if target is not None else None,
        }

    async def _list(self, execution: Execution, handle: SandboxHandle) -> list[Any]:
        try:
            reply = await self._runner.execute(
                handle.base_url, self._request(execution, "list"), token=handle.token
            )
        except ExecuteRefused as refused:
            raise _Refuse(refused.code, "list") from None
        except Exception:  # noqa: BLE001 -- a runner the worker cannot drive
            raise _Refuse("runner_unavailable", "list") from None
        tools = reply.get("tools")
        return list(tools) if isinstance(tools, list) else []

    async def _observe(
        self, execution: Execution, handle: SandboxHandle, target: Mapping[str, Any]
    ) -> str | None:
        """@spec ACTION-EXECUTOR-15: the version observed now, passed on unjudged."""

        try:
            reply = await self._runner.execute(
                handle.base_url,
                self._request(execution, "observe", target=target),
                token=handle.token,
            )
        except ExecuteRefused as refused:
            raise _Refuse(refused.code, "observe") from None
        except Exception:  # noqa: BLE001 -- a runner the worker cannot drive
            raise _Refuse("runner_unavailable", "observe") from None
        version = reply.get("version")
        return version if isinstance(version, str) else None

    async def _call(
        self, execution: Execution, handle: SandboxHandle, agent_name: str, arguments: str
    ) -> tuple[str, str | None]:
        """One grant, one ``call``. @spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-17.

        Past ``dispatched`` nothing is ``refused``: a reply maps through
        ``call_outcome``, and anything else may have reached the connector, so
        it is ``indeterminate`` and never repeated.
        """

        started = time.monotonic()
        # The module attribute, so one mint per call is observable and the
        # proxy's byte equality is the shared vector's.
        grant = connector_grant.mint(
            self._grant_signing_key,
            agent=agent_name,
            connector=execution.connector,
            tool=RESTORE_TOOL,
            args=arguments,
            exp=int(time.time() + self._dispatch_deadline_s),
            jti=str(uuid.uuid4()),
        )
        try:
            reply = await self._runner.execute(
                handle.base_url,
                self._request(
                    execution, "call", tool=RESTORE_TOOL, arguments=arguments, grant=grant
                ),
                token=handle.token,
                remaining_s=self._dispatch_deadline_s,
            )
        except Exception:  # noqa: BLE001 -- ExecuteRefused included: never refused here
            if time.monotonic() - started >= self._dispatch_deadline_s:
                return "indeterminate", _DEADLINE
            return "indeterminate", _LOST
        return call_outcome(
            is_error=bool(reply.get("is_error")), structured=reply.get("structured")
        )

    # -- API transitions -----------------------------------------------------

    async def _send(
        self, execution: Execution, route: str, body: Mapping[str, Any]
    ) -> ApiAnswer | None:
        """Send one fenced transition up to ``MAX_SENDS`` times; None if never answered."""

        for _ in range(MAX_SENDS):
            try:
                return await self._api.transition(execution, route, body)
            except httpx.HTTPError:
                continue
        return None

    async def _post_observation(self, execution: Execution, run: _Run, version: str | None) -> None:
        """@spec ACTION-EXECUTOR-15: the API compares; a conflict ends the run."""

        answer = await self._send(execution, "observation", {"version": version})
        if answer is None:
            raise _Refuse(_PLATFORM_UNAVAILABLE, "observation")
        if answer.status != 200:
            # A stale fence or an expired lease: this holder moves nothing more.
            raise _Abandon("observation")
        if answer.state == "refused":
            run.state = "refused"
            run.code = str((answer.row or {}).get("refusal_code") or "version_conflict")
            return
        if answer.state != "claimed":
            raise _Abandon("observation")

    async def _dispatch(self, execution: Execution) -> None:
        """@spec ACTION-EXECUTOR-17: no confirmed ``dispatched`` commit, no request."""

        answer = await self._send(execution, "dispatch", {})
        if answer is not None and answer.status == 200 and answer.state == "dispatched":
            return
        # Never confirmed, or refused by the API (409, or 503 with the executor
        # switched off). A refusal is offered; the API accepts it only if the
        # commit did not happen, and otherwise the lease sweep ends the row.
        raise _Refuse(_PLATFORM_UNAVAILABLE, "dispatch")

    async def _report(
        self,
        execution: Execution,
        run: _Run,
        state: str,
        code: str | None,
        *,
        advertised: list[str] | None = None,
    ) -> None:
        """@spec ACTION-EXECUTOR-18: report once, resend for the same fence, never re-call."""

        body: dict[str, Any] = {"state": state, "code": code}
        if advertised is not None:
            body["advertised"] = advertised
        answer = await self._send(execution, "outcome", body)
        run.code = code
        if answer is not None and answer.status == 200 and answer.state in _TERMINAL:
            run.state = state
            return
        run.state = "unreported"
        logger.info(
            "action execution %s outcome not recorded kind=%s state=%s code=%s connector=%s",
            execution.id,
            execution.kind,
            state,
            code,
            execution.connector,
        )

    # -- release and telemetry ----------------------------------------------

    async def _release(self, execution: Execution) -> None:
        """@spec ACTION-EXECUTOR-5: on every path; a release error is logged."""

        try:
            await asyncio.to_thread(self._substrate.release, execution.thread_key)
        except Exception as exc:  # noqa: BLE001 -- the outcome already stands
            logger.warning(
                "action execution %s sandbox release failed connector=%s error=%s",
                execution.id,
                execution.connector,
                type(exc).__name__,
            )

    def _record(self, execution: Execution, run: _Run) -> None:
        """@spec ACTION-EXECUTOR-22: kind, state, stage, code and connector only."""

        stage = _stage(run.state, run.code)
        logger.info(
            "action execution %s finished kind=%s state=%s stage=%s code=%s connector=%s",
            execution.id,
            execution.kind,
            run.state,
            run.stage if run.state == "unreported" else stage,
            run.code,
            execution.connector,
        )
        try:
            record_metric(
                "curie.action_executor.execution",
                attributes={
                    "service.name": "curie-worker",
                    "kind": execution.kind if execution.kind in {"restore", "probe"} else "other",
                    "state": run.state,
                    "stage": stage,
                    "code": run.code or "none",
                    "connector": execution.connector,
                },
            )
        except ValueError:
            logger.debug("action execution metric outside its declared domain")
