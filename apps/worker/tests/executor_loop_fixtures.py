"""Boundary doubles for the worker executor loop tests (plan task 11).

@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-15. The loop touches four
boundaries and nothing between them is faked:

* the platform API, as a stateful ``httpx.MockTransport`` handler that follows
  ``apps/api/src/curie_api/routers/action_executions.py``: fenced, idempotent
  transitions, the observation compare, the claim route's lease sweep, and the
  API's own code normalization (``curie_api.action_execution_codes``). Each
  route can be scripted to fail before applying (``down``) or after applying
  with the answer lost (``lost``);
* the runner, as a real aiohttp server answering ``POST /v1/execute`` with the
  frozen shapes of ``tests/vectors/runner-execute.json``. It counts every
  ``call`` that reached it as a write, before it answers;
* Kubernetes sandboxes, as an in-memory agent-sandbox double (the kernel
  harness's pattern) whose sandboxes resolve to ``127.0.0.1``, driven by the
  real ``SandboxSubstrate`` over real Valkey;
* the connector Deployment, as a fake ``AppsV1Api`` whose only verb is
  ``read_namespaced_deployment``.

Every identifier is a placeholder.
"""

from __future__ import annotations

import base64
import copy
import json
import threading
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from aiohttp import web
from curie_api.action_execution_codes import CodeRejected, outcome_code
from curie_worker import connector_grant
from curie_worker.sandbox.types import ClaimView, QuotaRejection, SandboxView

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
RUNNER_VECTOR: dict[str, Any] = json.loads((_VECTORS / "runner-execute.json").read_text("utf-8"))

AGENT_ID = "00000000-0000-4000-8000-0000000000a1"
AGENT_NAME = "example-agent"
CONNECTOR = "example-scale"
DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32
PROXY_DIGEST = "sha256:" + "ef" * 32
NAMESPACE = "example-ns"
DEPLOYMENT = f"example-release-{AGENT_NAME}-mcp-{CONNECTOR}"
API_BASE = "http://api.example.invalid"
API_KEY = "example-platform-key"
WORKER_TOKEN = "example-worker-token"
TARGET_SECRET = "EXAMPLE_SCALE_TOKEN"
OTHER_SECRET = "EXAMPLE_OTHER_TOKEN"
# A standard-base64 32-byte Ed25519 seed, generated for the tests only.
GRANT_SEED = base64.b64encode(bytes(range(32))).decode("ascii")

_CALL = RUNNER_VECTOR["phases"]["call"]
TARGET: dict[str, Any] = RUNNER_VECTOR["phases"]["observe"]["request"]["target"]
PRIOR_STATE: dict[str, Any] = json.loads(_CALL["request"]["arguments"])["prior_state"]
RECORDED_VERSION: str = RUNNER_VECTOR["phases"]["observe"]["response"]["version"]
MOVED_VERSION = "rv-2077"
# The exact text the ``call`` phase must send for the vector's restore schema
# (which declares ``expected_version``) when the observed version is unchanged.
CALL_ARGUMENTS: str = _CALL["request"]["arguments"]
RULING_SHA256 = connector_grant.arguments_sha256(
    connector_grant.canonical_arguments({"target": TARGET, "prior_state": PRIOR_STATE})
)


def list_tools() -> list[dict[str, Any]]:
    return copy.deepcopy(RUNNER_VECTOR["phases"]["list"]["response"]["tools"])


def decode_grant(grant: str) -> dict[str, Any]:
    """The claims of a ``ccg`` grant (signature checked by the proxy, not here)."""

    prefix, payload, _signature = grant.split(".")
    assert prefix == connector_grant.PREFIX
    padded = payload + "=" * (-len(payload) % 4)
    claims: dict[str, Any] = json.loads(base64.urlsafe_b64decode(padded))
    return claims


# --------------------------------------------------------------------------- #
# A shared timeline, so cross-boundary ordering can be asserted
# --------------------------------------------------------------------------- #


@dataclass
class Timeline:
    events: list[str] = field(default_factory=list)

    def add(self, event: str) -> None:
        self.events.append(event)

    def index(self, event: str) -> int:
        return self.events.index(event)


# --------------------------------------------------------------------------- #
# The platform API
# --------------------------------------------------------------------------- #

_TERMINAL = {"confirmed", "failed", "indeterminate", "refused"}
MAX_ATTEMPTS = 3


@dataclass
class Execution:
    id: str
    kind: str
    agent_id: str = AGENT_ID
    connector: str = CONNECTOR
    tool: str | None = "restore"
    subject_action_id: str | None = None
    connector_digest: str = DIGEST
    arguments_sha256: str | None = RULING_SHA256
    requested_by: str | None = "U-example-operator"
    state: str = "requested"
    attempt: int = 0
    lease_owner: str | None = None
    lease_expired: bool = False
    refusal_code: str | None = None
    failure_code: str | None = None
    observed: bool = False
    observed_version: str | None = None
    advertised: list[str] | None = None
    restore_capable: bool | None = None
    reports: list[dict[str, Any]] = field(default_factory=list)

    def out(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "id": self.id,
            "kind": self.kind,
            "state": self.state,
            "agent_id": self.agent_id,
            "connector": self.connector,
            "tool": self.tool,
            "subject_action_id": self.subject_action_id,
            "requested_by": self.requested_by,
            "attempt": self.attempt,
            "lease_owner": self.lease_owner,
            "lease_expires_at": (now + timedelta(seconds=60)).isoformat(),
            "refusal_code": self.refusal_code,
            "failure_code": self.failure_code,
            "dispatched_at": None,
            "finished_at": None,
            "created_at": now.isoformat(),
            # The additive claim fields the loop needs (plan: AE-14 and AE-7).
            "connector_digest": self.connector_digest,
            "arguments_sha256": self.arguments_sha256,
        }


class FakeApi:
    """The executor routes and the ledger read, as the worker reaches them."""

    def __init__(self, timeline: Timeline) -> None:
        self.timeline = timeline
        self.executions: dict[str, Execution] = {}
        self.order: list[str] = []
        self.post_versions: dict[str, str] = {}
        self.ledger: dict[str, dict[str, Any]] = {}
        self.requests: list[dict[str, Any]] = []
        self.faults: dict[str, list[str]] = {}
        self.statuses: dict[str, list[int]] = {}
        self.executor_enabled = True

    # -- seeding ------------------------------------------------------------

    def add_restore(self, *, post_version: str = RECORDED_VERSION, **fields: Any) -> Execution:
        action_id = str(uuid.uuid4())
        self.post_versions[action_id] = post_version
        self.ledger[action_id] = {
            "id": action_id,
            "agent_id": AGENT_ID,
            "tool": f"mcp__{CONNECTOR}__scale",
            "target": copy.deepcopy(TARGET),
            "prior_state": copy.deepcopy(PRIOR_STATE),
            "status": "succeeded",
            "undoable": True,
        }
        execution = Execution(
            id=str(uuid.uuid4()), kind="restore", subject_action_id=action_id, **fields
        )
        self.executions[execution.id] = execution
        self.order.append(execution.id)
        return execution

    def add_probe(self, **fields: Any) -> Execution:
        execution = Execution(
            id=str(uuid.uuid4()),
            kind="probe",
            tool=None,
            arguments_sha256=None,
            requested_by=None,
            **fields,
        )
        self.executions[execution.id] = execution
        self.order.append(execution.id)
        return execution

    def fail(self, route: str, *modes: str) -> None:
        """Script ``route`` (claim, observation, dispatch, outcome, ledger)."""

        self.faults.setdefault(route, []).extend(modes)

    def answer(self, route: str, *statuses: int) -> None:
        """Script ``route`` to answer these statuses without applying anything."""

        self.statuses.setdefault(route, []).extend(statuses)

    def expire_lease(self, execution: Execution) -> None:
        execution.lease_expired = True

    # -- reading what happened ---------------------------------------------

    def calls_to(self, route: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["route"] == route]

    # -- the transport -------------------------------------------------------

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        route, execution_id = self._route(request)
        body = json.loads(request.content) if request.content else None
        self.requests.append(
            {"route": route, "id": execution_id, "body": body, "headers": dict(request.headers)}
        )
        self.timeline.add(f"api:{route}")
        modes = self.faults.get(route)
        mode = modes.pop(0) if modes else "ok"
        if mode == "down":
            raise httpx.ConnectError("api unreachable", request=request)
        statuses = self.statuses.get(route)
        if statuses:
            return httpx.Response(statuses.pop(0), json={"detail": "scripted"})
        response = self._apply(route, execution_id, body, request)
        if mode == "lost":
            raise httpx.ReadError("response lost after the API applied it", request=request)
        return response

    def _route(self, request: httpx.Request) -> tuple[str, str | None]:
        parts = request.url.path.strip("/").split("/")
        if parts[:1] == ["actions"] and len(parts) == 2 and request.method == "GET":
            return "ledger", parts[1]
        if parts == ["action-executions", "claim"]:
            return "claim", None
        if parts[:1] == ["action-executions"] and len(parts) == 3:
            return parts[2], parts[1]
        if parts[:1] == ["action-executions"] and len(parts) == 2 and request.method == "GET":
            return "receipt", parts[1]
        raise AssertionError(f"unexpected API request {request.method} {request.url.path}")

    def _apply(
        self, route: str, execution_id: str | None, body: Any, request: httpx.Request
    ) -> httpx.Response:
        if route == "ledger":
            if request.headers.get("x-api-key") != API_KEY:
                return httpx.Response(401, json={"detail": "api key required"})
            row = self.ledger.get(str(execution_id))
            return httpx.Response(200, json=row) if row else httpx.Response(404)
        if request.headers.get("x-curie-worker-token") != WORKER_TOKEN:
            return httpx.Response(401, json={"detail": "worker token required"})
        if route == "claim":
            return self._claim(body)
        execution = self.executions.get(str(execution_id))
        if execution is None:
            return httpx.Response(404)
        if route == "receipt":
            return httpx.Response(200, json=execution.out())
        fence = (body or {}).get("lease_owner"), (body or {}).get("attempt")
        if fence != (execution.lease_owner, execution.attempt):
            return _conflict("the fence does not hold this execution")
        if execution.state not in _TERMINAL and execution.lease_expired:
            return _conflict("the lease on this execution has expired")
        handler = {
            "observation": self._observation,
            "dispatch": self._dispatch,
            "outcome": self._outcome,
        }[route]
        return handler(execution, body)

    def _claim(self, body: dict[str, Any]) -> httpx.Response:
        if not self.executor_enabled:
            return httpx.Response(204)
        # The claim route is the AE-17 sweeper: an expired dispatched lease is
        # indeterminate, an expired claimed lease is reclaimed (three times).
        for execution in self.executions.values():
            if execution.state == "dispatched" and execution.lease_expired:
                execution.state = "indeterminate"
                execution.failure_code = "response_lost"
        for execution_id in self.order:
            execution = self.executions[execution_id]
            claimable = execution.state == "requested" or (
                execution.state == "claimed" and execution.lease_expired
            )
            if not claimable:
                continue
            if execution.state == "claimed" and execution.attempt >= MAX_ATTEMPTS:
                execution.state = "refused"
                execution.refusal_code = "runner_unavailable"
                continue
            execution.state = "claimed"
            execution.attempt += 1
            execution.lease_owner = body["lease_owner"]
            execution.lease_expired = False
            execution.observed = False
            execution.observed_version = None
            return httpx.Response(200, json=execution.out())
        return httpx.Response(204)

    def _observation(self, execution: Execution, body: dict[str, Any]) -> httpx.Response:
        if execution.kind != "restore":
            return _conflict("only a restore observes a version")
        version = body.get("version")
        if execution.observed:
            if execution.observed_version != version:
                return _conflict("a different version was already observed")
            return httpx.Response(200, json=execution.out())
        if execution.state != "claimed":
            return _conflict(f"an execution in state {execution.state} observes nothing")
        execution.observed = True
        execution.observed_version = version
        recorded = self.post_versions[str(execution.subject_action_id)]
        if not (version and recorded and version == recorded and len(version) <= 256):
            execution.state = "refused"
            execution.refusal_code = "version_conflict"
        return httpx.Response(200, json=execution.out())

    def _dispatch(self, execution: Execution, body: dict[str, Any]) -> httpx.Response:
        del body
        if not self.executor_enabled:
            return httpx.Response(503, json={"detail": "the action executor is not enabled"})
        if execution.state == "dispatched":
            return httpx.Response(200, json=execution.out())
        if execution.state != "claimed":
            return _conflict(f"an execution in state {execution.state} cannot dispatch")
        if execution.kind != "restore":
            return _conflict(f"a {execution.kind} execution cannot dispatch")
        if not execution.observed:
            return _conflict("a restore dispatches only after an unchanged version is observed")
        execution.state = "dispatched"
        return httpx.Response(200, json=execution.out())

    def _outcome(self, execution: Execution, body: dict[str, Any]) -> httpx.Response:
        state = body.get("state")
        try:
            code = outcome_code(str(state), body.get("code"))
        except CodeRejected as exc:
            return httpx.Response(422, json={"detail": str(exc)})
        advertised = body.get("advertised")
        probe_confirmed = execution.kind == "probe" and state == "confirmed"
        if probe_confirmed and advertised is None:
            return httpx.Response(422, json={"detail": "a finished probe reports what it observed"})
        if not probe_confirmed and advertised is not None:
            return httpx.Response(422, json={"detail": "only a finished probe reports verbs"})
        execution.reports.append(dict(body))
        if execution.state in _TERMINAL:
            stored = execution.refusal_code or execution.failure_code
            if (execution.state, stored) != (state, code):
                return _conflict("this execution already ended with another outcome")
            return httpx.Response(200, json=execution.out())
        allowed: str | None
        if state == "refused":
            allowed = "claimed"
        elif execution.kind == "probe":
            allowed = "claimed" if state == "confirmed" else None
        else:
            allowed = "dispatched"
        if execution.state != allowed:
            return _conflict(f"a {execution.kind} in {execution.state} cannot end {state}")
        execution.state = str(state)
        if state == "refused":
            execution.refusal_code = code
        elif code is not None:
            execution.failure_code = code
        if probe_confirmed:
            execution.advertised = list(advertised or ())
            execution.restore_capable = {"restore", "observe_version"} <= set(advertised or ())
        return httpx.Response(200, json=execution.out())


def _conflict(reason: str) -> httpx.Response:
    return httpx.Response(409, json={"detail": reason})


# --------------------------------------------------------------------------- #
# The runner's executor route
# --------------------------------------------------------------------------- #

Hook = Callable[[dict[str, Any]], Awaitable[None] | None]


class FakeRunner:
    """``POST /v1/execute`` as an executor-mode runner answers it.

    ``call_mode``: ``reply`` (answer ``call_reply``), ``crash`` (the write
    reaches the connector, then the connection drops), ``unknown`` (the write
    reaches the connector, then the vector's ``call_transport_failure``), or
    ``refuse:<code>`` (a pre-dial route refusal: no write).
    """

    def __init__(self, timeline: Timeline) -> None:
        self.timeline = timeline
        self.requests: list[dict[str, Any]] = []
        self.writes: list[dict[str, Any]] = []
        self.tools = list_tools()
        self.version: str | None = RECORDED_VERSION
        self.call_mode = "reply"
        self.call_reply: dict[str, Any] = copy.deepcopy(_CALL["response"])
        self.phase_status: dict[str, tuple[int, dict[str, Any]]] = {}
        self.hooks: dict[str, Hook] = {}

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post(RUNNER_VECTOR["route"]["path"], self._execute)
        return app

    def phases(self) -> list[str]:
        return [r["body"]["phase"] for r in self.requests]

    def grants(self) -> list[str]:
        return [r["body"]["grant"] for r in self.requests if r["body"].get("grant")]

    async def _execute(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        self.requests.append({"body": body, "authorization": request.headers.get("Authorization")})
        phase = body.get("phase")
        self.timeline.add(f"runner:{phase}")
        hook = self.hooks.get(str(phase))
        if hook is not None:
            result = hook(body)
            if result is not None:
                await result
        if phase in self.phase_status:
            status, payload = self.phase_status[phase]
            return web.json_response(payload, status=status)
        if phase == "list":
            return web.json_response({"phase": "list", "tools": self.tools})
        if phase == "observe":
            return web.json_response({"phase": "observe", "version": self.version})
        if phase != "call":
            return web.json_response({"refused": "invalid_request"}, status=400)
        if self.call_mode.startswith("refuse:"):
            return web.json_response({"refused": self.call_mode.split(":", 1)[1]}, status=409)
        self.writes.append(body)
        self.timeline.add("connector:write")
        if self.call_mode == "crash":
            assert request.transport is not None
            request.transport.close()
            return web.Response(status=200)
        if self.call_mode == "unknown":
            failure = RUNNER_VECTOR["call_transport_failure"]
            return web.json_response(failure["body"], status=failure["status"])
        return web.json_response(self.call_reply)


# --------------------------------------------------------------------------- #
# Kubernetes: sandboxes and the connector Deployment
# --------------------------------------------------------------------------- #


@dataclass
class _Claim:
    name: str
    sandbox_name: str
    labels: dict[str, str]
    env: dict[str, str] | None
    executor_secret_names: frozenset[str] | None
    ready: bool
    quota_rejection: QuotaRejection | None


@dataclass
class FakeSandboxes:
    """The agent-sandbox control plane; every sandbox dials the local fake runner."""

    claims: dict[str, _Claim] = field(default_factory=dict)
    created: list[_Claim] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    quota_rejection: QuotaRejection | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
    ) -> None:
        del pool, runner_resources, agent_name
        with self._lock:
            claim = _Claim(
                name=name,
                sandbox_name=f"sbx-{name}",
                labels={"curietech.ai/managed-by": "curie-sandbox-substrate", **(labels or {})},
                env=dict(env) if env is not None else None,
                executor_secret_names=executor_secret_names,
                ready=self.quota_rejection is None,
                quota_rejection=self.quota_rejection,
            )
            self.claims[name] = claim
            self.created.append(claim)

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None:
        assert request_timeout_seconds > 0
        claim = self.claims.get(name)
        if claim is None:
            return None
        return ClaimView(
            name=claim.name,
            ready=claim.ready,
            sandbox_name=claim.sandbox_name if claim.ready else None,
            created_at=datetime.now(UTC),
            quota_rejection=claim.quota_rejection,
            ready_reason=None,
            ready_message=None,
        )

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        assert request_timeout_seconds > 0
        with self._lock:
            self.claims.pop(name, None)
            self.deleted.append(name)

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        key, _, value = label_selector.partition("=")
        views = []
        for claim in list(self.claims.values()):
            if claim.labels.get(key) == value:
                view = self.get_claim(claim.name, request_timeout_seconds=1.0)
                assert view is not None
                views.append(view)
        return views

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        return []

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        assert request_timeout_seconds > 0
        if not any(c.sandbox_name == name and c.ready for c in self.claims.values()):
            return None
        return SandboxView(
            name=name, ready=True, service_fqdn="127.0.0.1", operating_mode="Running", port=None
        )

    def quota_has_headroom(
        self, rejection: QuotaRejection, *, request_timeout_seconds: float
    ) -> bool:
        return False

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        return None

    def pod_termination(
        self, name: str, *, request_timeout_seconds: float, since: datetime
    ) -> Any | None:
        return None

    def set_sandbox_mode(self, name: str, mode: str) -> None:
        raise AssertionError("an executor sandbox is never suspended")


QUOTA_REJECTION = QuotaRejection(
    quota_name="curie-sandbox-quota",
    requested={"limits.cpu": "1"},
    used={"limits.cpu": "8"},
    hard={"limits.cpu": "8"},
)


def deployment(
    *,
    digest: str = DIGEST,
    rolled_out: bool = True,
    gated: list[str] | None = None,
) -> dict[str, Any]:
    """The connector's rendered Deployment as the API server returns it."""

    gated_tools = [f"mcp__{CONNECTOR}__restore"] if gated is None else gated
    proxy_env = [{"name": "CURIE_CALLER_PROXY_CONNECTOR", "value": CONNECTOR}]
    if gated_tools:
        proxy_env.append(
            {
                "name": "CURIE_CALLER_PROXY_GATED_TOOLS",
                "value": json.dumps(gated_tools, separators=(",", ":")),
            }
        )
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": DEPLOYMENT, "namespace": NAMESPACE, "generation": 4},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/name": DEPLOYMENT}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": DEPLOYMENT}},
                "spec": {
                    "containers": [
                        {
                            "name": "caller-proxy",
                            "image": f"registry.example/proxy@{PROXY_DIGEST}",
                            "env": proxy_env,
                        },
                        {"name": "server", "image": f"registry.example/connector@{digest}"},
                    ]
                },
            },
        },
        "status": {
            "observedGeneration": 4,
            "replicas": 2 if not rolled_out else 1,
            "updatedReplicas": 1,
            "availableReplicas": 1,
        },
    }


class _Raw:
    def __init__(self, body: dict[str, Any]) -> None:
        self.data = json.dumps(body).encode()
        self.status = 200

    def getheaders(self) -> dict[str, str]:
        return {}


class FakeDeployments:
    """The apps API with a single-object get as its only verb.

    Each read takes the next scripted body (or exception); when the script is
    empty, ``current`` answers.
    """

    def __init__(self, timeline: Timeline) -> None:
        self.timeline = timeline
        self.current: dict[str, Any] | BaseException = deployment()
        self.script: list[dict[str, Any] | BaseException] = []
        self.reads: list[str] = []

    def read_namespaced_deployment(self, name: str, namespace: str, **kwargs: Any) -> Any:
        self.reads.append(name)
        self.timeline.add("k8s:deployment")
        assert namespace == NAMESPACE
        outcome = self.script.pop(0) if self.script else self.current
        if isinstance(outcome, BaseException):
            raise outcome
        raw = _Raw(outcome)
        if kwargs.get("_preload_content") is False:
            return raw
        # Typed path: the same body through the client's own deserializer.
        from kubernetes import client as k8s_client

        return k8s_client.ApiClient().deserialize(raw, "V1Deployment")

    def __getattr__(self, attr: str) -> Any:
        raise AssertionError(f"the executor may only get one Deployment by name, not {attr}")
