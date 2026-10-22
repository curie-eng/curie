"""The executor's own connector reads without the reconcile loop (#4800).

@spec ACTION-EXECUTOR-12 @spec ACTION-EXECUTOR-14 (as amended by #4800): with
``CURIE_ACTION_EXECUTOR_CONNECTOR_READS`` on and the connector reconciler off,
the worker builds the executor and the digest recorder with the same
single-object Deployment reader and in-force render it builds beside the
reconciler, and builds no reconcile loop. A Deployment it cannot read refuses
``connector_digest_unavailable`` before any sandbox. With the setting off,
nothing changes: the reads follow ``CURIE_CONNECTOR_RECONCILE``.

The loop is built through ``run._build_action_executor`` and its serving check
runs against the real ``HttpManifestSource`` over HTTP; only the Kubernetes
apps API and the agent binding are stand-ins.
"""

from __future__ import annotations

import sys
import types
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker import action_digest, connector_k8s, run
from curie_worker.action_executor_loop import ActionExecutorLoop, Execution, _Refuse
from curie_worker.actions import ActionClient
from curie_worker.config import WorkerConfig
from kubernetes.client.exceptions import ApiException
from pydantic import ValidationError

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from executor_loop_fixtures import (  # noqa: E402
    AGENT_ID,
    AGENT_NAME,
    CONNECTOR,
    DEPLOYMENT,
    DIGEST,
    FORWARD_TOOL,
    NAMESPACE,
    OTHER_DIGEST,
    FakeDeployments,
    Timeline,
    deployment,
)

pytestmark = pytest.mark.anyio

SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
RELEASE = "example-release"
APP_NAME = "curie"
VERSION_ID = "00000000-0000-4000-8000-0000000000b2"
READS = "CURIE_ACTION_EXECUTOR_CONNECTOR_READS"


def _config(**fields: Any) -> WorkerConfig:
    base: dict[str, Any] = {
        "action_executor_enabled": True,
        "action_executor_connector_reads": True,
        "connector_reconcile_enabled": False,
        "internal_worker_token": "example-worker-token",
        "connector_caller_signing_key": SEED,
        "connector_namespace": NAMESPACE,
        "connector_release": RELEASE,
        "connector_app_name": APP_NAME,
    }
    return WorkerConfig(**{**base, **fields})


# -- configuration ------------------------------------------------------------


def test_the_setting_is_off_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(READS, raising=False)

    config = WorkerConfig()

    assert config.action_executor_connector_reads is False
    assert config.executor_reads_connectors is False


def test_the_setting_reads_its_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(READS, "true")
    monkeypatch.setenv("CURIE_ACTION_EXECUTOR_ENABLED", "true")
    monkeypatch.setenv("CURIE_NAMESPACE", NAMESPACE)
    monkeypatch.setenv("CURIE_RELEASE", RELEASE)
    monkeypatch.setenv("CURIE_CONNECTOR_APP_NAME", APP_NAME)
    monkeypatch.delenv("CURIE_CONNECTOR_RECONCILE", raising=False)

    config = WorkerConfig()

    assert config.action_executor_connector_reads is True
    assert config.executor_reads_connectors is True


@pytest.mark.parametrize(
    ("executor", "reconcile", "reads", "expected"),
    [
        (False, False, False, False),
        (False, True, False, False),
        (True, False, False, False),
        (True, True, False, True),
        (True, False, True, True),
    ],
    ids=["all_off", "reconciler_only", "executor_only", "executor_and_reconciler", "own_reads"],
)
def test_the_executor_reads_connectors_exactly_when_the_chart_grants_get(
    executor: bool, reconcile: bool, reads: bool, expected: bool
) -> None:
    config = _config(
        action_executor_enabled=executor,
        connector_reconcile_enabled=reconcile,
        action_executor_connector_reads=reads,
        connector_caller_signing_key=SEED,
    )

    assert config.executor_reads_connectors is expected


def test_the_setting_without_the_executor_is_refused_at_boot() -> None:
    with pytest.raises(ValidationError, match="requires CURIE_ACTION_EXECUTOR_ENABLED"):
        _config(action_executor_enabled=False)


def test_the_setting_beside_the_reconciler_is_refused_at_boot() -> None:
    with pytest.raises(ValidationError, match="connector reconciler off"):
        _config(connector_reconcile_enabled=True)


@pytest.mark.parametrize(
    ("field", "env"),
    [
        ("connector_namespace", "CURIE_NAMESPACE"),
        ("connector_release", "CURIE_RELEASE"),
        ("connector_app_name", "CURIE_CONNECTOR_APP_NAME"),
    ],
)
def test_the_setting_without_a_naming_input_is_refused_at_boot(field: str, env: str) -> None:
    with pytest.raises(ValidationError, match=env):
        _config(**{field: " "})


# -- wiring -------------------------------------------------------------------


class _Binding:
    """The agent's in-force deployment, as ``BindingResolver.resolve_agent`` reads it."""

    async def resolve_agent(self, agent_id: uuid.UUID) -> Any:
        assert str(agent_id) == AGENT_ID
        return types.SimpleNamespace(
            agent_id=agent_id, version_id=uuid.UUID(VERSION_ID), agent_name=AGENT_NAME
        )


class _Api:
    """The platform API's connector render route, served over real HTTP."""

    def __init__(self, digest: str) -> None:
        self.digest = digest
        self.queries: list[dict[str, str]] = []

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get(
            "/agents/{agent}/versions/{version}/connectors", self._rendered
        )
        return app

    async def _rendered(self, request: web.Request) -> web.Response:
        assert request.match_info["agent"] == AGENT_ID
        assert request.match_info["version"] == VERSION_ID
        self.queries.append(dict(request.query))
        return web.json_response({"manifests": [deployment(digest=self.digest)]})


def _patch_cluster(monkeypatch: pytest.MonkeyPatch, apps: Any) -> list[str]:
    built: list[str] = []

    def api(**_kwargs: Any) -> Any:
        built.append("apps")
        return apps

    async def name(agent_id: str | None, connector: str) -> str | None:
        assert agent_id == AGENT_ID and connector == CONNECTOR
        return DEPLOYMENT

    monkeypatch.setattr(connector_k8s, "connector_deployments_api", api)
    monkeypatch.setattr(
        action_digest, "agent_deployment_resolver", lambda *_a, **_k: name
    )
    return built


def _build(config: WorkerConfig, *, api_base_url: str = "http://api.example.invalid") -> Any:
    config = config.model_copy(update={"api_base_url": api_base_url})
    return run._build_action_executor(
        config,
        object(),  # type: ignore[arg-type]
        substrate=types.SimpleNamespace(claim_timeout_seconds=90.0),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
        killswitch=object(),  # type: ignore[arg-type]
        binding=_Binding(),  # type: ignore[arg-type]
        client=object(),  # type: ignore[arg-type]
        bundles=object(),  # type: ignore[arg-type]
    )


def _forward(digest: str | None = DIGEST) -> Execution:
    return Execution(
        id="00000000-0000-4000-8000-0000000000c3",
        kind="forward",
        agent_id=AGENT_ID,
        connector=CONNECTOR,
        subject_action_id=None,
        attempt=1,
        lease_owner="worker-a",
        connector_digest=digest,
        arguments_sha256=None,
        tool=FORWARD_TOOL,
    )


def _gated() -> list[str]:
    return [f"mcp__{CONNECTOR}__restore", f"mcp__{CONNECTOR}__{FORWARD_TOOL}"]


async def test_own_reads_build_the_reader_and_pass_a_forward_at_the_in_force_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apps = FakeDeployments(Timeline())
    apps.current = deployment(gated=_gated())
    built = _patch_cluster(monkeypatch, apps)
    api = _Api(DIGEST)
    server = TestServer(api.app(), host="127.0.0.1")
    await server.start_server()
    try:
        loop = _build(_config(), api_base_url=str(server.make_url("")).rstrip("/"))
        assert isinstance(loop, ActionExecutorLoop)

        await loop._require_serving(_forward(), gated_tool=FORWARD_TOOL)
    finally:
        await server.close()

    assert built == ["apps"]
    assert apps.reads == [DEPLOYMENT]
    assert api.queries == [{"release": RELEASE, "namespace": NAMESPACE, "app_name": APP_NAME}]


async def test_own_reads_refuse_a_forward_whose_deployment_cannot_be_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing grant answers 403; it refuses before any sandbox, never passes."""

    apps = FakeDeployments(Timeline())
    apps.current = ApiException(status=403, reason="Forbidden")
    _patch_cluster(monkeypatch, apps)
    api = _Api(DIGEST)
    server = TestServer(api.app(), host="127.0.0.1")
    await server.start_server()
    try:
        loop = _build(_config(), api_base_url=str(server.make_url("")).rstrip("/"))
        assert loop is not None

        with pytest.raises(_Refuse) as refused:
            await loop._require_serving(_forward(), gated_tool=FORWARD_TOOL)
    finally:
        await server.close()

    assert refused.value.code == "connector_digest_unavailable"
    assert apps.reads == [DEPLOYMENT]


async def test_own_reads_refuse_a_forward_when_the_in_force_digest_differs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apps = FakeDeployments(Timeline())
    apps.current = deployment(gated=_gated())
    _patch_cluster(monkeypatch, apps)
    api = _Api(OTHER_DIGEST)
    server = TestServer(api.app(), host="127.0.0.1")
    await server.start_server()
    try:
        loop = _build(_config(), api_base_url=str(server.make_url("")).rstrip("/"))
        assert loop is not None

        with pytest.raises(_Refuse) as refused:
            await loop._require_serving(_forward(), gated_tool=FORWARD_TOOL)
    finally:
        await server.close()

    assert refused.value.code == "connector_digest_unavailable"


async def test_without_any_connector_reads_a_forward_still_refuses_as_on_the_local_tier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default is unchanged: no reader is built and the forward refuses."""

    built = _patch_cluster(monkeypatch, FakeDeployments(Timeline()))
    loop = _build(_config(action_executor_connector_reads=False))
    assert loop is not None

    with pytest.raises(_Refuse) as refused:
        await loop._require_serving(_forward(), gated_tool=FORWARD_TOOL)

    assert refused.value.code == "tool_not_grant_bound"
    assert built == []
    assert loop._deployments is None


def test_own_reads_build_no_reconcile_loop() -> None:
    assert run._build_connector_loop(_config(), object()) is None  # type: ignore[arg-type]


def test_own_reads_compose_the_digest_recorder(monkeypatch: pytest.MonkeyPatch) -> None:
    built = _patch_cluster(monkeypatch, FakeDeployments(Timeline()))
    inner = ActionClient(api_base_url="http://api", api_key="k", client=httpx.AsyncClient())

    composed = run._build_action_recorder(_config(), object(), inner)  # type: ignore[arg-type]

    assert isinstance(composed, action_digest.DigestAttributingRecorder)
    assert built == ["apps"]


def test_without_own_reads_the_recorder_is_not_composed(monkeypatch: pytest.MonkeyPatch) -> None:
    built = _patch_cluster(monkeypatch, FakeDeployments(Timeline()))
    inner = ActionClient(api_base_url="http://api", api_key="k", client=httpx.AsyncClient())

    composed = run._build_action_recorder(
        _config(action_executor_connector_reads=False),
        object(),  # type: ignore[arg-type]
        inner,
    )

    assert composed is inner
    assert built == []
