"""Static deployment configuration crosses the worker boot boundary by agent."""

from __future__ import annotations

import json
import uuid

import pytest
from aci_protocol import EvalJob
from curie_internal.connector_endpoints import BINDING_ENV
from curie_runner import RunnerConfig
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.eval.stream import EvalStreamConsumer

_SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
_MAP = {
    "version": 1,
    "agents": {
        "acme-dev": {"reviewer": {"base_url": "https://connectors.example.com/dev/reviewer"}},
        "acme-prod": {"reviewer": {"base_url": "https://connectors.example.com/prod/reviewer"}},
    },
}


def _config(monkeypatch: pytest.MonkeyPatch) -> WorkerConfig:
    monkeypatch.setenv("CURIE_CONNECTOR_ENDPOINTS", json.dumps(_MAP))
    return WorkerConfig(connector_caller_signing_key=_SEED)


def test_worker_projects_only_authenticated_agent_and_keeps_route_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = _config(monkeypatch)
    resolved = ResolvedDeployment(
        agent_name="acme-dev",
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/example.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        secrets={BINDING_ENV: "untrusted", "CURIE_CONNECTOR_AGENT": "acme-prod"},
    )
    env = resolver.boot_env(resolved, "test-thread", route_connectors=frozenset({"reviewer"}))
    projected = json.loads(env[BINDING_ENV])
    assert projected == {
        "version": 1,
        "agent": "acme-dev",
        "connectors": _MAP["agents"]["acme-dev"],
    }
    assert "prod" not in env[BINDING_ENV]
    assert env["CURIE_CONNECTOR_CALLER_TOKEN"].startswith("cct.")
    assert json.loads(env["CURIE_CONNECTOR_ROUTE_TOKENS"])["reviewer"].startswith("ccr.")
    config = RunnerConfig.from_env({**env, "CURIE_SANDBOX_ID": "test-sandbox"})
    assert config.connector_agent == "acme-dev"
    assert config.connector_endpoints is not None
    assert config.connector_release is None and config.connector_namespace is None


def test_static_claim_refuses_missing_agent_and_unsigned_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    with pytest.raises(ValueError, match="authenticated agent"):
        config.connector_endpoints.bind("other-agent")
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(connector_caller_signing_key="")
    resolved = ResolvedDeployment(
        agent_name="acme-dev",
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/example.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )
    with pytest.raises(ValueError, match="caller token"):
        resolver.boot_env(resolved, "test-thread")


def test_eval_uses_the_same_static_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EvalStreamConsumer.__new__(EvalStreamConsumer)
    consumer._config = _config(monkeypatch)
    item = EvalJob(
        version_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        bundle_ref="bundles/example.zip",
        sha="example",
        suite="smoke",
        requested_at="2026-01-01T00:00:00Z",
    )
    env = consumer._boot_env(
        item, None, None, model=None, reviewer_model=None, agent_name="acme-dev"
    )
    assert json.loads(env[BINDING_ENV])["connectors"] == _MAP["agents"]["acme-dev"]
    assert env["CURIE_CONNECTOR_CALLER_TOKEN"].startswith("cct.")
    assert "CURIE_RELEASE" not in env and "CURIE_NAMESPACE" not in env


@pytest.mark.parametrize(
    "value",
    [
        "{}",
        "null",
        '{"version":2,"agents":{}}',
        '{"version":1,"agents":{},"fallback":true}',
        '{"version":1,"agents":{"acme-dev":{"reviewer":{"base_url":"http://"}}}}',
    ],
)
def test_invalid_static_configuration_fails_worker_boot(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_ENDPOINTS", value)
    with pytest.raises(ValueError):
        WorkerConfig()


def test_static_endpoints_refuse_the_action_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    _config(monkeypatch)
    with pytest.raises(ValueError, match="static.*action executor"):
        WorkerConfig(action_executor_enabled=True)


@pytest.mark.parametrize(
    "raw",
    [
        '{"version":1,"version":1,"agents":{}}',
        '{"version":1,"agents":{"acme-dev":{},"acme-dev":{}}}',
        '{"version":1,"agents":{"acme-dev":{"reviewer":{"base_url":"https://one.example.com","base_url":"https://two.example.com"}}}}',
    ],
)
def test_duplicate_keys_are_refused_by_worker_configuration(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("CURIE_CONNECTOR_ENDPOINTS", raw)
    with pytest.raises(ValueError, match="duplicate"):
        WorkerConfig()


def test_eval_static_configuration_error_returns_a_failed_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    class UnboundAgent:
        async def secrets_for(self, agent_id):
            return None

        async def eval_connector_settings_for(self, agent_id):
            return None

    consumer = EvalStreamConsumer.__new__(EvalStreamConsumer)
    consumer._config = _config(monkeypatch)
    consumer._repo_lookup = UnboundAgent()
    item = EvalJob(
        version_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        bundle_ref="bundles/example.zip",
        sha="example",
        suite="smoke",
        requested_at="2026-01-01T00:00:00Z",
    )
    assert asyncio.run(
        consumer._acquire_target(
            item,
            model=None,
            reviewer_model=None,
            thinking=None,
        )
    ) == (None, None, None)


@pytest.mark.parametrize(
    "name",
    [
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "ALL_PROXY",
        "NODE_TLS_REJECT_UNAUTHORIZED",
        "NODE_OPTIONS",
        "ssl_cert_file",
        "ssl_cert_dir",
        "all_proxy",
        "node_tls_reject_unauthorized",
        "node_options",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "NODE_EXTRA_CA_CERTS",
    ],
)
def test_connector_secrets_cannot_set_transport_controls(name: str) -> None:
    from curie_worker.binding import inject_connector_secrets

    env: dict[str, str] = {}
    inject_connector_secrets(
        env, {name: "untrusted", "EXAMPLE_TOKEN": "allowed"}, agent_label="acme-dev"
    )
    assert name not in env
    assert env["EXAMPLE_TOKEN"] == "allowed"
    assert env["CURIE_CONNECTOR_SECRET_KEYS"] == "EXAMPLE_TOKEN"
