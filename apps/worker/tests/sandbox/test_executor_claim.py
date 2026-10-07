"""The executor's sandbox claim, at the Kubernetes API boundary (ACTION-EXECUTOR-5).

@spec ACTION-EXECUTOR-5. An executor claim (thread key
``action-exec:<execution id>``) runs from its own per-claim SandboxTemplate: a
copy of the agent's pool template with ``CURIE_CREDENTIALS`` and the model
env-key declaration (``CURIE_MODEL_ENV_KEY``) removed, and the connector secret
``secretKeyRef`` entries limited to the target connector's header set. Labels
and the pool source stay the agent's. An ordinary turn's claim is unchanged.
Executor routes never reach the kernel's reclamation, and an executor claim over
quota surfaces as ``CapacityExhaustedError`` (the loop maps it to
``sandbox_unavailable``).

How "executor" reaches the substrate: an explicit keyword,
``executor_secret_names: frozenset[str] | None``, on
``SandboxSubstrate.claim`` and on ``SandboxClient.create_claim``. ``None`` is an
ordinary claim; a set (possibly empty) is an executor claim and names the
connector secrets the target connector's MCP entry headers expand. The thread
key prefix is still what pressure filtering keys on, so the substrate refuses a
claim whose key and keyword disagree.

The Kubernetes control plane is the in-memory API server double from
``test_k8s_claim``; every assertion reads the objects the real
``KubernetesSandboxClient`` POSTed. Routes live in the real Valkey.
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from collections.abc import Callable
from typing import Any

import pytest
import redis
from curie_worker.sandbox import (
    AffinityStore,
    CapacityExhaustedError,
    RouteRecord,
    RouteState,
    SandboxHandle,
    SandboxSubstrate,
    SubstrateConfig,
)
from redis.asyncio import Redis as AsyncRedis

from .test_k8s_claim import (
    _CONNECTOR_SECRET_REF,
    _chart_objects_untouched,
    _chart_template_spec,
    _client,
    _FakeApi,
    _runner_container,
    _seed_chart,
)

# Placeholder ids only.
_EXECUTION_ID = "00000000-0000-4000-8000-0000000000e1"
_EXEC_KEY = f"action-exec:{_EXECUTION_ID}"
_AGENT = "acme-a"
_AGENT_POOL = "curie-agent-acme-a-runner-pool"
_TARGET_SECRET = "GITHUB_PERSONAL_ACCESS_TOKEN"
_OTHER_SECRET = "JIRA_API_TOKEN"
_OTHER_SECRET_REF: dict[str, Any] = {
    "name": _OTHER_SECRET,
    "valueFrom": {
        "secretKeyRef": {
            "name": "curie-agent-acme-a-connector-secrets",
            "key": _OTHER_SECRET,
            "optional": False,
        }
    },
}
_CREDENTIALS_REF: dict[str, Any] = {
    "name": "CURIE_CREDENTIALS",
    "valueFrom": {"secretKeyRef": {"name": "curie", "key": "agentCredentials"}},
}
_MODEL_ENV_KEY_DECL: dict[str, Any] = {"name": "CURIE_MODEL_ENV_KEY", "value": "ANTHROPIC_API_KEY"}
# A secretKeyRef that is not a connector secret and is not on the runner: the
# executor copy must leave init containers alone.
_BUNDLE_FETCH_S3_REF: dict[str, Any] = {
    "name": "S3_SECRET_KEY",
    "valueFrom": {"secretKeyRef": {"name": "curie-s3", "key": "secretKey"}},
}


def _agent_template_spec() -> dict[str, Any]:
    """The agent's pool template with two connectors' secrets and a real model credential."""

    spec = _chart_template_spec(agent=True)
    runner = _runner_container(spec)
    env = runner["env"]
    assert env[0] == _CONNECTOR_SECRET_REF
    # Chart order: connector secrets sorted, then the model credential.
    env.insert(1, copy.deepcopy(_OTHER_SECRET_REF))
    credentials_at = next(i for i, e in enumerate(env) if e["name"] == "CURIE_CREDENTIALS")
    env.insert(credentials_at + 1, copy.deepcopy(_MODEL_ENV_KEY_DECL))
    init = spec["podTemplate"]["spec"]["initContainers"]
    bundle_fetch = next(c for c in init if c["name"] == "bundle-fetch")
    bundle_fetch["env"] = [{"name": "CURIE_BUNDLE_REF", "value": ""}, _BUNDLE_FETCH_S3_REF]
    return spec


def _executor_env() -> dict[str, str]:
    """AE-5 boot env: no model credential, no state tokens, the target's secret only."""

    return {
        "CURIE_BUDGET": "{}",
        "CURIE_BUNDLE_REF": "bundles/acme-a.tar.gz",
        "CURIE_RUNNER_MODE": "execute",
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.placeholder.signature",
        "CURIE_CONNECTOR_SECRET_KEYS": _TARGET_SECRET,
        _TARGET_SECRET: "target-secret-value-placeholder",
    }


def _ordinary_env() -> dict[str, str]:
    return {
        "CURIE_BUDGET": "{}",
        "CURIE_BUNDLE_REF": "bundles/acme-a.tar.gz",
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder",
        "CURIE_STATE_TOKEN": "sbx.state.placeholder",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.placeholder.signature",
        "CURIE_CONNECTOR_SECRET_KEYS": f"{_TARGET_SECRET},{_OTHER_SECRET}",
        _TARGET_SECRET: "target-secret-value-placeholder",
        _OTHER_SECRET: "other-secret-value-placeholder",
    }


def _posted(api: _FakeApi, kind: str) -> list[dict[str, Any]]:
    return [body for body in api.created if body.get("kind") == kind]


def _token_ref(claim: str, key: str) -> dict[str, Any]:
    return {
        "name": key,
        "valueFrom": {"secretKeyRef": {"name": f"{claim}-tokens", "key": key, "optional": False}},
    }


def _without_runner_env(spec: dict[str, Any]) -> dict[str, Any]:
    stripped = copy.deepcopy(spec)
    _runner_container(stripped).pop("env")
    return stripped


# -- KubernetesSandboxClient.create_claim ---------------------------------------


def test_executor_claim_template_strips_the_credential_and_non_target_secrets() -> None:
    """@spec ACTION-EXECUTOR-5: the per-claim template the client POSTs."""

    api = _FakeApi()
    source = _seed_chart(api, spec=_agent_template_spec())
    pristine = copy.deepcopy(source)
    claim = "curie-thread-0123456789-e1e1e1"
    env = _executor_env()

    _client(api).create_claim(
        claim,
        pool=_AGENT_POOL,
        env=env,
        labels={"curietech.ai/agent": _AGENT},
        agent_name=_AGENT,
        executor_secret_names=frozenset({_TARGET_SECRET}),
    )

    (template,) = _posted(api, "SandboxTemplate")
    spec = template["spec"]
    runner_env = _runner_container(spec)["env"]
    names = [entry["name"] for entry in runner_env]
    assert "CURIE_CREDENTIALS" not in names
    assert "CURIE_MODEL_ENV_KEY" not in names
    assert _OTHER_SECRET not in names
    # Nowhere in the pod spec, not only off the runner.
    spec_json = json.dumps(spec)
    for absent in ("CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY", _OTHER_SECRET, "agentCredentials"):
        assert absent not in spec_json, absent
    # Liveness: the target connector's secret still reaches the runner, verbatim.
    assert _CONNECTOR_SECRET_REF in runner_env
    # Every other source entry survives verbatim and in order; the scoped tokens
    # ride the per-claim Secret exactly as for any token-bearing claim.
    kept = [
        e
        for e in _runner_container(pristine)["env"]
        if e["name"] not in {"CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY", _OTHER_SECRET}
    ]
    tokens = sorted(("CURIE_CONNECTOR_CALLER_TOKEN", "CURIE_RUNNER_TOKEN"))
    assert runner_env == kept + [_token_ref(claim, key) for key in tokens]
    # The rest of the pod spec, init containers and their secretKeyRefs
    # included, is the agent's own.
    assert _without_runner_env(spec) == _without_runner_env(pristine)

    # The claim binds the per-claim pool, carries the agent label, and no
    # secret value is on it.
    (body,) = _posted(api, "SandboxClaim")
    assert body["spec"]["warmPoolRef"] == {"name": f"{claim}-resources-pool"}
    assert body["metadata"]["labels"]["curietech.ai/agent"] == _AGENT
    body_json = json.dumps(body)
    for value in (
        "target-secret-value-placeholder",
        "runner-token-placeholder",
        "cct.placeholder.signature",
    ):
        assert value not in body_json
    assert {"name": "CURIE_RUNNER_MODE", "value": "execute"} in body["spec"]["env"]
    # The chart's shared pool and template were read, never written.
    _chart_objects_untouched(api, pristine)


def test_executor_claim_with_an_empty_header_set_keeps_no_connector_secret() -> None:
    """@spec ACTION-EXECUTOR-5: a target whose headers expand no secret gets none."""

    api = _FakeApi()
    _seed_chart(api, spec=_agent_template_spec())
    claim = "curie-thread-0123456789-e2e2e2"
    env = {k: v for k, v in _executor_env().items() if k not in {_TARGET_SECRET}}
    env.pop("CURIE_CONNECTOR_SECRET_KEYS")

    _client(api).create_claim(
        claim,
        pool=_AGENT_POOL,
        env=env,
        labels={"curietech.ai/agent": _AGENT},
        agent_name=_AGENT,
        executor_secret_names=frozenset(),
    )

    (template,) = _posted(api, "SandboxTemplate")
    spec_json = json.dumps(template["spec"])
    for absent in (_TARGET_SECRET, _OTHER_SECRET, "CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY"):
        assert absent not in spec_json, absent


def test_an_ordinary_claim_template_is_unchanged() -> None:
    """@spec ACTION-EXECUTOR-5: without the executor keyword the copy is the token copy."""

    api = _FakeApi()
    source = _seed_chart(api, spec=_agent_template_spec())
    pristine = copy.deepcopy(source)
    claim = "curie-thread-0123456789-0a0a0a"

    _client(api).create_claim(
        claim,
        pool=_AGENT_POOL,
        env=_ordinary_env(),
        labels={"curietech.ai/agent": _AGENT},
        agent_name=_AGENT,
    )

    (template,) = _posted(api, "SandboxTemplate")
    runner_env = _runner_container(template["spec"])["env"]
    tokens = sorted(("CURIE_CONNECTOR_CALLER_TOKEN", "CURIE_RUNNER_TOKEN", "CURIE_STATE_TOKEN"))
    assert runner_env == _runner_container(pristine)["env"] + [
        _token_ref(claim, key) for key in tokens
    ]
    assert _without_runner_env(template["spec"]) == _without_runner_env(pristine)
    assert _CREDENTIALS_REF in runner_env
    assert _MODEL_ENV_KEY_DECL in runner_env
    assert _OTHER_SECRET_REF in runner_env


# -- SandboxSubstrate ------------------------------------------------------------


class _BindingApi(_FakeApi):
    """The API server double, plus the controller binding each new claim.

    With ``quota_message`` set the controller instead reports the claim's pod as
    rejected by the namespace ResourceQuota, as observed live.
    """

    def __init__(self, *, quota_message: str | None = None) -> None:
        super().__init__()
        self.quota_message = quota_message

    def create_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        created = super().create_namespaced_custom_object(
            group, version, namespace, plural, body, **kwargs
        )
        if plural != "sandboxclaims":
            return created
        name = body["metadata"]["name"]
        stored = self.objects[(plural, name)]
        if self.quota_message is not None:
            stored["status"] = {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "False",
                        "reason": "ReconcilerError",
                        "message": self.quota_message,
                    }
                ],
                "sandbox": {"name": name},
            }
            return created
        stored["status"] = {
            "conditions": [{"type": "Ready", "status": "True"}],
            "sandbox": {"name": name},
        }
        self.objects[("sandboxes", name)] = {
            "metadata": {"name": name, "namespace": "test-ns", "uid": self._next_uid()},
            "spec": {"operatingMode": "Running"},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "serviceFQDN": f"{name}.test-ns.svc.cluster.local",
            },
        }
        return created


def _substrate(api: _FakeApi, affinity: AffinityStore, key_prefix: str) -> SandboxSubstrate:
    config = SubstrateConfig(
        namespace="test-ns",
        warm_pool="curie-runner-pool",
        agent_pools=frozenset({_AGENT}),
        connector_secret_pools=frozenset({_AGENT}),
        route_ttl_seconds=60,
        claim_timeout_seconds=2.0,
        poll_interval_seconds=0.005,
        poll_interval_max_seconds=0.01,
        key_prefix=key_prefix,
    )
    return SandboxSubstrate(_client(api), affinity, config)


def test_substrate_executor_claim_runs_from_the_stripped_template(
    affinity: AffinityStore, key_prefix: str
) -> None:
    """@spec ACTION-EXECUTOR-5: same pool source and labels as a turn, stripped template."""

    api = _BindingApi()
    pristine = copy.deepcopy(_seed_chart(api, spec=_agent_template_spec()))
    substrate = _substrate(api, affinity, key_prefix)

    executor = substrate.claim(
        _EXEC_KEY,
        env=_executor_env(),
        agent_name=_AGENT,
        fresh_only=True,
        executor_secret_names=frozenset({_TARGET_SECRET}),
    )
    ordinary = substrate.claim("T-ordinary", env=_ordinary_env(), agent_name=_AGENT)

    templates = {
        t["metadata"]["labels"]["curietech.ai/sandbox-claim"]: t
        for t in _posted(api, "SandboxTemplate")
    }
    claims = {c["metadata"]["name"]: c for c in _posted(api, "SandboxClaim")}
    exec_spec = templates[executor.claim_name]["spec"]
    turn_spec = templates[ordinary.claim_name]["spec"]

    exec_json = json.dumps(exec_spec)
    for absent in ("CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY", _OTHER_SECRET):
        assert absent not in exec_json, absent
    assert _CONNECTOR_SECRET_REF in _runner_container(exec_spec)["env"]
    # The ordinary turn's pod still lists all of them.
    turn_env = _runner_container(turn_spec)["env"]
    for present in (
        _CREDENTIALS_REF,
        _MODEL_ENV_KEY_DECL,
        _OTHER_SECRET_REF,
        _CONNECTOR_SECRET_REF,
    ):
        assert present in turn_env, present["name"]

    # Pool source: both copies come from the agent's template, so everything
    # but the runner env (NetworkPolicy-relevant pod labels included) is equal.
    assert _without_runner_env(exec_spec) == _without_runner_env(turn_spec)
    assert _without_runner_env(exec_spec) == _without_runner_env(pristine)
    # Labels: the same keys, the same agent and managed-by values.
    exec_labels = claims[executor.claim_name]["metadata"]["labels"]
    turn_labels = claims[ordinary.claim_name]["metadata"]["labels"]
    assert set(exec_labels) == set(turn_labels)
    for key in ("curietech.ai/agent", "curietech.ai/managed-by"):
        assert exec_labels[key] == turn_labels[key]
    assert exec_labels["curietech.ai/agent"] == _AGENT
    _chart_objects_untouched(api, pristine)


def test_substrate_refuses_an_executor_keyword_on_a_non_executor_key(
    affinity: AffinityStore, key_prefix: str
) -> None:
    """@spec ACTION-EXECUTOR-5: pressure filtering keys on the prefix, so they must agree."""

    api = _BindingApi()
    _seed_chart(api, spec=_agent_template_spec())
    substrate = _substrate(api, affinity, key_prefix)

    with pytest.raises(ValueError):
        substrate.claim(
            "T-ordinary",
            env=_executor_env(),
            agent_name=_AGENT,
            fresh_only=True,
            executor_secret_names=frozenset({_TARGET_SECRET}),
        )
    with pytest.raises(ValueError):
        substrate.claim(_EXEC_KEY, env=_executor_env(), agent_name=_AGENT, fresh_only=True)
    assert api.created == []
    assert affinity.get("T-ordinary") is None
    assert affinity.get(_EXEC_KEY) is None


def test_executor_claim_over_quota_is_a_capacity_error_with_nothing_left(
    affinity: AffinityStore, key_prefix: str
) -> None:
    """@spec ACTION-EXECUTOR-5: the loop maps this error to ``sandbox_unavailable``."""

    api = _BindingApi(
        quota_message=(
            'Error seen: pods "curie-thread-example" is forbidden: exceeded quota: '
            "curie-sandbox-quota, requested: limits.cpu=1, used: limits.cpu=8, "
            "limited: limits.cpu=8"
        )
    )
    _seed_chart(api, spec=_agent_template_spec())
    substrate = _substrate(api, affinity, key_prefix)

    started = time.monotonic()
    with pytest.raises(CapacityExhaustedError):
        substrate.claim(
            _EXEC_KEY,
            env=_executor_env(),
            agent_name=_AGENT,
            fresh_only=True,
            executor_secret_names=frozenset({_TARGET_SECRET}),
        )
    # Terminal on the debounced rejection, not after the claim timeout.
    assert time.monotonic() - started < 1.5
    # No route, no claim, and the per-claim objects went with the claim.
    assert affinity.get(_EXEC_KEY) is None
    assert not any(plural == "sandboxclaims" for plural, _ in api.objects)
    assert not any(
        plural in {"sandboxtemplates", "sandboxwarmpools"} and name.startswith("curie-thread-")
        for plural, name in api.objects
    )
    assert api.secrets == {}


def _route(thread_key: str, claim: str, namespace: str = "test-ns") -> RouteRecord:
    return RouteRecord(
        handle=SandboxHandle(
            thread_key=thread_key,
            claim_name=claim,
            sandbox_name=claim,
            namespace=namespace,
            service_fqdn=f"{claim}.{namespace}.svc.cluster.local",
            port=8080,
            session_id="session-placeholder",
        )
    )


def test_pressure_candidates_exclude_executor_routes(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> None:
    """@spec ACTION-EXECUTOR-5: the kernel's reclamation never sees an executor route."""

    async def go() -> None:
        pressure_client = pressure_redis_factory()
        affinity = AffinityStore(
            redis_client, pressure_client=pressure_client, key_prefix=key_prefix
        )
        try:
            substrate = _substrate(_FakeApi(), affinity, key_prefix)
            assert affinity.put_if_absent("T-ordinary", _route("T-ordinary", "claim-turn"), 60)
            assert affinity.put_if_absent(_EXEC_KEY, _route(_EXEC_KEY, "claim-exec"), 60)

            result = await substrate.pressure_candidates(
                max_pages=8, max_records=256, deadline=time.monotonic() + 2.0
            )

            assert result.outcome == "complete"
            assert [c.thread_key for c in result.candidates] == ["T-ordinary"]
            # The executor route itself is untouched: still live and still routed.
            assert affinity.get(_EXEC_KEY) is not None
        finally:
            await pressure_client.aclose()

    asyncio.run(go())


# -- Review round 1 -------------------------------------------------------------

# The model credential an operator puts on the runner through
# ``agentSandbox.runner.extraEnv`` (rendered after the chart's own entries), in
# both shapes extraEnv allows. ``CURIE_MODEL_ENV_KEY`` declares a JSON array, so
# both names it resolves to are model credentials too.
_DECLARED_KEYS = ("OPENROUTER_API_KEY", "ACME_MODEL_KEY")
_EXTRA_ENV: tuple[dict[str, Any], ...] = (
    {"name": "CURIE_MODEL_ENV_KEY", "value": json.dumps(list(_DECLARED_KEYS))},
    {"name": "ANTHROPIC_API_KEY", "value": "sk-ant-placeholder"},
    {
        "name": "CLAUDE_CODE_OAUTH_TOKEN",
        "valueFrom": {"secretKeyRef": {"name": "acme-model", "key": "oauth"}},
    },
    {"name": "ANTHROPIC_AUTH_TOKEN", "value": "auth-placeholder"},
    {
        "name": "ANTHROPIC_FOUNDRY_API_KEY",
        "valueFrom": {"secretKeyRef": {"name": "acme-model", "key": "foundry"}},
    },
    {"name": "ANTHROPIC_CUSTOM_HEADERS", "value": "x-placeholder: 1"},
    {
        "name": "OPENROUTER_API_KEY",
        "valueFrom": {"secretKeyRef": {"name": "acme-model", "key": "openrouter"}},
    },
    {"name": "ACME_MODEL_KEY", "value": "acme-model-placeholder"},
    # Not a model credential: must survive the executor copy.
    {"name": "ACME_FEATURE_FLAG", "value": "on"},
)


def _extra_env_template_spec() -> dict[str, Any]:
    spec = _agent_template_spec()
    runner = _runner_container(spec)
    # The chart's CURIE_MODEL_ENV_KEY entry is replaced by the extraEnv one.
    runner["env"] = [e for e in runner["env"] if e["name"] != "CURIE_MODEL_ENV_KEY"]
    runner["env"] += [copy.deepcopy(e) for e in _EXTRA_ENV]
    return spec


def _runner_boot_env(spec: dict[str, Any]) -> dict[str, str]:
    """The runner's env as the kubelet would set it: every entry non-empty.

    A ``valueFrom`` entry resolves to a placeholder; a ``value`` entry keeps its
    literal, so a surviving declaration still names what it names.
    """

    env: dict[str, str] = {}
    for entry in _runner_container(spec)["env"]:
        env[entry["name"]] = entry.get("value") or f"resolved-{entry['name']}"
    return env


def _model_credential_names() -> frozenset[str]:
    """Every name the runner's executor mode refuses, read from the runner itself.

    ``executor_model_credentials`` is the runner's own refusal (no vector lists
    the names), so the worker cannot drift from it. The CLI parent model keys
    are the runner's other model credential inventory.
    """

    from curie_runner.__main__ import executor_model_credentials
    from curie_runner.subprocess_env import CLI_PARENT_MODEL_KEYS

    probe = {e["name"]: e.get("value") or "x" for e in _EXTRA_ENV}
    probe["CURIE_CREDENTIALS"] = "x"
    refused = frozenset(executor_model_credentials(probe))
    # The probe must have exercised the declaration, or the oracle is vacuous.
    assert set(_DECLARED_KEYS) <= refused
    assert {"CURIE_CREDENTIALS", "CURIE_MODEL_ENV_KEY", "ANTHROPIC_API_KEY"} <= refused
    assert "CLAUDE_CODE_OAUTH_TOKEN" in refused
    return refused | CLI_PARENT_MODEL_KEYS


def test_executor_template_drops_every_model_credential_the_runner_refuses() -> None:
    """@spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-4: the executor runner would boot."""

    from curie_runner.__main__ import executor_model_credentials

    api = _FakeApi()
    source = _seed_chart(api, spec=_extra_env_template_spec())
    pristine = copy.deepcopy(source)
    claim = "curie-thread-0123456789-e3e3e3"

    _client(api).create_claim(
        claim,
        pool=_AGENT_POOL,
        env=_executor_env(),
        labels={"curietech.ai/agent": _AGENT},
        agent_name=_AGENT,
        executor_secret_names=frozenset({_TARGET_SECRET}),
    )

    (template,) = _posted(api, "SandboxTemplate")
    spec = template["spec"]
    names = {e["name"] for e in _runner_container(spec)["env"]}
    leaked = names & _model_credential_names()
    assert not leaked, sorted(leaked)
    # The runner's own boot check finds nothing, so executor mode boots.
    assert executor_model_credentials(_runner_boot_env(spec)) == ()
    spec_json = json.dumps(spec)
    for value in ("sk-ant-placeholder", "acme-model", "auth-placeholder", "acme-model-placeholder"):
        assert value not in spec_json, value
    # Liveness: an unrelated extraEnv entry and the target secret survive.
    runner_env = _runner_container(spec)["env"]
    assert {"name": "ACME_FEATURE_FLAG", "value": "on"} in runner_env
    assert _CONNECTOR_SECRET_REF in runner_env
    assert _without_runner_env(spec) == _without_runner_env(pristine)


def test_an_ordinary_claim_keeps_extra_env_model_credentials() -> None:
    """@spec ACTION-EXECUTOR-5: only the executor copy is stripped."""

    api = _FakeApi()
    source = _seed_chart(api, spec=_extra_env_template_spec())
    pristine = copy.deepcopy(source)
    claim = "curie-thread-0123456789-0b0b0b"

    _client(api).create_claim(
        claim,
        pool=_AGENT_POOL,
        env=_ordinary_env(),
        labels={"curietech.ai/agent": _AGENT},
        agent_name=_AGENT,
    )

    (template,) = _posted(api, "SandboxTemplate")
    runner_env = _runner_container(template["spec"])["env"]
    tokens = sorted(("CURIE_CONNECTOR_CALLER_TOKEN", "CURIE_RUNNER_TOKEN", "CURIE_STATE_TOKEN"))
    assert runner_env == _runner_container(pristine)["env"] + [
        _token_ref(claim, key) for key in tokens
    ]


def _seed_route(affinity: AffinityStore, thread_key: str, state: RouteState) -> RouteRecord:
    record = RouteRecord(handle=_route(thread_key, "claim-exec-old").handle, state=state)
    assert affinity.put_if_absent(thread_key, record, 60)
    return record


def test_resume_refuses_an_executor_route(affinity: AffinityStore, key_prefix: str) -> None:
    """@spec ACTION-EXECUTOR-5: resume never builds an unstripped executor claim."""

    api = _BindingApi()
    _seed_chart(api, spec=_agent_template_spec())
    substrate = _substrate(api, affinity, key_prefix)
    record = _seed_route(affinity, _EXEC_KEY, RouteState.SUSPENDED)

    with pytest.raises(ValueError):
        substrate.resume(_EXEC_KEY, env=_executor_env(), agent_name=_AGENT)

    assert _posted(api, "SandboxClaim") == []
    assert _posted(api, "SandboxTemplate") == []
    assert not any(plural == "sandboxclaims" for plural, _ in api.deletes)
    still = affinity.get(_EXEC_KEY)
    assert still is not None and still.handle == record.handle


def test_handoff_refuses_an_executor_route(affinity: AffinityStore, key_prefix: str) -> None:
    """@spec ACTION-EXECUTOR-5: handoff never builds an unstripped executor claim."""

    api = _BindingApi()
    _seed_chart(api, spec=_agent_template_spec())
    substrate = _substrate(api, affinity, key_prefix)
    record = _seed_route(affinity, _EXEC_KEY, RouteState.LIVE)

    with pytest.raises(ValueError):
        substrate.handoff(
            _EXEC_KEY,
            expected=record.handle,
            env=_executor_env(),
            workspace_repo=None,
            agent_name=_AGENT,
        )

    assert _posted(api, "SandboxClaim") == []
    assert _posted(api, "SandboxTemplate") == []
    assert not any(plural == "sandboxclaims" for plural, _ in api.deletes)
    still = affinity.get(_EXEC_KEY)
    assert still is not None and still.handle == record.handle


def test_fake_sandbox_client_matches_the_protocol_keyword() -> None:
    """The substrate test double accepts and records ``executor_secret_names``."""

    from .conftest import FakeSandboxClient

    fake = FakeSandboxClient()
    fake.create_claim("c-exec", pool="p", executor_secret_names=frozenset({_TARGET_SECRET}))
    fake.create_claim("c-turn", pool="p")
    assert fake.claims["c-exec"].executor_secret_names == frozenset({_TARGET_SECRET})
    assert fake.claims["c-turn"].executor_secret_names is None
