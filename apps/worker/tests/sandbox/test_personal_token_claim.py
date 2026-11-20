"""Personal model grants at the Kubernetes claim creation boundary (#5374)."""

from __future__ import annotations

import copy
import json
import time
from typing import Any

import pytest
from kubernetes.client import ApiException

from .test_k8s_claim import (
    _CONNECTOR_SECRET_REF,
    _chart_template_spec,
    _client,
    _FakeApi,
    _runner_container,
    _seed_chart,
)

_POOL = "curie-agent-acme-a-runner-pool"
_TEMPLATE = "curie-agent-acme-a-runner"
_CLAIM = "curie-thread-0123456789-abcd12"
_OAUTH = "CLAUDE_CODE_OAUTH_TOKEN"
_MODE = "CURIE_MODEL_AUTH_MODE"
_GRANT = "personal-grant-placeholder"
_MODEL_NAMES = frozenset(
    {
        "CURIE_CREDENTIALS",
        "CURIE_MODEL_ENV_KEY",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        _OAUTH,
        "ANTHROPIC_FOUNDRY_API_KEY",
        "ANTHROPIC_CUSTOM_HEADERS",
    }
)
_SOURCE_TARGETS = ("ACME_SOURCE_MODEL_KEY", "ACME_SOURCE_SECOND_KEY")
_CLAIM_TARGETS = ("ACME_CLAIM_MODEL_KEY", "ACME_CLAIM_SECOND_KEY")


@pytest.mark.parametrize("personal", [True, False], ids=["personal", "service"])
def test_installation_endpoint_is_withheld_only_from_personal_claims(personal: bool) -> None:
    api = _FakeApi()
    spec = _source_spec()
    endpoint = {"name": "ANTHROPIC_BASE_URL", "valueFrom": {
        "secretKeyRef": {"name": "installation-model", "key": "ANTHROPIC_BASE_URL"},
    }}
    for container in _containers(spec):
        container["env"].append(copy.deepcopy(endpoint))
    _seed_chart(api, spec=spec)
    pristine = copy.deepcopy(api.objects)
    env = _personal_env() if personal else {
        _MODE: "service", "CURIE_RUNNER_TOKEN": "runner-token-placeholder",
    }
    if personal:
        env["ANTHROPIC_BASE_URL"] = "https://model.example.com"
    _create(api, env)
    copied = _copied_spec(api)
    if personal:
        assert all(
            entry["name"] != "ANTHROPIC_BASE_URL"
            for container in _containers(copied) for entry in container["env"]
        )
        assert "ANTHROPIC_BASE_URL" not in api.secrets[f"{_CLAIM}-tokens"]["stringData"]
        assert all(entry["name"] != "ANTHROPIC_BASE_URL" for entry in (
            api.objects[("sandboxclaims", _CLAIM)]["spec"]["env"]
        ))
    else:
        for container in _containers(copied):
            entries = [e for e in container["env"] if e["name"] == "ANTHROPIC_BASE_URL"]
            assert entries == [endpoint]
    _assert_shared_unchanged(api, pristine)


def _source_spec(*, runner_first: bool = True) -> dict[str, Any]:
    spec = _chart_template_spec()
    pod = spec["podTemplate"]["spec"]
    sidecar = {"name": "connector-sidecar", "image": "example.com/connector:latest"}
    pod["containers"].insert(1 if runner_first else 0, sidecar)
    for container in [*pod["containers"], *pod["initContainers"]]:
        env = container.setdefault("env", [])
        present = {entry["name"] for entry in env}
        env.extend(
            {"name": name, "value": f"installation-{name.lower()}-placeholder"}
            for name in sorted(_MODEL_NAMES - present - {"CURIE_MODEL_ENV_KEY"})
        )
        env.extend(
            [
                {"name": "CURIE_MODEL_ENV_KEY", "value": json.dumps(_SOURCE_TARGETS)},
                *(
                    {"name": name, "value": f"declared-{name.lower()}-placeholder"}
                    for name in (*_SOURCE_TARGETS, *_CLAIM_TARGETS)
                ),
                {"name": _MODE, "value": "service"},
                {"name": "ACME_PUBLIC_FLAG", "value": "public-control"},
            ]
        )
        if _CONNECTOR_SECRET_REF not in env:
            env.append(copy.deepcopy(_CONNECTOR_SECRET_REF))
        container["envFrom"] = [
            {"secretRef": {"name": "acme-installation-env"}},
            {"configMapRef": {"name": "acme-installation-config"}},
        ]
    return spec


def _create(api: _FakeApi, env: dict[str, str], **kwargs: Any) -> None:
    _client(api).create_claim(
        _CLAIM,
        pool=_POOL,
        env=env,
        deadline=time.monotonic() + 300,
        **kwargs,
    )


def _personal_env() -> dict[str, str]:
    return {
        _MODE: "personal",
        _OAUTH: _GRANT,
        "CURIE_MODEL_ENV_KEY": json.dumps(_CLAIM_TARGETS),
        **{
            name: f"incoming-{name.lower()}-placeholder"
            for name in (*_SOURCE_TARGETS, *_CLAIM_TARGETS)
        },
        "CURIE_CREDENTIALS": "incoming-installation-credential-placeholder",
        "ANTHROPIC_API_KEY": "incoming-api-key-placeholder",
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder",
        "CURIE_BUDGET": "{}",
        "CURIE_CONNECTOR_SECRET_KEYS": "GITHUB_PERSONAL_ACCESS_TOKEN",
        "GITHUB_PERSONAL_ACCESS_TOKEN": "connector-secret-placeholder",
    }


def _copied_spec(api: _FakeApi) -> dict[str, Any]:
    return api.objects[("sandboxtemplates", f"{_CLAIM}-resources")]["spec"]


def _containers(spec: dict[str, Any]) -> list[dict[str, Any]]:
    pod = spec["podTemplate"]["spec"]
    return [*pod["containers"], *pod["initContainers"]]


def _assert_shared_unchanged(
    api: _FakeApi, pristine: dict[tuple[str, str], dict[str, Any]]
) -> None:
    assert {key: api.objects[key] for key in pristine} == pristine
    assert not any(name in {_POOL, _TEMPLATE} for _, name, _ in api.patches)


@pytest.mark.parametrize("runner_first", [True, False], ids=["runner-first", "sidecar-first"])
def test_personal_claim_delivers_grant_only_to_runner_secret_ref(runner_first: bool) -> None:
    api = _FakeApi()
    source = _seed_chart(api, spec=_source_spec(runner_first=runner_first))
    pristine = copy.deepcopy(api.objects)
    env = _personal_env()
    incoming = copy.deepcopy(env)

    _create(api, env)

    spec = _copied_spec(api)
    runner_env = _runner_container(spec)["env"]
    assert [entry for entry in runner_env if entry["name"] == _OAUTH] == [
        {
            "name": _OAUTH,
            "valueFrom": {
                "secretKeyRef": {
                    "name": f"{_CLAIM}-tokens",
                    "key": _OAUTH,
                    "optional": False,
                }
            },
        }
    ]
    assert {"name": _MODE, "value": "personal"} in runner_env
    for container in _containers(spec):
        names = {entry["name"] for entry in container["env"]}
        assert names.isdisjoint((_MODEL_NAMES - {_OAUTH}) | set(_SOURCE_TARGETS + _CLAIM_TARGETS))
        assert "envFrom" not in container
        assert _CONNECTOR_SECRET_REF in container["env"]
        assert {"name": "ACME_PUBLIC_FLAG", "value": "public-control"} in container["env"]
        if container["name"] != "runner":
            assert names.isdisjoint({_OAUTH, _MODE})
        original = next(c for c in _containers(source) if c["name"] == container["name"])
        assert container.get("volumeMounts") == original.get("volumeMounts")
    assert spec["podTemplate"]["spec"]["volumes"] == source["podTemplate"]["spec"]["volumes"]
    claim = api.objects[("sandboxclaims", _CLAIM)]
    assert claim["spec"]["warmPoolRef"] == {"name": f"{_CLAIM}-resources-pool"}
    assert {entry["name"] for entry in claim["spec"]["env"]} == {"CURIE_BUDGET"}
    pool = api.objects[("sandboxwarmpools", f"{_CLAIM}-resources-pool")]
    assert pool["spec"] == {
        "replicas": 0,
        "sandboxTemplateRef": {"name": f"{_CLAIM}-resources"},
    }
    secret = api.secrets[f"{_CLAIM}-tokens"]
    assert secret["stringData"] == {
        _OAUTH: _GRANT,
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder",
    }
    for value in (
        _GRANT,
        "incoming-installation-credential-placeholder",
        "incoming-api-key-placeholder",
        "connector-secret-placeholder",
    ):
        assert value not in json.dumps(api.created)
    assert _GRANT not in json.dumps(spec["podTemplate"])
    assert env == incoming
    _assert_shared_unchanged(api, pristine)


@pytest.mark.parametrize("source_json", [False, True], ids=["source-single", "source-json"])
@pytest.mark.parametrize("incoming_json", [False, True], ids=["incoming-single", "incoming-json"])
def test_personal_claim_strips_declared_model_targets_from_every_container(
    source_json: bool, incoming_json: bool
) -> None:
    api = _FakeApi()
    spec = _source_spec()
    for container in _containers(spec):
        container["env"] = [
            entry for entry in container["env"] if entry["name"] != "CURIE_MODEL_ENV_KEY"
        ]
    _containers(spec)[-1]["env"].append(
        {
            "name": "CURIE_MODEL_ENV_KEY",
            "value": json.dumps(_SOURCE_TARGETS) if source_json else _SOURCE_TARGETS[0],
        }
    )
    _seed_chart(api, spec=spec)
    env = _personal_env()
    env["CURIE_MODEL_ENV_KEY"] = json.dumps(_CLAIM_TARGETS) if incoming_json else _CLAIM_TARGETS[0]

    _create(api, env)

    blocked = {_SOURCE_TARGETS[0], _CLAIM_TARGETS[0]}
    if source_json:
        blocked.add(_SOURCE_TARGETS[1])
    if incoming_json:
        blocked.add(_CLAIM_TARGETS[1])
    for container in _containers(_copied_spec(api)):
        entries = {entry["name"]: entry for entry in container["env"]}
        assert blocked.isdisjoint(entries)
        assert (set(_SOURCE_TARGETS + _CLAIM_TARGETS) - blocked) <= entries.keys()
        assert _CONNECTOR_SECRET_REF in container["env"]
        assert entries["ACME_PUBLIC_FLAG"]["value"] == "public-control"
    claim_names = {entry["name"] for entry in api.objects[("sandboxclaims", _CLAIM)]["spec"]["env"]}
    assert blocked.isdisjoint(claim_names)


@pytest.mark.parametrize("grant", [None, ""], ids=["absent", "empty"])
def test_personal_claim_without_grant_cannot_inherit_installation_auth(grant: str | None) -> None:
    api = _FakeApi()
    _seed_chart(api, spec=_source_spec())
    pristine = copy.deepcopy(api.objects)
    env = {_MODE: "personal", "CURIE_BUDGET": "{}"}
    if grant is not None:
        env[_OAUTH] = grant

    _create(api, env)

    spec = _copied_spec(api)
    assert {"name": _MODE, "value": "personal"} in _runner_container(spec)["env"]
    for container in _containers(spec):
        names = {entry["name"] for entry in container["env"]}
        assert names.isdisjoint(_MODEL_NAMES | set(_SOURCE_TARGETS))
        assert "envFrom" not in container
        assert _CONNECTOR_SECRET_REF in container["env"]
    assert api.secrets[f"{_CLAIM}-tokens"]["stringData"] == {}
    assert api.objects[("sandboxwarmpools", f"{_CLAIM}-resources-pool")]["spec"]["replicas"] == 0
    assert api.objects[("sandboxclaims", _CLAIM)]["spec"]["warmPoolRef"]["name"] != _POOL
    _assert_shared_unchanged(api, pristine)


@pytest.mark.parametrize("mode", [None, "service"], ids=["default", "explicit-service"])
def test_service_claim_keeps_installation_credentials_and_connector_delivery(
    mode: str | None,
) -> None:
    api = _FakeApi()
    source = _seed_chart(api, spec=_source_spec(runner_first=False))
    pristine = copy.deepcopy(api.objects)
    env = {"CURIE_RUNNER_TOKEN": "runner-token-placeholder", "CURIE_BUDGET": "{}"}
    if mode is not None:
        env[_MODE] = mode

    _create(api, env)

    copied = _copied_spec(api)
    for container, original in zip(_containers(copied), _containers(source), strict=True):
        assert container["envFrom"] == original["envFrom"]
        if container["name"] == "runner":
            assert container["env"][:-1] == original["env"]
            assert container["env"][-1]["name"] == "CURIE_RUNNER_TOKEN"
            assert any(
                entry["name"] == "CURIE_CREDENTIALS" and "valueFrom" in entry
                for entry in container["env"]
            )
        else:
            assert container == original
    assert api.secrets[f"{_CLAIM}-tokens"]["stringData"] == {
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder"
    }
    _assert_shared_unchanged(api, pristine)


def test_service_oauth_uses_claim_secret_and_keeps_installation_auth() -> None:
    api = _FakeApi()
    source = _seed_chart(api, spec=_source_spec())
    pristine = copy.deepcopy(api.objects)

    _create(api, {_MODE: "service", _OAUTH: _GRANT})

    copied = _copied_spec(api)
    for container, original in zip(_containers(copied), _containers(source), strict=True):
        assert container["envFrom"] == original["envFrom"]
        if container["name"] == "runner":
            assert [entry for entry in container["env"] if entry["name"] != _OAUTH] == [
                entry for entry in original["env"] if entry["name"] != _OAUTH
            ]
            (grant,) = [entry for entry in container["env"] if entry["name"] == _OAUTH]
            assert grant["valueFrom"]["secretKeyRef"] == {
                "name": f"{_CLAIM}-tokens",
                "key": _OAUTH,
                "optional": False,
            }
        else:
            assert container == original
    assert api.secrets[f"{_CLAIM}-tokens"]["stringData"] == {_OAUTH: _GRANT}
    assert _GRANT not in json.dumps(api.created)
    _assert_shared_unchanged(api, pristine)


@pytest.mark.parametrize(
    ("verb", "plural"),
    [
        ("create", "secrets"),
        ("create", "sandboxwarmpools"),
        ("create", "sandboxclaims"),
        ("patch", "sandboxtemplates"),
    ],
    ids=["secret", "pool", "claim", "owner-reference"],
)
def test_personal_claim_failure_collects_grant_resources(verb: str, plural: str) -> None:
    api = _FakeApi()
    _seed_chart(api, spec=_source_spec())
    pristine = copy.deepcopy(api.objects)
    error = ApiException(status=500, reason="SyntheticFailure")
    api.failures[(verb, plural)] = error

    with pytest.raises(ApiException) as raised:
        _create(api, _personal_env())

    assert raised.value is error
    assert api.objects == pristine
    assert api.secrets == {}
    _assert_shared_unchanged(api, pristine)


def test_deleting_personal_claim_collects_grant_secret_and_its_cold_pool() -> None:
    api = _FakeApi()
    _seed_chart(api, spec=_source_spec())
    pristine = copy.deepcopy(api.objects)
    _create(api, _personal_env())
    claim = api.objects[("sandboxclaims", _CLAIM)]
    template = api.objects[("sandboxtemplates", f"{_CLAIM}-resources")]
    (template_owner,) = template["metadata"]["ownerReferences"]
    assert template_owner["uid"] == claim["metadata"]["uid"]
    for child in (
        api.secrets[f"{_CLAIM}-tokens"],
        api.objects[("sandboxwarmpools", f"{_CLAIM}-resources-pool")],
    ):
        (owner,) = child["metadata"]["ownerReferences"]
        assert owner["uid"] == template["metadata"]["uid"]

    _client(api).delete_claim(_CLAIM, request_timeout_seconds=1.0)

    assert api.objects == pristine
    assert api.secrets == {}


@pytest.mark.parametrize("container_name", ["runner", "connector-sidecar", "bundle-fetch"])
@pytest.mark.parametrize("reference", ["secretKeyRef", "configMapKeyRef"])
def test_personal_claim_refuses_unreadable_declaration_before_writes(
    container_name: str, reference: str
) -> None:
    api = _FakeApi()
    source = _source_spec()
    container = next(c for c in _containers(source) if c["name"] == container_name)
    declaration = next(
        entry for entry in container["env"] if entry["name"] == "CURIE_MODEL_ENV_KEY"
    )
    declaration.pop("value")
    declaration["valueFrom"] = {reference: {"name": "acme-model-declaration", "key": "names"}}
    _seed_chart(api, spec=source)
    pristine = copy.deepcopy(api.objects)

    with pytest.raises(ValueError, match="CURIE_MODEL_ENV_KEY"):
        _create(api, _personal_env())

    assert api.created == []
    assert api.secrets == {}
    assert api.patches == []
    assert api.objects == pristine
    stored = api.objects[("sandboxtemplates", _TEMPLATE)]["spec"]
    repaired = next(c for c in _containers(stored) if c["name"] == container_name)
    declaration = next(
        entry for entry in repaired["env"] if entry["name"] == "CURIE_MODEL_ENV_KEY"
    )
    declaration.pop("valueFrom")
    declaration["value"] = json.dumps(_SOURCE_TARGETS)

    _create(api, _personal_env())

    for copied in _containers(_copied_spec(api)):
        assert _CONNECTOR_SECRET_REF in copied["env"]
        assert {"name": "ACME_PUBLIC_FLAG", "value": "public-control"} in copied["env"]
        assert set(_SOURCE_TARGETS).isdisjoint(entry["name"] for entry in copied["env"])


def test_executor_claim_still_withholds_personal_grant() -> None:
    api = _FakeApi()
    _seed_chart(api)
    env = _personal_env()
    env["CURIE_MODEL_ENV_KEY"] = json.dumps([*_CLAIM_TARGETS, "CURIE_STATE_TOKEN"])
    env["CURIE_STATE_TOKEN"] = "declared-state-token-placeholder"
    _create(api, env, executor_secret_names=frozenset())

    assert api.secrets[f"{_CLAIM}-tokens"]["stringData"] == {
        "CURIE_RUNNER_TOKEN": "runner-token-placeholder"
    }
    assert _OAUTH not in {entry["name"] for entry in _runner_container(_copied_spec(api))["env"]}
    assert _GRANT not in json.dumps([api.created, list(api.secrets.values())])
