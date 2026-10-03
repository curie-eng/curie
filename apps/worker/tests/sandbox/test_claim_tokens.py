"""Claim-scoped token delivery helpers (#3842).

``SandboxClaim.spec.env`` is value-only, so every scoped token written there is
stored in plain text in etcd and is readable by anyone who can ``get`` claims.
``claim_tokens`` is the pure half of the fix: it decides which boot-env keys are
tokens, splits them off the claim env, names the per-claim objects, and builds
the per-claim SandboxTemplate spec whose runner container reads each token from
the per-claim Secret by ``secretKeyRef``.

Token names are compared against the boot contract (``BootEnv``), never against
the module's own tuple, so a renamed or newly declared token cannot keep these
tests green while it lands on the claim in plain text.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest
from aci_protocol import BootEnv
from curie_worker.sandbox.claim_tokens import (
    CLAIM_LABEL,
    CLAIM_TOKEN_ENVS,
    claim_object_names,
    claim_template_spec,
    split_claim_tokens,
)
from curie_worker.sandbox.resources import resources_object_name
from curie_worker.sandbox.types import SubstrateConfig

_OVERRIDE: dict[str, Any] = {
    "requests": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "1Gi"},
    "limits": {"cpu": "1", "memory": "2Gi", "ephemeral-storage": "4Gi"},
}


def _chart_spec() -> dict[str, Any]:
    """A SandboxTemplate spec in the shape the chart renders for a per-agent pool."""

    resources = {
        "requests": {"cpu": "50m", "memory": "192Mi", "ephemeral-storage": "512Mi"},
        "limits": {"cpu": "1", "memory": "768Mi", "ephemeral-storage": "4Gi"},
    }
    return {
        "service": True,
        "envVarsInjectionPolicy": "Overrides",
        "networkPolicyManagement": "Unmanaged",
        "podTemplate": {
            "metadata": {"labels": {"curietech.ai/agent": "acme-a"}},
            "spec": {
                "automountServiceAccountToken": False,
                "runtimeClassName": "gvisor",
                "containers": [
                    {
                        "name": "runner",
                        "image": "example.com/runner:dev",
                        "env": [
                            {
                                "name": "GITHUB_PERSONAL_ACCESS_TOKEN",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "curie-agent-acme-a-connector-secrets",
                                        "key": "GITHUB_PERSONAL_ACCESS_TOKEN",
                                        "optional": False,
                                    }
                                },
                            },
                            {
                                "name": "CURIE_CREDENTIALS",
                                "valueFrom": {
                                    "secretKeyRef": {"name": "curie", "key": "agentCredentials"}
                                },
                            },
                            # A warm-pod placeholder of a token key: the copy
                            # must replace it, never carry two entries.
                            {"name": "CURIE_RUNNER_TOKEN", "value": "warm-placeholder"},
                            {"name": "CURIE_SESSION_ID", "value": "warm-unbound"},
                        ],
                        "resources": copy.deepcopy(resources),
                    }
                ],
                "initContainers": [
                    {
                        "name": "bundle-fetch",
                        "image": "example.com/aws-cli:dev",
                        "env": [{"name": "CURIE_BUNDLE_REF", "value": ""}],
                        "resources": copy.deepcopy(resources),
                    },
                    {
                        "name": "workspace-init",
                        "image": "example.com/runner:dev",
                        "resources": copy.deepcopy(resources),
                    },
                ],
            },
        },
    }


def _runner(spec: dict[str, Any]) -> dict[str, Any]:
    containers = spec["podTemplate"]["spec"]["containers"]
    return next(c for c in containers if c["name"] == "runner")


# -- 1. token inventory is complete against the boot contract -----------------


def test_claim_token_envs_cover_every_boot_env_token() -> None:
    declared_tokens = {key for key in BootEnv.env_keys() if key.endswith("_TOKEN")}
    # The guard is only meaningful if the contract still declares tokens; an
    # empty set would make the subset check vacuous.
    assert declared_tokens, "BootEnv declares no *_TOKEN keys; the guard is vacuous"
    missing = declared_tokens - set(CLAIM_TOKEN_ENVS)
    assert not missing, (
        f"boot-env tokens {sorted(missing)} are not in CLAIM_TOKEN_ENVS and would be "
        "persisted in plain text on every SandboxClaim"
    )
    # The scoped prefixes AC1 names, spelled from the contract.
    for field in (
        "runner_token",
        "history_token",
        "memory_token",
        "state_token",
        "progress_token",
        "issue_read_token",
        "connector_caller_token",
    ):
        assert BootEnv.env_key(field) in CLAIM_TOKEN_ENVS
    # Every entry is a real boot-env key: a typo would strip nothing.
    assert set(CLAIM_TOKEN_ENVS) <= set(BootEnv.env_keys())
    assert len(set(CLAIM_TOKEN_ENVS)) == len(CLAIM_TOKEN_ENVS)


def test_claim_label_is_the_admission_label() -> None:
    # The cleanup and worker-secrets admission policies key on this exact label.
    assert CLAIM_LABEL == "curietech.ai/sandbox-claim"


# -- 2. split ------------------------------------------------------------------


def test_split_claim_tokens_moves_only_token_keys() -> None:
    env = {
        "CURIE_BUDGET": "{}",
        "CURIE_SESSION_ID": "s-1",
        "CURIE_BUNDLE_REF": "bundles/x.tar.gz",
        BootEnv.env_key("runner_token"): "f3b2c1d0e9a8",
        BootEnv.env_key("state_token"): "sbx.state.sig",
        BootEnv.env_key("connector_caller_token"): "cct.payload.signature",
        BootEnv.env_key("issue_read_token"): "wir.payload.sig",
        # Empty means "this turn has no such token": absent, not an empty
        # Secret key the runner would read as a credential.
        BootEnv.env_key("memory_token"): "",
        # Not a token: the connector-secret marker is stripped later by the
        # existing claim filter, not moved into the token Secret.
        "CURIE_CONNECTOR_SECRET_KEYS": "API_KEY",
    }
    before = dict(env)

    tokens, rest = split_claim_tokens(env)

    assert tokens == {
        BootEnv.env_key("runner_token"): "f3b2c1d0e9a8",
        BootEnv.env_key("state_token"): "sbx.state.sig",
        BootEnv.env_key("connector_caller_token"): "cct.payload.signature",
        BootEnv.env_key("issue_read_token"): "wir.payload.sig",
    }
    assert rest == {
        "CURIE_BUDGET": "{}",
        "CURIE_SESSION_ID": "s-1",
        "CURIE_BUNDLE_REF": "bundles/x.tar.gz",
        "CURIE_CONNECTOR_SECRET_KEYS": "API_KEY",
    }
    # No token key survives in rest, empty or not.
    assert set(rest).isdisjoint(CLAIM_TOKEN_ENVS)
    assert env == before, "the caller's env must not be mutated"


def test_split_claim_tokens_of_a_token_free_env_is_empty() -> None:
    tokens, rest = split_claim_tokens({"CURIE_BUDGET": "{}"})
    assert tokens == {}
    assert rest == {"CURIE_BUDGET": "{}"}


# -- 3. secret refs on the runner only ----------------------------------------


def test_claim_template_spec_injects_secret_refs_on_the_runner_only() -> None:
    source = _chart_spec()
    pristine = copy.deepcopy(source)
    token_names = [BootEnv.env_key("runner_token"), BootEnv.env_key("state_token")]

    spec = claim_template_spec(
        source,
        secret_name="curie-thread-0123456789-abc123-tokens",
        token_names=token_names,
        runner_resources=None,
    )

    assert source == pristine, "the source template spec must not be mutated"

    env = _runner(spec)["env"]
    for key in token_names:
        matching = [entry for entry in env if entry["name"] == key]
        assert matching == [
            {
                "name": key,
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "curie-thread-0123456789-abc123-tokens",
                        "key": key,
                        "optional": False,
                    }
                },
            }
        ], f"{key} must appear exactly once, as a secretKeyRef"
    # No plaintext value of any token remains on the runner.
    assert all(
        "value" not in entry for entry in env if entry["name"] in CLAIM_TOKEN_ENVS
    )
    # Every pre-existing non-token entry survives verbatim (#1488: per-agent
    # connector secrets and the model credential ride the template).
    source_env = _runner(pristine)["env"]
    for entry in source_env:
        if entry["name"] not in token_names:
            assert entry in env
    # Init containers are untouched: a token reaches only the model's runner.
    assert spec["podTemplate"]["spec"]["initContainers"] == pristine["podTemplate"]["spec"][
        "initContainers"
    ]
    # Every other field is a copy.
    stripped = copy.deepcopy(spec)
    stripped_source = copy.deepcopy(pristine)
    _runner(stripped).pop("env")
    _runner(stripped_source).pop("env")
    assert stripped == stripped_source


def test_claim_template_spec_appends_when_the_runner_has_no_env() -> None:
    source = _chart_spec()
    del _runner(source)["env"]
    key = BootEnv.env_key("history_token")

    spec = claim_template_spec(
        source, secret_name="c-tokens", token_names=[key], runner_resources=None
    )

    assert _runner(spec)["env"] == [
        {
            "name": key,
            "valueFrom": {"secretKeyRef": {"name": "c-tokens", "key": key, "optional": False}},
        }
    ]


# -- 4. runner resources -------------------------------------------------------


def test_claim_template_spec_applies_runner_resources_to_every_container() -> None:
    source = _chart_spec()

    spec = claim_template_spec(
        source,
        secret_name="c-tokens",
        token_names=[BootEnv.env_key("runner_token")],
        runner_resources=_OVERRIDE,
    )

    pod = spec["podTemplate"]["spec"]
    for container in [*pod["containers"], *pod["initContainers"]]:
        assert container["resources"] == _OVERRIDE, container["name"]
    # And the token ref is still injected alongside the resources change.
    assert any(
        entry["name"] == BootEnv.env_key("runner_token") and "valueFrom" in entry
        for entry in _runner(spec)["env"]
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"requests": _OVERRIDE["requests"]},
        {**_OVERRIDE, "claims": []},
        {"requests": {"cpu": "1"}, "limits": _OVERRIDE["limits"]},
        {"requests": _OVERRIDE["requests"], "limits": "2Gi"},
    ],
    ids=["missing-limits", "extra-key", "missing-dimension", "non-mapping"],
)
def test_claim_template_spec_refuses_an_invalid_resources_shape(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        claim_template_spec(
            _chart_spec(),
            secret_name="c-tokens",
            token_names=[BootEnv.env_key("runner_token")],
            runner_resources=bad,
        )


# -- 5. no runner container ---------------------------------------------------


def test_claim_template_spec_without_runner_container_raises() -> None:
    source = _chart_spec()
    _runner(source)["name"] = "agent"

    with pytest.raises(ValueError, match="runner container"):
        claim_template_spec(
            source,
            secret_name="c-tokens",
            token_names=[BootEnv.env_key("runner_token")],
            runner_resources=None,
        )


def test_claim_template_spec_finds_the_runner_when_it_is_not_first() -> None:
    # Liveness pair for the refusal above: the lookup is by name, not position.
    source = _chart_spec()
    containers = source["podTemplate"]["spec"]["containers"]
    containers.insert(0, {"name": "sidecar", "image": "example.com/runner:dev", "env": []})
    key = BootEnv.env_key("runner_token")

    spec = claim_template_spec(
        source, secret_name="c-tokens", token_names=[key], runner_resources=None
    )

    out = spec["podTemplate"]["spec"]["containers"]
    assert out[0] == {"name": "sidecar", "image": "example.com/runner:dev", "env": []}
    assert any(e["name"] == key and "valueFrom" in e for e in _runner(spec)["env"])


# -- 6. object names -----------------------------------------------------------


def test_claim_object_names_pass_the_admission_suffix_rules() -> None:
    # A realistic claim name, produced the way the substrate produces it.
    claim = SubstrateConfig(namespace="ns", warm_pool="pool").claim_name_for("T1", "abc123")
    assert len(claim) == 30

    names = claim_object_names(claim)

    assert names.template == f"{claim}-resources"
    assert names.pool == f"{claim}-resources-pool"
    assert names.secret == f"{claim}-tokens"
    # The existing admission policy admits worker writes only under these
    # suffixes; both names must pass the same gate the resources path uses.
    assert resources_object_name("template", names.template) == names.template
    assert resources_object_name("warmpool", names.pool) == names.pool
    for name in (names.template, names.pool, names.secret):
        assert len(name) <= 63


def test_claim_object_names_accept_the_longest_claim_that_fits() -> None:
    # 63 minus len("-resources-pool") == 48: the boundary is accepted.
    claim = "c" * 48
    names = claim_object_names(claim)
    assert len(names.pool) == 63


def test_claim_object_names_refuse_an_over_long_claim() -> None:
    with pytest.raises(ValueError):
        claim_object_names("c" * 49)
