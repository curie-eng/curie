"""The ADR 0274 live provider tier reaches its model from a clean checkout.

The hosted connector session upgrades the qualification install with
``helm upgrade --reuse-values -f <tier values>``. These tests render the chart
with the install's scripted placeholder credential and stub egress, layer the
tier values the same way (maps merge, lists replace), and read back what the
runner sandbox would get. No cluster, container, network or model key is used.
"""

from __future__ import annotations

import base64
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from curie_runner.sdk_auth import resolve_sdk_env

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "cli/tests/local"))

from test_webhook_wake_qualification import (  # noqa: E402
    ANTHROPIC_API_CIDR,
    live_model_egress,
    provider_tier_values,
)

RELEASE = "wake-live"
SECRET = "acme-model-credential"
LIVE_KEY = "sk-ant-api03-PLACEHOLDER-live-tier"
STUB = {"cidr": "192.0.2.10/32", "ports": [{"protocol": "TCP", "port": 18080}]}
# The parts of WakeStack.start's install values this tier depends on: the
# scripted placeholder credential and the stub model URL with its egress.
INSTALL = {
    "worker": {
        "extraEnv": [{"name": "CURIE_MODEL_BASE_URL", "value": "http://192.0.2.10:18080/model"}]
    },
    "security": {"gvisor": {"mode": "off"}, "networkPolicy": {"allowedEgress": [STUB]}},
    "agentSandbox": {
        "deploy": True,
        "controller": {"deploy": True},
        "poolAgents": ["acme-wake-idle"],
        "runner": {"fakeModel": False, "credentials": "not-needed"},
    },
}


def render(tmp_path, *layers):
    """Render the chart with dev values plus each layer, in order."""
    files = []
    for index, layer in enumerate(layers):
        path = tmp_path / f"layer-{index}.yaml"
        path.write_text(yaml.safe_dump(layer))
        files += ["-f", str(path)]
    rendered = subprocess.run(
        [
            "helm",
            "template",
            RELEASE,
            str(ROOT / "charts/curie"),
            "--namespace",
            RELEASE,
            "-f",
            str(ROOT / "charts/curie/values-dev.yaml"),
            *files,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


def runner_envs(docs):
    """Each runner sandbox template's environment, with secret references read.

    A reference resolves against the chart's own rendered Secrets, or against
    the live credential Secret the test creates beside the release.
    """
    secrets = {SECRET: {"key": LIVE_KEY}}
    for doc in docs:
        if doc["kind"] == "Secret":
            values = dict(doc.get("stringData") or {})
            for key, value in (doc.get("data") or {}).items():
                values[key] = base64.b64decode(value).decode()
            secrets[doc["metadata"]["name"]] = values
    envs = []
    for doc in docs:
        if doc["kind"] != "SandboxTemplate":
            continue
        for container in doc["spec"]["podTemplate"]["spec"]["containers"]:
            if container["name"] != "runner":
                continue
            env = {}
            for entry in container.get("env", []):
                ref = entry.get("valueFrom", {}).get("secretKeyRef")
                if ref:
                    env[entry["name"]] = secrets[ref["name"]][ref["key"]]
                elif "value" in entry:
                    env[entry["name"]] = entry["value"]
            envs.append(env)
    assert envs, "no runner sandbox template rendered"
    return envs


def model_key(env):
    """The API key the runner's SDK session would send for ``env``."""
    env = dict(env)
    override = resolve_sdk_env(env)
    return (override or env).get("ANTHROPIC_API_KEY")


def runner_egress(docs):
    """(cidr, port) pairs the runner sandbox may dial from security.networkPolicy.allowedEgress."""
    [policy] = [
        doc
        for doc in docs
        if doc["kind"] == "NetworkPolicy"
        and doc["metadata"]["name"].endswith("-runner-allow-egress")
    ]
    return {
        (peer["ipBlock"]["cidr"], port["port"])
        for rule in policy["spec"]["egress"]
        for peer in rule["to"]
        for port in rule["ports"]
    }


@pytest.mark.parametrize("base_url", ["", "https://api.anthropic.com"])
def test_live_key_replaces_the_scripted_placeholder(tmp_path, base_url):
    # Negative control, the shape the live tier used before: the key in
    # ANTHROPIC_API_KEY loses to the CURIE_CREDENTIALS placeholder once a base
    # URL is set, because the runner reads the declared credential key first
    # (runner/src/curie_runner/sdk_auth.py, resolve_sdk_env).
    if base_url:
        extra = [
            {"name": "CURIE_MODEL_BASE_URL", "value": base_url},
            {
                "name": "ANTHROPIC_API_KEY",
                "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "key"}},
            },
        ]
        before = {"agentSandbox": {"runner": {"extraEnv": extra}}, "worker": {"extraEnv": extra}}
        assert {model_key(env) for env in runner_envs(render(tmp_path, INSTALL, before))} == {
            "not-needed"
        }

    tier = provider_tier_values(
        base_url=base_url,
        model="",
        credential_secret=SECRET,
        allowed_egress=None,
    )
    for env in runner_envs(render(tmp_path, INSTALL, tier)):
        assert env["CURIE_CREDENTIALS"] == LIVE_KEY
        assert env.get("CURIE_MODEL_BASE_URL", "") == base_url
        assert model_key(env) == LIVE_KEY


def test_live_tier_opens_runner_egress_to_the_model(tmp_path):
    assert (ANTHROPIC_API_CIDR, 443) not in runner_egress(render(tmp_path, INSTALL))

    egress = [STUB, *live_model_egress("", "")]
    tier = provider_tier_values(
        base_url="",
        model="",
        credential_secret=SECRET,
        allowed_egress=egress,
    )
    allowed = runner_egress(render(tmp_path, INSTALL, tier))
    assert (ANTHROPIC_API_CIDR, 443) in allowed
    assert ("192.0.2.10/32", 18080) in allowed


def test_live_model_egress_is_declared_never_resolved():
    assert live_model_egress("https://api.anthropic.com/", "") == [
        {"cidr": ANTHROPIC_API_CIDR, "ports": [{"protocol": "TCP", "port": 443}]}
    ]
    assert live_model_egress(
        "http://model.example.test:8443/v1", "198.51.100.7/32, 2001:db8::/48"
    ) == [
        {"cidr": "198.51.100.7/32", "ports": [{"protocol": "TCP", "port": 8443}]},
        {"cidr": "2001:db8::/48", "ports": [{"protocol": "TCP", "port": 8443}]},
    ]
    with pytest.raises(RuntimeError, match="WAKE_TEST_MODEL_EGRESS_CIDRS"):
        live_model_egress("https://model.example.test", "")
