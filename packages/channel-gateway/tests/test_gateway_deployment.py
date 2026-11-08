"""Rendered workloads preserve gateway credential custody and TLS requirements."""

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


def render(*settings):
    return subprocess.run(
        [
            "helm",
            "template",
            "acme",
            "charts/curie",
            "-f",
            "charts/curie/values-dev.yaml",
            *[argument for value in settings for argument in ("--set", value)],
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_gateway_chart_custody_and_persistent_replay():
    result = render("channelGateway.deploy=true", "channelGateway.allowInsecure=true")
    assert result.returncode == 0, result.stderr
    objects = list(yaml.safe_load_all(result.stdout))
    deployments = {
        item["metadata"]["name"]: item for item in objects if item and item["kind"] == "Deployment"
    }
    for suffix, caller in (("api", "Api"), ("worker", "Worker")):
        deployment = deployments["acme-curie-" + suffix]
        env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        names = {entry["name"] for entry in env}
        assert not {"SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_USER_TOKEN"} & names
        credential = next(entry for entry in env if entry["name"] == "CURIE_CHANNEL_GATEWAY_TOKEN")
        assert credential["valueFrom"]["secretKeyRef"]["key"] == "channelGateway" + caller + "Token"
    gateway = next(item for name, item in deployments.items() if name.endswith("-channel-gateway"))
    spec = gateway["spec"]
    assert spec["replicas"] == 1 and spec["strategy"]["type"] == "Recreate"
    pod = spec["template"]["spec"]
    env = pod["containers"][0]["env"]
    assert "SLACK_BOT_TOKEN" in {entry["name"] for entry in env}
    assert "SLACK_APP_TOKEN" not in {entry["name"] for entry in env}
    assert any(volume.get("persistentVolumeClaim") for volume in pod["volumes"])


def test_gateway_chart_refuses_an_implicit_http_listener():
    result = render("channelGateway.deploy=true")
    assert result.returncode != 0
    assert "channelGateway.tlsSecret is required" in result.stderr
    assert (
        render("channelGateway.deploy=true", "channelGateway.tlsSecret=acme-gateway-tls").returncode
        == 0
    )


@pytest.mark.parametrize(
    ("tls_secret", "allow_http", "scheme"),
    [("acme-gateway-tls", False, "https"), ("acme-gateway-tls", True, "https"), ("", True, "http")],
)
def test_gateway_chart_clients_match_the_listener(tls_secret, allow_http, scheme):
    result = render(
        "channelGateway.deploy=true",
        f"channelGateway.tlsSecret={tls_secret}",
        f"channelGateway.allowInsecure={str(allow_http).lower()}",
    )
    assert result.returncode == 0, result.stderr
    deployments = {
        item["metadata"]["name"]: item
        for item in yaml.safe_load_all(result.stdout)
        if item and item["kind"] == "Deployment"
    }
    for suffix in ("api", "worker"):
        env = deployments["acme-curie-" + suffix]["spec"]["template"]["spec"]["containers"][0][
            "env"
        ]
        endpoint = next(
            entry["value"] for entry in env if entry["name"] == "CURIE_CHANNEL_GATEWAY_URL"
        )
        assert endpoint == f"{scheme}://acme-curie-channel-gateway:8081"
    listener = deployments["acme-curie-channel-gateway"]["spec"]["template"]["spec"]["containers"][
        0
    ]
    assert (
        "CURIE_CHANNEL_GATEWAY_TLS_CERT" in {entry["name"] for entry in listener["env"]}
    ) == bool(tls_secret)


def test_compose_only_provider_services_hold_slack_tokens():
    document = yaml.safe_load((ROOT / "compose.dev.yaml").read_text())
    for name in ("curie-api", "curie-worker"):
        env = document["services"][name]["environment"]
        names = set(env) if isinstance(env, dict) else {item.split("=", 1)[0] for item in env}
        assert not {"SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_USER_TOKEN"} & names
        assert "CURIE_CHANNEL_GATEWAY_TOKEN" in names
