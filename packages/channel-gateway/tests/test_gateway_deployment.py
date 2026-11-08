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


def test_gateway_chart_provisions_tls_without_operator_values():
    result = render("channelGateway.deploy=true")
    assert result.returncode == 0, result.stderr
    docs = [item for item in yaml.safe_load_all(result.stdout) if item]
    secret = next(
        item for item in docs if item["metadata"]["name"] == "acme-curie-channel-gateway-tls"
    )
    assert secret["kind"] == "Secret"
    assert set(secret["data"]) == {"ca.crt", "tls.crt", "tls.key"}
    assert (
        render("channelGateway.deploy=true", "channelGateway.tlsSecret=acme-gateway-tls").returncode
        == 0
    )


@pytest.mark.parametrize(
    ("tls_secret", "allow_http", "scheme"),
    [
        ("", False, "https"),
        ("acme-gateway-tls", False, "https"),
        ("acme-gateway-tls", True, "https"),
        ("", True, "http"),
    ],
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
        insecure = next(
            entry["value"]
            for entry in env
            if entry["name"] == "CURIE_CHANNEL_GATEWAY_ALLOW_INSECURE"
        )
        assert insecure == str(scheme == "http").lower()
    listener = deployments["acme-curie-channel-gateway"]["spec"]["template"]["spec"]["containers"][
        0
    ]
    assert ("CURIE_CHANNEL_GATEWAY_TLS_CERT" in {entry["name"] for entry in listener["env"]}) == (
        scheme == "https"
    )


def test_compose_only_provider_services_hold_slack_tokens():
    document = yaml.safe_load((ROOT / "compose.dev.yaml").read_text())
    for name in ("curie-api", "curie-worker"):
        env = document["services"][name]["environment"]
        names = set(env) if isinstance(env, dict) else {item.split("=", 1)[0] for item in env}
        assert not {"SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_USER_TOKEN"} & names
        assert "CURIE_CHANNEL_GATEWAY_TOKEN" in names


@pytest.mark.parametrize("identity", ["inline", "reference", "listed"])
def test_slack_upgrade_requires_a_gateway(identity):
    settings = {
        "inline": [
            "dispatcher.slack.botToken=xoxb-PLACEHOLDER",
            "dispatcher.slack.appToken=xapp-PLACEHOLDER",
        ],
        "reference": ["dispatcher.slack.botTokenExistingSecret=acme-slack"],
        "listed": [
            "dispatcher.slack.identities[0].name=default",
            "dispatcher.slack.identities[0].botTokenExistingSecret=acme-slack",
            "dispatcher.slack.identities[0].appTokenExistingSecret=acme-slack",
        ],
    }[identity]
    automatic = render(*settings)
    assert automatic.returncode == 0, automatic.stderr
    assert "name: acme-curie-channel-gateway" in automatic.stdout
    result = render(*settings, "channelGateway.deploy=false")
    assert result.returncode != 0
    assert "set channelGateway.deploy=true or channelGateway.url" in result.stderr
    assert render(*settings, "channelGateway.url=https://gateway.example.com").returncode == 0
    assert (
        render(
            *settings, "channelGateway.deploy=true", "channelGateway.tlsSecret=acme-tls"
        ).returncode
        == 0
    )


@pytest.mark.parametrize("deploy", [False, True])
def test_gateway_tls_secret_rejects_an_explicit_http_url(deploy):
    result = render(
        f"channelGateway.deploy={str(deploy).lower()}",
        "channelGateway.tlsSecret=acme-tls",
        "channelGateway.allowInsecure=true",
        "channelGateway.url=http://gateway.example.com",
    )
    assert result.returncode != 0
    assert "channelGateway.url must use https when channelGateway.tlsSecret is set" in result.stderr


def test_gateway_remains_off_without_slack_configuration():
    result = render()
    assert result.returncode == 0, result.stderr
    assert "name: acme-curie-channel-gateway" not in result.stdout


def test_unchanged_slack_secret_references_move_to_gateway():
    result = render(
        "dispatcher.slack.botTokenExistingSecret=acme-slack",
        "dispatcher.slack.botTokenExistingSecretKey=bot",
        "dispatcher.slack.appTokenExistingSecret=acme-slack",
        "dispatcher.slack.appTokenExistingSecretKey=app",
    )
    assert result.returncode == 0, result.stderr
    deployments = {
        item["metadata"]["name"]: item
        for item in yaml.safe_load_all(result.stdout)
        if item and item["kind"] == "Deployment"
    }
    for suffix in ("api", "worker", "channel-gateway"):
        pod = deployments["acme-curie-" + suffix]["spec"]["template"]["spec"]
        env = {entry["name"]: entry for entry in pod["containers"][0]["env"]}
        assert pod["enableServiceLinks"] is False
        if suffix == "channel-gateway":
            assert env["SLACK_BOT_TOKEN"]["valueFrom"]["secretKeyRef"] == {
                "name": "acme-slack",
                "key": "bot",
            }
        else:
            assert "SLACK_BOT_TOKEN" not in env
            assert (
                env["CURIE_CHANNEL_GATEWAY_CA_FILE"]["value"] == "/var/run/curie/gateway-tls/ca.crt"
            )
            assert any(
                volume.get("secret", {}).get("secretName") == "acme-curie-channel-gateway-tls"
                for volume in pod["volumes"]
            )
