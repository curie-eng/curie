"""The end to end connector renders only when a test cluster is configured."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api import bundles
from curie_api.e2e_connector import E2EInstall, prepare_connectors
from plugin_format.connector_render import ConnectorProxy
from plugin_format.connectors import ConnectorsFile

_VECTOR = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/e2e-connector-sandbox.json").read_text()
)
_PROXY = ConnectorProxy(
    image="ghcr.io/curie-eng/curie-worker:0.0.0",
    public_keys=("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=",),
)
_SENTINEL = "kubeconfig-sentinel-value"
_INSTALL = E2EInstall(
    enabled=True,
    image="ghcr.io/curie-eng/curie-worker:test",
    namespace_prefix="curie-e2e-",
    owner_label_key="curietech.ai/e2e-owner",
    owner_label_value="acme",
    service_account="curie-e2e-connector",
    service_account_namespace="test-system",
    worker_cluster_role="curie-e2e-connector-namespace",
)


def _declared() -> ConnectorsFile:
    return ConnectorsFile.model_validate(
        {
            "connectors": {
                "e2e": {
                    "image": "curie-e2e-connector",
                    "secrets": ["E2E_CLUSTER_KUBECONFIG"],
                    "secret_files": {"E2E_CLUSTER_KUBECONFIG": "/secrets/kubeconfig"},
                    "env": {"NOTE": "not-a-credential"},
                },
                "grafana": {
                    "image": "grafana/mcp-grafana:0.17.2",
                    "secrets": ["GRAFANA_TOKEN"],
                },
            }
        }
    )


def _render(install: E2EInstall) -> tuple[list[dict], dict]:
    declared = prepare_connectors(_declared(), install)
    manifests = bundles.render_connector_manifests(
        declared,
        release="acme-rel",
        agent="acme-bot",
        namespace="acme-ns",
        app_name="curie",
        secret_name="acme-rel-acme-bot-connector-secrets",
        proxy=_PROXY,
        e2e=install,
    )
    entries = bundles.connector_mcp_entries(
        declared, release="acme-rel", agent="acme-bot", namespace="acme-ns"
    )
    return manifests, entries


def test_an_unconfigured_install_renders_no_connector_and_refuses() -> None:
    with pytest.raises(ValueError, match="e2e_connector_not_configured"):
        _render(E2EInstall())


def test_a_configured_install_renders_tools_without_the_credential() -> None:
    manifests, entries = _render(_INSTALL)
    rendered = json.dumps({"manifests": manifests, "mcp": entries})
    assert _SENTINEL not in rendered
    assert "E2E_CLUSTER_KUBECONFIG" not in json.dumps(entries)
    deployment = next(item for item in manifests if item["kind"] == "Deployment")
    server = next(
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container["name"] == "server"
    )
    assert server["image"] == _INSTALL.image
    assert server["command"] == ["python", "-m", "curie_e2e_connector"]
    assert server["args"] == []
    grafana = next(
        item
        for item in manifests
        if item["kind"] == "Deployment" and item["metadata"]["name"].endswith("-mcp-grafana")
    )
    grafana_server = next(
        container
        for container in grafana["spec"]["template"]["spec"]["containers"]
        if container["name"] == "server"
    )
    assert grafana_server["image"] == "grafana/mcp-grafana:0.17.2"
    assert "command" not in grafana_server
    env = {item["name"]: item.get("value") for item in server["env"] if "value" in item}
    assert env["E2E_NAMESPACE_PREFIX"] == "curie-e2e-"
    assert env["E2E_OWNER_LABEL_VALUE"] == "acme"
    assert "value" not in json.dumps(server.get("volumeMounts"))
    mounts = json.dumps(deployment["spec"]["template"]["spec"].get("volumes"))
    assert "E2E_CLUSTER_KUBECONFIG" in mounts
    assert _SENTINEL not in mounts
    assert entries["e2e"]["url"].startswith("http://")
    assert "headers" not in entries["e2e"]
    assert bundles.owned_secret_keys(prepare_connectors(_declared(), _INSTALL)) == [
        "E2E_CLUSTER_KUBECONFIG",
        "GRAFANA_TOKEN",
    ]


def test_vector_names_match_the_api() -> None:
    from curie_api import e2e_connector as mod

    assert mod.CONNECTOR_NAME == _VECTOR["connector_name"]
    assert mod.KUBECONFIG_SECRET == _VECTOR["kubeconfig_secret"]
    assert mod.KUBECONFIG_MOUNT == _VECTOR["kubeconfig_mount"]
    assert mod.SERVER_MODULE == _VECTOR["server_module"]
    assert mod.REFUSAL_NOT_CONFIGURED == _VECTOR["refusal_not_configured"]
