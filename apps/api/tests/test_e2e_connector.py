"""The end to end connector renders only when a test cluster is configured."""

from __future__ import annotations

import json
from dataclasses import replace
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


_PUSH = "E2E_REGISTRY_PUSH_CONFIG"
_CACHE = "E2E_BUILD_CACHE_CONFIG"
_BUILD_INSTALL = replace(
    _INSTALL,
    registry="registry.test:5000/e2e",
    build_cache_repo="registry.test:5000/e2e/cache",
    registry_insecure=True,
    registry_token_hosts="auth.test,auth2.test:8443",
    builder_image="builder.test/kaniko:1",
    git_image="git.test/git:1",
    push_image="push.test/crane:1",
    build_timeout_seconds=900,
    source_hosts="github.com,git.test",
)


def _declared(
    extra_secrets: tuple[str, ...] = (), extra_files: dict[str, str] | None = None
) -> ConnectorsFile:
    return ConnectorsFile.model_validate(
        {
            "connectors": {
                "e2e": {
                    "image": "curie-e2e-connector",
                    "secrets": ["E2E_CLUSTER_KUBECONFIG", *extra_secrets],
                    "secret_files": {
                        "E2E_CLUSTER_KUBECONFIG": "/secrets/kubeconfig",
                        **(extra_files or {}),
                    },
                    "env": {"NOTE": "not-a-credential"},
                },
                "grafana": {
                    "image": "grafana/mcp-grafana:0.17.2",
                    "secrets": ["GRAFANA_TOKEN"],
                },
            }
        }
    )


def _render(
    install: E2EInstall, declared_file: ConnectorsFile | None = None
) -> tuple[list[dict], dict]:
    declared = prepare_connectors(declared_file or _declared(), install)
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


def test_vector_names_for_the_image_build_match_the_api() -> None:
    from curie_api import e2e_connector as mod

    assert mod.REGISTRY_PUSH_SECRET == _VECTOR["registry_push_secret"]
    assert mod.REGISTRY_PUSH_MOUNT == _VECTOR["registry_push_mount"]
    assert mod.BUILD_CACHE_SECRET == _VECTOR["build_cache_secret"]
    assert mod.BUILD_CACHE_MOUNT == _VECTOR["build_cache_mount"]
    assert list(_VECTOR["withheld_from_sandbox"]) == [
        _VECTOR["kubeconfig_secret"],
        _VECTOR["registry_push_secret"],
        _VECTOR["build_cache_secret"],
    ]


def test_every_new_platform_env_reaches_the_connector_render() -> None:
    from curie_api import e2e_connector as mod

    new = {
        "E2E_REGISTRY",
        "E2E_BUILD_CACHE_REPO",
        "E2E_REGISTRY_INSECURE",
        "E2E_REGISTRY_TOKEN_HOSTS",
        "E2E_BUILDER_IMAGE",
        "E2E_GIT_IMAGE",
        "E2E_PUSH_IMAGE",
        "E2E_BUILD_TIMEOUT_SECONDS",
        "E2E_SOURCE_HOSTS",
    }
    assert new <= set(mod.PLATFORM_ENV)
    assert mod.PLATFORM_ENV.index("PORT") == len(mod.PLATFORM_ENV) - 1
    assert set(_BUILD_INSTALL.platform_env()) == set(mod.PLATFORM_ENV)
    manifests, _entries = _render(_BUILD_INSTALL)
    deployment = next(item for item in manifests if item["kind"] == "Deployment")
    server = next(
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container["name"] == "server"
    )
    env = {item["name"]: item.get("value") for item in server["env"] if "value" in item}
    assert new <= set(env)
    assert env["E2E_REGISTRY"] == "registry.test:5000/e2e"
    assert env["E2E_BUILD_CACHE_REPO"] == "registry.test:5000/e2e/cache"
    assert env["E2E_REGISTRY_TOKEN_HOSTS"] == "auth.test,auth2.test:8443"
    assert env["E2E_BUILDER_IMAGE"] == "builder.test/kaniko:1"
    assert env["E2E_GIT_IMAGE"] == "git.test/git:1"
    assert env["E2E_PUSH_IMAGE"] == "push.test/crane:1"
    assert env["E2E_BUILD_TIMEOUT_SECONDS"] == "900"
    assert env["E2E_SOURCE_HOSTS"] == "github.com,git.test"
    assert env["E2E_REGISTRY_INSECURE"] in ("true", "True", "1")


def test_a_platform_env_name_declared_by_the_bundle_is_overridden() -> None:
    declared = _declared()
    spec = declared.connectors["e2e"].model_copy(
        update={"env": {"E2E_REGISTRY": "evil.test/x", "E2E_PUSH_IMAGE": "evil.test/crane"}}
    )
    forged = declared.model_copy(update={"connectors": {**declared.connectors, "e2e": spec}})
    prepared = prepare_connectors(forged, _BUILD_INSTALL)
    env = prepared.connectors["e2e"].env
    assert env["E2E_REGISTRY"] == "registry.test:5000/e2e"
    assert env["E2E_PUSH_IMAGE"] == "push.test/crane:1"


def test_declared_registry_configs_mount_at_their_fixed_paths_and_stay_off_the_mcp_entry() -> None:
    declared = _declared(
        extra_secrets=(_PUSH, _CACHE),
        extra_files={_PUSH: "/elsewhere/push.json", _CACHE: "/elsewhere/cache.json"},
    )
    prepared = prepare_connectors(declared, _BUILD_INSTALL)
    files = prepared.connectors["e2e"].secret_files
    assert files[_PUSH] == "/secrets/registry/config.json"
    assert files[_CACHE] == "/secrets/registry-cache/config.json"
    manifests, entries = _render(_BUILD_INSTALL, declared)
    deployment = next(item for item in manifests if item["kind"] == "Deployment")
    pod = deployment["spec"]["template"]["spec"]
    server = next(c for c in pod["containers"] if c["name"] == "server")
    mounts = json.dumps(server.get("volumeMounts"))
    assert "/secrets/registry/config.json" in mounts
    assert "/secrets/registry-cache/config.json" in mounts
    assert "/secrets/kubeconfig" in mounts
    volumes = json.dumps(pod.get("volumes"))
    assert _PUSH in volumes
    assert _CACHE in volumes
    assert _PUSH not in json.dumps(entries)
    assert _CACHE not in json.dumps(entries)
    assert "push-sentinel" not in json.dumps({"m": manifests, "e": entries})


def test_registry_configs_declared_only_under_secrets_are_pinned_to_the_fixed_mount() -> None:
    declared = _declared(extra_secrets=(_PUSH, _CACHE))
    prepared = prepare_connectors(declared, _BUILD_INSTALL)
    spec = prepared.connectors["e2e"]
    assert spec.secret_files[_PUSH] == "/secrets/registry/config.json"
    assert spec.secret_files[_CACHE] == "/secrets/registry-cache/config.json"
    names = [item if isinstance(item, str) else item.name for item in spec.secrets]
    assert _PUSH not in names
    assert _CACHE not in names
    assert "E2E_CLUSTER_KUBECONFIG" not in names


def test_undeclared_registry_configs_add_no_mount_or_volume() -> None:
    """Liveness: an installation with the registry unset renders as before."""

    for install in (_INSTALL, _BUILD_INSTALL):
        prepared = prepare_connectors(_declared(), install)
        files = prepared.connectors["e2e"].secret_files
        assert _PUSH not in files
        assert _CACHE not in files
        manifests, _entries = _render(install)
        rendered = json.dumps(manifests)
        assert _PUSH not in rendered
        assert _CACHE not in rendered
        assert "/secrets/registry" not in rendered


def test_the_unset_registry_install_still_renders_with_empty_build_env() -> None:
    env = _INSTALL.platform_env()
    assert env["E2E_REGISTRY"] == ""
    assert env["E2E_BUILD_TIMEOUT_SECONDS"] == "1200"
    assert env["E2E_SOURCE_HOSTS"] == "github.com"
    manifests, _entries = _render(_INSTALL)
    assert any(item["kind"] == "Deployment" for item in manifests)


@pytest.mark.parametrize("seconds", [30, 59, 3601])
def test_a_build_timeout_outside_sixty_to_thirty_six_hundred_is_refused(seconds: int) -> None:
    with pytest.raises(ValueError, match="e2e_connector_misconfigured"):
        prepare_connectors(_declared(), replace(_INSTALL, build_timeout_seconds=seconds))


@pytest.mark.parametrize("seconds", [60, 1200, 3600])
def test_a_build_timeout_inside_the_bounds_is_accepted(seconds: int) -> None:
    prepare_connectors(_declared(), replace(_INSTALL, build_timeout_seconds=seconds))
