"""The API renders connector manifests but never applies them (ADR-0086, #1063).

The split is the security property worth pinning: rendering is a pure function,
so the API needs no cluster access to do it, and its deliberately read-only RBAC
(`pods: list`, `pods/log: get`) is untouched. That matters because the API is
the component that receives webhooks from the internet. Applying happens in the
CLI, under the operator's own kubectl credentials.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api import bundles


def _bundle(root: Path, connectors_yaml: str | None = None) -> Path:
    inner = root / "b"
    (inner / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (inner / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (inner / "skills" / "acme-bot").mkdir(parents=True, exist_ok=True)
    (inner / "skills" / "acme-bot" / "SKILL.md").write_text(
        "---\nname: acme-bot\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    if connectors_yaml is not None:
        (inner / "connectors.yaml").write_text(connectors_yaml, encoding="utf-8")
    return root


HOSTED = (
    "connectors:\n"
    "  grafana:\n"
    "    image: grafana/mcp-grafana:0.17.2\n"
    "    args: [-t, streamable-http]\n"
    "    env: {GRAFANA_URL: 'https://g.example.com'}\n"
    "    secrets: [GRAFANA_TOKEN]\n"
)


def _render(root: Path, agent: str = "acme-bot") -> list[dict]:
    return bundles.render_connector_manifests(
        bundles.read_connectors(root),
        release="acme-bot",
        agent=agent,
        namespace="acme-bot",
        app_name="acme-bot",
        secret_name=f"acme-bot-{agent}-connectors",
    )


def test_bundle_without_connectors_renders_nothing(tmp_path: Path) -> None:
    assert _render(_bundle(tmp_path)) == []


def test_hosted_connector_renders_the_full_object_set(tmp_path: Path) -> None:
    kinds = [o["kind"] for o in _render(_bundle(tmp_path, HOSTED))]
    assert kinds == ["Service", "Deployment", "NetworkPolicy"]


def test_rendering_needs_no_cluster_access(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The whole reason the API may do this: it is a pure function. If rendering
    # ever reached for a kube client, the API would need cluster-write RBAC --
    # on the service that receives internet webhooks.
    import curie_api.k8s as k8s_mod

    def _explode(*_a: object, **_k: object) -> None:
        raise AssertionError("rendering must not touch the cluster")

    for attr in dir(k8s_mod):
        if attr.startswith("_"):
            continue
        obj = getattr(k8s_mod, attr)
        if callable(obj) and not isinstance(obj, type):
            monkeypatch.setattr(k8s_mod, attr, _explode, raising=False)

    assert _render(_bundle(tmp_path, HOSTED))  # renders fine with k8s poisoned


def test_secret_is_referenced_not_inlined(tmp_path: Path) -> None:
    dep = next(o for o in _render(_bundle(tmp_path, HOSTED)) if o["kind"] == "Deployment")
    env = dep["spec"]["template"]["spec"]["containers"][0]["env"]
    token = next(e for e in env if e["name"] == "GRAFANA_TOKEN")
    assert token["valueFrom"]["secretKeyRef"]["name"] == "acme-bot-acme-bot-connectors"
    assert "value" not in token


def test_egress_policy_uses_a_podselector(tmp_path: Path) -> None:
    # The ClusterIP form silently never matches; see connector_render's module
    # docstring. Pinned here too because this is the path a real deploy takes.
    np = next(o for o in _render(_bundle(tmp_path, HOSTED)) if o["kind"] == "NetworkPolicy")
    assert "podSelector" in np["spec"]["egress"][0]["to"][0]


def test_mcp_entry_url_matches_the_rendered_service(tmp_path: Path) -> None:
    root = _bundle(tmp_path, HOSTED)
    svc = next(o for o in _render(root) if o["kind"] == "Service")
    entries = bundles.connector_mcp_entries(
        bundles.read_connectors(root), release="acme-bot", agent="acme-bot", namespace="acme-bot"
    )
    assert svc["metadata"]["name"] in entries["grafana"]["url"]


def test_remote_connector_contributes_an_entry_but_no_objects(tmp_path: Path) -> None:
    root = _bundle(tmp_path, "connectors:\n  x:\n    url: https://mcp.internal/mcp\n")
    assert _render(root) == []
    entries = bundles.connector_mcp_entries(
        bundles.read_connectors(root), release="acme-bot", agent="acme-bot", namespace="acme-bot"
    )
    assert entries["x"]["url"] == "https://mcp.internal/mcp"


def test_two_agents_sharing_a_release_render_distinct_objects(tmp_path: Path) -> None:
    # The deploy path's half of #1116: same bundle, same release, two agents.
    # Release-scoped naming made these identical, so deploying one silently
    # overwrote the other's Deployment and credential.
    root = _bundle(tmp_path, HOSTED)
    dev = {o["metadata"]["name"] for o in _render(root, agent="acme-dev")}
    prod = {o["metadata"]["name"] for o in _render(root, agent="acme-prod")}
    assert not dev & prod, f"agents collide: {dev} vs {prod}"


REFERENCED = (
    "connectors:\n"
    "  grafana:\n"
    "    image: grafana/mcp-grafana:0.17.2\n"
    "    secrets:\n"
    "      - name: GRAFANA_TOKEN\n"
    "        from_secret: grafana-mcp\n"
)


def test_a_referenced_secret_is_not_something_the_caller_must_resolve(tmp_path: Path) -> None:
    # The property ADR-0090 depends on: with every credential referenced, the
    # deploy path handles none, so a reconciler holding no secrets can apply
    # this connector.
    declared = bundles.read_connectors(_bundle(tmp_path, REFERENCED))
    assert bundles.owned_secret_keys(declared) == []


def test_a_literal_secret_still_must_be_resolved(tmp_path: Path) -> None:
    declared = bundles.read_connectors(_bundle(tmp_path, HOSTED))
    assert bundles.owned_secret_keys(declared) == ["GRAFANA_TOKEN"]


def test_a_referenced_secret_points_outside_curies_own_secret(tmp_path: Path) -> None:
    dep = next(o for o in _render(_bundle(tmp_path, REFERENCED)) if o["kind"] == "Deployment")
    env = dep["spec"]["template"]["spec"]["containers"][0]["env"]
    ref = next(e for e in env if e["name"] == "GRAFANA_TOKEN")["valueFrom"]["secretKeyRef"]
    assert ref["name"] == "grafana-mcp"
    assert "value" not in next(e for e in env if e["name"] == "GRAFANA_TOKEN")
