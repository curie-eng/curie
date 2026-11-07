"""Hosted endpoint selection preserves proxy authority across deployment backends."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_internal.connector_endpoints import ConnectorEndpointBinding
from curie_runner import RunnerConfig
from curie_runner.connectors import (
    derive_mcp_servers,
    materialize_connector_caller_headers,
    upload_endpoints,
)
from curie_runner.route_callers import connector_urls, route_callers

_SCOPE = {"release": None, "agent": "acme-dev", "namespace": None}


def _binding(**endpoint: str) -> ConnectorEndpointBinding:
    return ConnectorEndpointBinding.model_validate(
        {"version": 1, "agent": "acme-dev", "connectors": {"reviewer": endpoint}}
    )


def _bundle(root: Path) -> Path:
    (root / "connectors.yaml").write_text(
        "connectors:\n  reviewer:\n    image: example/reviewer:1\n"
        "    unhosted_url: https://untrusted.example.com/mcp\n"
        "    routes:\n      - path: /v1/review\n        callers: [review-tools]\n"
    )
    return root


def test_static_hosted_entries_keep_caller_and_conditional_grant(tmp_path: Path) -> None:
    binding = _binding(base_url="https://connectors.example.com/acme-dev/reviewer")
    root = _bundle(tmp_path)
    for grant in ("", "ccg.test"):
        source = {"CURIE_CONNECTOR_CALLER_TOKEN": "cct.test"}
        if grant:
            source["CURIE_CONNECTOR_TOOL_GRANT"] = grant
        entries = derive_mcp_servers(
            root, **_SCOPE, endpoints=binding, caller_header=True, env=source
        )
        assert entries["reviewer"]["url"] == (
            "https://connectors.example.com/acme-dev/reviewer/mcp"
        )
        materialize_connector_caller_headers(entries, source)
        assert entries["reviewer"]["headers"]["X-Curie-Caller"] == "cct.test"
        assert "CURIE_CONNECTOR_CALLER_TOKEN" not in source
        assert entries["reviewer"]["headers"].get("X-Curie-Connector-Grant") == (
            "${CURIE_CONNECTOR_TOOL_GRANT}" if grant else None
        )


def test_static_missing_endpoint_never_uses_unhosted_url(tmp_path: Path) -> None:
    empty = ConnectorEndpointBinding(version=1, agent="acme-dev", connectors={})
    with pytest.raises(ValueError, match="missing hosted connector endpoint"):
        derive_mcp_servers(_bundle(tmp_path), **_SCOPE, endpoints=empty)


def test_routes_share_static_map_and_deployment_restrictions(tmp_path: Path) -> None:
    binding = _binding(base_url="https://connectors.example.com/acme-dev/reviewer")
    root = _bundle(tmp_path)
    assert connector_urls(root, **_SCOPE, endpoints=binding) == {
        "reviewer": "https://connectors.example.com/acme-dev/reviewer"
    }
    assert route_callers(root, agent="acme-dev") == {"review-tools": frozenset({"reviewer"})}
    (root / "deploy.yaml").write_text(
        "targets:\n  dev:\n    agent: acme-dev\n    env: dev\n"
        "    slack_channel: C0EXAMPLE1\n    connectors: []\n"
    )
    assert derive_mcp_servers(root, **_SCOPE, endpoints=binding) == {}
    assert connector_urls(root, **_SCOPE, endpoints=binding) == {}
    assert route_callers(root, agent="acme-dev") == {}


def test_upload_capability_contains_only_declared_upload_urls(tmp_path: Path) -> None:
    binding = _binding(
        base_url="https://connectors.example.com/reviewer",
        upload_url="https://uploads.example.com/reviewer/v1/upload",
    )
    root = _bundle(tmp_path)
    assert upload_endpoints(root, agent="acme-dev", endpoints=binding) == {
        "reviewer": "https://uploads.example.com/reviewer/v1/upload"
    }
    assert upload_endpoints(root, agent="acme-dev", endpoints=None) == {}
    entries = derive_mcp_servers(root, **_SCOPE, endpoints=binding, caller_header=True)
    assert "uploads.example.com" not in json.dumps(entries)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "https://user:pass@example.com",
        "https://example.com/?token=x",
        "https://example.com/#x",
        "https://example.com/\n",
    ],
)
def test_invalid_static_urls_fail_at_boot(url: str) -> None:
    with pytest.raises(ValueError):
        _binding(base_url=url)


def test_static_boot_needs_no_kubernetes_scope() -> None:
    binding = _binding(base_url="https://connectors.example.com/reviewer")
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": "/bundle",
            "CURIE_SESSION_ID": "test",
            "CURIE_SANDBOX_ID": "test",
            "CURIE_BUDGET": '{"max_output_tokens_per_run":100}',
            "CURIE_HOSTED_CONNECTOR_ENDPOINTS": binding.model_dump_json(),
            "CURIE_CONNECTOR_CALLER_TOKEN": "cct.test",
        }
    )
    assert config.connector_agent == "acme-dev"
    assert config.connector_release is None and config.connector_namespace is None
    assert config.connector_endpoints == binding
    assert config.connector_caller_token == "cct.test"
