"""A hosted connector whose only secret is a SecretRef is not a failed capability (#2825).

A connector with one ``from_secret`` credential lets its pod read the upstream
token through a ``secretKeyRef``. Under ADR 0090, that value never reaches the
sandbox. A derived client ``Authorization: Bearer ${NAME}`` header could not
expand, so boot reported ``missing_credential`` on every session and the
exclusion hook denied each connector tool while the turn still finished.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import anyio
import pytest
from curie_runner.connectors import derive_mcp_servers
from curie_runner.hooks import build_gated_pre_tool_use_hooks
from curie_runner.mcp_tool_capability import (
    ConnectorAvailability,
    probe_mcp_tool_capability,
)

SCOPE = {"release": "acme", "agent": "acme-bot", "namespace": "acme-dev"}

SECRETREF_ONLY = (
    "connectors:\n"
    "  grafana:\n"
    "    image: docker.io/grafana/mcp-grafana:latest\n"
    "    secrets:\n"
    "      - name: GRAFANA_SERVICE_ACCOUNT_TOKEN\n"
    "        from_secret: grafana-connector\n"
    "        key: GRAFANA_SERVICE_ACCOUNT_TOKEN\n"
)

NAMED_SECRET = (
    "connectors:\n"
    "  github:\n"
    "    image: ghcr.io/github/github-mcp-server:v0.20.1\n"
    "    secrets: [GITHUB_PERSONAL_ACCESS_TOKEN]\n"
)

EXPLICIT_SECRETREF_BEARER = (
    "connectors:\n"
    "  github:\n"
    "    image: ghcr.io/github/github-mcp-server:v0.20.1\n"
    "    bearer_secret: GITHUB_PERSONAL_ACCESS_TOKEN\n"
    "    secrets:\n"
    "      - name: GITHUB_PERSONAL_ACCESS_TOKEN\n"
    "        from_secret: team-github\n"
    "        key: token\n"
)


def _bundle(root: Path, connectors: str) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "b", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors, encoding="utf-8")
    return root


def _answering_probe(
    dialed: list[dict[str, Any]],
):
    async def probe(
        config: Any,
        **_kwargs: object,
    ) -> tuple[int, bool, frozenset[str], frozenset[str]]:
        dialed.append(dict(config))
        tool = "mcp__grafana__search_dashboards"
        return 1, False, frozenset({tool}), frozenset({tool})

    return probe


def _hook_decision(availability: ConnectorAvailability, tool: str) -> dict[str, Any]:
    hooks = build_gated_pre_tool_use_hooks(None, availability)
    if hooks is None:
        return {}
    callback = hooks["PreToolUse"][0].hooks[0]
    return anyio.run(
        callback,
        {"hook_event_name": "PreToolUse", "tool_name": tool, "tool_input": {}},
        "toolu_1",
        {"signal": None},
    )


def test_secretref_only_connector_is_not_a_failed_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    dialed: list[dict[str, Any]] = []
    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", _answering_probe(dialed))
    monkeypatch.delenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", raising=False)

    derived = derive_mcp_servers(_bundle(tmp_path, SECRETREF_ONLY), **SCOPE)
    # The sandbox never holds a SecretRef value, so no client header can be
    # derived from it. The hosted server authenticates upstream itself.
    assert "Authorization" not in derived["grafana"].get("headers", {})

    with caplog.at_level(logging.WARNING):
        boot = anyio.run(probe_mcp_tool_capability, None, derived, {})

    assert boot.connector_failures == ()
    assert [config["url"] for config in dialed] == [derived["grafana"]["url"]]
    decision = _hook_decision(
        ConnectorAvailability(boot.connector_failures), "mcp__grafana__search_dashboards"
    )
    assert decision.get("hookSpecificOutput", {}).get("permissionDecision") != "deny"


def test_named_secret_still_derives_the_bearer_and_reports_it_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A Curie resolved secret is delivered to the sandbox, so the derived header
    # stays and a missing value is still a real failure.
    dialed: list[dict[str, Any]] = []
    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", _answering_probe(dialed))
    monkeypatch.delenv("GITHUB_PERSONAL_ACCESS_TOKEN", raising=False)

    derived = derive_mcp_servers(_bundle(tmp_path, NAMED_SECRET), **SCOPE)
    assert derived["github"]["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }
    boot = anyio.run(probe_mcp_tool_capability, None, derived, {})
    assert [(f.connector, f.reason) for f in boot.connector_failures] == [
        ("github", "missing_credential")
    ]
    assert dialed == []
    decision = _hook_decision(ConnectorAvailability(boot.connector_failures), "mcp__github__get_me")
    assert decision["hookSpecificOutput"]["permissionDecision"] == "deny"


def test_explicit_bearer_secret_naming_a_secretref_keeps_the_header(tmp_path: Path) -> None:
    # An author who explicitly asks for a client Bearer keeps it, so a
    # credential that cannot reach the sandbox is still surfaced.
    derived = derive_mcp_servers(_bundle(tmp_path, EXPLICIT_SECRETREF_BEARER), **SCOPE)
    assert derived["github"]["headers"] == {
        "Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"
    }


def test_secretref_server_that_refuses_the_client_is_still_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A hosted server that does authenticate the client fails on the real dial
    # and remains visible as a failed connector.
    async def refuses(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[int, bool, frozenset[str], frozenset[str]]:
        raise RuntimeError("401 Unauthorized")

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", refuses)
    derived = derive_mcp_servers(_bundle(tmp_path, SECRETREF_ONLY), **SCOPE)
    boot = anyio.run(probe_mcp_tool_capability, None, derived, {})
    assert [(f.connector, f.reason) for f in boot.connector_failures] == [
        ("grafana", "probe_failed")
    ]


REMOTE_SECRETREF_AUTHORED_BEARER = (
    "connectors:\n"
    "  vendor:\n"
    "    url: https://mcp.example.com/mcp\n"
    "    headers:\n"
    "      Authorization: Bearer ${VENDOR_TOKEN}\n"
    "    secrets:\n"
    "      - name: VENDOR_TOKEN\n"
    "        from_secret: vendor-token\n"
)


@pytest.mark.parametrize("scoped", [True, False])
def test_remote_connector_keeps_its_authored_bearer(tmp_path: Path, scoped: bool) -> None:
    # Only the hosted derived header is an upstream credential. A remote
    # connector's authored header authenticates the client and must survive.
    scope = SCOPE if scoped else {"release": None, "agent": None, "namespace": None}
    derived = derive_mcp_servers(_bundle(tmp_path, REMOTE_SECRETREF_AUTHORED_BEARER), **scope)
    assert derived["vendor"]["headers"] == {"Authorization": "Bearer ${VENDOR_TOKEN}"}
