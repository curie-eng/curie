"""Declared-connector capability diagnosis (#2519).

A SecretRef-form credential never reaches the sandbox under ADR-0090, so
``Authorization: Bearer ${NAME}`` expands empty and the MCP capability probe
only logged. This module pins the network-free diagnosis that names the
connector and the credential, never a value, and the vector #2352 must reuse
instead of probing HTTP at deploy time.
"""

from __future__ import annotations

import json
from pathlib import Path

import anyio
import pytest
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability

_VECTOR = (
    Path(__file__).resolve().parents[2] / "tests" / "vectors" / "connector-probe-diagnosis.json"
)
_VECTOR_CASE_KEYS = {
    "name",
    "connector",
    "headers",
    "env",
    "expected_reason",
    "expected_credentials",
    "expected_message",
}
_PLANTED = "must-not-appear-PLACEHOLDER"


def _load_vector() -> dict:
    payload = json.loads(_VECTOR.read_text(encoding="utf-8"))
    extra = set(payload) - {"comment", "vectors"}
    assert not extra, extra
    assert payload["comment"]
    return payload


def test_vector_rejects_unknown_fields() -> None:
    payload = _load_vector()
    for case in payload["vectors"]:
        extra = set(case) - _VECTOR_CASE_KEYS
        assert not extra, extra


def test_vector_cases_match_diagnose_derived_connector_headers() -> None:
    from curie_runner.mcp_tool_capability import diagnose_derived_connector_headers

    payload = _load_vector()
    assert payload["vectors"], "the diagnosis vector must not be empty"
    for case in payload["vectors"]:
        derived = {
            case["connector"]: {
                "type": "http",
                "url": "http://127.0.0.1:9/mcp",
                "headers": case["headers"],
            }
        }
        failures = diagnose_derived_connector_headers(derived, case["env"])
        if case["expected_reason"] is None:
            assert failures == ()
            continue
        assert len(failures) == 1, case["name"]
        failure = failures[0]
        assert failure.connector == case["connector"]
        assert failure.reason == case["expected_reason"]
        assert list(failure.credential_names) == case["expected_credentials"]
        assert failure.caller_message() == case["expected_message"]
        assert _PLANTED not in failure.caller_message()
        for value in case["env"].values():
            if value.strip():
                assert value not in failure.caller_message()


def test_empty_expansion_skips_http_probe_for_that_derived_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    async def boom(*_args: object, **_kwargs: object) -> tuple[int, bool, frozenset[str]]:
        called.append("probed")
        raise AssertionError("empty expansion must not dial the connector")

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", boom)
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"},
        }
    }
    result = anyio.run(probe_mcp_tool_capability, None, derived, {"GITHUB_TOKEN": ""})
    # getattr so reversing the product files yields AssertionError, not an
    # import/attribute ERROR the fix-pin gate refuses to attribute.
    failures = getattr(result, "connector_failures", ())
    assert called == []
    assert result.has_potential_write_tool
    assert failures
    assert failures[0].connector == "github"
    assert failures[0].reason == "empty_expansion"
    assert failures[0].credential_names == ("GITHUB_TOKEN",)


def test_missing_credential_skips_http_probe_for_that_derived_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    async def boom(*_args: object, **_kwargs: object) -> tuple[int, bool, frozenset[str]]:
        called.append("probed")
        raise AssertionError("missing credential must not dial the connector")

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", boom)
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${CURIE_TEST_CONNECTOR_TOKEN}"},
        }
    }
    result = anyio.run(probe_mcp_tool_capability, None, derived, {})
    assert called == []
    assert result.connector_failures[0].reason == "missing_credential"
    assert result.connector_failures[0].credential_names == ("CURIE_TEST_CONNECTOR_TOKEN",)


def test_healthy_nonempty_header_still_probes_the_derived_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    async def ok(*_args: object, **_kwargs: object) -> tuple[int, bool, frozenset[str]]:
        called.append("probed")
        return 1, True, frozenset()

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", ok)
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"},
        }
    }
    result = anyio.run(
        probe_mcp_tool_capability,
        None,
        derived,
        {"GITHUB_TOKEN": "ghp-not-a-real-token-PLACEHOLDER"},
    )
    assert called == ["probed"]
    assert result.connector_failures == ()
    assert result.complete
    assert result.has_potential_write_tool


def test_probe_exception_on_nonempty_expansion_is_probe_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(*_args: object, **_kwargs: object) -> tuple[int, bool, frozenset[str]]:
        raise ConnectionError(
            "dial http://127.0.0.1:9/mcp with Bearer ghp-not-a-real-token-PLACEHOLDER"
        )

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", fail)
    planted = "ghp-not-a-real-token-PLACEHOLDER"
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"},
        }
    }
    result = anyio.run(probe_mcp_tool_capability, None, derived, {"GITHUB_TOKEN": planted})
    assert result.connector_failures[0].reason == "probe_failed"
    assert result.has_potential_write_tool
    message = result.connector_failures[0].caller_message()
    assert planted not in message
    assert "github" in message
    assert "GITHUB_TOKEN" in message


def test_bundle_config_abort_still_reports_derived_empty_expansion(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot", "mcpServers": "config/servers.json"}),
        encoding="utf-8",
    )
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"},
        }
    }
    result = anyio.run(probe_mcp_tool_capability, root, derived, {"GITHUB_TOKEN": ""})
    assert result.failures == ("bundle-config",)
    assert result.has_potential_write_tool
    assert result.connector_failures[0].reason == "empty_expansion"
    assert result.connector_failures[0].connector == "github"


def test_bundle_mcp_json_probe_failure_is_not_a_connector_failure(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    (root / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"operations": {"command": str(root / "missing-server")}}}),
        encoding="utf-8",
    )

    result = anyio.run(probe_mcp_tool_capability, root, {}, {})
    assert result.failures == ("operations",)
    assert result.connector_failures == ()
    assert result.has_potential_write_tool


def test_caller_message_never_contains_a_planted_secret() -> None:
    from curie_runner.mcp_tool_capability import ConnectorCapabilityFailure

    failure = ConnectorCapabilityFailure(
        connector="github",
        credential_names=("GITHUB_TOKEN",),
        reason="empty_expansion",
    )
    message = failure.caller_message()
    assert "GITHUB_TOKEN" in message
    assert "github" in message
    assert "ghp-" not in message
    assert "${" not in message
