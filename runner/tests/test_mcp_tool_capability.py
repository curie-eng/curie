"""Runtime evidence for deciding whether a session can perform a write action."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import anyio
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability
from plugin_format.approval_policy import connector_tool_prefix

_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_capability_server.py"


def _bundle(tmp_path: Path, *, mode: str) -> Path:
    root = tmp_path / mode
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "operations": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": mode},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return root


def _probe(root: Path):
    return anyio.run(probe_mcp_tool_capability, root, {})


def test_all_tools_explicitly_read_only_proves_no_write_capability(tmp_path: Path) -> None:
    # Provider contract: MCP ToolAnnotations.readOnlyHint=true means the tool
    # does not modify its environment; absence defaults false. The same section
    # says annotations are untrusted hints, so this result only removes Curie's
    # model-invoked generic pager -- it never authorizes a tool or bypasses an
    # explicit gate.
    # https://modelcontextprotocol.io/specification/2025-11-25/schema#toolannotations
    result = _probe(_bundle(tmp_path, mode="read-only"))

    assert result.complete
    assert not result.has_potential_write_tool
    assert result.tool_count == 1
    assert result.observed_tools == frozenset(
        {"mcp__plugin_acme-bot_operations__inspect_or_change"}
    )
    assert result.readonly_tools == frozenset(
        {"mcp__plugin_acme-bot_operations__inspect_or_change"}
    )


def test_explicit_write_tool_keeps_write_capability(tmp_path: Path) -> None:
    result = _probe(_bundle(tmp_path, mode="write"))

    assert result.complete
    assert result.has_potential_write_tool
    assert result.observed_tools == frozenset(
        {"mcp__plugin_acme-bot_operations__inspect_or_change"}
    )
    assert result.readonly_tools == frozenset()


def test_missing_read_only_hint_is_conservatively_write_capable(tmp_path: Path) -> None:
    result = _probe(_bundle(tmp_path, mode="unknown"))

    assert result.complete
    assert result.has_potential_write_tool
    assert result.observed_tools == frozenset(
        {"mcp__plugin_acme-bot_operations__inspect_or_change"}
    )
    assert result.readonly_tools == frozenset()


def test_mixed_read_only_and_write_tools_keep_write_capability(tmp_path: Path) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "inventory": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": "read-only"},
                    },
                    "operations": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": "write"},
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    result = _probe(root)

    assert result.complete
    assert result.has_potential_write_tool
    assert result.tool_count == 2
    assert result.observed_tools == frozenset(
        {
            "mcp__plugin_acme-bot_inventory__inspect_or_change",
            "mcp__plugin_acme-bot_operations__inspect_or_change",
        }
    )
    # Even on a mixed surface that keeps the pager, the observed read-only tool
    # remains available to the receipt classifier by its SDK-visible name.
    assert result.readonly_tools == frozenset(
        {"mcp__plugin_acme-bot_inventory__inspect_or_change"}
    )


def test_successful_observed_names_survive_a_sibling_probe_failure(
    tmp_path: Path,
) -> None:
    root = _bundle(tmp_path, mode="policy-catalog")
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "inventory": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": "policy-catalog"},
                    },
                    "operations": {"command": str(root / "missing-server")},
                }
            }
        ),
        encoding="utf-8",
    )
    connector_name = "audit"
    derived = {
        connector_name: {
            "command": sys.executable,
            "args": [str(_SERVER)],
            "env": {"CURIE_TEST_TOOL_MODE": "policy-catalog"},
        }
    }

    result = anyio.run(probe_mcp_tool_capability, root, derived)

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.tool_count == 8
    assert result.failures == ("operations",)
    plugin_prefix = "mcp__plugin_acme-bot_inventory__"
    connector_prefix = connector_tool_prefix(connector_name)
    names = {"read_allowed", "write_approval", "write_denied", "write_unmatched"}
    assert result.observed_tools == frozenset(
        {f"{plugin_prefix}{name}" for name in names}
        | {f"{connector_prefix}{name}" for name in names}
    )
    assert (
        "mcp__plugin_acme-bot_operations__write_unmatched"
        not in result.observed_tools
    )
    assert result.readonly_tools == frozenset(
        {f"{plugin_prefix}read_allowed", f"{connector_prefix}read_allowed"}
    )


def test_successful_read_only_names_survive_a_sibling_probe_failure(
    tmp_path: Path,
) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "inventory": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": "read-only"},
                    },
                    "operations": {"command": str(root / "missing-server")},
                }
            }
        ),
        encoding="utf-8",
    )
    result = _probe(root)

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.tool_count == 1
    assert result.failures == ("operations",)
    assert result.readonly_tools == frozenset(
        {"mcp__plugin_acme-bot_inventory__inspect_or_change"}
    )


def test_derived_connector_read_only_tool_uses_connector_runtime_prefix(
    tmp_path: Path,
) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").unlink()
    connector_name = "inventory"
    derived = {
        connector_name: {
            "command": sys.executable,
            "args": [str(_SERVER)],
            "env": {"CURIE_TEST_TOOL_MODE": "read-only"},
        }
    }

    result = anyio.run(probe_mcp_tool_capability, root, derived)

    assert result.complete
    assert not result.has_potential_write_tool
    assert result.tool_count == 1
    assert result.observed_tools == frozenset(
        {f"{connector_tool_prefix(connector_name)}inspect_or_change"}
    )
    assert result.readonly_tools == frozenset(
        {f"{connector_tool_prefix(connector_name)}inspect_or_change"}
    )


def test_paginated_read_only_tools_prove_no_write_capability(tmp_path: Path) -> None:
    result = _probe(_bundle(tmp_path, mode="paginated"))

    assert result.complete
    assert not result.has_potential_write_tool
    assert result.tool_count == 2


def test_manifest_mcp_path_string_is_an_unknown_surface(tmp_path: Path) -> None:
    root = _bundle(tmp_path, mode="read-only")
    manifest = root / ".claude-plugin" / "plugin.json"
    manifest.write_text(
        json.dumps({"name": "acme-bot", "mcpServers": "config/servers.json"}),
        encoding="utf-8",
    )

    result = _probe(root)

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.failures == ("bundle-config",)
    assert result.observed_tools == frozenset()
    assert result.readonly_tools == frozenset()


def test_malformed_server_entry_is_an_unknown_surface(tmp_path: Path) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"operations": "config/operations.json"}}),
        encoding="utf-8",
    )

    result = _probe(root)

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.failures == ("bundle-config",)
    assert result.observed_tools == frozenset()
    assert result.readonly_tools == frozenset()


def test_unreachable_server_cannot_be_mistaken_for_read_only(tmp_path: Path) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "operations": {
                        "command": str(root / "missing-server"),
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = _probe(root)

    assert not result.complete
    assert result.has_potential_write_tool
    assert result.failures == ("operations",)
    assert result.observed_tools == frozenset()
    assert result.readonly_tools == frozenset()


def test_no_mcp_servers_is_a_complete_empty_surface(tmp_path: Path) -> None:
    root = _bundle(tmp_path, mode="read-only")
    (root / ".mcp.json").unlink()

    result = _probe(root)

    assert result.complete
    assert not result.has_potential_write_tool
    assert result.tool_count == 0
    assert result.observed_tools == frozenset()
    assert result.readonly_tools == frozenset()
