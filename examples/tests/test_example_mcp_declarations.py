"""Every example bundle declares its MCP servers in a form the real runner loads.

Two landmines the ``curie guide`` documents get enforced here across *all*
example bundles (discovered dynamically, so a future third example is covered
without editing this file):

1. A plugin manifest's ``mcpServers`` must be an inline **object**, never a
   string path. The string-pointer form (``"mcpServers": ".mcp.json"``) is
   silently ignored by Claude Code, disabling MCP loading entirely.
2. An in-bundle stdio server's script reference must be cwd-independent
   (``${CLAUDE_PLUGIN_ROOT}``-qualified or absolute). A bare relative path like
   ``scripts/engine_server.py`` only spawns when cwd happens to be the bundle
   root. This is checked on both ``command`` and every ``args`` entry, for any
   interpreter (python, node, shell, ...), so a future non-python example is
   covered too.
"""

import json
import os
from pathlib import Path

import yaml

EXAMPLES = Path(__file__).resolve().parents[1]

# A string that ends in one of these is a reference to a script shipped inside
# the bundle -- it has to resolve against the bundle root, so it must be
# cwd-independent. A bare interpreter name (``python3``, ``node``) has no such
# extension and is resolved from PATH, so it is correctly left alone.
_SCRIPT_SUFFIXES = (".py", ".js", ".mjs", ".cjs", ".ts", ".sh", ".rb")


def _discover_bundles() -> list[Path]:
    """Immediate ``examples/`` subdirs that carry a plugin manifest."""
    return sorted(
        child
        for child in EXAMPLES.iterdir()
        if child.is_dir()
        and child.name != "tests"
        and (child / ".claude-plugin" / "plugin.json").is_file()
    )


def _is_bundle_script_ref(value: object) -> bool:
    return isinstance(value, str) and value.endswith(_SCRIPT_SUFFIXES)


def test_no_example_manifest_uses_string_pointer_mcpservers() -> None:
    violations: list[str] = []
    for bundle in _discover_bundles():
        manifest = json.loads((bundle / ".claude-plugin" / "plugin.json").read_text())
        if "mcpServers" not in manifest:
            continue
        value = manifest["mcpServers"]
        if not isinstance(value, dict):
            violations.append(
                f"{bundle.name}: mcpServers must be an inline object, got "
                f"{type(value).__name__} {value!r} (string-pointer form silently "
                f"disables MCP loading)"
            )
    assert not violations, "String-pointer mcpServers declarations found:\n" + "\n".join(
        violations
    )


def test_example_mcp_server_script_args_are_cwd_independent() -> None:
    violations: list[str] = []
    for bundle in _discover_bundles():
        mcp_path = bundle / ".mcp.json"
        if not mcp_path.is_file():
            continue
        servers = json.loads(mcp_path.read_text()).get("mcpServers", {})
        for name, spec in servers.items():
            candidates = [spec.get("command", ""), *spec.get("args", [])]
            for value in candidates:
                if not _is_bundle_script_ref(value):
                    continue
                if value.startswith("${CLAUDE_PLUGIN_ROOT}") or os.path.isabs(value):
                    continue
                violations.append(
                    f"{bundle.name}/{name}: script reference {value!r} is a bare "
                    f"relative path; use a ${{CLAUDE_PLUGIN_ROOT}}-qualified or "
                    f"absolute path so the server spawns regardless of cwd"
                )
    assert not violations, "cwd-dependent MCP server script args found:\n" + "\n".join(
        violations
    )


def test_sre_bot_observability_connectors_ship_self_configured() -> None:
    path = EXAMPLES / "sre-bot" / "connectors.yaml"
    raw = path.read_text()
    connectors = yaml.safe_load(raw)["connectors"]

    kubernetes = connectors["kubernetes"]
    grafana = connectors["grafana"]
    tempo = connectors["tempo"]
    expected_url = "http://grafana.observability.svc.cluster.local"
    expected_token_ref = {
        "name": "GRAFANA_SERVICE_ACCOUNT_TOKEN",
        "from_secret": "curie-grafana-connector",
        "key": "GRAFANA_SERVICE_ACCOUNT_TOKEN",
    }

    assert (
        kubernetes["image"]
        == "ghcr.io/containers/kubernetes-mcp-server@sha256:"
        "6d650f4bd6ac303ad82713c997e73a2d001602f9bf17392c9b9a0e30e29c6423"
    )
    assert (
        grafana["image"]
        == "docker.io/grafana/mcp-grafana@sha256:"
        "5efeafd01cd7e1aea9c4b0f03305951f2944db8f43e5ae290cce9578c977f241"
    )
    assert tempo["build"] == {
        "context": "connectors/tempo",
        "platforms": ["linux/amd64", "linux/arm64"],
    }
    assert "image" not in tempo
    assert grafana["env"]["GRAFANA_URL"] == expected_url
    assert tempo["env"] == {"GRAFANA_URL": expected_url}
    assert grafana["secrets"] == [expected_token_ref]
    assert tempo["secrets"] == [expected_token_ref]
    assert "build" not in kubernetes
    assert "build" not in grafana

    assert "__Tempo__" not in raw
    assert "https://grafana.example.com" not in raw
    assert "THE ONE LINE TO EDIT PER INSTALL" not in raw
    assert "curie secrets set GRAFANA_SERVICE_ACCOUNT_TOKEN" not in raw
    assert "GRAFANA -- SHIPS OFF" not in raw
    assert "TEMPO (DISTRIBUTED TRACES) -- SHIPS OFF" not in raw


def test_sre_bot_python_connectors_pin_the_mcp_2_runtime_their_servers_import() -> None:
    connector_root = EXAMPLES / "sre-bot" / "connectors"
    # Discovered, not hardcoded: the shipped connector set changes as the bot
    # adopts upstream servers, and a stale name list would either fail on a
    # retired connector or silently skip a new one. The count assertion is the
    # anti-vacuity control, so an empty directory cannot report PASS.
    connectors = sorted(
        path.parent
        for path in connector_root.glob("*/requirements.txt")
        if (path.parent / "server.py").exists()
    )
    assert len(connectors) >= 2, (
        f"expected the sre-bot to ship Python connectors; found {connectors}"
    )

    for connector in connectors:
        requirements = (connector / "requirements.txt").read_text().splitlines()
        assert "mcp==2.1.1" in requirements, (
            f"{connector.name} imports mcp.server.mcpserver but its image "
            "does not pin MCP 2.1.1"
        )
