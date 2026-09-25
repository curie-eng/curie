"""Example stdio MCP servers are installed by each bundle's runner layer.

Bundles that declare `mcp-server-github` or `slack-mcp` carry a
`runner.Dockerfile` whose base is only the `CURIE_RUNNER_IMAGE` build
argument. The platform runner image blesses neither server.
"""

from __future__ import annotations

import json
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[1]

_INSTALL_BY_COMMAND = {
    "mcp-server-github": (
        "RUN npm install -g @modelcontextprotocol/server-github@2025.4.8"
    ),
    "slack-mcp": "RUN npm install -g @zencoderai/slack-mcp-server@0.0.1",
}

_PLATFORM_ABSENT = (
    "@modelcontextprotocol/server-github",
    "@zencoderai/slack-mcp-server",
    "bless another authed third-party MCP server",
    "mcp-server-github",
    "slack-mcp",
)


def _instruction_lines(text: str) -> list[str]:
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _declared_commands() -> dict[str, set[str]]:
    declared: dict[str, set[str]] = {}
    for child in sorted(EXAMPLES.iterdir()):
        mcp_path = child / ".mcp.json"
        if not child.is_dir() or not mcp_path.is_file():
            continue
        servers = json.loads(mcp_path.read_text()).get("mcpServers", {})
        commands = {
            server.get("command")
            for server in servers.values()
            if isinstance(server, dict) and server.get("command") in _INSTALL_BY_COMMAND
        }
        if commands:
            declared[child.name] = commands
    return declared


def _runner_layer_errors(text: str, commands: set[str]) -> list[str]:
    lines = _instruction_lines(text)
    errors: list[str] = []
    arg_lines = [line for line in lines if line.startswith("ARG ")]
    from_lines = [line for line in lines if line.startswith("FROM ")]
    if arg_lines != ["ARG CURIE_RUNNER_IMAGE"]:
        errors.append("ARG must be exactly ARG CURIE_RUNNER_IMAGE with no default")
    if from_lines != ["FROM ${CURIE_RUNNER_IMAGE}"]:
        errors.append("FROM must be exactly FROM ${CURIE_RUNNER_IMAGE}")
    npm_lines = [line for line in lines if line.startswith("RUN npm")]
    expected = {_INSTALL_BY_COMMAND[command] for command in commands}
    if set(npm_lines) != expected or len(npm_lines) != len(expected):
        errors.append(f"RUN npm lines {npm_lines!r} != {sorted(expected)!r}")
    npm_at = [index for index, line in enumerate(lines) if line.startswith("RUN npm")]
    root_at = [index for index, line in enumerate(lines) if line == "USER root"]
    drop_at = [index for index, line in enumerate(lines) if line == "USER 1000:1000"]
    if not npm_at or not root_at or not drop_at or not (
        min(root_at) < min(npm_at) and max(npm_at) < max(drop_at)
    ):
        errors.append(
            "USER root must precede the npm installs and USER 1000:1000 must follow"
        )
    return errors


def test_bundle_mcp_servers_are_installed_by_bundle_runner_layers() -> None:
    declared = _declared_commands()
    assert set(declared) == {"github-issues", "dark-factory", "mean-tester"}
    assert declared["github-issues"] == {"mcp-server-github"}
    assert declared["dark-factory"] == {"mcp-server-github"}
    assert declared["mean-tester"] == {"slack-mcp", "mcp-server-github"}
    for name, commands in declared.items():
        text = (EXAMPLES / name / "runner.Dockerfile").read_text()
        assert _runner_layer_errors(text, commands) == []
    platform = (EXAMPLES.parent / "runner" / "Dockerfile").read_text()
    for needle in _PLATFORM_ABSENT:
        assert needle not in platform
    github_issues = (EXAMPLES / "github-issues" / "runner.Dockerfile").read_text()
    defaulted_arg = github_issues.replace(
        "ARG CURIE_RUNNER_IMAGE",
        "ARG CURIE_RUNNER_IMAGE=curie-runner:dev",
        1,
    )
    assert _runner_layer_errors(defaulted_arg, {"mcp-server-github"})
    tagged_from = github_issues.replace(
        "FROM ${CURIE_RUNNER_IMAGE}",
        "FROM curie-runner:dev",
        1,
    )
    assert _runner_layer_errors(tagged_from, {"mcp-server-github"})
