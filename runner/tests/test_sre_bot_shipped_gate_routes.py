"""The shipped SRE bot bundle routes every Kubernetes mutation (#2722).

Boots ``build_runner`` on the SHIPPED plugin.json and connectors.yaml. The real
bundle directory cannot boot as-is: ``tempo`` and ``self-upgrade`` declare
``build:`` with no committed connectors.lock.yaml (the installer pins them at
install time), so the copy swaps each ``build:`` for a placeholder ``image:``.
The manifest itself is copied byte for byte. Each mutation in
``toolPolicy.approvalRequired`` must pend on route ``sre-approvals``; a route-less
approval can never be resolved by an operator principal.
"""

from __future__ import annotations

from pathlib import Path

import shutil

import anyio
import pytest
import yaml
from curie_runner.__main__ import build_runner
from curie_runner.approval import build_approval_hook
from curie_runner.config import RunnerConfig

BUNDLE = Path(__file__).resolve().parents[2] / "examples" / "sre-bot"
_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
MUTATIONS = (
    "pods_delete",
    "pods_exec",
    "pods_run",
    "resources_create_or_update",
    "resources_delete",
    "resources_scale",
)


def _gate(tmp_path: Path):
    (tmp_path / ".claude-plugin").mkdir()
    shutil.copyfile(
        BUNDLE / ".claude-plugin" / "plugin.json",
        tmp_path / ".claude-plugin" / "plugin.json",
    )
    declaration = yaml.safe_load((BUNDLE / "connectors.yaml").read_text())
    for name, spec in declaration["connectors"].items():
        if "build" in spec:
            del spec["build"]
            spec["image"] = f"ghcr.io/example/{name}:0.0.1"
    (tmp_path / "connectors.yaml").write_text(yaml.safe_dump(declaration))
    env = {
        "CURIE_PLUGIN_DIR": str(tmp_path),
        "CURIE_SESSION_ID": "s-sre",
        "CURIE_SANDBOX_ID": "b-sre",
        "CURIE_BUDGET": _BUDGET,
    }
    runner = build_runner(RunnerConfig.from_env(env), fake_model=True)
    gate = runner._approval_gate  # noqa: SLF001 - boot wiring is the assertion
    assert gate is not None
    return gate


@pytest.mark.parametrize("tool", MUTATIONS)
def test_shipped_kubernetes_mutation_pends_on_sre_approvals(tmp_path: Path, tool: str) -> None:
    gate = _gate(tmp_path)
    hook = build_approval_hook(gate)["PreToolUse"][0].hooks[0]
    name = f"mcp__kubernetes__{tool}"

    async def go() -> None:
        result = await hook({"tool_name": name, "tool_input": {}}, None, None)
        assert result["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert gate.pending_granted_tool == name
        assert gate.pending_route == "sre-approvals"

    anyio.run(go)


def test_shipped_kubernetes_read_is_not_gated(tmp_path: Path) -> None:
    gate = _gate(tmp_path)
    hook = build_approval_hook(gate)["PreToolUse"][0].hooks[0]

    async def go() -> None:
        result = await hook(
            {"tool_name": "mcp__kubernetes__pods_list", "tool_input": {}}, None, None
        )
        decision = (result or {}).get("hookSpecificOutput", {}).get(
            "permissionDecision"
        )
        assert decision != "deny"
        assert gate.pending_route is None

    anyio.run(go)
