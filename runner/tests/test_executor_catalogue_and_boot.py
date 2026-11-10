"""The model catalogue's ``restore`` rule and the executor-mode boot refusal.

@spec ACTION-EXECUTOR-8: from the runner's own boot ``tools/list``, a connector
that advertises both ``restore`` and ``observe_version`` has
``mcp__<connector>__restore`` passed to ``build_options`` as disallowed, so the
model never sees it; a lone ``restore`` stays an ordinary tool; hiding fails
closed for a connector whose boot probe failed; ``observe_version`` is never
hidden. Each case runs the real boot probe against
``fixtures/mcp_executor_connector.py`` over stdio, then the production
``hidden_restore_tools`` and ``build_options``.

@spec ACTION-EXECUTOR-4: a runner started in executor mode with a model
credential in its env refuses to boot; without one it serves the executor app
without resolving a harness or loading any boot fetch.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp import web
from curie_runner import __main__ as boot
from curie_runner.adapter import build_options, hidden_restore_tools
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability
from plugin_format.approval_policy import connector_tool_prefix

_FIXTURE = Path(__file__).parent / "fixtures" / "mcp_executor_connector.py"
_VECTOR = json.loads(
    (Path(__file__).resolve().parents[2] / "tests" / "vectors" / "runner-execute.json").read_text(
        "utf-8"
    )
)
_CONNECTOR = "example-scale"
_PREFIX = connector_tool_prefix(_CONNECTOR)


def _catalogue_disallowed(connector: dict[str, Any]) -> list[str]:
    """The disallowed tools a live boot passes for this one connector."""

    servers = {_CONNECTOR: connector}
    capability = anyio.run(probe_mcp_tool_capability, None, servers, {})
    hidden = hidden_restore_tools(
        servers,
        capability.observed_tools,
        frozenset(capability.failures)
        | frozenset(failure.connector for failure in capability.connector_failures),
        probe_complete=capability.complete,
    )
    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
        hidden_restore_tools=hidden,
    )
    return list(options.disallowed_tools)


def _fixture(tools: str) -> dict[str, Any]:
    return {
        "command": sys.executable,
        "args": [str(_FIXTURE)],
        "env": {"CURIE_TEST_EXECUTOR_TOOLS": tools},
    }


def test_a_paired_restore_is_hidden_and_observe_version_is_not() -> None:
    disallowed = _catalogue_disallowed(_fixture("paired"))
    assert f"{_PREFIX}restore" in disallowed
    assert f"{_PREFIX}observe_version" not in disallowed
    assert f"{_PREFIX}scale" not in disallowed


def test_a_lone_restore_stays_an_ordinary_tool() -> None:
    disallowed = _catalogue_disallowed(_fixture("lone_restore"))
    assert f"{_PREFIX}restore" not in disallowed
    assert disallowed == []


def test_a_connector_without_restore_changes_nothing() -> None:
    assert _catalogue_disallowed(_fixture("no_restore")) == []


def test_a_failed_boot_probe_hides_restore(tmp_path: Path) -> None:
    """Fails closed: a paired ``restore`` must never show while the proxy may not gate it."""

    broken = {"command": str(tmp_path / "example-missing-connector"), "args": []}
    disallowed = _catalogue_disallowed(broken)
    assert f"{_PREFIX}restore" in disallowed
    assert f"{_PREFIX}observe_version" not in disallowed


# --------------------------------------------------------------------------- #
# Executor-mode boot
# --------------------------------------------------------------------------- #


@pytest.fixture
def executor_boot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "example-bot"}), encoding="utf-8"
    )
    for name in boot.executor_model_credentials(os.environ):
        monkeypatch.delenv(name, raising=False)
    env = {
        "CURIE_PLUGIN_DIR": str(plugin),
        "CURIE_SESSION_ID": "example-session",
        "CURIE_SANDBOX_ID": "example-sandbox",
        "CURIE_BUDGET": '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 5.0}',
        "CURIE_RUNNER_TOKEN": "example-runner-token",
        _VECTOR["mode_variable"]["name"]: _VECTOR["mode_variable"]["value"],
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    record: dict[str, Any] = {"apps": [], "harness": 0, "fetches": 0}

    def _resolve_harness(_name: str) -> object:
        record["harness"] += 1
        return object()

    async def _load_boot_fetches(*_args: Any) -> Any:
        record["fetches"] += 1
        raise AssertionError("executor mode loads no boot fetch")

    def _run_app(app: web.Application, **_kwargs: Any) -> None:
        record["apps"].append(app)

    monkeypatch.setattr(boot, "_resolve_harness", _resolve_harness)
    monkeypatch.setattr(boot, "_load_boot_fetches", _load_boot_fetches)
    monkeypatch.setattr(web, "run_app", _run_app)
    return record


def test_an_executor_boot_without_a_model_credential_serves_the_executor_app(
    executor_boot: dict[str, Any],
) -> None:
    boot._serve()  # noqa: SLF001 -- the process entrypoint is the subject

    (app,) = executor_boot["apps"]
    paths = {resource.canonical for resource in app.router.resources()}
    assert _VECTOR["route"]["path"] in paths
    assert executor_boot["harness"] == 0
    assert executor_boot["fetches"] == 0


@pytest.mark.parametrize("credential", ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"])
def test_an_executor_boot_with_a_model_credential_refuses_to_start(
    executor_boot: dict[str, Any], monkeypatch: pytest.MonkeyPatch, credential: str
) -> None:
    monkeypatch.setenv(credential, "example-not-a-real-credential")

    with pytest.raises(boot.ExecutorModelCredentialError) as refused:
        boot._serve()  # noqa: SLF001 -- the process entrypoint is the subject

    assert credential in str(refused.value)
    assert "example-not-a-real-credential" not in str(refused.value)
    assert executor_boot["apps"] == []
    assert executor_boot["harness"] == 0
