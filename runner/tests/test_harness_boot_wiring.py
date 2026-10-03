"""build_runner routes its harness-shaped behavior through the resolved
HarnessContribution (ADR-0060, #844 phase 1) rather than through hardcoded
Claude imports. These drive the real boot path (``fake_model=True``) and assert
that the harness's declared fields are what the boot path actually consumes.
"""

from __future__ import annotations

import json

import pytest
from curie_runner import PluginBundleError
from curie_runner.__main__ import DEFAULT_HARNESS, _resolve_harness, build_runner
from curie_runner.config import RunnerConfig
from curie_runner.harness import registry
from curie_runner.harness.contribution import BundleCompileResult, HarnessContribution
from curie_runner.harness.registry import (
    MalformedHarnessContributionError,
    UnknownHarnessError,
    UnsupportedHarnessError,
)
from curie_runner.history import ConversationMessage, ConversationReplay

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


def _config(tmp_path) -> RunnerConfig:
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True)
    (plugin / "plugin.json").write_text(json.dumps({"name": "wiring"}))
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-wire",
            "CURIE_SANDBOX_ID": "b-wire",
            "CURIE_BUDGET": _BUDGET,
        }
    )


def _harness(**overrides) -> HarnessContribution:
    """A minimal, non-Claude contribution so the assertions can tell whether the
    boot path read from THIS manifest or fell back to hardcoded Claude values."""
    defaults: dict = dict(
        name="test-harness",
        readonly_tools=frozenset({"CustomReadOnly"}),
        build_spawn_env=lambda env: None,
        compile_bundle=lambda plugin_dir: BundleCompileResult(
            plugins=[], system_prompt=None
        ),
    )
    defaults.update(overrides)
    return HarnessContribution(**defaults)


def test_harness_without_structured_replay_refuses_recovered_history(tmp_path) -> None:
    from curie_runner.history import StructuredReplayUnsupported

    replay = ConversationReplay(
        messages=(ConversationMessage(role="user", content="prior turn"),),
        source_turns=1,
    )
    harness = _harness(supports_structured_replay=False)

    with pytest.raises(StructuredReplayUnsupported, match="declares structured replay absent"):
        build_runner(_config(tmp_path), conversation_replay=replay, harness=harness)


def test_claude_runner_materializes_history_without_system_prompt_preamble(tmp_path) -> None:
    config = _config(tmp_path)
    replay = ConversationReplay(
        messages=(
            ConversationMessage(role="user", content="prior question"),
            ConversationMessage(
                role="assistant", content=[{"type": "text", "text": "prior answer"}]
            ),
        ),
        source_turns=1,
    )

    runner = build_runner(config, conversation_replay=replay)
    options = runner._factory()._options

    # An ineligible boot preserves the pre-progress model surface exactly.
    assert options.system_prompt is None
    assert options.resume is not None
    assert options.session_store is not None
    assert runner._history_resumed is True


def test_only_an_eligible_boot_mounts_the_progress_tool_and_prompt(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import anyio
    import mcp.types as mcp_types
    from curie_runner.tool_names import TURN_PROGRESS_TOOL
    from curie_runner.turn_progress import (
        PROGRESS_PREAMBLE,
        TURN_PROGRESS_ELIGIBILITY_ENV,
    )

    async def tool_names(runner: object) -> set[str]:
        options = runner._factory()._options  # type: ignore[attr-defined]
        server = options.mcp_servers["curie"]
        entry = server["instance"].get_request_handler("tools/list")
        assert entry is not None
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return {tool.name for tool in result.tools}

    monkeypatch.delenv(TURN_PROGRESS_ELIGIBILITY_ENV, raising=False)
    ineligible = build_runner(_config(tmp_path / "off"))
    assert TURN_PROGRESS_TOOL not in anyio.run(tool_names, ineligible)
    assert ineligible._factory()._options.system_prompt is None

    monkeypatch.setenv(TURN_PROGRESS_ELIGIBILITY_ENV, "1")
    eligible = build_runner(_config(tmp_path / "on"))
    assert TURN_PROGRESS_TOOL in anyio.run(tool_names, eligible)
    assert eligible._factory()._options.system_prompt == PROGRESS_PREAMBLE


def test_claude_runner_offers_provider_web_search_by_default(tmp_path) -> None:
    runner = build_runner(_config(tmp_path))
    options = runner._factory()._options

    assert options.tools == {"type": "preset", "preset": "claude_code"}
    assert "WebSearch" not in options.disallowed_tools
    assert options.allowed_tools == []


def test_claude_runner_bundle_opt_out_suppresses_provider_web_search(tmp_path) -> None:
    config = _config(tmp_path)
    (tmp_path / "curie.bundle.json").write_text(
        json.dumps({"webSearch": False}), encoding="utf-8"
    )

    runner = build_runner(config)
    options = runner._factory()._options

    assert options.tools == {"type": "preset", "preset": "claude_code"}
    assert options.disallowed_tools == ["WebSearch"]
    assert options.allowed_tools == []


def test_claude_runner_refuses_a_misspelled_bundle_opt_out(tmp_path) -> None:
    config = _config(tmp_path)
    (tmp_path / "curie.bundle.json").write_text(
        json.dumps({"websearch": False}), encoding="utf-8"
    )

    with pytest.raises(PluginBundleError, match="unknown key.*websearch"):
        build_runner(config)


def test_build_runner_uses_the_harness_readonly_set(tmp_path) -> None:
    # The side-effect classifier is built from the HARNESS's declared read-only
    # set, not a hardcoded Claude one: this harness's own tool is idempotent, and
    # Claude's "Read" -- absent from this set -- reads as side-effecting.
    runner = build_runner(_config(tmp_path), fake_model=True, harness=_harness())
    assert runner._classifier.is_side_effecting("CustomReadOnly") is False
    assert runner._classifier.is_side_effecting("Read") is True


def test_build_runner_default_harness_is_claude(tmp_path) -> None:
    # With no harness passed, the default resolves the built-in Claude harness,
    # whose read-only set contains "Read" (so it is not side-effecting).
    runner = build_runner(_config(tmp_path), fake_model=True)
    assert runner._classifier.is_side_effecting("Read") is False


def test_build_runner_routes_bundle_compile_through_the_harness(tmp_path) -> None:
    # The bundle is compiled via the harness's compile_bundle hook, called once
    # with the config's plugin dir -- not via a direct load_plugins import.
    calls: list[str | None] = []

    def spy(plugin_dir: str | None) -> BundleCompileResult:
        calls.append(plugin_dir)
        return BundleCompileResult(plugins=[], system_prompt=None)

    config = _config(tmp_path)
    build_runner(config, fake_model=True, harness=_harness(compile_bundle=spy))
    assert calls == [config.session.plugin_dir]


def test_resolve_harness_default_and_builtin_names() -> None:
    assert _resolve_harness().name == "claude"
    for name in ("claude", "claude-sdk", "claude-code"):
        assert _resolve_harness(name).name == "claude"


@pytest.mark.parametrize("name", ["rival", "no-such-harness"])
def test_resolve_harness_refuses_an_alternate_engine(name: str) -> None:
    with pytest.raises(UnsupportedHarnessError, match=name):
        _resolve_harness(name)


def test_config_selected_unregistered_harness_fails_loud() -> None:
    # The config selection reaches the same refusal that process boot uses.
    cfg = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": "/b",
            "CURIE_SESSION_ID": "s",
            "CURIE_SANDBOX_ID": "b",
            "CURIE_BUDGET": _BUDGET,
            "CURIE_HARNESS": "no-such-harness",
        }
    )
    assert cfg.harness == "no-such-harness"
    with pytest.raises(UnsupportedHarnessError, match="no-such-harness"):
        _resolve_harness(cfg.harness)


def test_resolve_harness_falls_back_to_builtin_when_registry_misses(monkeypatch) -> None:
    # The built in and its aliases remain independent of registration metadata.
    def miss() -> dict[str, HarnessContribution]:
        raise UnknownHarnessError("no contributions registered")

    monkeypatch.setattr(registry, "discover_contributions", miss)
    assert _resolve_harness(DEFAULT_HARNESS).name == "claude"
    assert _resolve_harness("claude-sdk").name == "claude"
    assert _resolve_harness("claude-code").name == "claude"
    with pytest.raises(UnsupportedHarnessError, match="other"):
        _resolve_harness("other")


def test_default_harness_survives_a_malformed_sibling(monkeypatch) -> None:
    # A malformed sibling cannot take the built in or its aliases down. An
    # alternate selection is refused before a registry error escapes discovery.
    def boom() -> dict[str, HarnessContribution]:
        raise MalformedHarnessContributionError("a sibling entry point is broken")

    monkeypatch.setattr(registry, "discover_contributions", boom)
    assert _resolve_harness(DEFAULT_HARNESS).name == "claude"
    assert _resolve_harness("claude-sdk").name == "claude"
    assert _resolve_harness("claude-code").name == "claude"
    with pytest.raises(UnsupportedHarnessError, match="other"):
        _resolve_harness("other")
