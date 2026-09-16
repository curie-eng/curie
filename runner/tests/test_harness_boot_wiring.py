"""build_runner routes its harness-shaped behavior through the resolved
HarnessContribution (ADR-0060, #844 phase 1) rather than through hardcoded
Claude imports. These drive the real boot path (``fake_model=True``) and assert
that the harness's declared fields are what the boot path actually consumes.
"""

from __future__ import annotations

import json

import pytest
from curie_runner import PluginBundleError
from curie_runner import __main__ as boot
from curie_runner.__main__ import DEFAULT_HARNESS, _resolve_harness, build_runner
from curie_runner.config import RunnerConfig
from curie_runner.harness.contribution import (
    AuthSpec,
    BundleCompileResult,
    HarnessContribution,
    InstallSpec,
)
from curie_runner.harness.registry import (
    MalformedHarnessContributionError,
    UnknownHarnessError,
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
        image="test-image",
        install=InstallSpec(),
        auth=AuthSpec(credential_env_keys=(), oauth_token_prefix=None),
        readonly_tools=frozenset({"CustomReadOnly"}),
        model_override_env_keys=(),
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

    assert options.system_prompt is None
    assert options.resume is not None
    assert options.session_store is not None
    assert runner._history_resumed is True


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


def test_resolve_harness_default_alias_and_unknown() -> None:
    assert _resolve_harness().name == "claude"
    assert _resolve_harness("claude-sdk").name == "claude"  # an alias resolves
    with pytest.raises(UnknownHarnessError):
        _resolve_harness("no-such-harness")


def test_config_selected_unregistered_harness_fails_loud() -> None:
    # End to end: a config-selected harness that isn't registered raises through
    # the same _resolve_harness(config.harness) call main() makes -- no silent
    # fallback for a non-built-in name, so a misconfigured harness fails visibly.
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
    with pytest.raises(UnknownHarnessError):
        _resolve_harness(cfg.harness)


def test_resolve_harness_falls_back_to_builtin_when_registry_misses(monkeypatch) -> None:
    # If entry-point discovery somehow cannot surface the built-in, the default
    # falls back to its direct import so the boot path never loses Claude. A
    # non-built-in name still raises rather than silently falling back.
    def miss(name: str, **kwargs: object) -> HarnessContribution:
        raise UnknownHarnessError(name)

    monkeypatch.setattr(boot, "resolve_harness", miss)
    assert _resolve_harness(DEFAULT_HARNESS).name == "claude"
    with pytest.raises(UnknownHarnessError):
        _resolve_harness("other")


def test_default_harness_survives_a_malformed_sibling(monkeypatch) -> None:
    # #865: a malformed / colliding / import-crashing sibling entry point makes
    # discover_contributions raise a GUARD error (not UnknownHarnessError), which
    # the old code let propagate past the fallback and take the built-in down with
    # it. The built-in must never depend on that scan, so a built-in name -- its
    # declared name AND every alias -- still resolves to Claude. A non-built-in
    # name still surfaces the guard error loudly rather than silently falling back.
    def boom(name: str, **kwargs: object) -> HarnessContribution:
        raise MalformedHarnessContributionError("a sibling entry point is broken")

    monkeypatch.setattr(boot, "resolve_harness", boom)
    assert _resolve_harness(DEFAULT_HARNESS).name == "claude"
    assert _resolve_harness("claude-sdk").name == "claude"  # aliases protected too
    with pytest.raises(MalformedHarnessContributionError):
        _resolve_harness("other")
