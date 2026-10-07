"""The runner turns the SDK's commit and PR attribution off (#3193).

The Claude Agent SDK's CLI ships built-in instructions that tell the model to
end commit messages with a ``Co-Authored-By: Claude`` trailer and pull request
bodies with the ``Generated with [Claude Code]`` footer. Claude models
sometimes omit them; other models copy them in, and repositories whose CI
rejects AI attribution then pay a round removing them. The runner therefore
sets the CLI's attribution texts to empty strings on the flag-settings layer
for every SDK session it builds, so the CLI never emits the instruction at
all (with both texts empty the attribution system-reminder is skipped
entirely) -- no model, whatever it is, ever sees it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from curie_runner.adapter import build_options

# The current CLI attribution setting: each text defaults to the standard
# trailer/footer and an empty string hides it. The deprecated boolean form
# (``includeCoAuthoredBy: false``) means the same thing; the object form is
# the non-deprecated spelling shared across CLI versions.
ATTRIBUTION_OFF = {"attribution": {"commit": "", "pr": ""}}


def test_build_options_disables_sdk_attribution() -> None:
    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
    )
    assert json.loads(options.settings) == ATTRIBUTION_OFF


def test_build_options_disables_sdk_attribution_regardless_of_other_options() -> None:
    # The flag rides every session the runner builds, not a mode: the same
    # minimal-options call the connector check (check.py) makes carries it.
    check_tier = build_options(
        plugins=[],
        mcp_servers={},
        model=None,
        system_prompt=None,
        max_turns=1,
        max_budget_usd=None,
        resume=None,
    )
    assert json.loads(check_tier.settings) == ATTRIBUTION_OFF


@pytest.mark.parametrize(
    ("inherited_model", "option_model", "title_enabled"),
    [
        (None, None, False),
        ("haiku-inherited", None, True),
        (None, "haiku-explicit", True),
        ("haiku-inherited", "", False),
    ],
)
def test_build_options_only_enables_sdk_title_with_configured_model(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    inherited_model: str | None,
    option_model: str | None,
    title_enabled: bool,
) -> None:
    # Claude Code documents that disabling terminal titles also skips the
    # background title request, and that the Haiku override selects its model:
    # https://code.claude.com/docs/en/env-vars
    model_key = "ANTHROPIC_DEFAULT_HAIKU_MODEL"
    disable_key = "CLAUDE_CODE_DISABLE_TERMINAL_TITLE"
    monkeypatch.delenv(model_key, raising=False)
    monkeypatch.delenv(disable_key, raising=False)
    if inherited_model is not None:
        monkeypatch.setenv(model_key, inherited_model)
    env = {model_key: option_model} if option_model is not None else {}

    with caplog.at_level(logging.INFO, logger="curie_runner.adapter"):
        options = build_options(
            plugins=[],
            model=None,
            system_prompt=None,
            max_turns=20,
            max_budget_usd=1.0,
            resume=None,
            env=env,
        )

    if title_enabled:
        assert disable_key not in options.env
        assert "session title" not in caplog.text
    else:
        assert options.env[disable_key] == "1"
        assert len([record for record in caplog.records if "session title" in record.message]) == 1


@pytest.mark.parametrize(
    ("credential", "override", "expected"),
    [
        ("sk-ant-api03-PLACEHOLDER", None, "claude-opus-5-5"),
        ("sk-ant-oat01-PLACEHOLDER", None, "claude-opus-5-5"),
        ("sk-or-PLACEHOLDER", None, "anthropic/claude-opus-5.5"),
        ("sk-ant-api03-PLACEHOLDER", "acme-reviewer-model", "acme-reviewer-model"),
        ("sk-or-PLACEHOLDER", "acme-reviewer-model", "acme-reviewer-model"),
    ],
)
def test_reviewer_model_reaches_sdk_options_from_the_runner_boot_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    credential: str,
    override: str | None,
    expected: str,
) -> None:
    # The SDK resolves model: opus through this documented environment key:
    # https://code.claude.com/docs/en/model-config#environment-variables
    from curie_runner import __main__ as boot
    from curie_runner.config import RunnerConfig
    from curie_runner.sdk_auth import resolve_sdk_env

    for key in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_AUTH_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ANTHROPIC_DEFAULT_OPUS_MODEL", "stale-provider-model")
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-reviewer-probe", "version": "0.1.0"}),
        encoding="utf-8",
    )
    env = {
        "CURIE_PLUGIN_DIR": str(tmp_path),
        "CURIE_SESSION_ID": "reviewer-model-session",
        "CURIE_SANDBOX_ID": "reviewer-model-sandbox",
        "CURIE_BUDGET": '{"max_output_tokens_per_run":1000,"max_usd_per_day":1.0}',
        "CURIE_CREDENTIALS": credential,
        "CURIE_MODEL": "acme-implementer-model",
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": "acme-title-model",
    }
    if override is not None:
        env["CURIE_REVIEWER_MODEL"] = override
    config = RunnerConfig.from_env(env)
    spawn_env = resolve_sdk_env(env) or env

    class CapturedSession:
        def __init__(self, options: Any) -> None:
            self.options = options

    monkeypatch.setattr(boot, "ClaudeAgentSession", CapturedSession)
    session = boot.build_runner(config, sdk_env=spawn_env)._factory()

    assert isinstance(session, CapturedSession)
    assert session.options.env["ANTHROPIC_DEFAULT_OPUS_MODEL"] == expected
    assert session.options.model == "acme-implementer-model"
    assert session.options.env["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "acme-title-model"


def test_reviewer_model_default_uses_the_effective_sdk_credential() -> None:
    # sdk_auth's explicit SDK credential wins over CURIE_CREDENTIALS; reviewer
    # model selection must agree with the credential the SDK will actually use.
    from curie_runner.sdk_auth import resolve_sdk_env

    env = {
        "CURIE_CREDENTIALS": "sk-or-PLACEHOLDER",
        "ANTHROPIC_API_KEY": "sk-ant-api03-PLACEHOLDER",
    }
    spawn_env = resolve_sdk_env(env) or env
    options = build_options(
        plugins=[],
        model="acme-implementer-model",
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
        env=spawn_env,
    )
    assert options.env["ANTHROPIC_DEFAULT_OPUS_MODEL"] == "claude-opus-5-5"
    assert options.model == "acme-implementer-model"
