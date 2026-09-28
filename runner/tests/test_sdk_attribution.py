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
