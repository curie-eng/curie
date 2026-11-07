"""A deployed agent sees only its bundle's skills (#3766, ADR-0189 Draft).

The Claude Code CLI ships its own built-in skills (``update-config`` among them,
which steered #3625's "sign every reply" request away from ``remember``). With
``skills`` unset the SDK keeps every one of them in the model's listing. The
runner therefore always passes ``skills`` as the bundle's own list, and an
explicit ``setting_sources`` so the SDK does not quietly drop ``local``.
"""

import importlib
import json
import logging
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import anyio
import pytest
from curie_runner import build_options, load_plugins
from runner_boot_session import initialized_session_sync

# Today's settings loading, kept on purpose. With ``skills`` set and
# ``setting_sources`` left unset, the SDK fills in ``["user", "project"]``.
_SETTING_SOURCES = ["user", "project", "local"]


def _bundle(root: Path, skills: dict[str, str]) -> str:
    """A bundle named ``probe`` with one ``SKILL.md`` per relative skill dir."""
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "probe", "version": "0.1.0"}),
        encoding="utf-8",
    )
    for rel, frontmatter_name in skills.items():
        skill_dir = root / "skills" / rel
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(
            f"---\nname: {frontmatter_name}\ndescription: A test skill.\n---\n\nSay hello.\n",
            encoding="utf-8",
        )
    return str(root)


def _options(plugin_dir: str | None, **kwargs: object):
    return build_options(
        plugins=load_plugins(plugin_dir),
        model=None,
        system_prompt=None,
        max_turns=20,
        max_budget_usd=1.0,
        resume=None,
        **kwargs,
    )


def test_build_options_lists_only_the_bundle_skills(tmp_path: Path) -> None:
    from curie_runner.plugin import bundle_skill_names

    # The CLI names a skill by its directory, not its frontmatter ``name``, and
    # does not load nested folders, so neither may reach the list.
    plugin_dir = _bundle(
        tmp_path,
        {"a": "a", "b": "frontmatter-name-is-ignored", "group/nested": "nested"},
    )

    skills = bundle_skill_names(plugin_dir)
    options = _options(plugin_dir, skills=skills)

    assert skills == ["probe:a", "probe:b"]
    assert options.skills == ["probe:a", "probe:b"]
    assert options.setting_sources == _SETTING_SOURCES


def test_a_bundle_without_skills_lists_none(tmp_path: Path) -> None:
    from curie_runner.plugin import bundle_skill_names

    plugin_dir = _bundle(tmp_path, {})

    assert bundle_skill_names(plugin_dir) == []
    assert bundle_skill_names(None) == []
    options = _options(plugin_dir, skills=bundle_skill_names(plugin_dir))
    assert options.skills == []


def test_build_options_never_leaves_skills_unset() -> None:
    # ``None`` means "every skill the CLI has", built-ins included. A caller
    # that passes nothing must get the empty list, not the SDK default.
    options = _options(None)

    assert options.skills is not None
    assert options.skills == []


def test_build_options_passes_explicit_setting_sources() -> None:
    options = _options(None)

    assert options.setting_sources == _SETTING_SOURCES


# --- the boot wiring (review M1) ----------------------------------------------------
#
# ``build_options`` defaults ``skills`` to ``[]``, so a caller that forgets to pass
# the bundle's list hides every bundle skill and nothing else notices. These cases
# drive the real callers, not ``build_options``.

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


class _CapturedSession:
    """Stands in for ClaudeAgentSession so the options built at boot can be read."""

    def __init__(self, options: Any) -> None:
        self.options = options


def test_the_session_built_at_boot_lists_the_bundle_skills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: drop ``skills=bundle_skill_names(...)`` from ``build_runner`` ->
    # the default ``[]`` reaches the SDK and the deployed agent loses every one
    # of its own skills.
    from curie_runner import __main__ as boot
    from curie_runner.__main__ import build_runner
    from curie_runner.config import RunnerConfig

    plugin_dir = _bundle(tmp_path / "plugin", {"greet": "greet", "hello": "hello"})
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": plugin_dir,
            "CURIE_SESSION_ID": "s-3766",
            "CURIE_SANDBOX_ID": "b-3766",
            "CURIE_BUDGET": _BUDGET,
        }
    )
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)

    runner = build_runner(config)
    session = initialized_session_sync(runner)  # noqa: SLF001 -- the boot wiring is the subject

    assert isinstance(session, _CapturedSession)
    assert session.options.skills == ["probe:greet", "probe:hello"]
    assert session.options.setting_sources == _SETTING_SOURCES


def test_the_bundle_check_builds_its_options_with_the_bundle_skills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ``curie skill check`` connects a real client of its own; it should match
    # what a session would load.
    from curie_runner import check as check_mod

    captured: list[Any] = []

    class _Client:
        def __init__(self, options: Any) -> None:
            captured.append(options)

        async def connect(self) -> None:
            return None

        async def get_mcp_status(self) -> dict[str, Any]:
            return {"mcpServers": []}

        async def disconnect(self) -> None:
            return None

    monkeypatch.setattr(check_mod, "ClaudeSDKClient", _Client)
    plugin_dir = _bundle(tmp_path / "plugin", {"greet": "greet", "hello": "hello"})

    anyio.run(check_mod._connect_and_poll, load_plugins(plugin_dir), plugin_dir)  # noqa: SLF001

    [options] = captured
    assert options.skills == ["probe:greet", "probe:hello"]


# --- skill folder names the SDK cannot pass (review L3) --------------------------------
#
# The bundle validator only warns about these names, but the SDK raises
# ``ValueError`` while building the CLI command for any of them, so one oddly
# named folder used to stop the whole session at connect.

_UNPASSABLE = ("a,b", "a(b)", "trailing ")


def test_a_skill_folder_the_sdk_rejects_is_skipped_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
    from curie_runner.plugin import bundle_skill_names

    caplog.set_level(logging.WARNING, logger="curie_runner")
    plugin_dir = _bundle(
        tmp_path,
        {"greet": "greet", "hello": "hello", **{name: "odd" for name in _UNPASSABLE}},
    )

    skills = bundle_skill_names(plugin_dir)

    assert skills == ["probe:greet", "probe:hello"]
    warned = "\n".join(record.getMessage() for record in caplog.records)
    for name in _UNPASSABLE:
        assert name in warned, f"no warning names the skipped skill folder {name!r}"
    # The session still boots: the SDK builds its CLI command from these options.
    options = replace(_options(plugin_dir, skills=skills), cli_path="claude")
    command = SubprocessCLITransport(prompt="", options=options)._build_command()  # noqa: SLF001
    assert "Skill(probe:greet),Skill(probe:hello)" in command


# --- surviving a renamed SDK skill-name validator ------------------------------------
#
# ``curie_runner.plugin`` borrows the SDK's private ``_validate_skill_name``. The SDK
# is pinned only ``>=``, so an upgrade may move or rename it. That must not stop the
# runner importing: ``plugin`` keeps a local copy of the rule,
# ``_local_validate_skill_name``, and uses it when the SDK's is gone.

_SDK_VALIDATOR_MODULE = "claude_agent_sdk._internal.transport.subprocess_cli"


def _renamed_validator_module() -> ModuleType:
    """The SDK module as a later release might ship it: validator renamed."""
    import claude_agent_sdk._internal.transport.subprocess_cli as real

    stub = ModuleType(_SDK_VALIDATOR_MODULE)
    stub.__dict__.update(
        {key: value for key, value in vars(real).items() if key != "_validate_skill_name"}
    )
    return stub


@pytest.fixture
def plugin_without_sdk_validator(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    """Make the SDK validator unimportable and unload ``curie_runner.plugin``.

    The original module goes back into ``sys.modules`` (and onto the package) at
    teardown, so other tests keep the classes they already imported.
    """
    import curie_runner

    monkeypatch.setattr(curie_runner, "plugin", sys.modules["curie_runner.plugin"])
    monkeypatch.setitem(sys.modules, "curie_runner.plugin", sys.modules["curie_runner.plugin"])
    if request.param == "module-moved":
        monkeypatch.setitem(sys.modules, _SDK_VALIDATOR_MODULE, None)
    else:
        monkeypatch.setitem(sys.modules, _SDK_VALIDATOR_MODULE, _renamed_validator_module())
    with pytest.raises(ImportError):
        exec(f"from {_SDK_VALIDATOR_MODULE} import _validate_skill_name", {})
    del sys.modules["curie_runner.plugin"]


@pytest.mark.parametrize(
    "plugin_without_sdk_validator", ["module-moved", "function-renamed"], indirect=True
)
def test_plugin_imports_and_filters_skills_without_the_sdk_validator(
    plugin_without_sdk_validator: None, tmp_path: Path
) -> None:
    plugin = importlib.import_module("curie_runner.plugin")
    plugin_dir = _bundle(
        tmp_path,
        {"hello": "hello", **{name: "odd" for name in _UNPASSABLE}},
    )

    assert plugin.bundle_skill_names(plugin_dir) == ["probe:hello"]


# Names the CLI can and cannot take in a ``Skill(name)`` rule. ``True`` is valid.
_SKILL_NAME_SAMPLES = {
    "probe:hello": True,
    "greet": True,
    "probe:my-skill_2": True,
    "probe:a.b": True,
    "probe:a,b": False,
    "probe:a(b)": False,
    "probe:trailing ": False,
    " leading": False,
    "": False,
    "*": False,
    "probe:*": False,
    "/slash": False,
    "probe:tab\there": False,
    "probe:back\\\\slash": False,
    "probe:ends\\": False,
    "probe:a﻿": False,
    "﻿probe:a": False,
    "probe:a\x7f": False,
    "probe:a\x85": False,
    "probe:a\x9f": False,
    "probe:\ud800": False,
    "probe: *": False,
}


def _accepts(validator: Any, name: str) -> bool:
    try:
        validator(name)
    except ValueError:
        return False
    return True


@pytest.mark.parametrize("name", list(_SKILL_NAME_SAMPLES))
def test_the_local_skill_name_rule_matches_the_sdk(name: str) -> None:
    sdk_module = pytest.importorskip(_SDK_VALIDATOR_MODULE)
    sdk_validator = getattr(sdk_module, "_validate_skill_name", None)
    if sdk_validator is None:
        pytest.skip("the SDK no longer ships _validate_skill_name")
    from curie_runner.plugin import _local_validate_skill_name

    assert _accepts(_local_validate_skill_name, name) is _SKILL_NAME_SAMPLES[name]
    assert _accepts(_local_validate_skill_name, name) is _accepts(sdk_validator, name), (
        f"the local skill-name rule and the SDK's disagree on {name!r}"
    )


# --- personal mode keeps the settings loading (#5373, ADR 0268) -------------------------
#
# Personal mode changes who authenticates, not what the CLI loads: the same
# explicit setting sources (ADR 0189), the same bundle skill list and strict MCP
# config, and never ``--bare`` (bare mode skips CLAUDE_CODE_OAUTH_TOKEN:
# https://code.claude.com/docs/en/authentication, "Generate a long-lived token").
# The reviewer alias resolves from the explicit reviewer model alone.

_REVIEWER_ENV = "ANTHROPIC_DEFAULT_OPUS_MODEL"
_CREDENTIAL_ENVS = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "CURIE_MODEL_AUTH_MODE")


def _clear_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*_CREDENTIAL_ENVS, _REVIEWER_ENV):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    ("ambient_credential", "reviewer"),
    [
        # The service prefix rule would pick openai/gpt-6.1-sol for these.
        ("sk-or-PLACEHOLDER", "claude-opus-5-5"),
        ("PLACEHOLDER-non-anthropic", "claude-fable-5-1"),
        # And claude-opus-5-5 for this one.
        ("sk-ant-oat01-PLACEHOLDER-owner", "claude-sonnet-5-5[1m]"),
    ],
)
def test_personal_options_keep_settings_loading_and_take_the_explicit_reviewer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ambient_credential: str,
    reviewer: str,
) -> None:
    from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
    from curie_runner.personal_auth import AUTH_MODE_ENV
    from curie_runner.plugin import bundle_skill_names

    _clear_credentials(monkeypatch)
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", ambient_credential)
    monkeypatch.setenv(_REVIEWER_ENV, "claude-opus-4-1-20250805")
    plugin_dir = _bundle(tmp_path, {"greet": "greet", "hello": "hello"})

    options = _options(plugin_dir, skills=bundle_skill_names(plugin_dir), reviewer_model=reviewer)

    assert options.env[_REVIEWER_ENV] == reviewer
    assert options.setting_sources == _SETTING_SOURCES
    assert options.skills == ["probe:greet", "probe:hello"]
    assert options.strict_mcp_config is True
    command = SubprocessCLITransport(
        prompt="", options=replace(options, cli_path="claude")
    )._build_command()  # noqa: SLF001 -- the argv the CLI is spawned with
    assert "--bare" not in command
    assert "--setting-sources=user,project,local" in command
    assert "--strict-mcp-config" in command


@pytest.mark.parametrize(
    ("credential_env", "credential", "default"),
    [
        ("CLAUDE_CODE_OAUTH_TOKEN", "sk-ant-oat01-PLACEHOLDER", "claude-opus-5-5"),
        ("ANTHROPIC_API_KEY", "sk-ant-api03-PLACEHOLDER", "claude-opus-5-5"),
        ("ANTHROPIC_API_KEY", "sk-or-PLACEHOLDER", "openai/gpt-6.1-sol"),
    ],
)
@pytest.mark.parametrize("mode", [None, "service"], ids=["mode-absent", "mode-service"])
def test_service_options_keep_the_credential_prefix_reviewer_default(
    monkeypatch: pytest.MonkeyPatch,
    mode: str | None,
    credential_env: str,
    credential: str,
    default: str,
) -> None:
    _clear_credentials(monkeypatch)
    if mode is not None:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", mode)
    monkeypatch.setenv(credential_env, credential)

    assert _options(None).env[_REVIEWER_ENV] == default
    # An explicit service reviewer is passed through as it is, Claude or not.
    assert _options(None, reviewer_model="openai/gpt-6.1-sol").env[_REVIEWER_ENV] == (
        "openai/gpt-6.1-sol"
    )
    assert _options(None).setting_sources == _SETTING_SOURCES
