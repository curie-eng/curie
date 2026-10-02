"""A deployed agent sees only its bundle's skills (#3766, ADR-0189 Draft).

The Claude Code CLI ships its own built-in skills (``update-config`` among them,
which steered #3625's "sign every reply" request away from ``remember``). With
``skills`` unset the SDK keeps every one of them in the model's listing. The
runner therefore always passes ``skills`` as the bundle's own list, and an
explicit ``setting_sources`` so the SDK does not quietly drop ``local``.
"""

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any

import anyio
import pytest
from curie_runner import build_options, load_plugins

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
    session = runner._factory()  # noqa: SLF001 -- the boot wiring is the subject

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
