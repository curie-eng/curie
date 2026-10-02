"""A deployed agent sees only its bundle's skills (#3766, ADR-0189 Draft).

The Claude Code CLI ships its own built-in skills (``update-config`` among them,
which steered #3625's "sign every reply" request away from ``remember``). With
``skills`` unset the SDK keeps every one of them in the model's listing. The
runner therefore always passes ``skills`` as the bundle's own list, and an
explicit ``setting_sources`` so the SDK does not quietly drop ``local``.
"""

import json
from pathlib import Path

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
