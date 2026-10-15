"""Execute packaging contracts; preparation behavior is covered by artifact evals."""

import json
import re
import shutil
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
import yaml
from plugin_format import PROFILE_AGENT_SKILLS_STRICT, TOOL_POLICY_ENFORCEMENT, validate_bundle

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples/dark-factory"
SKILL = BUNDLE / "preparation/skills/prepare-factory-ticket"


def assert_self_contained(skill: Path) -> None:
    assert (skill / "SKILL.md").is_file()
    files = [path for path in skill.rglob("*") if path.is_file()]
    assert files
    for path in skill.rglob("*"):
        assert not path.is_symlink(), path
        assert path.resolve().is_relative_to(skill.resolve()), path
        if path.is_file():
            # The portable installation carries only instructions and references.
            assert path.suffix == ".md", path
            assert not path.stat().st_mode & 0o111, path
            document = path.read_text(encoding="utf-8")
            for destination in re.findall(r"\[[^\]]*\]\(([^)]+)\)", document):
                target = urlsplit(destination.strip("<>"))
                if target.scheme or target.netloc or not target.path:
                    continue
                reference = (path.parent / unquote(target.path)).resolve()
                assert reference.is_relative_to(skill.resolve()), (path, destination)
                assert reference.is_file(), (path, destination)


def test_frontmatter_passes_portable_production_validation(tmp_path: Path) -> None:
    assert (SKILL / "SKILL.md").is_file()
    document = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    frontmatter = re.match(r"\A---\n(.*?)\n---(?:\n|$)", document, re.DOTALL)
    assert frontmatter is not None
    metadata = yaml.safe_load(frontmatter.group(1))
    assert metadata["name"] == SKILL.name
    assert isinstance(metadata["description"], str) and metadata["description"].strip()
    manifest = tmp_path / ".claude-plugin/plugin.json"
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({"name": "portable-skill-check"}), encoding="utf-8")
    shutil.copytree(SKILL, tmp_path / "skills" / SKILL.name)
    result = validate_bundle(tmp_path, profile=PROFILE_AGENT_SKILLS_STRICT)
    assert result.valid, result.errors


@pytest.mark.parametrize(
    "provider_directory", [".agents/skills", ".claude/skills", ".github/skills"]
)
def test_folder_only_install_is_self_contained(tmp_path: Path, provider_directory: str) -> None:
    assert_self_contained(SKILL)
    destination = tmp_path / "client project café" / provider_directory / SKILL.name
    shutil.copytree(SKILL, destination)
    assert_self_contained(destination)
    original = {path.relative_to(SKILL): path.read_bytes() for path in SKILL.rglob("*.md")}
    installed = {
        path.relative_to(destination): path.read_bytes() for path in destination.rglob("*.md")
    }
    assert installed == original
    assert not (destination / "hooks").exists()
    assert not (destination / "scripts").exists()
    assert not (destination / ".claude-plugin").exists()


def test_execution_validation_does_not_discover_preparation_skill(tmp_path: Path) -> None:
    assert (SKILL / "SKILL.md").is_file()
    copy = tmp_path / "factory"
    shutil.copytree(BUNDLE, copy, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    execution = list((copy / "skills").rglob("SKILL.md"))
    assert [path.parent.name for path in execution] == ["implement-issue"]
    before = validate_bundle(copy, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    nested = copy / SKILL.relative_to(BUNDLE) / "SKILL.md"
    nested.write_text("---\nname: []\n---\n", encoding="utf-8")
    after = validate_bundle(copy, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert after == before
    execution[0].write_text("---\nname: []\n---\n", encoding="utf-8")
    rejected = validate_bundle(copy, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert any(
        issue.code == "skill.frontmatter_invalid"
        and issue.location == "skills/implement-issue/SKILL.md"
        for issue in rejected.errors
    )
