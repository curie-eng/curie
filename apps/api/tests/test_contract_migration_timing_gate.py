"""Contract migration timing gate (#4610).

A migration whose revision kind is ``contract`` or ``irreversible`` must not
drop or rename a column or table that source at the previous stable release tag
still references. The checker reads git objects only, so every case builds a
tiny throwaway git repo.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from _migration_support import run_script

REPO_ROOT = Path(__file__).resolve().parents[3]
CHECKER = REPO_ROOT / "scripts" / "check-contract-migration-timing.py"
CHECK_COMMAND = "uv run python scripts/check-contract-migration-timing.py"

KINDS_PATH = "apps/api/src/curie_api/revision_kinds.json"
VERSIONS_DIR = "apps/api/alembic/versions"
CRUD = "apps/api/src/curie_api/crud.py"
# The column name sits on line 3.
CRUD_REFERENCING_OLD_COL = "import os\n\nvalue = row.old_col\n"

BASE_KINDS = {"0001": "expand"}
HEAD_KINDS = {"0001": "expand", "0002": "contract"}


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", "-b", "main", str(root)], check=True)
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "Test")
    _git(root, "config", "commit.gpgsign", "false")
    _git(root, "config", "tag.gpgsign", "false")


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _commit(root: Path, message: str = "c") -> None:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "--allow-empty", "-m", message)


def _tag(root: Path, name: str, *, annotated: bool = False) -> None:
    if annotated:
        _git(root, "tag", "-a", name, "-m", name)
    else:
        _git(root, "tag", name)


def _write_kinds(root: Path, kinds: dict[str, str]) -> None:
    _write(root, KINDS_PATH, json.dumps(kinds, indent=2) + "\n")


def _revision_module(revision: str, down_revision: str | None, body: str) -> str:
    return (
        "from alembic import op\n"
        "\n"
        f'revision: str = "{revision}"\n'
        f"down_revision = {down_revision!r}\n"
        "\n" + body
    )


def _write_revision(root: Path, revision: str, down_revision: str | None, body: str) -> str:
    text = _revision_module(revision, down_revision, body)
    _write(root, f"{VERSIONS_DIR}/{revision}_x.py", text)
    return text


def _line_of(text: str, needle: str) -> int:
    for number, line in enumerate(text.splitlines(), start=1):
        if needle in line:
            return number
    raise AssertionError(f"{needle!r} not in module")


def _upgrade(*lines: str) -> str:
    body = "\n".join(f"    {line}" for line in lines)
    return f"def upgrade() -> None:\n{body}\n\n\ndef downgrade() -> None:\n    pass\n"


def _scenario(
    tmp_path: Path,
    body: str,
    *,
    sources: dict[str, str] | None = None,
    tag_kinds: dict[str, str] | None = None,
    head_kinds: dict[str, str] | None = None,
    annotated: bool = False,
) -> tuple[Path, str]:
    """Commit 1 (tagged v0.1.0) holds the sources; commit 2 adds revision 0002.

    Returns the repo root and the text of the 0002 revision module.
    """
    root = tmp_path / "repo"
    _init(root)
    _write_kinds(root, tag_kinds or BASE_KINDS)
    _write_revision(root, "0001", None, _upgrade("pass"))
    for rel, text in ({CRUD: CRUD_REFERENCING_OLD_COL} if sources is None else sources).items():
        _write(root, rel, text)
    _commit(root, "release")
    _tag(root, "v0.1.0", annotated=annotated)
    _write_kinds(root, head_kinds or HEAD_KINDS)
    module = _write_revision(root, "0002", "0001", body)
    _commit(root, "contract")
    return root, module


def _run(root: Path, *args: str):
    return run_script(CHECKER, "--repo-root", str(root), *args)


def _assert_violation(result, *needles: str) -> None:
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "Contract migration timing gate failed" in result.stderr
    for needle in needles:
        assert needle in result.stderr, (needle, result.stderr)


def _assert_ok(result) -> None:
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.startswith("contract-migration-timing OK"), result.stdout


def test_dropped_referenced_column_fails_with_revision_name_tag_and_line(
    tmp_path: Path,
) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    _assert_violation(_run(root), "0002", "old_col", "v0.1.0", f"{CRUD}:3")


def test_dropped_unreferenced_column_passes(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        sources={CRUD: "import os\n\nvalue = row.other\n"},
    )
    _assert_ok(_run(root))


def test_dropped_referenced_table_fails(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_table("widgets")'),
        sources={CRUD: "import os\n\nTABLE = widgets\n"},
    )
    _assert_violation(_run(root), "0002", "widgets", "v0.1.0", f"{CRUD}:3")


def test_rename_of_referenced_column_fails_naming_old_column(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.alter_column("t", "old_col", new_column_name="new_col")'),
    )
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_alter_column_without_rename_passes(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.alter_column("t", "old_col", nullable=False)'),
    )
    _assert_ok(_run(root))


def test_nonliteral_loop_variable_fails_with_revision_line_and_literal(
    tmp_path: Path,
) -> None:
    body = _upgrade(
        'for name in ("a", "b"):',
        '    op.drop_column("t", name)',
    )
    root, module = _scenario(tmp_path, body, sources={})
    line = _line_of(module, "op.drop_column")
    result = _run(root)
    _assert_violation(result, "0002", "literal")
    assert re.search(rf"\b{line}\b", result.stderr), result.stderr


def test_nonliteral_helper_parameter_fails(tmp_path: Path) -> None:
    body = 'def _drop(col):\n    op.drop_column("t", col)\n\n\n' + _upgrade('_drop("old_col")')
    root, module = _scenario(tmp_path, body)
    line = _line_of(module, "op.drop_column")
    result = _run(root)
    _assert_violation(result, "0002", "literal")
    assert re.search(rf"\b{line}\b", result.stderr), result.stderr


def test_nonliteral_fstring_fails(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('suffix = "col"', 'op.drop_column("t", f"old_{suffix}")'),
        sources={},
    )
    _assert_violation(_run(root), "0002", "literal")


def test_module_constants_resolve_plain_and_annotated(tmp_path: Path) -> None:
    body = '_T = "widgets"\n_C: str = "old_col"\n\n\n' + _upgrade("op.drop_column(_T, _C)")
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_module_constant_table_name_resolves_for_drop_table(tmp_path: Path) -> None:
    body = '_T = "widgets"\n\n\n' + _upgrade("op.drop_table(_T)")
    root, _ = _scenario(tmp_path, body, sources={CRUD: "import os\n\nTABLE = widgets\n"})
    _assert_violation(_run(root), "widgets", f"{CRUD}:3")


def test_expand_revision_is_ignored(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        head_kinds={"0001": "expand", "0002": "expand"},
    )
    _assert_ok(_run(root))


def test_irreversible_revision_behaves_like_contract(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        head_kinds={"0001": "expand", "0002": "irreversible"},
    )
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_contract_revision_already_present_at_tag_is_ignored(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        tag_kinds=HEAD_KINDS,
    )
    _assert_ok(_run(root))


def test_drop_in_helper_reachable_from_upgrade_is_found_transitively(
    tmp_path: Path,
) -> None:
    body = (
        "def _step_two() -> None:\n"
        '    op.drop_column("t", "old_col")\n'
        "\n\n"
        "def _step_one() -> None:\n"
        "    _step_two()\n"
        "\n\n" + _upgrade("_step_one()")
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_drop_only_in_downgrade_is_excluded(tmp_path: Path) -> None:
    body = (
        "def upgrade() -> None:\n"
        "    pass\n"
        "\n\n"
        "def downgrade() -> None:\n"
        '    op.drop_column("t", "old_col")\n'
    )
    root, _ = _scenario(tmp_path, body)
    _assert_ok(_run(root))


def test_drop_only_in_downgrade_only_helper_is_excluded(tmp_path: Path) -> None:
    body = (
        "def _undo() -> None:\n"
        '    op.drop_column("t", "old_col")\n'
        "\n\n"
        "def upgrade() -> None:\n"
        "    pass\n"
        "\n\n"
        "def downgrade() -> None:\n"
        "    _undo()\n"
    )
    root, _ = _scenario(tmp_path, body)
    _assert_ok(_run(root))


def test_batch_alter_table_drop_column_is_found(tmp_path: Path) -> None:
    body = _upgrade(
        'with op.batch_alter_table("t") as batch_op:',
        '    batch_op.drop_column("old_col")',
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_reference_match_is_whole_word_only(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "repo")'),
        sources={CRUD: "import os\n\nvalue = repository\n"},
    )
    _assert_ok(_run(root))


def test_reference_under_packages_src_is_found(tmp_path: Path) -> None:
    rel = "packages/foo/src/foo/mod.py"
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        sources={rel: "x = 1\ny = 2\nz = row.old_col\n"},
    )
    _assert_violation(_run(root), "old_col", f"{rel}:3")


def test_reference_only_in_tests_directory_is_ignored(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        sources={"apps/api/tests/test_x.py": CRUD_REFERENCING_OLD_COL},
    )
    _assert_ok(_run(root))


def test_reference_only_in_markdown_under_src_is_ignored(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.drop_column("t", "old_col")'),
        sources={"apps/api/src/curie_api/notes.md": "uses old_col\n"},
    )
    _assert_ok(_run(root))


def test_reference_is_read_at_the_tag_not_at_head(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    _write(root, CRUD, "import os\n\nvalue = row.new_col\n")
    _commit(root, "remove the reference after the release")
    _assert_violation(_run(root), "old_col", f"{CRUD}:3")


def test_explicit_previous_tag_and_head_ref(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'), annotated=True)
    _assert_violation(
        _run(root, "--previous-tag", "v0.1.0", "--head-ref", "HEAD"),
        "0002",
        "v0.1.0",
    )


def test_pass_prints_ok_line(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade("pass"))
    result = _run(root, "--previous-tag", "v0.1.0", "--head-ref", "HEAD")
    _assert_ok(result)
    assert "v0.1.0" in result.stdout


# Tag selection, default resolution. Each case is built so that choosing the
# wrong tag flips the exit code.


def test_default_tag_ignores_newer_release_candidate(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    # The rc already lists 0002, so picking it would pass.
    _tag(root, "v0.2.0-rc.1")
    _write(root, "README.txt", "later\n")
    _commit(root, "after rc")
    _assert_violation(_run(root), "0002", "v0.1.0")


def test_default_tag_ignores_newer_tag_not_merged_into_head(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    _git(root, "checkout", "-q", "-b", "side", "v0.1.0")
    _write_kinds(root, HEAD_KINDS)
    _commit(root, "side release")
    _tag(root, "v0.3.0")
    _git(root, "checkout", "-q", "main")
    _assert_violation(_run(root), "0002", "v0.1.0")


def test_default_tag_sort_is_numeric_not_lexicographic(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _init(root)
    _write_kinds(root, BASE_KINDS)
    _write_revision(root, "0001", None, _upgrade("pass"))
    _write(root, CRUD, CRUD_REFERENCING_OLD_COL)
    _commit(root, "nine")
    _tag(root, "v0.9.0")
    # v0.10.0 already contains the contract revision. Lexicographic order would
    # pick v0.9.0 and fail.
    _write_kinds(root, HEAD_KINDS)
    _write_revision(root, "0002", "0001", _upgrade('op.drop_column("t", "old_col")'))
    _commit(root, "ten")
    _tag(root, "v0.10.0")
    _write(root, "README.txt", "later\n")
    _commit(root, "after ten")
    _assert_ok(_run(root))


def test_default_tag_skips_stable_tag_on_head_itself(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    # The release commit is HEAD. Comparing against its own tag would pass.
    _tag(root, "v0.2.0", annotated=True)
    _assert_violation(_run(root), "0002", "v0.1.0")


# Usage and git errors.


def test_previous_tag_without_head_ref_is_usage_error(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade("pass"))
    result = _run(root, "--previous-tag", "v0.1.0")
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_head_ref_without_previous_tag_is_usage_error(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade("pass"))
    result = _run(root, "--head-ref", "HEAD")
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_unknown_previous_tag_is_error(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade("pass"))
    result = _run(root, "--previous-tag", "v9.9.9", "--head-ref", "HEAD")
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_no_reachable_stable_tag_is_error(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade("pass"))
    _git(root, "tag", "-d", "v0.1.0")
    _tag(root, "v0.2.0-rc.1")
    result = _run(root)
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_repo_root_that_is_not_a_git_repo_is_error(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    result = _run(plain)
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_cli_smoke_subprocess(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_column("t", "old_col")'))
    completed = subprocess.run(
        [sys.executable, str(CHECKER), "--repo-root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1, (completed.stdout, completed.stderr)
    assert "Contract migration timing gate failed" in completed.stderr
    assert f"{CRUD}:3" in completed.stderr


# Real repository.


def _real_ref_exists(ref: str) -> bool:
    return (
        subprocess.run(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "rev-parse",
                "-q",
                "--verify",
                f"refs/tags/{ref}^{{commit}}",
            ],
            capture_output=True,
            check=False,
        ).returncode
        == 0
    )


def test_historical_control_v0_13_0_would_have_been_caught() -> None:
    if not (_real_ref_exists("v0.12.5") and _real_ref_exists("v0.13.0")):
        pytest.skip("release tags v0.12.5 and v0.13.0 are not fetched")
    result = run_script(
        CHECKER,
        "--repo-root",
        str(REPO_ROOT),
        "--previous-tag",
        "v0.12.5",
        "--head-ref",
        "v0.13.0",
    )
    _assert_violation(
        result,
        "0103",
        "github_repository_id",
        "apps/api/src/curie_api/",
        # 0103 upgrade() delegates to these helpers; the gate must follow them.
        "work_items",
        "thread_publication_lineages",
    )


def test_real_repo_default_run_passes() -> None:
    listed = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "tag", "--list", "--merged", "HEAD", "v*"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    if not any(re.fullmatch(r"v\d+\.\d+\.\d+", tag) for tag in listed):
        pytest.skip("no stable release tag is reachable from HEAD")
    result = run_script(CHECKER, "--repo-root", str(REPO_ROOT))
    _assert_ok(result)


# CI wiring.


def test_ci_runs_gate_after_schema_window_gate_with_full_history() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "ci.yaml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["python-static"]["steps"]
    names = [step.get("name") for step in steps]
    runs = [step.get("run") for step in steps]
    assert "Schema window gate" in names
    assert CHECK_COMMAND in runs
    assert runs.index(CHECK_COMMAND) > names.index("Schema window gate")
    checkout = next(
        step for step in steps if str(step.get("uses", "")).startswith("actions/checkout")
    )
    assert checkout["with"]["fetch-depth"] == 0


def test_preflight_registers_the_gate() -> None:
    text = (REPO_ROOT / "tools" / "preflight" / "preflight.py").read_text(encoding="utf-8")
    assert CHECK_COMMAND in text


# Reviewer-found gaps: shadowing, aliases, nested code, unpacking, neighbors.

UNUSED_SOURCES = {CRUD: "import os\n\nvalue = row.other\n"}


def test_local_assignment_shadowing_module_constant_is_nonliteral(tmp_path: Path) -> None:
    body = 'COL = "unused_col"\n\n\n' + _upgrade('COL = "old_col"', 'op.drop_column("t", COL)')
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "literal")


def test_helper_local_assignment_shadowing_module_constant_is_nonliteral(
    tmp_path: Path,
) -> None:
    body = (
        'COL = "unused_col"\n\n\n'
        "def _drop() -> None:\n"
        '    COL = "old_col"\n'
        '    op.drop_column("t", COL)\n\n\n' + _upgrade("_drop()")
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "literal")


def test_helper_parameter_named_like_module_constant_is_nonliteral(tmp_path: Path) -> None:
    body = (
        'COL = "unused_col"\n\n\n'
        "def _drop(COL) -> None:\n"
        '    op.drop_column("t", COL)\n\n\n' + _upgrade('_drop("old_col")')
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "literal")


def test_comprehension_variable_shadowing_constant_is_nonliteral(tmp_path: Path) -> None:
    body = 'COL = "unused_col"\n\n\n' + _upgrade(
        '[op.drop_column("t", COL) for COL in ("old_col",)]'
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "literal")


def test_module_alias_of_helper_is_reachable(tmp_path: Path) -> None:
    body = (
        "def _drop() -> None:\n"
        '    op.drop_column("t", "old_col")\n\n\n'
        "_alias = _drop\n\n\n" + _upgrade("_alias()")
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_helper_passed_as_callback_is_reachable(tmp_path: Path) -> None:
    body = (
        "def _drop() -> None:\n"
        '    op.drop_column("t", "old_col")\n\n\n'
        "def _run(f) -> None:\n"
        "    f()\n\n\n" + _upgrade("_run(_drop)")
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_nested_function_invoked_in_upgrade_is_found(tmp_path: Path) -> None:
    body = _upgrade(
        "def inner() -> None:",
        '    op.drop_column("t", "old_col")',
        "inner()",
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_immediately_invoked_lambda_is_found(tmp_path: Path) -> None:
    body = _upgrade('(lambda: op.drop_column("t", "old_col"))()')
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_unpacked_keyword_arguments_are_nonliteral(tmp_path: Path) -> None:
    body = _upgrade('op.alter_column("t", "old_col", **{"new_column_name": "new_col"})')
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "literal")


def test_unpacked_positional_arguments_fail(tmp_path: Path) -> None:
    body = _upgrade('op.drop_column(*("t", "old_col"))')
    root, _ = _scenario(tmp_path, body)
    result = _run(root)
    assert result.returncode == 1, (result.stdout, result.stderr)
    assert "Contract migration timing gate failed" in result.stderr


def test_drop_table_of_unreferenced_table_passes(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _upgrade('op.drop_table("widgets")'), sources=UNUSED_SOURCES)
    _assert_ok(_run(root))


def test_rename_of_unreferenced_column_passes(tmp_path: Path) -> None:
    root, _ = _scenario(
        tmp_path,
        _upgrade('op.alter_column("t", "old_col", new_column_name="new_col")'),
        sources=UNUSED_SOURCES,
    )
    _assert_ok(_run(root))


def test_batch_drop_of_unreferenced_column_passes(tmp_path: Path) -> None:
    body = _upgrade(
        'with op.batch_alter_table("t") as batch_op:',
        '    batch_op.drop_column("old_col")',
    )
    root, _ = _scenario(tmp_path, body, sources=UNUSED_SOURCES)
    _assert_ok(_run(root))


def test_constant_drop_of_unreferenced_column_passes(tmp_path: Path) -> None:
    body = 'COL = "old_col"\n\n\n' + _upgrade('op.drop_column("t", COL)')
    root, _ = _scenario(tmp_path, body, sources=UNUSED_SOURCES)
    _assert_ok(_run(root))


def test_constant_with_bare_annotation_still_resolves_unreferenced_passes(
    tmp_path: Path,
) -> None:
    body = 'COL = "old_col"\nCOL: str\n\n\n' + _upgrade('op.drop_column("t", COL)')
    root, _ = _scenario(tmp_path, body, sources=UNUSED_SOURCES)
    _assert_ok(_run(root))


def test_constant_with_bare_annotation_still_resolves_referenced_fails(
    tmp_path: Path,
) -> None:
    body = 'COL = "old_col"\nCOL: str\n\n\n' + _upgrade('op.drop_column("t", COL)')
    root, _ = _scenario(tmp_path, body)
    result = _run(root)
    _assert_violation(result, "0002", "old_col", f"{CRUD}:3")
    assert "literal" not in result.stderr


def test_downgrade_helper_alias_does_not_make_it_reachable_from_upgrade(
    tmp_path: Path,
) -> None:
    body = (
        "def _undo() -> None:\n"
        '    op.drop_column("t", "old_col")\n\n\n'
        "def upgrade() -> None:\n"
        "    pass\n\n\n"
        "def downgrade() -> None:\n"
        "    alias = _undo\n"
        "    alias()\n"
    )
    root, _ = _scenario(tmp_path, body)
    _assert_ok(_run(root))


def test_global_declared_helper_is_still_reachable(tmp_path: Path) -> None:
    body = (
        "def _drop() -> None:\n"
        '    op.drop_column("t", "old_col")\n\n\n'
        "def upgrade() -> None:\n"
        "    global _drop\n"
        "    _drop()\n\n\n"
        "def downgrade() -> None:\n"
        "    pass\n"
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_comprehension_binding_does_not_hide_helper(tmp_path: Path) -> None:
    body = 'def _drop() -> None:\n    op.drop_column("t", "old_col")\n\n\n' + _upgrade(
        "[_drop for _drop in ()]", "_drop()"
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def test_alias_captured_before_redefinition_is_reachable(tmp_path: Path) -> None:
    body = (
        "def _drop() -> None:\n"
        '    op.drop_column("t", "old_col")\n\n\n'
        "_alias = _drop\n\n\n"
        "def _drop() -> None:\n"
        "    pass\n\n\n" + _upgrade("_alias()")
    )
    root, _ = _scenario(tmp_path, body)
    _assert_violation(_run(root), "0002", "old_col", f"{CRUD}:3")


def _global_constant_body() -> str:
    return (
        'COL = "old_col"\n\n\n'
        "def upgrade() -> None:\n"
        "    global COL\n"
        '    op.drop_column("t", COL)\n\n\n'
        "def downgrade() -> None:\n"
        "    pass\n"
    )


def test_global_declared_constant_still_resolves_referenced_fails(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _global_constant_body())
    result = _run(root)
    _assert_violation(result, "0002", "old_col", f"{CRUD}:3")
    assert "literal" not in result.stderr


def test_global_declared_constant_still_resolves_unreferenced_passes(tmp_path: Path) -> None:
    root, _ = _scenario(tmp_path, _global_constant_body(), sources=UNUSED_SOURCES)
    _assert_ok(_run(root))
