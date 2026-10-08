"""Security-boundary tests for trusted publication snapshot validation."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from curie_worker.config import WorkerConfig
from curie_worker.runner_client import RunnerWorkspaceSnapshot
from pydantic import ValidationError


def test_publication_git_environment_drops_ambient_credentials_and_config(
    monkeypatch: Any, tmp_path: Path
) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    monkeypatch.setenv("GITHUB_TOKEN", "ambient-secret")
    monkeypatch.setenv("GH_TOKEN", "ambient-secret")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "http.extraHeader")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "Authorization: ambient-secret")

    env = validation.publication_git_environment(tmp_path)

    assert "GITHUB_TOKEN" not in env
    assert "GH_TOKEN" not in env
    assert "GIT_CONFIG_COUNT" not in env
    assert "GIT_CONFIG_KEY_0" not in env
    assert "GIT_CONFIG_VALUE_0" not in env
    assert env["HOME"] == str(tmp_path)
    assert env["GIT_CONFIG_GLOBAL"] == os.devnull
    assert env["GIT_CONFIG_SYSTEM"] == os.devnull
    assert env["GIT_TERMINAL_PROMPT"] == "0"


def test_publication_validation_refuses_workflow_changes() -> None:
    validation = importlib.import_module("curie_worker.publication_validation")

    assert not validation._safe_changed_path(".github/workflows/publish.yml", ())
    assert not validation._safe_changed_path(".GIT/config", ())
    assert not validation._safe_changed_path(".github/actions/build/action.yml", ())
    assert not validation._safe_changed_path(".github/CODEOWNERS", ())
    assert validation._safe_changed_path("src/main.py", ())
    assert not validation._safe_changed_path("scripts/release.sh", ("scripts/release.sh",))
    assert validation._safe_changed_path("scripts/release.sh.bak", ("scripts/release.sh",))


def test_derived_workflow_path_cannot_hide_behind_a_safe_declared_path(
    tmp_path: Path,
) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    repo = tmp_path / "patch-source"
    repo.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
    subprocess.run(
        ["git", "config", "user.email", "publisher@example.test"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "Publisher"], cwd=repo, check=True)
    (repo / "README.md").write_text("base\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "base"], cwd=repo, check=True)
    workflow = repo / ".github" / "workflows" / "ci.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text("name: unsafe\n")
    patch_result = subprocess.run(
        [
            "git",
            "diff",
            "--binary",
            "--no-index",
            "/dev/null",
            ".github/workflows/ci.yml",
        ],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    assert patch_result.returncode == 1
    patch = patch_result.stdout

    archive_buffer = BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
        archive.add(repo / "README.md", arcname="README.md")
    archive_bytes = archive_buffer.getvalue()
    coordinator = SimpleNamespace(
        preparer=SimpleNamespace(limits=SimpleNamespace(max_archive_bytes=len(archive_bytes) + 1)),
        current=lambda _thread: SimpleNamespace(
            repo_full_name="acme-corp/acme-bot",
            base_sha="a" * 40,
        ),
        stream_current_base=lambda _thread: [archive_bytes],
    )
    snapshot = RunnerWorkspaceSnapshot(
        repo_full_name="acme-corp/acme-bot",
        base_sha="a" * 40,
        patch=patch,
        changed_paths=("src/main.py",),
        contains_workflow_files=False,
        publication_title="Update source",
        publication_body="Approved platform publication.",
    )

    with pytest.raises(
        validation.WorkspacePreparationError,
        match="GitHub workflow changes cannot be published",
    ):
        validation.validate_snapshot_against_base(
            coordinator,
            thread_key="1700000000.000100",
            snapshot=snapshot,
            scratch_root=tmp_path,
            protected_paths=(),
            allow_dependency_additions=False,
        )


def test_empty_snapshot_requires_matching_base_and_empty_patch() -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    prepared = SimpleNamespace(repo_full_name="acme-corp/acme-bot", base_sha="a" * 40)
    coordinator = SimpleNamespace(current=lambda _thread: prepared)
    snapshot = RunnerWorkspaceSnapshot(
        repo_full_name=prepared.repo_full_name,
        base_sha=prepared.base_sha,
        patch=b"",
        changed_paths=(),
        contains_workflow_files=False,
        publication_title="Correct the pull request",
        publication_body="Correct the body for CI.",
    )
    validation.validate_snapshot_against_base(
        coordinator,
        thread_key="example-thread",
        snapshot=snapshot,
        protected_paths=(),
        allow_dependency_additions=False,
    )
    for changed in (
        {"patch": b"diff --git a/a b/a\n"},
        {"base_sha": "b" * 40},
    ):
        with pytest.raises(validation.WorkspacePreparationError):
            validation.validate_snapshot_against_base(
                coordinator,
                thread_key="example-thread",
                snapshot=RunnerWorkspaceSnapshot(**{**snapshot.__dict__, **changed}),
                protected_paths=(),
                allow_dependency_additions=False,
            )


def test_base_mismatch_names_both_commits_by_twelve_characters() -> None:
    # #4121: the refusal names both commits so the factory run is actionable.
    validation = importlib.import_module("curie_worker.publication_validation")
    snapshot_sha = "0123456789abcdef0123456789abcdef01234567"
    prepared_sha = "fedcba9876543210fedcba9876543210fedcba98"
    prepared = SimpleNamespace(repo_full_name="acme-corp/acme-bot", base_sha=prepared_sha)
    coordinator = SimpleNamespace(current=lambda _thread: prepared)
    snapshot = RunnerWorkspaceSnapshot(
        repo_full_name=prepared.repo_full_name,
        base_sha=snapshot_sha,
        patch=b"",
        changed_paths=(),
        contains_workflow_files=False,
        publication_title="Correct the pull request",
        publication_body="Correct the body for CI.",
    )

    with pytest.raises(validation.WorkspacePreparationError) as raised:
        validation.validate_snapshot_against_base(
            coordinator,
            thread_key="example-thread",
            snapshot=snapshot,
            protected_paths=(),
            allow_dependency_additions=False,
        )

    message = str(raised.value)
    assert snapshot_sha[:12] in message
    assert prepared_sha[:12] in message
    assert snapshot_sha not in message
    assert prepared_sha not in message
    assert "snapshot commit 0123456789ab does not match sanitized base fedcba987654" in message


def _prepared_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "patch-source"
    repo.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
    subprocess.run(
        ["git", "config", "user.email", "publisher@example.test"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "Publisher"], cwd=repo, check=True)
    (repo / "README.md").write_text("base\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "base"], cwd=repo, check=True)
    return repo


def _archive_readme(repo: Path) -> bytes:
    archive_buffer = BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
        archive.add(repo / "README.md", arcname="README.md")
    return archive_buffer.getvalue()


def _coordinator(archive_bytes: bytes) -> SimpleNamespace:
    return SimpleNamespace(
        preparer=SimpleNamespace(limits=SimpleNamespace(max_archive_bytes=len(archive_bytes) + 1)),
        current=lambda _thread: SimpleNamespace(
            repo_full_name="acme-corp/acme-bot",
            base_sha="a" * 40,
        ),
        stream_current_base=lambda _thread: [archive_bytes],
    )


def _snapshot(paths: tuple[str, ...], patch: bytes) -> RunnerWorkspaceSnapshot:
    return RunnerWorkspaceSnapshot(
        repo_full_name="acme-corp/acme-bot",
        base_sha="a" * 40,
        patch=patch,
        changed_paths=paths,
        contains_workflow_files=False,
        publication_title="Update source",
        publication_body="Approved platform publication.",
    )


def _cached_patch(repo: Path) -> bytes:
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    return subprocess.run(
        ["git", "diff", "--cached", "--binary", "--no-renames"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout


def test_publication_refuses_github_actions_and_codeowners(tmp_path: Path) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    repo = _prepared_repo(tmp_path)
    action = repo / ".github" / "actions" / "build" / "action.yml"
    action.parent.mkdir(parents=True)
    action.write_text("name: build\n")
    (repo / ".github" / "CODEOWNERS").write_text("* @acme\n")
    patch = _cached_patch(repo)

    with pytest.raises(
        validation.WorkspacePreparationError,
        match="GitHub metadata changes cannot be published",
    ):
        validation.validate_snapshot_against_base(
            _coordinator(_archive_readme(repo)),
            thread_key="1700000000.000100",
            snapshot=_snapshot(
                (".github/actions/build/action.yml", ".github/CODEOWNERS"),
                patch,
            ),
            scratch_root=tmp_path,
            protected_paths=(),
            allow_dependency_additions=False,
        )


def test_publication_refuses_operator_protected_paths(tmp_path: Path) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    repo = _prepared_repo(tmp_path)
    script = repo / "scripts" / "release.sh"
    script.parent.mkdir()
    script.write_text("echo release\n")
    patch = _cached_patch(repo)

    with pytest.raises(
        validation.WorkspacePreparationError,
        match="operator-protected path changes cannot be published",
    ):
        validation.validate_snapshot_against_base(
            _coordinator(_archive_readme(repo)),
            thread_key="1700000000.000100",
            snapshot=_snapshot(("scripts/release.sh",), patch),
            scratch_root=tmp_path,
            protected_paths=("scripts",),
            allow_dependency_additions=False,
        )


def test_publication_accepts_a_readme_outside_protected_paths(tmp_path: Path) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    repo = _prepared_repo(tmp_path)
    archive = _archive_readme(repo)
    (repo / "README.md").write_text("changed\n")
    neighbor = repo / "scripts" / "release.sh.bak"
    neighbor.parent.mkdir()
    neighbor.write_text("echo not protected\n")
    readme_patch = subprocess.run(
        ["git", "diff", "--binary", "--no-renames"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout
    neighbor_patch = subprocess.run(
        ["git", "diff", "--binary", "--no-index", "/dev/null", "scripts/release.sh.bak"],
        cwd=repo,
        check=False,
        capture_output=True,
    )
    assert neighbor_patch.returncode == 1
    validation.validate_snapshot_against_base(
        _coordinator(archive),
        thread_key="1700000000.000100",
        snapshot=_snapshot(("README.md",), readme_patch),
        scratch_root=tmp_path,
        protected_paths=("scripts/release.sh",),
        allow_dependency_additions=False,
    )
    validation.validate_snapshot_against_base(
        _coordinator(archive),
        thread_key="1700000000.000100",
        snapshot=_snapshot(("scripts/release.sh.bak",), neighbor_patch.stdout),
        scratch_root=tmp_path,
        protected_paths=("scripts/release.sh",),
        allow_dependency_additions=False,
    )


def test_hidden_github_metadata_is_refused_by_name(tmp_path: Path) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    repo = _prepared_repo(tmp_path)
    action = repo / ".GITHUB" / "actions" / "build" / "action.yml"
    action.parent.mkdir(parents=True)
    action.write_text("name: build\n")
    patch = _cached_patch(repo)

    with pytest.raises(
        validation.WorkspacePreparationError,
        match="GitHub metadata changes cannot be published",
    ):
        validation.validate_snapshot_against_base(
            _coordinator(_archive_readme(repo)),
            thread_key="1700000000.000100",
            snapshot=_snapshot(("README.md",), patch),
            scratch_root=tmp_path,
            protected_paths=(),
            allow_dependency_additions=False,
        )


def test_worker_config_reads_repository_relative_protected_paths(monkeypatch: Any) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    monkeypatch.delenv("CURIE_PUBLICATION_PROTECTED_PATHS", raising=False)
    assert WorkerConfig().publication_protected_paths == ()
    monkeypatch.setenv(
        "CURIE_PUBLICATION_PROTECTED_PATHS",
        '["scripts/release.sh", "build", "scripts/release,prod.sh"]',
    )
    assert WorkerConfig().publication_protected_paths == (
        "scripts/release.sh",
        "build",
        "scripts/release,prod.sh",
    )
    assert (
        validation.publication_path_refusal(
            "scripts/release,prod.sh",
            WorkerConfig().publication_protected_paths,
        )
        == validation.OPERATOR_PROTECTED_REFUSAL
    )
    monkeypatch.setenv("CURIE_PUBLICATION_PROTECTED_PATHS", "scripts/release.sh, build")
    with pytest.raises(ValidationError, match="JSON array"):
        WorkerConfig()
    monkeypatch.setenv("CURIE_PUBLICATION_PROTECTED_PATHS", '["scripts/../secret"]')
    with pytest.raises(ValidationError, match="repository-relative"):
        WorkerConfig()


_DEPENDENCY_FILES: dict[str, tuple[str, str]] = {
    "Cargo.toml": (
        '[package]\nname = "acme-tool"\nversion = "1.0.0"\n[dependencies]\nacme-base = "1.0.0"\n',
        '[package]\nname = "acme-tool"\nversion = "1.0.0"\n'
        '[dependencies]\nacme-base = "1.0.0"\nacme-new = "1.0.0"\n',
    ),
    "Cargo.lock": (
        'version = 4\n[[package]]\nname = "acme-base"\nversion = "1.0.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n',
        'version = 4\n[[package]]\nname = "acme-base"\nversion = "1.0.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        '[[package]]\nname = "acme-new"\nversion = "1.0.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n',
    ),
    "pyproject.toml": (
        '[project]\nname = "acme-tool"\nversion = "1.0.0"\ndependencies = ["acme-base>=1.0.0"]\n',
        '[project]\nname = "acme-tool"\nversion = "1.0.0"\n'
        'dependencies = ["acme-base>=1.0.0", "acme-new>=1.0.0"]\n',
    ),
    "uv.lock": (
        'version = 1\n[[package]]\nname = "acme-base"\nversion = "1.0.0"\n'
        'source = { registry = "https://pypi.org/simple" }\n',
        'version = 1\n[[package]]\nname = "acme-base"\nversion = "1.0.0"\n'
        'source = { registry = "https://pypi.org/simple" }\n'
        '[[package]]\nname = "acme-new"\nversion = "1.0.0"\n'
        'source = { registry = "https://pypi.org/simple" }\n',
    ),
    "package.json": (
        '{"name":"acme-tool","dependencies":{"acme-base":"1.0.0"}}\n',
        '{"name":"acme-tool","dependencies":{"acme-base":"1.0.0","acme-new":"1.0.0"}}\n',
    ),
    "pnpm-lock.yaml": (
        "lockfileVersion: '9.0'\npackages:\n"
        "  acme-base@1.0.0:\n    resolution: {integrity: sha512-base}\n",
        "lockfileVersion: '9.0'\npackages:\n"
        "  acme-base@1.0.0:\n    resolution: {integrity: sha512-base}\n"
        "  acme-new@1.0.0:\n    resolution: {integrity: sha512-new}\n",
    ),
    "package-lock.json": (
        '{"name":"acme-tool","lockfileVersion":3,"packages":'
        '{"": {"name":"acme-tool"},"node_modules/acme-base":'
        '{"version":"1.0.0","resolved":"https://registry.npmjs.org/acme-base/"}}}\n',
        '{"name":"acme-tool","lockfileVersion":3,"packages":'
        '{"": {"name":"acme-tool"},"node_modules/acme-base":'
        '{"version":"1.0.0","resolved":"https://registry.npmjs.org/acme-base/"},'
        '"node_modules/acme-new":'
        '{"version":"1.0.0","resolved":"https://registry.npmjs.org/acme-new/"}}}\n',
    ),
}


def _dependency_snapshot(
    tmp_path: Path,
    before: dict[str, str],
    after: dict[str, str | None],
) -> tuple[SimpleNamespace, RunnerWorkspaceSnapshot]:
    """Archive a committed base and generate the actual candidate Git patch."""

    repo = _prepared_repo(tmp_path)
    for path, contents in before.items():
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents, encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "dependency base"],
        cwd=repo,
        check=True,
    )
    archive = subprocess.run(
        ["git", "archive", "--format=tar.gz", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout
    for path, contents in after.items():
        target = repo / path
        if contents is None:
            target.unlink()
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(contents, encoding="utf-8")
    patch = _cached_patch(repo)
    paths = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--no-renames"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    assert patch and paths, "candidate must contain a real file change"
    return _coordinator(archive), _snapshot(tuple(paths), patch)


def _validate_dependency_snapshot(
    tmp_path: Path,
    before: dict[str, str],
    after: dict[str, str | None],
    *,
    allow_dependency_additions: bool = False,
    protected_paths: tuple[str, ...] = (),
) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    coordinator, snapshot = _dependency_snapshot(tmp_path, before, after)
    try:
        validation.validate_snapshot_against_base(
            coordinator,
            thread_key="example-dependency-thread",
            snapshot=snapshot,
            scratch_root=tmp_path,
            protected_paths=protected_paths,
            allow_dependency_additions=allow_dependency_additions,
        )
    finally:
        assert not list(tmp_path.glob("publication-validate-*")), "validator leaked its checkout"


def _assert_dependency_refusal(
    tmp_path: Path,
    before: dict[str, str],
    after: dict[str, str | None],
    path: str,
) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    with pytest.raises(validation.WorkspacePreparationError) as raised:
        _validate_dependency_snapshot(tmp_path, before, after)
    detail = str(raised.value)
    assert validation.DEPENDENCY_ADDITION_REFUSAL == (
        "dependency additions cannot be published by this capability"
    )
    assert validation.DEPENDENCY_ADDITION_REFUSAL in detail
    assert path in detail
    assert "acme-new" not in detail, "untrusted dependency names must not reach refusal details"


@pytest.mark.parametrize(
    ("manifest", "lockfile"),
    [
        ("Cargo.toml", "Cargo.lock"),
        ("pyproject.toml", "uv.lock"),
        ("package.json", "pnpm-lock.yaml"),
        ("package.json", "package-lock.json"),
    ],
    ids=["cargo", "uv", "pnpm", "npm"],
)
def test_nested_dependency_manifest_and_lockfile_addition_is_refused(
    tmp_path: Path, manifest: str, lockfile: str
) -> None:
    before = {
        f"crates/acme-tool/{name}": _DEPENDENCY_FILES[name][0] for name in (manifest, lockfile)
    }
    after = {
        f"crates/acme-tool/{name}": _DEPENDENCY_FILES[name][1] for name in (manifest, lockfile)
    }
    _assert_dependency_refusal(tmp_path, before, after, f"crates/acme-tool/{manifest}")


@pytest.mark.parametrize("basename", _DEPENDENCY_FILES)
@pytest.mark.parametrize("prefix", ["", "packages/nested/acme-tool/"])
@pytest.mark.parametrize("new_file", [False, True], ids=["existing", "new-file"])
def test_dependency_basenames_are_checked_at_every_depth(
    tmp_path: Path, basename: str, prefix: str, new_file: bool
) -> None:
    path = prefix + basename
    before = {} if new_file else {path: _DEPENDENCY_FILES[basename][0]}
    _assert_dependency_refusal(tmp_path, before, {path: _DEPENDENCY_FILES[basename][1]}, path)


@pytest.mark.parametrize(
    ("basename", "contents"),
    [
        ("pyproject.toml", '[project]\ndependencies = ["acme-new>=1"]\n'),
        ("pyproject.toml", '[project.optional-dependencies]\nextra = ["acme-new>=1"]\n'),
        ("pyproject.toml", '[dependency-groups]\ntest = ["acme-new>=1"]\n'),
        ("pyproject.toml", '[tool.uv]\ndev-dependencies = ["acme-new>=1"]\n'),
        ("pyproject.toml", '[build-system]\nrequires = ["acme-new>=1"]\n'),
        ("Cargo.toml", '[dependencies]\nacme-new = "1"\n'),
        ("Cargo.toml", '[dev-dependencies]\nacme-new = "1"\n'),
        ("Cargo.toml", '[build-dependencies]\nacme-new = "1"\n'),
        ("Cargo.toml", "[target.'cfg(unix)'.dependencies]\nacme-new = \"1\"\n"),
        ("Cargo.toml", "[target.'cfg(unix)'.dev-dependencies]\nacme-new = \"1\"\n"),
        ("Cargo.toml", "[target.'cfg(unix)'.build-dependencies]\nacme-new = \"1\"\n"),
        ("Cargo.toml", '[workspace.dependencies]\nacme-new = "1"\n'),
        ("package.json", '{"dependencies":{"acme-new":"1"}}\n'),
        ("package.json", '{"devDependencies":{"acme-new":"1"}}\n'),
        ("package.json", '{"optionalDependencies":{"acme-new":"1"}}\n'),
        ("package.json", '{"peerDependencies":{"acme-new":"1"}}\n'),
    ],
    ids=[
        "python-runtime",
        "python-extra",
        "python-group",
        "python-uv-dev",
        "python-build",
        "cargo-runtime",
        "cargo-dev",
        "cargo-build",
        "cargo-target-runtime",
        "cargo-target-dev",
        "cargo-target-build",
        "cargo-workspace",
        "js-runtime",
        "js-dev",
        "js-optional",
        "js-peer",
    ],
)
def test_each_dependency_manifest_section_refuses_a_third_party_entry(
    tmp_path: Path, basename: str, contents: str
) -> None:
    path = f"nested/{basename}"
    _assert_dependency_refusal(tmp_path, {}, {path: contents}, path)


@pytest.mark.parametrize("basename", _DEPENDENCY_FILES)
def test_existing_dependency_version_changes_publish(tmp_path: Path, basename: str) -> None:
    path = f"nested/{basename}"
    before = _DEPENDENCY_FILES[basename][0]
    _validate_dependency_snapshot(
        tmp_path, {path: before}, {path: before.replace("1.0.0", "2.0.0")}
    )


@pytest.mark.parametrize("basename", _DEPENDENCY_FILES)
def test_dependency_manifest_metadata_edits_publish(tmp_path: Path, basename: str) -> None:
    path = f"nested/{basename}"
    before = _DEPENDENCY_FILES[basename][0]
    if basename.endswith(".json"):
        parsed = json.loads(before)
        parsed["description"] = "An updated description"
        after = json.dumps(parsed) + "\n"
    else:
        after = before + "# An updated description\n"
    _validate_dependency_snapshot(tmp_path, {path: before}, {path: after})


@pytest.mark.parametrize("basename", _DEPENDENCY_FILES)
def test_removing_a_dependency_file_publishes(tmp_path: Path, basename: str) -> None:
    path = f"nested/{basename}"
    _validate_dependency_snapshot(tmp_path, {path: _DEPENDENCY_FILES[basename][0]}, {path: None})


@pytest.mark.parametrize(
    "basename", ["uv.lock", "Cargo.lock", "pnpm-lock.yaml", "package-lock.json"]
)
def test_transitive_dependency_lockfile_addition_is_refused(tmp_path: Path, basename: str) -> None:
    path = f"nested/{basename}"
    _assert_dependency_refusal(
        tmp_path,
        {path: _DEPENDENCY_FILES[basename][0]},
        {path: _DEPENDENCY_FILES[basename][1]},
        path,
    )


@pytest.mark.parametrize(
    ("basename", "contents"),
    [
        ("Cargo.toml", '[dependencies]\nacme-local = { path = "../acme-local" }\n'),
        ("Cargo.toml", "[dependencies]\nacme-local = { workspace = true }\n"),
        ("Cargo.lock", 'version = 4\n[[package]]\nname = "acme-local"\nversion = "1"\n'),
        ("uv.lock", '[[package]]\nname = "acme-local"\nsource = { editable = "." }\n'),
        ("uv.lock", '[[package]]\nname = "acme-local"\nsource = { virtual = "." }\n'),
        ("uv.lock", '[[package]]\nname = "acme-local"\nsource = { directory = "../local" }\n'),
        ("uv.lock", '[[package]]\nname = "acme-local"\nsource = { path = "../local" }\n'),
        ("uv.lock", '[[package]]\nname = "acme-local"\nsource = { workspace = "." }\n'),
        ("package.json", '{"dependencies":{"acme-local":"link:../local"}}\n'),
        ("package.json", '{"devDependencies":{"acme-local":"file:../local"}}\n'),
        ("package.json", '{"optionalDependencies":{"acme-local":"workspace:*"}}\n'),
        (
            "package-lock.json",
            '{"packages":{"": {"name":"acme-tool"},'
            '"node_modules/acme-local":{"resolved":"packages/local","link":true},'
            '"packages/local":{"name":"acme-local","version":"1"}}}\n',
        ),
        (
            "package-lock.json",
            '{"packages":{"node_modules/acme-local":{"resolved":"file:../local"}}}\n',
        ),
        (
            "pnpm-lock.yaml",
            "packages:\n  acme-local@file:../local:\n    resolution: {directory: ../local, type: directory}\n",
        ),
        ("pnpm-lock.yaml", "packages:\n  acme-local@link:../local: {}\n"),
        ("pnpm-lock.yaml", "packages:\n  acme-local@workspace:*: {}\n"),
        (
            "pyproject.toml",
            '[project]\ndependencies = ["acme-local"]\n'
            "[tool.uv.sources]\nacme-local = { workspace = true }\n",
        ),
        (
            "pyproject.toml",
            '[project]\ndependencies = ["acme-local"]\n'
            '[tool.uv.sources]\nacme-local = { path = "../local", editable = true }\n',
        ),
        ("pyproject.toml", '[project]\ndependencies = ["acme-local @ file:///tmp/acme-local"]\n'),
    ],
)
def test_new_internal_dependency_sources_publish(
    tmp_path: Path, basename: str, contents: str
) -> None:
    _validate_dependency_snapshot(tmp_path, {}, {f"nested/{basename}": contents})


def test_python_dependency_names_follow_pep503_normalization(tmp_path: Path) -> None:
    path = "nested/pyproject.toml"
    _validate_dependency_snapshot(
        tmp_path,
        {path: '[project]\ndependencies = ["Acme_Base>=1"]\n'},
        {path: '[project]\ndependencies = ["acme.base>=2", "acme-base[extra]>=2"]\n'},
    )


@pytest.mark.parametrize("rename_only", [False, True], ids=["new-package", "alias-only"])
def test_cargo_dependency_identity_uses_the_renamed_package(
    tmp_path: Path, rename_only: bool
) -> None:
    path = "nested/Cargo.toml"
    before = '[dependencies]\nold-alias = { package = "acme-base", version = "1" }\n'
    package = "acme-base" if rename_only else "acme-new"
    after = f'[dependencies]\nnew-alias = {{ package = "{package}", version = "2" }}\n'
    if rename_only:
        _validate_dependency_snapshot(tmp_path, {path: before}, {path: after})
    else:
        _assert_dependency_refusal(tmp_path, {path: before}, {path: after}, path)


@pytest.mark.parametrize(
    ("basename", "contents"),
    [
        (
            "Cargo.toml",
            '[dependencies]\nacme-new = { git = "https://example.com/acme.git" }\n',
        ),
        (
            "Cargo.lock",
            '[[package]]\nname = "acme-new"\nversion = "1"\n'
            'source = "git+https://example.com/acme.git#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"\n',
        ),
        (
            "pyproject.toml",
            '[project]\ndependencies = ["acme-new @ git+https://example.com/acme.git"]\n',
        ),
        (
            "uv.lock",
            '[[package]]\nname = "acme-new"\nsource = { git = "https://example.com/acme.git" }\n',
        ),
        ("package.json", '{"dependencies":{"acme-new":"git+https://example.com/acme.git"}}\n'),
        (
            "package-lock.json",
            '{"packages":{"node_modules/acme-new":'
            '{"version":"1","resolved":"git+https://example.com/acme.git"}}}\n',
        ),
        (
            "pnpm-lock.yaml",
            "packages:\n  acme-new@1.0.0:\n"
            "    resolution: {type: git, repo: 'https://example.com/acme.git', commit: '0123456'}\n",
        ),
    ],
)
def test_git_dependency_sources_are_third_party(
    tmp_path: Path, basename: str, contents: str
) -> None:
    path = f"nested/{basename}"
    _assert_dependency_refusal(tmp_path, {}, {path: contents}, path)


@pytest.mark.parametrize(
    ("basename", "malformed"),
    [
        ("pyproject.toml", "[project\ndependencies = [\n"),
        ("uv.lock", "[[package]\nname = [\n"),
        ("Cargo.toml", "[dependencies\nacme-new = [\n"),
        ("Cargo.lock", "[[package]\nname = [\n"),
        ("package.json", '{"dependencies":'),
        ("package-lock.json", '{"packages":'),
        ("pnpm-lock.yaml", "packages: [\n"),
        ("pyproject.toml", '[project]\ndependencies = "acme-new"\n'),
        ("uv.lock", '[[package]]\nname = ["acme-new"]\n'),
        ("Cargo.toml", 'dependencies = ["acme-new"]\n'),
        ("Cargo.lock", '[[package]]\nname = ["acme-new"]\n'),
        ("package.json", '{"dependencies":["acme-new"]}\n'),
        ("package-lock.json", '{"packages":["acme-new"]}\n'),
        ("pnpm-lock.yaml", "packages: [acme-new]\n"),
        ("pnpm-lock.yaml", "packages: !!python/object/apply:os.system [acme-new]\n"),
    ],
)
def test_malformed_dependency_files_fail_closed(
    tmp_path: Path, basename: str, malformed: str
) -> None:
    path = f"nested/{basename}"
    _assert_dependency_refusal(
        tmp_path,
        {path: _DEPENDENCY_FILES[basename][0]},
        {path: malformed},
        path,
    )


def test_allow_dependency_additions_publishes_nested_cargo_patch(tmp_path: Path) -> None:
    paths = ("crates/acme-tool/Cargo.toml", "crates/acme-tool/Cargo.lock")
    _validate_dependency_snapshot(
        tmp_path,
        {path: _DEPENDENCY_FILES[Path(path).name][0] for path in paths},
        {path: _DEPENDENCY_FILES[Path(path).name][1] for path in paths},
        allow_dependency_additions=True,
    )


@pytest.mark.parametrize(
    ("guarded_path", "protected_paths", "reason"),
    [
        (".github/workflows/ci.yml", (), "GitHub workflow changes cannot be published"),
        (".github/CODEOWNERS", (), "GitHub metadata changes cannot be published"),
        (
            "scripts/release.sh",
            ("scripts",),
            "operator-protected path changes cannot be published",
        ),
        (
            "nested/Cargo.toml",
            ("nested/Cargo.toml",),
            "operator-protected path changes cannot be published",
        ),
    ],
)
def test_allow_dependency_additions_preserves_existing_publication_guards(
    tmp_path: Path,
    guarded_path: str,
    protected_paths: tuple[str, ...],
    reason: str,
) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    after = {"nested/Cargo.toml": _DEPENDENCY_FILES["Cargo.toml"][1]}
    after.setdefault(guarded_path, "guarded content\n")
    with pytest.raises(validation.WorkspacePreparationError, match=reason):
        _validate_dependency_snapshot(
            tmp_path,
            {},
            after,
            allow_dependency_additions=True,
            protected_paths=protected_paths,
        )


def test_dependency_check_uses_paths_derived_from_the_patch(tmp_path: Path) -> None:
    validation = importlib.import_module("curie_worker.publication_validation")
    path = "nested/Cargo.toml"
    coordinator, snapshot = _dependency_snapshot(
        tmp_path, {}, {path: _DEPENDENCY_FILES["Cargo.toml"][1]}
    )
    hidden_snapshot = RunnerWorkspaceSnapshot(
        **{**snapshot.__dict__, "changed_paths": ("README.md",)}
    )
    with pytest.raises(validation.WorkspacePreparationError):
        validation.validate_snapshot_against_base(
            coordinator,
            thread_key="example-dependency-thread",
            snapshot=hidden_snapshot,
            scratch_root=tmp_path,
            protected_paths=(),
            allow_dependency_additions=False,
        )


def test_worker_config_reads_dependency_addition_opt_in(monkeypatch: Any) -> None:
    monkeypatch.delenv("CURIE_PUBLICATION_ALLOW_DEPENDENCY_ADDITIONS", raising=False)
    assert WorkerConfig().publication_allow_dependency_additions is False
    monkeypatch.setenv("CURIE_PUBLICATION_ALLOW_DEPENDENCY_ADDITIONS", "true")
    assert WorkerConfig().publication_allow_dependency_additions is True
    monkeypatch.setenv("CURIE_PUBLICATION_ALLOW_DEPENDENCY_ADDITIONS", "false")
    assert WorkerConfig().publication_allow_dependency_additions is False
