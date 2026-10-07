"""Security-boundary tests for trusted publication snapshot validation."""

from __future__ import annotations

import importlib
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
    )
    validation.validate_snapshot_against_base(
        _coordinator(archive),
        thread_key="1700000000.000100",
        snapshot=_snapshot(("scripts/release.sh.bak",), neighbor_patch.stdout),
        scratch_root=tmp_path,
        protected_paths=("scripts/release.sh",),
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
