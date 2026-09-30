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
from curie_worker.runner_client import RunnerWorkspaceSnapshot


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

    assert not validation._safe_changed_path(".github/workflows/publish.yml")
    assert not validation._safe_changed_path(".GIT/config")
    assert validation._safe_changed_path("src/main.py")


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
    subprocess.run(
        ["git", "config", "user.name", "Publisher"], cwd=repo, check=True
    )
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
        preparer=SimpleNamespace(
            limits=SimpleNamespace(max_archive_bytes=len(archive_bytes) + 1)
        ),
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
        match="changed_paths do not exactly match",
    ):
        validation.validate_snapshot_against_base(
            coordinator,
            thread_key="1700000000.000100",
            snapshot=snapshot,
            scratch_root=tmp_path,
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
        coordinator, thread_key="example-thread", snapshot=snapshot
    )
    for changed in (
        {"patch": b"diff --git a/a b/a\n"},
        {"base_sha": "b" * 40},
    ):
        with pytest.raises(validation.WorkspacePreparationError):
            validation.validate_snapshot_against_base(
                coordinator,
                thread_key="example-thread",
                snapshot=RunnerWorkspaceSnapshot(
                    **{**snapshot.__dict__, **changed}
                ),
            )
