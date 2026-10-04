"""Authenticated, bounded repository snapshots for approval-gated publication."""

from __future__ import annotations

import base64
import importlib
import json
import subprocess
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner

TOKEN = "runner-token-value"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
REPO = "acme-corp/acme-bot"


@pytest.fixture
def snapshot(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.delenv("CURIE_GITHUB_API_URL", raising=False)
    return importlib.import_module("curie_runner.workspace_snapshot")


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "workspace"
    repo.mkdir(parents=True)
    _git(repo, "init", "--quiet")
    _git(repo, "remote", "add", "origin", f"https://github.com/{REPO}.git")
    (repo / "README.md").write_text("base\n")
    (repo / "asset.bin").write_bytes(b"\x00base\xff")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=Curie Test",
        "-c",
        "user.email=curie@example.com",
        "commit",
        "--quiet",
        "-m",
        "Initial fixture",
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def test_snapshot_captures_staged_unstaged_untracked_and_binary_changes(
    snapshot: Any, tmp_path: Path
) -> None:
    repo, base_sha = _repo(tmp_path)
    (repo / "README.md").write_text("staged\n")
    _git(repo, "add", "README.md")
    (repo / "asset.bin").write_bytes(b"\x00changed\xfe")
    (repo / "untracked.bin").write_bytes(b"\x00new\xfd")

    captured = snapshot.capture_workspace_snapshot(
        repo,
        expected_repo=REPO,
        publication_title="Update assets",
        publication_body="Keep the exact requested body.",
    )

    assert captured.repo_full_name == REPO
    assert captured.base_sha == base_sha
    assert set(captured.changed_paths) == {"README.md", "asset.bin", "untracked.bin"}
    assert b"GIT binary patch" in captured.patch
    assert len(captured.patch) <= 900_000
    assert captured.publication_title == "Update assets"
    assert captured.publication_body == "Keep the exact requested body."

    clean, _ = _repo(tmp_path / "apply-check")
    patch_file = tmp_path / "publication.patch"
    patch_file.write_bytes(captured.patch)
    _git(clean, "apply", "--check", "--binary", str(patch_file))


@pytest.mark.parametrize("prefix", ["", "/forge"])
def test_snapshot_accepts_the_configured_ghes_origin(
    snapshot: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix: str
) -> None:
    repo, base_sha = _repo(tmp_path)
    html_base = f"https://github.example.com{prefix}"
    monkeypatch.setenv("CURIE_GITHUB_API_URL", f"{html_base}/api/v3")
    _git(repo, "remote", "set-url", "origin", f"{html_base}/{REPO}.git")
    (repo / "README.md").write_text("GHES change\n")

    captured = snapshot.capture_workspace_snapshot(repo, expected_repo=REPO)

    assert captured.repo_full_name == REPO
    assert captured.base_sha == base_sha
    assert captured.changed_paths == ("README.md",)
    assert b"GHES change" in captured.patch


@pytest.mark.parametrize(
    "origin",
    [
        f"https://github.com/{REPO}.git",
        f"https://other.example.com/forge/{REPO}.git",
        f"http://github.example.com/forge/{REPO}.git",
        f"https://token@github.example.com/forge/{REPO}.git",
        f"https://github.example.com/{REPO}.git",
        f"https://github.example.com/forge/{REPO}",
        f"https://github.example.com/forge/{REPO}.git/",
        f"https://github.example.com/forge/{REPO}.git?download=1",
        f"https://github.example.com/forge/{REPO}.git#HEAD",
        "https://github.example.com/forge/acme-corp//acme-bot.git",
        f"https://github.example.com:443/forge/{REPO}.git",
    ],
)
def test_snapshot_refuses_noncanonical_or_foreign_origins(
    snapshot: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, origin: str
) -> None:
    repo, _ = _repo(tmp_path)
    monkeypatch.setenv("CURIE_GITHUB_API_URL", "https://github.example.com/forge/api/v3")
    _git(repo, "remote", "set-url", "origin", origin)
    (repo / "README.md").write_text("must not publish\n")

    with pytest.raises(snapshot.WorkspaceSnapshotError, match="origin"):
        snapshot.capture_workspace_snapshot(repo, expected_repo=REPO)


def test_snapshot_default_github_host_refuses_an_unconfigured_ghes_origin(
    snapshot: Any, tmp_path: Path
) -> None:
    repo, _ = _repo(tmp_path)
    _git(repo, "remote", "set-url", "origin", f"https://github.example.com/{REPO}.git")

    with pytest.raises(snapshot.WorkspaceSnapshotError, match="origin"):
        snapshot.capture_workspace_snapshot(repo, expected_repo=REPO)


def test_snapshot_preserves_real_top_level_a_and_b_paths_with_spaces(
    snapshot: Any, tmp_path: Path
) -> None:
    repo, _ = _repo(tmp_path)
    (repo / "a").mkdir()
    (repo / "b").mkdir()
    (repo / "a" / "read me.md").write_text("base a\n")
    (repo / "b" / "release notes.md").write_text("base b\n")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=Curie Test",
        "-c",
        "user.email=curie@example.com",
        "commit",
        "--quiet",
        "-m",
        "Add path fixtures",
    )
    (repo / "a" / "read me.md").write_text("changed a\n")
    (repo / "b" / "release notes.md").write_text("changed b\n")

    captured = snapshot.capture_workspace_snapshot(repo, expected_repo=REPO)

    assert captured.changed_paths == ("a/read me.md", "b/release notes.md")
    assert b"diff --git a/a/read me.md b/a/read me.md" in captured.patch
    assert b"diff --git a/b/release notes.md b/b/release notes.md" in captured.patch


def test_snapshot_represents_pure_rename_as_source_and_destination_with_spaces(
    snapshot: Any, tmp_path: Path
) -> None:
    repo, _ = _repo(tmp_path)
    source = "old release notes.md"
    destination = "new release notes.md"
    (repo / source).write_text("rename me\n")
    _git(repo, "add", source)
    _git(
        repo,
        "-c",
        "user.name=Curie Test",
        "-c",
        "user.email=curie@example.com",
        "commit",
        "--quiet",
        "-m",
        "Add rename fixture",
    )
    _git(repo, "mv", source, destination)

    captured = snapshot.capture_workspace_snapshot(repo, expected_repo=REPO)

    assert captured.changed_paths == (destination, source)
    assert f"diff --git a/{source} b/{source}".encode() in captured.patch
    assert f"diff --git a/{destination} b/{destination}".encode() in captured.patch
    assert b"rename from" not in captured.patch
    assert b"rename to" not in captured.patch

    clean = tmp_path / "apply-rename"
    subprocess.run(
        ["git", "clone", "--quiet", str(repo), str(clean)],
        check=True,
        capture_output=True,
    )
    patch_file = tmp_path / "rename.patch"
    patch_file.write_bytes(captured.patch)
    _git(clean, "apply", "--check", "--binary", str(patch_file))


@pytest.mark.parametrize(
    "patch",
    [
        b"diff --git a/read me.md b/read me.md\n",
        b'diff --git "a/read me.md" "b/read me.md"\n',
    ],
)
def test_snapshot_accepts_unquoted_and_c_quoted_git_headers_with_spaces(
    snapshot: Any, patch: bytes
) -> None:
    assert snapshot.validate_patch(patch) == patch


@pytest.mark.parametrize("size", [899_999, 900_000])
def test_snapshot_patch_cap_accepts_every_raw_byte_through_900000(
    snapshot: Any, size: int
) -> None:
    payload = b"x" * size
    assert snapshot.enforce_patch_cap(payload) == payload


def test_snapshot_patch_cap_rejects_900001_raw_bytes(snapshot: Any) -> None:
    with pytest.raises(snapshot.WorkspaceSnapshotError, match="900000"):
        snapshot.enforce_patch_cap(b"x" * 900_001)


@pytest.mark.parametrize(
    "patch",
    [
        b"diff --git a/../outside b/../outside\n",
        b"diff --git a/.git/config b/.git/config\n",
        b"diff --git a//absolute b//absolute\n",
        b"diff --git a/link b/link\nnew file mode 120000\n",
    ],
)
def test_snapshot_refuses_unsafe_patch_paths_and_special_files(
    snapshot: Any, patch: bytes
) -> None:
    with pytest.raises(snapshot.WorkspaceSnapshotError):
        snapshot.validate_patch(patch)


def test_snapshot_refuses_missing_empty_and_wrong_repository(
    snapshot: Any, tmp_path: Path
) -> None:
    with pytest.raises(snapshot.WorkspaceSnapshotError, match="workspace"):
        snapshot.capture_workspace_snapshot(tmp_path / "missing", expected_repo=REPO)

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(snapshot.WorkspaceSnapshotError, match="git"):
        snapshot.capture_workspace_snapshot(empty, expected_repo=REPO)

    repo, _ = _repo(tmp_path / "wrong")
    with pytest.raises(snapshot.WorkspaceSnapshotError, match="repository"):
        snapshot.capture_workspace_snapshot(repo, expected_repo="acme-corp/other")


def _runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="snapshot-test",
    )


def test_snapshot_route_requires_runner_token_and_returns_base64_binary_patch(
    snapshot: Any
) -> None:
    captured = snapshot.WorkspaceSnapshot(
        repo_full_name=REPO,
        base_sha="a" * 40,
        patch=b"\x00binary-patch\xff",
        changed_paths=("asset.bin",),
        contains_workflow_files=False,
        publication_title="Update binary asset",
        publication_body="Exact requested body",
    )

    async def go() -> None:
        runner = _runner()
        await runner.start()
        async with TestClient(
            TestServer(create_app(runner, token=TOKEN, snapshotter=lambda: captured))
        ) as client:
            unauthenticated = await client.post("/v1/snapshot")
            assert unauthenticated.status == 401
            response = await client.post("/v1/snapshot", headers=AUTH)
            assert response.status == 200
            body = await response.json()
            assert base64.b64decode(body.pop("patch_base64")) == captured.patch
            assert body == {
                "repo_full_name": REPO,
                "base_sha": "a" * 40,
                "changed_paths": ["asset.bin"],
                "contains_workflow_files": False,
                "patch_size_bytes": len(captured.patch),
                "publication_title": "Update binary asset",
                "publication_body": "Exact requested body",
            }

    anyio.run(go)


def test_snapshot_route_returns_intentional_conflict_without_managed_workspace() -> None:
    async def go() -> None:
        runner = _runner()
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=TOKEN))) as client:
            response = await client.post("/v1/snapshot", headers=AUTH)
            assert response.status == 409
            body = await response.json()
            assert "no managed repository workspace" in body["error"]

    anyio.run(go)


def test_snapshot_json_never_contains_raw_binary_or_credential_material(snapshot: Any) -> None:
    captured = snapshot.WorkspaceSnapshot(
        repo_full_name=REPO,
        base_sha="a" * 40,
        patch=b"patch-without-secrets",
        changed_paths=("README.md",),
        contains_workflow_files=False,
        publication_title="Update README",
        publication_body="",
    )
    encoded = json.dumps(captured.to_json())
    assert "Authorization" not in encoded
    assert "GITHUB_TOKEN" not in encoded
    assert "github.com@" not in encoded
