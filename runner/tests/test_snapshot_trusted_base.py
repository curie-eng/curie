"""Publication snapshots are the net change from the worker's prepared base (#5230).

The worker names the prepared commit P. The runner proves the agent's HEAD is P
or a bounded local descendant of it, then captures every committed, staged,
unstaged and untracked change from P to the final working tree files. Local
commits never move the base and never hide a change.
"""

from __future__ import annotations

import base64
import os
import stat
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app, workspace_snapshot
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner

REPO = "acme-corp/acme-bot"
GITHUB = "https://github.com"
TOKEN = "runner-token-value"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
TITLE = "Publish the agent change"
BODY = "Carries every local change from the prepared base."
IDENTITY = (
    "-c",
    "user.name=Acme Bot",
    "-c",
    "user.email=acme-bot@example.com",
    "-c",
    "commit.gpgsign=false",
)
# The route's exact 400 answer for a request that does not name P.
BODY_REFUSAL = "snapshot request must name the prepared base_sha"
# Bound refusals name their bound, so a refusal at one bound cannot pass for another.
TOO_MANY_COMMITS = "256"
TOO_MANY_BYTES = r"1 ?MiB|1[,_]?048[,_]?576"

_PREPARED_FILES = {
    ".gitignore": b"build/\n",
    "README.md": b"base readme\n",
    "src/app.py": b"x = 1\n",
    "src/twice.py": b"twice = 0\n",
    "asset.bin": b"\x00base asset\xff",
    "script.sh": b"echo prepared\n",
    "obsolete.txt": b"remove me\n",
    "kept.cfg": b"kept = base\n",
}


def _git(repo: Path, *args: str, stdin: bytes | None = None) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, input=stdin, check=True, capture_output=True
    )
    return result.stdout.decode("utf-8", errors="replace").strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, *IDENTITY, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _write(repo: Path, path: str, data: bytes) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def _prepared(root: Path) -> tuple[Path, str]:
    """A credential-free checkout whose only commit is the prepared base P."""

    repo = root / "workspace"
    repo.mkdir(parents=True)
    _git(repo, "init", "--quiet")
    _git(repo, "remote", "add", "origin", f"{GITHUB}/{REPO}.git")
    for path, data in _PREPARED_FILES.items():
        _write(repo, path, data)
    (repo / "script.sh").chmod(0o644)
    _git(repo, "add", "-A")
    return repo, _commit(repo, "Prepared base")


def _capture(repo: Path, base_sha: str) -> Any:
    return workspace_snapshot.capture_workspace_snapshot(
        repo,
        base_sha=base_sha,
        expected_repo=REPO,
        trusted_origin=GITHUB,
        repository_path=REPO,
    )


def _checkout_of(repo: Path, commit: str, dest: Path) -> Path:
    subprocess.run(
        ["git", "clone", "--quiet", "--no-checkout", str(repo), str(dest)],
        check=True,
        capture_output=True,
    )
    _git(dest, "checkout", "--quiet", "--detach", commit)
    return dest


def _apply(checkout: Path, patch: bytes) -> None:
    patch_file = checkout.parent / f"{checkout.name}.patch"
    patch_file.write_bytes(patch)
    _git(checkout, "apply", "--binary", str(patch_file))


def _files(root: Path, *, skip: frozenset[str] = frozenset()) -> dict[str, tuple[bytes, bool]]:
    """Every working tree file's bytes and owner execute bit, outside .git."""

    found: dict[str, tuple[bytes, bool]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if relative.split("/", 1)[0] in {".git", *skip}:
            continue
        if path.is_symlink():
            found[relative] = (os.readlink(path).encode(), False)
        elif path.is_file():
            found[relative] = (path.read_bytes(), bool(path.stat().st_mode & stat.S_IXUSR))
    return found


def _header_paths(patch: bytes) -> list[str]:
    return [
        line.removeprefix(b"diff --git a/").split(b" b/", 1)[0].decode()
        for line in patch.splitlines()
        if line.startswith(b"diff --git ")
    ]


def _sections(patch: bytes) -> dict[str, bytes]:
    sections: dict[str, bytes] = {}
    current: str | None = None
    for line in patch.splitlines(keepends=True):
        if line.startswith(b"diff --git "):
            current = line.removeprefix(b"diff --git a/").split(b" b/", 1)[0].decode()
            sections[current] = b""
        if current is not None:
            sections[current] += line
    return sections


def _local_history(repo: Path, parent: str, count: int, *, branch: str) -> str:
    """Check out ``count`` linear local commits on top of ``parent``.

    git fast-import writes the whole chain in one process, so a history at the
    ancestry bound stays fast. Only the last commit changes a file.
    """

    stream = bytearray()
    for index in range(1, count + 1):
        message = b"Local step %d\n" % index
        stream += b"commit refs/heads/%s\n" % branch.encode()
        stream += b"committer Acme Bot <acme-bot@example.com> %d +0000\n" % (
            1_700_000_000 + index
        )
        stream += b"data %d\n%s" % (len(message), message)
        if index == 1:
            stream += b"from %s\n" % parent.encode()
        if index == count:
            content = b"readme after %d local commits\n" % count
            stream += b"M 100644 inline README.md\ndata %d\n%s" % (len(content), content)
        stream += b"\n"
    _git(repo, "fast-import", "--quiet", stdin=bytes(stream))
    _git(repo, "checkout", "--quiet", branch)
    return _git(repo, "rev-parse", "HEAD")


def _mixed_workspace(root: Path) -> tuple[Path, str, set[str]]:
    """Committed, staged, unstaged, binary, mode, deleted and untracked changes."""

    repo, prepared = _prepared(root)
    # Committed: an edit and a deletion.
    _write(repo, "README.md", b"committed readme\n")
    _git(repo, "add", "README.md")
    _git(repo, "rm", "--quiet", "obsolete.txt")
    _commit(repo, "Edit README and drop obsolete.txt")
    # Committed: a prepared file leaves the index and is ignored, then changes on disk.
    _git(repo, "rm", "--quiet", "--cached", "kept.cfg")
    with (repo / ".gitignore").open("ab") as ignore:
        ignore.write(b"kept.cfg\n")
    _git(repo, "add", ".gitignore")
    _commit(repo, "Stop tracking kept.cfg")
    _write(repo, "kept.cfg", b"kept = final\n")
    # Staged new file.
    _write(repo, "new/staged.txt", b"staged new file\n")
    _git(repo, "add", "new/staged.txt")
    # Staged, then modified again: the final bytes win.
    _write(repo, "src/twice.py", b"twice = 'staged intermediate'\n")
    _git(repo, "add", "src/twice.py")
    _write(repo, "src/twice.py", b"twice = 'final'\n")
    # Unstaged edit, binary edit and an executable bit.
    _write(repo, "src/app.py", b"x = 2\n")
    _write(repo, "asset.bin", b"\x00\x01final asset\x00\xfe")
    (repo / "script.sh").chmod(0o755)
    # Untracked: one eligible, one ignored.
    _write(repo, "notes/new.txt", b"untracked note\n")
    _write(repo, "build/out.log", b"ignored output\n")
    expected = {
        ".gitignore",
        "README.md",
        "asset.bin",
        "kept.cfg",
        "new/staged.txt",
        "notes/new.txt",
        "obsolete.txt",
        "script.sh",
        "src/app.py",
        "src/twice.py",
    }
    return repo, prepared, expected


def _runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="snapshot-trusted-base-test",
    )


def _route(
    repo: Path, requests: list[dict[str, Any]]
) -> tuple[list[tuple[int, Any]], list[str]]:
    """POST each request to the real route over the production snapshotter.

    Returns each status and JSON body, plus every base the route handed to the
    snapshotter, so a refused request is proven never to reach capture.
    """

    calls: list[str] = []
    production = workspace_snapshot.workspace_snapshotter(
        repo,
        trusted_origin=GITHUB,
        repository_path=REPO,
        proposal=lambda: (TITLE, BODY),
    )

    def recording(base_sha: str) -> Any:
        calls.append(base_sha)
        return production(base_sha)

    responses: list[tuple[int, Any]] = []

    async def go() -> None:
        runner = _runner()
        await runner.start()
        app = create_app(runner, token=TOKEN, snapshotter=recording)
        async with TestClient(TestServer(app)) as client:
            for request in requests:
                options = dict(request)
                headers = {**AUTH, **options.pop("headers", {})}
                response = await client.post("/v1/snapshot", headers=headers, **options)
                responses.append((response.status, await response.json()))

    anyio.run(go)
    return responses, calls


def _commit_change(repo: Path, path: str = "src/app.py", data: bytes = b"x = 2\n") -> str:
    _write(repo, path, data)
    _git(repo, "add", path)
    return _commit(repo, f"Change {path}")


# --- capture from P --------------------------------------------------------------


def test_a_committed_only_change_with_a_clean_tree_is_captured_from_the_prepared_base(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    head = _commit_change(repo)
    assert head != prepared
    assert _git(repo, "status", "--porcelain") == ""

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert captured.changed_paths == ("src/app.py",)
    assert b"+x = 2" in captured.patch
    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    _apply(fresh, captured.patch)
    assert (fresh / "src" / "app.py").read_bytes() == b"x = 2\n"


def test_equivalent_unstaged_staged_and_committed_changes_capture_one_patch(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _write(repo, "src/app.py", b"x = 2\n")
    _write(repo, "added.txt", b"added by the agent\n")

    unstaged = _capture(repo, prepared)
    _git(repo, "add", "-A")
    staged = _capture(repo, prepared)
    _commit(repo, "Agent change")
    committed = _capture(repo, prepared)

    assert unstaged.base_sha == staged.base_sha == committed.base_sha == prepared
    assert unstaged.patch
    assert unstaged.patch == staged.patch == committed.patch
    assert unstaged.changed_paths == staged.changed_paths == committed.changed_paths
    assert set(committed.changed_paths) == {"added.txt", "src/app.py"}


def test_mixed_changes_appear_once_with_their_final_bytes_and_modes(tmp_path: Path) -> None:
    repo, prepared, expected = _mixed_workspace(tmp_path)

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert len(captured.changed_paths) == len(set(captured.changed_paths))
    assert set(captured.changed_paths) == expected
    assert Counter(_header_paths(captured.patch)) == Counter(expected)
    assert captured.contains_workflow_files is False
    sections = _sections(captured.patch)
    assert b"old mode 100644\nnew mode 100755\n" in sections["script.sh"]
    assert b"GIT binary patch" in sections["asset.bin"]
    assert b"deleted file mode" in sections["obsolete.txt"]
    # Leaving the index and being ignored does not remove a prepared path.
    assert b"deleted file mode" not in sections["kept.cfg"]
    assert b"+kept = final" in sections["kept.cfg"]
    assert b"staged intermediate" not in captured.patch
    assert b"+twice = 'final'" in sections["src/twice.py"]
    assert b"ignored output" not in captured.patch

    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    _apply(fresh, captured.patch)
    assert _files(fresh) == _files(repo, skip=frozenset({"build"}))


def test_a_revision_captures_only_its_delta_from_the_prior_publication_head(
    tmp_path: Path,
) -> None:
    repo, _original = _prepared(tmp_path)
    _write(repo, "src/feature.py", b"feature = 1\n")
    _write(repo, "README.md", b"readme from the prior publication\n")
    _git(repo, "add", "-A")
    prior_head = _commit(repo, "Prior publication")
    _commit_change(repo, "src/feature.py", b"feature = 2\n")

    captured = _capture(repo, prior_head)

    assert captured.base_sha == prior_head
    assert captured.changed_paths == ("src/feature.py",)
    assert b"-feature = 1" in captured.patch
    assert b"+feature = 2" in captured.patch
    assert b"README.md" not in captured.patch


def test_a_change_committed_then_reverted_is_an_empty_capture_at_the_prepared_base(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit_change(repo, "src/app.py", b"x = 2\n")
    _commit_change(repo, "src/app.py", _PREPARED_FILES["src/app.py"])

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert captured.patch == b""
    assert captured.changed_paths == ()


def test_a_verified_descendant_with_a_local_merge_is_captured(tmp_path: Path) -> None:
    # Liveness control for every history refusal below: a legitimate local
    # history, merge included, publishes its whole net change.
    repo, prepared = _prepared(tmp_path)
    _git(repo, "checkout", "--quiet", "-b", "side")
    _commit_change(repo, "side.txt", b"from the side branch\n")
    _git(repo, "checkout", "--quiet", "-")
    _commit_change(repo, "src/app.py", b"x = 2\n")
    _git(repo, *IDENTITY, "merge", "--quiet", "--no-ff", "--no-edit", "side")

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert set(captured.changed_paths) == {"side.txt", "src/app.py"}


def test_capture_leaves_the_agents_git_metadata_byte_identical(tmp_path: Path) -> None:
    repo, prepared, _ = _mixed_workspace(tmp_path)
    head = _git(repo, "rev-parse", "HEAD")
    status = _git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    git_dir = repo / ".git"

    def metadata() -> dict[str, bytes]:
        return {
            path.relative_to(git_dir).as_posix(): path.read_bytes()
            for path in sorted(git_dir.rglob("*"))
            if path.is_file()
        }

    before = metadata()
    captured = _capture(repo, prepared)
    after = metadata()

    assert captured.patch
    # The real index, HEAD, refs and object store are only read: hashing goes
    # to a scratch index and object directory.
    assert after == before
    assert _git(repo, "rev-parse", "HEAD") == head
    assert _git(repo, "status", "--porcelain=v1", "--untracked-files=all") == status


def test_the_runner_git_config_environment_reaches_the_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The chart configures the runner's git through GIT_CONFIG_COUNT/KEY/VALUE
    # (safe.directory for the mounted workspace). Here an excludes file stands in
    # for it, so whether capture's git calls saw the setting shows in the result.
    repo, prepared = _prepared(tmp_path)
    excludes = tmp_path / "runner-excludes"
    excludes.write_bytes(b"scratch.log\n")
    _write(repo, "added.txt", b"added by the agent\n")
    _write(repo, "scratch.log", b"excluded by the runner git config\n")

    # Control: without the environment the untracked file is part of the candidate.
    unconfigured = _capture(repo, prepared)
    assert unconfigured.changed_paths == ("added.txt", "scratch.log")

    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.excludesFile")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(excludes))
    configured = _capture(repo, prepared)

    assert configured.base_sha == prepared
    assert configured.changed_paths == ("added.txt",)
    assert b"excluded by the runner git config" not in configured.patch


# --- history refusals -------------------------------------------------------------


def test_an_unrelated_head_is_refused(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "checkout", "--quiet", "--orphan", "unrelated")
    _write(repo, "README.md", b"unrelated history\n")
    _git(repo, "add", "-A")
    _commit(repo, "Unrelated root")

    with pytest.raises(workspace_snapshot.WorkspaceHistoryError):
        _capture(repo, prepared)


def test_a_prepared_base_missing_from_the_object_store_is_refused(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit_change(repo)
    missing = "0123456789abcdef0123456789abcdef01234567"
    assert missing != prepared

    with pytest.raises(workspace_snapshot.WorkspaceHistoryError):
        _capture(repo, missing)
    assert _capture(repo, prepared).changed_paths == ("src/app.py",)


def test_a_substituted_base_that_head_does_not_descend_from_is_refused(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    tree = _git(repo, "rev-parse", f"{prepared}^{{tree}}")
    # A real commit in the object store on a sibling line of history.
    sibling = _git(repo, *IDENTITY, "commit-tree", tree, "-p", prepared, "-m", "Sibling")
    _commit_change(repo)

    with pytest.raises(workspace_snapshot.WorkspaceHistoryError):
        _capture(repo, sibling)
    captured = _capture(repo, prepared)
    assert captured.base_sha == prepared
    assert captured.changed_paths == ("src/app.py",)


def test_local_history_over_256_commits_is_refused_and_256_is_captured(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)

    _local_history(repo, prepared, 256, branch="history-256")
    at_bound = _capture(repo, prepared)
    assert at_bound.base_sha == prepared
    assert at_bound.changed_paths == ("README.md",)

    _local_history(repo, prepared, 257, branch="history-257")
    with pytest.raises(workspace_snapshot.WorkspaceHistoryError, match=TOO_MANY_COMMITS):
        _capture(repo, prepared)


def test_local_commit_objects_over_one_mebibyte_are_refused(tmp_path: Path) -> None:
    small, small_prepared = _prepared(tmp_path / "small")
    _commit_change(small)
    assert _capture(small, small_prepared).changed_paths == ("src/app.py",)

    large, large_prepared = _prepared(tmp_path / "large")
    _write(large, "src/app.py", b"x = 2\n")
    _git(large, "add", "src/app.py")
    message = tmp_path / "message.txt"
    message.write_bytes(b"x" * 1_048_576 + b"\n")
    _git(large, *IDENTITY, "commit", "--quiet", "--cleanup=verbatim", "-F", str(message))

    with pytest.raises(workspace_snapshot.WorkspaceHistoryError, match=TOO_MANY_BYTES):
        _capture(large, large_prepared)


# --- path type refusals -----------------------------------------------------------


@pytest.mark.parametrize("change", ["retarget", "add"])
def test_a_symlink_change_is_refused_while_an_unchanged_symlink_is_kept(
    tmp_path: Path, change: str
) -> None:
    repo, _ = _prepared(tmp_path)
    os.symlink("README.md", repo / "link")
    _git(repo, "add", "link")
    prepared = _commit(repo, "Prepared base with a symlink")
    _write(repo, "src/app.py", b"x = 2\n")

    kept = _capture(repo, prepared)
    assert kept.changed_paths == ("src/app.py",)
    assert b"link" not in kept.patch

    if change == "retarget":
        (repo / "link").unlink()
        os.symlink("src/app.py", repo / "link")
        path = "link"
    else:
        os.symlink("README.md", repo / "new-link")
        path = "new-link"
    with pytest.raises(workspace_snapshot.WorkspaceSnapshotError) as refused:
        _capture(repo, prepared)
    assert f"workspace path {path!r} is not a regular file" in str(refused.value)
    assert not isinstance(refused.value, workspace_snapshot.WorkspaceHistoryError)


# --- skip-worktree entries the worker's preparation leaves behind -----------------


def _drop_from_worktree(repo: Path, path: str) -> None:
    """What preparation does to a committed link that escapes the checkout.

    ``workspace.py::_drop_escaping_links`` unlinks it and marks it skip-worktree,
    so the file is gone while the index keeps P's entry and git status is clean.
    """

    (repo / path).unlink()
    _git(repo, "update-index", "--skip-worktree", "--", path)
    assert _git(repo, "ls-files", "-v", "--", path).startswith("S ")


def _prepared_with_escaping_link(root: Path) -> tuple[Path, str]:
    """P commits ``out -> ../outside``; the agent's checkout is P after preparation."""

    repo, _ = _prepared(root)
    os.symlink("../outside", repo / "out")
    _git(repo, "add", "out")
    prepared = _commit(repo, "Prepared base with an escaping link")
    _drop_from_worktree(repo, "out")
    return repo, prepared


@pytest.mark.parametrize("committed", [False, True], ids=["uncommitted", "committed"])
def test_a_dropped_escaping_link_stays_the_prepared_entry_beside_an_ordinary_edit(
    tmp_path: Path, committed: bool
) -> None:
    repo, prepared = _prepared_with_escaping_link(tmp_path)
    if committed:
        _commit_change(repo)
    else:
        _write(repo, "src/app.py", b"x = 2\n")

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    # The link preparation removed is P's entry, not a deletion to publish.
    assert captured.changed_paths == ("src/app.py",)
    assert _header_paths(captured.patch) == ["src/app.py"]
    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    _apply(fresh, captured.patch)
    assert (fresh / "src" / "app.py").read_bytes() == b"x = 2\n"
    assert os.readlink(fresh / "out") == "../outside"


def test_a_skip_worktree_file_recreated_with_new_bytes_publishes_them(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _drop_from_worktree(repo, "kept.cfg")
    _write(repo, "kept.cfg", b"kept = recreated\n")
    # git status does not show it; the final file still decides the candidate.
    assert _git(repo, "status", "--porcelain") == ""

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert captured.changed_paths == ("kept.cfg",)
    section = _sections(captured.patch)["kept.cfg"]
    assert b"-kept = base" in section
    assert b"+kept = recreated" in section
    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    _apply(fresh, captured.patch)
    assert (fresh / "kept.cfg").read_bytes() == b"kept = recreated\n"


@pytest.mark.parametrize("path", ["out", "kept.cfg"])
def test_a_symlink_recreated_at_a_skip_worktree_path_is_refused(tmp_path: Path, path: str) -> None:
    repo, prepared = _prepared_with_escaping_link(tmp_path)
    if path != "out":
        _drop_from_worktree(repo, path)
    os.symlink("README.md", repo / path)

    with pytest.raises(workspace_snapshot.WorkspaceSnapshotError) as refused:
        _capture(repo, prepared)

    assert f"workspace path {path!r} is not a regular file" in str(refused.value)
    assert not isinstance(refused.value, workspace_snapshot.WorkspaceHistoryError)


def test_an_absent_skip_worktree_entry_the_agent_restaged_is_a_deletion_of_the_base_entry(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    staged_only = b"index only bytes never written to disk\n"
    blob = _git(repo, "hash-object", "-w", "--stdin", stdin=staged_only)
    (repo / "kept.cfg").unlink()
    _git(repo, "update-index", "--cacheinfo", f"100644,{blob},kept.cfg")
    _git(repo, "update-index", "--skip-worktree", "--", "kept.cfg")
    listed = _git(repo, "ls-files", "-s", "-v", "--", "kept.cfg").split()
    assert listed[0] == "S"
    assert listed[2] == blob

    captured = _capture(repo, prepared)

    # The index entry differs from P, so the filesystem decides: the file is gone.
    assert captured.changed_paths == ("kept.cfg",)
    assert b"deleted file mode 100644" in _sections(captured.patch)["kept.cfg"]
    assert staged_only not in captured.patch
    assert blob.encode() not in captured.patch
    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    _apply(fresh, captured.patch)
    assert not (fresh / "kept.cfg").exists()


# --- tracked files publish git's view of them --------------------------------------

_HIDDEN = b"# HIDDEN: never shown by git diff\n"


def _crlf_checkout(root: Path) -> tuple[Path, str]:
    """P stores LF content under ``*.bat text eol=crlf``; checkout writes it as CRLF."""

    repo, _ = _prepared(root)
    _write(repo, ".gitattributes", b"*.bat text eol=crlf\n")
    _write(repo, "run.bat", b"@echo off\necho one\necho two\n")
    _git(repo, "add", "-A")
    prepared = _commit(repo, "Prepared base with a CRLF script")
    (repo / "run.bat").unlink()
    _git(repo, "checkout", "--", "run.bat")
    assert (repo / "run.bat").read_bytes() == b"@echo off\r\necho one\r\necho two\r\n"
    assert _git(repo, "status", "--porcelain") == ""
    return repo, prepared


def _changed_lines(section: bytes) -> list[bytes]:
    return [
        line
        for line in section.splitlines()
        if line.startswith((b"+", b"-")) and not line.startswith((b"+++", b"---"))
    ]


def test_an_untouched_crlf_checkout_is_an_empty_capture(tmp_path: Path) -> None:
    repo, prepared = _crlf_checkout(tmp_path)

    captured = _capture(repo, prepared)

    assert captured.base_sha == prepared
    assert captured.patch == b""
    assert captured.changed_paths == ()


def test_an_edit_to_one_line_of_a_crlf_file_publishes_that_line_in_git_normal_form(
    tmp_path: Path,
) -> None:
    repo, prepared = _crlf_checkout(tmp_path)
    _write(repo, "run.bat", b"@echo off\r\necho one\r\necho changed\r\n")

    captured = _capture(repo, prepared)

    assert captured.changed_paths == ("run.bat",)
    assert _changed_lines(_sections(captured.patch)["run.bat"]) == [
        b"-echo two",
        b"+echo changed",
    ]
    assert b"\r" not in captured.patch


def test_an_untracked_crlf_file_under_an_eol_attribute_is_published_byte_for_byte(
    tmp_path: Path,
) -> None:
    repo, _ = _prepared(tmp_path)
    _write(repo, ".gitattributes", b"*.bat text eol=crlf\n")
    _git(repo, "add", ".gitattributes")
    prepared = _commit(repo, "Prepared base with an eol attribute")
    _write(repo, "new.bat", b"@echo off\r\necho new\r\n")

    captured = _capture(repo, prepared)

    # git does not read an untracked file, so its bytes publish unconverted.
    assert captured.changed_paths == ("new.bat",)
    assert b"+@echo off\r\n+echo new\r\n" in _sections(captured.patch)["new.bat"]


@pytest.mark.parametrize("visible", [False, True], ids=["hidden_line_alone", "beside_an_edit"])
def test_a_line_a_checkout_clean_filter_hides_from_git_diff_is_not_published(
    tmp_path: Path, visible: bool
) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "config", "filter.strip.clean", "grep -v HIDDEN || true")
    _git(repo, "config", "filter.strip.smudge", "cat")
    attributes = repo / ".git" / "info" / "attributes"
    attributes.parent.mkdir(exist_ok=True)
    attributes.write_bytes(b"src/app.py filter=strip\n")
    _write(repo, "src/app.py", (b"x = 2\n" if visible else b"x = 1\n") + _HIDDEN)
    assert "HIDDEN" not in _git(repo, "diff", "HEAD")

    captured = _capture(repo, prepared)

    assert b"HIDDEN" not in captured.patch
    if not visible:
        # git sees no change, so nothing publishes.
        assert captured.patch == b""
        assert captured.changed_paths == ()
        return
    assert captured.changed_paths == ("src/app.py",)
    assert _changed_lines(_sections(captured.patch)["src/app.py"]) == [b"-x = 1", b"+x = 2"]


def test_a_staged_visible_edit_publishes_without_a_line_the_clean_filter_hides(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "config", "filter.strip.clean", "grep -v HIDDEN || true")
    _git(repo, "config", "filter.strip.smudge", "cat")
    attributes = repo / ".git" / "info" / "attributes"
    attributes.parent.mkdir(exist_ok=True)
    attributes.write_bytes(b"src/app.py filter=strip\n")
    _write(repo, "src/app.py", b"x = 2\n")
    _git(repo, "add", "src/app.py")
    with (repo / "src" / "app.py").open("ab") as handle:
        handle.write(_HIDDEN)

    captured = _capture(repo, prepared)

    assert captured.changed_paths == ("src/app.py",)
    assert _changed_lines(_sections(captured.patch)["src/app.py"]) == [b"-x = 1", b"+x = 2"]
    assert b"HIDDEN" not in captured.patch


def _trust_stat(repo: Path, path: str) -> int:
    """Make git trust ``path``'s cached stat; the mtime it caches, in nanoseconds.

    Whole-second mtime and size become git's only checks, and the index is
    refreshed after an mtime a minute in the past, so the entry is not racy.
    Every tracked file must equal its index entry when this runs.
    """

    _git(repo, "config", "core.checkStat", "minimal")
    _git(repo, "config", "core.trustctime", "false")
    target = repo / path
    past = target.stat().st_mtime_ns - 60 * 1_000_000_000
    os.utime(target, ns=(past, past))
    _git(repo, "update-index", "--refresh")
    return past


def _rewrite_keeping_stat(repo: Path, path: str, data: bytes, mtime_ns: int) -> None:
    """Rewrite ``path`` with same-size bytes and put its mtime back."""

    target = repo / path
    assert len(data) == target.stat().st_size and data != target.read_bytes()
    target.write_bytes(data)
    os.utime(target, ns=(mtime_ns, mtime_ns))


def _prepared_with_attributes(root: Path, attributes: bytes | None) -> tuple[Path, str]:
    """P, optionally with a committed ``.gitattributes``, so tracked files convert at check-in."""

    repo, prepared = _prepared(root)
    if attributes is None:
        return repo, prepared
    _write(repo, ".gitattributes", attributes)
    _git(repo, "add", ".gitattributes")
    return repo, _commit(repo, "Prepared base with attributes")


@pytest.mark.parametrize("attributes", [None, b"*.py text\n"], ids=["raw", "text_attribute"])
def test_a_stale_stat_rewrite_of_a_file_whose_index_entry_is_the_base_is_published(
    tmp_path: Path, attributes: bytes | None
) -> None:
    repo, prepared = _prepared_with_attributes(tmp_path, attributes)
    past = _trust_stat(repo, "src/app.py")
    _rewrite_keeping_stat(repo, "src/app.py", b"x = 3\n", past)
    # git trusts the cached stat: git diff <P> does not list the file.
    assert _git(repo, "diff", "--name-only", prepared) == ""

    captured = _capture(repo, prepared)

    # Final disk bytes win over the stat cache.
    assert captured.base_sha == prepared
    assert captured.changed_paths == ("src/app.py",)
    assert _changed_lines(_sections(captured.patch)["src/app.py"]) == [b"-x = 1", b"+x = 3"]


@pytest.mark.parametrize("attributes", [None, b"*.py text\n"], ids=["raw", "text_attribute"])
@pytest.mark.parametrize("recorded", ["staged", "committed"])
def test_a_stale_stat_rewrite_after_the_edit_was_recorded_publishes_the_rewrite(
    tmp_path: Path, recorded: str, attributes: bytes | None
) -> None:
    repo, prepared = _prepared_with_attributes(tmp_path, attributes)
    _write(repo, "src/app.py", b"x = 2\n")
    _git(repo, "add", "src/app.py")
    if recorded == "committed":
        _commit(repo, "Change src/app.py")
    past = _trust_stat(repo, "src/app.py")
    _rewrite_keeping_stat(repo, "src/app.py", b"x = 3\n", past)

    captured = _capture(repo, prepared)

    # Final disk bytes win over staging, commits and the stat cache.
    assert captured.base_sha == prepared
    assert captured.changed_paths == ("src/app.py",)
    assert _changed_lines(_sections(captured.patch)["src/app.py"]) == [b"-x = 1", b"+x = 3"]
    assert b"x = 2" not in captured.patch


# --- attributes git finds only in the index still convert the final bytes ---------


def test_a_deleted_gitattributes_kept_in_the_index_still_normalizes_an_edited_crlf_file(
    tmp_path: Path,
) -> None:
    repo, prepared = _crlf_checkout(tmp_path)
    (repo / ".gitattributes").unlink()
    assert _git(repo, "ls-files", "--", ".gitattributes") == ".gitattributes"
    _write(repo, "run.bat", b"@echo off\r\necho one\r\necho changed\r\n")

    captured = _capture(repo, prepared)

    sections = _sections(captured.patch)
    assert captured.changed_paths == (".gitattributes", "run.bat")
    assert b"deleted file mode 100644" in sections[".gitattributes"]
    # git's attribute lookup falls back to the index copy: the edit is normalized to LF.
    assert _changed_lines(sections["run.bat"]) == [b"-echo two", b"+echo changed"]
    assert b"\r" not in sections["run.bat"]


def test_a_deleted_gitattributes_kept_in_the_index_still_applies_the_clean_filter(
    tmp_path: Path,
) -> None:
    repo, _ = _prepared(tmp_path)
    _git(repo, "config", "filter.strip.clean", "grep -v HIDDEN || true")
    _git(repo, "config", "filter.strip.smudge", "cat")
    _write(repo, ".gitattributes", b"src/app.py filter=strip\n")
    _git(repo, "add", ".gitattributes")
    prepared = _commit(repo, "Prepared base with a clean filter attribute")
    (repo / ".gitattributes").unlink()
    assert _git(repo, "ls-files", "--", ".gitattributes") == ".gitattributes"
    _write(repo, "src/app.py", b"x = 2\n" + _HIDDEN)

    captured = _capture(repo, prepared)

    sections = _sections(captured.patch)
    assert captured.changed_paths == (".gitattributes", "src/app.py")
    assert b"deleted file mode 100644" in sections[".gitattributes"]
    assert _changed_lines(sections["src/app.py"]) == [b"-x = 1", b"+x = 2"]
    assert b"HIDDEN" not in captured.patch


# --- one builder: the capture publishes the rendered candidate --------------------


def test_the_capture_patch_is_the_rendered_shared_candidate(tmp_path: Path) -> None:
    repo, prepared, expected = _mixed_workspace(tmp_path)

    built = workspace_snapshot.build_candidate(repo, prepared, render_patch=True)
    captured = _capture(repo, prepared)

    assert built.base_sha == prepared
    assert set(built.changed_paths) == expected
    assert tuple(built.changed_paths) == captured.changed_paths
    assert built.patch == captured.patch
    # The tree is the one the patch describes: P with the patch applied.
    fresh = _checkout_of(repo, prepared, tmp_path / "fresh")
    patch_file = tmp_path / "candidate.patch"
    patch_file.write_bytes(built.patch)
    _git(fresh, "apply", "--binary", "--index", str(patch_file))
    assert _git(fresh, "write-tree") == built.tree


def test_a_mode_only_chmod_publishes_when_the_checkout_sets_core_filemode_false(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "config", "core.fileMode", "false")
    (repo / "script.sh").chmod(0o755)
    assert _git(repo, "status", "--porcelain") == ""

    captured = _capture(repo, prepared)

    assert captured.changed_paths == ("script.sh",)
    assert b"old mode 100644\nnew mode 100755\n" in _sections(captured.patch)["script.sh"]


def test_an_assume_unchanged_file_publishes_its_bytes_on_disk(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "update-index", "--assume-unchanged", "--", "README.md")
    _write(repo, "README.md", b"edited behind an index flag\n")
    assert _git(repo, "status", "--porcelain") == ""

    captured = _capture(repo, prepared)

    # git no longer reads a flagged file, so its bytes on disk publish.
    assert captured.changed_paths == ("README.md",)
    assert b"+edited behind an index flag" in _sections(captured.patch)["README.md"]


# --- the /v1/snapshot route -------------------------------------------------------


def test_the_route_captures_a_committed_change_at_the_requested_base(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit_change(repo)

    responses, calls = _route(repo, [{"json": {"base_sha": prepared}}])

    ((status, body),) = responses
    assert status == 200
    assert calls == [prepared]
    assert body["base_sha"] == prepared
    assert body["changed_paths"] == ["src/app.py"]
    assert b"+x = 2" in base64.b64decode(body["patch_base64"])
    assert body["publication_title"] == TITLE
    assert body["publication_body"] == BODY


def test_the_route_refuses_a_request_that_does_not_name_the_prepared_base(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit_change(repo)
    malformed: list[dict[str, Any]] = [
        {},
        {"json": {}},
        {"json": {"base_sha": prepared, "extra": "value"}},
        {"json": {"base_sha": "g" * 40}},
        {"json": {"base_sha": prepared.upper()}},
        {"json": {"base_sha": prepared[:39]}},
        {"json": {"base_sha": None}},
        {"json": [prepared]},
        {"data": b"{", "headers": {"Content-Type": "application/json"}},
    ]

    responses, calls = _route(repo, [*malformed, {"json": {"base_sha": prepared}}])

    refused, accepted = responses[:-1], responses[-1]
    assert refused == [(400, {"error": BODY_REFUSAL})] * len(malformed)
    # Only the well formed request reached capture.
    assert calls == [prepared]
    assert accepted[0] == 200
    assert accepted[1]["base_sha"] == prepared


@pytest.mark.parametrize("history", ["unrelated", "missing"])
def test_the_route_answers_422_for_a_history_refusal(tmp_path: Path, history: str) -> None:
    repo, prepared = _prepared(tmp_path)
    base = prepared
    if history == "unrelated":
        _git(repo, "checkout", "--quiet", "--orphan", "unrelated")
        _commit(repo, "Unrelated root")
    else:
        _commit_change(repo)
        base = "fedcba9876543210fedcba9876543210fedcba98"
    with pytest.raises(workspace_snapshot.WorkspaceHistoryError) as direct:
        _capture(repo, base)

    responses, calls = _route(repo, [{"json": {"base_sha": base}}])

    assert calls == [base]
    assert responses == [(422, {"error": str(direct.value)})]

