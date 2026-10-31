"""Cross-boundary regression for #5230: a locally committed change publishes.

The real runner route (the production ``workspace_snapshotter`` over a real git
checkout) answers the real worker ``RunnerClient``, and the real
``validate_snapshot_against_base`` rebuilds the candidate on the trusted base
archive. Only the coordinator's retained facts are a stand-in: they name the
prepared commit P and stream an archive of P, either ``git archive`` of P's tree
or the normalized checkout archive preparation itself builds.
"""

from __future__ import annotations

import asyncio
import os
import stat
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, create_app, workspace_snapshot
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_worker.publication_validation import (
    DEPENDENCY_ADDITION_REFUSAL,
    GITHUB_WORKFLOW_REFUSAL,
    publication_git_environment,
    validate_snapshot_against_base,
)
from curie_worker.runner_client import RunnerClient, RunnerWorkspaceSnapshot
from curie_worker.workspace import (
    SubprocessCommands,
    WorkspaceLimits,
    WorkspacePreparationError,
    WorkspacePreparer,
    validate_workspace_archive,
)

REPO = "acme-corp/acme-bot"
GITHUB = "https://github.com"
TOKEN = "runner-token-value"
TITLE = "Publish the agent change"
BODY = "Carries every local change from the prepared base."
THREAD = "1700000000.000100"
IDENTITY = (
    "-c",
    "user.name=Acme Bot",
    "-c",
    "user.email=acme-bot@example.com",
    "-c",
    "commit.gpgsign=false",
)
PYPROJECT_BASE = (
    b'[project]\nname = "acme-tool"\nversion = "1.0.0"\n'
    b'dependencies = ["acme-base>=1.0.0"]\n'
)
PYPROJECT_ADDED = (
    b'[project]\nname = "acme-tool"\nversion = "1.0.0"\n'
    b'dependencies = ["acme-base>=1.0.0", "acme-new>=1.0.0"]\n'
)
_PREPARED_FILES = {
    ".gitignore": b"build/\n",
    "README.md": b"base readme\n",
    "pyproject.toml": PYPROJECT_BASE,
    "src/app.py": b"x = 1\n",
    "src/twice.py": b"twice = 0\n",
    "asset.bin": b"\x00base asset\xff",
    "script.sh": b"echo prepared\n",
    "obsolete.txt": b"remove me\n",
    "kept.cfg": b"kept = base\n",
}


def _run_git(repo: Path, *args: str, env: dict[str, str] | None = None) -> bytes:
    return subprocess.run(
        ["git", *args], cwd=repo, env=env, check=True, capture_output=True
    ).stdout


def _git(repo: Path, *args: str) -> str:
    return _run_git(repo, *args).decode("utf-8", errors="replace").strip()


def _write(repo: Path, path: str, data: bytes) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def _commit(repo: Path, message: str) -> str:
    _git(repo, *IDENTITY, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _commit_change(repo: Path, path: str, data: bytes) -> str:
    _write(repo, path, data)
    _git(repo, "add", path)
    return _commit(repo, f"Change {path}")


def _prepared(root: Path) -> tuple[Path, str]:
    """A credential-free agent checkout whose only commit is the prepared base P."""

    repo = root / "workspace"
    repo.mkdir(parents=True)
    _git(repo, "init", "--quiet")
    _git(repo, "remote", "add", "origin", f"{GITHUB}/{REPO}.git")
    for path, data in _PREPARED_FILES.items():
        _write(repo, path, data)
    (repo / "script.sh").chmod(0o644)
    _git(repo, "add", "-A")
    return repo, _commit(repo, "Prepared base")


def _tree_of(repo: Path, commit: str) -> str:
    return _git(repo, "rev-parse", f"{commit}^{{tree}}")


def _runner() -> SessionRunner:
    fake = FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="publication-snapshot-trusted-base",
    )


def _snapshot_over_http(repo: Path, base_sha: str) -> RunnerWorkspaceSnapshot:
    """Ask the real runner route for a snapshot through the real worker client."""

    async def go() -> RunnerWorkspaceSnapshot:
        runner = _runner()
        await runner.start()
        app = create_app(
            runner,
            token=TOKEN,
            snapshotter=workspace_snapshot.workspace_snapshotter(
                repo,
                trusted_origin=GITHUB,
                repository_path=REPO,
                proposal=lambda: (TITLE, BODY),
            ),
        )
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        try:
            return await client.snapshot(
                f"http://127.0.0.1:{server.port}", token=TOKEN, base_sha=base_sha
            )
        finally:
            await client.close()
            await server.close()

    return asyncio.run(go())


def _archive(repo: Path, commit: str) -> bytes:
    return _run_git(repo, "archive", "--format=tar.gz", commit)


def _coordinator(base_sha: str, archive: bytes) -> SimpleNamespace:
    """The retained facts the worker holds for this thread: P and P's archive."""

    return SimpleNamespace(
        preparer=SimpleNamespace(limits=SimpleNamespace(max_archive_bytes=len(archive) + 1)),
        current=lambda _thread: SimpleNamespace(repo_full_name=REPO, base_sha=base_sha),
        stream_current_base=lambda _thread: [archive],
    )


def _validate(
    coordinator: SimpleNamespace, snapshot: RunnerWorkspaceSnapshot, scratch: Path
) -> None:
    scratch.mkdir(parents=True, exist_ok=True)
    validate_snapshot_against_base(
        coordinator,
        thread_key=THREAD,
        snapshot=snapshot,
        scratch_root=scratch,
        protected_paths=(),
        allow_dependency_additions=False,
    )
    assert not list(scratch.glob("publication-validate-*")), "validator leaked its checkout"


def _result(root: Path, archive: bytes, patch: bytes) -> tuple[str, Path]:
    """The published result: P's archive with the patch applied, as a tree id."""

    root.mkdir(parents=True)
    checkout = root / "checkout"
    checkout.mkdir()
    with tarfile.open(fileobj=BytesIO(archive), mode="r:gz") as source:
        source.extractall(checkout, filter="data")
    env = publication_git_environment(root)
    _run_git(checkout, "init", "--quiet", env=env)
    _run_git(checkout, "add", "-A", env=env)
    if patch:
        patch_file = root / "candidate.patch"
        patch_file.write_bytes(patch)
        _run_git(checkout, "apply", "--binary", "--index", str(patch_file), env=env)
    tree = _run_git(checkout, "write-tree", env=env).decode("ascii").strip()
    return tree, checkout


def _files(root: Path, *, skip: frozenset[str] = frozenset()) -> dict[str, tuple[bytes, bool]]:
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


def _normalized_with_escaping_link(root: Path) -> tuple[Path, str, bytes]:
    """The agent's workspace, P and P's archive, for a P that commits an escaping link.

    Preparation's own steps run on the checkout: ``_drop_escaping_links`` unlinks
    ``out -> ../outside`` and marks it skip-worktree, ``archive_checkout`` writes
    the normalized checkout (its ``.git`` included) as the private base archive,
    and ``validate_workspace_archive`` admits it. The sandbox workspace is that
    archive extracted.
    """

    source, _ = _prepared(root / "preparation")
    os.symlink("../outside", source / "out")
    _git(source, "add", "out")
    prepared = _commit(source, "Prepared base with an escaping link")
    scratch = root / "preparation-scratch"
    scratch.mkdir()
    limits = WorkspaceLimits()
    preparer = WorkspacePreparer(
        credentials=None,
        commands=SubprocessCommands(),
        objects=None,
        scratch_root=scratch,
        limits=limits,
    )
    preparer._drop_escaping_links(source)
    archive = b"".join(preparer.archive_checkout(source, archive_path=scratch / "base.tar.gz"))
    validate_workspace_archive([archive], limits=limits, scratch_root=scratch)

    workspace = root / "sandbox" / "workspace"
    workspace.mkdir(parents=True)
    with tarfile.open(fileobj=BytesIO(archive), mode="r:gz") as extracted:
        names = extracted.getnames()
        extracted.extractall(workspace, filter="data")
    assert "out" not in names
    assert ".git/index" in names
    assert not (workspace / "out").exists() and not (workspace / "out").is_symlink()
    assert _git(workspace, "ls-files", "-v", "--", "out").startswith("S ")
    return workspace, prepared, archive


def test_a_dropped_escaping_link_beside_a_committed_edit_publishes_only_the_edit(
    tmp_path: Path,
) -> None:
    workspace, prepared, archive = _normalized_with_escaping_link(tmp_path)
    head = _commit_change(workspace, "src/app.py", b"x = 2\n")
    # The agent sees a clean checkout: the dropped link is hidden from git status.
    assert _git(workspace, "status", "--porcelain") == ""

    snapshot = _snapshot_over_http(workspace, prepared)

    assert snapshot.base_sha == prepared
    assert snapshot.changed_paths == ("src/app.py",)
    assert snapshot.patch.count(b"diff --git ") == 1
    assert b"diff --git a/out " not in snapshot.patch
    _validate(_coordinator(prepared, archive), snapshot, tmp_path / "validation")
    tree, checkout = _result(tmp_path / "result", archive, snapshot.patch)
    assert (checkout / "src" / "app.py").read_bytes() == b"x = 2\n"
    assert tree == _tree_of(workspace, head)


def test_committed_only_change_publishes_against_the_prepared_base(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    head = _commit_change(repo, "src/app.py", b"x = 2\n")
    assert head != prepared
    assert _git(repo, "status", "--porcelain") == ""

    snapshot = _snapshot_over_http(repo, prepared)

    assert snapshot.base_sha == prepared
    assert snapshot.changed_paths == ("src/app.py",)
    archive = _archive(repo, prepared)
    _validate(_coordinator(prepared, archive), snapshot, tmp_path / "validation")
    tree, checkout = _result(tmp_path / "result", archive, snapshot.patch)
    assert (checkout / "src" / "app.py").read_bytes() == b"x = 2\n"
    assert tree == _tree_of(repo, head)
    assert tree != _tree_of(repo, prepared)


def test_committed_and_uncommitted_equivalents_validate_to_one_result_tree(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    archive = _archive(repo, prepared)
    coordinator = _coordinator(prepared, archive)
    _write(repo, "src/app.py", b"x = 3\n")
    _write(repo, "notes.md", b"notes added by the agent\n")

    unstaged = _snapshot_over_http(repo, prepared)
    _git(repo, "add", "-A")
    staged = _snapshot_over_http(repo, prepared)
    head = _commit(repo, "Agent change")
    committed = _snapshot_over_http(repo, prepared)

    trees: list[str] = []
    for name, snapshot in (("unstaged", unstaged), ("staged", staged), ("committed", committed)):
        assert snapshot.base_sha == prepared
        _validate(coordinator, snapshot, tmp_path / f"validation-{name}")
        tree, _ = _result(tmp_path / f"result-{name}", archive, snapshot.patch)
        trees.append(tree)
    assert trees == [_tree_of(repo, head)] * 3
    assert unstaged.changed_paths == staged.changed_paths == committed.changed_paths


def test_mixed_changes_validate_to_the_agents_final_files(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    archive = _archive(repo, prepared)
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
    # Staged new, staged then modified, unstaged, binary and executable changes.
    _write(repo, "new/staged.txt", b"staged new file\n")
    _git(repo, "add", "new/staged.txt")
    _write(repo, "src/twice.py", b"twice = 'staged intermediate'\n")
    _git(repo, "add", "src/twice.py")
    _write(repo, "src/twice.py", b"twice = 'final'\n")
    _write(repo, "src/app.py", b"x = 2\n")
    _write(repo, "asset.bin", b"\x00\x01final asset\x00\xfe")
    (repo / "script.sh").chmod(0o755)
    # Untracked: one eligible, one ignored.
    _write(repo, "notes/new.txt", b"untracked note\n")
    _write(repo, "build/out.log", b"ignored output\n")

    snapshot = _snapshot_over_http(repo, prepared)

    assert snapshot.base_sha == prepared
    assert sorted(snapshot.changed_paths) == [
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
    ]
    _validate(_coordinator(prepared, archive), snapshot, tmp_path / "validation")
    _, checkout = _result(tmp_path / "result", archive, snapshot.patch)
    assert _files(checkout) == _files(repo, skip=frozenset({"build"}))


@pytest.mark.parametrize(
    ("path", "content", "refusal"),
    [
        pytest.param(
            ".github/workflows/x.yml", b"name: x\n", GITHUB_WORKFLOW_REFUSAL, id="workflow"
        ),
        pytest.param(
            "pyproject.toml", PYPROJECT_ADDED, DEPENDENCY_ADDITION_REFUSAL, id="dependency"
        ),
        pytest.param("src/app.py", b"x = 4\n", None, id="ordinary-control"),
    ],
)
def test_committed_protected_changes_meet_the_existing_worker_refusals(
    tmp_path: Path, path: str, content: bytes, refusal: str | None
) -> None:
    repo, prepared = _prepared(tmp_path)
    head = _commit_change(repo, path, content)
    assert _git(repo, "status", "--porcelain") == ""
    archive = _archive(repo, prepared)

    snapshot = _snapshot_over_http(repo, prepared)

    assert snapshot.base_sha == prepared
    assert snapshot.changed_paths == (path,)
    assert snapshot.contains_workflow_files is path.startswith(".github/workflows/")
    coordinator = _coordinator(prepared, archive)
    if refusal is None:
        _validate(coordinator, snapshot, tmp_path / "validation")
        tree, _ = _result(tmp_path / "result", archive, snapshot.patch)
        assert tree == _tree_of(repo, head)
        return
    with pytest.raises(WorkspacePreparationError) as refused:
        _validate(coordinator, snapshot, tmp_path / "validation")
    assert refusal in str(refused.value)
    if refusal == DEPENDENCY_ADDITION_REFUSAL:
        assert path in str(refused.value)


def test_the_worker_names_the_prepared_base_not_the_agents_descendant_head(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    for step in range(3):
        _commit_change(repo, f"steps/step-{step}.txt", f"step {step}\n".encode())
    head = _git(repo, "rev-parse", "HEAD")

    snapshot = _snapshot_over_http(repo, prepared)

    assert head != prepared
    assert snapshot.base_sha == prepared
    assert set(snapshot.changed_paths) == {f"steps/step-{step}.txt" for step in range(3)}


def test_a_snapshot_at_another_commit_is_refused_against_the_retained_base(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    head = _commit_change(repo, "src/app.py", b"x = 2\n")
    retained = _coordinator(prepared, _archive(repo, prepared))

    # The worker names a commit other than the one the coordinator retained.
    substituted = _snapshot_over_http(repo, head)

    assert substituted.base_sha == head
    with pytest.raises(WorkspacePreparationError, match="does not match sanitized base") as error:
        _validate(retained, substituted, tmp_path / "validation-substituted")
    assert f"snapshot commit {head[:12]}" in str(error.value)
    assert f"sanitized base {prepared[:12]}" in str(error.value)
    # Liveness: the retained base itself publishes the same committed change.
    _validate(retained, _snapshot_over_http(repo, prepared), tmp_path / "validation-retained")


def test_a_revision_publishes_only_its_delta_from_the_prior_publication_head(
    tmp_path: Path,
) -> None:
    repo, _original = _prepared(tmp_path)
    _write(repo, "src/feature.py", b"feature = 1\n")
    _write(repo, "README.md", b"readme from the prior publication\n")
    _git(repo, "add", "-A")
    prior_head = _commit(repo, "Prior publication")
    head = _commit_change(repo, "src/feature.py", b"feature = 2\n")
    archive = _archive(repo, prior_head)

    snapshot = _snapshot_over_http(repo, prior_head)

    assert snapshot.base_sha == prior_head
    assert snapshot.changed_paths == ("src/feature.py",)
    _validate(_coordinator(prior_head, archive), snapshot, tmp_path / "validation")
    tree, checkout = _result(tmp_path / "result", archive, snapshot.patch)
    assert tree == _tree_of(repo, head)
    assert (checkout / "README.md").read_bytes() == b"readme from the prior publication\n"


def test_a_change_committed_then_reverted_validates_as_an_empty_publication(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit_change(repo, "src/app.py", b"x = 2\n")
    _commit_change(repo, "src/app.py", _PREPARED_FILES["src/app.py"])
    archive = _archive(repo, prepared)

    snapshot = _snapshot_over_http(repo, prepared)

    assert snapshot.base_sha == prepared
    assert snapshot.patch == b""
    assert snapshot.changed_paths == ()
    _validate(_coordinator(prepared, archive), snapshot, tmp_path / "validation")
    tree, _ = _result(tmp_path / "result", archive, snapshot.patch)
    assert tree == _tree_of(repo, prepared)


def test_a_line_a_checkout_clean_filter_hides_from_git_diff_is_published_and_validated(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)
    archive = _archive(repo, prepared)
    # A checkout-local clean filter drops every HIDDEN line from git's view of src/app.py.
    _git(repo, "config", "filter.strip.clean", "grep -v HIDDEN || true")
    _git(repo, "config", "filter.strip.smudge", "cat")
    attributes = repo / ".git" / "info" / "attributes"
    attributes.parent.mkdir(exist_ok=True)
    attributes.write_text("src/app.py filter=strip\n")
    final = b"x = 1\n# HIDDEN: published, but never shown by git diff\n"
    _write(repo, "src/app.py", final)
    assert _git(repo, "diff", "HEAD") == ""

    snapshot = _snapshot_over_http(repo, prepared)

    # The snapshot publishes the raw bytes, not git's filtered view of them.
    assert snapshot.base_sha == prepared
    assert snapshot.changed_paths == ("src/app.py",)
    assert b"+# HIDDEN: published, but never shown by git diff" in snapshot.patch
    _validate(_coordinator(prepared, archive), snapshot, tmp_path / "validation")
    _, checkout = _result(tmp_path / "result", archive, snapshot.patch)
    assert (checkout / "src" / "app.py").read_bytes() == final
