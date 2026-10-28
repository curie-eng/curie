"""Real Git replay, leased publication and private credential transport."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from curie_api.forges.types import CredentialHeader, RepositoryRef
from curie_api.repository_access import IssuedCredential

COMMIT_TIME = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
BRANCH = "curie/publication-0123456789abcdef0123456789abcdef"
SECRET = "factory-transport-fixture-value"


def git(path: Path, *arguments: str, check: bool = True) -> str:
    env = {
        **os.environ,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_AUTHOR_NAME": "Fixture Author",
        "GIT_AUTHOR_EMAIL": "author@example.com",
        "GIT_COMMITTER_NAME": "Fixture Committer",
        "GIT_COMMITTER_EMAIL": "committer@example.com",
        "GIT_AUTHOR_DATE": "2026-09-01T12:00:00+00:00",
        "GIT_COMMITTER_DATE": "2026-09-01T12:00:00+00:00",
    }
    result = subprocess.run(
        ["git", "-C", str(path), *arguments],
        env=env,
        capture_output=True,
        text=True,
        check=check,
        timeout=15,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


@dataclass(frozen=True)
class GitGraph:
    root: Path
    origin: Path
    writer: Path
    old_base: str
    target: str
    source: str
    first: str
    work: Path

    def head(self) -> str:
        return git(self.origin, "rev-parse", f"refs/heads/{BRANCH}")

    def commit(self, name: str, value: str, message: str) -> str:
        (self.writer / name).write_text(value)
        git(self.writer, "add", "--", name)
        git(self.writer, "commit", "-m", message)
        return git(self.writer, "rev-parse", "HEAD")

    def snapshot(self, **overrides: Any) -> Any:
        from curie_api.factory_rebase import RebaseSnapshot

        values = {
            "transition_id": uuid.UUID("11111111-2222-4333-8444-555555555555"),
            "repository": RepositoryRef("gitlab", "gitlab.example.com", "41", "acme-corp/acme-bot"),
            "branch": BRANCH,
            "old_base_sha": self.old_base,
            "old_head_sha": self.source,
            "target_ref": "next",
            "target_sha": self.target,
            "committer_name": "Factory",
            "committer_email": "factory@example.com",
            "committer_time": COMMIT_TIME,
            "max_seconds": 15.0,
            "max_bytes": 32 * 1024 * 1024,
            "work_dir": self.work,
        }
        values.update(overrides)
        return RebaseSnapshot(**values)

    def credential(self, clone_url: str | None = None) -> IssuedCredential:
        return IssuedCredential(
            clone_url=clone_url or str(self.origin),
            authorization_header=f"Bearer {SECRET}",
            origin=clone_url or str(self.origin),
            header_form=CredentialHeader.AUTHORIZATION_BEARER,
            ca_bundle_ref=None,
        )

    def assert_clean(self) -> None:
        assert not list(self.work.iterdir()), "private checkout or credential material survived"


@pytest.fixture
def graph(tmp_path: Path) -> GitGraph:
    origin = tmp_path / "origin.git"
    writer = tmp_path / "writer"
    work = tmp_path / "private"
    origin.mkdir()
    writer.mkdir()
    work.mkdir(mode=0o700)
    git(origin, "init", "--bare", "--initial-branch=main")
    git(writer, "init", "--initial-branch=main")
    git(writer, "remote", "add", "origin", str(origin))
    (writer / "shared.txt").write_text("shared\n")
    git(writer, "add", ".")
    git(writer, "commit", "-m", "Common ancestor")
    common = git(writer, "rev-parse", "HEAD")
    (writer / "old-only.txt").write_text("unrelated old base\n")
    git(writer, "add", ".")
    git(writer, "commit", "-m", "Old base only")
    old_base = git(writer, "rev-parse", "HEAD")
    git(writer, "checkout", "-b", BRANCH)
    (writer / "feature-one.txt").write_text("factory one\n")
    git(writer, "add", ".")
    git(writer, "commit", "-m", "Factory first change")
    first = git(writer, "rev-parse", "HEAD")
    (writer / "feature-two.txt").write_text("factory two\n")
    git(writer, "add", ".")
    git(writer, "commit", "-m", "Factory second change")
    source = git(writer, "rev-parse", "HEAD")
    git(writer, "checkout", "-b", "next", common)
    (writer / "target-only.txt").write_text("new base\n")
    git(writer, "add", ".")
    git(writer, "commit", "-m", "Target base only")
    target = git(writer, "rev-parse", "HEAD")
    git(writer, "push", "origin", "main", "next", BRANCH)
    git(writer, "checkout", BRANCH)
    return GitGraph(tmp_path, origin, writer, old_base, target, source, first, work)


@dataclass
class GitHttp:
    url: str
    origin: str
    requests: list[tuple[str, str]]
    headers: list[dict[str, str]]
    delay: threading.Event | None
    before_receive: Callable[[], None] | None = None
    fail_reads: bool = False

    @property
    def pushes(self) -> int:
        return sum(
            method == "POST" and path.endswith("/git-receive-pack")
            for method, path in self.requests
        )


@contextmanager
def git_http(graph: GitGraph, *, delayed: bool = False) -> Iterator[GitHttp]:
    """Git's documented smart HTTP CGI protocol, served by the real http-backend.

    https://git-scm.com/docs/git-http-backend
    """

    git(graph.origin, "config", "http.receivepack", "true")
    release = threading.Event() if delayed else None
    observed = GitHttp("", "", [], [], release)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            self.serve_git()

        def do_POST(self) -> None:
            self.serve_git()

        def serve_git(self) -> None:
            parsed = urlsplit(self.path)
            observed.requests.append((self.command, parsed.path))
            observed.headers.append({key.lower(): value for key, value in self.headers.items()})
            if self.command == "GET" and observed.fail_reads:
                self.send_error(503, "Unavailable")
                return
            if self.command == "POST" and parsed.path.endswith("/git-receive-pack"):
                if observed.before_receive is not None:
                    observed.before_receive()
            if release is not None:
                release.wait(5)
            size = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(size)
            env = {
                **os.environ,
                "GIT_PROJECT_ROOT": str(graph.root),
                "GIT_HTTP_EXPORT_ALL": "1",
                "PATH_INFO": parsed.path,
                "QUERY_STRING": parsed.query,
                "REQUEST_METHOD": self.command,
                "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                "CONTENT_LENGTH": str(size),
                "REMOTE_USER": "fixture",
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
            }
            reply = subprocess.run(
                ["git", "http-backend"], input=body, capture_output=True, env=env, timeout=15
            )
            header_bytes, _, response = reply.stdout.partition(b"\r\n\r\n")
            headers = []
            status = 200
            for line in header_bytes.decode("latin-1").splitlines():
                name, _, value = line.partition(":")
                if name.lower() == "status":
                    status = int(value.strip().split()[0])
                else:
                    headers.append((name, value.strip()))
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    observed.origin = f"http://127.0.0.1:{server.server_port}"
    observed.url = f"{observed.origin}/origin.git"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield observed
    finally:
        if release is not None:
            release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_rebase_replays_only_factory_changes_onto_the_exact_target(graph: GitGraph) -> None:
    from curie_api.factory_rebase import prepare_rebase, push_rebase

    snapshot = graph.snapshot()
    prepared = asyncio.run(prepare_rebase(snapshot, graph.credential()))
    assert graph.head() == graph.source
    graph.assert_clean()
    observed = asyncio.run(push_rebase(snapshot, graph.credential(), prepared))

    assert observed.head_sha == prepared.head_sha == graph.head()
    assert git(graph.origin, "merge-base", graph.target, observed.head_sha) == graph.target
    assert git(graph.origin, "rev-list", "--count", f"{graph.target}..{observed.head_sha}") == "2"
    assert git(graph.origin, "show", f"{observed.head_sha}:feature-one.txt") == "factory one"
    assert git(graph.origin, "show", f"{observed.head_sha}:feature-two.txt") == "factory two"
    assert git(graph.origin, "show", f"{observed.head_sha}:target-only.txt") == "new base"
    files = git(graph.origin, "ls-tree", "--name-only", observed.head_sha).splitlines()
    assert "old-only.txt" not in files
    messages = git(
        graph.origin, "log", "--format=%s", f"{graph.target}..{observed.head_sha}"
    ).splitlines()
    assert messages == ["Factory second change", "Factory first change"]
    graph.assert_clean()


def test_rebase_conflict_leaves_origin_unchanged_and_removes_checkout(graph: GitGraph) -> None:
    from curie_api.factory_rebase import RebaseRefused, prepare_rebase

    source = graph.commit("shared.txt", "factory edit\n", "Conflicting factory edit")
    git(graph.writer, "push", "origin", BRANCH)
    git(graph.writer, "checkout", "next")
    target = graph.commit("shared.txt", "target edit\n", "Conflicting target edit")
    git(graph.writer, "push", "origin", "next")
    before = git(graph.origin, "show-ref")

    with pytest.raises(RebaseRefused) as refused:
        asyncio.run(
            prepare_rebase(
                graph.snapshot(old_head_sha=source, target_sha=target), graph.credential()
            )
        )

    assert refused.value.code == "conflict"
    assert git(graph.origin, "show-ref") == before
    graph.assert_clean()


def test_rebase_push_refuses_a_moved_source_with_an_exact_lease(graph: GitGraph) -> None:
    from curie_api.factory_rebase import RebaseRefused, prepare_rebase, push_rebase

    prepared = asyncio.run(prepare_rebase(graph.snapshot(), graph.credential()))
    moved = graph.commit("human.txt", "concurrent work\n", "Concurrent source edit")
    git(graph.writer, "push", "origin", BRANCH)

    with pytest.raises(RebaseRefused) as refused:
        asyncio.run(push_rebase(graph.snapshot(), graph.credential(), prepared))

    assert refused.value.code == "head_changed"
    assert graph.head() == moved
    assert git(graph.origin, "show", f"{moved}:human.txt") == "concurrent work"
    graph.assert_clean()


def test_rebase_reconstruction_has_identical_commit_ids_and_preserves_authors(
    graph: GitGraph,
) -> None:
    from curie_api.factory_rebase import prepare_rebase, push_rebase

    first = asyncio.run(prepare_rebase(graph.snapshot(), graph.credential()))
    graph.assert_clean()
    second = asyncio.run(prepare_rebase(graph.snapshot(), graph.credential()))
    assert first.head_sha == second.head_sha
    asyncio.run(push_rebase(graph.snapshot(), graph.credential(), second))
    authors = git(
        graph.origin, "log", "--format=%an <%ae>", f"{graph.target}..{first.head_sha}"
    ).splitlines()
    assert authors == ["Fixture Author <author@example.com>"] * 2
    times = git(
        graph.origin, "log", "--format=%ct", f"{graph.target}..{first.head_sha}"
    ).splitlines()
    assert times == [str(int(COMMIT_TIME.timestamp()))] * 2
    graph.assert_clean()


def test_rebase_refuses_a_non_linear_factory_range(graph: GitGraph) -> None:
    from curie_api.factory_rebase import RebaseRefused, prepare_rebase

    git(graph.writer, "checkout", "-b", "human-side", graph.old_base)
    graph.commit("side.txt", "merge resolution matters\n", "Side change")
    git(graph.writer, "checkout", BRANCH)
    git(graph.writer, "merge", "--no-ff", "human-side", "-m", "Preserved human merge")
    source = git(graph.writer, "rev-parse", "HEAD")
    git(graph.writer, "push", "origin", BRANCH)

    with pytest.raises(RebaseRefused) as refused:
        asyncio.run(prepare_rebase(graph.snapshot(old_head_sha=source), graph.credential()))

    assert refused.value.code == "non_linear"
    assert graph.head() == source
    graph.assert_clean()


def test_rebase_keeps_already_upstream_and_original_empty_commits(graph: GitGraph) -> None:
    from curie_api.factory_rebase import prepare_rebase, push_rebase

    git(graph.writer, "commit", "--allow-empty", "-m", "Original empty factory change")
    source = git(graph.writer, "rev-parse", "HEAD")
    git(graph.writer, "push", "origin", BRANCH)
    git(graph.writer, "checkout", "next")
    git(graph.writer, "cherry-pick", graph.first)
    target = git(graph.writer, "rev-parse", "HEAD")
    git(graph.writer, "push", "origin", "next")
    snapshot = graph.snapshot(old_head_sha=source, target_sha=target)
    prepared = asyncio.run(prepare_rebase(snapshot, graph.credential()))
    asyncio.run(push_rebase(snapshot, graph.credential(), prepared))

    assert git(graph.origin, "rev-list", "--count", f"{target}..{prepared.head_sha}") == "3"
    replayed = git(
        graph.origin, "rev-list", "--reverse", f"{target}..{prepared.head_sha}"
    ).splitlines()
    assert git(graph.origin, "rev-parse", f"{replayed[0]}^{{tree}}") == git(
        graph.origin, "rev-parse", f"{target}^{{tree}}"
    )
    messages = git(
        graph.origin, "log", "--format=%s", f"{target}..{prepared.head_sha}"
    ).splitlines()
    assert messages == [
        "Original empty factory change",
        "Factory second change",
        "Factory first change",
    ]
    graph.assert_clean()


def test_final_ci_marker_is_deterministic_same_tree_and_exactly_leased(graph: GitGraph) -> None:
    from curie_api.factory_rebase import (
        prepare_ci_marker,
        prepare_rebase,
        push_ci_marker,
        push_rebase,
    )

    snapshot = graph.snapshot()
    credential = graph.credential()
    rebased = asyncio.run(prepare_rebase(snapshot, credential))
    asyncio.run(push_rebase(snapshot, credential, rebased))
    marker = asyncio.run(prepare_ci_marker(snapshot, credential, rebased.head_sha))
    reconstructed = asyncio.run(prepare_ci_marker(snapshot, credential, rebased.head_sha))
    assert marker.head_sha == reconstructed.head_sha != rebased.head_sha
    assert graph.head() == rebased.head_sha
    final = asyncio.run(push_ci_marker(snapshot, credential, marker, rebased.head_sha))

    assert final.head_sha == marker.head_sha == graph.head()
    assert git(graph.origin, "rev-parse", f"{final.head_sha}^") == rebased.head_sha
    assert git(graph.origin, "rev-parse", f"{final.head_sha}^{{tree}}") == git(
        graph.origin, "rev-parse", f"{rebased.head_sha}^{{tree}}"
    )
    assert str(snapshot.transition_id) in git(
        graph.origin, "show", "-s", "--format=%B", final.head_sha
    )
    graph.assert_clean()


@pytest.mark.parametrize(
    ("header_form", "value", "header"),
    [
        (CredentialHeader.AUTHORIZATION_BEARER, f"Bearer {SECRET}", "authorization"),
        (CredentialHeader.AUTHORIZATION_BASIC, f"Basic {SECRET}", "authorization"),
        (CredentialHeader.PRIVATE_TOKEN, SECRET, "private-token"),
    ],
)
def test_git_http_auth_never_enters_argv_config_logs_or_returned_artifacts(
    graph: GitGraph,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    header_form: CredentialHeader,
    value: str,
    header: str,
) -> None:
    from curie_api.factory_rebase import prepare_rebase, push_rebase

    real_git = shutil.which("git")
    assert real_git is not None
    commands = graph.root / "commands.jsonl"
    tools = graph.root / "observed-tools"
    tools.mkdir()
    wrapper = tools / "git"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        f"root = pathlib.Path({str(graph.work)!r})\n"
        "configs = [p.read_text(errors='replace') for p in root.rglob('config') if p.is_file()]\n"
        f"with open({str(commands)!r}, 'a') as output:\n"
        "    output.write(json.dumps({'argv': sys.argv, 'configs': configs}) + '\\n')\n"
        f"os.execv({real_git!r}, [{real_git!r}, *sys.argv[1:]])\n"
    )
    wrapper.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tools}{os.pathsep}{os.environ['PATH']}")
    with git_http(graph) as remote:
        credential = replace(
            graph.credential(remote.url),
            origin=remote.origin,
            header_form=header_form,
            authorization_header=value,
        )
        prepared = asyncio.run(prepare_rebase(graph.snapshot(), credential))
        result = asyncio.run(push_rebase(graph.snapshot(), credential, prepared))
        assert any(request.get(header) == value for request in remote.headers)
        assert remote.pushes == 1

    assert commands.is_file(), "the observer must see real Git subprocesses"
    observations = [json.loads(line) for line in commands.read_text().splitlines()]
    assert observations
    assert SECRET not in commands.read_text()
    assert SECRET not in caplog.text
    assert SECRET not in repr(prepared) + repr(result)
    assert all(
        SECRET not in path.read_text(errors="replace")
        for path in graph.work.rglob("*")
        if path.is_file()
    )
    graph.assert_clean()


def test_disk_limit_refuses_before_push_and_a_bounded_valid_replay_progresses(
    graph: GitGraph,
) -> None:
    from curie_api.factory_rebase import RebaseRefused, prepare_rebase

    with pytest.raises(RebaseRefused) as refused:
        asyncio.run(prepare_rebase(graph.snapshot(max_bytes=1), graph.credential()))
    assert refused.value.code == "disk_limit"
    assert graph.head() == graph.source
    graph.assert_clean()
    assert (
        asyncio.run(prepare_rebase(graph.snapshot(), graph.credential())).head_sha != graph.source
    )
    graph.assert_clean()


def test_elapsed_limit_terminates_real_git_and_a_later_healthy_replay_progresses(
    graph: GitGraph,
) -> None:
    from curie_api.factory_rebase import RebaseRefused, prepare_rebase

    with git_http(graph, delayed=True) as remote:
        credential = replace(graph.credential(remote.url), origin=remote.origin)
        started = time.monotonic()
        with pytest.raises(RebaseRefused) as refused:
            asyncio.run(prepare_rebase(graph.snapshot(max_seconds=0.2), credential))
        assert refused.value.code == "elapsed_limit"
        assert time.monotonic() - started < 3
        assert remote.requests, "the bound must interrupt a real in-flight Git request"
        assert graph.head() == graph.source
        graph.assert_clean()
        assert remote.delay is not None
        remote.delay.set()
        assert asyncio.run(prepare_rebase(graph.snapshot(), credential)).head_sha != graph.source
    graph.assert_clean()
