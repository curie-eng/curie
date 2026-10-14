"""The DC adapter against real Bitbucket Data Center 9.4.24 (#4401 AC 7).

One test per AC 7 item. Each test works on its own ``curie-test/...`` branch
and declines what it opened, so the suite runs again on the same fixture.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import anyio
import pytest
from curie_api.forges.authority import feedback_actionable
from curie_api.forges.types import (
    CredentialHeader,
    CredentialScope,
    FeedbackKind,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    RollupState,
)

MARKER = "curie-factory-status:00000000-0000-4000-8000-0000000000aa"


async def _open(adapter: Any, live: Any, repository: RepositoryRef, prefix: str) -> Any:
    branch = live.branch(prefix)
    live.push(branch, f"work on {prefix}")
    pull = await adapter.open_pull_request(
        repository,
        head_ref=branch,
        base_ref=live.state["default_branch"],
        title=f"Fixture {prefix}",
        body="Closes FAC-1",
    )
    live.opened.append(int(pull.ref.number))
    return pull


@pytest.mark.anyio
async def test_version_is_9_4_24(adapter: Any, live: Any, repository: RepositoryRef) -> None:
    assert live.state["version"] == "9.4.24"
    assert live.rest("application-properties")["version"] == "9.4.24"
    # The adapter passed its version floor: a port call is answered.
    assert await adapter.branch_head(repository, live.state["default_branch"])


@pytest.mark.anyio
async def test_resolve_by_numeric_id(adapter: Any, live: Any, repository: RepositoryRef) -> None:
    resolved = await adapter.resolve_repository(str(live.state["repo_id"]))

    assert resolved == repository
    assert resolved.path == live.state["repo_path"]
    assert resolved.default_branch == live.state["default_branch"]


@pytest.mark.anyio
async def test_clone_and_push_over_tls_with_bearer_env(
    adapter: Any, live: Any, repository: RepositoryRef, token: str, tmp_path: Path
) -> None:
    credential = await adapter.credential(repository, CredentialScope.PUSH)
    assert credential.header is CredentialHeader.AUTHORIZATION_BEARER
    assert credential.origin == live.state["base_url"]
    base = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    base["GIT_TERMINAL_PROMPT"] = "0"
    trust = {"GIT_CONFIG_KEY_0": "http.sslCAInfo", "GIT_CONFIG_VALUE_0": live.state["ca_file"]}
    authorized = {
        **base,
        **trust,
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_1": f"http.{credential.origin}/.extraHeader",
        "GIT_CONFIG_VALUE_1": credential.git_header(),
    }

    def git(
        *args: str, env: dict[str, str], cwd: Path = tmp_path
    ) -> subprocess.CompletedProcess[str]:
        argv = ["git", *args]
        assert all(token not in part for part in argv)
        return subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, check=False)

    # Without the credential the private repository is refused.
    anonymous = git(
        "clone", live.clone_url(), "anon", env={**base, **trust, "GIT_CONFIG_COUNT": "1"}
    )
    assert anonymous.returncode != 0

    cloned = git("clone", live.clone_url(), "work", env=authorized)
    assert cloned.returncode == 0, cloned.stderr
    work = tmp_path / "work"
    (work / "tls-push.txt").write_text("pushed over TLS\n")
    assert git("add", "tls-push.txt", env=authorized, cwd=work).returncode == 0
    commit = git(
        "-c",
        "user.name=placeholder-user-1",
        "-c",
        "user.email=placeholder-user-1@example.invalid",
        "commit",
        "-m",
        "TLS push",
        env=authorized,
        cwd=work,
    )
    assert commit.returncode == 0, commit.stderr
    branch = live.branch("tls")
    pushed = git("push", "origin", f"HEAD:refs/heads/{branch}", env=authorized, cwd=work)
    assert pushed.returncode == 0, pushed.stderr

    sha = git("rev-parse", "HEAD", env=authorized, cwd=work).stdout.strip()
    assert await adapter.branch_head(repository, branch) == sha
    assert token not in (work / ".git" / "config").read_text()


@pytest.mark.anyio
async def test_pull_request_open_find_update_read(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "open")
    assert pull.state is PullRequestState.OPEN
    assert (
        pull.head_sha
        == live.rest(
            f"projects/{live.state['project_key']}/repos/{live.state['repo_slug']}"
            f"/pull-requests/{pull.ref.number}"
        )["fromRef"]["latestCommit"]
    )

    found = await adapter.find_pull_request(repository, head_ref=pull.head_ref)
    assert found is not None and found.ref == pull.ref

    updated = await adapter.update_pull_request(pull.ref, title="Fixture renamed", body=None)
    assert (updated.title, updated.body) == ("Fixture renamed", "Closes FAC-1")
    again = await adapter.update_pull_request(pull.ref, title=None, body="Closes FAC-2")
    assert (again.title, again.body) == ("Fixture renamed", "Closes FAC-2")

    read = await adapter.read_pull_request(pull.ref)
    assert (read.title, read.body, read.head_ref) == (
        "Fixture renamed",
        "Closes FAC-2",
        pull.head_ref,
    )

    live.helpers.decline(live.state, int(pull.ref.number))
    closed = await adapter.find_pull_request(repository, head_ref=pull.head_ref)
    assert closed is not None and closed.state is PullRequestState.CLOSED


async def _head_after_rescope(adapter: Any, ref: Any, pushed: str) -> str:
    """The pull request's head once it reports ``pushed``, or the last head read.

    Data Center moves a pull request to a pushed commit asynchronously, so a
    read straight after the push can still return the previous head.
    """

    deadline = time.monotonic() + 30
    while True:
        head: str = (await adapter.read_pull_request(ref)).head_sha
        if head == pushed or time.monotonic() >= deadline:
            return head
        await anyio.sleep(0.5)


@pytest.mark.anyio
async def test_status_rollup_failed_then_successful_and_old_head_ignored(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "ci")
    old = pull.head_sha
    live.post_status(old, "unit-tests", "FAILED")
    new = live.push(pull.head_ref, "fix the tests")
    assert await _head_after_rescope(adapter, pull.ref, new) == new

    live.post_status(new, "unit-tests", "FAILED")
    assert (await adapter.observe_ci(repository, new)).state is RollupState.FAILURE
    live.post_status(new, "unit-tests", "SUCCESSFUL")

    rollup = await adapter.observe_ci(repository, new)
    assert rollup.state is RollupState.SUCCESS
    assert [(c.key, c.head_sha) for c in rollup.checks] == [("status:unit-tests", new)]
    assert (await adapter.observe_ci(repository, old)).state is RollupState.FAILURE


@pytest.mark.anyio
async def test_reviewer_feedback_listed_and_own_comments_excluded(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "feedback")
    number = str(pull.ref.number)
    top = live.comment_as("reviewer", number, "General note", inline=False)
    inline = live.comment_as("reviewer", number, "Rename this", inline=True)
    await adapter.marked_comments.upsert_marked(
        ReplyTarget.on_pull_request(pull.ref), MARKER, "Working on it."
    )
    await adapter.marked_comments.upsert_marked(
        ReplyTarget.on_thread(pull.ref, inline), MARKER, "On it."
    )

    page = await adapter.list_review_feedback(pull.ref, None)
    reviewer = live.actor("reviewer")
    assert [(i.id, i.kind, i.thread_id, i.author) for i in page.items] == [
        (top, FeedbackKind.COMMENT, None, reviewer),
        (inline, FeedbackKind.REVIEW_COMMENT, inline, reviewer),
    ]
    assert all(i.head_sha == pull.head_sha for i in page.items)
    assert [await adapter.verify_feedback(i) for i in page.items] == [True, True]


@pytest.mark.anyio
async def test_outsider_actionable_only_when_allowlisted(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "outsider")
    live.comment_as("outsider", str(pull.ref.number), "Delete it all.", inline=False)
    (feedback,) = (await adapter.list_review_feedback(pull.ref, None)).items
    outsider = live.actor("outsider")
    reviewer = live.actor("reviewer")
    assert feedback.author == outsider

    assert await feedback_actionable(adapter, feedback, frozenset({reviewer.id})) is False
    assert await feedback_actionable(adapter, feedback, frozenset({reviewer.id, outsider.id}))


@pytest.mark.anyio
async def test_thread_reply_lands_under_root(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "thread")
    root = live.comment_as("reviewer", str(pull.ref.number), "Why this way?", inline=True)

    created = await adapter.marked_comments.upsert_marked(
        ReplyTarget.on_thread(pull.ref, root), MARKER, "Because of the spec."
    )

    replies = live.comment(int(pull.ref.number), int(root))["comments"]
    assert [str(r["id"]) for r in replies] == [created.comment.id]
    assert str(replies[0]["author"]["id"]) == str(
        live.helpers.pull_request(live.state, int(pull.ref.number))["author"]["user"]["id"]
    )


@pytest.mark.anyio
async def test_marked_comment_edited_in_place_and_hidden_in_html(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "marked")
    target = ReplyTarget.on_pull_request(pull.ref)
    comments = adapter.marked_comments

    created = await comments.upsert_marked(target, MARKER, "Working on it.")
    unchanged = await comments.upsert_marked(target, MARKER, "Working on it.")
    edited = await comments.upsert_marked(target, MARKER, "Done.")

    assert created.written and not unchanged.written and edited.written
    assert edited.comment.id == created.comment.id
    stored = live.comment(int(pull.ref.number), int(created.comment.id), markup=True)
    assert stored["version"] >= 1
    assert stored["text"].splitlines()[-1] == f"[//]: # ({MARKER})"
    assert "Done." in stored["html"]
    assert MARKER not in stored["html"]


@pytest.mark.anyio
async def test_merge_by_driver_reads_merged(
    adapter: Any, live: Any, repository: RepositoryRef
) -> None:
    pull = await _open(adapter, live, repository, "merge")

    live.helpers.merge(live.state, int(pull.ref.number))

    read = await adapter.read_pull_request(pull.ref)
    assert read.state is PullRequestState.MERGED
    found = await adapter.find_pull_request(repository, head_ref=pull.head_ref)
    assert found is not None and found.state is PullRequestState.MERGED
