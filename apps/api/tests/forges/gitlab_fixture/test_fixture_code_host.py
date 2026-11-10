"""The GitLab code host against real GitLab CE 19.4.1 (#4399 AC 6, decisions 15 to 20).

Select with ``-k code_host``. Every branch, merge request and note is new and
uniquely named; the ``renamable`` project is renamed back in ``finally``. CI
runs on the fixture's shell runner: ``widgets``'s seeded ``add`` subtracts, so
a branch pushed as is goes red and one that fixes that line goes green.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import anyio
import pytest
from curie_api.forges.errors import NotFound, Unavailable
from curie_api.forges.gitlab.comments import GitLabMarkedComments
from curie_api.forges.types import (
    CheckSource,
    CiRollup,
    Credential,
    CredentialHeader,
    CredentialScope,
    PullRequest,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    RerunJob,
    RerunOutcome,
    RollupState,
)

from .conftest import Fixture, encode, unique

pytestmark = pytest.mark.anyio

MARKER = "curie-factory-status:00000000-0000-4000-8000-0000000000ab"
CI_TIMEOUT_S = 300.0
BUGGY = "add() { echo $(( $1 - $2 )); }"
FIXED = "add() { echo $(( $1 + $2 )); }"


class Clone:
    """A clone of ``widgets`` that pushes with the factory's push credential."""

    def __init__(self, fixture: Fixture, credential: Credential, name: str) -> None:
        self.fixture = fixture
        self.credential = credential
        self.path = fixture.work / name
        url = fixture.state["projects"]["widgets"]["http_url_to_repo"]
        fixture.git(credential, "clone", "--quiet", url, str(self.path))

    def git(self, *args: str) -> str:
        return self.fixture.git(self.credential, *args, cwd=self.path)

    def commit(self, branch: str, files: dict[str, str], message: str) -> str:
        """Commit ``files`` on ``branch`` (from ``main`` when new) and push it."""

        current = self.git("rev-parse", "--abbrev-ref", "HEAD").strip()
        if current != branch:
            self.git("checkout", "--quiet", "-B", branch, "origin/main")
        for name, text in files.items():
            (self.path / name).write_text(text)
            self.git("add", name)
        self.git("commit", "--quiet", "-m", message)
        self.git("push", "--quiet", "origin", f"HEAD:refs/heads/{branch}")
        return self.git("rev-parse", "HEAD").strip()

    def fix_widget(self, branch: str) -> str:
        widget = (self.path / "widget.sh").read_text()
        assert BUGGY in widget
        return self.commit(branch, {"widget.sh": widget.replace(BUGGY, FIXED)}, "Fix add")


async def _clone(fixture: Fixture, code_host: Any, name: str) -> Clone:
    credential = await code_host.credential(fixture.repository(), CredentialScope.PUSH)
    return Clone(fixture, credential, name)


async def _settled(
    code_host: Any,
    repository: RepositoryRef,
    head: str,
    done: Callable[[CiRollup], bool] | None = None,
) -> CiRollup:
    """The rollup on ``head`` once ``done`` holds (default: no check pending)."""

    def finished(rollup: CiRollup) -> bool:
        return rollup.state not in (RollupState.PENDING, RollupState.NONE)

    check = done or finished
    deadline = time.monotonic() + CI_TIMEOUT_S
    while True:
        rollup: CiRollup = await code_host.observe_ci(repository, head)
        if check(rollup):
            return rollup
        if time.monotonic() >= deadline:
            pytest.fail(f"CI on {head[:12]} did not settle in {CI_TIMEOUT_S:.0f} s: {rollup}")
        await anyio.sleep(3)


async def _open_merge_request(
    code_host: Any, repository: RepositoryRef, branch: str, title: str, body: str
) -> PullRequest:
    """Open the merge request, retrying while GitLab has not yet seen the push.

    GitLab refreshes its branch name cache after a push asynchronously, so a
    merge request opened straight after one is refused with 400
    ``{"source_branch": ["does not exist"]}`` for a moment, even while
    ``GET /repository/branches/:branch`` already answers. The adapter reads
    that as ``Unavailable("rejected")``, which the core retries.
    """

    deadline = time.monotonic() + 60
    while True:
        try:
            pull: PullRequest = await code_host.open_pull_request(
                repository, head_ref=branch, base_ref="main", title=title, body=body
            )
            return pull
        except Unavailable as refused:
            if str(refused) != "rejected" or time.monotonic() >= deadline:
                raise
            await anyio.sleep(1)


async def _open(fixture: Fixture, code_host: Any, clone: Clone, prefix: str) -> PullRequest:
    branch = unique(f"curie-test/{prefix}")
    clone.commit(branch, {f"{prefix}.txt": f"{prefix}\n"}, f"Work on {prefix}")
    return await _open_merge_request(
        code_host, fixture.repository(), branch, f"Fixture {prefix}", "Fixture change."
    )


def _close(fixture: Fixture, pull: PullRequest) -> None:
    fixture.root.call(
        "PUT",
        f"{fixture.project}/merge_requests/{pull.ref.number}",
        json={"state_event": "close"},
    )


async def test_code_host_credential_refuses_a_renamed_project_until_renamed_back(
    fixture: Fixture,
) -> None:
    repository = fixture.repository("renamable")
    project = f"/projects/{repository.project_id}"
    original = repository.path.rsplit("/", 1)[1]
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        before = await code_host.credential(repository, CredentialScope.CLONE)
        assert before.header is CredentialHeader.AUTHORIZATION_BASIC
        assert (before.username, before.origin) == ("oauth2", fixture.state["local_base_url"])
        try:
            fixture.root.call("PUT", project, json={"path": unique(original)})
            with pytest.raises(NotFound) as refused:
                await code_host.credential(repository, CredentialScope.PUSH)
            assert str(refused.value) == "repository_renamed"
        finally:
            fixture.root.call("PUT", project, json={"path": original})
        assert fixture.root.call("GET", project)["path_with_namespace"] == repository.path
        after = await code_host.credential(repository, CredentialScope.PUSH)
        assert after.git_header() == before.git_header()


async def test_code_host_clone_and_push_with_the_basic_header_in_the_environment(
    fixture: Fixture,
) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        repository = fixture.repository()
        clone = await _clone(fixture, code_host, "push")
        branch = unique("curie-test/push")

        sha = clone.commit(branch, {"push.txt": "pushed\n"}, "Push over TLS")

        assert clone.credential.git_header().startswith("Authorization: Basic ")
        assert await code_host.branch_head(repository, branch) == sha
        assert clone.credential.secret not in (clone.path / ".git" / "config").read_text()


async def test_code_host_merge_request_opens_with_the_closing_reference(
    fixture: Fixture,
) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        tracker = fixture.tracker(client, label=unique("factory"))
        repository = fixture.repository()
        iid = fixture.open_issue(unique("closes"))
        closing = await tracker.closing_reference(fixture.issue_ref(iid), repository)
        assert closing == f"Closes #{iid}"
        clone = await _clone(fixture, code_host, "closes")
        branch = unique("curie-test/closes")
        head = clone.commit(branch, {"closes.txt": "closes\n"}, "Close the issue")

        pull = await _open_merge_request(
            code_host, repository, branch, "Fixture closes", f"Factory change.\n\n{closing}"
        )
        try:
            assert (pull.state, pull.head_sha, pull.head_ref) == (
                PullRequestState.OPEN,
                head,
                branch,
            )
            assert pull.body.endswith(closing)
            # GitLab links the closed issues asynchronously after the create.
            closes = fixture.wait(
                "the merge request to list the issue it closes",
                lambda: fixture.root.call(
                    "GET", f"{fixture.project}/merge_requests/{pull.ref.number}/closes_issues"
                ),
                timeout_s=60,
                every_s=1,
            )
            assert [issue["iid"] for issue in closes] == [iid]
            found = await code_host.find_pull_request(repository, head_ref=branch)
            assert found is not None and found.ref == pull.ref
        finally:
            _close(fixture, pull)


async def test_code_host_ci_red_then_green_with_diagnostics_and_rerun(fixture: Fixture) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        repository = fixture.repository()
        clone = await _clone(fixture, code_host, "ci")
        pull = await _open(fixture, code_host, clone, "ci")
        red = pull.head_sha
        try:
            failed = await _settled(code_host, repository, red)
            assert failed.state is RollupState.FAILURE
            (job,) = failed.checks
            assert (job.key, job.source, job.native, job.reported_state) == (
                "test",
                CheckSource.RUN,
                True,
                "failed",
            )

            report = await code_host.ci_diagnostics(repository, red, base_ref="main")
            (diagnostic,) = report.diagnostics
            assert diagnostic.check_id == job.check_id
            assert "FAIL: add 2 3 = -1, want 5" in diagnostic.excerpt
            assert report.base is not None and report.base.state is RollupState.FAILURE

            record = await code_host.rerun_failed(repository, [RerunJob.of(job)])
            assert [(a.unit, a.outcome) for a in record.attempts] == [
                (job.check_id, RerunOutcome.ACCEPTED)
            ]
            retried = await _settled(
                code_host,
                repository,
                red,
                lambda r: (
                    all(c.check_id != job.check_id for c in r.checks)
                    and r.state is RollupState.FAILURE
                ),
            )
            (rerun,) = retried.checks
            assert rerun.key == "test" and rerun.check_id != job.check_id
            old = fixture.root.call("GET", f"{fixture.project}/jobs/{job.check_id}")
            assert (old["status"], old["name"]) == ("failed", "test")

            green = clone.fix_widget(pull.head_ref)
            passed = await _settled(code_host, repository, green)
            assert passed.state is RollupState.SUCCESS
            assert {c.head_sha for c in passed.checks} == {green}
            assert {c.check_id for c in passed.checks}.isdisjoint({job.check_id, rerun.check_id})
            assert (await code_host.read_pull_request(pull.ref)).head_sha == green
            # The red head keeps its own verdict.
            assert (await code_host.observe_ci(repository, red)).state is RollupState.FAILURE
        finally:
            _close(fixture, pull)


async def test_code_host_external_status_is_a_status_key(fixture: Fixture) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        repository = fixture.repository()
        clone = await _clone(fixture, code_host, "status")
        branch = unique("curie-test/status")
        head = clone.commit(branch, {"status.txt": "status\n"}, "External status")
        name = unique("external-ci")
        # The push pipeline is created asynchronously. Read before it is listed
        # under the head but after its job's commit status exists, that job
        # would show as an external ``status:test``, so wait for it first.
        await _settled(
            code_host,
            repository,
            head,
            lambda r: any(c.source is CheckSource.RUN for c in r.checks),
        )

        fixture.root.call(
            "POST",
            f"{fixture.project}/statuses/{head}",
            sudo=fixture.username("developer"),
            json={"state": "failed", "name": name, "ref": branch},
        )

        rollup = await code_host.observe_ci(repository, head)
        external = [c for c in rollup.checks if c.source is CheckSource.STATUS]
        assert [(c.key, c.state.value, c.native, c.head_sha) for c in external] == [
            (f"status:{name}", "failure", False, head)
        ]
        assert rollup.state is RollupState.FAILURE


async def test_code_host_thread_reply_is_posted_once_and_edited_in_place(
    fixture: Fixture,
) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        clone = await _clone(fixture, code_host, "thread")
        pull = await _open(fixture, code_host, clone, "thread")
        mr = f"{fixture.project}/merge_requests/{pull.ref.number}"
        try:
            thread = fixture.root.call(
                "POST",
                f"{mr}/discussions",
                sudo=fixture.username("developer"),
                json={"body": "Why this way?"},
            )
            target = ReplyTarget.on_thread(pull.ref, thread["id"])
            comments = code_host.marked_comments

            first = await comments.upsert_marked(target, MARKER, "Because of the spec.")
            same = await comments.upsert_marked(target, MARKER, "Because of the spec.")
            edited = await comments.upsert_marked(target, MARKER, "Because of the new spec.")

            assert (first.written, same.written, edited.written) == (True, False, True)
            assert first.comment.id == same.comment.id == edited.comment.id
            found = await comments.find_marked(target, MARKER)
            assert found is not None and found.id == first.comment.id
            notes = fixture.root.call("GET", f"{mr}/discussions/{encode(thread['id'])}")["notes"]
            bot_id = fixture.state["users"]["service_account"]["id"]
            assert [(n["author"]["id"], n["body"]) for n in notes] == [
                (fixture.state["users"]["developer"]["id"], "Why this way?"),
                (bot_id, GitLabMarkedComments.embed(MARKER, "Because of the new spec.")),
            ]
        finally:
            _close(fixture, pull)


async def test_code_host_human_note_carrying_the_marker_is_never_found_or_edited(
    fixture: Fixture,
) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        clone = await _clone(fixture, code_host, "pasted")
        pull = await _open(fixture, code_host, clone, "pasted")
        mr = f"{fixture.project}/merge_requests/{pull.ref.number}"
        pasted = GitLabMarkedComments.embed(MARKER, "Pasted by a person.")
        try:
            human = fixture.root.call(
                "POST", f"{mr}/notes", sudo=fixture.username("developer"), json={"body": pasted}
            )
            target = ReplyTarget.on_pull_request(pull.ref)
            comments = code_host.marked_comments

            assert await comments.find_marked(target, MARKER) is None
            written = await comments.upsert_marked(target, MARKER, "The factory's own.")

            assert written.written is True and written.comment.id != str(human["id"])
            again = fixture.root.call("GET", f"{mr}/notes/{human['id']}")
            assert (again["body"], again["updated_at"]) == (pasted, human["updated_at"])
            found = await comments.find_marked(target, MARKER)
            assert found is not None and found.id == written.comment.id
        finally:
            _close(fixture, pull)


async def test_code_host_user_can_write_only_from_developer_up(fixture: Fixture) -> None:
    async with fixture.client() as client:
        code_host = fixture.code_host(client)
        repository = fixture.repository()
        assert await code_host.user_can_write(repository, fixture.actor("reporter")) is False
        assert await code_host.user_can_write(repository, fixture.actor("developer")) is True
