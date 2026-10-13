"""The GitLab code host over the fake (#4399 AC 4; decisions 4, 11, 15 to 17 and 20).

The code host is built the production way (`forge_fakes.gitlab.gitlab_code_host`:
`build_registry`, then ``code_host(...).adapter``). Every request names the
project by its numeric id (decision 4).
"""

from __future__ import annotations

import pytest
from curie_api.forges import types
from curie_api.forges.errors import Ambiguous, NotFound, Unauthorized
from curie_api.forges.types import (
    Actor,
    Commit,
    CredentialScope,
    FeedbackKind,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
)
from forge_fakes.contract_harness import OUTSIDER, WRITER
from forge_fakes.gitlab import (
    BOT,
    HOST,
    PROJECT_ID,
    PROJECT_PATH,
    TOKEN,
    WEB,
    GitLabAPI,
    basic_oauth2_header,
    gitlab_code_host,
)
from forge_fakes.gitlab_harness import embed

PROJECT = f"/projects/{PROJECT_ID}"
BRANCH = "fix/#12%done"
ENCODED = "fix%2F%2312%25done"
MARKER = "curie-factory-status:00000000-0000-4000-8000-000000000017"
TOKEN_BOT = Actor(id="950", login="group_77_bot_0a1b2c")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> GitLabAPI:
    fake = GitLabAPI()
    for actor in (WRITER, OUTSIDER):
        fake.add_user(actor)
        fake.set_member(actor, 30)
    fake.add_user(TOKEN_BOT, bot=True)
    return fake


def _project_paths(fake: GitLabAPI) -> list[str]:
    return [p for p in map(fake.api_path, fake.requests) if p.startswith("/projects/")]


# Decision 4: the repository by id ---


@pytest.mark.anyio
async def test_resolve_repository_reads_the_project_by_numeric_id(fake: GitLabAPI) -> None:
    repository = await gitlab_code_host(fake).resolve_repository(str(PROJECT_ID))

    assert repository == RepositoryRef(types.GITLAB, HOST, str(PROJECT_ID), PROJECT_PATH)
    assert (repository.path, repository.default_branch) == (PROJECT_PATH, "main")
    assert _project_paths(fake) == [PROJECT]


@pytest.mark.anyio
async def test_an_unknown_project_is_not_found(fake: GitLabAPI) -> None:
    with pytest.raises(NotFound):
        await gitlab_code_host(fake).resolve_repository(str(PROJECT_ID + 1))


@pytest.mark.anyio
async def test_every_project_request_names_the_project_by_id(fake: GitLabAPI) -> None:
    code_host = gitlab_code_host(fake)
    repository = fake.repository()
    fake.push("factory/ids")
    pull = await code_host.open_pull_request(
        repository, head_ref="factory/ids", base_ref="main", title="T", body="B"
    )
    await code_host.find_pull_request(repository, head_ref="factory/ids")
    await code_host.read_pull_request(pull.ref)
    await code_host.observe_ci(repository, pull.head_sha)
    await code_host.list_review_feedback(pull.ref, None)
    await code_host.credential(repository, CredentialScope.CLONE)

    paths = _project_paths(fake)
    assert paths and all(p == PROJECT or p.startswith(f"{PROJECT}/") for p in paths)


# Decision 16: credential ---


@pytest.mark.anyio
async def test_a_renamed_project_refuses_the_credential_until_renamed_back(
    fake: GitLabAPI,
) -> None:
    code_host = gitlab_code_host(fake)
    repository = fake.repository()

    fake.rename_project("acme/platform/gadgets")
    with pytest.raises(NotFound) as renamed:
        await code_host.credential(repository, CredentialScope.PUSH)
    assert str(renamed.value) == "repository_renamed"

    fake.rename_project(PROJECT_PATH)
    credential = await code_host.credential(repository, CredentialScope.PUSH)
    assert credential.git_header() == basic_oauth2_header(TOKEN)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "clone_url",
    [
        f"https://elsewhere.example/{PROJECT_PATH}.git",
        f"http://{HOST}/{PROJECT_PATH}.git",
        f"https://{HOST}:8443/{PROJECT_PATH}.git",
        f"https://{HOST}.evil.example/{PROJECT_PATH}.git",
        f"https://{HOST}{PROJECT_PATH}.git",
    ],
)
async def test_a_clone_url_off_the_web_base_is_an_origin_mismatch(
    fake: GitLabAPI, clone_url: str
) -> None:
    fake.set_clone_url(clone_url)

    with pytest.raises(Unauthorized) as refused:
        await gitlab_code_host(fake).credential(fake.repository(), CredentialScope.CLONE)
    assert str(refused.value) == "origin_mismatch"
    assert TOKEN not in str(refused.value)


@pytest.mark.anyio
async def test_the_clone_url_on_the_web_base_gets_the_credential(fake: GitLabAPI) -> None:
    fake.set_clone_url(f"{WEB}/{PROJECT_PATH}.git")

    credential = await gitlab_code_host(fake).credential(fake.repository(), CredentialScope.CLONE)

    assert credential.origin == WEB
    assert credential.secret == TOKEN


# Branches and commits; decision 17's encoding ---


@pytest.mark.anyio
async def test_a_branch_with_hash_and_percent_is_percent_encoded_in_the_path(
    fake: GitLabAPI,
) -> None:
    sha = fake.push(BRANCH)

    assert await gitlab_code_host(fake).branch_head(fake.repository(), BRANCH) == sha

    (read,) = fake.calls("GET", f"{PROJECT}/repository/branches/{ENCODED}")
    assert read.url.raw_path.decode().endswith(f"/repository/branches/{ENCODED}")


@pytest.mark.anyio
async def test_a_missing_branch_has_no_head(fake: GitLabAPI) -> None:
    assert await gitlab_code_host(fake).branch_head(fake.repository(), "factory/none") is None


@pytest.mark.anyio
async def test_read_commit(fake: GitLabAPI) -> None:
    parent = fake.branches["main"]
    sha = fake.push("factory/commit", "Fix the widget\n\nLonger text.")

    commit = await gitlab_code_host(fake).read_commit(fake.repository(), sha)

    assert commit == Commit(sha=sha, parents=(parent,), message="Fix the widget\n\nLonger text.")


# Decision 17: merge requests ---


@pytest.mark.anyio
async def test_find_pull_request_lists_every_state_by_source_branch(fake: GitLabAPI) -> None:
    code_host = gitlab_code_host(fake)
    assert await code_host.find_pull_request(fake.repository(), head_ref=BRANCH) is None

    iid = fake.add_merge_request(BRANCH, title="Change")
    fake.set_mr_state(iid, "merged")
    found = await code_host.find_pull_request(fake.repository(), head_ref=BRANCH)

    assert found is not None
    assert (found.ref.number, found.state, found.head_ref) == (
        str(iid),
        PullRequestState.MERGED,
        BRANCH,
    )
    for listing in fake.calls("GET", f"{PROJECT}/merge_requests"):
        params = fake.params(listing)
        assert (params.get("source_branch"), params.get("state")) == (BRANCH, "all")


@pytest.mark.anyio
async def test_more_than_one_merge_request_from_a_branch_is_ambiguous(fake: GitLabAPI) -> None:
    first = fake.add_merge_request("factory/twice")
    fake.set_mr_state(first, "closed")
    fake.add_merge_request("factory/twice")

    with pytest.raises(Ambiguous):
        await gitlab_code_host(fake).find_pull_request(fake.repository(), head_ref="factory/twice")


@pytest.mark.anyio
@pytest.mark.parametrize("draft", [False, True])
async def test_open_pull_request_posts_the_merge_request(fake: GitLabAPI, draft: bool) -> None:
    head = fake.push("factory/open")

    pull = await gitlab_code_host(fake).open_pull_request(
        fake.repository(),
        head_ref="factory/open",
        base_ref="main",
        title="Factory change",
        body="Closes #7",
        draft=draft,
    )

    (post,) = fake.calls("POST", f"{PROJECT}/merge_requests")
    body = fake.body(post)
    assert body["source_branch"] == "factory/open"
    assert body["target_branch"] == "main"
    assert body["title"] == ("Draft: Factory change" if draft else "Factory change")
    assert body["description"] == "Closes #7"
    assert body["remove_source_branch"] in (False, "false")
    assert (pull.title, pull.draft, pull.body) == ("Factory change", draft, "Closes #7")
    assert (pull.head_sha, pull.head_ref, pull.base_ref) == (head, "factory/open", "main")
    assert pull.ref.number == str(fake.merge_requests[int(pull.ref.number)].iid)
    assert pull.url == f"{WEB}/{PROJECT_PATH}/-/merge_requests/{pull.ref.number}"


@pytest.mark.anyio
async def test_a_draft_title_reads_back_without_its_prefix(fake: GitLabAPI) -> None:
    iid = fake.add_merge_request("factory/draft", title="Draft: Factory change")

    pull = await gitlab_code_host(fake).read_pull_request(
        PullRequestRef(fake.repository(), str(iid))
    )

    assert (pull.title, pull.draft) == ("Factory change", True)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("gitlab_state", "state"),
    [
        ("opened", PullRequestState.OPEN),
        ("merged", PullRequestState.MERGED),
        ("closed", PullRequestState.CLOSED),
        ("locked", PullRequestState.CLOSED),
    ],
)
async def test_merge_request_states_map(
    fake: GitLabAPI, gitlab_state: str, state: PullRequestState
) -> None:
    iid = fake.add_merge_request("factory/state")
    if gitlab_state != "opened":
        fake.set_mr_state(iid, gitlab_state)

    pull = await gitlab_code_host(fake).read_pull_request(
        PullRequestRef(fake.repository(), str(iid))
    )

    assert pull.state is state


@pytest.mark.anyio
@pytest.mark.parametrize("conflicted", [False, True])
async def test_has_conflicts_is_conflicted(fake: GitLabAPI, conflicted: bool) -> None:
    iid = fake.add_merge_request("factory/conflict")
    fake.set_conflicts(iid, conflicted)

    pull = await gitlab_code_host(fake).read_pull_request(
        PullRequestRef(fake.repository(), str(iid))
    )

    assert pull.conflicted is conflicted


@pytest.mark.anyio
async def test_an_open_merge_request_follows_its_branch_head(fake: GitLabAPI) -> None:
    iid = fake.add_merge_request("factory/follow")
    code_host = gitlab_code_host(fake)
    ref = PullRequestRef(fake.repository(), str(iid))
    newer = fake.push("factory/follow")

    assert (await code_host.read_pull_request(ref)).head_sha == newer


@pytest.mark.anyio
async def test_update_pull_request_sends_only_what_changes(fake: GitLabAPI) -> None:
    iid = fake.add_merge_request("factory/update", title="Old", description="Old body")
    ref = PullRequestRef(fake.repository(), str(iid))

    pull = await gitlab_code_host(fake).update_pull_request(ref, title=None, body="New body")

    (put,) = fake.calls("PUT", f"{PROJECT}/merge_requests/{iid}")
    assert fake.body(put).get("description") == "New body"
    assert "title" not in fake.body(put)
    assert (pull.title, pull.body) == ("Old", "New body")


# Decision 11: write access ---


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("access_level", "state", "allowed"),
    [
        pytest.param(30, "active", True, id="developer"),
        pytest.param(40, "active", True, id="maintainer"),
        pytest.param(20, "active", False, id="reporter"),
        pytest.param(30, "blocked", False, id="blocked-developer"),
        pytest.param(None, "active", False, id="not-a-member"),
    ],
)
async def test_user_can_write_needs_developer_or_higher_and_an_active_user(
    fake: GitLabAPI, access_level: int | None, state: str, allowed: bool
) -> None:
    fake.set_member(OUTSIDER, access_level, state=state)

    assert await gitlab_code_host(fake).user_can_write(fake.repository(), OUTSIDER) is allowed
    assert fake.calls("GET", f"{PROJECT}/members/all/{OUTSIDER.id}")


# Decision 15: status comments on merge requests and threads ---


@pytest.mark.anyio
async def test_a_thread_reply_is_posted_once_in_its_discussion_and_edited_in_place(
    fake: GitLabAPI,
) -> None:
    iid = fake.add_merge_request("factory/thread")
    thread = fake.mr_discussion(iid, WRITER, "Why this way?")
    other = fake.mr_discussion(iid, WRITER, "And this?")
    # Our own marked reply in another thread is not this thread's.
    fake.discussion_reply(iid, other, BOT, embed(MARKER, "elsewhere"))
    target = ReplyTarget.on_thread(PullRequestRef(fake.repository(), str(iid)), thread)
    comments = gitlab_code_host(fake).marked_comments

    assert await comments.find_marked(target, MARKER) is None
    created = await comments.upsert_marked(target, MARKER, "Revised.")
    same = await comments.upsert_marked(target, MARKER, "Revised.")
    edited = await comments.upsert_marked(target, MARKER, "Revised again.")

    assert fake.calls("POST", f"{PROJECT}/merge_requests/{iid}/discussions/{thread}/notes")
    assert (created.written, same.written, edited.written) == (True, False, True)
    assert edited.comment.id == created.comment.id
    (put,) = [r for r in fake.requests if r.method == "PUT"]
    assert fake.api_path(put).endswith(f"/notes/{created.comment.id}")
    replies = [n for n in fake.thread_notes(thread)[1:]]
    assert [(n.author, "Revised again." in n.body) for n in replies] == [(int(BOT.id), True)]


@pytest.mark.anyio
async def test_a_person_s_note_carrying_the_marker_is_never_found_or_edited(
    fake: GitLabAPI,
) -> None:
    iid = fake.add_merge_request("factory/pasted")
    pasted = fake.mr_note(iid, WRITER, embed(MARKER, "pasted status"))
    target = ReplyTarget.on_pull_request(PullRequestRef(fake.repository(), str(iid)))
    comments = gitlab_code_host(fake).marked_comments

    assert await comments.find_marked(target, MARKER) is None
    created = await comments.upsert_marked(target, MARKER, "Working on it.")

    assert created.comment.id != str(pasted)
    assert fake.notes[pasted].body == embed(MARKER, "pasted status")
    assert fake.calls("POST", f"{PROJECT}/merge_requests/{iid}/notes")
    assert not fake.calls("PUT", f"{PROJECT}/merge_requests/{iid}/notes/{pasted}")


# Decision 20: review feedback ---


@pytest.mark.anyio
async def test_review_feedback_keeps_human_notes_and_types_them_by_discussion(
    fake: GitLabAPI,
) -> None:
    head = fake.push("factory/review")
    iid = fake.add_merge_request("factory/review")
    comment = fake.mr_note(iid, WRITER, "Top-level comment")
    thread = fake.mr_discussion(iid, OUTSIDER, "Diff comment")
    fake.mr_note(iid, BOT, "Our own status")
    fake.mr_note(iid, TOKEN_BOT, "A bot's note")
    fake.mr_note(iid, WRITER, "approved this merge request", system=True)
    ref = PullRequestRef(fake.repository(), str(iid))

    page = await gitlab_code_host(fake).list_review_feedback(ref, None)

    assert [(f.body, f.kind, f.author, f.thread_id) for f in page.items] == [
        ("Top-level comment", FeedbackKind.COMMENT, WRITER, None),
        ("Diff comment", FeedbackKind.REVIEW_COMMENT, OUTSIDER, thread),
    ]
    assert page.items[0].id == str(comment)
    assert all(f.head_sha == head and f.pull_request == ref for f in page.items)
    assert fake.calls("GET", f"{PROJECT}/merge_requests/{iid}/discussions")


@pytest.mark.anyio
async def test_the_feedback_cursor_is_the_largest_note_id_and_admits_later_replies(
    fake: GitLabAPI,
) -> None:
    iid = fake.add_merge_request("factory/cursor")
    thread = fake.mr_discussion(iid, WRITER, "First")
    ref = PullRequestRef(fake.repository(), str(iid))
    code_host = gitlab_code_host(fake)
    first = await code_host.list_review_feedback(ref, None)
    newest = max(fake.notes)
    assert first.cursor == str(newest)

    reply = fake.discussion_reply(iid, thread, OUTSIDER, "A reply in the old thread")
    later = await code_host.list_review_feedback(ref, first.cursor)

    assert [(f.id, f.kind, f.thread_id) for f in later.items] == [
        (str(reply), FeedbackKind.REVIEW_COMMENT, thread)
    ]
    assert later.cursor == str(reply)
    unchanged = await code_host.list_review_feedback(ref, later.cursor)
    assert (unchanged.items, unchanged.cursor) == ((), later.cursor)


@pytest.mark.anyio
async def test_verify_feedback_needs_the_same_body_and_author_on_an_open_merge_request(
    fake: GitLabAPI,
) -> None:
    iid = fake.add_merge_request("factory/verify")
    first = fake.mr_note(iid, WRITER, "Rename it")
    second = fake.mr_note(iid, WRITER, "And this")
    gone = fake.mr_note(iid, WRITER, "Deleted later")
    ref = PullRequestRef(fake.repository(), str(iid))
    code_host = gitlab_code_host(fake)
    items = {f.id: f for f in (await code_host.list_review_feedback(ref, None)).items}

    assert await code_host.verify_feedback(items[str(first)]) is True
    fake.edit_note(second, "Edited afterwards")
    assert await code_host.verify_feedback(items[str(second)]) is False
    fake.delete_note(gone)
    assert await code_host.verify_feedback(items[str(gone)]) is False

    fake.set_mr_state(iid, "closed")
    assert await code_host.verify_feedback(items[str(first)]) is False
