"""Pure core sites a gitlab+gitlab install reaches (#4399 AC 8), each beside the
GitHub (or Jira) answer that must keep holding.

1. `forges.hosts.resolve_stored` reads a non-GitHub repository by its numeric
   project id (decision 4: GitLab names projects by id, never by path) and
   refuses one whose path moved; GitHub still resolves by ``path:<owner/name>``
   and refuses a changed id.
2. `forges.trackers.comment_prefix` joins an issue link and a GitLab note as
   ``#note_<id>``, GitLab's own anchor, so a mention's objective counts as a
   follow-up; GitHub and Jira keep their prefixes.
3. The worker's publication snapshot (`schemas.publications.PublicationCreate`)
   accepts a nested GitLab project path and still refuses a malformed one.
4. The tracker's answers (#5789 decisions 1 to 3 and 6): on an ``answering``
   issue every note by a person is a `Disposition.ANSWER` notice, mention or
   not; elsewhere a plain note is nothing; a system note, the token's own note
   and a bot's note never are; an answer is current only while its note stands
   on the open, labelled issue.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.factory_reply_target import is_issue_follow_up
from curie_api.forges import types
from curie_api.forges.errors import NotFound
from curie_api.forges.hosts import resolve_stored
from curie_api.forges.trackers import comment_prefix, mention_comment_id, mention_objective
from curie_api.forges.types import Actor, Disposition, MarkedNotice, NoticePage, RepositoryRef
from curie_api.schemas.publications import PublicationCreate
from forge_fakes.contract_harness import OUTSIDER, WRITER
from forge_fakes.gitlab import BOT, LABEL, GitLabAPI, gitlab_tracker
from pydantic import ValidationError

GITLAB_HOST = "gitlab.example"
GITLAB_PATH = "acme/platform/widgets"


class _Resolving:
    """A code host whose `resolve_repository` answers ``answer`` and records the id asked."""

    def __init__(self, answer: RepositoryRef) -> None:
        self.answer = answer
        self.asked: list[str] = []

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        self.asked.append(project_id)
        return self.answer


def _resolve(code_host: _Resolving, stored: RepositoryRef) -> RepositoryRef:
    return asyncio.run(resolve_stored(code_host, stored))  # type: ignore[arg-type]


# 1. resolve_stored ------------------------------------------------------------------------


def test_gitlab_core_resolve_stored_reads_a_gitlab_project_by_its_numeric_id() -> None:
    stored = RepositoryRef(types.GITLAB, GITLAB_HOST, "4242", GITLAB_PATH)
    current = RepositoryRef(types.GITLAB, GITLAB_HOST, "4242", GITLAB_PATH, default_branch="main")
    code_host = _Resolving(current)

    assert _resolve(code_host, stored) == current
    assert code_host.asked == ["4242"]


def test_gitlab_core_resolve_stored_refuses_a_gitlab_project_whose_path_moved() -> None:
    stored = RepositoryRef(types.GITLAB, GITLAB_HOST, "4242", GITLAB_PATH)
    moved = RepositoryRef(types.GITLAB, GITLAB_HOST, "4242", "acme/platform/renamed")

    with pytest.raises(NotFound):
        _resolve(_Resolving(moved), stored)


def test_gitlab_core_resolve_stored_still_reads_github_by_path_and_checks_the_id() -> None:
    stored = RepositoryRef(types.GITHUB, "github.com", "812345678", "acme-corp/api")
    same = RepositoryRef(types.GITHUB, "github.com", "812345678", "acme-corp/api")
    code_host = _Resolving(same)

    assert _resolve(code_host, stored) == same
    assert code_host.asked == ["path:acme-corp/api"]

    recreated = RepositoryRef(types.GITHUB, "github.com", "999", "acme-corp/api")
    with pytest.raises(NotFound, match="repository_mismatch"):
        _resolve(_Resolving(recreated), stored)


# 2. comment_prefix -------------------------------------------------------------------------


def test_gitlab_core_a_gitlab_note_is_linked_as_note_anchor_and_is_a_follow_up() -> None:
    link = f"https://{GITLAB_HOST}/{GITLAB_PATH}/-/issues/7"

    assert comment_prefix(types.GITLAB) == "#note_"
    objective = mention_objective(link, types.GITLAB, "88")
    assert objective == f"{link}#note_88"
    assert is_issue_follow_up(
        objective, issue_link=link, comment_prefix=comment_prefix(types.GITLAB)
    )


@pytest.mark.parametrize(
    ("kind", "prefix"),
    [(types.GITHUB, "#issuecomment-"), (types.JIRA_CLOUD, "?focusedCommentId=")],
)
def test_gitlab_core_github_and_jira_keep_their_comment_prefixes(kind: str, prefix: str) -> None:
    link = "https://example.test/issue/7"

    assert comment_prefix(kind) == prefix
    assert is_issue_follow_up(
        mention_objective(link, kind, "88"), issue_link=link, comment_prefix=prefix
    )


# 3. PublicationCreate ----------------------------------------------------------------------


def _publication(repo_full_name: str) -> dict[str, Any]:
    return {
        "deployment_id": str(uuid.uuid4()),
        "conversation_id": "gitlab:acme%2Fplatform%2Fwidgets:issue-7",
        "repo_full_name": repo_full_name,
        "author": "gitlab:31:dev-dana",
        "summary": "Publish the widget parser fix",
        "reply_kind": "gitlab",
        "reply_channel": GITLAB_PATH,
        "dedupe_key": f"gitlab-publication-{uuid.uuid4().hex}",
        "base_sha": "a" * 40,
        "patch_b64": "ZGlmZgo=",
        "changed_paths": ["README.md"],
    }


@pytest.mark.parametrize("path", [GITLAB_PATH, "acme/platform/team/widgets"])
def test_gitlab_core_a_publication_snapshot_accepts_a_nested_gitlab_path(path: str) -> None:
    created = PublicationCreate.model_validate(_publication(path))
    assert created.repo_full_name == path


@pytest.mark.parametrize(
    "path", ["acme", "acme//widgets", "acme/../widgets", "/acme/widgets", "acme/widgets/"]
)
def test_gitlab_core_a_publication_snapshot_still_refuses_a_malformed_path(path: str) -> None:
    with pytest.raises(ValidationError):
        PublicationCreate.model_validate(_publication(path))


def test_gitlab_core_a_publication_snapshot_still_accepts_a_github_name() -> None:
    created = PublicationCreate.model_validate(
        {**_publication("acme-corp/api"), "reply_kind": "github", "reply_channel": "acme-corp/api"}
    )
    assert created.repo_full_name == "acme-corp/api"


# 4. Answers -------------------------------------------------------------------------------
#
# https://docs.gitlab.com/api/notes/#list-project-issue-notes: a note carries
# ``system`` (true for a note GitLab writes about a change) and its ``author``;
# a project or group access token's user is ``bot: true`` on
# https://docs.gitlab.com/api/users/#get-a-single-user.

# A project access token's user: GitLab marks it ``bot: true``.
TOKEN_BOT = Actor(id="950", login="project_4242_bot_0a1b2c")


def _gitlab() -> GitLabAPI:
    fake = GitLabAPI()
    for actor in (WRITER, OUTSIDER):
        fake.add_user(actor)
        fake.set_member(actor, 30)
    fake.add_user(TOKEN_BOT, bot=True)
    fake.set_member(TOKEN_BOT, 30)
    return fake


def _answers(page: NoticePage) -> list[MarkedNotice]:
    return [n for n in page.notices if n.disposition is Disposition.ANSWER]


def _labelled(fake: GitLabAPI, title: str) -> int:
    iid = fake.add_issue(title, author=WRITER)
    fake.label(iid, LABEL, WRITER)
    return iid


def test_gitlab_core_answer_a_plain_note_on_an_answering_issue_is_an_answer() -> None:
    async def scenario() -> None:
        fake = _gitlab()
        tracker = gitlab_tracker(fake)
        asked, quiet = _labelled(fake, "Asked"), _labelled(fake, "Not asked")
        start = await tracker.poll_marked(None, running=(), answering=())
        note = fake.issue_note(asked, OUTSIDER, "Use the blue one.")
        fake.issue_note(quiet, OUTSIDER, "A plain note elsewhere.")
        mention = fake.issue_note(asked, WRITER, f"@{BOT.login} the blue one, please")

        page = await tracker.poll_marked(
            start.cursor, running=(), answering=(fake.issue_ref(asked),)
        )

        assert [(n.issue, n.event_id, n.actor, n.marker) for n in page.notices] == [
            (fake.issue_ref(asked), f"note:{note}", OUTSIDER, LABEL),
            (fake.issue_ref(asked), f"note:{mention}", WRITER, LABEL),
        ]
        assert all(n.disposition is Disposition.ANSWER for n in page.notices)
        first = page.notices[0]
        assert first.occurred_at == fake.notes[note].created_at
        assert mention_comment_id(types.GITLAB, first.event_id) == str(note)
        assert await tracker.verify_current(first) is True
        again = await tracker.poll_marked(
            page.cursor, running=(), answering=(fake.issue_ref(asked),)
        )
        assert again.notices == ()

    asyncio.run(scenario())


def test_gitlab_core_answer_a_plain_note_on_a_non_answering_issue_is_nothing() -> None:
    async def scenario() -> None:
        fake = _gitlab()
        tracker = gitlab_tracker(fake)
        iid = _labelled(fake, "Ticket")
        start = await tracker.poll_marked(None, running=(), answering=())
        fake.issue_note(iid, OUTSIDER, "Use the blue one.")
        mention = fake.issue_note(iid, WRITER, f"@{BOT.login} please look")

        page = await tracker.poll_marked(start.cursor, running=(), answering=())

        assert [(n.event_id, n.disposition) for n in page.notices] == [
            (f"note:{mention}", Disposition.MENTION)
        ]

    asyncio.run(scenario())


def test_gitlab_core_answer_the_first_poll_delivers_answers_only_on_answering_issues() -> None:
    """A label whose first page held an issue saves no cursor and polls as a
    first poll again, so a first poll delivers an answering issue's notes by
    a person as answers; elsewhere a plain note and a mention are only
    remembered."""

    async def scenario() -> None:
        fake = _gitlab()
        tracker = gitlab_tracker(fake)
        asked, quiet = _labelled(fake, "Asked"), _labelled(fake, "Not asked")
        early = fake.issue_note(asked, OUTSIDER, "An early answer.")
        fake.issue_note(asked, BOT, "Status: WORKING")
        fake.issue_note(asked, WRITER, "changed the description", system=True)
        fake.issue_note(quiet, OUTSIDER, "A plain note.")
        fake.issue_note(quiet, WRITER, f"@{BOT.login} please look")
        late = fake.issue_note(asked, WRITER, f"@{BOT.login} the blue one")

        first = await tracker.poll_marked(None, running=(), answering=(fake.issue_ref(asked),))

        assert [(n.issue, n.event_id, n.actor) for n in _answers(first)] == [
            (fake.issue_ref(asked), f"note:{early}", OUTSIDER),
            (fake.issue_ref(asked), f"note:{late}", WRITER),
        ]
        assert _answers(first)[0].occurred_at == fake.notes[early].created_at
        assert [n.disposition for n in first.notices if n.issue == fake.issue_ref(quiet)] == [
            Disposition.ADMIT
        ]
        unasked = await tracker.poll_marked(None, running=(), answering=())
        assert {n.disposition for n in unasked.notices} == {Disposition.ADMIT}
        again = await tracker.poll_marked(
            first.cursor, running=(), answering=(fake.issue_ref(asked),)
        )
        assert again.notices == ()

    asyncio.run(scenario())


def test_gitlab_core_answer_system_own_and_bot_notes_are_never_answers() -> None:
    async def scenario() -> None:
        fake = _gitlab()
        tracker = gitlab_tracker(fake)
        iid = _labelled(fake, "Ticket")
        start = await tracker.poll_marked(None, running=(), answering=())
        fake.issue_note(iid, WRITER, "changed the description", system=True)
        fake.issue_note(iid, BOT, "Status: WORKING")
        fake.issue_note(iid, TOKEN_BOT, "A token says hello.")
        person = fake.issue_note(iid, OUTSIDER, "Use the blue one.")

        page = await tracker.poll_marked(start.cursor, running=(), answering=(fake.issue_ref(iid),))

        assert [(n.event_id, n.actor) for n in _answers(page)] == [(f"note:{person}", OUTSIDER)]
        assert len(page.notices) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["closed", "unlabelled", "note_deleted"])
def test_gitlab_core_answer_is_current_only_while_open_labelled_and_standing(
    change: str,
) -> None:
    async def scenario() -> None:
        fake = _gitlab()
        tracker = gitlab_tracker(fake)
        iid = _labelled(fake, "Ticket")
        start = await tracker.poll_marked(None, running=(), answering=())
        note = fake.issue_note(iid, OUTSIDER, "Use the blue one.")
        page = await tracker.poll_marked(start.cursor, running=(), answering=(fake.issue_ref(iid),))
        (answer,) = _answers(page)
        assert await tracker.verify_current(answer) is True

        if change == "closed":
            fake.set_issue_state(iid, "closed", WRITER)
        elif change == "unlabelled":
            fake.label(iid, LABEL, WRITER, add=False)
        else:
            fake.delete_note(note)

        assert await tracker.verify_current(answer) is False

    asyncio.run(scenario())
