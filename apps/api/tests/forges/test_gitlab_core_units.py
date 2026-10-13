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
from curie_api.forges.trackers import comment_prefix, mention_objective
from curie_api.forges.types import RepositoryRef
from curie_api.schemas.publications import PublicationCreate
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
