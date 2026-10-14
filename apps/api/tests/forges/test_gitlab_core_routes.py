"""Core sites a gitlab+gitlab install reaches through the API's routes (#4399 AC 8).

Each test boots the same gitlab-only install as the offline lifecycle
(`forge_fakes.gitlab_lifecycle`: the GitLab fake served over a socket, no
GitHub configured, no seam replaced) and drives one core site to its GitLab
answer, beside the neighbouring answer that must keep holding:

1. the lineage accepts ``<web>/<path>/-/merge_requests/<iid>`` and refuses any
   other shape, the GitHub ``/pull/<n>`` form included (``crud/lineages.py``);
2. workspace selection takes the kind from the conversation's WorkItem, and a
   GitHub-shaped path is still held to ``GITHUB_REPO_ALLOWLIST``
   (``routers/workspaces.py``, ``schemas/workspaces.py``);
3. admission does not apply the GitHub allowlist to a GitLab repository, and
   still applies it to a GitHub one (``workitem_dispatch.py``);
4. the not-adopted status note links the stored GitLab MR
   (``factory_notices.py``).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.forges import types
from curie_api.forges.identity import issue_lock_keys
from curie_api.forges.types import RepositoryRef, TrackerIssueRef
from curie_api.workitem_dispatch import Admission, DispatchConflict
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes.gitlab_lifecycle import DEVELOPER, LABEL, PROJECT_ID, PROJECT_PATH, GitLabInstall
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")


def _reconcile() -> None:
    from test_factory_terminus import _reconcile as reconcile

    reconcile()


@pytest.fixture
def install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path)


def _admitted(install: GitLabInstall) -> tuple[int, dict[str, Any], dict[str, Any]]:
    iid = install.fake.add_issue("Widget parser drops the last field", author=DEVELOPER)
    install.fake.label(iid, LABEL, DEVELOPER)
    _reconcile()
    [item] = gl.work_items(iid)
    [first] = gl.requests_of(item["id"])
    return iid, item, first


def _opened(install: GitLabInstall) -> tuple[dict[str, Any], dict[str, Any], int, dict[str, Any]]:
    """An admitted, running WorkItem whose publication opened its MR; the lineage
    has not recorded it yet. Returns the WorkItem, its request (with epoch), the
    MR iid and the publication."""

    _iid, item, first = _admitted(install)
    epoch = gl.start_request(first["id"])
    deployment_id = gl.deployment(install, item["agent_id"])
    selected = gl.select_workspace(install, deployment_id, item, first["requester"])
    assert selected.status_code == 200, selected.text
    request_row = {**gl.requests_of(item["id"])[0], "runtime_epoch": epoch}
    created = gl.publish(install, deployment_id, item, request_row)
    assert created.status_code == 201, created.text
    publication = created.json()
    gl.approve(publication["id"])
    head = install.fake.push(publication["branch"], "Fix the widget parser")
    recovered = gl.recover_pull_request(install, publication["id"], head)
    assert recovered.status_code == 200, recovered.text
    return item, request_row, int(recovered.json()["number"]), {**publication, "head": head}


# 1. crud/lineages.py: the MR URL ----------------------------------------------------------


def test_gitlab_core_lineage_accepts_a_merge_request_url_and_refuses_another_shape(
    install: GitLabInstall,
) -> None:
    item, _request, number, publication = _opened(install)
    head = publication["head"]

    for wrong in (
        f"{install.origin}/{PROJECT_PATH}/pull/{number}",
        f"{install.origin}/{PROJECT_PATH}/-/merge_requests/{number + 1}",
        f"{install.origin}/acme/platform/other/-/merge_requests/{number}",
        f"https://gitlab.invalid/{PROJECT_PATH}/-/merge_requests/{number}",
    ):
        stale = gl.advance_lineage(
            install,
            publication,
            pr_number=number,
            pr_url=wrong,
            head_sha=head,
            expected_head_sha=None,
        )
        assert stale.status_code == 409, (wrong, stale.text)
        assert "publication.lineage_stale" in stale.text, stale.text
        assert gl.publication_lineage(publication["id"])["pr_url"] is None

    accepted = gl.advance_lineage(
        install,
        publication,
        pr_number=number,
        pr_url=install.mr_url(number),
        head_sha=head,
        expected_head_sha=None,
    )
    assert accepted.status_code == 200, accepted.text
    assert gl.publication_lineage(publication["id"])["pr_url"] == install.mr_url(number)
    assert gl.lineage_of(item["id"])["pr_url"] == install.mr_url(number)


# 2. routers/workspaces.py: the selection kind -------------------------------------------


def test_gitlab_core_workspace_selection_takes_the_kind_from_the_work_item(
    install: GitLabInstall,
) -> None:
    _iid, item, first = _admitted(install)
    deployment_id = gl.deployment(install, item["agent_id"])
    stranger = {**item, "conversation_id": f"stranger-{uuid.uuid4().hex}"}

    # A conversation with no GitLab WorkItem cannot select the deep path.
    refused = gl.select_workspace(install, deployment_id, stranger, "someone")
    assert refused.status_code in {403, 422}, refused.text
    # A GitHub-shaped path is still held to the (empty) GitHub allowlist.
    github = gl.select_workspace(
        install, deployment_id, {**stranger, "repository_path": "acme-corp/widgets"}, "someone"
    )
    assert github.status_code == 403, github.text

    selected = gl.select_workspace(install, deployment_id, item, first["requester"])
    assert selected.status_code == 200, selected.text
    assert selected.json()["repo_full_name"] == PROJECT_PATH
    # Selecting again reuses the winner.
    again = gl.select_workspace(install, deployment_id, item, first["requester"])
    assert again.status_code == 200, again.text


# 3. workitem_dispatch.py: the allowlist is GitHub's ----------------------------------------


def _readmit(facts: Admission) -> Any:
    from curie_api import workitem_dispatch

    async def go() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                classid, objid = issue_lock_keys(facts.issue)
                await session.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "CAST(:classid AS integer), CAST(:objid AS integer))"
                    ),
                    {"classid": classid, "objid": objid},
                )
                result = await workitem_dispatch.readmit(session, facts)
                await session.commit()
                return result
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _facts(install: GitLabInstall, iid: int, repository: RepositoryRef) -> Admission:
    issue = TrackerIssueRef(
        types.GITLAB, install.host, str(PROJECT_ID), str(iid), f"{PROJECT_PATH}#{iid}"
    )
    return Admission(
        agent_id=install.agent_id,
        kind="gitlab",
        address=PROJECT_PATH,
        reply_conversation_id=f"issue-{iid}",
        issue=issue,
        repository=repository,
        code_host_installation_id=None,
        objective=install.issue_url(iid),
        requester=f"gitlab:{DEVELOPER.id}:{DEVELOPER.login}",
        request_id=uuid.uuid4(),
        trigger_label=LABEL,
    )


def test_gitlab_core_admission_skips_the_github_allowlist_for_a_gitlab_repository(
    install: GitLabInstall,
) -> None:
    gitlab_repository = RepositoryRef(
        types.GITLAB, install.host, str(PROJECT_ID), PROJECT_PATH, default_branch="main"
    )
    admitted = _readmit(_facts(install, 41, gitlab_repository))
    assert not isinstance(admitted, DispatchConflict), admitted
    assert len(gl.work_items(41)) == 1

    # Neighbour: a GitHub repository is still refused when the allowlist
    # (empty on this install) does not list it.
    github_repository = RepositoryRef(
        types.GITHUB, "github.com", "812345678", "acme-corp/widgets", default_branch="main"
    )
    refused = _readmit(_facts(install, 42, github_repository))
    assert isinstance(refused, DispatchConflict), refused
    assert refused.code == "repository_not_allowed"
    assert gl.work_items(42) == []


# 4. factory_notices.py: the not-adopted note links the stored MR ---------------------------


def test_gitlab_core_not_adopted_note_links_the_stored_merge_request(
    install: GitLabInstall,
) -> None:
    item, request, number, publication = _opened(install)
    advanced = gl.advance_lineage(
        install,
        publication,
        pr_number=number,
        pr_url=install.mr_url(number),
        head_sha=publication["head"],
        expected_head_sha=None,
    )
    assert advanced.status_code == 200, advanced.text
    failed = install.call(
        "POST",
        f"/v1/internal/work-items/requests/{request['id']}/finish",
        json={
            "runtime_epoch": request["runtime_epoch"],
            "outcome": "failed",
            "cause": "pull_request_not_adopted",
        },
    )
    assert failed.status_code == 200, failed.text

    _reconcile()
    _reconcile()

    iid = int(item["tracker_issue_id"])
    [note] = install.bot_notes(iid)
    assert "Cause: pull_request_not_adopted" in note.body
    assert install.mr_url(number) in note.body
    assert "/pull/" not in note.body
